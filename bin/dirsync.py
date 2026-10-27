#!/usr/bin/env python3
"""SSH 없이 HTTP 하나로 디렉터리를 서버 기준으로 맞추는 단일 파일 동기화 도구다.

배포 대상 호스트에 SSH 로 들어갈 수 없으면 ansible·rsync 를 쓸 수 없다. 그래서 iperf 처럼 한쪽은
``server`` 로 띄워 원본 디렉터리를 읽기 전용으로 내보내고, 다른 쪽은 ``client`` 로 붙어 서버의 파일·
디렉터리 목록을 받아 자기 디렉터리를 서버와 똑같이 맞춘다. 표준 라이브러리만 쓰므로 파이썬 3.9 이상이
있는 호스트에 이 파일 하나만 복사하면 된다.

    python3 dirsync.py server --base /data1/distributed-query-executor --port 9900 \\
        --exclude-dir logs --exclude '*.pyc'
    python3 dirsync.py client --server 10.0.0.1 --port 9900 \\
        --base /data1/distributed-query-executor --exclude-dir logs --exclude config.properties

클라이언트는 무엇을 만들고 바꾸고 지울지 먼저 전부 보여 준 뒤 y/n 을 묻고, 동기화하는 동안 항목별 결과와
진행 막대를 화면에 표시한다. ``-y`` 를 주면 묻지 않고, ``--dry-run`` 이면 계획만 보여 주고 끝낸다.

**서버는 어떤 경우에도 원본을 바꾸지 않는다.** 서버 쪽 코드에는 디렉터리 읽기·stat·파일 읽기만 있고 쓰기·
생성·삭제·권한이나 시각 변경 호출이 없다. 받는 요청은 GET 세 가지(목록·파일·헬스)뿐이고 다른 메서드는
405 로 거절하므로, 클라이언트가 서버에 무언가를 보낼 경로 자체가 없다. 해시 캐시도 메모리에만 두어 원본
디렉터리에 아무 파일도 만들지 않는다. 클라이언트가 실수로 서버와 같은 호스트의 같은 디렉터리를 대상으로
삼으면 시작하기 전에 거절한다.

제외 규칙은 두 가지다. ``--exclude-dir`` 은 base 기준 상대 경로로 그 디렉터리와 그 아래 전부를 뺀다.
``--exclude`` 는 fnmatch 패턴으로, ``/`` 가 없으면 경로의 각 이름에, ``/`` 가 있으면 상대 경로 전체에
맞춘다. 서버는 자기 제외 규칙을 목록에 실어 보내고 클라이언트는 이를 자기 규칙과 합쳐 쓴다. 제외된 경로는
클라이언트에서도 절대 건드리지 않으므로(복사도 삭제도 하지 않는다), 클라이언트에서 ``logs`` 지정을 빠뜨려도
로그가 지워지지 않는다.

파일이 같은지는 크기와 수정 시각(ns)으로 판단하고 ``--checksum`` 이면 sha256 까지 비교한다. 받은 파일은 같은
디렉터리의 임시 파일에 쓴 뒤 sha256 을 검증하고, 권한과 수정 시각을 서버 값으로 맞춘 다음 ``os.replace`` 로
한 번에 바꾼다. 실행 중인 프로세스가 그 파일을 열고 있어도 반쯤 쓰인 파일을 보지 않는다. 소유자는 맞추지
않는다(클라이언트를 실행한 사용자가 소유한다).
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import fnmatch
import hashlib
import http.client
import http.server
import json
import os
import shutil
import socket
import stat
import sys
import tempfile
import threading
import time
import unicodedata
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlsplit

PROTOCOL_VERSION = 1
DEFAULT_PORT = 9900
_CHUNK = 1024 * 1024
# 클라이언트가 내려받는 동안 쓰는 임시 파일 이름이다. 양쪽 모두 항상 제외한다.
_TMP_PREFIX = ".dirsync-"
_TMP_SUFFIX = ".tmp"
_BUILTIN_PATTERNS = (f"{_TMP_PREFIX}*{_TMP_SUFFIX}",)
# 전송 중 서버 파일이 바뀌어 해시가 맞지 않을 때 다시 받는 횟수다.
_DOWNLOAD_ATTEMPTS = 3

EXIT_OK, EXIT_FAILED, EXIT_ABORTED = 0, 1, 2


# ───────── 공용: 경로·제외 규칙·디렉터리 훑기(읽기 전용) ─────────

class PathError(ValueError):
    """상대 경로로 쓸 수 없는 값(절대 경로, ``..``, 빈 조각)이다."""


def normalize_rel(path: str) -> str:
    """base 기준 상대 경로를 ``a/b/c`` 꼴로 정규화한다. base 밖을 가리킬 수 있는 값은 거절한다."""
    text = str(path).strip().replace("\\", "/")
    if text.startswith("/"):
        raise PathError(f"절대 경로는 쓸 수 없다: {path}")
    parts = [p for p in text.split("/") if p not in ("", ".")]
    if not parts:
        raise PathError(f"빈 경로다: {path!r}")
    if any(p == ".." for p in parts) or any("\0" in p for p in parts):
        raise PathError(f"base 밖을 가리키는 경로다: {path}")
    return "/".join(parts)


def _split_values(values: Optional[List[str]]) -> List[str]:
    """여러 번 준 옵션과 쉼표로 나열한 값을 한 목록으로 편다."""
    out: List[str] = []
    for v in values or []:
        out.extend(x.strip() for x in v.split(",") if x.strip())
    return out


class Excludes:
    """제외할 디렉터리(base 기준 상대 경로)와 fnmatch 패턴 묶음이다."""

    def __init__(self, dirs=(), patterns=()):
        self.dirs = sorted({normalize_rel(d) for d in dirs if str(d).strip()})
        self.patterns = sorted({str(p).strip() for p in patterns if str(p).strip()} | set(_BUILTIN_PATTERNS))

    def merged(self, other: "Excludes") -> "Excludes":
        return Excludes(self.dirs + other.dirs, self.patterns + other.patterns)

    def match(self, rel: str) -> bool:
        """경로나 그 상위 경로 중 하나라도 제외 대상이면 True 다."""
        for d in self.dirs:
            if rel == d or rel.startswith(d + "/"):
                return True
        parts = rel.split("/")
        for i, name in enumerate(parts):
            sub = "/".join(parts[: i + 1])
            for p in self.patterns:
                target = sub if "/" in p else name
                if fnmatch.fnmatchcase(target, p.strip("/")):
                    return True
        return False

    def as_json(self) -> dict:
        return {"dirs": self.dirs, "patterns": [p for p in self.patterns if p not in _BUILTIN_PATTERNS]}

    @classmethod
    def from_json(cls, data: dict) -> "Excludes":
        return cls(data.get("dirs") or (), data.get("patterns") or ())

    def describe(self) -> str:
        dirs = ", ".join(self.dirs) or "없음"
        pats = ", ".join(p for p in self.patterns if p not in _BUILTIN_PATTERNS) or "없음"
        return f"디렉터리 {dirs} / 패턴 {pats}"


def scan_tree(base: str, excludes: Excludes) -> Dict[str, dict]:
    """base 아래를 심링크를 따라가지 않고 훑어 상대 경로별 항목을 돌려준다(읽기만 한다).

    항목은 ``type``(d=디렉터리, f=파일, l=심링크)·``mode``·``mtime_ns`` 와 파일이면 ``size``, 심링크면
    ``target`` 을 담는다. 제외 대상 디렉터리는 들어가지 않는다. 장치·소켓·FIFO 는 옮길 대상이 아니라 뺀다.
    """
    out: Dict[str, dict] = {}

    def walk(abs_dir: str, rel_dir: str) -> None:
        try:
            with os.scandir(abs_dir) as it:
                items = sorted(it, key=lambda e: e.name)
        except OSError as exc:
            raise OSError(f"디렉터리를 읽을 수 없다: {abs_dir}: {exc.strerror or exc}") from exc
        for entry in items:
            rel = f"{rel_dir}/{entry.name}" if rel_dir else entry.name
            if excludes.match(rel):
                continue
            st = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(st.st_mode):
                out[rel] = {"type": "l", "target": os.readlink(entry.path), "mode": 0, "mtime_ns": st.st_mtime_ns}
            elif stat.S_ISDIR(st.st_mode):
                out[rel] = {"type": "d", "mode": stat.S_IMODE(st.st_mode), "mtime_ns": st.st_mtime_ns}
                walk(entry.path, rel)
            elif stat.S_ISREG(st.st_mode):
                out[rel] = {"type": "f", "size": st.st_size, "mode": stat.S_IMODE(st.st_mode),
                            "mtime_ns": st.st_mtime_ns}
    walk(base, "")
    return out


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def host_id() -> str:
    """같은 호스트인지 가리는 식별자다. machine-id 가 있으면 그것을, 없으면 호스트 이름을 쓴다."""
    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            with open(path, encoding="ascii") as fp:
                value = fp.read().strip()
            if value:
                return value
        except OSError:
            continue
    return socket.gethostname()


def _within(path: str, base: str) -> bool:
    return path == base or path.startswith(base.rstrip("/") + "/")


# ───────── 화면 표시 ─────────

def disp_width(text: str) -> int:
    """터미널에서 차지하는 칸 수다. 한글 같은 전각 문자는 두 칸이다."""
    return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in text)


def cut(text: str, width: int) -> str:
    """표시 폭 width 안에 들어가도록 자른다."""
    out, used = [], 0
    for c in text:
        w = 2 if unicodedata.east_asian_width(c) in ("W", "F") else 1
        if used + w > width:
            break
        out.append(c)
        used += w
    return "".join(out)


def human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def human_secs(s: float) -> str:
    s = int(max(0, s))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{s % 3600 // 60:02d}m"


# ───────── 서버(원본을 읽기만 한다) ─────────

class TreeSource:
    """서버가 내보내는 원본 디렉터리다. 이 클래스의 어떤 메서드도 디스크에 쓰지 않는다.

    sha256 은 파일마다 (크기, 수정 시각, inode) 를 키로 메모리에 캐시한다. 바뀌지 않은 파일은 다시 읽지
    않으므로 클라이언트가 목록을 반복해 받아도 가볍다.
    """

    def __init__(self, base: str, excludes: Excludes):
        self.base = os.path.realpath(base)
        if not os.path.isdir(self.base):
            raise SystemExit(f"base 디렉터리가 없다: {base}")
        self.excludes = excludes
        self.host = host_id()
        self._hashes: Dict[str, Tuple[tuple, str]] = {}
        self._lock = threading.Lock()

    def _hash(self, rel: str, st: os.stat_result) -> str:
        key = (st.st_size, st.st_mtime_ns, st.st_ino)
        with self._lock:
            hit = self._hashes.get(rel)
        if hit and hit[0] == key:
            return hit[1]
        digest = sha256_file(os.path.join(self.base, rel))
        with self._lock:
            self._hashes[rel] = (key, digest)
        return digest

    def manifest(self) -> dict:
        entries = scan_tree(self.base, self.excludes)
        listed = []
        for rel, e in entries.items():
            item = {"path": rel, **e}
            if e["type"] == "f":
                try:
                    item["sha256"] = self._hash(rel, os.stat(os.path.join(self.base, rel), follow_symlinks=False))
                except OSError:
                    continue  # 목록을 만드는 사이 지워진 파일
            listed.append(item)
        with self._lock:  # 사라진 파일의 캐시는 버린다
            for rel in set(self._hashes) - set(entries):
                self._hashes.pop(rel, None)
        return {
            "version": PROTOCOL_VERSION, "host_id": self.host, "hostname": socket.gethostname(),
            "base": self.base, "excludes": self.excludes.as_json(), "entries": listed,
        }

    def open_file(self, raw_path: str):
        """요청 경로의 일반 파일을 읽기 전용으로 연다. 열 수 없는 경로면 (HTTP 코드, 사유) 를 돌려준다."""
        try:
            rel = normalize_rel(raw_path)
        except PathError as exc:
            return None, (400, str(exc))
        if self.excludes.match(rel):
            return None, (404, "제외 대상")
        full = os.path.join(self.base, rel)
        # 상위 경로 어딘가의 심링크가 base 밖을 가리키면 거절한다.
        if not _within(os.path.realpath(os.path.dirname(full)), self.base):
            return None, (404, "base 밖")
        try:
            st = os.stat(full, follow_symlinks=False)
        except OSError:
            return None, (404, "없음")
        if not stat.S_ISREG(st.st_mode):
            return None, (404, "일반 파일이 아님")
        try:
            fp = open(full, "rb")
        except OSError as exc:
            return None, (403, f"읽을 수 없음: {exc.strerror}")
        st = os.fstat(fp.fileno())
        return (rel, fp, st), None


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "dirsync/1"
    source: TreeSource  # make_server 가 채운다

    def setup(self):
        super().setup()
        # 응답 머리와 본문을 따로 쓰므로, Nagle 알고리즘이 켜져 있으면 클라이언트의 지연 ACK 와 맞물려 작은
        # 파일마다 약 40ms 씩 멈춘다(파일 수백 개면 수십 초). 바로 보내도록 끈다.
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

    def log_message(self, fmt, *args):  # noqa: N802 — 표준 이름
        sys.stderr.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {self.client_address[0]} {fmt % args}\n")

    def _send_json(self, code: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        url = urlsplit(self.path)
        if url.path == "/health":
            self._send_json(200, {"ok": True, "version": PROTOCOL_VERSION, "base": self.source.base})
        elif url.path == "/manifest":
            try:
                self._send_json(200, self.source.manifest())
            except OSError as exc:
                self._send_json(500, {"error": str(exc)})
        elif url.path == "/file":
            self._send_file((parse_qs(url.query).get("path") or [""])[0])
        else:
            self._send_json(404, {"error": "알 수 없는 경로"})

    def _send_file(self, raw_path: str) -> None:
        opened, err = self.source.open_file(raw_path)
        if err:
            self._send_json(err[0], {"error": err[1], "path": raw_path})
            return
        rel, fp, st = opened
        with fp:
            try:
                digest = self.source._hash(rel, st)
            except OSError as exc:
                self._send_json(500, {"error": str(exc)})
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(st.st_size))
            self.send_header("X-Sha256", digest)
            self.send_header("X-Mode", str(stat.S_IMODE(st.st_mode)))
            self.send_header("X-Mtime-Ns", str(st.st_mtime_ns))
            self.end_headers()
            remaining = st.st_size
            while remaining > 0:
                chunk = fp.read(min(_CHUNK, remaining))
                if not chunk:  # 보내는 사이 파일이 줄었다. 길이가 모자라면 클라이언트가 실패로 보고 다시 받는다.
                    self.close_connection = True
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)

    def _deny(self):
        # 읽기 전용 서버다. 본문을 읽지 않고 연결을 닫아 다음 요청과 섞이지 않게 한다.
        self.close_connection = True
        data = b'{"error": "read-only server: GET only"}'
        self.send_response(405)
        self.send_header("Allow", "GET")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = _deny  # noqa: N815


def make_server(base: str, excludes: Excludes, bind: str = "0.0.0.0", port: int = DEFAULT_PORT):
    source = TreeSource(base, excludes)
    handler = type("Handler", (_Handler,), {"source": source})
    server = http.server.ThreadingHTTPServer((bind, port), handler)
    server.daemon_threads = True
    return server


def run_server(args) -> int:
    excludes = Excludes(_split_values(args.exclude_dir), _split_values(args.exclude))
    server = make_server(args.base, excludes, args.bind, args.port)
    source = server.RequestHandlerClass.source
    entries = scan_tree(source.base, excludes)
    files = [e for e in entries.values() if e["type"] == "f"]
    print(f"dirsync 서버  {args.bind}:{server.server_address[1]}  (읽기 전용, GET 만 받는다)")
    print(f"  base   {source.base}")
    print(f"  제외   {excludes.describe()}")
    print(f"  대상   파일 {len(files)}개, 디렉터리 {sum(e['type'] == 'd' for e in entries.values())}개, "
          f"{human_size(sum(e['size'] for e in files))}")
    print("  종료   Ctrl+C", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버를 멈춘다.")
    finally:
        server.server_close()
    return EXIT_OK


# ───────── 클라이언트 ─────────

class ServerError(RuntimeError):
    pass


class Remote:
    """서버와의 HTTP 연결 하나다. keep-alive 로 재사용하고 끊기면 한 번 다시 붙는다."""

    def __init__(self, host: str, port: int, timeout: float):
        self.host, self.port, self.timeout = host, port, timeout
        self._conn: Optional[http.client.HTTPConnection] = None

    def _request(self, path: str) -> http.client.HTTPResponse:
        for attempt in (1, 2):
            if self._conn is None:
                self._conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            try:
                self._conn.request("GET", path)
                return self._conn.getresponse()
            except (http.client.HTTPException, ConnectionError, socket.timeout, OSError) as exc:
                self.close()
                if attempt == 2:
                    raise ServerError(f"서버 {self.host}:{self.port} 에 연결할 수 없다: {exc}") from exc
        raise AssertionError("unreachable")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def manifest(self) -> dict:
        resp = self._request("/manifest")
        body = resp.read()
        if resp.status != 200:
            raise ServerError(f"목록 요청 실패({resp.status}): {body[:200].decode('utf-8', 'replace')}")
        data = json.loads(body.decode("utf-8"))
        if data.get("version") != PROTOCOL_VERSION:
            raise ServerError(f"서버 프로토콜 버전이 다르다(서버 {data.get('version')}, 클라이언트 {PROTOCOL_VERSION})")
        return data

    def fetch(self, rel: str, out, on_bytes) -> dict:
        """파일 하나를 out 에 쓰며 sha256 을 계산한다. 서버가 알린 메타데이터와 계산한 해시를 돌려준다."""
        resp = self._request("/file?path=" + quote(rel, safe=""))
        if resp.status != 200:
            body = resp.read()
            raise ServerError(f"{resp.status} {body[:200].decode('utf-8', 'replace')}")
        expected = int(resp.getheader("Content-Length") or -1)
        h = hashlib.sha256()
        got = 0
        try:
            while True:
                chunk = resp.read(_CHUNK)
                if not chunk:
                    break
                out.write(chunk)
                h.update(chunk)
                got += len(chunk)
                on_bytes(len(chunk))
        except (http.client.HTTPException, OSError) as exc:
            self.close()
            raise ServerError(f"전송이 끊겼다: {exc}") from exc
        if got != expected:
            self.close()
            raise ServerError(f"받은 크기 {got} 가 알린 크기 {expected} 와 다르다")
        return {"sha256": resp.getheader("X-Sha256"), "actual": h.hexdigest(), "size": got,
                "mode": int(resp.getheader("X-Mode") or 0o644), "mtime_ns": int(resp.getheader("X-Mtime-Ns") or 0)}


class Action:
    """계획의 항목 하나다. kind 는 mkdir·add·update·link·chmod·delete_file·delete_dir 중 하나다."""

    LABELS = {
        "mkdir": ("+", "디렉터리 생성"), "add": ("+", "파일 추가"), "update": ("~", "파일 갱신"),
        "link": ("+", "링크 생성"), "relink": ("~", "링크 변경"), "chmod": ("m", "권한 변경"),
        "delete_file": ("-", "파일 삭제"), "delete_dir": ("-", "디렉터리 삭제"),
    }

    __slots__ = ("kind", "path", "remote", "local", "note")

    def __init__(self, kind: str, path: str, remote: Optional[dict] = None, local: Optional[dict] = None,
                 note: str = ""):
        self.kind, self.path, self.remote, self.local, self.note = kind, path, remote, local, note

    @property
    def size(self) -> int:
        return int(self.remote.get("size") or 0) if self.kind in ("add", "update") and self.remote else 0

    def label(self) -> str:
        mark, text = self.LABELS[self.kind]
        return f"[{mark}] {text}"

    def detail(self) -> str:
        if self.kind in ("add", "update"):
            return human_size(self.size)
        if self.kind == "chmod":
            return f"{self.local['mode']:o} → {self.remote['mode']:o}"
        if self.kind in ("link", "relink"):
            return f"→ {self.remote['target']}"
        return self.note


def _depth_key(rel: str) -> tuple:
    return tuple(rel.split("/"))


def build_plan(remote: Dict[str, dict], local: Dict[str, dict], *, delete: bool = True,
               checksum: bool = False, local_base: str = "") -> List[Action]:
    """서버 목록과 로컬 목록을 비교해 실행 순서대로 정렬한 계획을 만든다.

    순서는 삭제(깊은 경로부터) → 생성·갱신(얕은 경로부터) → 권한 변경이다. 종류가 바뀐 경로(파일이
    디렉터리가 되는 등)는 로컬 것을 먼저 지우고 서버 것을 새로 만드는 두 항목이 된다. 디렉터리의 수정 시각은
    하위 항목이 바뀔 때마다 움직이므로 비교하지 않는다.
    """
    deletes: List[Action] = []
    creates: List[Action] = []
    chmods: List[Action] = []
    for rel in sorted(remote, key=_depth_key):
        r, l = remote[rel], local.get(rel)
        if l is not None and l["type"] != r["type"]:
            kind = "delete_dir" if l["type"] == "d" else "delete_file"
            deletes.append(Action(kind, rel, local=l, note="종류 변경"))
            l = None
        if l is None:
            kind = {"d": "mkdir", "f": "add", "l": "link"}[r["type"]]
            creates.append(Action(kind, rel, remote=r))
        elif r["type"] == "f":
            changed = r["size"] != l["size"] or r["mtime_ns"] != l["mtime_ns"]
            if not changed and checksum and r.get("sha256"):
                changed = sha256_file(os.path.join(local_base, rel)) != r["sha256"]
            if changed:
                creates.append(Action("update", rel, remote=r, local=l))
            elif r["mode"] != l["mode"]:
                chmods.append(Action("chmod", rel, remote=r, local=l))
        elif r["type"] == "l":
            if r["target"] != l["target"]:
                creates.append(Action("relink", rel, remote=r, local=l))
        elif r["mode"] != l["mode"]:
            chmods.append(Action("chmod", rel, remote=r, local=l))
    if delete:
        for rel, l in local.items():
            if rel not in remote:
                deletes.append(Action("delete_dir" if l["type"] == "d" else "delete_file", rel, local=l))
    deletes.sort(key=lambda a: _depth_key(a.path), reverse=True)
    return deletes + creates + chmods


class Progress:
    """완료한 항목을 한 줄씩 쌓고, 맨 아래 한 줄에 진행 막대와 받는 중인 파일을 덮어쓰며 갱신한다.

    터미널이 아니면(파일로 리다이렉트) 막대 없이 완료 줄만 남긴다.
    """

    def __init__(self, total_items: int, total_bytes: int, out=None):
        self.out = out or sys.stdout
        self.tty = bool(getattr(self.out, "isatty", lambda: False)())
        self.total_items, self.total_bytes = total_items, total_bytes
        self.items = self.bytes = 0
        self.started = time.monotonic()
        self._last_draw = 0.0
        self._current: Optional[Tuple[str, int, int]] = None
        self._width = shutil.get_terminal_size((100, 20)).columns

    def _status(self) -> str:
        elapsed = max(1e-6, time.monotonic() - self.started)
        frac = self.bytes / self.total_bytes if self.total_bytes else (
            self.items / self.total_items if self.total_items else 1.0)
        bar_w = 24
        filled = int(bar_w * min(1.0, frac))
        speed = self.bytes / elapsed
        eta = (self.total_bytes - self.bytes) / speed if speed > 0 and self.total_bytes else 0
        text = (f"[{'#' * filled}{'.' * (bar_w - filled)}] {frac * 100:3.0f}%  "
                f"{self.items}/{self.total_items}  {human_size(self.bytes)}/{human_size(self.total_bytes)}  "
                f"{human_size(speed)}/s  남은 {human_secs(eta)}")
        if self._current:
            name, done, total = self._current
            text += f"  ↳ {name} ({human_size(done)}/{human_size(total)})"
        return cut(text, self._width - 1)

    def _draw(self, force: bool = False) -> None:
        if not self.tty:
            return
        now = time.monotonic()
        if not force and now - self._last_draw < 0.1:
            return
        self._last_draw = now
        self.out.write("\r\033[K" + self._status())
        self.out.flush()

    def start_file(self, name: str, total: int) -> None:
        self._current = (name, 0, total)
        self._draw(force=True)

    def add_bytes(self, n: int) -> None:
        self.bytes += n
        if self._current:
            name, done, total = self._current
            self._current = (name, done + n, total)
        self._draw()

    def done(self, action: Action, ok: bool = True, reason: str = "") -> None:
        self.items += 1
        self._current = None
        width = len(str(self.total_items))
        mark = action.label() if ok else "[!] 실패"
        tail = action.detail() if ok else reason
        line = f"[{self.items:>{width}}/{self.total_items}] {mark}  {action.path}" + (f"  {tail}" if tail else "")
        if self.tty:
            self.out.write("\r\033[K")
        self.out.write(line + "\n")
        self._draw(force=True)

    def finish(self) -> None:
        if self.tty:
            self.out.write("\r\033[K")
            self.out.flush()


class Applier:
    """계획을 로컬 디렉터리에 적용한다. 쓰기는 모두 이 클래스에서만 일어난다(서버 쪽에는 쓰기 코드가 없다)."""

    def __init__(self, base: str, remote: Remote, progress: Progress):
        self.base = base
        self.remote = remote
        self.progress = progress
        # 하위 항목을 쓰려고 잠시 쓰기 권한을 연 디렉터리와, 끝에 서버 권한으로 맞출 디렉터리다.
        self._dir_modes: Dict[str, int] = {}

    def _abs(self, rel: str) -> str:
        full = os.path.join(self.base, rel)
        # 상위 경로가 심링크로 base 밖을 가리키면 쓰지 않는다(목록이 오염됐거나 로컬에 심링크가 남은 경우).
        if not _within(os.path.realpath(os.path.dirname(full)), self.base):
            raise OSError(errno.EPERM, "상위 경로가 base 밖을 가리킨다")
        return full

    def _writable_parent(self, rel: str) -> str:
        parent = os.path.dirname(self._abs(rel))
        if not os.access(parent, os.W_OK):
            mode = stat.S_IMODE(os.stat(parent).st_mode)
            os.chmod(parent, mode | stat.S_IRWXU)
            prel = os.path.relpath(parent, self.base)
            self._dir_modes.setdefault(prel, mode)
        return parent

    def apply(self, action: Action) -> None:
        kind, rel = action.kind, action.path
        if kind == "delete_file":
            self._writable_parent(rel)
            os.unlink(self._abs(rel))
        elif kind == "delete_dir":
            self._writable_parent(rel)
            try:
                os.rmdir(self._abs(rel))
            except OSError as exc:
                if exc.errno in (errno.ENOTEMPTY, errno.EEXIST):
                    raise OSError(exc.errno, "제외 항목이나 남은 파일이 있어 지우지 않았다") from exc
                raise
        elif kind == "mkdir":
            self._writable_parent(rel)
            os.mkdir(self._abs(rel), 0o700)
            self._dir_modes[rel] = action.remote["mode"]
        elif kind in ("add", "update"):
            self._download(action)
        elif kind in ("link", "relink"):
            parent = self._writable_parent(rel)
            tmp = os.path.join(parent, f"{_TMP_PREFIX}{os.getpid()}-{threading.get_ident()}{_TMP_SUFFIX}")
            os.symlink(action.remote["target"], tmp)
            try:
                os.replace(tmp, self._abs(rel))
            except BaseException:
                os.unlink(tmp)
                raise
        elif kind == "chmod":
            if action.remote["type"] == "d":
                self._dir_modes[rel] = action.remote["mode"]
            else:
                os.chmod(self._abs(rel), action.remote["mode"])

    def _download(self, action: Action) -> None:
        rel = action.path
        dest = self._abs(rel)
        parent = self._writable_parent(rel)
        last_error = ""
        for _ in range(_DOWNLOAD_ATTEMPTS):
            fd, tmp = tempfile.mkstemp(prefix=_TMP_PREFIX, suffix=_TMP_SUFFIX, dir=parent)
            sent = 0

            def on_bytes(n):
                nonlocal sent
                sent += n
                self.progress.add_bytes(n)

            try:
                self.progress.start_file(rel, action.size)
                with os.fdopen(fd, "wb") as out:
                    meta = self.remote.fetch(rel, out, on_bytes)
                if meta["sha256"] and meta["sha256"] != meta["actual"]:
                    raise ServerError("받는 사이 서버 파일이 바뀌어 해시가 맞지 않는다")
                os.chmod(tmp, meta["mode"])
                os.utime(tmp, ns=(meta["mtime_ns"], meta["mtime_ns"]))
                os.replace(tmp, dest)
                # 목록 이후 서버 파일이 바뀌었으면 크기가 다를 수 있다. 진행률 합계를 실제 받은 양에 맞춘다.
                self.progress.total_bytes += meta["size"] - action.size
                return
            except ServerError as exc:
                last_error = str(exc)
                self.progress.bytes -= sent  # 다시 받으므로 이번 시도분은 진행률에서 뺀다
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
        raise ServerError(last_error)

    def finish(self) -> List[str]:
        """미뤄 둔 디렉터리 권한을 깊은 경로부터 서버 값으로 맞춘다. 실패한 경로의 사유를 돌려준다."""
        errors = []
        for rel in sorted(self._dir_modes, key=_depth_key, reverse=True):
            try:
                os.chmod(os.path.join(self.base, rel), self._dir_modes[rel])
            except OSError as exc:
                errors.append(f"{rel}: 권한을 맞추지 못했다({exc.strerror or exc})")
        return errors


def _print_plan(manifest: dict, base: str, excludes: Excludes, plan: List[Action], args, out) -> None:
    entries = manifest["entries"]
    files = [e for e in entries if e["type"] == "f"]
    out.write(f"서버  {args.server}:{args.port}  {manifest.get('hostname', '')}:{manifest['base']}  "
              f"(파일 {len(files)}개, 디렉터리 {sum(e['type'] == 'd' for e in entries)}개, "
              f"{human_size(sum(e['size'] for e in files))})\n")
    out.write(f"로컬  {base}\n")
    out.write(f"제외  {excludes.describe()}\n\n")
    limit = None if args.verbose else args.show
    shown = plan if limit is None else plan[:limit]
    label_w = max((disp_width(a.label()) for a in shown), default=0)
    path_w = min(70, max((len(a.path) for a in shown), default=0))
    for a in shown:
        label = a.label() + " " * (label_w - disp_width(a.label()))
        detail = a.detail()
        out.write(f"  {label}  {a.path.ljust(path_w)}  {detail}".rstrip() + "\n")
    if limit is not None and len(plan) > limit:
        out.write(f"  ... 외 {len(plan) - limit}건 (-v 로 전체 목록)\n")
    counts: Dict[str, int] = {}
    for a in plan:
        counts[a.kind] = counts.get(a.kind, 0) + 1
    parts = [f"{Action.LABELS[k][1]} {counts[k]}" for k in Action.LABELS if counts.get(k)]
    total = sum(a.size for a in plan)
    out.write(f"\n요약: {' · '.join(parts)}   전송 {human_size(total)}\n")


def _stdin_is_tty() -> bool:
    return sys.stdin is not None and sys.stdin.isatty()


def _confirm(prompt: str) -> bool:
    try:
        answer = input(prompt)
    except EOFError:
        return False
    return answer.strip().lower() in ("y", "yes")


class _Lock:
    """같은 로컬 base 에 클라이언트가 겹쳐 돌지 않게 한다. lock 파일은 base 밖(임시 디렉터리)에 둔다."""

    def __init__(self, base: str):
        digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
        self.path = os.path.join(tempfile.gettempdir(), f"dirsync-{digest}.lock")
        self._fp = None

    def __enter__(self):
        self._fp = open(self.path, "w")
        try:
            fcntl.flock(self._fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._fp.close()
            raise SystemExit("같은 디렉터리를 동기화하는 클라이언트가 이미 돌고 있다")
        return self

    def __exit__(self, *exc):
        fcntl.flock(self._fp, fcntl.LOCK_UN)
        self._fp.close()


def run_client(args, out=None) -> int:
    out = out or sys.stdout
    base = os.path.realpath(args.base)
    if os.path.exists(base) and not os.path.isdir(base):
        out.write(f"로컬 base 가 디렉터리가 아니다: {base}\n")
        return EXIT_ABORTED
    remote = Remote(args.server, args.port, args.timeout)
    try:
        manifest = remote.manifest()
    except ServerError as exc:
        out.write(f"{exc}\n")
        return EXIT_ABORTED

    # 서버 원본 위에서 클라이언트를 돌리면 원본이 바뀐다. 같은 호스트에서 경로가 겹치면 시작 전에 거절한다.
    if manifest.get("host_id") == host_id() and (
        _within(base, manifest["base"]) or _within(manifest["base"], base)
    ):
        out.write(f"서버 원본({manifest['base']})과 같은 호스트의 겹치는 디렉터리라 동기화하지 않는다: {base}\n")
        return EXIT_ABORTED

    excludes = Excludes(_split_values(args.exclude_dir), _split_values(args.exclude)).merged(
        Excludes.from_json(manifest.get("excludes") or {}))
    remote_entries: Dict[str, dict] = {}
    for item in manifest["entries"]:
        try:
            rel = normalize_rel(item["path"])
        except PathError as exc:
            out.write(f"서버 목록에 쓸 수 없는 경로가 있어 중단한다: {exc}\n")
            return EXIT_ABORTED
        if not excludes.match(rel):
            remote_entries[rel] = item

    with _Lock(base):
        local_entries = scan_tree(base, excludes) if os.path.isdir(base) else {}
        if args.checksum:
            out.write("체크섬 비교 중...\n")
        plan = build_plan(remote_entries, local_entries, delete=not args.no_delete,
                          checksum=args.checksum, local_base=base)
        if not os.path.isdir(base):
            out.write(f"로컬 base 가 없어 새로 만든다: {base}\n")
        if not plan:
            out.write("이미 서버와 같다. 바꿀 것이 없다.\n")
            return EXIT_OK
        _print_plan(manifest, base, excludes, plan, args, out)

        # 서버 목록이 비었거나 로컬 대부분을 지우게 되면 base 를 잘못 짚었을 가능성이 크다.
        deletes = sum(a.kind.startswith("delete") for a in plan)
        risky = local_entries and (not remote_entries or (len(local_entries) >= 20
                                                           and deletes > len(local_entries) / 2))
        if risky:
            out.write(f"\n경고: 로컬 항목 {len(local_entries)}개 중 {deletes}개를 지운다. base 와 제외 규칙을 확인하라.\n")
        if args.dry_run:
            out.write("\n--dry-run 이라 바꾸지 않았다.\n")
            return EXIT_OK
        if args.yes:
            if risky and not args.force:
                out.write("삭제가 많아 -y 만으로는 진행하지 않는다. 확인했다면 --force 를 함께 준다.\n")
                return EXIT_ABORTED
        else:
            if not _stdin_is_tty():
                out.write("확인 입력을 받을 수 없다(터미널 아님). 묻지 않고 진행하려면 -y 를 준다.\n")
                return EXIT_ABORTED
            out.flush()
            if not _confirm("\n동기화를 진행할까요? [y/N] "):
                out.write("취소했다. 아무것도 바꾸지 않았다.\n")
                return EXIT_ABORTED

        os.makedirs(base, exist_ok=True)
        out.write("\n")
        progress = Progress(len(plan), sum(a.size for a in plan), out)
        applier = Applier(base, remote, progress)
        failures: List[Tuple[Action, str]] = []
        started = time.monotonic()
        failed_dirs: List[str] = []
        for action in plan:
            # 만들지 못한 디렉터리 아래 항목은 시도하지 않고 같은 사유로 실패 처리한다.
            blocked = next((d for d in failed_dirs if action.path.startswith(d + "/")), None)
            if blocked:
                reason = f"상위 디렉터리 {blocked} 를 만들지 못했다"
                failures.append((action, reason))
                progress.done(action, ok=False, reason=reason)
                continue
            try:
                applier.apply(action)
            except (OSError, ServerError) as exc:
                reason = getattr(exc, "strerror", None) or str(exc)
                failures.append((action, reason))
                if action.kind == "mkdir":
                    failed_dirs.append(action.path)
                progress.done(action, ok=False, reason=reason)
            else:
                progress.done(action)
        late_errors = applier.finish()
        progress.finish()
        remote.close()

    counts: Dict[str, int] = {}
    failed = {id(a) for a, _ in failures}
    for a in plan:
        if id(a) not in failed:
            counts[a.kind] = counts.get(a.kind, 0) + 1
    parts = [f"{Action.LABELS[k][1]} {counts[k]}" for k in Action.LABELS if counts.get(k)]
    out.write(f"\n완료: {' · '.join(parts) or '변경 없음'} · 실패 {len(failures)}   "
              f"{human_size(progress.bytes)}, {time.monotonic() - started:.1f}초\n")
    for a, reason in failures:
        out.write(f"  실패  {a.label()}  {a.path}  — {reason}\n")
    for msg in late_errors:
        out.write(f"  실패  {msg}\n")
    return EXIT_FAILED if failures or late_errors else EXIT_OK


# ───────── 명령행 ─────────

def _add_excludes(p: argparse.ArgumentParser) -> None:
    p.add_argument("--exclude-dir", action="append", metavar="DIR",
                   help="제외할 디렉터리(base 기준 상대 경로, 여러 번 또는 쉼표로 나열). 그 아래 전부를 뺀다")
    p.add_argument("--exclude", action="append", metavar="PATTERN",
                   help="제외할 이름 패턴(fnmatch, 여러 번 또는 쉼표로 나열). '/' 가 있으면 상대 경로 전체에 맞춘다")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=os.environ.get("PROG_NAME") or "dirsync.py",
        description="SSH 없이 HTTP 로 디렉터리를 서버 기준으로 동기화한다(서버는 읽기 전용).",
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    s = sub.add_parser("server", help="원본 디렉터리를 읽기 전용으로 내보낸다")
    s.add_argument("--base", required=True, help="내보낼 원본 디렉터리")
    s.add_argument("--bind", default="0.0.0.0", help="수신 주소(기본 0.0.0.0)")
    s.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"수신 포트(기본 {DEFAULT_PORT})")
    _add_excludes(s)

    c = sub.add_parser("client", help="서버에 붙어 로컬 디렉터리를 서버와 같게 맞춘다")
    c.add_argument("--server", required=True, help="서버 IP 또는 호스트 이름")
    c.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"서버 포트(기본 {DEFAULT_PORT})")
    c.add_argument("--base", required=True, help="맞출 로컬 디렉터리(없으면 만든다)")
    _add_excludes(c)
    c.add_argument("--no-delete", action="store_true", help="서버에 없는 로컬 항목을 지우지 않는다")
    c.add_argument("--checksum", action="store_true", help="크기·수정 시각이 같아도 sha256 으로 다시 비교한다")
    c.add_argument("--dry-run", action="store_true", help="계획만 보여 주고 바꾸지 않는다")
    c.add_argument("-y", "--yes", action="store_true", help="묻지 않고 진행한다")
    c.add_argument("--force", action="store_true", help="-y 와 함께, 삭제가 많아도 진행한다")
    c.add_argument("--show", type=int, default=200, help="계획 화면에 보여 줄 최대 항목 수(기본 200)")
    c.add_argument("--timeout", type=float, default=30.0, help="서버 응답 대기 상한(초, 기본 30)")
    c.add_argument("-v", "--verbose", action="store_true", help="계획을 전부 보여 준다")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.mode == "server":
            return run_server(args)
        return run_client(args)
    except PathError as exc:
        print(f"잘못된 경로: {exc}", file=sys.stderr)
        return EXIT_ABORTED
    except KeyboardInterrupt:
        print("\n중단했다. 받던 파일의 임시 파일은 지웠고, 이미 끝난 항목은 반영된 채로 남는다.", file=sys.stderr)
        return EXIT_ABORTED


if __name__ == "__main__":
    sys.exit(main())
