"""coordinator 의 지정 파일을 executor 로 옮기는 동기화 번들의 공용 순수 함수다.

동기화 대상은 설정 ``sync.paths``(앱 홈 기준 상대 경로)와 ``sync.patterns``(포함할 파일명 패턴),
``sync.exclude``(뺄 이름 패턴)로 정한다. coordinator 는 이 규칙으로 원본을 훑어 번들을 만들고, executor 는
같은 규칙으로 자기 쪽 현재 트리를 훑어 버전을 계산한다. 양쪽이 같은 함수를 쓰므로 내용이 같으면 버전도
같고, 처음 붙인 executor 가 이미 같은 파일을 갖고 있으면 아무것도 옮기지 않는다.

버전은 내용 해시다. 포함된 파일마다 ``상대경로·sha256·권한`` 을 한 줄로 만들어 경로순으로 이은 것의
sha256 앞 16자리이며, 수정 시각은 넣지 않는다(같은 내용을 다시 저장해도 버전이 바뀌지 않게).

번들은 tar.gz 하나다. 풀 때는 절대 경로·``..``·심링크·장치 파일을 거절하고, 푼 결과의 버전이 번들이
주장한 버전과 같은지 다시 확인한다. 네트워크로 받은 압축 파일이 대상 디렉터리 밖에 쓰거나 다른 내용을
담는 일을 막기 위해서다.
"""

from __future__ import annotations

import fnmatch
import hashlib
import io
import os
import stat
import tarfile
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

_CHUNK = 1024 * 1024
# 해시가 같은지만 보면 되므로 버전은 앞 16자리(64비트)만 쓴다. 디렉터리 이름으로도 쓴다.
VERSION_LEN = 16


class BundleError(ValueError):
    """번들을 만들거나 풀 수 없는 상태(잘못된 경로, 크기 초과, 버전 불일치)다."""


@dataclass(frozen=True)
class SyncRules:
    """무엇을 동기화할지 정하는 규칙이다."""

    paths: Tuple[str, ...]
    patterns: Tuple[str, ...]
    exclude: Tuple[str, ...] = ()

    @classmethod
    def from_settings(cls, settings) -> "SyncRules":
        return cls(
            tuple(normalize_rel(p) for p in getattr(settings, "sync_paths", []) or []),
            tuple(getattr(settings, "sync_patterns", []) or []),
            tuple(getattr(settings, "sync_exclude", []) or []),
        )

    @property
    def configured(self) -> bool:
        return bool(self.paths and self.patterns)

    def excluded(self, rel: str) -> bool:
        """경로의 어느 조각이든 제외 패턴에 맞으면 True 다(``/`` 가 든 패턴은 상대 경로 전체에 맞춘다)."""
        parts = rel.split("/")
        for i, name in enumerate(parts):
            sub = "/".join(parts[: i + 1])
            for p in self.exclude:
                if fnmatch.fnmatchcase(sub if "/" in p else name, p.strip("/")):
                    return True
        return False

    def included(self, rel: str) -> bool:
        name = rel.rsplit("/", 1)[-1]
        return any(fnmatch.fnmatchcase(rel if "/" in p else name, p.strip("/")) for p in self.patterns)


def normalize_rel(path: str) -> str:
    """앱 홈 기준 상대 경로를 ``a/b`` 꼴로 바꾼다. 앱 홈 밖을 가리킬 수 있는 값은 거절한다."""
    text = str(path).strip().replace("\\", "/")
    parts = [p for p in text.split("/") if p not in ("", ".")]
    if text.startswith("/") or not parts or ".." in parts:
        raise BundleError(f"sync.paths 에는 앱 홈 기준 상대 경로만 쓸 수 있다: {path!r}")
    return "/".join(parts)


@dataclass
class Manifest:
    """동기화 대상 파일 목록과 그 버전이다. files 는 상대 경로 → (sha256, mode, size, 절대 경로) 다."""

    files: Dict[str, Tuple[str, int, int, str]] = field(default_factory=dict)
    version: str = ""

    @property
    def total_bytes(self) -> int:
        return sum(v[2] for v in self.files.values())


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_version(entries: Iterable[Tuple[str, str, int]]) -> str:
    """(상대 경로, sha256, mode) 목록의 버전이다. 순서와 무관하게 같은 내용이면 같은 값이다."""
    h = hashlib.sha256()
    for rel, digest, mode in sorted(entries):
        h.update(f"{rel}\0{digest}\0{mode:o}\n".encode("utf-8"))
    return h.hexdigest()[:VERSION_LEN]


class _HashCache:
    """(크기, 수정 시각, inode) 가 같으면 sha256 을 다시 계산하지 않는다. 주기 감시가 매번 파일을 읽지 않게 한다."""

    def __init__(self) -> None:
        self._items: Dict[str, Tuple[tuple, str]] = {}

    def get(self, path: str, st: os.stat_result) -> str:
        key = (st.st_size, st.st_mtime_ns, st.st_ino)
        hit = self._items.get(path)
        if hit and hit[0] == key:
            return hit[1]
        digest = sha256_file(path)
        self._items[path] = (key, digest)
        return digest


def scan(base: str, rules: SyncRules, cache: Optional[_HashCache] = None) -> Manifest:
    """앱 홈(base) 아래 sync.paths 를 훑어 포함 패턴에 맞는 일반 파일의 목록과 버전을 만든다(읽기만 한다).

    경로 자체가 심링크여도(executor 쪽은 동기화 뒤 ``customs -> .sync/current/customs`` 가 된다) 따라가서
    내용을 본다. 그 아래의 심링크는 따라가지 않고 뺀다. 경로가 없으면 빈 것으로 본다.
    """
    cache = cache or _HashCache()
    files: Dict[str, Tuple[str, int, int, str]] = {}
    for top in rules.paths:
        root = os.path.join(base, top)
        if os.path.isfile(root):
            candidates = [(top, root)]
        elif os.path.isdir(root):
            candidates = []
            for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
                rel_dir = os.path.relpath(dirpath, base).replace(os.sep, "/")
                if rel_dir == ".":
                    rel_dir = ""
                # 정렬해 두면 결과가 실행마다 같다. 제외된 디렉터리는 들어가지 않는다.
                dirnames[:] = sorted(d for d in dirnames if not rules.excluded(f"{rel_dir}/{d}".lstrip("/")))
                for name in sorted(filenames):
                    candidates.append((f"{rel_dir}/{name}".lstrip("/"), os.path.join(dirpath, name)))
        else:
            continue
        for rel, full in candidates:
            if rules.excluded(rel) or not rules.included(rel):
                continue
            try:
                st = os.lstat(full)
            except OSError:
                continue  # 훑는 사이 지워진 파일
            if not stat.S_ISREG(st.st_mode):
                continue
            files[rel] = (cache.get(full, st), stat.S_IMODE(st.st_mode), st.st_size, full)
    manifest = Manifest(files)
    manifest.version = compute_version((rel, v[0], v[1]) for rel, v in files.items())
    return manifest


def syntax_errors(manifest: Manifest) -> List[str]:
    """포함된 ``.py`` 를 컴파일해 문법 오류를 모은다. 오류가 있으면 executor 로 보내지 않는다."""
    errors = []
    for rel, (_, _, _, full) in sorted(manifest.files.items()):
        if not rel.endswith(".py"):
            continue
        try:
            with open(full, "rb") as fp:
                compile(fp.read(), rel, "exec", dont_inherit=True)
        except SyntaxError as exc:
            errors.append(f"{rel}:{exc.lineno}: {exc.msg}")
        except (OSError, ValueError) as exc:
            errors.append(f"{rel}: {exc}")
    return errors


def build_bundle(manifest: Manifest) -> bytes:
    """목록의 파일을 tar.gz 하나로 묶는다. 시각과 소유자는 고정해 같은 내용이면 같은 바이트가 나오게 한다."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tar:
        for rel, (digest, mode, size, full) in sorted(manifest.files.items()):
            with open(full, "rb") as fp:
                data = fp.read()
            if hashlib.sha256(data).hexdigest() != digest:
                raise BundleError(f"묶는 사이 파일이 바뀌었다: {rel}")
            info = tarfile.TarInfo(rel)
            info.size, info.mode, info.mtime = len(data), mode, 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def extract_bundle(data: bytes, dest: str, rules: SyncRules, *, max_bytes: int) -> str:
    """번들을 dest(비어 있는 새 디렉터리)에 풀고 푼 내용의 버전을 돌려준다.

    sync.paths 밖의 경로, 절대 경로, ``..``, 일반 파일이 아닌 항목이 하나라도 있으면 아무것도 풀지 않고
    거절한다. 풀린 크기가 max_bytes 를 넘어도 거절한다(압축 폭탄 방지).
    """
    try:
        tar = tarfile.open(fileobj=io.BytesIO(data), mode="r:gz")
    except (tarfile.TarError, OSError, EOFError) as exc:
        raise BundleError(f"번들을 열 수 없다: {exc}") from exc
    with tar:
        members = tar.getmembers()
        total = 0
        for m in members:
            rel = m.name
            if not m.isfile():
                raise BundleError(f"일반 파일이 아닌 항목: {rel}")
            norm = normalize_rel(rel)
            if norm != rel or not any(norm == p or norm.startswith(p + "/") for p in rules.paths):
                raise BundleError(f"동기화 경로 밖의 항목: {rel}")
            total += m.size
            if total > max_bytes:
                raise BundleError(f"번들을 풀면 {max_bytes // 1048576}MB 를 넘는다")
        entries = []
        for m in members:
            target = os.path.join(dest, m.name)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            src = tar.extractfile(m)
            h = hashlib.sha256()
            with open(target, "wb") as out:
                for chunk in iter(lambda: src.read(_CHUNK), b""):
                    out.write(chunk)
                    h.update(chunk)
            mode = stat.S_IMODE(m.mode)
            os.chmod(target, mode)
            entries.append((m.name, h.hexdigest(), mode))
    return compute_version(entries)


def host_id() -> str:
    """같은 호스트인지 가리는 식별자다. machine-id 가 있으면 그것을, 없으면 호스트 이름을 쓴다."""
    import socket

    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            with open(path, encoding="ascii") as fp:
                value = fp.read().strip()
            if value:
                return value
        except OSError:
            continue
    return socket.gethostname()


def module_prefixes(paths: Sequence[str]) -> List[str]:
    """동기화 경로를 파이썬 모듈 이름 접두사로 바꾼다(``customs`` → ``customs``, ``a/b.py`` → ``a.b``)."""
    out = []
    for p in paths:
        name = p[:-3] if p.endswith(".py") else p
        out.append(name.replace("/", "."))
    return out
