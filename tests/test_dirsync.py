"""bin/dirsync.py(서버 기준 디렉터리 동기화)를 실제 HTTP 서버 스레드와 임시 디렉터리 두 개로 검증한다."""

from __future__ import annotations

import hashlib
import http.client
import importlib.util
import io
import json
import os
import threading
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location("dirsync", Path(__file__).resolve().parents[1] / "bin" / "dirsync.py")
ds = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(ds)


def _write(path: Path, text: str, mode: int = 0o644, mtime: int = 1_700_000_000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    os.chmod(path, mode)
    os.utime(path, (mtime, mtime))


def _snapshot(root: Path) -> dict:
    """트리의 모든 항목을 (종류, mode, mtime, 내용 해시 또는 링크 대상) 으로 떠 둔다. 디렉터리 mtime 도 포함한다."""
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            p = Path(dirpath) / name
            st = p.lstat()
            rel = str(p.relative_to(root))
            if p.is_symlink():
                out[rel] = ("l", os.readlink(p))
            elif p.is_dir():
                out[rel] = ("d", st.st_mode, st.st_mtime_ns)
            else:
                out[rel] = ("f", st.st_mode, st.st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
    st = root.stat()
    out["."] = ("d", st.st_mode, st.st_mtime_ns)
    return out


def _mirror_view(root: Path, skip=()) -> dict:
    """동기화 결과 비교용 뷰다(디렉터리 mtime 은 동기화 대상이 아니라 뺀다)."""
    snap = _snapshot(root)
    snap.pop(".")
    view = {}
    for rel, v in snap.items():
        if any(rel == s or rel.startswith(s + "/") for s in skip):
            continue
        view[rel] = v[:2] if v[0] == "d" else v
    return view


@pytest.fixture()
def src(tmp_path):
    root = tmp_path / "src"
    _write(root / "bin/start.sh", "#!/bin/sh\necho hi\n", 0o755)
    _write(root / "src/core/a.py", "A = 1\n")
    _write(root / "src/core/b.py", "B = 2\n")
    _write(root / "src/core/__pycache__/a.cpython-39.pyc", "junk")
    _write(root / "config/config.properties", "dsn=server\n", 0o640)
    _write(root / "logs/server.log", "server log\n")
    _write(root / "big.bin", "x" * 3_000_000)
    (root / "empty").mkdir()
    os.symlink("src/core", root / "core-link")
    return root


@pytest.fixture()
def server(src):
    srv = ds.make_server(str(src), ds.Excludes(["logs"], ["__pycache__"]), "127.0.0.1", 0)
    t = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    t.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def _client(server, dst, *extra, answer=None, monkeypatch=None):
    out = io.StringIO()
    argv = ["client", "--server", "127.0.0.1", "--port", str(server.server_address[1]), "--base", str(dst), *extra]
    args = ds.build_parser().parse_args(argv)
    if answer is not None:
        monkeypatch.setattr(ds, "_stdin_is_tty", lambda: True)
        monkeypatch.setattr("builtins.input", lambda prompt="": answer)
    code = ds.run_client(args, out)
    return code, out.getvalue()


# ───────── 제외 규칙·경로 ─────────

def test_제외_규칙():
    ex = ds.Excludes(["logs", "./data/tmp/"], ["*.pyc", "__pycache__", "config/*.bak"])
    assert ex.match("logs") and ex.match("logs/a/b.log")
    assert not ex.match("logsx") and not ex.match("src/logs")  # 디렉터리는 base 기준 상대 경로다
    assert ex.match("data/tmp/x") and not ex.match("data/tmpx")
    assert ex.match("src/a.pyc") and ex.match("src/__pycache__/x.py")
    assert ex.match("config/x.bak") and not ex.match("other/x.bak")
    assert ex.match("a/.dirsync-123.tmp")  # 임시 파일은 늘 뺀다
    merged = ex.merged(ds.Excludes(["cache"], ["*.swp"]))
    assert merged.match("cache/x") and merged.match("a.swp") and merged.match("logs/x")


@pytest.mark.parametrize("bad", ["/etc/passwd", "../x", "a/../../x", "", "."])
def test_base_밖을_가리키는_경로는_거절한다(bad):
    with pytest.raises(ds.PathError):
        ds.normalize_rel(bad)
    assert ds.normalize_rel("./a//b/") == "a/b"


# ───────── 동기화 ─────────

def test_빈_디렉터리에_서버와_같게_만든다(server, src, tmp_path, monkeypatch):
    dst = tmp_path / "dst"
    code, out = _client(server, dst, "-y")
    assert code == ds.EXIT_OK, out
    assert _mirror_view(dst) == _mirror_view(src, skip=("logs", "src/core/__pycache__"))
    assert "완료:" in out and "실패 0" in out
    assert "[+] 파일 추가" in out and "[+] 링크 생성" in out and "big.bin" in out
    # 다시 돌리면 바꿀 것이 없다(mtime 을 서버 값으로 맞췄기 때문이다).
    code, out = _client(server, dst, "-y")
    assert code == ds.EXIT_OK and "바꿀 것이 없다" in out


def test_갱신_삭제_종류변경_권한을_서버에_맞추고_제외_항목은_건드리지_않는다(server, src, tmp_path):
    dst = tmp_path / "dst"
    assert _client(server, dst, "-y")[0] == ds.EXIT_OK
    _write(dst / "src/core/a.py", "A = 999  # 로컬에서 고침\n")  # 갱신 대상
    _write(dst / "stale.txt", "서버에 없음\n")                  # 삭제 대상
    _write(dst / "old/deep/x.txt", "x")                          # 디렉터리째 삭제 대상
    (dst / "empty").rmdir()
    _write(dst / "empty", "이제 파일")                           # 종류 변경: 파일 → 디렉터리
    os.chmod(dst / "bin/start.sh", 0o644)                        # 권한 변경
    _write(dst / "logs/client.log", "로컬 로그\n")               # 서버가 제외한 경로: 보존
    _write(dst / "config/config.properties", "dsn=client\n")    # 클라이언트가 제외: 보존

    code, out = _client(server, dst, "-y", "--exclude", "config.properties")
    assert code == ds.EXIT_OK, out
    assert (dst / "src/core/a.py").read_text() == "A = 1\n"
    assert not (dst / "stale.txt").exists() and not (dst / "old").exists()
    assert (dst / "empty").is_dir()
    assert os.stat(dst / "bin/start.sh").st_mode & 0o777 == 0o755
    assert (dst / "logs/client.log").read_text() == "로컬 로그\n"
    assert (dst / "config/config.properties").read_text() == "dsn=client\n"
    assert not (dst / "logs/server.log").exists()
    for label in ("[~] 파일 갱신", "[-] 파일 삭제", "[-] 디렉터리 삭제", "[+] 디렉터리 생성", "[m] 권한 변경"):
        assert label in out


def test_서버는_절대_바뀌지_않는다(server, src, tmp_path):
    before = _snapshot(src)
    dst = tmp_path / "dst"
    _write(dst / "only-local.txt", "x")
    assert _client(server, dst, "-y")[0] == ds.EXIT_OK
    _write(dst / "src/core/a.py", "changed")
    assert _client(server, dst, "-y", "--checksum")[0] == ds.EXIT_OK
    port = server.server_address[1]
    for method in ("POST", "PUT", "DELETE", "PATCH"):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        conn.request(method, "/file?path=src/core/a.py", body=b"overwrite")
        assert conn.getresponse().status == 405
        conn.close()
    assert _snapshot(src) == before
    assert not any(p.name.startswith(".dirsync") for p in src.rglob("*"))


def test_서버는_base_밖과_제외_대상을_내주지_않는다(server, src, tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("비밀")
    os.symlink(str(tmp_path), src / "escape")
    port = server.server_address[1]

    def get(path):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        conn.close()
        return resp.status, body

    assert get("/file?path=../secret.txt")[0] == 400
    assert get("/file?path=/etc/passwd")[0] == 400
    assert get("/file?path=escape/secret.txt")[0] == 404
    assert get("/file?path=logs/server.log")[0] == 404
    assert get("/file?path=core-link")[0] == 404  # 심링크는 목록으로만 옮긴다
    status, body = get("/file?path=src/core/a.py")
    assert status == 200 and body == b"A = 1\n"
    manifest = json.loads(get("/manifest")[1])
    paths = {e["path"] for e in manifest["entries"]}
    assert "logs" not in paths and "src/core/__pycache__" not in paths and "escape" in paths
    assert manifest["excludes"] == {"dirs": ["logs"], "patterns": ["__pycache__"]}


def test_계획만_보거나_n_으로_답하면_아무것도_바꾸지_않는다(server, tmp_path, monkeypatch):
    dst = tmp_path / "dst"
    _write(dst / "keep.txt", "x")
    before = _snapshot(dst)
    code, out = _client(server, dst, "--dry-run")
    assert code == ds.EXIT_OK and "--dry-run" in out and "요약:" in out
    code, out = _client(server, dst, answer="n", monkeypatch=monkeypatch)
    assert code == ds.EXIT_ABORTED and "취소했다" in out
    assert _snapshot(dst) == before


def test_y_로_답하면_진행한다(server, src, tmp_path, monkeypatch):
    dst = tmp_path / "dst"
    code, out = _client(server, dst, answer="y", monkeypatch=monkeypatch)
    assert code == ds.EXIT_OK and (dst / "big.bin").stat().st_size == 3_000_000


def test_터미널이_아니면_y_없이는_진행하지_않는다(server, tmp_path, monkeypatch):
    monkeypatch.setattr(ds, "_stdin_is_tty", lambda: False)
    code, out = _client(server, tmp_path / "dst")
    assert code == ds.EXIT_ABORTED and "-y" in out and not (tmp_path / "dst").exists()


def test_no_delete_면_로컬에만_있는_것을_남긴다(server, tmp_path):
    dst = tmp_path / "dst"
    _write(dst / "extra.txt", "x")
    code, out = _client(server, dst, "-y", "--no-delete")
    assert code == ds.EXIT_OK and (dst / "extra.txt").exists()


def test_대량_삭제는_y_만으로_진행하지_않는다(server, tmp_path):
    dst = tmp_path / "dst"
    for i in range(30):
        _write(dst / f"junk/{i}.txt", "x")
    code, out = _client(server, dst, "-y")
    assert code == ds.EXIT_ABORTED and "--force" in out and (dst / "junk/0.txt").exists()
    code, out = _client(server, dst, "-y", "--force")
    assert code == ds.EXIT_OK and not (dst / "junk").exists()


def test_서버와_같은_디렉터리는_거절한다(server, src):
    code, out = _client(server, src, "-y")
    assert code == ds.EXIT_ABORTED and "겹치는 디렉터리" in out
    code, out = _client(server, src / "src", "-y")
    assert code == ds.EXIT_ABORTED


def test_목록에_위험한_경로가_있으면_중단한다(server, tmp_path, monkeypatch):
    real = server.RequestHandlerClass.source.manifest

    def evil():
        data = real()
        data["entries"].append({"path": "../outside.txt", "type": "f", "size": 1, "mode": 0o644, "mtime_ns": 0})
        return data

    monkeypatch.setattr(server.RequestHandlerClass.source, "manifest", evil)
    code, out = _client(server, tmp_path / "dst", "-y")
    assert code == ds.EXIT_ABORTED and "쓸 수 없는 경로" in out and not (tmp_path / "outside.txt").exists()


def test_전송_중_해시가_틀리면_다시_받고_끝내_틀리면_실패한다(server, src, tmp_path, monkeypatch):
    calls = {"n": 0}
    real_fetch = ds.Remote.fetch

    def flaky(self, rel, out, on_bytes):
        meta = real_fetch(self, rel, out, on_bytes)
        if rel == "src/core/b.py":
            calls["n"] += 1
            if calls["n"] == 1:
                meta["actual"] = "0" * 64
        return meta

    monkeypatch.setattr(ds.Remote, "fetch", flaky)
    dst = tmp_path / "dst"
    code, out = _client(server, dst, "-y")
    assert code == ds.EXIT_OK and calls["n"] == 2 and (dst / "src/core/b.py").read_text() == "B = 2\n"

    def always_bad(self, rel, out, on_bytes):
        meta = real_fetch(self, rel, out, on_bytes)
        meta["actual"] = "0" * 64
        return meta

    monkeypatch.setattr(ds.Remote, "fetch", always_bad)
    _write(src / "new.txt", "n")
    code, out = _client(server, dst, "-y")
    assert code == ds.EXIT_FAILED and "해시가 맞지 않는다" in out
    assert not (dst / "new.txt").exists()
    assert not any(p.name.startswith(".dirsync") for p in dst.rglob("*"))


def test_읽기_전용_디렉터리_아래도_갱신하고_권한을_되돌린다(server, src, tmp_path):
    dst = tmp_path / "dst"
    assert _client(server, dst, "-y")[0] == ds.EXIT_OK
    os.chmod(src / "src/core", 0o555)
    try:
        assert _client(server, dst, "-y")[0] == ds.EXIT_OK
        assert os.stat(dst / "src/core").st_mode & 0o777 == 0o555
        os.chmod(dst / "src/core", 0o755)
        (dst / "src/core/a.py").write_text("local")
        os.chmod(dst / "src/core", 0o555)
        code, out = _client(server, dst, "-y")
        assert code == ds.EXIT_OK, out
        assert (dst / "src/core/a.py").read_text() == "A = 1\n"
        assert os.stat(dst / "src/core").st_mode & 0o777 == 0o555
    finally:
        os.chmod(src / "src/core", 0o755)
        os.chmod(dst / "src/core", 0o755)


def test_서버에_닿지_않으면_중단한다(tmp_path):
    args = ds.build_parser().parse_args(
        ["client", "--server", "127.0.0.1", "--port", "1", "--base", str(tmp_path / "d"), "-y", "--timeout", "2"])
    out = io.StringIO()
    assert ds.run_client(args, out) == ds.EXIT_ABORTED and "연결할 수 없다" in out.getvalue()


def test_진행_표시는_터미널에서_막대를_그린다():
    class Tty(io.StringIO):
        def isatty(self):
            return True

    out = Tty()
    p = ds.Progress(2, 2048, out)
    a = ds.Action("add", "src/한글.py", remote={"type": "f", "size": 2048, "mode": 0o644, "mtime_ns": 0})
    p.start_file(a.path, 2048)
    p.add_bytes(2048)
    p.done(a)
    p.finish()
    text = out.getvalue()
    assert "\r\033[K" in text and "[1/2] [+] 파일 추가  src/한글.py  2.0KB" in text and "100%" in text
    assert ds.disp_width("한글a") == 5 and ds.cut("한글abc", 5) == "한글a"
