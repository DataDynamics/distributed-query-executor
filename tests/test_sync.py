"""coordinator → executor 파일 동기화(core.bundle, executor.sync, coordinator.sync)를 검증한다.

executor 는 실제 FastAPI 앱을 임시 앱 홈마다 하나씩 띄워 ASGI 로 붙이고, coordinator 쪽 동기화 관리자는
호스트 이름으로 각 앱에 나눠 보내는 전송 계층으로 이들과 2단계 전환을 주고받는다.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from types import SimpleNamespace

import httpx
import pytest

from core import bundle as bm
from coordinator.dispatcher import JobAdmission
from coordinator.sync import BundleSyncer
from executor import sync as es


def _write(path, text, mode=0o644):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(text)
    os.chmod(path, mode)


def _rules(pkg, patterns=("*.py", "*.yml"), exclude=("__pycache__", "*.pyc")):
    return bm.SyncRules((pkg,), tuple(patterns), tuple(exclude))


@pytest.fixture()
def pkg():
    """테스트마다 이름이 다른 파이썬 패키지 이름(sys.modules 충돌 방지)."""
    name = f"syncpkg_{uuid.uuid4().hex[:8]}"
    yield name
    for mod in [m for m in sys.modules if m == name or m.startswith(name + ".")]:
        del sys.modules[mod]


def _make_pkg(base, pkg, value="1"):
    _write(f"{base}/{pkg}/__init__.py", "")
    _write(f"{base}/{pkg}/mod.py", f"VALUE = {value}\n\ndef run():\n    return helper()\n\ndef helper():\n    return VALUE\n")
    _write(f"{base}/{pkg}/conf.yml", f"value: {value}\n")
    _write(f"{base}/{pkg}/notes.txt", "패턴에 없는 파일\n")
    _write(f"{base}/{pkg}/__pycache__/mod.cpython-39.pyc", "junk")


# ───────── core.bundle ─────────

def test_버전은_내용으로만_정해지고_패턴과_제외를_따른다(tmp_path, pkg):
    base = str(tmp_path)
    _make_pkg(base, pkg)
    m1 = bm.scan(base, _rules(pkg))
    assert sorted(m1.files) == [f"{pkg}/__init__.py", f"{pkg}/conf.yml", f"{pkg}/mod.py"]
    os.utime(f"{base}/{pkg}/mod.py", (1, 1))  # 시각만 바꾸면 버전은 그대로다
    assert bm.scan(base, _rules(pkg)).version == m1.version
    _write(f"{base}/{pkg}/notes.txt", "바뀜")  # 패턴 밖 파일은 무관하다
    assert bm.scan(base, _rules(pkg)).version == m1.version
    _write(f"{base}/{pkg}/mod.py", "VALUE = 2\n")
    assert bm.scan(base, _rules(pkg)).version != m1.version
    os.chmod(f"{base}/{pkg}/conf.yml", 0o600)  # 권한도 버전에 들어간다
    assert len(bm.scan(base, _rules(pkg)).version) == bm.VERSION_LEN


def test_문법_오류를_찾는다(tmp_path, pkg):
    base = str(tmp_path)
    _make_pkg(base, pkg)
    assert bm.syntax_errors(bm.scan(base, _rules(pkg))) == []
    _write(f"{base}/{pkg}/bad.py", "def x(:\n")
    errs = bm.syntax_errors(bm.scan(base, _rules(pkg)))
    assert len(errs) == 1 and errs[0].startswith(f"{pkg}/bad.py:1")


def test_번들을_풀면_같은_버전이_나오고_위험한_항목은_거절한다(tmp_path, pkg):
    import io
    import tarfile

    base = str(tmp_path / "src")
    _make_pkg(base, pkg)
    m = bm.scan(base, _rules(pkg))
    data = bm.build_bundle(m)
    assert bm.build_bundle(m) == data  # 같은 내용이면 같은 바이트
    dest = tmp_path / "out"
    dest.mkdir()
    assert bm.extract_bundle(data, str(dest), _rules(pkg), max_bytes=10 ** 6) == m.version

    def tar_with(name, kind=tarfile.REGTYPE):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            info = tarfile.TarInfo(name)
            info.type = kind
            info.size = 0 if kind != tarfile.REGTYPE else 1
            if kind == tarfile.SYMTYPE:
                info.linkname = "/etc/passwd"
            tar.addfile(info, io.BytesIO(b"x") if kind == tarfile.REGTYPE else None)
        return buf.getvalue()

    for bad in (tar_with("../x.py"), tar_with("/etc/x.py"), tar_with("other/x.py"),
                tar_with(f"{pkg}/l.py", tarfile.SYMTYPE)):
        d = tmp_path / uuid.uuid4().hex
        d.mkdir()
        with pytest.raises(bm.BundleError):
            bm.extract_bundle(bad, str(d), _rules(pkg), max_bytes=10 ** 6)
        assert list(d.iterdir()) == []
    with pytest.raises(bm.BundleError):
        bm.extract_bundle(data, str(tmp_path / "small"), _rules(pkg), max_bytes=5)


@pytest.mark.parametrize("bad", ["/abs", "../up", "", "a/../../b"])
def test_동기화_경로는_앱_홈_안의_상대_경로만(bad):
    with pytest.raises(bm.BundleError):
        bm.normalize_rel(bad)


# ───────── executor.sync.BundleStore ─────────

def _settings(pkg, **kw):
    base = dict(sync_enabled=True, sync_paths=[pkg], sync_patterns=["*.py", "*.yml"],
                sync_exclude=["__pycache__", "*.pyc"], sync_keep_releases=3, sync_max_bundle_mb=10,
                sync_verify_import=True, sync_verify_modules=[f"{pkg}.mod:run"], sync_verify_timeout_s=30,
                query_func_module="", query_func_fetch_module="")
    base.update(kw)
    return SimpleNamespace(**base)


def _bundle_from(tmp_path, pkg, value):
    src = str(tmp_path / f"coord-{uuid.uuid4().hex[:6]}")
    _make_pkg(src, pkg, value)
    m = bm.scan(src, _rules(pkg))
    return m.version, bm.build_bundle(m)


def test_prepare_commit_은_current_를_바꾸고_모듈을_새로_불러온다(tmp_path, pkg, monkeypatch):
    base = str(tmp_path / "exec")
    _make_pkg(base, pkg, "1")
    monkeypatch.syspath_prepend(base)
    store = es.BundleStore(_settings(pkg), base=base)
    old_version = store.version()

    import importlib

    old_mod = importlib.import_module(f"{pkg}.mod")
    old_run = old_mod.run
    assert old_run() == 1

    version, data = _bundle_from(tmp_path, pkg, "2")
    assert version != old_version
    assert store.prepare(data, version)["prepared"]
    assert store.version(refresh=True) == old_version  # 준비만으로는 바뀌지 않는다
    assert importlib.import_module(f"{pkg}.mod").run() == 1

    store.commit(version)
    assert os.path.islink(f"{base}/{pkg}") and store.version(refresh=True) == version
    assert os.readlink(f"{base}/.sync/current") == f"releases/{version}"
    assert importlib.import_module(f"{pkg}.mod").run() == 2
    # 이미 쥐고 있던 옛 함수는 옛 globals 로 끝까지 돈다(reload 가 아니라 새 모듈 객체라서).
    assert old_run() == 1
    # 처음 전환한 실제 디렉터리는 지우지 않고 옮겨 둔다(패턴에 없던 notes.txt 포함).
    displaced = [os.path.join(dp, f) for dp, _, fs in os.walk(f"{base}/.sync/displaced") for f in fs]
    assert any(p.endswith(f"{pkg}/notes.txt") for p in displaced)


def test_import_검증에_실패하면_전환하지_않는다(tmp_path, pkg, monkeypatch):
    base = str(tmp_path / "exec")
    _make_pkg(base, pkg, "1")
    store = es.BundleStore(_settings(pkg), base=base)
    before = store.version()
    src = str(tmp_path / "coord")
    _make_pkg(src, pkg, "1")
    _write(f"{src}/{pkg}/mod.py", "import no_such_module_xyz\n\ndef run():\n    return 1\n")
    m = bm.scan(src, _rules(pkg))
    with pytest.raises(bm.BundleError, match="import 검증 실패"):
        store.prepare(bm.build_bundle(m), m.version)
    assert store.version(refresh=True) == before and not os.path.islink(f"{base}/{pkg}")
    assert not os.listdir(f"{base}/.sync/releases")
    with pytest.raises(bm.BundleError):
        store.commit(m.version)


def test_abort_는_준비한_것을_지우고_잘못된_버전값은_거절한다(tmp_path, pkg):
    base = str(tmp_path / "exec")
    _make_pkg(base, pkg)
    store = es.BundleStore(_settings(pkg, sync_verify_import=False), base=base)
    version, data = _bundle_from(tmp_path, pkg, "5")
    store.prepare(data, version)
    assert os.path.isdir(f"{base}/.sync/releases/{version}")
    store.abort(version)
    assert not os.path.exists(f"{base}/.sync/releases/{version}")
    for bad in ("../../etc", "ZZZ", version + "0"):
        with pytest.raises(bm.BundleError):
            store.prepare(data, bad)


def test_꺼져_있으면_받지_않는다(tmp_path, pkg):
    store = es.BundleStore(_settings(pkg, sync_enabled=False), base=str(tmp_path))
    version, data = _bundle_from(tmp_path, pkg, "1")
    with pytest.raises(es.SyncDisabled):
        store.prepare(data, version)


def test_coordinator_원본과_같은_디렉터리면_파일을_건드리지_않는다(tmp_path, pkg):
    base = str(tmp_path / "home")
    _make_pkg(base, pkg)
    store = es.BundleStore(_settings(pkg), base=base)
    version = store.version()
    out = store.prepare(b"", version, source_host=bm.host_id(), source_base=base)
    assert out["shared"] and store.commit(version)["shared"]
    assert not os.path.islink(f"{base}/{pkg}") and not os.path.exists(f"{base}/.sync/releases")


def test_오래된_release_는_keep_개만_남긴다(tmp_path, pkg):
    base = str(tmp_path / "exec")
    _make_pkg(base, pkg)
    store = es.BundleStore(_settings(pkg, sync_verify_import=False, sync_keep_releases=2), base=base)
    versions = []
    for v in range(4):
        version, data = _bundle_from(tmp_path, pkg, str(10 + v))
        store.prepare(data, version)
        store.commit(version)
        versions.append(version)
        os.utime(f"{base}/.sync/releases/{version}", (1000 + v, 1000 + v))
    assert sorted(os.listdir(f"{base}/.sync/releases")) == sorted(versions[-2:])
    # 보관 중인 버전으로 되돌리면 다시 받지 않고 재사용한다.
    assert store.prepare(b"not-a-bundle", versions[-2])["reused"]


# ───────── JobAdmission 관문 ─────────

async def test_관문을_닫으면_새_job_은_기다리고_실행_중인_job_이_끝나면_비었다고_본다():
    adm = JobAdmission(SimpleNamespace(max_concurrent_jobs=2, max_pending_jobs=2))
    release = asyncio.Event()

    async def running_job():
        async with adm.slot():
            await release.wait()

    t1 = asyncio.create_task(running_job())
    await asyncio.sleep(0.05)
    adm.pause("test")
    entered = asyncio.Event()

    async def new_job():
        async with adm.slot():
            entered.set()

    t2 = asyncio.create_task(new_job())
    await asyncio.sleep(0.1)
    assert not entered.is_set()  # 슬롯이 비어 있어도 관문이 닫혀 있으면 못 들어간다
    assert not await adm.wait_idle(0.1)
    release.set()
    assert await adm.wait_idle(1.0)
    adm.resume()
    await asyncio.wait_for(entered.wait(), 1.0)
    await asyncio.gather(t1, t2)


# ───────── coordinator.sync.BundleSyncer + 실제 executor 앱 ─────────

class _Router(httpx.AsyncBaseTransport):
    """호스트:포트로 executor 앱을 골라 보내는 전송 계층이다. down 에 든 호스트는 연결 실패를 낸다."""

    def __init__(self, apps):
        self.apps = {host: httpx.ASGITransport(app=app) for host, app in apps.items()}
        self.down: set = set()

    async def handle_async_request(self, request):
        host = f"{request.url.host}:{request.url.port}"
        if host in self.down:
            raise httpx.ConnectError("down", request=request)
        return await self.apps[host].handle_async_request(request)


@pytest.fixture()
def cluster(tmp_path, pkg, monkeypatch):
    from core.config import settings
    from executor.app import create_app
    from executor.backend import MockBackend

    coord_base = str(tmp_path / "coord")
    _make_pkg(coord_base, pkg, "1")
    for k, v in dict(sync_enabled=True, sync_paths=[pkg], sync_patterns=["*.py", "*.yml"],
                     sync_exclude=["__pycache__", "*.pyc"], sync_verify_import=True,
                     sync_verify_modules=[f"{pkg}.mod:run"]).items():
        monkeypatch.setattr(settings, k, v, raising=False)
    apps, bases = {}, {}
    for i in (1, 2):
        base = str(tmp_path / f"exec{i}")
        _make_pkg(base, pkg, "1")
        monkeypatch.setattr(settings, "sync_base_dir", base, raising=False)
        apps[f"e{i}:8087"] = create_app(backend=MockBackend())
        bases[f"http://e{i}:8087"] = base
    router = _Router(apps)
    cs = SimpleNamespace(
        sync_paths=[pkg], sync_patterns=["*.py", "*.yml"], sync_exclude=["__pycache__", "*.pyc"],
        sync_base_dir=coord_base, sync_interval_s=5, sync_settle_s=0, sync_drain_timeout_s=2,
        sync_retry_backoff_s=60, sync_max_bundle_mb=10, executors=list(bases),
        max_concurrent_jobs=4, max_pending_jobs=4,
    )
    adm = JobAdmission(cs)
    syncer = BundleSyncer(cs, adm, transport=router)
    return SimpleNamespace(syncer=syncer, router=router, bases=bases, coord=coord_base, pkg=pkg, adm=adm,
                           apps=apps)


def _ver(base, pkg):
    return bm.scan(base, _rules(pkg)).version


async def test_같은_내용이면_옮기지_않고_현재_버전으로_삼는다(cluster):
    st = await cluster.syncer.run_once()
    v = _ver(cluster.coord, cluster.pkg)
    assert st["active_version"] == v == st["source_version"]
    assert all(e["in_sync"] for e in st["executors"])
    assert not any(os.path.islink(f"{b}/{cluster.pkg}") for b in cluster.bases.values())


async def test_원본이_바뀌면_모든_executor_를_한_번에_전환한다(cluster):
    await cluster.syncer.run_once()
    _write(f"{cluster.coord}/{cluster.pkg}/mod.py", "VALUE = 2\n\ndef run():\n    return VALUE\n")
    st = await cluster.syncer.run_once(force=True)
    v2 = _ver(cluster.coord, cluster.pkg)
    assert st["active_version"] == v2 and st["last_transition"]["to"] == v2
    for base in cluster.bases.values():
        assert _ver(base, cluster.pkg) == v2 and os.path.islink(f"{base}/{cluster.pkg}")
    assert all(cluster.syncer.eligible(u) for u in cluster.bases)


async def test_한_대라도_준비에_실패하면_아무도_바꾸지_않는다(cluster):
    await cluster.syncer.run_once()
    v1 = cluster.syncer.active_version
    # executor 2 에서만 import 가 실패하도록 그 쪽에 없는 모듈에 의존하게 만든다.
    _write(f"{cluster.coord}/{cluster.pkg}/mod.py",
           "import os\nif os.environ.get('SYNC_TEST_BREAK'):\n    import nope_xyz\n\ndef run():\n    return 3\n")
    store2 = cluster.apps["e2:8087"].state.bundle_store
    store2.verify_modules = [f"{cluster.pkg}.nope_xyz_missing:run"]
    st = await cluster.syncer.run_once(force=True)
    assert st["active_version"] == v1 and "준비 실패" in st["last_error"]
    for base in cluster.bases.values():
        assert _ver(base, cluster.pkg) == v1
        assert os.listdir(f"{base}/.sync/releases") == []  # 준비했던 executor 1 도 abort 로 지웠다


async def test_내려가_있던_executor_는_제외됐다가_따라잡는다(cluster):
    await cluster.syncer.run_once()
    cluster.router.down.add("e2:8087")
    _write(f"{cluster.coord}/{cluster.pkg}/mod.py", "VALUE = 4\n\ndef run():\n    return VALUE\n")
    st = await cluster.syncer.run_once(force=True)
    v = _ver(cluster.coord, cluster.pkg)
    assert st["active_version"] == v
    assert cluster.syncer.eligible("http://e1:8087") and not cluster.syncer.eligible("http://e2:8087")
    cluster.router.down.clear()
    await cluster.syncer.run_once()
    assert cluster.syncer.eligible("http://e2:8087")
    assert _ver(cluster.bases["http://e2:8087"], cluster.pkg) == v


async def test_전환은_실행_중인_job_이_끝나기를_기다린다(cluster):
    await cluster.syncer.run_once()
    release = asyncio.Event()

    async def job():
        async with cluster.adm.slot():
            await release.wait()

    t = asyncio.create_task(job())
    await asyncio.sleep(0.05)
    _write(f"{cluster.coord}/{cluster.pkg}/mod.py", "VALUE = 5\n\ndef run():\n    return VALUE\n")
    sync_task = asyncio.create_task(cluster.syncer.run_once(force=True))
    await asyncio.sleep(0.5)
    assert cluster.adm.paused and not sync_task.done()
    assert _ver(cluster.bases["http://e1:8087"], cluster.pkg) != _ver(cluster.coord, cluster.pkg)
    release.set()
    st = await asyncio.wait_for(sync_task, 5)
    assert st["last_transition"]["drained"] and not cluster.adm.paused
    await t


async def test_문법_오류가_있으면_배포하지_않는다(cluster):
    await cluster.syncer.run_once()
    v1 = cluster.syncer.active_version
    _write(f"{cluster.coord}/{cluster.pkg}/mod.py", "def broken(:\n")
    st = await cluster.syncer.run_once(force=True)
    assert st["source_errors"] and st["active_version"] == v1


async def test_executor_는_버전이_다른_task_를_409_로_거절한다(cluster):
    async with httpx.AsyncClient(transport=cluster.router, base_url="http://e1:8087") as client:
        resp = await client.post("/tasks", json={
            "task_id": "t-sync", "job_id": "j", "sub_query": "SELECT 1", "target_table": "t",
            "partition_column": "dt", "exec_mode": "copy", "bundle_version": "0" * 16,
        })
    assert resp.status_code == 409 and resp.json()["detail"]["code"] == "BUNDLE_VERSION_MISMATCH"


def test_디스패처는_버전이_다른_executor_를_후보에서_뺀다():
    from coordinator.dispatcher import HttpDispatcher
    from core.config import settings

    disp = HttpDispatcher(settings)
    gate = SimpleNamespace(enforcing=True, active_version="a" * 16, eligible=lambda u: u != "http://b")
    disp.bundle_gate = gate
    assert disp._gate_order(["http://a", "http://b", "http://c"]) == ["http://a", "http://c"]
    gate.enforcing = False
    assert disp._gate_order(["http://a", "http://b"]) == ["http://a", "http://b"]


def test_설정_도움말이_있다():
    from core.config_help import FIELD_HELP

    for key in ("sync.enabled", "sync.paths", "sync.patterns", "sync.exclude", "sync.push"):
        assert key in FIELD_HELP
