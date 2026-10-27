"""coordinator 이벤트 루프를 블로킹 DB 호출로 막지 않는지 검증한다.

단일 워커인 coordinator 는 헬스 체크·task 폴링·API 응답을 이벤트 루프 하나로 처리한다. 그 루프에서
job 저장(새 DB 연결 + UPSERT), 폴링마다의 취소 플래그 조회, ``/cluster`` 의 전체 job 복원이 동기로
돌면 그동안 모든 통신이 멈춰 executor 연결이 timeout 으로 끊긴 것처럼 보였다. 실제 DB 없이 블로킹
구간을 ``time.sleep`` 으로 흉내 내 검증한다.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

import httpx

from coordinator.app import create_app
from coordinator.dispatcher import LocalDispatcher
from coordinator.job_store import InMemoryJobStore, SqlJobStore, count_jobs_by_status, save_job
from coordinator.models import Job, JobStatus
from core.config import settings as core_settings
from core.loopwatch import LoopStallWatchdog
from executor.backend import MockBackend


def _job(**kw) -> Job:
    base = dict(
        original_sql="SELECT * FROM t WHERE dt IN ('a')", partition_column="dt",
        target_table="public.t", write_mode="append", parallelism=1,
        split_strategy="contiguous", failure_policy="partial",
    )
    base.update(kw)
    return Job(**base)


class _SlowSqlStore(SqlJobStore):
    """DB 대신 느린 쓰기를 흉내 내고 쓰인 순서를 기록하는 SqlJobStore 다."""

    def __init__(self, delay_s: float):
        super().__init__(dsn="postgresql://fake", table="public.jobs")
        self.delay_s = delay_s
        self.written: list[tuple[str, str]] = []
        self.threads: set[int] = set()

    def _upsert(self, job_id, status, cancel_requested, data):
        self.threads.add(threading.get_ident())
        time.sleep(self.delay_s)
        self.written.append((job_id, status))


async def _ticks_during(coro, step_s: float = 0.01) -> int:
    """coro 가 도는 동안 이벤트 루프가 몇 번 다른 일을 처리했는지 센다(루프가 막히면 0 에 가깝다)."""
    ticks = 0
    done = False

    async def ticker():
        nonlocal ticks
        while not done:
            await asyncio.sleep(step_s)
            ticks += 1

    t = asyncio.create_task(ticker())
    try:
        await coro
    finally:
        done = True
        await t
    return ticks


async def test_sql_저장은_스레드에서_돌아_루프를_막지_않는다():
    store = _SlowSqlStore(delay_s=0.3)
    ticks = await _ticks_during(save_job(store, _job()))
    assert ticks >= 10  # 0.3초 동안 루프가 계속 돌았다
    assert threading.get_ident() not in store.threads


async def test_같은_job_의_저장은_호출_순서대로_쓰인다():
    """먼저 만든 스냅샷이 나중 스냅샷을 덮어쓰면 job 상태가 되돌아간다."""
    store = _SlowSqlStore(delay_s=0.05)
    job = _job()
    saves = []
    for status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.DONE):
        job.status = status
        saves.append(asyncio.create_task(store.asave(job)))
        # 실제 호출부(await self._save)처럼 저장이 시작돼 스냅샷을 굳힌 뒤 다음 변경으로 넘어간다.
        await asyncio.sleep(0)
    await asyncio.gather(*saves)
    assert [s for _, s in store.written] == ["PENDING", "RUNNING", "DONE"]
    assert store._job_locks == {} and store._job_lock_users == {}  # 대기자가 없으면 락을 지운다


async def test_asave_는_호출_시점의_스냅샷을_쓴다():
    """직렬화를 루프에서 끝내므로, 쓰기 도중 Job 이 바뀌어도 호출 시점의 내용이 들어간다."""
    store = _SlowSqlStore(delay_s=0.1)
    job = _job(status=JobStatus.RUNNING)
    t = asyncio.create_task(store.asave(job))
    await asyncio.sleep(0)
    job.status = JobStatus.DONE
    await t
    assert store.written == [(job.job_id, "RUNNING")]


def test_상태별_건수는_목록_복원_없이_센다():
    store = InMemoryJobStore()
    for st in (JobStatus.DONE, JobStatus.DONE, JobStatus.RUNNING):
        store.add(_job(status=st))
    assert count_jobs_by_status(store) == {"DONE": 2, "RUNNING": 1}

    class _ListOnly:  # count_by_status 가 없는 저장소는 목록으로 센다
        def list(self):
            return [_job(status=JobStatus.FAILED)]

    assert count_jobs_by_status(_ListOnly()) == {"FAILED": 1}


def test_sql_상태별_건수는_GROUP_BY_로_센다():
    executed = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            executed.append(sql)

        def fetchall(self):
            return [("DONE", 3), ("FAILED", 1)]

    class _Conn(_Cur):
        def cursor(self):
            return _Cur()

    store = SqlJobStore(dsn="postgresql://fake", table="public.jobs")
    store._conn = lambda: _Conn()
    assert store.count_by_status() == {"DONE": 3, "FAILED": 1}
    assert executed == ["SELECT status, count(*) FROM public.jobs GROUP BY status"]


class _CountingStore(InMemoryJobStore):
    """공유 store 의 취소 플래그 조회 횟수를 센다(로컬 Job 플래그와 분리된 공유 상태)."""

    def __init__(self):
        super().__init__()
        self.calls = 0
        self.flag = False

    def is_cancel_requested(self, job_id):
        self.calls += 1
        return self.flag


async def test_취소_조회는_job_단위로_간격을_둔다(monkeypatch):
    monkeypatch.setattr(core_settings, "cancel_check_interval_s", 60.0, raising=False)
    store = _CountingStore()
    disp = LocalDispatcher(core_settings, backend=MockBackend(), store=store)
    job = _job(status=JobStatus.RUNNING)
    # 같은 job 의 task 여럿이 동시에 폴링해도 store 는 한 번만 조회한다.
    results = await asyncio.gather(*(disp._cancel_observed(job) for _ in range(20)))
    assert results == [False] * 20 and store.calls == 1
    # 간격 안에서는 공유 플래그가 서도 조회하지 않는다. 로컬 플래그는 즉시 보인다.
    store.flag = True
    assert await disp._cancel_observed(job) is False and store.calls == 1
    job.cancel_requested = True
    assert await disp._cancel_observed(job) is True and store.calls == 1


async def test_취소_조회_간격이_지나면_공유_플래그를_감지한다(monkeypatch):
    monkeypatch.setattr(core_settings, "cancel_check_interval_s", 0.05, raising=False)
    store = _CountingStore()
    disp = LocalDispatcher(core_settings, backend=MockBackend(), store=store)
    job = _job(status=JobStatus.RUNNING)
    assert await disp._cancel_observed(job) is False
    store.flag = True
    await asyncio.sleep(0.06)
    assert await disp._cancel_observed(job) is True and job.cancel_requested is True


async def test_cluster_는_전체_job_을_복원하지_않는다():
    class _NoListStore(InMemoryJobStore):
        def list(self):
            raise AssertionError("/cluster 가 store.list() 를 불렀다")

    class _Runner:
        async def run(self, job):
            return job.job_id

        async def cancel(self, job):
            pass

    store = _NoListStore()
    for st in (JobStatus.RUNNING, JobStatus.PENDING, JobStatus.DONE):
        store.add(_job(status=st))
    app = create_app(runner=_Runner(), store=store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        r = await c.get("/cluster", params={"refresh": "false"})
    assert r.status_code == 200, r.text
    jobs = r.json()["jobs"]
    assert jobs["total"] == 3 and jobs["running"] == 1 and jobs["active"] == 2


async def test_루프_정지_감시는_멈춘_위치를_남긴다(caplog):
    dog = LoopStallWatchdog(threshold_s=0.1, interval_s=0.02)
    dog.start()
    try:
        with caplog.at_level(logging.WARNING, logger="core.loopwatch"):
            await asyncio.sleep(0.1)  # 감시 스레드가 한 바퀴 돌 시간을 준다
            time.sleep(0.4)  # 루프를 블로킹 호출로 막는다
            await asyncio.sleep(0.1)
    finally:
        dog.stop()
    assert dog.stalls == 1
    stall = [r.getMessage() for r in caplog.records if "응답하지 않음" in r.getMessage()]
    assert stall and "test_루프_정지_감시는_멈춘_위치를_남긴다" in stall[0]
    assert any("정지 해소" in r.getMessage() for r in caplog.records)


async def test_정상_루프에서는_경고하지_않는다(caplog):
    dog = LoopStallWatchdog(threshold_s=0.2, interval_s=0.02)
    dog.start()
    try:
        with caplog.at_level(logging.WARNING, logger="core.loopwatch"):
            for _ in range(20):
                await asyncio.sleep(0.01)
    finally:
        dog.stop()
    assert dog.stalls == 0


async def test_store_조회는_기본_스레드_풀이_차도_기다리지_않는다():
    """기본 풀을 긴 GP INSERT 가 채워도 취소 조회·저장은 전용 풀에서 바로 돈다."""
    import concurrent.futures as cf

    loop = asyncio.get_running_loop()
    loop.set_default_executor(cf.ThreadPoolExecutor(max_workers=2))
    release = threading.Event()
    hogs = [loop.run_in_executor(None, release.wait, 5) for _ in range(2)]
    try:
        store = _SlowSqlStore(delay_s=0.0)
        t0 = time.monotonic()
        await asyncio.wait_for(save_job(store, _job()), timeout=1.0)
        assert time.monotonic() - t0 < 1.0 and len(store.written) == 1
    finally:
        release.set()
        await asyncio.gather(*hogs)


class _Hogged:
    """asyncio 기본 풀을 긴 작업으로 채워 둔다(Phase 2 GP INSERT·task 적재가 풀을 점유한 상황)."""

    def __init__(self, workers: int = 2):
        import concurrent.futures as cf

        self.loop = asyncio.get_running_loop()
        self.loop.set_default_executor(cf.ThreadPoolExecutor(max_workers=workers))
        self.release = threading.Event()
        self.hogs = [self.loop.run_in_executor(None, self.release.wait, 5) for _ in range(workers)]

    async def close(self):
        self.release.set()
        await asyncio.gather(*self.hogs)


async def test_이력_기록은_기본_풀이_차도_기다리지_않는다():
    """시작·종료 이력은 실행 경로에서 await 되므로, 기본 풀에서 돌면 슬롯 반납이 적재 뒤로 밀린다."""
    from coordinator.history import JobHistoryRepository
    from executor.history import TaskHistoryRepository

    import types

    s = types.SimpleNamespace(history_db_dsn="postgresql://fake")
    written = []
    job_repo = JobHistoryRepository(s)
    job_repo._write = lambda job: written.append(("job", threading.current_thread().name))
    task_repo = TaskHistoryRepository(s)
    task_repo._write = lambda task: written.append(("task", threading.current_thread().name))

    hog = _Hogged()
    try:
        job = _job()
        await asyncio.wait_for(job_repo.record(job), timeout=1.0)
        task = types.SimpleNamespace(task_id="t1", status=JobStatus.RUNNING)
        await asyncio.wait_for(task_repo.record(task), timeout=1.0)
    finally:
        await hog.close()
    assert [k for k, _ in written] == ["job", "task"]
    assert all(name.startswith("meta-io") for _, name in written)


async def test_self_report_는_기본_풀이_차도_밀리지_않는다():
    """self-report 가 밀리면 coordinator 가 executor 를 오래된 상태로 본다."""
    from core.metaio import run_meta_io

    hog = _Hogged()
    try:
        name = await asyncio.wait_for(run_meta_io(lambda: threading.current_thread().name), timeout=1.0)
    finally:
        await hog.close()
    assert name.startswith("meta-io")


async def test_기록_풀이_막혀도_job_저장은_진행된다():
    """이력·heartbeat 쪽 DB 호출이 몰리거나 걸려도 job 상태 저장은 store 풀에서 따로 돈다."""
    from core import metaio

    release = threading.Event()
    stuck = [asyncio.ensure_future(metaio.run_meta_io(release.wait, 5))
             for _ in range(metaio._POOL_WORKERS["meta"])]
    try:
        store = _SlowSqlStore(delay_s=0.0)
        await asyncio.wait_for(save_job(store, _job()), timeout=1.0)
        assert len(store.written) == 1
    finally:
        release.set()
        await asyncio.gather(*stuck)


def test_메타_연결에는_연결_대기_상한이_붙는다(monkeypatch):
    import sys
    import types

    from core import metaio

    calls = []
    fake = types.ModuleType("psycopg")
    fake.connect = lambda dsn, **kw: calls.append((dsn, kw)) or "conn"
    monkeypatch.setitem(sys.modules, "psycopg", fake)

    monkeypatch.setattr(core_settings, "db_connect_timeout_s", 7, raising=False)
    assert metaio.meta_connect("postgresql://h/db") == "conn"
    # DSN 에 직접 적은 값은 덮어쓰지 않는다.
    metaio.meta_connect("postgresql://h/db?connect_timeout=2")
    metaio.meta_connect("host=h dbname=db connect_timeout=3")
    # 0 이면 상한 없이 연다(libpq 기본).
    monkeypatch.setattr(core_settings, "db_connect_timeout_s", 0, raising=False)
    metaio.meta_connect("postgresql://h/db")
    assert calls == [
        ("postgresql://h/db", {"connect_timeout": 7}),
        ("postgresql://h/db?connect_timeout=2", {}),
        ("host=h dbname=db connect_timeout=3", {}),
        ("postgresql://h/db", {}),
    ]


def test_메타_저장소들은_모두_meta_connect_로_연다():
    """새 저장소가 psycopg.connect 를 직접 불러 연결 대기 상한을 빠뜨리지 않게 막는다."""
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "src"
    files = [
        "coordinator/job_store.py", "coordinator/history.py", "coordinator/executor_status.py",
        "coordinator/ha.py", "coordinator/reservation.py", "coordinator/monitor.py",
        "executor/history.py", "executor/status.py",
    ]
    for rel in files:
        text = (src / rel).read_text(encoding="utf-8")
        assert "psycopg.connect(" not in text, rel
        assert "asyncio.to_thread(" not in text, rel
