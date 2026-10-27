"""``POST /query-run`` 이 이관 task 와 같은 동시 처리 슬롯·집계·이력을 공유하는지 검증한다.

query-execute 가 위임한 SELECT 실행도 executor 에서는 exec_mode="query_run" 인 Task 로 등록된다.
그래서 (1) /tasks·/metrics 의 처리중 집계에 들어가고, (2) executor.max_concurrent_tasks 세마포어를
기다리며(QUEUED), (3) 상태 전이마다 task_history 에 남고, (4) 드레이닝 중에는 503 으로 거절된다.
실제 Impala 없이 run_impala_select 를 가짜로 바꿔 검증한다.
"""

from __future__ import annotations

import asyncio
import threading

import httpx
import pytest

from core import config as core_config
from core.dbprobe import QueryResult
from executor.app import create_app as create_executor_app


class _RecordingHistory:
    """task 이력 기록을 메모리에 모으는 가짜 저장소다."""

    def __init__(self):
        self.records: list[dict] = []

    async def record(self, task):
        self.records.append({
            "task_id": task.task_id, "job_id": task.job_id, "status": task.status.value,
            "exec_mode": task.exec_mode, "rows_read": task.rows_read, "error": task.error,
            "sub_query": task.sub_query, "username": task.username,
        })


@pytest.fixture(autouse=True)
def _impala_settings(monkeypatch):
    s = core_config.settings
    monkeypatch.setattr(s, "executor_self_report", False, raising=False)
    monkeypatch.setattr(s, "impala_host", "impala-1", raising=False)
    monkeypatch.setattr(s, "impala_query_options", {}, raising=False)
    monkeypatch.setattr(s, "executor_max_concurrent_tasks", 0, raising=False)


def _fake_impala(monkeypatch, *, gate: threading.Event | None = None, fail: Exception | None = None):
    """run_impala_select 를 가짜로 바꾼다. gate 가 있으면 set 될 때까지 블로킹한다(to_thread 안)."""
    import executor.app as ea

    def fake(dsn, sql, *, query_options=None, limit, datasource="impala", log=True):
        if gate is not None:
            gate.wait(timeout=5)
        if fail is not None:
            raise fail
        return QueryResult(columns=["a"], rows=[[1], [2], [3]], row_count=3, truncated=False, elapsed_ms=1.0)

    monkeypatch.setattr(ea, "run_impala_select", fake)


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://x")


async def _wait_until(pred, timeout=3.0):
    for _ in range(int(timeout / 0.02)):
        if pred():
            return True
        await asyncio.sleep(0.02)
    return False


async def test_query_run_은_task_로_등록되고_이력이_남는다(monkeypatch):
    hist = _RecordingHistory()
    _fake_impala(monkeypatch)
    app = create_executor_app(task_history=hist)
    async with _client(app) as c:
        r = await c.post(
            "/query-run",
            json={"sql": "SELECT 42", "limit": 10, "datasource": "impala", "username": "kim"},
            headers={"X-Request-ID": "abc123"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["row_count"] == 3
        listing = (await c.get("/tasks")).json()
        assert listing["total"] == 1 and listing["active"] == 0
        t = listing["tasks"][0]
        assert t["exec_mode"] == "query_run" and t["status"] == "DONE"
        assert t["task_id"].startswith("qr-") and t["job_id"] == "req-abc123"
        assert t["rows_read"] == 3 and t["rows_written"] == 0
        assert [p["name"] for p in t["phases"]] == ["QUEUE_WAIT", "SOURCE_QUERY"]
        assert t["phases"][1]["rows"] == 3 and t["phases"][1]["finished_at"]
        detail = (await c.get(f"/tasks/{t['task_id']}/detail")).json()
        assert detail["sub_query"] == "SELECT 42"
    # 이력은 QUEUED → READING → DONE 세 행이고, DONE 행에 반환 행수와 SQL 이 담긴다.
    assert [h["status"] for h in hist.records] == ["QUEUED", "READING", "DONE"]
    done = hist.records[-1]
    assert done["exec_mode"] == "query_run" and done["rows_read"] == 3
    assert done["sub_query"] == "SELECT 42" and done["username"] == "kim"
    assert done["job_id"] == "req-abc123" and done["error"] is None


async def test_query_run_은_동시_처리_슬롯을_기다린다(monkeypatch):
    """max_concurrent_tasks=1 이면 두 번째 query-run 은 첫 번째가 끝날 때까지 QUEUED 로 기다린다."""
    monkeypatch.setattr(core_config.settings, "executor_max_concurrent_tasks", 1, raising=False)
    gate = threading.Event()
    _fake_impala(monkeypatch, gate=gate)
    app = create_executor_app(task_history=_RecordingHistory())
    async with _client(app) as c:
        first = asyncio.create_task(c.post("/query-run", json={"sql": "SELECT 1", "datasource": "impala"}))
        assert await _wait_until(lambda: any(
            t.status.value == "READING" for t in app.state.tasks.values()))
        second = asyncio.create_task(c.post("/query-run", json={"sql": "SELECT 2", "datasource": "impala"}))
        assert await _wait_until(lambda: len(app.state.tasks) == 2)
        await asyncio.sleep(0.1)  # 두 번째가 슬롯을 얻지 못했음을 확인할 여유를 둔다
        by_status = {t.sub_query: t.status.value for t in app.state.tasks.values()}
        assert by_status == {"SELECT 1": "READING", "SELECT 2": "QUEUED"}
        m = (await c.get("/metrics")).json()["tasks"]
        assert m == {"active": 1, "queued": 1, "max": 1}
        gate.set()
        r1, r2 = await asyncio.gather(first, second)
    assert r1.status_code == 200 and r2.status_code == 200
    assert {t.status.value for t in app.state.tasks.values()} == {"DONE"}


async def test_query_run_과_이관_task_가_같은_슬롯을_나눈다(monkeypatch):
    """이관 task 가 슬롯을 쥐고 있으면 query-run 은 QUEUED 로 기다린다(집계에도 함께 센다)."""
    monkeypatch.setattr(core_config.settings, "executor_max_concurrent_tasks", 1, raising=False)
    gate = threading.Event()
    _fake_impala(monkeypatch)

    class _SlowBackend:
        def move(self, *a, on_progress=None, **k):
            gate.wait(timeout=5)
            return 7

        def execute(self, sql, **k):  # pragma: no cover
            return 0

    app = create_executor_app(backend=_SlowBackend(), task_history=_RecordingHistory())
    async with _client(app) as c:
        r = await c.post("/tasks", json={
            "task_id": "t1", "job_id": "j1", "sub_query": "SELECT 1", "target_table": "public.t",
            "partition_column": "dt", "partition_values": ["1"], "exec_mode": "copy",
        })
        assert r.status_code == 202
        assert await _wait_until(lambda: app.state.tasks["t1"].status.value in ("READING", "WRITING"))
        qr = asyncio.create_task(c.post("/query-run", json={"sql": "SELECT 9", "datasource": "impala"}))
        assert await _wait_until(lambda: len(app.state.tasks) == 2)
        await asyncio.sleep(0.1)
        qtask = next(t for t in app.state.tasks.values() if t.exec_mode == "query_run")
        assert qtask.status.value == "QUEUED"
        info = (await c.get("/metrics")).json()["tasks"]
        assert info["active"] == 1 and info["queued"] == 1
        gate.set()
        assert (await qr).status_code == 200
    assert qtask.status.value == "DONE"


async def test_query_run_실패는_FAILED_이력으로_남고_502(monkeypatch):
    hist = _RecordingHistory()
    _fake_impala(monkeypatch, fail=RuntimeError("auth error"))
    app = create_executor_app(task_history=hist)
    async with _client(app) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "impala"})
        assert r.status_code == 502 and "auth error" in r.json()["detail"]
        t = (await c.get("/tasks")).json()["tasks"][0]
    assert t["status"] == "FAILED" and "auth error" in t["error"]
    assert [h["status"] for h in hist.records] == ["QUEUED", "READING", "FAILED"]
    assert "auth error" in hist.records[-1]["error"]


async def test_query_run_설정_오류도_FAILED_이력으로_남는다(monkeypatch):
    """커스텀 함수 미설정(400)처럼 실행 전에 막히는 오류도 이력에 FAILED 로 남는다."""
    monkeypatch.setattr(core_config.settings, "query_func_module", "", raising=False)
    hist = _RecordingHistory()
    app = create_executor_app(task_history=hist)
    async with _client(app) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "trino"})
    assert r.status_code == 400
    assert [h["status"] for h in hist.records] == ["QUEUED", "READING", "FAILED"]
    assert "query.func.module" in hist.records[-1]["error"]


async def test_드레이닝_중_query_run_은_503_이고_task_를_만들지_않는다(monkeypatch):
    _fake_impala(monkeypatch)
    app = create_executor_app(task_history=_RecordingHistory())
    app.state.drain["on"] = True
    async with _client(app) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "impala"})
    assert r.status_code == 503 and "draining" in r.json()["detail"]
    assert app.state.tasks == {}


async def test_요청_헤더가_없어도_job_id_는_요청_식별자다(monkeypatch):
    """HTTP 로깅 미들웨어가 요청마다 X-Request-ID 를 만들어 주므로 job_id 는 항상 req-<id> 꼴이다."""
    _fake_impala(monkeypatch)
    app = create_executor_app(task_history=_RecordingHistory())
    async with _client(app) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "impala"})
    t = next(iter(app.state.tasks.values()))
    assert t.job_id == f"req-{r.headers['X-Request-ID']}"


class _GrowingTask:
    """status 를 읽는 순간 ``tasks`` 에 새 항목을 끼워 넣는 가짜 task 다.

    /metrics·/info·self-report 는 이벤트 루프 밖 스레드에서 task 를 세고, 그사이 이벤트 루프는
    /query-run 접수로 ``tasks`` 에 항목을 더한다. 그 끼어듦을 순회 도중에 결정적으로 재현한다.
    """

    def __init__(self, tasks: dict):
        self._tasks = tasks
        self._n = 0

    @property
    def status(self):
        from executor.models import TaskStatus

        self._n += 1
        self._tasks[f"grown-{id(self)}-{self._n}"] = _DoneTask()
        return TaskStatus.READING


class _DoneTask:
    @property
    def status(self):
        from executor.models import TaskStatus

        return TaskStatus.DONE


async def test_task_집계는_순회_중_task_가_추가돼도_실패하지_않는다():
    """집계 도중 tasks 가 커져도 /metrics·/info 가 500(dictionary changed size) 이 되지 않는다."""
    app = create_executor_app(task_history=_RecordingHistory())
    app.state.tasks["growing"] = _GrowingTask(app.state.tasks)
    async with _client(app) as c:
        r = await c.get("/metrics")
        assert r.status_code == 200, r.text
        assert r.json()["tasks"]["active"] == 1
        r = await c.get("/info")
        assert r.status_code == 200, r.text
