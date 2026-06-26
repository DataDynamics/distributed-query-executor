"""exec_mode=statement (wrapper INSERT 직접 실행) vs copy 모드 테스트."""

from __future__ import annotations

import asyncio

import httpx

from executor.app import create_app as create_executor_app
from executor.backend import MockBackend


class _RecordingBackend:
    """move/execute 호출을 기록하는 가짜 백엔드."""

    def __init__(self):
        self.moved = False
        self.executed_sql = None

    def move(self, *a, **k):
        self.moved = True
        return 5

    def execute(self, sql):
        self.executed_sql = sql
        return 9


def _payload(task_id, exec_mode, sub_query="INSERT INTO t SELECT * FROM (SELECT 1) s"):
    return {
        "task_id": task_id,
        "job_id": "j",
        "sub_query": sub_query,
        "target_table": "public.t",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
        "exec_mode": exec_mode,
    }


async def _run_task(app, payload):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(200):
            st = (await c.get(f"/tasks/{payload['task_id']}")).json()
            if st["status"] in ("DONE", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
        return st


async def test_statement_mode_calls_execute_not_move():
    backend = _RecordingBackend()
    app = create_executor_app(backend=backend)
    st = await _run_task(app, _payload("ts", "statement"))
    assert st["status"] == "DONE"
    assert backend.executed_sql is not None          # execute() 사용
    assert backend.moved is False                     # COPY(move) 미사용
    assert "INSERT INTO t" in backend.executed_sql


async def test_copy_mode_calls_move_not_execute():
    backend = _RecordingBackend()
    app = create_executor_app(backend=backend)
    st = await _run_task(app, _payload("tc", "copy", sub_query="SELECT 1"))
    assert st["status"] == "DONE"
    assert backend.moved is True                       # COPY(move) 사용
    assert backend.executed_sql is None                # execute() 미사용


async def test_statement_mode_rows_written_from_execute():
    app = create_executor_app(backend=MockBackend(rows_per_value=7))
    st = await _run_task(app, _payload("tm", "statement"))
    assert st["status"] == "DONE"
    assert st["rows_written"] == 7


def test_create_app_picks_real_backend_with_only_greenplum(monkeypatch):
    # greenplum.dsn 만 있어도 실제 백엔드 선택(statement 모드 가능)
    import executor.app as ea
    from core import config as core_config

    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://u@h/db", raising=False)
    monkeypatch.setattr(core_config.settings, "impala_host", "", raising=False)
    backend = ea._build_backend()
    assert backend.__class__.__name__ == "ImpalaToGreenplumBackend"


# ───────────────────── coordinator → executor 전달 ─────────────────────


def test_coordinator_passes_exec_mode_to_job(client, store, valid_payload):
    payload = {**valid_payload, "exec_mode": "statement",
               "wrapper_query": "INSERT INTO public.mirror SELECT * FROM ({{SUBQUERY}}) s"}
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 202
    job = store.get(resp.json()["job_id"])
    # job 에 exec_mode 가 저장되고, sub_query 는 INSERT 로 감싸짐
    assert job.exec_mode == "statement"
    assert job.tasks[0].sub_query.startswith("INSERT INTO public.mirror")
