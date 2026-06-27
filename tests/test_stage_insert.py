"""stage_insert 모드: Impala SELECT → Greenplum staging COPY → staging→target INSERT."""

from __future__ import annotations

import asyncio

import httpx

from executor.app import create_app as create_executor_app
from executor.backend import MockBackend


class _RecordingBackend:
    def __init__(self):
        self.staged = None  # (impala_select, staging_table, staging_ddl, insert_sql)
        self.moved = False
        self.executed = None

    def move(self, *a, **k):
        self.moved = True
        return 1

    def execute(self, sql):
        self.executed = sql
        return 1

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None, query_options=None):
        self.staged = (impala_select, staging_table, staging_ddl, insert_sql)
        if on_progress:
            on_progress(11)
        return 11


def _payload(task_id, **over):
    base = {
        "task_id": task_id,
        "job_id": "j",
        "sub_query": "SELECT a, dt FROM imp WHERE dt IN ('1')",  # Impala SELECT
        "target_table": "public.target",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
        "exec_mode": "stage_insert",
        "staging_table": "stg_t",
        "staging_ddl": "CREATE TEMP TABLE stg_t (a int, dt text)",
        "insert_sql": "INSERT INTO public.target (a, dt) SELECT a, dt FROM stg_t",
    }
    base.update(over)
    return base


async def _run(app, payload):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(200):
            st = (await c.get(f"/tasks/{payload['task_id']}")).json()
            if st["status"] in ("DONE", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
        return st


async def test_stage_insert_calls_stage_and_insert():
    backend = _RecordingBackend()
    st = await _run(create_executor_app(backend=backend), _payload("ts"))
    assert st["status"] == "DONE"
    assert st["rows_written"] == 11
    assert backend.moved is False and backend.executed is None  # copy/statement 미사용
    impala_select, staging_table, staging_ddl, insert_sql = backend.staged
    assert impala_select.startswith("SELECT a, dt FROM imp")
    assert staging_table == "stg_t"
    assert "CREATE TEMP TABLE stg_t" in staging_ddl
    assert insert_sql.startswith("INSERT INTO public.target")


async def test_stage_insert_rows_from_mock():
    st = await _run(create_executor_app(backend=MockBackend(rows_per_value=13)), _payload("tm"))
    assert st["status"] == "DONE"
    assert st["rows_written"] == 13


# ───────────────────── coordinator 검증/전달 ─────────────────────


def _job_payload(**over):
    base = {
        "sql": "SELECT a, dt FROM imp WHERE dt IN ('1','2','3')",
        "partition_column": "dt",
        "target_table": "public.target",
        "parallelism": 3,
        "exec_mode": "stage_insert",
        "staging_table": "stg_t",
        "staging_ddl": "CREATE TEMP TABLE stg_t (a int, dt text)",
        "wrapper_query": "INSERT INTO public.target (a, dt) SELECT a, dt FROM stg_t",
    }
    base.update(over)
    return base


def test_coordinator_stage_insert_job(client, store):
    resp = client.post("/jobs", json=_job_payload())
    assert resp.status_code == 202
    job = store.get(resp.json()["job_id"])
    assert job.exec_mode == "stage_insert"
    assert job.staging_table == "stg_t"
    assert "CREATE TEMP TABLE" in job.staging_ddl
    assert job.insert_sql.startswith("INSERT INTO public.target")
    # sub-query 는 분할된 Impala SELECT 그대로(래핑/INSERT 아님)
    assert job.tasks[0].sub_query.startswith("SELECT a, dt FROM imp")
    assert "INSERT" not in job.tasks[0].sub_query.upper()


def test_coordinator_stage_insert_missing_fields_rejected(client):
    payload = _job_payload()
    del payload["staging_ddl"]
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "STAGE_INSERT_REQUIRES_FIELDS"
