"""local 모드: executor를 HTTP로 호출하지 않고 coordinator 안에서 직접 실행."""

from __future__ import annotations

import types

from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.config import settings as core_settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.job_store import JobStore
from coordinator.models import Job, JobStatus, TaskStatus
from executor.backend import MockBackend


def _job(exec_mode="copy", n=2, **over):
    job = Job(
        original_sql="SELECT a, dt FROM t WHERE dt IN ('1','2')",
        partition_column="dt",
        target_table="public.t",
        write_mode="append",
        parallelism=n,
        split_strategy="contiguous",
        failure_policy="fail_fast",
        exec_mode=exec_mode,
        **over,
    )
    from coordinator.models import Task
    job.tasks = [
        Task(job_id=job.job_id, executor_url=None, sub_query=f"SELECT {i}", partition_values=[f"'{i}'"])
        for i in range(n)
    ]
    return job


async def test_local_dispatcher_runs_copy_in_process():
    disp = LocalDispatcher(core_settings, backend=MockBackend(rows_per_value=5))
    job = _job(exec_mode="copy", n=2)
    result = await disp.run(job)
    assert result == job.job_id
    assert job.status == JobStatus.DONE
    assert all(t.status == TaskStatus.DONE for t in job.tasks)
    assert job.total_rows_written == 10  # 2 task * 5


async def test_local_dispatcher_statement_uses_execute():
    class _Rec(MockBackend):
        def __init__(self):
            super().__init__()
            self.executed = []

        def execute(self, sql):
            self.executed.append(sql)
            return 3

    backend = _Rec()
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _job(exec_mode="statement", n=2)
    await disp.run(job)
    assert len(backend.executed) == 2
    assert job.total_rows_written == 6


async def test_local_dispatcher_cancel():
    disp = LocalDispatcher(core_settings, backend=MockBackend())
    job = _job(n=2)
    await disp.cancel(job)
    assert job.cancel_requested is True
    assert all(t.status == TaskStatus.CANCELLED for t in job.tasks)


def test_create_app_selects_local_dispatcher(monkeypatch):
    monkeypatch.setattr(core_settings, "executor_mode", "local", raising=False)
    app = create_app(store=JobStore())  # runner 미주입 → 설정 기반 선택
    assert app.state.runner.__class__.__name__ == "LocalDispatcher"


def test_local_mode_end_to_end_via_api(monkeypatch):
    # 로컬 검증: executor 없이 제출 → 즉시 실행 → 상태 DONE
    runner = LocalDispatcher(core_settings, backend=MockBackend(rows_per_value=4))
    client = TestClient(create_app(runner=runner, store=JobStore()))
    payload = {
        "sql": "SELECT a, dt FROM t WHERE dt IN ('1','2','3')",
        "partition_column": "dt",
        "target_table": "public.t",
        "parallelism": 3,
    }
    job_id = client.post("/jobs", json=payload).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "DONE"
    assert body["total"] == 3
    assert body["total_rows_written"] == 12  # 3 * 4
