"""Job/Task 취소 기능 테스트."""

from __future__ import annotations

import asyncio
import threading

import httpx

from coordinator.models import Job, JobStatus, TaskStatus
from executor.app import create_app as create_executor_app
from executor.models import TaskStatus as ExTaskStatus


# ───────────────────── Coordinator 취소 API ─────────────────────


def _running_job() -> Job:
    job = Job(
        original_sql="SELECT 1 FROM t WHERE dt IN ('1','2')",
        partition_column="dt",
        target_table="public.t",
        write_mode="append",
        parallelism=2,
        split_strategy="contiguous",
        failure_policy="fail_fast",
        status=JobStatus.RUNNING,
    )
    return job


def test_cancel_running_job(client, store, runner):
    job = _running_job()
    store.add(job)

    resp = client.post(f"/jobs/{job.job_id}/cancel")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "CANCELLED"
    assert body["cancel_requested"] is True
    # runner.cancel 이 호출되었는지
    assert runner.cancels and runner.cancels[0].job_id == job.job_id


def test_cancel_already_finished_job_returns_409(client, store):
    job = _running_job()
    job.status = JobStatus.DONE
    store.add(job)
    resp = client.post(f"/jobs/{job.job_id}/cancel")
    assert resp.status_code == 409


def test_cancel_unknown_job_404(client):
    assert client.post("/jobs/none/cancel").status_code == 404


# ───────────────────── Executor 취소 (실행 중) ─────────────────────


class _RecordingTaskHistory:
    def __init__(self):
        self.records = []

    async def record(self, task):
        self.records.append(task.status.value)


class _BlockingBackend:
    """move 를 release 이벤트가 풀릴 때까지 블로킹한다(취소 시점 제어용)."""

    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()

    def move(self, *a, **k):
        self.started.set()
        self.release.wait(timeout=5)
        return 7


def _task_payload(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "job_id": "job-c",
        "sub_query": "SELECT 1",
        "target_table": "public.t",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
    }


async def test_executor_cancel_running_task():
    backend = _BlockingBackend()
    app = create_executor_app(backend=backend, task_history=_RecordingTaskHistory())
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=_task_payload("tc"))
        # move 가 시작될 때까지(=WRITING) 대기
        for _ in range(200):
            if backend.started.is_set():
                break
            await asyncio.sleep(0.01)
        assert backend.started.is_set()

        r = await c.post("/tasks/tc/cancel")
        assert r.status_code == 200
        assert r.json()["cancel_requested"] is True

        backend.release.set()  # move 종료시킴
        for _ in range(200):
            st = (await c.get("/tasks/tc")).json()
            if st["status"] in ("DONE", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
        assert st["status"] == "CANCELLED"


async def test_executor_cancel_unknown_task_404():
    app = create_executor_app(task_history=_RecordingTaskHistory())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        r = await c.post("/tasks/nope/cancel")
        assert r.status_code == 404


# ───────────────────── Dispatcher 종료 상태 ─────────────────────


async def test_dispatcher_finalizes_cancelled():
    from coordinator.config import settings as core_settings
    from coordinator.dispatcher import HttpDispatcher

    disp = HttpDispatcher(core_settings)
    job = _running_job()
    job.tasks = []
    job.cancel_requested = True
    result = await disp.run(job)
    assert result == job.job_id
    assert job.status == JobStatus.CANCELLED
