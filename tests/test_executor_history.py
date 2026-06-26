"""executor 가 task 상태 전이를 이력으로 남기는지 테스트."""

from __future__ import annotations

import asyncio

import httpx

from executor.app import create_app as create_executor_app
from executor.history import TaskHistoryRepository
from executor.models import Task, TaskStatus


class _RecordingTaskHistory:
    """task 이력 기록을 메모리에 모으는 가짜 저장소."""

    def __init__(self):
        self.records: list[tuple[str, str]] = []  # (task_id, status)

    async def record(self, task):
        self.records.append((task.task_id, task.status.value))


async def test_executor_records_task_lifecycle_history():
    fake = _RecordingTaskHistory()
    app = create_executor_app(task_history=fake)
    transport = httpx.ASGITransport(app=app)

    payload = {
        "task_id": "t-1",
        "job_id": "job-1",
        "sub_query": "SELECT 1 WHERE dt IN ('1')",
        "target_table": "public.t",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        r = await c.post("/tasks", json=payload)
        assert r.status_code == 202
        # 백그라운드 _run 이 완료될 때까지 대기
        for _ in range(100):
            st = (await c.get("/tasks/t-1")).json()
            if st["status"] in ("DONE", "FAILED"):
                break
            await asyncio.sleep(0.01)
        assert st["status"] == "DONE"

    statuses = [s for (_tid, s) in fake.records]
    # 하나의 job/task 에 대해 상태 전이가 순서대로 기록되어야 한다
    assert statuses[0] == "QUEUED"
    assert "READING" in statuses
    assert "WRITING" in statuses
    assert statuses[-1] == "DONE"
    # 모든 기록이 같은 task_id 로 묶임
    assert {tid for (tid, _s) in fake.records} == {"t-1"}


async def test_executor_records_failure_history():
    fake = _RecordingTaskHistory()

    class _BoomBackend:
        def move(self, *a, **k):
            raise RuntimeError("boom")

    app = create_executor_app(backend=_BoomBackend(), task_history=fake)
    transport = httpx.ASGITransport(app=app)
    payload = {
        "task_id": "t-err",
        "job_id": "job-9",
        "sub_query": "SELECT 1",
        "target_table": "public.t",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(100):
            st = (await c.get("/tasks/t-err")).json()
            if st["status"] in ("DONE", "FAILED"):
                break
            await asyncio.sleep(0.01)
        assert st["status"] == "FAILED"

    assert [s for (_t, s) in fake.records][-1] == "FAILED"


def test_task_history_disabled_without_dsn():
    class _S:
        history_db_dsn = ""
        task_history_table = "task_history"

    repo = TaskHistoryRepository(_S())
    assert repo.enabled is False
    assert repo.executor_id  # 식별자는 생성됨


async def test_task_history_disabled_record_is_noop():
    class _S:
        history_db_dsn = ""
        task_history_table = "task_history"

    repo = TaskHistoryRepository(_S())
    task = Task(
        task_id="x", job_id="j", sub_query="SELECT 1", target_table="t",
        write_mode="append", partition_column="dt", partition_values=["'1'"],
        status=TaskStatus.QUEUED,
    )
    await repo.record(task)  # 예외 없이 통과
