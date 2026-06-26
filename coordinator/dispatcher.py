"""Job 실행: sub-query를 executor로 디스패치하고 상태를 추적한다.

Coordinator는 결과 행을 직접 받지 않는다. executor가 Impala -> Greenplum 으로 직접
스트리밍한다. 여기서는 sub-query를 POST하고, 상태를 polling하며, row count만 집계한다.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, Protocol

import httpx

from .config import Settings
from .history import JobHistoryRepository
from .models import Job, JobStatus, Task, TaskStatus


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobRunner(Protocol):
    async def run(self, job: Job) -> str: ...


class HttpDispatcher:
    """executor 서비스와 HTTP로 통신하는 실제 디스패처."""

    def __init__(self, settings: Settings, history: Optional[JobHistoryRepository] = None):
        self.settings = settings
        self._sem = asyncio.Semaphore(settings.max_dispatch_concurrency)
        self.history = history or JobHistoryRepository(settings)

    async def run(self, job: Job) -> str:
        """Job 을 실행하고 job_id 를 반환한다. 시작/종료 이력을 DB에 기록한다."""
        job.status = JobStatus.RUNNING
        job.started_at = _now_iso()
        await self.history.record(job)  # 시작 이력
        try:
            async with httpx.AsyncClient(timeout=self.settings.task_timeout_s) as client:
                await asyncio.gather(
                    *(self._run_task(client, job, t) for t in job.tasks)
                )
        finally:
            self._finalize(job)
            job.finished_at = _now_iso()
            await self.history.record(job)  # 종료 이력
        return job.job_id

    async def _run_task(self, client: httpx.AsyncClient, job: Job, task: Task) -> None:
        async with self._sem:
            task.attempt += 1
            try:
                await client.post(
                    f"{task.executor_url}/tasks",
                    json={
                        "task_id": task.task_id,
                        "job_id": job.job_id,
                        "sub_query": task.sub_query,
                        "target_table": job.target_table,
                        "write_mode": job.write_mode,
                        "partition_column": job.partition_column,
                        "partition_values": task.partition_values,
                    },
                )
                await self._poll(client, task)
            except Exception as exc:  # 네트워크 / 타임아웃 / executor 오류
                task.status = TaskStatus.FAILED
                task.error = str(exc)

    async def _poll(self, client: httpx.AsyncClient, task: Task) -> None:
        terminal = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}
        while task.status not in terminal:
            await asyncio.sleep(self.settings.poll_interval_s)
            resp = await client.get(f"{task.executor_url}/tasks/{task.task_id}")
            data = resp.json()
            task.status = TaskStatus(data["status"])
            task.rows_written = data.get("rows_written", task.rows_written)
            task.error = data.get("error")

    def _finalize(self, job: Job) -> None:
        failed = [t for t in job.tasks if t.status == TaskStatus.FAILED]
        if not failed:
            job.status = JobStatus.DONE
        elif job.failure_policy == "best_effort":
            job.status = JobStatus.PARTIAL
        else:
            job.status = JobStatus.FAILED
            job.error = "; ".join(f"{t.task_id}: {t.error}" for t in failed)
