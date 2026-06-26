"""Job execution: dispatch sub-queries to executors and track their status.

The coordinator never receives result rows — executors stream Impala -> Greenplum
directly. Here we only POST sub-queries, poll status, and aggregate row counts.
"""

from __future__ import annotations

import asyncio
from typing import Protocol

import httpx

from .config import Settings
from .models import Job, JobStatus, Task, TaskStatus


class JobRunner(Protocol):
    async def run(self, job: Job) -> None: ...


class HttpDispatcher:
    """Real dispatcher that talks to executor services over HTTP."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._sem = asyncio.Semaphore(settings.max_dispatch_concurrency)

    async def run(self, job: Job) -> None:
        job.status = JobStatus.RUNNING
        try:
            async with httpx.AsyncClient(timeout=self.settings.task_timeout_s) as client:
                await asyncio.gather(
                    *(self._run_task(client, job, t) for t in job.tasks)
                )
        finally:
            self._finalize(job)

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
            except Exception as exc:  # network / timeout / executor error
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
