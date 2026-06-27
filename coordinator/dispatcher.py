"""Job 실행: sub-query를 executor로 디스패치하고 상태를 추적한다.

Coordinator는 결과 행을 직접 받지 않는다. executor가 Impala -> Greenplum 으로 직접
스트리밍한다. 여기서는 sub-query를 POST하고, 상태를 polling하며, row count만 집계한다.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Protocol

import httpx

from core.logging import job_log_context
from .config import Settings
from .history import JobHistoryRepository
from .models import Job, JobStatus, Task, TaskStatus


logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_TERMINAL = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}


def finalize_job(job: Job) -> None:
    """하위 task 상태를 집계해 Job 최종 상태를 정한다(취소/실패/부분/완료)."""
    failed = [t for t in job.tasks if t.status == TaskStatus.FAILED]
    if job.cancel_requested:
        job.status = JobStatus.CANCELLED
    elif not failed:
        job.status = JobStatus.DONE
    elif job.failure_policy == "best_effort":
        job.status = JobStatus.PARTIAL
    else:
        job.status = JobStatus.FAILED
        job.error = "; ".join(f"{t.task_id}: {t.error}" for t in failed)


class JobRunner(Protocol):
    async def run(self, job: Job) -> str: ...

    async def cancel(self, job: Job) -> None: ...


class JobAdmission:
    """동시 실행 job 수 제한 + 대기 큐 상한(큐잉 + 큐 상한 방식).

    - 실행 슬롯: 동시에 RUNNING 일 수 있는 job 수 = max_concurrent_jobs
    - 대기 큐 : 슬롯이 없을 때 PENDING 으로 대기 가능한 job 수 = max_pending_jobs
    - in-flight(=대기+실행)가 (실행+대기) 용량을 넘는 요청은 admit 거부 → 호출측에서 429.
    max_concurrent_jobs 가 0 이하이면 무제한(슬롯 제한/거부 모두 비활성).

    단일 이벤트 루프에서만 변경되므로 카운터는 별도 락 없이 안전하다.
    """

    def __init__(self, settings: Settings):
        self.max_running = int(getattr(settings, "max_concurrent_jobs", 0) or 0)
        self.max_pending = int(getattr(settings, "max_pending_jobs", 0) or 0)
        self._sem = asyncio.Semaphore(self.max_running) if self.max_running > 0 else None
        self._inflight = 0

    @property
    def capacity(self) -> Optional[int]:
        """admit 가능한 최대 in-flight 수(실행+대기). None 이면 무제한."""
        if self.max_running <= 0:
            return None
        return self.max_running + max(0, self.max_pending)

    @property
    def inflight(self) -> int:
        return self._inflight

    def try_admit(self) -> bool:
        """수용 가능하면 in-flight 를 1 늘리고 True, 용량 초과면 False(→429)."""
        cap = self.capacity
        if cap is not None and self._inflight >= cap:
            return False
        self._inflight += 1
        return True

    def release(self) -> None:
        """job 이 종료(또는 수용 후 실행 실패)되어 in-flight 슬롯을 반납."""
        if self._inflight > 0:
            self._inflight -= 1

    @asynccontextmanager
    async def slot(self):
        """실행 슬롯을 확보(없으면 대기). max_running<=0 이면 즉시 통과."""
        if self._sem is None:
            yield
            return
        async with self._sem:
            yield


class _DispatcherBase:
    """admission control(큐잉+큐상한) + PENDING 전이 + 종료 처리를 공통화한 베이스.

    하위 클래스는 실제 task 실행부 `_execute(job)` 만 구현한다.
    """

    def __init__(self, settings: Settings, history: Optional[JobHistoryRepository] = None, store=None):
        self.settings = settings
        self._sem = asyncio.Semaphore(settings.max_dispatch_concurrency)
        self.history = history or JobHistoryRepository(settings)
        self.store = store
        self.admission = JobAdmission(settings)

    def _save(self, job: Job) -> None:
        if self.store is None:
            return
        try:
            self.store.save(job)
        except Exception:
            logger.exception("job %s 저장 실패", job.job_id)

    def _cancel_observed(self, job: Job) -> bool:
        """로컬 플래그 또는 공유 store 의 취소 요청을 확인(멀티 coordinator)."""
        if job.cancel_requested:
            return True
        if self.store is not None:
            try:
                if self.store.is_cancel_requested(job.job_id):
                    job.cancel_requested = True
                    return True
            except Exception:
                logger.exception("취소 플래그 조회 실패 job=%s", job.job_id)
        return False

    async def _execute(self, job: Job) -> None:
        """하위 클래스가 구현: job.tasks 를 실제로 디스패치/실행한다."""
        raise NotImplementedError

    async def run(self, job: Job) -> str:
        """Job 을 실행하고 job_id 를 반환한다.

        실행 슬롯이 없으면 PENDING 으로 대기(큐잉)하다 슬롯 확보 후 RUNNING 으로 전이한다.
        시작/종료 이력을 DB에 기록한다. in-flight 반납은 항상 보장한다.
        """
        with job_log_context(job.job_id):
            try:
                # 슬롯이 빌 때까지 대기. 그동안 job 은 PENDING 으로 노출된다.
                job.status = JobStatus.PENDING
                self._save(job)
                async with self.admission.slot():
                    # 대기 중 취소되었으면 실행하지 않고 즉시 종료 처리.
                    if self._cancel_observed(job):
                        finalize_job(job)
                        job.finished_at = _now_iso()
                        self._save(job)
                        await self.history.record(job)
                        return job.job_id
                    job.status = JobStatus.RUNNING
                    job.started_at = _now_iso()
                    self._save(job)
                    await self.history.record(job)  # 시작 이력
                    try:
                        await self._execute(job)
                    finally:
                        finalize_job(job)
                        job.finished_at = _now_iso()
                        self._save(job)
                        await self.history.record(job)  # 종료 이력
            finally:
                self.admission.release()
            return job.job_id


class HttpDispatcher(_DispatcherBase):
    """executor 서비스와 HTTP로 통신하는 실제 디스패처."""

    async def _execute(self, job: Job) -> None:
        async with httpx.AsyncClient(timeout=self.settings.task_timeout_s) as client:
            await asyncio.gather(
                *(self._run_task(client, job, t) for t in job.tasks)
            )

    async def _run_task(self, client: httpx.AsyncClient, job: Job, task: Task) -> None:
        async with self._sem:
            if job.cancel_requested:
                task.status = TaskStatus.CANCELLED
                return
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
                        "exec_mode": job.exec_mode,
                        "staging_table": job.staging_table,
                        "staging_ddl": job.staging_ddl,
                        "insert_sql": job.insert_sql,
                        "username": job.username,
                    },
                )
                await self._poll(client, job, task)
            except Exception as exc:  # 네트워크 / 타임아웃 / executor 오류
                task.status = TaskStatus.FAILED
                task.error = str(exc)
            finally:
                self._save(job)

    async def _poll(self, client: httpx.AsyncClient, job: Job, task: Task) -> None:
        while task.status not in _TERMINAL:
            if self._cancel_observed(job):
                task.status = TaskStatus.CANCELLED
                return
            await asyncio.sleep(self.settings.poll_interval_s)
            resp = await client.get(f"{task.executor_url}/tasks/{task.task_id}")
            data = resp.json()
            task.status = TaskStatus(data["status"])
            task.rows_written = data.get("rows_written", task.rows_written)
            task.error = data.get("error")

    async def cancel(self, job: Job) -> None:
        """취소 요청: 플래그를 세우고 비종료 task의 executor에 취소를 전파한다."""
        with job_log_context(job.job_id):
            job.cancel_requested = True
            targets = [
                t for t in job.tasks if t.executor_url and t.status not in _TERMINAL
            ]
            if targets:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await asyncio.gather(
                        *(self._cancel_task(client, t) for t in targets)
                    )

    async def _cancel_task(self, client: httpx.AsyncClient, task: Task) -> None:
        try:
            await client.post(f"{task.executor_url}/tasks/{task.task_id}/cancel")
        except Exception as exc:
            task.error = task.error or str(exc)
        task.status = TaskStatus.CANCELLED


class LocalDispatcher(_DispatcherBase):
    """로컬(in-process) 디스패처: executor를 HTTP로 호출하지 않고 백엔드를 직접 실행한다.

    별도 executor 프로세스 없이 coordinator 안에서 동작 검증을 하기 위한 모드.
    기본 백엔드는 build_backend(settings)(greenplum.dsn 없으면 MockBackend).
    """

    def __init__(self, settings: Settings, history: Optional[JobHistoryRepository] = None, backend=None, store=None):
        super().__init__(settings, history=history, store=store)
        self._backend = backend

    def _get_backend(self):
        if self._backend is None:
            from executor.backend import build_backend  # 지연 임포트(순환 방지)
            self._backend = build_backend(self.settings)
        return self._backend

    async def _execute(self, job: Job) -> None:
        await asyncio.gather(*(self._run_task(job, t) for t in job.tasks))

    async def _run_task(self, job: Job, task: Task) -> None:
        async with self._sem:
            if self._cancel_observed(job):
                task.status = TaskStatus.CANCELLED
                return
            backend = self._get_backend()
            loop = asyncio.get_running_loop()
            try:
                task.status = TaskStatus.READING
                task.status = TaskStatus.WRITING
                if job.exec_mode == "statement":
                    rows = await loop.run_in_executor(
                        None, lambda: backend.execute(task.sub_query)
                    )
                elif job.exec_mode == "stage_insert":
                    rows = await loop.run_in_executor(
                        None,
                        lambda: backend.stage_and_insert(
                            task.sub_query, job.staging_table, job.staging_ddl, job.insert_sql
                        ),
                    )
                else:
                    rows = await loop.run_in_executor(
                        None,
                        lambda: backend.move(
                            task.sub_query, job.target_table, job.write_mode,
                            job.partition_column, task.partition_values,
                        ),
                    )
                task.rows_written = rows
                task.status = (
                    TaskStatus.CANCELLED if job.cancel_requested else TaskStatus.DONE
                )
            except Exception as exc:
                task.status = TaskStatus.FAILED
                task.error = str(exc)
            finally:
                self._save(job)

    async def cancel(self, job: Job) -> None:
        with job_log_context(job.job_id):
            job.cancel_requested = True
            for t in job.tasks:
                if t.status not in _TERMINAL:
                    t.status = TaskStatus.CANCELLED
