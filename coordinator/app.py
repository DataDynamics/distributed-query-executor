"""Coordinator FastAPI 애플리케이션 팩토리."""

from __future__ import annotations

import itertools
import logging
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import Settings, settings as default_settings
from .dispatcher import HttpDispatcher, JobRunner
from .job_store import JobStore
from .models import CreateJobRequest, CreateJobResponse, Job, JobStatus, Task
from .parser import QueryValidationError, validate_and_parse
from .splitter import split

logger = logging.getLogger(__name__)


def _assign_executors(count: int, executors: list[str]) -> list[Optional[str]]:
    """N개의 task에 executor URL을 라운드로빈 배정(설정 없으면 None)."""
    if not executors:
        return [None] * count
    cycle = itertools.cycle(executors)
    return [next(cycle) for _ in range(count)]


def create_app(
    runner: Optional[JobRunner] = None,
    store: Optional[JobStore] = None,
    settings: Optional[Settings] = None,
) -> FastAPI:
    settings = settings or default_settings
    store = store or JobStore()
    runner = runner or HttpDispatcher(settings)

    app = FastAPI(title="Query Coordinator", version="0.1.0")
    app.state.store = store
    app.state.runner = runner
    app.state.settings = settings

    @app.exception_handler(QueryValidationError)
    async def _validation_handler(_: Request, exc: QueryValidationError):
        return JSONResponse(
            status_code=422,
            content={"error_code": exc.code, "message": exc.message},
        )

    @app.post("/jobs", response_model=CreateJobResponse, status_code=202)
    def create_job(req: CreateJobRequest, background: BackgroundTasks):
        # 동기 검증 + 분할: 오류는 지금 즉시 클라이언트에게 반환한다.
        parsed = validate_and_parse(req.sql, req.partition_column)
        sub_queries = split(parsed, req.parallelism, req.split_strategy)

        job = Job(
            original_sql=req.sql,
            partition_column=req.partition_column,
            target_table=req.target_table,
            write_mode=req.write_mode,
            parallelism=req.parallelism,
            split_strategy=req.split_strategy,
            failure_policy=req.failure_policy,
            status=JobStatus.SPLITTING,
        )
        executor_urls = _assign_executors(len(sub_queries), settings.executors)
        job.tasks = [
            Task(
                job_id=job.job_id,
                executor_url=url,
                sub_query=sq.sql,
                partition_values=sq.partition_values,
            )
            for sq, url in zip(sub_queries, executor_urls)
        ]
        store.add(job)

        logger.info(
            "job %s 생성: %d개 sub-query로 분할 (partition=%s, target=%s)",
            job.job_id,
            len(job.tasks),
            req.partition_column,
            req.target_table,
        )
        background.add_task(runner.run, job)
        return CreateJobResponse(job_id=job.job_id)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.status_view()

    @app.get("/jobs/{job_id}/result")
    def get_job_result(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.result_view()

    @app.get("/jobs/{job_id}/tasks/{task_id}")
    def get_task_detail(job_id: str, task_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        for task in job.tasks:
            if task.task_id == task_id:
                return task.detail()  # sub_query 전문 포함
        raise HTTPException(status_code=404, detail="task not found")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app


app = create_app()
