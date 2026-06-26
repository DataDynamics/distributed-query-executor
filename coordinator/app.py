"""Coordinator FastAPI application factory."""

from __future__ import annotations

import itertools
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import Settings, settings as default_settings
from .dispatcher import HttpDispatcher, JobRunner
from .job_store import JobStore
from .models import CreateJobRequest, CreateJobResponse, Job, JobStatus, Task
from .parser import QueryValidationError, validate_and_parse
from .splitter import split


def _assign_executors(count: int, executors: list[str]) -> list[Optional[str]]:
    """Round-robin executor URLs across N tasks (None if none configured)."""
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
        # Synchronous validation + split: errors are returned to the client now.
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
                return task.detail()  # includes the full sub_query text
        raise HTTPException(status_code=404, detail="task not found")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app


app = create_app()
