"""Coordinator FastAPI 애플리케이션 팩토리."""

from __future__ import annotations

import itertools
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from core.metrics import collect_system_metrics
from .config import Settings, settings as default_settings
from .dispatcher import HttpDispatcher, JobRunner
from .job_store import JobStore
from .models import CreateJobRequest, CreateJobResponse, Job, JobStatus, Task
from .monitor import HealthMonitor
from .parser import QueryValidationError, validate_and_parse
from .splitter import split, wrap

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
    monitor = HealthMonitor(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await monitor.start()
        try:
            yield
        finally:
            await monitor.stop()

    app = FastAPI(
        title="Distributed Query Coordinator",
        version="0.1.0",
        description=(
            "Impala `SELECT` 쿼리를 파티션 컬럼의 `IN` 목록 기준으로 N분할하여 여러 "
            "executor에 분배하고, 각 executor가 Greenplum에 병렬 적재하도록 조율한다.\n\n"
            "- 검증/분할/디스패치/상태추적, executor 헬스 모니터링\n"
            "- Swagger UI: `/docs`, ReDoc: `/redoc`, OpenAPI 스키마: `/openapi.json`"
        ),
        openapi_tags=[
            {"name": "Jobs", "description": "쿼리 작업 생성·조회·결과·태스크 상세"},
            {"name": "Monitoring", "description": "헬스 체크, 시스템 메트릭, executor 상태"},
        ],
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.runner = runner
    app.state.settings = settings
    app.state.monitor = monitor

    @app.exception_handler(QueryValidationError)
    async def _validation_handler(_: Request, exc: QueryValidationError):
        return JSONResponse(
            status_code=422,
            content={"error_code": exc.code, "message": exc.message},
        )

    @app.post(
        "/jobs",
        response_model=CreateJobResponse,
        status_code=202,
        tags=["Jobs"],
        summary="쿼리 작업 생성",
        description="SQL을 검증·분할하여 작업을 생성하고 비동기로 디스패치한다. "
        "검증 실패 시 422(error_code 포함)를 반환한다.",
    )
    def create_job(req: CreateJobRequest, background: BackgroundTasks):
        # 동기 검증 + 분할: 오류는 지금 즉시 클라이언트에게 반환한다.
        dialect = req.sql_dialect or settings.query_default_dialect
        parsed = validate_and_parse(
            req.sql,
            req.partition_column,
            dialect=dialect,
            strict=req.strict_validation,
        )
        sub_queries = split(parsed, req.parallelism, req.split_strategy)

        # 감싸는 쿼리가 있으면 각 sub-query를 placeholder 자리에 끼워 넣는다.
        if req.wrapper_query:
            if req.wrapper_placeholder not in req.wrapper_query:
                raise QueryValidationError(
                    "WRAPPER_PLACEHOLDER_MISSING",
                    f"wrapper_query 에 placeholder '{req.wrapper_placeholder}' 가 없습니다.",
                )
            for sq in sub_queries:
                sq.sql = wrap(sq.sql, req.wrapper_query, req.wrapper_placeholder)

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

    @app.get("/jobs/{job_id}", tags=["Jobs"], summary="작업 상태 조회")
    def get_job(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.status_view()

    @app.get("/jobs/{job_id}/result", tags=["Jobs"], summary="작업 결과(적재 요약) 조회")
    def get_job_result(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.result_view()

    @app.get(
        "/jobs/{job_id}/tasks/{task_id}",
        tags=["Jobs"],
        summary="태스크 상세 조회(sub-query 전문 포함)",
    )
    def get_task_detail(job_id: str, task_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        for task in job.tasks:
            if task.task_id == task_id:
                return task.detail()  # sub_query 전문 포함
        raise HTTPException(status_code=404, detail="task not found")

    @app.get(
        "/executors",
        tags=["Monitoring"],
        summary="executor 헬스/메트릭 상태",
        description="모니터가 주기 폴링으로 보유한 executor별 CPU/메모리/디스크 상태.",
    )
    def list_executor_health():
        return {"executors": monitor.snapshot()}

    @app.get("/health", tags=["Monitoring"], summary="헬스 체크(liveness)")
    def health():
        return {"status": "ok", "service": "coordinator", "version": "0.1.0"}

    @app.get("/healthz", tags=["Monitoring"], summary="헬스 체크 별칭(하위 호환)")
    def healthz():
        return {"status": "ok"}

    @app.get(
        "/metrics",
        tags=["Monitoring"],
        summary="시스템 메트릭(CPU/메모리/디스크)",
    )
    def metrics():
        return collect_system_metrics(settings.monitor_disk_path)

    return app


app = create_app()
