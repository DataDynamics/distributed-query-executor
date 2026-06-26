"""Coordinator FastAPI 애플리케이션 팩토리."""

from __future__ import annotations

import asyncio
import itertools
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from core.metrics import collect_system_metrics
from .config import Settings, settings as default_settings
from .dispatcher import HttpDispatcher, JobRunner, LocalDispatcher
from .job_store import JobStore
from .models import CreateJobRequest, CreateJobResponse, Job, JobStatus, Task
from .monitor import HealthMonitor
from .parser import QueryValidationError, is_row_returning, validate_and_parse
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
    if runner is None:
        runner = (
            LocalDispatcher(settings)
            if settings.executor_mode == "local"
            else HttpDispatcher(settings)
        )
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
        description="SQL을 검증·분할하여 작업을 생성하고 비동기로 디스패치한다(202). "
        "dry_run=true 면 executor 호출 없이 생성된 쿼리만 반환한다(200). "
        "검증 실패 시 422(error_code 포함).",
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

        if req.exec_mode == "stage_insert":
            # Impala SELECT 결과를 Greenplum staging 에 적재 후 INSERT.
            # sub-query(분할된 SELECT)는 그대로 두고, staging/INSERT 정보를 함께 보낸다.
            if not (req.staging_table and req.staging_ddl and req.wrapper_query):
                raise QueryValidationError(
                    "STAGE_INSERT_REQUIRES_FIELDS",
                    "stage_insert 모드는 staging_table, staging_ddl, wrapper_query(INSERT) "
                    "가 모두 필요합니다.",
                )
        elif req.wrapper_query:
            # 감싸는 쿼리가 있으면 각 sub-query를 placeholder 자리에 끼워 넣는다.
            if req.wrapper_placeholder not in req.wrapper_query:
                raise QueryValidationError(
                    "WRAPPER_PLACEHOLDER_MISSING",
                    f"wrapper_query 에 placeholder '{req.wrapper_placeholder}' 가 없습니다.",
                )
            # copy(STDIN) 모드는 결과 행을 fetch→COPY 하므로 래퍼가 SELECT(행 반환)여야 한다.
            # INSERT 등 비-SELECT 래퍼는 statement/stage_insert 모드를 써야 한다.
            if req.exec_mode == "copy":
                probe = wrap("(SELECT 1)", req.wrapper_query, req.wrapper_placeholder)
                if not is_row_returning(probe, dialect):
                    raise QueryValidationError(
                        "COPY_WRAPPER_NOT_SELECT",
                        "copy 모드의 wrapper_query 는 행을 반환하는 SELECT 여야 합니다. "
                        "INSERT 등으로 감싸려면 exec_mode=statement 또는 stage_insert 를 사용하세요.",
                    )
            for sq in sub_queries:
                sq.sql = wrap(sq.sql, req.wrapper_query, req.wrapper_placeholder)

        executor_urls = _assign_executors(len(sub_queries), settings.executors)

        # dry-run: executor 호출 없이 생성된 쿼리만 로깅/반환(작업 미저장)
        if req.dry_run:
            plan = []
            for idx, (sq, url) in enumerate(zip(sub_queries, executor_urls), 1):
                entry = {
                    "executor_url": url,
                    "partition_values": sq.partition_values,
                    "sub_query": sq.sql,
                }
                logger.info(
                    "[dry-run] task#%d (exec_mode=%s) sub_query=%s",
                    idx, req.exec_mode, sq.sql,
                )
                if req.exec_mode == "stage_insert":
                    entry["staging_table"] = req.staging_table
                    entry["staging_ddl"] = req.staging_ddl
                    entry["insert_sql"] = req.wrapper_query
                    logger.info("[dry-run] task#%d staging_ddl=%s", idx, req.staging_ddl)
                    logger.info("[dry-run] task#%d insert_sql=%s", idx, req.wrapper_query)
                plan.append(entry)
            return JSONResponse(
                status_code=200,
                content={
                    "dry_run": True,
                    "exec_mode": req.exec_mode,
                    "partition_column": req.partition_column,
                    "target_table": req.target_table,
                    "task_count": len(plan),
                    "tasks": plan,
                },
            )

        job = Job(
            original_sql=req.sql,
            partition_column=req.partition_column,
            target_table=req.target_table,
            write_mode=req.write_mode,
            parallelism=req.parallelism,
            split_strategy=req.split_strategy,
            failure_policy=req.failure_policy,
            exec_mode=req.exec_mode,
            staging_table=req.staging_table,
            staging_ddl=req.staging_ddl,
            insert_sql=req.wrapper_query if req.exec_mode == "stage_insert" else None,
            status=JobStatus.SPLITTING,
        )
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

    @app.get("/jobs/{job_id}", tags=["Jobs"], summary="작업 상태 조회(태스크 포함)")
    def get_job(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.status_view()

    @app.get(
        "/jobs/{job_id}/status",
        tags=["Jobs"],
        summary="작업 진행 상태(진행률) 조회",
        description="job_id 로 현재 상태/진행률을 조회한다(태스크 목록 제외, 경량).",
    )
    def get_job_progress(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.progress_view()

    @app.post(
        "/jobs/{job_id}/cancel",
        tags=["Jobs"],
        summary="작업 취소",
        description="진행 중인 작업을 취소한다. 각 executor에 취소를 전파하고 job을 "
        "CANCELLED 로 표시한다. 이미 종료된 작업은 409.",
    )
    async def cancel_job(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status in (JobStatus.DONE, JobStatus.FAILED, JobStatus.CANCELLED):
            raise HTTPException(
                status_code=409,
                detail=f"이미 종료된 작업입니다(status={job.status.value}).",
            )
        await runner.cancel(job)
        job.status = JobStatus.CANCELLED
        return job.progress_view()

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

    @app.get(
        "/cluster",
        tags=["Monitoring"],
        summary="클러스터 전체 상태(coordinator+executor health/metrics + 실행 중 job 수)",
        description="coordinator와 모든 executor의 health 및 CPU/메모리/디스크, 그리고 "
        "실행 중인 job 수를 한 번에 반환한다. refresh=true(기본)면 executor를 즉시 폴링한다.",
    )
    async def cluster(refresh: bool = True):
        executors = await monitor.poll_now() if refresh else monitor.snapshot()
        coord_metrics = await asyncio.to_thread(
            collect_system_metrics, settings.monitor_disk_path
        )

        by_status: dict[str, int] = {}
        for job in store.list():
            by_status[job.status.value] = by_status.get(job.status.value, 0) + 1
        running = by_status.get(JobStatus.RUNNING.value, 0)
        active = running + by_status.get(JobStatus.SPLITTING.value, 0) + by_status.get(
            JobStatus.PENDING.value, 0
        )
        healthy = sum(1 for e in executors if e.get("healthy"))

        return {
            "coordinator": {
                "service": "coordinator",
                "status": "ok",
                "metrics": coord_metrics,
            },
            "executors": executors,
            "executors_summary": {
                "total": len(executors),
                "healthy": healthy,
                "unhealthy": len(executors) - healthy,
            },
            "jobs": {
                "running": running,
                "active": active,  # RUNNING + SPLITTING + PENDING
                "total": len(store.list()),
                "by_status": by_status,
            },
        }

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
