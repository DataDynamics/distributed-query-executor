"""Executor FastAPI 애플리케이션: task를 받아 실행하고 상태를 노출한다."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException

from core.config import settings
from core.logging import job_log_context
from core.metrics import collect_system_metrics
from .backend import Backend, build_backend
from .history import TaskHistoryRepository
from .models import CreateTaskRequest, Task, TaskStatus

logger = logging.getLogger(__name__)


def _build_backend() -> Backend:
    """설정 기반 백엔드 선택(공용 build_backend 위임)."""
    return build_backend(settings)


def create_app(
    backend: Optional[Backend] = None,
    task_history: Optional[TaskHistoryRepository] = None,
) -> FastAPI:
    backend = backend or _build_backend()
    history = task_history or TaskHistoryRepository(settings)
    tasks: dict[str, Task] = {}

    app = FastAPI(
        title="Distributed Query Executor",
        version="0.1.0",
        description=(
            "coordinator가 분할한 Impala sub-query를 받아 실행하고, 결과를 Greenplum에 "
            "적재한다. 자신의 task 상태와 시스템 메트릭을 노출한다.\n\n"
            "- Swagger UI: `/docs`, ReDoc: `/redoc`, OpenAPI 스키마: `/openapi.json`"
        ),
        openapi_tags=[
            {"name": "Tasks", "description": "sub-query 태스크 접수·상태·결과"},
            {"name": "Monitoring", "description": "헬스 체크, 시스템 메트릭"},
        ],
    )
    app.state.backend = backend
    app.state.tasks = tasks
    app.state.task_history = history

    async def _run_with_ctx(task: Task) -> None:
        # 백그라운드 실행 로그에도 [job_id][task_id] 가 붙도록 컨텍스트 바인딩
        with job_log_context(task.job_id, task.task_id):
            await _run(task)

    async def _run(task: Task) -> None:
        def progress(n: int) -> None:
            task.rows_written = n

        try:
            if task.cancel_requested:
                task.status = TaskStatus.CANCELLED
                await history.record(task)  # CANCELLED 이력
                return
            task.status = TaskStatus.READING
            await history.record(task)  # READING 이력
            loop = asyncio.get_running_loop()
            # impyla/psycopg는 블로킹이므로 스레드에서 실행해 이벤트 루프를 막지 않는다.
            task.status = TaskStatus.WRITING
            await history.record(task)  # WRITING 이력
            if task.exec_mode == "statement":
                # wrapper 로 감싼 INSERT 등을 대상 DB에서 그대로 실행(COPY 미사용)
                rows = await loop.run_in_executor(
                    None, lambda: app.state.backend.execute(task.sub_query)
                )
            elif task.exec_mode == "stage_insert":
                # Impala 결과를 Greenplum staging(TEMP)에 COPY → staging→target INSERT
                rows = await loop.run_in_executor(
                    None,
                    lambda: app.state.backend.stage_and_insert(
                        task.sub_query,
                        task.staging_table,
                        task.staging_ddl,
                        task.insert_sql,
                        progress,
                    ),
                )
            else:
                # copy 모드: Impala read → Greenplum COPY
                rows = await loop.run_in_executor(
                    None,
                    lambda: app.state.backend.move(
                        task.sub_query,
                        task.target_table,
                        task.write_mode,
                        task.partition_column,
                        task.partition_values,
                        progress,
                    ),
                )
            task.rows_written = rows
            # 실행 중 취소 요청이 들어왔으면 DONE 대신 CANCELLED 처리
            if task.cancel_requested:
                task.status = TaskStatus.CANCELLED
                logger.info("task %s 취소됨", task.task_id)
                await history.record(task)
                return
            task.status = TaskStatus.DONE
            logger.info("task %s 완료: %s행 적재", task.task_id, rows)
            await history.record(task)  # DONE 이력
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = str(exc)
            logger.exception("task %s 실패", task.task_id)
            await history.record(task)  # FAILED 이력

    @app.post(
        "/tasks",
        status_code=202,
        tags=["Tasks"],
        summary="sub-query 태스크 접수",
        description="sub-query를 받아 Impala 읽기 → Greenplum 적재를 비동기로 시작한다.",
    )
    async def create_task(req: CreateTaskRequest):
        task = Task(
            task_id=req.task_id,
            job_id=req.job_id,
            sub_query=req.sub_query,
            target_table=req.target_table,
            write_mode=req.write_mode,
            partition_column=req.partition_column,
            partition_values=req.partition_values,
            exec_mode=req.exec_mode,
            staging_table=req.staging_table,
            staging_ddl=req.staging_ddl,
            insert_sql=req.insert_sql,
        )
        tasks[task.task_id] = task
        with job_log_context(task.job_id, task.task_id):
            await history.record(task)  # QUEUED 이력
            asyncio.create_task(_run_with_ctx(task))
            logger.info("task %s 접수 (job=%s)", task.task_id, task.job_id)
        return {"task_id": task.task_id, "status": task.status.value}

    @app.get("/tasks/{task_id}", tags=["Tasks"], summary="태스크 상태 조회")
    def get_task(task_id: str):
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return task.view()

    @app.get("/tasks/{task_id}/result", tags=["Tasks"], summary="태스크 결과(적재 행수) 조회")
    def get_task_result(task_id: str):
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return {"rows_written": task.rows_written}

    @app.post("/tasks/{task_id}/cancel", tags=["Tasks"], summary="태스크 취소")
    async def cancel_task(task_id: str):
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        terminal = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}
        if task.status in terminal:
            return task.view()  # 이미 종료 — 변경 없음
        task.cancel_requested = True
        # 아직 시작 전이면 즉시 취소 확정, 실행 중이면 _run 이 완료 후 CANCELLED 처리
        if task.status == TaskStatus.QUEUED:
            task.status = TaskStatus.CANCELLED
            await history.record(task)
        return task.view()

    @app.get("/health", tags=["Monitoring"], summary="헬스 체크(liveness)")
    def health():
        return {"status": "ok", "service": "executor", "version": "0.1.0"}

    @app.get("/healthz", tags=["Monitoring"], summary="헬스 체크 별칭(하위 호환)")
    def healthz():
        return {"status": "ok"}

    @app.get("/metrics", tags=["Monitoring"], summary="시스템 메트릭(CPU/메모리/디스크)")
    def metrics():
        return collect_system_metrics(settings.monitor_disk_path)

    return app


app = create_app()
