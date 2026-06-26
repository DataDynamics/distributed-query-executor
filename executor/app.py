"""Executor FastAPI 애플리케이션: task를 받아 실행하고 상태를 노출한다."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException

from core.config import settings
from core.metrics import collect_system_metrics
from .backend import Backend, ImpalaToGreenplumBackend, MockBackend
from .models import CreateTaskRequest, Task, TaskStatus

logger = logging.getLogger(__name__)


def _build_backend() -> Backend:
    """설정에 따라 실제 백엔드 또는 MockBackend를 선택한다.

    impala.host 와 greenplum.dsn 이 모두 설정되어 있으면 실제 Impala→Greenplum
    백엔드를 사용하고, 그렇지 않으면 MockBackend(실제 I/O 없음)로 동작한다.
    """
    if settings.impala_host and settings.greenplum_dsn:
        impala_dsn: dict = {
            "host": settings.impala_host,
            "port": settings.impala_port,
            "database": settings.impala_database,
            "auth_mechanism": settings.impala_auth_mechanism,
            "use_ssl": settings.impala_use_ssl,
        }
        # TLS: CA 인증서로 서버 검증
        if settings.impala_ca_cert:
            impala_dsn["ca_cert"] = settings.impala_ca_cert
        # Kerberos(GSSAPI): 서비스명 지정. 티켓은 OS 자격증명 캐시(KRB5CCNAME)를 사용한다.
        if settings.impala_auth_mechanism.upper() == "GSSAPI":
            impala_dsn["kerberos_service_name"] = settings.impala_kerberos_service_name
        else:
            # LDAP/PLAIN 인증일 때만 user/password 사용
            if settings.impala_user:
                impala_dsn["user"] = settings.impala_user
            if settings.impala_password:
                impala_dsn["password"] = settings.impala_password
        logger.info(
            "ImpalaToGreenplumBackend 사용 (impala=%s:%s, auth=%s, ssl=%s, batch=%s)",
            settings.impala_host,
            settings.impala_port,
            settings.impala_auth_mechanism,
            settings.impala_use_ssl,
            settings.copy_batch_size,
        )
        return ImpalaToGreenplumBackend(
            impala_dsn=impala_dsn,
            greenplum_dsn=settings.greenplum_dsn,
            batch_size=settings.copy_batch_size,
        )
    logger.warning("impala.host/greenplum.dsn 미설정 → MockBackend 사용")
    return MockBackend()


def create_app(backend: Optional[Backend] = None) -> FastAPI:
    backend = backend or _build_backend()
    tasks: dict[str, Task] = {}

    app = FastAPI(title="Query Executor", version="0.1.0")
    app.state.backend = backend
    app.state.tasks = tasks

    async def _run(task: Task) -> None:
        def progress(n: int) -> None:
            task.rows_written = n

        try:
            task.status = TaskStatus.READING
            loop = asyncio.get_running_loop()
            # impyla/psycopg는 블로킹이므로 스레드에서 실행해 이벤트 루프를 막지 않는다.
            task.status = TaskStatus.WRITING
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
            task.status = TaskStatus.DONE
            logger.info("task %s 완료: %s행 적재", task.task_id, rows)
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = str(exc)
            logger.exception("task %s 실패", task.task_id)

    @app.post("/tasks", status_code=202)
    async def create_task(req: CreateTaskRequest):
        task = Task(
            task_id=req.task_id,
            job_id=req.job_id,
            sub_query=req.sub_query,
            target_table=req.target_table,
            write_mode=req.write_mode,
            partition_column=req.partition_column,
            partition_values=req.partition_values,
        )
        tasks[task.task_id] = task
        asyncio.create_task(_run(task))
        logger.info("task %s 접수 (job=%s)", task.task_id, task.job_id)
        return {"task_id": task.task_id, "status": task.status.value}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return task.view()

    @app.get("/tasks/{task_id}/result")
    def get_task_result(task_id: str):
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return {"rows_written": task.rows_written}

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "executor", "version": "0.1.0"}

    @app.get("/healthz")  # 하위 호환 별칭
    def healthz():
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics():
        return collect_system_metrics(settings.monitor_disk_path)

    return app


app = create_app()
