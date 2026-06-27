"""Executor FastAPI 애플리케이션: task를 받아 실행하고 상태를 노출한다.

coordinator 가 분할한 sub-query(task)를 ``POST /tasks`` 로 접수해 백그라운드에서 실행하고,
실행 상태/시스템 메트릭/이력을 REST 및 대시보드로 노출하는 executor 프로세스의 본체다.

핵심 흐름:
    1. 접수: ``create_task`` 가 요청을 ``Task`` 로 만들어 인메모리 ``tasks`` dict 에 넣고
       QUEUED 이력을 남긴 뒤, ``asyncio.create_task`` 로 실행을 비동기 시작하고 202 를 반환.
    2. 실행: ``_run`` 이 상태를 READING → WRITING → DONE(또는 FAILED/CANCELLED)으로 전이시키며,
       블로킹 DB 드라이버(impyla/psycopg)는 스레드 풀에서 돌려 이벤트 루프를 막지 않는다.
       각 전이마다 ``history.record`` 로 한 행씩 이력을 append 한다.
    3. 동시성 제어: ``_run_with_ctx`` 가 admission 세마포어로 동시에 실행되는 task 수를
       상한(executor_max_concurrent_tasks)으로 제한한다. 0/미설정이면 무제한.

상태는 인메모리(dict)이므로 인스턴스당 단일 워커로 동작한다(__main__ 참고).
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from core.config import settings
from core.logging import job_log_context
from core.metrics import collect_system_metrics
from .backend import Backend, build_backend
from .dashboard import DASHBOARD_HTML, masked_config
from .history import TaskHistoryRepository, _executor_id
from .models import CreateTaskRequest, Task, TaskStatus
from .status import ExecutorStatusReporter

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """현재 시각을 ISO8601(UTC, tz 포함) 문자열로 반환. started_at/finished_at 기록용."""
    return datetime.now(timezone.utc).isoformat()


# "활성"(처리중)으로 간주하는 task 상태: 처리중 Task 탭 집계 기준.
_ACTIVE_STATUSES = {TaskStatus.QUEUED, TaskStatus.READING, TaskStatus.WRITING}


def _build_backend() -> Backend:
    """설정 기반 백엔드 선택(공용 build_backend 위임)."""
    return build_backend(settings)


def create_app(
    backend: Optional[Backend] = None,
    task_history: Optional[TaskHistoryRepository] = None,
) -> FastAPI:
    """Executor FastAPI 앱을 구성해 반환하는 팩토리.

    의존성(backend, task_history)을 인자로 주입할 수 있어 테스트에서 MockBackend/
    가짜 이력 저장소를 끼워 넣기 쉽다. 미지정 시 설정 기반 기본 구현을 생성한다.
    인메모리 ``tasks`` dict, 동시 실행 상한 세마포어, self-report 리포터, 모든 라우트가
    이 클로저 안에서 만들어진다(앱 인스턴스마다 독립적인 상태).

    인자:
        backend: Impala→Greenplum 적재 백엔드. None 이면 설정으로 자동 선택.
        task_history: task 상태 전이 이력 저장소. None 이면 설정으로 생성.

    반환:
        라우트와 lifespan 이 등록된 ``FastAPI`` 인스턴스.
    """
    backend = backend or _build_backend()
    history = task_history or TaskHistoryRepository(settings)
    tasks: dict[str, Task] = {}
    # 동시 task 상한(admission control). 0 이면 무제한.
    _max = settings.executor_max_concurrent_tasks
    sem = asyncio.Semaphore(_max) if _max and _max > 0 else None
    reporter = ExecutorStatusReporter(settings, tasks_provider=lambda: _task_counts())

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # 앱 수명주기 훅: 기동 시 self-report 백그라운드 루프를 켜고(설정 시),
        # 종료 시 반드시 멈춘다(finally 로 정상/예외 종료 모두 정리 보장).
        if settings.executor_self_report:
            await reporter.start()
        try:
            yield
        finally:
            await reporter.stop()

    app = FastAPI(
        lifespan=lifespan,
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
        """admission 세마포어와 로그 컨텍스트로 감싼 task 실행 래퍼.

        백그라운드 실행 로그에도 [job_id][task_id] 가 붙도록 컨텍스트를 바인딩하고,
        동시 실행 task 수를 상한으로 제한한다. 세마포어가 있으면(상한 설정) 슬롯을 얻을
        때까지 대기하므로, 접수된 task 는 QUEUED 상태로 머물다 슬롯이 나면 실행된다.
        세마포어가 None(무제한)이면 즉시 실행한다.
        """
        with job_log_context(task.job_id, task.task_id):
            if sem is not None:
                async with sem:
                    await _run(task)
            else:
                await _run(task)

    async def _run(task: Task) -> None:
        """task 본 실행: 상태 전이(READING→WRITING→DONE)와 이력 기록을 수행한다.

        exec_mode 에 따라 백엔드 호출이 갈린다(statement / stage_insert / copy). 블로킹
        DB 드라이버는 ``run_in_executor`` 로 스레드 풀에서 실행해 이벤트 루프를 막지 않는다.
        실행 전후로 취소 요청을 확인해 CANCELLED 로 전이할 수 있고, 어느 단계든 예외가
        나면 FAILED 로 전이하며 메시지를 ``task.error`` 에 담는다. 모든 상태 전이마다
        ``history.record`` 로 이력 한 행을 남기고, 종료 시각은 ``finished_at`` 에 기록한다.
        예외를 밖으로 던지지 않는다(백그라운드 task 이므로 자체적으로 마무리).
        """
        def progress(n: int) -> None:
            # 백엔드가 배치 적재마다 호출하는 진행률 콜백 — 누적 적재 행수를 task 에 반영.
            task.rows_written = n

        try:
            if task.cancel_requested:
                task.status = TaskStatus.CANCELLED
                task.finished_at = _now_iso()
                await history.record(task)  # CANCELLED 이력
                return
            task.status = TaskStatus.READING
            task.started_at = _now_iso()
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
                task.finished_at = _now_iso()
                logger.info("task %s 취소됨", task.task_id)
                await history.record(task)
                return
            task.status = TaskStatus.DONE
            task.finished_at = _now_iso()
            logger.info("task %s 완료: %s행 적재", task.task_id, rows)
            await history.record(task)  # DONE 이력
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = str(exc)
            task.finished_at = _now_iso()
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
        """sub-query task 를 접수해 비동기 실행을 시작한다(HTTP 202).

        요청을 ``Task`` 로 변환해 인메모리 ``tasks`` 에 등록하고, QUEUED 이력을 남긴 뒤
        ``_run_with_ctx`` 를 백그라운드 task 로 띄운다. 실행 완료를 기다리지 않고 즉시
        task_id 와 현재 상태(QUEUED)를 반환하므로, 호출자(coordinator)는 폴링으로 진행을
        추적한다. 동일 task_id 재요청 시 기존 항목을 덮어쓴다.
        """
        task = Task(
            task_id=req.task_id,
            job_id=req.job_id,
            sub_query=req.sub_query,
            target_table=req.target_table,
            write_mode=req.write_mode,
            partition_column=req.partition_column,
            partition_values=req.partition_values,
            username=req.username,
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

    @app.get("/tasks", tags=["Tasks"], summary="태스크 목록(현재 executor 보유분)")
    def list_tasks(status: Optional[str] = None, limit: int = 0):
        """현재 executor 가 보유한 task 목록과 집계(total/active/running)를 반환한다.

        인메모리 dict 만 보므로 이 executor 가 처리한 분량만 나온다(전역 이력은 /history).
        ``status`` 로 필터링하고(특수값 "active"/"running" 은 처리중 상태 집합으로 매핑),
        ``limit>0`` 이면 상위 N개로 자른다.

        인자:
            status: 상태 필터. "active"/"running" 또는 개별 상태값(대소문자 무시).
            limit: 0 이면 전체, 양수면 최근 시작 기준 상위 N개.
        """
        all_tasks = list(tasks.values())
        total = len(all_tasks)
        active = sum(1 for t in all_tasks if t.status in _ACTIVE_STATUSES)
        running = sum(
            1 for t in all_tasks
            if t.status in (TaskStatus.READING, TaskStatus.WRITING)
        )
        # 최근 시작분이 위로 오도록 정렬(시작 전 task 는 started_at 이 없어 "" 로 뒤로).
        rows = sorted(all_tasks, key=lambda t: t.started_at or "", reverse=True)
        if status:
            s = status.lower()
            if s in ("active", "running"):
                rows = [t for t in rows if t.status in _ACTIVE_STATUSES]
            else:
                rows = [t for t in rows if t.status.value.lower() == s]
        if limit and limit > 0:
            rows = rows[:limit]
        return {
            "tasks": [t.view() for t in rows],
            "total": total,
            "active": active,
            "running": running,
        }

    @app.get("/tasks/{task_id}", tags=["Tasks"], summary="태스크 상태 조회")
    def get_task(task_id: str):
        """단일 task 의 현재 상태를 조회한다. 없으면 404."""
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return task.view()

    @app.get("/tasks/{task_id}/result", tags=["Tasks"], summary="태스크 결과(적재 행수) 조회")
    def get_task_result(task_id: str):
        """task 의 결과(누적 적재 행수)를 조회한다. 없으면 404.

        진행 중에는 중간 누적값, 완료 후에는 최종 적재 행수를 반환한다.
        """
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return {"rows_written": task.rows_written}

    @app.post("/tasks/{task_id}/cancel", tags=["Tasks"], summary="태스크 취소")
    async def cancel_task(task_id: str):
        """task 취소를 요청한다(협력적 취소).

        이미 종료(DONE/FAILED/CANCELLED)된 task 는 변경 없이 현재 상태를 반환한다.
        아직 시작 전(QUEUED)이면 즉시 CANCELLED 로 확정하고 종료 시각/이력을 남긴다.
        실행 중이면 ``cancel_requested`` 플래그만 세우고, ``_run`` 이 다음 안전 지점에서
        이를 확인해 DONE 대신 CANCELLED 로 마무리한다(강제 중단하지 않음).
        """
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
            task.finished_at = _now_iso()
            await history.record(task)
        return task.view()

    @app.get("/health", tags=["Monitoring"], summary="헬스 체크(liveness)")
    def health():
        """liveness 체크: 프로세스가 떠 있으면 서비스명/버전과 함께 ok 반환."""
        return {"status": "ok", "service": "executor", "version": "0.1.0"}

    @app.get("/healthz", tags=["Monitoring"], summary="헬스 체크 별칭(하위 호환)")
    def healthz():
        """k8s 등 관례적 ``/healthz`` 경로용 헬스 체크 별칭."""
        return {"status": "ok"}

    def _task_counts() -> tuple[int, int, int]:
        """동시 처리 현황 (active, queued, max) 튜플을 계산한다.

        active 는 실제 실행 중(READING/WRITING), queued 는 대기 중(QUEUED) task 수,
        max 는 동시 실행 상한(0 이면 무제한). self-report 와 /metrics·/info 가 공유한다.
        """
        active = sum(
            1 for t in tasks.values()
            if t.status in (TaskStatus.READING, TaskStatus.WRITING)
        )
        queued = sum(1 for t in tasks.values() if t.status == TaskStatus.QUEUED)
        return active, queued, (_max or 0)

    @app.get("/metrics", tags=["Monitoring"], summary="시스템 메트릭(CPU/메모리/디스크) + 동시 처리")
    def metrics():
        """시스템 메트릭(CPU/메모리/디스크)에 동시 처리 현황을 더해 반환한다.

        coordinator 가 executor 부하를 보고 스케줄링 판단에 쓸 수 있도록 노출한다.
        """
        m = collect_system_metrics(settings.monitor_disk_path)
        active, queued, mx = _task_counts()
        m["tasks"] = {"active": active, "queued": queued, "max": mx}
        return m

    # 대시보드 활성화 시에만 UI 및 보조 조회 엔드포인트(/, /history, /config, /info)를 등록.
    # started_at/start_monotonic 은 /info 의 uptime 계산 기준점(monotonic 은 시계 변경에 둔감).
    if settings.dashboard_enabled:
        started_at = datetime.now(timezone.utc)
        start_monotonic = time.monotonic()
        history_reader = TaskHistoryRepository(settings)

        @app.get("/", include_in_schema=False)
        def dashboard():
            """대시보드 단일 페이지 HTML 을 그대로 서빙한다."""
            return HTMLResponse(DASHBOARD_HTML)

        @app.get("/history", tags=["Monitoring"], summary="이 executor의 task 실행 이력(페이징)")
        def get_history(limit: int = 50, offset: int = 0):
            """이 executor 의 task 실행 이력을 페이징 조회한다.

            limit 는 1~200 으로 클램프, offset 은 음수 방지. 저장소가 task_id 별 최신 1건만
            추려서 반환한다(상세는 history.read 참고).
            """
            limit = max(1, min(limit, 200))
            offset = max(0, offset)
            return history_reader.read(limit=limit, offset=offset)

        @app.get("/config", tags=["Monitoring"], summary="환경설정(비밀값 마스킹)")
        def get_config():
            """현재 적용된 환경설정을 비밀값 마스킹해 반환한다(대시보드 환경설정 탭용)."""
            return {"config": masked_config(settings)}

        @app.get("/info", tags=["Monitoring"], summary="기타 정보")
        def get_info():
            """버전/식별자/가동시간/상태별 task 집계 등 인스턴스 메타 정보를 반환한다."""
            by_status: dict[str, int] = {}
            for t in tasks.values():
                by_status[t.status.value] = by_status.get(t.status.value, 0) + 1
            active, queued, mx = _task_counts()
            return {
                "version": "0.1.0",
                "executor_id": _executor_id(),
                "self_report": settings.executor_self_report,
                "max_concurrent_tasks": mx,
                "active_tasks": active,
                "queued_tasks": queued,
                "started_at": started_at.isoformat(),
                "uptime_seconds": round(time.monotonic() - start_monotonic, 1),
                "tasks_total": len(tasks),
                "tasks_by_status": by_status,
            }

    return app


# uvicorn 이 "executor.app:app" 으로 임포트하는 모듈 레벨 ASGI 앱 인스턴스.
app = create_app()
