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
import contextvars
import logging
import socket
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from core.config import is_custom_source, settings
from core.dbprobe import (
    QueryResult,
    _is_dataframe,
    _shape,
    clamp_limit,
    run_impala_select,
    run_postgres_select,
)
from core.http_logging import install_http_logging
from core.logging import job_log_context
from core.metrics import collect_system_metrics
from core.phases import close_open_phases
from core.timeutil import format_at_fields, now_dt, now_iso
from core.version import __version__
from core.webassets import mount_static, register_offline_docs
from .backend import Backend, build_backend, build_impala_dsn, load_dotted
from .dashboard import DASHBOARD_HTML, masked_config
from .history import TaskHistoryRepository, _executor_id
from .models import CreateTaskRequest, DatasourceQueryRequest, QueryRunRequest, Task, TaskStatus
from .status import ExecutorStatusReporter

logger = logging.getLogger(__name__)

def _load_query_func(dotted: str):
    """``module:func`` 또는 ``module.func`` dotted path 로 커스텀 실행 함수를 import 한다.

    실제 로딩·캐시는 ``backend.load_dotted`` 하나로 모았다(이관 소스 접속 함수
    ``query.func.<name>.connect`` 도 같은 규약을 쓰므로 로더가 둘일 이유가 없다).
    이 얇은 래퍼는 기존 이름/테스트 대체 지점을 유지하기 위해 남긴다.
    """
    return load_dotted(dotted)


def _resolve_query_func(datasource: str | None) -> tuple[str, dict]:
    """datasource 이름으로 실행할 커스텀 함수와 그 설정을 고른다.

    ``query.func.<name>.module`` 항목이 있으면 그걸 쓰고(설정은 ``query.func.<name>.config.*``),
    없으면 단일 ``query.func.module`` + ``query.func.config.*`` 로 폴백한다. 폴백이 있어야
    소스별 설정을 안 쓰는 기존 배포와, datasource 를 안 보내는 구버전 coordinator 가
    그대로 동작한다. 둘 다 없으면 ``("", {})`` 를 돌려 호출부가 400 으로 안내한다.
    """
    name = str(datasource or "").strip().lower()
    entry = settings.query_func_by_source.get(name) if name else None
    if entry and entry.get("module"):
        return str(entry["module"]), dict(entry.get("config") or {})
    return settings.query_func_module, dict(settings.query_func_config)


def _now_iso() -> str:
    """현재 시각을 KST(타임존 없는) ISO 문자열로 반환. started_at/finished_at 기록용."""
    return now_iso()


def _gp_hostname() -> str:
    """이 executor 가 보고할 GP 세그먼트 호스트명. 설정값 우선, 없으면 OS hostname.

    local_stage 의 file:// URI(``file://<hostname>/...``)에서 hostname 은
    ``gp_segment_configuration.hostname`` 과 정확히 일치해야 한다. co-locate 배포에서 OS
    hostname 이 대개 그 값이지만, FQDN/짧은이름/별칭 차이가 있으면 ``executor.gp_hostname``
    으로 명시 오버라이드한다. coordinator 는 이 값을 file:// URI 조립의 근거로 쓴다.
    """
    return settings.executor_gp_hostname or socket.gethostname()


def _snip(text: Optional[str], limit: int = 300) -> str:
    """긴 SQL 을 로그용으로 한 줄·상한 길이로 줄인다(개행→공백, 초과분은 …).

    상세 로그(DEBUG)에 sub_query/INSERT 전문을 통째로 남기면 로그가 비대해지므로,
    흐름 추적에 충분한 앞부분만 남긴다.
    """
    if not text:
        return ""
    flat = " ".join(text.split())
    return flat if len(flat) <= limit else flat[:limit] + "…"


# 진행률 DEBUG 로그를 남기는 행수 간격(이 값마다 한 번씩만 찍어 로그 IO 를 억제).
_PROGRESS_LOG_EVERY = 100_000


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
    # 진행 중인 백그라운드 task(코루틴) 집합 — graceful drain 의 대기 대상.
    inflight: set = set()
    # 종료(SIGTERM) 시 신규 접수를 막는 드레이닝 플래그(dict 로 클로저에서 가변 공유).
    drain = {"on": False}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # 앱 수명주기 훅: 기동 시 self-report 백그라운드 루프를 켜고(설정 시),
        # 종료 시 드레이닝(진행 중 task 안전 완료 대기) 후 리포터를 멈춘다.
        if settings.executor_self_report:
            await reporter.start()
        try:
            yield
        finally:
            # 1) 드레이닝 시작: 이후 신규 task 는 503 으로 거부한다.
            drain["on"] = True
            # 2) 진행 중 task 를 타임아웃 내에서 완료 대기(강제 취소하지 않음).
            pending = [t for t in inflight if not t.done()]
            if pending:
                timeout = settings.executor_shutdown_drain_timeout_s
                logger.info(
                    "드레이닝: 진행 중 task %d개 완료 대기(최대 %ss)", len(pending), timeout
                )
                _, still = await asyncio.wait(pending, timeout=timeout)
                if still:
                    logger.warning(
                        "드레이닝 타임아웃(%ss): 미완료 task %d개 — 종료 진행",
                        timeout, len(still),
                    )
            # 3) self-report 루프 정리.
            await reporter.stop()

    app = FastAPI(
        lifespan=lifespan,
        title="Distributed Query Executor",
        version=__version__,
        description=(
            "coordinator가 분할한 Impala sub-query를 받아 실행하고, 결과를 Greenplum에 "
            "적재한다. 자신의 task 상태와 시스템 메트릭을 노출한다.\n\n"
            "- Swagger UI: `/docs`, ReDoc: `/redoc`, OpenAPI 스키마: `/openapi.json`"
        ),
        # 에어갭: 기본 docs 라우트는 외부 CDN 을 참조하므로 끄고, 아래에서
        # register_offline_docs 로 내장 에셋 기반 /docs·/redoc 를 다시 등록한다.
        docs_url=None,
        redoc_url=None,
        openapi_tags=[
            {"name": "Tasks", "description": "sub-query 태스크 접수·상태·결과"},
            {"name": "Monitoring", "description": "헬스 체크, 시스템 메트릭"},
        ],
    )
    # 에어갭: 내장 정적 에셋(/assets)과 오프라인 docs(/docs·/redoc)를 등록한다.
    mount_static(app)
    register_offline_docs(app)
    # HTTP 요청/응답 DEBUG 로깅(로그 레벨이 DEBUG 일 때만 자동 기록). 잡음 경로는 기본 제외.
    install_http_logging(app, settings)
    app.state.backend = backend
    app.state.tasks = tasks
    app.state.task_history = history
    # 종료 드레이닝 상태(테스트/디버깅에서 참조). {"on": bool}
    app.state.drain = drain
    app.state.inflight = inflight

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
        # 진행률 DEBUG 로그 스로틀 상태(마지막으로 로그를 남긴 누적 행수).
        _last_logged = {"n": 0}

        def progress(n: int) -> None:
            # 백엔드가 배치 적재마다 호출하는 진행률 콜백 — 누적 적재 행수를 task 에 반영.
            task.rows_written = n
            # 상세 추적(DEBUG): 매 배치마다 찍으면 IO 가 커지므로 일정 행수 간격으로만 남긴다.
            if logger.isEnabledFor(logging.DEBUG) and n - _last_logged["n"] >= _PROGRESS_LOG_EVERY:
                _last_logged["n"] = n
                logger.debug("진행률: 누적 %s행 적재", n)

        try:
            if task.cancel_requested:
                task.status = TaskStatus.CANCELLED
                task.finished_at = _now_iso()
                close_open_phases(task.phases)  # 열린 단계(QUEUE_WAIT 등) 마감
                await history.record(task)  # CANCELLED 이력
                return
            # 슬롯 대기(QUEUE_WAIT) 단계 종료: 접수(create_task)에서 시작해 여기서 닫는다.
            task.on_stage("QUEUE_WAIT", "end")
            task.status = TaskStatus.READING
            task.started_at = _now_iso()
            await history.record(task)  # READING 이력
            loop = asyncio.get_running_loop()
            # impyla/psycopg는 블로킹이므로 스레드에서 실행해 이벤트 루프를 막지 않는다.
            # 스레드로 넘길 때 현재 로그 컨텍스트(job_id/task_id)를 복사해 함께 넘긴다. 그래야
            # 백엔드 스레드에서 찍히는 상세 로그(단계 전이·진행률)에도 [job][task] 가 붙는다.
            ctx = contextvars.copy_context()
            task.status = TaskStatus.WRITING
            await history.record(task)  # WRITING 이력
            logger.debug(
                "적재 시작 exec_mode=%s target=%s sub_query=%s",
                task.exec_mode, task.target_table, _snip(task.sub_query),
            )
            # 소스 엔진 인자는 **커스텀 소스일 때만** 넘긴다. impala(기본)면 인자를 아예
            # 붙이지 않아 백엔드 호출이 예전과 완전히 동일하다(새 kwarg 를 모르는 기존
            # 백엔드 구현/테스트 더블도 그대로 동작).
            src_kw = (
                {"datasource": task.datasource}
                if is_custom_source(task.datasource) else {}
            )
            if task.exec_mode == "statement":
                # wrapper 로 감싼 INSERT 등을 대상 DB에서 그대로 실행(COPY 미사용)
                rows = await loop.run_in_executor(
                    None, lambda: ctx.run(
                        app.state.backend.execute,
                        task.sub_query, on_stage=task.on_stage,
                    )
                )
            elif task.exec_mode == "stage_insert":
                # Impala 결과를 Greenplum staging(TEMP)에 COPY → staging→target INSERT
                rows = await loop.run_in_executor(
                    None,
                    lambda: ctx.run(
                        app.state.backend.stage_and_insert,
                        task.sub_query,
                        task.staging_table,
                        task.staging_ddl,
                        task.insert_sql,
                        progress,
                        query_options=task.impala_query_options,
                        on_stage=task.on_stage,
                        **src_kw,
                    ),
                )
            elif task.exec_mode == "local_stage":
                # local_stage: Impala 결과를 자기 호스트 로컬 CSV 로 export(Phase 1).
                # GP file:// 적재(Phase 2)는 coordinator 가 배리어 후 별도로 수행한다.
                rows = await loop.run_in_executor(
                    None,
                    lambda: ctx.run(
                        app.state.backend.export_to_local_csv,
                        task.sub_query,
                        task.out_path,
                        task.csv_options,
                        progress,
                        query_options=task.impala_query_options,
                        on_stage=task.on_stage,
                        **src_kw,
                    ),
                )
            elif task.exec_mode == "s3_stage":
                # s3_stage Phase 1: Impala 결과를 로컬 CSV 로 export → S3 업로드(로컬 삭제).
                # 외부테이블 생성/target INSERT(Phase 2)는 coordinator 가 배리어 후 수행한다.
                rows = await loop.run_in_executor(
                    None,
                    lambda: ctx.run(
                        app.state.backend.export_to_s3,
                        task.sub_query,
                        task.out_path,   # coordinator 가 확정한 S3 객체 키
                        task.job_id,
                        task.task_id,
                        task.csv_options,
                        progress,
                        query_options=task.impala_query_options,
                        on_stage=task.on_stage,
                        **src_kw,
                    ),
                )
            else:
                # copy 모드: Impala read → Greenplum COPY
                rows = await loop.run_in_executor(
                    None,
                    lambda: ctx.run(
                        app.state.backend.move,
                        task.sub_query,
                        task.target_table,
                        task.write_mode,
                        task.partition_column,
                        task.partition_values,
                        progress,
                        query_options=task.impala_query_options,
                        on_stage=task.on_stage,
                        **src_kw,
                    ),
                )
            task.rows_written = rows
            # 실행 중 취소 요청이 들어왔으면 DONE 대신 CANCELLED 처리
            if task.cancel_requested:
                task.status = TaskStatus.CANCELLED
                task.finished_at = _now_iso()
                close_open_phases(task.phases)  # 실행 중 취소 — 열린 단계 마감
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
            # 실패한 단계(start 만 있고 end 없는)를 지금으로 마감 → 소요시간이 계속 증가하지 않게.
            close_open_phases(task.phases)
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
        # 종료(드레이닝) 중에는 신규 task 를 받지 않는다 → coordinator 가 다른 executor 로
        # failover 하거나 재시도하도록 503 으로 거부한다.
        if drain["on"]:
            # coordinator 쪽 failover 의 원인이 되므로, 거부 사실을 남긴다(job 추적용).
            logger.info("드레이닝 중 신규 task 거부 job=%s task=%s", req.job_id, req.task_id)
            raise HTTPException(
                status_code=503,
                detail="executor 종료 중(draining) — 신규 task 를 받지 않습니다.",
            )
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
            impala_query_options=req.impala_query_options,
            datasource=req.datasource,
            out_path=req.out_path,
            csv_options=req.csv_options,
        )
        # 접수 시각부터 슬롯 확보까지의 대기(QUEUE_WAIT) 단계를 연다. _run 진입 시 닫힌다.
        task.on_stage("QUEUE_WAIT", "start")
        tasks[task.task_id] = task
        with job_log_context(task.job_id, task.task_id):
            await history.record(task)  # QUEUED 이력
            # 백그라운드 실행 task 를 추적해 종료 시 드레이닝(완료 대기)할 수 있게 한다.
            bg = asyncio.create_task(_run_with_ctx(task))
            inflight.add(bg)
            bg.add_done_callback(inflight.discard)
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
        return format_at_fields({
            "tasks": [t.view() for t in rows],
            "total": total,
            "active": active,
            "running": running,
        })

    @app.get("/tasks/{task_id}", tags=["Tasks"], summary="태스크 상태 조회")
    def get_task(task_id: str):
        """단일 task 의 현재 상태를 조회한다. 없으면 404."""
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return format_at_fields(task.view())

    @app.get(
        "/tasks/{task_id}/detail",
        tags=["Tasks"],
        summary="태스크 상세 조회(실행 SQL 전문 포함)",
        description="목록/상태 응답에서 제외되는 sub_query·staging DDL·INSERT 전문까지 "
        "포함해 반환한다. coordinator 의 GET /jobs/{job_id}/tasks/{task_id} 와 대칭.",
    )
    def get_task_detail(task_id: str):
        """단일 task 의 상세(실행 SQL 전문 포함)를 조회한다. 없으면 404."""
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return format_at_fields(task.detail())

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
            return format_at_fields(task.view())  # 이미 종료 — 변경 없음
        logger.info("task %s 취소 요청 수신 (현재 status=%s)", task_id, task.status.value)
        task.cancel_requested = True
        # 아직 시작 전이면 즉시 취소 확정, 실행 중이면 _run 이 완료 후 CANCELLED 처리
        if task.status == TaskStatus.QUEUED:
            task.status = TaskStatus.CANCELLED
            task.finished_at = _now_iso()
            await history.record(task)
        return format_at_fields(task.view())

    @app.post(
        "/stage/{job_id}/cleanup",
        tags=["Tasks"],
        summary="local_stage 로컬 CSV 디렉터리 정리",
    )
    def cleanup_stage(job_id: str):
        """local_stage 의 로컬 CSV 디렉터리(``{stage.local_dir}/{job_id}``)를 삭제한다.

        Phase 2(GP file:// 적재)가 끝난 뒤 coordinator 가 각 executor 에 호출한다. 경로
        이탈을 막기 위해 job_id 의 basename 으로 만든 하위 디렉터리만 지우며, 없으면 조용히
        통과한다(멱등). 반환: 실제 삭제 여부.
        """
        import os
        import shutil

        safe = os.path.basename(job_id)  # 경로 조작 방지: 하위 디렉터리명만 사용
        target = os.path.join(settings.stage_local_dir, safe)
        removed = False
        with job_log_context(job_id):
            if safe and os.path.isdir(target):
                shutil.rmtree(target, ignore_errors=True)
                removed = True
                logger.info("local_stage 로컬 CSV 정리: %s 삭제", target)
            else:
                # coordinator 가 이미 정리했거나 이 호스트엔 파일이 없던 경우(멱등).
                logger.debug("local_stage 로컬 CSV 정리: 대상 없음(%s)", target)
        return {"job_id": job_id, "removed": removed}

    @app.post(
        "/s3/{job_id}/cleanup",
        tags=["Tasks"],
        summary="s3_stage S3 스테이징 객체 정리",
    )
    def cleanup_s3(job_id: str):
        """s3_stage 의 S3 스테이징 프리픽스(``{s3.prefix}/{job_id}/``) 아래 객체를 삭제한다.

        Phase 2(GP PXF 적재)가 끝난 뒤 coordinator 가 executor 하나에 호출한다(S3 는 세그먼트
        로컬이 아니라 executor 아무나 삭제 가능). job_id 의 basename 으로 프리픽스를 만들어
        해당 job 하위 객체만 지운다(멱등). 반환: 삭제한 객체 수."""
        import os

        from core import s3_stage as s3sql

        safe = os.path.basename(job_id)  # 경로 조작 방지: job 하위 프리픽스만
        prefix = s3sql.s3_job_prefix(settings.s3_prefix, safe)
        deleted = 0
        with job_log_context(job_id):
            try:
                deleted = app.state.backend.cleanup_s3_prefix(prefix)
                logger.info("s3_stage S3 정리: %s (%d개 삭제)", prefix, deleted)
            except Exception:
                # 정리는 best-effort — 실패해도 적재는 이미 커밋됨.
                logger.warning("s3_stage S3 정리 실패 — 무시: %s", prefix, exc_info=True)
        return {"job_id": job_id, "deleted": deleted}

    @app.get("/health", tags=["Monitoring"], summary="헬스 체크(liveness)")
    def health():
        """liveness 체크: 프로세스가 떠 있으면 서비스명/버전과 함께 ok 반환."""
        return {"status": "ok", "service": "executor", "version": __version__}

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
        # local_stage: coordinator 가 file:// URI 조립에 쓸 GP 세그먼트 호스트명(항상 노출).
        m["gp_hostname"] = _gp_hostname()
        return m

    @app.get("/datasources", tags=["Monitoring"], summary="테스트 가능한 데이터소스 목록/구성여부")
    def list_datasources():
        """이 executor 가 직접 SELECT 테스트할 수 있는 데이터소스와 구성 여부를 반환한다."""
        return {
            "datasources": [
                {"name": "impala", "configured": bool(settings.impala_host)},
                {"name": "greenplum", "configured": bool(settings.greenplum_dsn)},
                {"name": "history", "configured": bool(settings.history_db_dsn)},
            ],
            # 이 executor 가 task 실행(copy/stage_insert/local_stage 읽기)에 쓰는 소스 종류(impala).
            "source_type": settings.source_type,
        }

    @app.post(
        "/datasources/{name}/query",
        tags=["Monitoring"],
        summary="데이터소스에 임의 SELECT 실행(연결 확인 + 결과 미리보기)",
    )
    async def query_datasource(name: str, req: DatasourceQueryRequest):
        """``name`` 데이터소스(impala/greenplum/history)에 임의 SQL 을 실행해 상위 N행을 반환.

        블로킹 드라이버 호출이므로 ``asyncio.to_thread`` 로 스레드에서 돌려 이벤트 루프를
        막지 않는다. 미구성 데이터소스는 400, 알 수 없는 이름은 404, 연결/인증/SQL 오류는
        원인 메시지와 함께 502 로 응답한다.
        """
        limit = clamp_limit(req.limit)
        try:
            if name == "impala":
                dsn = build_impala_dsn(settings)
                if not dsn:
                    raise HTTPException(status_code=400, detail="impala.host 미설정 — Impala 접속 정보가 없습니다")
                result = await asyncio.to_thread(
                    run_impala_select, dsn, req.sql,
                    query_options=settings.impala_query_options, limit=limit,
                )
            elif name == "greenplum":
                if not settings.greenplum_dsn:
                    raise HTTPException(status_code=400, detail="greenplum.dsn 미설정")
                result = await asyncio.to_thread(run_postgres_select, settings.greenplum_dsn, req.sql, limit=limit)
            elif name == "history":
                if not settings.history_db_dsn:
                    raise HTTPException(status_code=400, detail="history.db_dsn(또는 monitor.db_dsn) 미설정")
                result = await asyncio.to_thread(run_postgres_select, settings.history_db_dsn, req.sql, limit=limit)
            else:
                raise HTTPException(status_code=404, detail=f"알 수 없는 데이터소스: {name}")
        except HTTPException:
            raise
        except Exception as e:  # 연결/인증/SQL 오류 → 502 + 원인
            logger.warning("데이터소스 %s 쿼리 실패: %s", name, e)
            raise HTTPException(status_code=502, detail=f"{name} 쿼리 실패: {e}")
        return {"datasource": name, "limit": limit, **result.to_dict()}

    @app.post(
        "/query-run",
        tags=["Query"],
        summary="query-execute 위임: 설정된 커스텀 함수로 SELECT 실행",
    )
    async def query_run(req: QueryRunRequest):
        """coordinator 가 렌더·검증한 SELECT 를 **설정된 커스텀 함수**에 실행 위임한다.

        executor 는 Trino 등 백엔드를 직접 알지 않는다 — ``query.func.module`` 로 지정한 외부
        함수에 SQL·설정(``query.func.config.*``)·limit 을 넘겨 호출하고, 반환된 결과
        (QueryResult 또는 동일 키 dict)를 그대로 응답한다. 블로킹일 수 있으므로 to_thread 로 감싼다.
        미설정 시 400, 함수 로드/실행 실패 시 502.

        ``datasource`` 가 오면 그 이름의 함수(``query.func.<name>.module``)를 골라 impala/trino
        를 **서로 다른 함수로** 실행한다. 이름별 항목이 없으면 단일 ``query.func.module`` 로
        폴백하므로 기존 배포와 구버전 coordinator 모두 그대로 동작한다.
        """
        module, func_config = _resolve_query_func(req.datasource)
        if not module:
            configured = sorted(settings.query_func_by_source)
            raise HTTPException(
                status_code=400,
                detail=(
                    f"query-execute 실행 함수가 구성되지 않았습니다(datasource={req.datasource or '(미지정)'}). "
                    f"query.func.<datasource>.module 또는 query.func.module 을 설정하세요. "
                    f"현재 소스별 설정={configured or '(없음)'}"
                ),
            )
        limit = clamp_limit(req.limit)
        try:
            fn = _load_query_func(module)
        except ValueError as e:
            raise HTTPException(status_code=502, detail=str(e))
        try:
            result = await asyncio.to_thread(
                fn, req.sql, config=dict(func_config), limit=limit
            )
        except Exception as e:  # 커스텀 함수 내부 오류(연결/인증/SQL 등) → 502 + 원인
            # 커스텀 함수가 자체 로깅을 하지 않아도 원인을 추적할 수 있도록
            # 스택 트레이스까지 남긴다(WARNING 이상 → *-warn.log 에도 기록).
            logger.warning("커스텀 실행 함수(%s) 실패: %s", module, e, exc_info=True)
            raise HTTPException(status_code=502, detail=f"커스텀 실행 함수 실패: {e}")
        # 반환은 QueryResult 또는 {columns, rows, row_count, truncated, elapsed_ms} dict 를 허용한다.
        # pandas DataFrame 을 그대로 돌려주는 함수도 받아 준다 — 사내 게이트웨이/래퍼가 커서
        # 대신 DataFrame 을 주는 경우가 흔한데, 이때 dict(df) 는 {컬럼: Series} 라는 엉뚱한
        # 모양이 되어 직렬화 단계에서 깨진다. 여기서 _shape 로 정형해 그 실패를 없앤다
        # (elapsed_ms 는 함수 안 실행시간을 알 수 없으므로 이 정형 시점 기준이다).
        if isinstance(result, QueryResult):
            body = result.to_dict()
        elif _is_dataframe(result):
            body = _shape(None, result, limit, time.perf_counter()).to_dict()
        else:
            body = dict(result)
        return {"limit": limit, **body}

    # 대시보드 활성화 시에만 UI 및 보조 조회 엔드포인트(/, /history, /config, /info)를 등록.
    # started_at/start_monotonic 은 /info 의 uptime 계산 기준점(monotonic 은 시계 변경에 둔감).
    if settings.dashboard_enabled:
        started_at = now_dt()
        start_monotonic = time.monotonic()
        history_reader = TaskHistoryRepository(settings)

        @app.get("/", include_in_schema=False)
        def dashboard():
            """대시보드 단일 페이지 HTML 을 그대로 서빙한다."""
            return HTMLResponse(DASHBOARD_HTML)

        @app.get("/history", tags=["Monitoring"], summary="이 executor의 task 실행 이력(필터/페이징)")
        def get_history(
            limit: int = 50,
            offset: int = 0,
            status: Optional[str] = None,
            username: Optional[str] = None,
            job_id: Optional[str] = None,
        ):
            """이 executor 의 task 실행 이력을 필터링/페이징 조회한다.

            limit 는 1~200 으로 클램프, offset 은 음수 방지. 저장소가 task_id 별 최신 1건만
            추려서 반환한다(상세는 history.read 참고). status/username 은 정확 일치,
            job_id 는 전방일치(prefix) 필터다(빈 값은 무시).
            """
            limit = max(1, min(limit, 200))
            offset = max(0, offset)
            return format_at_fields(history_reader.read(
                limit=limit, offset=offset,
                status=status, username=username, job_id=job_id,
            ))

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
            return format_at_fields({
                "version": __version__,
                "executor_id": _executor_id(),
                "source_type": settings.source_type,
                "self_report": settings.executor_self_report,
                "advertise_url": settings.executor_advertise_url,
                "gp_hostname": _gp_hostname(),
                "max_concurrent_tasks": mx,
                "active_tasks": active,
                "queued_tasks": queued,
                "started_at": started_at.isoformat(),
                "uptime_seconds": round(time.monotonic() - start_monotonic, 1),
                "tasks_total": len(tasks),
                "tasks_by_status": by_status,
            })

    return app


# uvicorn 이 "executor.app:app" 으로 임포트하는 모듈 레벨 ASGI 앱 인스턴스.
app = create_app()
