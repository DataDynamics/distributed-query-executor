"""Executor FastAPI 애플리케이션으로, task 를 받아 실행하고 상태를 노출한다.

coordinator 가 분할한 sub-query(task)를 ``POST /tasks`` 로 접수해 백그라운드에서 실행하고,
실행 상태/시스템 메트릭/이력을 REST 및 대시보드로 노출하는 executor 프로세스의 본체다.

핵심 흐름:
    1. 접수: ``create_task`` 가 요청을 ``Task`` 로 만들어 인메모리 ``tasks`` dict 에 넣고
       QUEUED 이력을 남긴 뒤, ``asyncio.create_task`` 로 실행을 비동기 시작하고 202 를 반환.
    2. 실행 단계에서는 ``_run`` 이 상태를 READING, WRITING 을 거쳐 DONE(또는 FAILED·CANCELLED)으로 옮기며,
       블로킹 DB 드라이버(impyla/psycopg)는 스레드 풀에서 돌려 이벤트 루프를 막지 않는다.
       각 전이마다 ``history.record`` 로 한 행씩 이력을 append 한다.
    3. 동시성 제어: ``_run_with_ctx`` 가 admission 세마포어로 동시에 실행되는 task 수를
       상한(executor_max_concurrent_tasks)으로 제한한다. 0/미설정이면 무제한.

상태는 인메모리(dict)이므로 인스턴스당 단일 워커로 동작한다(__main__ 참고).
"""

from __future__ import annotations

import asyncio
import uuid
import contextvars
import logging
import re
import socket
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from core.config import is_custom_source, settings
from core.dbprobe import QueryResult, clamp_limit, run_impala_select, run_postgres_select
from core.http_logging import install_http_logging
from core.logging import job_log_context, request_id_var
from core.metrics import collect_system_metrics
from core.phases import close_open_phases
from core.sqllog import log_sql
from core.loopwatch import start_loop_watchdog
from core.timeutil import format_at_fields, now_dt, now_iso
from core.version import __version__
from core.webassets import mount_static, register_offline_docs
from .backend import Backend, build_backend, build_impala_dsn
from .dashboard import DASHBOARD_HTML, masked_config
from .history import TaskHistoryRepository, _executor_id
from .models import CreateTaskRequest, DatasourceQueryRequest, QueryRunRequest, Task, TaskStatus
from .status import ExecutorStatusReporter

logger = logging.getLogger(__name__)

# query-execute 위임용 커스텀 실행 함수를 dotted path 별로 캐시한다. executor 는 단일 워커라
# 프로세스 안 캐시가 안전하며, 첫 호출에서 import 한 뒤 계속 재사용한다.
_query_func_cache: dict = {}


def _load_query_func(dotted: str):
    """``module:func`` 또는 ``module.func`` dotted path 로 커스텀 실행 함수를 import 한다.

    template.func_modules 로딩과 같은 importlib 방식이며, 결과를 캐시한다. import/getattr
    실패는 ValueError 로 올려 호출부가 502 로 변환하게 한다.
    """
    fn = _query_func_cache.get(dotted)
    if fn is not None:
        return fn
    import importlib

    mod_path, sep, attr = dotted.partition(":")
    if not sep:  # ':' 가 없으면 마지막 '.' 을 함수명 경계로 본다
        mod_path, _, attr = dotted.rpartition(".")
    if not mod_path or not attr:
        raise ValueError(f"잘못된 함수 경로: {dotted!r} (module:func 형식이어야 함)")
    try:
        module = importlib.import_module(mod_path)
        fn = getattr(module, attr)
    except Exception as exc:
        raise ValueError(f"커스텀 실행 함수 로드 실패: {dotted} ({exc})") from exc
    if not callable(fn):
        raise ValueError(f"커스텀 실행 함수가 호출 가능하지 않습니다: {dotted}")
    _query_func_cache[dotted] = fn
    return fn


def _validate_cleanup_job_id(job_id: str) -> None:
    """정리 경로와 S3 프리픽스에 쓸 단일 job 식별자를 검증한다."""
    if not re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9_.-]*", job_id):
        raise HTTPException(status_code=422, detail={
            "code": "INVALID_JOB_ID", "message": "안전한 단일 job 식별자가 필요합니다.",
        })


def _src_kw(task) -> dict:
    """백엔드 호출에 실을 소스 엔진 인자(커스텀 소스일 때만).

    impala/미지정이면 **빈 dict** 라 호출 시그니처가 예전과 완전히 동일해진다 — 새 kwarg 를
    모르는 백엔드 구현·테스트 더블도 그대로 동작한다(영향 차단).
    """
    ds = getattr(task, "datasource", None)
    return {"datasource": ds} if is_custom_source(ds) else {}


def _now_iso() -> str:
    """현재 시각을 타임존 없는 KST ISO 문자열로 돌려준다. started_at 과 finished_at 을 기록하는 데 쓴다."""
    return now_iso()


def _gp_hostname() -> str:
    """이 executor 가 보고할 GP 세그먼트 호스트명을 정한다. 설정값을 우선하고 없으면 OS hostname 을 쓴다.

    local_stage 의 file:// URI(``file://<hostname>/...``)에서 hostname 은
    ``gp_segment_configuration.hostname`` 과 정확히 일치해야 한다. co-locate 배포에서 OS
    hostname 이 대개 그 값이지만, FQDN/짧은이름/별칭 차이가 있으면 ``executor.gp_hostname``
    으로 명시 오버라이드한다. coordinator 는 이 값을 file:// URI 조립의 근거로 쓴다.
    """
    return settings.executor_gp_hostname or socket.gethostname()


def _snip(text: Optional[str], limit: int = 300) -> str:
    """긴 SQL 을 로그용으로 한 줄이자 상한 길이로 줄인다. 개행은 공백으로 바꾸고 넘치는 부분은 … 로 잘라 낸다.

    상세 로그(DEBUG)에 sub_query/INSERT 전문을 통째로 남기면 로그가 비대해지므로,
    흐름 추적에 충분한 앞부분만 남긴다.
    """
    if not text:
        return ""
    flat = " ".join(text.split())
    return flat if len(flat) <= limit else flat[:limit] + "…"


# 진행률 DEBUG 로그를 남기는 행수 간격이다. 이 값마다 한 번씩만 찍어 로그 IO 를 억제한다.
_PROGRESS_LOG_EVERY = 100_000


# "활성"(처리 중)으로 볼 task 상태다. 처리중 Task 탭 집계의 기준이 된다.
_ACTIVE_STATUSES = {TaskStatus.QUEUED, TaskStatus.READING, TaskStatus.WRITING}
_FINISHED_STATUSES = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}

# 종료 직후의 task 는 보관 개수 상한을 넘어도 지우지 않는 최소 보관 시간(초)이다. coordinator 는
# 종료 상태를 한 번 더 폴링해야 결과를 확정하고, 접수 응답을 못 받은 POST /tasks 를 재시도하면
# task_requests 로 재생해야 하므로, 그 사이에 지우면 404 폴링 실패나 같은 task 의 중복 실행이 된다.
_MIN_FINISHED_RETAIN_S = 60.0


def _finished_age_s(task, now) -> Optional[float]:
    """종료된 task 가 끝난 지 몇 초 지났는지 돌려준다. 진행 중이거나 종료 시각을 모르면 None 이다."""
    if task.status not in _FINISHED_STATUSES or not task.finished_at:
        return None
    try:
        return (now - datetime.fromisoformat(task.finished_at)).total_seconds()
    except (TypeError, ValueError):
        return None


def _prune_finished_tasks(
    tasks: dict, task_requests: dict, *, now, retention_s: float, max_finished: int,
) -> list[str]:
    """인메모리 ``tasks`` 에서 오래된 종료 task 를 지우고 지운 task_id 목록을 돌려준다.

    executor 는 이관 task 와 query-run 을 모두 ``tasks`` 에 쌓는데, 지우지 않으면 query-execute 가
    올 때마다 늘어나 메모리가 끝없이 자란다. 오래된 실행 기록은 task_history 에 남아 있으므로
    인메모리에는 최근 것만 두면 된다. 두 기준을 함께 적용한다. 끝난 지 ``retention_s`` 초가 지난
    task 는 지우고(0 이면 이 기준을 끈다), 그래도 종료 task 가 ``max_finished`` 개를 넘으면 오래
    끝난 것부터 지운다(0 이면 끈다). 어느 기준이든 진행 중인 task 와 끝난 지
    ``_MIN_FINISHED_RETAIN_S`` 초가 안 된 task 는 건드리지 않는다.

    이벤트 루프 스레드에서만 부르므로 다른 변경과 겹치지 않는다. 스레드에서 도는 집계는
    ``list(tasks.values())`` 스냅샷으로 세므로 여기서 항목이 빠져도 안전하다.
    """
    finished = []
    for tid, t in list(tasks.items()):
        age = _finished_age_s(t, now)
        if age is not None and age >= _MIN_FINISHED_RETAIN_S:
            finished.append((age, tid))
    victims = set()
    if retention_s and retention_s > 0:
        victims.update(tid for age, tid in finished if age >= retention_s)
    if max_finished and max_finished > 0:
        total_finished = sum(1 for t in tasks.values() if t.status in _FINISHED_STATUSES)
        excess = total_finished - len(victims) - max_finished
        if excess > 0:
            # 오래 끝난 것부터 지운다. 이미 보관 시간으로 지울 task 는 건너뛴다.
            remaining = sorted(
                ((age, tid) for age, tid in finished if tid not in victims), reverse=True,
            )
            victims.update(tid for _, tid in remaining[:excess])
    for tid in victims:
        tasks.pop(tid, None)
        task_requests.pop(tid, None)
    return sorted(victims)


def _build_backend() -> Backend:
    """설정을 보고 백엔드를 고른다(공용 build_backend 에 위임한다)."""
    return build_backend(settings)


def create_app(
    backend: Optional[Backend] = None,
    task_history: Optional[TaskHistoryRepository] = None,
) -> FastAPI:
    """Executor FastAPI 앱을 구성해 돌려주는 팩토리다.

    의존성(backend, task_history)을 인자로 주입할 수 있어 테스트에서 MockBackend/
    가짜 이력 저장소를 끼워 넣기 쉽다. 미지정 시 설정 기반 기본 구현을 생성한다.
    인메모리 ``tasks`` dict, 동시 실행 상한 세마포어, self-report 리포터, 모든 라우트가
    이 클로저 안에서 만들어진다(앱 인스턴스마다 독립적인 상태).

    인자:
        backend: 소스에서 Greenplum 으로 적재하는 백엔드다. None 이면 설정을 보고 자동으로 고른다.
        task_history: task 상태 전이 이력 저장소. None 이면 설정으로 생성.

    반환:
        라우트와 lifespan 이 등록된 ``FastAPI`` 인스턴스.
    """
    backend = backend or _build_backend()
    history = task_history or TaskHistoryRepository(settings)
    tasks: dict[str, Task] = {}
    task_requests: dict[str, dict] = {}
    # 동시 task 상한이다(admission control). 0 이면 무제한으로 본다.
    _max = settings.executor_max_concurrent_tasks
    sem = asyncio.Semaphore(_max) if _max and _max > 0 else None
    reporter = ExecutorStatusReporter(settings, tasks_provider=lambda: _task_counts())
    # 진행 중인 백그라운드 task(코루틴) 집합이며, graceful drain 이 이들을 기다린다.
    inflight: set = set()
    # SIGTERM 을 받았을 때 신규 접수를 막는 드레이닝 플래그다. 클로저에서 함께 바꾸려고 dict 로 뒀다.
    drain = {"on": False}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # 앱 수명주기 훅: 기동 시 self-report 백그라운드 루프를 켜고(설정 시),
        # 종료 시 드레이닝(진행 중 task 안전 완료 대기) 후 리포터를 멈춘다.
        if settings.executor_self_report:
            await reporter.start()
        # 이벤트 루프가 블로킹 호출로 멈추면 coordinator 의 헬스 체크·폴링이 timeout 나므로,
        # 멈춘 순간의 스택을 남긴다.
        watchdog = start_loop_watchdog(getattr(settings, "log_loop_stall_ms", 0))
        try:
            yield
        finally:
            # 1) 드레이닝 시작: 이후 신규 task 는 503 으로 거부한다.
            drain["on"] = True
            # 2) 진행 중인 task 를 타임아웃 안에서 기다린다. 강제로 취소하지는 않는다.
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
            # 3) self-report 루프와 정지 감시를 정리한다.
            await reporter.stop()
            if watchdog is not None:
                watchdog.stop()

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
    # HTTP 요청/응답을 DEBUG 로 남긴다. 로그 레벨이 DEBUG 일 때만 자동으로 기록하고 잡음 경로는 기본 제외한다.
    install_http_logging(app, settings)
    app.state.backend = backend
    app.state.tasks = tasks
    app.state.task_history = history
    # 종료 드레이닝 상태를 {"on": bool} 형태로 노출한다. 테스트와 디버깅에서 참조한다.
    app.state.drain = drain
    app.state.inflight = inflight

    def _prune() -> None:
        """새 task 를 넣기 직전에 오래된 종료 task 를 정리한다. 정리 기준은 _prune_finished_tasks 에 있다."""
        removed = _prune_finished_tasks(
            tasks, task_requests, now=now_dt(),
            retention_s=settings.executor_task_retention_s,
            max_finished=settings.executor_max_finished_tasks,
        )
        if removed:
            logger.debug("종료 task %d개 인메모리 정리", len(removed))

    async def _run_with_ctx(task: Task) -> None:
        """admission 세마포어와 로그 컨텍스트로 task 실행을 감싸는 래퍼다.

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
        """task 를 실제로 실행하면서 상태를 READING, WRITING, DONE 으로 옮기고 이력을 기록한다.

        exec_mode 에 따라 백엔드 호출이 갈린다(statement / stage_insert / copy). 블로킹
        DB 드라이버는 ``run_in_executor`` 로 스레드 풀에서 실행해 이벤트 루프를 막지 않는다.
        실행 전후로 취소 요청을 확인해 CANCELLED 로 전이할 수 있고, 어느 단계든 예외가
        나면 FAILED 로 전이하며 메시지를 ``task.error`` 에 담는다. 모든 상태 전이마다
        ``history.record`` 로 이력 한 행을 남기고, 종료 시각은 ``finished_at`` 에 기록한다.
        예외를 밖으로 던지지 않는다(백그라운드 task 이므로 자체적으로 마무리).
        """
        # 진행률 DEBUG 로그의 스로틀 상태다. 마지막으로 로그를 남긴 누적 행수를 들고 있다.
        _last_logged = {"n": 0}

        def progress(n: int) -> None:
            # 백엔드가 배치를 적재할 때마다 부르는 진행률 콜백이다. 누적 적재 행수를 task 에 반영한다.
            task.rows_written = n
            # 상세 추적(DEBUG): 매 배치마다 찍으면 IO 가 커지므로 일정 행수 간격으로만 남긴다.
            if logger.isEnabledFor(logging.DEBUG) and n - _last_logged["n"] >= _PROGRESS_LOG_EVERY:
                _last_logged["n"] = n
                logger.debug("진행률: 누적 %s행 적재", n)

        try:
            if task.cancel_requested:
                task.status = TaskStatus.CANCELLED
                task.finished_at = _now_iso()
                close_open_phases(task.phases)  # QUEUE_WAIT 처럼 열려 있는 단계를 마감한다
                await history.record(task)  # CANCELLED 이력을 남긴다
                return
            # 슬롯 대기(QUEUE_WAIT) 단계 종료: 접수(create_task)에서 시작해 여기서 닫는다.
            task.on_stage("QUEUE_WAIT", "end")
            task.status = TaskStatus.READING
            task.started_at = _now_iso()
            await history.record(task)  # READING 이력을 남긴다
            loop = asyncio.get_running_loop()
            # impyla/psycopg는 블로킹이므로 스레드에서 실행해 이벤트 루프를 막지 않는다.
            # 스레드로 넘길 때 현재 로그 컨텍스트(job_id/task_id)를 복사해 함께 넘긴다. 그래야
            # 백엔드 스레드에서 찍히는 상세 로그(단계 전이·진행률)에도 [job][task] 가 붙는다.
            ctx = contextvars.copy_context()
            task.status = TaskStatus.WRITING
            await history.record(task)  # WRITING 이력을 남긴다
            logger.debug(
                "적재 시작 exec_mode=%s target=%s sub_query=%s",
                task.exec_mode, task.target_table, _snip(task.sub_query),
            )
            if task.exec_mode == "statement":
                # wrapper 로 감싼 INSERT 등을 대상 DB 에서 그대로 실행한다. COPY 는 쓰지 않는다.
                rows = await loop.run_in_executor(
                    None, lambda: ctx.run(
                        app.state.backend.execute,
                        task.sub_query, on_stage=task.on_stage,
                    )
                )
            elif task.exec_mode == "stage_insert":
                # 소스 결과를 Greenplum staging(TEMP)에 COPY 한 뒤 staging 에서 target 으로 INSERT 한다.
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
                        **_src_kw(task),
                    ),
                )
            elif task.exec_mode == "s3_stage":
                # s3_stage Phase 1 로 소스 결과를 로컬 CSV 로 내려 S3 에 올린 뒤 로컬 파일을 지운다.
                # 외부테이블 생성과 target INSERT 인 Phase 2 는 coordinator 가 배리어 뒤에 수행한다.
                rows = await loop.run_in_executor(
                    None,
                    lambda: ctx.run(
                        app.state.backend.export_to_s3,
                        task.sub_query,
                        task.out_path,   # coordinator 가 확정해 준 S3 객체 키다
                        task.job_id,
                        task.task_id,
                        task.csv_options,
                        progress,
                        query_options=task.impala_query_options,
                        on_stage=task.on_stage,
                        **_src_kw(task),
                    ),
                )
            else:
                # copy 모드에서는 소스에서 읽어 Greenplum 으로 COPY 한다.
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
                    ),
                )
            task.rows_written = rows
            # 실행 중에 취소 요청이 들어왔으면 DONE 대신 CANCELLED 로 처리한다.
            if task.cancel_requested:
                task.status = TaskStatus.CANCELLED
                task.finished_at = _now_iso()
                close_open_phases(task.phases)  # 실행 중에 취소됐으니 열린 단계를 마감한다
                logger.info("task %s 취소됨", task.task_id)
                await history.record(task)
                return
            task.status = TaskStatus.DONE
            task.finished_at = _now_iso()
            logger.info("task %s 완료: %s행 적재", task.task_id, rows)
            await history.record(task)  # DONE 이력을 남긴다
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = str(exc)
            task.finished_at = _now_iso()
            # start 만 있고 end 가 없는 실패 단계를 지금 시각으로 마감한다. 그래야 소요시간이 계속 늘지 않는다.
            close_open_phases(task.phases)
            logger.exception("task %s 실패", task.task_id)
            await history.record(task)  # FAILED 이력을 남긴다

    @app.post(
        "/tasks",
        status_code=202,
        tags=["Tasks"],
        summary="sub-query 태스크 접수",
        description="sub-query를 받아 Impala 읽기 → Greenplum 적재를 비동기로 시작한다.",
    )
    async def create_task(req: CreateTaskRequest):
        """task_id 를 선점하고 같은 요청은 재생하며 다른 요청은 409 로 거절한다."""
        request_body = req.model_dump(mode="json")
        if req.task_id in tasks:
            if task_requests[req.task_id] != request_body:
                raise HTTPException(status_code=409, detail={
                    "code": "TASK_ID_CONFLICT", "message": "task_id가 다른 요청에 사용되었습니다.",
                })
            existing = tasks[req.task_id]
            return {"task_id": existing.task_id, "status": existing.status.value}
        # 드레이닝 중에는 신규 task 를 받지 않는다. coordinator 가 다른 executor 로 failover 하거나
        # 재시도하도록 503 으로 거부한다.
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
        _prune()
        tasks[task.task_id] = task
        task_requests[task.task_id] = request_body

        async def accepted():
            with job_log_context(task.job_id, task.task_id):
                await history.record(task)
                await _run_with_ctx(task)

        # 응답 연결이 끊겨도 선점한 task 는 독립적으로 이력을 남기고 실행한다.
        bg = asyncio.create_task(accepted())
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
        # 최근에 시작한 것이 위로 오도록 정렬한다. 아직 시작 전인 task 는 started_at 이 없어 "" 로 뒤에 놓인다.
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
        """단일 task 의 현재 상태를 조회한다. 없으면 404 를 낸다."""
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
        """실행 SQL 전문까지 포함한 단일 task 의 상세를 조회한다. 없으면 404 를 낸다."""
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return format_at_fields(task.detail())

    @app.get("/tasks/{task_id}/result", tags=["Tasks"], summary="태스크 결과(적재 행수) 조회")
    def get_task_result(task_id: str):
        """task 의 결과인 누적 적재 행수를 조회한다. 없으면 404 를 낸다.

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
        # 아직 시작 전이면 곧바로 취소를 확정하고, 실행 중이면 _run 이 끝난 뒤 CANCELLED 로 처리한다.
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
        """검증된 job 하위 디렉터리를 멱등하게 삭제한다."""
        import os
        import shutil

        _validate_cleanup_job_id(job_id)
        root = os.path.realpath(settings.stage_local_dir)
        target = os.path.join(root, job_id)
        resolved = os.path.realpath(target)
        if os.path.islink(target) or os.path.commonpath([root, resolved]) != root or resolved == root:
            raise HTTPException(status_code=422, detail={
                "code": "INVALID_JOB_ID", "message": "스테이징 디렉터리 밖의 경로입니다.",
            })
        removed = False
        with job_log_context(job_id):
            if os.path.isdir(target):
                shutil.rmtree(target, ignore_errors=True)
                removed = True
                logger.info("local_stage 로컬 CSV 정리: %s 삭제", target)
            else:
                # coordinator 가 이미 정리했거나 이 호스트에는 파일이 없던 경우다. 멱등하게 넘어간다.
                logger.debug("local_stage 로컬 CSV 정리: 대상 없음(%s)", target)
        return {"job_id": job_id, "removed": removed}

    @app.post(
        "/s3/{job_id}/cleanup",
        tags=["Tasks"],
        summary="s3_stage S3 스테이징 객체 정리",
    )
    def cleanup_s3(job_id: str):
        """검증된 job 의 정확한 S3 프리픽스를 best-effort 로 정리한다."""
        from core import s3_stage as s3sql

        _validate_cleanup_job_id(job_id)
        prefix = s3sql.s3_job_prefix(settings.s3_prefix, job_id)
        deleted = 0
        with job_log_context(job_id):
            try:
                deleted = app.state.backend.cleanup_s3_prefix(prefix)
                logger.info("s3_stage S3 정리: %s (%d개 삭제)", prefix, deleted)
            except Exception:
                # 정리는 best-effort 다. 실패해도 적재는 이미 커밋된 상태다.
                logger.warning("s3_stage S3 정리 실패 — 무시: %s", prefix, exc_info=True)
        return {"job_id": job_id, "deleted": deleted}

    @app.get("/health", tags=["Monitoring"], summary="헬스 체크(liveness)")
    def health():
        """liveness 를 확인한다. 프로세스가 떠 있으면 서비스명과 버전을 붙여 ok 를 돌려준다."""
        return {"status": "ok", "service": "executor", "version": __version__}

    @app.get("/healthz", tags=["Monitoring"], summary="헬스 체크 별칭(하위 호환)")
    def healthz():
        """k8s 등에서 관례로 쓰는 ``/healthz`` 경로를 위한 헬스 체크 별칭이다."""
        return {"status": "ok"}

    def _task_counts() -> tuple[int, int, int]:
        """동시 처리 현황 (active, queued, max) 튜플을 계산한다.

        active 는 실제 실행 중(READING/WRITING), queued 는 대기 중(QUEUED) task 수,
        max 는 동시 실행 상한(0 이면 무제한). self-report 와 /metrics·/info 가 공유한다.

        호출자가 모두 이벤트 루프 밖의 스레드라는 점이 중요하다(동기 엔드포인트는 스레드 풀,
        self-report 는 ``to_thread``). 그동안 이벤트 루프는 ``/tasks``·``/query-run`` 접수로
        ``tasks`` 에 새 항목을 넣으므로, 제너레이터로 dict 를 직접 순회하면 GIL 이 순회 도중에
        넘어가 ``dictionary changed size during iteration`` 이 나고 /metrics 가 500 이 된다
        (coordinator 모니터가 executor 를 다운으로 본다). C 수준에서 한 번에 끝나는
        ``list(tasks.values())`` 로 먼저 스냅샷을 떠서 센다.
        """
        snapshot = list(tasks.values())
        active = sum(
            1 for t in snapshot
            if t.status in (TaskStatus.READING, TaskStatus.WRITING)
        )
        queued = sum(1 for t in snapshot if t.status == TaskStatus.QUEUED)
        return active, queued, (_max or 0)

    @app.get("/metrics", tags=["Monitoring"], summary="시스템 메트릭(CPU/메모리/디스크) + 동시 처리")
    def metrics():
        """시스템 메트릭(CPU/메모리/디스크)에 동시 처리 현황을 더해 반환한다.

        coordinator 가 executor 부하를 보고 스케줄링 판단에 쓸 수 있도록 노출한다.
        """
        m = collect_system_metrics(settings.monitor_disk_path)
        active, queued, mx = _task_counts()
        m["tasks"] = {"active": active, "queued": queued, "max": mx}
        # local_stage 에서 coordinator 가 file:// URI 를 조립할 때 쓸 GP 세그먼트 호스트명이며 항상 노출한다.
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
            # 이 executor 가 task 실행(copy·stage_insert·local_stage 읽기)에 쓰는 소스 종류다(impala).
            "source_type": settings.source_type,
        }

    @app.post(
        "/datasources/{name}/query",
        tags=["Monitoring"],
        summary="데이터소스에 임의 SELECT 실행(연결 확인 + 결과 미리보기)",
    )
    async def query_datasource(name: str, req: DatasourceQueryRequest):
        """``name`` 데이터소스(impala·greenplum·history)에 임의 SQL 을 실행해 상위 N행을 돌려준다.

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
                    datasource="impala",
                )
            elif name == "greenplum":
                if not settings.greenplum_dsn:
                    raise HTTPException(status_code=400, detail="greenplum.dsn 미설정")
                result = await asyncio.to_thread(
                    run_postgres_select, settings.greenplum_dsn, req.sql,
                    limit=limit, datasource="greenplum",
                )
            elif name == "history":
                if not settings.history_db_dsn:
                    raise HTTPException(status_code=400, detail="history.db_dsn(또는 monitor.db_dsn) 미설정")
                result = await asyncio.to_thread(
                    run_postgres_select, settings.history_db_dsn, req.sql,
                    limit=limit, datasource="history",
                )
            else:
                raise HTTPException(status_code=404, detail=f"알 수 없는 데이터소스: {name}")
        except HTTPException:
            raise
        except Exception as e:  # 연결·인증·SQL 오류는 원인을 담아 502 로 돌려준다
            logger.warning("데이터소스 %s 쿼리 실패: %s", name, e)
            raise HTTPException(status_code=502, detail=f"{name} 쿼리 실패: {e}")
        return {"datasource": name, "limit": limit, **result.to_dict()}

    @app.post(
        "/query-run",
        tags=["Query"],
        summary="query-execute 위임: datasource 에 맞는 경로로 SELECT 실행(동시 처리 슬롯 공유)",
    )
    async def query_run(req: QueryRunRequest):
        """coordinator 가 렌더·검증한 SELECT 를 ``datasource`` 에 맞는 경로로 실행하고 결과를 돌려준다.

        이 호출도 이관 task 와 **같은 실행 단위**로 다룬다. ``exec_mode="query_run"`` 인 Task 를
        인메모리 ``tasks`` 에 등록하고 같은 세마포어(``executor.max_concurrent_tasks``)를 거치므로
        처리중 목록·동시 처리 집계(/tasks·/metrics·/info·self-report)에 포함되고, 상태 전이마다
        task_history 에 이력이 남는다. 슬롯이 다 차 있으면 QUEUED 로 기다리며(응답 연결은 열어
        둔다), 드레이닝 중이면 503 으로 거절한다. 결과 행은 적재하지 않으므로 ``rows_read`` 에
        반환 행수만 기록한다.

        실행 경로 분기는 이관의 소스 읽기와 같은 기준인 :func:`core.config.is_custom_source` 하나다.
        ``impala``·``source``·미지정이면 executor 가 task 에 쓰는 것과 같은 **impyla 접속**
        (``impala.*`` 설정, :func:`build_impala_dsn`)으로 직접 실행하고, 그 외 이름(``trino`` 등)만
        ``query.func.module`` 로 지정한 커스텀 함수에 SQL·설정(``query.func.config.*``)·limit 을
        넘겨 위임한다. impyla 경로는 impala.host 미설정 시 400, 접속·SQL 오류 시 502 이고, 커스텀
        경로는 함수 미설정 시 400, 로드/실행 실패 시 502 다. 실패도 FAILED 이력으로 남는다.
        """
        if drain["on"]:
            logger.info("드레이닝 중 query-run 거부 datasource=%s", req.datasource)
            raise HTTPException(
                status_code=503,
                detail="executor 종료 중(draining) — 신규 query-run 을 받지 않습니다.",
            )
        task = _new_query_task(req)
        task.on_stage("QUEUE_WAIT", "start")
        _prune()
        tasks[task.task_id] = task
        # 요청 처리 코루틴 자체를 inflight 에 넣어 드레이닝이 이 실행의 완료도 기다리게 한다.
        me = asyncio.current_task()
        if me is not None:
            inflight.add(me)
        try:
            with job_log_context(task.job_id, task.task_id):
                await history.record(task)  # QUEUED 이력을 남긴다
                if sem is not None:
                    async with sem:
                        return await _query_run_execute(req, task)
                return await _query_run_execute(req, task)
        finally:
            if me is not None:
                inflight.discard(me)

    def _new_query_task(req: QueryRunRequest) -> Task:
        """``/query-run`` 한 건을 나타내는 Task 를 만든다.

        job_id 는 coordinator 가 실어 보낸 요청 식별자(``X-Request-ID`` → ``req-<id>``)를 써서
        coordinator 로그·이력과 같은 식별자로 묶이게 하고, 없으면 task_id 와 같은 값을 쓴다.
        적재가 없으므로 target_table 등 이관 전용 필드는 비워 둔다.
        """
        task_id = f"qr-{uuid.uuid4().hex[:12]}"
        rid = request_id_var.get()
        job_id = f"req-{rid}" if rid and rid != "-" else task_id
        return Task(
            task_id=task_id,
            job_id=job_id,
            sub_query=req.sql,
            target_table="",
            write_mode="append",
            partition_column="",
            partition_values=[],
            username=req.username,
            exec_mode="query_run",
            datasource=req.datasource,
        )

    async def _query_run_execute(req: QueryRunRequest, task: Task) -> dict:
        """슬롯을 얻은 뒤의 ``/query-run`` 본체다. 상태 전이와 이력 기록을 맡고 실제 실행은 경로별 함수에 맡긴다.

        READING 으로 올리고 SOURCE_QUERY 단계를 연 뒤 impyla 또는 커스텀 함수로 실행한다. 성공하면
        반환 행수를 ``rows_read`` 와 단계의 rows 에 적고 DONE, 실패(HTTPException 포함)하면
        FAILED 와 ``error`` 를 남긴 채 같은 예외를 다시 던져 coordinator 에 상태 코드가 그대로 간다.
        슬롯을 기다리는 동안 취소되었으면 실행하지 않고 CANCELLED 로 마감한다.
        """
        if task.cancel_requested:
            task.status = TaskStatus.CANCELLED
            task.finished_at = _now_iso()
            close_open_phases(task.phases)
            await history.record(task)
            raise HTTPException(status_code=409, detail="query-run 이 실행 전에 취소되었습니다.")
        task.on_stage("QUEUE_WAIT", "end")
        task.status = TaskStatus.READING
        task.started_at = _now_iso()
        await history.record(task)  # READING 이력을 남긴다
        task.on_stage("SOURCE_QUERY", "start")
        try:
            if not is_custom_source(req.datasource):
                body = await _query_run_impala(req)
            else:
                body = await _query_run_custom(req)
        except HTTPException as e:
            await _finish_query_task(task, TaskStatus.FAILED, error=str(e.detail))
            raise
        except Exception as e:  # 예상 밖 오류도 이력에 남기고 502 로 돌려준다
            logger.warning("query-run 실패: %s", e, exc_info=True)
            await _finish_query_task(task, TaskStatus.FAILED, error=str(e))
            raise HTTPException(status_code=502, detail=f"query-run 실패: {e}")
        rows = body.get("row_count")
        task.rows_read = int(rows or 0)
        await _finish_query_task(task, TaskStatus.DONE, rows=rows)
        return body

    async def _finish_query_task(task: Task, status: TaskStatus, *, rows=None, error=None) -> None:
        """query-run task 를 종료 상태로 마감하고(단계 닫기·종료 시각·오류) 이력을 남긴다."""
        task.on_stage("SOURCE_QUERY", "end", {"rows": rows} if rows is not None else None)
        close_open_phases(task.phases)
        task.status = status
        task.error = error
        task.finished_at = _now_iso()
        await history.record(task)

    async def _query_run_custom(req: QueryRunRequest) -> dict:
        """``/query-run`` 의 커스텀 함수 경로다. ``query.func.module`` 함수에 SQL·설정·limit 을 넘겨 실행한다."""
        if not settings.query_func_module:
            raise HTTPException(
                status_code=400,
                detail="query.func.module 미설정 — query-execute 실행 함수가 구성되지 않았습니다",
            )
        limit = clamp_limit(req.limit)
        try:
            fn = _load_query_func(settings.query_func_module)
        except ValueError as e:
            raise HTTPException(status_code=502, detail=str(e))
        # 커스텀 함수가 내부에서 무엇을 하든, executor 가 어떤 엔진에 어떤 SQL 을 넘겼는지는
        # 여기서 확정적으로 남긴다(커스텀 함수의 자체 로깅 여부에 의존하지 않는다).
        log_sql(req.datasource or "source", req.sql, phase="QUERY_EXECUTE")
        try:
            result = await asyncio.to_thread(
                fn, req.sql, config=dict(settings.query_func_config), limit=limit
            )
        except Exception as e:  # 커스텀 함수 안에서 난 오류는 원인을 담아 502 로 돌려준다
            # 커스텀 함수가 자체 로깅을 하지 않아도 원인을 추적할 수 있도록 스택 트레이스까지 남긴다.
            # WARNING 이상이라 *-warn.log 에도 기록된다.
            logger.warning(
                "커스텀 실행 함수(%s) 실패: %s", settings.query_func_module, e, exc_info=True,
            )
            raise HTTPException(status_code=502, detail=f"커스텀 실행 함수 실패: {e}")
        # 반환은 QueryResult 또는 {columns, rows, row_count, truncated, elapsed_ms} dict 를 허용한다.
        body = result.to_dict() if isinstance(result, QueryResult) else dict(result)
        return {"limit": limit, **body}

    async def _query_run_impala(req: QueryRunRequest) -> dict:
        """``/query-run`` 의 built-in Impala 경로다. 이관 task 와 같은 impyla 접속으로 SELECT 를 실행한다.

        ``/datasources/impala/query`` 미리보기와 같은 :func:`run_impala_select` 를 쓰되, 실행 SQL
        로그는 QUERY_EXECUTE 단계로 남겨 query-execute 호출임이 구분되게 한다. 전역
        ``impala.query_options`` 를 SET 으로 적용한다(task 와 동일). impala.host 가 비어 있으면
        커스텀 함수로 조용히 넘어가지 않고 400 으로 명확히 거절한다 — 폴백하면 Impala 를 읽는 줄
        알면서 다른 엔진의 결과를 돌려주게 된다.
        """
        dsn = build_impala_dsn(settings)
        if not dsn:
            raise HTTPException(
                status_code=400,
                detail="impala.host 미설정 — datasource=impala 로 실행할 Impala 접속 정보가 없습니다",
            )
        limit = clamp_limit(req.limit)
        name = (req.datasource or "impala").strip().lower() or "impala"
        log_sql(name, req.sql, phase="QUERY_EXECUTE")
        try:
            result = await asyncio.to_thread(
                run_impala_select, dsn, req.sql,
                query_options=settings.impala_query_options, limit=limit, datasource=name,
                log=False,
            )
        except Exception as e:  # 접속·인증·SQL 오류는 원인을 담아 502 로 돌려준다
            logger.warning("impala query-run 실패: %s", e, exc_info=True)
            raise HTTPException(status_code=502, detail=f"impala 쿼리 실패: {e}")
        return {"limit": limit, **result.to_dict()}

    # 대시보드가 켜져 있을 때만 UI 와 보조 조회 엔드포인트(/, /history, /config, /info)를 등록한다.
    # started_at 과 start_monotonic 은 /info 의 uptime 계산 기준점이며, monotonic 은 시계 변경에 둔감하다.
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
            # 스레드 풀에서 도는 동기 엔드포인트라 _task_counts 와 같은 이유로 스냅샷을 순회한다.
            for t in list(tasks.values()):
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


# uvicorn 이 "executor.app:app" 으로 임포트하는 모듈 레벨 ASGI 앱 인스턴스다.
app = create_app()
