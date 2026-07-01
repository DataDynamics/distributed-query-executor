"""Coordinator FastAPI 애플리케이션 팩토리.

이 모듈은 분산 쿼리 Coordinator의 HTTP API를 구성하는 진입점이다. 핵심 책임은
다음과 같다.

- **작업 생성 파이프라인**: 클라이언트가 보낸 SQL을 검증·파싱하고(`parser`),
  파티션 컬럼의 `IN` 목록 기준으로 N개의 sub-query로 분할한 뒤(`splitter`),
  각 sub-query를 executor에 라운드로빈 배정하여 비동기로 디스패치한다.
- **Admission control(과부하 보호)**: 동시 실행 슬롯과 대기 큐 용량을 합한 한도를
  초과하는 요청은 즉시 429로 거부한다(자세한 흐름은 `_create_job` 주석 참고).
- **상태/결과 조회 및 취소**: job/task 단위 상태·진행률·결과 조회와 취소 API를 제공.
- **모니터링**: coordinator/executor 헬스·시스템 메트릭, 클러스터 요약, 대시보드.

Coordinator 자신은 쿼리 결과 행을 받지 않는다. 실제 데이터 이동(Impala→Greenplum)은
executor가 수행하고, 여기서는 분배·상태추적·집계만 담당한다.

`create_app()`은 의존성(runner/store/settings)을 주입받는 팩토리로, 테스트에서
가짜 구현을 끼워 넣기 쉽게 설계되어 있다. 모듈 마지막 줄에서 기본 설정으로
싱글턴 `app` 을 생성해 ASGI 서버가 import 할 수 있게 한다.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

import httpx

from core.dbprobe import clamp_limit, run_postgres_select
from core.logging import job_log_context
from core.metrics import collect_system_metrics
from core.timeutil import format_at_fields, now_dt
from core.webassets import mount_static, register_offline_docs
from .dashboard import DASHBOARD_HTML, masked_config
from .config import Settings, settings as default_settings
from .dispatcher import HttpDispatcher, JobRunner, LocalDispatcher
from .executor_status import ExecutorStatusRepository
from .ha import CoordinatorHeartbeat, reconcile_orphaned_jobs
from .history import JobHistoryRepository
from .job_store import JobStore, build_job_store, reconcile_interrupted_jobs
from .models import (
    CreateJobRequest,
    CreateJobResponse,
    DatasourceQueryRequest,
    Job,
    JobStatus,
    Task,
    TaskStatus,
    new_job_id,
)
from .monitor import HealthMonitor
from .parser import QueryValidationError, is_row_returning, validate_and_parse
from .reservation import ReservationRepository, ReservingLoadView
from .selector import ExecutorSelector, SharedLoadView
from .splitter import split, wrap

logger = logging.getLogger(__name__)


def _assign_executors(count: int, executors: list[str]) -> list[Optional[str]]:
    """`count`개의 task에 executor URL을 라운드로빈으로 배정한다.

    분할된 sub-query를 설정된 executor 목록에 고르게 분산시키기 위한 헬퍼다.
    executor 수보다 task가 많으면 `itertools.cycle` 로 목록을 순환하며 반복 배정한다.

    Args:
        count: 배정 대상 task 개수.
        executors: 후보 executor base URL 목록(설정값).

    Returns:
        task 순서에 1:1 대응하는 URL 리스트. executor 목록이 비어 있으면
        (예: local 모드라 원격 호출이 없을 때) 모두 None 을 채워 반환한다.
    """
    if not executors:
        # local 모드 등 원격 executor가 없는 경우: 배정할 URL이 없으므로 None 으로 채운다.
        return [None] * count
    cycle = itertools.cycle(executors)
    return [next(cycle) for _ in range(count)]


def create_app(
    runner: Optional[JobRunner] = None,
    store: Optional[JobStore] = None,
    settings: Optional[Settings] = None,
) -> FastAPI:
    """FastAPI 앱을 조립하여 반환하는 팩토리.

    의존성을 인자로 주입받아 라우트를 등록한 `FastAPI` 인스턴스를 만든다. 인자를
    생략하면 기본 설정(`default_settings`)을 바탕으로 store와 runner를 자동 구성하므로,
    운영 기동 시에는 인자 없이 호출하고 테스트에서는 가짜 구현을 주입할 수 있다.

    Args:
        runner: job을 실제로 실행하는 디스패처(JobRunner). None 이면 executor_mode 에
            따라 LocalDispatcher / HttpDispatcher 를 자동 선택한다.
        store: job 상태 저장소. None 이면 설정 기반으로 빌드한다(인메모리/공유 DB 등).
        settings: 환경설정. None 이면 모듈 전역 기본 설정을 사용한다.

    Returns:
        라우트·예외 핸들러·lifespan 이 모두 등록된 FastAPI 앱.
    """
    settings = settings or default_settings
    store = store or build_job_store(settings)
    monitor = HealthMonitor(settings)
    started_at = now_dt()
    start_monotonic = time.monotonic()
    # self-report 모드면 executor 상태를 공유 테이블에서 읽는다(coordinator 폴링/기록 안 함)
    status_repo = (
        ExecutorStatusRepository(settings.history_db_dsn, settings.executor_status_table)
        if settings.executor_self_report and settings.history_db_dsn
        else None
    )

    # 헬스/부하 기반 executor 선택(Phase 1·2·3). round_robin(기본)이 아니면 부하 뷰를 보고
    # 초기 배정과 failover 순서를 헬스 기반으로 정한다(p2c 권장 — HA 분산 스탬피드 방지).
    _select_policy = getattr(settings, "executor_select", "round_robin")
    selection_enabled = _select_policy in ("least_loaded", "p2c")
    selector = ExecutorSelector(policy=_select_policy) if selection_enabled else None
    # 부하 뷰 소스(Phase 3): HA(self_report 공유 테이블, URL 키) vs 단일(monitor 폴링).
    #   auto    → status_repo 있으면 공유 테이블, 없으면 monitor
    #   self_report → 공유 테이블(없으면 monitor 폴백)
    #   monitor → 항상 monitor 폴링
    _health_source = getattr(settings, "executor_health_source", "auto")
    _use_shared = (
        status_repo is not None and _health_source in ("auto", "self_report")
    )
    if selection_enabled:
        load_view = SharedLoadView(status_repo.read_all) if _use_shared \
            else SharedLoadView(monitor.snapshot)
    else:
        load_view = None
    # 공유 예약(Phase 3-B, 엄격 균형): 켜져 있고 공유 DB 가 있으면 부하 뷰를 예약 합산 뷰로
    # 감싸고, dispatch 중 task 를 예약/해제한다(active_tasks + 예약 = effective load).
    reservations = None
    if selection_enabled and getattr(settings, "executor_reservation", False) \
            and settings.history_db_dsn:
        reservations = ReservationRepository(
            settings.history_db_dsn, settings.coordinator_id, table=settings.reservation_table
        )
        load_view = ReservingLoadView(load_view, reservations, settings.reservation_ttl_s)
    # 죽은 coordinator 소유 job 정합(Phase 3-C): 공유 postgres store + heartbeat 일 때만.
    heartbeat = None
    if getattr(settings, "store_backend", "memory") == "postgres" and settings.history_db_dsn \
            and getattr(settings, "orphan_reconcile_interval_s", 0) > 0:
        heartbeat = CoordinatorHeartbeat(
            settings.history_db_dsn, settings.coordinator_id, table=settings.coordinator_status_table
        )
    # 이 coordinator 가 기동 후 executor 별로 배정한 누적 task 수(배정 분포 관측용, Phase 2).
    # 멀티 coordinator 에선 인스턴스별 카운트다(전역 분포는 각 인스턴스 합산).
    assign_counts: dict = {}
    if runner is None:
        # executor_mode 에 따라 디스패처 선택: local 은 in-process 직접 실행,
        # 그 외(http)는 원격 executor에 HTTP로 task를 분배한다.
        runner = (
            LocalDispatcher(settings, store=store)
            if settings.executor_mode == "local"
            else HttpDispatcher(settings, store=store, load_view=load_view,
                                selector=selector, reservations=reservations)
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # 앱 수명주기 훅: 기동 시 (1) 재기동 정합 — 영속 저장소(file/postgres)에서 비종료로
        # 남은 job 을 '중단됨(FAILED)'으로 표시해 retry 로 재개 가능하게 한다(인메모리면 no-op).
        # (2) 헬스 모니터를 켠다. status_repo 가 있으면 executor self-report 모드라 폴링 모니터는
        # 보통 띄우지 않지만, 헬스 기반 executor 선택이 켜져 있으면 선택용 부하 뷰를 위해 폴링한다.
        reconcile_interrupted_jobs(store)
        # 모니터는 (a) self-report 가 아니어서 /executors 용 폴링이 필요하거나,
        # (b) 헬스 기반 선택이 켜졌고 그 부하 소스가 monitor 일 때만 띄운다.
        # HA self-report(_use_shared) 면 공유 테이블을 읽으므로 폴링이 불필요하다.
        need_monitor = status_repo is None or (selection_enabled and not _use_shared)
        if need_monitor:
            await monitor.start()
        # HA: coordinator heartbeat + 죽은 coordinator 소유 job 정합 백그라운드 루프.
        ha_task = asyncio.create_task(_ha_loop()) if heartbeat is not None else None
        try:
            yield
        finally:
            if ha_task is not None:
                ha_task.cancel()
                try:
                    await ha_task
                except asyncio.CancelledError:
                    pass
            if need_monitor:
                await monitor.stop()

    async def _ha_loop() -> None:
        # 주기적으로 자기 생존을 heartbeat 하고, 죽은 coordinator 소유의 비종료 job 을
        # FAILED 로 정합한다(블로킹 DB 작업은 to_thread 로 이벤트 루프 비차단). 한 번의 오류가
        # 루프를 멈추지 않도록 예외는 로깅만 한다.
        interval = settings.orphan_reconcile_interval_s
        while True:
            try:
                await asyncio.to_thread(heartbeat.beat)
                await asyncio.to_thread(
                    reconcile_orphaned_jobs, store, heartbeat, settings.coordinator_stale_s
                )
            except Exception:
                logger.exception("HA heartbeat/reconcile 루프 오류")
            await asyncio.sleep(interval)

    app = FastAPI(
        title="Distributed Query Coordinator",
        version="0.1.0",
        description=(
            "Impala `SELECT` 쿼리를 파티션 컬럼의 `IN` 목록 기준으로 N분할하여 여러 "
            "executor에 분배하고, 각 executor가 Greenplum에 병렬 적재하도록 조율한다.\n\n"
            "- 검증/분할/디스패치/상태추적, executor 헬스 모니터링\n"
            "- Swagger UI: `/docs`, ReDoc: `/redoc`, OpenAPI 스키마: `/openapi.json`"
        ),
        # 에어갭: 기본 docs 라우트는 외부 CDN 을 참조하므로 끄고, 아래에서
        # register_offline_docs 로 내장 에셋 기반 /docs·/redoc 를 다시 등록한다.
        docs_url=None,
        redoc_url=None,
        openapi_tags=[
            {"name": "Jobs", "description": "쿼리 작업 생성·조회·결과·태스크 상세"},
            {"name": "Monitoring", "description": "헬스 체크, 시스템 메트릭, executor 상태"},
        ],
        lifespan=lifespan,
    )
    # 에어갭: 내장 정적 에셋(/assets)과 오프라인 docs(/docs·/redoc)를 등록한다.
    mount_static(app)
    register_offline_docs(app)
    # 핸들러들이 클로저로 직접 참조하지만, 미들웨어/디버깅/테스트에서 꺼내 쓸 수
    # 있도록 핵심 의존성을 app.state 에도 보관해 둔다.
    app.state.store = store
    app.state.runner = runner
    app.state.settings = settings
    app.state.monitor = monitor
    app.state.status_repo = status_repo
    # 헬스 기반 선택 관련(관측/테스트용). 선택 비활성이면 selector/load_view 는 None.
    app.state.selector = selector
    app.state.load_view = load_view
    app.state.assign_counts = assign_counts

    @app.exception_handler(QueryValidationError)
    async def _validation_handler(_: Request, exc: QueryValidationError):
        # 검증/분할 단계에서 던지는 도메인 예외를 일관된 422 JSON 으로 변환한다.
        # (error_code 를 본문에 실어 클라이언트가 실패 원인을 분기할 수 있게 한다.)
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
        # POST /jobs 의 얇은 진입 래퍼.
        # 실제 처리에 앞서 job_id 를 먼저 발급하고 로깅 컨텍스트에 바인딩하여,
        # 검증 실패로 작업이 저장되지 않더라도 이 요청에서 찍히는 모든 로그에
        # [job_id] 가 일관되게 붙도록 한다(요청 추적성 확보).
        job_id = new_job_id()
        with job_log_context(job_id):
            return _create_job(req, background, job_id)

    def _create_job(req: CreateJobRequest, background: BackgroundTasks, job_id: str):
        """작업 생성 본체: 검증 → 분할 → (dry-run 분기) → admission → 디스패치.

        흐름 요약:
        1. SQL을 동기로 검증·파싱하고 parallelism 만큼 sub-query로 분할한다. 이때의
           오류(QueryValidationError)는 백그라운드로 넘기기 전에 즉시 클라이언트에
           422로 반환된다.
        2. exec_mode 에 따라 필수 필드와 래퍼 쿼리 적용을 검증한다.
        3. dry_run 이면 executor 호출 없이 생성될 쿼리 계획만 200으로 반환한다(미저장).
        4. admission control 로 동시 실행/대기 용량을 확인하고, 초과면 429로 거부한다.
        5. 수용되면 Job/Task 를 store 에 저장하고 background 로 runner.run 을 예약한 뒤
           202(job_id)를 반환한다.
        """
        logger.info(
            "쿼리 실행 요청 수신 (partition=%s, target=%s, exec_mode=%s, dry_run=%s)",
            req.partition_column, req.target_table, req.exec_mode, req.dry_run,
        )
        # 동기 검증 + 분할: 비동기 디스패치 전에 마치므로, 여기서 발생하는 오류는
        # 백그라운드 작업으로 넘어가지 않고 즉시(이 요청-응답 사이클에서) 클라이언트에
        # 반환된다. dialect 가 지정되지 않으면 설정의 기본 방언으로 파싱한다.
        dialect = req.sql_dialect or settings.query_default_dialect
        parsed = validate_and_parse(
            req.sql,
            req.partition_column,
            dialect=dialect,
            strict=req.strict_validation,
        )
        # 파티션 컬럼 기준으로 parallelism 개의 sub-query로 분할(전략에 따라 분배 방식 결정).
        sub_queries = split(parsed, req.parallelism, req.split_strategy)

        if req.exec_mode == "stage_insert":
            # stage_insert: Impala SELECT 결과를 Greenplum staging 테이블에 적재한 뒤
            # INSERT 로 최종 테이블에 반영하는 2단계 모드. 분할된 SELECT(sub-query)는
            # 변형하지 않고 그대로 두며, staging DDL/테이블/INSERT 문은 task 에 함께 실어
            # executor 로 전달한다. staging_table(적재 대상)과 wrapper_query(INSERT)는 단계가
            # 성립하려면 반드시 필요하므로 강제한다. staging_ddl 은 선택이며, 비어 있으면
            # executor 가 테이블 생성을 건너뛰고 이미 존재하는 staging_table 을 그대로 쓴다.
            if not (req.staging_table and req.wrapper_query):
                raise QueryValidationError(
                    "STAGE_INSERT_REQUIRES_FIELDS",
                    "stage_insert 모드는 staging_table 과 wrapper_query(INSERT) 가 필요합니다. "
                    "staging_ddl 은 선택이며, 없으면 기존 staging_table 을 사용합니다(생성 건너뜀).",
                )
        elif req.exec_mode == "local_stage":
            # local_stage: 각 executor 가 세그먼트 호스트 로컬 CSV 로 export(Phase 1) →
            # coordinator 가 GP file:// 외부테이블로 적재(Phase 2). 분할된 SELECT 는 그대로
            # export 하므로 wrapper 를 쓰지 않는다(아래 wrapper 분기로 내려가지 않도록 여기서
            # 분기를 끊는다). 외부테이블 컬럼 정의·staging·최종 INSERT 는 요청자가 명시한다.
            if not (req.staging_table and req.external_columns and req.insert_sql):
                raise QueryValidationError(
                    "LOCAL_STAGE_REQUIRES_FIELDS",
                    "local_stage 모드는 staging_table, external_columns, insert_sql 가 "
                    "필요합니다. staging_ddl 은 선택입니다.",
                )
        elif req.wrapper_query:
            # stage_insert 가 아니면서 래퍼 쿼리가 주어진 경우: 분할된 각 sub-query를
            # 래퍼의 placeholder 자리에 치환해 최종 실행 SQL을 만든다(예: 집계/CTE로 감싸기).
            if req.wrapper_placeholder not in req.wrapper_query:
                # placeholder 가 없으면 어디에 sub-query를 끼워 넣을지 알 수 없으므로 거부.
                raise QueryValidationError(
                    "WRAPPER_PLACEHOLDER_MISSING",
                    f"wrapper_query 에 placeholder '{req.wrapper_placeholder}' 가 없습니다.",
                )
            # copy(STDIN) 모드는 결과 행을 fetch 해서 COPY 로 흘려보내므로, 최종 래핑 결과가
            # 반드시 행을 반환하는 SELECT여야 한다. INSERT 등 비-SELECT 래퍼는 행을 돌려주지
            # 않으므로 statement/stage_insert 모드를 써야 한다.
            if req.exec_mode == "copy":
                # 더미 sub-query("(SELECT 1)")로 래핑한 결과를 파싱해 행 반환 여부만 검사한다
                # (실제 sub-query 내용과 무관하게 래퍼 구조만 검증하면 충분하므로).
                probe = wrap("(SELECT 1)", req.wrapper_query, req.wrapper_placeholder)
                if not is_row_returning(probe, dialect):
                    raise QueryValidationError(
                        "COPY_WRAPPER_NOT_SELECT",
                        "copy 모드의 wrapper_query 는 행을 반환하는 SELECT 여야 합니다. "
                        "INSERT 등으로 감싸려면 exec_mode=statement 또는 stage_insert 를 사용하세요.",
                    )
            # 검증을 통과하면 모든 sub-query를 래퍼로 감싸 최종 SQL로 교체한다.
            for sq in sub_queries:
                sq.sql = wrap(sq.sql, req.wrapper_query, req.wrapper_placeholder)

        # 분할된 task 에 executor 배정. 헬스 기반 선택(least_loaded/p2c)이 켜져 있으면
        # 부하 뷰를 보고 한가한 노드에 분산 배정(같은 job 의 task 가 한 노드로 몰리지 않도록
        # 임시 부하 가산). 그 외에는 기존 라운드로빈. local 모드(executors 없음)면 전부 None.
        if selection_enabled and load_view is not None and settings.executors:
            # 배정용 selector 는 요청마다 새로 만든다(동기 핸들러는 스레드풀에서 돌아
            # 디스패처의 selector 와 상태를 공유하지 않게 — 스레드 안전).
            executor_urls = ExecutorSelector(policy=_select_policy).assign(
                len(sub_queries), list(settings.executors), load_view.by_url()
            )
        else:
            executor_urls = _assign_executors(len(sub_queries), settings.executors)
        for _u in executor_urls:
            if _u:
                assign_counts[_u] = assign_counts.get(_u, 0) + 1

        # dry-run: executor 호출·작업 저장 없이 "이렇게 실행될 것"이라는 계획만 만들어
        # 200으로 반환한다. 실제 부작용이 없으므로 admission 체크 이전에 빠르게 빠져나간다.
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

        # ── Admission control(과부하 보호) ──
        # 동시 실행 슬롯 + 대기 큐 상한을 합한 용량을 기준으로 수용 여부를 결정한다.
        # 의도적으로 "거부는 여기서, 대기는 runner 안에서" 로 역할을 나눈다:
        #   - 정상 부하: try_admit() 통과 → 작업은 PENDING 으로 저장되고, runner.run 내부의
        #     슬롯 세마포어에서 자연스럽게 줄을 서다(큐잉) 슬롯이 나면 RUNNING 으로 전이.
        #   - 폭주: 실행+대기 용량(capacity)을 넘는 요청만 try_admit() 이 False 를 돌려
        #     아래에서 429 로 즉시 거부 → 무한정 쌓이는 PENDING 으로 인한 메모리/다운스트림
        #     과부하를 차단한다.
        # runner가 admission 속성을 갖지 않을 수도 있으므로(테스트용 더미 등) getattr 로 방어.
        admission = getattr(runner, "admission", None)
        if admission is not None and not admission.try_admit():
            logger.warning(
                "동시 job 한도 초과로 거부 (in-flight=%s, capacity=%s)",
                admission.inflight, admission.capacity,
            )
            raise HTTPException(
                status_code=429,
                detail=(
                    f"동시 실행/대기 job 한도 초과(capacity={admission.capacity}). "
                    "잠시 후 재시도하세요."
                ),
                headers={"Retry-After": "5"},
            )
        # try_admit() 이 성공해 in-flight 카운트를 1 점유한 상태다. 이후 background 로
        # run 이 예약되면 그 안에서 슬롯 반납이 보장되지만, 예약 직전에 예외가 나면
        # run 이 영영 호출되지 않아 슬롯이 누수되므로 except 에서 직접 반납한다(아래 참고).
        try:
            job = Job(
                job_id=job_id,
                original_sql=req.sql,
                partition_column=req.partition_column,
                target_table=req.target_table,
                write_mode=req.write_mode,
                parallelism=req.parallelism,
                split_strategy=req.split_strategy,
                failure_policy=req.failure_policy,
                username=req.username,
                exec_mode=req.exec_mode,
                staging_table=req.staging_table,
                staging_ddl=req.staging_ddl,
                insert_sql=(
                    req.insert_sql if req.exec_mode == "local_stage"
                    else req.wrapper_query if req.exec_mode == "stage_insert"
                    else None
                ),
                impala_query_options=req.impala_query_options,
                external_columns=req.external_columns,
                export_local_dir=req.export_local_dir,
                csv_delimiter=req.csv_delimiter,
                csv_null=req.csv_null,
                csv_quote=req.csv_quote,
                status=JobStatus.SPLITTING,
            )
            # local_stage: 각 task 가 쓸 로컬 CSV 경로를 확정한다({local_dir}/{job_id}/f{idx}.csv).
            # 이 경로가 executor 의 write 대상이자 coordinator 의 file:// URI 조립 근거가 된다.
            _local_dir = (
                (req.export_local_dir or settings.stage_local_dir)
                if req.exec_mode == "local_stage" else None
            )
            job.tasks = [
                Task(
                    job_id=job.job_id,
                    executor_url=url,
                    sub_query=sq.sql,
                    partition_values=sq.partition_values,
                    out_path=(
                        f"{_local_dir}/{job.job_id}/f{idx}.csv" if _local_dir else None
                    ),
                )
                for idx, (sq, url) in enumerate(zip(sub_queries, executor_urls))
            ]
            # 분할 결과를 Task 목록으로 펼쳐 Job 에 담고 저장소에 등록한다.
            store.add(job)

            logger.info(
                "job %s 생성: %d개 sub-query로 분할 (partition=%s, target=%s)",
                job.job_id,
                len(job.tasks),
                req.partition_column,
                req.target_table,
            )
            # 응답을 막지 않도록 실제 실행은 백그라운드로 예약한다. run 내부에서 PENDING
            # 대기→RUNNING 전이와 종료 시 슬롯 반납까지 책임진다.
            background.add_task(runner.run, job)
        except Exception:
            # try_admit() 으로 점유한 슬롯을 보상 반납한다. 여기 도달했다는 것은
            # run 이 예약되지 못했다는 뜻이므로(=정상 경로의 release 가 실행되지 않음),
            # 직접 반납하지 않으면 in-flight 카운트가 영구히 새어 용량이 줄어든다.
            if admission is not None:
                admission.release()
            raise
        # 202 Accepted: 접수만 확정됐을 뿐 실행은 비동기로 진행된다. 진행 상황은
        # 반환된 job_id 로 /jobs/{job_id} 등에서 폴링한다.
        return CreateJobResponse(job_id=job.job_id)

    @app.get("/jobs/{job_id}", tags=["Jobs"], summary="작업 상태 조회(태스크 포함)")
    def get_job(job_id: str):
        # 단일 job의 상태 + 태스크 목록 요약을 반환. 없으면 404.
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return format_at_fields(job.status_view())

    @app.get(
        "/jobs/{job_id}/status",
        tags=["Jobs"],
        summary="작업 진행 상태(진행률) 조회",
        description="job_id 로 현재 상태/진행률을 조회한다(태스크 목록 제외, 경량).",
    )
    def get_job_progress(job_id: str):
        # 폴링에 적합한 경량 응답: 태스크 목록 없이 상태/진행률만 돌려준다.
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return format_at_fields(job.progress_view())

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
        # 이미 종료(완료/실패/취소)된 작업은 되돌릴 수 없으므로 409로 거절한다.
        if job.status in (JobStatus.DONE, JobStatus.FAILED, JobStatus.CANCELLED):
            raise HTTPException(
                status_code=409,
                detail=f"이미 종료된 작업입니다(status={job.status.value}).",
            )
        # 멀티 coordinator 환경 대비: 공유 store 에 취소 플래그를 남겨,
        # 이 작업을 실제로 실행 중인(=소유한) 다른 coordinator의 runner도 폴링 중에
        # 취소를 감지하도록 한다. 로컬 플래그도 함께 세운다.
        store.request_cancel(job_id)
        job.cancel_requested = True
        # 이 coordinator가 소유한 경우엔 즉시 취소가 일어나고 진행 중 task의 executor로
        # 취소가 전파된다(원격 소유면 위 공유 플래그를 통해 비동기로 반영됨).
        await runner.cancel(job)
        job.status = JobStatus.CANCELLED
        store.save(job)
        return format_at_fields(job.progress_view())

    @app.post(
        "/jobs/{job_id}/retry",
        status_code=202,
        tags=["Jobs"],
        summary="실패 파티션만 재실행",
        description="종료된 작업(PARTIAL/FAILED/CANCELLED)의 실패·취소 task 만 모아 새 작업으로 "
        "재실행한다(성공 파티션은 건너뜀). 새 job_id 를 반환한다(202). 재실행 대상이 없거나 "
        "아직 종료되지 않은 작업이면 409.",
    )
    def retry_job(req_background: BackgroundTasks, job_id: str):
        # 실패 파티션만 재실행: 원본 job 의 FAILED/CANCELLED task 를 새 Job 으로 복제해 디스패치한다.
        # 멱등성: copy 모드의 실패 task 는 트랜잭션 미커밋(=적재 없음)이거나
        # overwrite_partitions(선삭제)이므로 재실행이 안전하다.
        src = store.get(job_id)
        if src is None:
            raise HTTPException(status_code=404, detail="job not found")
        if src.status not in (JobStatus.PARTIAL, JobStatus.FAILED, JobStatus.CANCELLED):
            raise HTTPException(
                status_code=409,
                detail=f"종료된(PARTIAL/FAILED/CANCELLED) 작업만 재실행할 수 있습니다"
                f"(status={src.status.value}).",
            )
        retriable = [
            t for t in src.tasks
            if t.status in (TaskStatus.FAILED, TaskStatus.CANCELLED)
        ]
        if not retriable:
            raise HTTPException(status_code=409, detail="재실행할 실패/취소 task 가 없습니다.")

        admission = getattr(runner, "admission", None)
        if admission is not None and not admission.try_admit():
            raise HTTPException(
                status_code=429,
                detail=f"동시 실행/대기 job 한도 초과(capacity={admission.capacity}).",
                headers={"Retry-After": "5"},
            )
        new_id = new_job_id()
        with job_log_context(new_id):
            try:
                new = Job(
                    job_id=new_id,
                    original_sql=src.original_sql,
                    partition_column=src.partition_column,
                    target_table=src.target_table,
                    write_mode=src.write_mode,
                    parallelism=src.parallelism,
                    split_strategy=src.split_strategy,
                    failure_policy=src.failure_policy,
                    username=src.username,
                    exec_mode=src.exec_mode,
                    staging_table=src.staging_table,
                    staging_ddl=src.staging_ddl,
                    insert_sql=src.insert_sql,
                    impala_query_options=src.impala_query_options,
                    status=JobStatus.SPLITTING,
                    retry_of=src.job_id,
                )
                # 실패/취소 task 만 새 task(QUEUED, attempt=0)로 복제한다. 원본 sub_query·
                # partition_values·executor 배정을 그대로 재사용한다.
                new.tasks = [
                    Task(
                        job_id=new.job_id,
                        executor_url=t.executor_url,
                        sub_query=t.sub_query,
                        partition_values=t.partition_values,
                    )
                    for t in retriable
                ]
                store.add(new)
                logger.info(
                    "job %s 재실행 → 새 job %s (실패 task %d개)",
                    src.job_id, new.job_id, len(new.tasks),
                )
                req_background.add_task(runner.run, new)
            except Exception:
                if admission is not None:
                    admission.release()
                raise
        return JSONResponse(
            status_code=202,
            content={
                "job_id": new.job_id,
                "retry_of": src.job_id,
                "retried_tasks": len(new.tasks),
            },
        )

    @app.get("/jobs/{job_id}/result", tags=["Jobs"], summary="작업 결과(적재 요약) 조회")
    def get_job_result(job_id: str):
        # 적재 결과 요약(태스크별 row count 등)을 반환.
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
        # 특정 job 안의 단일 task 상세를 찾아 반환. job/ task 어느 쪽이 없어도 404.
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        for task in job.tasks:
            if task.task_id == task_id:
                # detail() 은 목록 뷰와 달리 실행된 sub_query 전문까지 포함한다.
                return task.detail()
        raise HTTPException(status_code=404, detail="task not found")

    @app.get(
        "/executors",
        tags=["Monitoring"],
        summary="executor 헬스/메트릭 상태",
        description="모니터가 주기 폴링으로 보유한 executor별 CPU/메모리/디스크 상태.",
    )
    def list_executor_health():
        # self-report 모드면 공유 테이블에서 읽고(coordinator는 폴링하지 않음),
        # 아니면 모니터가 주기 폴링으로 캐시해 둔 스냅샷을 돌려준다.
        if status_repo is not None:
            return {"executors": status_repo.read_all()}
        return {"executors": monitor.snapshot()}

    @app.get(
        "/cluster",
        tags=["Monitoring"],
        summary="클러스터 전체 상태(coordinator+executor health/metrics + 실행 중 job 수)",
        description="coordinator와 모든 executor의 health 및 CPU/메모리/디스크, 그리고 "
        "실행 중인 job 수를 한 번에 반환한다. refresh=true(기본)면 executor를 즉시 폴링한다.",
    )
    async def cluster(refresh: bool = True):
        # executor 상태 수집: self-report 모드면 공유 테이블에서 읽고, 아니면
        # refresh=true 일 때 즉시 폴링(최신값), false 면 캐시 스냅샷(저비용)을 쓴다.
        if status_repo is not None:
            # 멀티 coordinator: executor self-report 공유 테이블에서 조회
            executors = status_repo.read_all()
        else:
            executors = await monitor.poll_now() if refresh else monitor.snapshot()
        # coordinator 자신의 시스템 메트릭 수집은 blocking I/O 라 스레드로 오프로드한다.
        coord_metrics = await asyncio.to_thread(
            collect_system_metrics, settings.monitor_disk_path
        )

        # 전체 job을 상태별로 집계한다. running 은 순수 실행 중,
        # active 는 실행 + 분할 중(SPLITTING) + 대기(PENDING)까지 포함한 "처리 중" 합계.
        by_status: dict[str, int] = {}
        for job in store.list():
            by_status[job.status.value] = by_status.get(job.status.value, 0) + 1
        running = by_status.get(JobStatus.RUNNING.value, 0)
        active = running + by_status.get(JobStatus.SPLITTING.value, 0) + by_status.get(
            JobStatus.PENDING.value, 0
        )
        healthy = sum(1 for e in executors if e.get("healthy"))

        return format_at_fields({
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
            # 이 coordinator 가 기동 후 executor 별로 배정한 누적 task 수(배정 분포 관측).
            # 선택 정책이 균형을 맞추고 있는지 확인하는 용도. 멀티 coordinator 면 인스턴스별 값.
            "assignment_counts": dict(assign_counts),
            "executor_select": _select_policy,
        })

    @app.get("/health", tags=["Monitoring"], summary="헬스 체크(liveness)")
    def health():
        # 프로세스가 살아 있는지 확인하는 liveness 프로브용 가벼운 응답.
        return {"status": "ok", "service": "coordinator", "version": "0.1.0"}

    @app.get("/healthz", tags=["Monitoring"], summary="헬스 체크 별칭(하위 호환)")
    def healthz():
        # 쿠버네티스 등에서 흔히 쓰는 /healthz 경로 호환용 별칭.
        return {"status": "ok"}

    @app.get(
        "/metrics",
        tags=["Monitoring"],
        summary="시스템 메트릭(CPU/메모리/디스크)",
    )
    def metrics():
        # coordinator 호스트의 현재 CPU/메모리/디스크 메트릭(동기 수집).
        return collect_system_metrics(settings.monitor_disk_path)

    # ───────── 모니터링 대시보드 & 데이터 API ─────────

    # "처리 중"으로 묶어서 셀 상태 집합: 실행/분할/대기. status 필터의 running·active
    # 키워드와 active 카운트 계산에 공통으로 쓰인다.
    _active_set = {JobStatus.RUNNING, JobStatus.SPLITTING, JobStatus.PENDING}

    @app.get("/jobs", tags=["Jobs"], summary="작업 목록")
    def list_jobs(status: Optional[str] = None, limit: int = 100):
        # total/running/active 요약 카운트는 필터·페이징과 무관하게 항상 전체 기준으로 센다.
        all_jobs = store.list()
        total_all = len(all_jobs)
        running = sum(1 for j in all_jobs if j.status == JobStatus.RUNNING)
        active = sum(1 for j in all_jobs if j.status in _active_set)
        # 최신순 정렬(created_at 내림차순). created_at 이 없으면 빈 문자열로 안전 비교.
        jobs = sorted(all_jobs, key=lambda j: j.created_at or "", reverse=True)
        if status:
            s = status.lower()
            # running/active 는 단일 상태가 아니라 _active_set 집합 필터로 처리하고,
            # 그 외 키워드는 정확한 상태값 일치로 필터한다.
            if s in ("running", "active"):
                jobs = [j for j in jobs if j.status in _active_set]
            else:
                jobs = [j for j in jobs if j.status.value.lower() == s]
        # limit<=0 이면 전체, 아니면 상위 limit 개만 잘라 행을 구성한다.
        rows = [
            {
                "job_id": j.job_id, "status": j.status.value,
                "username": j.username,
                "progress_percent": j.progress_percent,
                "completed": j.completed, "total": len(j.tasks),
                "total_rows_written": j.total_rows_written,
                "total_rows_read": j.total_rows_read,
                "phase_summary": j.phase_summary(),
                "exec_mode": j.exec_mode, "partition_column": j.partition_column,
                "target_table": j.target_table,
                "created_at": j.created_at, "started_at": j.started_at,
                "finished_at": j.finished_at,
                "original_sql": j.original_sql,
            }
            for j in (jobs if limit <= 0 else jobs[:limit])
        ]
        return format_at_fields(
            {"jobs": rows, "total": total_all, "running": running, "active": active}
        )

    history_reader = JobHistoryRepository(settings)

    @app.get("/history", tags=["Jobs"], summary="과거 실행 이력(페이징)")
    def get_history(limit: int = 20, offset: int = 0):
        # 외부 입력을 안전 범위로 강제(clamp): limit 은 1~200, offset 은 0 이상.
        # 과도한 페이지 크기나 음수 입력으로 인한 DB 부하/오류를 방지한다.
        limit = max(1, min(limit, 200))
        offset = max(0, offset)
        return format_at_fields(history_reader.read(limit=limit, offset=offset))

    @app.get("/datasources", tags=["Monitoring"], summary="테스트 가능한 데이터소스 목록")
    def list_datasources():
        """coordinator 가 SELECT 테스트할 수 있는 데이터소스를 반환한다.

        history/greenplum 은 coordinator 가 직접(psycopg) 테스트하고, impala 는 드라이버가
        없어 ``executor_url`` 을 지정해 executor 로 프록시해야 한다.
        """
        return {
            "local": [
                {"name": "history", "configured": bool(settings.history_db_dsn)},
                {"name": "greenplum", "configured": bool(settings.greenplum_dsn)},
            ],
            "via_executor": ["impala", "greenplum", "history"],
            "executors": list(settings.executors),
        }

    @app.post(
        "/datasources/{name}/query",
        tags=["Monitoring"],
        summary="데이터소스에 임의 SELECT 실행(연결 확인 + 결과 미리보기)",
    )
    async def query_datasource(name: str, req: DatasourceQueryRequest):
        """``name`` 데이터소스에 임의 SQL 을 실행해 상위 N행을 반환한다.

        ``executor_url`` 이 있으면 그 executor 의 동일 엔드포인트로 프록시한다(impala 등
        coordinator 에 드라이버가 없는 데이터소스용). 없으면 coordinator 가 직접 접속
        가능한 history/greenplum 만 로컬 실행한다.
        """
        limit = clamp_limit(req.limit)

        # 1) executor 프록시: impala 처럼 coordinator 가 직접 못 붙는 데이터소스를 executor 경유.
        if req.executor_url:
            target = req.executor_url.rstrip("/") + f"/datasources/{name}/query"
            timeout = httpx.Timeout(settings.task_timeout_s, connect=settings.task_connect_timeout_s)
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.post(target, json={"sql": req.sql, "limit": limit})
            except httpx.HTTPError as e:
                raise HTTPException(status_code=502, detail=f"executor 프록시 실패({req.executor_url}): {e}")
            if resp.status_code >= 400:
                # executor 가 돌려준 에러 본문(detail)을 그대로 전달한다.
                try:
                    payload = resp.json()
                    detail = payload.get("detail") if isinstance(payload, dict) else None
                except Exception:
                    detail = None
                if detail is None:
                    detail = resp.text
                raise HTTPException(status_code=resp.status_code, detail=detail)
            body = resp.json()
            body["proxied_to"] = req.executor_url
            return body

        # 2) 로컬 실행: coordinator 가 직접 접속 가능한 데이터소스만.
        try:
            if name == "greenplum":
                if not settings.greenplum_dsn:
                    raise HTTPException(status_code=400, detail="greenplum.dsn 미설정")
                result = await asyncio.to_thread(run_postgres_select, settings.greenplum_dsn, req.sql, limit=limit)
            elif name == "history":
                if not settings.history_db_dsn:
                    raise HTTPException(status_code=400, detail="history.db_dsn(또는 monitor.db_dsn) 미설정")
                result = await asyncio.to_thread(run_postgres_select, settings.history_db_dsn, req.sql, limit=limit)
            elif name == "impala":
                raise HTTPException(
                    status_code=400,
                    detail="impala 는 coordinator 에서 직접 테스트할 수 없습니다 — executor_url 을 지정하세요",
                )
            else:
                raise HTTPException(status_code=404, detail=f"알 수 없는 데이터소스: {name}")
        except HTTPException:
            raise
        except Exception as e:  # 연결/인증/SQL 오류 → 502 + 원인
            raise HTTPException(status_code=502, detail=f"{name} 쿼리 실패: {e}")
        return {"datasource": name, "limit": limit, **result.to_dict()}

    # 대시보드는 설정으로 끌 수 있다(예: 외부 노출 환경에서 비활성화).
    # 활성화된 경우에만 루트 HTML 및 설정/정보 API 라우트를 등록한다.
    if settings.dashboard_enabled:

        @app.get("/", include_in_schema=False)
        def dashboard():
            # 단일 페이지 모니터링 대시보드 HTML(정적 문자열) 제공.
            return HTMLResponse(DASHBOARD_HTML)

        @app.get("/config", tags=["Monitoring"], summary="환경설정(비밀값 마스킹)")
        def get_config():
            # 현재 설정을 노출하되 DSN/비밀번호 등 민감값은 masked_config 로 가린다.
            return {"config": masked_config(settings)}

        @app.get("/info", tags=["Monitoring"], summary="기타 정보")
        def get_info():
            # 대시보드 상단에 표시할 런타임/구성 요약 정보를 모아 반환한다.
            all_jobs = store.list()
            by_status: dict[str, int] = {}
            for j in all_jobs:
                by_status[j.status.value] = by_status.get(j.status.value, 0) + 1
            return format_at_fields({
                "version": "0.1.0",
                "coordinator_id": settings.coordinator_id,
                "executor_mode": settings.executor_mode,
                "store_backend": settings.store_backend,
                "executor_self_report": settings.executor_self_report,
                "executor_select": settings.executor_select,
                "executor_health_source": settings.executor_health_source,
                "executor_reservation": settings.executor_reservation,
                "started_at": started_at.isoformat(),
                "uptime_seconds": round(time.monotonic() - start_monotonic, 1),
                "jobs_total": len(all_jobs),
                "executors_configured": len(settings.executors),
                "max_concurrent_jobs": settings.max_concurrent_jobs,
                "max_pending_jobs": settings.max_pending_jobs,
                "max_dispatch_concurrency": settings.max_dispatch_concurrency,
                "jobs_by_status": by_status,
            })

    return app


# ASGI 서버(uvicorn 등)가 import 할 수 있도록 기본 설정으로 앱 싱글턴을 생성한다.
app = create_app()
