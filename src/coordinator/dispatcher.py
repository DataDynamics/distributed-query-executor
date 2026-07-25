"""Job 실행 계층: sub-query를 executor로 디스패치하고 상태를 추적한다.

Coordinator는 쿼리 결과 행을 직접 받지 않는다. executor가 Impala -> Greenplum 으로
직접 스트리밍하고, 여기서는 sub-query를 POST한 뒤 상태를 polling 하며 row count만
집계한다(데이터는 흐르지 않고 제어/상태만 흐른다).

이 모듈의 구성 요소:

- ``JobAdmission`` : 동시 실행 슬롯(세마포어)과 대기 큐 상한을 합친 admission control.
  과부하 시 새 요청을 거부할지(429), 받아서 줄 세울지(PENDING)를 판단하는 카운터.
- ``_DispatcherBase`` : 모든 디스패처가 공유하는 실행 골격. PENDING→(슬롯대기)→RUNNING
  →종료 전이, 이력 기록, in-flight 슬롯 반납, 취소 감지를 한곳에 모았다. 하위 클래스는
  실제 task 실행부 ``_execute`` 만 구현한다.
- ``HttpDispatcher`` : 원격 executor 와 HTTP로 통신하는 운영 디스패처.
- ``LocalDispatcher`` : executor 프로세스 없이 coordinator 안에서 백엔드를 직접 실행하는
  개발/검증용 디스패처.

동시성 모델: 모든 카운터·상태 변경은 단일 asyncio 이벤트 루프 안에서 일어나므로,
구간 사이에 ``await`` 양보가 끼지 않는 한 추가 락 없이 안전하다. 실제 병렬성은
``asyncio.gather`` + 세마포어로 task 단위에서 제어한다.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Protocol

import httpx

from core import s3_stage as s3_sql
from core.logging import job_log_context
from core.phases import close_open_phases
from . import stage as stage_sql
from .config import Settings
from .history import JobHistoryRepository
from .models import Job, JobStatus, Task, TaskStatus


logger = logging.getLogger(__name__)


from core.timeutil import now_iso as _now_iso  # KST(naive) ISO 시각 생성


# task가 더 이상 변하지 않는 종료 상태 집합. 폴링 종료 조건, 취소 전파 대상 선별,
# 종료 여부 판정 등에서 "끝났는가?"의 기준으로 재사용한다.
_TERMINAL = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}

# 재시도/failover 대상으로 보는 예외: 연결 계열(TransportError: connect/read 타임아웃,
# 네트워크 오류 등) + executor 가 5xx/4xx 로 응답한 경우(HTTPStatusError).
# 이들은 "executor 에 닿지 못했거나 일시적으로 처리하지 못한" 상황이라 재시도가 의미 있다.
# 반대로 executor 가 task 를 정상 접수해 FAILED 로 보고한 백엔드 오류는 여기 해당하지 않는다
# (재시도해도 같은 결과이므로 그대로 실패 처리한다).
_RETRYABLE = (httpx.TransportError, httpx.HTTPStatusError)


def finalize_job(job: Job) -> None:
    """하위 task들의 결과를 집계해 Job의 최종 상태를 확정한다.

    우선순위가 있는 판정이다(위에서부터 먼저 매칭되는 규칙 적용):

    1. 취소 요청이 있었으면 → CANCELLED (실패 task가 있어도 취소가 우선).
    2. 실패 task가 하나도 없으면 → DONE.
    3. 실패가 있고 정책이 ``best_effort`` 이면 → PARTIAL (일부 성공 허용).
    4. 그 외(실패 + 엄격 정책) → FAILED 이며, 실패 task들의 사유를 모아 ``job.error``에
       기록한다.

    Args:
        job: 모든 task 실행이 끝난(또는 취소된) Job. 이 함수가 status/error 를 직접 갱신한다.
    """
    failed = [t for t in job.tasks if t.status == TaskStatus.FAILED]
    if job.cancel_requested:
        job.status = JobStatus.CANCELLED
    elif not failed:
        job.status = JobStatus.DONE
    elif job.failure_policy == "best_effort" and job.exec_mode != "local_stage":
        # local_stage 는 Phase 2(GP file:// 적재)가 단일 원자 연산이라 "부분 성공"이 없다.
        # export/적재 중 하나라도 실패하면 target 에 온전히 반영되지 않으므로 FAILED 로 본다.
        job.status = JobStatus.PARTIAL
    else:
        job.status = JobStatus.FAILED
        job.error = "; ".join(f"{t.task_id}: {t.error}" for t in failed)


class JobRunner(Protocol):
    """디스패처가 만족해야 하는 구조적 인터페이스(덕 타이핑 계약).

    app 계층은 구체 디스패처 종류를 몰라도 이 프로토콜에 기대 ``run``/``cancel`` 만
    호출한다. 덕분에 테스트에서 가짜 runner를 자유롭게 주입할 수 있다.
    """

    async def run(self, job: Job) -> str: ...

    async def cancel(self, job: Job) -> None: ...


class JobAdmission:
    """동시 실행 job 수 제한 + 대기 큐 상한을 합친 admission control(큐잉 + 큐 상한).

    두 개의 독립된 한도를 조합해 과부하를 두 단계로 막는다.

    - **실행 슬롯**: 동시에 RUNNING 일 수 있는 job 수 = ``max_concurrent_jobs``.
      세마포어(``_sem``)로 강제하며, 슬롯이 없으면 job은 PENDING 으로 줄을 선다.
    - **대기 큐**: 슬롯이 없을 때 PENDING 으로 대기 가능한 job 수 = ``max_pending_jobs``.
    - **in-flight**: 현재 수용된(대기 + 실행) job 수. 이 값이 (실행 + 대기) 용량
      ``capacity`` 를 넘는 요청은 ``try_admit`` 이 거부하고, 호출측(app)이 429를 던진다.

    역할 분담이 핵심이다: ``try_admit`` 은 "받을지 말지"(거부=429)만 결정하고, 실제
    줄서기(슬롯 대기)는 ``slot`` 컨텍스트가 담당한다. 즉 용량 안의 부하는 PENDING 으로
    흡수해 순차 처리하고, 용량을 넘는 폭주만 입구에서 잘라낸다.

    ``max_concurrent_jobs`` 가 0 이하이면 무제한 모드: 슬롯 제한도 admit 거부도 모두
    비활성화된다(``_sem`` 은 None, ``capacity`` 는 None).

    스레드 안전성: 카운터(``_inflight``)는 단일 asyncio 이벤트 루프에서만, 그것도
    ``await`` 양보 없이 동기적으로 증감되므로 별도 락이 필요 없다.
    """

    def __init__(self, settings: Settings):
        # 설정이 누락/None 이어도 0(=무제한 또는 비활성)으로 안전 변환한다.
        self.max_running = int(getattr(settings, "max_concurrent_jobs", 0) or 0)
        self.max_pending = int(getattr(settings, "max_pending_jobs", 0) or 0)
        # 실행 슬롯 세마포어. max_running<=0(무제한)이면 세마포어 자체를 두지 않는다.
        self._sem = asyncio.Semaphore(self.max_running) if self.max_running > 0 else None
        # 현재 수용된 job 수(대기 + 실행). try_admit/release 로만 증감한다.
        self._inflight = 0

    @property
    def capacity(self) -> Optional[int]:
        """admit 가능한 최대 in-flight 수(실행 + 대기). None 이면 무제한.

        무제한 모드(max_running<=0)에서는 상한이 없음을 None 으로 표현한다. 그 외에는
        실행 슬롯 수와 대기 큐 상한의 합이 한 번에 품을 수 있는 job 총량이다.
        """
        if self.max_running <= 0:
            return None
        return self.max_running + max(0, self.max_pending)

    @property
    def inflight(self) -> int:
        """현재 수용되어 처리 중(대기 + 실행)인 job 수(읽기 전용 노출)."""
        return self._inflight

    def try_admit(self) -> bool:
        """비차단 수용 시도.

        용량에 여유가 있으면 in-flight 를 1 늘리고 True 를 반환한다(수용). 이미 용량이
        가득 찼으면 카운터를 건드리지 않고 False 를 반환한다 → 호출측은 이를 429로 변환한다.
        무제한 모드(capacity is None)에서는 항상 수용한다.

        주의: 수용에 성공하면 반드시 나중에 ``release`` 로 1을 되돌려야 한다(누수 방지).
        정상 경로에서는 ``_DispatcherBase.run`` 의 finally 가 이를 보장한다.
        """
        cap = self.capacity
        if cap is not None and self._inflight >= cap:
            return False
        self._inflight += 1
        return True

    def release(self) -> None:
        """수용했던 in-flight 슬롯 하나를 반납한다.

        job 이 종료되었거나, 수용은 됐지만 실행 예약/시작에 실패한 경우 호출한다.
        0 미만으로 내려가지 않도록 가드해 이중 반납에 안전하다.
        """
        if self._inflight > 0:
            self._inflight -= 1

    @asynccontextmanager
    async def slot(self):
        """실행 슬롯을 확보하는 async 컨텍스트(슬롯이 없으면 대기).

        세마포어가 있으면 빈 슬롯이 생길 때까지 대기했다가 진입하고, 블록을 빠져나오며
        자동 반납한다. 대기하는 동안 job 은 호출측에서 PENDING 상태로 노출된다.
        무제한 모드(_sem is None)에서는 대기 없이 즉시 통과한다.
        """
        if self._sem is None:
            yield
            return
        async with self._sem:
            yield


class _DispatcherBase:
    """모든 디스패처의 공통 실행 골격: admission + 상태 전이 + 이력 + 취소 감지.

    여기에 "흐름"을 모아두고, 하위 클래스는 task를 실제로 어떻게 실행하는지
    (``_execute(job)``)만 책임진다. 덕분에 HTTP/Local 두 구현이 PENDING→RUNNING→종료
    전이, 이력 기록, 슬롯 반납 같은 정책을 중복 없이 공유한다.

    보유 자원:
        - ``admission`` : job 단위 동시성/대기 큐 제어(JobAdmission).
        - ``_sem`` : task 단위 디스패치 동시성 제한(한 job 안에서 동시에 띄울 task 수).
        - ``history`` : 시작/종료 이력 기록 리포지토리.
        - ``store`` : (선택) job 상태 공유 저장소. 없으면 인메모리만으로 동작.
    """

    def __init__(self, settings: Settings, history: Optional[JobHistoryRepository] = None,
                 store=None, load_view=None, selector=None, reservations=None):
        self.settings = settings
        # job 내부에서 동시에 진행할 수 있는 task 수 상한(예: executor 동시 호출 제한).
        # 위 admission(job 단위)과 층위가 다른, task 단위 동시성 제어다.
        self._sem = asyncio.Semaphore(settings.max_dispatch_concurrency)
        self.history = history or JobHistoryRepository(settings)
        self.store = store
        self.admission = JobAdmission(settings)
        # 헬스/부하 기반 executor 선택(선택). 둘 다 주입되면 failover 순서를 헬스 기반으로
        # 정한다. 미주입(기본)이면 기존 정적 순서를 그대로 쓴다.
        self.load_view = load_view
        self.selector = selector
        # 공유 예약(Phase 3-B). 주입되면 task 실행 동안 배정 executor 를 예약/해제해
        # 다른 coordinator 의 선택에 실시간 부하로 반영한다.
        self.reservations = reservations
        # local_stage Phase 2(GP file:// 적재)용 백엔드. coordinator 가 GP master 에 직접
        # 실행하므로 export 디스패치(HTTP/local)와 무관하게 공용이다. lazy 로 생성한다.
        self._stage_backend = None
        # executor_url → GP 세그먼트 호스트명 캐시. gp_hostname 은 정적이라 URL 별로 한 번만
        # 조회한다(local_stage file:// URI 조립용). HttpDispatcher 가 /metrics 로 채운다.
        self._gp_host_cache: dict = {}

    def _get_stage_backend(self):
        """local_stage Phase 2 용 GP 백엔드를 lazy 로 생성한다(순수 GP 작업, Impala 불필요).

        greenplum.dsn 이 있으면 ImpalaToGreenplumBackend, 없으면 MockBackend 로 폴백한다.
        LocalDispatcher 는 주입 백엔드를 재사용한다(생성자에서 미리 채워 둔다).
        """
        if self._stage_backend is None:
            from executor.backend import build_backend  # 지연 임포트(순환 방지)
            self._stage_backend = build_backend(self.settings)
        return self._stage_backend

    def _save(self, job: Job) -> None:
        # store 가 주입된 경우에만 영속화한다(인메모리 모드면 no-op).
        # 저장 실패가 작업 진행을 막아선 안 되므로 예외는 로깅만 하고 삼킨다.
        if self.store is None:
            return
        try:
            self.store.save(job)
        except Exception:
            logger.exception("job %s 저장 실패", job.job_id)

    def _task_staging(self, job: Job, task: Task) -> tuple[str, str | None, str | None]:
        """이 task 가 쓸 ``(staging_table, staging_ddl, insert_sql)`` 을 정한다.

        stage_insert 이고 ``coordinator.stage_unique_staging`` 이 켜져 있으면 staging
        이름에 task_id 접미사를 붙여 고유화하고(SQL 두 조각을 일관 치환), 그렇지 않으면
        job 값을 그대로 쓴다. 다른 exec_mode(copy/statement/local_stage)는 이 필드를
        stage_insert 처럼 쓰지 않으므로 job 값을 그대로 반환한다.
        """
        if job.exec_mode != "stage_insert":
            return job.staging_table, job.staging_ddl, job.insert_sql
        return stage_sql.per_task_staging(
            job.staging_table, job.staging_ddl, job.insert_sql, task.task_id,
            enabled=getattr(self.settings, "stage_unique_staging", True),
            target_table=job.target_table,
        )

    def _cancel_observed(self, job: Job) -> bool:
        """이 job에 취소가 요청됐는지 확인한다(로컬 플래그 + 공유 store 양쪽).

        멀티 coordinator 환경에서는 취소 API를 받은 coordinator와 job을 실제로 실행하는
        coordinator가 다를 수 있다. 그래서 로컬 플래그뿐 아니라 공유 store 의 취소 요청도
        조회하고, 발견하면 로컬 플래그에도 반영(메모이즈)해 이후 검사를 빠르게 한다.

        store 조회가 실패하더라도 취소 감지 실패가 실행을 중단시켜선 안 되므로, 예외는
        로깅만 하고 "취소 아님(False)"으로 간주해 계속 진행한다.
        """
        if job.cancel_requested:
            return True
        if self.store is not None:
            try:
                if self.store.is_cancel_requested(job.job_id):
                    job.cancel_requested = True
                    return True
            except Exception:
                logger.exception("취소 플래그 조회 실패 job=%s", job.job_id)
        return False

    async def _execute(self, job: Job) -> None:
        """하위 클래스 구현 지점: job.tasks 를 실제로 디스패치/실행한다(추상 메서드)."""
        raise NotImplementedError

    async def _resolve_url_hosts(self, urls) -> dict:
        """``executor_url`` 목록 → GP 세그먼트 호스트명 매핑(file:// URI·파일 예산 배분용).

        기본 구현은 원격 조회 없이 URL 호스트(로컬 모드로 URL 이 None 이면 설정
        ``executor.gp_hostname``)를 쓴다. HttpDispatcher 는 executor 가 보고한 gp_hostname 을
        우선 쓰도록 오버라이드한다(HTTP URL 호스트 ≠ GP 호스트명인 경우를 바로잡는다).
        """
        out: dict = {}
        for url in urls:
            if url in out:
                continue
            out[url] = (
                stage_sql.host_of(url) if url else (self.settings.executor_gp_hostname or "")
            )
        return out

    async def _resolve_hosts(self, job: Job) -> dict:
        """job 의 각 task ``executor_url`` → GP 세그먼트 호스트명 매핑(``_resolve_url_hosts`` 위임)."""
        return await self._resolve_url_hosts([t.executor_url for t in job.tasks])

    async def _plan_local_stage(self, job: Job) -> bool:
        """local_stage: 세그먼트 토폴로지(호스트별 S_h)에 맞춰 파일(=export task)을 호스트에 배분.

        Phase 1(export) 디스패치 **전에** 실행한다. ``segment_host_counts`` 로 호스트별 primary
        세그먼트 수를 얻고, 설정 executor 들을 gp_hostname 으로 호스트에 묶은 뒤,
        ``plan_file_budget`` 으로 "호스트당 파일 수 ≤ S_h" 를 지키도록 각 task 의 ``executor_url``
        과 ``out_path`` 를 재확정한다. 여러 executor 가 같은 호스트에 있으면 그 안에서 라운드로빈.

        반환값(진행 여부):
            - True  : local_stage 가 아니거나, 토폴로지/executor 정보를 얻지 못해(목·로컬)
                      기존 배정을 그대로 유지하거나, 배분에 성공한 경우 → 실행 계속.
            - False : 파일 수가 총 예산(Σ S_h)을 초과해 배치 불가 → 모든 task 를 FAILED 로
                      표시하고 실행을 건너뛴다(finalize 가 FAILED 로 확정).
        """
        if job.exec_mode != "local_stage" or not job.tasks:
            return True
        backend = self._get_stage_backend()
        loop = asyncio.get_running_loop()
        try:
            host_segments = await loop.run_in_executor(None, backend.segment_host_counts)
        except Exception as exc:
            logger.warning(
                "job %s: 세그먼트 토폴로지 조회 실패 → 파일 예산 배분 생략(기존 배정 유지): %s",
                job.job_id, exc,
            )
            return True
        exec_urls = [u for u in self.settings.executors if u]
        if not host_segments or not exec_urls:
            # 토폴로지 미상(목) 또는 executor 목록 없음(로컬) → 기존 배정을 그대로 쓴다.
            return True

        url_hosts = await self._resolve_url_hosts(exec_urls)
        executors_by_host: dict = {}
        for u in exec_urls:
            host = url_hosts.get(u, "")
            if host:
                executors_by_host.setdefault(host, []).append(u)

        cap = self.settings.stage_max_files_per_host
        plan = stage_sql.plan_file_budget(
            len(job.tasks), host_segments, executors_by_host, cap
        )
        if plan is None:
            capacity = stage_sql.budget_capacity(host_segments, executors_by_host, cap)
            msg = (
                f"local_stage 파일 수({len(job.tasks)})가 호스트 파일 예산(Σ min(S_h,cap)="
                f"{capacity})을 초과합니다. parallelism 을 낮추거나 세그먼트/executor 호스트를 "
                "늘리세요."
            )
            logger.error("job %s: %s", job.job_id, msg)
            job.error = msg
            for t in job.tasks:
                t.status = TaskStatus.FAILED
                t.error = msg
            self._save(job)
            return False

        # 배분 반영: 각 파일(task)의 담당 executor 와 로컬 경로를 확정한다.
        local_dir = job.export_local_dir or self.settings.stage_local_dir
        for idx, (task, (url, _host)) in enumerate(zip(job.tasks, plan)):
            task.executor_url = url
            task.out_path = f"{local_dir}/{job.job_id}/f{idx}.csv"
        per_host: dict = {}
        for _u, h in plan:
            per_host[h] = per_host.get(h, 0) + 1
        logger.info(
            "job %s: local_stage 파일 예산 배분 — %d파일, 호스트별=%s",
            job.job_id, len(plan), per_host,
        )
        self._save(job)
        return True

    async def _run_stage_load(self, job: Job) -> None:
        """local_stage Phase 2: 모든 export 완료(배리어) 후 GP file:// 적재 → target INSERT.

        ``_execute`` 가 반환하면 모든 export task 가 종료 상태다(자연 배리어). 이 시점에서
        export 가 하나라도 실패했거나 취소됐으면 건너뛴다(finalize 가 상태를 정한다). 정상이면
        executor 가 보고한 gp_hostname 으로 file:// URI 를 조립하고(호스트 검증 통과 시),
        coordinator 의 GP 백엔드로 ``load_external_csv`` 를 호출해 외부테이블 생성→staging
        적재→(멱등 선삭제)→target INSERT 를 한 트랜잭션으로 수행한 뒤 로컬 파일을 정리한다.

        local_stage 가 아니면 즉시 반환하므로 다른 exec_mode 에는 영향이 없다.
        """
        if job.exec_mode != "local_stage":
            return
        if self._cancel_observed(job):
            return
        if any(t.status == TaskStatus.FAILED for t in job.tasks):
            logger.warning(
                "job %s: export 실패 task 존재 → Phase 2(GP 적재) 건너뜀", job.job_id
            )
            return

        backend = self._get_stage_backend()
        loop = asyncio.get_running_loop()

        # 각 task 파일이 물리적으로 놓인 GP 세그먼트 호스트를 executor 보고값으로 확정한다.
        host_map = await self._resolve_hosts(job)

        # file:// 호스트 검증: 매핑된 호스트가 gp_segment_configuration 에 실제로 있는지 확인해
        # 오타/잘못된 gp_hostname 을 load 전에 조기 실패시킨다(런타임 파일 미발견 예방).
        if self.settings.stage_validate_hosts:
            try:
                seg_hosts = await loop.run_in_executor(None, backend.segment_hosts)
            except Exception as exc:
                # 조회 실패(권한/미지원 등)는 검증 생략으로 폴백한다 — 적재 자체를 막지 않는다.
                logger.warning(
                    "job %s: gp_segment_configuration 조회 실패 → 호스트 검증 생략: %s",
                    job.job_id, exc,
                )
                seg_hosts = set()
            if seg_hosts:
                bad = sorted({h for h in host_map.values() if h and h not in seg_hosts})
                if bad:
                    msg = (
                        f"file:// 호스트가 gp_segment_configuration 에 없습니다: {bad}. "
                        "executor.gp_hostname 을 세그먼트 호스트명과 일치시키세요."
                    )
                    logger.error("job %s: %s", job.job_id, msg)
                    job.error = msg
                    if job.tasks:
                        job.tasks[0].status = TaskStatus.FAILED
                        job.tasks[0].error = msg
                    self._save(job)
                    return

        # coordinator 가 GP master 에 실행할 SQL 을 조립한다(coordinator/stage.py — 순수 함수).
        csv_options = stage_sql.resolve_csv_options(job, self.settings)
        ext = stage_sql.external_table_name(job.job_id)
        # 파일 목록: 각 task 의 (gp_hostname, out_path).
        uris = [
            (host_map.get(t.executor_url, ""), t.out_path)
            for t in job.tasks if t.out_path
        ]
        external_ddl = stage_sql.build_external_ddl(
            ext, job.external_columns, uris, csv_options
        )
        staging_load = stage_sql.build_staging_load(job.staging_table, ext)
        # overwrite_partitions 멱등: job 전체 파티션 값을 모아 최종 INSERT 전에 선삭제.
        pre_delete = (
            stage_sql.build_pre_delete(
                job.target_table, job.partition_column,
                [v for t in job.tasks for v in t.partition_values],
            )
            if job.write_mode == "overwrite_partitions" else None
        )
        cleanup = stage_sql.build_cleanup(ext)

        try:
            # 블로킹 GP 호출이므로 스레드로 넘겨 이벤트 루프를 막지 않는다.
            rows = await loop.run_in_executor(
                None,
                lambda: backend.load_external_csv(
                    external_ddl, job.staging_ddl, staging_load,
                    pre_delete, job.insert_sql, cleanup,
                ),
            )
            logger.info("job %s: local_stage Phase 2 적재 완료(target 반영 %s행)",
                        job.job_id, rows)
        except Exception as exc:
            # export 는 됐지만 GP 적재가 실패 → job 을 실패로 확정한다. finalize_job 이
            # local_stage 는 실패 task 가 있으면(정책 무관) FAILED 로 판정한다.
            logger.exception("job %s: local_stage Phase 2 실패", job.job_id)
            job.error = f"local_stage Phase 2 실패: {exc}"
            if job.tasks:
                job.tasks[0].status = TaskStatus.FAILED
                job.tasks[0].error = job.tasks[0].error or f"Phase 2 적재 실패: {exc}"
            self._save(job)
            return
        # Phase 3: 각 세그먼트 호스트의 로컬 CSV 정리(디스패처별 구현).
        await self._cleanup_stage(job)
        self._save(job)

    async def _run_s3_load(self, job: Job) -> None:
        """s3_stage Phase 2: 모든 업로드 완료(배리어) 후 GP PXF 외부테이블 적재 → target INSERT.

        ``_execute`` 가 반환하면 모든 Phase 1(업로드) task 가 종료 상태다(자연 배리어). 업로드가
        하나라도 실패/취소됐으면 건너뛴다(finalize 가 상태를 정한다). 정상이면 coordinator 가
        GP master 에 **job 프리픽스(``<prefix>/<job_id>/``)를 가리키는 외부테이블 하나**를 만들어
        (PXF 가 그 아래 모든 task CSV 를 세그먼트 병렬 read) → (멱등 선삭제) → target INSERT 를
        한 트랜잭션으로 수행하고, S3 스테이징 객체를 정리한다(Phase 3).

        local_stage 와 달리 세그먼트 co-locate/파일예산/호스트 검증이 없다(S3 는 위치 무관).
        s3_stage 가 아니면 즉시 반환하므로 다른 exec_mode 에는 영향이 없다.
        """
        if job.exec_mode != "s3_stage":
            return
        if self._cancel_observed(job):
            return
        if any(t.status == TaskStatus.FAILED for t in job.tasks):
            logger.warning(
                "job %s: 업로드 실패 task 존재 → s3_stage Phase 2(GP 적재) 건너뜀", job.job_id
            )
            return

        backend = self._get_stage_backend()
        loop = asyncio.get_running_loop()

        # coordinator 가 GP master 에 실행할 SQL 을 조립한다(core/s3_stage.py — 순수 함수).
        csv_options = stage_sql.resolve_csv_options(job, self.settings)
        ext = s3_sql.external_table_name(job.job_id)
        prefix = s3_sql.s3_job_prefix(self.settings.s3_prefix, job.job_id)
        location = s3_sql.build_s3_location(
            self.settings.s3_bucket, prefix,
            profile=self.settings.s3_pxf_profile,
            server=self.settings.s3_pxf_server,
            location_template=self.settings.s3_gp_location_template,
        )
        external_ddl = s3_sql.build_s3_external_ddl(
            ext, job.external_columns, location, csv_options
        )
        # 외부테이블이 staging 을 겸한다 — insert_sql 의 staging 참조를 job 고유 외부테이블
        # 이름으로 치환해 INSERT INTO target SELECT ... FROM <ext> 가 되게 한다.
        insert_sql = stage_sql.rewrite_staging_name(
            job.insert_sql, job.staging_table, ext, (job.target_table,)
        )
        # 적재 전 파티션 선삭제(DELETE) 여부: 요청의 pre_delete 가 명시되면 그것을, 아니면
        # write_mode 를 따른다(overwrite_partitions→삭제, append→미삭제). 켜져 있으면 job 전체
        # 파티션 값을 모아 최종 INSERT 전에 선삭제한다(재실행 멱등).
        do_delete = (
            job.pre_delete if job.pre_delete is not None
            else (job.write_mode == "overwrite_partitions")
        )
        pre_delete = (
            s3_sql.build_pre_delete(
                job.target_table, job.partition_column,
                [v for t in job.tasks for v in t.partition_values],
            )
            if do_delete else None
        )
        cleanup = [s3_sql.build_cleanup_ddl(ext)]

        try:
            rows = await loop.run_in_executor(
                None,
                lambda: backend.load_external_s3(
                    external_ddl, pre_delete, insert_sql, cleanup,
                ),
            )
            logger.info("job %s: s3_stage Phase 2 적재 완료(target 반영 %s행)",
                        job.job_id, rows)
        except Exception as exc:
            # 업로드는 됐지만 GP 적재가 실패 → job 을 실패로 확정한다.
            logger.exception("job %s: s3_stage Phase 2 실패", job.job_id)
            job.error = f"s3_stage Phase 2 실패: {exc}"
            if job.tasks:
                job.tasks[0].status = TaskStatus.FAILED
                job.tasks[0].error = job.tasks[0].error or f"Phase 2 적재 실패: {exc}"
            self._save(job)
            return
        # Phase 3: S3 스테이징 객체 정리(디스패처별 구현).
        await self._cleanup_s3(job)
        self._save(job)

    async def _cleanup_stage(self, job: Job) -> None:
        """local_stage Phase 3: 로컬 CSV 정리(디스패처별 구현). 기본은 no-op.

        HttpDispatcher 는 각 executor 의 ``/stage/{job_id}/cleanup`` 을 호출해 로컬 파일을
        지운다. LocalDispatcher(개발/목)는 정리할 원격 파일이 없어 그대로 둔다.
        """
        return

    async def _cleanup_s3(self, job: Job) -> None:
        """s3_stage Phase 3: S3 스테이징 객체 정리(디스패처별 구현). 기본은 no-op.

        HttpDispatcher 는 executor 하나에 ``/s3/{job_id}/cleanup`` 을 호출한다(S3 는 세그먼트
        로컬이 아니라 아무 executor 나 삭제 가능). LocalDispatcher 는 in-process 백엔드로 직접
        지운다. ``s3.delete_on_cleanup=false`` 면 건너뛴다(S3 수명주기 정책에 맡길 때).
        """
        return

    async def run(self, job: Job) -> str:
        """Job 한 건의 전체 수명주기를 구동하고 job_id 를 반환한다.

        상태 흐름:
            PENDING → (슬롯 대기/큐잉) → RUNNING → (_execute) → 종료(finalize)

        세부 동작:
            1. 우선 PENDING 으로 표시하고 저장한다. 실행 슬롯이 차 있으면 ``admission.slot()``
               에서 대기하며, 그동안 외부에는 PENDING 으로 노출된다.
            2. 슬롯을 잡은 직후 다시 취소를 확인한다. 대기 중 취소됐다면 _execute 를 건너뛰고
               즉시 종료 처리(finalize + 이력 기록)한다 — 헛된 실행을 막기 위함.
            3. RUNNING 으로 전이하고 started_at 과 시작 이력을 남긴 뒤 _execute 를 호출한다.
            4. _execute 의 성공/실패와 무관하게 finally 에서 finalize_job 으로 최종 상태를
               확정하고 finished_at 과 종료 이력을 기록한다.
            5. 가장 바깥 finally 에서 admission 슬롯을 반드시 반납한다. try_admit 으로
               점유한 in-flight 카운트를 이 한 곳에서 정확히 1 되돌려 누수를 막는다.
        """
        with job_log_context(job.job_id):
            try:
                # 슬롯이 빌 때까지 대기. 그동안 job 은 PENDING 으로 노출된다.
                job.status = JobStatus.PENDING
                self._save(job)
                # 슬롯이 가득 차 대기하게 되면 그 사실을 남긴다(admission/큐 병목 진단용).
                if self.admission.inflight > self.admission.max_running > 0:
                    logger.info(
                        "job PENDING — 실행 슬롯 대기(inflight=%d, 슬롯=%d, 큐=%d)",
                        self.admission.inflight, self.admission.max_running,
                        self.admission.max_pending,
                    )
                async with self.admission.slot():
                    # 대기 중 취소되었으면 실행하지 않고 즉시 종료 처리.
                    if self._cancel_observed(job):
                        logger.info("대기 중 취소 감지 → 실행 건너뜀(CANCELLED)")
                        finalize_job(job)
                        job.finished_at = _now_iso()
                        self._save(job)
                        await self.history.record(job)
                        return job.job_id
                    job.status = JobStatus.RUNNING
                    job.started_at = _now_iso()
                    self._save(job)
                    await self.history.record(job)  # 시작 이력
                    # 실행 시작을 INFO 로 남긴다(로그 파일만 봐도 언제 어떤 형태로 떴는지 파악).
                    t0 = time.monotonic()
                    logger.info(
                        "job 실행 시작 — exec_mode=%s tasks=%d target=%s write_mode=%s",
                        job.exec_mode, len(job.tasks), job.target_table, job.write_mode,
                    )
                    try:
                        # local_stage: Phase 1 전에 파일을 호스트별 예산(S_h)에 맞춰 배분한다.
                        # 예산 초과면 배치 불가 → 실행을 건너뛰고 FAILED 로 마감한다.
                        if await self._plan_local_stage(job):
                            await self._execute(job)
                            # local_stage: 모든 export 완료(배리어) 후 GP file:// 적재(Phase 2+3).
                            # s3_stage: 모든 업로드 완료(배리어) 후 GP PXF 적재(Phase 2+3).
                            # 다른 exec_mode 에는 둘 다 즉시 반환하는 no-op 이다.
                            await self._run_stage_load(job)
                            await self._run_s3_load(job)
                    finally:
                        # _execute 가 예외로 끝나도 최종 상태/종료시각/이력은 반드시 남긴다.
                        finalize_job(job)
                        job.finished_at = _now_iso()
                        self._save(job)
                        await self.history.record(job)  # 종료 이력
                        # 종료 요약(status·완료수·적재행수·소요시간)을 한 줄로. FAILED/PARTIAL 은
                        # 사유 파악이 급하므로 WARNING 으로, 정상 종료는 INFO 로 남긴다.
                        done = sum(1 for t in job.tasks if t.status == TaskStatus.DONE)
                        rows = sum(t.rows_written or 0 for t in job.tasks)
                        level = (
                            logging.WARNING
                            if job.status in (JobStatus.FAILED, JobStatus.PARTIAL)
                            else logging.INFO
                        )
                        logger.log(
                            level,
                            "job 실행 종료 — status=%s 완료=%d/%d 적재=%d행 소요=%.1fs%s",
                            job.status.value, done, len(job.tasks), rows,
                            time.monotonic() - t0,
                            f" error={job.error}" if job.error else "",
                        )
            finally:
                # PENDING 대기 중 취소든 정상 종료든 예외든, 모든 경로에서 슬롯을 반납한다.
                self.admission.release()
            return job.job_id


class HttpDispatcher(_DispatcherBase):
    """원격 executor 서비스와 HTTP로 통신하는 운영 디스패처.

    각 task의 sub-query를 executor에 POST 하여 시작시키고, 완료될 때까지 상태를
    폴링한다. 결과 데이터는 받지 않으며 상태와 rows_written 만 수집한다.
    """

    async def _execute(self, job: Job) -> None:
        # job의 모든 task를 동시에 시작한다. 하나의 HTTP 클라이언트를 공유해
        # 연결을 재사용하고, 실제 동시 실행 수는 _run_task 내부의 _sem 으로 제한한다.
        # 타임아웃은 connect(접속)와 read(전체)를 분리한다: read 는 task 가 오래 걸려도
        # 기다리되, connect 는 짧게 잡아 죽은 executor 에서 1시간씩 매달리지 않고 빠르게
        # 실패→재시도/failover 하도록 한다.
        timeout = httpx.Timeout(
            self.settings.task_timeout_s,
            connect=self.settings.task_connect_timeout_s,
        )
        async with httpx.AsyncClient(timeout=timeout) as client:
            await asyncio.gather(
                *(self._run_task(client, job, t) for t in job.tasks)
            )

    def _failover_order(self, task: Task) -> list[str]:
        """이 task 를 시도할 executor URL 순서를 만든다.

        - task_failover 가 꺼져 있으면 배정된 executor 하나만 시도한다(기존 동작).
        - selector+load_view 가 주입돼 있으면 **헬스/부하 기반(P2C 등)** 으로 dispatch 시점의
          최신 스냅샷을 보고 순서를 정한다(살아있는·한가한 노드 먼저, 죽은 후보는 뒤로).
          설정에 없는 배정 executor 가 있으면 보존을 위해 앞에 둔다.
        - 미주입(기본)이면 배정 executor 를 먼저, 나머지를 설정 순서대로 붙인다.
        짧은 connect 타임아웃 덕분에 죽은 후보는 빠르게 건너뛴다.
        """
        primary = task.executor_url
        if not self.settings.task_failover:
            return [primary] if primary else []

        if self.selector is not None and self.load_view is not None:
            order = self.selector.order(list(self.settings.executors), self.load_view.by_url())
            if primary and primary not in order:
                order = [primary] + order
            return order or ([primary] if primary else [])

        order = [primary] if primary else []
        for url in self.settings.executors:
            if url and url not in order:
                order.append(url)
        return order or [primary]

    async def _run_task(self, client: httpx.AsyncClient, job: Job, task: Task) -> None:
        # 디스패치 동시성 세마포어로 한 번에 떠 있는 task 수를 제한한다.
        async with self._sem:
            # 슬롯을 잡기까지 기다리는 사이 취소됐을 수 있으므로 실행 직전에 재확인.
            if self._cancel_observed(job):
                task.status = TaskStatus.CANCELLED
                self._save(job)
                return
            # 공유 예약: 실행 동안 배정 executor 를 예약(다른 coordinator 의 선택에 반영).
            # 배정 executor 기준으로 예약하고, 종료 시 해제한다(근사 — 실제 self-report 가 보정).
            reserved_url = task.executor_url if self.reservations is not None else None
            if reserved_url:
                await asyncio.to_thread(self.reservations.reserve, reserved_url)
            try:
                await self._dispatch_with_failover(client, job, task)
            except Exception as exc:
                # _RETRYABLE 외 예기치 못한 오류(JSON 파싱 실패 등)는 이 task 만 실패로
                # 격리한다(asyncio.gather 의 다른 task 에 영향 주지 않도록). job 최종 상태는
                # finalize_job 이 failure_policy 로 결정한다.
                # 조용한 실패를 막기 위해 스택트레이스와 함께 남긴다(디버깅의 유일한 단서일 수 있음).
                logger.exception(
                    "task %s 예기치 못한 오류 → FAILED (executor=%s): %s",
                    task.task_id, task.executor_url, exc,
                )
                task.status = TaskStatus.FAILED
                task.error = str(exc)
                self._save(job)
            finally:
                if reserved_url:
                    await asyncio.to_thread(self.reservations.release, reserved_url)

    async def _dispatch_with_failover(
        self, client: httpx.AsyncClient, job: Job, task: Task
    ) -> None:
        """task 를 executor 후보들에 순서대로 시도한다(재시도 + failover).

        - 시작(POST /tasks) 단계의 연결 실패는 같은 executor 에 지수 백오프로 재시도하고,
          재시도를 소진하면 다음 후보 executor 로 넘어간다(failover). 시작 전이라 재시도는
          항상 안전하다(중복 적재 위험 없음).
        - 시작에 성공한 뒤 폴링 단계에서 연결이 끊기면, 멱등 모드(overwrite_partitions)이고
          남은 후보가 있을 때만 다른 executor 로 재실행한다. append 는 중복 적재 위험이
          있으므로 재배정하지 않고 실패 처리한다.
        """
        order = self._failover_order(task)
        last_err: Exception | None = None

        for idx, url in enumerate(order):
            # 1) 시작 단계: 같은 executor 에 (1 + task_max_retries)회까지 시도.
            started = False
            for attempt in range(self.settings.task_max_retries + 1):
                if self._cancel_observed(job):
                    task.status = TaskStatus.CANCELLED
                    self._save(job)
                    return
                task.attempt += 1
                task.executor_url = url  # 현재 시도 중인 executor 를 task 에 반영
                try:
                    logger.debug(
                        "task %s 디스패치 시도 executor=%s attempt=%s",
                        task.task_id, url, task.attempt,
                    )
                    await self._start_task(client, job, task, url)
                    started = True
                    logger.debug("task %s 시작 성공 executor=%s", task.task_id, url)
                    break
                except _RETRYABLE as exc:
                    last_err = exc
                    task.error = str(exc)
                    self._save(job)
                    logger.warning(
                        "task %s 시작 실패(executor=%s, 시도=%s/%s): %s",
                        task.task_id, url, attempt + 1,
                        self.settings.task_max_retries + 1, exc,
                    )
                    if attempt < self.settings.task_max_retries:
                        # 지수 백오프: backoff * 2**시도횟수
                        await asyncio.sleep(
                            self.settings.task_retry_backoff_s * (2 ** attempt)
                        )
            if not started:
                if idx + 1 < len(order):
                    logger.warning(
                        "task %s → 다른 executor 로 failover (%s)", task.task_id, url,
                    )
                continue  # 다음 후보 executor 로 failover

            # 2) 폴링 단계: 시작 성공. 종료 상태까지 추적한다.
            try:
                await self._poll(client, job, task)
                self._save(job)
                return
            except _RETRYABLE as exc:
                last_err = exc
                can_redo = (
                    self.settings.task_failover
                    and job.write_mode == "overwrite_partitions"
                    and idx + 1 < len(order)
                )
                logger.warning(
                    "task %s 폴링 중 연결 실패(executor=%s): %s%s",
                    task.task_id, url, exc,
                    " → 다른 executor 로 재실행(멱등)" if can_redo else "",
                )
                if can_redo:
                    continue  # 멱등이므로 다른 executor 에 재실행 안전
                task.status = TaskStatus.FAILED
                task.error = f"폴링 실패: {exc}"
                self._save(job)
                return

        # 모든 후보/재시도 소진 → 최종 실패
        task.status = TaskStatus.FAILED
        task.error = str(last_err) if last_err else (task.error or "executor 연결 실패")
        logger.warning(
            "task %s 모든 executor 후보(%d개) 소진 — 최종 실패: %s",
            task.task_id, len(order), task.error,
        )
        self._save(job)

    async def _start_task(
        self, client: httpx.AsyncClient, job: Job, task: Task, url: str
    ) -> None:
        """executor 에 task 실행을 요청한다(POST /tasks). 비2xx 응답은 예외로 올린다."""
        # stage_insert 는 task 마다 staging 이름을 고유화(task_id 접미사)해 커넥션 풀
        # 재사용 시 TEMP "already exists" 충돌을 막는다. job 원본은 건드리지 않고 task
        # 전용 SQL 사본만 만들어 보낸다(executor 는 받은 이름을 그대로 쓰고 종료 시 DROP).
        st_table, st_ddl, st_insert = self._task_staging(job, task)
        resp = await client.post(
            f"{url}/tasks",
            json={
                "task_id": task.task_id,
                "job_id": job.job_id,
                "sub_query": task.sub_query,
                "target_table": job.target_table,
                "write_mode": job.write_mode,
                "partition_column": job.partition_column,
                "partition_values": task.partition_values,
                "exec_mode": job.exec_mode,
                "staging_table": st_table,
                "staging_ddl": st_ddl,
                "insert_sql": st_insert,
                "impala_query_options": job.impala_query_options,
                "username": job.username,
                # local_stage 는 로컬 CSV 경로, s3_stage 는 S3 객체 키를 out_path 로 싣는다
                # (둘 다 coordinator 가 확정한 "이 task 가 쓸 위치"). 외부테이블 생성/INSERT 는
                # 두 모드 모두 coordinator 가 배리어 후 Phase 2 에서 하므로 여기선 보내지 않는다.
                "out_path": task.out_path,
                # csv_options 는 local_stage/s3_stage 가 공유(CSV 방언 = 외부테이블 FORMAT).
                "csv_options": (
                    stage_sql.resolve_csv_options(job, self.settings)
                    if job.exec_mode in ("local_stage", "s3_stage") else None
                ),
            },
        )
        resp.raise_for_status()  # 5xx/4xx → HTTPStatusError(=_RETRYABLE) 로 재시도/failover

    async def _poll(self, client: httpx.AsyncClient, job: Job, task: Task) -> None:
        # executor가 task를 종료 상태로 보고할 때까지 주기적으로 상태를 조회한다.
        # 일시적 전송 오류(blip)는 task_max_retries 회까지 흡수하고, 그 한도를 넘으면
        # 예외를 올려 상위(_dispatch_with_failover)가 failover/실패를 결정하게 한다.
        transient = 0
        while task.status not in _TERMINAL:
            # 매 폴링마다 취소를 확인해, 취소가 감지되면 더 기다리지 않고 즉시 빠져나온다.
            if self._cancel_observed(job):
                task.status = TaskStatus.CANCELLED
                return
            # 폴링 간격만큼 쉬고 다음 상태를 조회(executor에 과도한 부하를 주지 않도록).
            await asyncio.sleep(self.settings.poll_interval_s)
            try:
                resp = await client.get(f"{task.executor_url}/tasks/{task.task_id}")
                resp.raise_for_status()
                transient = 0  # 성공하면 일시 오류 카운터 초기화
            except _RETRYABLE:
                transient += 1
                if transient > self.settings.task_max_retries:
                    raise  # 한도 초과 → 상위에서 failover/실패 처리
                continue
            data = resp.json()
            # executor가 보고한 상태/진척으로 로컬 task를 갱신한다. rows_written 은
            # 아직 보고 전이면 기존 값을 유지한다.
            prev_status = task.status
            prev_phase = task.current_phase
            task.status = TaskStatus(data["status"])
            task.rows_written = data.get("rows_written", task.rows_written)
            task.error = data.get("error")
            # 세부 단계 가시화: executor 가 보고한 phase 타임라인/조회 건수/현재 단계를
            # 그대로 수집해 coordinator 대시보드가 단계별 진행·소요를 보여줄 수 있게 한다.
            task.rows_read = data.get("rows_read", task.rows_read)
            task.current_phase = data.get("current_phase", task.current_phase)
            if data.get("phases") is not None:
                task.phases = data["phases"]
            # 상세 추적(DEBUG): 폴링에서 상태나 단계가 바뀐 순간만 남긴다(매 폴링 잡음 억제).
            if logger.isEnabledFor(logging.DEBUG) and (
                task.status != prev_status or task.current_phase != prev_phase
            ):
                logger.debug(
                    "task %s 폴링: 상태=%s 단계=%s 읽음=%s 적재=%s",
                    task.task_id, task.status.value, task.current_phase,
                    task.rows_read, task.rows_written,
                )

    async def cancel(self, job: Job) -> None:
        """취소 요청을 처리한다: 취소 플래그를 세우고 진행 중 task의 executor에 전파한다.

        이미 종료된 task(_TERMINAL)나 executor_url 이 없는 task는 전파 대상에서 제외하고,
        아직 진행 중인 task들에 대해서만 executor의 취소 엔드포인트를 동시에 호출한다.
        취소 플래그를 먼저 세우므로, 폴링 루프(_poll)들도 곧 이를 감지해 스스로 멈춘다.
        """
        with job_log_context(job.job_id):
            job.cancel_requested = True
            targets = [
                t for t in job.tasks if t.executor_url and t.status not in _TERMINAL
            ]
            logger.info("job 취소 요청 — 진행 중 task %d개에 전파", len(targets))
            if targets:
                # 취소 전파는 짧은 타임아웃의 별도 클라이언트로(메인 실행 클라이언트와 분리).
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await asyncio.gather(
                        *(self._cancel_task(client, t) for t in targets)
                    )

    async def _cleanup_stage(self, job: Job) -> None:
        """local_stage Phase 3(원격): 각 executor 에 로컬 CSV 디렉터리 정리를 지시한다.

        같은 호스트에 여러 executor 가 있어도 각자 자기 로컬 디렉터리를 지우면 되므로,
        중복 executor_url 만 제거해 병렬 호출한다. 설정에서 정리를 끄면 건너뛴다. 정리 실패는
        적재 결과에 영향이 없으므로 로깅만 한다(파일은 다음 job 이 job_id 로 격리되어 무해).
        """
        if not self.settings.stage_cleanup:
            return
        urls = {t.executor_url for t in job.tasks if t.executor_url}
        if not urls:
            return

        async def _one(client: httpx.AsyncClient, url: str) -> None:
            try:
                await client.post(f"{url}/stage/{job.job_id}/cleanup")
            except Exception as exc:
                logger.warning(
                    "job %s: executor %s 로컬 정리 실패: %s", job.job_id, url, exc
                )

        async with httpx.AsyncClient(timeout=10.0) as client:
            await asyncio.gather(*(_one(client, u) for u in urls))

    async def _cleanup_s3(self, job: Job) -> None:
        """s3_stage Phase 3(원격): executor 하나에 S3 스테이징 객체 정리를 지시한다.

        S3 객체는 세그먼트 로컬이 아니라 어느 executor 나 지울 수 있으므로, 배정된 executor 중
        하나에만 ``/s3/{job_id}/cleanup`` 을 호출한다(프리픽스 전체 삭제라 한 번이면 충분).
        설정에서 정리를 끄면 건너뛴다. 실패는 적재 결과에 무영향이라 로깅만 한다.
        """
        if not self.settings.s3_delete_on_cleanup:
            return
        url = next((t.executor_url for t in job.tasks if t.executor_url), None)
        if not url:
            return
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                await client.post(f"{url}/s3/{job.job_id}/cleanup")
        except Exception as exc:
            logger.warning("job %s: executor %s S3 정리 실패: %s", job.job_id, url, exc)

    async def _resolve_url_hosts(self, urls) -> dict:
        """각 executor 의 ``/metrics`` 를 조회해 보고된 gp_hostname 으로 매핑을 만든다(캐시).

        HTTP URL 의 호스트(IP/별칭/컨테이너명)는 ``gp_segment_configuration.hostname`` 과 다를
        수 있으므로, executor 가 신고한 실제 GP 세그먼트 호스트명을 file:// URI·파일 예산 배분에
        쓴다. 조회 실패/미보고면 URL 호스트로 폴백한다. gp_hostname 은 정적이라 URL 별로 한 번만
        조회한다(캐시). URL 이 None(로컬 배정 없음)이면 설정 gp_hostname 으로 채운다.
        """
        real = [u for u in dict.fromkeys(urls) if u]  # 중복 제거 + None 제외
        missing = [u for u in real if u not in self._gp_host_cache]
        if missing:
            async with httpx.AsyncClient(timeout=10.0) as client:
                async def _fetch(u: str) -> None:
                    host = ""
                    try:
                        resp = await client.get(f"{u}/metrics")
                        resp.raise_for_status()
                        host = (resp.json().get("gp_hostname") or "").strip()
                    except Exception as exc:
                        logger.warning("executor %s gp_hostname 조회 실패: %s", u, exc)
                    # 미보고/조회 실패 → URL 호스트로 폴백(최소한의 동작 보장).
                    self._gp_host_cache[u] = host or stage_sql.host_of(u)

                await asyncio.gather(*(_fetch(u) for u in missing))
        out = {u: self._gp_host_cache[u] for u in real}
        if any(u is None for u in urls):
            out[None] = self.settings.executor_gp_hostname or ""
        return out

    async def _cancel_task(self, client: httpx.AsyncClient, task: Task) -> None:
        try:
            await client.post(f"{task.executor_url}/tasks/{task.task_id}/cancel")
        except Exception as exc:
            # 전파 호출이 실패해도(이미 죽은 executor 등) 취소 자체는 진행한다.
            # 기존 에러를 덮어쓰지 않도록 비어 있을 때만 사유를 남긴다.
            task.error = task.error or str(exc)
        # 전파 성공/실패와 무관하게 로컬상으로는 취소된 것으로 표시한다.
        task.status = TaskStatus.CANCELLED


class LocalDispatcher(_DispatcherBase):
    """로컬(in-process) 디스패처: executor를 HTTP로 호출하지 않고 백엔드를 직접 실행한다.

    별도 executor 프로세스 없이 coordinator 안에서 동작 검증을 하기 위한 모드.
    기본 백엔드는 build_backend(settings)(greenplum.dsn 없으면 MockBackend).
    """

    def __init__(self, settings: Settings, history: Optional[JobHistoryRepository] = None, backend=None, store=None):
        super().__init__(settings, history=history, store=store)
        # 백엔드를 주입받을 수 있게 하여(테스트 등) 기본 빌드를 우회 가능하게 둔다.
        self._backend = backend

    def _get_backend(self):
        # 백엔드를 최초 사용 시점에 한 번만 생성해 캐싱(lazy init)한다.
        if self._backend is None:
            from executor.backend import build_backend  # 지연 임포트(순환 방지)
            self._backend = build_backend(self.settings)
        return self._backend

    def _get_stage_backend(self):
        # local 모드는 export 와 Phase 2(GP 적재)가 같은 프로세스의 동일 백엔드를 공유한다
        # (주입된 MockBackend 포함) → export 용 백엔드를 그대로 재사용한다.
        return self._get_backend()

    async def _cleanup_s3(self, job: Job) -> None:
        # s3_stage Phase 3(local): in-process 백엔드로 S3 프리픽스 객체를 직접 지운다.
        if not self.settings.s3_delete_on_cleanup:
            return
        prefix = s3_sql.s3_job_prefix(self.settings.s3_prefix, job.job_id)
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, self._get_backend().cleanup_s3_prefix, prefix
            )
        except Exception as exc:
            logger.warning("job %s: local S3 정리 실패: %s", job.job_id, exc)

    async def _execute(self, job: Job) -> None:
        # HTTP 버전과 동일하게 모든 task를 동시에 돌리되, 실제 작업은 backend가 수행한다.
        await asyncio.gather(*(self._run_task(job, t) for t in job.tasks))

    async def _run_task(self, job: Job, task: Task) -> None:
        async with self._sem:
            # 실행 직전 취소 확인(슬롯 대기 중 취소되었을 수 있음).
            if self._cancel_observed(job):
                task.status = TaskStatus.CANCELLED
                return
            backend = self._get_backend()
            loop = asyncio.get_running_loop()
            try:
                # backend 호출은 동기(blocking) I/O이므로 run_in_executor 로 스레드에
                # 넘겨 이벤트 루프를 막지 않는다. 상태는 READING→WRITING 으로 표시한다.
                task.status = TaskStatus.READING
                task.status = TaskStatus.WRITING
                # local 모드도 세부 단계를 채우도록 진행률/단계 콜백을 백엔드에 넘긴다.
                def _progress(n: int) -> None:
                    task.rows_written = n
                # 블로킹 백엔드를 스레드로 넘길 때 로그 컨텍스트(job_id/task_id)를 복사해 함께
                # 넘긴다 → 백엔드 스레드의 상세 로그(단계 전이 등)에도 [job][task] 가 붙는다.
                ctx = contextvars.copy_context()
                logger.debug(
                    "적재 시작 exec_mode=%s target=%s", job.exec_mode, job.target_table
                )
                # exec_mode 에 따라 backend 의 다른 실행 경로를 선택한다.
                if job.exec_mode == "statement":
                    rows = await loop.run_in_executor(
                        None, lambda: ctx.run(
                            backend.execute, task.sub_query, on_stage=task.on_stage
                        )
                    )
                elif job.exec_mode == "stage_insert":
                    # HTTP 경로와 동일하게 task 별 고유 staging 이름을 쓴다(종료 시 backend 가 DROP).
                    st_table, st_ddl, st_insert = self._task_staging(job, task)
                    rows = await loop.run_in_executor(
                        None,
                        lambda: ctx.run(
                            backend.stage_and_insert,
                            task.sub_query, st_table, st_ddl, st_insert,
                            on_progress=_progress,
                            query_options=job.impala_query_options,
                            on_stage=task.on_stage,
                        ),
                    )
                elif job.exec_mode == "local_stage":
                    # local_stage Phase 1: Impala 결과를 로컬 CSV 로 export(Phase 2 는 run() 이 배리어 후 수행).
                    csv_options = stage_sql.resolve_csv_options(job, self.settings)
                    rows = await loop.run_in_executor(
                        None,
                        lambda: ctx.run(
                            backend.export_to_local_csv,
                            task.sub_query, task.out_path, csv_options,
                            _progress,
                            query_options=job.impala_query_options,
                            on_stage=task.on_stage,
                        ),
                    )
                elif job.exec_mode == "s3_stage":
                    # s3_stage Phase 1: Impala 결과를 로컬 CSV 로 export → S3 업로드(Phase 2 는
                    # run() 이 배리어 후 수행). out_path 는 coordinator 가 확정한 S3 객체 키.
                    csv_options = stage_sql.resolve_csv_options(job, self.settings)
                    rows = await loop.run_in_executor(
                        None,
                        lambda: ctx.run(
                            backend.export_to_s3,
                            task.sub_query, task.out_path, job.job_id, task.task_id,
                            csv_options,
                            _progress,
                            query_options=job.impala_query_options,
                            on_stage=task.on_stage,
                        ),
                    )
                else:
                    rows = await loop.run_in_executor(
                        None,
                        lambda: ctx.run(
                            backend.move,
                            task.sub_query, job.target_table, job.write_mode,
                            job.partition_column, task.partition_values,
                            on_progress=_progress,
                            query_options=job.impala_query_options,
                            on_stage=task.on_stage,
                        ),
                    )
                task.rows_written = rows
                # 작업은 끝났지만 그 사이 취소가 들어왔다면 DONE 대신 CANCELLED 로 표시한다.
                task.status = (
                    TaskStatus.CANCELLED if job.cancel_requested else TaskStatus.DONE
                )
                if task.status == TaskStatus.CANCELLED:
                    close_open_phases(task.phases)  # 취소 — 열린 단계 마감
            except Exception as exc:
                # local 백엔드 실행 실패 — 스택트레이스와 함께 남긴다(조용한 실패 방지).
                logger.exception(
                    "task %s 백엔드 실행 실패 → FAILED (exec_mode=%s): %s",
                    task.task_id, job.exec_mode, exc,
                )
                task.status = TaskStatus.FAILED
                task.error = str(exc)
                # 실패한 단계를 지금으로 마감 → 대시보드 소요시간이 계속 증가하지 않게.
                close_open_phases(task.phases)
            finally:
                self._save(job)

    async def cancel(self, job: Job) -> None:
        # 로컬 모드: 원격 전파가 없으므로 플래그를 세우고 아직 진행 중인 task를
        # 곧바로 CANCELLED 로 표시하면 된다. 실행 중 task는 run_in_executor 의 동기
        # 호출이 끝난 뒤 위 분기에서 취소로 마감된다.
        with job_log_context(job.job_id):
            job.cancel_requested = True
            for t in job.tasks:
                if t.status not in _TERMINAL:
                    t.status = TaskStatus.CANCELLED
