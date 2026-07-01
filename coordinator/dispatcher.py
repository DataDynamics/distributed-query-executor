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
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Protocol

import httpx

from core.logging import job_log_context
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
    elif job.failure_policy == "best_effort":
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

    def _save(self, job: Job) -> None:
        # store 가 주입된 경우에만 영속화한다(인메모리 모드면 no-op).
        # 저장 실패가 작업 진행을 막아선 안 되므로 예외는 로깅만 하고 삼킨다.
        if self.store is None:
            return
        try:
            self.store.save(job)
        except Exception:
            logger.exception("job %s 저장 실패", job.job_id)

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
                async with self.admission.slot():
                    # 대기 중 취소되었으면 실행하지 않고 즉시 종료 처리.
                    if self._cancel_observed(job):
                        finalize_job(job)
                        job.finished_at = _now_iso()
                        self._save(job)
                        await self.history.record(job)
                        return job.job_id
                    job.status = JobStatus.RUNNING
                    job.started_at = _now_iso()
                    self._save(job)
                    await self.history.record(job)  # 시작 이력
                    try:
                        await self._execute(job)
                    finally:
                        # _execute 가 예외로 끝나도 최종 상태/종료시각/이력은 반드시 남긴다.
                        finalize_job(job)
                        job.finished_at = _now_iso()
                        self._save(job)
                        await self.history.record(job)  # 종료 이력
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
                    await self._start_task(client, job, task, url)
                    started = True
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
        self._save(job)

    async def _start_task(
        self, client: httpx.AsyncClient, job: Job, task: Task, url: str
    ) -> None:
        """executor 에 task 실행을 요청한다(POST /tasks). 비2xx 응답은 예외로 올린다."""
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
                "staging_table": job.staging_table,
                "staging_ddl": job.staging_ddl,
                "insert_sql": job.insert_sql,
                "impala_query_options": job.impala_query_options,
                "username": job.username,
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
            task.status = TaskStatus(data["status"])
            task.rows_written = data.get("rows_written", task.rows_written)
            task.error = data.get("error")
            # 세부 단계 가시화: executor 가 보고한 phase 타임라인/조회 건수/현재 단계를
            # 그대로 수집해 coordinator 대시보드가 단계별 진행·소요를 보여줄 수 있게 한다.
            task.rows_read = data.get("rows_read", task.rows_read)
            task.current_phase = data.get("current_phase", task.current_phase)
            if data.get("phases") is not None:
                task.phases = data["phases"]

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
            if targets:
                # 취소 전파는 짧은 타임아웃의 별도 클라이언트로(메인 실행 클라이언트와 분리).
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await asyncio.gather(
                        *(self._cancel_task(client, t) for t in targets)
                    )

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
                # exec_mode 에 따라 backend 의 다른 실행 경로를 선택한다.
                if job.exec_mode == "statement":
                    rows = await loop.run_in_executor(
                        None, lambda: backend.execute(task.sub_query, on_stage=task.on_stage)
                    )
                elif job.exec_mode == "stage_insert":
                    rows = await loop.run_in_executor(
                        None,
                        lambda: backend.stage_and_insert(
                            task.sub_query, job.staging_table, job.staging_ddl, job.insert_sql,
                            on_progress=_progress,
                            query_options=job.impala_query_options,
                            on_stage=task.on_stage,
                        ),
                    )
                else:
                    rows = await loop.run_in_executor(
                        None,
                        lambda: backend.move(
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
            except Exception as exc:
                task.status = TaskStatus.FAILED
                task.error = str(exc)
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
