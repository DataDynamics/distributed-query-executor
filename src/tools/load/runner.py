"""부하 테스트 오케스트레이터다. VU 를 단계 곡선에 맞춰 띄우고 회수하며, 종료와 drain 을 조율한다.

**VU 하나는 closed loop 다.** 요청을 보내고, 그 요청이 완료될 때까지 기다린 뒤, think time 을 쉬고
다시 보낸다. 완료의 뜻은 요청 type 이 정한다(:mod:`tools.load.request`). sync 는 응답을 받는 순간이
완료이고, async 는 접수 응답에서 id 를 꺼내 상태 URL 을 종료 상태가 될 때까지 폴링한다. 어느 쪽이든
동시에 진행 중인 요청 수는 VU 수를 넘지 않으므로, VU 수를 늘려 가며 서버가 어디서 포화되는지(429 가
나기 시작하는 지점, 완료 TPS 가 더 오르지 않는 지점)를 찾는다.

컨트롤러는 ``tick`` 초마다 목표 VU 수를 계산해 모자라면 새 VU 를 띄우고, 넘치면 가장 최근에 띄운
VU 부터 멈춤 표시를 한다. 멈춤 표시를 받은 VU 는 진행 중인 요청을 끝까지 지켜본 뒤 빠진다.
요청을 버리고 나가면 그 결과가 측정에서 빠지고, 서버에는 여전히 부하로 남아 다음 구간의 측정을
오염시키기 때문이다.

테스트를 멈출 때(시간 만료, 반복 완료, Ctrl-C)는 먼저 새 요청을 막고 ``drain_timeout`` 동안 진행
중인 요청을 기다린다. 그래도 남은 async 요청은 ``on_stop`` 정책에 따라 취소를 보내거나(cancel) 그대로
둔 채(abandon) ABANDONED 로 기록한다. 응답을 기다리던 sync 요청은 취소할 길이 없어 연결만 끊고
ABANDONED 로 남긴다.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from . import scenario
from .client import CoordinatorClient, Response, server_seconds
from .request import RequestSpec
from .sampler import ResourceSampler
from .stats import Collector

logger = logging.getLogger(__name__)


@dataclass
class LoadConfig:
    """실행 설정이다. cli 가 명령행·YAML 을 합쳐 만든다(시간 값은 모두 초)."""

    request: RequestSpec
    stages: List[scenario.Stage]
    iterations: Optional[int] = None       # VU 당 반복 횟수 상한(없으면 시간으로만 끝낸다)
    think_time: float = 0.0
    poll_interval: float = 2.0
    poll_jitter: float = 0.2               # 폴링 간격의 ± 비율
    job_timeout: float = 3600.0
    reject_backoff: Optional[float] = None  # None 이면 Retry-After 를 따른다(없으면 1초)
    error_backoff: float = 1.0
    drain_timeout: float = 300.0
    on_stop: str = "cancel"                # cancel | abandon
    idempotency_key: bool = False
    baseline: float = 0.0
    sample_interval: float = 5.0
    tick: float = 0.1
    seed: Optional[int] = None
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])


@dataclass
class _VU:
    vu_id: int
    task: Optional[asyncio.Task] = None
    stop: bool = False
    iteration: int = 0
    exhausted: bool = False                # 반복 횟수를 다 채워 스스로 끝난 VU
    busy: bool = False                     # 요청을 보내고 응답을 기다리는 중
    job: Optional[Dict[str, Any]] = None   # 폴링 중인 async 요청(base, id, submitted)


class LoadRunner:
    """부하 테스트 한 번을 실행한다. ``await run()`` 이 끝나면 collector·sampler 에 결과가 있다."""

    def __init__(self, config: LoadConfig, client: CoordinatorClient,
                 sampler_enabled: bool = True,
                 on_tick: Optional[Callable[["LoadRunner"], None]] = None) -> None:
        self.cfg = config
        self.client = client
        self.collector = Collector()
        self.rng = random.Random(config.seed)
        self._t0: Optional[float] = None
        self.sampler = ResourceSampler(client, config.sample_interval, self.elapsed) \
            if sampler_enabled else None
        self.on_tick = on_tick
        self.vus: Dict[int, _VU] = {}
        self._next_vu = 1
        self._seq = 0
        self._stop_new = False          # 새 제출 금지(종료 시작)
        self._abort = asyncio.Event()   # Ctrl-C 등 외부 중단 요청
        self._wake = asyncio.Event()    # 중단이든 정상 종료든 멈추기 시작하면 sleep 을 깨운다
        self.stop_reason = ""
        self.load_elapsed = 0.0          # 부하 구간(제출이 가능했던 시간)
        self.total_elapsed = 0.0
        self.phase = "init"

    # ── 시간과 상태 ──

    def elapsed(self) -> float:
        """부하 시작(기준선 뒤) 기준 경과 초다. 기준선 수집 중에는 음수가 된다."""
        if self._t0 is None:
            return 0.0
        return time.monotonic() - self._t0

    @property
    def active_vus(self) -> int:
        return sum(1 for v in self.vus.values() if v.task and not v.task.done())

    @property
    def inflight(self) -> int:
        return sum(1 for v in self.vus.values() if v.busy or v.job is not None)

    def request_stop(self, reason: str = "사용자 중단") -> None:
        """외부(시그널 처리기)에서 테스트를 멈추게 한다. 두 번째 요청은 drain 도 건너뛴다."""
        if self._abort.is_set() or self._stop_new:
            self.stop_reason = self.stop_reason or reason
            self.cfg.drain_timeout = 0.0
        self.stop_reason = self.stop_reason or reason
        self._abort.set()
        self._wake.set()

    # ── 실행 ──

    async def run(self) -> None:
        stop_sampler = asyncio.Event()
        sampler_task = None
        # 기준선: 부하 전의 자원 수준을 먼저 잰다. 이때 elapsed() 는 음수로 기록된다.
        self._t0 = time.monotonic() + self.cfg.baseline
        if self.sampler:
            self.sampler.phase = "baseline"
            sampler_task = asyncio.ensure_future(self.sampler.run(stop_sampler))
        if self.cfg.baseline > 0:
            self.phase = "baseline"
            try:
                await asyncio.wait_for(self._abort.wait(), timeout=self.cfg.baseline)
            except asyncio.TimeoutError:
                pass
        self._t0 = time.monotonic()
        try:
            if not self._abort.is_set():
                self.phase = "load"
                if self.sampler:
                    self.sampler.phase = "load"
                await self._control_loop()
            self.load_elapsed = self.elapsed()
            self.phase = "drain"
            if self.sampler:
                self.sampler.phase = "drain"
            await self._drain()
        finally:
            self.total_elapsed = self.elapsed()
            self.phase = "done"
            stop_sampler.set()
            if sampler_task is not None:
                await sampler_task
                # 마지막 상태를 한 번 더 남겨 drain 뒤 자원이 내려왔는지 보이게 한다.
                await self.sampler.sample_once()

    async def _control_loop(self) -> None:
        total = scenario.total_duration(self.cfg.stages)
        while True:
            t = self.elapsed()
            if self._abort.is_set():
                self.stop_reason = self.stop_reason or "사용자 중단"
                return
            if t >= total:
                self.stop_reason = "지정 시간 종료"
                return
            target = scenario.target_vus(self.cfg.stages, t)
            self._reconcile(target)
            self.collector.record_gauges(t, self.active_vus, self.inflight)
            if self.on_tick:
                self.on_tick(self)
            # 반복 횟수 모드: 모든 VU 가 반복을 채웠고 더 띄울 VU 가 없으면 끝낸다.
            if self.cfg.iterations is not None and self._all_exhausted(t):
                self.stop_reason = "반복 횟수 완료"
                return
            try:
                await asyncio.wait_for(self._abort.wait(), timeout=self.cfg.tick)
            except asyncio.TimeoutError:
                pass

    def _slots(self) -> List[_VU]:
        """목표 VU 수와 비교할 자리다. 반복을 채우고 끝난 VU 도 자리를 차지한다.

        JMeter 처럼 반복을 다 채운 스레드를 새로 채워 넣지 않기 위해서다. 채워 넣으면
        ``--iterations`` 가 VU 당 상한이 아니라 무한 반복이 된다.
        """
        return [v for v in self.vus.values() if not v.stop and (
            v.exhausted or (v.task is not None and not v.task.done()))]

    def _reconcile(self, target: int) -> None:
        slots = self._slots()
        if len(slots) < target:
            for _ in range(target - len(slots)):
                self._spawn()
        elif len(slots) > target:
            # 가장 최근에 띄운 VU 부터 멈춘다. 진행 중인 job 은 끝까지 지켜본 뒤 빠진다.
            for v in sorted(slots, key=lambda x: x.vu_id, reverse=True)[: len(slots) - target]:
                v.stop = True

    def _all_exhausted(self, t: float) -> bool:
        """도는 VU 가 없고, 남은 일정에서도 새 VU 를 띄울 일이 없으면 참이다."""
        if self.active_vus > 0:
            return False
        return len(self._slots()) >= scenario.max_target_after(self.cfg.stages, t)

    def _spawn(self) -> None:
        vu = _VU(self._next_vu)
        self._next_vu += 1
        self.vus[vu.vu_id] = vu
        vu.task = asyncio.ensure_future(self._vu_loop(vu))

    async def _sleep(self, seconds: float) -> None:
        """멈추기 시작하면 바로 깨는 sleep 이다(think time·backoff 용)."""
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def _next_body(self, vu: _VU) -> Optional[Dict[str, Any]]:
        self._seq += 1
        if self.cfg.request.body is None:
            return None
        ctx = scenario.RenderContext(vu.vu_id, vu.iteration, self._seq, self.cfg.run_id, self.rng)
        return scenario.render_body(self.cfg.request.body, ctx)

    async def _vu_loop(self, vu: _VU) -> None:
        try:
            while not self._stop_new and not vu.stop and not self._abort.is_set():
                if self.cfg.iterations is not None and vu.iteration >= self.cfg.iterations:
                    vu.exhausted = True
                    return
                await self._iteration(vu)
                vu.iteration += 1
                if self.cfg.think_time > 0:
                    await self._sleep(self.cfg.think_time)
        except asyncio.CancelledError:
            raise
        except Exception:  # VU 하나의 예상치 못한 오류가 테스트 전체를 죽이지 않게 한다
            logger.exception("VU %d 오류로 종료", vu.vu_id)

    def _classify(self, res: Response) -> str:
        """응답을 요청 분류(stats.SUBMIT_OUTCOMES)로 나눈다. 판정 기준은 요청 type 이다."""
        code = res.status_code
        if code is None:
            return "conn_error"
        if code == 429:
            return "rejected_429"
        spec = self.cfg.request
        if spec.is_async:
            if 200 <= code < 300:
                return "accepted" if self._request_id(res) else "missing_id"
        elif spec.is_success_status(code):
            return "ok"
        elif 200 <= code < 300:
            return "unexpected_status"
        return "server_error" if code >= 500 else "client_error"

    def _request_id(self, res: Response) -> Optional[str]:
        data = res.data
        if isinstance(data, dict):
            value = data.get(self.cfg.request.poll.id_field)
            if value not in (None, ""):
                return str(value)
        return None

    async def _iteration(self, vu: _VU) -> None:
        spec = self.cfg.request
        body = self._next_body(vu)
        headers = ({"Idempotency-Key": f"load-{self.cfg.run_id}-{self._seq}"}
                   if self.cfg.idempotency_key else None)
        vu.busy = True
        try:
            res = await self.client.send(spec.method, spec.path, body, headers)
        finally:
            vu.busy = False
        outcome = self._classify(res)
        error = res.error
        if outcome == "missing_id":
            error = f"응답에 {spec.poll.id_field} 가 없습니다: {str(res.data)[:120]}"
        elif outcome == "unexpected_status":
            error = f"성공 코드로 지정하지 않은 응답 HTTP {res.status_code}"
        self.collector.record_submit(self.elapsed(), outcome, res.latency, error,
                                     vus=self.active_vus)
        if outcome == "accepted":
            vu.job = {"base": res.coordinator, "id": self._request_id(res),
                      "submitted": time.monotonic() - (res.latency or 0.0)}
            try:
                await self._wait_job(vu)
            finally:
                vu.job = None
        elif outcome == "rejected_429":
            backoff = self.cfg.reject_backoff
            if backoff is None:
                backoff = res.retry_after if res.retry_after is not None else 1.0
            await self._sleep(backoff)
        elif outcome != "ok":
            await self._sleep(self.cfg.error_backoff)

    def _poll_delay(self) -> float:
        j = self.cfg.poll_jitter
        return self.cfg.poll_interval * (1 + self.rng.uniform(-j, j))

    async def _wait_job(self, vu: _VU) -> None:
        """async 요청이 종료 상태가 될 때까지 폴링해 결과를 기록한다.

        중단 요청(_abort)으로는 폴링을 멈추지 않는다. 진행 중인 요청을 drain 동안 계속 지켜봐야
        종료 결과가 측정에 들어가기 때문이다. drain 이 끝나면 _drain 이 이 코루틴을 취소한다.
        """
        job = vu.job
        poll = self.cfg.request.poll
        path = poll.status_path(job["id"])
        not_found = 0
        while True:
            await asyncio.sleep(self._poll_delay())
            waited = time.monotonic() - job["submitted"]
            if waited > self.cfg.job_timeout:
                self.collector.record_job(self.elapsed(), "POLL_TIMEOUT", False, None,
                                          error=f"{job['id']} {int(waited)}초 초과")
                return
            self.collector.poll_requests += 1
            pr = await self.client.poll(job["base"], path)
            if pr.status_code == 404:
                # 연속으로 사라져 있으면 coordinator 재기동 등으로 요청을 잃은 것이다.
                not_found += 1
                if not_found >= 3:
                    self.collector.record_job(self.elapsed(), "LOST", False, None,
                                              error=f"{job['id']} 404")
                    return
                continue
            if pr.status_code != 200 or not isinstance(pr.data, dict):
                self.collector.poll_errors += 1
                continue
            not_found = 0
            b = pr.data
            status = str(b.get(poll.status_field, ""))
            if status in poll.terminal:
                success = status in poll.success
                self.collector.record_job(
                    self.elapsed(), status, success, time.monotonic() - job["submitted"],
                    queue=server_seconds(b.get(poll.created_field), b.get(poll.started_field)),
                    run=server_seconds(b.get(poll.started_field), b.get(poll.finished_field)),
                    rows=_as_int(b.get(poll.rows_field)),
                    error=None if success else b.get(poll.error_field),
                )
                return

    async def _drain(self) -> None:
        self._stop_new = True
        self._wake.set()
        tasks = [v.task for v in self.vus.values() if v.task and not v.task.done()]
        if tasks:
            timeout = self.cfg.drain_timeout
            deadline = time.monotonic() + timeout
            pending = set(tasks)
            while pending:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                done, pending = await asyncio.wait(pending, timeout=min(left, 1.0))
                self.collector.record_gauges(self.elapsed(), self.active_vus, self.inflight)
                if self.on_tick:
                    self.on_tick(self)
                # 두 번째 Ctrl-C 는 drain_timeout 을 0 으로 만든다.
                if self.cfg.drain_timeout <= 0:
                    break
            leftovers = [v for v in self.vus.values() if v.task and not v.task.done()]
            # 태스크를 취소하면 _iteration 의 finally 가 vu.job 을 지우므로 먼저 챙겨 둔다.
            jobs = [v.job for v in leftovers if v.job is not None]
            waiting = sum(1 for v in leftovers if v.busy)
            for v in leftovers:
                v.task.cancel()
            if leftovers:
                await asyncio.gather(*(v.task for v in leftovers), return_exceptions=True)
            await self._abandon(jobs, waiting)

    async def _abandon(self, jobs: List[Dict[str, Any]], waiting: int) -> None:
        """drain 뒤에도 남은 요청을 ABANDONED 로 기록하고, async 는 정책에 따라 취소한다.

        ``waiting`` 은 응답을 기다리다 끊긴 요청 수다. sync 요청이거나 async 의 접수 응답을
        아직 못 받은 경우라 취소할 id 가 없다.
        """
        cancel = self.cfg.on_stop == "cancel"
        targets = []
        if cancel:
            for j in jobs:
                url = self.cfg.request.poll.cancel_url(j["id"])
                if url:
                    targets.append((j["base"], url))
        if targets:
            results = await asyncio.gather(*(self.client.cancel(b, u) for b, u in targets))
            self.collector.cancel_sent += sum(1 for r in results if r)
        how = "취소 요청" if targets else "방치"
        for _ in jobs:
            self.collector.record_job(self.elapsed(), "ABANDONED", False, None,
                                      error=f"drain 시간 초과로 {how}")
        for _ in range(waiting):
            self.collector.record_job(self.elapsed(), "ABANDONED", False, None,
                                      error="drain 시간 초과로 응답 대기 중 연결 종료")


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
