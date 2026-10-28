"""성능 튜닝용 지표를 모아 별도 로그 파일(``*-perf.log``)에 주기적으로 남기는 공용 모듈이다.

coordinator 와 executor 는 부하가 몰릴 때 어디서 기다리는지(실행 슬롯, 디스패치 한도, 스레드 풀,
GP 커넥션 풀, 이벤트 루프)를 겉으로 드러내지 않는다. 그래서 각 지점이 이 모듈의 레지스트리에 값을
흘려 넣고, 리포터가 ``logging.perf.interval_s`` 마다 한 줄로 묶어 남긴다. 한 줄은 사람이 바로 읽을 수
있게 ``key=value`` 를 공백으로 이어 붙인 형식이고, ``bin/perf-report`` 가 이 파일을 다시 읽어 시간대별
요약과 병목 의심 구간을 뽑는다.

값은 세 종류다.

- **카운터**(``inc``): 이번 구간에 일어난 횟수·양이다(429 건수, 적재 행수). 기록할 때마다 0 으로 돌린다.
- **분포**(``observe``): 이번 구간에 관측한 값들의 건수·p50·p95·max 다(슬롯 대기시간, 루프 지연).
  관측이 없으면 줄에서 뺀다. 샘플은 구간마다 최대 ``_RESERVOIR`` 개까지만 두고 넘치면 무작위로
  일부만 남겨(reservoir sampling) 메모리와 계산량을 묶는다. max 는 샘플과 무관하게 정확히 잰다.
- **게이지**(``gauge``): 기록하는 순간 함수를 불러 읽는 현재 값이다(실행 중 job 수, 풀 큐 길이).

기록 비용은 관측 한 번에 락 한 번과 리스트 추가 정도라 실행 경로에 넣어도 부담이 없다. 리포터는 메모리
값만 읽고 DB 나 네트워크를 건드리지 않으므로, 지표를 남기느라 다시 병목을 만들지 않는다.

Prometheus exporter(:mod:`core.prometheus`)도 같은 레지스트리를 읽는다. 다만 Prometheus 는 구간마다 비우는
값이 아니라 프로세스가 뜬 뒤 쌓인 누적값을 긁어 가서 ``rate()`` 로 나누므로, 레지스트리는 구간 값과 별도로
카운터의 누적 합(:meth:`PerfRegistry.totals`)과 분포의 누적 히스토그램(:meth:`PerfRegistry.histograms`)을
함께 쌓는다. 지표 로그를 비우는 :meth:`PerfRegistry.collect` 는 이 누적값을 건드리지 않으므로 두 소비자가
서로의 값을 흐트러뜨리지 않는다.
"""

from __future__ import annotations

import asyncio
import bisect
import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

# 지표 줄을 받는 전용 로거다. setup_logging 이 *-perf.log 핸들러를 붙이고 상위로 전파하지 않게 한다.
PERF_LOGGER_NAME = "dqe.perf"
perf_logger = logging.getLogger(PERF_LOGGER_NAME)

# 구간마다 분포 하나가 들고 있을 최대 샘플 수다. 백분위 추정에는 이 정도면 충분하다.
_RESERVOIR = 10000

# 누적 히스토그램의 버킷 상한(초)이다. 루프 지연(수 ms)부터 job 소요(수십 분)까지 한 벌로 덮는다.
SECONDS_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600,
)
# 단위를 알 수 없는 분포에 쓰는 버킷 상한이다(현재 그런 지표는 없고, 새 지표의 안전망이다).
PLAIN_BUCKETS = (1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000)


def unit_scale(name: str) -> Optional[float]:
    """지표 이름 끝에서 시간 단위를 읽어 초로 바꾸는 배율을 돌려준다(시간 지표가 아니면 None).

    ``_ms``·``.ms`` 로 끝나면 밀리초(0.001), ``_s`` 로 끝나면 초(1)다. Prometheus 의 기본 단위가 초라서
    exporter 는 이 배율로 값을 초로 바꿔 ``*_seconds`` 로 내보내고, 히스토그램 버킷도 같은 배율로 맞춘다.
    """
    if name.endswith(("_ms", ".ms")):
        return 0.001
    if name.endswith("_s"):
        return 1.0
    return None


class _Histogram:
    """프로세스가 뜬 뒤 관측한 값 전체의 누적 히스토그램이다. 값은 지표 이름의 원래 단위로 센다."""

    __slots__ = ("bounds", "counts", "count", "sum")

    def __init__(self, name: str) -> None:
        scale = unit_scale(name)
        # 버킷 상한을 지표의 원래 단위로 바꿔 둔다(ms 지표면 0.001초 → 1ms). 관측할 때 매번 나누지 않으려는 것이다.
        self.bounds = tuple(b / scale for b in SECONDS_BUCKETS) if scale else PLAIN_BUCKETS
        self.counts = [0] * len(self.bounds)
        self.count = 0
        self.sum = 0.0

    def add(self, value: float) -> None:
        """값 하나를 그 값 이상인 가장 작은 상한의 버킷에 센다. 마지막 상한보다 크면 건수와 합에만 들어간다."""
        self.count += 1
        self.sum += value
        i = bisect.bisect_left(self.bounds, value)
        if i < len(self.counts):
            self.counts[i] += 1

    def snapshot(self) -> Dict[str, object]:
        """Prometheus 형식에 맞춘 누적 버킷(상한 이하 건수)과 합·건수를 돌려준다."""
        cumulative, running = [], 0
        for bound, n in zip(self.bounds, self.counts):
            running += n
            cumulative.append((bound, running))
        return {"buckets": cumulative, "count": self.count, "sum": self.sum}


class _Dist:
    """한 구간의 관측값 분포다. 샘플은 상한까지만 두고 건수와 max 는 정확히 센다."""

    __slots__ = ("count", "max", "samples")

    def __init__(self) -> None:
        self.count = 0
        self.max = 0.0
        self.samples: list = []

    def add(self, value: float) -> None:
        """값 하나를 센다. 건수와 max 는 모든 값으로, 백분위는 상한(_RESERVOIR)까지의 무작위 표본으로 낸다."""
        self.count += 1
        if self.count == 1 or value > self.max:
            self.max = value
        if len(self.samples) < _RESERVOIR:
            self.samples.append(value)
        else:
            # Algorithm R: 지금까지 본 값 하나하나가 같은 확률로 표본에 남도록 바꿔 넣는다.
            j = random.randrange(self.count)
            if j < _RESERVOIR:
                self.samples[j] = value

    def summary(self) -> Dict[str, float]:
        """구간의 건수·p50·p95·max 를 돌려준다. 지표 줄에는 ``<이름>.n``·``<이름>.p50`` 처럼 펼쳐진다."""
        ordered = sorted(self.samples)
        return {
            "n": self.count,
            "p50": _percentile(ordered, 50),
            "p95": _percentile(ordered, 95),
            "max": self.max,
        }


def _percentile(ordered: list, pct: float) -> float:
    """정렬된 표본의 백분위를 nearest-rank 로 구한다(표본이 비면 0)."""
    if not ordered:
        return 0.0
    rank = max(1, -(-len(ordered) * pct // 100))  # ceil(n * pct / 100)
    return ordered[int(rank) - 1]


class PerfRegistry:
    """카운터·분포·게이지를 모아 두었다가 한 번에 꺼내 주는 저장소다. 여러 스레드에서 기록해도 안전하다."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, float] = {}
        self._dists: Dict[str, _Dist] = {}
        self._gauges: Dict[str, tuple] = {}
        # Prometheus 용 누적값이다. collect() 가 비우지 않는다.
        self._totals: Dict[str, float] = {}
        self._hists: Dict[str, _Histogram] = {}

    def inc(self, name: str, n: float = 1) -> None:
        """카운터를 n 만큼 올린다. 처음 쓰는 이름이면 만든다."""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + n
            self._totals[name] = self._totals.get(name, 0) + n

    def observe(self, name: str, value: float) -> None:
        """분포에 값 하나를 더한다."""
        with self._lock:
            dist = self._dists.get(name)
            if dist is None:
                dist = self._dists[name] = _Dist()
            dist.add(float(value))
            hist = self._hists.get(name)
            if hist is None:
                hist = self._hists[name] = _Histogram(name)
            hist.add(float(value))

    def gauge(self, name: str, fn: Callable[[], object], *, prefix: bool = True) -> None:
        """기록 시점에 부를 게이지 함수를 등록한다. 같은 이름이면 새 함수로 바꾼다.

        함수는 숫자 하나를 돌려주거나, ``{하위키: 숫자}`` dict 를 돌려준다. dict 면 ``이름.하위키`` 로
        펼치고, ``prefix=False`` 면 하위키를 그대로 키로 쓴다(``executor[host].inflight`` 처럼 키 모양을
        게이지가 정할 때). None 을 돌려주거나 예외가 나면 그 게이지는 이번 줄에서 뺀다.
        """
        with self._lock:
            self._gauges[name] = (fn, prefix)

    def unregister(self, name: str) -> None:
        """게이지를 뗀다. 없는 이름이면 아무것도 하지 않는다."""
        with self._lock:
            self._gauges.pop(name, None)

    def collect(self) -> Dict[str, object]:
        """게이지를 읽고 이번 구간의 카운터·분포를 꺼낸 뒤 비운다. 키 이름순 dict 를 돌려준다.

        게이지 함수는 락 밖에서 부른다. 게이지가 다른 객체의 락을 잡거나 이 레지스트리에 다시 기록해도
        교착에 빠지지 않게 하기 위해서다.
        """
        with self._lock:
            counters, self._counters = self._counters, {k: 0 for k in self._counters}
            dists, self._dists = self._dists, {}
        out = self.read_gauges()
        out.update(counters)
        for name, dist in dists.items():
            for sub, v in dist.summary().items():
                out[f"{name}.{sub}"] = v
        return dict(sorted(out.items()))

    def read_gauges(self, exclude=()) -> Dict[str, object]:
        """등록된 게이지를 지금 읽어 펼친 dict 를 돌려준다. 카운터·분포는 건드리지 않는다.

        ``exclude`` 에 든 게이지 이름은 부르지 않는다. 직전 호출과의 차이로 값을 내는 게이지(시스템 I/O
        속도)는 다른 소비자가 부르면 지표 로그의 구간이 흐트러지므로 exporter 는 이것으로 뺀다.
        """
        with self._lock:
            gauges = [(n, g) for n, g in self._gauges.items() if n not in exclude]
        out: Dict[str, object] = {}
        for name, (fn, use_prefix) in gauges:
            try:
                value = fn()
            except Exception:
                logger.debug("perf 게이지 %s 읽기 실패", name, exc_info=True)
                continue
            if isinstance(value, dict):
                for sub, v in value.items():
                    if v is not None:
                        out[f"{name}.{sub}" if use_prefix else sub] = v
            elif value is not None:
                out[name] = value
        return out

    def totals(self) -> Dict[str, float]:
        """프로세스가 뜬 뒤 쌓인 카운터 누적 합이다(Prometheus counter 용)."""
        with self._lock:
            return dict(self._totals)

    def histograms(self) -> Dict[str, Dict[str, object]]:
        """프로세스가 뜬 뒤 관측한 분포의 누적 히스토그램이다(Prometheus histogram 용)."""
        with self._lock:
            return {name: hist.snapshot() for name, hist in self._hists.items()}

    def reset(self) -> None:
        """모든 값과 게이지를 지운다(테스트용)."""
        with self._lock:
            self._counters.clear()
            self._dists.clear()
            self._gauges.clear()
            self._totals.clear()
            self._hists.clear()


# 프로세스 하나에 레지스트리 하나를 둔다(coordinator 와 executor 는 서로 다른 프로세스다).
perf = PerfRegistry()


class _Timer:
    """:func:`timer` 가 돌려주는 컨텍스트 관리자다. 블록을 빠져나올 때 경과 ms 를 분포에 더하고 예외는 삼키지 않는다."""

    __slots__ = ("name", "registry", "t0")

    def __init__(self, name: str, registry: PerfRegistry) -> None:
        self.name, self.registry = name, registry

    def __enter__(self):
        self.t0 = time.monotonic()
        return self

    def __exit__(self, *exc):
        self.registry.observe(self.name, (time.monotonic() - self.t0) * 1000)
        return False


def timer(name: str, registry: PerfRegistry = perf) -> _Timer:
    """``with timer("x_ms"):`` 블록의 소요(ms)를 분포 ``x_ms`` 에 더한다. 예외로 끝나도 잰다."""
    return _Timer(name, registry)


# ───────── 키·값 줄 형식 ─────────

def format_value(value: object) -> str:
    """값 하나를 줄에 쓸 문자열로 바꾼다. 공백·따옴표·= 가 들어간 문자열은 큰따옴표로 감싼다."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:  # NaN
            return "nan"
        if abs(value) >= 10:
            value = round(value, 1)
        else:
            value = round(value, 2)  # 아주 작은 값이 지수 표기(9.3e-08)로 나오지 않게 소수 둘째 자리에서 자른다
        if value == int(value) and abs(value) < 1e15:
            return str(int(value))
        return f"{value:.1f}" if abs(value) >= 10 else f"{value:.2f}".rstrip("0")
    text = "" if value is None else str(value)
    if text == "" or any(c in text for c in ' "=\t\n'):
        escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("\t", " ")
        return f'"{escaped}"'
    return text


def format_kv(fields: Dict[str, object]) -> str:
    """dict 를 ``k=v k=v`` 한 줄로 만든다. 순서는 넘긴 순서를 따른다."""
    return " ".join(f"{k}={format_value(v)}" for k, v in fields.items())


def endpoint_key(url: str) -> str:
    """executor URL 을 지표 키에 넣을 짧은 이름(호스트:포트)으로 줄인다."""
    text = url.split("://", 1)[-1]
    return text.rstrip("/")


def emit_event(kind: str, fields: Dict[str, object]) -> None:
    """job·task 종료 같은 사건 한 건을 지표 파일에 바로 한 줄 남긴다. 지표 로그가 꺼져 있으면 버린다."""
    if not perf_logger.isEnabledFor(logging.INFO):
        return
    perf_logger.info(format_kv({"kind": kind, **fields}))


# ───────── 스레드 풀 계측 ─────────

class InstrumentedThreadPoolExecutor(ThreadPoolExecutor):
    """작업마다 큐 대기시간과 실행시간을 재고 사용 중·큐 길이를 게이지로 내보내는 스레드 풀이다.

    풀이 포화되면 작업이 스레드를 얻기까지 기다리는데, 그 시간이 ``pool.<이름>.wait_ms`` 다. 적재처럼
    긴 작업이 풀을 독점하면 이 값이 먼저 치솟으므로, 기본 풀과 store·meta 전용 풀을 모두 이것으로 만든다.
    """

    def __init__(self, name: str, max_workers: int, *, thread_name_prefix: Optional[str] = None,
                 registry: PerfRegistry = perf) -> None:
        super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix or name)
        self.perf_name = name
        self._registry = registry
        self._busy = 0
        self._busy_lock = threading.Lock()
        registry.gauge(f"pool.{name}", self.gauges)

    def gauges(self) -> Dict[str, int]:
        """지금 일하는 스레드 수·풀 크기·큐에서 스레드를 기다리는 작업 수다."""
        return {"busy": self._busy, "max": self._max_workers, "queued": self._work_queue.qsize()}

    def submit(self, fn, /, *args, **kwargs):
        """작업을 감싸 넣는다. 넣은 시각과 스레드가 잡은 시각의 차이가 대기, 그 뒤 끝날 때까지가 실행이다."""
        enqueued = time.monotonic()
        registry, name = self._registry, self.perf_name

        def _run():
            started = time.monotonic()
            registry.observe(f"pool.{name}.wait_ms", (started - enqueued) * 1000)
            with self._busy_lock:
                self._busy += 1
            try:
                return fn(*args, **kwargs)
            finally:
                with self._busy_lock:
                    self._busy -= 1
                registry.observe(f"pool.{name}.run_ms", (time.monotonic() - started) * 1000)

        return super().submit(_run)


# 기본 풀을 설정한 동시성에 맞출 때 그 위에 더 얹는 여유 스레드 수다. self-report·번들 상태 조회·메트릭 수집처럼
# 같은 풀을 잠깐 쓰는 짧은 호출이 오래 걸리는 적재 작업 뒤에 줄 서지 않게 한다.
_POOL_HEADROOM = 8


def default_pool_workers(role: str, settings=None) -> int:
    """역할과 설정에 맞는 asyncio 기본 스레드 풀 크기를 정한다.

    asyncio 기본값 min(32, CPU+4) 는 CPU 수에만 묶여 있어 설정한 동시성과 무관하다. 그런데 executor 는 task
    의 소스 읽기와 GP 적재를, coordinator 는 Phase 2 GP INSERT 와 local 모드 task 를 모두 이 풀에서 돌리므로,
    풀이 세마포어보다 작으면 슬롯을 잡은 task 가 스레드를 얻지 못해 줄을 서고 실제 동시성은 풀 크기에서 멈춘다
    (예: 4코어 executor 는 max_concurrent_tasks=24 여도 8개만 돈다). 그래서 기본값과 "필요한 동시 블로킹 호출 수 +
    여유" 중 큰 값을 쓴다. 동시성이 무제한(0)이면 상한을 알 수 없으므로 기본값을 그대로 둔다.
    """
    base = min(32, (os.cpu_count() or 1) + 4)
    if settings is None:
        return base
    if role == "executor":
        need = int(getattr(settings, "executor_max_concurrent_tasks", 0) or 0)
    else:
        # Phase 2 적재는 실행 중인 job 마다 하나씩 돈다. local 모드면 task 도 같은 풀에서 돌며 디스패치 상한이 그 수를 묶는다.
        need = int(getattr(settings, "max_concurrent_jobs", 0) or 0)
        if need > 0 and getattr(settings, "executor_mode", "remote") == "local":
            need += int(getattr(settings, "max_dispatch_concurrency", 0) or 0)
    return max(base, need + _POOL_HEADROOM) if need > 0 else base


def install_default_executor(loop: Optional[asyncio.AbstractEventLoop] = None,
                             workers: Optional[int] = None) -> None:
    """실행 중인 루프의 기본 스레드 풀을 계측용 풀로 바꾼다. 크기를 주지 않으면 asyncio 기본과 같은 min(32, CPU+4) 다.

    ``run_in_executor(None, …)`` 과 ``asyncio.to_thread`` 를 쓰는 모든 호출이 이 풀을 거치므로, 호출부를
    하나도 고치지 않고 기본 풀의 포화를 잴 수 있다. 이미 계측용이면 그대로 둔다.
    """
    loop = loop or asyncio.get_running_loop()
    if isinstance(getattr(loop, "_default_executor", None), InstrumentedThreadPoolExecutor):
        return
    workers = workers or default_pool_workers("")
    loop.set_default_executor(
        InstrumentedThreadPoolExecutor("default", workers, thread_name_prefix="asyncio")
    )
    logger.info("기본 스레드 풀 크기=%d", workers)


def _anyio_gauges() -> Optional[Dict[str, float]]:
    """FastAPI 동기 엔드포인트가 쓰는 anyio 스레드 limiter 의 대여 수와 한도다(루프 안에서만 읽힌다)."""
    try:
        import anyio.to_thread

        limiter = anyio.to_thread.current_default_thread_limiter()
    except Exception:
        return None
    return {"busy": int(limiter.borrowed_tokens), "max": int(limiter.total_tokens)}


# ───────── 프로세스·시스템 ─────────

class SystemSampler:
    """프로세스와 호스트 자원을 게이지로 내보낸다. 누적 카운터(네트워크·디스크 I/O)는 직전 기록과의 차이를 초로 나눈다.

    psutil 의 CPU 사용률은 ``interval=None`` 으로 읽어 블로킹하지 않는다(직전 호출 이후 평균). 그래서
    생성할 때 한 번 불러 기준점을 잡아 둔다.
    """

    def __init__(self, disk_path: str = "/") -> None:
        import psutil

        self._psutil = psutil
        self._proc = psutil.Process()
        self._disk_path = disk_path
        self._proc.cpu_percent(None)
        psutil.cpu_percent(None)
        self._last = time.monotonic()
        self._net = self._safe(psutil.net_io_counters)
        self._disk = self._safe(psutil.disk_io_counters)

    @staticmethod
    def _safe(fn):
        try:
            return fn()
        except Exception:
            return None

    def gauges(self) -> Dict[str, float]:
        """자원 값을 읽는다. I/O 속도는 직전 호출 이후의 평균이므로 이 함수는 지표 로그 한 곳에서만 부른다.

        읽을 수 없는 항목(컨테이너에서 막힌 fd 수·디스크 I/O 등)은 줄에서 빼고 나머지는 그대로 낸다.
        """
        ps = self._psutil
        now = time.monotonic()
        dt = max(1e-6, now - self._last)
        out: Dict[str, float] = {
            "process.cpu_pct": self._proc.cpu_percent(None),
            "process.rss_mb": round(self._proc.memory_info().rss / 1048576, 1),
            "process.threads": self._proc.num_threads(),
            "system.cpu_pct": ps.cpu_percent(None),
            "system.mem_pct": ps.virtual_memory().percent,
        }
        fds = self._safe(self._proc.num_fds)
        if fds is not None:
            out["process.fds"] = fds
        disk = self._safe(lambda: ps.disk_usage(self._disk_path))
        if disk is not None:
            out["system.disk_pct"] = disk.percent
        net = self._safe(ps.net_io_counters)
        if net is not None and self._net is not None:
            out["system.net_rx_mbps"] = round((net.bytes_recv - self._net.bytes_recv) / dt / 1048576, 2)
            out["system.net_tx_mbps"] = round((net.bytes_sent - self._net.bytes_sent) / dt / 1048576, 2)
        dio = self._safe(ps.disk_io_counters)
        if dio is not None and self._disk is not None:
            out["system.disk_read_mbps"] = round((dio.read_bytes - self._disk.read_bytes) / dt / 1048576, 2)
            out["system.disk_write_mbps"] = round((dio.write_bytes - self._disk.write_bytes) / dt / 1048576, 2)
        self._net, self._disk, self._last = net, dio, now
        return out


# ───────── 단계(phase) 해석 ─────────

def stage_durations(phases: list) -> Dict[str, int]:
    """task 의 단계 타임라인에서 단계 이름별 소요(ms) 합을 구한다(같은 단계가 여러 번이면 더한다)."""
    out: Dict[str, int] = {}
    for p in phases or []:
        name, dur = p.get("name"), p.get("duration_ms")
        if name and isinstance(dur, (int, float)):
            out[name] = out.get(name, 0) + int(dur)
    return out


def copy_metrics(phases: list) -> Dict[str, int]:
    """읽기·쓰기 단계가 남긴 병목 지표(read_wait·write_wait·read_starve·finalize_wait, ms)를 모은다."""
    out: Dict[str, int] = {}
    for p in phases or []:
        extra = p.get("extra") or {}
        for key in ("read_wait_ms", "write_wait_ms", "read_starve_ms", "finalize_wait_ms"):
            v = extra.get(key)
            if isinstance(v, (int, float)):
                out[key] = out.get(key, 0) + int(v)
    return out


def bottleneck(metrics: Dict[str, int]) -> str:
    """읽기와 쓰기 중 어느 쪽이 시간을 더 먹었는지 판정한다: source·target·mixed·-(지표 없음).

    파이프라인 모드에서는 라이터가 다음 배치를 기다린 시간(read_starve)이 곧 소스를 기다린 시간이고,
    아니면 읽기 누적(read_wait)이 그 몫이다. 쓰기 쪽은 쓰기 누적(write_wait)에 COPY 종료 후 서버 반영
    대기(finalize_wait)를 더한다. 한쪽이 다른 쪽의 1.5배를 넘으면 그쪽을 병목으로 본다.
    """
    if not metrics:
        return "-"
    source = metrics.get("read_starve_ms") or metrics.get("read_wait_ms") or 0
    target = (metrics.get("write_wait_ms") or 0) + (metrics.get("finalize_wait_ms") or 0)
    if source <= 0 and target <= 0:
        return "-"
    if source > 1.5 * target:
        return "source"
    if target > 1.5 * source:
        return "target"
    return "mixed"


def slowest_stage(durations: Dict[str, int], *, skip=("QUEUE_WAIT",)) -> tuple:
    """대기 단계를 뺀 단계 중 가장 오래 걸린 (이름, ms) 를 돌려준다. 없으면 ("-", 0)."""
    items = [(n, d) for n, d in durations.items() if n not in skip]
    if not items:
        return "-", 0
    return max(items, key=lambda kv: kv[1])


def phases_span_s(phases: list) -> Optional[float]:
    """단계 타임라인의 첫 시작부터 마지막 종료까지의 길이(초)다. 시각이 없으면 None."""
    from datetime import datetime

    starts, ends = [], []
    for p in phases or []:
        for key, bucket in (("started_at", starts), ("finished_at", ends)):
            v = p.get(key)
            if v:
                try:
                    bucket.append(datetime.fromisoformat(v))
                except (TypeError, ValueError):
                    pass
    if not starts or not ends:
        return None
    return max(0.0, (max(ends) - min(starts)).total_seconds())


# ───────── 리포터 ─────────

class PerfReporter:
    """주기마다 지표 한 줄을 쓰고, 그 사이 이벤트 루프 지연을 재는 백그라운드 작업이다.

    루프 지연은 ``_LAG_TICK`` 마다 잠들었다 깨어나 예정보다 늦게 깬 시간으로 잰다. 루프가 블로킹 호출이나
    밀린 콜백 때문에 바쁘면 이 값이 커지므로, 정지 감시(loopwatch)가 잡지 못하는 수십~수백 ms 의 잦은
    지연까지 분포로 드러난다.
    """

    _LAG_TICK = 0.1

    def __init__(self, role: str, interval_s: Optional[float], registry: PerfRegistry = perf,
                 out: logging.Logger = perf_logger) -> None:
        self.role = role
        self.interval_s = interval_s
        self.registry = registry
        self.out = out
        self._tasks: list = []
        self._last = time.monotonic()

    def write_snapshot(self) -> Dict[str, object]:
        """구간 지표를 꺼내 ``kind=snapshot`` 한 줄로 남기고 그 값을 돌려준다(카운터·분포는 여기서 비워진다).

        ``window_s`` 는 직전 줄 이후 실제로 흐른 시간이라, 루프가 밀려 주기가 늘어진 구간도 비율을 바로 낼 수 있다.
        """
        now = time.monotonic()
        fields: Dict[str, object] = {
            "kind": "snapshot", "role": self.role, "window_s": round(now - self._last, 1),
        }
        self._last = now
        fields.update(self.registry.collect())
        self.out.info(format_kv(fields))
        return fields

    async def _report_loop(self) -> None:
        """주기마다 한 줄을 남긴다. 기록이 실패해도 루프는 이어 가 다음 구간을 잃지 않는다."""
        while True:
            await asyncio.sleep(self.interval_s)
            try:
                self.write_snapshot()
            except Exception:
                logger.exception("perf 지표 기록 실패")

    async def _lag_loop(self) -> None:
        """짧게 잠들었다 깨기를 반복하며 예정보다 늦게 깬 만큼을 ``loop.lag_ms`` 분포에 더한다."""
        loop = asyncio.get_running_loop()
        while True:
            t0 = loop.time()
            await asyncio.sleep(self._LAG_TICK)
            self.registry.observe("loop.lag_ms", max(0.0, (loop.time() - t0 - self._LAG_TICK) * 1000))

    def start(self) -> None:
        self._last = time.monotonic()
        # 주기가 없으면(지표 로그는 끄고 Prometheus 만 켠 경우) 줄은 쓰지 않고 루프 지연만 잰다.
        self._tasks = [asyncio.create_task(self._lag_loop())]
        if self.interval_s:
            self._tasks.append(asyncio.create_task(self._report_loop()))

    async def stop(self) -> None:
        """띄운 루프들을 취소하고 끝날 때까지 기다린다."""
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks = []


def start_perf_reporter(role: str, settings) -> Optional[PerfReporter]:
    """기본 스레드 풀을 설정한 동시성에 맞는 계측용 풀로 바꾸고, 지표 로그나 Prometheus 가 켜져 있으면 공통
    게이지를 등록한 뒤 리포터를 띄운다.

    루프 안(lifespan)에서 불러야 한다. 지표 로그만 꺼져 있으면 줄은 쓰지 않고 Prometheus 가 읽을 루프 지연만
    잰다. 둘 다 꺼져 있으면 아무것도 하지 않고 None 을 돌려준다.
    """
    # 기본 풀 크기는 실제 동시성 상한이므로 지표를 끄더라도 설정에 맞춰 키운다.
    install_default_executor(workers=default_pool_workers(role, settings))
    interval = float(getattr(settings, "log_perf_interval_s", 30) or 0)
    log_on = bool(getattr(settings, "log_perf_enabled", False)) and interval > 0
    if not log_on and not getattr(settings, "prometheus_enabled", False):
        return None
    perf.gauge("pool.anyio", _anyio_gauges)
    if log_on:
        try:
            sampler = SystemSampler(getattr(settings, "monitor_disk_path", "/") or "/")
            perf.gauge("system", sampler.gauges, prefix=False)  # process.* · system.* 키를 그대로 쓴다
        except Exception:
            logger.warning("perf 시스템 지표 수집을 켜지 못함(psutil)", exc_info=True)
    reporter = PerfReporter(role, interval if log_on else None)
    reporter.start()
    return reporter


def job_summary_enabled(settings) -> bool:
    """job·task 종료 요약 줄을 남길지 여부다."""
    return bool(getattr(settings, "log_perf_enabled", False)) and bool(
        getattr(settings, "log_perf_job_summary", True)
    )
