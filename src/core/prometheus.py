"""성능 지표 레지스트리(:mod:`core.perf`)를 Prometheus 텍스트 형식으로 내보내는 exporter 다.

coordinator 와 executor 는 이미 실행 슬롯·디스패치·스레드 풀·GP 커넥션 풀·루프 지연 같은 튜닝 지표를
``perf`` 레지스트리에 흘려 넣고 있다. 이 모듈은 그 값을 다시 재지 않고 그대로 읽어
``GET /metrics/prometheus`` 응답으로 바꾼다. 그래서 지표 로그와 Prometheus 가 언제나 같은 측정점을 본다.
``/metrics`` 는 대시보드와 coordinator 헬스 모니터가 읽는 JSON 응답이라 경로를 따로 둔다.

이름은 레지스트리 키에서 기계적으로 만든다. 키의 점(``.``)은 밑줄로 바꾸고 앞에 ``dqe_`` 를 붙이며, 키
안에 든 가변 부분(executor 주소, 풀 이름, 단계 이름, 종료 상태, 오류 종류)은 이름이 아니라 레이블로 뺀다.
이름에 넣으면 executor 하나, 상태 하나마다 지표 이름이 따로 생겨 PromQL 로 묶을 수 없기 때문이다. 시간
지표(``_ms``·``_s``)는 Prometheus 관례대로 초로 바꿔 ``_seconds`` 로 내보낸다.

값의 종류는 이렇게 대응한다. 레지스트리 게이지는 gauge, 카운터는 누적 합을 ``_total`` 이 붙은 counter 로,
분포는 누적 히스토그램(``_bucket``·``_sum``·``_count``)으로 내보낸다. 지표 로그의 p50·p95 는 구간마다
새로 세는 값이라 내보내지 않는다. 백분위는 Prometheus 쪽에서 ``histogram_quantile()`` 로 구한다.

프로세스·호스트 자원은 레지스트리의 ``system`` 게이지를 쓰지 않고 여기서 따로 읽는다. 그 게이지는 직전
호출과의 차이로 I/O 속도를 내므로 scrape 가 부르면 지표 로그의 구간이 흐트러지고, Prometheus 에는 속도보다
누적 바이트를 주는 편이 ``rate()`` 와 맞기 때문이다. 모두 메모리와 psutil 값만 읽으므로 scrape 가 DB 나
네트워크 부하를 더하지 않는다.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit

from core.perf import PerfRegistry, perf, unit_scale

logger = logging.getLogger(__name__)

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
METRICS_PATH = "/metrics/prometheus"
PREFIX = "dqe_"

Labels = Tuple[Tuple[str, str], ...]
# (지표 이름, 종류, 설명, [(레이블, 값)]) 묶음이다. 앱이 레지스트리 밖의 값을 더할 때도 이 모양으로 넘긴다.
Family = Tuple[str, str, str, List[Tuple[Dict[str, str], float]]]

# 레지스트리 키의 가변 부분을 레이블로 빼는 규칙이다. 위에서부터 처음 맞는 규칙 하나만 쓴다.
# ``rest``·``base`` 는 이름을 만들 때 쓰는 조각이고 나머지 이름 있는 그룹이 레이블이 된다.
_LABEL_RULES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"^executor\[(?P<executor>[^\]]+)\]\.(?P<rest>.+)$"), "executor.{rest}"),
    (re.compile(r"^pool\.(?P<pool>[^.]+)\.(?P<rest>.+)$"), "pool.{rest}"),
    (re.compile(r"^stage\.(?P<stage>.+)_ms$"), "task.stage_ms"),
    (re.compile(r"^copy\.(?P<metric>.+)_ms$"), "task.copy_ms"),
    (re.compile(r"^(?P<base>jobs_done|tasks_done)\.(?P<status>.+)$"), "{base}"),
    (re.compile(r"^dispatch\.(?P<base>start_errors|poll_errors)\.(?P<kind>.+)$"), "dispatch.{base}"),
]
_NAME_PIECES = ("rest", "base")
_INVALID = re.compile(r"[^a-zA-Z0-9_]")

# 주요 지표의 설명이다. 여기 없는 지표는 원래 레지스트리 키를 설명으로 쓴다.
_HELP: Dict[str, str] = {
    "dqe_admission_running": "실행 슬롯을 차지한 job 수",
    "dqe_admission_max_running": "실행 슬롯 상한(coordinator.max_concurrent_jobs)",
    "dqe_admission_pending": "실행 슬롯을 기다리는 job 수",
    "dqe_admission_max_pending": "대기 큐 상한(coordinator.max_pending_jobs)",
    "dqe_admission_rejected_total": "대기 큐가 차서 429 로 거절한 job 수",
    "dqe_admission_slot_wait_seconds": "job 이 PENDING 에서 실행 슬롯을 기다린 시간",
    "dqe_dispatch_inflight": "executor 에 띄워 둔 task 수",
    "dqe_dispatch_max": "동시 디스패치 상한(coordinator.max_dispatch_concurrency)",
    "dqe_dispatch_waiting": "디스패치 한도에 막혀 기다리는 task 수",
    "dqe_dispatch_phase2_running": "Phase 2 적재 중인 job 수",
    "dqe_dispatch_slot_wait_seconds": "task 가 디스패치 한도 앞에서 기다린 시간",
    "dqe_dispatch_tasks_started_total": "executor 에 시작시킨 task 수",
    "dqe_dispatch_start_errors_total": "task 시작 요청 실패 수(종류별)",
    "dqe_dispatch_poll_errors_total": "task 상태 조회 실패 수(종류별)",
    "dqe_dispatch_failover_total": "다른 executor 로 넘긴 task 수",
    "dqe_executor_inflight": "executor 별로 coordinator 가 맡긴 task 수",
    "dqe_executor_poll_rtt_seconds": "executor 별 task 상태 조회 왕복 시간",
    "dqe_phase2_load_seconds": "local_stage·s3_stage Phase 2 적재 소요",
    "dqe_job_elapsed_seconds": "끝난 job 의 전체 소요",
    "dqe_jobs_done_total": "종료 상태별로 끝난 job 수",
    "dqe_tasks_running": "실행 슬롯을 차지한 task 수",
    "dqe_tasks_queued": "실행 슬롯을 기다리는 task 수",
    "dqe_tasks_max": "task 실행 상한(executor.max_concurrent_tasks, 0=무제한)",
    "dqe_tasks_in_memory": "메모리에 보관 중인 task 수",
    "dqe_tasks_done_total": "종료 상태별로 끝난 task 수",
    "dqe_task_elapsed_seconds": "끝난 task 의 전체 소요",
    "dqe_task_stage_seconds": "끝난 task 의 단계별 소요",
    "dqe_task_copy_seconds": "끝난 task 의 소스 대기(read_*)·GP 쓰기 대기(write_*·finalize_*) 시간",
    "dqe_query_run_seconds": "query-run(미리보기) 한 건의 소요",
    "dqe_rows_read_total": "소스에서 읽은 행 수",
    "dqe_rows_written_total": "대상에 적재한 행 수",
    "dqe_gp_pool_in_use": "사용 중인 GP 커넥션 수",
    "dqe_gp_pool_idle": "풀에서 쉬고 있는 GP 커넥션 수",
    "dqe_gp_pool_max": "GP 커넥션 풀 상한",
    "dqe_gp_pool_acquire_wait_seconds": "GP 커넥션을 얻기까지 기다린 시간",
    "dqe_gp_pool_connect_seconds": "GP 새 연결을 여는 데 걸린 시간",
    "dqe_gp_pool_connect_errors_total": "GP 연결 실패 수",
    "dqe_gp_pool_reset_failures_total": "GP 세션 초기화(DISCARD ALL) 실패 수",
    "dqe_pool_busy": "스레드 풀에서 작업 중인 스레드 수",
    "dqe_pool_max": "스레드 풀 크기",
    "dqe_pool_queued": "스레드 풀 큐에서 기다리는 작업 수",
    "dqe_pool_wait_seconds": "작업이 스레드 풀 큐에서 기다린 시간",
    "dqe_pool_run_seconds": "작업이 스레드 풀에서 실행된 시간",
    "dqe_meta_db_connect_seconds": "메타 저장소 연결을 여는 데 걸린 시간",
    "dqe_meta_db_connect_errors_total": "메타 저장소 연결 실패 수",
    "dqe_meta_db_connect_timeouts_total": "메타 저장소 연결이 대기 상한에 걸린 수",
    "dqe_loop_lag_seconds": "이벤트 루프가 예정보다 늦게 깬 시간",
    "dqe_loop_stalls_total": "이벤트 루프 정지 감지 수(logging.loop_stall_ms 초과)",
}


def metric_name(key: str, *, counter: bool = False) -> Tuple[str, Dict[str, str], float]:
    """레지스트리 키를 (Prometheus 이름, 레이블, 값 배율)로 바꾼다.

    시간 지표는 이름 끝을 ``_seconds`` 로 바꾸고 배율로 초 단위를 맞춘다. 카운터면 ``_total`` 을 붙인다.
    """
    labels: Dict[str, str] = {}
    base = key
    for pattern, template in _LABEL_RULES:
        m = pattern.match(key)
        if m:
            groups = m.groupdict()
            base = template.format(**{k: groups[k] for k in _NAME_PIECES if k in groups})
            labels = {k: v for k, v in groups.items() if k not in _NAME_PIECES}
            break
    scale = unit_scale(base)
    if scale is not None:
        # 이름 끝의 단위 표기(_ms·.ms·_s)를 떼고 _seconds 를 붙인다.
        for suffix in ("_ms", ".ms", "_s"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
                break
        base += "_seconds"
    elif base.endswith("_pct"):
        base = base[:-4] + "_percent"
    name = PREFIX + _INVALID.sub("_", base.replace(".", "_"))
    if counter and not name.endswith("_total"):
        name += "_total"
    return name, labels, scale or 1.0


# ───────── 텍스트 형식 ─────────

def _escape_label(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _escape_help(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\n", "\\n")


def format_number(value: float) -> str:
    """값 하나를 Prometheus 숫자 표기로 바꾼다(정수는 소수점 없이, 무한대·NaN 은 규격 표기로)."""
    if isinstance(value, bool):
        return "1" if value else "0"
    v = float(value)
    if v != v:
        return "NaN"
    if v in (float("inf"), float("-inf")):
        return "+Inf" if v > 0 else "-Inf"
    if v == int(v) and abs(v) < 1e15:
        return str(int(v))
    return repr(v)


def _label_text(labels: Dict[str, str]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{_escape_label(v)}"' for k, v in sorted(labels.items()))
    return "{" + inner + "}"


class _Families:
    """이름별로 지표 묶음(종류·설명·표본)을 모은다. 같은 이름에 다른 종류가 오면 뒤의 것을 버린다."""

    def __init__(self) -> None:
        self._items: Dict[str, dict] = {}

    def add(self, name: str, kind: str, help_text: str, labels: Dict[str, str], value) -> None:
        """표본 하나를 이름의 묶음에 더한다. 설명과 종류는 그 이름으로 처음 들어온 것을 쓴다."""
        fam = self._items.get(name)
        if fam is None:
            fam = self._items[name] = {"kind": kind, "help": help_text, "samples": []}
        elif fam["kind"] != kind:
            logger.debug("prometheus 지표 %s 의 종류 충돌(%s/%s) — 버림", name, fam["kind"], kind)
            return
        fam["samples"].append((labels, value))

    def render(self) -> str:
        """Prometheus 텍스트 형식으로 직렬화한다.

        이름과 레이블을 정렬해 내보내므로 같은 상태면 늘 같은 본문이 나와 테스트와 비교가 쉽다. 형식상
        ``# HELP``·``# TYPE`` 은 이름마다 한 번만 와야 하므로 묶음 단위로 쓴다.
        """
        lines: List[str] = []
        for name in sorted(self._items):
            fam = self._items[name]
            lines.append(f"# HELP {name} {_escape_help(fam['help'])}")
            lines.append(f"# TYPE {name} {fam['kind']}")
            for labels, value in sorted(fam["samples"], key=lambda s: sorted(s[0].items())):
                if fam["kind"] == "histogram":
                    lines.extend(_histogram_lines(name, labels, value))
                else:
                    lines.append(f"{name}{_label_text(labels)} {format_number(value)}")
        return "\n".join(lines) + "\n"


def _histogram_lines(name: str, labels: Dict[str, str], hist: dict) -> Iterable[str]:
    """누적 히스토그램 하나를 ``_bucket``·``_sum``·``_count`` 줄로 펼친다.

    레지스트리의 버킷은 이미 누적 개수이고 경계는 원래 단위(ms 등)라, ``scale`` 을 곱해 초 단위 경계(``le``)와
    합으로 바꾼다. 마지막 ``+Inf`` 버킷은 전체 개수와 같아야 하므로 ``count`` 를 그대로 쓴다.
    """
    scale = hist["scale"]
    for bound, count in hist["buckets"]:
        le = format_number(round(bound * scale, 6))
        yield f"{name}_bucket{_label_text({**labels, 'le': le})} {count}"
    yield f"{name}_bucket{_label_text({**labels, 'le': '+Inf'})} {hist['count']}"
    yield f"{name}_sum{_label_text(labels)} {format_number(hist['sum'] * scale)}"
    yield f"{name}_count{_label_text(labels)} {hist['count']}"


# ───────── 프로세스·호스트 ─────────

def _process_families(fams: _Families) -> None:
    """Prometheus 표준 이름(process_*)으로 이 프로세스의 CPU·메모리·fd·스레드·기동 시각을 더한다."""
    try:
        import psutil

        proc = psutil.Process()
        cpu = proc.cpu_times()
        mem = proc.memory_info()
        fams.add("process_cpu_seconds_total", "counter", "프로세스가 쓴 CPU 시간(user+system)", {},
                 cpu.user + cpu.system)
        fams.add("process_resident_memory_bytes", "gauge", "프로세스 상주 메모리", {}, mem.rss)
        fams.add("process_virtual_memory_bytes", "gauge", "프로세스 가상 메모리", {}, mem.vms)
        fams.add("process_start_time_seconds", "gauge", "프로세스 기동 시각(epoch 초)", {}, proc.create_time())
        fams.add("process_threads", "gauge", "프로세스 스레드 수", {}, proc.num_threads())
        try:
            fams.add("process_open_fds", "gauge", "열린 파일 디스크립터 수", {}, proc.num_fds())
        except Exception:  # Windows 등 fd 개념이 없는 플랫폼
            pass
    except Exception:
        logger.debug("prometheus 프로세스 지표 읽기 실패", exc_info=True)


def _system_families(fams: _Families, disk_path: str) -> None:
    """호스트 CPU 시간·메모리·디스크 용량과 네트워크·디스크 누적 바이트를 더한다.

    CPU 는 사용률이 아니라 모드별 누적 시간으로 준다. psutil 의 사용률은 직전 호출과의 차이로 계산되고 그
    기준점이 프로세스 전역이라, scrape 가 부르면 다른 곳(헬스 응답·지표 로그)의 값이 함께 흐트러진다.
    """
    try:
        import psutil
    except Exception:
        return
    try:
        cpu = psutil.cpu_times()
        for mode in ("user", "system", "idle", "iowait", "irq", "softirq", "steal", "nice"):
            v = getattr(cpu, mode, None)
            if v is not None:
                fams.add(f"{PREFIX}system_cpu_seconds_total", "counter", "호스트 CPU 모드별 누적 시간",
                         {"mode": mode}, v)
        fams.add(f"{PREFIX}system_cpus", "gauge", "호스트 논리 CPU 수", {}, psutil.cpu_count() or 0)
        vm = psutil.virtual_memory()
        fams.add(f"{PREFIX}system_memory_total_bytes", "gauge", "호스트 전체 메모리", {}, vm.total)
        fams.add(f"{PREFIX}system_memory_available_bytes", "gauge", "호스트 가용 메모리", {}, vm.available)
        disk = psutil.disk_usage(disk_path)
        fams.add(f"{PREFIX}system_disk_total_bytes", "gauge", "디스크 용량(monitor.disk_path)",
                 {"path": disk_path}, disk.total)
        fams.add(f"{PREFIX}system_disk_used_bytes", "gauge", "디스크 사용량(monitor.disk_path)",
                 {"path": disk_path}, disk.used)
    except Exception:
        logger.debug("prometheus 시스템 지표 읽기 실패", exc_info=True)
    for fn, pairs in (
        (psutil.net_io_counters, (("bytes_recv", "network_receive_bytes_total", "네트워크 수신 누적 바이트"),
                                  ("bytes_sent", "network_transmit_bytes_total", "네트워크 송신 누적 바이트"))),
        (psutil.disk_io_counters, (("read_bytes", "disk_read_bytes_total", "디스크 읽기 누적 바이트"),
                                   ("write_bytes", "disk_written_bytes_total", "디스크 쓰기 누적 바이트"))),
    ):
        try:
            counters = fn()
        except Exception:
            counters = None
        if counters is None:
            continue
        for attr, name, help_text in pairs:
            fams.add(f"{PREFIX}system_{name}", "counter", help_text, {}, getattr(counters, attr))


# ───────── 조립 ─────────

def render(role: str, version: str, *, registry: PerfRegistry = perf, disk_path: str = "/",
           extra: Optional[Callable[[], Iterable[Family]]] = None) -> str:
    """레지스트리·프로세스·호스트 지표와 앱이 더한 값을 Prometheus 텍스트 형식 한 덩어리로 만든다."""
    fams = _Families()
    fams.add(f"{PREFIX}info", "gauge", "서비스 역할과 버전(값은 항상 1)", {"role": role, "version": version}, 1)
    fams.add(f"{PREFIX}scrape_timestamp_seconds", "gauge", "이 응답을 만든 시각(epoch 초)", {}, time.time())

    # 직전 호출과의 차이로 값을 내는 system 게이지는 지표 로그 몫이라 부르지 않는다(위 모듈 설명 참고).
    for key, value in registry.read_gauges(exclude=("system",)).items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        name, labels, scale = metric_name(key)
        fams.add(name, "gauge", _HELP.get(name, key), labels, value * scale)
    for key, value in registry.totals().items():
        name, labels, _ = metric_name(key, counter=True)
        fams.add(name, "counter", _HELP.get(name, key), labels, value)
    for key, hist in registry.histograms().items():
        name, labels, scale = metric_name(key)
        fams.add(name, "histogram", _HELP.get(name, key), labels, {**hist, "scale": scale})

    _process_families(fams)
    _system_families(fams, disk_path)
    if extra is not None:
        try:
            for name, kind, help_text, samples in extra():
                for labels, value in samples:
                    fams.add(name, kind, help_text, labels, value)
        except Exception:
            logger.debug("prometheus 추가 지표 읽기 실패", exc_info=True)
    return fams.render()


def http_sd_targets(executor_urls: Iterable[str]) -> List[dict]:
    """executor 목록을 Prometheus ``http_sd_configs`` 응답 형식으로 바꾼다.

    URL 의 스킴과 exporter 경로를 ``__scheme__``·``__metrics_path__`` 로 실어 주므로, Prometheus 쪽 설정에
    경로를 따로 적지 않아도 executor 의 exporter 를 긁는다.
    """
    groups: Dict[str, List[str]] = {}
    for url in executor_urls:
        parts = urlsplit(url if "://" in url else f"http://{url}")
        if not parts.netloc:
            continue
        groups.setdefault(parts.scheme or "http", []).append(parts.netloc)
    return [
        {"targets": sorted(set(targets)),
         "labels": {"role": "executor", "__scheme__": scheme, "__metrics_path__": METRICS_PATH}}
        for scheme, targets in sorted(groups.items())
    ]


def install_prometheus(app, role: str, settings, *, extra: Optional[Callable[[], Iterable[Family]]] = None) -> bool:
    """설정이 켜져 있으면 ``GET /metrics/prometheus`` 를 등록한다. 등록했으면 True 다.

    핸들러는 async 로 둔다. anyio 스레드 limiter 게이지가 이벤트 루프 안에서만 읽히고, 읽는 값이 모두
    메모리와 psutil 이라 루프를 오래 막지 않기 때문이다.
    """
    if not getattr(settings, "prometheus_enabled", False):
        return False
    from fastapi.responses import Response

    from core.version import __version__

    disk_path = getattr(settings, "monitor_disk_path", "/") or "/"

    @app.get(METRICS_PATH, tags=["Monitoring"], summary="Prometheus 지표(텍스트 형식)",
             response_class=Response)
    async def prometheus_metrics():
        body = render(role, __version__, disk_path=disk_path, extra=extra)
        return Response(content=body, media_type=CONTENT_TYPE)

    return True
