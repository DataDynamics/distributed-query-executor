"""Prometheus exporter(core.prometheus)와 레지스트리 누적값을 검증한다.

prometheus_client 는 의존성이 아니므로(에어갭) 텍스트 형식은 여기서 규격대로 직접 파싱해 확인한다.
"""

from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

from core import perf as perfmod
from core import prometheus as prom
from core.perf import PerfRegistry

_SAMPLE = re.compile(
    r'^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})? (?P<value>[-+]?(?:[0-9.eE+-]+|Inf|NaN))$'
)
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_exposition(text: str) -> dict:
    """텍스트 형식을 {이름: {"type", "help", "samples": [(이름, 레이블, 값)]}} 로 파싱하며 규격을 검사한다."""
    assert text.endswith("\n")
    fams: dict = {}
    current = None
    for line in text.splitlines():
        if line.startswith("# HELP "):
            name = line.split(" ", 3)[2]
            assert name not in fams, f"중복 지표 {name}"
            current = fams[name] = {"help": line.split(" ", 3)[3], "type": None, "samples": []}
            continue
        if line.startswith("# TYPE "):
            _, _, name, kind = line.split(" ")
            assert current is fams[name] and kind in ("gauge", "counter", "histogram")
            current["type"] = kind
            continue
        m = _SAMPLE.match(line)
        assert m, f"형식 오류: {line!r}"
        labels = dict(_LABEL.findall(m.group("labels") or ""))
        sample = m.group("name")
        base = re.sub(r"_(bucket|sum|count)$", "", sample) if current["type"] == "histogram" else sample
        assert base in fams and fams[base] is current, f"TYPE 없이 나온 표본 {sample}"
        current["samples"].append((sample, labels, float(m.group("value").replace("Inf", "inf"))))
    for name, fam in fams.items():
        if fam["type"] == "counter":
            assert name.endswith("_total"), name
        if fam["type"] == "histogram":
            _check_histogram(name, fam["samples"])
    return fams


def _check_histogram(name: str, samples: list) -> None:
    series: dict = {}
    for sample, labels, value in samples:
        key = tuple(sorted((k, v) for k, v in labels.items() if k != "le"))
        series.setdefault(key, {"buckets": []})
        if sample.endswith("_bucket"):
            series[key]["buckets"].append((float(labels["le"].replace("+Inf", "inf")), value))
        else:
            series[key][sample.rsplit("_", 1)[1]] = value
    for s in series.values():
        bounds = [b for b, _ in s["buckets"]]
        counts = [c for _, c in s["buckets"]]
        assert bounds == sorted(bounds) and bounds[-1] == float("inf"), name
        assert counts == sorted(counts), f"{name} 버킷이 누적이 아님"
        assert counts[-1] == s["count"], name


def _value(fams, name, **labels):
    family = name if name in fams else re.sub(r"_(bucket|sum|count)$", "", name)
    for sample, lb, v in fams[family]["samples"]:
        if sample == name and all(lb.get(k) == val for k, val in labels.items()):
            return v
    raise KeyError((name, labels))


# ───────── 레지스트리 누적값 ─────────

def test_누적값은_지표_로그가_비워도_남는다():
    reg = PerfRegistry()
    reg.inc("rows_written", 5)
    reg.observe("loop.lag_ms", 3)
    reg.collect()
    reg.inc("rows_written", 2)
    reg.observe("loop.lag_ms", 700)
    assert reg.collect()["rows_written"] == 2  # 지표 로그는 구간 값
    assert reg.totals()["rows_written"] == 7    # exporter 는 누적값
    h = reg.histograms()["loop.lag_ms"]
    assert h["count"] == 2 and h["sum"] == 703
    # ms 지표의 버킷 상한은 초 단위 상한을 ms 로 바꾼 것이다(5ms 버킷에 3, 1000ms 버킷에 둘 다).
    buckets = dict(h["buckets"])
    assert buckets[5.0] == 1 and buckets[1000.0] == 2


def test_게이지만_읽으면_카운터와_분포는_그대로다():
    reg = PerfRegistry()
    reg.inc("x")
    reg.gauge("a", lambda: {"b": 1})
    reg.gauge("system", lambda: {"system.cpu_pct": 1}, prefix=False)
    assert reg.read_gauges(exclude=("system",)) == {"a.b": 1}
    assert reg.collect()["x"] == 1


# ───────── 이름 변환 ─────────

@pytest.mark.parametrize("key,counter,expected", [
    ("admission.running", False, ("dqe_admission_running", {}, 1.0)),
    ("admission.slot_wait_ms", False, ("dqe_admission_slot_wait_seconds", {}, 0.001)),
    ("job.elapsed_s", False, ("dqe_job_elapsed_seconds", {}, 1.0)),
    ("query_run.ms", False, ("dqe_query_run_seconds", {}, 0.001)),
    ("rows_written", True, ("dqe_rows_written_total", {}, 1.0)),
    ("jobs_done.PARTIAL", True, ("dqe_jobs_done_total", {"status": "PARTIAL"}, 1.0)),
    ("dispatch.poll_errors.http_503", True, ("dqe_dispatch_poll_errors_total", {"kind": "http_503"}, 1.0)),
    ("executor[10.0.0.1:8087].poll_rtt_ms", False,
     ("dqe_executor_poll_rtt_seconds", {"executor": "10.0.0.1:8087"}, 0.001)),
    ("executor[10.0.0.1:8087].inflight", False, ("dqe_executor_inflight", {"executor": "10.0.0.1:8087"}, 1.0)),
    ("pool.meta.wait_ms", False, ("dqe_pool_wait_seconds", {"pool": "meta"}, 0.001)),
    ("pool.default.queued", False, ("dqe_pool_queued", {"pool": "default"}, 1.0)),
    ("stage.STREAM_COPY_ms", False, ("dqe_task_stage_seconds", {"stage": "STREAM_COPY"}, 0.001)),
    ("copy.read_starve_ms", False, ("dqe_task_copy_seconds", {"metric": "read_starve"}, 0.001)),
    ("system.cpu_pct", False, ("dqe_system_cpu_percent", {}, 1.0)),
])
def test_레지스트리_키를_이름과_레이블로_바꾼다(key, counter, expected):
    assert prom.metric_name(key, counter=counter) == expected


def test_레이블_값은_이스케이프한다():
    reg = PerfRegistry()
    reg.inc('jobs_done.a"b\\c')
    fams = parse_exposition(prom.render("coordinator", "1.0", registry=reg))
    assert _value(fams, "dqe_jobs_done_total", status='a\\"b\\\\c') == 1


# ───────── 렌더링 ─────────

def test_렌더_결과는_규격에_맞고_값을_초로_바꾼다():
    reg = PerfRegistry()
    reg.gauge("admission", lambda: {"running": 3, "max_running": 16, "label": "문자열은 뺀다"})
    reg.gauge("executor", lambda: {"executor[h:1].inflight": 2}, prefix=False)
    reg.gauge("system", lambda: {"system.net_rx_mbps": 9}, prefix=False)
    reg.inc("admission.rejected", 4)
    reg.inc("jobs_done.DONE", 2)
    reg.inc("jobs_done.FAILED")
    for ms in (2, 40, 1500):
        reg.observe("admission.slot_wait_ms", ms)
    reg.observe("job.elapsed_s", 75)

    extra = lambda: [("dqe_executor_up", "gauge", "생존", [({"executor": "h:1"}, 1), ({"executor": "h:2"}, 0)])]
    fams = parse_exposition(prom.render("coordinator", "9.9.9", registry=reg, extra=extra))

    assert _value(fams, "dqe_info", role="coordinator", version="9.9.9") == 1
    assert _value(fams, "dqe_admission_running") == 3
    assert "dqe_admission_label" not in fams
    assert _value(fams, "dqe_executor_inflight", executor="h:1") == 2
    assert "dqe_system_net_rx_mbps" not in fams  # 지표 로그용 속도 게이지는 부르지 않는다
    assert fams["dqe_admission_rejected_total"]["type"] == "counter"
    assert _value(fams, "dqe_jobs_done_total", status="DONE") == 2
    assert _value(fams, "dqe_jobs_done_total", status="FAILED") == 1
    h = fams["dqe_admission_slot_wait_seconds"]
    assert h["type"] == "histogram" and h["help"] == "job 이 PENDING 에서 실행 슬롯을 기다린 시간"
    assert _value(fams, "dqe_admission_slot_wait_seconds_sum") == pytest.approx(1.542)
    assert _value(fams, "dqe_admission_slot_wait_seconds_count") == 3
    assert _value(fams, "dqe_admission_slot_wait_seconds_bucket", le="0.05") == 2
    assert _value(fams, "dqe_admission_slot_wait_seconds_bucket", le="2.5") == 3
    assert _value(fams, "dqe_job_elapsed_seconds_bucket", le="60") == 0
    assert _value(fams, "dqe_job_elapsed_seconds_bucket", le="120") == 1
    assert _value(fams, "dqe_executor_up", executor="h:2") == 0
    # 프로세스·호스트 자원은 표준 이름과 누적값으로 나간다.
    assert fams["process_cpu_seconds_total"]["type"] == "counter"
    assert _value(fams, "process_resident_memory_bytes") > 0
    assert _value(fams, "dqe_system_cpu_seconds_total", mode="idle") > 0
    assert _value(fams, "dqe_system_disk_total_bytes", path="/") > 0


def test_추가_지표가_실패해도_나머지는_나간다():
    def boom():
        raise RuntimeError("x")

    fams = parse_exposition(prom.render("executor", "1", registry=PerfRegistry(), extra=boom))
    assert "dqe_info" in fams


def test_http_sd_대상은_스킴별로_묶는다():
    out = prom.http_sd_targets(["http://a:8087", "http://b:8087/", "https://c:9443", "d:8087"])
    assert out == [
        {"targets": ["a:8087", "b:8087", "d:8087"],
         "labels": {"role": "executor", "__scheme__": "http", "__metrics_path__": "/metrics/prometheus"}},
        {"targets": ["c:9443"],
         "labels": {"role": "executor", "__scheme__": "https", "__metrics_path__": "/metrics/prometheus"}},
    ]


# ───────── 앱 통합 ─────────

async def test_executor_는_task_지표를_내보낸다():
    import httpx

    from executor.app import create_app as create_executor_app
    from executor.backend import MockBackend
    from tests.test_exec_mode import _payload, _run_task

    app = create_executor_app(backend=MockBackend())
    st = await _run_task(app, _payload("prom-t1", "copy", sub_query="SELECT * FROM t WHERE dt IN ('1')"))
    assert st["status"] == "DONE"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
        resp = await client.get("/metrics/prometheus")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain; version=0.0.4")
    fams = parse_exposition(resp.text)
    assert _value(fams, "dqe_info", role="executor") == 1
    assert _value(fams, "dqe_tasks_done_total", status="DONE") >= 1
    assert _value(fams, "dqe_task_stage_seconds_count", stage="QUEUE_WAIT") >= 1


def test_coordinator_는_exporter_와_http_sd_를_등록한다(monkeypatch):
    from coordinator.app import create_app
    from core.config import settings

    monkeypatch.setattr(settings, "executors", ["http://10.0.0.1:8087", "http://10.0.0.2:8087"])
    with TestClient(create_app()) as client:
        fams = parse_exposition(client.get("/metrics/prometheus").text)
        assert _value(fams, "dqe_info", role="coordinator") == 1
        assert _value(fams, "dqe_admission_max_running") == settings.max_concurrent_jobs
        # 헬스 모니터가 돌면 executor 생존이 레이블별로 나간다(아직 폴링 전이거나 닿지 않으면 0).
        assert {lb["executor"] for _, lb, _ in fams["dqe_executor_up"]["samples"]} == {
            "10.0.0.1:8087", "10.0.0.2:8087",
        }
        sd = client.get("/prometheus/targets").json()
        assert sd[0]["targets"] == ["10.0.0.1:8087", "10.0.0.2:8087"]


def test_꺼져_있으면_경로를_등록하지_않는다(monkeypatch):
    from coordinator.app import create_app
    from core.config import settings

    monkeypatch.setattr(settings, "prometheus_enabled", False)
    with TestClient(create_app()) as client:
        assert client.get("/metrics/prometheus").status_code == 404
        assert client.get("/prometheus/targets").status_code == 404


def test_지표_로그를_꺼도_exporter_용_루프_지연은_잰다():
    class S:
        log_perf_enabled = False
        prometheus_enabled = True
        log_perf_interval_s = 30

    import asyncio

    async def run():
        reporter = perfmod.start_perf_reporter("executor", S())
        assert reporter is not None and reporter.interval_s is None and len(reporter._tasks) == 1
        await reporter.stop()

    asyncio.run(run())


def test_설정_도움말이_있다():
    from core.config_help import FIELD_HELP

    assert "prometheus.enabled" in FIELD_HELP
