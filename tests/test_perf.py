"""성능 지표 로그(core/perf.py)와 요약 도구(tools/perf_report.py)를 검증한다."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta

import pytest

from core import perf as perfmod
from core.perf import (
    InstrumentedThreadPoolExecutor, PerfRegistry, PerfReporter, bottleneck, copy_metrics, format_kv,
    format_value, phases_span_s, slowest_stage, stage_durations,
)
from tools import perf_report as pr


# ───────── 레지스트리 ─────────

def test_카운터는_기록할_때마다_0_으로_돌아간다():
    reg = PerfRegistry()
    reg.inc("jobs_done.DONE")
    reg.inc("jobs_done.DONE", 2)
    assert reg.collect()["jobs_done.DONE"] == 3
    assert reg.collect()["jobs_done.DONE"] == 0  # 한 번 생긴 카운터는 0 으로 계속 남는다


def test_분포는_건수와_백분위와_최대를_남기고_관측이_없으면_뺀다():
    reg = PerfRegistry()
    for v in range(1, 101):
        reg.observe("lag_ms", v)
    out = reg.collect()
    assert (out["lag_ms.n"], out["lag_ms.p50"], out["lag_ms.p95"], out["lag_ms.max"]) == (100, 50, 95, 100)
    assert not any(k.startswith("lag_ms") for k in reg.collect())


def test_분포_표본은_상한을_넘지_않고_최대는_정확하다(monkeypatch):
    monkeypatch.setattr(perfmod, "_RESERVOIR", 50)
    reg = PerfRegistry()
    for v in range(1000):
        reg.observe("x", v)
    assert len(reg._dists["x"].samples) == 50
    out = reg.collect()
    assert out["x.n"] == 1000 and out["x.max"] == 999


def test_게이지는_펼치고_실패하면_뺀다():
    reg = PerfRegistry()
    reg.gauge("admission", lambda: {"running": 3, "pending": None})
    reg.gauge("plain", lambda: 7)
    reg.gauge("broken", lambda: 1 / 0)
    reg.gauge("system", lambda: {"system.cpu_pct": 12.5}, prefix=False)
    out = reg.collect()
    assert out["admission.running"] == 3 and "admission.pending" not in out
    assert out["plain"] == 7 and out["system.cpu_pct"] == 12.5
    assert not any(k.startswith("broken") for k in out)


def test_여러_스레드에서_기록해도_잃지_않는다():
    reg = PerfRegistry()

    def work():
        for _ in range(1000):
            reg.inc("n")
            reg.observe("d", 1)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    out = reg.collect()
    assert out["n"] == 8000 and out["d.n"] == 8000


# ───────── 줄 형식 ─────────

def test_값_형식():
    assert format_value(3) == "3" and format_value(3.0) == "3" and format_value(12.345) == "12.3"
    assert format_value(0.1234) == "0.12" and format_value(0.5) == "0.5" and format_value(9.31e-08) == "0"
    assert format_value(True) == "true"
    assert format_value("DONE") == "DONE" and format_value("a b") == '"a b"' and format_value("") == '""'
    assert format_value('x="y"') == '"x=\\"y\\""'
    assert format_kv({"kind": "snapshot", "role": "executor", "n": 2}) == "kind=snapshot role=executor n=2"


def test_요약_도구는_자기가_쓴_줄을_그대로_읽는다():
    line = "2026-10-29 14:03:30 " + format_kv({
        "kind": "job_done", "job_id": "job_1", "error": 'x = "y" z', "rows": 12, "elapsed_s": 3.5,
    })
    rec = pr.parse_line(line)
    assert rec.ts == datetime(2026, 10, 29, 14, 3, 30)
    assert rec.fields == {"kind": "job_done", "job_id": "job_1", "error": 'x = "y" z', "rows": 12, "elapsed_s": 3.5}
    assert pr.parse_line("깨진 줄") is None and pr.parse_line("2026-10-29 14:03:30 a=1") is None


# ───────── 스레드 풀 계측 ─────────

def test_계측_풀은_대기와_실행을_나눠_잰다():
    reg = PerfRegistry()
    pool = InstrumentedThreadPoolExecutor("t", 1, registry=reg)
    release = threading.Event()
    first = pool.submit(release.wait, 2)
    second = pool.submit(lambda: "ok")
    time.sleep(0.15)
    gauges = reg.collect()
    assert gauges["pool.t.busy"] == 1 and gauges["pool.t.queued"] == 1 and gauges["pool.t.max"] == 1
    release.set()
    assert first.result() is True and second.result() == "ok"
    pool.shutdown()
    out = reg.collect()
    # 첫 작업의 대기는 앞선 collect 가 이미 꺼냈으므로, 이번 구간에는 첫 작업 뒤에서 기다린 두 번째 몫만 있다.
    assert out["pool.t.wait_ms.n"] == 1 and out["pool.t.wait_ms.max"] >= 100
    assert out["pool.t.run_ms.n"] == 2 and out["pool.t.busy"] == 0


async def test_기본_풀_교체는_한_번만_한다():
    loop = asyncio.get_running_loop()
    perfmod.install_default_executor(loop)
    first = loop._default_executor
    perfmod.install_default_executor(loop)
    assert loop._default_executor is first and isinstance(first, InstrumentedThreadPoolExecutor)
    assert await asyncio.to_thread(lambda: 5) == 5


# ───────── 리포터 ─────────

async def test_리포터는_주기마다_한_줄을_쓰고_루프_지연을_잰다():
    reg = PerfRegistry()
    lines = []

    class _Out:
        def info(self, msg):
            lines.append(msg)

    reg.gauge("admission", lambda: {"running": 2})
    rep = PerfReporter("coordinator", 0.3, registry=reg, out=_Out())
    rep.start()
    await asyncio.sleep(0.15)
    time.sleep(0.25)  # 루프를 막아 지연을 만든다
    await asyncio.sleep(0.2)
    await rep.stop()
    assert len(lines) == 1
    rec = pr.parse_line("2026-10-29 00:00:00 " + lines[0])
    assert rec.fields["kind"] == "snapshot" and rec.fields["role"] == "coordinator"
    assert rec.fields["admission.running"] == 2
    assert rec.fields["loop.lag_ms.max"] >= 150


async def test_꺼져_있으면_리포터를_띄우지_않는다():
    class S:
        log_perf_enabled = False

    assert perfmod.start_perf_reporter("executor", S()) is None


def test_기본_풀은_설정한_동시성보다_작아지지_않는다(monkeypatch):
    # 4코어면 asyncio 기본은 8 이라, 세마포어 24 를 줘도 8개만 돌던 문제를 막는다.
    monkeypatch.setattr(perfmod.os, "cpu_count", lambda: 4)

    class E:
        executor_max_concurrent_tasks = 24

    class C:
        max_concurrent_jobs = 24
        max_dispatch_concurrency = 32
        executor_mode = "remote"

    class L(C):
        executor_mode = "local"

    class Unlimited:
        executor_max_concurrent_tasks = 0

    assert perfmod.default_pool_workers("executor", E()) == 24 + 8
    assert perfmod.default_pool_workers("coordinator", C()) == 24 + 8
    assert perfmod.default_pool_workers("coordinator", L()) == 24 + 32 + 8
    assert perfmod.default_pool_workers("executor", Unlimited()) == 8
    # 동시성이 작으면 asyncio 기본값 아래로 줄이지 않는다.
    monkeypatch.setattr(perfmod.os, "cpu_count", lambda: 64)
    assert perfmod.default_pool_workers("executor", E()) == 32


async def test_지표가_꺼져_있어도_기본_풀은_설정에_맞춘다(monkeypatch):
    monkeypatch.setattr(perfmod.os, "cpu_count", lambda: 4)

    class S:
        log_perf_enabled = False
        executor_max_concurrent_tasks = 24

    loop = asyncio.get_running_loop()
    perfmod.start_perf_reporter("executor", S())
    assert loop._default_executor._max_workers == 32


def test_종료_요약은_지표_로그가_꺼져_있으면_버린다(caplog):
    perfmod.perf_logger.setLevel(logging.WARNING)
    with caplog.at_level(logging.INFO, logger=perfmod.PERF_LOGGER_NAME):
        perfmod.perf_logger.setLevel(logging.WARNING)
        perfmod.emit_event("job_done", {"job_id": "x"})
    assert not caplog.records


# ───────── 단계 해석 ─────────

_PHASES = [
    {"name": "QUEUE_WAIT", "started_at": "2026-10-29T10:00:00", "finished_at": "2026-10-29T10:00:05",
     "duration_ms": 5000},
    {"name": "IMPALA_SUBMIT", "started_at": "2026-10-29T10:00:05", "finished_at": "2026-10-29T10:00:07",
     "duration_ms": 2000},
    {"name": "STREAM_COPY", "started_at": "2026-10-29T10:00:07", "finished_at": "2026-10-29T10:01:07",
     "duration_ms": 60000,
     "extra": {"read_wait_ms": 50000, "write_wait_ms": 8000, "read_starve_ms": 45000, "finalize_wait_ms": 1000}},
    {"name": "COMMIT", "started_at": "2026-10-29T10:01:07", "finished_at": None, "duration_ms": None},
]


def test_단계_소요와_병목_지표를_뽑는다():
    assert stage_durations(_PHASES) == {"QUEUE_WAIT": 5000, "IMPALA_SUBMIT": 2000, "STREAM_COPY": 60000}
    assert copy_metrics(_PHASES) == {
        "read_wait_ms": 50000, "write_wait_ms": 8000, "read_starve_ms": 45000, "finalize_wait_ms": 1000,
    }
    assert slowest_stage(stage_durations(_PHASES)) == ("STREAM_COPY", 60000)
    assert slowest_stage({"QUEUE_WAIT": 9}) == ("-", 0)
    assert phases_span_s(_PHASES) == 67.0 and phases_span_s([]) is None


@pytest.mark.parametrize("metrics,expected", [
    ({"read_starve_ms": 45000, "read_wait_ms": 50000, "write_wait_ms": 8000, "finalize_wait_ms": 1000}, "source"),
    ({"read_wait_ms": 1000, "write_wait_ms": 9000}, "target"),
    ({"read_wait_ms": 1000, "write_wait_ms": 500, "finalize_wait_ms": 600}, "mixed"),
    ({}, "-"),
    ({"read_wait_ms": 0, "write_wait_ms": 0}, "-"),
])
def test_병목_판정(metrics, expected):
    assert bottleneck(metrics) == expected


# ───────── 요약 도구 ─────────

def _snap(ts: str, **fields) -> str:
    return f"{ts} " + format_kv({"kind": "snapshot", "window_s": 30, **fields})


def _write_logs(tmp_path):
    coord = tmp_path / "query-coordinator-server-perf.log"
    coord.write_text("\n".join([
        _snap("2026-10-29 10:00:00", role="coordinator", **{
            "jobs_done.DONE": 2, "jobs_done.FAILED": 1, "rows_written": 3000, "admission.running": 16,
            "admission.pending": 90, "admission.max_pending": 100, "admission.rejected": 4,
            "loop.lag_ms.max": 850, "dispatch.poll_errors.timeout": 2,
        }),
        _snap("2026-10-29 10:00:30", role="coordinator", **{
            "jobs_done.DONE": 1, "rows_written": 1500, "admission.running": 10, "loop.lag_ms.max": 3,
        }),
        "2026-10-29 10:00:40 " + format_kv({"kind": "job_done", "job_id": "job_slow", "status": "DONE",
                                            "exec_mode": "copy", "elapsed_s": 120.0, "rows_per_s": 10,
                                            "bottleneck": "source", "slowest_stage": "STREAM_COPY",
                                            "slowest_stage_s": 110.0}),
        "2026-10-29 10:00:50 " + format_kv({"kind": "job_done", "job_id": "job_fast", "status": "DONE",
                                            "elapsed_s": 5.0, "bottleneck": "target"}),
    ]) + "\n", encoding="utf-8")
    # 날짜가 붙은 보관 파일도 같은 서비스로 묶인다.
    (tmp_path / "query-executor-8087-perf_20261028.log").write_text(
        _snap("2026-10-28 23:59:30", role="executor", **{
            "tasks.running": 8, "tasks.max": 8, "tasks.queued": 5, "gp_pool.acquire_wait_ms.p95": 3000,
        }) + "\n", encoding="utf-8")
    (tmp_path / "unrelated.log").write_text("x\n", encoding="utf-8")
    return tmp_path


def test_디렉터리를_서비스별로_나눠_읽는다(tmp_path):
    files = pr.expand_paths([str(_write_logs(tmp_path))])
    assert sorted(f.name for f in files) == [
        "query-coordinator-server-perf.log", "query-executor-8087-perf_20261028.log",
    ]
    by_source = pr.load(files)
    assert sorted(by_source) == ["query-coordinator-server", "query-executor-8087"]
    assert len(by_source["query-coordinator-server"]) == 4


def test_시간대별_요약은_합_최대_비율로_묶는다(tmp_path):
    recs = pr.load(pr.expand_paths([str(_write_logs(tmp_path))]))["query-coordinator-server"]
    snaps = [r for r in recs if r.kind == "snapshot"]
    cols = {c.label: c for c in pr.COLUMNS["coordinator"]}
    assert pr.aggregate(snaps, cols["완료job"]) == 4
    assert pr.aggregate(snaps, cols["적재행/s"]) == 75  # 4500행 / 60초
    assert pr.aggregate(snaps, cols["실행"]) == 16
    lines = pr.timeline(snaps, "coordinator", timedelta(minutes=10))
    assert lines[0].startswith("시각") and lines[1].startswith("10-29 10:00")


def test_규칙에_걸린_구간을_모아_조정_방향을_준다(tmp_path):
    recs = pr.load(pr.expand_paths([str(_write_logs(tmp_path))]))
    titles = {f.rule.title: f for f in pr.find_bottlenecks(
        [r for r in recs["query-coordinator-server"] if r.kind == "snapshot"], "coordinator")}
    assert set(titles) == {"이벤트 루프 지연", "job 거절(429)", "job 대기 큐 80% 이상", "task 폴링 오류"}
    assert titles["이벤트 루프 지연"].count == 1 and titles["이벤트 루프 지연"].peak == 850
    exec_titles = {f.rule.title for f in pr.find_bottlenecks(recs["query-executor-8087"], "executor")}
    assert exec_titles == {"executor 슬롯 포화", "GP 커넥션 풀 대기"}


def test_보고서_전체(tmp_path, capsys):
    root = _write_logs(tmp_path)
    assert pr.main([str(root), "--top", "1"]) == 0
    out = capsys.readouterr().out
    assert "== query-coordinator-server (coordinator)" in out
    assert "[job 거절(429)] 1구간" in out and "max_concurrent_jobs" in out
    assert "job_slow" in out and "job_fast" not in out  # 상위 1개만
    assert "STREAM_COPY(110s)" in out
    assert "병목 분포: source(소스 대기) 1건(50%), target(GP 쓰기) 1건(50%)" in out
    assert "== query-executor-8087 (executor)" in out


def test_보고서_필터와_열_지정(tmp_path, capsys):
    root = _write_logs(tmp_path)
    assert pr.main([str(root), "--source", "query-executor-8087", "--keys", "tasks.queued,tasks.max:last"]) == 0
    out = capsys.readouterr().out
    assert "query-coordinator-server" not in out
    assert "tasks.queued" in out and "tasks.max" in out
    assert pr.main([str(root), "--since", "2026-10-30"]) == 1


def test_구간_인자():
    assert pr.parse_bucket("30s") == timedelta(seconds=30)
    assert pr.parse_bucket("10m") == timedelta(minutes=10)
    assert pr.parse_bucket("1h") == timedelta(hours=1)
    with pytest.raises(Exception):
        pr.parse_bucket("10x")


# ───────── 서비스 계측 통합 ─────────

class _Capture(logging.Handler):
    """지표 로거로 나가는 줄을 모은다."""

    def __init__(self):
        super().__init__()
        self.lines: list = []

    def emit(self, record):
        self.lines.append(record.getMessage())


@pytest.fixture()
def perf_capture(monkeypatch):
    from core.config import settings

    monkeypatch.setattr(settings, "log_perf_enabled", True, raising=False)
    monkeypatch.setattr(settings, "log_perf_job_summary", True, raising=False)
    cap = _Capture()
    log = perfmod.perf_logger
    old_level = log.level
    log.addHandler(cap)
    log.setLevel(logging.INFO)
    perfmod.perf.collect()  # 앞선 테스트가 남긴 구간 값을 비운다
    yield cap
    log.removeHandler(cap)
    log.setLevel(old_level)


def _events(cap, kind):
    recs = [pr.parse_line("2026-10-29 00:00:00 " + line) for line in cap.lines]
    return [r for r in recs if r and r.kind == kind]


async def test_coordinator_는_failover_와_job_요약을_남긴다(monkeypatch, perf_capture):
    import httpx

    from coordinator.dispatcher import HttpDispatcher
    from tests.test_failover import DOWN, UP, _down_up_handler, _job, _patch_client, _settings

    calls: list = []
    _patch_client(monkeypatch, _down_up_handler(calls))
    disp = HttpDispatcher(_settings(monkeypatch, executors=[DOWN, UP], retries=0))
    job = _job(DOWN)
    await asyncio.wait_for(disp.run(job), timeout=3)
    out = perfmod.perf.collect()
    assert out["dispatch.start_errors.connect"] == 1 and out["dispatch.failover"] == 1
    assert out["dispatch.tasks_started"] == 1 and out["jobs_done.DONE"] == 1 and out["rows_written"] == 7
    assert out["admission.slot_wait_ms.n"] == 1 and out["dispatch.slot_wait_ms.n"] == 1
    assert out["executor[127.0.0.1:9002].poll_rtt_ms.n"] >= 1
    # 끝난 뒤에는 실행·대기·폴링 중인 수가 모두 0 으로 돌아온다.
    assert out["admission.running"] == 0 and out["dispatch.inflight"] == 0
    assert not any(k.endswith(".inflight") and k.startswith("executor[") for k in out)
    done = _events(perf_capture, "job_done")
    assert len(done) == 1
    f = done[0].fields
    assert f["job_id"] == job.job_id and f["status"] == "DONE" and f["rows"] == 7 and f["tasks"] == 1


async def test_executor_는_task_요약과_단계_분포를_남긴다(perf_capture):
    from executor.app import create_app as create_executor_app
    from executor.backend import MockBackend
    from tests.test_exec_mode import _payload, _run_task

    app = create_executor_app(backend=MockBackend())
    st = await _run_task(app, _payload("perf-t1", "copy", sub_query="SELECT * FROM t WHERE dt IN ('1')"))
    assert st["status"] == "DONE"
    out = perfmod.perf.collect()
    assert out["tasks_done.DONE"] == 1
    assert out["stage.QUEUE_WAIT_ms.n"] == 1
    done = _events(perf_capture, "task_done")
    assert len(done) == 1
    f = done[0].fields
    assert f["task_id"] == "perf-t1" and f["status"] == "DONE" and f["exec_mode"] == "copy"
    assert "stage.QUEUE_WAIT_ms" in f and "bottleneck" in f
