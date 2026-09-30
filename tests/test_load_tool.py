"""`POST /jobs` 부하 테스트 도구(`tools.load`) 검증.

실제 coordinator 없이 httpx.MockTransport 로 만든 가짜 coordinator 를 끼워 돌린다. 가짜 서버는
job 을 받은 뒤 정해진 횟수만큼 폴링되면 DONE 이 되고, 동시 job 이 용량을 넘으면 429 를 준다.
시간 값은 모두 수십 ms 로 줄여 테스트가 1초 안팎에 끝나게 한다.
"""

from __future__ import annotations

import json
import math
import random

import httpx
import pytest

from tools.load import cli, report, request, scenario
from tools.load.client import CoordinatorClient, server_seconds
from tools.load.request import PollSpec, RequestSpec
from tools.load.runner import LoadConfig, LoadRunner
from tools.load.sampler import extract_servers
from tools.load.stats import Collector, percentile, summarize


# ───────────────────────── 가짜 coordinator ─────────────────────────

class FakeCoordinator:
    """job 수명주기와 admission 을 흉내 내는 최소 coordinator 다."""

    def __init__(self, polls_to_finish=2, capacity=None, final="DONE", rows=100,
                 lose_jobs=False, never_finish=False, sync_status=200):
        self.polls_to_finish = polls_to_finish
        self.capacity = capacity
        self.final = final
        self.rows = rows
        self.lose_jobs = lose_jobs
        self.never_finish = never_finish
        self.sync_status = sync_status
        self.sync_calls = 0
        self.jobs = {}
        self.bodies = []
        self.headers = []
        self.cancelled = []
        self.cluster_calls = 0
        self._n = 0

    def active(self):
        return sum(1 for j in self.jobs.values() if j["status"] == "RUNNING")

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/jobs":
            body = json.loads(request.content)
            self.bodies.append(body)
            self.headers.append(dict(request.headers))
            if body.get("dry_run"):
                return httpx.Response(200, json={"dry_run": True, "task_count": 2})
            if self.capacity is not None and self.active() >= self.capacity:
                return httpx.Response(429, json={"detail": "한도 초과"},
                                      headers={"Retry-After": "0"})
            self._n += 1
            job_id = f"job{self._n}"
            self.jobs[job_id] = {"status": "RUNNING", "polls": 0}
            return httpx.Response(202, json={"job_id": job_id})
        if request.method == "GET" and path.startswith("/jobs/") and path.endswith("/status"):
            job_id = path.split("/")[2]
            job = self.jobs.get(job_id)
            if job is None or self.lose_jobs:
                return httpx.Response(404, json={"detail": "job not found"})
            job["polls"] += 1
            if not self.never_finish and job["status"] == "RUNNING" \
                    and job["polls"] >= self.polls_to_finish:
                job["status"] = self.final
            done = job["status"] != "RUNNING"
            return httpx.Response(200, json={
                "job_id": job_id, "status": job["status"],
                "created_at": "2026-09-30 10:00:00.000",
                "started_at": "2026-09-30 10:00:00.500",
                "finished_at": "2026-09-30 10:00:02.000" if done else None,
                "total_rows_written": self.rows if done else 0,
                "error": "boom" if job["status"] == "FAILED" else None,
            })
        if request.method == "POST" and path.endswith("/cancel"):
            job_id = path.split("/")[2]
            self.cancelled.append(job_id)
            if job_id in self.jobs:
                self.jobs[job_id]["status"] = "CANCELLED"
            return httpx.Response(200, json={"job_id": job_id})
        if request.method == "POST" and path == "/query-execute":
            # sync 요청: 응답이 곧 결과다.
            self.sync_calls += 1
            if self.capacity is not None and self.sync_calls > self.capacity:
                return httpx.Response(429, json={"detail": "busy"}, headers={"Retry-After": "0"})
            return httpx.Response(self.sync_status, json={"rows": [[1]], "executed_by": None})
        if request.method == "POST" and path == "/api/tasks":
            # 이 저장소 밖의 비동기 API 흉내: id 필드·상태 필드·상태 값이 /jobs 와 다르다.
            self._n += 1
            tid = f"t{self._n}"
            self.jobs[tid] = {"status": "RUNNING", "polls": 0}
            return httpx.Response(201, json={"task_id": tid})
        if request.method == "GET" and path.startswith("/api/tasks/"):
            job = self.jobs[path.rsplit("/", 1)[1]]
            job["polls"] += 1
            state = "SUCCEEDED" if job["polls"] >= self.polls_to_finish else "IN_PROGRESS"
            return httpx.Response(200, json={"state": state})
        if request.method == "GET" and path == "/cluster":
            self.cluster_calls += 1
            return httpx.Response(200, json={
                "coordinator": {"metrics": {"cpu_percent": 10.0 + self.active(),
                                            "memory": {"percent": 30.0}}},
                "executors": [
                    {"executor_url": "http://e1:8001", "healthy": True, "cpu_percent": 50.0,
                     "memory_percent": 40.0, "active_tasks": 3, "max_concurrent_tasks": 8},
                    {"executor_url": "http://e2:8001", "healthy": False, "cpu_percent": None,
                     "memory_percent": None, "active_tasks": None,
                     "max_concurrent_tasks": None},
                ],
                "jobs": {"active": self.active(), "running": self.active()},
            })
        return httpx.Response(404)


JOB_BODY = {"sql": "SELECT 1", "target_table": "t_${vu}", "parallelism": "${vu}"}


def _config(**over):
    spec = over.pop("request", None) or RequestSpec(
        type="async", body=over.pop("body", JOB_BODY))
    base = dict(
        request=spec,
        stages=scenario.stages_from_ramp(3, 0.0, 0.4),
        poll_interval=0.01, poll_jitter=0.0, tick=0.01, error_backoff=0.01,
        reject_backoff=0.01, drain_timeout=1.0, sample_interval=0.05, seed=1,
    )
    base.update(over)
    return LoadConfig(**base)


async def _run(fake, sampler=False, **over):
    client = CoordinatorClient(["http://coord"], transport=httpx.MockTransport(fake.handler))
    async with client:
        runner = LoadRunner(_config(**over), client, sampler_enabled=sampler)
        await runner.run()
    return runner


# ───────────────────────── scenario ─────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("90", 90.0), (15, 15.0), ("30s", 30.0), ("2m", 120.0), ("1h", 3600.0),
    ("1m30s", 90.0), ("500ms", 0.5), ("1.5s", 1.5),
])
def test_parse_duration(text, expected):
    assert scenario.parse_duration(text) == expected


@pytest.mark.parametrize("text", ["", "abc", "10x", "1m 30s", "-5"])
def test_parse_duration_은_잘못된_표기를_거부한다(text):
    with pytest.raises(ValueError):
        scenario.parse_duration(text)


def test_ramp_up_은_JMeter_처럼_VU_를_선형으로_늘린다():
    stages = scenario.stages_from_ramp(20, 60.0, 600.0)
    assert scenario.total_duration(stages) == 600.0
    # 첫 VU 는 곧바로 뜨고, 3초마다 한 명씩 늘어 60초에 20명이 된다.
    assert scenario.target_vus(stages, 0.01) == 1
    assert scenario.target_vus(stages, 30.0) == 10
    assert scenario.target_vus(stages, 59.99) == 20
    assert scenario.target_vus(stages, 300.0) == 20


def test_ramp_up_0_이면_처음부터_목표_VU_를_띄운다():
    # 회귀: 예전에는 유지 단계가 0 에서 출발하는 선형 구간이 되어 전체 시간에 걸쳐 천천히 늘었다.
    stages = scenario.stages_from_ramp(4, 0.0, 60.0)
    assert scenario.target_vus(stages, 0.0) == 4
    assert scenario.target_vus(stages, 5.0) == 4


def test_ramp_down_은_구간_끝보다_먼저_한_명씩_뺀다():
    stages = scenario.parse_stages("10s:10,10s:0")
    assert scenario.target_vus(stages, 10.0) == 10
    assert scenario.target_vus(stages, 15.0) == 5
    assert scenario.target_vus(stages, 19.99) == 0
    assert scenario.target_vus(stages, 100.0) == 0


def test_duration_없이_iterations_만_주면_무한_유지_단계가_붙는다():
    stages = scenario.stages_from_ramp(5, 10.0, None)
    assert scenario.total_duration(stages) == math.inf
    assert scenario.target_vus(stages, 1e9) == 5


def test_duration_이_ramp_up_보다_짧으면_거부한다():
    with pytest.raises(ValueError):
        scenario.stages_from_ramp(5, 60.0, 30.0)


def test_max_target_after_는_남은_일정의_최대_VU_다():
    stages = scenario.parse_stages("10s:5,10s:20,10s:0")
    assert scenario.max_target_after(stages, 0.0) == 20
    assert scenario.max_target_after(stages, 25.0) == 10  # 20 → 0 으로 내려가는 중
    assert scenario.max_target_after(stages, 40.0) == 0


@pytest.mark.parametrize("text", ["30s", "30s:x", "30s:-1", ""])
def test_parse_stages_는_잘못된_단계를_거부한다(text):
    with pytest.raises(ValueError):
        scenario.parse_stages(text)


def test_render_body_는_값만_치환하고_원본을_보존한다():
    tpl = {
        "target_table": "t_${vu}_${iter}",
        "parallelism": "${vu}",
        "params": [{"name": "d", "value": "${choice:a,b}"}, {"n": "${randint:3-3}"}],
        "keep": 1,
        "${vu}": "키는 치환하지 않는다",
    }
    ctx = scenario.RenderContext(vu=7, iteration=2, seq=9, run_id="r1", rng=random.Random(0))
    out = scenario.render_body(tpl, ctx)
    assert out["target_table"] == "t_7_2"
    assert out["parallelism"] == 7          # 문자열 전체가 정수 변수면 정수로 넣는다
    assert out["params"][0]["value"] in ("a", "b")
    assert out["params"][1]["n"] == 3
    assert "${vu}" in out and tpl["target_table"] == "t_${vu}_${iter}"


def test_render_body_는_모르는_자리표시자를_시작_전에_잡는다():
    with pytest.raises(ValueError):
        scenario.validate_body({"x": "${nope}"})


# ───────────────────────── stats·client·sampler ─────────────────────────

def test_percentile_nearest_rank():
    vals = list(range(1, 101))
    assert percentile(vals, 50) == 50
    assert percentile(vals, 99) == 99
    assert percentile(vals, 100) == 100
    assert percentile([], 50) is None
    assert summarize([])["count"] == 0


def test_summary_는_429_를_오류율에서_뺀다():
    c = Collector()
    c.record_submit(0.1, "accepted", 0.01)
    c.record_submit(0.2, "rejected_429", 0.01, vus=5)
    c.record_submit(0.3, "server_error", 0.01, error="HTTP 500")
    c.record_job(1.5, "DONE", True, 1.4, queue=0.1, run=1.0, rows=10)
    s = c.summary(elapsed=2.0, load_elapsed=1.0)
    assert s["submitted"] == 3 and s["accepted"] == 1 and s["completed"] == 1
    assert s["succeeded"] == 1 and s["failed"] == 1
    assert s["error_rate"] == pytest.approx(0.5)   # 시도 2건(429 제외) 중 오류 1건
    assert s["reject_rate"] == pytest.approx(1 / 3)
    assert s["submit_tps"] == pytest.approx(3.0)   # 부하 구간으로 나눈다
    assert s["complete_tps"] == pytest.approx(0.5)  # drain 까지 포함한 전체 시간으로 나눈다
    assert s["first_reject_vus"] == 5 and s["rows_written"] == 10


def test_server_seconds_는_서버_표기_시각의_차이다():
    assert server_seconds("2026-09-30 10:00:00.000", "2026-09-30 10:00:01.250") == 1.25
    assert server_seconds(None, "2026-09-30 10:00:01.250") is None
    assert server_seconds("garbage", "2026-09-30 10:00:01.250") is None


def test_extract_servers_는_coordinator_와_executor_를_편다():
    cluster = {
        "coordinator": {"metrics": {"cpu_percent": 12.5, "memory": {"percent": 40}}},
        "executors": [{"executor_url": "http://e1", "healthy": True, "cpu_percent": 80,
                       "memory_percent": 60, "active_tasks": 4, "max_concurrent_tasks": 8}],
    }
    out = extract_servers(cluster, "coordinator")
    assert out["coordinator"]["cpu"] == 12.5 and out["coordinator"]["mem"] == 40
    assert out["http://e1"] == {"role": "executor", "cpu": 80, "mem": 60,
                                "active_tasks": 4, "max_tasks": 8, "healthy": True}


# ───────────────────────── runner ─────────────────────────

async def test_VU_는_job_이_끝날_때까지_폴링한_뒤_다음을_제출한다():
    fake = FakeCoordinator(polls_to_finish=3)
    runner = await _run(fake)
    c = runner.collector
    assert c.submit_counts["accepted"] > 0
    # closed model: 받아들인 job 은 모두 종료를 확인했고 버려진 것이 없다.
    assert c.job_counts["DONE"] == c.submit_counts["accepted"]
    assert c.job_counts["ABANDONED"] == 0
    assert c.poll_requests >= 3 * c.job_counts["DONE"]
    assert c.rows_written == 100 * c.job_counts["DONE"]
    assert c.queue_time[0] == pytest.approx(0.5) and c.run_time[0] == pytest.approx(1.5)
    assert runner.stop_reason == "지정 시간 종료"
    # 본문 치환이 VU 별로 적용됐다.
    assert {b["target_table"] for b in fake.bodies} <= {"t_1", "t_2", "t_3"}
    assert all(isinstance(b["parallelism"], int) for b in fake.bodies)


async def test_용량을_넘으면_429_를_따로_센다():
    fake = FakeCoordinator(polls_to_finish=5, capacity=1)
    runner = await _run(fake)
    s = runner.collector.summary(runner.total_elapsed, runner.load_elapsed)
    assert s["submit_outcomes"]["rejected_429"] > 0
    assert s["first_reject_at"] is not None
    assert s["error_rate"] == 0.0  # 429 는 오류가 아니라 포화 신호다


async def test_iterations_를_채우면_스스로_끝난다():
    fake = FakeCoordinator(polls_to_finish=1)
    runner = await _run(fake, stages=scenario.stages_from_ramp(2, 0.0, None), iterations=3)
    assert len(fake.bodies) == 6
    assert runner.stop_reason == "반복 횟수 완료"
    assert runner.collector.job_counts["DONE"] == 6


async def test_drain_시간을_넘긴_job_은_취소하고_ABANDONED_로_남긴다():
    fake = FakeCoordinator(never_finish=True)
    runner = await _run(fake, stages=scenario.stages_from_ramp(2, 0.0, 0.1),
                        drain_timeout=0.05, on_stop="cancel")
    c = runner.collector
    assert c.job_counts["ABANDONED"] == 2
    assert c.cancel_sent == 2 and sorted(fake.cancelled) == ["job1", "job2"]


async def test_on_stop_abandon_은_취소하지_않는다():
    fake = FakeCoordinator(never_finish=True)
    runner = await _run(fake, stages=scenario.stages_from_ramp(1, 0.0, 0.1),
                        drain_timeout=0.05, on_stop="abandon")
    assert runner.collector.job_counts["ABANDONED"] == 1
    assert fake.cancelled == []


async def test_job_timeout_을_넘기면_POLL_TIMEOUT():
    fake = FakeCoordinator(never_finish=True)
    runner = await _run(fake, stages=scenario.stages_from_ramp(1, 0.0, 0.2), job_timeout=0.05)
    assert runner.collector.job_counts["POLL_TIMEOUT"] >= 1


async def test_폴링_중_job_이_사라지면_LOST():
    fake = FakeCoordinator(lose_jobs=True)
    runner = await _run(fake, stages=scenario.stages_from_ramp(1, 0.0, 0.2))
    assert runner.collector.job_counts["LOST"] >= 1


async def test_성공이_아닌_종료_상태는_오류로_집계하고_사유를_남긴다():
    fake = FakeCoordinator(polls_to_finish=1, final="FAILED")
    runner = await _run(fake, stages=scenario.stages_from_ramp(1, 0.0, 0.1))
    s = runner.collector.summary(runner.total_elapsed, runner.load_elapsed)
    assert s["job_outcomes"]["FAILED"] >= 1 and s["error_rate"] == 1.0
    assert any("boom" in msg for msg, _ in s["top_errors"])


async def test_dry_run_본문은_sync_로_폴링_없이_완료로_센다():
    fake = FakeCoordinator()
    spec = request.build_spec({}, {"sql": "SELECT 1", "dry_run": True})
    assert spec.type == "sync"
    runner = await _run(fake, request=spec,
                        stages=scenario.stages_from_ramp(1, 0.0, None), iterations=3)
    c = runner.collector
    assert c.submit_counts["ok"] == 3 and c.completed == 3 and c.poll_requests == 0


async def test_idempotency_key_는_요청마다_다르다():
    fake = FakeCoordinator(polls_to_finish=1)
    await _run(fake, stages=scenario.stages_from_ramp(1, 0.0, None), iterations=3,
               idempotency_key=True)
    keys = [h.get("idempotency-key") for h in fake.headers]
    assert len(set(keys)) == 3 and all(k and k.startswith("load-") for k in keys)


async def test_연결_오류도_결과로_세고_멈추지_않는다():
    def boom(request):
        raise httpx.ConnectError("refused")

    client = CoordinatorClient(["http://coord"], transport=httpx.MockTransport(boom))
    async with client:
        runner = LoadRunner(_config(stages=scenario.stages_from_ramp(1, 0.0, 0.1)), client,
                            sampler_enabled=False)
        await runner.run()
    assert runner.collector.submit_counts["conn_error"] >= 1


async def test_sampler_는_기준선과_부하_구간을_나눠_모은다():
    fake = FakeCoordinator(polls_to_finish=2)
    runner = await _run(fake, sampler=True, baseline=0.1)
    summary = runner.sampler.summary()
    names = [s["name"] for s in summary["servers"]]
    assert names[0] == "coordinator" and "http://e1:8001" in names
    coord = summary["servers"][0]["phases"]
    assert "baseline" in coord and "load" in coord
    e2 = next(s for s in summary["servers"] if s["name"] == "http://e2:8001")
    assert e2["unhealthy_samples"] > 0


async def test_request_stop_은_제출을_멈추고_진행중_job_을_끝까지_본다():
    fake = FakeCoordinator(polls_to_finish=3)
    client = CoordinatorClient(["http://coord"], transport=httpx.MockTransport(fake.handler))

    def stop_early(r):
        if r.collector.submit_counts["accepted"] >= 2:
            r.request_stop()

    async with client:
        runner = LoadRunner(_config(stages=scenario.stages_from_ramp(2, 0.0, 60.0)), client,
                            sampler_enabled=False, on_tick=stop_early)
        await runner.run()
    c = runner.collector
    assert runner.stop_reason == "사용자 중단"
    assert runner.load_elapsed < 5
    assert c.job_counts["DONE"] == c.submit_counts["accepted"]


# ───────────────────────── report·cli ─────────────────────────

async def test_요약_보고서에_처리량과_서버_자원이_함께_나온다(tmp_path):
    fake = FakeCoordinator(polls_to_finish=2, capacity=2)
    runner = await _run(fake, sampler=True)
    meta = {"run_id": "r1", "coordinators": ["http://coord"], "scenario": "test",
            "request_type": "async"}
    result = report.build_result(runner, meta)
    text = report.format_summary(result)
    for word in ("[처리량]", "완료", "TPS", "[지연]", "[서버 자원]", "coordinator",
                 "http://e1:8001", "[포화 신호]"):
        assert word in text
    assert "진행중" in report.format_progress(runner)

    out, tl = tmp_path / "r.json", tmp_path / "t.csv"
    report.write_json(str(out), result)
    report.write_csv(str(tl), result["timeline"])
    assert json.loads(out.read_text(encoding="utf-8"))["summary"]["completed"] > 0
    assert tl.read_text(encoding="utf-8").splitlines()[0].startswith("second,vus,inflight")


def _opts(argv):
    return cli.merge_options(cli.build_parser().parse_args(argv))


def test_명령행이_시나리오_YAML_보다_우선한다(tmp_path):
    (tmp_path / "job.json").write_text('{"sql": "SELECT 1"}', encoding="utf-8")
    scn = tmp_path / "s.yml"
    scn.write_text("vus: 5\nramp_up: 10s\nduration: 1m\nbody: job.json\n"
                   "poll-interval: 3s\n", encoding="utf-8")
    opts = _opts(["run", "--scenario", str(scn), "--vus", "8"])
    assert opts["vus"] == 8 and opts["ramp_up"] == "10s" and opts["poll_interval"] == "3s"
    # 시나리오의 본문 경로는 시나리오 파일 기준으로 풀린다.
    assert cli.build_request(opts).body == {"sql": "SELECT 1"}
    assert scenario.max_vus(cli.build_stages(opts)) == 8


def test_시나리오의_모르는_키는_거부한다(tmp_path):
    scn = tmp_path / "s.yml"
    scn.write_text("vus: 5\nramp: 10s\n", encoding="utf-8")
    with pytest.raises(cli.ConfigError, match="ramp"):
        _opts(["run", "--scenario", str(scn)])


@pytest.mark.parametrize("argv,match", [
    (["run"], "--vus 또는 --stages"),
    (["run", "--vus", "3"], "--duration 또는 --iterations"),
    (["run", "--stages", "10s:3", "--vus", "3"], "함께 쓸 수 없습니다"),
])
def test_부하_곡선_설정_오류(argv, match):
    with pytest.raises(cli.ConfigError, match=match):
        cli.build_stages(_opts(argv))


def test_dry_run_body_는_본문에_dry_run_을_넣는다(tmp_path):
    body = tmp_path / "b.json"
    body.write_text('{"sql": "SELECT 1"}', encoding="utf-8")
    opts = _opts(["run", "--body", str(body), "--dry-run-body"])
    spec = cli.build_request(opts)
    assert spec.body["dry_run"] is True and spec.type == "sync"


def test_실제_적재는_비대화형이면_yes_없이_시작하지_않는다(monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False, raising=False)
    stages = scenario.stages_from_ramp(1, 0, 1)
    jobs = request.build_spec({}, {"sql": "x"})
    assert cli.confirm({}, jobs, stages, ["http://c"]) is False
    assert cli.confirm({"yes": True}, jobs, stages, ["http://c"]) is True
    # dry_run 과 읽기 전용 sync 요청은 묻지 않는다.
    dry = request.build_spec({}, {"sql": "x", "dry_run": True})
    preview = request.build_spec({"path": "/query-execute"}, {"template_id": "a"})
    assert cli.confirm({}, dry, stages, ["http://c"]) is True
    assert cli.confirm({}, preview, stages, ["http://c"]) is True


def test_plan_은_요청_없이_VU_곡선을_보여준다(capsys):
    assert cli.main(["plan", "--vus", "4", "--ramp-up", "8s", "--duration", "20s"]) == 0
    out = capsys.readouterr().out
    assert "최대 VU: 4" in out and "4 VU" in out


def test_설정_오류는_한_줄로_알리고_1_로_끝난다(capsys):
    assert cli.main(["run", "--vus", "3"]) == cli.EXIT_CONFIG
    assert "설정 오류" in capsys.readouterr().err


# ───────────────────────── 요청 type(sync/async)·폴링 설정 ─────────────────────────

def test_type_기본값은_경로와_dry_run_으로_정한다():
    assert request.default_type("/jobs", {"sql": "x"}) == "async"
    assert request.default_type("/jobs/", None) == "async"
    assert request.default_type("/jobs", {"dry_run": True}) == "sync"
    assert request.default_type("/query-execute", {}) == "sync"


@pytest.mark.parametrize("opts,body,match", [
    ({"type": "async"}, {"dry_run": True}, "sync 로 보내야"),
    ({"poll_path": "/jobs/status"}, {}, r"\$\{id\}"),
    ({"poll_success": "OK"}, {}, "종료 상태에 없습니다"),
    ({"type": "later"}, {}, "type 은"),
    ({"path": "jobs"}, {}, "'/' 로 시작"),
])
def test_요청_명세의_모순을_시작_전에_잡는다(opts, body, match):
    with pytest.raises(ValueError, match=match):
        request.build_spec(opts, body)


def test_성공_상태_코드_표기():
    assert request.parse_status_codes("200,201") == (200, 201)
    assert len(request.parse_status_codes("2xx")) == 100
    assert request.parse_status_codes(None) == ()
    with pytest.raises(ValueError):
        request.parse_status_codes("abc")
    spec = RequestSpec(type="sync", success_status=(200,))
    assert spec.is_success_status(200) and not spec.is_success_status(201)
    assert RequestSpec(type="sync").is_success_status(204)


def test_폴링_경로의_id_는_URL_인코딩된다():
    assert PollSpec().status_path("a/b c") == "/jobs/a%2Fb%20c/status"
    assert PollSpec(cancel_path=None).cancel_url("x") is None


async def test_sync_요청은_응답이_곧_완료다():
    fake = FakeCoordinator()
    spec = RequestSpec(type="sync", path="/query-execute", body={"template_id": "a"})
    runner = await _run(fake, request=spec, stages=scenario.stages_from_ramp(2, 0.0, None),
                        iterations=3)
    c = runner.collector
    assert fake.sync_calls == 6
    assert c.submit_counts["ok"] == 6 and c.completed == 6 and c.succeeded == 6
    assert c.poll_requests == 0 and not fake.jobs
    s = c.summary(runner.total_elapsed, runner.load_elapsed)
    assert s["latency"]["submit"]["count"] == 6 and s["latency"]["e2e_all"]["count"] == 0


async def test_sync_에서_지정하지_않은_2xx_는_오류다():
    fake = FakeCoordinator(sync_status=202)
    spec = RequestSpec(type="sync", path="/query-execute", body={}, success_status=(200,))
    runner = await _run(fake, request=spec, stages=scenario.stages_from_ramp(1, 0.0, None),
                        iterations=2)
    s = runner.collector.summary(runner.total_elapsed, runner.load_elapsed)
    assert s["submit_outcomes"]["unexpected_status"] == 2 and s["error_rate"] == 1.0


async def test_sync_에서도_429_는_포화_신호로_따로_센다():
    fake = FakeCoordinator(capacity=2)
    spec = RequestSpec(type="sync", path="/query-execute", body={})
    runner = await _run(fake, request=spec, stages=scenario.stages_from_ramp(1, 0.0, None),
                        iterations=4)
    s = runner.collector.summary(runner.total_elapsed, runner.load_elapsed)
    assert s["submit_outcomes"]["ok"] == 2 and s["submit_outcomes"]["rejected_429"] == 2
    assert s["error_rate"] == 0.0


async def test_폴링_규칙을_설정하면_다른_비동기_API_도_잰다():
    fake = FakeCoordinator(polls_to_finish=2)
    spec = RequestSpec(type="async", path="/api/tasks", body={}, poll=PollSpec(
        path="/api/tasks/${id}", id_field="task_id", status_field="state",
        terminal=("SUCCEEDED", "FAILED"), success=("SUCCEEDED",), cancel_path=None))
    runner = await _run(fake, request=spec, stages=scenario.stages_from_ramp(1, 0.0, None),
                        iterations=3)
    c = runner.collector
    assert c.job_counts["SUCCEEDED"] == 3 and c.succeeded == 3
    # 이 API 에는 서버 타임스탬프가 없어 대기·실행 시간이 비어 있을 뿐 동작은 같다.
    assert c.queue_time == [] and len(c.e2e_success) == 3


async def test_async_인데_응답에_id_가_없으면_missing_id_오류다():
    fake = FakeCoordinator()
    spec = RequestSpec(type="async", path="/query-execute", body={})  # id 없는 응답
    runner = await _run(fake, request=spec, stages=scenario.stages_from_ramp(1, 0.0, None),
                        iterations=2)
    c = runner.collector
    assert c.submit_counts["missing_id"] == 2 and c.poll_requests == 0
    assert any("job_id" in msg for msg in c.errors)


async def test_cancel_path_를_비우면_drain_뒤에도_취소하지_않는다():
    fake = FakeCoordinator(never_finish=True)
    spec = RequestSpec(type="async", body=JOB_BODY, poll=PollSpec(cancel_path=None))
    runner = await _run(fake, request=spec, stages=scenario.stages_from_ramp(1, 0.0, 0.1),
                        drain_timeout=0.05, on_stop="cancel")
    assert runner.collector.job_counts["ABANDONED"] == 1 and fake.cancelled == []


async def test_sync_요약은_TPS_와_응답_지연_하나씩만_낸다():
    fake = FakeCoordinator()
    spec = RequestSpec(type="sync", path="/query-execute", body={})
    runner = await _run(fake, request=spec, stages=scenario.stages_from_ramp(1, 0.0, None),
                        iterations=3)
    meta = {"run_id": "r", "coordinators": ["http://coord"], "scenario": "s",
            "request_type": "sync"}
    text = report.format_summary(report.build_result(runner, meta))
    assert "요청  3건" in text and "응답" in text
    assert "완료  " not in text and "e2e" not in text and "폴링" not in text


# ───────────────────────── 시나리오 request 블록·명령행 왕복 ─────────────────────────

def test_시나리오의_request_블록을_평평한_옵션으로_편다(tmp_path):
    (tmp_path / "q.json").write_text('{"template_id": "a"}', encoding="utf-8")
    scn = tmp_path / "s.yml"
    scn.write_text(
        "request:\n  type: async\n  path: /api/tasks\n  body: q.json\n"
        "  poll:\n    path: /api/tasks/${id}\n    id_field: task_id\n"
        "    terminal: [SUCCEEDED, FAILED]\n    success: [SUCCEEDED]\n    cancel_path: ''\n"
        "vus: 3\nduration: 1m\n", encoding="utf-8")
    opts = _opts(["run", "--scenario", str(scn)])
    spec = cli.build_request(opts)
    assert spec.path == "/api/tasks" and spec.poll.id_field == "task_id"
    assert spec.poll.terminal == ("SUCCEEDED", "FAILED") and spec.poll.cancel_path is None
    assert spec.body == {"template_id": "a"}


def test_request_블록의_모르는_키는_거부한다(tmp_path):
    scn = tmp_path / "s.yml"
    scn.write_text("request:\n  poll:\n    interval: 1s\n", encoding="utf-8")
    with pytest.raises(cli.ConfigError, match="interval"):
        _opts(["run", "--scenario", str(scn)])


def test_옵션을_명령행으로_되돌려도_같은_설정이_된다(tmp_path):
    body = tmp_path / "b.json"
    body.write_text("{}", encoding="utf-8")
    opts = {
        "coordinator": ["http://a:8088", "http://b:8088"], "type": "async", "path": "/api/tasks",
        "body": str(body), "poll_path": "/api/tasks/${id}", "poll_terminal": "OK,NG",
        "poll_success": "OK", "cancel_path": "", "vus": 5, "ramp_up": "30s", "duration": "5m",
        "idempotency_key": True, "poll_interval": "2s",  # 기본값과 같은 값은 생략된다
    }
    argv = cli.opts_to_argv(opts)
    assert argv[0] == "run" and "--poll-interval" not in argv
    assert argv.count("--coordinator") == 2 and "--idempotency-key" in argv
    # 빈 cancel_path 는 "취소하지 않음"이라 명령행에도 남는다.
    assert argv[argv.index("--cancel-path") + 1] == ""
    back = cli.merge_options(cli.build_parser().parse_args(argv))
    spec = cli.build_request(back)
    assert spec.poll.terminal == ("OK", "NG") and spec.poll.cancel_path is None
    assert back["coordinator"] == opts["coordinator"] and back["vus"] == 5
    assert "\\\n" in cli.format_command(argv) and "''" in cli.format_command(argv)


def test_시나리오_YAML_로_저장하면_다시_읽어_같은_요청이_된다(tmp_path):
    body = tmp_path / "b.json"
    body.write_text('{"template_id": "a"}', encoding="utf-8")
    opts = {"type": "sync", "path": "/query-execute", "body": str(body),
            "success_status": "200", "vus": 4, "duration": "2m", "baseline": "10s"}
    spec = cli.build_request(opts)
    text = cli.opts_to_scenario(opts, spec)
    assert text.startswith("request:") and "baseline" not in text  # 기본값 생략
    scn = tmp_path / "s.yml"
    scn.write_text(text, encoding="utf-8")
    back = cli.load_scenario(str(scn))
    spec2 = cli.build_request(back)
    assert (spec2.type, spec2.path, spec2.success_status, spec2.body) == (
        "sync", "/query-execute", (200,), {"template_id": "a"})
    assert back["vus"] == 4


def test_http_timeout_기본값은_type_별로_다르다():
    assert cli.http_timeout({}, RequestSpec(type="async")) == 30.0
    assert cli.http_timeout({}, RequestSpec(type="sync")) == 300.0
    assert cli.http_timeout({"http_timeout": "10s"}, RequestSpec(type="sync")) == 10.0


# ───────────────────────── 대화형 마법사 ─────────────────────────

class _Script:
    """마법사에 대본대로 답한다. 대본이 떨어지면 EOF(중단)다."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.prompts = []

    def __call__(self, prompt):
        self.prompts.append(prompt)
        if not self.answers:
            raise EOFError
        return self.answers.pop(0)


def _wizard(answers, preset=None, health=None):
    import io
    from tools.load.wizard import Wizard
    ran = []
    script = _Script(answers)
    out = io.StringIO()
    wz = Wizard(preset, input_fn=script, out=out,
                health_check=health or (lambda urls: [(u, None) for u in urls]),
                runner=lambda argv: ran.append(argv) or 0)
    return wz, script, out, ran


def test_마법사_async_jobs_기본_흐름(tmp_path, capsys):
    body = tmp_path / "job.json"
    body.write_text('{"sql": "SELECT 1", "target_table": "t_${vu}"}', encoding="utf-8")
    wz, script, out, ran = _wizard([
        "http://coord:8088",   # 대상
        "",                    # type: 기본(async)
        "", "",                # 경로 /jobs, 메서드 POST
        str(body),             # 본문
        "",                    # 폴링 규칙 기본값 사용(y)
        "n",                   # Idempotency-Key 안 붙임
        "", "20", "60s", "", "10m",  # ramp 방식, VU 20, ramp-up 60s, 시간 기준, 10m
        "", "", "", "",        # think time, 폴링 간격, job 타임아웃, HTTP 타임아웃
        "", "", "",            # 자원 수집 y, baseline, 간격
        "r.json", "-", "-",    # 결과 파일
        "",                    # 다음으로: 지금 실행
    ])
    assert wz.run() == 0
    argv = ran[0]
    assert argv[:1] == ["run"] and "--vus" in argv and argv[argv.index("--vus") + 1] == "20"
    assert argv[argv.index("--type") + 1] == "async"
    assert "--poll-path" not in argv and "--idempotency-key" not in argv
    assert "--http-timeout" not in argv  # type 기본값과 같으면 생략한다
    assert "bin/load-test run" in capsys.readouterr().out  # 완성된 명령을 stdout 으로 보여 준다
    text = out.getvalue()
    assert "연결 확인: http://coord:8088 정상" in text and "20 VU" in text
    assert "실제로 적재합니다" in text


def test_마법사_sync_는_폴링을_묻지_않고_잘못된_답은_다시_묻는다(tmp_path):
    body = tmp_path / "q.json"
    body.write_text('{"template_id": "a"}', encoding="utf-8")
    wz, script, out, ran = _wizard([
        "",                    # 대상 기본값
        "2",                   # sync
        "", "",                # 경로 /query-execute, POST
        "없는파일.json",        # 잘못된 본문 → 다시 묻는다
        str(body),
        "200",                 # 성공 코드
        "2",                   # 단계 지정 방식
        "abc",                 # 잘못된 단계 → 다시 묻는다
        "10s:2,20s:2",
        "y", "5",              # VU 당 5회
        "", "",                # think time, HTTP 타임아웃
        "n",                   # 자원 수집 안 함
        "-", "-", "-",
        "3",                   # 명령만 보여 주고 끝내기
    ])
    assert wz.run() == 0 and ran == []
    argv = cli.opts_to_argv(wz.opts)
    assert argv[argv.index("--type") + 1] == "sync"
    assert argv[argv.index("--success-status") + 1] == "200"
    assert "--no-sample" in argv and "--poll-interval" not in argv
    joined = " ".join(script.prompts)
    assert "폴링" not in joined
    assert "읽을 수 없습니다" in out.getvalue() and "형식이어야" in out.getvalue()


def test_마법사_사용자_정의_폴링_규칙과_YAML_저장(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "t.json").write_text("{}", encoding="utf-8")
    wz, script, out, ran = _wizard([
        "", "1", "/api/tasks", "", "t.json",
        "n",                        # 폴링 규칙 직접 지정
        "/api/tasks/status",        # ${id} 없음 → 다시 묻는다
        "/api/tasks/${id}", "task_id", "state", "SUCCEEDED,FAILED",
        "OK",                       # 종료 상태에 없는 성공 값 → 다시 묻는다
        "SUCCEEDED", "-",           # 취소 안 함
        "", "3", "0", "2", "4",     # ramp, VU 3, ramp-up 0, 반복 기준, 4회
        "", "1s", "", "",           # think, 폴링 간격 1s, job 타임아웃, HTTP 타임아웃
        "n", "-", "-", "-",
        "2", "",                    # YAML 저장, 기본 파일명
        "3",                        # 그다음 끝내기
    ])
    assert wz.run() == 0
    back = cli.load_scenario(str(tmp_path / "load-scenario.yml"))
    spec = cli.build_request(back)
    assert spec.path == "/api/tasks" and spec.poll.path == "/api/tasks/${id}"
    assert spec.poll.success == ("SUCCEEDED",) and spec.poll.cancel_path is None
    assert back["iterations"] == 4 and back["poll_interval"] == "1s"


def test_마법사는_중단하면_아무것도_실행하지_않는다():
    wz, script, out, ran = _wizard(["http://c:1"])  # 두 번째 질문에서 EOF
    assert wz.run() == cli.EXIT_CONFIG and ran == []
    assert "중단했습니다" in out.getvalue()


def test_마법사는_물음표에_설명을_보여_주고_연결_실패도_알린다():
    wz, script, out, ran = _wizard(["?", "http://c:1"],
                                   health=lambda urls: [(urls[0], "ConnectError")])
    wz.run()
    text = out.getvalue()
    assert "라운드로빈" in text and "연결 실패: http://c:1 (ConnectError)" in text


def test_wizard_는_비대화형이면_거절한다(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False, raising=False)
    assert cli.main(["wizard"]) == cli.EXIT_CONFIG
    assert "대화형 터미널" in capsys.readouterr().err


# ───────────────────────── 라이브 진행 패널 ─────────────────────────

async def test_라이브_패널은_진행과_서버_자원을_여러_줄로_낸다():
    fake = FakeCoordinator(polls_to_finish=2)
    runner = await _run(fake, sampler=True)
    lines = report.format_live(runner, width=100)
    assert lines[0].startswith("■ 부하")
    joined = "\n".join(lines)
    assert "완료" in joined and "/s" in joined
    assert "서버" in joined and "CPU" in joined and "MEM" in joined
    assert "coordinator" in joined and "e1:8001" in joined
    assert any("다운" in ln for ln in lines)


def test_라이브_패널은_기준선_단계와_sync_를_구분한다():
    import time
    fake = FakeCoordinator()
    spec = RequestSpec(type="sync", path="/query-execute", body={})
    client = CoordinatorClient(["http://c"], transport=httpx.MockTransport(fake.handler))
    runner = LoadRunner(_config(request=spec), client, sampler_enabled=False)
    runner._t0 = time.monotonic() + 5
    runner.phase = "baseline"
    assert report.format_live(runner)[0].startswith("■ 기준선 수집 중")
    runner._t0 = time.monotonic()
    runner.phase = "load"
    runner.collector.record_submit(0.1, "ok", 0.02)
    lines = report.format_live(runner)
    assert any("요청" in ln and "성공" in ln for ln in lines)
    assert any("서버 자원 수집 꺼짐" in ln for ln in lines)


def test_라이브_패널은_화면_폭을_넘지_않는다():
    import time
    fake = FakeCoordinator()
    client = CoordinatorClient(["http://c"], transport=httpx.MockTransport(fake.handler))
    runner = LoadRunner(_config(), client, sampler_enabled=False)
    runner._t0 = time.monotonic()
    runner.phase = "load"
    for ln in report.format_live(runner, width=30):
        assert report.display_width(ln) <= 30


def test_LiveRenderer_는_줄이_줄면_남은_줄을_지운다():
    import io
    buf = io.StringIO()
    r = report.LiveRenderer(buf)
    r.render(["a", "b", "c"])
    buf.truncate(0); buf.seek(0)
    r.render(["x"])
    out = buf.getvalue()
    assert out.startswith("\033[3A")
    assert out.count("\033[K") >= 3
    buf.truncate(0); buf.seek(0)
    r.clear()
    assert buf.getvalue().startswith("\033[1A")


# ───────────────────────── 포화 분석(saturation) ─────────────────────────

from tools.load import analysis, charts


def _staircase_timeline(plan, secs=4, warmup_ok=True):
    """{vus: complete_per_sec} 를 계단형 시계열로 만든다."""
    tl, sec = [], 0
    for vus, cps in plan.items():
        for _ in range(secs):
            tl.append({"second": sec, "vus": vus, "inflight": vus, "submitted": cps,
                       "accepted": cps, "rejected_429": 0, "request_errors": 0,
                       "completed": cps, "success": cps, "failed": 0, "rows": cps * 100})
            sec += 1
    return tl


def test_per_vu_levels_는_수준별로_묶고_warmup_을_버린다():
    tl = _staircase_timeline({1: 5, 2: 10, 4: 18}, secs=4)
    levels = analysis.per_vu_levels(tl, warmup_frac=0.5)
    assert [l["vus"] for l in levels] == [1, 2, 4]
    # 4초 중 앞 2초(50%)를 버려 2초만 센다.
    assert all(l["seconds"] == 2 for l in levels)
    assert levels[2]["complete_tps"] == 18.0 and levels[2]["rows_per_sec"] == 1800.0
    # VU 0 은 제외된다.
    tl0 = [{"second": 0, "vus": 0, "completed": 0, "submitted": 0, "rejected_429": 0,
            "success": 0, "failed": 0, "rows": 0}] + tl
    assert [l["vus"] for l in analysis.per_vu_levels(tl0)] == [1, 2, 4]


def test_per_vu_levels_는_스쳐간_과도_수준을_버린다():
    # 1,2,4,8 은 6초씩 유지하고, 그 사이 전환 중 잡힌 3(1초)·6(1초)은 버려야 한다.
    tl, sec = [], 0
    for vus, secs, cps in [(1, 6, 5), (3, 1, 7), (2, 6, 10), (6, 1, 14), (4, 6, 18)]:
        for _ in range(secs):
            tl.append({"second": sec, "vus": vus, "completed": cps, "submitted": cps,
                       "rejected_429": 0, "success": cps, "failed": 0, "rows": 0})
            sec += 1
    levels = analysis.per_vu_levels(tl, warmup_frac=0.0, drop_short_frac=0.5)
    assert [l["vus"] for l in levels] == [1, 2, 4]   # 3, 6 은 빠진다
    # 필터를 끄면 모두 남는다.
    assert len(analysis.per_vu_levels(tl, warmup_frac=0.0, drop_short_frac=0.0)) == 5


def test_per_vu_levels_는_짧은_구간이면_마지막_1초만_쓴다():
    tl = _staircase_timeline({1: 5, 2: 8}, secs=1)
    levels = analysis.per_vu_levels(tl, warmup_frac=0.9)
    assert all(l["seconds"] == 1 for l in levels)


def test_find_knee_는_증가율이_꺾이는_지점을_찾는다():
    # 8→16 에서 3.8% 만 늘어 포화(기본 임계 5%).
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 18, 8: 26, 16: 27}))
    k = analysis.find_knee(levels)
    assert k["saturated"] and k["knee_vus"] == 8 and k["peak_vus"] == 16


def test_find_knee_는_계속_오르면_포화_안됨으로_본다():
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 20, 8: 40}))
    k = analysis.find_knee(levels)
    assert not k["saturated"] and k["peak_vus"] == 8


def test_find_knee_는_과부하로_TPS_가_줄면_그_앞을_무릎점으로():
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 20, 8: 15}))
    k = analysis.find_knee(levels)
    assert k["saturated"] and k["knee_vus"] == 4


def test_find_knee_는_수준이_하나면_None():
    levels = analysis.per_vu_levels(_staircase_timeline({4: 20}))
    assert analysis.find_knee(levels) is None


def test_ascii_chart_는_포화점만_다이아몬드로_찍는다():
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 18, 8: 26, 16: 27}))
    k = analysis.find_knee(levels)
    chart = "\n".join(analysis.ascii_chart(levels, knee=k))
    assert "◆" in chart and "●" in chart and "VU" in chart
    # 포화 안 됐으면 ◆ 를 찍지 않는다.
    lv2 = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 20, 8: 40}))
    chart2 = "\n".join(analysis.ascii_chart(lv2, knee=analysis.find_knee(lv2)))
    assert "◆" not in chart2


def test_format_saturation_은_표와_곡선과_무릎점을_낸다():
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 18, 8: 26, 16: 27}))
    sat = {"warmup_frac": 0.3, "levels": levels, "knee": analysis.find_knee(levels)}
    out = "\n".join(report.format_saturation(sat))
    assert "완료TPS" in out and "포화 VU ≈ 8" in out and "과도구간" in out
    # 수준이 하나뿐이면 빈 목록.
    assert report.format_saturation({"levels": analysis.per_vu_levels(
        _staircase_timeline({4: 10})), "knee": None}) == []


@pytest.mark.skipif(not charts.available(), reason="Pillow 미설치(에어갭 번들 등)")
def test_charts_png_은_Pillow_로_파일을_만든다(tmp_path):
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 18, 8: 26, 16: 27, 32: 25}))
    k = analysis.find_knee(levels)
    png = tmp_path / "sat.png"
    assert charts.render_saturation_png(levels, k, str(png))
    assert png.exists() and png.stat().st_size > 1000
    # 점이 부족하면 False.
    assert not charts.render_saturation_png(levels[:1], k, str(tmp_path / "x.png"))


def test_charts_svg_는_의존성_없이_문자열을_만든다():
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 18, 8: 26}))
    svg = charts.render_saturation_svg(levels, analysis.find_knee(levels))
    assert svg.startswith("<svg") and "polyline" in svg and svg.rstrip().endswith("</svg>")


async def test_build_result_에_포화_분석이_들어간다():
    fake = FakeCoordinator(polls_to_finish=1)
    stages = []
    for lv in (1, 2, 4):
        stages += [scenario.Stage(0.0, lv), scenario.Stage(1.5, lv)]
    runner = await _run(fake, stages=stages)
    res = report.build_result(runner, {"run_id": "r", "coordinators": ["c"], "scenario": "s",
                                       "request_type": "async"}, warmup_frac=0.0)
    levels = res["saturation"]["levels"]
    vus = [l["vus"] for l in levels]
    # 계단으로 올렸으니 서로 다른 수준이 둘 이상 잡히고 최고 수준(4)이 포함된다.
    assert len(levels) >= 2 and max(vus) == 4


# ───────────────────────── saturation 명령 ─────────────────────────

def test_build_saturation_stages_는_각_수준을_즉시전이_후_유지한다():
    stages = cli.build_saturation_stages({"levels": "1,2,4", "step_duration": "30s"})
    # 수준마다 (0초 전이, 30초 유지) 두 단계.
    assert len(stages) == 6
    assert stages[0].duration == 0.0 and stages[0].target == 1
    assert stages[1].duration == 30.0 and stages[1].target == 1
    assert stages[5].target == 4 and stages[5].duration == 30.0
    assert scenario.max_vus(stages) == 4


@pytest.mark.parametrize("levels,match", [
    ("1,x,4", "정수 목록"),
    ("0,2", "1 이상"),
    ("", "하나 이상"),
])
def test_build_saturation_stages_는_잘못된_levels_를_거부한다(levels, match):
    with pytest.raises(cli.ConfigError, match=match):
        cli.build_saturation_stages({"levels": levels, "step_duration": "10s"})


def test_saturation_명령은_계단을_구성해_옵션에_담는다():
    parser = cli.build_parser()
    opts = cli.merge_options(parser.parse_args(
        ["saturation", "--body", "j.json", "--levels", "1,2,4,8", "--step-duration", "45s"]))
    assert opts["levels"] == "1,2,4,8" and opts["step_duration"] == "45s"
    stages = cli.build_saturation_stages(opts)
    assert scenario.max_vus(stages) == 8


def test_write_chart_는_svg_를_의존성_없이_쓴다(tmp_path, capsys):
    levels = analysis.per_vu_levels(_staircase_timeline({1: 5, 2: 10, 4: 18, 8: 26}))
    sat = {"levels": levels, "knee": analysis.find_knee(levels)}
    svg = tmp_path / "c.svg"
    cli._write_chart(str(svg), sat)
    assert svg.exists() and svg.read_text(encoding="utf-8").startswith("<svg")
    assert "SVG" in capsys.readouterr().err
