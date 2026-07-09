"""local_stage 모드: executor 로컬 CSV export(Phase 1) → GP file:// 적재(Phase 2).

실 DB/실 디스크 없이 MockBackend 계열 레코더로 배선(라우팅·2-phase·검증)만 검증한다.
CSV 기본 구분자는 backtick(`)이며 설정으로 바꿀 수 있다는 계약도 함께 확인한다.
"""

from __future__ import annotations

import asyncio

import httpx

from coordinator import stage as stage_sql
from coordinator.config import settings as core_settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.models import Job, JobStatus, Task, TaskStatus
from executor.app import create_app as create_executor_app
from executor.backend import MockBackend


# ───────────────────── coordinator/stage.py 순수 함수 ─────────────────────


def test_external_table_name_is_safe():
    assert stage_sql.external_table_name("job_a1-b2") == "ext_job_a1_b2"


def test_open_impala_cursor_convert_types_and_fallback():
    """export 커서가 convert_types=False 를 넘기고, 구버전 impyla 면 기본 커서로 폴백."""
    from executor.backend import ImpalaToGreenplumBackend

    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x")  # 연결 없음(pool 은 lazy)

    class _Cur:
        def __init__(self, **kw):
            self.kw = kw

    class _ConnNew:
        def cursor(self, **kw):
            return _Cur(**kw)

    # convert_types=False → kwarg 로 전달
    assert be._open_source_cursor(_ConnNew(), convert_types=False).kw == {"convert_types": False}
    # None → 기본 커서(kwarg 없음)
    assert be._open_source_cursor(_ConnNew(), convert_types=None).kw == {}

    class _ConnOld:
        def cursor(self, **kw):
            if "convert_types" in kw:
                raise TypeError("unexpected keyword argument 'convert_types'")
            return _Cur(**kw)

    # 구버전 impyla(kwarg 미지원) → 기본 커서로 폴백
    assert be._open_source_cursor(_ConnOld(), convert_types=False).kw == {}


def test_host_of_from_url_and_fallback():
    assert stage_sql.host_of("http://seg1.gp:8087") == "seg1.gp"
    assert stage_sql.host_of(None) == ""
    assert stage_sql.host_of("", fallback="h") == "h"


def test_build_external_ddl_backtick_default_and_uris():
    ddl = stage_sql.build_external_ddl(
        "ext_j",
        "a int, dt date",
        [("seg1", "/data/j/f0.csv"), ("seg2", "/data/j/f1.csv")],
        {"delimiter": "`", "null": "", "quote": '"'},
    )
    assert ddl.startswith("CREATE EXTERNAL TABLE ext_j (a int, dt date)")
    assert "'file://seg1/data/j/f0.csv'" in ddl
    assert "'file://seg2/data/j/f1.csv'" in ddl
    assert "DELIMITER '`'" in ddl  # 기본 구분자 backtick
    assert "FORMAT 'CSV'" in ddl


def test_build_external_ddl_no_host_uses_bare_file_uri():
    ddl = stage_sql.build_external_ddl("ext_j", "a int", [("", "/data/j/f0.csv")], None)
    assert "'file:///data/j/f0.csv'" in ddl  # host 없으면 file:// + 절대경로


def test_build_pre_delete_only_for_values():
    assert stage_sql.build_pre_delete("public.t", "dt", []) is None
    sql = stage_sql.build_pre_delete("public.t", "dt", ["'1'", "'2'"])
    assert sql == "DELETE FROM public.t WHERE dt IN ('1', '2')"


def test_resolve_csv_options_defaults_and_override():
    job = _local_stage_job(n=1)
    opts = stage_sql.resolve_csv_options(job, core_settings)
    assert opts["delimiter"] == "`"  # 설정 기본값
    job2 = _local_stage_job(n=1, csv_delimiter="|")
    assert stage_sql.resolve_csv_options(job2, core_settings)["delimiter"] == "|"


# ───────────────────── executor 라우팅 (Phase 1 + cleanup) ─────────────────────


class _ExecRecorder(MockBackend):
    """export_to_local_csv 호출 인자를 기록하는 executor 백엔드."""

    def __init__(self):
        super().__init__()
        self.export = None

    def export_to_local_csv(self, sub_query, out_path, csv_options=None, on_progress=None, query_options=None, on_stage=None):
        self.export = (sub_query, out_path, csv_options)
        if on_progress:
            on_progress(9)
        return 9


async def _run_task(app, payload):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(200):
            st = (await c.get(f"/tasks/{payload['task_id']}")).json()
            if st["status"] in ("DONE", "FAILED", "CANCELLED"):
                return st
            await asyncio.sleep(0.01)
        return st


async def test_executor_local_stage_calls_export():
    backend = _ExecRecorder()
    payload = {
        "task_id": "te",
        "job_id": "j",
        "sub_query": "SELECT a, dt FROM imp WHERE dt IN ('1')",
        "target_table": "public.t",
        "partition_column": "dt",
        "partition_values": ["'1'"],
        "exec_mode": "local_stage",
        "out_path": "/tmp/stage/j/f0.csv",
        "csv_options": {"delimiter": "`", "null": "", "quote": '"'},
    }
    st = await _run_task(create_executor_app(backend=backend), payload)
    assert st["status"] == "DONE"
    assert st["rows_written"] == 9
    sub_query, out_path, csv_options = backend.export
    assert out_path == "/tmp/stage/j/f0.csv"
    assert csv_options["delimiter"] == "`"


def test_executor_cleanup_endpoint(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(core_settings, "stage_local_dir", str(tmp_path), raising=False)
    job_dir = tmp_path / "job_x"
    job_dir.mkdir()
    (job_dir / "f0.csv").write_text("a`b\n")

    client = TestClient(create_executor_app(backend=MockBackend()))
    resp = client.post("/stage/job_x/cleanup")
    assert resp.status_code == 200
    assert resp.json()["removed"] is True
    assert not job_dir.exists()
    # 멱등: 없는 job 을 정리하면 removed=False 로 조용히 통과.
    assert client.post("/stage/none/cleanup").json()["removed"] is False


# ───────────────────── LocalDispatcher 2-phase end-to-end ─────────────────────


class _StageRecorder(MockBackend):
    """export(Phase 1)와 load_external_csv(Phase 2) 호출을 기록하는 백엔드."""

    def __init__(self, rows_per_value=7, load_rows=42):
        super().__init__(rows_per_value=rows_per_value)
        self.load_rows = load_rows
        self.exports = []  # (sub_query, out_path, csv_options)
        self.loads = []    # kwargs dict

    def export_to_local_csv(self, sub_query, out_path, csv_options=None, on_progress=None, query_options=None, on_stage=None):
        self.exports.append((sub_query, out_path, csv_options))
        if on_progress:
            on_progress(self.rows_per_value)
        return self.rows_per_value

    def load_external_csv(self, external_ddl, staging_ddl, staging_load_sql, pre_delete_sql, insert_sql, cleanup_sqls=None, on_stage=None):
        self.loads.append({
            "external_ddl": external_ddl,
            "staging_ddl": staging_ddl,
            "staging_load_sql": staging_load_sql,
            "pre_delete_sql": pre_delete_sql,
            "insert_sql": insert_sql,
            "cleanup_sqls": cleanup_sqls,
        })
        return self.load_rows


def _local_stage_job(n=3, write_mode="append", url=None, **over):
    job = Job(
        original_sql="SELECT a, dt FROM t WHERE dt IN ('1','2','3')",
        partition_column="dt",
        target_table="public.t",
        write_mode=write_mode,
        parallelism=n,
        split_strategy="contiguous",
        failure_policy=over.pop("failure_policy", "fail_fast"),
        exec_mode="local_stage",
        external_columns="a int, dt text",
        staging_table="stg",
        insert_sql="INSERT INTO public.t SELECT * FROM stg",
        **over,
    )
    job.tasks = [
        Task(
            job_id=job.job_id,
            executor_url=url,
            sub_query=f"SELECT {i}",
            partition_values=[f"'{i + 1}'"],
            out_path=f"/tmp/stage/{job.job_id}/f{i}.csv",
        )
        for i in range(n)
    ]
    return job


async def test_local_stage_two_phase_end_to_end():
    backend = _StageRecorder()
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _local_stage_job(n=3)
    await disp.run(job)

    assert job.status == JobStatus.DONE
    # Phase 1: task 마다 export 1회.
    assert len(backend.exports) == 3
    assert {e[1] for e in backend.exports} == {
        f"/tmp/stage/{job.job_id}/f{i}.csv" for i in range(3)
    }
    # Phase 2: job 당 load 1회, coordinator 가 조립한 SQL 전달.
    assert len(backend.loads) == 1
    load = backend.loads[0]
    assert "CREATE EXTERNAL TABLE" in load["external_ddl"]
    assert "DELIMITER '`'" in load["external_ddl"]  # backtick 기본 구분자 계약
    assert load["staging_load_sql"] == "INSERT INTO stg SELECT * FROM ext_" + \
        "".join(c if c.isalnum() else "_" for c in job.job_id)
    assert load["insert_sql"] == "INSERT INTO public.t SELECT * FROM stg"
    assert load["pre_delete_sql"] is None  # append → 선삭제 없음
    assert load["cleanup_sqls"] and "DROP EXTERNAL TABLE" in load["cleanup_sqls"][0]


async def test_local_stage_overwrite_builds_pre_delete():
    backend = _StageRecorder()
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _local_stage_job(n=3, write_mode="overwrite_partitions")
    await disp.run(job)
    assert job.status == JobStatus.DONE
    pre = backend.loads[0]["pre_delete_sql"]
    assert pre is not None
    assert pre.startswith("DELETE FROM public.t WHERE dt IN (")
    # 모든 task 의 파티션 값이 선삭제 IN 절에 모인다.
    assert "'1'" in pre and "'2'" in pre and "'3'" in pre


async def test_local_stage_phase2_skipped_when_export_fails():
    class _ExportFails(_StageRecorder):
        def export_to_local_csv(self, sub_query, out_path, csv_options=None, on_progress=None, query_options=None, on_stage=None):
            raise RuntimeError("impala down")

    backend = _ExportFails()
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _local_stage_job(n=2)
    await disp.run(job)
    assert job.status == JobStatus.FAILED  # export 실패
    assert len(backend.loads) == 0  # Phase 2 는 실행되지 않음(배리어 후 실패 감지)


async def test_local_stage_phase2_failure_marks_job_failed_even_best_effort():
    class _LoadFails(_StageRecorder):
        def load_external_csv(self, *a, **k):
            raise RuntimeError("gp boom")

    backend = _LoadFails()
    disp = LocalDispatcher(core_settings, backend=backend)
    # best_effort 여도 local_stage 는 단일 원자 적재라 Phase 2 실패면 FAILED.
    job = _local_stage_job(n=2, failure_policy="best_effort")
    await disp.run(job)
    assert job.status == JobStatus.FAILED
    assert "Phase 2" in (job.error or "")


# ───────────────────── coordinator 검증/생성 (POST /jobs) ─────────────────────


def _job_payload(**over):
    base = {
        "sql": "SELECT a, dt FROM imp WHERE dt IN ('1','2','3')",
        "partition_column": "dt",
        "target_table": "public.target",
        "parallelism": 3,
        "exec_mode": "local_stage",
        "staging_table": "stg",
        "external_columns": "a int, dt text",
        "insert_sql": "INSERT INTO public.target SELECT * FROM stg",
    }
    base.update(over)
    return base


def test_coordinator_local_stage_job_created(client, store):
    resp = client.post("/jobs", json=_job_payload())
    assert resp.status_code == 202
    job = store.get(resp.json()["job_id"])
    assert job.exec_mode == "local_stage"
    assert job.external_columns == "a int, dt text"
    assert job.insert_sql == "INSERT INTO public.target SELECT * FROM stg"
    assert len(job.tasks) == 3
    # out_path 확정: {stage_local_dir}/{job_id}/f{idx}.csv (분할 SELECT 는 래핑되지 않음).
    assert job.tasks[0].out_path.endswith(f"/{job.job_id}/f0.csv")
    assert job.tasks[2].out_path.endswith(f"/{job.job_id}/f2.csv")
    assert job.tasks[0].sub_query.startswith("SELECT a, dt FROM imp")


def test_coordinator_local_stage_dir_override(client, store):
    resp = client.post("/jobs", json=_job_payload(export_local_dir="/mnt/seg"))
    job = store.get(resp.json()["job_id"])
    assert job.tasks[0].out_path == f"/mnt/seg/{job.job_id}/f0.csv"


def test_coordinator_local_stage_missing_fields_rejected(client):
    for missing in ("staging_table", "external_columns", "insert_sql"):
        payload = _job_payload()
        del payload[missing]
        resp = client.post("/jobs", json=payload)
        assert resp.status_code == 422, missing
        assert resp.json()["error_code"] == "LOCAL_STAGE_REQUIRES_FIELDS"


# ───────────────────── gp_hostname 기반 host 매핑 + 검증 ─────────────────────


class _SegHosts(_StageRecorder):
    """segment_hosts()가 지정된 집합을 반환하는 백엔드(호스트 검증 테스트용)."""

    def __init__(self, hosts):
        super().__init__()
        self._hosts = set(hosts)

    def segment_hosts(self):
        return self._hosts


def test_executor_metrics_reports_gp_hostname(monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(core_settings, "executor_gp_hostname", "sdw9", raising=False)
    client = TestClient(create_executor_app(backend=MockBackend()))
    m = client.get("/metrics").json()
    assert m["gp_hostname"] == "sdw9"  # 설정 오버라이드가 그대로 노출


async def test_resolve_hosts_base_uses_url_or_setting(monkeypatch):
    disp = LocalDispatcher(core_settings, backend=MockBackend())
    # executor_url 이 있으면 URL 호스트, 없으면(None) 설정 gp_hostname.
    monkeypatch.setattr(core_settings, "executor_gp_hostname", "localseg", raising=False)
    job = _local_stage_job(n=1, url="http://seg1.gp:8087")
    assert (await disp._resolve_hosts(job))["http://seg1.gp:8087"] == "seg1.gp"
    job2 = _local_stage_job(n=1, url=None)
    assert (await disp._resolve_hosts(job2))[None] == "localseg"


async def test_http_resolve_hosts_prefers_reported_gp_hostname():
    from coordinator.dispatcher import HttpDispatcher

    disp = HttpDispatcher(core_settings)
    url = "http://10.0.1.5:8087"
    # executor 가 보고한 GP 호스트명이 URL 호스트(10.0.1.5)와 다른 상황을 캐시로 재현.
    disp._gp_host_cache[url] = "sdw1"
    job = _local_stage_job(n=1, url=url)
    assert (await disp._resolve_hosts(job))[url] == "sdw1"


async def test_http_resolve_hosts_fallback_to_url_on_failure():
    from coordinator.dispatcher import HttpDispatcher

    disp = HttpDispatcher(core_settings)
    # 도달 불가한 executor → /metrics 조회 실패 → URL 호스트로 폴백.
    job = _local_stage_job(n=1, url="http://127.0.0.1:1")
    assert (await disp._resolve_hosts(job))["http://127.0.0.1:1"] == "127.0.0.1"


async def test_http_dispatcher_uses_reported_gp_hostname_in_uri():
    from coordinator.dispatcher import HttpDispatcher

    disp = HttpDispatcher(core_settings)
    backend = _StageRecorder()
    disp._stage_backend = backend  # Phase 2 백엔드 주입(HTTP export 는 우회)
    url = "http://10.0.1.5:8087"
    disp._gp_host_cache[url] = "sdw1"  # executor 가 보고한 GP 호스트명
    job = _local_stage_job(n=1, url=url)
    for t in job.tasks:
        t.status = TaskStatus.DONE  # export 완료 상태로 두고 Phase 2 만 직접 구동
    await disp._run_stage_load(job)
    ddl = backend.loads[0]["external_ddl"]
    assert "file://sdw1/" in ddl  # URL 호스트가 아니라 보고된 gp_hostname 사용
    assert "10.0.1.5" not in ddl


async def test_local_stage_host_validation_fails_on_mismatch():
    backend = _SegHosts({"sdw1", "sdw2"})  # 실제 세그먼트 호스트
    disp = LocalDispatcher(core_settings, backend=backend)
    # 배정 호스트(seg1)가 gp_segment_configuration 에 없음 → 조기 실패.
    job = _local_stage_job(n=2, url="http://seg1:8087")
    await disp.run(job)
    assert job.status == JobStatus.FAILED
    assert "gp_segment_configuration" in (job.error or "")
    assert len(backend.loads) == 0  # 검증 실패 → Phase 2 미실행


async def test_local_stage_host_validation_passes():
    backend = _SegHosts({"seg1"})
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _local_stage_job(n=2, url="http://seg1:8087")
    await disp.run(job)
    assert job.status == JobStatus.DONE
    assert len(backend.loads) == 1
    assert "file://seg1/" in backend.loads[0]["external_ddl"]


# ───────────────────── 파일 예산 배분 (files_per_host ≤ S_h) ─────────────────────


def test_plan_file_budget_balances_across_hosts():
    plan = stage_sql.plan_file_budget(3, {"h1": 2, "h2": 2}, {"h1": ["e1"], "h2": ["e2"]})
    assert plan is not None and len(plan) == 3
    hosts = [h for _, h in plan]
    assert hosts.count("h1") <= 2 and hosts.count("h2") <= 2
    assert hosts.count("h1") + hosts.count("h2") == 3  # 고르게: 2/1


def test_plan_file_budget_infeasible_returns_none():
    # 4파일 > 용량(2+... )? h1=1,h2=1 → 용량 2 < 3
    assert stage_sql.plan_file_budget(3, {"h1": 1, "h2": 1}, {"h1": ["e1"], "h2": ["e2"]}) is None


def test_plan_file_budget_respects_max_per_host_cap():
    # S_h=4 지만 max_per_host=2 로 낮추면 호스트당 2까지만.
    assert stage_sql.plan_file_budget(3, {"h1": 4}, {"h1": ["e1"]}, max_per_host=2) is None
    plan = stage_sql.plan_file_budget(2, {"h1": 4}, {"h1": ["e1"]}, max_per_host=2)
    assert plan is not None and all(h == "h1" for _, h in plan)


def test_plan_file_budget_round_robins_executors_within_host():
    plan = stage_sql.plan_file_budget(4, {"h1": 4}, {"h1": ["e1", "e2"]})
    urls = [u for u, _ in plan]
    assert urls.count("e1") == 2 and urls.count("e2") == 2


def test_plan_file_budget_excludes_hosts_without_executors():
    # h2 는 세그먼트가 많아도 executor 가 없으면 용량에서 제외 → 용량 = h1 의 2.
    assert stage_sql.plan_file_budget(3, {"h1": 2, "h2": 5}, {"h1": ["e1"]}) is None
    plan = stage_sql.plan_file_budget(2, {"h1": 2, "h2": 5}, {"h1": ["e1"]})
    assert plan is not None and all(h == "h1" for _, h in plan)


def test_budget_capacity():
    assert stage_sql.budget_capacity({"h1": 2, "h2": 3}, {"h1": ["e1"], "h2": ["e2"]}) == 5
    assert stage_sql.budget_capacity({"h1": 2, "h2": 3}, {"h1": ["e1"]}) == 2  # h2 executor 없음
    assert stage_sql.budget_capacity({"h1": 4}, {"h1": ["e1"]}, max_per_host=2) == 2


class _TopoBackend(_StageRecorder):
    """segment_host_counts()가 지정된 {host: S_h}를 반환하는 백엔드(파일 예산 배분 테스트용)."""

    def __init__(self, counts):
        super().__init__()
        self._counts = dict(counts)

    def segment_host_counts(self):
        return self._counts


async def test_local_stage_file_budget_reassigns_across_hosts(monkeypatch):
    monkeypatch.setattr(
        core_settings, "executors",
        ["http://seg1:8087", "http://seg2:8087"], raising=False,
    )
    backend = _TopoBackend({"seg1": 2, "seg2": 2})  # 용량 4
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _local_stage_job(n=4, url=None)  # 4파일
    await disp.run(job)

    assert job.status == JobStatus.DONE
    from collections import Counter
    urls = [t.executor_url for t in job.tasks]
    assert set(urls) <= {"http://seg1:8087", "http://seg2:8087"}  # 재배정됨
    per_host = Counter(stage_sql.host_of(u) for u in urls)
    assert per_host["seg1"] <= 2 and per_host["seg2"] <= 2  # 호스트당 S_h 이하
    assert sum(per_host.values()) == 4
    # Phase 2 URI 가 배정된 두 호스트를 모두 참조.
    ddl = backend.loads[0]["external_ddl"]
    assert "file://seg1/" in ddl and "file://seg2/" in ddl


async def test_local_stage_file_budget_infeasible_fails_job(monkeypatch):
    monkeypatch.setattr(core_settings, "executors", ["http://seg1:8087"], raising=False)
    backend = _TopoBackend({"seg1": 2})  # 용량 2
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _local_stage_job(n=3, url=None)  # 3파일 > 2
    await disp.run(job)

    assert job.status == JobStatus.FAILED
    assert "예산" in (job.error or "")
    assert len(backend.exports) == 0  # 배치 불가 → Phase 1 건너뜀
    assert len(backend.loads) == 0    # Phase 2 도 미실행


async def test_local_stage_file_budget_skipped_without_topology(monkeypatch):
    # 토폴로지 미상(segment_host_counts 빈 dict)이면 기존 배정 유지, 정상 진행.
    monkeypatch.setattr(core_settings, "executors", ["http://seg1:8087"], raising=False)
    backend = _StageRecorder()  # segment_host_counts() = {} (MockBackend 기본)
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _local_stage_job(n=3, url=None)
    await disp.run(job)
    assert job.status == JobStatus.DONE
    assert len(backend.exports) == 3 and len(backend.loads) == 1
