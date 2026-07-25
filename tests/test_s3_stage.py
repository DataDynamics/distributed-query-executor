"""s3_stage 모드(2-phase, coordinator 중앙): executor 업로드 → coordinator PXF 적재.

local_stage 의 형제. Phase 1(executor): Impala→로컬 CSV→S3 업로드. 배리어. Phase 2
(coordinator): job 프리픽스로 PXF 외부테이블 하나 생성→target INSERT. Phase 3: S3 정리.
순수 SQL/키 조립(core.s3_stage) + backend Phase 1/2/3 + executor 라우팅/정리 엔드포인트 +
coordinator 검증/전달 + LocalDispatcher 2-phase e2e + 템플릿 렌더/날짜 fan-out 을 검증한다.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from core import s3_stage as s3sql
from coordinator.app import create_app
from coordinator.config import settings as coord_settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.job_store import JobStore
from coordinator.models import JobStatus
from executor.app import create_app as create_executor_app
from executor.backend import ImpalaToGreenplumBackend, MockBackend

REPO_TEMPLATES = Path(__file__).resolve().parent.parent / "templates"


# ───────────────────── 순수 함수(core.s3_stage) ─────────────────────


def test_s3_object_key_and_job_prefix():
    assert s3sql.s3_object_key("dqe-stage", "job_x", "t_y") == "dqe-stage/job_x/t_y.csv"
    assert s3sql.s3_object_key("/a/b/", "j", "t") == "a/b/j/t.csv"
    assert s3sql.s3_object_key("", "j", "t") == "j/t.csv"
    # 모든 task 키가 job 프리픽스 아래에 모인다.
    assert s3sql.s3_job_prefix("dqe-stage", "job_x") == "dqe-stage/job_x/"
    assert s3sql.s3_object_key("dqe-stage", "job_x", "t_y").startswith(
        s3sql.s3_job_prefix("dqe-stage", "job_x"))


def test_external_table_name_job_unique_and_safe():
    assert s3sql.external_table_name("job_abc-1") == "s3ext_job_abc_1"
    # job 마다 다르다(동시 실행 job 간 카탈로그 충돌 방지).
    assert s3sql.external_table_name("a") != s3sql.external_table_name("b")


def test_build_s3_location_default_pxf_directory():
    prefix = s3sql.s3_job_prefix("dqe-stage", "j")
    loc = s3sql.build_s3_location("mybkt", prefix, profile="s3:csv", server="s3srv")
    assert loc == "pxf://mybkt/dqe-stage/j/?PROFILE=s3:csv&SERVER=s3srv"


def test_build_s3_location_without_server_and_template_override():
    assert "SERVER" not in s3sql.build_s3_location("b", "k/", server="")
    loc = s3sql.build_s3_location(
        "b", "k/", profile="s3:text", server="srv",
        location_template="s3a://{bucket}/{key}?p={profile}&s={server}",
    )
    assert loc == "s3a://b/k/?p=s3:text&s=srv"


def test_build_s3_external_ddl_and_helpers():
    loc = s3sql.build_s3_location("b", "dqe/j/", server="srv")
    ddl = s3sql.build_s3_external_ddl(
        "s3ext_j", "id int, dt date", loc, {"delimiter": "`", "null": "", "quote": '"'})
    assert ddl.startswith("CREATE EXTERNAL TABLE s3ext_j (id int, dt date)")
    assert "LOCATION ('pxf://b/dqe/j/?PROFILE=s3:csv&SERVER=srv')" in ddl
    assert "FORMAT 'CSV' ( DELIMITER '`' NULL '' QUOTE '\"' )" in ddl
    assert s3sql.build_pre_delete("t", "dt", []) is None
    assert s3sql.build_pre_delete("t", "dt", ["'1'"]) == "DELETE FROM t WHERE dt IN ('1')"
    assert s3sql.build_cleanup_ddl("s3ext_j") == "DROP EXTERNAL TABLE IF EXISTS s3ext_j"


# ───────────────────── backend Phase 1/2/3(가짜 S3/GP) ─────────────────────


class _FakeS3Client:
    def __init__(self):
        self.uploaded = []   # (local_path, key)
        self.deleted_prefixes = []
        self.objects = set()  # 업로드된 키(프리픽스 삭제 시뮬레이션용)

    def upload(self, local_path, key):
        self.uploaded.append((local_path, key))
        self.objects.add(key)

    def delete(self, key):
        self.objects.discard(key)

    def delete_prefix(self, prefix):
        gone = {k for k in self.objects if k.startswith(prefix)}
        self.objects -= gone
        self.deleted_prefixes.append(prefix)
        return len(gone)


class _FakeGpCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    @property
    def rowcount(self):
        return 7

    def execute(self, sql):
        self.conn.executed.append(sql)


class _FakeGpConn:
    def __init__(self):
        self.executed = []

    def cursor(self):
        return _FakeGpCursor(self)

    def commit(self):
        self.executed.append("COMMIT")


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    @contextmanager
    def connection(self):
        yield self._conn


def _backend(gp_conn=None, s3_client=None, s3_config=None):
    cfg = {"bucket": "mybkt", "prefix": "dqe-stage", "pxf_server": "s3srv",
           "local_tmp_dir": "/tmp/dqe-test"}
    cfg.update(s3_config or {})
    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x",
                                  s3_config=cfg, s3_client=s3_client)
    if gp_conn is not None:
        be._gp_pool = _FakePool(gp_conn)
    return be


def test_export_to_s3_uploads_and_removes_local(tmp_path):
    s3 = _FakeS3Client()
    be = _backend(s3_client=s3, s3_config={"local_tmp_dir": str(tmp_path)})
    # export_to_local_csv 를 스텁: 실제 파일을 하나 만들어 업로드 대상이 존재하게 한다.
    written = {}

    def _fake_export(sql, out_path, csv_options, on_progress, query_options=None, on_stage=None):
        import os
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w") as f:
            f.write("row\n")
        written["path"] = out_path
        return 42

    be.export_to_local_csv = _fake_export
    n = be.export_to_s3("SELECT 1", "dqe-stage/job_x/t_y.csv", "job_x", "t_y", {})
    assert n == 42
    assert s3.uploaded == [(written["path"], "dqe-stage/job_x/t_y.csv")]
    # 업로드 후 로컬 임시 파일은 삭제됐다.
    import os
    assert not os.path.exists(written["path"])


def test_load_external_s3_runs_ddl_insert_cleanup():
    conn = _FakeGpConn()
    be = _backend(gp_conn=conn)
    ext_ddl = "CREATE EXTERNAL TABLE s3ext_j (id int) LOCATION ('pxf://b/dqe/j/?PROFILE=s3:csv')\n  FORMAT 'CSV' ( )"
    n = be.load_external_s3(
        ext_ddl,
        "DELETE FROM public.t WHERE dt IN ('1')",
        "INSERT INTO public.t SELECT * FROM s3ext_j",
        ["DROP EXTERNAL TABLE IF EXISTS s3ext_j"],
    )
    assert n == 7
    joined = " | ".join(conn.executed)
    # 외부테이블 생성 → 선삭제 → INSERT → COMMIT → (별도 tx) DROP.
    assert conn.executed[0].startswith("CREATE EXTERNAL TABLE s3ext_j")
    assert "DELETE FROM public.t" in joined
    assert "INSERT INTO public.t SELECT * FROM s3ext_j" in joined
    assert "COMMIT" in conn.executed
    assert any(s.startswith("DROP EXTERNAL TABLE IF EXISTS s3ext_j") for s in conn.executed)


def test_load_external_s3_append_skips_delete():
    conn = _FakeGpConn()
    be = _backend(gp_conn=conn)
    be.load_external_s3("CREATE EXTERNAL TABLE s3ext_j (id int)", None,
                        "INSERT INTO t SELECT * FROM s3ext_j", None)
    assert not any(s.strip().upper().startswith("DELETE") for s in conn.executed)


def test_cleanup_s3_prefix_deletes_objects():
    s3 = _FakeS3Client()
    s3.objects.update({"dqe-stage/job_x/a.csv", "dqe-stage/job_x/b.csv",
                       "dqe-stage/other/c.csv"})
    be = _backend(s3_client=s3)
    deleted = be.cleanup_s3_prefix("dqe-stage/job_x/")
    assert deleted == 2
    assert s3.objects == {"dqe-stage/other/c.csv"}  # 다른 job 은 안 지운다


def test_export_to_s3_missing_bucket_raises(tmp_path):
    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x",
                                  s3_config={"local_tmp_dir": str(tmp_path)})
    be.export_to_local_csv = lambda *a, **k: 1
    raised = False
    try:
        be.export_to_s3("SELECT 1", "k.csv", "j", "t", {})
    except ValueError as exc:
        raised = "s3.bucket" in str(exc)
    assert raised


# ───────────────────── executor task 라우팅 + 정리 엔드포인트 ─────────────────────


def _task_payload(task_id, **over):
    base = {
        "task_id": task_id, "job_id": "j",
        "sub_query": "SELECT a, dt FROM imp WHERE dt IN ('1')",
        "target_table": "public.target", "write_mode": "append",
        "partition_column": "dt", "partition_values": ["'1'"],
        "exec_mode": "s3_stage",
        "out_path": "dqe-stage/j/" + task_id + ".csv",  # coordinator 가 준 S3 키
        "csv_options": {"delimiter": "`", "null": "", "quote": '"'},
    }
    base.update(over)
    return base


async def _run_task(app, payload):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(200):
            st = (await c.get(f"/tasks/{payload['task_id']}")).json()
            if st["status"] in ("DONE", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
        return st


async def test_executor_s3_stage_phase1_routes_to_export():
    st = await _run_task(create_executor_app(backend=MockBackend(rows_per_value=9)),
                         _task_payload("s1"))
    assert st["status"] == "DONE"
    assert st["rows_written"] == 9


def test_executor_s3_cleanup_endpoint():
    app = create_executor_app(backend=MockBackend())
    client = TestClient(app)
    resp = client.post("/s3/job_x/cleanup")
    assert resp.status_code == 200
    # MockBackend.cleanup_s3_prefix → 0 (지울 것 없음), 엔드포인트는 정상 응답.
    assert resp.json() == {"job_id": "job_x", "deleted": 0}


# ───────────────────── coordinator 검증/전달 ─────────────────────


def _job_payload(**over):
    base = {
        "sql": "SELECT a, dt FROM imp WHERE dt IN ('1','2','3')",
        "partition_column": "dt", "target_table": "public.target",
        "parallelism": 3, "exec_mode": "s3_stage", "staging_table": "stg_t",
        "external_columns": "a int, dt date",
        "insert_sql": "INSERT INTO public.target SELECT * FROM stg_t",
    }
    base.update(over)
    return base


def test_coordinator_s3_stage_job_and_keys(client, store):
    resp = client.post("/jobs", json=_job_payload())
    assert resp.status_code == 202
    job = store.get(resp.json()["job_id"])
    assert job.exec_mode == "s3_stage"
    assert job.external_columns == "a int, dt date"
    assert job.insert_sql.startswith("INSERT INTO public.target")
    # 각 task 의 out_path 는 job 프리픽스 아래의 S3 키다.
    prefix = s3sql.s3_job_prefix(coord_settings.s3_prefix, job.job_id)
    for t in job.tasks:
        assert t.out_path.startswith(prefix) and t.out_path.endswith(".csv")
    # sub-query 는 분할된 SELECT 그대로.
    assert job.tasks[0].sub_query.startswith("SELECT a, dt FROM imp")


def test_coordinator_s3_stage_missing_fields_rejected(client):
    payload = _job_payload()
    del payload["external_columns"]
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "S3_STAGE_REQUIRES_FIELDS"


def test_coordinator_s3_stage_dry_run(client):
    resp = client.post("/jobs", json=_job_payload(dry_run=True))
    assert resp.status_code == 200
    b = resp.json()
    assert b["dry_run"] is True and b["exec_mode"] == "s3_stage"
    assert b["tasks"][0]["external_columns"] == "a int, dt date"
    assert b["tasks"][0]["insert_sql"].startswith("INSERT INTO public.target")


# ───────────────────── LocalDispatcher 2-phase e2e ─────────────────────


class _MockS3StageBackend(MockBackend):
    """s3_stage 2-phase 를 in-process 로 검증하는 목 백엔드.

    Phase 1(export_to_s3): 업로드 키를 기록하고 rows 반환. Phase 2(load_external_s3):
    coordinator 가 조립한 external_ddl/insert_sql 을 기록하고 rows 반환. Phase 3
    (cleanup_s3_prefix): 정리 프리픽스 기록.
    """

    def __init__(self, rows_per_value=100):
        super().__init__(rows_per_value)
        self.uploaded_keys = []
        self.phase2 = None   # (external_ddl, pre_delete, insert_sql, cleanup)
        self.cleaned = []

    def export_to_s3(self, impala_select, key, job_id, task_id, csv_options=None,
                     on_progress=None, query_options=None, on_stage=None):
        self.uploaded_keys.append(key)
        if on_progress:
            on_progress(self.rows_per_value)
        return self.rows_per_value

    def load_external_s3(self, external_ddl, pre_delete_sql, insert_sql, cleanup_sqls=None,
                         on_stage=None):
        self.phase2 = (external_ddl, pre_delete_sql, insert_sql, cleanup_sqls)
        return 55

    def cleanup_s3_prefix(self, prefix):
        self.cleaned.append(prefix)
        return len(self.uploaded_keys)


def _s3_settings(monkeypatch):
    monkeypatch.setattr(coord_settings, "s3_bucket", "mybkt", raising=False)
    monkeypatch.setattr(coord_settings, "s3_prefix", "dqe-stage", raising=False)
    monkeypatch.setattr(coord_settings, "s3_pxf_server", "s3srv", raising=False)
    monkeypatch.setattr(coord_settings, "s3_pxf_profile", "s3:csv", raising=False)
    monkeypatch.setattr(coord_settings, "s3_gp_location_template", "", raising=False)
    monkeypatch.setattr(coord_settings, "s3_delete_on_cleanup", True, raising=False)
    monkeypatch.setattr(coord_settings, "executors", [], raising=False)
    monkeypatch.setattr(coord_settings, "executor_mode", "local", raising=False)


async def test_localdispatcher_s3_stage_two_phase(monkeypatch):
    _s3_settings(monkeypatch)
    backend = _MockS3StageBackend()
    store = JobStore()
    disp = LocalDispatcher(coord_settings, backend=backend, store=store)
    app = create_app(runner=disp, store=store, settings=coord_settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        resp = await c.post("/jobs", json=_job_payload(parallelism=3, write_mode="overwrite_partitions"))
        assert resp.status_code == 202, resp.text
        job_id = resp.json()["job_id"]
        for _ in range(300):
            st = (await c.get(f"/jobs/{job_id}/status")).json()
            if st["status"] in ("DONE", "PARTIAL", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
    assert st["status"] == "DONE", st
    # Phase 1: 각 task 가 job 프리픽스 아래 고유 키로 업로드했다.
    prefix = s3sql.s3_job_prefix("dqe-stage", job_id)
    assert len(backend.uploaded_keys) == 3
    assert all(k.startswith(prefix) and k.endswith(".csv") for k in backend.uploaded_keys)
    # Phase 2: coordinator 가 job 프리픽스로 외부테이블 하나를 만들고 INSERT 했다.
    ext_ddl, pre_delete, insert_sql, cleanup = backend.phase2
    ext_name = s3sql.external_table_name(job_id)
    assert f"CREATE EXTERNAL TABLE {ext_name}" in ext_ddl
    assert f"pxf://mybkt/{prefix}?PROFILE=s3:csv&SERVER=s3srv" in ext_ddl
    # insert_sql 의 staging 참조가 job 고유 외부테이블 이름으로 치환됐다.
    assert f"FROM {ext_name}" in insert_sql
    assert "public.target" in insert_sql
    # overwrite_partitions → 선삭제 존재.
    assert pre_delete and pre_delete.startswith("DELETE FROM public.target")
    # Phase 3: S3 스테이징 프리픽스 정리.
    assert backend.cleaned == [prefix]


# ───────────────────── 템플릿 렌더 + 날짜 fan-out ─────────────────────


@pytest.fixture
def tpl_client(monkeypatch):
    monkeypatch.setattr(coord_settings, "template_enabled", True, raising=False)
    monkeypatch.setattr(coord_settings, "template_dir", str(REPO_TEMPLATES), raising=False)
    monkeypatch.setattr(coord_settings, "template_auto_reload", False, raising=False)
    monkeypatch.setattr(coord_settings, "template_func_modules", [], raising=False)
    monkeypatch.setattr(coord_settings, "executors", [], raising=False)
    return TestClient(create_app(store=JobStore()))


def test_s3_stage_template_renders(tpl_client):
    resp = tpl_client.post("/jobs", json={
        "template_id": "sales_migration_s3",
        "params": [
            {"name": "start_dt", "value": "2026-07-01"},
            {"name": "end_dt", "value": "2026-07-02"},
        ],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    b = resp.json()
    assert b["exec_mode"] == "s3_stage"
    assert b["target_table"] == "public.sales"
    task = b["tasks"][0]
    assert task["external_columns"].startswith("user_id bigint")
    assert task["insert_sql"].startswith("INSERT INTO public.sales")
    assert task["sub_query"].startswith("SELECT user_id")


def test_s3_stage_fanout_one_task_per_day(tpl_client, tmp_path, monkeypatch):
    tpl = tmp_path / "daily_s3"
    tpl.mkdir()
    (tpl / "manifest.yml").write_text(
        "id: daily_s3\nexec_mode: s3_stage\ntarget_table: public.sales\n"
        "staging_table: stg_s3\npartition_column: dt\nstrict_validation: false\n"
        "sql_dialect: trino\ntask_bound: point\n"
        "params:\n  - {name: from_date_no, type: int}\n  - {name: to_date_no, type: int}\n"
        "files:\n  select: select.sql.j2\n  insert: insert.sql.j2\n"
        "  external_columns: ext.sql.j2\n",
        encoding="utf-8",
    )
    (tpl / "select.sql.j2").write_text(
        "SELECT id, dt FROM sales WHERE dt = current_date() "
        "{{ from_date_no_sign | sql_sign }} interval {{ from_date_no | sql_num }} day\n",
        encoding="utf-8",
    )
    (tpl / "insert.sql.j2").write_text(
        "INSERT INTO public.sales SELECT * FROM stg_s3\n", encoding="utf-8")
    (tpl / "ext.sql.j2").write_text("id bigint, dt date\n", encoding="utf-8")
    monkeypatch.setattr(coord_settings, "template_dir", str(tmp_path), raising=False)
    client = TestClient(create_app(store=JobStore()))

    resp = client.post("/jobs", json={
        "template_id": "daily_s3",
        "params": [
            {"name": "from_date_no", "value": 2, "sign": "-"},
            {"name": "to_date_no", "value": 0, "sign": "+"},
        ],
        "task_params": ["from_date_no", "to_date_no"],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    b = resp.json()
    assert b["exec_mode"] == "s3_stage"
    assert b["task_count"] == 3
    assert b["tasks"][0]["external_columns"].startswith("id bigint")
    assert b["tasks"][0]["insert_sql"].startswith("INSERT INTO public.sales")
