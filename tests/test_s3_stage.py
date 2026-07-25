"""s3_stage 모드: Impala→로컬 CSV→S3 업로드→GP PXF 외부테이블→target INSERT→S3 정리.

local_stage 의 형제지만 세그먼트 co-locate/파일예산 배분이 없고, task 하나가 자체 완결한다.
순수 SQL/키 조립(core.s3_stage) + coordinator 이름 고유화(coordinator.stage.per_task_external)
+ 백엔드 흐름(가짜 S3/GP) + coordinator 검증/전달 + 날짜 fan-out 연동을 검증한다.
"""

from __future__ import annotations

import asyncio
import datetime
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from core import s3_stage as s3sql
from core.timeutil import now_dt
from coordinator import stage as stage_sql
from coordinator.app import create_app
from coordinator.job_store import JobStore
from executor.app import create_app as create_executor_app
from executor.backend import ImpalaToGreenplumBackend, MockBackend

REPO_TEMPLATES = Path(__file__).resolve().parent.parent / "templates"


# ───────────────────── 순수 함수(core.s3_stage) ─────────────────────


def test_s3_object_key_normalizes_prefix():
    assert s3sql.s3_object_key("dqe-stage", "job_x", "t_y") == "dqe-stage/job_x/t_y.csv"
    assert s3sql.s3_object_key("/a/b/", "j", "t") == "a/b/j/t.csv"
    assert s3sql.s3_object_key("", "j", "t") == "j/t.csv"


def test_build_s3_location_default_pxf():
    key = s3sql.s3_object_key("dqe-stage", "j", "t")
    loc = s3sql.build_s3_location("mybkt", key, profile="s3:csv", server="s3srv")
    assert loc == "pxf://mybkt/dqe-stage/j/t.csv?PROFILE=s3:csv&SERVER=s3srv"


def test_build_s3_location_without_server_omits_server():
    loc = s3sql.build_s3_location("mybkt", "k.csv", profile="s3:csv", server="")
    assert loc == "pxf://mybkt/k.csv?PROFILE=s3:csv"
    assert "SERVER" not in loc


def test_build_s3_location_template_override():
    loc = s3sql.build_s3_location(
        "mybkt", "k.csv", profile="s3:text", server="srv",
        location_template="s3://{bucket}/{key}?PROFILE={profile}&SERVER={server}",
    )
    assert loc == "s3://mybkt/k.csv?PROFILE=s3:text&SERVER=srv"


def test_build_s3_external_ddl_has_location_and_format():
    loc = s3sql.build_s3_location("b", "k.csv", server="srv")
    ddl = s3sql.build_s3_external_ddl(
        "public.stg", "id int, dt date", loc,
        {"delimiter": "`", "null": "", "quote": '"'},
    )
    assert ddl.startswith("CREATE EXTERNAL TABLE public.stg (id int, dt date)")
    assert "LOCATION ('pxf://b/k.csv?PROFILE=s3:csv&SERVER=srv')" in ddl
    assert "FORMAT 'CSV' ( DELIMITER '`' NULL '' QUOTE '\"' )" in ddl


def test_build_pre_delete_and_cleanup():
    assert s3sql.build_pre_delete("t", "dt", []) is None
    assert s3sql.build_pre_delete("t", "dt", ["'1'", "'2'"]) == \
        "DELETE FROM t WHERE dt IN ('1', '2')"
    assert s3sql.build_cleanup_ddl("ext") == "DROP EXTERNAL TABLE IF EXISTS ext"


# ───────────────────── 이름 고유화(coordinator.stage) ─────────────────────


def test_per_task_external_uniquifies_name_and_insert():
    name, insert = stage_sql.per_task_external(
        "stg", "INSERT INTO public.target SELECT * FROM stg", "t_abc",
        target_table="public.target",
    )
    assert name == "stg_t_abc"
    assert "FROM stg_t_abc" in insert
    # target 은 보호되어 접미사가 붙지 않는다(이름 겹침 방지).
    assert "public.target" in insert


def test_per_task_external_disabled_returns_input():
    name, insert = stage_sql.per_task_external(
        "stg", "INSERT INTO t SELECT * FROM stg", "t_abc", enabled=False,
    )
    assert name == "stg" and insert == "INSERT INTO t SELECT * FROM stg"


# ───────────────────── 백엔드 흐름(가짜 S3/GP) ─────────────────────


class _FakeS3Client:
    def __init__(self):
        self.uploaded = []  # (local_path, key)
        self.deleted = []

    def upload(self, local_path, key):
        self.uploaded.append((local_path, key))

    def delete(self, key):
        self.deleted.append(key)


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


def _s3_backend(gp_conn, s3_client, s3_config=None):
    cfg = {"bucket": "mybkt", "prefix": "dqe-stage", "pxf_server": "s3srv",
           "local_tmp_dir": "/tmp/dqe-test"}
    cfg.update(s3_config or {})
    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x",
                                  s3_config=cfg, s3_client=s3_client)
    be._gp_pool = _FakePool(gp_conn)
    # export_to_local_csv 를 스텁: 실제 Impala/파일 없이 행수만 반환(단계 이벤트도 생략).
    be.export_to_local_csv = lambda *a, **k: 42
    return be


def test_stage_via_s3_full_flow():
    conn = _FakeGpConn()
    s3 = _FakeS3Client()
    be = _s3_backend(conn, s3)
    n = be.stage_via_s3(
        "SELECT a, dt FROM imp", "public.stg", "INSERT INTO public.t SELECT * FROM public.stg",
        "a int, dt date", {"delimiter": "`", "null": "", "quote": '"'},
        "public.t", "dt", ["'1'"], "append", "job_x", "t_y",
    )
    assert n == 7  # INSERT rowcount
    # 업로드가 올바른 키로 일어났고, 성공 정리로 S3 객체가 삭제됐다.
    assert s3.uploaded == [("/tmp/dqe-test/job_x/t_y.csv", "dqe-stage/job_x/t_y.csv")]
    assert s3.deleted == ["dqe-stage/job_x/t_y.csv"]
    # GP: 외부테이블 DROP→CREATE, INSERT, 외부테이블 DROP, COMMIT 순.
    joined = " | ".join(conn.executed)
    assert "CREATE EXTERNAL TABLE public.stg" in joined
    assert "pxf://mybkt/dqe-stage/job_x/t_y.csv?PROFILE=s3:csv&SERVER=s3srv" in joined
    assert "INSERT INTO public.t" in joined
    assert conn.executed[-1] == "COMMIT"
    # append 이므로 DELETE 없음.
    assert not any(s.strip().upper().startswith("DELETE") for s in conn.executed)


def test_stage_via_s3_overwrite_pre_deletes():
    conn = _FakeGpConn()
    be = _s3_backend(conn, _FakeS3Client())
    be.stage_via_s3(
        "SELECT a, dt FROM imp", "stg", "INSERT INTO t SELECT * FROM stg",
        "a int, dt date", {"delimiter": "`", "null": "", "quote": '"'},
        "t", "dt", ["'1'", "'2'"], "overwrite_partitions", "j", "tk",
    )
    assert any(s.startswith("DELETE FROM t WHERE dt IN ('1', '2')") for s in conn.executed)


def test_stage_via_s3_deletes_orphan_on_gp_failure():
    # INSERT(=GP)에서 실패하면 이미 올린 S3 객체를 고아로 남기지 않고 지운다.
    class _BoomCursor(_FakeGpCursor):
        def execute(self, sql):
            self.conn.executed.append(sql)
            if sql.strip().upper().startswith("INSERT"):
                raise RuntimeError("GP boom")

    class _BoomConn(_FakeGpConn):
        def cursor(self):
            return _BoomCursor(self)

    conn = _BoomConn()
    s3 = _FakeS3Client()
    be = _s3_backend(conn, s3)
    raised = False
    try:
        be.stage_via_s3(
            "SELECT 1", "stg", "INSERT INTO t SELECT * FROM stg", "a int",
            {"delimiter": "`", "null": "", "quote": '"'},
            "t", "dt", [], "append", "j", "tk",
        )
    except RuntimeError:
        raised = True
    assert raised
    assert s3.uploaded  # 업로드는 됐고
    assert s3.deleted == ["dqe-stage/j/tk.csv"]  # 실패 후 고아 객체 정리


def test_stage_via_s3_missing_bucket_raises():
    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x", s3_config={})
    be.export_to_local_csv = lambda *a, **k: 1
    raised = False
    try:
        be.stage_via_s3("SELECT 1", "stg", "INSERT INTO t SELECT * FROM stg", "a int",
                        {}, "t", "dt", [], "append", "j", "tk")
    except ValueError as exc:
        raised = "s3.bucket" in str(exc)
    assert raised


# ───────────────────── executor task 라우팅(MockBackend) ─────────────────────


def _task_payload(task_id, **over):
    base = {
        "task_id": task_id, "job_id": "j",
        "sub_query": "SELECT a, dt FROM imp WHERE dt IN ('1')",
        "target_table": "public.target", "write_mode": "append",
        "partition_column": "dt", "partition_values": ["'1'"],
        "exec_mode": "s3_stage", "staging_table": "stg_t",
        "insert_sql": "INSERT INTO public.target SELECT * FROM stg_t",
        "external_columns": "a int, dt date",
        "csv_options": {"delimiter": "`", "null": "", "quote": '"'},
    }
    base.update(over)
    return base


async def _run(app, payload):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(200):
            st = (await c.get(f"/tasks/{payload['task_id']}")).json()
            if st["status"] in ("DONE", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
        return st


async def test_executor_s3_stage_routes_to_mock():
    st = await _run(create_executor_app(backend=MockBackend(rows_per_value=9)),
                    _task_payload("s1"))
    assert st["status"] == "DONE"
    assert st["rows_written"] == 9


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


def test_coordinator_s3_stage_job(client, store):
    resp = client.post("/jobs", json=_job_payload())
    assert resp.status_code == 202
    job = store.get(resp.json()["job_id"])
    assert job.exec_mode == "s3_stage"
    assert job.staging_table == "stg_t"
    assert job.external_columns == "a int, dt date"
    assert job.insert_sql.startswith("INSERT INTO public.target")
    # sub-query 는 분할된 Impala SELECT 그대로(래핑/INSERT 아님).
    assert job.tasks[0].sub_query.startswith("SELECT a, dt FROM imp")
    assert "INSERT" not in job.tasks[0].sub_query.upper()


def test_coordinator_s3_stage_missing_fields_rejected(client):
    payload = _job_payload()
    del payload["external_columns"]
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "S3_STAGE_REQUIRES_FIELDS"


def test_coordinator_s3_stage_dry_run(client):
    resp = client.post("/jobs", json=_job_payload(dry_run=True))
    assert resp.status_code == 200
    body = resp.json()
    assert body["dry_run"] is True and body["exec_mode"] == "s3_stage"
    assert body["tasks"][0]["external_columns"] == "a int, dt date"
    assert body["tasks"][0]["insert_sql"].startswith("INSERT INTO public.target")


# ───────────────────── 템플릿 렌더 + 날짜 fan-out ─────────────────────


@pytest.fixture
def tpl_client(monkeypatch):
    from coordinator.config import settings
    monkeypatch.setattr(settings, "template_enabled", True, raising=False)
    monkeypatch.setattr(settings, "template_dir", str(REPO_TEMPLATES), raising=False)
    monkeypatch.setattr(settings, "template_auto_reload", False, raising=False)
    monkeypatch.setattr(settings, "template_func_modules", [], raising=False)
    monkeypatch.setattr(settings, "executors", [], raising=False)
    return TestClient(create_app(store=JobStore()))


def test_s3_stage_template_renders(tpl_client):
    # 예제 템플릿 sales_migration_s3 이 s3_stage 로 렌더되어 필드가 채워진다.
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
    # s3_stage 도 날짜 fan-out(task_params)을 지원한다(하루=1 task, append 적재).
    from coordinator.config import settings
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
        "INSERT INTO public.sales SELECT * FROM stg_s3\n", encoding="utf-8"
    )
    (tpl / "ext.sql.j2").write_text("id bigint, dt date\n", encoding="utf-8")
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
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
    assert b["task_count"] == 3  # [-2, 0] 양끝 포함
    assert b["tasks"][0]["external_columns"].startswith("id bigint")
    assert b["tasks"][0]["insert_sql"].startswith("INSERT INTO public.sales")
