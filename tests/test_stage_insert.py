"""stage_insert 모드: Impala SELECT → Greenplum staging COPY → staging→target INSERT."""

from __future__ import annotations

import asyncio

import httpx

from executor.app import create_app as create_executor_app
from executor.backend import MockBackend


class _RecordingBackend:
    def __init__(self):
        self.staged = None  # (impala_select, staging_table, staging_ddl, insert_sql)
        self.moved = False
        self.executed = None

    def move(self, *a, **k):
        self.moved = True
        return 1

    def execute(self, sql, on_stage=None):
        self.executed = sql
        return 1

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None, query_options=None, on_stage=None):
        self.staged = (impala_select, staging_table, staging_ddl, insert_sql)
        if on_progress:
            on_progress(11)
        return 11


def _payload(task_id, **over):
    base = {
        "task_id": task_id,
        "job_id": "j",
        "sub_query": "SELECT a, dt FROM imp WHERE dt IN ('1')",  # Impala SELECT
        "target_table": "public.target",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
        "exec_mode": "stage_insert",
        "staging_table": "stg_t",
        "staging_ddl": "CREATE TEMP TABLE stg_t (a int, dt text)",
        "insert_sql": "INSERT INTO public.target (a, dt) SELECT a, dt FROM stg_t",
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


async def test_stage_insert_calls_stage_and_insert():
    backend = _RecordingBackend()
    st = await _run(create_executor_app(backend=backend), _payload("ts"))
    assert st["status"] == "DONE"
    assert st["rows_written"] == 11
    assert backend.moved is False and backend.executed is None  # copy/statement 미사용
    impala_select, staging_table, staging_ddl, insert_sql = backend.staged
    assert impala_select.startswith("SELECT a, dt FROM imp")
    assert staging_table == "stg_t"
    assert "CREATE TEMP TABLE stg_t" in staging_ddl
    assert insert_sql.startswith("INSERT INTO public.target")


async def test_stage_insert_rows_from_mock():
    st = await _run(create_executor_app(backend=MockBackend(rows_per_value=13)), _payload("tm"))
    assert st["status"] == "DONE"
    assert st["rows_written"] == 13


# ───────────────────── coordinator 검증/전달 ─────────────────────


def _job_payload(**over):
    base = {
        "sql": "SELECT a, dt FROM imp WHERE dt IN ('1','2','3')",
        "partition_column": "dt",
        "target_table": "public.target",
        "parallelism": 3,
        "exec_mode": "stage_insert",
        "staging_table": "stg_t",
        "staging_ddl": "CREATE TEMP TABLE stg_t (a int, dt text)",
        "wrapper_query": "INSERT INTO public.target (a, dt) SELECT a, dt FROM stg_t",
    }
    base.update(over)
    return base


def test_coordinator_stage_insert_job(client, store):
    resp = client.post("/jobs", json=_job_payload())
    assert resp.status_code == 202
    job = store.get(resp.json()["job_id"])
    assert job.exec_mode == "stage_insert"
    assert job.staging_table == "stg_t"
    assert "CREATE TEMP TABLE" in job.staging_ddl
    assert job.insert_sql.startswith("INSERT INTO public.target")
    # sub-query 는 분할된 Impala SELECT 그대로(래핑/INSERT 아님)
    assert job.tasks[0].sub_query.startswith("SELECT a, dt FROM imp")
    assert "INSERT" not in job.tasks[0].sub_query.upper()


def test_coordinator_stage_insert_missing_fields_rejected(client):
    # staging_table(또는 wrapper_query)이 빠지면 거부된다.
    payload = _job_payload()
    del payload["staging_table"]
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "STAGE_INSERT_REQUIRES_FIELDS"


def test_coordinator_stage_insert_without_ddl_accepted(client, store):
    # staging_ddl 은 선택: 없어도 작업이 수용되고, staging_ddl 은 None 으로 전달된다
    # (executor 는 테이블 생성을 건너뛰고 기존 staging_table 을 사용).
    payload = _job_payload()
    del payload["staging_ddl"]
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 202
    job = store.get(resp.json()["job_id"])
    assert job.exec_mode == "stage_insert"
    assert job.staging_table == "stg_t"
    assert job.staging_ddl is None


async def test_stage_insert_skips_ddl_when_absent():
    # staging_ddl 이 없으면 backend.stage_and_insert 에 None 으로 전달된다.
    backend = _RecordingBackend()
    payload = _payload("tn")
    del payload["staging_ddl"]
    st = await _run(create_executor_app(backend=backend), payload)
    assert st["status"] == "DONE"
    _impala_select, _staging_table, staging_ddl, _insert_sql = backend.staged
    assert staging_ddl is None


# ───── ImpalaToGreenplumBackend.stage_and_insert: TEMP staging 선삭제 회귀 ─────
#
# 버그: 풀에서 재사용한 GP 연결에 이전 task 의 TEMP staging 이 남아 있으면(일부 GP/
# WarehousePG 는 반납 시 DISCARD ALL 이 TEMP 를 안 떨군다) 다음 task 의 CREATE TEMP TABLE
# 이 "already exists" 로 실패한다. stage_insert 와 날짜 fan-out 은 같은 staging_table 이름을
# 여러 task 가 재사용하므로 두 번째 task 부터 터진다. 수정: CREATE 직전 DROP TABLE IF EXISTS.

from contextlib import contextmanager  # noqa: E402

from executor.backend import ImpalaToGreenplumBackend  # noqa: E402


class _FakeImpalaCursor:
    description = [("a",), ("dt",)]

    def execute(self, sql, configuration=None):
        pass


class _FakeImpalaConn:
    def cursor(self):
        return _FakeImpalaCursor()

    def close(self):
        pass


class _FakeGpCursor:
    """CREATE TEMP TABLE 이 이미 있는 테이블이면 실제 GP 처럼 'already exists' 로 실패한다."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    @property
    def rowcount(self):
        return 5

    def execute(self, sql):
        self.conn.executed.append(sql)
        low = sql.strip().lower()
        if low.startswith("drop table if exists"):
            self.conn.tables.discard(sql.split()[-1])
        elif low.startswith("create temp table"):
            name = sql.split()[3]  # CREATE TEMP TABLE <name> (...)
            if name in self.conn.tables:
                raise RuntimeError(f'relation "{name}" already exists')
            self.conn.tables.add(name)


class _FakeGpConn:
    def __init__(self, tables=()):
        self.tables = set(tables)  # 세션에 이미 존재하는 TEMP 테이블(재사용 연결 재현)
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


def _make_backend(gp_conn):
    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x")  # 연결 없음(pool lazy)
    be._gp_pool = _FakePool(gp_conn)
    be._source_connect = lambda: _FakeImpalaConn()
    be._stream_to_copy = lambda *a, **k: (5, 0.0, 0.0, 0.0, 0.0)  # 스트리밍 스텁
    return be


def test_stage_insert_drops_leftover_temp_before_create():
    # 재사용 연결에 이전 task 의 stg_t 가 남아 있어도 선삭제 덕에 재생성이 성공해야 한다.
    conn = _FakeGpConn(tables={"stg_t"})
    be = _make_backend(conn)
    n = be.stage_and_insert(
        "SELECT a, dt FROM imp",
        "stg_t",
        "CREATE TEMP TABLE stg_t (a int, dt text)",
        "INSERT INTO public.t SELECT * FROM stg_t",
    )
    assert n == 5
    drop_i = next(i for i, s in enumerate(conn.executed)
                  if s.strip().lower().startswith("drop table if exists"))
    create_i = next(i for i, s in enumerate(conn.executed)
                    if s.strip().lower().startswith("create temp table"))
    assert drop_i < create_i  # DROP 이 CREATE 앞에 실행된다


def test_stage_insert_create_succeeds_on_fresh_connection():
    # 남은 TEMP 가 없는 새 연결에서도 정상 동작(DROP 은 무해한 no-op).
    conn = _FakeGpConn()
    be = _make_backend(conn)
    n = be.stage_and_insert(
        "SELECT a, dt FROM imp",
        "stg_t",
        "CREATE TEMP TABLE stg_t (a int, dt text)",
        "INSERT INTO public.t SELECT * FROM stg_t",
    )
    assert n == 5
    assert "stg_t" in conn.tables  # 최종적으로 생성돼 있음


def test_stage_insert_no_drop_when_ddl_absent():
    # staging_ddl 이 없으면(기존 영구 staging 사용) 선삭제하지 않는다 — 데이터 유실 방지.
    conn = _FakeGpConn()
    be = _make_backend(conn)
    be.stage_and_insert(
        "SELECT a, dt FROM imp",
        "stg_perm",
        None,
        "INSERT INTO public.t SELECT * FROM stg_perm",
    )
    assert not any(s.strip().lower().startswith("drop table") for s in conn.executed)
