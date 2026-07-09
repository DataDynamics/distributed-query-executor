"""Trino 소스(source.type=trino) 지원 테스트.

실제 Trino 없이 가짜 trino 모듈(sys.modules 주입)로 검증한다. 확인 항목:

- ``build_trino_dsn``: 기본값/미설정 처리, password 설정 시 https 승격, verify 해석
  (true/false/CA 경로), 세션 프로퍼티 전달.
- 백엔드 소스 분기: ``_source_connect`` 가 trino 클라이언트로 접속하며 password 를
  ``BasicAuthentication`` 으로 변환, ``_source_execute`` 는 요청별 query_options 를
  무시(트리노는 쿼리 단위 configuration 미지원), ``_open_source_cursor`` 는
  convert_types(impyla 전용)를 무시.
- ``build_backend`` 가 source.type=trino 를 반영해 trino DSN 으로 백엔드를 만든다.
- ``run_trino_select``(dbprobe) 가 상위 N행/truncated 를 올바르게 반환한다.
- 기본 설정에서 source_type=impala, trino 미구성 시 /datasources/trino/query 는 400.
"""

from __future__ import annotations

import sys
import types

import httpx
import pytest

from executor.backend import (
    ImpalaToGreenplumBackend,
    build_backend,
    build_source_dsn,
    build_trino_dsn,
)


# ---------- 가짜 trino 모듈 ----------

class _FakeBasicAuth:
    def __init__(self, user, password):
        self.user = user
        self.password = password


class _FakeTrinoCursor:
    """execute 호출을 기록하고 고정 결과를 돌려주는 가짜 커서."""

    def __init__(self, rows):
        self._rows = list(rows)
        self.executed: list[tuple[str, dict]] = []
        self.description = [("a",), ("b",)]

    def execute(self, sql, **kwargs):
        self.executed.append((sql, kwargs))

    def fetchmany(self, n):
        out, self._rows = self._rows[:n], self._rows[n:]
        return out


class _FakeTrinoConn:
    def __init__(self, rows):
        self.cursor_obj = _FakeTrinoCursor(rows)
        self.closed = False

    def cursor(self, **kwargs):
        self.cursor_kwargs = kwargs
        return self.cursor_obj

    def close(self):
        self.closed = True


@pytest.fixture()
def fake_trino(monkeypatch):
    """sys.modules 에 가짜 trino 모듈을 주입한다(지연 임포트 가로채기).

    반환 dict: connects(연결 kwargs 기록), rows(다음 연결이 돌려줄 행), conns(생성된 연결).
    """
    state = {"connects": [], "rows": [(1, "x"), (2, "y"), (3, "z")], "conns": []}
    mod = types.ModuleType("trino")
    auth_mod = types.ModuleType("trino.auth")
    dbapi_mod = types.ModuleType("trino.dbapi")
    auth_mod.BasicAuthentication = _FakeBasicAuth

    def connect(**kwargs):
        state["connects"].append(kwargs)
        conn = _FakeTrinoConn(state["rows"])
        state["conns"].append(conn)
        return conn

    dbapi_mod.connect = connect
    mod.auth = auth_mod
    mod.dbapi = dbapi_mod
    monkeypatch.setitem(sys.modules, "trino", mod)
    monkeypatch.setitem(sys.modules, "trino.auth", auth_mod)
    monkeypatch.setitem(sys.modules, "trino.dbapi", dbapi_mod)
    return state


def _settings(**kw):
    base = {
        "trino_host": "trino-1", "trino_port": 8080, "trino_user": "svc",
        "trino_password": "", "trino_catalog": "hive", "trino_schema": "default",
        "trino_http_scheme": "http", "trino_verify": "", "trino_session_properties": {},
    }
    base.update(kw)
    return types.SimpleNamespace(**base)


# ---------- build_trino_dsn / build_source_dsn ----------

def test_build_trino_dsn_basic():
    dsn = build_trino_dsn(_settings())
    assert dsn == {
        "host": "trino-1", "port": 8080, "user": "svc",
        "catalog": "hive", "schema": "default", "http_scheme": "http",
    }
    # host 미설정이면 빈 dict(Trino 미사용)
    assert build_trino_dsn(_settings(trino_host="")) == {}


def test_build_trino_dsn_password_forces_https():
    dsn = build_trino_dsn(_settings(trino_password="secret"))
    # Basic 인증은 trino 클라이언트 제약상 https 필수 → http 여도 https 로 승격
    assert dsn["http_scheme"] == "https"
    assert dsn["password"] == "secret"


def test_build_trino_dsn_verify_and_session_properties():
    assert "verify" not in build_trino_dsn(_settings())            # 비움=클라이언트 기본
    assert build_trino_dsn(_settings(trino_verify="false"))["verify"] is False
    assert build_trino_dsn(_settings(trino_verify="true"))["verify"] is True
    assert build_trino_dsn(_settings(trino_verify="/etc/ca.pem"))["verify"] == "/etc/ca.pem"
    dsn = build_trino_dsn(_settings(trino_session_properties={"query_max_run_time": "1h"}))
    assert dsn["session_properties"] == {"query_max_run_time": "1h"}


def test_build_source_dsn_selects_by_type():
    s = _settings(source_type="trino")
    assert build_source_dsn(s)["host"] == "trino-1"
    s2 = types.SimpleNamespace(source_type="impala", impala_host="")
    assert build_source_dsn(s2) == {}  # impala 미설정 → build_impala_dsn 의 빈 dict


def test_build_backend_uses_trino_source():
    s = _settings(
        source_type="trino", greenplum_dsn="postgresql://u@h/db", copy_batch_size=1000,
    )
    be = build_backend(s)
    assert isinstance(be, ImpalaToGreenplumBackend)
    assert be.source_type == "trino"
    assert be.impala_dsn["host"] == "trino-1"  # 소스 dict 에 trino 접속 정보


# ---------- 백엔드 소스 분기 ----------

def test_source_connect_trino_converts_password_to_basic_auth(fake_trino):
    be = ImpalaToGreenplumBackend(
        impala_dsn={"host": "trino-1", "port": 8080, "user": "svc",
                    "http_scheme": "https", "password": "secret"},
        greenplum_dsn="x", source_type="trino",
    )
    be._source_connect()
    kwargs = fake_trino["connects"][0]
    assert "password" not in kwargs                     # dict 로는 넘기지 않음
    assert isinstance(kwargs["auth"], _FakeBasicAuth)   # BasicAuthentication 으로 변환
    assert kwargs["auth"].user == "svc" and kwargs["auth"].password == "secret"
    assert kwargs["host"] == "trino-1"


def test_source_execute_trino_ignores_query_options():
    be = ImpalaToGreenplumBackend(
        impala_dsn={}, greenplum_dsn="x", source_type="trino",
        query_options={"MEM_LIMIT": "2g"},  # 전역 옵션(impala 전용)도 무시돼야 함
    )
    cur = _FakeTrinoCursor([])
    be._source_execute(cur, "SELECT 1", {"REQUEST_POOL": "etl"})
    # trino 커서는 configuration kwarg 를 받지 않는다 — sql 만 그대로 실행
    assert cur.executed == [("SELECT 1", {})]


def test_open_source_cursor_trino_maps_convert_types_to_legacy_primitive_types(fake_trino):
    """convert_types=False(형변환 끄기)가 trino 에선 legacy_primitive_types=True 로 매핑된다.

    local_stage export 의 재파싱 제거 최적화(impala 의 convert_types=False 와 동일 목적):
    TIMESTAMP/DATE/DECIMAL 을 datetime/Decimal 로 파싱하지 않고 서버 문자열 그대로 받는다.
    """
    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x", source_type="trino")
    conn = _FakeTrinoConn([])
    be._open_source_cursor(conn, convert_types=False)
    assert conn.cursor_kwargs == {"legacy_primitive_types": True}
    # 형변환 유지(None/True)면 기본 커서
    be._open_source_cursor(conn, convert_types=None)
    assert conn.cursor_kwargs == {}
    be._open_source_cursor(conn, convert_types=True)
    assert conn.cursor_kwargs == {}


def test_open_source_cursor_trino_falls_back_on_old_client(fake_trino):
    """legacy_primitive_types 를 모르는 구버전 trino 클라이언트면 기본 커서로 폴백한다."""

    class _OldConn(_FakeTrinoConn):
        def cursor(self, **kwargs):
            if kwargs:
                raise TypeError("unexpected keyword argument")
            self.cursor_kwargs = kwargs
            return self.cursor_obj

    be = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x", source_type="trino")
    conn = _OldConn([])
    cur = be._open_source_cursor(conn, convert_types=False)
    assert cur is conn.cursor_obj and conn.cursor_kwargs == {}


# ---------- dbprobe.run_trino_select ----------

def test_run_trino_select_shapes_and_truncates(fake_trino):
    from core.dbprobe import run_trino_select

    fake_trino["rows"] = [(i, f"r{i}") for i in range(5)]
    result = run_trino_select(
        {"host": "trino-1", "port": 8080, "user": "svc", "password": "pw"},
        "SELECT * FROM t", limit=3,
    )
    d = result.to_dict()
    assert d["columns"] == ["a", "b"]
    assert d["row_count"] == 3 and d["truncated"] is True   # limit+1 로 잘림 감지
    assert isinstance(fake_trino["connects"][0]["auth"], _FakeBasicAuth)
    assert fake_trino["conns"][0].closed                     # 연결 정리


# ---------- 엔드포인트/설정 기본값 ----------

def test_default_settings_source_is_impala():
    from coordinator.config import settings as core_settings

    assert core_settings.source_type == "impala"
    assert core_settings.trino_host == ""       # 기본 미구성
    assert core_settings.trino_port == 8080


async def test_executor_trino_query_unconfigured_returns_400():
    from executor.app import create_app as create_executor_app

    transport = httpx.ASGITransport(app=create_executor_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        r = await c.post("/datasources/trino/query", json={"sql": "SELECT 1"})
    assert r.status_code == 400
    assert "trino.host" in r.json()["detail"]


def test_coordinator_datasources_offer_trino_via_executor(client):
    body = client.get("/datasources").json()
    assert "trino" in body["via_executor"]


def test_masked_config_masks_trino_password(monkeypatch):
    from core.config import settings as exec_settings
    from executor.dashboard import masked_config

    monkeypatch.setattr(exec_settings, "trino_password", "secret", raising=False)
    rows = {(r["section"], r["key"]): str(r["value"]) for r in masked_config(exec_settings)}
    assert rows[("trino", "password")] == "***"
    assert ("source", "type") in rows
    for v in rows.values():
        assert "secret" not in v


# ---------- copy 모드 전체 경로(e2e, 가짜 trino + 가짜 GP 풀) ----------

class _FakeGpCopy:
    def __init__(self):
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def write_row(self, row):
        self.rows.append(row)


class _FakeGpCursor:
    def __init__(self):
        self.copied = _FakeGpCopy()
        self.executed = []
        self.rowcount = 0

    def copy(self, sql):
        self.copy_sql = sql
        return self.copied

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeGpConn:
    def __init__(self):
        self.cur = _FakeGpCursor()
        self.committed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True


class _FakeGpPool:
    """백엔드의 _GreenplumPool 자리에 꽂는 가짜(연결 1개 재사용)."""

    def __init__(self):
        self.conn = _FakeGpConn()

    def connection(self):
        import contextlib

        @contextlib.contextmanager
        def _ctx():
            yield self.conn
        return _ctx()


def test_move_copy_mode_end_to_end_with_trino_source(fake_trino):
    """copy 모드 전체 경로(_source_connect→execute→스트리밍→COPY→commit)를 trino 로 구동."""
    be = ImpalaToGreenplumBackend(
        impala_dsn={"host": "trino-1", "port": 8080, "user": "svc"},
        greenplum_dsn="postgresql://x", batch_size=2,
        copy_preflight=False, pipeline=False, source_type="trino",
    )
    pool = _FakeGpPool()
    be._gp_pool = pool
    n = be.move("SELECT a, b FROM t", "public.t", "append", "b", ["'x'"])
    assert n == 3                                        # 기본 fake 행 3개 적재
    assert pool.conn.cur.copied.rows == [(1, "x"), (2, "y"), (3, "z")]
    assert pool.conn.cur.copy_sql == "COPY public.t (a, b) FROM STDIN"
    assert pool.conn.committed
    # 소스가 trino 클라이언트로 열렸고(sql 은 configuration 없이 실행) 연결이 정리됐다
    assert fake_trino["connects"][0]["host"] == "trino-1"
    assert fake_trino["conns"][0].cursor_obj.executed == [("SELECT a, b FROM t", {})]
    assert fake_trino["conns"][0].closed
