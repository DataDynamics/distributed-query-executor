"""데이터소스 SELECT 테스트 엔드포인트(/datasources, /datasources/{name}/query) 검증.

실제 DB 없이 검증한다 — core.dbprobe 의 실행 함수를 가짜로 바꿔치기하거나(미리보기
성공 경로), 미구성/알 수 없는 데이터소스의 4xx 경로를 확인한다. coordinator 의 executor
프록시는 httpx.AsyncClient 를 가짜로 대체해 검증한다.
"""
from __future__ import annotations

import httpx

import core.config as core_config
from core.dbprobe import QueryResult
from executor.app import create_app as create_executor_app


def _fake_result() -> QueryResult:
    return QueryResult(
        columns=["a", "b"], rows=[[1, "x"], [2, "y"]],
        row_count=2, truncated=False, elapsed_ms=1.5,
    )


def _exec_client(app) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://x")


# ----------------------------- executor -----------------------------

async def test_executor_list_datasources():
    async with _exec_client(create_executor_app()) as c:
        body = (await c.get("/datasources")).json()
    names = {d["name"] for d in body["datasources"]}
    assert names == {"impala", "greenplum", "history"}
    # task 실행에 쓰는 소스 종류도 함께 노출한다(impala 전용)
    assert body["source_type"] == "impala"


async def test_executor_query_unknown_datasource():
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/datasources/nope/query", json={"sql": "SELECT 1"})
    assert r.status_code == 404


async def test_executor_query_unconfigured_greenplum(monkeypatch):
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "")
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/datasources/greenplum/query", json={"sql": "SELECT 1"})
    assert r.status_code == 400


async def test_executor_query_greenplum_preview(monkeypatch):
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import executor.app as ea
    monkeypatch.setattr(ea, "run_postgres_select", lambda dsn, sql, *, limit, datasource="greenplum": _fake_result())
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/datasources/greenplum/query", json={"sql": "SELECT * FROM t", "limit": 50})
    assert r.status_code == 200
    body = r.json()
    assert body["datasource"] == "greenplum"
    assert body["columns"] == ["a", "b"]
    assert body["rows"] == [[1, "x"], [2, "y"]]
    assert body["row_count"] == 2 and body["truncated"] is False
    assert body["limit"] == 50


async def test_executor_query_impala_preview(monkeypatch):
    monkeypatch.setattr(core_config.settings, "impala_host", "impala-host")
    import executor.app as ea
    monkeypatch.setattr(
        ea, "run_impala_select",
        lambda dsn, sql, *, query_options, limit, datasource="impala": _fake_result(),
    )
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/datasources/impala/query", json={"sql": "SELECT 1"})
    assert r.status_code == 200
    assert r.json()["datasource"] == "impala"


async def test_executor_query_impala_unconfigured(monkeypatch):
    monkeypatch.setattr(core_config.settings, "impala_host", "")
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/datasources/impala/query", json={"sql": "SELECT 1"})
    assert r.status_code == 400


async def test_executor_query_error_maps_502(monkeypatch):
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import executor.app as ea

    def _boom(*a, **k):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(ea, "run_postgres_select", _boom)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/datasources/greenplum/query", json={"sql": "SELECT 1"})
    assert r.status_code == 502
    assert "connection refused" in r.json()["detail"]


# ----------------------------- coordinator -----------------------------

def test_coordinator_list_datasources(client):
    body = client.get("/datasources").json()
    local_names = {d["name"] for d in body["local"]}
    assert local_names == {"history", "greenplum"}
    assert "impala" in body["via_executor"]


def test_coordinator_impala_requires_executor(client):
    r = client.post("/datasources/impala/query", json={"sql": "SELECT 1"})
    assert r.status_code == 400
    assert "executor_url" in r.json()["detail"]


def test_coordinator_history_preview(client, monkeypatch):
    import coordinator.app as ca
    monkeypatch.setattr(core_config.settings, "history_db_dsn", "postgresql://x")
    monkeypatch.setattr(ca, "run_postgres_select", lambda dsn, sql, *, limit, datasource="greenplum": _fake_result())
    body = client.post("/datasources/history/query", json={"sql": "SELECT 1"}).json()
    assert body["datasource"] == "history"
    assert body["columns"] == ["a", "b"]


def test_coordinator_proxy_to_executor(client, monkeypatch):
    import coordinator.app as ca

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "datasource": "impala", "columns": ["a"], "rows": [[1]],
                "row_count": 1, "truncated": False, "elapsed_ms": 1.0, "limit": 100,
            }

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None):
            assert url == "http://exec:8001/datasources/impala/query"
            assert json["sql"] == "SELECT 1"
            return _Resp()

    monkeypatch.setattr(ca.httpx, "AsyncClient", _FakeClient)
    body = client.post(
        "/datasources/impala/query",
        json={"sql": "SELECT 1", "executor_url": "http://exec:8001/"},
    ).json()
    assert body["proxied_to"] == "http://exec:8001/"
    assert body["datasource"] == "impala"


def test_coordinator_proxy_propagates_error(client, monkeypatch):
    import coordinator.app as ca

    class _Resp:
        status_code = 502

        def json(self):
            return {"detail": "impala 쿼리 실패: auth error"}

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None):
            return _Resp()

    monkeypatch.setattr(ca.httpx, "AsyncClient", _FakeClient)
    r = client.post(
        "/datasources/greenplum/query",
        json={"sql": "SELECT 1", "executor_url": "http://exec:8001"},
    )
    assert r.status_code == 502
    assert "auth error" in r.json()["detail"]


# ---------------------- executor /query-run (impala built-in / 커스텀 함수 위임) ----------------------

async def test_query_run_calls_custom_func(monkeypatch):
    # 설정에 커스텀 함수가 지정되면 sql/config/limit 이 그대로 전달되고 결과가 반환된다.
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    monkeypatch.setattr(core_config.settings, "query_func_config",
                        {"host": "trino.example.com", "port": "8080"}, raising=False)
    captured = {}

    def fake_fn(sql, *, config, limit):
        captured.update(sql=sql, config=config, limit=limit)
        return QueryResult(columns=["a"], rows=[[1]], row_count=1, truncated=False, elapsed_ms=2.0)

    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: fake_fn)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 42", "limit": 50, "datasource": "trino"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columns"] == ["a"] and body["rows"] == [[1]] and body["limit"] == 50
    # 커스텀 함수는 sql·자유 설정 dict·limit 을 그대로 넘겨받는다.
    assert captured == {"sql": "SELECT 42", "config": {"host": "trino.example.com", "port": "8080"}, "limit": 50}


async def test_query_run_이_datasource_를_실행_sql_로그에_남긴다(monkeypatch, caplog):
    """coordinator 가 실어 보낸 datasource 로 실행 SQL 이 기록된다(커스텀 함수 경로).

    커스텀 함수는 사내 API 라 자체 로깅을 기대할 수 없으므로, executor 가 "어떤 엔진에
    어떤 SQL 을 넘겼는지"를 확정적으로 남겨야 한다.
    """
    import logging

    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: (
        lambda sql, *, config, limit: QueryResult(
            columns=["a"], rows=[[1]], row_count=1, truncated=False, elapsed_ms=1.0)))
    async with _exec_client(create_executor_app()) as c:
        with caplog.at_level(logging.INFO, logger="core.sql"):
            r = await c.post(
                "/query-run",
                json={"sql": "SELECT 42", "limit": 10, "datasource": "trino"},
            )
    assert r.status_code == 200, r.text
    msg = [rec.getMessage() for rec in caplog.records if rec.name == "core.sql"][-1]
    assert "datasource=trino" in msg and "SELECT 42" in msg


async def test_query_run_datasource_생략시_impala_경로로_간다(monkeypatch, caplog):
    """구버전 coordinator 가 datasource 를 안 보내면 built-in Impala 경로(impyla)로 실행된다(하위 호환).

    is_custom_source 가 빈 값을 built-in 으로 판정하는 것과 같은 기준이다. 커스텀 함수는 호출되지
    않고, 실행 SQL 로그에는 impala 로 남는다.
    """
    import logging

    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: (
        lambda sql, *, config, limit: (_ for _ in ()).throw(AssertionError("custom func called"))))
    _patch_impala(monkeypatch)
    async with _exec_client(create_executor_app()) as c:
        with caplog.at_level(logging.INFO, logger="core.sql"):
            r = await c.post("/query-run", json={"sql": "SELECT 1"})
    assert r.status_code == 200, r.text
    assert "datasource=impala" in [
        rec.getMessage() for rec in caplog.records if rec.name == "core.sql"
    ][-1]


def _patch_impala(monkeypatch, host="impala-1", rows=None):
    """executor 의 impala.* 설정을 채우고 run_impala_select 를 가짜로 바꿔 호출 인자를 기록한다."""
    import executor.app as ea
    monkeypatch.setattr(core_config.settings, "impala_host", host, raising=False)
    monkeypatch.setattr(core_config.settings, "impala_port", 21050, raising=False)
    monkeypatch.setattr(core_config.settings, "impala_database", "sales", raising=False)
    monkeypatch.setattr(core_config.settings, "impala_query_options", {"REQUEST_POOL": "etl"}, raising=False)
    captured: dict = {}

    def fake_run(dsn, sql, *, query_options=None, limit, datasource="impala", log=True):
        captured.update(dsn=dsn, sql=sql, query_options=query_options, limit=limit,
                        datasource=datasource, log=log)
        return QueryResult(columns=["order_id"], rows=rows or [[1], [2]], row_count=2,
                           truncated=False, elapsed_ms=3.0)

    monkeypatch.setattr(ea, "run_impala_select", fake_run)
    return captured


async def test_query_run_impala_는_커스텀_함수_대신_impyla_로_실행한다(monkeypatch):
    """datasource=impala 면 query.func.module(trino 함수)을 건드리지 않고 impala.* 설정으로 실행한다.

    template_id 를 주고 datasource 를 impala 로 지정했을 때 이관 task 와 같은 impyla 접속으로
    조회 결과를 그대로 돌려받는 경로다. 커스텀 함수가 설정돼 있어도(또는 ODBC 등 다른 드라이버를
    쓰더라도) 호출되지 않는다.
    """
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: (
        lambda sql, *, config, limit: (_ for _ in ()).throw(AssertionError("custom func called"))))
    captured = _patch_impala(monkeypatch)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 42", "limit": 7, "datasource": "impala"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columns"] == ["order_id"] and body["rows"] == [[1], [2]] and body["limit"] == 7
    assert captured["dsn"]["host"] == "impala-1" and captured["dsn"]["database"] == "sales"
    assert captured["sql"] == "SELECT 42" and captured["limit"] == 7
    assert captured["query_options"] == {"REQUEST_POOL": "etl"}
    assert captured["datasource"] == "impala" and captured["log"] is False


async def test_query_run_impala_는_host_미설정이면_400_이고_커스텀으로_폴백하지_않는다(monkeypatch):
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: (
        lambda sql, *, config, limit: (_ for _ in ()).throw(AssertionError("custom func called"))))
    _patch_impala(monkeypatch, host="")
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "impala"})
    assert r.status_code == 400 and "impala.host" in r.json()["detail"]


async def test_query_run_impala_접속_오류는_502(monkeypatch):
    import executor.app as ea
    _patch_impala(monkeypatch)

    def boom(*a, **k):
        raise RuntimeError("auth error")

    monkeypatch.setattr(ea, "run_impala_select", boom)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "source"})
    assert r.status_code == 502 and "auth error" in r.json()["detail"]


async def test_query_run_trino_는_impala_설정이_없어도_커스텀_함수로_간다(monkeypatch):
    """impala.host 가 비어 있는 Trino 전용 executor 에서도 datasource=trino 는 그대로 동작한다."""
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    monkeypatch.setattr(core_config.settings, "impala_host", "", raising=False)
    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: (
        lambda sql, *, config, limit: QueryResult(
            columns=["a"], rows=[[1]], row_count=1, truncated=False, elapsed_ms=1.0)))
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "trino"})
    assert r.status_code == 200 and r.json()["columns"] == ["a"]


async def test_query_run_accepts_dict_return(monkeypatch):
    # 함수가 QueryResult 대신 동일 키 dict 를 반환해도 그대로 응답에 실린다.
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: (
        lambda sql, *, config, limit: {
            "columns": ["x"], "rows": [["v"]], "row_count": 1, "truncated": False, "elapsed_ms": 1.0}))
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "trino"})
    assert r.status_code == 200 and r.json()["columns"] == ["x"]


async def test_query_run_unconfigured_400(monkeypatch):
    monkeypatch.setattr(core_config.settings, "query_func_module", "", raising=False)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "trino"})
    assert r.status_code == 400


async def test_query_run_func_error_502(monkeypatch):
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea

    def boom(sql, *, config, limit):
        raise RuntimeError("gateway down")

    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: boom)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "trino"})
    assert r.status_code == 502 and "gateway down" in r.json()["detail"]


def test_load_query_func_resolves_and_validates():
    import pytest
    import core.dbprobe as dbp
    from executor.app import _load_query_func
    # module:func / module.func 두 표기 모두 동일 함수를 찾는다(캐시 재사용).
    assert _load_query_func("core.dbprobe:clamp_limit") is dbp.clamp_limit
    assert _load_query_func("core.dbprobe.clamp_limit") is dbp.clamp_limit
    with pytest.raises(ValueError):
        _load_query_func("no_such_module_zzz:foo")       # import 실패
    with pytest.raises(ValueError):
        _load_query_func("core.dbprobe:NOPE")            # getattr 실패
    with pytest.raises(ValueError):
        _load_query_func("core.dbprobe:MAX_PREVIEW_ROWS")  # 호출 불가(정수)


def test_collect_prefix_free_config(monkeypatch):
    # config.properties 의 query.func.config.* 프리픽스를 접두어 제거해 dict 로 수집한다.
    import core.config as cc
    monkeypatch.setattr(cc, "_props", {
        "query.func.module": "pkg.mod:run",
        "query.func.config.host": "h", "query.func.config.port": "8080",
        "query.func.config.token": "abc", "other.key": "ignored",
    })
    assert cc._collect_prefix("query.func.config.") == {"host": "h", "port": "8080", "token": "abc"}
