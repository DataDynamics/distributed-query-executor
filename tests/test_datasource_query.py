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
    monkeypatch.setattr(ea, "run_postgres_select", lambda dsn, sql, *, limit: _fake_result())
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
        lambda dsn, sql, *, query_options, limit: _fake_result(),
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
    monkeypatch.setattr(ca, "run_postgres_select", lambda dsn, sql, *, limit: _fake_result())
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

        async def post(self, url, json=None):
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

        async def post(self, url, json=None):
            return _Resp()

    monkeypatch.setattr(ca.httpx, "AsyncClient", _FakeClient)
    r = client.post(
        "/datasources/greenplum/query",
        json={"sql": "SELECT 1", "executor_url": "http://exec:8001"},
    )
    assert r.status_code == 502
    assert "auth error" in r.json()["detail"]


# ---------------------- executor /query-run (커스텀 함수 위임) ----------------------

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
        r = await c.post("/query-run", json={"sql": "SELECT 42", "limit": 50})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columns"] == ["a"] and body["rows"] == [[1]] and body["limit"] == 50
    # 커스텀 함수는 sql·자유 설정 dict·limit 을 그대로 넘겨받는다.
    assert captured == {"sql": "SELECT 42", "config": {"host": "trino.example.com", "port": "8080"}, "limit": 50}


async def test_query_run_accepts_dict_return(monkeypatch):
    # 함수가 QueryResult 대신 동일 키 dict 를 반환해도 그대로 응답에 실린다.
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: (
        lambda sql, *, config, limit: {
            "columns": ["x"], "rows": [["v"]], "row_count": 1, "truncated": False, "elapsed_ms": 1.0}))
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1"})
    assert r.status_code == 200 and r.json()["columns"] == ["x"]


async def test_query_run_unconfigured_400(monkeypatch):
    monkeypatch.setattr(core_config.settings, "query_func_module", "", raising=False)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1"})
    assert r.status_code == 400


async def test_query_run_func_error_502(monkeypatch):
    monkeypatch.setattr(core_config.settings, "query_func_module", "pkg.mod:run", raising=False)
    import executor.app as ea

    def boom(sql, *, config, limit):
        raise RuntimeError("gateway down")

    monkeypatch.setattr(ea, "_load_query_func", lambda dotted: boom)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1"})
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


def test_collect_query_funcs_by_source(monkeypatch):
    """query.func.<name>.module / .config.* 를 소스별로 묶고, 단일 형태는 제외한다."""
    import core.config as cc
    monkeypatch.setattr(cc, "_props", {
        # 단일 형태(폴백) — by_source 에 들어가면 안 된다.
        "query.func.module": "single:run",
        "query.func.config.host": "single-host",
        # 소스별 형태
        "query.func.trino.module": "pkg.trino:run",
        "query.func.trino.config.host": "t.example",
        "query.func.trino.config.port": "8080",
        "query.func.impala.module": "pkg.impala:run",
        # 무관한 키
        "other.key": "ignored",
    })
    funcs = cc._collect_query_funcs()
    assert set(funcs) == {"trino", "impala"}
    assert funcs["trino"] == {
        "module": "pkg.trino:run", "config": {"host": "t.example", "port": "8080"}}
    # 설정이 없는 소스는 config 가 빈 dict 다(폴백 판단은 호출부 몫).
    assert funcs["impala"] == {"module": "pkg.impala:run", "config": {}}


# ------------- /query-run 데이터소스별 함수 선택(query.func.<name>.module) -------------

async def test_query_run_picks_func_by_datasource(monkeypatch):
    """datasource 이름의 항목이 있으면 그 함수·설정으로 실행한다(단일 설정을 이긴다)."""
    monkeypatch.setattr(core_config.settings, "query_func_module", "single:run", raising=False)
    monkeypatch.setattr(core_config.settings, "query_func_config", {"who": "single"}, raising=False)
    monkeypatch.setattr(core_config.settings, "query_func_by_source", {
        "trino": {"module": "pkg.trino:run", "config": {"who": "trino", "host": "t.example"}},
        "impala": {"module": "pkg.impala:run", "config": {"who": "impala"}},
    }, raising=False)
    seen = {}

    def fake_loader(dotted):
        def fn(sql, *, config, limit):
            seen.update(module=dotted, config=config)
            return QueryResult(columns=["a"], rows=[[1]], row_count=1, truncated=False, elapsed_ms=1.0)
        return fn

    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", fake_loader)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "trino"})
        assert r.status_code == 200, r.text
        assert seen == {"module": "pkg.trino:run", "config": {"who": "trino", "host": "t.example"}}
        # 같은 executor 가 datasource 만 바꿔도 다른 함수로 간다.
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "impala"})
        assert r.status_code == 200
        assert seen["module"] == "pkg.impala:run" and seen["config"] == {"who": "impala"}


async def test_query_run_falls_back_to_single_func(monkeypatch):
    """소스별 항목이 없거나 datasource 가 안 오면 단일 query.func.module 로 폴백한다."""
    monkeypatch.setattr(core_config.settings, "query_func_module", "single:run", raising=False)
    monkeypatch.setattr(core_config.settings, "query_func_config", {"who": "single"}, raising=False)
    monkeypatch.setattr(core_config.settings, "query_func_by_source", {
        "trino": {"module": "pkg.trino:run", "config": {"who": "trino"}},
    }, raising=False)
    seen = {}

    def fake_loader(dotted):
        def fn(sql, *, config, limit):
            seen.update(module=dotted, config=config)
            return QueryResult(columns=["a"], rows=[[1]], row_count=1, truncated=False, elapsed_ms=1.0)
        return fn

    import executor.app as ea
    monkeypatch.setattr(ea, "_load_query_func", fake_loader)
    async with _exec_client(create_executor_app()) as c:
        # 등록되지 않은 이름 → 폴백
        assert (await c.post("/query-run", json={"sql": "S", "datasource": "impala"})).status_code == 200
        assert seen == {"module": "single:run", "config": {"who": "single"}}
        # datasource 미지정(구버전 coordinator) → 폴백
        seen.clear()
        assert (await c.post("/query-run", json={"sql": "S"})).status_code == 200
        assert seen["module"] == "single:run"


async def test_query_run_unconfigured_datasource_400_lists_sources(monkeypatch):
    """단일 폴백도 없고 그 이름의 항목도 없으면 400 + 구성된 소스 목록을 안내한다."""
    monkeypatch.setattr(core_config.settings, "query_func_module", "", raising=False)
    monkeypatch.setattr(core_config.settings, "query_func_by_source", {
        "trino": {"module": "pkg.trino:run", "config": {}},
    }, raising=False)
    async with _exec_client(create_executor_app()) as c:
        r = await c.post("/query-run", json={"sql": "S", "datasource": "impala"})
    assert r.status_code == 400
    assert "impala" in r.json()["detail"] and "trino" in r.json()["detail"]
