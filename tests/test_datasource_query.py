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
