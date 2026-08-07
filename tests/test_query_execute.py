"""POST /query-execute 검증 — 템플릿+params 로 SELECT 를 렌더/실행해 결과를 돌려받는 경로.

실제 DB 불필요 — coordinator 직접 실행(greenplum/history)은 core.dbprobe 실행 함수를
가짜로 바꾸고, 소스(impala/trino) 프록시는 httpx.AsyncClient 를 가짜로 대체한다.
executor 선택은 클라이언트가 아니라 coordinator 가 하며, 응답에 executor 를 노출하지 않는다.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import core.config as core_config
from coordinator.app import create_app
from coordinator.template import TemplateEngine, TemplateError
from core.dbprobe import QueryResult

REPO_TEMPLATES = Path(__file__).resolve().parent.parent / "templates"


def _fake_result() -> QueryResult:
    return QueryResult(
        columns=["user_id", "dt"], rows=[[1, "2026-01-01"], [2, "2026-01-01"]],
        row_count=2, truncated=False, elapsed_ms=1.5,
    )


@pytest.fixture
def qe_client(runner, store, monkeypatch):
    """예제 템플릿을 켜고 executor 2대를 설정한 coordinator 클라이언트 팩토리.

    executors 를 인자로 조정해 재생성할 수 있도록 팩토리를 돌려준다.
    """
    from coordinator.config import settings
    monkeypatch.setattr(settings, "template_enabled", True, raising=False)
    monkeypatch.setattr(settings, "template_dir", str(REPO_TEMPLATES), raising=False)
    monkeypatch.setattr(settings, "template_auto_reload", False, raising=False)
    monkeypatch.setattr(settings, "template_func_modules", [], raising=False)

    def _build(executors=None):
        monkeypatch.setattr(settings, "executors", list(executors or []), raising=False)
        return TestClient(create_app(runner=runner, store=store))

    return _build


# ───────────────────────── 엔진: render_query(select-only) ─────────────────────────


class _Settings:
    def __init__(self, template_dir):
        self.template_dir = str(template_dir)
        self.template_auto_reload = False
        self.template_func_modules = []
        self.template_validate_ddl_single_stmt = True


def test_render_query_select_only_ignores_insert_role(tmp_path):
    """stage_insert 템플릿이라도 render_query 는 insert/staging 조각 없이 select 만 렌더한다."""
    d = tmp_path / "si"
    d.mkdir()
    (d / "manifest.yml").write_text(
        "id: si\nexec_mode: stage_insert\npartition_column: dt\n"
        "target_table: public.t\nparams:\n  - {name: dts, type: list, required: true}\n"
        "files: {select: s.j2}\n",
        encoding="utf-8",
    )
    (d / "s.j2").write_text("SELECT a, dt FROM t WHERE dt IN ( {{ dts | sql_in }} )\n", encoding="utf-8")
    eng = TemplateEngine(_Settings(tmp_path))
    sql = eng.render_query("si", {"dts": ["2026-01-01", "2026-01-02"]})
    assert "dt IN ( '2026-01-01', '2026-01-02' )" in sql


def test_render_query_missing_select_role(tmp_path):
    d = tmp_path / "nosel"
    d.mkdir()
    (d / "manifest.yml").write_text(
        "id: nosel\nexec_mode: copy\npartition_column: dt\nfiles: {wrapper: w.j2}\n",
        encoding="utf-8",
    )
    (d / "w.j2").write_text("SELECT 1", encoding="utf-8")
    eng = TemplateEngine(_Settings(tmp_path))
    with pytest.raises(TemplateError) as ei:
        eng.render_query("nosel", {})
    assert ei.value.code == "TEMPLATE_MISSING_ROLE"


# ───────────────────────── API: coordinator 직접 실행(greenplum) ─────────────────────────


def test_query_execute_greenplum_direct(qe_client, monkeypatch):
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    monkeypatch.setattr(ca, "run_postgres_select", lambda dsn, sql, *, limit: _fake_result())
    client = qe_client()
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [
            {"name": "start_dt", "value": "2026-01-01"},
            {"name": "end_dt", "value": "2026-01-02"},
            {"name": "regions", "value": ["KR"]},
        ],
        "datasource": "greenplum",
        "limit": 50,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["template_id"] == "sales_migration"
    assert body["datasource"] == "greenplum"
    assert "dt IN" in body["sql"] and "region IN ( 'KR' )" in body["sql"]
    assert body["columns"] == ["user_id", "dt"]
    assert body["rows"] == [[1, "2026-01-01"], [2, "2026-01-01"]]
    assert body["row_count"] == 2 and body["truncated"] is False
    # coordinator 직접 실행이므로 executed_by 는 null(그 외 예전 필드명은 쓰지 않는다).
    assert body["executed_by"] is None
    assert "executor_url" not in body and "proxied_to" not in body


def test_query_execute_order_search_example(qe_client, monkeypatch):
    """예제 템플릿 order_search — WHERE 에 region IN(N개) + order_dt BETWEEN 구간을 렌더한다."""
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    captured = {}

    def _capture(dsn, sql, *, limit):
        captured["sql"] = sql
        return _fake_result()

    monkeypatch.setattr(ca, "run_postgres_select", _capture)
    client = qe_client()
    resp = client.post("/query-execute", json={
        "template_id": "order_search",
        "params": [
            {"name": "regions", "value": ["KR", "US", "JP"]},
            {"name": "start_dt", "value": "2026-01-01"},
            {"name": "end_dt", "value": "2026-03-31"},
            {"name": "min_amount", "value": 1000},
        ],
        "datasource": "greenplum",
    })
    assert resp.status_code == 200, resp.text
    sql = captured["sql"]
    assert "region IN ( 'KR', 'US', 'JP' )" in sql
    assert "order_dt BETWEEN '2026-01-01' AND '2026-03-31'" in sql
    assert "amount >= 1000" in sql
    # 응답 sql 에도 렌더 결과가 그대로 실린다.
    assert resp.json()["sql"] == sql


def test_query_execute_order_search_optional_amount_omitted(qe_client, monkeypatch):
    """min_amount 를 빼면 금액 조건이 생략된다."""
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    captured = {}
    monkeypatch.setattr(ca, "run_postgres_select",
                        lambda dsn, sql, *, limit: (captured.update(sql=sql) or _fake_result()))
    client = qe_client()
    resp = client.post("/query-execute", json={
        "template_id": "order_search",
        "params": [
            {"name": "regions", "value": ["KR"]},
            {"name": "start_dt", "value": "2026-01-01"},
            {"name": "end_dt", "value": "2026-01-31"},
        ],
        "datasource": "greenplum",
    })
    assert resp.status_code == 200, resp.text
    assert "amount >=" not in captured["sql"]


# ───────────────────────── API: 소스(impala) executor 프록시 ─────────────────────────


class _Resp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {
            "datasource": "impala", "limit": 100, "columns": ["a"], "rows": [[1]],
            "row_count": 1, "truncated": False, "elapsed_ms": 1.0,
        }
        self.text = "err"

    def json(self):
        return self._payload


def _fake_client_factory(recorder, *, fail_urls=(), status_map=None):
    """호출 URL 을 recorder 에 기록하는 가짜 httpx.AsyncClient. fail_urls 는 transport 오류."""
    import httpx

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None):
            recorder.append(url)
            base = "/".join(url.split("/")[:3])  # scheme://host:port (경로 무관하게 executor base)
            if base in fail_urls:
                raise httpx.ConnectError("refused")
            if status_map and base in status_map:
                return _Resp(status_code=status_map[base], payload={"detail": "쿼리 실패: boom"})
            common = {"limit": 100, "columns": ["a"], "rows": [[1]],
                      "row_count": 1, "truncated": False, "elapsed_ms": 1.0}
            if url.endswith("/query-run"):
                # trino 위임 경로 — executor 는 datasource 를 모른다(응답에 datasource 없음).
                return _Resp(payload=dict(common))
            # 데이터소스 미리보기 경로는 실제 executor 처럼 datasource 명을 실어 돌려준다.
            name = url.rsplit("/datasources/", 1)[1].split("/", 1)[0]
            return _Resp(payload={"datasource": name, **common})

    return _FakeClient


@pytest.mark.parametrize("ds", ["trino", "impala", "source"])
def test_query_execute_source_proxies_to_query_run(ds, qe_client, monkeypatch):
    """모든 소스 datasource(trino/impala/source)는 executor 의 /query-run(커스텀 함수)으로 통일 위임된다.

    coordinator 는 소스 드라이버를 갖지 않고, datasource 종류와 무관하게 /query-run 하나로 보낸다.
    /query-run 응답엔 datasource 가 없으므로 coordinator 가 확정값으로 보정한다.
    """
    import coordinator.app as ca
    monkeypatch.setattr(core_config.settings, "source_type", "impala", raising=False)
    calls: list = []
    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_client_factory(calls))
    client = qe_client(executors=["http://exec1:8001"])
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [{"name": "start_dt", "value": "2026-01-01"}, {"name": "end_dt", "value": "2026-01-01"}],
        "datasource": ds,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["datasource"] == ds and body["template_id"] == "sales_migration"
    assert body["columns"] == ["a"] and body["rows"] == [[1]]
    assert body["executed_by"] == "http://exec1:8001"
    assert len(calls) == 1 and calls[0].endswith("/query-run")
    assert "proxied_to" not in body and "executor_url" not in body


def test_query_execute_default_datasource_uses_query_run(qe_client, monkeypatch):
    """datasource 미지정 시 source.type 을 따르며, 소스는 /query-run 으로 간다(built-in 경로 없음)."""
    import coordinator.app as ca
    monkeypatch.setattr(core_config.settings, "source_type", "impala", raising=False)
    calls: list = []
    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_client_factory(calls))
    client = qe_client(executors=["http://exec1:8001"])
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [{"name": "start_dt", "value": "2026-01-01"}, {"name": "end_dt", "value": "2026-01-01"}],
    })
    assert resp.status_code == 200, resp.text
    assert len(calls) == 1 and calls[0].endswith("/query-run")


def test_query_execute_impala_failover_on_transport_error(qe_client, monkeypatch):
    import coordinator.app as ca
    calls: list = []
    # exec1 은 항상 연결 실패 → exec2 로 failover.
    monkeypatch.setattr(
        ca.httpx, "AsyncClient",
        _fake_client_factory(calls, fail_urls=("http://exec1:8001",)),
    )
    client = qe_client(executors=["http://exec1:8001", "http://exec2:8001"])
    # 라운드로빈이 exec1 부터 시작하도록 여러 번 시도해도 결국 성공해야 한다.
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [{"name": "start_dt", "value": "2026-01-01"}, {"name": "end_dt", "value": "2026-01-01"}],
        "datasource": "impala",
    })
    assert resp.status_code == 200, resp.text
    # exec1 시작이었다면 2번 호출(1 실패 + 1 성공), exec2 시작이었다면 1번. 성공한 마지막은 exec2.
    assert calls[-1].startswith("http://exec2:8001")
    # failover 후 실제 실행 executor 는 exec2 로 보고된다.
    assert resp.json()["executed_by"] == "http://exec2:8001"


def test_query_execute_executor_error_propagated_no_failover(qe_client, monkeypatch):
    import coordinator.app as ca
    calls: list = []
    monkeypatch.setattr(
        ca.httpx, "AsyncClient",
        _fake_client_factory(calls, status_map={
            "http://exec1:8001": 502, "http://exec2:8001": 502,
        }),
    )
    client = qe_client(executors=["http://exec1:8001", "http://exec2:8001"])
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [{"name": "start_dt", "value": "2026-01-01"}, {"name": "end_dt", "value": "2026-01-01"}],
        "datasource": "impala",
    })
    assert resp.status_code == 502
    assert "boom" in resp.json()["detail"]
    # executor 가 확정 4xx/5xx 를 주면 failover 하지 않는다 → 정확히 1번만 호출.
    assert len(calls) == 1


def test_query_execute_impala_without_executors_400(qe_client):
    client = qe_client(executors=[])
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [{"name": "start_dt", "value": "2026-01-01"}, {"name": "end_dt", "value": "2026-01-01"}],
        "datasource": "impala",
    })
    assert resp.status_code == 400
    assert "executor" in resp.json()["detail"]


# ───────────────────────── 검증/에러 경로 ─────────────────────────


def test_query_execute_duplicate_param_422(qe_client):
    client = qe_client()
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [
            {"name": "start_dt", "value": "2026-01-01"},
            {"name": "start_dt", "value": "2026-01-02"},
        ],
        "datasource": "greenplum",
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "DUPLICATE_PARAM"


def test_query_execute_missing_required_param_422(qe_client):
    client = qe_client()
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [{"name": "start_dt", "value": "2026-01-01"}],  # end_dt 누락
        "datasource": "greenplum",
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TEMPLATE_PARAM_ERROR"


def test_query_execute_template_not_found_422(qe_client):
    client = qe_client()
    resp = client.post("/query-execute", json={
        "template_id": "does_not_exist", "params": [], "datasource": "greenplum",
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TEMPLATE_NOT_FOUND"


def test_query_execute_engine_disabled_404(runner, store, monkeypatch):
    from coordinator.config import settings
    monkeypatch.setattr(settings, "template_enabled", False, raising=False)
    client = TestClient(create_app(runner=runner, store=store))
    resp = client.post("/query-execute", json={"template_id": "x", "params": []})
    assert resp.status_code == 404


def test_query_execute_unconfigured_greenplum_400(qe_client, monkeypatch):
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "")
    client = qe_client()
    resp = client.post("/query-execute", json={
        "template_id": "sales_migration",
        "params": [{"name": "start_dt", "value": "2026-01-01"}, {"name": "end_dt", "value": "2026-01-01"}],
        "datasource": "greenplum",
    })
    assert resp.status_code == 400


# ───────────── manifest datasource + 방언 유도(요청 > manifest > source.type) ─────────────


def _recording_client_factory(recorder: list):
    """`/query-run` 호출의 (url, 본문)을 기록하는 가짜 httpx.AsyncClient.

    `_fake_client_factory` 는 URL 만 모으므로, datasource 가 executor 까지 실려 가는지
    확인하려면 본문이 필요해 따로 둔다.
    """
    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None):
            recorder.append((url, json))
            return _Resp(payload={"limit": 100, "columns": ["a"], "rows": [[1]],
                                  "row_count": 1, "truncated": False, "elapsed_ms": 1.0})

    return _FakeClient


def _tpl(tmp_path, name: str, header: str, select: str = "SELECT a FROM t") -> None:
    """테스트용 최소 템플릿(manifest + select 조각)을 만든다."""
    d = tmp_path / name
    d.mkdir()
    (d / "manifest.yml").write_text(
        f"id: {name}\nexec_mode: copy\npartition_column: dt\ntarget_table: public.t\n"
        f"strict_validation: false\n{header}files: {{select: s.j2}}\n",
        encoding="utf-8",
    )
    (d / "s.j2").write_text(select + "\n", encoding="utf-8")


def test_manifest_datasource_selects_engine_and_is_sent_to_executor(qe_client, monkeypatch, tmp_path):
    """manifest 의 datasource 가 실행 엔진을 정하고, executor 까지 실려 간다."""
    from coordinator.config import settings
    import coordinator.app as ca
    _tpl(tmp_path, "ds_trino", "datasource: trino\n")
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
    monkeypatch.setattr(core_config.settings, "source_type", "impala", raising=False)
    calls: list = []
    monkeypatch.setattr(ca.httpx, "AsyncClient", _recording_client_factory(calls))
    client = qe_client(executors=["http://exec1:8001"])
    resp = client.post("/query-execute", json={"template_id": "ds_trino", "params": []})
    assert resp.status_code == 200, resp.text
    # source.type=impala 인데도 manifest 의 trino 가 이겼다.
    assert resp.json()["datasource"] == "trino"
    url, body = calls[0]
    assert url.endswith("/query-run") and body["datasource"] == "trino"


def test_request_datasource_beats_manifest(qe_client, monkeypatch, tmp_path):
    """요청의 datasource 는 manifest 기본값을 이긴다."""
    from coordinator.config import settings
    import coordinator.app as ca
    _tpl(tmp_path, "ds_trino2", "datasource: trino\n")
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
    calls: list = []
    monkeypatch.setattr(ca.httpx, "AsyncClient", _recording_client_factory(calls))
    client = qe_client(executors=["http://exec1:8001"])
    resp = client.post("/query-execute", json={
        "template_id": "ds_trino2", "params": [], "datasource": "impala",
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["datasource"] == "impala"
    assert calls[0][1]["datasource"] == "impala"


def test_datasource_derives_dialect_for_trino_only_syntax(qe_client, monkeypatch, tmp_path):
    """datasource=trino 면 sql_dialect 생략에도 trino 로 파싱된다(hive 면 파싱 실패할 SQL)."""
    from coordinator.config import settings
    import coordinator.app as ca
    # trunc(date) 1-인자 + interval 은 hive 파서가 거부하고 trino 파서만 읽는다.
    # 1-인자 trunc(date) + 따옴표 없는 interval — Impala 관용 문법. trino 파서만 읽고 hive 는 거부한다.
    sql = ("SELECT a FROM t WHERE dt BETWEEN trunc(current_date() - interval 7 day) "
           "AND trunc(current_date())")
    _tpl(tmp_path, "ds_dialect", "datasource: trino\n", select=sql)
    _tpl(tmp_path, "ds_dialect_hive", "datasource: impala\n", select=sql)
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
    monkeypatch.setattr(core_config.settings, "query_default_dialect", "hive", raising=False)
    calls: list = []
    monkeypatch.setattr(ca.httpx, "AsyncClient", _recording_client_factory(calls))
    client = qe_client(executors=["http://exec1:8001"])
    # trino 유도 → 통과
    assert client.post("/query-execute", json={
        "template_id": "ds_dialect", "params": []}).status_code == 200
    # impala 는 유도하지 않으므로 전역 기본(hive)으로 파싱 → 같은 SQL 이 422
    bad = client.post("/query-execute", json={"template_id": "ds_dialect_hive", "params": []})
    assert bad.status_code == 422, bad.text


def test_manifest_sql_dialect_beats_datasource_derivation(qe_client, monkeypatch, tmp_path):
    """manifest 의 sql_dialect 는 datasource 유도를 이긴다(Impala 실행 + trino 파싱 조합)."""
    from coordinator.config import settings
    import coordinator.app as ca
    # 1-인자 trunc(date) + 따옴표 없는 interval — Impala 관용 문법. trino 파서만 읽고 hive 는 거부한다.
    sql = ("SELECT a FROM t WHERE dt BETWEEN trunc(current_date() - interval 7 day) "
           "AND trunc(current_date())")
    _tpl(tmp_path, "ds_override", "datasource: impala\nsql_dialect: trino\n", select=sql)
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
    monkeypatch.setattr(core_config.settings, "query_default_dialect", "hive", raising=False)
    calls: list = []
    monkeypatch.setattr(ca.httpx, "AsyncClient", _recording_client_factory(calls))
    client = qe_client(executors=["http://exec1:8001"])
    resp = client.post("/query-execute", json={"template_id": "ds_override", "params": []})
    assert resp.status_code == 200, resp.text
    assert resp.json()["datasource"] == "impala"
