"""POST /query-count 검증 — 템플릿으로 렌더한 SELECT 를 COUNT(*) 로 감싸 건수만 돌려받는 경로.

실제 DB 는 필요 없다. coordinator 직접 실행(greenplum/history)은 run_postgres_select 를 가짜로
바꾸고, 소스(impala/trino) 프록시는 test_query_execute 의 가짜 httpx.AsyncClient 를 재사용한다.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import core.config as core_config
from coordinator.app import create_app
from coordinator.parser import build_count_query, validate_select_query
from core.dbprobe import QueryResult
from tests.test_query_execute import _Resp, qe_client  # noqa: F401 — fixture 재사용

SALES_PARAMS = [
    {"name": "start_dt", "value": "2026-01-01"},
    {"name": "end_dt", "value": "2026-01-02"},
    {"name": "regions", "value": ["KR"]},
]


def _count_result(value) -> QueryResult:
    return QueryResult(columns=["row_count"], rows=[[value]], row_count=1, truncated=False, elapsed_ms=2.5)


# ───────────────────────── 건수 쿼리 조립 ─────────────────────────


def test_build_count_query_wraps_and_strips_semicolon():
    """원문을 그대로 서브쿼리로 감싸고, 끝의 세미콜론은 떼어 낸다."""
    sql = build_count_query("SELECT a FROM t WHERE dt IN ('x') ;  \n")
    assert sql == "SELECT COUNT(*) AS row_count FROM (\nSELECT a FROM t WHERE dt IN ('x')\n) qc_src"
    validate_select_query(sql)


def test_build_count_query_trailing_line_comment_keeps_paren():
    """마지막 줄이 -- 주석이어도 닫는 괄호가 다음 줄에 있어 주석에 먹히지 않는다."""
    sql = build_count_query("SELECT a FROM t -- 끝 주석")
    assert sql.endswith("-- 끝 주석\n) qc_src")
    validate_select_query(sql)


# ───────────────────────── API: coordinator 직접 실행(greenplum) ─────────────────────────


def test_query_count_greenplum_direct(qe_client, monkeypatch):  # noqa: F811
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    captured = {}

    def _fake(dsn, sql, *, limit, datasource="greenplum"):
        captured.update(sql=sql, limit=limit)
        return _count_result(12345)

    monkeypatch.setattr(ca, "run_postgres_select", _fake)
    resp = qe_client().post("/query-count", json={
        "template_id": "sales_migration", "params": SALES_PARAMS, "datasource": "greenplum",
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["row_count"] == 12345
    assert body["datasource"] == "greenplum" and body["executed_by"] is None
    assert body["elapsed_ms"] == 2.5
    # 실제로 실행한 것은 렌더한 SELECT 를 감싼 건수 쿼리이고, 한 행만 받으면 된다.
    assert captured["sql"] == body["count_sql"]
    assert captured["limit"] == 1
    assert body["count_sql"].startswith("SELECT COUNT(*) AS row_count FROM (")
    assert body["sql"] in body["count_sql"] and "region IN ( 'KR' )" in body["sql"]
    # 행 자체는 응답에 싣지 않는다.
    assert "rows" not in body and "columns" not in body


@pytest.mark.parametrize("raw", ["987", 987.0, {"row_count": 987}])
def test_query_count_coerces_value_shapes(raw, qe_client, monkeypatch):  # noqa: F811
    """문자열·실수·dict 행으로 와도 정수 건수로 맞춘다."""
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    rows = [raw] if isinstance(raw, dict) else [[raw]]
    monkeypatch.setattr(
        ca, "run_postgres_select",
        lambda dsn, sql, *, limit, datasource="greenplum": QueryResult(
            columns=["row_count"], rows=rows, row_count=1, truncated=False, elapsed_ms=1.0),
    )
    resp = qe_client().post("/query-count", json={
        "template_id": "sales_migration", "params": SALES_PARAMS, "datasource": "greenplum",
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["row_count"] == 987


def test_query_count_empty_result_502(qe_client, monkeypatch):  # noqa: F811
    """건수 쿼리가 행을 돌려주지 않으면 실행 쪽 계약 위반이라 502 다."""
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    monkeypatch.setattr(
        ca, "run_postgres_select",
        lambda dsn, sql, *, limit, datasource="greenplum": QueryResult(
            columns=["row_count"], rows=[], row_count=0, truncated=False, elapsed_ms=1.0),
    )
    resp = qe_client().post("/query-count", json={
        "template_id": "sales_migration", "params": SALES_PARAMS, "datasource": "greenplum",
    })
    assert resp.status_code == 502


# ───────────────────────── API: 소스 executor 위임 ─────────────────────────


def test_query_count_source_proxies_count_sql_to_query_run(qe_client, monkeypatch):  # noqa: F811
    """소스 데이터소스는 /query-run 으로 건수 쿼리(limit=1)를 보낸다."""
    import coordinator.app as ca
    sent: list = []

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None):
            sent.append((url, json))
            return _Resp(payload={"limit": 1, "columns": ["row_count"], "rows": [[42]],
                                  "row_count": 1, "truncated": False, "elapsed_ms": 3.0})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _FakeClient)
    resp = qe_client(executors=["http://exec1:8001"]).post("/query-count", json={
        "template_id": "sales_migration", "params": SALES_PARAMS, "datasource": "trino",
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["row_count"] == 42 and body["datasource"] == "trino"
    assert body["executed_by"] == "http://exec1:8001"
    url, payload = sent[0]
    assert url.endswith("/query-run")
    assert payload == {"sql": body["count_sql"], "limit": 1, "datasource": "trino"}


# ───────────────────────── 검증/에러 경로 ─────────────────────────


def test_query_count_template_not_found_422(qe_client):  # noqa: F811
    resp = qe_client().post("/query-count", json={
        "template_id": "does_not_exist", "params": [], "datasource": "greenplum",
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TEMPLATE_NOT_FOUND"


def test_query_count_engine_disabled_404(runner, store, monkeypatch):
    from coordinator.config import settings
    monkeypatch.setattr(settings, "template_enabled", False, raising=False)
    client = TestClient(create_app(runner=runner, store=store))
    resp = client.post("/query-count", json={"template_id": "x", "params": []})
    assert resp.status_code == 404


def test_query_count_unknown_datasource_404(qe_client):  # noqa: F811
    resp = qe_client().post("/query-count", json={
        "template_id": "sales_migration", "params": SALES_PARAMS, "datasource": "oracle",
    })
    assert resp.status_code == 404


# ───────────────────────── 예제 템플릿: order_count ─────────────────────────


def _post_order_count(client, params):
    return client.post("/query-count", json={
        "template_id": "order_count", "params": params, "datasource": "greenplum",
    })


def test_query_count_order_count_example(qe_client, monkeypatch):  # noqa: F811
    """예제 order_count — 조인한 SELECT 를 감싸 세고, 겹치는 컬럼에는 별칭이 붙어 있다."""
    import sqlglot

    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    captured = {}
    monkeypatch.setattr(ca, "run_postgres_select",
                        lambda dsn, sql, *, limit, datasource="greenplum":
                        (captured.update(sql=sql) or _count_result(321)))
    resp = _post_order_count(qe_client(), [
        {"name": "regions", "value": ["KR", "US"]},
        {"name": "start_dt", "value": "2026-01-01"},
        {"name": "end_dt", "value": "2026-01-31"},
        {"name": "grade", "value": "VIP"},
    ])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["row_count"] == 321 and captured["sql"] == body["count_sql"]
    sql = body["sql"]
    assert "o.region IN ( 'KR', 'US' )" in sql
    assert "o.order_dt BETWEEN '2026-01-01' AND '2026-01-31'" in sql
    assert "c.grade = 'VIP'" in sql
    # 감싼 쿼리가 엔진에서 거절되지 않도록 출력 컬럼 이름이 겹치지 않아야 한다.
    names = [e.alias_or_name for e in sqlglot.parse_one(sql, read="hive").expressions]
    assert len(names) == len(set(names)), names
    for dialect in ("hive", "trino", "postgres"):
        validate_select_query(body["count_sql"], dialect=dialect)


def test_query_count_order_count_grade_omitted(qe_client, monkeypatch):  # noqa: F811
    """grade 를 빼면 등급 조건이 생략된다."""
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    monkeypatch.setattr(ca, "run_postgres_select",
                        lambda dsn, sql, *, limit, datasource="greenplum": _count_result(5))
    resp = _post_order_count(qe_client(), [
        {"name": "regions", "value": ["KR"]},
        {"name": "start_dt", "value": "2026-01-01"},
        {"name": "end_dt", "value": "2026-01-31"},
    ])
    assert resp.status_code == 200, resp.text
    assert "c.grade =" not in resp.json()["sql"]


def test_query_count_logs_rendered_and_count_sql_multiline(qe_client, monkeypatch, caplog):  # noqa: F811
    """렌더한 SELECT 와 감싼 건수 쿼리가 원문 줄 그대로 남고, 요청 식별자가 함께 찍힌다."""
    import logging

    from core.logging import current_job_label

    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    monkeypatch.setattr(core_config.settings, "log_sql_format", "multiline", raising=False)
    import coordinator.app as ca
    monkeypatch.setattr(ca, "run_postgres_select",
                        lambda dsn, sql, *, limit, datasource="greenplum": _count_result(1))
    labels = []
    real = ca.log_template_render

    def _spy(*a, **k):
        labels.append(current_job_label())
        return real(*a, **k)

    monkeypatch.setattr(ca, "log_template_render", _spy)
    with caplog.at_level(logging.INFO, logger="core.sql"):
        resp = _post_order_count(qe_client(), [
            {"name": "regions", "value": ["KR"]},
            {"name": "start_dt", "value": "2026-01-01"},
            {"name": "end_dt", "value": "2026-01-31"},
        ])
    assert resp.status_code == 200, resp.text
    msg = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("템플릿 렌더"))
    assert "template=order_count" in msg and "(datasource=greenplum)" in msg
    assert "    | SELECT o.order_id," in msg          # 여러 줄 원문이 줄 단위로 남는다
    assert "    |        o.customer_id," in msg
    assert "  [count]" in msg and "    | SELECT COUNT(*) AS row_count FROM (" in msg
    # 렌더 로그는 그 요청의 식별자(req-<X-Request-ID>)로 찍힌다.
    assert labels == ["req-" + resp.headers["x-request-id"]]


def test_query_count_logs_render_even_when_validation_fails(qe_client, monkeypatch, caplog, tmp_path):  # noqa: F811
    """검증이 실패해 422 가 나가도 원인이 된 렌더 SQL 은 로그에 남는다."""
    import logging

    from coordinator.config import settings
    tdir = tmp_path / "bad"
    tdir.mkdir()
    (tdir / "manifest.yml").write_text("id: bad\nexec_mode: copy\nfiles: {select: s.j2}\n", encoding="utf-8")
    (tdir / "s.j2").write_text("DELETE FROM orders\n", encoding="utf-8")
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
    client = qe_client()
    with caplog.at_level(logging.INFO, logger="core.sql"):
        resp = client.post("/query-count", json={"template_id": "bad", "params": [], "datasource": "greenplum"})
    assert resp.status_code == 422
    assert any("    | DELETE FROM orders" in r.getMessage() for r in caplog.records)


# ───────────────────────── manifest 의 datasource·sql_dialect 반영 ─────────────────────────


def _trino_template(tmp_path, manifest_extra: str):
    """manifest 스칼라만 바꿔 가며 쓰는 select 전용 템플릿을 만든다.

    select 는 1-인자 ``trunc(date)`` 를 쓰는데, 서버 기본 방언 hive 는 이를 거부하고 trino 는
    받아들이므로 어느 방언으로 검증했는지 응답 코드로 드러난다.
    """
    tdir = tmp_path / "tq"
    tdir.mkdir()
    (tdir / "manifest.yml").write_text(
        "id: tq\nexec_mode: copy\n" + manifest_extra + "files: {select: s.sql.j2}\n", encoding="utf-8",
    )
    (tdir / "s.sql.j2").write_text(
        "SELECT order_id FROM hive.default.orders WHERE order_dt >= trunc(current_date)\n",
        encoding="utf-8",
    )
    return str(tmp_path)


def _capture_query_run(monkeypatch):
    """/query-run 으로 나가는 요청 본문을 모으는 가짜 httpx.AsyncClient 를 심는다."""
    import coordinator.app as ca
    sent: list = []

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None):
            sent.append(json)
            return _Resp(payload={"limit": 1, "columns": ["row_count"], "rows": [[7]],
                                  "row_count": 1, "truncated": False, "elapsed_ms": 1.0})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _FakeClient)
    return sent


def test_query_count_uses_manifest_datasource_and_dialect(qe_client, monkeypatch, tmp_path):  # noqa: F811
    """요청에 없으면 manifest 의 datasource·sql_dialect 를 쓴다."""
    from coordinator.config import settings
    monkeypatch.setattr(settings, "source_type", "impala", raising=False)
    monkeypatch.setattr(settings, "template_dir",
                        _trino_template(tmp_path, "datasource: trino\nsql_dialect: trino\n"), raising=False)
    sent = _capture_query_run(monkeypatch)
    resp = qe_client(executors=["http://exec1:8001"]).post(
        "/query-count", json={"template_id": "tq", "params": []},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["datasource"] == "trino"
    assert sent[0]["datasource"] == "trino"


def test_query_count_request_overrides_manifest(qe_client, monkeypatch, tmp_path):  # noqa: F811
    """요청의 datasource·sql_dialect 가 manifest 보다 우선한다."""
    from coordinator.config import settings
    monkeypatch.setattr(settings, "template_dir",
                        _trino_template(tmp_path, "datasource: trino\nsql_dialect: trino\n"), raising=False)
    sent = _capture_query_run(monkeypatch)
    client = qe_client(executors=["http://exec1:8001"])
    resp = client.post("/query-count", json={"template_id": "tq", "params": [], "datasource": "impala"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["datasource"] == "impala" and sent[0]["datasource"] == "impala"
    # 요청이 hive 를 고르면 manifest 의 trino 대신 hive 로 검증해 trunc(date) 가 거절된다.
    resp = client.post("/query-count", json={"template_id": "tq", "params": [], "sql_dialect": "hive"})
    assert resp.status_code == 422


def test_query_execute_falls_back_to_server_settings(qe_client, monkeypatch, tmp_path):  # noqa: F811
    """manifest 에도 없으면 서버 source.type·query.sql_dialect 를 따른다."""
    from coordinator.config import settings
    monkeypatch.setattr(settings, "source_type", "impala", raising=False)
    monkeypatch.setattr(settings, "query_default_dialect", "hive", raising=False)
    monkeypatch.setattr(settings, "template_dir", _trino_template(tmp_path, ""), raising=False)
    _capture_query_run(monkeypatch)
    client = qe_client(executors=["http://exec1:8001"])
    # 기본 방언 hive 로 검증하므로 trunc(date) 가 거절된다.
    assert client.post("/query-execute", json={"template_id": "tq", "params": []}).status_code == 422
    # 서버 기본 방언을 trino 로 바꾸면 통과하고, datasource 는 source.type 을 따른다.
    monkeypatch.setattr(settings, "query_default_dialect", "trino", raising=False)
    resp = client.post("/query-execute", json={"template_id": "tq", "params": []})
    assert resp.status_code == 200, resp.text
    assert resp.json()["datasource"] == "impala"


# ───────────────────────── 부호(sign) 파라미터 ─────────────────────────


@pytest.mark.parametrize("params", [
    # sign 을 명시한 표기 — 값은 절대값, 방향은 sign.
    [{"name": "from_date_no", "value": 7, "sign": "-"}, {"name": "to_date_no", "value": 1, "sign": "-"}],
    # sign 없이 값에 부호를 붙인 표기 — 같은 SQL 로 렌더돼야 한다.
    [{"name": "from_date_no", "value": -7}, {"name": "to_date_no", "value": -1}],
])
def test_query_count_renders_sign_params(params, qe_client, monkeypatch):  # noqa: F811
    """interval 템플릿의 <name>_sign 이 query-count 경로에서도 채워진다(예전엔 미정의 변수 422)."""
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    monkeypatch.setattr(ca, "run_postgres_select", lambda dsn, sql, **k: _count_result(3))
    resp = qe_client().post("/query-count", json={
        "template_id": "daily_sales_interval", "params": params, "datasource": "greenplum",
    })
    assert resp.status_code == 200, resp.text
    sql = resp.json()["sql"]
    assert "current_date() - interval 7 day" in sql
    assert "current_date() - interval 1 day" in sql


def test_query_execute_positive_value_without_sign_renders_plus(qe_client, monkeypatch):  # noqa: F811
    """sign 이 없고 값이 양수면 + 로 렌더된다(query-execute 도 같은 경로)."""
    monkeypatch.setattr(core_config.settings, "greenplum_dsn", "postgresql://x")
    import coordinator.app as ca
    monkeypatch.setattr(ca, "run_postgres_select", lambda dsn, sql, **k: _count_result(3))
    resp = qe_client().post("/query-execute", json={
        "template_id": "daily_sales_interval", "datasource": "greenplum",
        "params": [{"name": "from_date_no", "value": -3}, {"name": "to_date_no", "value": 2}],
    })
    assert resp.status_code == 200, resp.text
    sql = resp.json()["sql"]
    assert "current_date() - interval 3 day" in sql and "current_date() + interval 2 day" in sql
