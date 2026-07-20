"""쿼리 템플릿 엔진 테스트(실 DB 불필요 — 렌더는 순수 함수, 디스패치는 FakeRunner).

검증 범위:
  - 커스텀 함수/필터(sql_str/sql_in/sql_num/sql_ident/date_range)와 인젝션 이스케이프.
  - 엔진 렌더: 파라미터 검증(필수/타입/기본값), exec_mode 별 조각, 단일 문 검사, 경로 탈출 차단.
  - API 통합: template_id + params 로 POST /jobs → 렌더된 SELECT 가 splitter 로 분할되는지.
  - 하위 호환: template_id 없이 raw-SQL 요청이 그대로 동작, 필수 필드 누락 시 422.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.job_store import JobStore
from coordinator.template import TemplateEngine, TemplateError
from coordinator import template_funcs as tf


# ───────────────────────── 커스텀 함수/필터 ─────────────────────────


def test_sql_str_escapes_single_quote():
    assert tf.sql_str("O'Brien") == "'O''Brien'"
    assert tf.sql_str(None) == "NULL"


def test_sql_in_mixed_types_and_empty():
    assert tf.sql_in(["KR", "US"]) == "'KR', 'US'"
    assert tf.sql_in([1, 2, 3]) == "1, 2, 3"
    assert tf.sql_in([]) == "NULL"  # 빈 목록 → 항상 거짓(안전한 빈 집합)


def test_sql_in_escapes_injection():
    # 인젝션 시도 문자열이 리터럴로 이스케이프되어 SQL 구조를 깨지 못한다.
    out = tf.sql_in(["x'); DROP TABLE t;--"])
    assert out == "'x''); DROP TABLE t;--'"


def test_sql_num_rejects_non_numeric():
    assert tf.sql_num("42") == "42"
    assert tf.sql_num(3.5) == "3.5"
    with pytest.raises(ValueError):
        tf.sql_num("1 OR 1=1")


def test_sql_ident_quotes_and_qualifies():
    assert tf.sql_ident("sales") == '"sales"'
    assert tf.sql_ident("public.sales") == '"public"."sales"'
    assert tf.sql_ident('a"b') == '"a""b"'


def test_date_range_inclusive():
    assert tf.date_range("2026-01-01", "2026-01-03") == [
        "2026-01-01", "2026-01-02", "2026-01-03",
    ]
    assert tf.date_range("2026-01-03", "2026-01-01") == []  # start>end → 빈 리스트


# ───────────────────────── 엔진(격리 tmp 템플릿) ─────────────────────────


class _Settings:
    """엔진 단위 테스트용 최소 설정 셰임."""

    def __init__(self, template_dir: Path):
        self.template_dir = str(template_dir)
        self.template_auto_reload = False
        self.template_func_modules = []
        self.template_validate_ddl_single_stmt = True


def _make_template(root: Path, tid: str, manifest: str, files: dict):
    d = root / tid
    d.mkdir(parents=True)
    (d / "manifest.yml").write_text(textwrap.dedent(manifest), encoding="utf-8")
    for name, content in files.items():
        (d / name).write_text(textwrap.dedent(content), encoding="utf-8")


@pytest.fixture
def engine(tmp_path):
    _make_template(
        tmp_path, "t_copy",
        """
        id: t_copy
        exec_mode: copy
        partition_column: dt
        target_table: public.t
        params:
          - {name: dts, type: list, required: true}
          - {name: region, type: string, required: false}
        files:
          select: select.sql.j2
        """,
        {"select.sql.j2": (
            "SELECT a, dt FROM t\n"
            "WHERE dt IN ( {{ dts | sql_in }} )\n"
            "{%- if region %}\n  AND region = {{ region | sql_str }}\n{%- endif %}\n"
        )},
    )
    return TemplateEngine(_Settings(tmp_path))


def test_engine_renders_copy(engine):
    r = engine.render("t_copy", {"dts": ["2026-01-01", "2026-01-02"]})
    assert r.exec_mode == "copy"
    assert "dt IN ( '2026-01-01', '2026-01-02' )" in r.select
    assert "region" not in r.select  # region 미지정 → 조건 생략
    assert r.defaults["partition_column"] == "dt"


def test_engine_optional_param_applied(engine):
    r = engine.render("t_copy", {"dts": ["x"], "region": "KR"})
    assert "AND region = 'KR'" in r.select


def test_engine_required_param_missing(engine):
    with pytest.raises(TemplateError) as ei:
        engine.render("t_copy", {})
    assert ei.value.code == "TEMPLATE_PARAM_ERROR"


def test_engine_list_type_mismatch(engine):
    with pytest.raises(TemplateError) as ei:
        engine.render("t_copy", {"dts": "not-a-list"})
    assert ei.value.code == "TEMPLATE_PARAM_ERROR"


def test_engine_template_not_found(engine):
    with pytest.raises(TemplateError) as ei:
        engine.render("does_not_exist", {"dts": ["x"]})
    assert ei.value.code == "TEMPLATE_NOT_FOUND"


def test_engine_id_path_traversal_blocked(engine):
    with pytest.raises(TemplateError) as ei:
        engine.render("../secrets", {"dts": ["x"]})
    assert ei.value.code == "TEMPLATE_ID_INVALID"


def test_engine_undefined_variable_fails(tmp_path):
    _make_template(
        tmp_path, "bad",
        """
        id: bad
        exec_mode: copy
        partition_column: dt
        files: {select: s.j2}
        """,
        {"s.j2": "SELECT * FROM t WHERE dt IN ({{ missing_var }})"},
    )
    eng = TemplateEngine(_Settings(tmp_path))
    with pytest.raises(TemplateError) as ei:
        eng.render("bad", {})
    assert ei.value.code == "TEMPLATE_RENDER_ERROR"


def test_engine_stage_insert_requires_insert(tmp_path):
    # stage_insert 인데 insert 조각이 없으면 오류.
    _make_template(
        tmp_path, "si",
        """
        id: si
        exec_mode: stage_insert
        partition_column: dt
        files: {select: s.j2}
        """,
        {"s.j2": "SELECT a, dt FROM t WHERE dt IN ('1')"},
    )
    eng = TemplateEngine(_Settings(tmp_path))
    with pytest.raises(TemplateError) as ei:
        eng.render("si", {})
    assert ei.value.code == "TEMPLATE_MISSING_ROLE"


def test_engine_multi_statement_ddl_rejected(tmp_path):
    _make_template(
        tmp_path, "si2",
        """
        id: si2
        exec_mode: stage_insert
        partition_column: dt
        staging_table: stg
        target_table: public.t
        files: {select: s.j2, staging_ddl: d.j2, insert: i.j2}
        """,
        {
            "s.j2": "SELECT a, dt FROM t WHERE dt IN ('1')",
            "d.j2": "CREATE TEMP TABLE stg (a int); DROP TABLE users",
            "i.j2": "INSERT INTO public.t SELECT * FROM stg",
        },
    )
    eng = TemplateEngine(_Settings(tmp_path))
    with pytest.raises(TemplateError) as ei:
        eng.render("si2", {})
    assert ei.value.code == "TEMPLATE_MULTIPLE_STATEMENTS"


# ───────────────────────── API 통합(예제 템플릿) ─────────────────────────


@pytest.fixture
def tmpl_client(runner, store, monkeypatch):
    """예제 템플릿 디렉터리를 가리키도록 설정을 조정한 coordinator 클라이언트."""
    from coordinator.config import settings
    repo_templates = Path(__file__).resolve().parent.parent / "conf" / "templates"
    monkeypatch.setattr(settings, "template_enabled", True, raising=False)
    monkeypatch.setattr(settings, "template_dir", str(repo_templates), raising=False)
    monkeypatch.setattr(settings, "template_auto_reload", False, raising=False)
    monkeypatch.setattr(settings, "template_func_modules", [], raising=False)
    app = create_app(runner=runner, store=store)
    return TestClient(app)


def test_api_list_templates(tmpl_client):
    body = tmpl_client.get("/templates").json()
    assert body["enabled"] is True
    ids = [t["template_id"] for t in body["templates"]]
    assert "sales_migration" in ids


def test_api_template_dry_run(tmpl_client):
    resp = tmpl_client.post("/jobs", json={
        "template_id": "sales_migration",
        "params": {"start_dt": "2026-01-01", "end_dt": "2026-01-04", "regions": ["KR"]},
        "parallelism": 2,
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["exec_mode"] == "stage_insert"
    assert body["partition_column"] == "dt"       # manifest 기본값
    assert body["target_table"] == "public.sales"  # manifest 기본값
    assert body["task_count"] == 2
    # 렌더된 SELECT 가 4개 날짜로 확장되어 2분할됐는지(각 2일)
    all_vals = [v for t in body["tasks"] for v in t["partition_values"]]
    assert all_vals == ["'2026-01-01'", "'2026-01-02'", "'2026-01-03'", "'2026-01-04'"]
    # stage_insert 조각 동봉
    assert body["tasks"][0]["insert_sql"].startswith("INSERT INTO")


def test_api_template_dispatches(tmpl_client, runner):
    resp = tmpl_client.post("/jobs", json={
        "template_id": "sales_migration",
        "params": {"start_dt": "2026-01-01", "end_dt": "2026-01-02"},
    })
    assert resp.status_code == 202, resp.text
    assert len(runner.runs) == 1
    job = runner.runs[0]
    assert job.template_id == "sales_migration"
    assert "dt IN" in job.original_sql
    assert job.exec_mode == "stage_insert"


def test_api_template_missing_required_param(tmpl_client):
    resp = tmpl_client.post("/jobs", json={
        "template_id": "sales_migration",
        "params": {"start_dt": "2026-01-01"},  # end_dt 누락
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TEMPLATE_PARAM_ERROR"


def test_api_template_not_found(tmpl_client):
    resp = tmpl_client.post("/jobs", json={
        "template_id": "nope", "params": {},
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TEMPLATE_NOT_FOUND"


def test_api_request_overrides_manifest(tmpl_client, runner):
    # 요청이 exec_mode/target_table 을 명시하면 manifest 기본값을 덮는다.
    resp = tmpl_client.post("/jobs", json={
        "template_id": "sales_migration",
        "params": {"start_dt": "2026-01-01", "end_dt": "2026-01-01"},
        "exec_mode": "copy",
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["exec_mode"] == "copy"


# ───────────────────────── 하위 호환(raw 모드) ─────────────────────────


def test_raw_mode_still_works(client, runner):
    resp = client.post("/jobs", json={
        "sql": "SELECT a, dt FROM t WHERE dt IN ('1','2')",
        "partition_column": "dt",
        "target_table": "public.t",
    })
    assert resp.status_code == 202
    assert len(runner.runs) == 1


def test_raw_mode_missing_fields_422(client):
    # sql 누락(raw 모드) → 필수 필드 검증에서 422
    resp = client.post("/jobs", json={"partition_column": "dt", "target_table": "public.t"})
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "MISSING_REQUIRED_FIELDS"
