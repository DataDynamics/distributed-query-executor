"""날짜 태스크 컬럼 fan-out(/jobs) 검증 — IN 분할 대신 '하루=1 task'.

실 DB 불필요: 날짜 계산은 순수 함수, API 는 dry_run 으로 executor 없이 계획만 확인한다.
"""
from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from coordinator.app import _compute_task_dates, create_app
from coordinator.job_store import JobStore
from coordinator.parser import QueryValidationError
from core.timeutil import now_dt

REPO_TEMPLATES = Path(__file__).resolve().parent.parent / "packaging" / "config" / "templates"


# ───────────────────────── 순수 함수 ─────────────────────────


def test_compute_task_dates_inclusive():
    today = datetime.date(2026, 7, 10)
    assert _compute_task_dates([-7, 0], today) == [
        "2026-07-03", "2026-07-04", "2026-07-05", "2026-07-06",
        "2026-07-07", "2026-07-08", "2026-07-09", "2026-07-10",
    ]
    assert _compute_task_dates([0, 0], today) == ["2026-07-10"]
    # 뒤집힌 범위도 정상 정렬
    assert _compute_task_dates([0, -2], today) == ["2026-07-08", "2026-07-09", "2026-07-10"]


def test_compute_task_dates_format():
    today = datetime.date(2026, 7, 10)
    assert _compute_task_dates([-1, 0], today, fmt="%Y%m%d") == ["20260709", "20260710"]


def test_compute_task_dates_invalid():
    with pytest.raises(QueryValidationError) as ei:
        _compute_task_dates([1], datetime.date(2026, 7, 10))
    assert ei.value.code == "TASK_RANGE_INVALID"


# ───────────────────────── API (dry-run) ─────────────────────────


@pytest.fixture
def jobs_client(monkeypatch):
    from coordinator.config import settings
    monkeypatch.setattr(settings, "template_enabled", True, raising=False)
    monkeypatch.setattr(settings, "template_dir", str(REPO_TEMPLATES), raising=False)
    monkeypatch.setattr(settings, "template_auto_reload", False, raising=False)
    monkeypatch.setattr(settings, "template_func_modules", [], raising=False)
    monkeypatch.setattr(settings, "executors", [], raising=False)
    return TestClient(create_app(store=JobStore()))


def _expected_dates(rng):
    today = now_dt().date()
    lo, hi = sorted(rng)
    return [(today + datetime.timedelta(days=d)).strftime("%Y-%m-%d") for d in range(lo, hi + 1)]


def test_fanout_dry_run_one_task_per_day(jobs_client):
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales",
        "params": {"region": "KR"},
        "task_column": "dt",
        "task_range": [-2, 0],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    b = resp.json()
    dates = _expected_dates([-2, 0])
    assert b["exec_mode"] == "stage_insert"
    assert b["partition_column"] == "dt"
    assert b["target_table"] == "public.sales"
    assert b["task_count"] == len(dates) == 3
    # 각 task 는 그 하루치만 조회하고(partition_values 는 raw 날짜), IN 절이 없다.
    for task, date in zip(b["tasks"], dates):
        assert task["partition_values"] == [date]
        assert f"= '{date}'" in task["sub_query"]
        assert " IN (" not in task["sub_query"]
    # INSERT 는 날짜 독립(모든 task 공유)
    assert b["tasks"][0]["insert_sql"].startswith("INSERT INTO")


def test_fanout_requires_template(jobs_client):
    resp = jobs_client.post("/jobs", json={
        "sql": "SELECT 1", "target_table": "public.t",
        "task_column": "dt", "task_range": [-1, 0], "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "FANOUT_REQUIRES_TEMPLATE"


def test_fanout_requires_stage_insert(jobs_client):
    # sales_migration 도 stage_insert 라 통과하므로, copy 예제(order_search? 는 query-execute용)…
    # 여기서는 exec_mode 를 copy 로 강제 오버라이드해 거부되는지 본다.
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales",
        "task_column": "dt", "task_range": [-1, 0],
        "exec_mode": "copy",
        "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "FANOUT_REQUIRES_STAGE_INSERT"


def test_fanout_invalid_range(jobs_client):
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales", "task_column": "dt", "task_range": [1, 2, 3], "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TASK_RANGE_INVALID"
