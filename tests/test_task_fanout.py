"""날짜 fan-out(/jobs) 검증 — IN 분할 대신 '하루=1 task'.

구간은 ``task_params`` 가 가리키는 두 파라미터의 (값, sign)에서 도출한다. sign 은 값의
부호가 아니라 **SQL 연산자의 방향**이라, Impala ``interval``(절대값만 허용)에도 그대로
들어간다. 실 DB 불필요: 오프셋 계산은 순수 함수, API 는 dry_run 으로 executor 없이 계획만 본다.
"""
from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from coordinator.app import (
    _compute_task_offsets,
    _offset_value_sign,
    _param_offset,
    _split_params,
    create_app,
)
from coordinator.job_store import JobStore
from coordinator.parser import QueryValidationError
from core.timeutil import now_dt

REPO_TEMPLATES = Path(__file__).resolve().parent.parent / "templates"


# ───────────────────────── 순수 함수 ─────────────────────────


def test_param_offset_sign_over_value():
    # sign 이 있으면 값은 크기로만 본다(부호를 이중 적용하지 않는다).
    assert _param_offset("from_date_no", 7, "-") == -7
    assert _param_offset("from_date_no", -7, "-") == -7
    assert _param_offset("to_date_no", 1, "+") == 1
    # sign 이 없으면 값 자체의 부호를 쓴다(두 표기가 같은 뜻이 되도록).
    assert _param_offset("to_date_no", -7, None) == -7
    assert _param_offset("to_date_no", "3", None) == 3


def test_param_offset_not_numeric():
    with pytest.raises(QueryValidationError) as ei:
        _param_offset("from_date_no", "7일", "-")
    assert ei.value.code == "TASK_PARAM_NOT_NUMERIC"


def test_compute_task_offsets_point_and_pair():
    # point: 양끝 포함 비교(BETWEEN/=)용 → 날짜 수만큼
    assert _compute_task_offsets(-7, 1, "point") == [
        (-7, -7), (-6, -6), (-5, -5), (-4, -4), (-3, -3),
        (-2, -2), (-1, -1), (0, 0), (1, 1),
    ]
    # pair: 반열림 비교(>= AND <)용 → 날짜 수 - 1, 경계가 겹치지 않고 이어붙는다
    assert _compute_task_offsets(-7, 1, "pair") == [
        (-7, -6), (-6, -5), (-5, -4), (-4, -3), (-3, -2), (-2, -1), (-1, 0), (0, 1),
    ]
    assert _compute_task_offsets(0, 0, "point") == [(0, 0)]


def test_compute_task_offsets_empty_and_too_large():
    with pytest.raises(QueryValidationError) as ei:
        _compute_task_offsets(0, 0, "pair")  # 폭 0 → pair 로는 task 가 안 생긴다
    assert ei.value.code == "TASK_RANGE_EMPTY"
    with pytest.raises(QueryValidationError) as ei:
        _compute_task_offsets(-5000, 0, "point")
    assert ei.value.code == "TASK_RANGE_TOO_LARGE"


def test_offset_value_sign_is_absolute():
    # interval 뒤에는 절대값만 와야 하므로 부호는 항상 분리된다.
    assert _offset_value_sign(-7) == (7, "-")
    assert _offset_value_sign(0) == (0, "+")
    assert _offset_value_sign(1) == (1, "+")


def test_split_params_dict_and_array():
    assert _split_params({"region": "KR"}) == ({"region": "KR"}, {})
    values, signs = _split_params([
        {"name": "from_date_no", "value": 7, "sign": "-"},
        {"name": "to_date_no", "value": 1, "sign": "+"},
        {"name": "region", "value": "KR"},
    ])
    # sign 은 값에 적용하지 않는다(연산자 방향이지 값의 부호가 아니므로).
    assert values == {"from_date_no": 7, "to_date_no": 1, "region": "KR"}
    assert signs == {"from_date_no": "-", "to_date_no": "+"}


def test_split_params_duplicate():
    with pytest.raises(QueryValidationError) as ei:
        _split_params([{"name": "a", "value": 1}, {"name": "a", "value": 2}])
    assert ei.value.code == "DUPLICATE_PARAM"


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


def _dates(lo: int, hi: int) -> list[str]:
    today = now_dt().date()
    return [(today + datetime.timedelta(days=d)).strftime("%Y-%m-%d") for d in range(lo, hi + 1)]


def test_fanout_dry_run_one_task_per_day(jobs_client):
    """날짜 리터럴 방식(daily_sales) — 구간 [-2, 0] → 3 task, 각각 하루치."""
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales",
        "params": [
            {"name": "from_date_no", "value": 2, "sign": "-"},
            {"name": "to_date_no", "value": 0, "sign": "+"},
            {"name": "region", "value": "KR"},
        ],
        "task_params": ["from_date_no", "to_date_no"],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    b = resp.json()
    dates = _dates(-2, 0)
    assert b["exec_mode"] == "stage_insert"
    assert b["partition_column"] == "dt"      # manifest 기본값(표시용)
    assert b["target_table"] == "public.sales"
    assert b["task_count"] == len(dates) == 3
    for task, date in zip(b["tasks"], dates):
        assert task["partition_values"] == [date]
        assert f"= '{date}'" in task["sub_query"]
        assert " IN (" not in task["sub_query"]
    # INSERT 는 날짜 독립(모든 task 공유)
    assert b["tasks"][0]["insert_sql"].startswith("INSERT INTO")


def test_fanout_interval_sign_absolute_values(jobs_client):
    """interval 방식(daily_sales_interval) — 구간 [-7, +1] → 9 task.

    핵심: interval 뒤에는 **절대값만** 나와야 하고, 방향은 연산자로 나가야 한다.
    각 task 는 두 끝이 같은 날이라 BETWEEN 이 하루로 붕괴한다.
    """
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales_interval",
        "params": [
            {"name": "from_date_no", "value": 7, "sign": "-"},
            {"name": "to_date_no", "value": 1, "sign": "+"},
        ],
        "task_params": ["from_date_no", "to_date_no"],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    b = resp.json()
    assert b["task_count"] == 9  # -7 … +1 (양끝 포함)

    expected = [(abs(d), "-" if d < 0 else "+") for d in range(-7, 2)]
    for task, (magnitude, sign) in zip(b["tasks"], expected):
        sql = task["sub_query"]
        # 두 끝이 같은 부호·같은 크기 → 하루로 붕괴
        assert sql.count(f"{sign} interval {magnitude} day") == 2
        assert "interval -" not in sql          # 음수 리터럴이 절대 나오면 안 된다
    assert b["tasks"][0]["partition_values"] == _dates(-7, -7)


def test_fanout_pair_bound_halfopen(jobs_client):
    """task_bound=pair → (d, d+1) 로 이어붙어 task 수가 하나 줄어든다."""
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales_interval",
        "params": [
            {"name": "from_date_no", "value": 3, "sign": "-"},
            {"name": "to_date_no", "value": 0, "sign": "+"},
        ],
        "task_params": ["from_date_no", "to_date_no"],
        "task_bound": "pair",
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    b = resp.json()
    assert b["task_count"] == 3  # [-3, 0] → (-3,-2) (-2,-1) (-1,0)
    first = b["tasks"][0]
    assert first["partition_values"] == [_dates(-3, -3)[0], _dates(-2, -2)[0]]
    assert "- interval 3 day" in first["sub_query"]
    assert "- interval 2 day" in first["sub_query"]


def test_fanout_params_dict_uses_value_sign(jobs_client):
    """params 를 dict 로 보내면(하위 호환) 값 자체의 부호가 방향이 된다."""
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales",
        "params": {"from_date_no": -2, "to_date_no": 0},
        "task_params": ["from_date_no", "to_date_no"],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["task_count"] == 3


def test_fanout_missing_sign_var_rejected(jobs_client, tmp_path, monkeypatch):
    """부호 없이 interval 을 쓰는 템플릿은 422 — 조용히 넓은 구간을 읽는 사고를 막는다."""
    from coordinator.config import settings
    tpl = tmp_path / "bad_interval"
    tpl.mkdir()
    (tpl / "manifest.yml").write_text(
        "id: bad_interval\nexec_mode: stage_insert\ntarget_table: public.sales\n"
        "staging_table: stg\nstrict_validation: false\n"
        "params:\n  - {name: from_date_no, type: int}\n  - {name: to_date_no, type: int}\n"
        "files:\n  select: select.sql.j2\n  insert: insert.sql.j2\n",
        encoding="utf-8",
    )
    (tpl / "select.sql.j2").write_text(
        "SELECT 1 FROM sales WHERE dt BETWEEN "
        "trunc(current_date() - interval {{ from_date_no | sql_num }} day) AND "
        "trunc(current_date() + interval {{ to_date_no | sql_num }} day)\n",
        encoding="utf-8",
    )
    (tpl / "insert.sql.j2").write_text(
        "INSERT INTO public.sales SELECT * FROM stg\n", encoding="utf-8"
    )
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
    client = TestClient(create_app(store=JobStore()))

    resp = client.post("/jobs", json={
        "template_id": "bad_interval",
        "params": [
            {"name": "from_date_no", "value": 7, "sign": "-"},
            {"name": "to_date_no", "value": 1, "sign": "+"},
        ],
        "task_params": ["from_date_no", "to_date_no"],
        "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TEMPLATE_MISSING_SIGN_VAR"


def test_fanout_requires_template(jobs_client):
    resp = jobs_client.post("/jobs", json={
        "sql": "SELECT 1", "target_table": "public.t",
        "task_params": ["from_date_no", "to_date_no"], "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "FANOUT_REQUIRES_TEMPLATE"


def test_fanout_requires_stage_insert(jobs_client):
    # exec_mode 를 copy 로 강제 오버라이드해 거부되는지 본다.
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales",
        "params": {"from_date_no": -1, "to_date_no": 0},
        "task_params": ["from_date_no", "to_date_no"],
        "exec_mode": "copy",
        "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "FANOUT_REQUIRES_STAGE_INSERT"


def test_fanout_task_params_must_be_two(jobs_client):
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales",
        "params": {"from_date_no": -1},
        "task_params": ["from_date_no"],
        "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TASK_PARAMS_INVALID"


def test_fanout_task_params_must_exist_in_params(jobs_client):
    resp = jobs_client.post("/jobs", json={
        "template_id": "daily_sales",
        "params": {"from_date_no": -1, "to_date_no": 0},
        "task_params": ["from_date_no", "nope"],
        "dry_run": True,
    })
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TASK_PARAMS_INVALID"
