"""분할된 sub-query를 감싸는 wrapper_query 기능 테스트."""

from __future__ import annotations

from coordinator.splitter import DEFAULT_WRAPPER_PLACEHOLDER, wrap


def test_wrap_helper_replaces_placeholder():
    out = wrap("SELECT 1", "SELECT * FROM ({{SUBQUERY}}) t")
    assert out == "SELECT * FROM (SELECT 1) t"


def test_wrap_helper_replaces_all_occurrences():
    out = wrap("X", "{{SUBQUERY}} UNION ALL {{SUBQUERY}}")
    assert out == "X UNION ALL X"


def test_default_placeholder_value():
    assert DEFAULT_WRAPPER_PLACEHOLDER == "{{SUBQUERY}}"


def _payload(**over):
    base = {
        "sql": "SELECT a, dt FROM sales WHERE dt IN ('1','2','3','4')",
        "partition_column": "dt",
        "target_table": "public.mirror",
        "parallelism": 2,
    }
    base.update(over)
    return base


def test_api_wraps_each_subquery(client):
    payload = _payload(
        wrapper_query="SELECT cnt FROM (SELECT count(*) cnt FROM ({{SUBQUERY}}) s) w"
    )
    job_id = client.post("/jobs", json=payload).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["total"] == 2

    for t in body["tasks"]:
        detail = client.get(f"/jobs/{job_id}/tasks/{t['task_id']}").json()
        sub = detail["sub_query"]
        # 감싸는 쿼리 + 그 안에 분할된 쿼리가 들어가 있어야 한다
        assert sub.startswith("SELECT cnt FROM (SELECT count(*) cnt FROM (")
        assert "dt IN (" in sub
        assert "{{SUBQUERY}}" not in sub  # placeholder는 모두 치환됨


def test_api_custom_placeholder(client):
    payload = _payload(
        wrapper_query="INSERT INTO t SELECT * FROM (@@Q@@) x",
        wrapper_placeholder="@@Q@@",
    )
    job_id = client.post("/jobs", json=payload).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    detail = client.get(f"/jobs/{job_id}/tasks/{body['tasks'][0]['task_id']}").json()
    assert detail["sub_query"].startswith("INSERT INTO t SELECT * FROM (")
    assert "@@Q@@" not in detail["sub_query"]


def test_api_missing_placeholder_rejected(client):
    payload = _payload(wrapper_query="SELECT * FROM (no placeholder here) t")
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "WRAPPER_PLACEHOLDER_MISSING"


def test_api_no_wrapper_keeps_plain_subquery(client):
    job_id = client.post("/jobs", json=_payload()).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    detail = client.get(f"/jobs/{job_id}/tasks/{body['tasks'][0]['task_id']}").json()
    # wrapper 없으면 분할된 쿼리 그대로
    assert detail["sub_query"].startswith("SELECT a, dt FROM sales")