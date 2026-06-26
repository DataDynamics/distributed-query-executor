"""Coordinator HTTP API 엔드투엔드 테스트(검증 + Job 라이프사이클)."""

from __future__ import annotations

import pytest


def test_create_job_returns_202_and_dispatches(client, runner, valid_payload):
    resp = client.post("/jobs", json=valid_payload)
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]
    assert job_id

    # BackgroundTasks를 통해 FakeRunner가 정확히 한 번 호출됨
    assert len(runner.runs) == 1
    assert runner.runs[0].job_id == job_id


def test_job_split_into_parallelism_tasks(client, valid_payload):
    valid_payload["parallelism"] = 2
    job_id = client.post("/jobs", json=valid_payload).json()["job_id"]

    body = client.get(f"/jobs/{job_id}").json()
    assert body["total"] == 2
    assert body["status"] == "DONE"  # FakeRunner가 완료 처리
    assert body["total_rows_written"] == 20  # task당 10행 * 2 task


def test_task_detail_retains_full_sub_query(client, valid_payload):
    job_id = client.post("/jobs", json=valid_payload).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    task_id = body["tasks"][0]["task_id"]

    detail = client.get(f"/jobs/{job_id}/tasks/{task_id}").json()
    # 요구사항: coordinator는 보낸 sub-query 전문을 그대로 기억한다
    assert "sub_query" in detail
    assert "dt IN" in detail["sub_query"].replace("`", "")


def test_result_endpoint(client, valid_payload):
    job_id = client.post("/jobs", json=valid_payload).json()["job_id"]
    result = client.get(f"/jobs/{job_id}/result").json()
    assert result["total_rows_written"] == 20
    assert len(result["per_task"]) == 2


def test_unknown_job_returns_404(client):
    assert client.get("/jobs/nope").status_code == 404
    assert client.get("/jobs/nope/result").status_code == 404


# --------------------- HTTP로 노출되는 검증 결과 ---------------------


@pytest.mark.parametrize(
    "sql, code",
    [
        ("SELECT FROM WHERE ((", "PARSE_ERROR"),
        ("INSERT INTO t VALUES (1)", "NOT_A_SELECT"),
        ("SELECT a FROM t WHERE region='KR'", "NO_PARTITION_IN_CLAUSE"),
        ("SELECT a FROM t WHERE dt NOT IN ('1')", "NEGATED_IN"),
        ("SELECT count(*) FROM t WHERE dt IN ('1')", "UNSUPPORTED_AGGREGATE"),
        ("SELECT a FROM t WHERE dt IN ('1') GROUP BY a", "UNSUPPORTED_GROUP_BY"),
        ("SELECT DISTINCT a FROM t WHERE dt IN ('1')", "UNSUPPORTED_DISTINCT"),
    ],
)
def test_invalid_query_returns_422_with_error_code(client, valid_payload, sql, code):
    payload = {**valid_payload, "sql": sql}
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == code


def test_parallelism_zero_rejected_by_schema(client, valid_payload):
    payload = {**valid_payload, "parallelism": 0}
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422  # pydantic ge=1 제약


def test_missing_required_fields_rejected(client):
    resp = client.post("/jobs", json={"sql": "SELECT 1"})
    assert resp.status_code == 422
