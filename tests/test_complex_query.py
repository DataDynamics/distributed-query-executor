"""복합 쿼리(중첩 서브쿼리/JOIN/GROUP BY) 의 lenient 분할 테스트.

파티션 IN(REGION_NO)이 중첩 서브쿼리의 WHERE 안에 있고, IN 절이 여러 개인 경우를 다룬다.
(테스트용 가상 스키마/테이블/컬럼 이름 사용 — 실제 도메인과 무관)
"""

from __future__ import annotations

import pytest

from coordinator.parser import QueryValidationError, validate_and_parse
from coordinator.splitter import split

# REGION_NO IN 이 중첩 서브쿼리 안에 있고, 그 앞에 STORE_ID IN 이 먼저 등장한다.
COMPLEX_SQL = """
SELECT T.REGION_ID, T.ZONE, T.STORE_ID
FROM (
    SELECT A.REGION_NO AS REGION_ID, A.ZONE, A.STORE_ID
    FROM SALES.ORDER_FACT A
    LEFT OUTER JOIN (
        SELECT REGION, ZONE, STORE_ID FROM REF.STORE_DIM
        GROUP BY REGION, ZONE, STORE_ID
    ) B ON A.REGION_NO = B.REGION AND A.ZONE = B.ZONE AND A.STORE_ID = B.STORE_ID
    WHERE 1 = 1
      AND A.STORE_ID IN ('S001', 'S002', 'S003')
      AND A.REGION_NO IN ('R1', 'R2', 'R3')
      AND A.ORDER_TIME BETWEEN date_trunc('day', CURRENT_DATE + interval '-7 DAY')
                           AND date_trunc('day', CURRENT_DATE + interval '1 DAY')
) T
"""


def test_strict_mode_rejects_complex_query():
    # 최상위 WHERE가 없으므로(=서브쿼리 안에 있음) strict 모드는 거부
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse(COMPLEX_SQL, "A.REGION_NO", dialect="postgres", strict=True)
    assert exc.value.code == "NO_PARTITION_IN_CLAUSE"


def test_lenient_mode_finds_nested_partition_column():
    parsed = validate_and_parse(
        COMPLEX_SQL, "A.REGION_NO", dialect="postgres", strict=False
    )
    # STORE_ID IN 이 먼저 나오지만 REGION_NO 값을 정확히 잡아야 한다
    assert parsed.partition_values == ["'R1'", "'R2'", "'R3'"]


def test_lenient_split_targets_partition_column_only():
    parsed = validate_and_parse(
        COMPLEX_SQL, "A.REGION_NO", dialect="postgres", strict=False
    )
    subs = split(parsed, parallelism=3, strategy="contiguous")
    assert len(subs) == 3
    assert [s.partition_values for s in subs] == [["'R1'"], ["'R2'"], ["'R3'"]]
    for s in subs:
        # REGION_NO 만 분할되고 다른 IN 절은 그대로 보존되어야 한다
        assert "A.STORE_ID IN ('S001', 'S002', 'S003')" in s.sql
        # 다시 lenient 로 파싱해도 동일 값
        re = validate_and_parse(s.sql, "A.REGION_NO", dialect="postgres", strict=False)
        assert re.partition_values == s.partition_values


def test_lenient_split_into_pairs():
    # 2분할 → ['R1','R2'] / ['R3'] (여러 '쌍'으로 나누기)
    parsed = validate_and_parse(
        COMPLEX_SQL, "A.REGION_NO", dialect="postgres", strict=False
    )
    subs = split(parsed, parallelism=2, strategy="contiguous")
    assert [s.partition_values for s in subs] == [["'R1'", "'R2'"], ["'R3'"]]


def test_api_create_job_with_complex_query(client):
    # POST /jobs 에서 sql_dialect + strict_validation=false 로 복합 쿼리 분할
    payload = {
        "sql": COMPLEX_SQL,
        "partition_column": "A.REGION_NO",
        "target_table": "public.orders_mirror",
        "parallelism": 3,
        "sql_dialect": "postgres",
        "strict_validation": False,
    }
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    body = client.get(f"/jobs/{job_id}").json()
    assert body["total"] == 3

    # 각 task 의 sub_query 가 서로 다른 REGION_NO 를 담는지
    region_filters = []
    for t in body["tasks"]:
        detail = client.get(f"/jobs/{job_id}/tasks/{t['task_id']}").json()
        assert "A.STORE_ID IN ('S001', 'S002', 'S003')" in detail["sub_query"]
        region_filters.append(detail["partition_values"])
    assert sorted(region_filters) == [["'R1'"], ["'R2'"], ["'R3'"]]


def test_api_complex_query_rejected_in_strict_mode(client):
    payload = {
        "sql": COMPLEX_SQL,
        "partition_column": "A.REGION_NO",
        "target_table": "public.orders_mirror",
        "parallelism": 3,
        "sql_dialect": "postgres",
        # strict_validation 기본 True → 거부
    }
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "NO_PARTITION_IN_CLAUSE"
