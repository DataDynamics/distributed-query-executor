"""제공된 실제 SELECT 의 *구조*를 모사한 다양한 분할 시나리오 테스트.

특징(가상 이름으로 모사):
  - 파티션 IN(REGION_NO)이 중첩 서브쿼리의 WHERE 깊은 곳에 있음
  - 같은 서브쿼리에 다른 IN 절이 여러 개(STORE_ID, CHANNEL)
  - 배열 컬럼 unnest(...), 그룹핑된 디멘전 테이블과의 LEFT OUTER JOIN
  - EVENT_TIME BETWEEN date_trunc(...) AND date_trunc(...)
실제 도메인(스키마/테이블/컬럼)과 무관한 가상 이름만 사용한다.
"""

from __future__ import annotations

import pytest

from coordinator.parser import QueryValidationError, validate_and_parse
from coordinator.splitter import split

DIALECT = "postgres"  # unnest / date_trunc / interval 사용

# REGION_NO 값 4개. STORE_ID/CHANNEL IN 은 분할되면 안 되고 그대로 보존되어야 한다.
BASE_SQL = """
SELECT
    99999999,
    'svcuser' AS USER_KEY,
    T.REGION_ID, T.ZONE, T.STORE_ID, T.STORE_NAME, T.CATEGORY,
    T.ITEM_NAME, T.METRIC_IDX, T.EVENT_TIME, T.SEQ_NO,
    T.ORDER_ID, T.ROOT_ORDER_ID, T.NODE_ID,
    T.UPPER_LIMIT, T.LOWER_LIMIT, T.METRIC_VALUE, T.FLOW_ID, T.PLAN_ID, T.RANK_NO
FROM (
    SELECT
        A.REGION_NO AS REGION_ID, A.ZONE, A.STORE_ID, B.STORE_NAME, B.CATEGORY,
        unnest(A.ITEM_NAME) AS ITEM_NAME,
        unnest(A.RANK_NO) AS RANK_NO,
        unnest(A.METRIC_IDX) AS METRIC_IDX,
        unnest(A.EVENT_TIME) AS EVENT_TIME,
        unnest(A.METRIC_VALUE) AS METRIC_VALUE,
        A.SEQ_NO, A.ORDER_ID, A.ROOT_ORDER_ID, A.NODE_ID,
        A.FLOW_ID, A.PLAN_ID, A.CHANNEL, A.NODE_IDX
    FROM SALES.ORDER_FACT A
    LEFT OUTER JOIN (
        SELECT REGION, ZONE, STORE_ID, STORE_NAME, CATEGORY
        FROM REF.STORE_DIM
        GROUP BY REGION, ZONE, STORE_ID, STORE_NAME, CATEGORY
    ) B ON A.REGION_NO = B.REGION AND A.ZONE = B.ZONE AND A.STORE_ID = B.STORE_ID
    WHERE 1 = 1
      AND A.REGION_NO IN ('R1', 'R2', 'R3', 'R4')
      AND A.STORE_ID IN ('S001', 'S002', 'S003')
      AND A.CHANNEL IN ('ONLINE', 'OFFLINE')
      AND A.EVENT_TIME BETWEEN date_trunc('day', CURRENT_DATE + interval '-7 DAY')
                           AND date_trunc('day', CURRENT_DATE + interval '1 DAY')
) T
"""

ALL_REGIONS = ["'R1'", "'R2'", "'R3'", "'R4'"]


def _parse(sql: str = BASE_SQL, pcol: str = "REGION_NO"):
    return validate_and_parse(sql, pcol, dialect=DIALECT, strict=False)


def _with_regions(values_sql: str) -> str:
    """REGION_NO IN 목록을 교체한 변형 SQL 생성."""
    return BASE_SQL.replace(
        "A.REGION_NO IN ('R1', 'R2', 'R3', 'R4')", f"A.REGION_NO IN ({values_sql})"
    )


# ───────────────────────── 분할 수/분배 ─────────────────────────


@pytest.mark.parametrize(
    "parallelism, expected",
    [
        (1, [["'R1'", "'R2'", "'R3'", "'R4'"]]),
        (2, [["'R1'", "'R2'"], ["'R3'", "'R4'"]]),
        (3, [["'R1'", "'R2'"], ["'R3'"], ["'R4'"]]),
        (4, [["'R1'"], ["'R2'"], ["'R3'"], ["'R4'"]]),
        (8, [["'R1'"], ["'R2'"], ["'R3'"], ["'R4'"]]),  # 값보다 크면 클램프
    ],
)
def test_contiguous_split_counts(parallelism, expected):
    subs = split(_parse(), parallelism=parallelism, strategy="contiguous")
    assert [s.partition_values for s in subs] == expected


def test_round_robin_distribution():
    subs = split(_parse(), parallelism=2, strategy="round_robin")
    assert [s.partition_values for s in subs] == [["'R1'", "'R3'"], ["'R2'", "'R4'"]]


@pytest.mark.parametrize("parallelism", [1, 2, 3, 4, 5, 6])
@pytest.mark.parametrize("strategy", ["contiguous", "round_robin"])
def test_full_coverage_no_loss_or_dup(parallelism, strategy):
    # 어떤 분할이든 전체 파티션 값을 정확히 한 번씩 커버해야 한다
    subs = split(_parse(), parallelism=parallelism, strategy=strategy)
    flat = [v for s in subs for v in s.partition_values]
    assert sorted(flat) == sorted(ALL_REGIONS)
    assert len(flat) == len(ALL_REGIONS)  # 중복/누락 없음
    assert len(subs) == min(parallelism, len(ALL_REGIONS))


# ───────────────────── 다른 절 보존 / 라운드트립 ─────────────────────


@pytest.mark.parametrize("strategy", ["contiguous", "round_robin"])
def test_other_clauses_preserved_in_every_subquery(strategy):
    subs = split(_parse(), parallelism=3, strategy=strategy)
    for s in subs:
        up = s.sql.upper()
        # 분할되지 않은 다른 IN 절은 값이 모두 보존
        for tok in ("'S001'", "'S002'", "'S003'", "'ONLINE'", "'OFFLINE'"):
            assert tok in s.sql
        # unnest / join / between / date_trunc 구조 보존
        assert "UNNEST" in up
        assert "LEFT OUTER JOIN" in up
        assert "BETWEEN" in up
        assert "DATE_TRUNC" in up


def test_subquery_roundtrip_reparses_to_same_values():
    subs = split(_parse(), parallelism=4)
    for s in subs:
        re = validate_and_parse(s.sql, "REGION_NO", dialect=DIALECT, strict=False)
        assert re.partition_values == s.partition_values


def test_only_in_clause_changes_rest_byte_identical():
    # 원문 보존: IN 목록만 바뀌고 나머지는 원문과 바이트 단위로 동일해야 한다
    subs = split(_parse(), parallelism=2, strategy="contiguous")
    expected = [["'R1'", "'R2'"], ["'R3'", "'R4'"]]
    for s, vals in zip(subs, expected):
        restored = s.sql.replace(
            f"A.REGION_NO IN ({', '.join(vals)})",
            "A.REGION_NO IN ('R1', 'R2', 'R3', 'R4')",
        )
        assert restored == BASE_SQL  # IN 절 외 전부 원형 보존


def test_original_formatting_preserved():
    # AST 재직렬화 시 사라지던 원문 포맷(소문자 함수/들여쓰기)이 유지되어야 한다
    s = split(_parse(), parallelism=2)[0].sql
    assert "unnest(A.ITEM_NAME)" in s          # 소문자 unnest 보존
    assert "date_trunc('day'," in s            # 소문자 date_trunc 보존
    assert "LEFT OUTER JOIN" in s
    assert "\n" in s                            # 줄바꿈/들여쓰기 보존


# ───────────────────── 한정자/대소문자 ─────────────────────


@pytest.mark.parametrize("pcol", ["A.REGION_NO", "REGION_NO", "region_no", "T.REGION_NO"])
def test_partition_column_qualifier_and_case_insensitive(pcol):
    parsed = _parse(pcol=pcol)
    assert parsed.partition_values == ALL_REGIONS


# ───────────────────── 값 개수 변형 ─────────────────────


def test_single_value_in_yields_one_subquery():
    parsed = _parse(_with_regions("'R1'"))
    subs = split(parsed, parallelism=4)
    assert len(subs) == 1
    assert subs[0].partition_values == ["'R1'"]


def test_many_values_contiguous_remainder_distribution():
    vals = ", ".join(f"'R{i}'" for i in range(1, 11))  # 10개
    parsed = _parse(_with_regions(vals))
    subs = split(parsed, parallelism=4, strategy="contiguous")
    # 10 = 3+3+2+2
    assert [len(s.partition_values) for s in subs] == [3, 3, 2, 2]
    flat = [v for s in subs for v in s.partition_values]
    assert len(flat) == 10 and len(set(flat)) == 10


# ───────────────────── 검증/에러 케이스 ─────────────────────


def test_strict_mode_rejects_nested_complex_query():
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse(BASE_SQL, "REGION_NO", dialect=DIALECT, strict=True)
    assert exc.value.code == "NO_PARTITION_IN_CLAUSE"


def test_unknown_partition_column_rejected():
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse(BASE_SQL, "NOPE_COL", dialect=DIALECT, strict=False)
    assert exc.value.code == "NO_PARTITION_IN_CLAUSE"


def test_not_in_on_partition_column_rejected():
    sql = BASE_SQL.replace(
        "A.REGION_NO IN ('R1', 'R2', 'R3', 'R4')",
        "A.REGION_NO NOT IN ('R1', 'R2', 'R3', 'R4')",
    )
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse(sql, "REGION_NO", dialect=DIALECT, strict=False)
    assert exc.value.code == "NEGATED_IN"


def test_subquery_in_on_partition_column_rejected():
    sql = BASE_SQL.replace(
        "A.REGION_NO IN ('R1', 'R2', 'R3', 'R4')",
        "A.REGION_NO IN (SELECT REGION FROM REF.REGION_DIM)",
    )
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse(sql, "REGION_NO", dialect=DIALECT, strict=False)
    assert exc.value.code == "SUBQUERY_IN_CLAUSE"


# ───────────────────── 방언 ─────────────────────


def test_hive_dialect_also_parses_and_splits():
    # 방언을 hive 로 줘도 구조 파싱·분할은 가능(값 동일)
    parsed = validate_and_parse(BASE_SQL, "REGION_NO", dialect="hive", strict=False)
    subs = split(parsed, parallelism=2)
    assert [s.partition_values for s in subs] == [["'R1'", "'R2'"], ["'R3'", "'R4'"]]


# ───────────────────── API 레벨 ─────────────────────


def _payload(**over):
    base = {
        "sql": BASE_SQL,
        "partition_column": "REGION_NO",
        "target_table": "public.orders_mirror",
        "parallelism": 4,
        "sql_dialect": DIALECT,
        "strict_validation": False,
    }
    base.update(over)
    return base


def test_api_complex_job_splits_into_tasks(client):
    resp = client.post("/jobs", json=_payload(parallelism=4))
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["total"] == 4

    seen = []
    for t in body["tasks"]:
        detail = client.get(f"/jobs/{job_id}/tasks/{t['task_id']}").json()
        # 저장된 sub_query 전문에 다른 IN 절이 보존되어 있어야 함
        assert "'S001'" in detail["sub_query"] and "'ONLINE'" in detail["sub_query"]
        seen.append(detail["partition_values"])
    assert sorted(seen) == [["'R1'"], ["'R2'"], ["'R3'"], ["'R4'"]]


def test_api_complex_job_round_robin(client):
    job_id = client.post("/jobs", json=_payload(parallelism=2, split_strategy="round_robin")).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["total"] == 2


def test_api_complex_job_strict_default_rejected(client):
    payload = _payload()
    payload.pop("strict_validation")  # 기본 True
    resp = client.post("/jobs", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "NO_PARTITION_IN_CLAUSE"
