"""쿼리 파서 검증 테스트 케이스(1단계 안정성의 핵심)."""

from __future__ import annotations

import pytest

from coordinator.parser import QueryValidationError, validate_and_parse

PCOL = "dt"


# ----------------------------- 유효한 쿼리 -----------------------------


def test_simple_select_ok():
    parsed = validate_and_parse(
        "SELECT a, b FROM t WHERE dt IN ('1','2','3')", PCOL
    )
    assert parsed.partition_values == ["'1'", "'2'", "'3'"]


def test_select_with_order_by_and_limit_ok():
    parsed = validate_and_parse(
        "SELECT a FROM t WHERE dt IN ('1','2') ORDER BY a DESC LIMIT 100", PCOL
    )
    assert len(parsed.partition_values) == 2


def test_select_with_extra_predicates_ok():
    parsed = validate_and_parse(
        "SELECT a FROM t WHERE region='KR' AND dt IN ('1') AND amount > 0", PCOL
    )
    assert parsed.partition_values == ["'1'"]


def test_qualified_partition_column_ok():
    # partition_column이 't.dt'로 주어지고 IN도 동일한 한정 컬럼을 사용
    parsed = validate_and_parse(
        "SELECT a FROM t WHERE t.dt IN ('1','2')", "t.dt"
    )
    assert len(parsed.partition_values) == 2


def test_integer_partition_values_ok():
    parsed = validate_and_parse(
        "SELECT a FROM t WHERE dt IN (20260101, 20260102)", PCOL
    )
    assert parsed.partition_values == ["20260101", "20260102"]


def test_case_insensitive_keywords_ok():
    parsed = validate_and_parse(
        "select a from t where dt in ('1','2')", PCOL
    )
    assert len(parsed.partition_values) == 2


# --------------------------- 거부되는 쿼리 ---------------------------


@pytest.mark.parametrize(
    "sql, code",
    [
        # 파싱 불가
        ("SELECT FROM WHERE ((", "PARSE_ERROR"),
        ("", "PARSE_ERROR"),
        # SELECT가 아님
        ("INSERT INTO t VALUES (1)", "NOT_A_SELECT"),
        ("UPDATE t SET a=1 WHERE dt IN ('1')", "NOT_A_SELECT"),
        ("DELETE FROM t WHERE dt IN ('1')", "NOT_A_SELECT"),
        ("CREATE TABLE t (a INT)", "NOT_A_SELECT"),
        (
            "SELECT a FROM t WHERE dt IN ('1') "
            "UNION ALL SELECT a FROM t WHERE dt IN ('2')",
            "NOT_A_SELECT",
        ),
        # 다중 문
        ("SELECT a FROM t WHERE dt IN ('1'); SELECT 1", "MULTIPLE_STATEMENTS"),
        # 파티션 IN 없음
        ("SELECT a FROM t", "NO_PARTITION_IN_CLAUSE"),
        ("SELECT a FROM t WHERE region='KR'", "NO_PARTITION_IN_CLAUSE"),
        ("SELECT a FROM t WHERE dt = '1'", "NO_PARTITION_IN_CLAUSE"),
        ("SELECT a FROM t WHERE other IN ('1','2')", "NO_PARTITION_IN_CLAUSE"),
        # 부정(NOT IN) / 서브쿼리 IN
        ("SELECT a FROM t WHERE dt NOT IN ('1','2')", "NEGATED_IN"),
        ("SELECT a FROM t WHERE dt IN (SELECT dt FROM cal)", "SUBQUERY_IN_CLAUSE"),
        # 1단계 미지원 구문
        ("SELECT a FROM t WHERE dt IN ('1') GROUP BY a", "UNSUPPORTED_GROUP_BY"),
        ("SELECT count(*) FROM t WHERE dt IN ('1')", "UNSUPPORTED_AGGREGATE"),
        ("SELECT sum(amount) FROM t WHERE dt IN ('1')", "UNSUPPORTED_AGGREGATE"),
        ("SELECT DISTINCT a FROM t WHERE dt IN ('1')", "UNSUPPORTED_DISTINCT"),
        (
            "SELECT a FROM t JOIN u ON t.id=u.id WHERE dt IN ('1')",
            "UNSUPPORTED_JOIN",
        ),
    ],
)
def test_rejected(sql, code):
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse(sql, PCOL)
    assert exc.value.code == code


def test_missing_partition_column_arg():
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse("SELECT a FROM t WHERE dt IN ('1')", "")
    assert exc.value.code == "MISSING_PARTITION_COLUMN"
