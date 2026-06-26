"""Validation test cases for the query parser (the heart of stage-1 safety)."""

from __future__ import annotations

import pytest

from coordinator.parser import QueryValidationError, validate_and_parse

PCOL = "dt"


# ----------------------------- valid queries -----------------------------


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
    # partition_column given as 't.dt', IN uses the same qualified column
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


# --------------------------- rejected queries ---------------------------


@pytest.mark.parametrize(
    "sql, code",
    [
        # not parseable
        ("SELECT FROM WHERE ((", "PARSE_ERROR"),
        ("", "PARSE_ERROR"),
        # not a SELECT
        ("INSERT INTO t VALUES (1)", "NOT_A_SELECT"),
        ("UPDATE t SET a=1 WHERE dt IN ('1')", "NOT_A_SELECT"),
        ("DELETE FROM t WHERE dt IN ('1')", "NOT_A_SELECT"),
        ("CREATE TABLE t (a INT)", "NOT_A_SELECT"),
        (
            "SELECT a FROM t WHERE dt IN ('1') "
            "UNION ALL SELECT a FROM t WHERE dt IN ('2')",
            "NOT_A_SELECT",
        ),
        # multiple statements
        ("SELECT a FROM t WHERE dt IN ('1'); SELECT 1", "MULTIPLE_STATEMENTS"),
        # missing partition IN
        ("SELECT a FROM t", "NO_PARTITION_IN_CLAUSE"),
        ("SELECT a FROM t WHERE region='KR'", "NO_PARTITION_IN_CLAUSE"),
        ("SELECT a FROM t WHERE dt = '1'", "NO_PARTITION_IN_CLAUSE"),
        ("SELECT a FROM t WHERE other IN ('1','2')", "NO_PARTITION_IN_CLAUSE"),
        # negated / subquery IN
        ("SELECT a FROM t WHERE dt NOT IN ('1','2')", "NEGATED_IN"),
        ("SELECT a FROM t WHERE dt IN (SELECT dt FROM cal)", "SUBQUERY_IN_CLAUSE"),
        # unsupported stage-1 constructs
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
