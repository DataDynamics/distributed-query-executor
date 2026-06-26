"""Tests for splitting the IN-list into N sub-queries."""

from __future__ import annotations

from coordinator.parser import validate_and_parse
from coordinator.splitter import split


def _parse(values_sql: str):
    return validate_and_parse(
        f"SELECT a, b FROM t WHERE region='KR' AND dt IN ({values_sql})", "dt"
    )


def test_contiguous_split_sizes_and_coverage():
    parsed = _parse("'1','2','3','4','5','6','7','8','9','10'")
    subs = split(parsed, parallelism=4, strategy="contiguous")
    assert [len(s.partition_values) for s in subs] == [3, 3, 2, 2]
    all_values = [v for s in subs for v in s.partition_values]
    assert sorted(all_values) == sorted([f"'{i}'" for i in range(1, 11)])


def test_round_robin_distribution():
    parsed = _parse("'1','2','3','4','5'")
    subs = split(parsed, parallelism=2, strategy="round_robin")
    assert subs[0].partition_values == ["'1'", "'3'", "'5'"]
    assert subs[1].partition_values == ["'2'", "'4'"]


def test_parallelism_greater_than_values_is_clamped():
    parsed = _parse("'1','2','3'")
    subs = split(parsed, parallelism=10, strategy="contiguous")
    assert len(subs) == 3  # one value per sub-query, no empty sub-queries


def test_parallelism_one_keeps_all_values():
    parsed = _parse("'1','2','3'")
    subs = split(parsed, parallelism=1)
    assert len(subs) == 1
    assert len(subs[0].partition_values) == 3


def test_each_subquery_is_valid_and_preserves_other_predicates():
    parsed = _parse("'1','2','3','4'")
    subs = split(parsed, parallelism=2)
    for s in subs:
        # every sub-query must re-validate cleanly
        reparsed = validate_and_parse(s.sql, "dt")
        assert reparsed.partition_values == s.partition_values
        # the unrelated predicate must survive the rewrite
        assert "region" in s.sql.lower()
