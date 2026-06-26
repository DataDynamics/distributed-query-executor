"""Split a validated query into N sub-queries by chunking the partition IN-list."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from .parser import DIALECT, ParsedQuery, find_partition_in


@dataclass
class SubQuery:
    sql: str
    partition_values: list[str]


def _chunk(values: list, n: int, strategy: str) -> list[list]:
    """Split ``values`` into at most ``n`` non-empty buckets."""
    n = max(1, min(n, len(values)))
    if strategy == "round_robin":
        buckets: list[list] = [[] for _ in range(n)]
        for i, v in enumerate(values):
            buckets[i % n].append(v)
        return buckets

    # contiguous (default): distribute the remainder across the first buckets
    size, rem = divmod(len(values), n)
    buckets = []
    start = 0
    for i in range(n):
        end = start + size + (1 if i < rem else 0)
        buckets.append(values[start:end])
        start = end
    return buckets


def split(
    parsed: ParsedQuery, parallelism: int, strategy: str = "contiguous"
) -> list[SubQuery]:
    """Produce N complete sub-query SQL strings, each scanning a value subset.

    ``parallelism`` is clamped to the number of IN values (no empty sub-query).
    """
    value_exprs = list(parsed.expression.find(exp.In).expressions)
    buckets = _chunk(value_exprs, parallelism, strategy)

    sub_queries: list[SubQuery] = []
    for bucket in buckets:
        if not bucket:
            continue
        cloned = parsed.expression.copy()
        target_in = find_partition_in(cloned, parsed.partition_column)
        target_in.set("expressions", [b.copy() for b in bucket])
        sub_queries.append(
            SubQuery(
                sql=cloned.sql(dialect=DIALECT),
                partition_values=[b.sql(dialect=DIALECT) for b in bucket],
            )
        )
    return sub_queries
