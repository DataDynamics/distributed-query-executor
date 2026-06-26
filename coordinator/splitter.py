"""검증된 쿼리를 파티션 IN 목록 기준으로 N개의 sub-query로 분할한다."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from .parser import DIALECT, ParsedQuery, find_partition_in


@dataclass
class SubQuery:
    sql: str
    partition_values: list[str]


def _chunk(values: list, n: int, strategy: str) -> list[list]:
    """``values`` 를 최대 ``n`` 개의 비어 있지 않은 버킷으로 분할."""
    n = max(1, min(n, len(values)))
    if strategy == "round_robin":
        buckets: list[list] = [[] for _ in range(n)]
        for i, v in enumerate(values):
            buckets[i % n].append(v)
        return buckets

    # contiguous(기본): 나머지를 앞쪽 버킷부터 하나씩 분배
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
    """각각 값 부분집합을 스캔하는 완전한 sub-query SQL 문자열 N개를 생성한다.

    ``parallelism`` 은 IN 값 개수로 클램핑된다(빈 sub-query 생성 안 함).
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
