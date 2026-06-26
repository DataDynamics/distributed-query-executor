"""검증된 쿼리를 파티션 IN 목록 기준으로 N개의 sub-query로 분할한다.

원문 SQL 포맷(들여쓰기·대소문자·주석 등)을 최대한 보존하기 위해, AST를 재직렬화하지 않고
**파티션 IN 절의 값 목록 부분만 문자열로 치환**한다. (값 목록을 찾지 못하면 AST 재생성으로
폴백한다.)
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .parser import ParsedQuery, find_partition_in


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


def _value_span(sql: str, partition_column: str) -> tuple[int, int] | None:
    """원문 SQL에서 파티션 컬럼 IN 절의 *값 목록*(괄호 안) 문자 구간을 찾는다.

    반환: (start, end) — 여는 괄호 바로 다음 ~ 닫는 괄호 직전. 못 찾으면 None.
    테이블 한정자 유무·대소문자는 무시한다.
    """
    bare = re.escape(partition_column.split(".")[-1])
    # (선택적 한정자)<컬럼> IN ( ...   ※ 식별자 중간 매칭 방지 lookbehind
    pat = re.compile(
        r"(?<![\w.])(?:[A-Za-z_]\w*\.)?" + bare + r"\s+IN\s*\(",
        re.IGNORECASE,
    )
    m = pat.search(sql)
    if not m:
        return None

    start = m.end()  # 여는 괄호 다음 위치
    depth = 1
    for i in range(start, len(sql)):
        c = sql[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return (start, i)
    return None


def split(
    parsed: ParsedQuery, parallelism: int, strategy: str = "contiguous"
) -> list[SubQuery]:
    """각각 값 부분집합을 스캔하는 완전한 sub-query SQL 문자열 N개를 생성한다.

    ``parallelism`` 은 IN 값 개수로 클램핑된다(빈 sub-query 생성 안 함).
    파티션 IN 절은 트리 어디에 있든(중첩 서브쿼리 포함) 정확히 찾아 대체한다.
    원문 포맷을 보존하기 위해 값 목록만 문자열로 치환한다.
    """
    src_in = find_partition_in(parsed.expression, parsed.partition_column)
    value_exprs = list(src_in.expressions)
    buckets = [b for b in _chunk(value_exprs, parallelism, strategy) if b]

    span = _value_span(parsed.sql, parsed.partition_column)

    sub_queries: list[SubQuery] = []
    for bucket in buckets:
        rendered = [b.sql(dialect=parsed.dialect) for b in bucket]
        if span is not None:
            # 원문 보존: 값 목록 구간만 치환
            s, e = span
            sub_sql = parsed.sql[:s] + ", ".join(rendered) + parsed.sql[e:]
        else:
            # 폴백: AST 재생성
            cloned = parsed.expression.copy()
            target_in = find_partition_in(cloned, parsed.partition_column)
            target_in.set("expressions", [b.copy() for b in bucket])
            sub_sql = cloned.sql(dialect=parsed.dialect)
        sub_queries.append(SubQuery(sql=sub_sql, partition_values=rendered))

    return sub_queries
