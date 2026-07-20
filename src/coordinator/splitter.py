"""검증된 쿼리를 파티션 IN 목록 기준으로 N개의 sub-query로 분할하는 모듈.

:mod:`parser` 가 만든 :class:`~coordinator.parser.ParsedQuery` 를 입력받아, IN 목록의
값들을 ``parallelism`` 개의 버킷으로 나눈 뒤 각 버킷만 스캔하는 완전한 sub-query SQL을
생성한다. 이렇게 만든 sub-query들은 서로 다른 executor에 분산 실행된다.

핵심 설계: 원문 SQL 포맷(들여쓰기·대소문자·주석 등)을 최대한 보존하기 위해 AST를 통째로
재직렬화하지 않고 **파티션 IN 절의 값 목록(괄호 안) 부분만 문자열로 치환**한다. 정규식으로
값 목록의 문자 구간을 찾고 그 구간만 갈아끼우므로 나머지 SQL은 원문 그대로 남는다. 단,
정규식으로 구간을 찾지 못하면 AST를 복제·수정 후 재직렬화하는 방식으로 폴백한다.

분배 전략(strategy):
  - contiguous(기본) : 값을 연속된 덩어리로 나눈다(예: [a,b,c,d] → [a,b],[c,d]).
                       정렬된 파티션에서 범위 지역성이 좋다.
  - round_robin      : 값을 버킷에 번갈아 배분한다(예: [a,b,c,d] → [a,c],[b,d]).
                       크기가 들쭉날쭉한 파티션의 부하를 고르게 섞을 때 유리하다.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from .parser import ParsedQuery, find_partition_in


logger = logging.getLogger(__name__)


# wrapper_query 안에서 분할된 sub-query가 치환될 기본 자리표시자.
DEFAULT_WRAPPER_PLACEHOLDER = "{{SUBQUERY}}"


@dataclass
class SubQuery:
    """분할로 생성된 sub-query 한 건.

    필드:
        sql              : 해당 버킷의 값만 스캔하는 완전한 SQL 문자열.
        partition_values : 이 sub-query가 담당하는 파티션 값들(버킷 내용).
    """

    sql: str
    partition_values: list[str]


def wrap(sub_sql: str, wrapper: str, placeholder: str = DEFAULT_WRAPPER_PLACEHOLDER) -> str:
    """감싸는 쿼리(wrapper)의 placeholder 자리에 분할된 sub-query를 끼워 넣는다.

    placeholder가 여러 번 나오면 모두 치환한다. (괄호 등은 wrapper 작성자가 직접 둔다.)
    """
    return wrapper.replace(placeholder, sub_sql)


def _chunk(values: list, n: int, strategy: str) -> list[list]:
    """``values`` 를 최대 ``n`` 개의 (가능한 한 균등한) 버킷으로 분할한다.

    인자:
        values   : 분배할 값 목록(파티션 IN 값들).
        n        : 원하는 버킷(병렬도) 개수.
        strategy : "round_robin" 또는 그 외(=contiguous).

    반환:
        값 리스트들의 리스트. 길이는 ``min(n, len(values))`` 이며 빈 버킷은 없다.

    ``n`` 을 ``[1, len(values)]`` 로 클램핑하는 이유: 값보다 버킷이 많으면 빈 sub-query가
    생기므로 값 개수를 상한으로 둔다. n이 0 이하로 들어와도 최소 1을 보장한다.
    """
    n = max(1, min(n, len(values)))
    if strategy == "round_robin":
        # i번째 값을 (i % n)번 버킷에 넣어 번갈아 분배한다.
        buckets: list[list] = [[] for _ in range(n)]
        for i, v in enumerate(values):
            buckets[i % n].append(v)
        return buckets

    # contiguous(기본): 연속 구간으로 자른다. 균등 분할 후 남는 나머지(rem)는
    # 앞쪽 버킷부터 1개씩 더 배분해(i < rem) 버킷 간 크기 차이를 최대 1로 유지한다.
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

    포맷 보존 치환을 위해, AST가 아니라 원문 문자열 상에서 갈아끼울 정확한 구간을
    찾는 함수다.

    반환:
        (start, end) — 여는 괄호 바로 다음 위치 ~ 짝이 맞는 닫는 괄호 직전 위치.
        IN 절을 못 찾으면 None.

    동작:
        1) 정규식으로 ``<선택적 한정자><컬럼> IN (`` 패턴을 찾는다. 테이블 한정자
           유무·대소문자는 무시하며, lookbehind ``(?<![\\w.])`` 로 ``xcol``·``a.col``
           같이 식별자 중간에 걸리는 오매칭을 막는다.
        2) 여는 괄호 다음부터 괄호 깊이(depth)를 세며 스캔해, 깊이가 0이 되는
           '짝이 맞는' 닫는 괄호를 찾는다. 값 목록 안에 함수 호출 등 중첩 괄호가
           있어도 올바른 끝을 찾기 위함이다.
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
    depth = 1  # 매치가 여는 괄호까지 포함하므로 깊이 1에서 시작
    for i in range(start, len(sql)):
        c = sql[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:  # 처음 여는 괄호와 짝이 맞는 닫는 괄호 도달
                return (start, i)
    return None


def split(
    parsed: ParsedQuery, parallelism: int, strategy: str = "contiguous"
) -> list[SubQuery]:
    """각각 값 부분집합을 스캔하는 완전한 sub-query SQL 문자열 N개를 생성한다.

    인자:
        parsed      : 검증을 통과한 :class:`~coordinator.parser.ParsedQuery`.
        parallelism : 원하는 분할(병렬) 개수. IN 값 개수로 클램핑된다(빈 sub-query
                      생성 안 함).
        strategy    : 값 분배 전략("contiguous" 또는 "round_robin").

    반환:
        :class:`SubQuery` 리스트(버킷 수만큼).

    동작:
        1) IN 노드를 다시 찾아 값 표현식들을 ``_chunk`` 로 버킷 분할한다(빈 버킷 제거).
        2) 원문에서 값 목록 구간(span)을 한 번만 찾아 두고, 버킷마다 그 구간만
           새 값 문자열로 치환해 sub-query를 만든다(원문 포맷 보존).
        3) 정규식으로 span을 못 찾으면 AST를 복제해 IN 값 목록만 교체 후 재직렬화하는
           폴백 경로를 쓴다(포맷은 보존되지 않을 수 있음).

    파티션 IN 절은 트리 어디에 있든(중첩 서브쿼리 포함) ``find_partition_in`` 으로
    정확히 찾아 대체한다.
    """
    src_in = find_partition_in(parsed.expression, parsed.partition_column)
    value_exprs = list(src_in.expressions)
    buckets = [b for b in _chunk(value_exprs, parallelism, strategy) if b]

    # 원문 치환 구간은 버킷마다 동일하므로 루프 밖에서 한 번만 계산한다.
    span = _value_span(parsed.sql, parsed.partition_column)
    # 요청 parallelism 보다 적은 task 가 왜 생겼는지(값 개수로 클램프) 추적할 수 있게 남긴다.
    logger.debug(
        "분할: 요청 parallelism=%d → %d버킷 (파티션 값 %d개, 전략=%s, 컬럼=%s)",
        parallelism, len(buckets), len(value_exprs), strategy, parsed.partition_column,
    )
    if span is None:
        # 원문 포맷이 바뀔 수 있는 결정 — 정규식으로 값 구간을 못 찾아 AST 재직렬화로 폴백.
        logger.debug(
            "포맷 보존 실패(값 span 미발견) → AST 재직렬화 폴백 (컬럼=%s)",
            parsed.partition_column,
        )

    sub_queries: list[SubQuery] = []
    for bucket in buckets:
        # 각 값 노드를 방언 기준 SQL 문자열로 렌더링한다.
        rendered = [b.sql(dialect=parsed.dialect) for b in bucket]
        if span is not None:
            # 원문 보존 경로: 값 목록 구간만 새 값들로 치환하고 앞뒤 SQL은 그대로 둔다.
            s, e = span
            sub_sql = parsed.sql[:s] + ", ".join(rendered) + parsed.sql[e:]
        else:
            # 폴백 경로: 정규식으로 구간을 못 찾았을 때 AST를 복제·수정 후 재직렬화.
            # 원본 AST를 건드리지 않도록 copy() 후 작업한다.
            cloned = parsed.expression.copy()
            target_in = find_partition_in(cloned, parsed.partition_column)
            target_in.set("expressions", [b.copy() for b in bucket])
            sub_sql = cloned.sql(dialect=parsed.dialect)
        sub_queries.append(SubQuery(sql=sub_sql, partition_values=rendered))

    return sub_queries
