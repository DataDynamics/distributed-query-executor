"""들어온 Impala SELECT 쿼리에 대한 검증 및 파싱.

1단계(Stage 1)는 *단순* SELECT만 지원한다:
  - 단일 문이어야 하며 반드시 SELECT
  - 술어(predicate)에 ``<partition_column> IN (<리터럴>, ...)`` 가 있어야 함
  - GROUP BY / HAVING / 집계 함수 / DISTINCT / JOIN 미지원
  - IN 목록은 리터럴 값이어야 하고(서브쿼리 불가), 비어 있지 않으며, 부정(NOT IN)이 아니어야 함

Impala SQL은 sqlglot의 ``hive`` 방언으로 파싱한다(가장 근접한 방언).
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

DIALECT = "hive"


class QueryValidationError(Exception):
    """들어온 쿼리가 1단계에서 지원되지 않을 때 발생."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")


@dataclass
class ParsedQuery:
    sql: str
    partition_column: str
    expression: exp.Select
    partition_values: list[str]
    dialect: str = DIALECT


def _column_name(node: exp.Expression | None) -> str | None:
    """테이블 한정자를 제외한 순수 컬럼명을 반환(없으면 None)."""
    if isinstance(node, exp.Column):
        return node.name
    return None


def find_partition_in(select: exp.Expression, partition_column: str) -> exp.In | None:
    """좌변이 파티션 컬럼인 ``IN`` 노드를 찾는다."""
    target = partition_column.split(".")[-1].lower()
    for in_node in select.find_all(exp.In):
        if _column_name(in_node.this) == target.lower() or (
            _column_name(in_node.this) or ""
        ).lower() == target:
            return in_node
    return None


def validate_and_parse(
    sql: str,
    partition_column: str,
    dialect: str = DIALECT,
    strict: bool = True,
) -> ParsedQuery:
    """쿼리를 검증/파싱하고 :class:`ParsedQuery` 를 반환한다.

    strict=True (1단계 기본): 단순 SELECT만 허용한다. GROUP BY/집계/DISTINCT/JOIN을
        거부하고, 파티션 IN이 최상위 WHERE에 있어야 한다.
    strict=False (lenient): 복합 쿼리(중첩 서브쿼리/JOIN/GROUP BY/unnest 등)를 허용하며,
        파티션 컬럼의 IN 절을 트리 어디에 있든 찾아 분할한다. IN 절 자체에 대한
        제약(리터럴·비부정·비어있지 않음)만 검사한다.
        ※ 결과 보존 가정: 분할 기준 컬럼이 출력 행을 분할하는 위치(주로 소스 스캔의
          필터)에 있어야 한다. 분할 기준 컬럼 위에서 집계/DISTINCT 하는 쿼리는 결과가
          달라질 수 있으므로 호출자가 책임진다.

    위반 시 안정적인 ``code`` 를 가진 :class:`QueryValidationError` 를 발생시킨다.
    """
    if not sql or not sql.strip():
        raise QueryValidationError("PARSE_ERROR", "빈 쿼리입니다.")
    if not partition_column or not partition_column.strip():
        raise QueryValidationError("MISSING_PARTITION_COLUMN", "partition_column이 필요합니다.")

    try:
        statements = [s for s in sqlglot.parse(sql, read=dialect) if s is not None]
    except Exception as exc:  # sqlglot은 ParseError / TokenError 를 발생시킴
        raise QueryValidationError("PARSE_ERROR", f"SQL 파싱 실패: {exc}") from exc

    if not statements:
        raise QueryValidationError("PARSE_ERROR", "유효한 SQL 문이 없습니다.")
    if len(statements) > 1:
        raise QueryValidationError(
            "MULTIPLE_STATEMENTS", "여러 개의 SQL 문은 지원하지 않습니다."
        )

    stmt = statements[0]
    if not isinstance(stmt, exp.Select):
        raise QueryValidationError("NOT_A_SELECT", "SELECT 문만 지원합니다.")

    if strict:
        if stmt.args.get("distinct"):
            raise QueryValidationError("UNSUPPORTED_DISTINCT", "DISTINCT는 1단계에서 지원하지 않습니다.")
        if stmt.args.get("group"):
            raise QueryValidationError("UNSUPPORTED_GROUP_BY", "GROUP BY는 1단계에서 지원하지 않습니다.")
        if stmt.args.get("having"):
            raise QueryValidationError("UNSUPPORTED_HAVING", "HAVING은 1단계에서 지원하지 않습니다.")
        if stmt.args.get("joins"):
            raise QueryValidationError("UNSUPPORTED_JOIN", "JOIN은 1단계에서 지원하지 않습니다.")

        agg = stmt.find(exp.AggFunc)
        if agg is not None:
            raise QueryValidationError(
                "UNSUPPORTED_AGGREGATE",
                f"집계 함수({agg.sql(dialect=dialect)})는 1단계에서 지원하지 않습니다.",
            )

        if stmt.args.get("where") is None:
            raise QueryValidationError(
                "NO_PARTITION_IN_CLAUSE", "WHERE 절에 파티션 IN 조건이 필요합니다."
            )

    in_node = find_partition_in(stmt, partition_column)
    if in_node is None:
        raise QueryValidationError(
            "NO_PARTITION_IN_CLAUSE",
            f"파티션 컬럼 '{partition_column}'에 대한 IN 조건을 찾지 못했습니다.",
        )

    if isinstance(in_node.parent, exp.Not):
        raise QueryValidationError(
            "NEGATED_IN", "NOT IN 조건은 분할 시 의미가 달라지므로 지원하지 않습니다."
        )

    if in_node.args.get("query") is not None:
        raise QueryValidationError(
            "SUBQUERY_IN_CLAUSE", "IN 절의 서브쿼리는 지원하지 않습니다."
        )

    values = in_node.expressions
    if not values:
        raise QueryValidationError("EMPTY_IN_LIST", "IN 값 목록이 비어 있습니다.")

    partition_values = [v.sql(dialect=dialect) for v in values]
    return ParsedQuery(
        sql=sql,
        partition_column=partition_column,
        expression=stmt,
        partition_values=partition_values,
        dialect=dialect,
    )
