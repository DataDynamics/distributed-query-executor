"""SQL validation & parsing for incoming Impala SELECT queries.

Stage 1 only supports a *simple* SELECT:
  - single statement, must be a SELECT
  - must contain ``<partition_column> IN (<literal>, ...)`` in the predicate
  - no GROUP BY / HAVING / aggregate functions / DISTINCT / JOIN
  - the IN list must be literal values (no subquery), non-empty, not negated

Impala SQL is parsed with sqlglot's ``hive`` dialect (closest available).
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

DIALECT = "hive"


class QueryValidationError(Exception):
    """Raised when an incoming query is not supported by stage 1."""

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


def _column_name(node: exp.Expression | None) -> str | None:
    """Return the bare column name (without table qualifier), or None."""
    if isinstance(node, exp.Column):
        return node.name
    return None


def find_partition_in(select: exp.Expression, partition_column: str) -> exp.In | None:
    """Find the ``IN`` node whose left-hand side is the partition column."""
    target = partition_column.split(".")[-1].lower()
    for in_node in select.find_all(exp.In):
        if _column_name(in_node.this) == target.lower() or (
            _column_name(in_node.this) or ""
        ).lower() == target:
            return in_node
    return None


def validate_and_parse(sql: str, partition_column: str) -> ParsedQuery:
    """Validate the query for stage-1 support and return a :class:`ParsedQuery`.

    Raises :class:`QueryValidationError` with a stable ``code`` on any violation.
    """
    if not sql or not sql.strip():
        raise QueryValidationError("PARSE_ERROR", "빈 쿼리입니다.")
    if not partition_column or not partition_column.strip():
        raise QueryValidationError("MISSING_PARTITION_COLUMN", "partition_column이 필요합니다.")

    try:
        statements = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except Exception as exc:  # sqlglot raises ParseError / TokenError
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
            f"집계 함수({agg.sql(dialect=DIALECT)})는 1단계에서 지원하지 않습니다.",
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

    partition_values = [v.sql(dialect=DIALECT) for v in values]
    return ParsedQuery(
        sql=sql,
        partition_column=partition_column,
        expression=stmt,
        partition_values=partition_values,
    )
