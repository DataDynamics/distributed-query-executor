"""들어온 Impala SELECT 쿼리의 검증 및 파싱 모듈.

이 모듈은 사용자가 제출한 SQL이 '파티션 IN 목록 기준 분할'에 적합한지 검사하고,
분할에 필요한 정보(파티션 컬럼의 IN 값 목록, 파싱된 AST)를 추출한다. 실제 분할은
:mod:`splitter` 가 담당하며, 여기서는 분할 가능 여부 판단과 값 추출까지만 한다.

검증은 두 가지 모드로 동작한다.

strict 모드(1단계 기본): *단순* SELECT만 지원한다.
  - 단일 문이어야 하며 반드시 SELECT
  - 술어(predicate)에 ``<partition_column> IN (<리터럴>, ...)`` 가 있어야 함
  - GROUP BY / HAVING / 집계 함수 / DISTINCT / JOIN 미지원
  - 파티션 IN은 최상위 WHERE 절에 존재해야 함

lenient 모드(strict=False): 중첩 서브쿼리·JOIN·GROUP BY 등 복합 쿼리도 허용하며,
파티션 컬럼의 IN 절을 AST 트리 어디에 있든 찾아 분할한다. 다만 IN 절 자체의
제약(리터럴·비부정·비어있지 않음)은 두 모드 모두 동일하게 검사한다.

공통적으로 IN 목록은 리터럴 값이어야 하고(서브쿼리 불가), 비어 있지 않으며,
부정(NOT IN)이 아니어야 한다.

Impala SQL은 전용 방언이 없으므로 sqlglot의 ``hive`` 방언으로 파싱한다
(문법이 가장 근접하기 때문).
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

# sqlglot 파싱 시 사용하는 기본 방언. Impala 전용 방언이 없어 가장 가까운 hive를 쓴다.
DIALECT = "hive"


class QueryValidationError(Exception):
    """들어온 쿼리가 지원되지 않거나 분할 불가일 때 발생하는 예외.

    ``code`` 는 호출자(API 계층 등)가 안정적으로 분기·매핑할 수 있도록 한 식별자이며
    (예: ``NO_PARTITION_IN_CLAUSE``, ``UNSUPPORTED_JOIN``), ``message`` 는 사람이 읽는
    한글 설명이다. 부모 예외 메시지는 ``[code] message`` 형태로 합쳐 저장한다.
    """

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")


@dataclass
class ParsedQuery:
    """검증을 통과한 쿼리의 파싱 결과 묶음(splitter로 전달되는 입력).

    필드:
        sql              : 원본 SQL 문자열(splitter가 포맷 보존 치환에 사용).
        partition_column : 분할 기준 컬럼명.
        expression       : sqlglot로 파싱된 SELECT AST 루트.
        partition_values : 추출된 IN 목록의 각 값(방언 기준 SQL 문자열 형태).
        dialect          : 파싱·재직렬화에 쓰인 sqlglot 방언.
    """

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
    """좌변이 파티션 컬럼인 ``IN`` 노드를 AST 전체에서 찾아 반환한다(없으면 None).

    ``find_all`` 로 트리 전체를 훑으므로 중첩 서브쿼리 안의 IN 절도 찾는다(lenient 모드
    대응). 비교 시 ``schema.col`` 같은 테이블 한정자는 ``split(".")[-1]`` 로 떼어내고
    대소문자도 무시하여, 사용자가 준 컬럼명과 쿼리 내 표기가 달라도 매칭되게 한다.
    """
    target = partition_column.split(".")[-1].lower()
    for in_node in select.find_all(exp.In):
        if _column_name(in_node.this) == target.lower() or (
            _column_name(in_node.this) or ""
        ).lower() == target:
            return in_node
    return None


def is_row_returning(sql: str, dialect: str = DIALECT) -> bool:
    """sql 의 최상위 문이 행을 반환하는지(SELECT/UNION/서브쿼리) 여부.

    copy(STDIN) 모드는 결과셋이 필요하므로, 래퍼가 행을 반환하는지 검사하는 데 쓴다.
    파싱 실패 시 False.
    """
    try:
        stmt = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return False
    return isinstance(stmt, (exp.Select, exp.Union, exp.Subquery))


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

    # strict 모드에서만 복합 쿼리 요소를 거부한다. lenient 모드는 이 블록을 건너뛰어
    # 집계/JOIN/중첩 서브쿼리가 있어도 IN 절만 찾으면 분할을 허용한다.
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

    # 분할의 핵심: 파티션 컬럼에 대한 IN 노드를 찾는다(두 모드 공통 필수 조건).
    in_node = find_partition_in(stmt, partition_column)
    if in_node is None:
        raise QueryValidationError(
            "NO_PARTITION_IN_CLAUSE",
            f"파티션 컬럼 '{partition_column}'에 대한 IN 조건을 찾지 못했습니다.",
        )

    # NOT IN: 값 목록을 부분집합으로 쪼개면 '여집합' 의미가 깨지므로(각 조각의 NOT IN
    # 합집합 ≠ 원래 NOT IN) 분할 대상에서 제외한다.
    if isinstance(in_node.parent, exp.Not):
        raise QueryValidationError(
            "NEGATED_IN", "NOT IN 조건은 분할 시 의미가 달라지므로 지원하지 않습니다."
        )

    # ``IN (SELECT ...)`` 형태는 분할할 리터럴 목록이 없으므로 지원하지 않는다.
    if in_node.args.get("query") is not None:
        raise QueryValidationError(
            "SUBQUERY_IN_CLAUSE", "IN 절의 서브쿼리는 지원하지 않습니다."
        )

    values = in_node.expressions
    if not values:
        raise QueryValidationError("EMPTY_IN_LIST", "IN 값 목록이 비어 있습니다.")

    # 각 값 노드를 방언 기준 SQL 문자열로 직렬화해 보관(splitter가 다시 조합에 사용).
    partition_values = [v.sql(dialect=dialect) for v in values]
    return ParsedQuery(
        sql=sql,
        partition_column=partition_column,
        expression=stmt,
        partition_values=partition_values,
        dialect=dialect,
    )
