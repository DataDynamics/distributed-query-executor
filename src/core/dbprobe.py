"""데이터소스 SELECT 미리보기 / 연결 테스트 공용 로직.

coordinator·executor 의 ``/datasources`` 테스트 엔드포인트가 공유한다. 임의 SQL 을
대상 데이터소스(Impala / Greenplum / history DB)에서 실행해 상위 N행을 JSON 안전
형태로 돌려준다 — 운영 점검 시 "연결이 되는가 + 어떤 데이터가 보이는가" 를 한 번에
확인하는 용도다.

설계 원칙:
- **임의 SQL 허용**(내부 점검용). 단 반환 행수는 ``limit`` 으로 잘라 대량 결과로
  메모리가 터지지 않게 한다. SQL 에 ``LIMIT`` 을 주입하지 않고 ``fetchmany`` 로
  잘라내므로 원본 쿼리를 그대로 보존한다(truncated 플래그로 잘림 여부를 알린다).
- 드라이버 호출은 **블로킹**이다. async 핸들러는 ``asyncio.to_thread`` 등으로 감싼다.
- PostgreSQL(Greenplum/history) 은 **커밋하지 않고** 연결을 닫는다(implicit rollback).
  '테스트' 가 의도치 않게 데이터를 영구화하지 않게 하기 위함이다.
- psycopg/impyla 는 **지연 임포트**한다(coordinator 에는 impyla 가 없을 수 있음).
"""
from __future__ import annotations

import time
from dataclasses import dataclass

# 미리보기 반환 행수 상한/기본값. 임의 SQL 이라도 응답 크기는 이 범위로 제한한다.
MAX_PREVIEW_ROWS = 10_000
DEFAULT_PREVIEW_ROWS = 100


@dataclass
class QueryResult:
    """SELECT 미리보기 결과. ``to_dict`` 로 API 응답(JSON)에 그대로 펼쳐 쓴다."""

    columns: list
    rows: list
    row_count: int
    truncated: bool   # limit 을 넘겨 결과가 잘렸는지
    elapsed_ms: float

    def to_dict(self) -> dict:
        return {
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "truncated": self.truncated,
            "elapsed_ms": round(self.elapsed_ms, 3),
        }


def clamp_limit(limit) -> int:
    """외부 입력 limit 을 1~``MAX_PREVIEW_ROWS`` 범위로 강제한다(잘못된 값은 기본값)."""
    try:
        n = int(limit)
    except (TypeError, ValueError):
        n = DEFAULT_PREVIEW_ROWS
    return max(1, min(n, MAX_PREVIEW_ROWS))


def _json_safe(value):
    """JSON 직렬화 가능한 형태로 변환한다.

    DB 드라이버가 돌려주는 Decimal/날짜/시각/바이트/UUID/배열/JSONB 등을 안전하게
    바꾼다. 미리보기 목적이므로 표현 보존을 우선해, 알 수 없는 타입은 ``str`` 로 떨군다.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    # Decimal, datetime/date/time, UUID, 기타 → 문자열
    return str(value)


def _shape(columns, raw_rows, limit: int, started: float) -> QueryResult:
    """드라이버에서 받은 원시 행들을 limit 으로 자르고 JSON 안전 형태로 정형한다.

    ``raw_rows`` 는 limit+1 개를 받아오므로, limit 을 넘으면 truncated=True 로 표시한다.
    """
    truncated = len(raw_rows) > limit
    rows = [[_json_safe(c) for c in row] for row in raw_rows[:limit]]
    elapsed = (time.perf_counter() - started) * 1000.0
    return QueryResult(
        columns=list(columns),
        rows=rows,
        row_count=len(rows),
        truncated=truncated,
        elapsed_ms=elapsed,
    )


def run_postgres_select(dsn: str, sql: str, *, limit: int) -> QueryResult:
    """psycopg 로 PostgreSQL/Greenplum 에 ``sql`` 을 실행해 상위 ``limit`` 행을 반환한다.

    커밋하지 않고 연결을 닫는다(implicit rollback) — 테스트가 데이터를 영구화하지 않도록.
    행을 돌려주지 않는 SQL(SET·DDL 등)은 columns/rows 가 빈 리스트가 된다.
    """
    import psycopg  # 지연 임포트

    started = time.perf_counter()
    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return _shape([], [], limit, started)
            columns = [d[0] for d in cur.description]
            raw = cur.fetchmany(limit + 1)
            return _shape(columns, raw, limit, started)
    finally:
        conn.close()  # commit 없이 닫음 → 변경분 롤백


def run_impala_select(
    impala_dsn: dict, sql: str, *, query_options=None, limit: int
) -> QueryResult:
    """impyla 로 Impala 에 ``sql`` 을 실행해 상위 ``limit`` 행을 반환한다.

    ``query_options`` 가 있으면 ``configuration`` 으로 넘긴다(executor 의 백엔드와 동일).
    """
    from impala.dbapi import connect as impala_connect  # 지연 임포트(executor 전용 드라이버)

    started = time.perf_counter()
    conn = impala_connect(**impala_dsn)
    try:
        cur = conn.cursor()
        opts = dict(query_options or {})
        if opts:
            cur.execute(sql, configuration=opts)
        else:
            cur.execute(sql)
        if cur.description is None:
            return _shape([], [], limit, started)
        columns = [d[0] for d in cur.description]
        raw = cur.fetchmany(limit + 1)
        return _shape(columns, raw, limit, started)
    finally:
        conn.close()
