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

import math
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


# pandas 의 결측 싱글턴 타입 이름. pandas 를 import 하지 않고 판정하려고 타입명으로 본다
# (이 모듈은 pandas 를 의존성으로 두지 않는다 — 쓰는 배포에만 설치돼 있으면 된다).
_MISSING_TYPE_NAMES = ("NaTType", "NAType")


def _is_missing(value) -> bool:
    """``None``/``NaN``/``NaT``/``pd.NA`` 처럼 '값 없음' 을 뜻하는지 판정한다.

    JSON 에는 ``NaN``/``NaT`` 표현이 없다(``json.dumps`` 가 내는 ``NaN`` 리터럴은 표준
    JSON 이 아니라 엄격한 파서가 거부한다). 그래서 결측은 모두 ``null`` 로 떨군다.
    pandas/numpy 없이도 동작해야 하므로 import 없이 판정한다.
    """
    if value is None:
        return True
    if type(value).__name__ in _MISSING_TYPE_NAMES:  # pd.NaT / pd.NA
        return True
    # NaN 은 자기 자신과 같지 않다. np.float64(nan) 은 float 하위형이라 함께 걸린다.
    return isinstance(value, float) and value != value


def _json_safe(value):
    """JSON 직렬화 가능한 형태로 변환한다.

    DB 드라이버가 돌려주는 Decimal/날짜/시각/바이트/UUID/배열/JSONB 와, pandas/numpy 가
    돌려주는 numpy 스칼라·배열·결측값(NaN/NaT/NA)을 안전하게 바꾼다. 미리보기 목적이므로
    표현 보존을 우선해, 알 수 없는 타입은 ``str`` 로 떨군다.

    **numpy 값 처리가 왜 필요한가**: ``np.int64``/``np.bool_`` 은 파이썬 ``int``/``bool`` 을
    상속하지 **않아** 아래 ``str`` 폴백으로 떨어진다(숫자가 ``'7'`` 로, 불리언이 ``'True'``
    로 나간다). 반면 ``np.float64`` 는 ``float`` 하위형이라 우연히 통과한다 — 그대로 두면
    **컬럼 dtype 에 따라 결과 타입이 갈리는** 찾기 어려운 불일치가 된다. numpy 스칼라·배열은
    모두 ``dtype`` + ``tolist`` 를 가지므로 이를 덕타이핑해 파이썬 기본형으로 낮춘 뒤 재귀한다.
    """
    if _is_missing(value):
        return None
    # numpy 스칼라/배열(np.int64, np.bool_, np.ndarray …) → 파이썬 기본형으로 낮춘 뒤 재귀.
    # 아래 스칼라 분기보다 먼저 둬야 np.float64 도 파이썬 float 로 정규화된다.
    if hasattr(value, "dtype") and hasattr(value, "tolist"):
        try:
            return _json_safe(value.tolist())
        except Exception:
            return str(value)
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        # inf/-inf 도 표준 JSON 이 아니라 null 로 떨군다(NaN 은 _is_missing 이 먼저 걸렀다).
        return value if math.isfinite(value) else None
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    # Decimal, datetime/date/time, pandas.Timestamp, UUID, 기타 → 문자열
    return str(value)


def _is_dataframe(obj) -> bool:
    """pandas DataFrame(또는 같은 인터페이스를 가진 객체)인지 덕타이핑으로 판정한다.

    pandas 는 이 프로젝트의 의존성이 아니므로(쓰는 배포에만 설치) ``isinstance`` 대신
    DataFrame 의 특징적인 속성 조합으로 본다. 리스트/튜플 같은 일반 시퀀스는 ``columns``
    를 갖지 않으므로 오탐하지 않는다(오탐하면 기존 커서 경로가 깨진다).
    """
    return (
        obj is not None
        and hasattr(obj, "columns")
        and hasattr(obj, "iloc")
        and hasattr(obj, "itertuples")
    )


def _dataframe_head(df, limit: int) -> tuple[list, list]:
    """DataFrame 에서 ``(컬럼명, 상위 limit+1행 튜플 목록)`` 을 뽑는다.

    - ``iloc[:limit+1]``: 커서의 ``fetchmany(limit+1)`` 과 **같은 규약**이라 truncated
      판정 로직이 그대로다. 100만 행짜리가 와도 값 변환 비용은 미리보기 크기에 비례한다.
    - ``itertuples``: ``.values``/``.to_numpy()`` 는 혼합 dtype 을 object/float 로 업캐스트해
      int 가 float 로 변한다. itertuples 는 컬럼별 dtype 을 보존한다(``name=None`` 은
      네임드튜플 컬럼 수 제한 회피).
    - ``index=False``: SQL 결과셋에는 인덱스 개념이 없다. 인덱스를 값으로 쓰려면 호출부가
      ``df.reset_index()`` 로 컬럼화해서 넘긴다.
    - 컬럼명은 정수 등일 수 있어 문자열로 정규화한다.
    """
    cols = [str(c) for c in df.columns]
    head = df.iloc[: limit + 1]
    return cols, list(head.itertuples(index=False, name=None))


def _shape(columns, raw_rows, limit: int, started: float) -> QueryResult:
    """드라이버에서 받은 원시 행들을 limit 으로 자르고 JSON 안전 형태로 정형한다.

    ``raw_rows`` 는 두 형태를 받는다:

    1. ``fetchmany(limit+1)`` 로 받은 **튜플 목록**(커서 경로) — limit 을 넘으면 truncated.
    2. **pandas DataFrame** — 커스텀 실행 함수가 커서 대신 DataFrame 을 돌려받는 사내
       게이트웨이/API 를 쓰는 경우. 컬럼명·행·truncated 를 DataFrame 에서 뽑는다::

           return _shape(None, df, limit, started)

       ``columns`` 를 함께 주면 그 값이 우선한다(컬럼명을 갈아끼우고 싶을 때).
    """
    if _is_dataframe(raw_rows):
        df_cols, raw_rows = _dataframe_head(raw_rows, limit)
        if not columns:  # None/빈 목록이면 DataFrame 의 컬럼명을 쓴다
            columns = df_cols
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
