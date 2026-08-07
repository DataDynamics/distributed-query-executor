"""실행 SQL 로깅 — "어떤 데이터소스에 어떤 쿼리를 보냈는가"를 한 줄로 남긴다.

coordinator·executor 가 데이터소스(Impala/Trino 등 소스, Greenplum 타깃, history DB)에
실제로 던지는 모든 SQL 을 같은 형식으로 기록한다. 사고가 났을 때 "무엇을 읽어 무엇을
적재했는지"를 되짚는 1차 근거이므로, 다음 세 가지를 설계 원칙으로 삼았다.

1. **기본 INFO** — HTTP 본문 로깅(:mod:`core.http_logging`)과 달리 DEBUG 를 요구하지
   않는다. 운영 기본 레벨이 INFO 라 DEBUG 로만 남기면 정작 필요한 순간에 기록이 없다.
   끄려면 ``logging.sql.enabled=false`` 로 명시해야 한다.
2. **datasource 를 반드시 표기** — 같은 SELECT 라도 Impala 커서로 읽었는지 커스텀 API
   (Trino 등)로 읽었는지에 따라 결과가 달라진다. 이 값이 없으면 로그만 봐서는 어느
   엔진이 실행했는지 알 수 없다.
3. **job_id/task_id 는 자동** — :mod:`core.logging` 의 record factory 가 모든 레코드에
   ``[job_id][task_id]`` 를 붙이므로 이 모듈은 식별자를 따로 받지 않는다. 호출부가
   ``job_log_context`` 안에서 실행되기만 하면 된다(그렇지 않으면 ``-`` 로 찍힌다).

한 줄 형식::

    SQL 실행 datasource=greenplum phase=INSERT target=public.sales | INSERT INTO ... | params=[...]

SQL 은 공백을 접어 **한 줄**로 만든다. 로그 파일이 한 줄=한 레코드 형식이라, 여러 줄
SQL 을 그대로 쓰면 grep·수집기가 레코드 경계를 잃기 때문이다.
"""

# Python 3.9 호환을 위해 어노테이션 평가를 지연한다. PEP 604 (``X | None``) 유니언을 시그니처에 쓰기 때문이다.
from __future__ import annotations

import logging
import re

from core.config import settings as default_settings
from core.masking import mask_text

# 실행 SQL 전용 로거. 이름을 분리해 두면 운영에서 이 로거만 레벨을 따로 조정하거나
# 별도 핸들러로 뽑아낼 수 있다(core.http 와 같은 관례).
logger = logging.getLogger("core.sql")

# 연속된 공백과 개행, 탭을 공백 하나로 접을 때 쓰는 패턴이다.
_WS_RE = re.compile(r"\s+")


def collapse_sql(sql: str | None) -> str:
    """SQL 의 개행·연속 공백을 공백 하나로 접어 한 줄로 만든다(앞뒤 공백 제거)."""
    if not sql:
        return ""
    return _WS_RE.sub(" ", str(sql)).strip()


def format_sql(sql: str | None, max_length: int) -> str:
    """로그에 실을 SQL 문자열을 만든다. 마스킹한 뒤 한 줄로 접고 길이를 자르는 순서다.

    인자:
        sql: 원본 SQL.
        max_length: 로그에 남길 최대 길이. 0 이하면 절단하지 않는다.

    반환:
        마스킹·접기·절단이 끝난 한 줄 문자열. 절단된 경우 뒤에
        ``… (총 N자 중 M자 절단)`` 을 붙여 **원문이 더 길었다는 사실을 숨기지 않는다**
        (절단 표시가 없으면 로그의 SQL 을 그대로 재실행 가능한 전문으로 오해한다).
    """
    # 마스킹을 접기보다 먼저 한다: 마스킹 정규식이 개행을 포함한 원문 형태를 전제로 한다.
    text = collapse_sql(mask_text(sql) if sql else "")
    if max_length and max_length > 0 and len(text) > max_length:
        return f"{text[:max_length]}… (총 {len(text)}자 중 {len(text) - max_length}자 절단)"
    return text


def format_params(params, max_length: int) -> str:
    """바인드 파라미터를 로그용 문자열로 만든다(마스킹·절단은 SQL 과 동일 규칙).

    파라미터는 튜플/리스트/dict 무엇이든 올 수 있어 ``repr`` 로 문자열화한다.
    """
    if params is None:
        return ""
    return format_sql(repr(params), max_length)


def _resolve(settings):
    """설정 객체를 결정한다(명시 인자 > 전역 기본 설정)."""
    return settings if settings is not None else default_settings


def log_sql(
    datasource: str | None,
    sql: str | None,
    *,
    phase: str | None = None,
    target: str | None = None,
    params=None,
    settings=None,
) -> None:
    """실행 직전의 SQL 한 건을 표준 형식으로 기록한다.

    인자:
        datasource: 실행 엔진 이름(``impala``/``trino``/``greenplum``/``history`` 등).
            소문자로 정규화해 남긴다. 비어 있으면 ``unknown``.
        sql: 실행할 SQL 전문.
        phase: 실행 단계 힌트(``SOURCE_SELECT``/``INSERT``/``EXTERNAL_DDL`` 등).
            :mod:`core.phases` 의 단계명과 맞춰 쓰면 대시보드 타임라인과 대조하기 쉽다.
        target: 대상 테이블 등 부가 식별자(선택).
        params: 바인드 파라미터(선택). ``logging.sql.params=false`` 면 기록하지 않는다.
        settings: 설정 객체(미지정 시 전역 설정). 테스트에서 주입한다.

    로깅은 **부가 기능**이므로 어떤 예외도 호출부로 올리지 않는다 — 로그를 남기다
    실패해서 실제 적재가 깨지는 일이 없어야 한다.
    """
    cfg = _resolve(settings)
    if not getattr(cfg, "log_sql_enabled", True):
        return
    try:
        max_length = int(getattr(cfg, "log_sql_max_length", 4000))
        name = str(datasource or "").strip().lower() or "unknown"
        parts = [f"SQL 실행 datasource={name}"]
        if phase:
            parts.append(f"phase={phase}")
        if target:
            parts.append(f"target={target}")
        line = " ".join(parts) + " | " + format_sql(sql, max_length)
        if params is not None and getattr(cfg, "log_sql_params", True):
            line += " | params=" + format_params(params, max_length)
        logger.info("%s", line)
    except Exception:  # 로깅 실패가 적재를 깨뜨리지 않게 한다
        logger.debug("SQL 로깅 실패 — 무시", exc_info=True)


def datasource_of(cursor, default: str = "impala") -> str:
    """커서가 어느 데이터소스의 것인지 추론한다(로그 표기용).

    커스텀 소스 어댑터(:class:`executor.backend._FunctionCursor`)는 자기 datasource
    이름을 ``_name`` 으로 들고 있고, impyla 커서에는 그런 속성이 없다. 이 한 곳에서
    분기하므로 ``_source_execute`` 는 시그니처를 바꾸지 않아도 된다(기존 호출부 무변경).
    """
    return str(getattr(cursor, "_name", "") or default).strip().lower()
