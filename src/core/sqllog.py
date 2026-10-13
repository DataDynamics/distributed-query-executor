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

출력 형식은 ``logging.sql.format`` 으로 고르며 기본은 ``multiline`` 이다. 템플릿으로 렌더한
SQL 을 줄바꿈과 들여쓰기 그대로 남겨야 읽고 재실행하기 쉽기 때문이다. 레코드 하나에 머리줄과
본문을 함께 담고, 본문 각 줄 앞에는 ``    | `` 를 붙여 다른 로그 줄과 구분한다. 레코드가 하나라
동시에 실행되는 다른 요청의 로그와 섞이지 않는다::

    SQL 실행 datasource=greenplum phase=INSERT target=public.sales (3줄)
        | INSERT INTO public.sales
        | SELECT *
        |   FROM stg_sales
        params=[...]

로그 수집기가 한 줄을 한 레코드로 다루는 환경이라면 ``oneline`` 으로 바꾼다. 그때는 공백을
접어 예전처럼 한 줄로 남긴다::

    SQL 실행 datasource=greenplum phase=INSERT target=public.sales | INSERT INTO ... | params=[...]

템플릿 렌더 결과는 :func:`log_template_render` 가 같은 형식으로 남긴다. 실행 직전 SQL 이
executor 쪽에 흩어져 남는 것과 달리, coordinator 가 어떤 파라미터로 어떤 SQL 을 만들었는지를
렌더 시점에 한곳에 남겨 둔다.
"""

# Python 3.9 호환을 위해 어노테이션 평가를 지연한다. PEP 604 (``X | None``) 유니언을 시그니처에 쓰기 때문이다.
from __future__ import annotations

import logging
import re
import textwrap

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


# multiline 형식에서 SQL 본문 각 줄 앞에 붙이는 표시다. 머리줄과 본문을 눈으로 가르고,
# ``grep -A`` 로 뽑은 뒤에도 어디까지가 SQL 인지 알 수 있게 한다.
BLOCK_PREFIX = "    | "


def format_sql_block(sql: str | None, max_length: int) -> list[str]:
    """SQL 을 multiline 로그용 줄 목록으로 만든다. 마스킹하고 원래 줄바꿈과 들여쓰기를 지킨다.

    템플릿이 Jinja 블록 때문에 공통 들여쓰기나 앞뒤 빈 줄, 줄 끝 공백을 남기는 경우가 많아,
    공통 들여쓰기는 ``textwrap.dedent`` 로 걷고 앞뒤 빈 줄과 줄 끝 공백만 정리한다. 줄 사이의
    빈 줄과 상대 들여쓰기는 그대로 둔다. ``max_length`` 를 넘으면 글자 수 기준으로 자르고
    마지막 줄에 절단 사실을 적는다(0 이하면 자르지 않는다).
    """
    text = mask_text(sql) if sql else ""
    text = textwrap.dedent(text.expandtabs(4))
    lines = [ln.rstrip() for ln in text.splitlines()]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    body = "\n".join(lines)
    if max_length and max_length > 0 and len(body) > max_length:
        cut = body[:max_length].rstrip()
        lines = cut.splitlines() + [f"… (총 {len(body)}자 중 {len(body) - len(cut)}자 절단)"]
    return lines


def render_sql_block(sql: str | None, max_length: int) -> str:
    """:func:`format_sql_block` 의 줄들에 :data:`BLOCK_PREFIX` 를 붙여 하나의 문자열로 잇는다."""
    return "\n".join(BLOCK_PREFIX + ln for ln in format_sql_block(sql, max_length))


def _is_multiline(cfg) -> bool:
    """설정의 ``logging.sql.format`` 이 multiline 인지 본다. 모르는 값은 기본(multiline)으로 다룬다."""
    return str(getattr(cfg, "log_sql_format", "multiline") or "multiline").strip().lower() != "oneline"


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
        with_params = params is not None and getattr(cfg, "log_sql_params", True)
        if _is_multiline(cfg):
            lines = format_sql_block(sql, max_length)
            text = " ".join(parts) + f" ({len(lines)}줄)"
            if lines:
                text += "\n" + "\n".join(BLOCK_PREFIX + ln for ln in lines)
            if with_params:
                text += "\n    params=" + format_params(params, max_length)
            logger.info("%s", text)
            return
        line = " ".join(parts) + " | " + format_sql(sql, max_length)
        if with_params:
            line += " | params=" + format_params(params, max_length)
        logger.info("%s", line)
    except Exception:  # 로깅 실패가 적재를 깨뜨리지 않게 한다
        logger.debug("SQL 로깅 실패 — 무시", exc_info=True)


def log_sql_parts(head: str, sqls: dict, *, settings=None) -> None:
    """머리줄 하나와 이름 붙은 SQL 조각 여러 개를 레코드 하나로 남긴다(실행이 아니라 '만든 SQL' 기록용).

    인자:
        head: 머리줄. 무엇을 만들었는지(템플릿 렌더·dry-run 계획 등)를 적는다.
        sqls: ``{조각 이름: SQL}`` 이다. 비어 있는 조각은 건너뛴다.
        settings: 설정 객체(미지정 시 전역 설정). 테스트에서 주입한다.

    실행 SQL 로그와 같은 스위치(``logging.sql.enabled``)·형식·길이 상한을 따른다. multiline 이면
    조각마다 ``  [이름]`` 줄 아래에 원문 줄을 붙이고, oneline 이면 조각마다 한 줄씩 남긴다.
    :func:`log_sql` 과 마찬가지로 예외를 호출부로 올리지 않는다.
    """
    cfg = _resolve(settings)
    if not getattr(cfg, "log_sql_enabled", True):
        return
    try:
        max_length = int(getattr(cfg, "log_sql_max_length", 4000))
        parts = [(role, sql) for role, sql in sqls.items() if sql]
        if _is_multiline(cfg):
            lines = [head]
            for role, sql in parts:
                lines.append(f"  [{role}]")
                lines.extend(BLOCK_PREFIX + ln for ln in format_sql_block(sql, max_length))
            logger.info("%s", "\n".join(lines))
            return
        for role, sql in parts:
            logger.info("%s [%s] | %s", head, role, format_sql(sql, max_length))
    except Exception:  # 로깅 실패가 요청 처리를 깨뜨리지 않게 한다
        logger.debug("SQL 조각 로깅 실패 — 무시", exc_info=True)


def log_template_render(
    template_id: str | None,
    params,
    sqls: dict,
    *,
    note: str | None = None,
    settings=None,
) -> None:
    """템플릿 렌더 결과를 조각(role)별로 남긴다. coordinator 가 렌더 직후, 검증보다 먼저 호출한다.

    인자:
        template_id: 렌더한 템플릿 ID.
        params: 렌더에 쓴 파라미터(요청 그대로). 마스킹을 거쳐 머리줄에 붙이며,
            ``logging.sql.params=false`` 면 남기지 않는다.
        sqls: ``{조각 이름: SQL}`` 이다. 예를 들어 ``select``·``staging_ddl``·``insert``·``count``.
        note: 머리줄 끝 괄호에 덧붙일 설명(선택). 날짜 fan-out 의 task 수처럼 SQL 만으로는 알 수
            없는 정보를 적는다.
        settings: 설정 객체(미지정 시 전역 설정). 테스트에서 주입한다.
    """
    cfg = _resolve(settings)
    try:
        head = f"템플릿 렌더 template={template_id or '-'}"
        if params is not None and getattr(cfg, "log_sql_params", True):
            head += " params=" + format_params(params, int(getattr(cfg, "log_sql_max_length", 4000)))
        if note:
            head += f" ({note})"
    except Exception:
        logger.debug("템플릿 렌더 로깅 실패 — 무시", exc_info=True)
        return
    log_sql_parts(head, sqls, settings=cfg)


def datasource_of(cursor, default: str = "impala") -> str:
    """커서가 어느 데이터소스의 것인지 추론한다(로그 표기용).

    커스텀 소스 어댑터(:class:`executor.backend._FunctionCursor`)는 자기 datasource
    이름을 ``_name`` 으로 들고 있고, impyla 커서에는 그런 속성이 없다. 이 한 곳에서
    분기하므로 ``_source_execute`` 는 시그니처를 바꾸지 않아도 된다(기존 호출부 무변경).
    """
    return str(getattr(cursor, "_name", "") or default).strip().lower()
