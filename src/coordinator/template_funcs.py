"""템플릿에서 호출 가능한 커스텀 함수(필터/글로벌) 레지스트리와 내장 구현.

쿼리 템플릿(:mod:`coordinator.template`)은 클라이언트가 보낸 **파라미터**를 SQL 문자열로
렌더링한다. 이때 파라미터는 비신뢰 입력이므로, 그대로 SQL 에 이어붙이면 SQL 인젝션 위험이
있다. 그래서 템플릿 작성자는 값을 SQL 에 넣을 때 **반드시 이 모듈의 필터**(``sql_str`` /
``sql_in`` / ``sql_ident`` / ``sql_num``)를 거쳐 안전하게 리터럴/식별자로 만든다.

또한 "무엇을·왜" 를 담는 도메인 헬퍼(예: ``date_range`` — 날짜 구간을 IN 목록으로 펼침)를
글로벌 함수로 제공한다. 개발자는 아래 두 데코레이터로 자기 함수를 등록할 수 있고,
설정 ``template.func_modules`` 에 모듈 경로를 적으면 엔진 기동 시 자동 import 되어 등록된다.

설계 메모:
    - **필터(FILTERS)**: ``{{ value | 이름 }}`` 형태로 값을 변환한다(파이프 오른쪽).
    - **글로벌(GLOBALS)**: ``{{ 이름(인자) }}`` 형태로 템플릿 안에서 호출하는 함수다.
    - 두 레지스트리는 모듈 전역 dict 이며, :func:`register_builtins` 가 내장 함수를 채운다.
      :func:`load_func_modules` 가 사용자 모듈을 import 해 추가 등록을 수행한다.
"""

from __future__ import annotations

import datetime as _dt
import importlib
import logging
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

# 템플릿 엔진에 주입되는 커스텀 함수 레지스트리. 엔진이 env.filters/env.globals 로 복사한다.
FILTERS: dict[str, Callable] = {}
GLOBALS: dict[str, Callable] = {}


def template_filter(name: str) -> Callable[[Callable], Callable]:
    """함수를 이름 있는 Jinja2 **필터**로 등록하는 데코레이터.

    ``@template_filter("sql_in")`` 로 감싸면 ``FILTERS["sql_in"]`` 에 등록되어
    템플릿에서 ``{{ values | sql_in }}`` 처럼 쓸 수 있다.
    """

    def deco(fn: Callable) -> Callable:
        FILTERS[name] = fn
        return fn

    return deco


def template_global(name: str) -> Callable[[Callable], Callable]:
    """함수를 이름 있는 Jinja2 **글로벌 함수**로 등록하는 데코레이터.

    ``@template_global("date_range")`` 로 감싸면 템플릿에서 ``{{ date_range(a, b) }}``
    처럼 직접 호출할 수 있다.
    """

    def deco(fn: Callable) -> Callable:
        GLOBALS[name] = fn
        return fn

    return deco


# ───────────────────────── SQL 안전 렌더 필터 ─────────────────────────
# 파라미터(비신뢰 입력)를 SQL 에 넣을 때 반드시 거쳐야 하는 이스케이프/인용 필터들.


@template_filter("sql_str")
def sql_str(value) -> str:
    """값을 작은따옴표로 감싼 **SQL 문자열 리터럴**로 만든다(작은따옴표 이스케이프).

    ``O'Brien`` → ``'O''Brien'`` 처럼 내부 ``'`` 를 ``''`` 로 이중화해 인젝션을 막는다.
    None 은 SQL ``NULL`` 로 렌더한다(빈 문자열 리터럴과 구분).
    """
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


@template_filter("sql_num")
def sql_num(value) -> str:
    """값이 숫자인지 확인하고 숫자 리터럴 문자열로 만든다(숫자가 아니면 오류).

    숫자 컬럼용. 문자열로 들어와도 int/float 로 변환 가능하면 허용하고, 불가하면
    ``ValueError`` 를 던져 인젝션(예: ``1 OR 1=1``)이 SQL 로 새어 나가지 않게 한다.
    """
    if isinstance(value, bool):  # bool 은 int 의 하위형이라 먼저 걸러 명시적으로 처리
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    # 문자열이면 숫자 파싱을 강제해 검증한다(실패 시 예외 → 렌더 실패 → 4xx).
    text = str(value).strip()
    try:
        int(text)
        return text
    except ValueError:
        float(text)  # 실패하면 여기서 ValueError 가 그대로 전파된다
        return text


@template_filter("sql_ident")
def sql_ident(value) -> str:
    """값을 큰따옴표로 감싼 **SQL 식별자**(컬럼/테이블명)로 만든다.

    내부 ``"`` 는 ``""`` 로 이중화한다. 점(``.``)이 있으면 스키마 한정으로 보고 각 조각을
    따로 인용한다(예: ``public.sales`` → ``"public"."sales"``).
    """
    parts = str(value).split(".")
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)


@template_filter("sql_sign")
def sql_sign(value) -> str:
    """부호 토큰(``+``/``-``)만 허용해 SQL 연산자 자리에 그대로 렌더한다.

    날짜 fan-out 에서 ``current_date() {{ from_date_no_sign | sql_sign }} interval
    {{ from_date_no | sql_num }} day`` 처럼 **연산자 방향을 데이터로** 표현할 때 쓴다
    (Impala ``interval`` 은 절대값만 받으므로 부호는 값이 아니라 연산자로 나가야 한다).
    값이 아니라 SQL 토큰을 직접 찍는 유일한 필터라, ``+``/``-`` 외에는 ``ValueError`` 로
    막아 임의 문자열이 SQL 로 새어 나가지 못하게 한다(요청 스키마의 Literal 과 이중 방어).
    """
    text = str(value).strip()
    if text not in ("+", "-"):
        raise ValueError(f"sign 은 '+' 또는 '-' 만 허용합니다: {value!r}")
    return text


@template_filter("sql_in")
def sql_in(values: Iterable) -> str:
    """리스트를 ``IN (...)`` 안에 넣을 **콤마 구분 리터럴 목록**으로 만든다.

    문자열은 :func:`sql_str` 로 인용하고, 숫자/불리언은 숫자 리터럴로, None 은 NULL 로
    렌더한다. 예: ``['KR', 'US']`` → ``'KR', 'US'`` / ``[1, 2]`` → ``1, 2``.
    빈 목록이면 ``IN ()`` 이 문법 오류이자 "아무것도 매칭 안 됨" 을 뜻하므로 ``NULL`` 을
    돌려준다(``col IN (NULL)`` → 항상 거짓, 안전한 빈 집합 의미).
    """
    rendered = []
    for v in values:
        if v is None:
            rendered.append("NULL")
        elif isinstance(v, bool):
            rendered.append("1" if v else "0")
        elif isinstance(v, (int, float)):
            rendered.append(repr(v))
        else:
            rendered.append(sql_str(v))
    return ", ".join(rendered) if rendered else "NULL"


# ───────────────────────── 도메인 글로벌 함수 ─────────────────────────


@template_global("date_range")
def date_range(start, end, step_days: int = 1, fmt: str = "%Y-%m-%d") -> list[str]:
    """``start`` 부터 ``end`` 까지(양 끝 포함) 날짜 문자열 리스트를 만든다.

    파티션 컬럼이 날짜일 때, 시작/끝만 파라미터로 받아 IN 목록을 자동 생성하는 데 쓴다.
    예: ``WHERE dt IN ( {{ date_range(start_dt, end_dt) | sql_in }} )``.

    인자:
        start, end : ``date``/``datetime`` 또는 ``fmt`` 로 파싱 가능한 문자열.
        step_days  : 간격(일). 기본 1(매일).
        fmt        : 입력 파싱 및 출력 포맷(기본 ``YYYY-MM-DD``).

    반환:
        ``fmt`` 로 포맷된 날짜 문자열 리스트(오름차순). start>end 면 빈 리스트.
    """
    d0 = _as_date(start, fmt)
    d1 = _as_date(end, fmt)
    if step_days <= 0:
        raise ValueError("date_range step_days 는 1 이상이어야 합니다.")
    out: list[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime(fmt))
        cur += _dt.timedelta(days=step_days)
    return out


def _as_date(value, fmt: str) -> _dt.date:
    """``date``/``datetime``/문자열을 ``date`` 로 정규화한다(문자열은 ``fmt`` 로 파싱)."""
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return _dt.datetime.strptime(str(value), fmt).date()


# ───────────────────────── 등록/로딩 ─────────────────────────


def register_builtins() -> None:
    """내장 필터/글로벌을 레지스트리에 채운다(모듈 import 시 자동 호출).

    데코레이터가 정의 시점에 이미 등록하므로, 이 함수는 명시적 재등록/문서화 용도의
    no-op 안전장치다. (import 부작용에 의존하지 않고 호출로도 보장할 수 있게 둔다.)
    """
    # 데코레이터 실행으로 이미 채워져 있음. 여기서는 최소한의 존재만 단언한다.
    assert "sql_in" in FILTERS and "date_range" in GLOBALS


def load_func_modules(module_paths: Iterable[str]) -> None:
    """설정으로 지정한 사용자 함수 모듈들을 import 해 커스텀 함수를 추가 등록한다.

    각 모듈은 임포트되는 것만으로 ``@template_filter``/``@template_global`` 데코레이터가
    실행되어 자기 함수를 이 레지스트리에 등록해야 한다(argus 스타일 플러그인 로딩).

    한 모듈의 import 실패가 전체 기동을 막지 않도록 예외는 로깅만 하고 계속 진행한다
    (그 모듈이 제공하는 함수만 누락되고, 템플릿에서 참조하면 렌더 시점에 실패한다).
    """
    for path in module_paths:
        path = path.strip()
        if not path:
            continue
        try:
            importlib.import_module(path)
            logger.info("템플릿 커스텀 함수 모듈 로드: %s", path)
        except Exception:
            logger.exception("템플릿 커스텀 함수 모듈 로드 실패: %s", path)


register_builtins()
