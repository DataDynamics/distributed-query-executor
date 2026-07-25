"""Coordinator FastAPI 애플리케이션 팩토리.

이 모듈은 분산 쿼리 Coordinator의 HTTP API를 구성하는 진입점이다. 핵심 책임은
다음과 같다.

- **작업 생성 파이프라인**: 클라이언트가 보낸 SQL을 검증·파싱하고(`parser`),
  파티션 컬럼의 `IN` 목록 기준으로 N개의 sub-query로 분할한 뒤(`splitter`),
  각 sub-query를 executor에 라운드로빈 배정하여 비동기로 디스패치한다.
- **Admission control(과부하 보호)**: 동시 실행 슬롯과 대기 큐 용량을 합한 한도를
  초과하는 요청은 즉시 429로 거부한다(자세한 흐름은 `_create_job` 주석 참고).
- **상태/결과 조회 및 취소**: job/task 단위 상태·진행률·결과 조회와 취소 API를 제공.
- **모니터링**: coordinator/executor 헬스·시스템 메트릭, 클러스터 요약, 대시보드.

Coordinator 자신은 쿼리 결과 행을 받지 않는다. 실제 데이터 이동(Impala→Greenplum)은
executor가 수행하고, 여기서는 분배·상태추적·집계만 담당한다.

`create_app()`은 의존성(runner/store/settings)을 주입받는 팩토리로, 테스트에서
가짜 구현을 끼워 넣기 쉽게 설계되어 있다. 모듈 마지막 줄에서 기본 설정으로
싱글턴 `app` 을 생성해 ASGI 서버가 import 할 수 있게 한다.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

import httpx

from core.dbprobe import clamp_limit, run_postgres_select
from core.http_logging import install_http_logging
from core.logging import job_log_context
from core.metrics import collect_system_metrics
from core import s3_stage
from core.timeutil import format_at_fields, now_dt
from core.version import __version__
from core.webassets import mount_static, register_offline_docs
from .dashboard import DASHBOARD_HTML, masked_config
from .config import Settings, settings as default_settings
from .dispatcher import HttpDispatcher, JobRunner, LocalDispatcher
from .executor_status import ExecutorStatusRepository
from .ha import CoordinatorHeartbeat, reconcile_orphaned_jobs
from .history import JobHistoryRepository
from .job_store import JobStore, build_job_store, reconcile_interrupted_jobs
from .models import (
    CreateJobRequest,
    CreateJobResponse,
    DatasourceQueryRequest,
    Job,
    JobStatus,
    QueryExecuteRequest,
    Task,
    TaskStatus,
    new_job_id,
)
from .monitor import HealthMonitor
from .parser import (
    DIALECT,
    QueryValidationError,
    is_row_returning,
    validate_and_parse,
    validate_select_query,
)
from .reservation import ReservationRepository, ReservingLoadView
from .selector import ExecutorSelector, SharedLoadView
from .splitter import SubQuery, split, wrap
from .template import TemplateEngine, TemplateError

logger = logging.getLogger(__name__)


def _assign_executors(count: int, executors: list[str]) -> list[Optional[str]]:
    """`count`개의 task에 executor URL을 라운드로빈으로 배정한다.

    분할된 sub-query를 설정된 executor 목록에 고르게 분산시키기 위한 헬퍼다.
    executor 수보다 task가 많으면 `itertools.cycle` 로 목록을 순환하며 반복 배정한다.

    Args:
        count: 배정 대상 task 개수.
        executors: 후보 executor base URL 목록(설정값).

    Returns:
        task 순서에 1:1 대응하는 URL 리스트. executor 목록이 비어 있으면
        (예: local 모드라 원격 호출이 없을 때) 모두 None 을 채워 반환한다.
    """
    if not executors:
        # local 모드 등 원격 executor가 없는 경우: 배정할 URL이 없으므로 None 으로 채운다.
        return [None] * count
    cycle = itertools.cycle(executors)
    return [next(cycle) for _ in range(count)]


def _query_params_to_dict(params: list) -> dict:
    """``/query-execute`` 의 params 배열([{name, value}])을 렌더용 ``{name: value}`` dict 로 접는다.

    같은 name 이 두 번 오면(모호함) 422 로 거부한다. 타입/필수 검증은 이후 템플릿 엔진의
    ``ParamSpec`` 이 하므로 여기서는 형태(이름 유일성)만 본다.
    """
    values, _ = _split_params(params)
    return values


def _split_params(params) -> tuple[dict, dict]:
    """요청 params 를 ``(값 dict, 부호 dict)`` 로 가른다 — dict/배열 두 형태를 모두 받는다.

    - ``{"region": "KR"}`` (하위 호환, 부호 없음) → ``({"region": "KR"}, {})``
    - ``[{"name": "from_date_no", "value": 7, "sign": "-"}, ...]`` → 값과 부호를 분리

    부호는 값에 적용하지 **않는다**. ``sign`` 은 SQL 연산자의 방향(``- interval``)을
    뜻하므로 값에 다시 붙이면 이중 적용이 된다. 렌더 컨텍스트에는 :func:`_render_params`
    가 ``<name>_sign`` 으로 따로 실어 보낸다. 같은 이름이 두 번 오면 422.
    """
    if params is None:
        return {}, {}
    if isinstance(params, dict):
        return dict(params), {}
    values: dict = {}
    signs: dict = {}
    for item in params:
        # pydantic 모델(QueryParam)로 파싱된 경우와 평범한 dict 둘 다 받는다.
        name = item.name if hasattr(item, "name") else item.get("name")
        value = item.value if hasattr(item, "value") else item.get("value")
        sign = item.sign if hasattr(item, "sign") else item.get("sign")
        if not name:
            raise QueryValidationError("PARAM_NAME_MISSING", "params 항목에 name 이 없습니다.")
        if name in values:
            raise QueryValidationError("DUPLICATE_PARAM", f"중복된 파라미터 이름: {name}")
        values[name] = value
        if sign:
            signs[name] = sign
    return values, signs


def _render_params(values: dict, signs: dict) -> dict:
    """값 dict + 부호 dict → 렌더 컨텍스트(부호는 ``<name>_sign`` 으로 노출).

    템플릿은 ``current_date() {{ from_date_no_sign | sql_sign }} interval
    {{ from_date_no | sql_num }} day`` 처럼 연산자 방향을 데이터로 받는다.
    """
    ctx = dict(values)
    ctx.update({f"{name}_sign": sign for name, sign in signs.items()})
    return ctx


# 템플릿 렌더 시 manifest 기본값과 병합하는 스칼라 필드(요청이 명시하면 요청이 이긴다).
_TEMPLATE_SCALAR_KEYS = (
    "exec_mode", "partition_column", "target_table", "staging_table",
    "write_mode", "split_strategy", "failure_policy", "parallelism",
    "sql_dialect", "strict_validation",
)


# 날짜 fan-out 이 만들 수 있는 task 수 상한. 오타(-10000 등)로 수만 개의 task 가 생겨
# executor 와 메타 저장소를 마비시키는 것을 막는다(1년치 + 윤년 여유).
_MAX_FANOUT_TASKS = 366


def _param_offset(name: str, value, sign: Optional[str]) -> int:
    """(값, 부호) → 오늘 기준 **부호 있는 오프셋(일)**.

    ``sign`` 이 있으면 값은 크기로만 보고 부호는 sign 이 결정한다(``value:7, sign:"-"``
    → ``-7``). ``sign`` 이 없으면 값 자체의 부호를 쓴다(``value:-7`` → ``-7``). 이렇게
    두 표기가 같은 뜻이 되어, 부호가 SQL 에 박힌 쿼리(``- interval 7 day``)와 값에
    부호가 있는 쿼리를 같은 방식으로 다룰 수 있다.
    """
    try:
        num = int(value)
    except (TypeError, ValueError) as exc:
        raise QueryValidationError(
            "TASK_PARAM_NOT_NUMERIC",
            f"task_params 의 '{name}' 는 정수여야 합니다(현재 {value!r}).",
        ) from exc
    if not sign:
        return num
    return -abs(num) if sign == "-" else abs(num)


def _compute_task_offsets(lo: int, hi: int, bound: str) -> list[tuple[int, int]]:
    """구간 ``[lo, hi]`` 를 하루 단위 task 의 ``(시작 오프셋, 끝 오프셋)`` 목록으로 자른다.

    ``bound`` 가 task 하나의 폭을 정한다 — 어느 쪽이 맞는지는 **템플릿의 비교식**이 결정한다:

    - ``point``: ``(d, d)``. ``BETWEEN a AND b``(양끝 포함)나 ``= a`` 처럼 경계를 모두
      포함하는 비교용. task 수 = 날짜 수. 예 ``[-7, 1]`` → 9개.
    - ``pair`` : ``(d, d+1)``. ``>= a AND < b``(반열림) 비교용. 경계 중복 없이 이어붙는다.
      task 수 = 날짜 수 - 1. 예 ``[-7, 1]`` → 8개.

    잘못 고르면 조용히 틀린다(point + 반열림 → 0행, pair + BETWEEN → 경계 날짜 중복
    적재). 그래서 manifest 가 자기 SQL 에 맞는 값을 못 박아 두는 것을 권한다.
    """
    pairs = ([(d, d) for d in range(lo, hi + 1)] if bound == "point"
             else [(d, d + 1) for d in range(lo, hi)])
    if not pairs:
        raise QueryValidationError(
            "TASK_RANGE_EMPTY",
            f"task_params 로 만든 구간 [{lo}, {hi}] 에서 task 가 생기지 않습니다"
            f"(task_bound={bound}).",
        )
    if len(pairs) > _MAX_FANOUT_TASKS:
        raise QueryValidationError(
            "TASK_RANGE_TOO_LARGE",
            f"날짜 fan-out task 수({len(pairs)})가 상한 {_MAX_FANOUT_TASKS} 를 초과했습니다. "
            "구간을 좁혀 여러 job 으로 나누세요.",
        )
    return pairs


def _offset_value_sign(offset: int) -> tuple[int, str]:
    """오프셋 → ``(절대값, 부호)``. Impala ``interval`` 이 절대값만 받으므로 부호를 분리한다."""
    return abs(offset), ("-" if offset < 0 else "+")


def _validate_sign_contract(engine: TemplateEngine, template_id: str, names: list) -> None:
    """task 파라미터를 쓰는 select 조각이 부호 변수(``<name>_sign``)도 쓰는지 검사한다.

    이게 없으면 조용히 틀린다: task 마다 값이 절대값으로 들어가므로, 부호가 SQL 에 고정된
    ``BETWEEN today - interval {{from}} AND today + interval {{to}}`` 템플릿은 ``d=-3``
    task 에서 ``BETWEEN today-3 AND today+3`` (7일치)을 읽는다 → 모든 task 가 겹쳐 append
    적재가 중복된다. 오류 없이 데이터만 틀리는 실패라 접수 시점에 막는다.

    파라미터를 아예 참조하지 않는 템플릿(예: ``task_date`` 로 하루를 고르는 방식)은
    구간 도출에만 쓰는 것이므로 통과시킨다.
    """
    refs = engine.referenced_variables(template_id, "select")
    if not refs:
        return
    missing = [n for n in names if n in refs and f"{n}_sign" not in refs]
    if missing:
        raise QueryValidationError(
            "TEMPLATE_MISSING_SIGN_VAR",
            f"select 조각이 {', '.join(missing)} 를 쓰면서 부호 변수("
            f"{', '.join(n + '_sign' for n in missing)})를 쓰지 않습니다. interval 은 "
            "절대값만 받으므로 부호를 연산자로 내보내야 합니다: "
            "current_date() {{ %s_sign | sql_sign }} interval {{ %s | sql_num }} day"
            % (missing[0], missing[0]),
        )


def _build_fanout(
    req: "CreateJobRequest", engine: Optional[TemplateEngine], today, *, default_dialect: str
) -> list:
    """날짜 fan-out: task_params 가 가리키는 두 끝 → 하루 단위 구간 → **하루 = 1 sub-query**.

    IN 값 분할(validate_and_parse+split)을 우회한다. 동작:
      1) task_params 두 개의 (값, sign)에서 오늘 기준 오프셋 구간을 도출하고,
         task_bound 에 따라 (d, d) 또는 (d, d+1) 쌍 목록으로 자른다.
      2) 부호 계약(``<name>_sign`` 사용 여부)을 검사한다 — 어기면 조용한 중복 적재가 된다.
      3) INSERT/staging 은 날짜 독립이라 대표 구간으로 **1회** 렌더해 req 필드(staging_ddl/
         wrapper_query=INSERT)와 manifest 스칼라 기본값을 채운다.
      4) task 마다 두 파라미터를 그 하루로 좁혀(값=절대값, 부호=방향) SELECT 조각만 렌더한다.
         partition_values 에는 그 날짜를 담는다(관측/표시용). stage_insert 는 append 적재다.

    템플릿 stage_insert 전용이다(select+insert 필요). template_id 미지정/타 exec_mode 는 422.
    """
    if engine is None:
        raise HTTPException(
            status_code=400, detail="템플릿 기능이 비활성화되어 있습니다(template.enabled=false)."
        )
    if not req.template_id:
        raise QueryValidationError(
            "FANOUT_REQUIRES_TEMPLATE", "task_params(날짜 fan-out) 모드는 template_id 가 필요합니다."
        )
    names = list(req.task_params or [])
    if len(names) != 2:
        raise QueryValidationError(
            "TASK_PARAMS_INVALID",
            f"task_params 는 구간의 두 끝을 가리키는 파라미터 이름 2개여야 합니다(현재 {names}).",
        )

    values, signs = _split_params(req.params)
    missing = [n for n in names if n not in values]
    if missing:
        raise QueryValidationError(
            "TASK_PARAMS_INVALID",
            f"task_params 가 가리키는 파라미터가 params 에 없습니다: {', '.join(missing)}",
        )
    offsets = [_param_offset(n, values[n], signs.get(n)) for n in names]
    lo, hi = min(offsets), max(offsets)

    # task_bound 는 요청 > manifest 기본값 순. 렌더 전에 확정해야 구간을 자를 수 있으므로
    # manifest 를 먼저 읽는다(캐시되어 있어 추가 I/O 는 사실상 없다).
    if "task_bound" not in req.model_fields_set:
        req.task_bound = engine.load_manifest(req.template_id).defaults.get(
            "task_bound", req.task_bound
        )
    pairs = _compute_task_offsets(lo, hi, req.task_bound)
    _validate_sign_contract(engine, req.template_id, names)

    def _ctx(pair: tuple[int, int]) -> dict:
        """task 하나(구간 두 끝)의 렌더 컨텍스트 — 값은 절대값, 부호는 <name>_sign 으로."""
        ctx = _render_params(values, signs)
        for name, offset in zip(names, pair):
            magnitude, sign = _offset_value_sign(offset)
            ctx[name] = magnitude
            ctx[f"{name}_sign"] = sign
        # 절대 날짜/오프셋도 노출한다 — `WHERE dt = {{ task_date | sql_str }}` 처럼 interval
        # 대신 날짜 리터럴로 하루를 고르는 템플릿을 같은 fan-out 으로 지원하기 위함.
        ctx["task_offset"] = pair[0]
        ctx["task_offset_end"] = pair[1]
        ctx["task_date"] = (today + timedelta(days=pair[0])).strftime(req.task_date_format)
        ctx["task_date_end"] = (today + timedelta(days=pair[1])).strftime(req.task_date_format)
        return ctx

    request_scalars = {k: getattr(req, k) for k in _TEMPLATE_SCALAR_KEYS if k in req.model_fields_set}
    # INSERT/staging 은 날짜 독립 → 대표 구간으로 1회 렌더(모든 task 공유).
    base = engine.render(req.template_id, _ctx(pairs[0]), request_scalars=request_scalars)
    # manifest 스칼라 기본값 채움(요청이 명시하지 않은 것만). exec_mode 는 아래에서 확정.
    for key, val in base.defaults.items():
        if key == "exec_mode":
            continue
        if key not in req.model_fields_set and hasattr(req, key):
            setattr(req, key, val)
    req.exec_mode = base.exec_mode
    if base.exec_mode not in ("stage_insert", "s3_stage"):
        raise QueryValidationError(
            "FANOUT_REQUIRES_STAGE_INSERT",
            "task_params(날짜 fan-out) 모드는 exec_mode=stage_insert 또는 s3_stage 만 "
            f"지원합니다(현재 {base.exec_mode}).",
        )
    if base.exec_mode == "s3_stage":
        # s3_stage: 외부테이블(=staging)은 executor 가 external_columns 로 생성한다. INSERT 와
        # 외부테이블 컬럼 정의를 job 필드에 싣는다(하루=1 task 마다 자체 S3 왕복, append 적재).
        req.insert_sql = base.insert
        req.external_columns = base.external_columns
    else:
        if base.staging_ddl:
            req.staging_ddl = base.staging_ddl
        req.wrapper_query = base.insert  # stage_insert INSERT → job.insert_sql 로 실린다
    # 필수필드(partition_column) 충족용 표시값. 이 모드는 IN 분할을 안 하므로 실제 분할에는
    # 쓰이지 않는다 — manifest 가 컬럼을 선언했으면 그걸, 아니면 task_params 이름을 보여준다.
    if not req.partition_column:
        req.partition_column = ",".join(names)

    dialect = req.sql_dialect or default_dialect
    sub_queries: list = []
    for pair in pairs:
        ctx = _ctx(pair)
        select_sql = engine.render_query(req.template_id, ctx)
        validate_select_query(select_sql, dialect=dialect)  # 단일 SELECT 방어(다중 문/비-SELECT 차단)
        # 관측용 파티션 값: point 는 그 하루, pair 는 [시작일, 끝일).
        marks = [ctx["task_date"]] if req.task_bound == "point" else [ctx["task_date"], ctx["task_date_end"]]
        sub_queries.append(SubQuery(sql=select_sql, partition_values=marks))
    req.sql = sub_queries[0].sql  # original_sql 기록/필수필드 검증용 대표값
    return sub_queries


def _apply_template(req: "CreateJobRequest", engine: TemplateEngine) -> None:
    """template_id 가 지정된 요청을 렌더링해 SQL 필드/스칼라 기본값을 제자리에서 채운다.

    렌더 결과를 기존 요청 필드에 주입하므로, 이후 파이프라인(parser/splitter/dispatcher)은
    raw-SQL 요청과 완전히 동일하게 동작한다. 병합 원칙:

    - 스칼라(partition_column/target_table/write_mode 등): **요청이 명시한 값이 우선**이고,
      명시하지 않은 필드만 manifest 기본값으로 채운다(``model_fields_set`` 로 구분).
    - exec_mode: 요청 명시값 > manifest 기본 > 'copy' 순으로 엔진이 결정한 값으로 확정한다.
    - SQL 조각: exec_mode 가 요구하는 필드에 렌더 결과를 넣는다. stage_insert 는 관례상
      INSERT 를 wrapper_query 에 싣고(executor 계약), local_stage 는 insert_sql 에 싣는다.
    """
    # 요청이 명시한 스칼라만 추린다(model_fields_set). manifest 기본값을 이기고, 렌더
    # 컨텍스트에도 노출되어 템플릿이 effective target_table 등을 참조할 수 있게 한다.
    request_scalars = {
        k: getattr(req, k) for k in _TEMPLATE_SCALAR_KEYS if k in req.model_fields_set
    }
    # params 는 dict/배열 두 형태를 받는다. 배열의 sign 은 값에 적용하지 않고 <name>_sign
    # 으로 따로 노출한다(SQL 연산자 방향이지 값의 부호가 아니므로).
    values, signs = _split_params(req.params)
    result = engine.render(
        req.template_id, _render_params(values, signs), request_scalars=request_scalars
    )

    # 요청이 명시하지 않은 스칼라만 manifest 기본값으로 채운다(exec_mode 는 아래에서 확정).
    for key, val in result.defaults.items():
        if key == "exec_mode":
            continue
        if key not in req.model_fields_set and hasattr(req, key):
            setattr(req, key, val)
    req.exec_mode = result.exec_mode

    # 렌더된 SQL 조각 주입.
    req.sql = result.select
    if result.exec_mode in ("copy", "statement"):
        if result.wrapper:
            req.wrapper_query = result.wrapper
    elif result.exec_mode == "stage_insert":
        if result.staging_ddl:
            req.staging_ddl = result.staging_ddl
        req.wrapper_query = result.insert  # stage_insert 는 wrapper_query 를 INSERT 로 사용
    elif result.exec_mode == "local_stage":
        if result.staging_ddl:
            req.staging_ddl = result.staging_ddl
        req.insert_sql = result.insert
        req.external_columns = result.external_columns
    elif result.exec_mode == "s3_stage":
        # s3_stage: 외부테이블(=staging)은 executor 가 external_columns 로 생성하므로 staging_ddl
        # 은 쓰지 않는다. INSERT(external→target)와 외부테이블 컬럼 정의만 주입한다.
        req.insert_sql = result.insert
        req.external_columns = result.external_columns


def _request_fingerprint(req: "CreateJobRequest") -> str:
    """요청 본문의 안정적 지문(sha256 hex).

    같은 ``Idempotency-Key`` 를 **다른 본문**으로 재사용했는지 감지하는 데 쓴다. 키를
    쓸 때만 계산하며(비용 절약), 키가 없으면 호출하지 않는다. dict 를 정렬 직렬화해
    필드 순서에 무관하게 같은 요청이면 같은 지문이 나오도록 한다.
    """
    body = req.model_dump(mode="json")
    # params 배열은 순서만 다른 같은 요청이 다른 지문이 되지 않도록 이름순으로 정규화한다
    # (dict 는 sort_keys 가 처리). 순서가 의미를 갖지 않는 이름-값 목록이라 안전하다.
    if isinstance(body.get("params"), list):
        body["params"] = sorted(body["params"], key=lambda p: str(p.get("name", "")))
    canonical = json.dumps(body, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _idempotent_replay_response(job_id: str) -> JSONResponse:
    """멱등 재생 응답: 기존 job_id 를 200 으로 돌려주고 재생임을 헤더로 표시한다.

    새 job 생성은 202 인데, 재생은 새로 만든 게 아니므로 200 + ``Idempotency-Replayed: true``
    헤더로 구분한다(클라이언트가 '이미 접수된 것'임을 알 수 있게).
    """
    return JSONResponse(
        status_code=200,
        content={"job_id": job_id},
        headers={"Idempotency-Replayed": "true"},
    )


def create_app(
    runner: Optional[JobRunner] = None,
    store: Optional[JobStore] = None,
    settings: Optional[Settings] = None,
) -> FastAPI:
    """FastAPI 앱을 조립하여 반환하는 팩토리.

    의존성을 인자로 주입받아 라우트를 등록한 `FastAPI` 인스턴스를 만든다. 인자를
    생략하면 기본 설정(`default_settings`)을 바탕으로 store와 runner를 자동 구성하므로,
    운영 기동 시에는 인자 없이 호출하고 테스트에서는 가짜 구현을 주입할 수 있다.

    Args:
        runner: job을 실제로 실행하는 디스패처(JobRunner). None 이면 executor_mode 에
            따라 LocalDispatcher / HttpDispatcher 를 자동 선택한다.
        store: job 상태 저장소. None 이면 설정 기반으로 빌드한다(인메모리/공유 DB 등).
        settings: 환경설정. None 이면 모듈 전역 기본 설정을 사용한다.

    Returns:
        라우트·예외 핸들러·lifespan 이 모두 등록된 FastAPI 앱.
    """
    settings = settings or default_settings
    store = store or build_job_store(settings)
    monitor = HealthMonitor(settings)
    # 쿼리 템플릿 엔진: 켜져 있으면 1개 생성해 요청 렌더링에 재사용(단일 워커라 in-process 안전).
    template_engine = TemplateEngine(settings) if settings.template_enabled else None
    started_at = now_dt()
    start_monotonic = time.monotonic()
    # self-report 모드면 executor 상태를 공유 테이블에서 읽는다(coordinator 폴링/기록 안 함)
    status_repo = (
        ExecutorStatusRepository(settings.history_db_dsn, settings.executor_status_table)
        if settings.executor_self_report and settings.history_db_dsn
        else None
    )

    # 헬스/부하 기반 executor 선택(Phase 1·2·3). round_robin(기본)이 아니면 부하 뷰를 보고
    # 초기 배정과 failover 순서를 헬스 기반으로 정한다(p2c 권장 — HA 분산 스탬피드 방지).
    _select_policy = getattr(settings, "executor_select", "round_robin")
    selection_enabled = _select_policy in ("least_loaded", "p2c")
    selector = ExecutorSelector(policy=_select_policy) if selection_enabled else None
    # 부하 뷰 소스(Phase 3): HA(self_report 공유 테이블, URL 키) vs 단일(monitor 폴링).
    #   auto    → status_repo 있으면 공유 테이블, 없으면 monitor
    #   self_report → 공유 테이블(없으면 monitor 폴백)
    #   monitor → 항상 monitor 폴링
    _health_source = getattr(settings, "executor_health_source", "auto")
    _use_shared = (
        status_repo is not None and _health_source in ("auto", "self_report")
    )
    if selection_enabled:
        load_view = SharedLoadView(status_repo.read_all) if _use_shared \
            else SharedLoadView(monitor.snapshot)
    else:
        load_view = None
    # 공유 예약(Phase 3-B, 엄격 균형): 켜져 있고 공유 DB 가 있으면 부하 뷰를 예약 합산 뷰로
    # 감싸고, dispatch 중 task 를 예약/해제한다(active_tasks + 예약 = effective load).
    reservations = None
    if selection_enabled and getattr(settings, "executor_reservation", False) \
            and settings.history_db_dsn:
        reservations = ReservationRepository(
            settings.history_db_dsn, settings.coordinator_id, table=settings.reservation_table
        )
        load_view = ReservingLoadView(load_view, reservations, settings.reservation_ttl_s)
    # 죽은 coordinator 소유 job 정합(Phase 3-C): 공유 postgres store + heartbeat 일 때만.
    heartbeat = None
    if getattr(settings, "store_backend", "memory") == "postgres" and settings.history_db_dsn \
            and getattr(settings, "orphan_reconcile_interval_s", 0) > 0:
        heartbeat = CoordinatorHeartbeat(
            settings.history_db_dsn, settings.coordinator_id, table=settings.coordinator_status_table
        )
    # 이 coordinator 가 기동 후 executor 별로 배정한 누적 task 수(배정 분포 관측용, Phase 2).
    # 멀티 coordinator 에선 인스턴스별 카운트다(전역 분포는 각 인스턴스 합산).
    assign_counts: dict = {}
    if runner is None:
        # executor_mode 에 따라 디스패처 선택: local 은 in-process 직접 실행,
        # 그 외(http)는 원격 executor에 HTTP로 task를 분배한다.
        runner = (
            LocalDispatcher(settings, store=store)
            if settings.executor_mode == "local"
            else HttpDispatcher(settings, store=store, load_view=load_view,
                                selector=selector, reservations=reservations)
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # 앱 수명주기 훅: 기동 시 (1) 재기동 정합 — 영속 저장소(file/postgres)에서 비종료로
        # 남은 job 을 '중단됨(FAILED)'으로 표시해 retry 로 재개 가능하게 한다(인메모리면 no-op).
        # (2) 헬스 모니터를 켠다. status_repo 가 있으면 executor self-report 모드라 폴링 모니터는
        # 보통 띄우지 않지만, 헬스 기반 executor 선택이 켜져 있으면 선택용 부하 뷰를 위해 폴링한다.
        reconcile_interrupted_jobs(store)
        # 모니터는 (a) self-report 가 아니어서 /executors 용 폴링이 필요하거나,
        # (b) 헬스 기반 선택이 켜졌고 그 부하 소스가 monitor 일 때만 띄운다.
        # HA self-report(_use_shared) 면 공유 테이블을 읽으므로 폴링이 불필요하다.
        need_monitor = status_repo is None or (selection_enabled and not _use_shared)
        if need_monitor:
            await monitor.start()
        # HA: coordinator heartbeat + 죽은 coordinator 소유 job 정합 백그라운드 루프.
        ha_task = asyncio.create_task(_ha_loop()) if heartbeat is not None else None
        try:
            yield
        finally:
            if ha_task is not None:
                ha_task.cancel()
                try:
                    await ha_task
                except asyncio.CancelledError:
                    pass
            if need_monitor:
                await monitor.stop()

    async def _ha_loop() -> None:
        # 주기적으로 자기 생존을 heartbeat 하고, 죽은 coordinator 소유의 비종료 job 을
        # FAILED 로 정합한다(블로킹 DB 작업은 to_thread 로 이벤트 루프 비차단). 한 번의 오류가
        # 루프를 멈추지 않도록 예외는 로깅만 한다.
        interval = settings.orphan_reconcile_interval_s
        while True:
            try:
                await asyncio.to_thread(heartbeat.beat)
                await asyncio.to_thread(
                    reconcile_orphaned_jobs, store, heartbeat, settings.coordinator_stale_s
                )
            except Exception:
                logger.exception("HA heartbeat/reconcile 루프 오류")
            await asyncio.sleep(interval)

    app = FastAPI(
        title="Distributed Query Coordinator",
        version=__version__,
        description=(
            "Impala `SELECT` 쿼리를 파티션 컬럼의 `IN` 목록 기준으로 N분할하여 여러 "
            "executor에 분배하고, 각 executor가 Greenplum에 병렬 적재하도록 조율한다.\n\n"
            "- 검증/분할/디스패치/상태추적, executor 헬스 모니터링\n"
            "- Swagger UI: `/docs`, ReDoc: `/redoc`, OpenAPI 스키마: `/openapi.json`"
        ),
        # 에어갭: 기본 docs 라우트는 외부 CDN 을 참조하므로 끄고, 아래에서
        # register_offline_docs 로 내장 에셋 기반 /docs·/redoc 를 다시 등록한다.
        docs_url=None,
        redoc_url=None,
        openapi_tags=[
            {"name": "Jobs", "description": "쿼리 작업 생성·조회·결과·태스크 상세"},
            {"name": "Monitoring", "description": "헬스 체크, 시스템 메트릭, executor 상태"},
        ],
        lifespan=lifespan,
    )
    # 에어갭: 내장 정적 에셋(/assets)과 오프라인 docs(/docs·/redoc)를 등록한다.
    mount_static(app)
    register_offline_docs(app)
    # HTTP 요청/응답 DEBUG 로깅(로그 레벨이 DEBUG 일 때만 자동 기록). 잡음 경로는 기본 제외.
    install_http_logging(app, settings)
    # 핸들러들이 클로저로 직접 참조하지만, 미들웨어/디버깅/테스트에서 꺼내 쓸 수
    # 있도록 핵심 의존성을 app.state 에도 보관해 둔다.
    app.state.store = store
    app.state.runner = runner
    app.state.settings = settings
    app.state.monitor = monitor
    app.state.status_repo = status_repo
    # 헬스 기반 선택 관련(관측/테스트용). 선택 비활성이면 selector/load_view 는 None.
    app.state.selector = selector
    app.state.load_view = load_view
    app.state.assign_counts = assign_counts
    app.state.template_engine = template_engine

    @app.exception_handler(QueryValidationError)
    async def _validation_handler(request: Request, exc: QueryValidationError):
        # 검증/분할 단계에서 던지는 도메인 예외를 일관된 422 JSON 으로 변환한다.
        # (error_code 를 본문에 실어 클라이언트가 실패 원인을 분기할 수 있게 한다.)
        # 왜 거부됐는지 서버 로그에도 남긴다 — 이게 없으면 422 사유가 클라이언트에만 있어
        # 서버 측 진단이 불가능하다(어느 경로에서 어떤 코드로 걸렸는지 파악).
        logger.warning("요청 검증 실패 [%s] %s (%s)", exc.code, exc.message, request.url.path)
        return JSONResponse(
            status_code=422,
            content={"error_code": exc.code, "message": exc.message},
        )

    @app.exception_handler(TemplateError)
    async def _template_handler(request: Request, exc: TemplateError):
        # 템플릿 로드/검증/렌더 실패도 검증 오류와 동일하게 422 + error_code 로 변환한다.
        logger.warning("템플릿 오류 [%s] %s (%s)", exc.code, exc.message, request.url.path)
        return JSONResponse(
            status_code=422,
            content={"error_code": exc.code, "message": exc.message},
        )

    @app.post(
        "/jobs",
        response_model=CreateJobResponse,
        status_code=202,
        tags=["Jobs"],
        summary="쿼리 작업 생성",
        description="SQL을 검증·분할하여 작업을 생성하고 비동기로 디스패치한다(202). "
        "dry_run=true 면 executor 호출 없이 생성된 쿼리만 반환한다(200). "
        "검증 실패 시 422(error_code 포함).",
    )
    def create_job(req: CreateJobRequest, background: BackgroundTasks, request: Request):
        # POST /jobs 의 얇은 진입 래퍼.
        # 실제 처리에 앞서 job_id 를 먼저 발급하고 로깅 컨텍스트에 바인딩하여,
        # 검증 실패로 작업이 저장되지 않더라도 이 요청에서 찍히는 모든 로그에
        # [job_id] 가 일관되게 붙도록 한다(요청 추적성 확보).
        job_id = new_job_id()
        # 요청 멱등: 클라이언트가 Idempotency-Key 헤더를 주면 중복 제출(타임아웃 재시도 등)을
        # 흡수해 같은 job 을 돌려준다(중복 적재 방지). 미지정이면 기존 동작 그대로.
        idempotency_key = request.headers.get("Idempotency-Key") or None
        with job_log_context(job_id):
            return _create_job(req, background, job_id, idempotency_key)

    def _create_job(req: CreateJobRequest, background: BackgroundTasks, job_id: str,
                    idempotency_key: Optional[str] = None):
        """작업 생성 본체: 검증 → 분할 → (dry-run 분기) → admission → 디스패치.

        흐름 요약:
        1. SQL을 동기로 검증·파싱하고 parallelism 만큼 sub-query로 분할한다. 이때의
           오류(QueryValidationError)는 백그라운드로 넘기기 전에 즉시 클라이언트에
           422로 반환된다.
        2. exec_mode 에 따라 필수 필드와 래퍼 쿼리 적용을 검증한다.
        3. dry_run 이면 executor 호출 없이 생성될 쿼리 계획만 200으로 반환한다(미저장).
        4. admission control 로 동시 실행/대기 용량을 확인하고, 초과면 429로 거부한다.
        5. 수용되면 Job/Task 를 store 에 저장하고 background 로 runner.run 을 예약한 뒤
           202(job_id)를 반환한다.

        요청 멱등(idempotency_key)이 있으면 (0) 먼저 같은 키의 job 이 있는지 확인해 재검증
        없이 바로 돌려주고(200 재생), 없으면 아래 흐름을 타되 저장 단계에서 키를 원자적으로
        선점한다(동시 제출 경쟁에서도 job 하나만 생성).
        """
        # (0) 요청 멱등 사전 확인: 같은 키의 job 이 이미 있으면 재검증/분할 없이 바로 재생한다.
        # 지문은 이 시점(템플릿 적용 전, 클라이언트 원본 요청)에서 계산해야 재시도 간 일관된다.
        fingerprint = _request_fingerprint(req) if idempotency_key else None
        if idempotency_key and not req.dry_run:
            existing = store.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                if fingerprint and existing.request_fingerprint and \
                        existing.request_fingerprint != fingerprint:
                    raise HTTPException(
                        status_code=409,
                        detail="같은 Idempotency-Key 로 다른 요청 본문이 접수되었습니다.",
                    )
                logger.info("멱등 재생: 기존 job %s 반환(Idempotency-Key 사전확인)", existing.job_id)
                return _idempotent_replay_response(existing.job_id)

        # 날짜 fan-out 모드: task_params 가 있으면 IN 값 분할 대신 '하루 1 task' 로 펼친다
        # (_build_fanout 이 구간 도출 + task 별 SELECT 렌더 + req 필드 주입까지 끝낸다).
        fanout = bool(req.task_params)
        if fanout:
            sub_queries = _build_fanout(
                req, template_engine, now_dt().date(),
                default_dialect=settings.query_default_dialect,
            )
        # 템플릿 모드: SQL 을 클라이언트가 보내는 대신, 서버 템플릿을 params 로 렌더링해
        # sql/staging_ddl/insert_sql/external_columns/wrapper_query 와 스칼라 기본값을 채운다.
        # 렌더 결과는 아래 raw-SQL 경로와 동일한 필드에 주입되므로 이후 흐름은 바뀌지 않는다.
        elif req.template_id:
            if template_engine is None:
                raise HTTPException(
                    status_code=400,
                    detail="템플릿 기능이 비활성화되어 있습니다(template.enabled=false).",
                )
            _apply_template(req, template_engine)  # TemplateError → 422 예외 핸들러

        # 공통 필수 필드 검증(raw/템플릿 공통). 템플릿 모드는 렌더/병합 후 채워졌어야 한다.
        _missing = [n for n in ("sql", "partition_column", "target_table") if not getattr(req, n)]
        if _missing:
            raise QueryValidationError(
                "MISSING_REQUIRED_FIELDS",
                f"필수 필드가 비어 있습니다: {', '.join(_missing)}. "
                "raw 모드는 요청에, 템플릿 모드는 요청 또는 manifest 기본값으로 제공하세요.",
            )

        logger.info(
            "쿼리 실행 요청 수신 (template=%s, partition=%s, target=%s, exec_mode=%s, dry_run=%s)",
            req.template_id, req.partition_column, req.target_table, req.exec_mode, req.dry_run,
        )
        # 동기 검증 + 분할: 비동기 디스패치 전에 마치므로, 여기서 발생하는 오류는
        # 백그라운드 작업으로 넘어가지 않고 즉시(이 요청-응답 사이클에서) 클라이언트에
        # 반환된다. dialect 가 지정되지 않으면 설정의 기본 방언으로 파싱한다.
        # fan-out 모드는 _build_fanout 이 이미 날짜별 sub_queries 를 만들었으므로 IN 분할을 건너뛴다.
        if not fanout:
            dialect = req.sql_dialect or settings.query_default_dialect
            parsed = validate_and_parse(
                req.sql,
                req.partition_column,
                dialect=dialect,
                strict=req.strict_validation,
            )
            # 파티션 컬럼 기준으로 parallelism 개의 sub-query로 분할(전략에 따라 분배 방식 결정).
            sub_queries = split(parsed, req.parallelism, req.split_strategy)

        if req.exec_mode == "stage_insert":
            # stage_insert: Impala SELECT 결과를 Greenplum staging 테이블에 적재한 뒤
            # INSERT 로 최종 테이블에 반영하는 2단계 모드. 분할된 SELECT(sub-query)는
            # 변형하지 않고 그대로 두며, staging DDL/테이블/INSERT 문은 task 에 함께 실어
            # executor 로 전달한다. staging_table(적재 대상)과 wrapper_query(INSERT)는 단계가
            # 성립하려면 반드시 필요하므로 강제한다. staging_ddl 은 선택이며, 비어 있으면
            # executor 가 테이블 생성을 건너뛰고 이미 존재하는 staging_table 을 그대로 쓴다.
            if not (req.staging_table and req.wrapper_query):
                raise QueryValidationError(
                    "STAGE_INSERT_REQUIRES_FIELDS",
                    "stage_insert 모드는 staging_table 과 wrapper_query(INSERT) 가 필요합니다. "
                    "staging_ddl 은 선택이며, 없으면 기존 staging_table 을 사용합니다(생성 건너뜀).",
                )
        elif req.exec_mode == "local_stage":
            # local_stage: 각 executor 가 세그먼트 호스트 로컬 CSV 로 export(Phase 1) →
            # coordinator 가 GP file:// 외부테이블로 적재(Phase 2). 분할된 SELECT 는 그대로
            # export 하므로 wrapper 를 쓰지 않는다(아래 wrapper 분기로 내려가지 않도록 여기서
            # 분기를 끊는다). 외부테이블 컬럼 정의·staging·최종 INSERT 는 요청자가 명시한다.
            if not (req.staging_table and req.external_columns and req.insert_sql):
                raise QueryValidationError(
                    "LOCAL_STAGE_REQUIRES_FIELDS",
                    "local_stage 모드는 staging_table, external_columns, insert_sql 가 "
                    "필요합니다. staging_ddl 은 선택입니다.",
                )
        elif req.exec_mode == "s3_stage":
            # s3_stage: 각 executor 가 Impala 결과를 로컬 CSV → S3 업로드 → PXF 외부테이블 →
            # target INSERT → S3 정리로 자체 완결한다(local_stage 형제, 세그먼트 co-locate 불필요).
            # 외부테이블이 staging 을 겸하며, 그 이름(staging_table)·컬럼(external_columns)·
            # 최종 INSERT(insert_sql)를 요청자가 명시한다(래퍼 분기로 내려가지 않게 여기서 끊는다).
            if not (req.staging_table and req.external_columns and req.insert_sql):
                raise QueryValidationError(
                    "S3_STAGE_REQUIRES_FIELDS",
                    "s3_stage 모드는 staging_table, external_columns, insert_sql 가 필요합니다.",
                )
        elif req.wrapper_query:
            # stage_insert 가 아니면서 래퍼 쿼리가 주어진 경우: 분할된 각 sub-query를
            # 래퍼의 placeholder 자리에 치환해 최종 실행 SQL을 만든다(예: 집계/CTE로 감싸기).
            if req.wrapper_placeholder not in req.wrapper_query:
                # placeholder 가 없으면 어디에 sub-query를 끼워 넣을지 알 수 없으므로 거부.
                raise QueryValidationError(
                    "WRAPPER_PLACEHOLDER_MISSING",
                    f"wrapper_query 에 placeholder '{req.wrapper_placeholder}' 가 없습니다.",
                )
            # copy(STDIN) 모드는 결과 행을 fetch 해서 COPY 로 흘려보내므로, 최종 래핑 결과가
            # 반드시 행을 반환하는 SELECT여야 한다. INSERT 등 비-SELECT 래퍼는 행을 돌려주지
            # 않으므로 statement/stage_insert 모드를 써야 한다.
            if req.exec_mode == "copy":
                # 더미 sub-query("(SELECT 1)")로 래핑한 결과를 파싱해 행 반환 여부만 검사한다
                # (실제 sub-query 내용과 무관하게 래퍼 구조만 검증하면 충분하므로).
                probe = wrap("(SELECT 1)", req.wrapper_query, req.wrapper_placeholder)
                if not is_row_returning(probe, dialect):
                    raise QueryValidationError(
                        "COPY_WRAPPER_NOT_SELECT",
                        "copy 모드의 wrapper_query 는 행을 반환하는 SELECT 여야 합니다. "
                        "INSERT 등으로 감싸려면 exec_mode=statement 또는 stage_insert 를 사용하세요.",
                    )
            # 검증을 통과하면 모든 sub-query를 래퍼로 감싸 최종 SQL로 교체한다.
            for sq in sub_queries:
                sq.sql = wrap(sq.sql, req.wrapper_query, req.wrapper_placeholder)

        # 분할된 task 에 executor 배정. 헬스 기반 선택(least_loaded/p2c)이 켜져 있으면
        # 부하 뷰를 보고 한가한 노드에 분산 배정(같은 job 의 task 가 한 노드로 몰리지 않도록
        # 임시 부하 가산). 그 외에는 기존 라운드로빈. local 모드(executors 없음)면 전부 None.
        if selection_enabled and load_view is not None and settings.executors:
            # 배정용 selector 는 요청마다 새로 만든다(동기 핸들러는 스레드풀에서 돌아
            # 디스패처의 selector 와 상태를 공유하지 않게 — 스레드 안전).
            executor_urls = ExecutorSelector(policy=_select_policy).assign(
                len(sub_queries), list(settings.executors), load_view.by_url()
            )
        else:
            executor_urls = _assign_executors(len(sub_queries), settings.executors)
        for _u in executor_urls:
            if _u:
                assign_counts[_u] = assign_counts.get(_u, 0) + 1

        # dry-run: executor 호출·작업 저장 없이 "이렇게 실행될 것"이라는 계획만 만들어
        # 200으로 반환한다. 실제 부작용이 없으므로 admission 체크 이전에 빠르게 빠져나간다.
        if req.dry_run:
            plan = []
            for idx, (sq, url) in enumerate(zip(sub_queries, executor_urls), 1):
                entry = {
                    "executor_url": url,
                    "partition_values": sq.partition_values,
                    "sub_query": sq.sql,
                }
                logger.info(
                    "[dry-run] task#%d (exec_mode=%s) sub_query=%s",
                    idx, req.exec_mode, sq.sql,
                )
                if req.exec_mode == "stage_insert":
                    entry["staging_table"] = req.staging_table
                    entry["staging_ddl"] = req.staging_ddl
                    entry["insert_sql"] = req.wrapper_query
                    logger.info("[dry-run] task#%d staging_ddl=%s", idx, req.staging_ddl)
                    logger.info("[dry-run] task#%d insert_sql=%s", idx, req.wrapper_query)
                elif req.exec_mode == "s3_stage":
                    entry["staging_table"] = req.staging_table
                    entry["external_columns"] = req.external_columns
                    entry["insert_sql"] = req.insert_sql
                    logger.info("[dry-run] task#%d external_columns=%s insert_sql=%s",
                                idx, req.external_columns, req.insert_sql)
                plan.append(entry)
            return JSONResponse(
                status_code=200,
                content={
                    "dry_run": True,
                    "exec_mode": req.exec_mode,
                    "partition_column": req.partition_column,
                    "target_table": req.target_table,
                    "task_count": len(plan),
                    "tasks": plan,
                },
            )

        # ── Admission control(과부하 보호) ──
        # 동시 실행 슬롯 + 대기 큐 상한을 합한 용량을 기준으로 수용 여부를 결정한다.
        # 의도적으로 "거부는 여기서, 대기는 runner 안에서" 로 역할을 나눈다:
        #   - 정상 부하: try_admit() 통과 → 작업은 PENDING 으로 저장되고, runner.run 내부의
        #     슬롯 세마포어에서 자연스럽게 줄을 서다(큐잉) 슬롯이 나면 RUNNING 으로 전이.
        #   - 폭주: 실행+대기 용량(capacity)을 넘는 요청만 try_admit() 이 False 를 돌려
        #     아래에서 429 로 즉시 거부 → 무한정 쌓이는 PENDING 으로 인한 메모리/다운스트림
        #     과부하를 차단한다.
        # runner가 admission 속성을 갖지 않을 수도 있으므로(테스트용 더미 등) getattr 로 방어.
        admission = getattr(runner, "admission", None)
        if admission is not None and not admission.try_admit():
            logger.warning(
                "동시 job 한도 초과로 거부 (in-flight=%s, capacity=%s)",
                admission.inflight, admission.capacity,
            )
            raise HTTPException(
                status_code=429,
                detail=(
                    f"동시 실행/대기 job 한도 초과(capacity={admission.capacity}). "
                    "잠시 후 재시도하세요."
                ),
                headers={"Retry-After": "5"},
            )
        # try_admit() 이 성공해 in-flight 카운트를 1 점유한 상태다. 이후 background 로
        # run 이 예약되면 그 안에서 슬롯 반납이 보장되지만, 예약 직전에 예외가 나면
        # run 이 영영 호출되지 않아 슬롯이 누수되므로 except 에서 직접 반납한다(아래 참고).
        try:
            job = Job(
                job_id=job_id,
                original_sql=req.sql,
                partition_column=req.partition_column,
                target_table=req.target_table,
                write_mode=req.write_mode,
                parallelism=req.parallelism,
                split_strategy=req.split_strategy,
                failure_policy=req.failure_policy,
                username=req.username,
                exec_mode=req.exec_mode,
                pre_delete=req.pre_delete,
                staging_table=req.staging_table,
                staging_ddl=req.staging_ddl,
                insert_sql=(
                    req.insert_sql if req.exec_mode in ("local_stage", "s3_stage")
                    else req.wrapper_query if req.exec_mode == "stage_insert"
                    else None
                ),
                impala_query_options=req.impala_query_options,
                template_id=req.template_id,
                template_params=(_split_params(req.params)[0] if req.template_id else None),
                external_columns=req.external_columns,
                export_local_dir=req.export_local_dir,
                csv_delimiter=req.csv_delimiter,
                csv_null=req.csv_null,
                csv_quote=req.csv_quote,
                status=JobStatus.SPLITTING,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
            )
            # task 별 out_path 확정(모드에 따라 의미가 다르다):
            #  - local_stage: 로컬 CSV 경로({local_dir}/{job_id}/f{idx}.csv) — executor write 대상
            #    이자 coordinator 의 file:// URI 조립 근거.
            #  - s3_stage: S3 객체 키({prefix}/{job_id}/{task_id}.csv) — executor 업로드 대상.
            #    모든 키가 {prefix}/{job_id}/ 아래 모여, Phase 2 가 그 디렉터리 하나를 외부테이블로 읽는다.
            _local_dir = (
                (req.export_local_dir or settings.stage_local_dir)
                if req.exec_mode == "local_stage" else None
            )

            def _out_path(idx: int, task: Task) -> Optional[str]:
                if _local_dir:
                    return f"{_local_dir}/{job.job_id}/f{idx}.csv"
                if req.exec_mode == "s3_stage":
                    return s3_stage.s3_object_key(settings.s3_prefix, job.job_id, task.task_id)
                return None

            tasks = []
            for idx, (sq, url) in enumerate(zip(sub_queries, executor_urls)):
                t = Task(
                    job_id=job.job_id,
                    executor_url=url,
                    sub_query=sq.sql,
                    partition_values=sq.partition_values,
                )
                t.out_path = _out_path(idx, t)  # task_id 발급 후(s3 키가 task_id 를 씀) 채운다
                tasks.append(t)
            job.tasks = tasks
            # 분할 결과를 Task 목록으로 펼쳐 Job 에 담고 저장소에 등록한다.
            # 멱등 키가 있으면 원자적 선점(claim_and_add): 사전확인을 통과한 동시 요청 둘이
            # 여기서 만나도 job 은 하나만 생성되고, 진 쪽은 기존 job 을 돌려받는다.
            if idempotency_key:
                winner = store.claim_and_add(job)
            else:
                store.add(job)
                winner = None
            if winner is not None:
                # 같은 키로 이미 다른 요청이 job 을 만들었다(동시 제출 경쟁).
                if fingerprint and winner.request_fingerprint and \
                        winner.request_fingerprint != fingerprint:
                    # 409 는 아래 except 가 슬롯을 반납한다(중복 반납 방지 위해 여기서 release 안 함).
                    raise HTTPException(
                        status_code=409,
                        detail="같은 Idempotency-Key 로 다른 요청 본문이 접수되었습니다.",
                    )
                if admission is not None:
                    admission.release()  # 점유한 실행 슬롯 반납 — 우리는 run 하지 않는다
                logger.info("멱등 재생: 기존 job %s 반환(claim 경쟁)", winner.job_id)
                return _idempotent_replay_response(winner.job_id)

            logger.info(
                "job %s 생성: %d개 sub-query로 분할 (partition=%s, target=%s)",
                job.job_id,
                len(job.tasks),
                req.partition_column,
                req.target_table,
            )
            # 응답을 막지 않도록 실제 실행은 백그라운드로 예약한다. run 내부에서 PENDING
            # 대기→RUNNING 전이와 종료 시 슬롯 반납까지 책임진다.
            background.add_task(runner.run, job)
        except Exception:
            # try_admit() 으로 점유한 슬롯을 보상 반납한다. 여기 도달했다는 것은
            # run 이 예약되지 못했다는 뜻이므로(=정상 경로의 release 가 실행되지 않음),
            # 직접 반납하지 않으면 in-flight 카운트가 영구히 새어 용량이 줄어든다.
            if admission is not None:
                admission.release()
            raise
        # 202 Accepted: 접수만 확정됐을 뿐 실행은 비동기로 진행된다. 진행 상황은
        # 반환된 job_id 로 /jobs/{job_id} 등에서 폴링한다.
        return CreateJobResponse(job_id=job.job_id)

    @app.get("/jobs/{job_id}", tags=["Jobs"], summary="작업 상태 조회(태스크 포함)")
    def get_job(job_id: str):
        # 단일 job의 상태 + 태스크 목록 요약을 반환. 없으면 404.
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return format_at_fields(job.status_view())

    @app.get(
        "/jobs/{job_id}/status",
        tags=["Jobs"],
        summary="작업 진행 상태(진행률) 조회",
        description="job_id 로 현재 상태/진행률을 조회한다(태스크 목록 제외, 경량).",
    )
    def get_job_progress(job_id: str):
        # 폴링에 적합한 경량 응답: 태스크 목록 없이 상태/진행률만 돌려준다.
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return format_at_fields(job.progress_view())

    @app.post(
        "/jobs/{job_id}/cancel",
        tags=["Jobs"],
        summary="작업 취소",
        description="진행 중인 작업을 취소한다. 각 executor에 취소를 전파하고 job을 "
        "CANCELLED 로 표시한다. 이미 종료된 작업은 409.",
    )
    async def cancel_job(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        # 이미 종료(완료/실패/취소)된 작업은 되돌릴 수 없으므로 409로 거절한다.
        if job.status in (JobStatus.DONE, JobStatus.FAILED, JobStatus.CANCELLED):
            raise HTTPException(
                status_code=409,
                detail=f"이미 종료된 작업입니다(status={job.status.value}).",
            )
        # 취소는 주요 이벤트이므로 API 수신 시점을 남긴다(누가 언제 어떤 상태에서 눌렀는지).
        logger.info("작업 취소 요청 수신 job=%s (현재 status=%s)", job_id, job.status.value)
        # 멀티 coordinator 환경 대비: 공유 store 에 취소 플래그를 남겨,
        # 이 작업을 실제로 실행 중인(=소유한) 다른 coordinator의 runner도 폴링 중에
        # 취소를 감지하도록 한다. 로컬 플래그도 함께 세운다.
        store.request_cancel(job_id)
        job.cancel_requested = True
        # 이 coordinator가 소유한 경우엔 즉시 취소가 일어나고 진행 중 task의 executor로
        # 취소가 전파된다(원격 소유면 위 공유 플래그를 통해 비동기로 반영됨).
        await runner.cancel(job)
        job.status = JobStatus.CANCELLED
        store.save(job)
        return format_at_fields(job.progress_view())

    @app.post(
        "/jobs/{job_id}/retry",
        status_code=202,
        tags=["Jobs"],
        summary="실패 파티션만 재실행",
        description="종료된 작업(PARTIAL/FAILED/CANCELLED)의 실패·취소 task 만 모아 새 작업으로 "
        "재실행한다(성공 파티션은 건너뜀). 새 job_id 를 반환한다(202). 재실행 대상이 없거나 "
        "아직 종료되지 않은 작업이면 409.",
    )
    def retry_job(req_background: BackgroundTasks, job_id: str):
        # 실패 파티션만 재실행: 원본 job 의 FAILED/CANCELLED task 를 새 Job 으로 복제해 디스패치한다.
        # 멱등성: copy 모드의 실패 task 는 트랜잭션 미커밋(=적재 없음)이거나
        # overwrite_partitions(선삭제)이므로 재실행이 안전하다.
        src = store.get(job_id)
        if src is None:
            raise HTTPException(status_code=404, detail="job not found")
        if src.status not in (JobStatus.PARTIAL, JobStatus.FAILED, JobStatus.CANCELLED):
            raise HTTPException(
                status_code=409,
                detail=f"종료된(PARTIAL/FAILED/CANCELLED) 작업만 재실행할 수 있습니다"
                f"(status={src.status.value}).",
            )
        retriable = [
            t for t in src.tasks
            if t.status in (TaskStatus.FAILED, TaskStatus.CANCELLED)
        ]
        if not retriable:
            raise HTTPException(status_code=409, detail="재실행할 실패/취소 task 가 없습니다.")

        admission = getattr(runner, "admission", None)
        if admission is not None and not admission.try_admit():
            logger.warning(
                "재실행 거부: 동시 실행/대기 한도 초과 job=%s (capacity=%s, inflight=%s)",
                job_id, admission.capacity, admission.inflight,
            )
            raise HTTPException(
                status_code=429,
                detail=f"동시 실행/대기 job 한도 초과(capacity={admission.capacity}).",
                headers={"Retry-After": "5"},
            )
        new_id = new_job_id()
        with job_log_context(new_id):
            try:
                new = Job(
                    job_id=new_id,
                    original_sql=src.original_sql,
                    partition_column=src.partition_column,
                    target_table=src.target_table,
                    write_mode=src.write_mode,
                    parallelism=src.parallelism,
                    split_strategy=src.split_strategy,
                    failure_policy=src.failure_policy,
                    username=src.username,
                    exec_mode=src.exec_mode,
                    staging_table=src.staging_table,
                    staging_ddl=src.staging_ddl,
                    insert_sql=src.insert_sql,
                    impala_query_options=src.impala_query_options,
                    template_id=src.template_id,
                    template_params=src.template_params,
                    status=JobStatus.SPLITTING,
                    retry_of=src.job_id,
                )
                # 실패/취소 task 만 새 task(QUEUED, attempt=0)로 복제한다. 원본 sub_query·
                # partition_values·executor 배정을 그대로 재사용한다.
                new.tasks = [
                    Task(
                        job_id=new.job_id,
                        executor_url=t.executor_url,
                        sub_query=t.sub_query,
                        partition_values=t.partition_values,
                    )
                    for t in retriable
                ]
                store.add(new)
                logger.info(
                    "job %s 재실행 → 새 job %s (실패 task %d개)",
                    src.job_id, new.job_id, len(new.tasks),
                )
                req_background.add_task(runner.run, new)
            except Exception:
                if admission is not None:
                    admission.release()
                raise
        return JSONResponse(
            status_code=202,
            content={
                "job_id": new.job_id,
                "retry_of": src.job_id,
                "retried_tasks": len(new.tasks),
            },
        )

    @app.get("/jobs/{job_id}/result", tags=["Jobs"], summary="작업 결과(적재 요약) 조회")
    def get_job_result(job_id: str):
        # 적재 결과 요약(태스크별 row count 등)을 반환.
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.result_view()

    @app.get(
        "/jobs/{job_id}/tasks/{task_id}",
        tags=["Jobs"],
        summary="태스크 상세 조회(sub-query 전문 포함)",
    )
    def get_task_detail(job_id: str, task_id: str):
        # 특정 job 안의 단일 task 상세를 찾아 반환. job/ task 어느 쪽이 없어도 404.
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        for task in job.tasks:
            if task.task_id == task_id:
                # detail() 은 목록 뷰와 달리 실행된 sub_query 전문까지 포함한다.
                return task.detail()
        raise HTTPException(status_code=404, detail="task not found")

    def _annotate_executor_index(executors: list) -> list:
        """각 executor 엔트리에 설정 목록(settings.executors) 기준 ``index`` 를 붙인다.

        이 index 가 executor 상세 프록시(``GET /executors/{idx}/...``)의 키다. 클라이언트
        (대시보드/TUI)는 이 값으로 특정 executor 로 드릴인한다. 설정 목록에 없는 URL
        (self-report 로만 올라온 예외 노드)은 index=None 이라 드릴인 대상이 아니다.
        """
        urls = settings.executors
        for e in executors:
            if isinstance(e, dict):
                u = e.get("executor_url")
                e["index"] = urls.index(u) if u in urls else None
        return executors

    async def _proxy_executor_get(idx: int, path: str, params: Optional[dict] = None):
        """설정된 executor(``settings.executors[idx]``)의 읽기 엔드포인트로 GET 프록시한다.

        executor 를 **설정 목록 인덱스(allowlist)** 로만 지정받아 임의 URL 프록시(SSRF)를
        막는다. 모니터링 용도라 짧은 타임아웃을 쓰고, executor 의 상태코드·에러 본문을
        그대로 중계한다(``/datasources`` 프록시와 동일한 관례).
        """
        urls = settings.executors
        if idx < 0 or idx >= len(urls):
            raise HTTPException(
                status_code=404,
                detail=f"executor 인덱스 범위 밖: {idx} (설정된 executor {len(urls)}개)",
            )
        target = urls[idx].rstrip("/") + path
        timeout = httpx.Timeout(10.0, connect=settings.task_connect_timeout_s)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(target, params=params or {})
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"executor 프록시 실패({urls[idx]}): {e}")
        if resp.status_code >= 400:
            try:
                payload = resp.json()
                detail = payload.get("detail") if isinstance(payload, dict) else None
            except Exception:
                detail = None
            raise HTTPException(status_code=resp.status_code, detail=detail or resp.text)
        body = resp.json()
        if isinstance(body, dict):
            body["proxied_to"] = urls[idx]
        return body

    @app.get(
        "/executors",
        tags=["Monitoring"],
        summary="executor 헬스/메트릭 상태",
        description="모니터가 주기 폴링으로 보유한 executor별 CPU/메모리/디스크 상태.",
    )
    def list_executor_health():
        # self-report 모드면 공유 테이블에서 읽고(coordinator는 폴링하지 않음),
        # 아니면 모니터가 주기 폴링으로 캐시해 둔 스냅샷을 돌려준다.
        executors = status_repo.read_all() if status_repo is not None else monitor.snapshot()
        return {"executors": _annotate_executor_index(executors)}

    @app.get(
        "/cluster",
        tags=["Monitoring"],
        summary="클러스터 전체 상태(coordinator+executor health/metrics + 실행 중 job 수)",
        description="coordinator와 모든 executor의 health 및 CPU/메모리/디스크, 그리고 "
        "실행 중인 job 수를 한 번에 반환한다. refresh=true(기본)면 executor를 즉시 폴링한다.",
    )
    async def cluster(refresh: bool = True):
        # executor 상태 수집: self-report 모드면 공유 테이블에서 읽고, 아니면
        # refresh=true 일 때 즉시 폴링(최신값), false 면 캐시 스냅샷(저비용)을 쓴다.
        if status_repo is not None:
            # 멀티 coordinator: executor self-report 공유 테이블에서 조회
            executors = status_repo.read_all()
        else:
            executors = await monitor.poll_now() if refresh else monitor.snapshot()
        # 각 executor 에 드릴인 프록시 키(index)를 붙인다(대시보드/TUI 가 상세로 진입할 때 사용).
        _annotate_executor_index(executors)
        # coordinator 자신의 시스템 메트릭 수집은 blocking I/O 라 스레드로 오프로드한다.
        coord_metrics = await asyncio.to_thread(
            collect_system_metrics, settings.monitor_disk_path
        )

        # 전체 job을 상태별로 집계한다. running 은 순수 실행 중,
        # active 는 실행 + 분할 중(SPLITTING) + 대기(PENDING)까지 포함한 "처리 중" 합계.
        by_status: dict[str, int] = {}
        for job in store.list():
            by_status[job.status.value] = by_status.get(job.status.value, 0) + 1
        running = by_status.get(JobStatus.RUNNING.value, 0)
        active = running + by_status.get(JobStatus.SPLITTING.value, 0) + by_status.get(
            JobStatus.PENDING.value, 0
        )
        healthy = sum(1 for e in executors if e.get("healthy"))

        return format_at_fields({
            "coordinator": {
                "service": "coordinator",
                "status": "ok",
                "metrics": coord_metrics,
            },
            "executors": executors,
            "executors_summary": {
                "total": len(executors),
                "healthy": healthy,
                "unhealthy": len(executors) - healthy,
            },
            "jobs": {
                "running": running,
                "active": active,  # RUNNING + SPLITTING + PENDING
                "total": len(store.list()),
                "by_status": by_status,
            },
            # 이 coordinator 가 기동 후 executor 별로 배정한 누적 task 수(배정 분포 관측).
            # 선택 정책이 균형을 맞추고 있는지 확인하는 용도. 멀티 coordinator 면 인스턴스별 값.
            "assignment_counts": dict(assign_counts),
            "executor_select": _select_policy,
        })

    @app.get(
        "/executors/{idx}/info",
        tags=["Monitoring"],
        summary="executor 인스턴스 메타(프록시)",
        description="설정 목록 index 로 지정한 executor 의 /info 를 프록시한다.",
    )
    async def executor_info(idx: int):
        return await _proxy_executor_get(idx, "/info")

    @app.get(
        "/executors/{idx}/metrics",
        tags=["Monitoring"],
        summary="executor 시스템 메트릭+동시처리(프록시)",
        description="설정 목록 index 로 지정한 executor 의 /metrics 를 프록시한다.",
    )
    async def executor_metrics(idx: int):
        return await _proxy_executor_get(idx, "/metrics")

    @app.get(
        "/executors/{idx}/tasks",
        tags=["Monitoring"],
        summary="executor 보유 task 목록(프록시)",
        description="설정 목록 index 로 지정한 executor 의 /tasks 를 프록시한다(status/limit 전달).",
    )
    async def executor_tasks(idx: int, status: Optional[str] = None, limit: int = 0):
        params: dict = {}
        if status:
            params["status"] = status
        if limit:
            params["limit"] = limit
        return await _proxy_executor_get(idx, "/tasks", params)

    @app.get(
        "/executors/{idx}/tasks/{task_id}/detail",
        tags=["Monitoring"],
        summary="executor task 상세(프록시)",
        description="설정 목록 index 로 지정한 executor 의 task 상세(실행 SQL 포함)를 프록시한다.",
    )
    async def executor_task_detail(idx: int, task_id: str):
        return await _proxy_executor_get(idx, f"/tasks/{task_id}/detail")

    @app.get("/health", tags=["Monitoring"], summary="헬스 체크(liveness)")
    def health():
        # 프로세스가 살아 있는지 확인하는 liveness 프로브용 가벼운 응답.
        return {"status": "ok", "service": "coordinator", "version": __version__}

    @app.get("/healthz", tags=["Monitoring"], summary="헬스 체크 별칭(하위 호환)")
    def healthz():
        # 쿠버네티스 등에서 흔히 쓰는 /healthz 경로 호환용 별칭.
        return {"status": "ok"}

    @app.get(
        "/metrics",
        tags=["Monitoring"],
        summary="시스템 메트릭(CPU/메모리/디스크)",
    )
    def metrics():
        # coordinator 호스트의 현재 CPU/메모리/디스크 메트릭(동기 수집).
        return collect_system_metrics(settings.monitor_disk_path)

    # ───────── 모니터링 대시보드 & 데이터 API ─────────

    # "처리 중"으로 묶어서 셀 상태 집합: 실행/분할/대기. status 필터의 running·active
    # 키워드와 active 카운트 계산에 공통으로 쓰인다.
    _active_set = {JobStatus.RUNNING, JobStatus.SPLITTING, JobStatus.PENDING}

    @app.get("/jobs", tags=["Jobs"], summary="작업 목록")
    def list_jobs(status: Optional[str] = None, limit: int = 100):
        # total/running/active 요약 카운트는 필터·페이징과 무관하게 항상 전체 기준으로 센다.
        all_jobs = store.list()
        total_all = len(all_jobs)
        running = sum(1 for j in all_jobs if j.status == JobStatus.RUNNING)
        active = sum(1 for j in all_jobs if j.status in _active_set)
        # 대시보드 admission 게이지용: 실행 슬롯을 기다리는(PENDING) job 수.
        pending = sum(1 for j in all_jobs if j.status == JobStatus.PENDING)
        # 최신순 정렬(created_at 내림차순). created_at 이 없으면 빈 문자열로 안전 비교.
        jobs = sorted(all_jobs, key=lambda j: j.created_at or "", reverse=True)
        if status:
            s = status.lower()
            # running/active 는 단일 상태가 아니라 _active_set 집합 필터로 처리하고,
            # 그 외 키워드는 정확한 상태값 일치로 필터한다.
            if s in ("running", "active"):
                jobs = [j for j in jobs if j.status in _active_set]
            else:
                jobs = [j for j in jobs if j.status.value.lower() == s]
        # limit<=0 이면 전체, 아니면 상위 limit 개만 잘라 행을 구성한다.
        rows = [
            {
                "job_id": j.job_id, "status": j.status.value,
                "username": j.username,
                "progress_percent": j.progress_percent,
                "completed": j.completed, "total": len(j.tasks),
                "total_rows_written": j.total_rows_written,
                "total_rows_read": j.total_rows_read,
                "phase_summary": j.phase_summary(),
                "exec_mode": j.exec_mode, "partition_column": j.partition_column,
                "target_table": j.target_table,
                "created_at": j.created_at, "started_at": j.started_at,
                "finished_at": j.finished_at,
                "original_sql": j.original_sql,
                "error": j.error,
            }
            for j in (jobs if limit <= 0 else jobs[:limit])
        ]
        return format_at_fields(
            {
                "jobs": rows, "total": total_all, "running": running, "active": active,
                # admission 게이지(실행 슬롯/대기 큐 소진율) 표시용. 0 이하는 무제한.
                "pending": pending,
                "max_concurrent_jobs": settings.max_concurrent_jobs,
                "max_pending_jobs": settings.max_pending_jobs,
            }
        )

    history_reader = JobHistoryRepository(settings)

    @app.get("/history", tags=["Jobs"], summary="과거 실행 이력(필터/페이징)")
    def get_history(
        limit: int = 20,
        offset: int = 0,
        status: Optional[str] = None,
        username: Optional[str] = None,
        job_id: Optional[str] = None,
    ):
        # 외부 입력을 안전 범위로 강제(clamp): limit 은 1~200, offset 은 0 이상.
        # 과도한 페이지 크기나 음수 입력으로 인한 DB 부하/오류를 방지한다.
        # status/username 은 정확 일치, job_id 는 전방일치(prefix) 필터다(빈 값은 무시).
        limit = max(1, min(limit, 200))
        offset = max(0, offset)
        return format_at_fields(history_reader.read(
            limit=limit, offset=offset,
            status=status, username=username, job_id=job_id,
        ))

    @app.get(
        "/templates",
        tags=["Jobs"],
        summary="사용 가능한 쿼리 템플릿 목록(파라미터 스키마 포함)",
        description="서버 템플릿 루트를 스캔해 template_id·설명·기본 exec_mode·파라미터 스키마를 반환한다. "
        "클라이언트는 이 스키마를 보고 params 를 구성해 POST /jobs 에 template_id 로 실행한다.",
    )
    def list_templates():
        if template_engine is None:
            return {"enabled": False, "templates": []}
        return {"enabled": True, "templates": template_engine.list_templates()}

    @app.get("/datasources", tags=["Monitoring"], summary="테스트 가능한 데이터소스 목록")
    def list_datasources():
        """coordinator 가 SELECT 테스트할 수 있는 데이터소스를 반환한다.

        history/greenplum 은 coordinator 가 직접(psycopg) 테스트하고, impala 는
        드라이버가 없어 ``executor_url`` 을 지정해 executor 로 프록시해야 한다.
        """
        return {
            "local": [
                {"name": "history", "configured": bool(settings.history_db_dsn)},
                {"name": "greenplum", "configured": bool(settings.greenplum_dsn)},
            ],
            "via_executor": ["impala", "greenplum", "history"],
            "executors": list(settings.executors),
        }

    @app.post(
        "/datasources/{name}/query",
        tags=["Monitoring"],
        summary="데이터소스에 임의 SELECT 실행(연결 확인 + 결과 미리보기)",
    )
    async def query_datasource(name: str, req: DatasourceQueryRequest):
        """``name`` 데이터소스에 임의 SQL 을 실행해 상위 N행을 반환한다.

        ``executor_url`` 이 있으면 그 executor 의 동일 엔드포인트로 프록시한다(impala 처럼
        coordinator 에 드라이버가 없는 데이터소스용). 없으면 coordinator 가 직접 접속
        가능한 history/greenplum 만 로컬 실행한다.
        """
        limit = clamp_limit(req.limit)

        # 1) executor 프록시: impala 처럼 coordinator 가 직접 못 붙는 데이터소스를 executor 경유.
        if req.executor_url:
            target = req.executor_url.rstrip("/") + f"/datasources/{name}/query"
            timeout = httpx.Timeout(settings.task_timeout_s, connect=settings.task_connect_timeout_s)
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.post(target, json={"sql": req.sql, "limit": limit})
            except httpx.HTTPError as e:
                raise HTTPException(status_code=502, detail=f"executor 프록시 실패({req.executor_url}): {e}")
            if resp.status_code >= 400:
                # executor 가 돌려준 에러 본문(detail)을 그대로 전달한다.
                try:
                    payload = resp.json()
                    detail = payload.get("detail") if isinstance(payload, dict) else None
                except Exception:
                    detail = None
                if detail is None:
                    detail = resp.text
                raise HTTPException(status_code=resp.status_code, detail=detail)
            body = resp.json()
            body["proxied_to"] = req.executor_url
            return body

        # 2) 로컬 실행: coordinator 가 직접 접속 가능한 데이터소스만.
        try:
            if name == "greenplum":
                if not settings.greenplum_dsn:
                    raise HTTPException(status_code=400, detail="greenplum.dsn 미설정")
                result = await asyncio.to_thread(run_postgres_select, settings.greenplum_dsn, req.sql, limit=limit)
            elif name == "history":
                if not settings.history_db_dsn:
                    raise HTTPException(status_code=400, detail="history.db_dsn(또는 monitor.db_dsn) 미설정")
                result = await asyncio.to_thread(run_postgres_select, settings.history_db_dsn, req.sql, limit=limit)
            elif name == "impala":
                # coordinator 에는 소스 드라이버(impyla)가 없다 — executor 로 프록시해야 한다.
                raise HTTPException(
                    status_code=400,
                    detail=f"{name} 은(는) coordinator 에서 직접 테스트할 수 없습니다 — executor_url 을 지정하세요",
                )
            else:
                raise HTTPException(status_code=404, detail=f"알 수 없는 데이터소스: {name}")
        except HTTPException:
            raise
        except Exception as e:  # 연결/인증/SQL 오류 → 502 + 원인
            raise HTTPException(status_code=502, detail=f"{name} 쿼리 실패: {e}")
        return {"datasource": name, "limit": limit, **result.to_dict()}

    # /query-execute 의 라운드로빈 폴백용 회전 카운터(헬스 기반 선택이 꺼진 기본 정책).
    # 앱 인스턴스별 상태 — 요청마다 시작 executor 를 한 칸씩 돌려 단일 쿼리도 분산시킨다.
    _qe_rr = itertools.count()

    def _pick_executor_order() -> list:
        """query-execute 가 소스 쿼리를 보낼 executor 시도 순서를 만든다.

        /jobs 디스패치와 **같은 선택 정책**을 쓴다 — 헬스/부하 기반 선택(least_loaded/p2c)이
        켜져 있으면 selector+load_view 로 '가장 한가한' 순서를, 아니면 라운드로빈으로 회전한
        순서를 반환한다. 클라이언트는 어떤 executor 가 뽑히는지 알 필요가 없다.
        """
        cand = [u for u in settings.executors if u]
        if not cand:
            return []
        if selector is not None and load_view is not None:
            return selector.order(list(cand), load_view.by_url())
        start = next(_qe_rr) % len(cand)
        return cand[start:] + cand[:start]

    async def _run_source_query(name: str, sql: str, limit: int) -> dict:
        """렌더된 SELECT 를 **executor 의 커스텀 실행 함수(``POST /query-run``)** 에 위임한다.

        query-execute 의 소스 실행(impala/trino 등)은 datasource 종류와 무관하게 이 한 경로로
        통일된다 — executor 가 ``query.func.module`` 함수에 SQL 을 넘겨 실행한다. coordinator 는
        소스 드라이버를 직접 갖지 않는다. _pick_executor_order 로 정한 순서대로 시도하고,
        **연결(transport) 실패** 시 다음 executor 로 failover 한다(SELECT 는 멱등이라 재시도 안전).
        executor 가 도달 후 돌려준 4xx/5xx(SQL 오류·함수 미설정 등)는 확정 응답이므로 failover
        하지 않고 그대로 전달한다. 성공한 executor URL 은 관측용으로 응답 dict 의 ``executed_by``
        에 실어 돌려준다.
        """
        order = _pick_executor_order()
        if not order:
            raise HTTPException(
                status_code=400,
                detail=f"{name} 실행에 필요한 executor 가 설정되지 않았습니다(coordinator.executors 비어 있음)",
            )
        timeout = httpx.Timeout(settings.task_timeout_s, connect=settings.task_connect_timeout_s)
        last_err: Optional[Exception] = None
        async with httpx.AsyncClient(timeout=timeout) as http:
            for url in order:
                target = url.rstrip("/") + "/query-run"
                try:
                    resp = await http.post(target, json={"sql": sql, "limit": limit})
                except httpx.HTTPError as e:
                    last_err = e
                    logger.warning("query-execute 프록시 실패, 다음 executor 로 failover: %s (%s)", url, e)
                    continue
                if resp.status_code >= 400:
                    try:
                        payload = resp.json()
                        detail = payload.get("detail") if isinstance(payload, dict) else None
                    except Exception:
                        detail = None
                    raise HTTPException(status_code=resp.status_code, detail=detail or resp.text)
                logger.info("query-execute 실행 executor=%s datasource=%s", url, name)
                body = resp.json()
                # 실제 실행 executor 를 관측용으로 응답에 싣는다(failover 후면 성공한 executor).
                body["executed_by"] = url
                return body
        raise HTTPException(status_code=502, detail=f"모든 executor 프록시 실패({name}): {last_err}")

    @app.post(
        "/query-execute",
        tags=["Query"],
        summary="템플릿+파라미터로 SELECT 를 실행하고 결과(상위 N행)를 반환",
    )
    async def query_execute(req: QueryExecuteRequest):
        """서버 템플릿을 params 로 렌더해 SELECT 를 만들고, 지정 데이터소스에 실행해 상위 N행을 반환한다.

        ``/jobs``(이관)와 달리 결과를 동기로 돌려주는 미리보기성 실행이다. impala/trino 소스는
        coordinator 에 드라이버가 없어 부하가 가장 낮은 executor 를 골라 프록시하고(클라이언트는
        executor 를 모른다), greenplum/history 는 coordinator 가 직접 실행한다. 응답 크기는
        ``limit``(최대 10000)으로 강제한다 — 대량 이관은 계속 ``/jobs`` 를 쓴다.
        """
        if template_engine is None:
            raise HTTPException(
                status_code=404, detail="템플릿 엔진이 비활성화돼 있습니다(template.enabled=false)"
            )
        limit = clamp_limit(req.limit)
        params = _query_params_to_dict(req.params)

        # 1) 템플릿에서 select 조각만 렌더(TemplateError → 422 핸들러).
        sql = template_engine.render_query(req.template_id, params)
        # 2) 경량 검증: 단일 행 반환 SELECT 인지(다중 문/비-SELECT 차단, QueryValidationError → 422).
        validate_select_query(sql, dialect=req.sql_dialect or DIALECT)

        # 3) 데이터소스 결정(요청 > 서버 source.type).
        datasource = (req.datasource or getattr(settings, "source_type", "impala") or "impala").lower()

        # 4) 실행 라우팅(2갈래로 통일):
        #    - greenplum/history: 메타/타깃 DB → coordinator 가 직접 실행(psycopg).
        #    - 그 외 소스(impala/trino/source 등): executor 의 커스텀 함수(/query-run)에 통일 위임.
        if datasource in ("greenplum", "history"):
            dsn = settings.greenplum_dsn if datasource == "greenplum" else settings.history_db_dsn
            if not dsn:
                raise HTTPException(status_code=400, detail=f"{datasource} 접속 정보(dsn) 미설정")
            try:
                result = await asyncio.to_thread(run_postgres_select, dsn, sql, limit=limit)
            except Exception as e:  # 연결/인증/SQL 오류 → 502 + 원인
                raise HTTPException(status_code=502, detail=f"{datasource} 쿼리 실패: {e}")
            # coordinator 가 직접 실행했으므로 executor 는 없다(executed_by=null).
            body = {"datasource": datasource, "limit": limit, "executed_by": None, **result.to_dict()}
        elif datasource in ("impala", "trino", "source"):
            body = await _run_source_query(datasource, sql, limit)
        else:
            raise HTTPException(status_code=404, detail=f"알 수 없는 데이터소스: {datasource}")

        # 렌더된 SQL(감사용)과 실제 실행 executor(executed_by, 직접 실행이면 null)를 함께 반환.
        # datasource 는 항상 확정값으로 싣는다(trino /query-run 응답엔 datasource 가 없으므로 보정).
        return {"template_id": req.template_id, "datasource": datasource, "sql": sql, **body}

    # 대시보드는 설정으로 끌 수 있다(예: 외부 노출 환경에서 비활성화).
    # 활성화된 경우에만 루트 HTML 및 설정/정보 API 라우트를 등록한다.
    if settings.dashboard_enabled:

        @app.get("/", include_in_schema=False)
        def dashboard():
            # 단일 페이지 모니터링 대시보드 HTML(정적 문자열) 제공.
            return HTMLResponse(DASHBOARD_HTML)

        @app.get("/config", tags=["Monitoring"], summary="환경설정(비밀값 마스킹)")
        def get_config():
            # 현재 설정을 노출하되 DSN/비밀번호 등 민감값은 masked_config 로 가린다.
            return {"config": masked_config(settings)}

        @app.get("/info", tags=["Monitoring"], summary="기타 정보")
        def get_info():
            # 대시보드 상단에 표시할 런타임/구성 요약 정보를 모아 반환한다.
            all_jobs = store.list()
            by_status: dict[str, int] = {}
            for j in all_jobs:
                by_status[j.status.value] = by_status.get(j.status.value, 0) + 1
            return format_at_fields({
                "version": __version__,
                "coordinator_id": settings.coordinator_id,
                "executor_mode": settings.executor_mode,
                "store_backend": settings.store_backend,
                "executor_self_report": settings.executor_self_report,
                "executor_select": settings.executor_select,
                "executor_health_source": settings.executor_health_source,
                "executor_reservation": settings.executor_reservation,
                "started_at": started_at.isoformat(),
                "uptime_seconds": round(time.monotonic() - start_monotonic, 1),
                "jobs_total": len(all_jobs),
                "executors_configured": len(settings.executors),
                "max_concurrent_jobs": settings.max_concurrent_jobs,
                "max_pending_jobs": settings.max_pending_jobs,
                "max_dispatch_concurrency": settings.max_dispatch_concurrency,
                "jobs_by_status": by_status,
            })

    return app


# ASGI 서버(uvicorn 등)가 import 할 수 있도록 기본 설정으로 앱 싱글턴을 생성한다.
app = create_app()
