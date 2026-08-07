"""쿼리 템플릿 엔진 — 서버 템플릿 파일 + 파라미터로 SQL 을 런타임 생성한다.

클라이언트가 SQL 전문을 JSON 에 담아 보내는 대신, **파라미터만** 보내면 coordinator 가
서버에 보관된 템플릿(SELECT / STAGING DDL / INSERT / 외부테이블 컬럼)을 렌더링해 완성된
SQL 을 만든다. 렌더 결과는 기존 요청 필드(sql/staging_ddl/insert_sql/external_columns/
wrapper_query)에 그대로 주입되므로, 이후 검증(parser)·분할(splitter)·디스패치 파이프라인은
바뀌지 않는다.

구성:
    - 템플릿 루트(``template.dir``) 아래 ``<template_id>/`` 디렉터리 하나가 하나의
      이관 시나리오를 이룬다. ``manifest.yml`` 이 메타(exec_mode·partition_column 등 기본값)와
      파라미터 스키마, 조각 파일 매핑을 담고, ``*.sql.j2`` 조각들이 실제 SQL 템플릿이다.
    - Jinja2 ``SandboxedEnvironment`` + ``StrictUndefined`` 로 렌더한다(미정의 변수 즉시 실패,
      위험 속성 접근 차단). 커스텀 함수는 :mod:`coordinator.template_funcs` 레지스트리에서 주입.

보안:
    - 템플릿 파일은 서버 신뢰 자산, 파라미터는 비신뢰 입력. 파라미터를 SQL 에 넣을 때는
      템플릿 작성자가 ``sql_str``/``sql_in``/``sql_ident``/``sql_num`` 필터를 쓰도록 한다.
    - ``template_id`` 는 안전한 이름(영숫자/``_``/``-``)만 허용해 경로 탈출을 막는다.
    - 렌더된 SELECT 는 이후에도 ``validate_and_parse`` 를 통과해야 하므로, 다중 문/비-SELECT
      인젝션은 기존 검증에서 걸러진다. DDL/INSERT 조각은 옵션으로 단일 문 검사를 한다.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import sqlglot
import yaml
from jinja2 import StrictUndefined, TemplateError as _JinjaTemplateError
from jinja2 import meta as jinja2_meta
from jinja2.sandbox import SandboxedEnvironment

from . import template_funcs

logger = logging.getLogger(__name__)

# template_id 로 허용하는 문자(경로 탈출·특수문자 차단). 디렉터리명이자 URL 조각이 된다.
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# exec_mode 별로 렌더가 필요한 조각(role)들. required=반드시 있어야 함, optional=있으면 렌더.
# role 이름은 manifest.files 의 키이자 렌더 결과 dict 의 키다.
_ROLES_BY_MODE: dict[str, dict[str, tuple[str, ...]]] = {
    "copy":         {"required": ("select",),                          "optional": ("wrapper",)},
    "statement":    {"required": ("select",),                          "optional": ("wrapper",)},
    "stage_insert": {"required": ("select", "insert"),                 "optional": ("staging_ddl",)},
    "local_stage":  {"required": ("select", "insert", "external_columns"), "optional": ("staging_ddl",)},
    # s3_stage: 외부테이블(=staging)은 executor 가 external_columns 로 생성하므로 staging_ddl 은
    # 렌더하지 않는다(local_stage 와 달리 optional 에도 없다). select+insert+external_columns 만.
    "s3_stage":     {"required": ("select", "insert", "external_columns"), "optional": ()},
}

# manifest 가 기본값을 줄 수 있는 스칼라 필드(요청이 명시하면 요청이 이긴다). app.py 병합 대상.
# ``datasource`` 는 **query-execute 전용**이다 — 그 템플릿의 SELECT 를 어느 엔진에서 실행할지
# (impala|trino|greenplum|history) 선언한다. /jobs(이관)의 소스는 Impala 전용이라 이관 경로가
# impala 아닌 값을 만나면 조용히 Impala 로 도는 대신 422 로 거부한다(app.py ``_reject_...``).
_SCALAR_DEFAULT_KEYS = (
    "exec_mode", "partition_column", "target_table", "staging_table",
    "write_mode", "split_strategy", "failure_policy", "parallelism",
    "sql_dialect", "strict_validation", "task_bound", "datasource",
)


class TemplateError(Exception):
    """템플릿 로드/검증/렌더 실패 시 발생하는 예외.

    :class:`~coordinator.parser.QueryValidationError` 와 같은 (code, message) 형태로,
    API 계층이 일관된 422 JSON(error_code 포함)으로 변환한다.
    """

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")


@dataclass
class ParamSpec:
    """manifest 의 파라미터 하나에 대한 스키마(이름/타입/필수/기본값)."""

    name: str
    type: str = "string"
    required: bool = False
    default: Any = None


@dataclass
class Manifest:
    """``manifest.yml`` 을 파싱한 결과.

    필드:
        template_id : 템플릿 식별자(디렉터리명).
        description : 사람이 읽는 설명(대시보드/문서용).
        files       : role→상대 파일명 매핑(select/wrapper/staging_ddl/insert/external_columns).
        params      : 파라미터 스키마 목록.
        defaults    : exec_mode·partition_column 등 스칼라 기본값(요청이 명시하면 무시됨).
        mtime       : manifest 파일 수정 시각(auto_reload 캐시 무효화 기준).
    """

    template_id: str
    description: str = ""
    files: dict = field(default_factory=dict)
    params: list = field(default_factory=list)
    defaults: dict = field(default_factory=dict)
    mtime: float = 0.0


@dataclass
class RenderResult:
    """렌더 결과. app.py 가 요청 필드에 주입하는 SQL 조각들과 스칼라 기본값 묶음."""

    exec_mode: str
    select: str
    wrapper: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert: Optional[str] = None
    external_columns: Optional[str] = None
    defaults: dict = field(default_factory=dict)


class TemplateEngine:
    """템플릿 로드·검증·렌더를 담당하는 엔진(create_app 에서 1개 생성해 주입).

    단일 워커 전제라 in-process 캐시가 안전하다. ``auto_reload`` 가 켜지면 manifest/템플릿의
    파일 수정 시각을 확인해 개발 중 변경을 반영한다(운영에선 꺼서 stat 비용을 없앤다).
    """

    def __init__(self, settings) -> None:
        self._settings = settings
        self._dir = Path(settings.template_dir)
        self._auto_reload = bool(getattr(settings, "template_auto_reload", False))
        self._lock = threading.Lock()  # 동기 핸들러가 스레드풀에서 도므로 캐시 접근 보호
        self._manifests: dict[str, Manifest] = {}
        # 사용자 커스텀 함수 모듈을 먼저 로드(레지스트리 채움) 후 env 를 만든다.
        template_funcs.load_func_modules(getattr(settings, "template_func_modules", []))
        # Jinja2 샌드박스 환경: HTML 이 아닌 SQL 이므로 autoescape 는 끄고, 대신 값은
        # 커스텀 SQL 필터로 이스케이프한다. StrictUndefined 로 미정의 변수를 즉시 실패시킨다.
        from jinja2 import FileSystemLoader

        self._env = SandboxedEnvironment(
            loader=FileSystemLoader(str(self._dir)),
            undefined=StrictUndefined,
            autoescape=False,
            auto_reload=self._auto_reload,
            cache_size=0 if self._auto_reload else 400,
            keep_trailing_newline=True,
        )
        self._env.filters.update(template_funcs.FILTERS)
        self._env.globals.update(template_funcs.GLOBALS)

    # ───────────────────────── manifest ─────────────────────────

    def _validate_id(self, template_id: str) -> None:
        """template_id 가 안전한 이름인지 검사(경로 탈출·특수문자 차단)."""
        if not template_id or not _ID_RE.match(template_id):
            raise TemplateError(
                "TEMPLATE_ID_INVALID",
                f"template_id 는 영숫자/밑줄/하이픈만 허용합니다: {template_id!r}",
            )

    def load_manifest(self, template_id: str) -> Manifest:
        """template_id 의 manifest.yml 을 로드/캐시해 :class:`Manifest` 로 반환한다."""
        self._validate_id(template_id)
        path = self._dir / template_id / "manifest.yml"
        with self._lock:
            cached = self._manifests.get(template_id)
            if cached is not None and not self._auto_reload:
                return cached
            if not path.is_file():
                raise TemplateError(
                    "TEMPLATE_NOT_FOUND",
                    f"템플릿을 찾을 수 없습니다: {template_id} ({path})",
                )
            mtime = path.stat().st_mtime
            if cached is not None and cached.mtime == mtime:
                return cached  # auto_reload 지만 변경 없음 → 캐시 재사용
            manifest = self._parse_manifest(template_id, path, mtime)
            self._manifests[template_id] = manifest
            return manifest

    def _parse_manifest(self, template_id: str, path: Path, mtime: float) -> Manifest:
        """manifest.yml 텍스트를 파싱·검증해 Manifest 를 만든다."""
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            raise TemplateError("TEMPLATE_MANIFEST_ERROR", f"manifest.yml 파싱 실패: {exc}") from exc
        if not isinstance(raw, dict):
            raise TemplateError("TEMPLATE_MANIFEST_ERROR", "manifest.yml 최상위는 매핑이어야 합니다.")

        files = raw.get("files") or {}
        if not isinstance(files, dict) or not files:
            raise TemplateError(
                "TEMPLATE_MANIFEST_ERROR",
                "manifest.files 에 최소한 select 조각 파일이 지정돼야 합니다.",
            )

        params: list[ParamSpec] = []
        for p in raw.get("params") or []:
            if not isinstance(p, dict) or not p.get("name"):
                raise TemplateError("TEMPLATE_MANIFEST_ERROR", f"잘못된 param 정의: {p!r}")
            params.append(
                ParamSpec(
                    name=str(p["name"]),
                    type=str(p.get("type", "string")).lower(),
                    required=bool(p.get("required", False)),
                    default=p.get("default"),
                )
            )

        # 스칼라 기본값: manifest 최상위에서 알려진 키만 추린다(요청이 명시하면 요청이 이김).
        defaults = {k: raw[k] for k in _SCALAR_DEFAULT_KEYS if k in raw}
        return Manifest(
            template_id=template_id,
            description=str(raw.get("description", "")),
            files={str(k): str(v) for k, v in files.items()},
            params=params,
            defaults=defaults,
            mtime=mtime,
        )

    # ───────────────────────── 파라미터 ─────────────────────────

    def _resolve_params(self, manifest: Manifest, params: dict) -> dict:
        """파라미터를 스키마로 검증하고(필수/타입) 기본값을 채운 렌더 컨텍스트를 만든다.

        - 선언된 param: 필수 누락 → 오류, 미제공 시 기본값 적용, 타입 강제 변환.
        - 미선언 param: 컨텍스트에 그대로 통과(템플릿이 자유롭게 참조 가능). 오타는
          StrictUndefined 가 렌더 시점에 잡는다.
        """
        if params is not None and not isinstance(params, dict):
            raise TemplateError("TEMPLATE_PARAM_ERROR", "params 는 객체(dict)여야 합니다.")
        provided = dict(params or {})
        ctx: dict = dict(provided)  # 미선언 값도 통과시키되, 선언값은 아래에서 덮어쓴다
        for spec in manifest.params:
            if spec.name in provided:
                ctx[spec.name] = _coerce(spec, provided[spec.name])
            elif spec.required:
                raise TemplateError(
                    "TEMPLATE_PARAM_ERROR", f"필수 파라미터 누락: {spec.name}"
                )
            elif spec.default is not None:
                ctx[spec.name] = _coerce(spec, spec.default)
            else:
                ctx[spec.name] = None
        return ctx

    # ───────────────────────── 렌더 ─────────────────────────

    def render(
        self,
        template_id: str,
        params: dict,
        request_scalars: Optional[dict] = None,
    ) -> RenderResult:
        """템플릿을 렌더링해 :class:`RenderResult` 를 반환한다.

        인자:
            template_id     : 템플릿 식별자.
            params          : 클라이언트가 보낸 렌더 파라미터.
            request_scalars : 요청이 **명시한** 스칼라 값들(exec_mode/partition_column/
                              target_table/staging_table 등). manifest 기본값을 이기며,
                              렌더 컨텍스트에도 노출되어 템플릿이 참조할 수 있다.

        동작:
            1) manifest 로드 → exec_mode 결정(요청 > manifest > 'copy').
            2) 파라미터 검증/기본값 → 렌더 컨텍스트. 유효 스칼라(manifest 기본 위에 요청
               오버라이드)도 컨텍스트에 노출한다(target_table 등을 템플릿이 참조 가능).
            3) exec_mode 가 요구하는 조각(role)을 렌더. 필수 role 누락 시 오류.
            4) DDL/INSERT 조각은 옵션에 따라 단일 문 검사(다중 문 방지).
        """
        manifest = self.load_manifest(template_id)
        request_scalars = request_scalars or {}
        exec_mode = (
            request_scalars.get("exec_mode")
            or manifest.defaults.get("exec_mode")
            or "copy"
        )
        roles = _ROLES_BY_MODE.get(exec_mode)
        if roles is None:
            raise TemplateError("TEMPLATE_EXEC_MODE", f"지원하지 않는 exec_mode: {exec_mode}")

        ctx = self._resolve_params(manifest, params)
        # 유효 스칼라(manifest 기본값 위에 요청 오버라이드)를 컨텍스트에 노출한다. 단, 같은
        # 이름의 param 이 있으면 param 이 우선하도록 setdefault 를 쓴다(값 충돌 방지).
        effective_scalars = {**manifest.defaults, **request_scalars}
        for k, v in effective_scalars.items():
            ctx.setdefault(k, v)
        # 결정된 exec_mode 와 template_id 도 노출(위 스칼라에 exec_mode 가 있어도 확정값으로 덮음).
        ctx["exec_mode"] = exec_mode
        ctx.setdefault("template_id", template_id)

        out: dict[str, Optional[str]] = {}
        for role in roles["required"]:
            fname = manifest.files.get(role)
            if not fname:
                raise TemplateError(
                    "TEMPLATE_MISSING_ROLE",
                    f"exec_mode={exec_mode} 는 '{role}' 조각이 필요하지만 manifest.files 에 없습니다.",
                )
            out[role] = self._render_file(template_id, fname, ctx)
        for role in roles["optional"]:
            fname = manifest.files.get(role)
            out[role] = self._render_file(template_id, fname, ctx) if fname else None

        # 렌더된 SELECT 는 이후 parser 가 다시 검증하지만, 빈 결과는 여기서 조기 실패시킨다.
        if not (out.get("select") or "").strip():
            raise TemplateError("TEMPLATE_RENDER_ERROR", "렌더된 select 가 비어 있습니다.")

        # DDL/INSERT/wrapper 는 parser 를 타지 않으므로 옵션에 따라 단일 문만 허용해
        # ';' 로 이어붙인 다중 문 인젝션을 차단한다.
        if getattr(self._settings, "template_validate_ddl_single_stmt", True):
            for role in ("staging_ddl", "insert"):
                self._assert_single_statement(role, out.get(role))

        return RenderResult(
            exec_mode=exec_mode,
            select=out["select"],
            wrapper=out.get("wrapper"),
            staging_ddl=out.get("staging_ddl"),
            insert=out.get("insert"),
            external_columns=out.get("external_columns"),
            defaults=dict(manifest.defaults),
        )

    def render_query(self, template_id: str, params: dict) -> str:
        """``/query-execute`` 전용 — **select 조각만** 렌더해 SELECT 문자열을 반환한다.

        :meth:`render`(이관용)와 달리 exec_mode 별 insert/staging 조각을 요구하지 않는다.
        결과를 바로 돌려주는 실행이라 SELECT 하나면 충분하므로, 어떤 exec_mode 템플릿이든
        select 조각만 있으면 동작한다. 파라미터 검증(필수/타입/기본값)·인젝션 방지 필터·
        StrictUndefined 는 :meth:`render` 와 완전히 같은 경로를 탄다.

        manifest 의 스칼라 기본값(target_table 등)도 렌더 컨텍스트에 노출해 select 템플릿이
        참조할 수 있게 하되, 같은 이름의 param 이 있으면 param 이 우선한다(setdefault).
        """
        manifest = self.load_manifest(template_id)
        fname = manifest.files.get("select")
        if not fname:
            raise TemplateError(
                "TEMPLATE_MISSING_ROLE",
                "query-execute 는 'select' 조각이 필요하지만 manifest.files 에 없습니다.",
            )
        ctx = self._resolve_params(manifest, params)
        for k, v in manifest.defaults.items():
            ctx.setdefault(k, v)
        ctx.setdefault("template_id", template_id)
        select = self._render_file(template_id, fname, ctx)
        if not select.strip():
            raise TemplateError("TEMPLATE_RENDER_ERROR", "렌더된 select 가 비어 있습니다.")
        return select

    def referenced_variables(self, template_id: str, role: str = "select") -> set:
        """조각 파일이 **참조하는 변수 이름 집합**을 Jinja2 AST 에서 뽑는다(렌더 없이).

        날짜 fan-out 의 안전장치용이다. task 파라미터를 ``interval {{ from_date_no }}``
        처럼 쓰면서 부호 변수(``from_date_no_sign``)를 안 쓰는 템플릿은, task 마다 절대값을
        받아 ``BETWEEN today-3 AND today+3`` 같은 **의도보다 넓은 구간**을 조용히 읽는다
        (append 적재라 중복이 그대로 쌓인다). 문자열 grep 이 아니라 AST 를 보므로 주석·
        조건절 안의 참조도 정확히 잡힌다. ``{% include %}`` 너머는 보지 않는다(조각은 단일
        파일 전제). 파일이 없으면 빈 집합.
        """
        manifest = self.load_manifest(template_id)
        fname = manifest.files.get(role)
        if not fname:
            return set()
        rel = f"{template_id}/{fname}"
        try:
            source, _, _ = self._env.loader.get_source(self._env, rel)
            return set(jinja2_meta.find_undeclared_variables(self._env.parse(source)))
        except Exception as exc:  # 문법 오류 등은 렌더 시점에 제대로 보고되므로 여기선 관대
            logger.debug("템플릿 '%s' 변수 추출 실패(무시): %s", rel, exc)
            return set()

    def _render_file(self, template_id: str, filename: str, ctx: dict) -> str:
        """조각 파일 하나를 렌더링한다(경로는 template_id 하위로 결합)."""
        rel = f"{template_id}/{filename}"
        try:
            template = self._env.get_template(rel)
            # 앞뒤 공백/개행 제거: 주석 블록({# #})이나 조건절로 생기는 여백을 없애 로그·감사에
            # 깔끔한 SQL 을 남긴다(파서/스플리터 동작에는 영향 없음).
            return template.render(**ctx).strip()
        except _JinjaTemplateError as exc:
            # 미정의 변수(StrictUndefined)·문법 오류·필터 오류 등 모두 여기로 온다.
            raise TemplateError(
                "TEMPLATE_RENDER_ERROR", f"'{rel}' 렌더 실패: {exc}"
            ) from exc
        except TemplateError:
            raise
        except Exception as exc:  # 커스텀 함수가 던진 ValueError 등
            raise TemplateError(
                "TEMPLATE_RENDER_ERROR", f"'{rel}' 렌더 중 오류: {exc}"
            ) from exc

    def _assert_single_statement(self, role: str, sql: Optional[str]) -> None:
        """SQL 조각이 단일 문인지 검사(다중 문 인젝션 방지). 파싱 실패는 통과(관대)."""
        if not sql or not sql.strip():
            return
        try:
            stmts = [s for s in sqlglot.parse(sql) if s is not None]
        except Exception:
            # 외부테이블 DDL 등 sqlglot 이 완전히 파싱 못 하는 문법일 수 있으니 관대하게 넘긴다.
            return
        if len(stmts) > 1:
            raise TemplateError(
                "TEMPLATE_MULTIPLE_STATEMENTS",
                f"'{role}' 조각은 단일 SQL 문이어야 합니다(다중 문 감지).",
            )

    def list_templates(self) -> list[dict]:
        """템플릿 루트를 스캔해 사용 가능한 템플릿 목록(요약)을 반환한다(대시보드/조회용)."""
        out: list[dict] = []
        if not self._dir.is_dir():
            return out
        for child in sorted(self._dir.iterdir()):
            if not child.is_dir() or not (child / "manifest.yml").is_file():
                continue
            try:
                m = self.load_manifest(child.name)
            except TemplateError as exc:
                # 깨진 manifest 를 조용히 빼면 템플릿이 소리 없이 사라져 원인 파악이 어렵다.
                # 목록에서 제외하되 이유를 남긴다(대시보드에 안 보이는 템플릿 진단).
                logger.warning("template '%s' manifest 로드 실패 — 목록 제외: %s", child.name, exc)
                continue
            out.append({
                "template_id": m.template_id,
                "description": m.description,
                "exec_mode": m.defaults.get("exec_mode"),
                "partition_column": m.defaults.get("partition_column"),
                "params": [
                    {"name": p.name, "type": p.type, "required": p.required, "default": p.default}
                    for p in m.params
                ],
            })
        return out


def _coerce(spec: ParamSpec, value: Any) -> Any:
    """파라미터 값을 선언된 타입으로 강제 변환/검증한다(실패 시 TemplateError)."""
    t = spec.type
    try:
        if t in ("string", "str", "date", "datetime"):
            # date/datetime 은 문자열로 보관(커스텀 date_range 가 파싱). None 은 그대로.
            return value if value is None else str(value)
        if t in ("int", "integer"):
            return int(value)
        if t in ("float", "number"):
            return float(value)
        if t in ("bool", "boolean"):
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("true", "1", "yes")
        if t == "list":
            if not isinstance(value, (list, tuple)):
                raise ValueError("리스트가 아님")
            return list(value)
        # 알 수 없는 타입은 그대로 통과(문서화된 타입 외에는 변환하지 않음)
        return value
    except (ValueError, TypeError) as exc:
        raise TemplateError(
            "TEMPLATE_PARAM_ERROR",
            f"파라미터 '{spec.name}' 타입 변환 실패(type={t}): {exc}",
        ) from exc
