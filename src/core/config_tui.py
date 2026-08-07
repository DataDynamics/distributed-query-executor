"""config.properties 를 편집하는 curses 기반 전체화면 설정 TUI 다.

## 왜 이렇게 만드나

이 저장소의 설정은 두 파일로 나뉜다(자세히는 ``core.config_loader``):

* ``config.yml``  — **구조·기본값·설명**. 각 값은 ``${키:기본값}`` 자리표시자이고,
  YAML 중첩이 곧 섹션 그룹이며, 줄 끝 ``# 주석`` 이 곧 그 항목의 설명이다.
* ``config.properties`` — 운영자가 실제로 편집하는 **오버라이드 파일**(flat key=value).

따라서 TUI 는 **스키마를 코드에 하드코딩하지 않는다.** ``config.yml`` 을 파싱해
항목 목록·섹션·기본값·도움말을 그대로 뽑아 화면을 구성하고(:func:`parse_schema`),
사용자가 바꾼 값만 ``config.properties`` 에 주석·순서를 보존한 채 diff-write 한다
(:func:`merge_properties_lines`). 설정 항목이 늘어도 YAML 만 고치면 TUI 가 자동으로 따라간다.

## 동시성 조정

처리량을 좌우하는 손잡이는 coordinator·executor·greenplum·copy 로 흩어져 있어 한 번
조정하려면 탭을 옮겨 다녀야 한다. 그래서 :data:`CONCURRENCY_KEYS` 의 항목만 모은 **동시성
가상 탭**을 맨 앞에 두고, 숫자는 ``+``/``-`` 로 :data:`INT_BOUNDS` 의 스텝만큼 움직인다.
화면 아래에는 :func:`concurrency_summary` 가 그 값에서 유도되는 실제 용량을 곱해 보여 주고,
:func:`check_concurrency` 가 값들 사이의 어긋난 조합을 경고한다.

## 항목별 설명

목록 화면 아래의 한 줄 설명은 길면 잘리므로, ``?`` 를 누르면 :func:`help_lines` 가 만든 전문이
뜬다. 여기에는 ``config.yml`` 주석에서 온 **무엇인가**와 :mod:`core.config_help` 에서 온
**어떻게 쓰는가**가 함께 오르고, 현재 값·기본값·허용 범위·함께 볼 항목도 곁들여 이 화면만으로
판단이 끝나게 했다. 화면에 쓰기 전에는 :func:`core.textui.cut` 으로 폭을 맞추는데, 한글이 섞인
줄을 글자 수로 자르면 실제 폭의 두 배까지 밀려 맨 아랫줄에서 curses 가 예외를 던지기 때문이다.

## 구성

* 순수 로직(테스트 대상, curses 무관): :func:`parse_schema`,
  :func:`merge_properties_lines`, :func:`validate`, :func:`infer_type`,
  :func:`display_value`, :func:`step_value`, :func:`concurrency_summary`,
  :func:`check_concurrency`, :func:`help_lines`.
* :class:`Field` — 항목 하나의 메타(프로퍼티 키/섹션 경로/기본값/도움말/타입/enum/비밀 여부).
* curses UI: :class:`ConfigTUI` 와 :func:`run_tui`.
* 진입점: :func:`main` (``python -m core.config_tui`` 또는 ``bin/config-tui``).

에어갭(외부 차단) 전제라 외부 TUI 라이브러리를 쓰지 않고 파이썬 표준 ``curses`` 만 쓴다.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field as dataclass_field
from pathlib import Path
from typing import Any

from core.config_help import help_for, related_to
from core.config_loader import DEFAULT_CONFIG_DIR, load_properties
from core.masking import mask_dsn
from core.textui import cut, pad, wrap

# ``${변수:기본값}`` 자리표시자를 찾는다(config_loader 와 같은 규칙이다). group(1)이 변수명이고 group(2)가 기본값이다.
_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")

# ─────────────────────────────────────────────────────────────────────────────
# enum / 비밀값 힌트
#
# 타입(bool/int/float)은 기본값에서 자동 추론하지만(:func:`infer_type`), 값이 정해진
# 집합인 항목만은 YAML 만으로 알 수 없어 여기 프로퍼티 키로 열거한다. 새 enum 설정을
# 추가하면 이 표에 한 줄만 더한다. (표에 없으면 자유 입력 문자열로 취급.)
# ─────────────────────────────────────────────────────────────────────────────
ENUM_CHOICES: dict[str, list[str]] = {
    "coordinator.executor_mode": ["remote", "local"],
    "coordinator.executor_select": ["round_robin", "least_loaded", "p2c"],
    "coordinator.executor_health_source": ["auto", "monitor", "self_report"],
    "store.backend": ["memory", "file", "postgres"],
    "source.type": ["impala"],
    "impala.auth_mechanism": ["LDAP", "PLAIN", "NOSASL"],
    "copy.format": ["text", "binary"],
    "log.level": ["DEBUG", "INFO", "WARNING", "ERROR"],
    "log.warn.level": ["WARNING", "ERROR"],
    "query.sql_dialect": ["hive", "impala", "trino", "postgres"],
}

# ─────────────────────────────────────────────────────────────────────────────
# 숫자 항목의 허용 범위와 증감 폭
#
# ``(최소, 최대, 스텝)`` 이며 스텝은 ``+``/``-`` 키로 한 번에 움직이는 폭이다. 자릿수가 큰
# 항목(배치 크기 등)은 스텝도 크게 잡아야 손으로 타이핑하지 않고도 실용적으로 조정된다.
# 최소·최대는 :func:`validate` 에서 error 로 막는데, 여기 적힌 하한은 대부분 "그 아래로
# 내려가면 조용히 멈춘다"는 뜻이라 경고로는 부족하다. 대표적으로
# ``max_dispatch_concurrency`` 는 0 이면 ``asyncio.Semaphore(0)`` 가 되어 디스패치가
# 영원히 대기하고, 음수면 기동 시 ValueError 로 죽는다.
# ─────────────────────────────────────────────────────────────────────────────
INT_BOUNDS: dict[str, tuple[int, int, int]] = {
    "coordinator.max_concurrent_jobs": (0, 4096, 1),        # 0=무제한(admission 자체 비활성)
    "coordinator.max_pending_jobs": (0, 100000, 10),        # 0=대기 큐 없음
    "coordinator.max_dispatch_concurrency": (1, 4096, 4),   # 0 이면 디스패치가 멈춘다
    "executor.max_concurrent_tasks": (0, 1024, 1),          # 0=무제한
    "greenplum.pool_max": (0, 1024, 1),                     # 0=max_concurrent_tasks 와 동일
    "copy.batch_size": (1, 1000000, 1000),
    "copy.queue_size": (1, 1024, 1),
    "stage.max_files_per_host": (0, 4096, 1),               # 0=세그먼트 수만큼 자동
}

# ─────────────────────────────────────────────────────────────────────────────
# 동시성 탭
#
# 동시 처리량을 좌우하는 손잡이는 coordinator·executor·greenplum·copy 로 흩어져 있어서
# 한 번 조정하려면 탭을 옮겨 다녀야 했다. 이 키들만 원래 순서(입구 → 디스패치 → executor
# → GP → 버퍼)대로 모아 가상 탭 하나로 먼저 보여 준다. 항목 자체는 원래 탭에도 그대로
# 남아 있고 같은 :class:`Field` 를 공유하므로 어느 쪽에서 고쳐도 결과는 같다.
# ─────────────────────────────────────────────────────────────────────────────
CONCURRENCY_SECTION = "__concurrency__"

CONCURRENCY_KEYS: list[str] = [
    "coordinator.executors",                    # 대수가 플릿 용량 계산의 기준이라 함께 본다
    "coordinator.max_concurrent_jobs",
    "coordinator.max_pending_jobs",
    "coordinator.max_dispatch_concurrency",
    "executor.max_concurrent_tasks",
    "greenplum.pool_max",
    "copy.batch_size",
    "copy.queue_size",
    "stage.max_files_per_host",
]

# YAML 최상위 키를 사람이 읽기 좋은 탭 이름으로 바꾼다. 표에 없으면 키를 그대로 쓴다.
SECTION_LABELS: dict[str, str] = {
    CONCURRENCY_SECTION: "동시성",
    "app": "App",
    "query": "Query",
    "template": "Template",
    "logging": "Logging",
    "coordinator": "Coordinator",
    "db": "DB",
    "store": "Store",
    "dashboard": "Dashboard",
    "monitor": "Monitor",
    "history": "History",
    "executor": "Executor",
}


def _is_secret(prop_key: str) -> bool:
    """자격증명성 키(화면에 가려야 하는 값)인지 판별한다.

    비밀번호·DSN(비밀번호 포함)·시크릿 접미 키를 대상으로 한다.
    """
    tail = prop_key.rsplit(".", 1)[-1]
    return tail in {"password", "dsn", "db_dsn", "secret"} or tail.endswith("password")


def infer_type(default: str) -> str:
    """기본값 문자열에서 항목 타입을 ``bool``·``int``·``float``·``str`` 중 하나로 추론한다.

    ``true`` 와 ``false`` 는 bool 로, 정수·실수 리터럴은 int·float 로 보고 나머지는 str 로 둔다.
    """
    if default in ("true", "false"):
        return "bool"
    if re.fullmatch(r"-?\d+", default):
        return "int"
    if re.fullmatch(r"-?\d+\.\d+", default):
        return "float"
    return "str"


@dataclass
class Field:
    """설정 항목 하나의 메타데이터이며 config.yml 한 줄에서 뽑아낸다."""

    prop_key: str                       # config.properties 의 키이자 자리표시자 변수명이다
    section: str                        # 소속된 최상위 섹션이며 YAML 최상위 키와 같다
    path: list[str]                     # YAML 중첩 경로이며 그룹 헤더를 표시할 때 쓴다
    default: str                        # ${...:기본값} 에서 뽑은 기본값이며 콜론이 없으면 "" 다
    has_default: bool                   # 자리표시자에 기본값(콜론)이 명시됐는지를 나타낸다
    help_inline: str = ""               # 줄 끝 주석에서 뽑은 짧은 설명이다
    help_long: str = ""                 # 항목 위 주석에서 뽑은 긴 설명이며 여러 줄일 수 있다
    ftype: str = "str"                  # bool|int|float|str
    enum: list[str] = dataclass_field(default_factory=list)  # 값 후보이며 있으면 토글로 고른다
    secret: bool = False                # 비밀값이라 화면에서 마스킹한다

    @property
    def group(self) -> str:
        """섹션 안 하위 그룹의 라벨이다. [executor,impala,host] 면 'impala' 가 되고, 최상위면 "" 다.

        path 의 첫 원소는 섹션, 마지막 원소는 이 항목의 YAML 키이므로 그 사이가 그룹이다.
        """
        return ".".join(self.path[1:-1])


# 구분선처럼 생긴 배너 주석(`───` 류)은 설명이 아니라 장식일 뿐이므로 걸러낸다.
_BANNER = re.compile(r"^[#\s─\-=*]+$")


def parse_schema(yaml_text: str) -> list[Field]:
    """``config.yml`` 원문을 파싱해 설정 항목(:class:`Field`) 목록을 YAML 순서대로 반환한다.

    ``yaml.safe_load`` 대신 **라인 기반**으로 직접 훑는다 — 각 항목의 줄 끝/줄 위
    주석(설명)을 함께 얻어야 하는데 safe_load 는 주석을 버리기 때문이다. 이 YAML 은
    2칸 들여쓰기·단순 매핑만 쓰므로(리스트·멀티라인 스칼라 없음) 들여쓰기 스택으로
    경로를 복원할 수 있다.

    인자:
        yaml_text: config.yml 파일 내용(문자열).

    반환:
        ``${...}`` 자리표시자를 값으로 가진 리프(leaf) 항목들의 :class:`Field` 목록.
    """
    fields: list[Field] = []
    stack: list[tuple[int, str]] = []   # 현재 열려 있는 상위 매핑들을 (indent, key) 로 쌓는다
    pending: list[str] = []             # 다음 항목에 붙일 위쪽 주석 줄을 모아 두는 버퍼다

    for raw in yaml_text.splitlines():
        if not raw.strip():
            pending.clear()             # 빈 줄은 주석 묶음을 끊는다
            continue

        indent = len(raw) - len(raw.lstrip(" "))
        body = raw.strip()

        if body.startswith("#"):
            text = body.lstrip("#").strip()
            if text and not _BANNER.match(body):
                pending.append(text)
            continue

        # ``key:`` 또는 ``key: value  # 주석`` — 키에는 콜론이 없으므로 첫 콜론에서 자른다.
        if ":" not in body:
            pending.clear()
            continue
        key, rest = body.split(":", 1)
        key = key.strip()
        rest = rest.strip()

        # rest 에서 값과 줄 끝 주석을 분리한다. 값이 ${...} 면 닫는 '}' 뒤의 '#' 부터만 주석으로 본다.
        value, comment = _split_value_comment(rest)

        # 들여쓰기 스택 정리: 현재보다 깊거나 같은 항목을 닫는다.
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = [k for _, k in stack] + [key]

        if value == "":
            # 값이 없는 매핑(섹션 노드) — 스택에 밀어넣고 항목으로는 치지 않는다.
            stack.append((indent, key))
            pending.clear()
            continue

        m = _VAR_PATTERN.search(value)
        if not m:
            # 상수 값(예: rolling.type: daily) — 오버라이드 대상이 아니므로 건너뛴다.
            pending.clear()
            continue

        prop_key = m.group(1)
        default = m.group(2) if m.group(2) is not None else ""
        enum = ENUM_CHOICES.get(prop_key, [])
        ftype = "enum" if enum else infer_type(default)
        fields.append(
            Field(
                prop_key=prop_key,
                section=path[0],
                path=path,
                default=default,
                has_default=m.group(2) is not None,
                help_inline=comment,
                help_long=" ".join(pending),
                ftype=ftype,
                enum=enum,
                secret=_is_secret(prop_key),
            )
        )
        pending.clear()

    return fields


def _split_value_comment(rest: str) -> tuple[str, str]:
    """YAML 값 부분에서 ``값`` 과 줄 끝 ``# 주석`` 을 분리한다.

    값이 ``${...}`` 자리표시자면 닫는 ``}`` 이후의 ``#`` 만 주석으로 본다(기본값 안의
    문자와 충돌 방지). 자리표시자가 아니면 첫 ``#`` 기준으로 나눈다.
    """
    if "${" in rest and "}" in rest:
        end = rest.index("}")
        value = rest[: end + 1].strip()
        after = rest[end + 1 :]
        comment = after.split("#", 1)[1].strip() if "#" in after else ""
        return value, comment
    if "#" in rest:
        value, comment = rest.split("#", 1)
        return value.strip(), comment.strip()
    return rest.strip(), ""


def step_value(fld: Field, value: str, direction: int) -> str:
    """숫자 항목의 값을 한 스텝 올리거나 내린 결과를 돌려준다(``direction`` 은 +1 또는 -1).

    스텝 폭과 상·하한은 :data:`INT_BOUNDS` 를 따르고, 표에 없는 int 항목은 1씩 움직이며
    하한을 두지 않는다. float 항목은 0.5 씩 움직인다. 숫자가 아니거나 현재 값이 숫자로
    읽히지 않으면 손대지 않고 그대로 돌려준다 — 사용자가 손으로 넣은 값을 키 하나로
    날려 버리지 않기 위해서다.
    """
    if fld.ftype == "float":
        try:
            cur = float(value if value != "" else (fld.default or "0"))
        except ValueError:
            return value
        nxt = max(0.0, round(cur + 0.5 * direction, 3))
        # 정수로 떨어지면 소수점을 남기지 않는다(0.5 → 0 이 아니라 1.0 → 1).
        return str(int(nxt)) if nxt == int(nxt) else str(nxt)

    if fld.ftype != "int":
        return value
    try:
        cur = int(value if value != "" else (fld.default or "0"))
    except ValueError:
        return value

    lo, hi, step = INT_BOUNDS.get(fld.prop_key, (0, 2**31 - 1, 1))
    nxt = cur + step * direction
    # 스텝이 크면 어중간한 값(예: 10500)에서 눈금(11000)으로 맞춰 주는 편이 쓰기 좋다.
    if step > 1 and cur % step:
        nxt = (cur // step + (1 if direction > 0 else 0)) * step
    return str(max(lo, min(hi, nxt)))


def _int_of(value: str, fallback: int = 0) -> int:
    """설정 문자열을 정수로 읽는다. 비었거나 숫자가 아니면 ``fallback`` 을 쓴다."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return fallback


def concurrency_summary(values: dict[str, str]) -> list[str]:
    """현재 값에서 유도되는 실제 동시 처리량을 사람이 읽을 문장들로 만든다.

    개별 손잡이의 숫자만 봐서는 전체 처리량이 얼마인지 알기 어렵다. 입구에서 몇 건을
    받아 주는지, 플릿 전체가 동시에 몇 개의 task 를 돌리는지, Greenplum 에 연결이 몇 개나
    열리는지, 파이프라인이 몇 행을 메모리에 들고 있는지를 곱셈으로 풀어 보여 준다.
    ``values`` 는 기본값이 이미 채워진 유효값 매핑이다.
    """
    n_exec = len([u for u in values.get("coordinator.executors", "").split(",") if u.strip()])
    jobs = _int_of(values.get("coordinator.max_concurrent_jobs", ""), 16)
    pending = _int_of(values.get("coordinator.max_pending_jobs", ""), 100)
    tasks = _int_of(values.get("executor.max_concurrent_tasks", ""), 8)
    pool = _int_of(values.get("greenplum.pool_max", ""), 0)
    batch = _int_of(values.get("copy.batch_size", ""), 10000)
    queue = _int_of(values.get("copy.queue_size", ""), 8)

    lines: list[str] = []
    if jobs <= 0:
        lines.append("입구: 무제한(max_concurrent_jobs≤0 이라 admission 을 쓰지 않는다)")
    else:
        lines.append(
            f"입구: 동시 {jobs}건 실행 + {max(0, pending)}건 대기 = {jobs + max(0, pending)}건까지 수용(초과 429)"
        )

    if n_exec == 0:
        lines.append("플릿: coordinator.executors 가 비어 있어 용량을 계산할 수 없다")
    elif tasks <= 0:
        lines.append(f"플릿: executor {n_exec}대 × 무제한 task(max_concurrent_tasks=0)")
    else:
        eff_pool = pool if pool > 0 else tasks
        lines.append(
            f"플릿: executor {n_exec}대 × task {tasks}개 = 동시 {n_exec * tasks}개, "
            f"GP 연결 최대 {n_exec * eff_pool}개(pool_max {'자동' if pool <= 0 else pool})"
        )

    lines.append(f"copy 버퍼: {queue} × {batch:,}행 ≈ task 당 최대 {queue * batch:,}행을 메모리에 보관")
    return lines


def check_concurrency(values: dict[str, str]) -> list[tuple[str, str, str]]:
    """동시성 값들 **사이의** 앞뒤가 맞는지 본다. 개별 항목 검증(:func:`validate`)의 보완이다.

    각 항목이 저마다 유효해도 조합이 어긋나면 처리량이 조용히 깎인다. 대표적으로 GP 풀이
    동시 task 수보다 작으면 task 는 매번 연결을 기다리고, 디스패치 상한이 플릿 용량보다
    작으면 executor 가 놀아도 task 가 나가지 않는다. 그런 조합을 warn 으로 알린다.
    """
    issues: list[tuple[str, str, str]] = []
    n_exec = len([u for u in values.get("coordinator.executors", "").split(",") if u.strip()])
    jobs = _int_of(values.get("coordinator.max_concurrent_jobs", ""), 16)
    pending = _int_of(values.get("coordinator.max_pending_jobs", ""), 100)
    dispatch = _int_of(values.get("coordinator.max_dispatch_concurrency", ""), 32)
    tasks = _int_of(values.get("executor.max_concurrent_tasks", ""), 8)
    pool = _int_of(values.get("greenplum.pool_max", ""), 0)

    if 0 < pool < tasks:
        issues.append((
            "warn", "greenplum.pool_max",
            f"동시 task({tasks})보다 작아 task 가 GP 연결을 기다린다(0 이면 자동으로 {tasks})",
        ))
    if jobs > 0 and pending <= 0:
        issues.append((
            "warn", "coordinator.max_pending_jobs",
            f"대기 큐가 없어 실행 슬롯({jobs})을 넘는 요청은 곧바로 429 가 된다",
        ))
    if n_exec and tasks > 0 and 0 < dispatch < n_exec * tasks:
        issues.append((
            "warn", "coordinator.max_dispatch_concurrency",
            f"플릿 용량({n_exec}대×{tasks}={n_exec * tasks})보다 작아 디스패치가 병목이 된다",
        ))
    return issues


def help_lines(fld: Field, value: str, cols: int) -> list[str]:
    """한 항목의 도움말 화면에 그릴 줄들을 만든다(curses 무관).

    화면에는 이 값이 **무엇을 정하는지**(config.yml 주석)와 **어떻게 정하는지**
    (:mod:`core.config_help` 의 안내)를 함께 올린다. 목록 화면의 한 줄짜리 설명은 길면
    잘리므로, 전문을 보려면 이 화면을 연다. 현재 값과 기본값, 허용 범위, 함께 볼 항목도
    같이 보여 주어 이 화면만으로 판단이 끝나게 한다.
    """
    out: list[str] = [fld.prop_key, ""]

    cur = display_value(fld, value)
    out.append(f"현재 값: {cur}")
    out.append(f"기본값: {fld.default or '(빈 값)'}")
    if fld.enum:
        out.append(f"고를 수 있는 값: {' | '.join(fld.enum)}")
    elif fld.prop_key in INT_BOUNDS:
        lo, hi, step = INT_BOUNDS[fld.prop_key]
        out.append(f"허용 범위: {lo} ~ {hi}   (+/- 한 번에 {step})")
    else:
        out.append(f"형식: {fld.ftype}")

    # config.yml 주석 — 이 설정이 무엇인지.
    meaning = " ".join(x for x in (fld.help_long, fld.help_inline) if x)
    if meaning:
        out += ["", "■ 무엇인가"] + wrap(meaning, cols)

    # 별도 안내 — 어떻게 정하는지.
    guide = help_for(fld.prop_key)
    if guide:
        out += ["", "■ 어떻게 쓰는가"] + wrap(guide, cols)

    rel = related_to(fld.prop_key)
    if rel:
        out += ["", "■ 함께 보기"] + wrap(", ".join(rel), cols)

    if not meaning and not guide:
        out += ["", "(이 항목에는 준비된 설명이 없다)"]
    return out


def display_value(fld: Field, value: str) -> str:
    """항목의 현재 값을 화면 표시용 문자열로 만든다(비밀값 마스킹 포함).

    DSN 은 :func:`core.masking.mask_dsn` 로 비밀번호만 가리고, 그 외 비밀값은 ``***``.
    빈 값은 대시보드 관례대로 ``(미설정)`` 으로 보인다.
    """
    if value == "":
        return "(미설정)"
    if fld.secret:
        tail = fld.prop_key.rsplit(".", 1)[-1]
        if "dsn" in tail:
            return mask_dsn(value)
        return "***"
    return value


# diff-write 로 새로 추가하는 키 앞에 붙이는 표식이다. 중복 추가를 막는 데도 쓴다.
_APPEND_MARKER = "# ─── config-tui 로 추가된 설정 ───"


def merge_properties_lines(existing: list[str], values: dict[str, str]) -> list[str]:
    """기존 properties 줄들에 ``values`` 를 반영해 **새 줄 목록**을 만든다. 주석과 순서는 보존한다.

    * 파일에 이미 있는 키는 **제자리에서 값만** 교체한다(주석·배치 유지).
    * 파일에 없는 키는 끝에 :data:`_APPEND_MARKER` 아래로 추가한다(마커는 1회만).
    * ``values`` 에 없는 기존 줄은 건드리지 않는다.

    인자:
        existing: config.properties 의 현재 줄 목록(개행 없는 문자열들).
        values:  기록할 프로퍼티키와 값의 매핑이며 사용자가 유지하려는 항목만 담는다.

    반환:
        기록할 새 줄 목록.
    """
    remaining = dict(values)
    out: list[str] = []

    for line in existing:
        stripped = line.strip()
        # 주석과 빈 줄은 그대로 두고 key=value 줄만 다룬다.
        if stripped and not stripped.startswith(("#", "!")):
            for sep in ("=", ":"):
                idx = line.find(sep)
                if idx >= 0:
                    k = line[:idx].strip()
                    if k in remaining:
                        out.append(f"{k}={remaining.pop(k)}")
                        line = None  # type: ignore[assignment]
                    break
        if line is not None:
            out.append(line)

    if remaining:
        if out and out[-1].strip():
            out.append("")
        if _APPEND_MARKER not in out:
            out.append(_APPEND_MARKER)
        for k, v in remaining.items():
            out.append(f"{k}={v}")

    return out


def write_config(config_dir: Path, persist: dict[str, str]) -> Path:
    """``persist`` 를 config.properties 에 반영해 저장하고 경로를 반환한다.

    기존 파일이 있으면 ``config.properties.bak`` 로 백업한 뒤,
    :func:`merge_properties_lines` 로 주석·순서를 보존한 채 갱신한다. curses 무관 —
    UI 의 저장 동작과 테스트가 이 함수를 공유한다.
    """
    props_path = config_dir / "config.properties"
    existing = (
        props_path.read_text(encoding="utf-8").splitlines() if props_path.is_file() else []
    )
    if props_path.is_file():
        backup = props_path.with_suffix(props_path.suffix + ".bak")
        backup.write_text("\n".join(existing) + "\n", encoding="utf-8")
    merged = merge_properties_lines(existing, persist)
    props_path.write_text("\n".join(merged) + "\n", encoding="utf-8")
    return props_path


def validate(fields: list[Field], values: dict[str, str]) -> list[tuple[str, str, str]]:
    """편집 중인 값들을 검증해 ``(심각도, 프로퍼티키, 메시지)`` 목록을 돌려준다.

    심각도는 ``error``(저장 차단)|``warn``(안내). 타입 불일치·enum 이탈·포트 범위는
    error 로 보고, 조건부 필수(예: store.backend 가 postgres 면 history.db_dsn 이 있어야 한다)와 URL 형식은 warn 으로 본다.
    """
    issues: list[tuple[str, str, str]] = []
    by_key = {f.prop_key: f for f in fields}

    def eff(key: str) -> str:
        """현재 유효한 값이다. 오버라이드가 없으면 기본값을 쓴다."""
        if key in values:
            return values[key]
        f = by_key.get(key)
        return f.default if f else ""

    for f in fields:
        val = values.get(f.prop_key)
        if val is None or val == "":
            continue
        if f.ftype == "int" and not re.fullmatch(r"-?\d+", val):
            issues.append(("error", f.prop_key, f"정수여야 함(입력: {val!r})"))
        elif f.ftype == "float" and not re.fullmatch(r"-?\d+(\.\d+)?", val):
            issues.append(("error", f.prop_key, f"숫자여야 함(입력: {val!r})"))
        elif f.ftype == "bool" and val not in ("true", "false"):
            issues.append(("error", f.prop_key, "true 또는 false 여야 함"))
        elif f.ftype == "enum" and val not in f.enum:
            issues.append(("error", f.prop_key, f"{'|'.join(f.enum)} 중 하나여야 함"))
        if f.prop_key.endswith(".port") and re.fullmatch(r"-?\d+", val):
            if not (1 <= int(val) <= 65535):
                issues.append(("error", f.prop_key, "포트는 1..65535"))
        # 범위를 아는 숫자 항목은 밖으로 나가면 막는다. 하한 아래는 대개 "조용히 멈춤"이라
        # 경고로 흘려보내면 기동 후에야 알게 된다.
        bounds = INT_BOUNDS.get(f.prop_key)
        if bounds and re.fullmatch(r"-?\d+", val):
            lo, hi, _ = bounds
            if not (lo <= int(val) <= hi):
                issues.append(("error", f.prop_key, f"{lo}..{hi} 범위여야 함(입력: {val})"))

    # 조건부 필수: postgres 저장소는 공유 DB DSN 이 있어야 한다.
    if eff("store.backend") == "postgres" and not eff("history.db_dsn"):
        issues.append(("warn", "history.db_dsn", "store.backend=postgres 는 history.db_dsn 이 필요"))
    # executor URL 목록의 형식을 점검한다.
    execs = eff("coordinator.executors")
    if execs:
        for u in [x.strip() for x in execs.split(",") if x.strip()]:
            if not u.startswith(("http://", "https://")):
                issues.append(("warn", "coordinator.executors", f"http(s):// URL 이어야 함: {u}"))
    # 동시성 손잡이들끼리 앞뒤가 맞는지 본다(기본값까지 반영한 유효값으로 판단).
    issues.extend(check_concurrency({k: eff(k) for k in CONCURRENCY_KEYS}))
    return issues


# ─────────────────────────────────────────────────────────────────────────────
# curses UI
# ─────────────────────────────────────────────────────────────────────────────


def _load_yaml_text(config_dir: Path) -> str:
    path = config_dir / "config.yml"
    if not path.is_file():
        raise FileNotFoundError(f"config.yml 을 찾을 수 없음: {path}")
    return path.read_text(encoding="utf-8")


class ConfigTUI:
    """섹션 탭과 스크롤 항목 목록, 편집과 저장을 담당하는 curses 애플리케이션이다.

    상태는 프로퍼티키별 오버라이드 값을 담은 ``self.values`` 에 모으고, 저장할 때
    :func:`merge_properties_lines` 로 config.properties 를 갱신한다.
    """

    def __init__(self, config_dir: Path, fields: list[Field], overrides: dict[str, str]):
        self.config_dir = config_dir
        self.fields = fields
        self.overrides = overrides                 # 파일에 원래 있던 오버라이드이며 저장의 기준선이 된다
        # 편집 중인 값은 파일에 있던 오버라이드에서 출발한다. 없으면 미설정이라 기본값을 쓴다.
        self.values: dict[str, str] = dict(overrides)
        # 동시성 가상 탭을 맨 앞에 둔다. 손잡이가 여러 섹션에 흩어져 있어 조정할 때
        # 탭을 옮겨 다녀야 했는데, 대개 TUI 를 여는 이유가 이 조정이라 첫 화면으로 삼는다.
        self._conc = [f for k in CONCURRENCY_KEYS for f in fields if f.prop_key == k]
        self.sections = list(dict.fromkeys(f.section for f in fields))
        if self._conc:
            self.sections.insert(0, CONCURRENCY_SECTION)
        self.tab = 0
        self.row = 0
        self.top = 0                               # 스크롤 오프셋이다
        self.status = (
            "↑↓ 이동  ←→ 탭  Enter 편집  스페이스 토글  +/- 증감  ? 설명  r 초기화  s 저장  q 종료"
        )
        self.dirty = False
        self.help_top = 0                          # 도움말 화면의 스크롤 위치다(열려 있을 때만 쓴다)
        self.help_open = False

    # 현재 탭에 속한 항목들을 모은다. 동시성 탭만 섹션이 아니라 키 목록으로 뽑는다.
    def _visible(self) -> list[Field]:
        if self.sections[self.tab] == CONCURRENCY_SECTION:
            return self._conc
        return [f for f in self.fields if f.section == self.sections[self.tab]]

    def _eff(self, f: Field) -> str:
        """유효한 값을 구한다. 오버라이드가 있으면 그 값을, 없으면 기본값을 쓴다."""
        return self.values.get(f.prop_key, f.default)

    def _is_override(self, f: Field) -> bool:
        return f.prop_key in self.values and self.values[f.prop_key] != f.default

    def run(self, stdscr: Any) -> bool:
        import curses

        curses.curs_set(0)
        stdscr.keypad(True)
        self._init_colors(curses)
        while True:
            if self.help_open:
                self._draw_help(stdscr, curses)
                ch = stdscr.getch()
                h = stdscr.getmaxyx()[0]
                if ch in (curses.KEY_DOWN, curses.KEY_NPAGE):
                    self.help_top += 1 if ch == curses.KEY_DOWN else max(1, h - 4)
                elif ch in (curses.KEY_UP, curses.KEY_PPAGE):
                    self.help_top = max(0, self.help_top - (1 if ch == curses.KEY_UP else max(1, h - 4)))
                else:                              # 그 밖의 키는 모두 닫기로 본다
                    self.help_open = False
                continue

            self._draw(stdscr, curses)
            ch = stdscr.getch()
            if ch in (ord("?"), ord("h")):
                self.help_open, self.help_top = True, 0
            elif ch in (ord("q"), 27):             # q / ESC
                if self.dirty and not self._confirm(stdscr, curses, "저장 안 함. 종료? (y/n)"):
                    continue
                return False
            elif ch in (curses.KEY_LEFT,):
                self.tab = (self.tab - 1) % len(self.sections)
                self.row = self.top = 0
            elif ch in (curses.KEY_RIGHT, ord("\t")):
                self.tab = (self.tab + 1) % len(self.sections)
                self.row = self.top = 0
            elif ch == curses.KEY_UP:
                self.row = max(0, self.row - 1)
            elif ch == curses.KEY_DOWN:
                self.row = min(len(self._visible()) - 1, self.row + 1)
            elif ch in (curses.KEY_PPAGE, curses.KEY_NPAGE):
                # 한 화면 단위 이동이다. 항목이 많은 Executor 탭에서 특히 쓸모 있다.
                page = max(1, stdscr.getmaxyx()[0] - 7)
                delta = -page if ch == curses.KEY_PPAGE else page
                self.row = max(0, min(len(self._visible()) - 1, self.row + delta))
            elif ch == curses.KEY_HOME:
                self.row = 0
            elif ch == curses.KEY_END:
                self.row = max(0, len(self._visible()) - 1)
            elif ch == ord(" "):
                self._toggle(self._visible()[self.row])
            elif ch in (ord("+"), ord("=")):
                self._step(self._visible()[self.row], +1)
            elif ch in (ord("-"), ord("_")):
                self._step(self._visible()[self.row], -1)
            elif ch in (curses.KEY_ENTER, 10, 13):
                self._edit(stdscr, curses, self._visible()[self.row])
            elif ch in (ord("r"),):
                self._reset(self._visible()[self.row])
            elif ch == ord("s"):
                if self._save(stdscr, curses):
                    return True

    def _init_colors(self, curses: Any) -> None:
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN, -1)     # 탭과 헤더에 쓴다
        curses.init_pair(2, curses.COLOR_YELLOW, -1)   # 오버라이드 표시에 쓴다
        curses.init_pair(3, curses.COLOR_GREEN, -1)    # 값에 쓴다
        curses.init_pair(4, curses.COLOR_RED, -1)      # 오류에 쓴다
        curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)  # 선택된 행에 쓴다

    def _toggle(self, f: Field) -> None:
        """bool 과 enum 항목을 다음 값으로 순환시킨다. 그 밖의 타입은 무시한다."""
        cur = self._eff(f)
        if f.ftype == "bool":
            self._set(f, "false" if cur == "true" else "true")
        elif f.ftype == "enum" and f.enum:
            nxt = f.enum[(f.enum.index(cur) + 1) % len(f.enum)] if cur in f.enum else f.enum[0]
            self._set(f, nxt)

    def _step(self, f: Field, direction: int) -> None:
        """숫자 항목을 한 스텝 증감한다. 숫자가 아닌 항목에서는 왜 안 되는지 알려 준다."""
        if f.ftype not in ("int", "float"):
            self.status = f"{f.prop_key} 은(는) 숫자가 아니라 +/- 로 조정할 수 없다(Enter 로 편집)"
            return
        new = step_value(f, self._eff(f), direction)
        if new == self._eff(f):
            lo, hi, _ = INT_BOUNDS.get(f.prop_key, (0, 0, 0))
            self.status = f"{f.prop_key}: 한계값({lo}..{hi})" if hi else self.status
            return
        self._set(f, new)

    def _set(self, f: Field, value: str) -> None:
        self.values[f.prop_key] = value
        self.dirty = True

    def _reset(self, f: Field) -> None:
        """항목을 기본값(오버라이드 제거)으로 되돌린다."""
        if f.prop_key in self.values:
            del self.values[f.prop_key]
            self.dirty = True

    def _edit(self, stdscr: Any, curses: Any, f: Field) -> None:
        """한 항목의 값을 인라인 입력받는다(enum/bool 은 토글 안내)."""
        if f.ftype in ("bool", "enum"):
            self._toggle(f)
            return
        h, w = stdscr.getmaxyx()
        prompt = f"{f.prop_key} = "
        curses.curs_set(1)
        curses.echo()
        stdscr.move(h - 1, 0)
        stdscr.clrtoeol()
        stdscr.addstr(h - 1, 0, cut(prompt, w - 1))
        try:
            raw = stdscr.getstr(h - 1, min(len(prompt), w - 1), max(1, w - len(prompt) - 2))
            new = raw.decode("utf-8", "replace").strip()
            self._set(f, new)
        except Exception:
            pass
        finally:
            curses.noecho()
            curses.curs_set(0)

    def _confirm(self, stdscr: Any, curses: Any, msg: str) -> bool:
        h, _ = stdscr.getmaxyx()
        stdscr.move(h - 1, 0)
        stdscr.clrtoeol()
        stdscr.addstr(h - 1, 0, msg, curses.color_pair(4))
        return stdscr.getch() in (ord("y"), ord("Y"))

    def _save(self, stdscr: Any, curses: Any) -> bool:
        """검증한 뒤 config.properties 를 백업하고 갱신한다. 성공하면 True 를 돌려준다."""
        # 파일에 원래 있던 키와 기본값에서 달라진 키를 기록 대상으로 삼는다.
        persist = {
            f.prop_key: self._eff(f)
            for f in self.fields
            if f.prop_key in self.overrides or self._is_override(f)
        }
        issues = validate(self.fields, persist)
        errors = [i for i in issues if i[0] == "error"]
        if errors:
            k, msg = errors[0][1], errors[0][2]
            self.status = f"저장 불가 — {k}: {msg}"
            return False
        props_path = write_config(self.config_dir, persist)
        warns = [i for i in issues if i[0] == "warn"]
        tail = f" (경고 {len(warns)}건)" if warns else ""
        self._confirm(
            stdscr, curses,
            f"저장됨: {props_path}{tail}  — 재시작해야 적용됩니다. 아무 키나 누르세요.",
        )
        return True

    def _draw_help(self, stdscr: Any, curses: Any) -> None:
        """선택한 항목의 설명을 화면 가득 그린다. 아무 키나 누르면 닫힌다."""
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        vis = self._visible()
        fld = vis[self.row] if vis else None
        if fld is None:
            self.help_open = False
            return

        lines = help_lines(fld, self._eff(fld), w - 4)
        body_h = h - 3
        # 끝을 넘겨 스크롤하면 빈 화면만 남으므로 마지막 화면에서 멈춘다.
        self.help_top = max(0, min(self.help_top, max(0, len(lines) - body_h)))

        stdscr.addstr(0, 0, cut(" 설정 설명 ", w - 1), curses.color_pair(1) | curses.A_BOLD)
        y = 2
        for line in lines[self.help_top : self.help_top + body_h]:
            if y >= h - 1:
                break
            # 소제목(■)과 키 이름은 눈에 띄게 둔다.
            attr = curses.color_pair(1) if line.startswith("■") else 0
            if line == fld.prop_key:
                attr = curses.color_pair(3) | curses.A_BOLD
            stdscr.addstr(y, 2, cut(line, w - 3), attr)
            y += 1

        more = "  (↑↓/PgUp/PgDn 스크롤)" if len(lines) > body_h else ""
        stdscr.addstr(h - 1, 0, cut(f" 아무 키나 누르면 닫힘{more}", w - 1), curses.A_REVERSE)
        stdscr.refresh()

    def _draw(self, stdscr: Any, curses: Any) -> None:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        # 타이틀과 탭 바를 그린다.
        title = " Distributed Query Executor — 설정 "
        stdscr.addstr(0, 0, cut(title, w - 1), curses.color_pair(1) | curses.A_BOLD)
        # 탭이 화면 폭을 넘으면 앞에서부터 잘라 낸다. 그대로 두면 뒤쪽 탭(Executor 등)이
        # 보이지 않아 그 탭에 있는 동안 어디인지 알 수 없다. 선택한 탭이 항상 보이도록
        # 시작 위치를 밀고, 잘린 쪽에는 화살표로 더 있음을 알린다.
        labels = [f" {SECTION_LABELS.get(s, s)} " for s in self.sections]
        start = 0
        while start < self.tab and sum(len(labels[i]) + 1 for i in range(start, self.tab + 1)) >= w - 2:
            start += 1
        x = 0
        if start > 0:
            stdscr.addstr(1, 0, "‹", curses.color_pair(1))
            x = 2
        for i in range(start, len(labels)):
            if x + len(labels[i]) >= w - 1:
                stdscr.addstr(1, min(x, w - 2), "›", curses.color_pair(1))
                break
            attr = curses.color_pair(5) if i == self.tab else curses.color_pair(1)
            stdscr.addstr(1, x, labels[i], attr)
            x += len(labels[i]) + 1

        # 항목 목록(스크롤). 하단은 설명·상태에 남기고, 동시성 탭은 유도값 요약에 더 쓴다.
        vis = self._visible()
        conc = self.sections[self.tab] == CONCURRENCY_SECTION
        summary = concurrency_summary({f.prop_key: self._eff(f) for f in self._conc}) if conc else []
        list_h = h - 6 - len(summary)
        if self.row < self.top:
            self.top = self.row
        elif self.row >= self.top + list_h:
            self.top = self.row - list_h + 1

        last_group = None
        y = 3
        for i in range(self.top, min(len(vis), self.top + list_h)):
            f = vis[i]
            # 동시성 탭은 섹션이 섞여 있어 그룹 헤더가 뜻을 잃는다(키 자체가 이미 한정돼 있다).
            if not conc and f.group != last_group and f.group:
                if y < h - 3:
                    stdscr.addstr(y, 1, f"[{f.group}]", curses.color_pair(1))
                    y += 1
                last_group = f.group
            elif not f.group or conc:
                last_group = None
            if y >= h - 3:
                break
            selected = i == self.row
            key_txt = f.prop_key
            val_txt = display_value(f, self._eff(f))
            marker = "●" if self._is_override(f) else " "
            row_attr = curses.color_pair(5) if selected else 0
            # 키 열은 가장 긴 키(coordinator.max_dispatch_concurrency, 36자)에 맞춘다.
            # 폭은 글자 수가 아니라 화면 칸 수로 재야 값 열이 어긋나지 않는다.
            line = f" {marker} {pad(key_txt, 36)} {val_txt}"
            stdscr.addstr(y, 0, cut(line, w - 1), row_attr)
            if not selected and self._is_override(f):
                stdscr.addstr(y, 1, marker, curses.color_pair(2))
            y += 1

        # 동시성 탭은 손잡이에서 유도되는 실제 처리량을 항목 목록 아래에 붙여 둔다.
        # 값을 바꿀 때마다 곱셈 결과가 즉시 갱신되므로 조정한 효과를 눈으로 확인할 수 있다.
        for i, line in enumerate(summary):
            sy = h - 4 - len(summary) + i
            if 3 <= sy < h - 3:
                stdscr.addstr(sy, 1, cut(line, w - 2), curses.color_pair(3))

        # 선택한 항목의 설명 패널을 그린다.
        cur = vis[self.row] if vis else None
        if cur:
            desc = cur.help_inline or cur.help_long
            base = f"기본값: {cur.default or '(빈 값)'}"
            if cur.enum:
                base += f"   값: {'|'.join(cur.enum)}"
            elif cur.prop_key in INT_BOUNDS:
                lo, hi, step = INT_BOUNDS[cur.prop_key]
                base += f"   범위: {lo}..{hi}   +/- 스텝: {step}"
            stdscr.addstr(h - 3, 0, cut(base, w - 1), curses.color_pair(3))
            if desc:
                stdscr.addstr(h - 2, 0, cut(desc, w - 1))

        stdscr.addstr(h - 1, 0, cut(self.status, w - 1), curses.A_REVERSE)
        stdscr.refresh()


def run_tui(config_dir: Path) -> bool:
    """curses TUI 를 띄운다. 저장하면 True 를, 취소하면 False 를 돌려준다."""
    import curses

    yaml_text = _load_yaml_text(config_dir)
    fields = parse_schema(yaml_text)
    props_path = config_dir / "config.properties"
    overrides = load_properties(props_path)
    # 스키마에 있는 키만 편집 대상으로 남긴다(오타/미지 키는 그대로 보존).
    known = {f.prop_key for f in fields}
    overrides = {k: v for k, v in overrides.items() if k in known}
    app = ConfigTUI(config_dir, fields, overrides)
    return curses.wrapper(app.run)


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점이다. ``python -m core.config_tui`` 나 ``bin/config-tui`` 로 실행한다."""
    parser = argparse.ArgumentParser(
        prog="config-tui",
        description="config.properties 를 편집하는 curses 설정 TUI(스키마는 config.yml 에서 자동).",
    )
    parser.add_argument(
        "--config-dir",
        default=os.environ.get("QUERY_EXECUTOR_CONFIG_DIR") or str(DEFAULT_CONFIG_DIR),
        help="설정 디렉터리(기본: $QUERY_EXECUTOR_CONFIG_DIR 또는 %(default)s)",
    )
    args = parser.parse_args(argv)
    config_dir = Path(args.config_dir)

    if not (config_dir / "config.yml").is_file():
        print(f"config.yml 이 없습니다: {config_dir}", file=sys.stderr)
        return 2
    if not sys.stdout.isatty():
        print("TTY 가 아닙니다 — 대화형 터미널에서 실행하세요.", file=sys.stderr)
        return 2

    saved = run_tui(config_dir)
    print("저장되었습니다. 서비스를 재시작하면 적용됩니다." if saved else "변경을 저장하지 않았습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
