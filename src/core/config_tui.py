"""config.properties 를 편집하는 curses 기반 전체화면 설정 TUI.

## 왜 이렇게 만드나

이 저장소의 설정은 두 파일로 나뉜다(자세히는 ``core.config_loader``):

* ``config.yml``  — **구조·기본값·설명**. 각 값은 ``${키:기본값}`` 자리표시자이고,
  YAML 중첩이 곧 섹션 그룹이며, 줄 끝 ``# 주석`` 이 곧 그 항목의 설명이다.
* ``config.properties`` — 운영자가 실제로 편집하는 **오버라이드 파일**(flat key=value).

따라서 TUI 는 **스키마를 코드에 하드코딩하지 않는다.** ``config.yml`` 을 파싱해
항목 목록·섹션·기본값·도움말을 그대로 뽑아 화면을 구성하고(:func:`parse_schema`),
사용자가 바꾼 값만 ``config.properties`` 에 주석·순서를 보존한 채 diff-write 한다
(:func:`merge_properties_lines`). 설정 항목이 늘어도 YAML 만 고치면 TUI 가 자동으로 따라간다.

## 구성

* 순수 로직(테스트 대상, curses 무관): :func:`parse_schema`,
  :func:`merge_properties_lines`, :func:`validate`, :func:`infer_type`,
  :func:`display_value`.
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

from core.config_loader import DEFAULT_CONFIG_DIR, load_properties
from core.masking import mask_dsn

# ``${변수:기본값}`` 자리표시자(config_loader 와 동일 규칙). group(1)=변수명, group(2)=기본값.
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

# 사람이 읽기 좋은 섹션 라벨(YAML 최상위 키 → 탭 이름). 없으면 키를 그대로 쓴다.
SECTION_LABELS: dict[str, str] = {
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
    """기본값 문자열에서 항목 타입을 추론한다: ``bool``|``int``|``float``|``str``.

    ``true``/``false`` → bool, 정수/실수 리터럴 → int/float, 그 외 → str.
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
    """설정 항목 하나의 메타데이터(config.yml 한 줄에서 추출)."""

    prop_key: str                       # config.properties 키(= 자리표시자 변수명)
    section: str                        # 소속 최상위 섹션(YAML 최상위 키)
    path: list[str]                     # YAML 중첩 경로(그룹 헤더 표시용)
    default: str                        # ${...:기본값} 의 기본값(콜론 없으면 "")
    has_default: bool                   # 자리표시자에 기본값(콜론)이 명시됐는지
    help_inline: str = ""               # 줄 끝 주석(짧은 설명)
    help_long: str = ""                 # 항목 위 주석 줄(긴 설명, 여러 줄 가능)
    ftype: str = "str"                  # bool|int|float|str
    enum: list[str] = dataclass_field(default_factory=list)  # 값 후보(있으면 토글/선택)
    secret: bool = False                # 비밀값(화면 마스킹)

    @property
    def group(self) -> str:
        """섹션 안 하위 그룹 라벨(예: [executor,impala,host] → 'impala'). 최상위면 "".

        path 의 첫 원소는 섹션, 마지막 원소는 이 항목의 YAML 키이므로 그 사이가 그룹이다.
        """
        return ".".join(self.path[1:-1])


# 설명으로 쓰기엔 장식일 뿐인 배너 주석(─── 류)을 걸러내는 판정.
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
    stack: list[tuple[int, str]] = []   # (indent, key) — 현재 열려 있는 상위 매핑들
    pending: list[str] = []             # 다음 항목에 붙일 위쪽 주석 줄 버퍼

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

        # rest 에서 값과 줄 끝 주석을 분리한다. 값이 ${...} 면 닫는 '}' 뒤의 '#' 만 주석.
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


# diff-write 로 추가되는 키 앞에 붙이는 표식(중복 추가 방지용으로도 쓴다).
_APPEND_MARKER = "# ─── config-tui 로 추가된 설정 ───"


def merge_properties_lines(existing: list[str], values: dict[str, str]) -> list[str]:
    """기존 properties 줄들에 ``values`` 를 반영한 **새 줄 목록**을 만든다(주석·순서 보존).

    * 파일에 이미 있는 키는 **제자리에서 값만** 교체한다(주석·배치 유지).
    * 파일에 없는 키는 끝에 :data:`_APPEND_MARKER` 아래로 추가한다(마커는 1회만).
    * ``values`` 에 없는 기존 줄은 건드리지 않는다.

    인자:
        existing: config.properties 의 현재 줄 목록(개행 없는 문자열들).
        values:  기록할 ``프로퍼티키 → 값`` 매핑(사용자가 유지하려는 항목만).

    반환:
        기록할 새 줄 목록.
    """
    remaining = dict(values)
    out: list[str] = []

    for line in existing:
        stripped = line.strip()
        # 주석/빈 줄은 그대로. key=value 줄만 대상.
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
    """편집 중인 값들을 검증해 ``(심각도, 프로퍼티키, 메시지)`` 목록을 반환한다.

    심각도는 ``error``(저장 차단)|``warn``(안내). 타입 불일치·enum 이탈·포트 범위는
    error, 조건부 필수(예: store.backend=postgres → history.db_dsn)·URL 형식은 warn.
    """
    issues: list[tuple[str, str, str]] = []
    by_key = {f.prop_key: f for f in fields}

    def eff(key: str) -> str:
        """현재 유효 값(오버라이드 없으면 기본값)."""
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

    # 조건부 필수: postgres 저장소는 공유 DB DSN 이 있어야 한다.
    if eff("store.backend") == "postgres" and not eff("history.db_dsn"):
        issues.append(("warn", "history.db_dsn", "store.backend=postgres 는 history.db_dsn 이 필요"))
    # executor URL 목록 형식 점검.
    execs = eff("coordinator.executors")
    if execs:
        for u in [x.strip() for x in execs.split(",") if x.strip()]:
            if not u.startswith(("http://", "https://")):
                issues.append(("warn", "coordinator.executors", f"http(s):// URL 이어야 함: {u}"))
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
    """섹션 탭 + 스크롤 항목 목록 + 편집/저장을 담당하는 curses 애플리케이션.

    상태는 ``self.values``(프로퍼티키 → 오버라이드 값)에 모으고, 저장 시
    :func:`merge_properties_lines` 로 config.properties 를 갱신한다.
    """

    def __init__(self, config_dir: Path, fields: list[Field], overrides: dict[str, str]):
        self.config_dir = config_dir
        self.fields = fields
        self.overrides = overrides                 # 파일에 원래 있던 오버라이드(저장 기준선)
        # 편집 중 값: 파일에 있던 오버라이드로 시작. 없으면 미설정(기본값 사용).
        self.values: dict[str, str] = dict(overrides)
        self.sections = list(dict.fromkeys(f.section for f in fields))
        self.tab = 0
        self.row = 0
        self.top = 0                               # 스크롤 오프셋
        self.status = "↑↓ 이동  ←→ 탭  Enter 편집  스페이스 토글  s 저장  q 종료"
        self.dirty = False

    # 현재 탭의 항목들.
    def _visible(self) -> list[Field]:
        return [f for f in self.fields if f.section == self.sections[self.tab]]

    def _eff(self, f: Field) -> str:
        """유효 값: 오버라이드가 있으면 그 값, 없으면 기본값."""
        return self.values.get(f.prop_key, f.default)

    def _is_override(self, f: Field) -> bool:
        return f.prop_key in self.values and self.values[f.prop_key] != f.default

    def run(self, stdscr: Any) -> bool:
        import curses

        curses.curs_set(0)
        stdscr.keypad(True)
        self._init_colors(curses)
        while True:
            self._draw(stdscr, curses)
            ch = stdscr.getch()
            if ch in (ord("q"), 27):               # q / ESC
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
            elif ch == ord(" "):
                self._toggle(self._visible()[self.row])
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
        curses.init_pair(1, curses.COLOR_CYAN, -1)     # 탭/헤더
        curses.init_pair(2, curses.COLOR_YELLOW, -1)   # 오버라이드 표시
        curses.init_pair(3, curses.COLOR_GREEN, -1)    # 값
        curses.init_pair(4, curses.COLOR_RED, -1)      # 오류
        curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)  # 선택 행

    def _toggle(self, f: Field) -> None:
        """bool/enum 항목을 다음 값으로 순환. 그 외에는 무시."""
        cur = self._eff(f)
        if f.ftype == "bool":
            self._set(f, "false" if cur == "true" else "true")
        elif f.ftype == "enum" and f.enum:
            nxt = f.enum[(f.enum.index(cur) + 1) % len(f.enum)] if cur in f.enum else f.enum[0]
            self._set(f, nxt)

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
        stdscr.addstr(h - 1, 0, prompt[: w - 1])
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
        """검증 후 config.properties 를 백업하고 갱신한다. 성공 시 True."""
        # 기록 대상: 파일에 원래 있던 키 + 기본값과 달라진 키.
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

    def _draw(self, stdscr: Any, curses: Any) -> None:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        # 타이틀 + 탭 바
        title = " Distributed Query Executor — 설정 "
        stdscr.addstr(0, 0, title[: w - 1], curses.color_pair(1) | curses.A_BOLD)
        x = 0
        for i, sec in enumerate(self.sections):
            label = f" {SECTION_LABELS.get(sec, sec)} "
            if x + len(label) >= w:
                break
            attr = curses.color_pair(5) if i == self.tab else curses.color_pair(1)
            stdscr.addstr(1, x, label, attr)
            x += len(label) + 1

        # 항목 목록(스크롤). 하단 4줄은 설명·상태에 남긴다.
        vis = self._visible()
        list_h = h - 6
        if self.row < self.top:
            self.top = self.row
        elif self.row >= self.top + list_h:
            self.top = self.row - list_h + 1

        last_group = None
        y = 3
        for i in range(self.top, min(len(vis), self.top + list_h)):
            f = vis[i]
            if f.group != last_group and f.group:
                if y < h - 3:
                    stdscr.addstr(y, 1, f"[{f.group}]", curses.color_pair(1))
                    y += 1
                last_group = f.group
            elif not f.group:
                last_group = None
            if y >= h - 3:
                break
            selected = i == self.row
            key_txt = f.prop_key
            val_txt = display_value(f, self._eff(f))
            marker = "●" if self._is_override(f) else " "
            row_attr = curses.color_pair(5) if selected else 0
            line = f" {marker} {key_txt:<34} {val_txt}"
            stdscr.addstr(y, 0, line[: w - 1], row_attr)
            if not selected and self._is_override(f):
                stdscr.addstr(y, 1, marker, curses.color_pair(2))
            y += 1

        # 설명 패널(선택 항목).
        cur = vis[self.row] if vis else None
        if cur:
            desc = cur.help_inline or cur.help_long
            base = f"기본값: {cur.default or '(빈 값)'}"
            if cur.enum:
                base += f"   값: {'|'.join(cur.enum)}"
            stdscr.addstr(h - 3, 0, base[: w - 1], curses.color_pair(3))
            if desc:
                stdscr.addstr(h - 2, 0, desc[: w - 1])

        stdscr.addstr(h - 1, 0, self.status[: w - 1], curses.A_REVERSE)
        stdscr.refresh()


def run_tui(config_dir: Path) -> bool:
    """curses TUI 를 띄운다. 저장하면 True, 취소면 False."""
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
    """CLI 진입점. ``python -m core.config_tui`` / ``bin/config-tui``."""
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
