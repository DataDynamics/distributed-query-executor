"""부하 시나리오의 순수 로직이다. 시간 표기 해석, ramp-up 단계 계산, 요청 본문 치환을 맡는다.

네트워크나 asyncio 와 무관하므로 단위 테스트로 계산을 그대로 검증할 수 있다.

**VU 수 곡선은 단계(stage)의 연속으로 표현한다.** 각 단계는 "이 시간 동안 목표 VU 수까지 선형으로
이동한다"는 뜻이며, 첫 단계는 0 VU 에서 출발한다. JMeter 식 ``--vus 20 --ramp-up 60s --duration
10m`` 은 ``[60s → 20, 540s → 20]`` 두 단계로 바뀐다(duration 은 ramp-up 을 포함한 전체 시간이다).
k6 식 ``--stages 30s:5,2m:5,1m:20`` 은 적은 그대로 단계가 된다.
"""

from __future__ import annotations

import copy
import csv
import json
import math
import random
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)(ms|h|m|s)")


def parse_duration(text: Any) -> float:
    """``90``·``30s``·``2m``·``1h``·``1m30s``·``500ms`` 같은 시간 표기를 초로 바꾼다.

    단위가 없는 숫자는 초로 본다. 명령행과 YAML 양쪽에서 같은 표기를 쓰기 위해 한 곳에 둔다.
    """
    if isinstance(text, (int, float)):
        if text < 0:
            raise ValueError(f"시간은 음수일 수 없습니다: {text}")
        return float(text)
    s = str(text).strip().lower()
    if not s:
        raise ValueError("시간 값이 비어 있습니다")
    try:
        value = float(s)
    except ValueError:
        pass
    else:
        if value < 0:
            raise ValueError(f"시간은 음수일 수 없습니다: {text}")
        return value
    pos = 0
    total = 0.0
    for m in _DURATION_PART.finditer(s):
        if m.start() != pos:
            break
        n = float(m.group(1))
        unit = m.group(2)
        total += n * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[unit]
        pos = m.end()
    if pos != len(s):
        raise ValueError(f"시간 표기를 해석할 수 없습니다: {text!r} (예: 30s, 2m, 1m30s, 500ms)")
    return total


def format_duration(seconds: float) -> str:
    """초를 ``1h02m03s`` 처럼 짧게 표기한다. 보고서의 설정 요약에 쓴다."""
    if seconds == math.inf:
        return "∞"
    if seconds < 60:
        return f"{seconds:g}s"
    total = int(round(seconds))
    h, rest = divmod(total, 3600)
    m, s = divmod(rest, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}m{s:02d}s"


@dataclass(frozen=True)
class Stage:
    """``duration`` 초 동안 VU 수를 직전 목표에서 ``target`` 까지 선형으로 옮기는 한 구간이다.

    ``duration`` 이 무한대면 그 목표를 끝없이 유지한다. ``--iterations`` 만 주고 ``--duration``
    을 주지 않았을 때 VU 들이 반복을 다 채울 때까지 기다리기 위해 쓴다.
    """

    duration: float
    target: int


def parse_stages(text: str) -> List[Stage]:
    """``30s:5,2m:10,30s:0`` 형태를 단계 목록으로 바꾼다."""
    stages = []
    for part in str(text).split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"단계는 '<시간>:<VU수>' 형식이어야 합니다: {part!r}")
        dur, target = part.rsplit(":", 1)
        try:
            n = int(target)
        except ValueError:
            raise ValueError(f"단계의 VU 수가 정수가 아닙니다: {part!r}") from None
        if n < 0:
            raise ValueError(f"단계의 VU 수는 음수일 수 없습니다: {part!r}")
        stages.append(Stage(parse_duration(dur), n))
    if not stages:
        raise ValueError("단계가 하나도 없습니다")
    return stages


def stages_from_ramp(vus: int, ramp_up: float, duration: Optional[float]) -> List[Stage]:
    """JMeter 식 설정(VU 수, ramp-up, 전체 시간)을 단계 목록으로 바꾼다.

    ``duration`` 은 ramp-up 을 포함한 전체 시간이다. None 이면 목표 VU 를 무한히 유지하며,
    이때 종료는 ``--iterations`` 에 맡긴다.
    """
    if vus <= 0:
        raise ValueError("VU 수는 1 이상이어야 합니다")
    # ramp-up 이 0 이면 길이 0 인 단계로 목표까지 곧바로 뛴다. 이 단계가 없으면 유지 단계가
    # 0 에서 출발하는 선형 구간이 되어 전체 시간에 걸쳐 VU 가 천천히 늘어난다.
    stages = [Stage(ramp_up, vus)]
    if duration is None:
        stages.append(Stage(math.inf, vus))
    else:
        if duration < ramp_up:
            raise ValueError(
                f"duration({format_duration(duration)})이 ramp-up({format_duration(ramp_up)})보다 "
                "짧습니다. duration 은 ramp-up 을 포함한 전체 시간입니다."
            )
        hold = duration - ramp_up
        if hold > 0 or not stages:
            stages.append(Stage(hold, vus))
    return stages


def total_duration(stages: Sequence[Stage]) -> float:
    """모든 단계의 시간 합이다. 무한 단계가 있으면 무한대다."""
    return sum(s.duration for s in stages)


def max_vus(stages: Sequence[Stage]) -> int:
    return max((s.target for s in stages), default=0)


def target_vus(stages: Sequence[Stage], t: float) -> int:
    """경과 시간 ``t`` 초에 떠 있어야 할 VU 수다.

    늘어나는 구간은 올림, 줄어드는 구간은 내림으로 정수화한다. 그래야 ramp-up 이 JMeter 처럼
    첫 VU 를 거의 곧바로 띄우고(0 에서 20 까지 60초면 3초 간격), ramp-down 도 구간이 끝나기
    전에 한 명씩 먼저 빠진다. 모든 단계를 지나면 마지막 목표를 유지한다.
    """
    prev = 0
    elapsed = 0.0
    for st in stages:
        if st.duration == math.inf or t < elapsed + st.duration:
            if st.duration == math.inf or st.duration <= 0:
                return st.target
            frac = (t - elapsed) / st.duration
            v = prev + (st.target - prev) * frac
            if st.target >= prev:
                return int(math.ceil(v - 1e-9))
            return int(math.floor(v + 1e-9))
        elapsed += st.duration
        prev = st.target
    return prev


# ───────── 요청 본문 치환 ─────────

_PLACEHOLDER = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")
# 문자열 전체가 이 자리표시자 하나뿐이면 문자열이 아니라 값의 타입 그대로 넣는다. 정수 변수는
# "parallelism": "${vu}" 가 정수가 되게 하고, data 는 JSON 데이터 파일의 숫자·목록이 그대로 들어가게
# 한다(CSV 는 원래 문자열이라 문자열로 들어간다).
_INT_VARS = ("vu", "iter", "seq", "randint", "data")

_DATE_FORMAT = "%Y-%m-%d"
_DATE_EXPR = re.compile(r"^(today|\d{4}-\d{2}-\d{2})\s*(?:([+-])\s*(\d+))?$")


@dataclass
class RenderContext:
    """자리표시자에 채울 반복별 값이다.

    ``row`` 는 데이터 파일에서 이번 요청 몫으로 고른 행이다. 한 요청 안의 ``${data:..}`` 는 모두 같은
    행을 보므로, 시작일·종료일처럼 서로 맞아야 하는 값을 한 행에 함께 두면 짝이 어긋나지 않는다.
    """

    vu: int
    iteration: int
    seq: int
    run_id: str
    rng: random.Random
    row: Optional[Dict[str, Any]] = None
    today: Optional[date] = None


def _parse_date_expr(expr: str, today: date) -> date:
    """``today``·``today-7``·``2024-01-01``·``2024-01-01+3`` 을 날짜로 바꾼다(단위는 일)."""
    m = _DATE_EXPR.match(expr.strip())
    if not m:
        raise ValueError(f"날짜 표현이 아닙니다: {expr!r} (예: today, today-7, 2024-01-01)")
    base = today if m.group(1) == "today" else datetime.strptime(m.group(1), _DATE_FORMAT).date()
    if m.group(3):
        days = int(m.group(3))
        base += timedelta(days=days if m.group(2) == "+" else -days)
    return base


def _resolve_date(arg: Optional[str], ctx: RenderContext) -> str:
    """``${date:<날짜>[~<날짜>][|<형식>]}`` 을 푼다. ``~`` 로 구간을 주면 양끝을 포함해 무작위로 고른다."""
    spec, _, fmt = (arg or "today").partition("|")
    today = ctx.today or date.today()
    lo_s, sep, hi_s = spec.partition("~")
    lo = _parse_date_expr(lo_s, today)
    if sep:
        hi = _parse_date_expr(hi_s, today)
        if hi < lo:
            raise ValueError(f"${{date:{spec}}} 의 끝이 시작보다 앞섭니다")
        lo += timedelta(days=ctx.rng.randint(0, (hi - lo).days))
    return lo.strftime(fmt or _DATE_FORMAT)


def _resolve_choice(arg: Optional[str], ctx: RenderContext) -> Any:
    """``${choice:a,b,c}`` 는 하나를, ``${choice:a,b,c|2}``·``${choice:a,b,c|1-3}`` 는 여러 개를 고른다.

    여러 개는 중복 없이 뽑고 후보에 적은 순서대로 돌려준다. 개수를 범위로 주면 요청마다 개수도
    무작위다. 목록을 돌려주므로, 문자열 전체가 이 자리표시자 하나뿐이면 JSON 배열로 들어가고(IN 값
    목록 파라미터) 다른 글자와 섞이면 쉼표로 이어 붙인다.
    """
    spec, sep, count = (arg or "").partition("|")
    items = [x.strip() for x in spec.split(",") if x.strip()]
    if not items:
        raise ValueError("${choice:a,b,c} 에 후보가 없습니다")
    if not sep:
        return ctx.rng.choice(items)
    try:
        lo_s, _, hi_s = count.strip().partition("-")
        lo = int(lo_s)
        hi = int(hi_s) if hi_s else lo
    except ValueError:
        raise ValueError("${choice:a,b,c|<개수>} 또는 ${choice:a,b,c|<최소>-<최대>} 형식이어야 합니다"
                         ) from None
    if not 1 <= lo <= hi <= len(items):
        raise ValueError(f"${{choice:..|{count.strip()}}} 의 개수는 1 이상 후보 수({len(items)}) "
                         "이하여야 합니다")
    picked = set(ctx.rng.sample(range(len(items)), ctx.rng.randint(lo, hi)))
    return [v for i, v in enumerate(items) if i in picked]


def _resolve_data(arg: Optional[str], ctx: RenderContext) -> Any:
    if ctx.row is None:
        raise ValueError(f"${{data:{arg or ''}}} 를 쓰려면 데이터 파일(--data)이 필요합니다")
    if not arg:
        raise ValueError("${data:<컬럼>} 형식이어야 합니다")
    if arg not in ctx.row:
        cols = ", ".join(map(str, ctx.row)) or "(없음)"
        raise ValueError(f"데이터 파일에 {arg!r} 컬럼이 없습니다 (있는 컬럼: {cols})")
    return copy.deepcopy(ctx.row[arg])


def _resolve(name: str, arg: Optional[str], ctx: RenderContext) -> Any:
    if name == "vu":
        return ctx.vu
    if name == "iter":
        return ctx.iteration
    if name == "seq":
        return ctx.seq
    if name == "run_id":
        return ctx.run_id
    if name == "uuid":
        return uuid.UUID(int=ctx.rng.getrandbits(128), version=4).hex
    if name == "now":
        return datetime.now().strftime(arg or "%Y%m%d%H%M%S")
    if name == "choice":
        return _resolve_choice(arg, ctx)
    if name == "randint":
        try:
            lo, hi = (int(x) for x in (arg or "").split("-", 1))
        except ValueError:
            raise ValueError("${randint:<최소>-<최대>} 형식이어야 합니다") from None
        return ctx.rng.randint(lo, hi)
    if name == "date":
        return _resolve_date(arg, ctx)
    if name == "data":
        return _resolve_data(arg, ctx)
    raise ValueError(
        f"알 수 없는 자리표시자 ${{{name}}} "
        "(vu·iter·seq·run_id·uuid·now·choice·randint·date·data 중 하나)"
    )


def _render_str(s: str, ctx: RenderContext) -> Any:
    whole = _PLACEHOLDER.fullmatch(s)
    if whole and (whole.group(1) in _INT_VARS
                  or (whole.group(1) == "choice" and "|" in (whole.group(2) or ""))):
        return _resolve(whole.group(1), whole.group(2), ctx)
    return _PLACEHOLDER.sub(lambda m: _as_text(_resolve(m.group(1), m.group(2), ctx)), s)


def _as_text(value: Any) -> str:
    """문자열 중간에 들어가는 값이다. 여러 개를 고른 choice 같은 목록은 쉼표로 잇는다."""
    if isinstance(value, list):
        return ",".join(map(str, value))
    return str(value)


def render_body(template: Any, ctx: RenderContext) -> Any:
    """요청 본문 JSON 을 훑어 문자열 안의 ``${...}`` 를 반복별 값으로 바꾼다.

    JSON 텍스트가 아니라 파싱된 구조를 치환하므로 값에 따옴표가 들어가도 JSON 이 깨지지 않는다.
    키는 건드리지 않고 값만 치환하며 원본은 바꾸지 않는다.
    """
    if isinstance(template, dict):
        return {k: render_body(v, ctx) for k, v in template.items()}
    if isinstance(template, list):
        return [render_body(v, ctx) for v in template]
    if isinstance(template, str) and "${" in template:
        return _render_str(template, ctx)
    return copy.copy(template)


def uses_data(template: Any) -> bool:
    """본문 어딘가에 ``${data:..}`` 자리표시자가 있는지다. 데이터 파일이 필요한지 가르는 데 쓴다."""
    if isinstance(template, dict):
        return any(uses_data(v) for v in template.values())
    if isinstance(template, list):
        return any(uses_data(v) for v in template)
    return isinstance(template, str) and any(
        m.group(1) == "data" for m in _PLACEHOLDER.finditer(template))


def validate_body(template: Any, rows: Optional[Sequence[Dict[str, Any]]] = None) -> None:
    """시작 전에 렌더해 잘못된 자리표시자를 부하를 걸기 전에 잡는다.

    데이터 파일이 있으면 모든 행으로 한 번씩 렌더한다. JSON 데이터는 행마다 키가 다를 수 있어서,
    첫 행만 보면 부하 도중에 컬럼 누락으로 요청이 깨지는 경우를 놓친다.
    """
    rng = random.Random(0)
    for i, row in enumerate(rows or [None]):
        try:
            render_body(template, RenderContext(1, 0, 0, "check", rng, row=row))
        except ValueError as exc:
            if row is None:
                raise
            raise ValueError(f"데이터 {i + 1}번째 행: {exc}") from None


# ───────── 데이터 파일 ─────────

DATA_MODES = ("sequential", "random")


def load_data_file(path: str) -> List[Dict[str, Any]]:
    """요청마다 하나씩 꺼내 쓸 행 목록을 읽는다. 형식은 확장자로 정한다.

    - ``.csv`` 는 첫 줄이 컬럼 이름인 CSV 다. 값은 모두 문자열이다(UTF-8, BOM 허용).
    - ``.jsonl``/``.ndjson`` 은 한 줄에 객체 하나다.
    - ``.json``/``.yml``/``.yaml`` 은 객체의 배열이다.

    JSON 계열은 값의 타입이 살아 있어서 숫자나 목록(IN 값 목록 등)을 그대로 넣을 수 있다.
    """
    lower = path.lower()
    with open(path, encoding="utf-8-sig", newline="") as f:
        if lower.endswith(".csv"):
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError(f"CSV 에 머리줄(컬럼 이름)이 없습니다: {path}")
            rows: Any = [dict(r) for r in reader]
            for i, r in enumerate(rows, start=2):
                if None in r or any(v is None for v in r.values()):
                    raise ValueError(f"CSV {i}번째 줄의 열 개수가 머리줄과 다릅니다: {path}")
        elif lower.endswith((".jsonl", ".ndjson")):
            rows = [json.loads(line) for line in f if line.strip()]
        elif lower.endswith(".json"):
            rows = json.load(f)
        elif lower.endswith((".yml", ".yaml")):
            import yaml  # 순수 로직 모듈이 PyYAML 없이도 임포트되도록 여기서만 쓴다
            rows = yaml.safe_load(f)
        else:
            raise ValueError(f"데이터 파일 확장자는 .csv·.jsonl·.json·.yml 중 하나여야 합니다: {path}")
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise ValueError(f"데이터 파일은 객체(행)의 목록이어야 합니다: {path}")
    if not rows:
        raise ValueError(f"데이터 파일에 행이 없습니다: {path}")
    return rows


class DataFeed:
    """요청마다 데이터 파일의 행 하나를 내준다.

    ``sequential`` 은 실행 전체에서 커서 하나로 차례대로 돌고, 끝에 닿으면 처음으로 돌아간다. VU 가
    여럿이어도 같은 커서를 나눠 쓰므로 행이 고르게 쓰인다. ``random`` 은 매번 무작위로 고른다.
    """

    def __init__(self, rows: Sequence[Dict[str, Any]], mode: str = "sequential") -> None:
        if mode not in DATA_MODES:
            raise ValueError(f"data_mode 는 {'/'.join(DATA_MODES)} 중 하나여야 합니다: {mode!r}")
        if not rows:
            raise ValueError("데이터 행이 없습니다")
        self.rows = list(rows)
        self.mode = mode
        self._next = 0

    def next(self, rng: random.Random) -> Dict[str, Any]:
        if self.mode == "random":
            return rng.choice(self.rows)
        row = self.rows[self._next % len(self.rows)]
        self._next += 1
        return row


def max_target_after(stages: Sequence[Stage], t: float) -> int:
    """경과 시간 ``t`` 이후 남은 일정에서 목표 VU 수가 가장 클 때의 값이다.

    ``--iterations`` 로 끝낼 때 "앞으로 VU 를 더 띄울 일이 있는가"를 판단하는 데 쓴다. 선형
    구간의 최댓값은 양 끝에서 나오므로 현재 값과 아직 끝나지 않은 단계들의 목표만 보면 된다.
    """
    best = target_vus(stages, t)
    elapsed = 0.0
    for st in stages:
        elapsed += st.duration
        if elapsed > t:
            best = max(best, st.target)
    return best
