"""부하 시나리오의 순수 로직이다. 시간 표기 해석, ramp-up 단계 계산, 요청 본문 치환을 맡는다.

네트워크나 asyncio 와 무관하므로 단위 테스트로 계산을 그대로 검증할 수 있다.

**VU 수 곡선은 단계(stage)의 연속으로 표현한다.** 각 단계는 "이 시간 동안 목표 VU 수까지 선형으로
이동한다"는 뜻이며, 첫 단계는 0 VU 에서 출발한다. JMeter 식 ``--vus 20 --ramp-up 60s --duration
10m`` 은 ``[60s → 20, 540s → 20]`` 두 단계로 바뀐다(duration 은 ramp-up 을 포함한 전체 시간이다).
k6 식 ``--stages 30s:5,2m:5,1m:20`` 은 적은 그대로 단계가 된다.
"""

from __future__ import annotations

import copy
import math
import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
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
# 문자열 전체가 이 자리표시자 하나뿐이면 정수로 넣는다(예: "parallelism": "${vu}").
_INT_VARS = ("vu", "iter", "seq", "randint")


@dataclass
class RenderContext:
    """자리표시자에 채울 반복별 값이다."""

    vu: int
    iteration: int
    seq: int
    run_id: str
    rng: random.Random


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
        items = [x.strip() for x in (arg or "").split(",") if x.strip()]
        if not items:
            raise ValueError("${choice:a,b,c} 에 후보가 없습니다")
        return ctx.rng.choice(items)
    if name == "randint":
        try:
            lo, hi = (int(x) for x in (arg or "").split("-", 1))
        except ValueError:
            raise ValueError("${randint:<최소>-<최대>} 형식이어야 합니다") from None
        return ctx.rng.randint(lo, hi)
    raise ValueError(
        f"알 수 없는 자리표시자 ${{{name}}} (vu·iter·seq·run_id·uuid·now·choice·randint 중 하나)"
    )


def _render_str(s: str, ctx: RenderContext) -> Any:
    whole = _PLACEHOLDER.fullmatch(s)
    if whole and whole.group(1) in _INT_VARS:
        return _resolve(whole.group(1), whole.group(2), ctx)
    return _PLACEHOLDER.sub(lambda m: str(_resolve(m.group(1), m.group(2), ctx)), s)


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


def validate_body(template: Any) -> None:
    """시작 전에 한 번 렌더해 잘못된 자리표시자를 부하를 걸기 전에 잡는다."""
    render_body(template, RenderContext(1, 0, 0, "check", random.Random(0)))


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
