"""포화 분석: VU 수준별 처리량을 집계해 saturation point(무릎점)를 찾는다.

closed-loop 부하에서 VU 를 늘리면 완료 TPS 가 함께 오르다가, 어느 지점부터는 VU 를 더 얹어도
TPS 가 늘지 않고 지연만 길어진다. 그 꺾이는 지점이 saturation point 이고, 서버가 그 워크로드에서
낼 수 있는 실효 처리량의 상한이다.

**제대로 재려면 각 VU 수준에서 잠시 유지해 정상상태를 봐야 한다.** 그래서 `saturation` 명령은 계단
부하를 구성하고, 이 모듈은 초 단위 시계열을 VU 수준별로 묶되 수준이 바뀐 직후의 과도구간(warmup)을
버리고 집계한다. 순수 함수만 두어 시계열만 있으면 실행 없이 검증할 수 있다.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence

#: 집계에서 합산하는 시계열 카운터. report.Collector.BUCKET_FIELDS 의 부분집합이다.
_SUM_FIELDS = ("submitted", "completed", "success", "failed", "rejected_429", "rows")


def _segments(timeline: Sequence[Dict[str, Any]]):
    """VU 수가 같은 연속 구간으로 나눈다. 계단 부하의 각 단(step)이 한 구간이 된다."""
    i, n = 0, len(timeline)
    while i < n:
        v = timeline[i]["vus"]
        j = i
        while j < n and timeline[j]["vus"] == v:
            j += 1
        yield v, timeline[i:j]
        i = j


def per_vu_levels(timeline: Sequence[Dict[str, Any]], warmup_frac: float = 0.3,
                  drop_short_frac: float = 0.5) -> List[Dict[str, Any]]:
    """시계열을 VU 수준별로 묶어 처리량을 집계한다.

    각 연속 구간에서 앞쪽 ``warmup_frac`` 비율의 초는 버린다(수준이 오른 직후 in-flight 가 차오르는
    과도구간이라 정상상태 TPS 를 왜곡한다). 남는 초가 없으면 마지막 1초만 쓴다. 같은 VU 수준이 여러
    구간에 나뉘어 있으면(예: ramp 뒤 hold) 합산한다. VU 0 은 제외한다.

    계단이 오르내릴 때 스쳐 가는 1~2초짜리 과도 VU 수준(예: 8→16 사이의 10·12)은 곡선을 어지럽히므로,
    센 초가 가장 오래 유지된 수준의 ``drop_short_frac`` 배에 못 미치는 수준은 버린다. 모든 수준이
    비슷하게 유지되면(합성 데이터·균일 계단) 아무것도 버리지 않는다.

    반환은 VU 오름차순의 항목 목록이며, 각 항목은 그 수준에서의 초 수와 제출·완료·성공 TPS,
    초당 적재 rows, 오류율(429 제외 시도 대비), 429 건수를 담는다.
    """
    agg: Dict[int, Dict[str, float]] = defaultdict(
        lambda: {"seconds": 0, **{f: 0 for f in _SUM_FIELDS}})
    for v, seg in _segments(timeline):
        if v <= 0:
            continue
        drop = int(len(seg) * warmup_frac)
        counted = seg[drop:] if len(seg) - drop >= 1 else seg[-1:]
        a = agg[v]
        a["seconds"] += len(counted)
        for row in counted:
            for f in _SUM_FIELDS:
                a[f] += row.get(f, 0)

    max_secs = max((a["seconds"] for a in agg.values()), default=0)
    min_secs = max_secs * drop_short_frac
    levels = []
    for v in sorted(agg):
        if agg[v]["seconds"] < min_secs:
            continue   # 스쳐 간 과도 수준
        a = agg[v]
        s = a["seconds"] or 1
        attempts = a["submitted"] - a["rejected_429"]
        levels.append({
            "vus": v,
            "seconds": int(a["seconds"]),
            "submit_tps": a["submitted"] / s,
            "complete_tps": a["completed"] / s,
            "success_tps": a["success"] / s,
            "rows_per_sec": a["rows"] / s,
            "error_rate": (a["failed"] / attempts) if attempts > 0 else 0.0,
            "reject": int(a["rejected_429"]),
        })
    return levels


def find_knee(levels: Sequence[Dict[str, Any]], field: str = "complete_tps",
              rel_gain: float = 0.05) -> Optional[Dict[str, Any]]:
    """포화 무릎점을 찾는다. 이웃한 두 VU 수준 사이에서 처리량 증가율이 ``rel_gain`` 아래로
    떨어지는 첫 지점을, 그 아래(더 낮은 VU) 수준으로 본다.

    VU 를 올렸는데 TPS 증가가 5% 미만이면(기본) 그 앞 수준에서 이미 포화된 것으로 판단한다. TPS 가
    오히려 줄면(과부하) 증가율이 음수라 역시 그 앞 수준이 무릎점이 된다. 끝까지 증가율이 유지되면
    측정 범위 안에서 포화에 닿지 않은 것이라 ``saturated=False`` 로, 관측된 최고점을 돌려준다.
    """
    pts = [lv for lv in levels if lv["seconds"] > 0]
    if len(pts) < 2:
        return None
    peak = max(pts, key=lambda lv: lv[field])
    knee = None
    for a, b in zip(pts, pts[1:]):
        if a[field] <= 0:
            continue
        gain = (b[field] - a[field]) / a[field]
        if gain < rel_gain:
            knee = a
            break
    saturated = knee is not None
    if knee is None:
        knee = peak
    # 무릎점 대비 최고점이 얼마나 더 높은지(측정 범위에서 더 짜낼 여지)
    headroom = (peak[field] - knee[field]) / knee[field] if knee[field] > 0 else 0.0
    return {
        "field": field,
        "knee_vus": knee["vus"],
        "knee_tps": knee[field],
        "peak_vus": peak["vus"],
        "peak_tps": peak[field],
        "saturated": saturated,
        "headroom": headroom,
    }


def ascii_chart(levels: Sequence[Dict[str, Any]], field: str = "complete_tps",
                knee: Optional[Dict[str, Any]] = None, height: int = 9,
                title: str = "완료 TPS vs VU") -> List[str]:
    """VU 수준별 처리량을 세로 산점도로 그린다. 무릎점은 ◆, 나머지는 ● 로 찍는다.

    y 축은 0~최댓값, x 축은 테스트한 VU 수준(범주)이다. 외부 의존성 없이 콘솔 요약에 바로 넣는다.
    """
    pts = [lv for lv in levels if lv["seconds"] > 0]
    if len(pts) < 2:
        return ["  (포화 곡선을 그리려면 서로 다른 VU 수준이 둘 이상 필요합니다)"]
    vals = [lv[field] for lv in pts]
    vmax = max(vals) or 1.0
    # 포화에 실제로 닿았을 때만 ◆ 를 찍는다(닿지 않았으면 무릎점이 최고점=허상이라 표시 안 함).
    knee_vus = knee["knee_vus"] if knee and knee.get("saturated") else None

    labels = [str(lv["vus"]) for lv in pts]
    cw = max(3, max(len(x) for x in labels) + 1)   # 열 폭
    gutter = 7                                       # y 라벨 칸
    plot_h = max(4, height)

    def yrow(val: float) -> int:
        return int(round(val / vmax * (plot_h - 1)))

    # 격자: 위(plot_h-1)에서 아래(0)로. 범례의 ◆ 는 포화점을 실제로 찍을 때만 넣는다.
    legend = "(● 측정, ◆ 포화점)" if knee_vus is not None else "(● 측정)"
    lines = [f"  {title}   {legend}"]
    for r in range(plot_h - 1, -1, -1):
        if r == plot_h - 1:
            lbl = f"{vmax:6.1f}"
        elif r == 0:
            lbl = f"{0.0:6.1f}"
        else:
            lbl = " " * 6
        cells = []
        for lv, val in zip(pts, vals):
            mark = " "
            if yrow(val) == r:
                mark = "◆" if lv["vus"] == knee_vus else "●"
            cells.append(mark.center(cw))
        axis = "┤" if r not in (0,) else "┼"
        lines.append(f"{lbl} {axis}" + "".join(cells))
    # x 축
    lines.append(" " * gutter + "└" + "─" * (cw * len(pts)))
    lines.append(" " * (gutter + 1) + "".join(x.center(cw) for x in labels) + "  VU")
    return lines
