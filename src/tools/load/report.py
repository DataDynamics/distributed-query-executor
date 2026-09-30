"""부하 테스트 결과를 사람이 읽을 요약과 기계가 읽을 파일(JSON·CSV)로 낸다.

콘솔 요약은 처리량 → 지연 → 결과 분포 → 서버 자원 → 포화 신호 → 주요 오류 순으로 쓴다.
"몇 TPS 가 나왔나"를 먼저 보고, "그때 서버가 얼마나 바빴나"를 바로 아래에서 이어 보게 하려는 것이다.

JSON 에는 설정·요약·시계열·자원 표본을 모두 담아 두 번의 실행을 나중에 비교할 수 있게 한다.
"""

from __future__ import annotations

import csv
import json
import unicodedata
from typing import Any, Dict, List, Optional, Sequence

from ..progress import display_width, pad
from ..table import render
from . import scenario
from .stats import SUBMIT_ERRORS, summarize


def _ms(v: Optional[float]) -> str:
    """초 단위 지연을 읽기 좋은 단위로 바꾼다(1초 미만은 ms, 1분 이상은 분·초)."""
    if v is None:
        return "-"
    if v < 1:
        return f"{v * 1000:.0f}ms"
    if v < 60:
        return f"{v:.2f}s"
    m, s = divmod(v, 60)
    return f"{int(m)}m{s:04.1f}s"


def _num(v: Any, digits: int = 2) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:,.{digits}f}"
    return f"{v:,}"


def _pct(v: Optional[float]) -> str:
    return "-" if v is None else f"{v:.0f}%"


def build_result(runner: Any, meta: Dict[str, Any]) -> Dict[str, Any]:
    """러너 상태를 JSON 으로 직렬화할 결과 dict 로 만든다."""
    col = runner.collector
    summary = col.summary(runner.total_elapsed, runner.load_elapsed)
    return {
        "meta": dict(meta, stop_reason=runner.stop_reason,
                     load_elapsed=runner.load_elapsed, total_elapsed=runner.total_elapsed,
                     max_vus_spawned=runner._next_vu - 1),
        "summary": summary,
        "resources": runner.sampler.summary() if runner.sampler else None,
        "timeline": col.timeline(runner.total_elapsed),
        "samples": runner.sampler.rows() if runner.sampler else [],
    }


def _section(title: str) -> str:
    return f"\n[{title}]"


def format_summary(result: Dict[str, Any]) -> str:
    meta, s, res = result["meta"], result["summary"], result["resources"]
    lines: List[str] = []
    drain = max(0.0, meta["total_elapsed"] - meta["load_elapsed"])
    lines.append(
        f"=== 부하 테스트 결과 (run_id={meta['run_id']}, 부하 "
        f"{scenario.format_duration(round(meta['load_elapsed'], 1))} + drain "
        f"{scenario.format_duration(round(drain, 1))}, 최대 VU {meta['max_vus_spawned']})"
    )
    lines.append(f"대상: {', '.join(meta['coordinators'])}  |  종료 사유: {meta['stop_reason']}")
    lines.append(f"시나리오: {meta['scenario']}")

    so, jo = s["submit_outcomes"], s["job_outcomes"]
    is_async = meta.get("request_type", "async") == "async"
    req_err = sum(so[o] for o in SUBMIT_ERRORS)
    lines.append(_section("처리량"))
    if is_async:
        # async 는 접수와 완료가 따로라 TPS 도 둘이다. 용량 판단은 완료 TPS 로 한다.
        lines.append(
            f"  제출  {s['submitted']:,}건  {_num(s['submit_tps'])} TPS  "
            f"(접수 {s['accepted']:,} · 429 {so['rejected_429']:,} · 오류 {req_err:,})"
        )
        lines.append(
            f"  완료  {s['completed']:,}건  {_num(s['complete_tps'])} TPS  "
            f"(성공 {s['succeeded']:,}건 {_num(s['success_tps'])} TPS)"
        )
    else:
        # sync 는 응답이 곧 완료라 TPS 가 하나다.
        lines.append(
            f"  요청  {s['submitted']:,}건  {_num(s['submit_tps'])} TPS  "
            f"(성공 {s['succeeded']:,}건 {_num(s['success_tps'])} TPS · "
            f"429 {so['rejected_429']:,} · 오류 {req_err:,})"
        )
    if s["rows_written"]:
        lines.append(f"  적재  {s['rows_written']:,} rows  {_num(s['rows_per_sec'], 0)} rows/s")
    lines.append(f"  오류율 {s['error_rate'] * 100:.2f}%  |  429 비율 {s['reject_rate'] * 100:.2f}%")

    lat = s["latency"]
    lines.append(_section("지연"))
    if is_async:
        wanted = (("제출(HTTP)", "submit"), ("e2e(성공)", "e2e_success"),
                  ("e2e(전체 종료)", "e2e_all"), ("서버 대기", "queue"), ("서버 실행", "run"))
    else:
        wanted = (("응답", "submit"),)
    rows = []
    for label, key in wanted:
        d = lat[key]
        if not d["count"]:
            continue
        rows.append([label, f"{d['count']:,}", _ms(d["mean"]), _ms(d["p50"]), _ms(d["p90"]),
                     _ms(d["p95"]), _ms(d["p99"]), _ms(d["max"])])
    if rows:
        lines.append(_indent(render(["구분", "건수", "평균", "p50", "p90", "p95", "p99", "최대"],
                                    rows)))
        if is_async and (lat["queue"]["count"] or lat["run"]["count"]):
            lines.append("  (서버 대기 = created→started, 서버 실행 = started→finished, 서버 시각 기준)")
        elif not is_async:
            lines.append("  (응답을 받은 요청 전체 기준. 오류 응답도 포함)")
    else:
        lines.append("  측정된 지연이 없습니다.")

    lines.append(_section("결과 분포"))
    parts = []
    if not is_async and so["ok"]:
        parts.append(f"성공 {so['ok']:,}")
    parts += [f"{k} {v:,}" for k, v in jo.items() if v]
    parts += [f"{k} {so[k]:,}" for k in SUBMIT_ERRORS if so[k]]
    if so["rejected_429"]:
        parts.append(f"429 {so['rejected_429']:,}")
    lines.append("  " + (" · ".join(parts) if parts else "완료된 요청이 없습니다."))
    if is_async:
        lines.append(f"  폴링 {s['poll_requests']:,}회 (실패 {s['poll_errors']:,})"
                     + (f" · 취소 요청 {s['cancel_sent']:,}건" if s["cancel_sent"] else ""))

    if res is not None:
        lines.append(_section("서버 자원"))
        if res["servers"]:
            rows = []
            for srv in res["servers"]:
                base = srv["phases"].get("baseline", {})
                load = srv["phases"].get("load", {})
                tasks = "-"
                if load.get("active_tasks_max") is not None:
                    tasks = f"{load['active_tasks_max']:.0f}/{_num(load.get('max_tasks'), 0)}"
                rows.append([
                    srv["name"], _pct(base.get("cpu_avg")), _pct(load.get("cpu_avg")),
                    _pct(load.get("cpu_p95")), _pct(load.get("cpu_max")),
                    _pct(base.get("mem_avg")), _pct(load.get("mem_avg")),
                    _pct(load.get("mem_max")), tasks,
                    str(srv["unhealthy_samples"]) if srv["unhealthy_samples"] else "-",
                ])
            lines.append(_indent(render(
                ["서버", "기준CPU", "CPU평균", "CPU p95", "CPU최대", "기준MEM", "MEM평균",
                 "MEM최대", "task최대/상한", "비정상"], rows)))
            lines.append(f"  (부하 구간 기준, 표본 {res['samples']}개 · {res['interval']:g}초 간격"
                         + (f" · 수집 실패 {res['errors']}회" if res["errors"] else "")
                         + ". '기준'은 부하 전 baseline 구간)")
        else:
            msg = "  자원 표본이 없습니다"
            if res["last_error"]:
                msg += f" (마지막 오류: {res['last_error']})"
            lines.append(msg)

    lines.append(_section("포화 신호"))
    if s["first_reject_at"] is not None:
        lines.append(f"  첫 429: {s['first_reject_at']:.1f}초 시점 (당시 VU {s['first_reject_vus']})")
    else:
        lines.append("  429 없음 (admission 한도에 닿지 않음)")
    if res and res.get("cpu_peak"):
        p = res["cpu_peak"]
        lines.append(f"  CPU 최고: {p['server']} {p['value']:.0f}% ({p['t']:.1f}초 시점)")
    if res and res.get("jobs_active_max") is not None:
        lines.append(f"  coordinator 처리 중 job 최대: {res['jobs_active_max']} "
                     "(도구 밖에서 들어온 job 포함)")

    if s["top_errors"]:
        lines.append(_section("주요 오류"))
        for msg, n in s["top_errors"]:
            lines.append(f"  {n:>6,} × {msg}")
    return "\n".join(lines)


def _indent(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line for line in text.splitlines())


def format_progress(runner: Any, window: float = 10.0) -> str:
    """실행 중 진행 줄이다. 순간 TPS 는 최근 ``window`` 초의 완료 건수로 낸다."""
    t = runner.elapsed()
    col = runner.collector
    so = col.submit_counts
    if runner.phase == "baseline":
        return f"[기준선 수집 중 {max(0.0, -t):.0f}초 남음]"
    start = max(0.0, t - window)
    span = t - start
    tps = col.completed_between(start, t) / span if span > 0 else 0.0
    line = (
        f"[{scenario.format_duration(int(t))}] VU {runner.active_vus} | "
        f"요청 {sum(so.values()):,} (429 {so['rejected_429']:,}) | 진행중 {runner.inflight} | "
        f"완료 {col.completed:,} (실패 {col.failed:,}) | 완료TPS {tps:.2f}"
    )
    if runner.sampler and runner.sampler.latest():
        coord = [m for m in runner.sampler.latest()["servers"].values()
                 if m["role"] == "coordinator"]
        execs = [m["cpu"] for m in runner.sampler.latest()["servers"].values()
                 if m["role"] == "executor" and m["cpu"] is not None]
        if coord and coord[0]["cpu"] is not None:
            line += f" | coord CPU {coord[0]['cpu']:.0f}%"
        if execs:
            line += f" | exec CPU 최대 {max(execs):.0f}%"
    if runner.phase == "drain":
        line = "[drain] " + line
    return line


def _instant(col: Any, field: str, start: float, end: float) -> float:
    """[start, end) 초 구간의 초당 ``field`` 건수다. 진행 패널의 순간 TPS 에 쓴다."""
    span = end - start
    if span <= 0:
        return 0.0
    total = sum(col.buckets[s][field] for s in range(int(start), int(end)) if s in col.buckets)
    return total / span


def format_live(runner: Any, width: int = 100, window: float = 10.0) -> List[str]:
    """실행 중 라이브 패널이다. 여러 줄을 돌려주며, 각 줄은 화면 폭에 맞게 잘려 있다.

    진행 상황과 제출·완료 TPS(최근 ``window`` 초 기준)를 위에, 서버별 CPU·메모리·task 를 아래
    표로 낸다. 서버 자원은 sampler 가 마지막으로 수집한 스냅샷을 그대로 보여 준다.
    """
    t = runner.elapsed()
    col = runner.collector
    is_async = runner.cfg.request.is_async
    if runner.phase == "baseline":
        return [_clip(f"■ 기준선 수집 중 · {max(0.0, -t):.0f}초 남으면 부하를 시작합니다", width)]

    head = "■ 부하" + ("(drain)" if runner.phase == "drain" else "")
    target = scenario.target_vus(runner.cfg.stages, t) if runner.phase == "load" else 0
    lines = [_clip(
        f"{head} · {scenario.format_duration(int(t))} · "
        f"VU {runner.active_vus}/{target} · 진행중 {runner.inflight}", width)]

    start = max(0.0, t - window)
    so = col.submit_counts
    submit_tps = _instant(col, "submitted", start, t)
    done_tps = _instant(col, "completed", start, t)
    if is_async:
        lines.append(_clip(
            f"  제출 {sum(so.values()):,} ({submit_tps:.1f}/s)   "
            f"완료 {col.completed:,} ({done_tps:.1f}/s, 실패 {col.failed:,})   "
            f"429 {so['rejected_429']:,}", width))
    else:
        lines.append(_clip(
            f"  요청 {sum(so.values()):,} ({submit_tps:.1f}/s)   "
            f"성공 {col.succeeded:,}   실패 {col.failed:,}   429 {so['rejected_429']:,}", width))

    # 지연은 지금까지 누적한 표본의 p50/p95 다(구간이 아니라 전체 — 흐름을 보는 용도).
    lat = summarize(col.e2e_success if is_async else col.submit_latency)
    if lat["count"]:
        label = "e2e" if is_async else "응답"
        extra = f"   적재 {_short_rows(col.rows_written)}" if col.rows_written else ""
        lines.append(_clip(
            f"  지연 {label} p50 {_ms(lat['p50'])} · p95 {_ms(lat['p95'])}{extra}", width))

    lines.extend(_clip(r, width) for r in _server_lines(runner))
    return lines


def _server_lines(runner: Any) -> List[str]:
    """서버별 CPU·메모리·task 표를 만든다. sampler 가 없거나 아직 수집 전이면 안내 한 줄이다."""
    if not runner.sampler:
        return ["  (서버 자원 수집 꺼짐 — --no-sample)"]
    snap = runner.sampler.latest()
    if not snap:
        return ["  (서버 자원 수집 대기 중…)"]
    names = list(snap["servers"])
    label = [n if len(n) <= 24 else "…" + n[-23:] for n in
             [_server_label(n, snap["servers"][n]["role"]) for n in names]]
    w = min(max((display_width(x) for x in label), default=8), 26)
    rows = [f"  {pad('서버', w)}   CPU    MEM   task"]
    for name, disp in zip(names, label):
        m = snap["servers"][name]
        if not m.get("healthy"):
            rows.append(f"  {pad(disp, w)}   다운")
            continue
        task = "-"
        if m.get("active_tasks") is not None:
            task = f"{m['active_tasks']:.0f}/{m['max_tasks']:.0f}" if m.get("max_tasks") \
                else f"{m['active_tasks']:.0f}"
        rows.append(f"  {pad(disp, w)}  {_bar(m.get('cpu'))} {_bar(m.get('mem'))}  {task}")
    return rows


def _server_label(name: str, role: str) -> str:
    if role == "coordinator":
        return name  # "coordinator" 또는 "coordinator@host"
    return name.split("://", 1)[-1]  # executor 는 scheme 을 떼 짧게


def _bar(pct: Optional[float]) -> str:
    """CPU·메모리 사용률을 '  72%▊' 처럼 값과 짧은 막대로 보인다."""
    if pct is None:
        return "  -  "
    filled = int(round(min(max(pct, 0), 100) / 100 * 4))
    return f"{pct:3.0f}%{'█' * filled}{'·' * (4 - filled)}"


def _short_rows(n: int) -> str:
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if n >= div:
            return f"{n / div:.1f}{unit} rows"
    return f"{n:,} rows"


def _clip(text: str, width: int) -> str:
    """표시 폭 기준으로 자른다. 라이브 패널이 화면 폭을 넘겨 줄바꿈되면 커서 계산이 깨지기 때문이다."""
    if width <= 0:
        return text
    out, w = [], 0
    for ch in text:
        cw = 2 if unicodedata.east_asian_width(ch) in "WF" else 1
        if w + cw > width:
            break
        out.append(ch)
        w += cw
    return "".join(out)


class LiveRenderer:
    """여러 줄 패널을 제자리에서 다시 그린다. ANSI 커서 이동으로 이전 줄을 덮어쓴다.

    줄 수가 매번 달라질 수 있어(서버가 늘거나 지연 줄이 생김) 직전에 그린 줄 수를 기억했다가
    남는 줄은 지운다. 커서는 항상 패널 바로 아래 줄에 두어, 다음 그리기가 위로 올라가 덮어쓴다.
    """

    def __init__(self, stream: Any) -> None:
        self.stream = stream
        self._prev = 0

    def render(self, lines: Sequence[str]) -> None:
        buf = []
        if self._prev:
            buf.append(f"\033[{self._prev}A")  # 이전 패널 맨 위로 올라간다
        for ln in lines:
            buf.append("\r" + ln + "\033[K\n")
        extra = self._prev - len(lines)
        for _ in range(max(0, extra)):
            buf.append("\r\033[K\n")           # 줄어든 만큼 남은 옛 줄을 지운다
        if extra > 0:
            buf.append(f"\033[{extra}A")       # 커서를 패널 바로 아래로 되돌린다
        self._prev = len(lines)
        self.stream.write("".join(buf))
        self.stream.flush()

    def clear(self) -> None:
        """패널을 지우고 커서를 패널이 있던 자리 맨 위로 되돌린다(뒤이어 요약이 그 위에 찍힌다)."""
        if not self._prev:
            return
        self.stream.write(f"\033[{self._prev}A")
        self.stream.write(("\r\033[K\n") * self._prev)
        self.stream.write(f"\033[{self._prev}A")
        self.stream.flush()
        self._prev = 0


def write_json(path: str, result: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)


def write_csv(path: str, rows: Sequence[Dict[str, Any]]) -> None:
    """dict 행 목록을 CSV 로 쓴다. 엑셀에서 바로 열리도록 쉼표 구분·헤더 포함이다."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        if not rows:
            return
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

