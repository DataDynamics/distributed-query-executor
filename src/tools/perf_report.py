"""성능 지표 로그(``*-perf.log``)를 읽어 시간대별 요약·병목 의심 구간·느린 job/task 를 보여 주는 도구다.

coordinator 와 executor 는 ``logging.perf.interval_s`` 마다 지표 한 줄(``kind=snapshot``)을, job·task 가
끝날 때마다 요약 한 줄(``kind=job_done``·``kind=task_done``)을 남긴다(core/perf.py). 이 도구는 그 파일을
서비스(파일)별로 나눠 읽고 다음 네 가지를 출력한다.

1. **시간대별 요약**: ``--bucket`` 구간마다 처리량과 포화 지표의 최대값을 한 행으로 보여 준다.
2. **병목 의심**: 규칙(:data:`RULES`)에 걸린 구간을 규칙별로 묶어 횟수·시각·최대값과 조정 방향을 적는다.
3. **느린 job/task**: 요약 줄 중 오래 걸린 것을 ``--top`` 개 보여 준다.
4. **병목 분포**: 요약 줄의 병목 판정(source·target·mixed)이 얼마나 되는지 센다.

줄 형식은 ``YYYY-MM-DD HH:MM:SS k=v k=v ...`` 이고 값의 공백은 큰따옴표로 감싸 있으므로 shlex 로 나눈다.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

from core.textui import disp_width, pad

_TS_LEN = 19  # "YYYY-MM-DD HH:MM:SS"
# 보관 파일명은 <이름>-perf_YYYYMMDD.log, 오늘 파일은 <이름>-perf.log 다(core.logging._DailyFileHandler).
_SOURCE_RE = re.compile(r"^(?P<name>.+?)-perf(?:_\d{8})?\.log$")


@dataclass
class Record:
    """지표 파일의 한 줄이다. 머리의 시각과 ``key=value`` 들을 숫자로 바꿀 수 있는 것은 바꿔 담는다."""

    ts: datetime
    fields: Dict[str, object]

    @property
    def kind(self) -> str:
        return str(self.fields.get("kind", ""))

    def num(self, key: str) -> Optional[float]:
        v = self.fields.get(key)
        return float(v) if isinstance(v, (int, float)) else None

    def sum_prefix(self, prefix: str) -> float:
        """``prefix.*`` 로 시작하는 숫자 값의 합이다(jobs_done.DONE + jobs_done.FAILED 처럼)."""
        return sum(v for k, v in self.fields.items() if k.startswith(prefix) and isinstance(v, (int, float)))


def _convert(text: str) -> object:
    """값 문자열을 int·float 순으로 바꿔 보고, 둘 다 아니면 문자열 그대로 둔다."""
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def parse_line(line: str) -> Optional[Record]:
    """지표 줄 하나를 읽는다. 형식이 맞지 않으면 None 이다."""
    line = line.rstrip("\n")
    if len(line) <= _TS_LEN:
        return None
    try:
        ts = datetime.strptime(line[:_TS_LEN], "%Y-%m-%d %H:%M:%S")
        tokens = shlex.split(line[_TS_LEN:])
    except ValueError:
        return None
    fields: Dict[str, object] = {}
    for tok in tokens:
        key, sep, value = tok.partition("=")
        if sep:
            fields[key] = _convert(value)
    if "kind" not in fields:
        return None
    return Record(ts, fields)


def source_name(path: Path) -> str:
    """파일명에서 서비스 이름(<이름>-perf 앞부분)을 뽑는다. 날짜가 다른 보관 파일도 같은 이름이 된다."""
    m = _SOURCE_RE.match(path.name)
    return m.group("name") if m else path.stem


def expand_paths(paths: Sequence[str]) -> List[Path]:
    """파일과 디렉터리를 받아 지표 파일 목록으로 펼친다. 디렉터리면 그 안의 ``*-perf*.log`` 를 모두 읽는다."""
    out: List[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            out.extend(sorted(x for x in p.iterdir() if x.is_file() and _SOURCE_RE.match(x.name)))
        elif p.is_file():
            out.append(p)
        else:
            raise FileNotFoundError(raw)
    return out


def load(paths: Iterable[Path], since: Optional[datetime] = None,
         until: Optional[datetime] = None) -> Dict[str, List[Record]]:
    """파일들을 서비스별 기록 목록(시각순)으로 읽는다."""
    by_source: Dict[str, List[Record]] = defaultdict(list)
    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                rec = parse_line(line)
                if rec is None:
                    continue
                if (since and rec.ts < since) or (until and rec.ts >= until):
                    continue
                by_source[source_name(path)].append(rec)
    for recs in by_source.values():
        recs.sort(key=lambda r: r.ts)
    return dict(by_source)


def parse_bucket(text: str) -> timedelta:
    """``--bucket`` 의 ``30s``·``10m``·``1h`` 를 구간 길이로 바꾼다."""
    m = re.fullmatch(r"(\d+)\s*([smh])", text.strip())
    if not m:
        raise argparse.ArgumentTypeError("구간은 30s·10m·1h 처럼 숫자와 s/m/h 로 준다")
    n, unit = int(m.group(1)), m.group(2)
    if n <= 0:
        raise argparse.ArgumentTypeError("구간은 0보다 커야 한다")
    return timedelta(seconds=n * {"s": 1, "m": 60, "h": 3600}[unit])


def floor_ts(ts: datetime, bucket: timedelta) -> datetime:
    day = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    step = int(bucket.total_seconds())
    return day + timedelta(seconds=(int((ts - day).total_seconds()) // step) * step)


# ───────── 시간대별 요약 ─────────

@dataclass(frozen=True)
class Column:
    """요약 표의 열 하나다. key 는 지표 키(또는 ``접두사.*``·``a|b`` 중 최대), agg 는 묶는 방법이다.

    agg: sum(구간 합) · max(구간 최대) · rate(구간 합 ÷ 구간 길이, 초당) · last(마지막 값).
    """

    label: str
    key: str
    agg: str
    scale: float = 1.0


COLUMNS: Dict[str, List[Column]] = {
    "coordinator": [
        Column("완료job", "jobs_done.*", "sum"),
        Column("적재행/s", "rows_written", "rate"),
        Column("실행", "admission.running", "max"),
        Column("대기", "admission.pending", "max"),
        Column("429", "admission.rejected", "sum"),
        Column("슬롯대기p95s", "admission.slot_wait_ms.p95", "max", 0.001),
        Column("디스패치대기", "dispatch.waiting", "max"),
        Column("Phase2", "dispatch.phase2_running", "max"),
        Column("폴링오류", "dispatch.poll_errors.*", "sum"),
        Column("루프지연ms", "loop.lag_ms.max", "max"),
        Column("기본풀큐", "pool.default.queued", "max"),
        Column("기본풀대기s", "pool.default.wait_ms.p95", "max", 0.001),
        Column("메타대기ms", "pool.meta.wait_ms.p95|pool.store.wait_ms.p95", "max"),
        Column("CPU%", "system.cpu_pct", "max"),
    ],
    "executor": [
        Column("완료task", "tasks_done.*", "sum"),
        Column("적재행/s", "rows_written", "rate"),
        Column("실행", "tasks.running", "max"),
        Column("슬롯", "tasks.max", "last"),
        Column("대기", "tasks.queued", "max"),
        Column("GP풀", "gp_pool.in_use", "max"),
        Column("GP대기ms", "gp_pool.acquire_wait_ms.p95", "max"),
        Column("소스대기s", "copy.read_starve_ms.p95", "max", 0.001),
        Column("쓰기s", "copy.write_wait_ms.p95", "max", 0.001),
        Column("루프지연ms", "loop.lag_ms.max", "max"),
        Column("기본풀큐", "pool.default.queued", "max"),
        Column("CPU%", "system.cpu_pct", "max"),
        Column("수신MB/s", "system.net_rx_mbps", "max"),
        Column("디스크W", "system.disk_write_mbps", "max"),
    ],
}


def _value(rec: Record, key: str) -> Optional[float]:
    """열 키로 줄의 값 하나를 꺼낸다.

    ``a.*`` 는 그 접두어로 시작하는 값의 합(상태별로 나뉜 카운터를 한 열로 볼 때), ``a|b`` 는 있는 값 중 최댓값
    (역할이나 버전에 따라 이름이 다른 지표를 한 열로 볼 때)이다. 줄에 없으면 None 이라 집계에서 빠진다.
    """
    if key.endswith(".*"):
        return rec.sum_prefix(key[:-1])
    if "|" in key:
        vals = [v for v in (rec.num(k) for k in key.split("|")) if v is not None]
        return max(vals) if vals else None
    return rec.num(key)


def aggregate(recs: List[Record], col: Column) -> Optional[float]:
    """한 구간에 든 줄들의 값을 열의 방법으로 묶는다.

    ``sum`` 은 합, ``rate`` 는 합을 줄들의 ``window_s`` 합으로 나눈 초당 값, ``last`` 는 마지막 줄의 값, 그 밖은
    최댓값이다. 지표 줄의 카운터는 구간 값이라 여러 줄을 더해야 시간대의 양이 되고, 게이지는 순간 값이라 최댓값이
    그 시간대에 가장 몰렸던 정도를 보여 준다.
    """
    vals = [(r, v) for r in recs if (v := _value(r, col.key)) is not None]
    if not vals:
        return None
    if col.agg == "sum":
        out = sum(v for _, v in vals)
    elif col.agg == "rate":
        span = sum(r.num("window_s") or 0 for r, _ in vals)
        out = sum(v for _, v in vals) / span if span > 0 else 0.0
    elif col.agg == "last":
        out = vals[-1][1]
    else:
        out = max(v for _, v in vals)
    return out * col.scale


def fmt_num(v: Optional[float]) -> str:
    """표에 넣을 짧은 숫자로 바꾼다. 큰 값은 k·M 으로 줄이고 값이 없으면 ``-`` 다."""
    if v is None:
        return "-"
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if abs(v) >= 10_000:
        return f"{v / 1000:.0f}k"
    if v == int(v):
        return str(int(v))
    return f"{v:.1f}" if abs(v) >= 10 else f"{v:.2f}"


def render_table(headers: List[str], rows: List[List[str]]) -> List[str]:
    """한글 폭을 고려해 열을 맞춘 표 줄 목록을 만든다."""
    widths = [max(disp_width(h), *(disp_width(r[i]) for r in rows)) if rows else disp_width(h)
              for i, h in enumerate(headers)]
    lines = ["  ".join(pad(h, w) for h, w in zip(headers, widths)).rstrip()]
    for r in rows:
        lines.append("  ".join(pad(c, w) for c, w in zip(r, widths)).rstrip())
    return lines


def timeline(snaps: List[Record], role: str, bucket: timedelta,
             columns: Optional[List[Column]] = None) -> List[str]:
    """주기 지표 줄을 구간 시작 시각별로 묶어 역할의 기본 열(또는 ``--keys``)로 표를 만든다."""
    cols = columns or COLUMNS.get(role, [])
    groups: Dict[datetime, List[Record]] = defaultdict(list)
    for r in snaps:
        groups[floor_ts(r.ts, bucket)].append(r)
    # 1분보다 짧은 구간이면 초까지 보여야 행이 서로 구분된다.
    ts_fmt = "%m-%d %H:%M:%S" if bucket.total_seconds() < 60 else "%m-%d %H:%M"
    rows = []
    for start in sorted(groups):
        rows.append([start.strftime(ts_fmt)] + [fmt_num(aggregate(groups[start], c)) for c in cols])
    return render_table(["시각"] + [c.label for c in cols], rows)


# ───────── 병목 의심 규칙 ─────────

@dataclass(frozen=True)
class Rule:
    """한 지표 줄을 보고 병목을 의심할지 정하는 규칙이다. check 가 숫자를 돌려주면 걸린 것이고 그 값이 크기다."""

    title: str
    role: str  # coordinator·executor·any
    check: Callable[[Record], Optional[float]]
    unit: str
    hint: str


def _gt(key: str, threshold: float) -> Callable[[Record], Optional[float]]:
    def check(r: Record) -> Optional[float]:
        v = _value(r, key)
        return v if v is not None and v > threshold else None
    return check


def _admission_pending(r: Record) -> Optional[float]:
    pending, mx = r.num("admission.pending"), r.num("admission.max_pending")
    if pending is None or not mx:
        return None
    return pending if pending >= 0.8 * mx else None


def _dispatch_saturated(r: Record) -> Optional[float]:
    waiting, inflight, mx = r.num("dispatch.waiting"), r.num("dispatch.inflight"), r.num("dispatch.max")
    if not waiting or inflight is None or not mx:
        return None
    return waiting if inflight >= mx else None


def _executor_slots(r: Record) -> Optional[float]:
    queued, running, mx = r.num("tasks.queued"), r.num("tasks.running"), r.num("tasks.max")
    if not queued or running is None or not mx:
        return None
    return queued if running >= mx else None


def _default_pool(r: Record) -> Optional[float]:
    queued, wait = r.num("pool.default.queued"), r.num("pool.default.wait_ms.p95")
    if not queued or wait is None or wait < 1000:
        return None
    return wait


RULES: List[Rule] = [
    Rule("이벤트 루프 지연", "any", _gt("loop.lag_ms.max", 200), "ms",
         "루프가 블로킹 호출이나 밀린 콜백으로 늦게 돈다. 같은 시각 경고 로그의 '이벤트 루프가' 스택을 확인한다. "
         "헬스 체크·폴링 timeout 과 'Server disconnected' 의 흔한 원인이다."),
    Rule("이벤트 루프 정지", "any", _gt("loop.stalls", 0), "회",
         "logging.loop_stall_ms 를 넘겨 멈춘 적이 있다. 경고 로그에 멈춘 순간의 스택이 남아 있다."),
    Rule("기본 스레드 풀 포화", "any", _default_pool, "ms",
         "run_in_executor 작업이 스레드를 얻으려고 1초 넘게 줄을 섰다. coordinator 면 Phase 2 GP INSERT, executor 면 "
         "task 적재 본체가 풀을 다 쓰고 있다. 동시 적재 수(max_concurrent_jobs·max_concurrent_tasks)를 CPU 수에 맞춘다."),
    Rule("메타 저장소 호출 대기", "any", _gt("pool.meta.wait_ms.p95|pool.store.wait_ms.p95", 500), "ms",
         "job 저장·이력·heartbeat 호출이 전용 풀에서 기다렸다. 메타 DB 가 느리거나(pool.*.run_ms 확인) 접속이 막혔다."),
    Rule("메타 DB 연결 실패", "any", _gt("meta_db.connect_errors", 0), "회",
         "history.db_dsn·monitor.db_dsn 연결이 실패했다. timeout 이면 db.connect_timeout_s 안에 붙지 못한 것이다."),
    Rule("job 거절(429)", "coordinator", _gt("admission.rejected", 0), "건",
         "실행+대기 한도를 넘었다. executor 에 여유가 있으면 coordinator.max_concurrent_jobs 를, 순간 몰림이면 "
         "max_pending_jobs 를 올린다."),
    Rule("job 대기 큐 80% 이상", "coordinator", _admission_pending, "건",
         "곧 429 가 난다. 실행 슬롯이 늘 차 있고 executor 가 한가하면 max_concurrent_jobs 를 올린다."),
    Rule("실행 슬롯 대기 1분 이상", "coordinator", _gt("admission.slot_wait_ms.p95", 60000), "ms",
         "job 이 PENDING 에서 오래 기다린다. 실행 슬롯이 병목이다."),
    Rule("디스패치 한도 포화", "coordinator", _dispatch_saturated, "task",
         "동시에 띄운 task 가 coordinator.max_dispatch_concurrency 에 닿아 나머지가 기다린다. executor 슬롯 합계보다 "
         "작으면 올린다."),
    Rule("task 폴링 오류", "coordinator", _gt("dispatch.poll_errors.*", 0), "회",
         "executor 상태 조회가 실패했다. 같은 시각 executor 쪽 루프 지연과 keep_alive_s 설정을 본다."),
    Rule("executor 슬롯 포화", "executor", _executor_slots, "task",
         "실행 슬롯이 다 차서 task 가 QUEUED 로 기다린다. CPU·네트워크에 여유가 있으면 executor.max_concurrent_tasks 를, "
         "없으면 executor 를 늘린다."),
    Rule("GP 커넥션 풀 대기", "executor", _gt("gp_pool.acquire_wait_ms.p95", 1000), "ms",
         "GP 연결을 얻으려고 1초 넘게 기다렸다. greenplum.pool_max 를 동시 task 수 이상으로 올린다."),
    Rule("GP 세션 초기화 실패", "executor", _gt("gp_pool.reset_failures", 0), "회",
         "DISCARD ALL 이 실패해 연결을 버렸다. 경고 로그에서 원인을 본다."),
    Rule("CPU 90% 이상", "any", _gt("system.cpu_pct", 90), "%",
         "호스트 CPU 가 포화됐다. 동시 실행 수를 줄이거나 인스턴스를 늘린다."),
    Rule("메모리 90% 이상", "any", _gt("system.mem_pct", 90), "%",
         "호스트 메모리가 부족하다. 청크 없는 커스텀 소스(fetch_module)나 큰 parallelism 을 의심한다."),
    Rule("디스크 90% 이상", "any", _gt("system.disk_pct", 90), "%",
         "스테이징 경로나 로그 디렉터리를 정리한다(local_stage·s3_stage 의 남은 CSV)."),
]


@dataclass
class Finding:
    """규칙 하나가 걸린 결과다. 걸린 줄 수·처음과 마지막 시각·가장 나빴던 값과 그 시각을 담는다."""

    rule: Rule
    count: int = 0
    first: Optional[datetime] = None
    last: Optional[datetime] = None
    peak: float = 0.0
    peak_at: Optional[datetime] = None


def find_bottlenecks(snaps: List[Record], role: str) -> List[Finding]:
    """모든 주기 줄에 역할에 맞는 규칙을 대 보고 걸린 규칙을 모은다. 순서는 RULES 에 적은 순서를 따른다."""
    found: Dict[str, Finding] = {}
    for r in snaps:
        for rule in RULES:
            if rule.role not in ("any", role):
                continue
            v = rule.check(r)
            if v is None:
                continue
            f = found.setdefault(rule.title, Finding(rule))
            f.count += 1
            f.first = f.first or r.ts
            f.last = r.ts
            if v > f.peak or f.peak_at is None:
                f.peak, f.peak_at = v, r.ts
    order = {rule.title: i for i, rule in enumerate(RULES)}
    return sorted(found.values(), key=lambda f: order[f.rule.title])


# ───────── 이벤트(job_done·task_done) ─────────

def slowest(events: List[Record], top: int) -> List[str]:
    """종료 요약 줄(job_done 또는 task_done)을 소요 시간이 긴 순으로 ``top`` 개 골라 표로 만든다."""
    if not events:
        return []
    kind = events[0].kind
    ranked = sorted(events, key=lambda r: r.num("elapsed_s") or 0, reverse=True)[:top]
    id_key = "job_id" if kind == "job_done" else "task_id"
    headers = ["종료 시각", "ID", "상태", "모드", "소요s", "행/s", "병목", "가장 느린 단계"]
    rows = []
    for r in ranked:
        stage = r.fields.get("slowest_stage", "-")
        stage_s = r.num("slowest_stage_s")
        rows.append([
            r.ts.strftime("%m-%d %H:%M:%S"), str(r.fields.get(id_key, "-")), str(r.fields.get("status", "-")),
            str(r.fields.get("exec_mode", "-")), fmt_num(r.num("elapsed_s")), fmt_num(r.num("rows_per_s")),
            str(r.fields.get("bottleneck", "-")),
            f"{stage}({fmt_num(stage_s)}s)" if stage not in ("-", None) else "-",
        ])
    return render_table(headers, rows)


def bottleneck_mix(events: List[Record]) -> str:
    """종료 요약의 병목 판정(source·target·mixed)이 각각 몇 건인지 한 줄로 만든다."""
    counts = Counter(str(r.fields.get("bottleneck", "-")) for r in events)
    total = sum(counts.values())
    labels = {"source": "source(소스 대기)", "target": "target(GP 쓰기)", "mixed": "mixed", "-": "판정 없음"}
    parts = [f"{labels.get(k, k)} {n}건({n * 100 // total}%)" for k, n in counts.most_common()]
    return ", ".join(parts)


# ───────── 출력 ─────────

def report(by_source: Dict[str, List[Record]], *, bucket: timedelta, top: int,
           keys: Optional[List[Column]] = None) -> List[str]:
    """서비스(지표 파일 이름)마다 시간대별 요약·병목 의심·느린 job/task 를 이어 붙인 출력 줄을 만든다.

    역할은 주기 줄의 ``role`` 로 정하고, 주기 줄 없이 종료 요약만 있으면 그 종류로 짐작한다.
    """
    out: List[str] = []
    for name in sorted(by_source):
        recs = by_source[name]
        snaps = [r for r in recs if r.kind == "snapshot"]
        events = [r for r in recs if r.kind in ("job_done", "task_done")]
        role = next((str(r.fields.get("role")) for r in snaps if r.fields.get("role")), "")
        if not role and events:
            role = "coordinator" if events[0].kind == "job_done" else "executor"
        span = f"{recs[0].ts:%Y-%m-%d %H:%M:%S} ~ {recs[-1].ts:%Y-%m-%d %H:%M:%S}" if recs else "-"
        out.append(f"== {name} ({role or '?'}) {span} — 지표 {len(snaps)}줄, 종료 요약 {len(events)}건")
        if snaps:
            minutes = int(bucket.total_seconds() // 60)
            label = f"{minutes}분" if minutes else f"{int(bucket.total_seconds())}초"
            out.append(f"-- 시간대별 요약({label} 단위, 개수는 합, 나머지는 최대)")
            out.extend(timeline(snaps, role, bucket, keys))
            out.append("")
            findings = find_bottlenecks(snaps, role)
            out.append("-- 병목 의심" + ("" if findings else ": 없음"))
            for f in findings:
                window = f"{f.first:%H:%M}~{f.last:%H:%M}" if f.first and f.last else "-"
                out.append(
                    f"[{f.rule.title}] {f.count}구간 ({window}), 최대 {fmt_num(f.peak)}{f.rule.unit}"
                    f" @ {f.peak_at:%H:%M:%S}"
                )
                out.append(f"    → {f.rule.hint}")
            out.append("")
        if events:
            kind = "job" if events[0].kind == "job_done" else "task"
            out.append(f"-- 느린 {kind} 상위 {min(top, len(events))}")
            out.extend(slowest(events, top))
            out.append(f"-- 병목 분포: {bottleneck_mix(events)}")
            out.append("")
    return out


def parse_keys(text: str) -> List[Column]:
    """``--keys a,b:sum`` 를 표 열로 만든다. 묶는 방법을 생략하면 max 다."""
    cols = []
    for item in [x.strip() for x in text.split(",") if x.strip()]:
        key, _, agg = item.partition(":")
        if agg and agg not in ("sum", "max", "rate", "last"):
            raise argparse.ArgumentTypeError(f"묶는 방법은 sum·max·rate·last 중 하나다: {item}")
        cols.append(Column(key, key, agg or "max"))
    return cols


def _parse_time(text: str) -> datetime:
    """``--since``·``--until`` 의 시각을 읽는다. 날짜만 주면 그날 0시다."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError("시각은 'YYYY-MM-DD HH:MM[:SS]' 또는 'YYYY-MM-DD' 로 준다")


def default_paths() -> List[str]:
    """경로를 주지 않으면 설정의 로그 디렉터리를 읽는다."""
    try:
        from core.config import settings

        return [str(settings.log_dir)]
    except Exception:
        return ["logs"]


def build_parser() -> argparse.ArgumentParser:
    """명령행 인자를 정의한다."""
    parser = argparse.ArgumentParser(
        prog=os.environ.get("PROG_NAME", "bin/perf-report"),
        description="성능 지표 로그(*-perf.log)를 요약해 시간대별 추이와 병목 의심 구간을 보여 준다.",
        epilog=(
            "예) perf-report                                  # 설정의 로그 디렉터리 전체\n"
            "    perf-report logs/query-executor-8087-perf.log --bucket 1m\n"
            "    perf-report logs --since '2026-10-29 09:00' --until '2026-10-29 12:00'\n"
            "    perf-report logs --source query-coordinator-server --keys admission.pending,rows_written:rate"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("paths", nargs="*", help="지표 파일이나 디렉터리(기본: 설정의 logging.dir)")
    parser.add_argument("--bucket", type=parse_bucket, default=timedelta(minutes=10),
                        help="시간대별 요약 구간(30s·10m·1h, 기본 10m)")
    parser.add_argument("--top", type=int, default=10, help="느린 job/task 를 몇 개 보일지(기본 10)")
    parser.add_argument("--since", type=_parse_time, help="이 시각 이후만 본다")
    parser.add_argument("--until", type=_parse_time, help="이 시각 이전만 본다")
    parser.add_argument("--source", action="append", help="이 서비스 이름만 본다(여러 번 줄 수 있다)")
    parser.add_argument("--keys", type=parse_keys,
                        help="요약 표 열을 이 지표로 바꾼다(쉼표 구분, 키:sum|max|rate|last)")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """지표 파일을 읽어 요약을 출력한다. 읽을 파일이나 조건에 맞는 줄이 없으면 1 을 돌려준다."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        files = expand_paths(args.paths or default_paths())
    except FileNotFoundError as exc:
        parser.error(f"경로가 없다: {exc}")
    if not files:
        print("지표 파일(*-perf.log)이 없다. logging.perf.enabled 와 로그 디렉터리를 확인한다.", file=sys.stderr)
        return 1
    by_source = load(files, args.since, args.until)
    if args.source:
        by_source = {k: v for k, v in by_source.items() if k in set(args.source)}
    if not by_source:
        print("조건에 맞는 지표 줄이 없다.", file=sys.stderr)
        return 1
    for line in report(by_source, bucket=args.bucket, top=args.top, keys=args.keys):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
