"""종료된 job 이 어느 구간에서 시간을 썼는지 집계하는 순수 로직이다(대시보드 "구간 분석").

task 타임라인은 task 하나의 단계만 보여 주므로, task 가 수십 개인 job 에서 "이 job 은 어디가 느렸나"를 읽으려면
task 를 하나씩 넘겨 봐야 한다. 여기서는 두 가지 관점으로 한 번에 요약한다.

첫째는 **job 구간**이다. job 의 벽시계 시간을 실행 슬롯 대기, 디스패치 대기, task 실행, 마감(Phase 2 적재 포함)
네 구간으로 나눈다. job 의 created_at·started_at·finished_at 과 task 단계들의 가장 이른 시작·가장 늦은 종료로
경계를 잡는다. coordinator 와 executor 의 시계가 조금 어긋날 수 있으므로 음수는 0 으로 자른다.

둘째는 **단계별 평균**이다. task 마다 같은 이름의 단계 소요를 합친 뒤(재시도로 단계가 여러 번 열릴 수 있다)
그 단계를 거친 task 들의 평균·최대·최소를 낸다. 가장 평균이 큰 단계가 이 job 의 병목 구간이다. 평균은 그
단계를 거친 task 수(``n``)로 나누므로, 일부 task 만 거친 단계(예: 재시도)도 실제 한 번의 비용으로 읽힌다.

입력은 dict 다. 저장소에 job 이 남아 있으면 ``Job.status_view()`` 를, 없으면 이력 테이블에서 복원한 같은 모양의
dict 를 넘긴다. DB·네트워크를 부르지 않으므로 그대로 단위 테스트한다.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from core.phases import relabel_phases

# Phase 1 뒤에 coordinator 가 GP 적재(Phase 2)와 정리(Phase 3)를 하는 모드다. 마감 구간의 이름을 가른다.
_STAGED_MODES = ("local_stage", "s3_stage")


def _ts(value) -> Optional[datetime]:
    """ISO 시각 문자열(또는 datetime)을 naive datetime 으로 바꾼다. 비었거나 읽을 수 없으면 None 이다."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "")).replace(tzinfo=None)
    except ValueError:
        return None


def _gap_ms(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    """두 시각의 간격(ms)이다. 한쪽이 없으면 None, 시계 어긋남으로 음수가 나오면 0 이다."""
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def _phase_ms(phase: dict) -> Optional[int]:
    """단계 하나의 소요(ms)다. duration_ms 가 없으면 시작·종료 시각으로 계산하고, 진행 중이면 None 이다."""
    dur = phase.get("duration_ms")
    if dur is not None:
        return max(0, int(dur))
    return _gap_ms(_ts(phase.get("started_at")), _ts(phase.get("finished_at")))


def _task_span(phases: list) -> tuple[Optional[datetime], Optional[datetime]]:
    """task 단계들 중 가장 이른 시작과 가장 늦은 종료다."""
    starts = [t for t in (_ts(p.get("started_at")) for p in phases) if t is not None]
    ends = [t for t in (_ts(p.get("finished_at")) for p in phases) if t is not None]
    return (min(starts) if starts else None, max(ends) if ends else None)


def _segment(key: str, label: str, ms: Optional[int], note: str) -> dict:
    return {"key": key, "label": label, "ms": ms, "note": note}


def job_breakdown(job: dict) -> dict:
    """job 하나의 구간별 소요와 task 단계별 평균 소요를 계산한다.

    반환 dict 의 ``segments`` 는 job 구간 목록이고 ``phases`` 는 단계별 통계(처음 나타난 순서)다. 가장 오래
    걸린 구간과 단계는 ``longest_segment``·``longest_phase`` 에 이름으로 담는다. 측정할 수 없는 값은 None 이다.
    """
    tasks = job.get("tasks") or []
    datasource = job.get("datasource")

    # ── 단계별 통계: task 마다 같은 이름의 단계 소요를 합친 뒤 task 들 사이에서 평균을 낸다 ──
    order: list[str] = []
    labels: dict[str, str] = {}
    per_task: dict[str, list[tuple[int, str]]] = {}
    task_totals: list[int] = []
    spans: list[tuple[Optional[datetime], Optional[datetime]]] = []
    for t in tasks:
        phases = relabel_phases(t.get("phases"), datasource) if datasource else list(t.get("phases") or [])
        sums: dict[str, int] = {}
        for p in phases:
            name = p.get("name")
            ms = _phase_ms(p)
            if not name or ms is None:
                continue
            if name not in labels:
                order.append(name)
                labels[name] = p.get("label") or name
            sums[name] = sums.get(name, 0) + ms
        for name, ms in sums.items():
            per_task.setdefault(name, []).append((ms, t.get("task_id") or ""))
        span = _task_span(phases)
        spans.append(span)
        total = _gap_ms(*span)
        if total is not None:
            task_totals.append(total)

    stats = []
    for name in order:
        vals = per_task.get(name) or []
        if not vals:
            continue
        ms_list = [v for v, _ in vals]
        slowest = max(vals, key=lambda v: v[0])
        stats.append({
            "name": name, "label": labels[name], "n": len(vals),
            "avg_ms": int(round(sum(ms_list) / len(ms_list))),
            "max_ms": slowest[0], "min_ms": min(ms_list), "slowest_task": slowest[1],
        })
    avg_sum = sum(s["avg_ms"] for s in stats)
    for s in stats:
        # 평균 task 한 건의 처리 시간 중 이 단계가 차지하는 비중이다.
        s["share_pct"] = round(s["avg_ms"] / avg_sum * 100, 1) if avg_sum else 0.0
    longest_phase = max(stats, key=lambda s: s["avg_ms"])["name"] if stats else None

    # ── job 구간: 슬롯 대기 → 디스패치 대기 → task 실행 → 마감 ──
    created = _ts(job.get("created_at"))
    started = _ts(job.get("started_at"))
    finished = _ts(job.get("finished_at"))
    first_start = min((s for s, _ in spans if s is not None), default=None)
    last_end = max((e for _, e in spans if e is not None), default=None)
    staged = job.get("exec_mode") in _STAGED_MODES
    segments = [
        _segment("slot_wait", "실행 슬롯 대기", _gap_ms(created, started),
                 "접수부터 job 실행 슬롯(coordinator.max_concurrent_jobs)을 얻을 때까지"),
        _segment("dispatch_wait", "디스패치 대기", _gap_ms(started, first_start),
                 "슬롯을 얻은 뒤 첫 task 가 executor 에서 시작될 때까지(max_dispatch_concurrency 대기 포함)"),
        _segment("tasks", "task 실행", _gap_ms(first_start, last_end),
                 "첫 task 시작부터 마지막 task 종료까지"),
        _segment("finalize", "Phase 2 적재·정리" if staged else "마감",
                 _gap_ms(last_end or started, finished),
                 "모든 task 종료 뒤 coordinator 의 GP 적재·정리와 상태 확정" if staged
                 else "모든 task 종료 뒤 상태 확정까지"),
    ]
    measured = [s for s in segments if s["ms"] is not None]
    longest_segment = max(measured, key=lambda s: s["ms"])["key"] if measured else None

    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "exec_mode": job.get("exec_mode"),
        "total_ms": _gap_ms(created, finished),
        "segments": segments,
        "longest_segment": longest_segment,
        "phases": stats,
        "longest_phase": longest_phase,
        "task_count": len(tasks),
        "task_avg_ms": int(round(sum(task_totals) / len(task_totals))) if task_totals else None,
        "task_max_ms": max(task_totals) if task_totals else None,
    }
