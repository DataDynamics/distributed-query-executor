"""task 실행의 세부 단계(phase) 타임라인을 다루는 공용 로직이며 coordinator 와 executor 가 공유한다.

하나의 task 는 status(QUEUED/READING/WRITING/DONE...) 아래에서 여러 **세부 단계**를
거친다. 예를 들어 소스 조회 제출, 스트리밍 COPY, INSERT, 커밋 순이다. 이 모듈은 그 단계의
시작/종료 시각과 처리량을 리스트로 누적하는 순수 로직을 제공한다. 백엔드(backend.py)는
``on_stage(name, event, meta)`` 콜백으로 단계 경계를 알리고, 그 콜백이 여기 함수를 호출해
각 Task(coordinator·executor 양쪽 dataclass)의 ``phases`` 리스트를 채운다.

단계 레코드(dict) 형태::

    {"name": "STREAM_COPY", "label": "스트리밍 COPY",
     "started_at": "2026-07-01T14:03:11.020",   # KST naive ISO
     "finished_at": "2026-07-01T14:15:44.900",   # 진행 중이면 None
     "duration_ms": 753880,                        # 종료 시 계산(진행 중이면 None)
     "rows": 8421000,                              # 이 단계가 처리한 행수(있을 때만)
     "extra": {"read_wait_ms": 610500, "write_wait_ms": 138300, "rows_per_sec": 11170}}

시각 권위는 앱 계층(now_iso, KST naive)에 두어 started_at/finished_at 표기와 일관되게 한다.
read_wait/write_wait 처럼 루프 내부에서만 잴 수 있는 값은 백엔드가 ``meta`` 로 실어 보낸다.
"""

from __future__ import annotations

from datetime import datetime

from core.timeutil import now_iso

# 단계 이름을 사람이 읽을 한글 라벨로 옮긴다. 대시보드와 이력이 공통으로 쓴다.
# 백엔드는 name 만 넘기고 라벨 매핑은 여기 한 곳에서 관리해 표기를 일관되게 유지한다.
PHASE_LABELS: dict[str, str] = {
    "QUEUE_WAIT": "대기(슬롯)",       # 접수부터 실행 슬롯을 확보할 때까지 기다리는 구간이다
    "IMPALA_SUBMIT": "Impala 조회 제출",  # execute() 를 제출하고 커서의 description 이 준비될 때까지다
    "STAGING_DDL": "staging 생성",     # CREATE TEMP TABLE (stage_insert)
    "PREFLIGHT": "컬럼 검증",          # copy 모드에서 COPY 전에 대상 컬럼을 사전검증한다
    "DELETE": "파티션 선삭제",         # copy 모드의 overwrite_partitions 가 파티션을 먼저 지운다
    "STREAM_COPY": "스트리밍 COPY",    # 소스 fetch 와 Greenplum COPY 를 교차로 스트리밍한다
    "EXPORT_WRITE": "CSV export",      # local_stage·s3_stage 에서 소스 fetch 결과를 로컬 CSV 로 쓴다
    "S3_UPLOAD": "S3 업로드",          # s3_stage 에서 로컬 CSV 를 S3 로 올린다
    "S3_EXTERNAL_DDL": "S3 외부테이블", # CREATE EXTERNAL TABLE (PXF, s3_stage)
    "INSERT": "INSERT",               # staging 을 target 으로 넣거나 statement 를 직접 실행한다
    "COMMIT": "커밋",                  # 트랜잭션을 커밋한다
    "CLEANUP": "정리",                 # local_stage·s3_stage 의 외부테이블과 스테이지를 정리한다
}


def _dur_ms(start_iso: str, end_iso: str) -> int | None:
    """두 naive ISO 시각 문자열의 간격을 밀리초 정수로 돌려준다. 파싱에 실패하면 None 이다."""
    try:
        delta = datetime.fromisoformat(end_iso) - datetime.fromisoformat(start_iso)
        return int(delta.total_seconds() * 1000)
    except (ValueError, TypeError):
        return None


def record_stage(phases: list, name: str, event: str, meta: dict | None = None):
    """``phases`` 리스트에 단계 시작(start)/종료(end)를 기록한다.

    - start: 새 단계 dict 를 append 하고 started_at 을 지금으로 찍는다.
    - end: 같은 이름의 **아직 열려 있는(finished_at=None)** 가장 최근 단계를 닫고,
      duration_ms 를 계산하며 meta 의 rows/기타 지표를 병합한다. rows 는 별도 필드로,
      나머지는 extra 로 넣는다. 종료된 단계의 rows(있으면)를 반환한다(호출자 편의).

    같은 이름 단계가 다시 시작될 수 있으므로(재시도 등) '열려 있는 마지막' 것을 닫는다.
    """
    if event == "start":
        phases.append({
            "name": name,
            "label": PHASE_LABELS.get(name, name),
            "started_at": now_iso(),
            "finished_at": None,
            "duration_ms": None,
            "rows": None,
            "extra": None,
        })
        return None
    if event == "end":
        for phase in reversed(phases):
            if phase["name"] == name and phase["finished_at"] is None:
                end = now_iso()
                phase["finished_at"] = end
                phase["duration_ms"] = _dur_ms(phase["started_at"], end)
                merged = dict(meta or {})
                rows = merged.pop("rows", None)
                if rows is not None:
                    phase["rows"] = rows
                phase["extra"] = merged or None
                return rows
    return None


def close_open_phases(phases: list) -> None:
    """아직 열려 있는(finished_at=None) 모든 단계를 지금 시각으로 마감한다.

    task 가 단계 도중 **실패/취소로 종료**될 때 호출한다. 백엔드가 ``on_stage(name,"start")``
    만 방출하고 예외로 빠지면 그 단계는 finished_at 이 None 인 채 남는데, 그대로 두면 대시보드가
    소요시간을 ``now - started_at`` 으로 계속 키워 "진행중"으로 표시한다(종료됐는데도 시간이
    증가). 종료 시점으로 finished_at/duration_ms 를 확정해 이 문제를 막는다. 이미 닫힌 단계는
    건드리지 않는다.
    """
    end = now_iso()
    for phase in phases:
        if phase.get("finished_at") is None and phase.get("started_at"):
            phase["finished_at"] = end
            phase["duration_ms"] = _dur_ms(phase["started_at"], end)


def phase_of(phases: list, name: str) -> dict | None:
    """이름이 ``name`` 인 마지막 단계 레코드를 반환한다(없으면 None)."""
    for phase in reversed(phases):
        if phase["name"] == name:
            return phase
    return None
