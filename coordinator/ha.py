"""HA(다중 coordinator) 정합: 죽은 coordinator 소유의 비종료 job 복구(Phase 3-C).

HA 에서 어떤 coordinator 가 죽으면, 그가 실행 중이던 job 은 더 진행되지 않는다(실행 루프
소멸). 공유 Job 저장소(SqlJobStore)에는 그 job 이 RUNNING 등으로 남아 있어 상태가 부정확하다.

각 coordinator 는 ``coordinator_status`` 테이블에 자기 생존을 주기적으로 heartbeat 하고,
살아있는 coordinator 집합을 읽어 **소유자가 죽은 비종료 job 을 FAILED 로 정합**한다(이후
``/jobs/{id}/retry`` 로 실패 파티션만 재개 가능). 정합 시 ``save`` 가 job 의 coordinator_id 를
자신으로 재스탬프해 소유를 이전한다.

구성:
- ``select_orphans`` : (job_id, status, owner) + 살아있는 id 집합 → 고아 job_id 목록(순수 함수).
- ``CoordinatorHeartbeat`` : coordinator_status 에 beat()/fresh_ids().
- ``reconcile_orphaned_jobs`` : store.list_owned() + heartbeat.fresh_ids() 로 고아를 FAILED 정합.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .models import JobStatus, TaskStatus

logger = logging.getLogger(__name__)

# 비종료(=소유자 사망 시 중단으로 간주) job/task 상태.
_NON_TERMINAL = {JobStatus.PENDING, JobStatus.SPLITTING, JobStatus.RUNNING}
_TERMINAL_TASK = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}


def select_orphans(owned, fresh_ids) -> list:
    """소유자가 죽은(또는 없는) 비종료 job 의 id 목록을 고른다(순수 함수).

    인자:
        owned: ``(job_id, status, coordinator_id)`` 튜플들의 반복자.
        fresh_ids: 살아있는 coordinator id 집합.
    """
    non_terminal = {s.value for s in _NON_TERMINAL}
    out = []
    for job_id, status, owner in owned:
        if status in non_terminal and (not owner or owner not in fresh_ids):
            out.append(job_id)
    return out


class CoordinatorHeartbeat:
    """coordinator_status 테이블에 자기 생존을 upsert 하고 살아있는 coordinator 집합을 읽는다.

    DB 작업 실패는 정합 자체가 실패해선 안 되므로 로깅만 하고 안전값(빈 집합)으로 처리한다.
    """

    def __init__(self, dsn: str, coordinator_id: str, table: str = "coordinator_status"):
        self.dsn = dsn
        self.coordinator_id = coordinator_id
        self.table = table

    def _conn(self):
        import psycopg  # 지연 임포트
        return psycopg.connect(self.dsn)

    def beat(self) -> None:
        sql = (
            f"INSERT INTO {self.table} (coordinator_id, updated_at) VALUES (%s, now()) "
            "ON CONFLICT (coordinator_id) DO UPDATE SET updated_at = now()"
        )
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (self.coordinator_id,))
                conn.commit()
        except Exception:
            logger.exception("coordinator heartbeat 실패")

    def fresh_ids(self, stale_s: float) -> set:
        sql = (
            f"SELECT coordinator_id FROM {self.table} "
            "WHERE now() - updated_at <= make_interval(secs => %s)"
        )
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (float(stale_s),))
                    rows = cur.fetchall()
            return {r[0] for r in rows}
        except Exception:
            logger.exception("coordinator fresh_ids 조회 실패")
            return set()


def reconcile_orphaned_jobs(store, heartbeat: "CoordinatorHeartbeat", stale_s: float) -> int:
    """죽은 coordinator 소유의 비종료 job 을 FAILED 로 정합한다(retry 로 재개 가능). 정합 수 반환.

    ``store`` 가 ``list_owned()``(=(job_id, status, coordinator_id) 목록)를 제공할 때만
    동작한다(SqlJobStore). 자기 자신은 항상 살아있는 것으로 본다(자기 job 은 건드리지 않음).
    """
    list_owned = getattr(store, "list_owned", None)
    if list_owned is None:
        return 0
    try:
        owned = list_owned()
    except Exception:
        logger.exception("orphan 정합용 job 조회 실패")
        return 0

    fresh = set(heartbeat.fresh_ids(stale_s))
    fresh.add(heartbeat.coordinator_id)  # 자기 자신은 항상 살아있음
    orphan_ids = select_orphans(owned, fresh)
    if not orphan_ids:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    reconciled = 0
    for jid in orphan_ids:
        job = store.get(jid)
        if job is None or job.status not in _NON_TERMINAL:
            continue  # 그 사이 종료/변경됐으면 건너뜀
        for t in job.tasks:
            if t.status not in _TERMINAL_TASK:
                t.status = TaskStatus.FAILED
                t.error = t.error or "소유 coordinator 장애로 중단됨"
        job.status = JobStatus.FAILED
        job.error = job.error or "소유 coordinator 장애로 중단됨"
        job.finished_at = job.finished_at or now
        store.save(job)  # 소유 이전(coordinator_id 재스탬프) + 상태 영속
        reconciled += 1
    if reconciled:
        logger.warning("orphan 정합: 죽은 coordinator 소유 job %d개를 FAILED 로 표시", reconciled)
    return reconciled
