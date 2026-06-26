"""coordinator가 executor self-report(공유 DB)를 읽는 저장소.

멀티 coordinator에서 executor 상태의 단일 출처(executor_status 테이블)를 읽는다.
liveness 는 updated_at 신선도(stale_seconds)로 판정한다.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ExecutorStatusRepository:
    def __init__(self, dsn: str, table: str = "executor_status", stale_seconds: float = 30):
        self.dsn = dsn
        self.table = table
        self.stale_seconds = stale_seconds

    def read_all(self) -> list[dict]:
        """executor 상태 목록. healthy 는 updated_at 신선도로 판정."""
        import psycopg  # 지연 임포트

        sql = (
            "SELECT executor_id, cpu_percent, memory_percent, memory_used_mb, "
            "memory_total_mb, disk_percent, disk_used_gb, disk_total_gb, "
            "active_tasks, max_concurrent_tasks, "
            "EXTRACT(EPOCH FROM (now() - updated_at)) AS age_s, updated_at "
            f"FROM {self.table}"
        )
        try:
            with psycopg.connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall()
        except Exception:
            logger.exception("executor_status 조회 실패")
            return []

        result = []
        for r in rows:
            age = float(r[10]) if r[10] is not None else None
            healthy = age is not None and age <= self.stale_seconds
            result.append({
                "executor_id": r[0],
                "healthy": healthy,
                "cpu_percent": r[1],
                "memory_percent": r[2],
                "memory_used_mb": r[3],
                "memory_total_mb": r[4],
                "disk_percent": r[5],
                "disk_used_gb": r[6],
                "disk_total_gb": r[7],
                "active_tasks": r[8],
                "max_concurrent_tasks": r[9],
                "age_seconds": round(age, 1) if age is not None else None,
                "updated_at": r[11].isoformat() if r[11] is not None else None,
            })
        return result
