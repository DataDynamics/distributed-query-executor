"""Job 실행 이력을 PostgreSQL 에 기록하는 저장소.

run() 안에서 상태 전이(시작/종료)마다 한 행씩 append 하여 감사(audit) 이력을 남긴다.
DSN이 설정되지 않으면 기록은 비활성화되고 경고만 남긴다.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    id                 BIGSERIAL PRIMARY KEY,
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    job_id             TEXT NOT NULL,
    status             TEXT NOT NULL,
    partition_column   TEXT,
    target_table       TEXT,
    parallelism        INTEGER,
    total_tasks        INTEGER,
    completed_tasks    INTEGER,
    total_rows_written BIGINT,
    error              TEXT,
    created_at         TIMESTAMPTZ,
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    original_sql       TEXT
)
"""

_INSERT = """
INSERT INTO {table}
    (job_id, status, partition_column, target_table, parallelism,
     total_tasks, completed_tasks, total_rows_written, error,
     created_at, started_at, finished_at, original_sql)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


class JobHistoryRepository:
    def __init__(self, settings):
        self.dsn: str = getattr(settings, "history_db_dsn", "") or ""
        self.table: str = getattr(settings, "history_table", "job_history")
        self.enabled: bool = bool(self.dsn)
        self._ddl_ready = False

    async def record(self, job) -> None:
        """현재 Job 상태를 이력 테이블에 한 행 기록한다(스레드에서 동기 psycopg 실행)."""
        if not self.enabled:
            logger.warning(
                "history.db_dsn 미설정 → job %s 이력 기록 생략 (status=%s)",
                job.job_id,
                job.status.value,
            )
            return
        try:
            await asyncio.to_thread(self._write, job)
        except Exception:
            logger.exception("job %s 이력 기록 실패", job.job_id)

    def read(self, limit: int = 20, offset: int = 0) -> dict:
        """과거 실행 이력 조회(페이징). 반환: {enabled, rows, total, limit, offset}."""
        if not self.enabled:
            return {"enabled": False, "rows": [], "total": 0, "limit": limit, "offset": offset}
        import psycopg  # 지연 임포트

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE.format(table=self.table))  # 없으면 생성
                cur.execute(f"SELECT count(*) FROM {self.table}")
                total = cur.fetchone()[0]
                cur.execute(
                    "SELECT recorded_at, job_id, status, partition_column, target_table, "
                    "completed_tasks, total_tasks, total_rows_written, error "
                    f"FROM {self.table} ORDER BY recorded_at DESC LIMIT %s OFFSET %s",
                    (limit, offset),
                )
                rows = cur.fetchall()
            conn.commit()
        out = [
            {
                "recorded_at": r[0].isoformat() if r[0] is not None else None,
                "job_id": r[1], "status": r[2], "partition_column": r[3],
                "target_table": r[4], "completed_tasks": r[5], "total_tasks": r[6],
                "total_rows_written": r[7], "error": r[8],
            }
            for r in rows
        ]
        return {"enabled": True, "rows": out, "total": total, "limit": limit, "offset": offset}

    def _write(self, job) -> None:
        import psycopg  # 지연 임포트

        row = (
            job.job_id,
            job.status.value,
            job.partition_column,
            job.target_table,
            job.parallelism,
            len(job.tasks),
            job.completed,
            job.total_rows_written,
            job.error,
            job.created_at,
            job.started_at,
            job.finished_at,
            job.original_sql,
        )
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                if not self._ddl_ready:
                    cur.execute(_CREATE_TABLE.format(table=self.table))
                    self._ddl_ready = True
                cur.execute(_INSERT.format(table=self.table), row)
            conn.commit()
        logger.info("job %s 이력 기록(status=%s) -> %s", job.job_id, job.status.value, self.table)
