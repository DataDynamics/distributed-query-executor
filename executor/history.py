"""executor task(=executor job) 실행 이력을 PostgreSQL 에 기록하는 저장소.

각 executor 가 자신이 처리하는 task 의 상태 전이(QUEUED/READING/WRITING/DONE/FAILED)
마다 한 행씩 append 한다. 하나의 job_id 아래 N개 task 가 executor 별로 기록된다.
DSN이 설정되지 않으면 비활성(경고)된다.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    id            BIGSERIAL PRIMARY KEY,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    job_id        TEXT NOT NULL,
    task_id       TEXT NOT NULL,
    username      TEXT,
    executor_id   TEXT,
    status        TEXT NOT NULL,
    rows_written  BIGINT,
    error         TEXT
)
"""

_INSERT = """
INSERT INTO {table} (job_id, task_id, username, executor_id, status, rows_written, error)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def _executor_id() -> str:
    """이 executor 인스턴스 식별자(호스트:포트 또는 EXECUTOR_INSTANCE)."""
    inst = os.getenv("EXECUTOR_INSTANCE") or os.getenv("EXECUTOR_PORT")
    host = socket.gethostname()
    return f"{host}:{inst}" if inst else host


class TaskHistoryRepository:
    def __init__(self, settings):
        self.dsn: str = getattr(settings, "history_db_dsn", "") or ""
        self.table: str = getattr(settings, "task_history_table", "task_history")
        self.executor_id: str = _executor_id()
        self.enabled: bool = bool(self.dsn)
        self._ddl_ready = False

    async def record(self, task) -> None:
        """task 의 현재 상태를 이력 테이블에 한 행 기록한다(동기 psycopg → 스레드)."""
        if not self.enabled:
            logger.warning(
                "history.db_dsn 미설정 → task %s 이력 기록 생략 (status=%s)",
                task.task_id,
                task.status.value,
            )
            return
        try:
            await asyncio.to_thread(self._write, task)
        except Exception:
            logger.exception("task %s 이력 기록 실패", task.task_id)

    def _write(self, task) -> None:
        import psycopg  # 지연 임포트

        row = (
            task.job_id,
            task.task_id,
            getattr(task, "username", None),
            self.executor_id,
            task.status.value,
            task.rows_written,
            task.error,
        )
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                if not self._ddl_ready:
                    cur.execute(_CREATE_TABLE.format(table=self.table))
                    self._ddl_ready = True
                cur.execute(_INSERT.format(table=self.table), row)
            conn.commit()
