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

# task_history 테이블 DDL. 상태 전이마다 한 행씩 쌓는 append-only 구조라 PK 는 단조
# 증가하는 id 이고, 같은 task_id 가 여러 번 등장한다. {table} 은 설정값으로 치환된다.
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
    """task 상태 전이 이력을 PostgreSQL 에 append/조회하는 저장소.

    각 상태 전이(QUEUED/READING/WRITING/DONE/FAILED/CANCELLED)마다 한 행씩 INSERT 하는
    append-only 로그라, 한 task 는 여러 행으로 남는다. 조회 시에는 task_id 별 최신 행만
    추려서 "현재 상태"처럼 보여준다(read 참고). DSN 이 없으면 비활성으로 동작한다.

    인자:
        settings: history_db_dsn(DSN), task_history_table(테이블명)을 읽는 설정 객체.
    """

    def __init__(self, settings):
        self.dsn: str = getattr(settings, "history_db_dsn", "") or ""
        self.table: str = getattr(settings, "task_history_table", "task_history")
        self.executor_id: str = _executor_id()
        self.enabled: bool = bool(self.dsn)
        self._ddl_ready = False

    def read(self, limit: int = 50, offset: int = 0) -> dict:
        """이 executor 의 task 이력 조회(task_id별 최신 1건, 페이징).

        append-only 로그에서 task 당 마지막 상태만 보여주기 위해 PostgreSQL 의
        ``DISTINCT ON (task_id) ... ORDER BY task_id, recorded_at DESC`` 를 사용한다.
        이는 task_id 별로 recorded_at 이 가장 최신인 행 하나만 남기는 패턴이다. 그 결과를
        바깥 쿼리에서 recorded_at DESC 로 다시 정렬해 최근 활동 순으로 페이징한다.
        total 은 (이 executor 의) 서로 다른 task_id 개수다.

        인자:
            limit: 페이지 크기(행 수).
            offset: 건너뛸 행 수.

        반환:
            {enabled, rows, total, limit, offset}. 비활성(DSN 미설정) 시 enabled=False,
            빈 rows 를 돌려준다.
        """
        if not self.enabled:
            return {"enabled": False, "rows": [], "total": 0, "limit": limit, "offset": offset}
        import psycopg  # 지연 임포트

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE.format(table=self.table))  # 없으면 생성
                cur.execute(
                    f"SELECT count(DISTINCT task_id) FROM {self.table} WHERE executor_id = %s",
                    (self.executor_id,),
                )
                total = cur.fetchone()[0]
                cur.execute(
                    "SELECT recorded_at, job_id, task_id, username, status, "
                    "rows_written, error FROM ("
                    "  SELECT DISTINCT ON (task_id) recorded_at, job_id, task_id, "
                    "    username, status, rows_written, error "
                    f"  FROM {self.table} WHERE executor_id = %s "
                    "  ORDER BY task_id, recorded_at DESC"
                    ") t ORDER BY recorded_at DESC LIMIT %s OFFSET %s",
                    (self.executor_id, limit, offset),
                )
                rows = cur.fetchall()
            conn.commit()
        out = [
            {
                "recorded_at": r[0].isoformat() if r[0] is not None else None,
                "job_id": r[1], "task_id": r[2], "username": r[3], "status": r[4],
                "rows_written": r[5], "error": r[6],
            }
            for r in rows
        ]
        return {"enabled": True, "rows": out, "total": total, "limit": limit, "offset": offset}

    async def record(self, task) -> None:
        """task 의 현재 상태를 이력 테이블에 한 행 기록한다(동기 psycopg → 스레드).

        블로킹 DB 쓰기를 ``to_thread`` 로 넘겨 이벤트 루프를 막지 않는다. 이력 기록은
        부가 기능이므로, 비활성(DSN 미설정)이면 경고만 남기고 넘어가고, 기록 실패도
        예외를 삼켜 로깅만 한다(본 task 실행을 중단시키지 않음).
        """
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
        """이력 테이블에 한 행 INSERT(동기). 첫 호출에서만 테이블 DDL 을 보장한다."""
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
