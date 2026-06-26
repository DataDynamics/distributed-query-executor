"""Job 저장소.

- InMemoryJobStore: 단일 coordinator(기본). 프로세스 메모리.
- SqlJobStore: 여러 coordinator 가 공유(PostgreSQL). 어느 coordinator로 요청이 가도
  조회/취소가 가능하도록 Job 스냅샷을 영속한다.

공통 인터페이스:
  add(job), get(job_id), list(), save(job),
  request_cancel(job_id), is_cancel_requested(job_id)
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from .models import Job

logger = logging.getLogger(__name__)


class InMemoryJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}

    def add(self, job: Job) -> None:
        self._jobs[job.job_id] = job

    def save(self, job: Job) -> None:
        # 객체를 그대로 들고 있으므로 별도 영속 불필요
        self._jobs[job.job_id] = job

    def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def list(self) -> list[Job]:
        return list(self._jobs.values())

    def request_cancel(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None:
            return False
        job.cancel_requested = True
        return True

    def is_cancel_requested(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        return bool(job and job.cancel_requested)


_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    job_id           TEXT PRIMARY KEY,
    coordinator_id   TEXT,
    status           TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    data             JSONB NOT NULL
)
"""


class SqlJobStore:
    """PostgreSQL 공유 Job 저장소(멀티 coordinator). psycopg 는 지연 임포트."""

    def __init__(self, dsn: str, table: str = "jobs", coordinator_id: str = "-"):
        self.dsn = dsn
        self.table = table
        self.coordinator_id = coordinator_id
        self._ddl_ready = False

    def _conn(self):
        import psycopg  # 지연 임포트

        conn = psycopg.connect(self.dsn)
        if not self._ddl_ready:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE.format(table=self.table))
            conn.commit()
            self._ddl_ready = True
        return conn

    def add(self, job: Job) -> None:
        self.save(job)

    def save(self, job: Job) -> None:
        data = json.dumps(job.to_record())
        sql = (
            f"INSERT INTO {self.table} "
            "(job_id, coordinator_id, status, cancel_requested, updated_at, data) "
            "VALUES (%s, %s, %s, %s, now(), %s) "
            "ON CONFLICT (job_id) DO UPDATE SET "
            "status=EXCLUDED.status, cancel_requested=EXCLUDED.cancel_requested, "
            "updated_at=now(), data=EXCLUDED.data"
        )
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (job.job_id, self.coordinator_id, job.status.value,
                     job.cancel_requested, data),
                )
            conn.commit()

    def get(self, job_id: str) -> Optional[Job]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT data, cancel_requested FROM {self.table} WHERE job_id=%s",
                    (job_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        job = Job.from_record(data)
        job.cancel_requested = bool(row[1])  # 최신 취소 플래그 반영
        return job

    def list(self) -> list[Job]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT data FROM {self.table}")
                rows = cur.fetchall()
        jobs = []
        for (raw,) in rows:
            data = raw if isinstance(raw, dict) else json.loads(raw)
            jobs.append(Job.from_record(data))
        return jobs

    def request_cancel(self, job_id: str) -> bool:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {self.table} SET cancel_requested=TRUE, updated_at=now() "
                    "WHERE job_id=%s",
                    (job_id,),
                )
                updated = cur.rowcount
            conn.commit()
        return updated > 0

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT cancel_requested FROM {self.table} WHERE job_id=%s",
                    (job_id,),
                )
                row = cur.fetchone()
        return bool(row and row[0])


# 하위 호환 별칭(기존 import 유지)
JobStore = InMemoryJobStore


def build_job_store(settings) -> InMemoryJobStore | SqlJobStore:
    """설정에 따라 Job 저장소를 만든다(기본 memory)."""
    backend = getattr(settings, "store_backend", "memory")
    dsn = getattr(settings, "history_db_dsn", "")
    if backend == "postgres" and dsn:
        logger.info("SqlJobStore 사용(table=%s)", getattr(settings, "store_table", "jobs"))
        return SqlJobStore(
            dsn=dsn,
            table=getattr(settings, "store_table", "jobs"),
            coordinator_id=getattr(settings, "coordinator_id", "-"),
        )
    if backend == "postgres" and not dsn:
        logger.warning("store.backend=postgres 이나 DSN 미설정 → InMemoryJobStore 폴백")
    return InMemoryJobStore()
