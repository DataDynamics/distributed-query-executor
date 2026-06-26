"""executor 가 자기 상태(CPU/메모리/디스크/heartbeat)를 공유 DB에 self-report.

멀티 coordinator 환경에서 각 coordinator가 executor를 중복 폴링/기록하지 않도록,
상태의 '기록 주인'을 executor 로 둔다. coordinator 는 이 테이블을 읽기만 한다.
"""

from __future__ import annotations

import asyncio
import logging

from core.metrics import collect_system_metrics
from .history import _executor_id

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    executor_id     TEXT PRIMARY KEY,
    cpu_percent     DOUBLE PRECISION,
    memory_percent  DOUBLE PRECISION,
    memory_used_mb  DOUBLE PRECISION,
    memory_total_mb DOUBLE PRECISION,
    disk_percent    DOUBLE PRECISION,
    disk_used_gb    DOUBLE PRECISION,
    disk_total_gb   DOUBLE PRECISION,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_UPSERT = """
INSERT INTO {table}
    (executor_id, cpu_percent, memory_percent, memory_used_mb, memory_total_mb,
     disk_percent, disk_used_gb, disk_total_gb, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (executor_id) DO UPDATE SET
    cpu_percent=EXCLUDED.cpu_percent, memory_percent=EXCLUDED.memory_percent,
    memory_used_mb=EXCLUDED.memory_used_mb, memory_total_mb=EXCLUDED.memory_total_mb,
    disk_percent=EXCLUDED.disk_percent, disk_used_gb=EXCLUDED.disk_used_gb,
    disk_total_gb=EXCLUDED.disk_total_gb, updated_at=now()
"""


class ExecutorStatusReporter:
    def __init__(self, settings):
        self.dsn: str = getattr(settings, "history_db_dsn", "") or ""
        self.table: str = getattr(settings, "executor_status_table", "executor_status")
        self.interval: float = float(getattr(settings, "executor_status_interval_s", 10))
        self.disk_path: str = getattr(settings, "monitor_disk_path", "/")
        self.executor_id: str = _executor_id()
        self.enabled: bool = bool(self.dsn)
        self._task: asyncio.Task | None = None
        self._ddl_ready = False

    async def start(self) -> None:
        if not self.enabled:
            logger.warning("history.db_dsn 미설정 → executor self-report 비활성")
            return
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "executor self-report 시작(%ss) executor_id=%s -> %s",
            self.interval, self.executor_id, self.table,
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.to_thread(self._report_once)
            except Exception:
                logger.exception("executor self-report 실패")
            await asyncio.sleep(self.interval)

    def _report_once(self) -> None:
        import psycopg  # 지연 임포트

        m = collect_system_metrics(self.disk_path)
        mem, disk = m["memory"], m["disk"]
        row = (
            self.executor_id, m["cpu_percent"], mem["percent"], mem["used_mb"],
            mem["total_mb"], disk["percent"], disk["used_gb"], disk["total_gb"],
        )
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                if not self._ddl_ready:
                    cur.execute(_CREATE_TABLE.format(table=self.table))
                    self._ddl_ready = True
                cur.execute(_UPSERT.format(table=self.table), row)
            conn.commit()
