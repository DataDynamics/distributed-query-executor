"""executor 헬스/메트릭 모니터.

coordinator는 각 executor의 /health·/metrics 를 주기적으로 폴링해 상태를 메모리에
보유하고, 별도 주기로 그 스냅샷(특히 CPU/메모리 사용량)을 PostgreSQL 테이블에 기록한다.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class ExecutorHealth:
    executor_url: str
    healthy: bool = False
    last_checked: Optional[str] = None
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    memory_used_mb: Optional[float] = None
    memory_total_mb: Optional[float] = None
    disk_percent: Optional[float] = None
    disk_used_gb: Optional[float] = None
    disk_total_gb: Optional[float] = None
    active_tasks: Optional[int] = None
    max_concurrent_tasks: Optional[int] = None
    error: Optional[str] = None

    def as_view(self) -> dict:
        return asdict(self)


_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    id              BIGSERIAL PRIMARY KEY,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    executor_url    TEXT NOT NULL,
    healthy         BOOLEAN NOT NULL,
    cpu_percent     DOUBLE PRECISION,
    memory_percent  DOUBLE PRECISION,
    memory_used_mb  DOUBLE PRECISION,
    memory_total_mb DOUBLE PRECISION,
    disk_percent    DOUBLE PRECISION,
    error           TEXT
)
"""

_INSERT = """
INSERT INTO {table}
    (executor_url, healthy, cpu_percent, memory_percent,
     memory_used_mb, memory_total_mb, disk_percent, error)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


class HealthMonitor:
    def __init__(self, settings):
        self.settings = settings
        self.executors: dict[str, ExecutorHealth] = {
            url: ExecutorHealth(executor_url=url) for url in settings.executors
        }
        self._tasks: list[asyncio.Task] = []
        self._ddl_ready = False

    # ───────── 생명주기 ─────────

    async def start(self) -> None:
        if not self.settings.monitor_enabled:
            logger.info("모니터 비활성(monitor.enabled=false)")
            return
        if not self.executors:
            logger.info("등록된 executor가 없어 모니터를 시작하지 않음")
            return
        self._tasks.append(asyncio.create_task(self._health_loop()))
        if self.settings.monitor_db_dsn:
            self._tasks.append(asyncio.create_task(self._record_loop()))
            logger.info(
                "헬스 모니터 시작: 폴링 %ss, 기록 %ss -> %s",
                self.settings.monitor_health_interval_s,
                self.settings.monitor_record_interval_s,
                self.settings.monitor_table,
            )
        else:
            logger.info(
                "헬스 모니터 시작: 폴링 %ss (monitor.db_dsn 미설정 → DB 기록 비활성)",
                self.settings.monitor_health_interval_s,
            )

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks.clear()

    def snapshot(self) -> list[dict]:
        return [h.as_view() for h in self.executors.values()]

    async def poll_now(self) -> list[dict]:
        """등록된 모든 executor를 즉시 1회 폴링하고 최신 스냅샷을 반환한다(on-demand)."""
        if not self.executors:
            return []
        async with httpx.AsyncClient(timeout=5.0) as client:
            await asyncio.gather(
                *(self._poll_one(client, url) for url in self.executors)
            )
        return self.snapshot()

    # ───────── 폴링 루프 ─────────

    async def _health_loop(self) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            while True:
                await asyncio.gather(
                    *(self._poll_one(client, url) for url in self.executors)
                )
                await asyncio.sleep(self.settings.monitor_health_interval_s)

    async def _poll_one(self, client: httpx.AsyncClient, url: str) -> None:
        rec = self.executors[url]
        rec.last_checked = datetime.now(timezone.utc).isoformat()
        try:
            health = await client.get(f"{url}/health")
            health.raise_for_status()
            metrics = await client.get(f"{url}/metrics")
            metrics.raise_for_status()
            md = metrics.json()
            mem = md.get("memory", {})
            disk = md.get("disk", {})
            rec.healthy = True
            rec.cpu_percent = md.get("cpu_percent")
            rec.memory_percent = mem.get("percent")
            rec.memory_used_mb = mem.get("used_mb")
            rec.memory_total_mb = mem.get("total_mb")
            rec.disk_percent = disk.get("percent")
            rec.disk_used_gb = disk.get("used_gb")
            rec.disk_total_gb = disk.get("total_gb")
            tasks = md.get("tasks", {})
            rec.active_tasks = tasks.get("active")
            rec.max_concurrent_tasks = tasks.get("max")
            rec.error = None
        except Exception as exc:
            rec.healthy = False
            rec.error = str(exc)
            logger.warning("executor %s 헬스 체크 실패: %s", url, exc)

    # ───────── DB 기록 루프 ─────────

    async def _record_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.monitor_record_interval_s)
            try:
                await asyncio.to_thread(self._write_pg, self.snapshot())
            except Exception:
                logger.exception("메트릭 PostgreSQL 기록 실패")

    def _write_pg(self, snapshot: list[dict]) -> None:
        import psycopg  # 지연 임포트(모니터 미사용 시 psycopg 불필요)

        table = self.settings.monitor_table
        rows = [
            (
                r["executor_url"],
                r["healthy"],
                r["cpu_percent"],
                r["memory_percent"],
                r["memory_used_mb"],
                r["memory_total_mb"],
                r["disk_percent"],
                r["error"],
            )
            for r in snapshot
        ]
        with psycopg.connect(self.settings.monitor_db_dsn) as conn:
            with conn.cursor() as cur:
                if not self._ddl_ready:
                    cur.execute(_CREATE_TABLE.format(table=table))
                    self._ddl_ready = True
                cur.executemany(_INSERT.format(table=table), rows)
            conn.commit()
        logger.info("메트릭 %d건 기록 완료 -> %s", len(rows), table)
