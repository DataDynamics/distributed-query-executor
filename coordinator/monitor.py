"""executor 헬스/메트릭 모니터.

coordinator 가 자신이 알고 있는 executor 목록(settings.executors)을 대상으로 다음 두 가지
백그라운드 루프를 돌린다.

  1) 헬스 폴링 루프(_health_loop): monitor_health_interval_s 주기로 각 executor 의
     /health·/metrics 엔드포인트를 호출해 살아있는지/자원 사용량을 확인하고, 그 결과를
     메모리(self.executors[url] 의 ExecutorHealth)에 최신값으로 덮어쓴다.
  2) DB 기록 루프(_record_loop): monitor_record_interval_s 주기로 위 메모리 스냅샷
     (특히 CPU/메모리 사용량)을 PostgreSQL 테이블에 누적 기록한다. DSN 미설정 시 이 루프는
     아예 시작하지 않는다.

대시보드/내부 코드는 snapshot() 으로 현재 메모리 상태를, poll_now() 로 즉시 1회 폴링 결과를
얻을 수 있다. 모든 폴링/기록 오류는 잡아서 로그만 남기고 루프는 계속 돈다(가용성 우선).
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
    """단일 executor 의 최신 헬스/메트릭 상태를 담는 메모리 레코드.

    헬스 폴링이 성공하면 healthy=True 와 함께 CPU/메모리/디스크/태스크 사용량이 채워지고,
    실패하면 healthy=False 와 error(예외 메시지)가 설정된다. as_view() 로 dict 화하여
    대시보드 응답이나 DB 기록 입력으로 재사용한다.
    """

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
        """이 레코드를 dataclass 필드 그대로의 dict 로 변환한다(JSON 응답/기록용)."""
        return asdict(self)


# 메트릭 기록 테이블 DDL. {table} 자리에 monitor_table 을 채워 사용한다.
# IF NOT EXISTS 이므로 최초 기록 시 1회 실행해도 안전하다.
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

# 메트릭 한 건을 추가하는 INSERT 문. 값 순서는 _write_pg() 의 rows 튜플과 1:1 대응한다.
_INSERT = """
INSERT INTO {table}
    (executor_url, healthy, cpu_percent, memory_percent,
     memory_used_mb, memory_total_mb, disk_percent, error)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


class HealthMonitor:
    """executor 헬스 폴링과 메트릭 DB 기록을 관리하는 모니터.

    생성 시 settings.executors 의 각 URL 에 대해 빈 ExecutorHealth 레코드를 미리 만들어 둔다.
    start() 가 백그라운드 asyncio 태스크(폴링 루프, 선택적으로 기록 루프)를 띄우고,
    stop() 이 이들을 취소·정리한다.
    """

    def __init__(self, settings):
        self.settings = settings
        # executor URL → 최신 헬스 레코드. 폴링 결과를 이 dict 의 값에 in-place 로 덮어쓴다.
        self.executors: dict[str, ExecutorHealth] = {
            url: ExecutorHealth(executor_url=url) for url in settings.executors
        }
        # start() 가 생성한 백그라운드 태스크 핸들(stop() 에서 취소용).
        self._tasks: list[asyncio.Task] = []
        # 메트릭 테이블 DDL 1회 실행 여부 플래그.
        self._ddl_ready = False

    # ───────── 생명주기 ─────────

    async def start(self) -> None:
        """모니터 백그라운드 루프를 시작한다.

        monitor.enabled 가 꺼져 있거나 등록된 executor 가 없으면 아무 것도 하지 않는다.
        헬스 폴링 루프는 항상 띄우고, monitor.db_dsn 이 설정된 경우에만 DB 기록 루프를 추가로 띄운다.
        """
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
        """백그라운드 루프를 모두 취소하고 정상 종료를 기다린다(서버 종료 시 호출)."""
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                # 우리가 의도적으로 취소했으므로 정상 종료로 간주하고 무시한다.
                pass
        self._tasks.clear()

    def snapshot(self) -> list[dict]:
        """현재 메모리에 보관된 모든 executor 의 최신 헬스 상태를 dict 리스트로 반환한다."""
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
        """무한 루프: 모든 executor 를 동시(gather) 폴링한 뒤 설정된 간격만큼 대기한다.

        하나의 AsyncClient 를 루프 전체에서 재사용해 커넥션 풀 효율을 높인다.
        루프는 start()/stop() 으로 띄워지고 취소되므로 여기서 별도 종료 조건은 두지 않는다.
        """
        async with httpx.AsyncClient(timeout=5.0) as client:
            while True:
                await asyncio.gather(
                    *(self._poll_one(client, url) for url in self.executors)
                )
                await asyncio.sleep(self.settings.monitor_health_interval_s)

    async def _poll_one(self, client: httpx.AsyncClient, url: str) -> None:
        """단일 executor 를 1회 폴링해 메모리 레코드(rec)를 갱신한다.

        /health 로 생존을 확인한 뒤 /metrics 로 자원/태스크 사용량을 받아 rec 에 채운다.
        둘 중 하나라도 실패하면 healthy=False 로 표시하고 error 에 사유를 기록한다.
        이 메서드는 예외를 외부로 던지지 않으므로(gather 가 끊기지 않음) 한 executor 의 장애가
        다른 executor 폴링에 영향을 주지 않는다.
        """
        rec = self.executors[url]
        # 시도 시각을 먼저 기록해 둔다(성공/실패와 무관하게 "마지막으로 본 시각"을 남김).
        rec.last_checked = datetime.now(timezone.utc).isoformat()
        try:
            health = await client.get(f"{url}/health")
            health.raise_for_status()
            metrics = await client.get(f"{url}/metrics")
            metrics.raise_for_status()
            md = metrics.json()
            # /metrics 응답은 memory/disk/tasks 를 중첩 객체로 담는다(없을 수 있어 기본 {}).
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
        """무한 루프: 기록 간격만큼 잔 뒤 현재 스냅샷을 PostgreSQL 에 기록한다.

        먼저 sleep 하고 기록하므로 기동 직후가 아니라 한 주기 뒤부터 기록이 시작된다.
        동기 DB 쓰기는 to_thread 로 위임하고, 실패해도 루프가 죽지 않도록 예외를 잡아 로그만 남긴다.
        """
        while True:
            await asyncio.sleep(self.settings.monitor_record_interval_s)
            try:
                await asyncio.to_thread(self._write_pg, self.snapshot())
            except Exception:
                logger.exception("메트릭 PostgreSQL 기록 실패")

    def _write_pg(self, snapshot: list[dict]) -> None:
        """스냅샷의 각 executor 행을 메트릭 테이블에 executemany 로 일괄 INSERT 한다(워커 스레드).

        snapshot dict 의 일부 필드만 골라(테이블 컬럼에 맞춰) 튜플로 변환한 뒤 한 번에 기록한다.
        첫 호출 시에만 DDL 을 실행해 테이블 존재를 보장한다(_ddl_ready).
        """
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
