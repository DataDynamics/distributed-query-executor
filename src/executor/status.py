"""executor 가 자기 상태(CPU·메모리·디스크·heartbeat)를 공유 DB 에 스스로 보고한다.

멀티 coordinator 환경에서 각 coordinator가 executor를 중복 폴링/기록하지 않도록,
상태의 '기록 주인'을 executor 로 둔다. coordinator 는 이 테이블을 읽기만 한다.
"""

from __future__ import annotations

import asyncio
import logging

from core.metrics import collect_system_metrics
from .history import _executor_id

logger = logging.getLogger(__name__)

# 스키마(executor_status 테이블)는 앱이 생성하지 않는다. 운영 전에
# config/postgresql.sql 로 미리 만들어 두어야 한다(executor_id 가 PK).

# self-report UPSERT: 같은 executor_id 행이 있으면(ON CONFLICT) 모든 메트릭과
# updated_at 을 새 값으로 덮어쓴다. 즉 "마지막으로 본 상태" 한 줄만 항상 유지된다.
_UPSERT = """
INSERT INTO {table}
    (executor_id, executor_url, cpu_percent, memory_percent, memory_used_mb, memory_total_mb,
     disk_percent, disk_used_gb, disk_total_gb, active_tasks, max_concurrent_tasks, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (now() AT TIME ZONE 'Asia/Seoul'))
ON CONFLICT (executor_id) DO UPDATE SET
    executor_url=EXCLUDED.executor_url,
    cpu_percent=EXCLUDED.cpu_percent, memory_percent=EXCLUDED.memory_percent,
    memory_used_mb=EXCLUDED.memory_used_mb, memory_total_mb=EXCLUDED.memory_total_mb,
    disk_percent=EXCLUDED.disk_percent, disk_used_gb=EXCLUDED.disk_used_gb,
    disk_total_gb=EXCLUDED.disk_total_gb, active_tasks=EXCLUDED.active_tasks,
    max_concurrent_tasks=EXCLUDED.max_concurrent_tasks, updated_at=(now() AT TIME ZONE 'Asia/Seoul')
"""


class ExecutorStatusReporter:
    """주기적으로 자기 상태(메트릭+동시 처리 현황)를 공유 DB 에 self-report 한다.

    ``start`` 가 백그라운드 asyncio 루프를 띄워 ``interval`` 초마다 ``_report_once`` 를
    호출하고, executor_status 테이블의 자기 행을 UPSERT 한다. coordinator 는 이 테이블을
    읽기만 하므로, 여러 coordinator 가 같은 executor 를 중복 폴링/기록하는 문제를 피한다.
    DSN(history_db_dsn)이 없으면 비활성으로 동작한다(경고만 남기고 아무 것도 안 함).

    인자:
        settings: history_db_dsn, executor_status_table, executor_status_interval_s,
            monitor_disk_path 등을 읽어오는 설정 객체.
        tasks_provider: 호출 시 (active, queued, max) 튜플을 돌려주는 콜러블(선택).
            동시 처리 현황을 메트릭과 함께 기록하는 데 사용한다.
    """

    def __init__(self, settings, tasks_provider=None):
        self.dsn: str = getattr(settings, "history_db_dsn", "") or ""
        self.table: str = getattr(settings, "executor_status_table", "executor_status")
        self.interval: float = float(getattr(settings, "executor_status_interval_s", 10))
        self.disk_path: str = getattr(settings, "monitor_disk_path", "/")
        self.executor_id: str = _executor_id()
        # HA 에서 coordinator 가 URL 키로 부하 뷰를 구성하도록 자기 base URL 도 함께 보고한다.
        self.executor_url: str = getattr(settings, "executor_advertise_url", "") or ""
        # () -> (active, queued, max) 동시 처리 현황 제공자
        self.tasks_provider = tasks_provider
        self.enabled: bool = bool(self.dsn)
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """self-report 백그라운드 루프를 시작한다(비활성이면 경고 후 무동작)."""
        if not self.enabled:
            logger.warning("history.db_dsn 미설정 → executor self-report 비활성")
            return
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "executor self-report 시작(%ss) executor_id=%s -> %s",
            self.interval, self.executor_id, self.table,
        )

    async def stop(self) -> None:
        """백그라운드 루프를 취소하고 정상 종료를 기다린다(앱 종료 시 호출).

        cancel 후 발생하는 CancelledError 는 정상적인 종료 신호이므로 삼킨다.
        """
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        """interval 초 간격으로 self-report 를 반복하는 루프다.

        한 번의 보고 실패가 루프를 멈추지 않도록 예외를 잡아 로깅만 하고 계속 진행한다.
        블로킹 DB 작업(``_report_once``)은 ``to_thread`` 로 스레드에서 실행해 이벤트 루프를
        막지 않는다.
        """
        while True:
            try:
                await asyncio.to_thread(self._report_once)
            except Exception:
                logger.exception("executor self-report 실패")
            await asyncio.sleep(self.interval)

    def _report_once(self) -> None:
        """현재 시스템 메트릭과 동시 처리 현황을 한 번 수집해 UPSERT 한다(동기).

        테이블은 postgresql.sql 로 사전 생성돼 있어야 한다(앱은 DDL 하지 않음).
        """
        import psycopg  # 드라이버가 없을 수 있어 지연 임포트한다

        m = collect_system_metrics(self.disk_path)
        mem, disk = m["memory"], m["disk"]
        active, mx = 0, 0
        if self.tasks_provider:
            counts = self.tasks_provider()
            active, mx = counts[0], counts[2]
        row = (
            self.executor_id, self.executor_url or None,
            m["cpu_percent"], mem["percent"], mem["used_mb"],
            mem["total_mb"], disk["percent"], disk["used_gb"], disk["total_gb"],
            active, mx,
        )
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(_UPSERT.format(table=self.table), row)
            conn.commit()
