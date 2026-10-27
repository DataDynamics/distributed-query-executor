"""메타 저장소의 오래된 행을 주기적으로 지우는 보관 정책이다.

job_history·task_history·executor_health_metrics 는 상태 전이나 측정마다 한 행씩 쌓이기만 하고, 앱에
지우는 코드가 없으면 끝없이 커진다. 테이블이 커지면 대시보드 이력 조회(``DISTINCT ON`` 이 전체 이력을
훑는다)가 갈수록 느려지고 디스크와 VACUUM 부담도 커진다. 그래서 coordinator 가 설정한 보관 일수보다
오래된 행을 주기적으로 지운다. 공유 job 저장소(``jobs``)도 종료된 job 이 계속 남으므로 원하면 같은
방식으로 정리한다. 다만 지운 job 은 조회·멱등 재생이 안 되므로 이쪽은 기본으로 끈다.

삭제는 배치 단위로 한다. 한 번에 수십만 행을 지우면 긴 트랜잭션이 테이블을 오래 잡고 WAL 이 몰리므로,
``_BATCH`` 행씩 지우고 커밋한 뒤 다음 배치로 넘어간다. 배치마다 meta 전용 스레드 풀에 따로 넘기므로
쌓인 행이 많은 첫 정리 때도 스레드 하나를 오래 붙잡지 않는다. 기준 시각은 DB 의 KST 벽시계로 잡는다
(시각 컬럼이 모두 KST naive TIMESTAMP 이기 때문이다).

멀티 coordinator 에서는 모든 인스턴스가 같은 정리를 돌린다. 같은 행을 동시에 지우려 하면 뒤쪽이
행 잠금을 기다렸다가 이미 지워진 것을 보고 0건으로 끝나므로, 별도 조정 없이도 결과가 맞는다.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional

from core.metaio import meta_connect, run_meta_io

from .models import JobStatus

logger = logging.getLogger(__name__)

# 한 배치에서 지울 최대 행 수다. 인덱스로 찾은 행을 짧은 트랜잭션으로 지울 만큼으로 둔다.
_BATCH = 5000
# 기동 직후에는 다른 초기화와 겹치지 않도록 이만큼 기다린 뒤 첫 정리를 한다(초).
_INITIAL_DELAY_S = 60.0

_TERMINAL_STATUSES = tuple(
    s.value for s in (JobStatus.DONE, JobStatus.PARTIAL, JobStatus.FAILED, JobStatus.CANCELLED)
)


@dataclass(frozen=True)
class RetentionTarget:
    """정리 대상 테이블 하나다.

    key_col 은 배치로 고를 행을 가리킬 식별 컬럼이고, time_col 은 보관 기간을 잴 시각 컬럼이다.
    where 는 시각 조건에 덧붙일 추가 조건(종료된 job 만 등)이며 바인드 값은 where_params 로 넘긴다.
    """

    name: str
    dsn: str
    table: str
    key_col: str
    time_col: str
    days: int
    where: str = ""
    where_params: tuple = ()

    def delete_sql(self) -> str:
        # 고른 키를 IN (서브쿼리) 로 넘기면 플래너가 바깥 DELETE 를 해시 세미조인으로 풀어 배치마다 테이블
        # 전체를 훑는다(100만 행에서 배치당 약 170ms, 테이블에 비례해 커진다). ARRAY(...) 로 먼저 값 배열을
        # 만들면 바깥은 = ANY(배열) 이 되어 PK 인덱스로 그 행들만 찾아간다.
        extra = f" AND {self.where}" if self.where else ""
        return (
            f"DELETE FROM {self.table} WHERE {self.key_col} = ANY(ARRAY("
            f"SELECT {self.key_col} FROM {self.table} "
            f"WHERE {self.time_col} < (now() AT TIME ZONE 'Asia/Seoul') - make_interval(days => %s)"
            f"{extra} LIMIT %s))"
        )

    def delete_params(self, batch: int) -> tuple:
        return (self.days, *self.where_params, batch)


def build_targets(settings) -> List[RetentionTarget]:
    """설정에서 켜진 정리 대상만 골라 돌려준다. 보관 일수가 0 이거나 DSN 이 없으면 뺀다."""
    targets: List[RetentionTarget] = []
    history_dsn = getattr(settings, "history_db_dsn", "") or ""
    history_days = int(getattr(settings, "history_retention_days", 0) or 0)
    if history_dsn and history_days > 0:
        for name, table in (
            ("job_history", settings.history_table),
            ("task_history", settings.task_history_table),
        ):
            targets.append(RetentionTarget(name, history_dsn, table, "id", "recorded_at", history_days))
    monitor_dsn = getattr(settings, "monitor_db_dsn", "") or ""
    monitor_days = int(getattr(settings, "monitor_retention_days", 0) or 0)
    if monitor_dsn and monitor_days > 0:
        targets.append(RetentionTarget(
            "executor_health_metrics", monitor_dsn, settings.monitor_table, "id", "recorded_at", monitor_days,
        ))
    store_days = int(getattr(settings, "store_retention_days", 0) or 0)
    if getattr(settings, "store_backend", "memory") == "postgres" and history_dsn and store_days > 0:
        # 종료된 job 만 지운다. 실행 중이거나 대기 중인 job 은 아무리 오래돼도 남긴다.
        placeholders = ", ".join(["%s"] * len(_TERMINAL_STATUSES))
        targets.append(RetentionTarget(
            "jobs", history_dsn, settings.store_table, "job_id", "updated_at", store_days,
            where=f"status IN ({placeholders})", where_params=_TERMINAL_STATUSES,
        ))
    return targets


def delete_batch(target: RetentionTarget, batch: int = _BATCH) -> int:
    """보관 기간을 넘긴 행을 최대 batch 개 지우고 지운 행 수를 돌려준다(블로킹, 한 트랜잭션)."""
    with meta_connect(target.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(target.delete_sql(), target.delete_params(batch))
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


async def purge(target: RetentionTarget, batch: int = _BATCH) -> int:
    """한 테이블의 보관 기간을 넘긴 행을 배치로 끝까지 지우고 총 삭제 수를 돌려준다."""
    total = 0
    while True:
        deleted = await run_meta_io(delete_batch, target, batch)
        total += deleted
        if deleted < batch:
            return total


class RetentionCleaner:
    """설정한 주기마다 모든 정리 대상을 비우는 백그라운드 루프다."""

    def __init__(self, targets: List[RetentionTarget], interval_s: float,
                 initial_delay_s: float = _INITIAL_DELAY_S):
        self.targets = targets
        self.interval_s = interval_s
        self.initial_delay_s = initial_delay_s
        self._task: Optional[asyncio.Task] = None

    async def run_once(self) -> dict:
        """대상마다 한 번씩 정리하고 테이블별 삭제 수를 돌려준다. 한 테이블의 실패는 다른 테이블을 막지 않는다."""
        result = {}
        for target in self.targets:
            try:
                deleted = await purge(target)
            except Exception:
                logger.exception("보관 기간 정리 실패 table=%s", target.table)
                continue
            result[target.name] = deleted
            if deleted:
                logger.info("보관 기간 정리 — %s 에서 %d일 지난 행 %d건 삭제", target.table, target.days, deleted)
        return result

    async def _loop(self) -> None:
        await asyncio.sleep(self.initial_delay_s)
        while True:
            await self.run_once()
            await asyncio.sleep(self.interval_s)

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None


def build_retention_cleaner(settings) -> Optional[RetentionCleaner]:
    """켜진 대상이 있고 주기가 양수일 때만 정리기를 만든다. 아니면 None 이다."""
    interval = float(getattr(settings, "retention_interval_s", 0) or 0)
    targets = build_targets(settings)
    if interval <= 0 or not targets:
        return None
    return RetentionCleaner(targets, interval)
