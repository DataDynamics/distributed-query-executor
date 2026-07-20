"""coordinator 가 executor self-report(공유 DB)를 읽는 저장소.

이 모듈은 executor self-report 모드에서 동작한다. 각 executor 가 주기적으로 자신의 상태
(CPU/메모리/디스크/태스크 수 등)를 공유 PostgreSQL 테이블(executor_status)에 직접
upsert 하고, coordinator 들은 그 테이블을 "읽기 전용"으로 조회한다. 따라서 여러 coordinator 가
떠 있어도 executor 상태의 단일 출처(single source of truth)가 DB 하나로 유지된다.

이 방식에서는 coordinator 가 executor 를 직접 폴링하지 않으므로(monitor.py 와 대비) 생존 여부를
직접 알 수 없다. 대신 행의 updated_at 이 얼마나 최근인지로 생존을 추정한다. 즉
'지금(now) - updated_at' 이 stale_seconds 이내이면 살아있는(healthy) 것으로, 그보다 오래
갱신이 끊겼으면 죽은(stale) 것으로 판정한다.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ExecutorStatusRepository:
    """공유 executor_status 테이블을 읽어 executor 상태 목록을 돌려주는 읽기 전용 저장소.

    Args:
        dsn: 공유 상태 테이블이 있는 PostgreSQL 접속 문자열.
        table: 조회할 상태 테이블 이름(기본 "executor_status").
        stale_seconds: updated_at 이 이 초 이내면 healthy 로 간주하는 신선도 임계값.
    """

    def __init__(self, dsn: str, table: str = "executor_status", stale_seconds: float = 30):
        self.dsn = dsn
        self.table = table
        self.stale_seconds = stale_seconds

    def read_all(self) -> list[dict]:
        """모든 executor 의 최신 상태를 조회해 dict 리스트로 반환한다.

        SQL 에서 '(now() AT TIME ZONE 'Asia/Seoul') - updated_at' 을 초(EXTRACT EPOCH)로 계산해 age_s 로 함께 가져온 뒤,
        age_s <= stale_seconds 인지로 각 행의 healthy 를 판정한다. age_s 가 NULL 이면(갱신 기록
        없음) healthy=False 로 본다. 조회 자체가 실패하면 예외를 잡아 로그를 남기고 빈 리스트를
        반환해 호출 측(대시보드)이 깨지지 않게 한다.
        """
        import psycopg  # 지연 임포트

        sql = (
            "SELECT executor_id, cpu_percent, memory_percent, memory_used_mb, "
            "memory_total_mb, disk_percent, disk_used_gb, disk_total_gb, "
            "active_tasks, max_concurrent_tasks, "
            "EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'Asia/Seoul') - updated_at)) AS age_s, updated_at, "
            "executor_url "
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
            # r[10] = age_s(마지막 갱신 이후 경과 초). NULL 이면 신선도 판정 불가 → 비정상 취급.
            age = float(r[10]) if r[10] is not None else None
            healthy = age is not None and age <= self.stale_seconds
            result.append({
                "executor_id": r[0],
                # executor_url(advertise_url). HA 에서 coordinator 가 URL 키로 부하 뷰를
                # 구성한다(미보고 시 None → URL 매칭 불가로 폴백). 키 일관성을 위해
                # executor_url 이 없으면 executor_id 를 대체 키로 노출한다.
                "executor_url": r[12] or r[0],
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
