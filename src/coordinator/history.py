"""Job 실행 이력을 PostgreSQL 에 기록하는 저장소(append-only audit log).

이 모듈은 coordinator 가 처리하는 각 Job 의 생애주기 상태 전이(생성/시작/완료/실패 등)를
PostgreSQL 테이블에 "한 상태당 한 행"으로 누적 기록하기 위한 저장소를 제공한다.
기존 행을 갱신(UPDATE)하지 않고 매 전이마다 새 행을 INSERT 하므로, 한 job_id 에는
시간순으로 여러 행이 쌓인다(감사/추적용 append-only 모델).

설계 의도:
  - 기록 실패가 본래 Job 처리 흐름을 막아서는 안 되므로, 기록은 best-effort 로 수행하고
    예외는 잡아서 로그만 남긴다(record()).
  - 동기 psycopg 호출이 이벤트 루프를 막지 않도록 asyncio.to_thread 로 별도 스레드에서 실행한다.
  - DSN(history.db_dsn)이 설정되지 않으면 기록 기능 전체를 비활성화하고 경고만 남긴다.

조회(read)는 job_id 별 "가장 최근 상태" 한 행씩만 추려서 페이징해 보여준다(아래 read() 참고).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# 이력 테이블 DDL. {table} 자리에 실제 테이블명을 format 으로 채워 사용한다.
# 스키마(job_history 테이블)는 앱이 생성하지 않는다. 운영 전에
# config/postgresql.sql 로 미리 만들어 두어야 한다.

# 한 건의 상태 스냅샷을 추가하는 INSERT 문. 컬럼 순서는 _write() 의 row 튜플과 1:1 대응한다.
_INSERT = """
INSERT INTO {table}
    (job_id, username, status, partition_column, target_table, parallelism,
     total_tasks, completed_tasks, total_rows_written, error,
     created_at, started_at, finished_at, original_sql)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


class JobHistoryRepository:
    """Job 상태 이력의 기록(record)과 조회(read)를 담당하는 저장소.

    설정에서 DSN/테이블명을 읽어 보관한다. DSN 이 비어 있으면 enabled=False 가 되어
    기록은 생략되고 조회는 빈 결과를 돌려준다. 테이블 스키마는 앱이 만들지 않으며,
    운영 전에 config/postgresql.sql 로 미리 생성돼 있어야 한다.
    """

    def __init__(self, settings):
        # DSN/테이블명은 settings 에 없을 수 있으므로 getattr 로 안전하게 읽고 기본값을 둔다.
        self.dsn: str = getattr(settings, "history_db_dsn", "") or ""
        self.table: str = getattr(settings, "history_table", "job_history")
        # DSN 이 있어야만 기록/조회 기능을 활성화한다.
        self.enabled: bool = bool(self.dsn)

    async def record(self, job) -> None:
        """현재 Job 상태를 이력 테이블에 한 행 기록한다(append-only).

        호출 시점의 job 스냅샷을 새 행으로 INSERT 한다. 동기 psycopg 작업은
        asyncio.to_thread 로 워커 스레드에 위임해 이벤트 루프 블로킹을 막는다.
        기록 실패가 Job 처리 흐름을 깨지 않도록 모든 예외를 잡아 로그만 남긴다(best-effort).

        Args:
            job: 기록할 Job 객체. job_id/status/started_at 등 속성을 _write() 에서 읽는다.
        """
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

    def read(
        self,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
        username: str | None = None,
        job_id: str | None = None,
    ) -> dict:
        """과거 실행 이력을 job_id 별 "최신 상태" 한 행씩 필터링/페이징해 조회한다.

        이력 테이블에는 한 job_id 에 대해 상태 전이마다 여러 행이 쌓여 있다. 대시보드의
        '실행 이력' 탭은 각 Job 의 최종(가장 최근) 상태만 보여주면 되므로, PostgreSQL 의
        DISTINCT ON 을 사용해 job_id 그룹별 최신 행 하나만 골라낸다.

          - 내부 서브쿼리: ORDER BY job_id, recorded_at DESC 로 정렬한 뒤
            DISTINCT ON (job_id) 가 각 job_id 의 첫 행(= recorded_at 이 가장 큰, 즉 최신 행)만 남긴다.
          - 외부 쿼리: 그렇게 추린 "job 별 최신 행" 집합에 검색 필터(WHERE)를 적용하고
            recorded_at DESC 로 다시 정렬해 LIMIT/OFFSET 으로 페이지를 자른다.

        필터는 "job 의 최신 상태" 기준으로 적용해야 하므로(중간 전이 행이 아니라)
        DISTINCT ON 서브쿼리 **바깥**에 건다. total 도 같은 필터가 적용된 파생
        테이블에서 세어 페이저와 어긋나지 않게 한다.

        Args:
            limit: 한 페이지에 반환할 최대 Job 수.
            offset: 건너뛸 Job 수(페이지 오프셋).
            status: 최종 상태 일치 필터(대소문자 무시, 예: FAILED).
            username: 요청 사용자 일치 필터.
            job_id: 작업 ID 전방일치(prefix) 필터.

        Returns:
            dict: {enabled, rows, total, limit, offset}. enabled=False 면 빈 결과를 돌려준다.
        """
        if not self.enabled:
            return {"enabled": False, "rows": [], "total": 0, "limit": limit, "offset": offset}
        import psycopg  # 지연 임포트(이력 미사용 환경에서 psycopg 의존을 강제하지 않기 위함)

        # 검색 필터(WHERE) 조립. 값은 전부 바인드 파라미터로 넘겨 SQL 주입을 차단한다.
        conds: list[str] = []
        params: list[object] = []
        if status:
            conds.append("status = %s")
            params.append(status.upper())
        if username:
            conds.append("username = %s")
            params.append(username)
        if job_id:
            # 전방일치. LIKE 와일드카드(%/_)가 값에 있어도 이스케이프하지 않는 단순 구현
            # (job_id 는 서버가 만든 uuid 계열이라 실사용에서 문제되지 않음).
            conds.append("job_id LIKE %s")
            params.append(f"{job_id}%")
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        # job_id 별 최신 한 행만 남기는 파생 테이블(위 docstring 참고). count/페이지 쿼리가 공유한다.
        latest = (
            "SELECT DISTINCT ON (job_id) recorded_at, job_id, username, status, "
            "  partition_column, target_table, completed_tasks, total_tasks, "
            "  total_rows_written, error, started_at, finished_at, original_sql "
            f"FROM {self.table} ORDER BY job_id, recorded_at DESC"
        )

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                # 테이블은 postgresql.sql 로 사전 생성돼 있어야 한다(앱은 DDL 하지 않음).
                # 페이징용 전체 건수: 필터가 적용된 "job 별 최신 행" 수를 센다.
                cur.execute(f"SELECT count(*) FROM ({latest}) t{where}", tuple(params))
                total = cur.fetchone()[0]
                cur.execute(
                    f"SELECT * FROM ({latest}) t{where} "
                    "ORDER BY recorded_at DESC LIMIT %s OFFSET %s",
                    tuple(params) + (limit, offset),
                )
                rows = cur.fetchall()
            conn.commit()
        # DB 행(튜플)을 JSON 직렬화 가능한 dict 로 변환한다.
        # TIMESTAMPTZ 컬럼은 None 이 아닐 때만 ISO-8601 문자열로 바꾼다.
        out = [
            {
                "recorded_at": r[0].isoformat() if r[0] is not None else None,
                "job_id": r[1], "username": r[2], "status": r[3],
                "partition_column": r[4], "target_table": r[5],
                "completed_tasks": r[6], "total_tasks": r[7],
                "total_rows_written": r[8], "error": r[9],
                "started_at": r[10].isoformat() if r[10] is not None else None,
                "finished_at": r[11].isoformat() if r[11] is not None else None,
                "original_sql": r[12],
            }
            for r in rows
        ]
        return {"enabled": True, "rows": out, "total": total, "limit": limit, "offset": offset}

    def _write(self, job) -> None:
        """job 스냅샷 한 건을 동기 psycopg 로 INSERT 한다(워커 스레드에서 호출됨).

        record() 가 asyncio.to_thread 로 이 메서드를 실행한다. 테이블은 postgresql.sql 로
        사전 생성돼 있어야 한다(앱은 DDL 하지 않음).
        row 튜플의 값 순서는 _INSERT 문의 컬럼 순서와 정확히 일치해야 한다.
        """
        import psycopg  # 지연 임포트

        row = (
            job.job_id,
            job.username,
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
        # 테이블은 postgresql.sql 로 사전 생성돼 있어야 한다(앱은 DDL 하지 않음).
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(_INSERT.format(table=self.table), row)
            conn.commit()
        logger.info("job %s 이력 기록(status=%s) -> %s", job.job_id, job.status.value, self.table)
