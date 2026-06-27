"""Job 저장소 구현 모듈.

Job(작업) 객체를 보관·조회·갱신하고 취소 플래그를 관리하는 저장소를 제공한다.
두 가지 백엔드가 동일한 인터페이스(덕 타이핑)를 구현하므로 호출부는 구현을 모른 채
교체할 수 있다.

- InMemoryJobStore : 단일 coordinator용 기본 구현. 프로세스 메모리(dict)에만 보관하며
                     프로세스가 죽으면 사라진다. Job 객체 참조를 그대로 들고 있어
                     별도 영속 단계가 필요 없다(가장 빠름).
- SqlJobStore      : 여러 coordinator가 공유하는 PostgreSQL 기반 구현. 로드밸런서 뒤에
                     여러 coordinator가 떠 있어도 어느 인스턴스로 요청이 가든
                     조회/취소가 가능하도록 Job 스냅샷(JSONB)을 영속한다.

공통 인터페이스:
  add(job), get(job_id), list(), save(job),
  request_cancel(job_id), is_cancel_requested(job_id)

어떤 백엔드를 쓸지는 설정에 따라 :func:`build_job_store` 가 결정한다.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from .models import Job

logger = logging.getLogger(__name__)


class InMemoryJobStore:
    """프로세스 메모리에 Job을 보관하는 단일 coordinator용 저장소.

    ``job_id → Job`` dict로 객체 참조를 그대로 들고 있으므로, 호출부가 Job을 수정하면
    저장소 안의 객체도 곧바로 바뀐다. 그래서 ``save`` 는 사실상 no-op에 가깝다.
    프로세스 재시작 시 모든 상태가 사라진다(영속성 없음).
    """

    def __init__(self) -> None:
        # job_id를 키로 Job 객체 참조를 보관한다.
        self._jobs: dict[str, Job] = {}

    def add(self, job: Job) -> None:
        """새 Job을 등록한다(같은 id면 덮어쓴다)."""
        self._jobs[job.job_id] = job

    def save(self, job: Job) -> None:
        """변경된 Job을 반영한다.

        객체 참조를 그대로 들고 있어 사실상 갱신이 이미 끝난 상태지만, SqlJobStore와
        동일한 인터페이스를 맞추기 위해 다시 매핑해 둔다(별도 영속 불필요).
        """
        # 객체를 그대로 들고 있으므로 별도 영속 불필요
        self._jobs[job.job_id] = job

    def get(self, job_id: str) -> Optional[Job]:
        """job_id로 Job을 조회한다(없으면 None)."""
        return self._jobs.get(job_id)

    def list(self) -> list[Job]:
        """보관 중인 모든 Job을 리스트로 반환한다."""
        return list(self._jobs.values())

    def request_cancel(self, job_id: str) -> bool:
        """해당 Job에 취소를 요청한다. 성공 시 True, 없는 id면 False.

        실행 루프가 ``is_cancel_requested`` 를 폴링해 실제 중단을 수행하므로, 여기서는
        플래그만 세운다.
        """
        job = self._jobs.get(job_id)
        if job is None:
            return False
        job.cancel_requested = True
        return True

    def is_cancel_requested(self, job_id: str) -> bool:
        """해당 Job에 취소가 요청되었는지 여부를 반환한다(없는 id면 False)."""
        job = self._jobs.get(job_id)
        return bool(job and job.cancel_requested)


# 스키마(jobs 테이블)는 앱이 생성하지 않는다. 운영 전에 packaging/config/postgresql.sql
# 을 적용해 테이블/인덱스를 미리 만들어 두어야 한다(아래 컬럼 구성 참고).
#   job_id, coordinator_id, status, cancel_requested, updated_at, data(JSONB)


class SqlJobStore:
    """여러 coordinator가 공유하는 PostgreSQL 기반 Job 저장소.

    Job을 ``to_record``/``from_record`` 로 JSONB 직렬화해 한 테이블에 영속한다.
    매 작업마다 연결을 새로 열고 닫는 단순한 방식이며, ``psycopg`` 는 무거운
    의존성이라 선택 설치/지연 임포트로 둔다(메모리 백엔드만 쓰는 배포에서는 불필요).

    인자:
        dsn            : PostgreSQL 접속 문자열.
        table          : 사용할 테이블명(기본 "jobs").
        coordinator_id : 이 레코드를 마지막으로 기록한 coordinator 식별자(추적용).
    """

    def __init__(self, dsn: str, table: str = "jobs", coordinator_id: str = "-"):
        self.dsn = dsn
        self.table = table
        self.coordinator_id = coordinator_id

    def _conn(self):
        """새 DB 연결을 연다.

        테이블 생성(DDL)은 앱이 하지 않는다 — 사전에 postgresql.sql 로 스키마를 만들어
        두어야 한다. psycopg는 여기서 지연 임포트한다(모듈 로드 비용·선택 의존성 처리).
        """
        import psycopg  # 지연 임포트

        return psycopg.connect(self.dsn)

    def add(self, job: Job) -> None:
        """새 Job을 저장한다(내부적으로 ``save`` 와 동일한 UPSERT)."""
        self.save(job)

    def save(self, job: Job) -> None:
        """Job 스냅샷을 UPSERT한다(같은 job_id면 갱신).

        ``ON CONFLICT (job_id) DO UPDATE`` 로 신규 삽입과 갱신을 한 문장으로 처리한다.
        status·cancel_requested는 별도 컬럼에도 함께 기록해 빠른 조회/취소에 대비한다.
        """
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
        """job_id로 Job 스냅샷을 읽어 객체로 복원한다(없으면 None).

        data 컬럼은 JSONB이므로 드라이버가 dict로 줄 수도, 문자열로 줄 수도 있어
        둘 다 처리한다. 별도 컬럼의 cancel_requested로 취소 플래그를 최신값으로
        덮어써, data 스냅샷이 갱신되기 전이라도 취소가 즉시 반영되게 한다.
        """
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
        """저장된 모든 Job을 복원해 리스트로 반환한다.

        get과 마찬가지로 JSONB가 dict/문자열 어느 쪽으로 와도 처리한다.
        """
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
        """취소 플래그 컬럼만 직접 UPDATE한다. 한 행이라도 갱신되면 True.

        무거운 data 스냅샷을 다시 쓰지 않고 cancel_requested 컬럼만 갱신하므로,
        다른 coordinator에서 실행 중인 Job도 즉시 취소 신호를 받을 수 있다.
        rowcount로 대상 존재 여부를 판단한다.
        """
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
        """취소 플래그 컬럼만 가볍게 조회한다(실행 루프가 폴링하는 용도).

        전체 Job을 복원하지 않고 boolean 한 컬럼만 읽어 폴링 비용을 줄인다.
        """
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
    """설정에 따라 알맞은 Job 저장소를 생성하는 팩토리(기본은 메모리).

    ``store_backend`` 가 "postgres"이고 DSN이 설정돼 있으면 SqlJobStore를,
    그 외에는 InMemoryJobStore를 만든다. postgres로 지정됐지만 DSN이 비어 있으면
    경고 로그를 남기고 안전하게 메모리 백엔드로 폴백한다.

    설정 객체에서 ``getattr`` 로 안전하게 값을 읽어, 해당 속성이 없는 설정과도
    호환되게 한다.
    """
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
