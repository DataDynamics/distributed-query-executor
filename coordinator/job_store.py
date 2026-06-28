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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .models import Job, JobStatus, TaskStatus

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


class FileJobStore(InMemoryJobStore):
    """단일 coordinator 용 **파일 영속** Job 저장소(크래시 복구).

    InMemoryJobStore 처럼 메모리 dict 로 동작하되, 변경(add/save/취소)마다 전체 스냅샷을
    JSON 파일에 **원자적으로**(tmp→replace) 기록하고, 기동 시 파일에서 복원한다.
    PostgreSQL 없이도 프로세스 재시작 후 job 상태/이력을 잃지 않게 한다.

    스냅샷은 매번 전체를 다시 쓴다(단일 노드 규모 기준 단순·안전). 대규모/멀티 coordinator
    환경이면 SqlJobStore(PostgreSQL)를 권장한다.
    """

    def __init__(self, path) -> None:
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self) -> None:
        """기동 시 스냅샷 파일에서 Job 들을 복원한다(없거나 깨졌으면 빈 상태로 시작)."""
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("job 스냅샷 로드 실패: %s", self.path)
            return
        for rec in data.get("jobs", []):
            try:
                job = Job.from_record(rec)
                self._jobs[job.job_id] = job
            except Exception:
                logger.exception("job 레코드 복원 실패(건너뜀)")
        logger.info("job 스냅샷 복원: %d개 (%s)", len(self._jobs), self.path)

    def _flush(self) -> None:
        """현재 전체 Job 을 스냅샷 파일에 원자적으로 기록한다(저장 실패는 로깅만)."""
        try:
            payload = {"jobs": [j.to_record() for j in self._jobs.values()]}
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            tmp.replace(self.path)  # 같은 파일시스템 내 원자적 교체
        except Exception:
            logger.exception("job 스냅샷 저장 실패: %s", self.path)

    def add(self, job: Job) -> None:
        super().add(job)
        self._flush()

    def save(self, job: Job) -> None:
        super().save(job)
        self._flush()

    def request_cancel(self, job_id: str) -> bool:
        ok = super().request_cancel(job_id)
        if ok:
            self._flush()
        return ok


# 비종료(=재기동 시 중단으로 간주) job/task 상태 집합.
_NON_TERMINAL_JOB = {JobStatus.PENDING, JobStatus.SPLITTING, JobStatus.RUNNING}
_TERMINAL_TASK = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}


def reconcile_interrupted_jobs(store) -> int:
    """기동 시, 비종료 상태로 남은 job 을 '중단됨(FAILED)'으로 정합한다(크래시 복구).

    프로세스 크래시/재시작으로 실행 루프(``run()``)가 사라진 job 은 더 진행될 수 없다.
    상태를 정확히 FAILED 로 바꾸고, 진행 중이던 task 도 FAILED 로 표시한다. 이렇게 하면
    ``POST /jobs/{id}/retry`` 로 실패 파티션만 재실행할 수 있다.

    인메모리 저장소(재시작 시 비어 있음)에서는 대상이 없어 no-op 이다. 반환: 정합한 job 수.
    """
    try:
        jobs = store.list()
    except Exception:
        logger.exception("재기동 정합용 job 조회 실패")
        return 0
    now = datetime.now(timezone.utc).isoformat()
    reconciled = 0
    for job in jobs:
        if job.status not in _NON_TERMINAL_JOB:
            continue
        for t in job.tasks:
            if t.status not in _TERMINAL_TASK:
                t.status = TaskStatus.FAILED
                t.error = t.error or "coordinator 재시작으로 중단됨"
        job.status = JobStatus.FAILED
        job.error = job.error or "coordinator 재시작으로 중단됨"
        job.finished_at = job.finished_at or now
        store.save(job)
        reconciled += 1
    if reconciled:
        logger.warning("재기동 정합: 중단된 job %d개를 FAILED 로 표시", reconciled)
    return reconciled


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

    - ``store_backend=postgres`` + DSN: SqlJobStore(멀티 coordinator 공유).
    - ``store_backend=file``: FileJobStore(단일 노드 파일 영속 → 크래시 복구).
    - 그 외: InMemoryJobStore(휘발성).
    postgres로 지정됐지만 DSN이 비어 있으면 경고 후 메모리로 폴백한다.

    설정 객체에서 ``getattr`` 로 안전하게 값을 읽어, 해당 속성이 없는 설정과도
    호환되게 한다.
    """
    backend = getattr(settings, "store_backend", "memory")
    dsn = getattr(settings, "history_db_dsn", "")
    if backend == "file":
        # 경로 미지정 시 로그 디렉터리 옆에 jobs-state.json 을 둔다.
        path = getattr(settings, "store_path", "") or str(
            Path(getattr(settings, "log_dir", "logs")) / "jobs-state.json"
        )
        logger.info("FileJobStore 사용(path=%s)", path)
        return FileJobStore(path)
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
