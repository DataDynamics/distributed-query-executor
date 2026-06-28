"""도메인 모델(Job/Task)과 API 요청/응답 스키마 정의.

이 모듈은 분산 쿼리 실행 시스템의 핵심 도메인 객체를 정의한다.

- ``Job``  : 사용자가 제출한 하나의 큰 쿼리 작업. 파티션 IN 목록을 N등분하여
             여러 ``Task`` 로 쪼개진 뒤, 여러 executor에 분산 실행된다.
- ``Task`` : Job을 구성하는 개별 sub-query 단위. 특정 executor가 파티션 값의
             부분집합 하나를 읽어 대상 테이블로 적재한다.

상태 머신은 두 개의 enum(``JobStatus``, ``TaskStatus``)으로 표현하며,
Job의 진행률은 종료된(terminal) Task 비율로 계산한다(아래 ``Job.progress_percent`` 참고).

도메인 객체는 ``@dataclass`` 로, 외부에 노출되는 HTTP 요청/응답 스키마는
``pydantic.BaseModel`` 로 분리되어 있다. dataclass 쪽 ``to_record``/``from_record``
메서드는 SqlJobStore(여러 coordinator 공유 저장소)용 JSON 직렬화/역직렬화를 담당한다.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field


def _now_iso() -> str:
    """현재 시각을 UTC 기준 ISO-8601 문자열로 반환한다.

    저장/직렬화 시 타임존 모호성을 없애기 위해 항상 UTC(``timezone.utc``)를 쓴다.
    """
    return datetime.now(timezone.utc).isoformat()


class JobStatus(str, enum.Enum):
    """Job 전체의 생명주기 상태.

    ``str`` 을 함께 상속하므로 enum 멤버가 곧 문자열이며, JSON 직렬화·DB 저장 시
    ``.value`` 가 그대로 쓰인다.

    상태 전이(개략):
        PENDING   → 작업 생성 직후, 아직 분할 전 대기 상태.
        SPLITTING → 쿼리를 파싱하고 파티션 IN 목록을 N등분하여 Task로 쪼개는 중.
        RUNNING   → Task들을 executor에 분산 실행 중.
        DONE      → 모든 Task 성공(터미널).
        PARTIAL   → 일부 Task만 성공(best_effort 정책에서 발생, 터미널).
        FAILED    → 작업 실패(fail_fast 정책 등, 터미널).
        CANCELLED → 사용자 취소 요청으로 중단됨(터미널).
    """

    PENDING = "PENDING"
    SPLITTING = "SPLITTING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskStatus(str, enum.Enum):
    """개별 Task(sub-query)의 생명주기 상태.

    ``JobStatus`` 와 마찬가지로 문자열 enum이다.

    상태 전이(개략):
        QUEUED    → 실행 대기.
        READING   → 소스(Impala 등)에서 결과를 읽는 중.
        WRITING   → 대상(Greenplum 등)에 적재(COPY/INSERT)하는 중.
        DONE      → 성공(터미널).
        FAILED    → 실패(터미널).
        CANCELLED → 취소됨(터미널).

    DONE/FAILED/CANCELLED 세 상태가 '종료(terminal)'로 취급되며,
    Job 진행률 계산(``Job.completed``)의 기준이 된다.
    """

    QUEUED = "QUEUED"
    READING = "READING"
    WRITING = "WRITING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


def _new_id(prefix: str) -> str:
    """``<prefix>_<랜덤12자리>`` 형태의 짧은 식별자를 생성한다.

    uuid4의 hex 앞 12자만 사용해 충돌 가능성은 낮추면서 로그·URL에서 다루기 쉬운
    길이를 유지한다. prefix로 종류(job/task)를 구분한다.
    """
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def new_job_id() -> str:
    """새 Job 식별자(``job_...``)를 발급한다."""
    return _new_id("job")


@dataclass
class Task:
    """Job을 구성하는 개별 실행 단위(sub-query 하나).

    파티션 IN 목록의 부분집합(``partition_values``)을 스캔하는 sub-query 한 건을
    특정 executor에 보내 실행한 결과를 추적한다.

    필드:
        job_id           : 소속 Job의 식별자.
        executor_url     : 이 Task를 실행할(또는 실행한) executor 엔드포인트. 미배정 시 None.
        sub_query        : executor로 전송한 sub-query 전문(재시도·감사 목적으로 원문 보관).
        partition_values : 이 Task가 담당하는 파티션 값들(분할된 IN 목록의 한 버킷).
        task_id          : ``t_...`` 형태 식별자(자동 발급).
        status           : 현재 ``TaskStatus``.
        rows_written     : 대상 테이블에 적재된 행 수(진행/결과 집계용).
        attempt          : 시도 횟수(재시도 시 증가).
        error            : 실패 시 오류 메시지(성공이면 None).
    """

    job_id: str
    executor_url: Optional[str]
    sub_query: str  # executor로 보낸 sub-query 전문(보관)
    partition_values: list[str]
    task_id: str = field(default_factory=lambda: _new_id("t"))
    status: TaskStatus = TaskStatus.QUEUED
    rows_written: int = 0
    attempt: int = 0
    error: Optional[str] = None

    def summary(self) -> dict:
        """상태 조회 응답용 경량 요약 dict를 반환한다(무거운 sub_query 전문은 제외)."""
        return {
            "task_id": self.task_id,
            "executor_url": self.executor_url,
            "status": self.status.value,
            "rows_written": self.rows_written,
            "attempt": self.attempt,
            "partition_values": self.partition_values,
            "error": self.error,
        }

    def detail(self) -> dict:
        """요약(summary)에 sub-query 전문을 더한 상세 dict를 반환한다(단건 조회용)."""
        return {**self.summary(), "sub_query": self.sub_query}

    def to_record(self) -> dict:
        """SqlJobStore(공유 저장소)에 넣기 위한 전체 직렬화 dict를 반환한다.

        ``summary`` 와 달리 sub_query 등 모든 필드를 포함하므로 ``from_record`` 로
        손실 없이 복원할 수 있다. enum은 ``.value`` 문자열로 평탄화한다.
        """
        return {
            "job_id": self.job_id,
            "executor_url": self.executor_url,
            "sub_query": self.sub_query,
            "partition_values": self.partition_values,
            "task_id": self.task_id,
            "status": self.status.value,
            "rows_written": self.rows_written,
            "attempt": self.attempt,
            "error": self.error,
        }

    @classmethod
    def from_record(cls, d: dict) -> "Task":
        """``to_record`` 로 만든 dict(주로 DB JSONB)를 다시 Task 객체로 복원한다.

        선택 필드는 ``dict.get`` 으로 안전하게 읽어 과거 스키마와의 호환성을 둔다.
        문자열로 저장된 상태는 ``TaskStatus(...)`` 로 enum 복원한다.
        """
        return cls(
            job_id=d["job_id"],
            executor_url=d.get("executor_url"),
            sub_query=d["sub_query"],
            partition_values=list(d.get("partition_values") or []),
            task_id=d["task_id"],
            status=TaskStatus(d.get("status", "QUEUED")),
            rows_written=d.get("rows_written", 0),
            attempt=d.get("attempt", 0),
            error=d.get("error"),
        )


@dataclass
class Job:
    """사용자가 제출한 하나의 분할 실행 작업 전체를 표현한다.

    원본 SQL을 파티션 컬럼 IN 목록 기준으로 N(``parallelism``)등분하여 ``tasks`` 로
    쪼갠 뒤, 각 Task를 executor에 분산 실행한다.

    주요 필드:
        original_sql    : 사용자가 제출한 원본 SELECT 쿼리.
        partition_column: IN 목록으로 분할할 기준 컬럼.
        target_table    : 결과를 적재할 대상 테이블.
        write_mode      : 적재 방식(append / overwrite_partitions).
        parallelism     : 분할 개수 상한(IN 값 개수로 클램핑됨).
        split_strategy  : 값 분배 전략(contiguous / round_robin).
        failure_policy  : 실패 처리 정책(fail_fast / best_effort).
        username        : 작업을 실행한 사용자(이력 기록용, 선택).
        exec_mode       : 실행 방식(copy / statement / stage_insert).
        staging_table   : stage_insert 모드에서 COPY 적재할 staging 테이블명.
        staging_ddl     : stage_insert 모드에서 staging 테이블 생성 DDL.
        insert_sql      : stage_insert 모드에서 staging→target 적재 INSERT 문.
        job_id          : ``job_...`` 형태 식별자(자동 발급).
        status          : 현재 ``JobStatus``.
        tasks           : 분할된 Task 목록.
        error           : 작업 수준 오류 메시지.
        created_at / started_at / finished_at : ISO-8601(UTC) 타임스탬프.
        cancel_requested: 취소 요청 플래그(executor 루프가 폴링하여 중단 판단).
    """

    original_sql: str
    partition_column: str
    target_table: str
    write_mode: str
    parallelism: int
    split_strategy: str
    failure_policy: str
    username: Optional[str] = None
    exec_mode: str = "copy"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
    # 요청별 Impala 쿼리 옵션(SET). executor 에서 전역 기본값 위에 병합되어 적용된다.
    impala_query_options: Optional[dict] = None
    job_id: str = field(default_factory=lambda: _new_id("job"))
    status: JobStatus = JobStatus.PENDING
    tasks: list[Task] = field(default_factory=list)
    error: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    cancel_requested: bool = False
    # 재실행으로 생성된 job 이면 원본 job_id 를 가리킨다(실패 파티션 재실행 추적용).
    retry_of: Optional[str] = None

    @property
    def total_rows_written(self) -> int:
        """모든 Task의 적재 행 수 합계(작업 전체 결과 규모)."""
        return sum(t.rows_written for t in self.tasks)

    @property
    def completed(self) -> int:
        """종료(terminal) 상태에 도달한 Task 수.

        성공·실패·취소를 모두 '완료'로 세는 이유는, 진행률이 '남은 일이 없는' 정도를
        나타내야 하기 때문이다(실패/취소도 더 진행할 일이 없으므로 완료로 카운트).
        """
        terminal = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}
        return sum(1 for t in self.tasks if t.status in terminal)

    @property
    def progress_percent(self) -> float:
        """완료된 Task 비율(%)을 소수점 1자리로 반환한다.

        Task가 아직 하나도 없으면(분할 전) 0으로 본다(0 나눗셈 방지).
        """
        total = len(self.tasks)
        return round(100.0 * self.completed / total, 1) if total else 0.0

    def status_view(self) -> dict:
        """상태 조회 API용 전체 뷰(각 Task 요약 목록 포함)를 반환한다."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "completed": self.completed,
            "total": len(self.tasks),
            "progress_percent": self.progress_percent,
            "total_rows_written": self.total_rows_written,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "retry_of": self.retry_of,
            "tasks": [t.summary() for t in self.tasks],
        }

    def to_record(self) -> dict:
        """SqlJobStore(공유 저장소)에 영속하기 위한 전체 직렬화 dict를 반환한다.

        하위 Task들도 각자 ``to_record`` 로 직렬화해 중첩 리스트로 포함하므로,
        이 dict 하나로 Job 전체 스냅샷을 손실 없이 복원할 수 있다(``from_record``).
        """
        return {
            "job_id": self.job_id,
            "original_sql": self.original_sql,
            "partition_column": self.partition_column,
            "target_table": self.target_table,
            "write_mode": self.write_mode,
            "parallelism": self.parallelism,
            "split_strategy": self.split_strategy,
            "failure_policy": self.failure_policy,
            "username": self.username,
            "exec_mode": self.exec_mode,
            "staging_table": self.staging_table,
            "staging_ddl": self.staging_ddl,
            "insert_sql": self.insert_sql,
            "impala_query_options": self.impala_query_options,
            "status": self.status.value,
            "error": self.error,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "cancel_requested": self.cancel_requested,
            "retry_of": self.retry_of,
            "tasks": [t.to_record() for t in self.tasks],
        }

    @classmethod
    def from_record(cls, d: dict) -> "Job":
        """``to_record`` 로 만든 dict(주로 DB JSONB)를 Job 객체로 복원한다.

        먼저 Job 본문을 복원한 뒤 중첩된 task 레코드들을 ``Task.from_record`` 로
        역직렬화해 ``tasks`` 에 채운다. 선택 필드는 ``get`` 으로 안전하게 읽고,
        문자열 상태는 ``JobStatus(...)`` 로 enum 복원한다.
        """
        job = cls(
            original_sql=d["original_sql"],
            partition_column=d["partition_column"],
            target_table=d["target_table"],
            write_mode=d["write_mode"],
            parallelism=d["parallelism"],
            split_strategy=d["split_strategy"],
            failure_policy=d["failure_policy"],
            username=d.get("username"),
            exec_mode=d.get("exec_mode", "copy"),
            staging_table=d.get("staging_table"),
            staging_ddl=d.get("staging_ddl"),
            insert_sql=d.get("insert_sql"),
            impala_query_options=d.get("impala_query_options"),
            job_id=d["job_id"],
            status=JobStatus(d.get("status", "PENDING")),
            error=d.get("error"),
            created_at=d.get("created_at") or _now_iso(),
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
            cancel_requested=d.get("cancel_requested", False),
            retry_of=d.get("retry_of"),
        )
        job.tasks = [Task.from_record(t) for t in d.get("tasks", [])]
        return job

    def progress_view(self) -> dict:
        """진행 상태 확인용 경량 뷰(태스크 목록 제외)."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "progress_percent": self.progress_percent,
            "completed": self.completed,
            "total": len(self.tasks),
            "total_rows_written": self.total_rows_written,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    def result_view(self) -> dict:
        """최종 결과 조회용 뷰. 전체 적재 행 수와 Task별 적재 행 수를 반환한다."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "total_rows_written": self.total_rows_written,
            "per_task": [
                {"task_id": t.task_id, "rows_written": t.rows_written}
                for t in self.tasks
            ],
        }


# ----------------------------- API 스키마 -----------------------------


class CreateJobRequest(BaseModel):
    """작업 생성 API(POST)의 요청 바디 스키마.

    pydantic이 타입·범위·허용값(Literal)을 자동 검증한다. 각 필드의 의미는
    아래 ``Field`` description을 참고. exec_mode/stage_insert·wrapper 관련 필드는
    서로 연동되며, 도메인 동작은 parser/splitter/executor 모듈에서 처리한다.
    """

    sql: str = Field(..., description="Impala SELECT 쿼리")
    partition_column: str = Field(..., description="IN 목록으로 분할할 기준 컬럼")
    target_table: str = Field(..., description="Greenplum 적재 대상 테이블")
    username: Optional[str] = Field(default=None, description="작업을 실행한 사용자")
    write_mode: Literal["append", "overwrite_partitions"] = "append"
    parallelism: int = Field(default=4, ge=1, le=128)
    split_strategy: Literal["contiguous", "round_robin"] = "contiguous"
    failure_policy: Literal["fail_fast", "best_effort"] = "fail_fast"
    exec_mode: Literal["copy", "statement", "stage_insert"] = Field(
        default="copy",
        description="copy: Impala read→Greenplum COPY. statement: SQL을 대상 DB에서 "
        "직접 실행. stage_insert: Impala 결과를 Greenplum staging(TEMP)에 COPY 후 "
        "staging→target INSERT 실행(서로 다른 엔진일 때).",
    )
    staging_table: Optional[str] = Field(
        default=None,
        description="stage_insert 모드: COPY 적재할 staging 테이블명(staging_ddl/INSERT가 참조).",
    )
    staging_ddl: Optional[str] = Field(
        default=None,
        description="stage_insert 모드: staging 테이블 생성 DDL(예: CREATE TEMP TABLE ...).",
    )
    dry_run: bool = Field(
        default=False,
        description="True면 executor를 호출하지 않고 생성된 쿼리만 로깅/반환한다(작업 미저장).",
    )
    sql_dialect: Optional[str] = Field(
        default=None,
        description="SQL 방언(미지정 시 서버 기본값). 예: hive, impala, postgres",
    )
    strict_validation: bool = Field(
        default=True,
        description="True면 단순 SELECT만 허용. 복합 쿼리(중첩 서브쿼리/JOIN/GROUP BY 등)는 "
        "False로 설정하면 파티션 IN 절을 트리 어디서든 찾아 분할한다.",
    )
    wrapper_query: Optional[str] = Field(
        default=None,
        description="분할된 sub-query를 감싸는 쿼리. placeholder 자리에 각 sub-query가 치환된다. "
        "예: 'SELECT * FROM ({{SUBQUERY}}) t'",
    )
    wrapper_placeholder: str = Field(
        default="{{SUBQUERY}}",
        description="wrapper_query 안에서 sub-query가 들어갈 자리표시자(기본 {{SUBQUERY}}).",
    )
    impala_query_options: Optional[dict[str, str]] = Field(
        default=None,
        description="이 작업의 Impala 쿼리 옵션(SET). 전역 impala.query_options 위에 병합된다. "
        "예: {\"MEM_LIMIT\": \"2g\", \"REQUEST_POOL\": \"etl\"}. 미지정 시 전역값만 적용.",
    )


class CreateJobResponse(BaseModel):
    """작업 생성 API의 응답 스키마. 생성된 Job 식별자를 돌려준다."""

    job_id: str
