"""도메인 모델(Job/Task) 및 API 요청/응답 스키마."""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    SPLITTING = "SPLITTING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    READING = "READING"
    WRITING = "WRITING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def new_job_id() -> str:
    return _new_id("job")


@dataclass
class Task:
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
        return {**self.summary(), "sub_query": self.sub_query}

    def to_record(self) -> dict:
        """공유 저장용 전체 직렬화."""
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
    original_sql: str
    partition_column: str
    target_table: str
    write_mode: str
    parallelism: int
    split_strategy: str
    failure_policy: str
    exec_mode: str = "copy"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
    job_id: str = field(default_factory=lambda: _new_id("job"))
    status: JobStatus = JobStatus.PENDING
    tasks: list[Task] = field(default_factory=list)
    error: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    cancel_requested: bool = False

    @property
    def total_rows_written(self) -> int:
        return sum(t.rows_written for t in self.tasks)

    @property
    def completed(self) -> int:
        terminal = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}
        return sum(1 for t in self.tasks if t.status in terminal)

    @property
    def progress_percent(self) -> float:
        total = len(self.tasks)
        return round(100.0 * self.completed / total, 1) if total else 0.0

    def status_view(self) -> dict:
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
            "tasks": [t.summary() for t in self.tasks],
        }

    def to_record(self) -> dict:
        """공유 저장(SqlJobStore)용 전체 직렬화."""
        return {
            "job_id": self.job_id,
            "original_sql": self.original_sql,
            "partition_column": self.partition_column,
            "target_table": self.target_table,
            "write_mode": self.write_mode,
            "parallelism": self.parallelism,
            "split_strategy": self.split_strategy,
            "failure_policy": self.failure_policy,
            "exec_mode": self.exec_mode,
            "staging_table": self.staging_table,
            "staging_ddl": self.staging_ddl,
            "insert_sql": self.insert_sql,
            "status": self.status.value,
            "error": self.error,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "cancel_requested": self.cancel_requested,
            "tasks": [t.to_record() for t in self.tasks],
        }

    @classmethod
    def from_record(cls, d: dict) -> "Job":
        job = cls(
            original_sql=d["original_sql"],
            partition_column=d["partition_column"],
            target_table=d["target_table"],
            write_mode=d["write_mode"],
            parallelism=d["parallelism"],
            split_strategy=d["split_strategy"],
            failure_policy=d["failure_policy"],
            exec_mode=d.get("exec_mode", "copy"),
            staging_table=d.get("staging_table"),
            staging_ddl=d.get("staging_ddl"),
            insert_sql=d.get("insert_sql"),
            job_id=d["job_id"],
            status=JobStatus(d.get("status", "PENDING")),
            error=d.get("error"),
            created_at=d.get("created_at") or _now_iso(),
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
            cancel_requested=d.get("cancel_requested", False),
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
    sql: str = Field(..., description="Impala SELECT 쿼리")
    partition_column: str = Field(..., description="IN 목록으로 분할할 기준 컬럼")
    target_table: str = Field(..., description="Greenplum 적재 대상 테이블")
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


class CreateJobResponse(BaseModel):
    job_id: str
