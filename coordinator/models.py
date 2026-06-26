"""Domain models (Job/Task) and API request/response schemas."""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from typing import Literal, Optional

from pydantic import BaseModel, Field


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


@dataclass
class Task:
    job_id: str
    executor_url: Optional[str]
    sub_query: str  # full sub-query text sent to the executor (retained)
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


@dataclass
class Job:
    original_sql: str
    partition_column: str
    target_table: str
    write_mode: str
    parallelism: int
    split_strategy: str
    failure_policy: str
    job_id: str = field(default_factory=lambda: _new_id("job"))
    status: JobStatus = JobStatus.PENDING
    tasks: list[Task] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def total_rows_written(self) -> int:
        return sum(t.rows_written for t in self.tasks)

    @property
    def completed(self) -> int:
        terminal = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}
        return sum(1 for t in self.tasks if t.status in terminal)

    def status_view(self) -> dict:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "completed": self.completed,
            "total": len(self.tasks),
            "total_rows_written": self.total_rows_written,
            "error": self.error,
            "tasks": [t.summary() for t in self.tasks],
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


# ----------------------------- API schemas -----------------------------


class CreateJobRequest(BaseModel):
    sql: str = Field(..., description="Impala SELECT query")
    partition_column: str = Field(..., description="Column used to split via IN-list")
    target_table: str = Field(..., description="Greenplum target table")
    write_mode: Literal["append", "overwrite_partitions"] = "append"
    parallelism: int = Field(default=4, ge=1, le=128)
    split_strategy: Literal["contiguous", "round_robin"] = "contiguous"
    failure_policy: Literal["fail_fast", "best_effort"] = "fail_fast"


class CreateJobResponse(BaseModel):
    job_id: str
