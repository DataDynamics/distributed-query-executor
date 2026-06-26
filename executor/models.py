"""Executor 측 task 모델 및 API 스키마."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Literal, Optional

from pydantic import BaseModel


class TaskStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    READING = "READING"
    WRITING = "WRITING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class Task:
    task_id: str
    job_id: str
    sub_query: str
    target_table: str
    write_mode: str
    partition_column: str
    partition_values: list[str]
    exec_mode: str = "copy"  # "copy" | "statement" | "stage_insert"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
    status: TaskStatus = TaskStatus.QUEUED
    rows_written: int = 0
    error: Optional[str] = None
    cancel_requested: bool = False

    def view(self) -> dict:
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "status": self.status.value,
            "rows_written": self.rows_written,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
        }


class CreateTaskRequest(BaseModel):
    task_id: str
    job_id: str
    sub_query: str
    target_table: str
    write_mode: Literal["append", "overwrite_partitions"] = "append"
    partition_column: str
    partition_values: list[str] = []
    exec_mode: Literal["copy", "statement", "stage_insert"] = "copy"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
