"""Executor 측 task 모델 및 API 스키마.

이 모듈은 두 가지를 정의한다.
- ``Task``: executor 프로세스가 인메모리로 들고 있는 실행 단위(가변 상태). 상태 전이
  (QUEUED→READING→WRITING→DONE/FAILED/CANCELLED)와 진행률(rows_written),
  시각(started_at/finished_at) 등 런타임 정보를 담는다.
- ``CreateTaskRequest``: coordinator → executor 의 ``POST /tasks`` 요청 본문 검증용
  pydantic 스키마. 외부에서 받는 값이므로 enum/Literal 로 허용값을 제한한다.

상태 머신(Task.status) 개요::

    QUEUED ──> READING ──> WRITING ──> DONE
       │                       │
       └────────> CANCELLED <──┘   (취소 요청 시)
                  FAILED            (예외 발생 시 어느 단계에서든)
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Literal, Optional

from pydantic import BaseModel


class TaskStatus(str, enum.Enum):
    """task 의 수명주기 상태.

    ``str`` 를 상속해 JSON 직렬화/비교 시 문자열 값("QUEUED" 등)처럼 다룰 수 있다.
    """

    QUEUED = "QUEUED"      # 접수 완료, 아직 실행 시작 전(세마포어 슬롯 대기 포함)
    READING = "READING"    # 소스(Impala)에서 결과를 읽는 중
    WRITING = "WRITING"    # 대상(Greenplum)에 적재(COPY/INSERT)하는 중
    DONE = "DONE"          # 정상 완료(terminal)
    FAILED = "FAILED"      # 예외로 실패(terminal). error 에 메시지 보관
    CANCELLED = "CANCELLED"  # 취소됨(terminal)


@dataclass
class Task:
    """executor 가 인메모리로 관리하는 실행 단위(하나의 sub-query).

    coordinator 가 분할한 sub-query 하나에 대응한다. 앞부분 필드(task_id~insert_sql)는
    접수 시점에 요청으로부터 채워지는 불변 입력값이고, 뒷부분 필드(status~finished_at)는
    실행 진행에 따라 갱신되는 런타임 상태다.

    필드:
        task_id: task 식별자(요청에서 부여, executor 인메모리 dict 의 키).
        job_id: 상위 job 식별자(여러 task 가 같은 job_id 를 공유). 로그 컨텍스트/이력에 사용.
        sub_query: 소스에서 실행할 SELECT(또는 statement 모드의 실행 문장).
        target_table: 적재 대상 테이블(스키마 포함 가능).
        write_mode: "append" | "overwrite_partitions". 후자는 적재 전 해당 파티션 DELETE.
        partition_column: overwrite_partitions 시 DELETE 조건 컬럼.
        partition_values: overwrite_partitions 대상 파티션 값 목록(멱등 DELETE 의 IN 절).
        username: 요청 사용자(이력 기록용, 선택).
        exec_mode: 실행 방식 — "copy"(Impala→Greenplum COPY) | "statement"(대상 DB에서
            SQL 직접 실행) | "stage_insert"(staging COPY 후 staging→target INSERT).
        staging_table: stage_insert 모드의 임시 staging 테이블명.
        staging_ddl: stage_insert 모드의 CREATE TEMP TABLE DDL.
        insert_sql: stage_insert 모드의 staging→target INSERT 문.
        status: 현재 상태(TaskStatus). 기본 QUEUED.
        rows_written: 지금까지 적재된 행 수(진행률 콜백으로 갱신, 완료 시 최종값).
        error: 실패 시 예외 메시지(FAILED 일 때만 설정).
        cancel_requested: 취소 요청 플래그. 실행 루프가 안전 지점에서 확인해 CANCELLED 로 전이.
        started_at: 실행(READING) 시작 시각(ISO8601 KST naive 문자열).
        finished_at: 종료(DONE/FAILED/CANCELLED) 시각(ISO8601 KST naive 문자열).
    """

    task_id: str
    job_id: str
    sub_query: str
    target_table: str
    write_mode: str
    partition_column: str
    partition_values: list[str]
    username: Optional[str] = None
    exec_mode: str = "copy"  # "copy" | "statement" | "stage_insert"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
    impala_query_options: Optional[dict] = None  # 요청별 Impala SET 옵션(전역에 병합)
    status: TaskStatus = TaskStatus.QUEUED
    rows_written: int = 0
    error: Optional[str] = None
    cancel_requested: bool = False
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    def view(self) -> dict:
        """API 응답/대시보드용 직렬화 dict 를 만든다.

        내부 전용 필드(sub_query, staging_* 등 원문 SQL)는 제외하고 모니터링에 필요한
        요약 정보만 노출한다. ``status`` 는 enum 값 문자열로 변환한다.
        """
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "status": self.status.value,
            "rows_written": self.rows_written,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
            "username": self.username,
            "exec_mode": self.exec_mode,
            "target_table": self.target_table,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


class CreateTaskRequest(BaseModel):
    """``POST /tasks`` 요청 본문 스키마(coordinator → executor).

    외부 입력이므로 ``write_mode``/``exec_mode`` 는 Literal 로 허용값을 제한해 잘못된
    값이 들어오면 FastAPI 가 422 로 거절하도록 한다. 필드 의미는 ``Task`` 와 동일하다.
    stage_insert 모드에서만 staging_table/insert_sql 이 필요하다. staging_ddl 은
    선택이며, 비어 있으면 executor 가 테이블 생성을 건너뛰고 기존 staging_table 을 쓴다.
    """

    task_id: str
    job_id: str
    sub_query: str
    target_table: str
    write_mode: Literal["append", "overwrite_partitions"] = "append"
    partition_column: str
    partition_values: list[str] = []
    username: Optional[str] = None
    exec_mode: Literal["copy", "statement", "stage_insert"] = "copy"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
    impala_query_options: Optional[dict] = None
