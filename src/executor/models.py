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
import logging
from dataclasses import dataclass, field
from typing import Literal, Optional

from pydantic import BaseModel

from core.phases import phase_of, record_stage

logger = logging.getLogger(__name__)


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
            stage_insert 는 최종 INSERT 영향 행수, copy 는 COPY 적재 행수.
        rows_read: Impala 에서 스트리밍으로 읽어들인 행 수(=조회 건수). STREAM_COPY
            단계 종료 시점에 확정된다. copy 모드에서는 rows_written 과 같다.
        current_phase: 지금 진행 중인 세부 단계 이름(QUEUE_WAIT/IMPALA_SUBMIT/...). 모니터링용.
        phases: 세부 단계 타임라인(core.phases 참고). 각 단계의 시작/종료/소요/행수/지표.
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
    exec_mode: str = "copy"  # "copy" | "statement" | "stage_insert" | "local_stage" | "s3_stage"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
    impala_query_options: Optional[dict] = None  # 요청별 Impala SET 옵션(전역에 병합)
    # SELECT 를 읽을 소스 엔진. None/impala 면 built-in Impala 커서(기존 동작), 그 외 이름이면
    # 설정 query.func.fetch_module 의 커스텀 API 로 읽는다(커서 없는 소스).
    datasource: Optional[str] = None
    # local_stage 모드: Impala 결과를 떨어뜨릴 로컬 CSV 파일의 절대 경로와 CSV 방언.
    # coordinator 가 파일 경로/인덱스를 확정해 넘긴다(file:// URI 와 짝을 이룬다).
    out_path: Optional[str] = None
    csv_options: Optional[dict] = None  # {"delimiter","null","quote"} — 외부테이블 FORMAT 과 일치
    status: TaskStatus = TaskStatus.QUEUED
    rows_written: int = 0
    rows_read: int = 0
    current_phase: Optional[str] = None
    phases: list = field(default_factory=list)
    error: Optional[str] = None
    cancel_requested: bool = False
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    def on_stage(self, name: str, event: str, meta: Optional[dict] = None) -> None:
        """백엔드가 알려 오는 단계 경계를 phases 타임라인에 반영한다(콜백).

        블로킹 백엔드는 별도 스레드(run_in_executor)에서 돌지만, 리스트 append/속성 대입은
        GIL 하에서 원자적이라 기존 on_progress(rows_written 갱신)와 동일하게 안전하다.
        STREAM_COPY 종료 시의 rows 는 곧 Impala 조회 건수이므로 rows_read 로도 반영한다.
        """
        rows = record_stage(self.phases, name, event, meta)
        if event == "start":
            self.current_phase = name
            # 상세 추적(DEBUG): 단계 시작. [job][task] 는 로그 컨텍스트가 자동 주입한다.
            logger.debug("단계 시작 %s", name)
        elif event == "end":
            if name == "STREAM_COPY" and rows is not None:
                self.rows_read = rows
            phase = phase_of(self.phases, name)
            dur = phase.get("duration_ms") if phase else None
            extra = phase.get("extra") if phase else None
            # 상세 추적(DEBUG): 단계 종료 + 소요(ms)/행수/부가지표(read/write 대기 등).
            logger.debug("단계 종료 %s (소요=%sms, 행수=%s, 지표=%s)",
                         name, dur, rows, extra)

    def impala_done_at(self) -> Optional[str]:
        """Impala 조회가 완료된 시각(STREAM_COPY 단계 종료 시각). 없으면 None."""
        phase = phase_of(self.phases, "STREAM_COPY")
        return phase["finished_at"] if phase else None

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
            "rows_read": self.rows_read,
            "current_phase": self.current_phase,
            "impala_done_at": self.impala_done_at(),
            "phases": [dict(p) for p in self.phases],
            "error": self.error,
            "cancel_requested": self.cancel_requested,
            "username": self.username,
            "exec_mode": self.exec_mode,
            "target_table": self.target_table,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    def detail(self) -> dict:
        """``view()`` 에 실행 SQL 전문(sub_query/staging/insert)을 더한 상세 dict.

        목록 응답을 가볍게 유지하려고 ``view()`` 에서 뺀 원문 SQL 을 단건 상세
        조회(``GET /tasks/{id}/detail``)에서만 노출한다. 대시보드 처리중 탭의
        task SQL 열람이 이를 사용한다(이력 탭은 task_history 에 저장된 SQL 사용).
        """
        return {
            **self.view(),
            "sub_query": self.sub_query,
            "staging_table": self.staging_table,
            "staging_ddl": self.staging_ddl,
            "insert_sql": self.insert_sql,
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
    exec_mode: Literal["copy", "statement", "stage_insert", "local_stage", "s3_stage"] = "copy"
    staging_table: Optional[str] = None
    staging_ddl: Optional[str] = None
    insert_sql: Optional[str] = None
    impala_query_options: Optional[dict] = None
    # 이 task 의 SELECT 를 읽을 소스 엔진(coordinator 가 job 에서 확정해 싣는다). 미지정이면
    # built-in Impala — 이 필드를 안 보내는 구버전 coordinator 와 호환된다.
    datasource: Optional[str] = None
    # local_stage 는 로컬 CSV 경로, s3_stage 는 S3 객체 키를 out_path 로 받는다(coordinator 확정).
    out_path: Optional[str] = None
    csv_options: Optional[dict] = None


class DatasourceQueryRequest(BaseModel):
    """``POST /datasources/{name}/query`` 요청 본문(임의 SELECT 미리보기/연결 테스트).

    내부 점검용이므로 임의 SQL 을 허용한다. 반환 행수는 limit 으로 잘린다(상한은
    서버측 ``MAX_PREVIEW_ROWS``). 기본 SQL 은 연결만 빠르게 확인하는 ``SELECT 1``.
    """

    sql: str = "SELECT 1"
    limit: int = 100


class QueryRunRequest(BaseModel):
    """``POST /query-run`` 요청 본문(coordinator → executor, query-execute 위임).

    coordinator 가 템플릿을 렌더·검증한 SELECT 를 그대로 넘긴다. executor 는 설정
    (``query.func.module`` + ``query.func.config.*``)으로 지정한 커스텀 함수에 이 SQL 을
    실행 위임한다 — executor 는 Trino 등 백엔드를 직접 알지 않는다.
    """

    sql: str
    limit: int = 100
    # 실행 엔진 이름(로그 표기 전용, 예: trino). 실행 함수 선택에는 쓰지 않는다 —
    # 위임 대상은 언제나 query.func.module 하나다. coordinator 가 확정한 datasource 를
    # 실어 보내야 executor 로그에 "어느 엔진으로 실행했는가"가 남는다. 구버전
    # coordinator 가 안 보내도 되도록 선택 필드로 두고, 없으면 source 로 표기한다.
    datasource: Optional[str] = None
