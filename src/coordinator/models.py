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
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, Field

from core.phases import phase_of, record_stage
from core.timeutil import now_iso as _now_iso  # KST(naive) ISO 시각 생성

logger = logging.getLogger(__name__)


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
    # 세부 단계 가시화(원격: executor 폴링으로 수집 / local: 직접 실행 중 on_stage 로 채움).
    rows_read: int = 0
    current_phase: Optional[str] = None
    phases: list = field(default_factory=list)
    # local_stage 전용: 이 task 가 로컬 CSV 를 쓸 절대 경로(coordinator 가 확정).
    out_path: Optional[str] = None

    def on_stage(self, name: str, event: str, meta: Optional[dict] = None) -> None:
        """local 모드에서 백엔드가 알려 오는 단계 경계를 phases 에 반영한다(executor.Task 와 동형)."""
        rows = record_stage(self.phases, name, event, meta)
        if event == "start":
            self.current_phase = name
            logger.debug("단계 시작 %s", name)  # 상세 추적(DEBUG). [job][task] 자동 주입
        elif event == "end":
            if name == "STREAM_COPY" and rows is not None:
                self.rows_read = rows
            phase = phase_of(self.phases, name)
            dur = phase.get("duration_ms") if phase else None
            extra = phase.get("extra") if phase else None
            logger.debug("단계 종료 %s (소요=%sms, 행수=%s, 지표=%s)",
                         name, dur, rows, extra)

    def impala_done_at(self) -> Optional[str]:
        """Impala 조회 완료 시각(STREAM_COPY 종료). 없으면 None."""
        phase = phase_of(self.phases, "STREAM_COPY")
        return phase["finished_at"] if phase else None

    def summary(self) -> dict:
        """상태 조회 응답용 경량 요약 dict를 반환한다(무거운 sub_query 전문은 제외)."""
        return {
            "task_id": self.task_id,
            "executor_url": self.executor_url,
            "status": self.status.value,
            "rows_written": self.rows_written,
            "rows_read": self.rows_read,
            "current_phase": self.current_phase,
            "impala_done_at": self.impala_done_at(),
            "phases": [dict(p) for p in self.phases],
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
            "rows_read": self.rows_read,
            "current_phase": self.current_phase,
            "phases": self.phases,
            "attempt": self.attempt,
            "error": self.error,
            "out_path": self.out_path,
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
            rows_read=d.get("rows_read", 0),
            current_phase=d.get("current_phase"),
            phases=list(d.get("phases") or []),
            attempt=d.get("attempt", 0),
            error=d.get("error"),
            out_path=d.get("out_path"),
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
        created_at / started_at / finished_at : ISO-8601(KST, 타임존 없는 naive) 타임스탬프.
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
    # 템플릿 모드로 생성된 job 이면 사용한 template_id 와 렌더 파라미터를 보관(감사·재현용).
    # original_sql 에는 이미 렌더된 SELECT 전문이 들어가므로 retry 는 재렌더 없이 동작한다.
    template_id: Optional[str] = None
    template_params: Optional[dict] = None
    # local_stage 전용(file:// 세그먼트 로컬 스테이징, §17).
    external_columns: Optional[str] = None   # file:// 외부테이블 컬럼 정의(요청자 명시)
    export_local_dir: Optional[str] = None   # 로컬 CSV 루트 오버라이드(없으면 stage.local_dir)
    csv_delimiter: Optional[str] = None      # CSV 방언 오버라이드(없으면 설정값)
    csv_null: Optional[str] = None
    csv_quote: Optional[str] = None
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
    # 요청 멱등(Idempotency-Key): 클라이언트가 헤더로 준 키와, 그때 요청 본문의 지문(sha256).
    # 같은 키로 다시 POST /jobs 하면 새 job 을 만들지 않고 이 job 을 그대로 돌려준다(중복 적재 방지).
    # 지문은 같은 키를 다른 본문으로 재사용했는지 감지(불일치 시 409)하는 데 쓴다.
    idempotency_key: Optional[str] = None
    request_fingerprint: Optional[str] = None

    @property
    def total_rows_written(self) -> int:
        """모든 Task의 적재 행 수 합계(작업 전체 결과 규모)."""
        return sum(t.rows_written for t in self.tasks)

    @property
    def total_rows_read(self) -> int:
        """모든 Task가 Impala 에서 읽어들인 행 수 합계(=조회 건수 합)."""
        return sum(getattr(t, "rows_read", 0) for t in self.tasks)

    def phase_summary(self) -> dict:
        """진행 중(비종료) Task 들이 현재 어느 세부 단계에 있는지 집계한다.

        ``{"STREAM_COPY": 3, "INSERT": 2, ...}`` 형태로, 대시보드에서 "지금 어디까지
        진행됐나"를 한눈에 보여주는 데 쓴다. 종료된 Task 나 단계 정보가 없는 Task 는 뺀다.
        """
        terminal = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}
        out: dict[str, int] = {}
        for t in self.tasks:
            if t.status in terminal:
                continue
            phase = getattr(t, "current_phase", None)
            if phase:
                out[phase] = out.get(phase, 0) + 1
        return out

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
            "total_rows_read": self.total_rows_read,
            "phase_summary": self.phase_summary(),
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
            "template_id": self.template_id,
            "template_params": self.template_params,
            "external_columns": self.external_columns,
            "export_local_dir": self.export_local_dir,
            "csv_delimiter": self.csv_delimiter,
            "csv_null": self.csv_null,
            "csv_quote": self.csv_quote,
            "status": self.status.value,
            "error": self.error,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "cancel_requested": self.cancel_requested,
            "retry_of": self.retry_of,
            "idempotency_key": self.idempotency_key,
            "request_fingerprint": self.request_fingerprint,
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
            template_id=d.get("template_id"),
            template_params=d.get("template_params"),
            external_columns=d.get("external_columns"),
            export_local_dir=d.get("export_local_dir"),
            csv_delimiter=d.get("csv_delimiter"),
            csv_null=d.get("csv_null"),
            csv_quote=d.get("csv_quote"),
            job_id=d["job_id"],
            status=JobStatus(d.get("status", "PENDING")),
            error=d.get("error"),
            created_at=d.get("created_at") or _now_iso(),
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
            cancel_requested=d.get("cancel_requested", False),
            retry_of=d.get("retry_of"),
            idempotency_key=d.get("idempotency_key"),
            request_fingerprint=d.get("request_fingerprint"),
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


class QueryParam(BaseModel):
    """템플릿 렌더 파라미터 한 항목(이름-값[-부호]).

    ``POST /query-execute`` 와 ``POST /jobs`` 가 공유한다(``/jobs`` 는 하위 호환을 위해
    이름→값 dict 도 계속 받는다). manifest 의 ``params`` 스키마와 이름으로 매칭되며,
    ``value`` 는 스칼라뿐 아니라 배열(list 타입 파라미터)도 될 수 있어 ``Any`` 로 둔다 —
    타입 강제/검증은 이후 템플릿 엔진의 ``ParamSpec`` 이 담당한다.

    ``sign`` 은 **값의 부호가 아니라 SQL 연산자의 방향**을 뜻한다. Impala ``interval`` 이
    절대값만 받기 때문에 ``current_date() - interval 7 day`` 처럼 부호가 SQL 텍스트에
    박히는 쿼리에서는, 파라미터 값(7)만으로 "오늘 기준 -7일" 이라는 의미를 복원할 수 없다.
    그 방향을 클라이언트가 명시하는 자리가 ``sign`` 이다. 렌더 컨텍스트에는
    ``<name>_sign`` 으로 노출되어 템플릿이 ``{{ from_date_no_sign | sql_sign }}`` 로
    연산자를 찍을 수 있고, 날짜 fan-out(``task_params``)은 이 부호로 구간을 도출한다.
    ``sign`` 이 없으면 값 자체의 부호를 쓴다(``value:-7`` == ``value:7, sign:"-"``).
    """

    name: str = Field(..., description="파라미터 이름(manifest params 의 name 과 일치)")
    value: Any = Field(default=None, description="파라미터 값(스칼라 또는 배열)")
    sign: Optional[Literal["+", "-"]] = Field(
        default=None,
        description="SQL 연산자 방향(+/-). interval 처럼 부호가 SQL 에 박힌 쿼리에서 "
        "값의 의미 방향을 나타낸다. 템플릿에는 <name>_sign 으로 노출된다.",
    )


class CreateJobRequest(BaseModel):
    """작업 생성 API(POST)의 요청 바디 스키마.

    pydantic이 타입·범위·허용값(Literal)을 자동 검증한다. 각 필드의 의미는
    아래 ``Field`` description을 참고. exec_mode/stage_insert·wrapper 관련 필드는
    서로 연동되며, 도메인 동작은 parser/splitter/executor 모듈에서 처리한다.
    """

    # ── 템플릿 모드(선택) ──
    # template_id 를 주면 서버 템플릿을 params 로 렌더링해 sql/staging_ddl/insert_sql/
    # external_columns/wrapper_query 를 자동 생성한다. 이 경우 아래 sql 등 SQL 필드는 생략 가능
    # (렌더 결과로 채워진다). partition_column/target_table/exec_mode 등 스칼라는 요청이 명시하면
    # 요청이, 없으면 manifest 기본값이 쓰인다. template_id 미지정 시 기존 raw-SQL 방식 그대로.
    template_id: Optional[str] = Field(
        default=None,
        description="서버 템플릿 ID(디렉터리명). 지정 시 params 로 SQL 을 런타임 생성한다.",
    )
    params: Union[dict, list[QueryParam]] = Field(
        default_factory=dict,
        description="템플릿 렌더링 파라미터(template_id 지정 시 사용). 이름→값 dict 또는 "
        "[{name, value, sign}] 배열. 배열 형태만 sign(연산자 방향)을 표현할 수 있다.",
    )

    # template_id 를 주면 렌더 결과가 채우므로 sql 은 선택이 된다(raw 모드에서는 필수처럼 취급).
    sql: Optional[str] = Field(default=None, description="Impala SELECT 쿼리(raw 모드 필수)")
    partition_column: Optional[str] = Field(
        default=None, description="IN 목록으로 분할할 기준 컬럼(raw 모드 필수, 템플릿은 manifest 기본값 가능)"
    )
    target_table: Optional[str] = Field(
        default=None, description="Greenplum 적재 대상 테이블(raw 모드 필수, 템플릿은 manifest 기본값 가능)"
    )
    # ── 날짜 fan-out 모드(파티션 IN 분할 대체) ──
    # task_params 로 '구간의 두 끝'을 담은 파라미터 두 개를 지목하면, IN 값 분할 대신
    # '하루=1 task' 로 펼친다. 각 끝의 오프셋은 (값, sign)에서 도출하고(sign 없으면 값의 부호),
    # task 마다 두 파라미터를 같은 날로 좁혀 렌더한다 — 값은 언제나 절대값, 부호는 <name>_sign
    # 으로 나가므로 Impala interval(절대값만 허용)에 그대로 들어간다.
    # 템플릿 stage_insert 전용(select+insert 조각 필요).
    task_params: Optional[list[str]] = Field(
        default=None,
        description="날짜 fan-out: 구간의 두 끝을 담은 params 이름 2개(예: "
        '["from_date_no", "to_date_no"]). 지정 시 IN 분할 대신 하루=1 task 로 펼친다.',
    )
    task_bound: Literal["point", "pair"] = Field(
        default="point",
        description="task 하나가 받는 두 오프셋의 형태. point=(d, d) — 양끝 포함 비교"
        "(BETWEEN/=)용, 날짜 수만큼 task. pair=(d, d+1) — 반열림 비교(>= AND <)용, "
        "날짜 수-1 만큼 task. manifest 기본값으로도 지정 가능.",
    )
    task_date_format: str = Field(
        default="%Y-%m-%d", description="task_date 로 주입할 날짜 문자열 포맷(strftime)."
    )
    username: Optional[str] = Field(default=None, description="작업을 실행한 사용자")
    write_mode: Literal["append", "overwrite_partitions"] = "append"
    parallelism: int = Field(default=4, ge=1, le=128)
    split_strategy: Literal["contiguous", "round_robin"] = "contiguous"
    failure_policy: Literal["fail_fast", "best_effort"] = "fail_fast"
    exec_mode: Literal["copy", "statement", "stage_insert", "local_stage", "s3_stage"] = Field(
        default="copy",
        description="copy: Impala read→Greenplum COPY. statement: SQL을 대상 DB에서 "
        "직접 실행. stage_insert: Impala 결과를 Greenplum staging(TEMP)에 COPY 후 "
        "staging→target INSERT 실행(서로 다른 엔진일 때). local_stage: executor 가 "
        "세그먼트 호스트 로컬 CSV 로 export 후, GP 가 file:// 외부테이블로 세그먼트 로컬 "
        "병렬 read 하여 staging 적재→target INSERT(2-phase, executor co-locate 필요). "
        "s3_stage: executor 가 로컬 CSV → S3 업로드 → GP PXF 외부테이블 read → target "
        "INSERT → S3 정리(per-task 완결, 세그먼트 co-locate 불필요).",
    )
    staging_table: Optional[str] = Field(
        default=None,
        description="stage_insert 모드: COPY 적재할 staging 테이블명(staging_ddl/INSERT가 참조).",
    )
    staging_ddl: Optional[str] = Field(
        default=None,
        description="stage_insert 모드: staging 테이블 생성 DDL(예: CREATE TEMP TABLE ...). "
        "선택 — 비우면 테이블 생성을 건너뛰고 기존 staging_table 을 사용한다.",
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
    # ── local_stage 전용 필드 ──
    external_columns: Optional[str] = Field(
        default=None,
        description="local_stage 모드: file:// 외부테이블 컬럼 정의(명시). CSV 컬럼 순서와 "
        "일치해야 한다. 예: 'user_id int, amount numeric, dt date'.",
    )
    insert_sql: Optional[str] = Field(
        default=None,
        description="local_stage 모드: staging→target 적재 INSERT 문. "
        "예: 'INSERT INTO public.sales SELECT * FROM stg_sales'.",
    )
    export_local_dir: Optional[str] = Field(
        default=None,
        description="local_stage 모드: 로컬 CSV 저장 루트 오버라이드(미지정 시 stage.local_dir). "
        "모든 세그먼트 호스트가 동일 경로를 쓴다.",
    )
    csv_delimiter: Optional[str] = Field(
        default=None,
        description="local_stage CSV 컬럼 구분자 오버라이드(미지정 시 stage.csv_delimiter, 기본 `).",
    )
    csv_null: Optional[str] = Field(
        default=None, description="local_stage CSV NULL 표현 오버라이드(미지정 시 설정값)."
    )
    csv_quote: Optional[str] = Field(
        default=None, description="local_stage CSV 인용문자 오버라이드(미지정 시 설정값)."
    )


class CreateJobResponse(BaseModel):
    """작업 생성 API의 응답 스키마. 생성된 Job 식별자를 돌려준다."""

    job_id: str


class QueryExecuteRequest(BaseModel):
    """``POST /query-execute`` 요청 — 서버 템플릿을 params 로 렌더한 SELECT 를 실행하고
    결과(상위 N행)를 동기로 돌려받는다.

    ``/jobs``(Impala→Greenplum 이관)와 달리 결과가 coordinator 를 거쳐 클라이언트로
    반환되는 미리보기성 실행이다. 클라이언트는 어떤 executor 가 실행하는지 모른다 —
    impala/trino 소스는 coordinator 가 부하가 가장 낮은 executor 를 골라 프록시하고,
    greenplum/history 는 coordinator 가 직접 실행한다.
    """

    template_id: str = Field(..., description="서버 템플릿 ID(디렉터리명). params 로 SELECT 를 렌더한다.")
    params: list[QueryParam] = Field(
        default_factory=list,
        description="쿼리 빌드 파라미터(이름-값 항목 배열). 내부에서 {name: value} 로 접어 렌더한다.",
    )
    datasource: Optional[str] = Field(
        default=None,
        description="실행 데이터소스(impala|trino|greenplum|history). 미지정 시 서버 source.type.",
    )
    limit: int = Field(default=100, ge=1, le=10_000, description="반환 최대 행수(1~10000).")
    sql_dialect: Optional[str] = Field(
        default=None, description="렌더된 SELECT 검증에 쓸 방언(미지정 시 서버 기본 hive)."
    )


class DatasourceQueryRequest(BaseModel):
    """``POST /datasources/{name}/query`` 요청(임의 SELECT 미리보기 / 연결 테스트).

    내부 점검용이므로 임의 SQL 을 허용한다. ``executor_url`` 을 주면 해당 executor 로
    프록시한다 — coordinator 가 직접 드라이버를 갖지 않는 데이터소스(특히 Impala)를
    executor 를 통해 테스트하기 위함이다. 생략하면 coordinator 가 직접 접속 가능한
    데이터소스(history/greenplum)만 로컬 실행한다.
    """

    sql: str = Field(default="SELECT 1", description="실행할 SQL(임의 SELECT 허용)")
    limit: int = Field(default=100, ge=1, le=10_000, description="반환 최대 행수")
    executor_url: Optional[str] = Field(
        default=None, description="지정 시 해당 executor 의 동일 엔드포인트로 프록시"
    )
