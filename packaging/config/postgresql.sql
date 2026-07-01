-- ============================================================================
-- Distributed Query Executor — PostgreSQL 통합 스키마
-- ============================================================================
-- 앱은 각 테이블을 CREATE TABLE IF NOT EXISTS 로 자동 생성하지만, 사전 생성이나
-- 권한 관리를 원할 때 이 스크립트 하나로 전체 스키마를 만들 수 있다.
--
-- 적용:  psql "postgresql://user:pass@pg-host:5432/queryexec" -f postgresql.sql
--
-- 스키마: 모든 메타 테이블은 public 스키마로 명시 한정한다. 앱 설정 db.schema(기본 public)와
--   반드시 일치해야 한다 — 앱 런타임 SQL 도 db.schema 로 테이블명을 한정하기 때문이다.
--   다른 스키마를 쓰려면 db.schema 를 바꾸고 이 파일의 public. 도 함께 치환할 것.
--
-- 시각 컬럼은 모두 KST(Asia/Seoul) 기준의 타임존 없는 TIMESTAMP 다(이 시스템은 한국 단일
-- 리전이라 UTC/타임존이 필요 없다). 기본값은 now() AT TIME ZONE 'Asia/Seoul' 로 KST 벽시계를
-- 넣는다. 앱이 직접 넣는 시각도 KST naive 문자열이다.
--   기존(TIMESTAMPTZ) DB 를 옮긴다면:
--     ALTER TABLE <t> ALTER COLUMN <col> TYPE TIMESTAMP USING (<col> AT TIME ZONE 'Asia/Seoul');
--
-- 테이블별 사용 DSN/설정:
--   jobs                    : store.backend=postgres (멀티 coordinator 공유 Job 저장소)
--   job_history             : history.db_dsn         (coordinator, job 단위 실행 이력)
--   task_history            : history.db_dsn         (executor, task 단위 실행 이력)
--   executor_status         : history.db_dsn         (executor.self_report=true 상태)
--   executor_health_metrics : monitor.db_dsn         (coordinator 헬스/메트릭 기록)
-- 보통 history.db_dsn 하나에 jobs/job_history/task_history/executor_status 를 함께 둔다.
-- ============================================================================


-- ─────────────────────────────────────────────────────────────────────────
-- 1) jobs — 멀티 coordinator 공유 Job 저장소 (store.backend=postgres)
--    어느 coordinator로 요청이 가도 조회/취소가 가능하도록 Job 스냅샷을 영속한다.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.jobs (
    job_id           TEXT PRIMARY KEY,
    coordinator_id   TEXT,
    status           TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul'),
    data             JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON public.jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON public.jobs (updated_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 2) job_history — coordinator 의 job 단위 실행 이력 (history.db_dsn)
--    run() 안에서 상태 전이(시작=RUNNING, 종료=DONE/PARTIAL/FAILED)마다 append.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.job_history (
    id                 BIGSERIAL PRIMARY KEY,
    recorded_at        TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul'),
    job_id             TEXT NOT NULL,
    username           TEXT,
    status             TEXT NOT NULL,
    partition_column   TEXT,
    target_table       TEXT,
    parallelism        INTEGER,
    total_tasks        INTEGER,
    completed_tasks    INTEGER,
    total_rows_written BIGINT,
    error              TEXT,
    created_at         TIMESTAMP,
    started_at         TIMESTAMP,
    finished_at        TIMESTAMP,
    original_sql       TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_history_job_id ON public.job_history (job_id);
CREATE INDEX IF NOT EXISTS idx_job_history_recorded_at ON public.job_history (recorded_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 3) task_history — 각 executor 의 task 단위 실행 이력 (history.db_dsn)
--    상태 전이(QUEUED/READING/WRITING/DONE/FAILED)마다 append. 하나의 job_id 아래
--    N개 task 가 executor_id 별로 기록된다.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.task_history (
    id           BIGSERIAL PRIMARY KEY,
    recorded_at  TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul'),
    job_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    username     TEXT,
    executor_id  TEXT,
    status       TEXT NOT NULL,
    rows_written BIGINT,
    error        TEXT,
    started_at   TIMESTAMP,   -- task 가 READING 을 시작한 시각
    finished_at  TIMESTAMP,   -- task 가 종료(DONE/FAILED/CANCELLED)된 시각
    sub_query    TEXT,          -- 이 task 가 실행한 SELECT sub-query 전문
    exec_mode    TEXT,          -- 적재 방식: copy | statement | stage_insert
    staging_ddl  TEXT,          -- stage_insert 의 staging(TEMP) 생성 DDL
    insert_sql   TEXT,          -- stage_insert 의 INSERT 문(staging→target)
    rows_read     BIGINT,       -- Impala 에서 읽어들인 행수(=조회 건수, STREAM_COPY 종료 시 확정)
    read_wait_ms  BIGINT,       -- STREAM_COPY 중 Impala 읽기(fetch) 누적 대기(ms)
    write_wait_ms BIGINT,       -- STREAM_COPY 중 Greenplum 쓰기(write_row 인코딩+송신) 누적 대기(ms)
    finalize_wait_ms BIGINT,    -- COPY 종료(PQputCopyEnd)+서버 ingest 완료 대기(ms). 크면 서버 병목
    impala_done_at TIMESTAMP,   -- Impala 조회 완료 시각(STREAM_COPY 종료)
    phases        JSONB         -- 세부 단계 타임라인(각 단계 시작/종료/소요/행수/지표)
);

-- 구버전 테이블 보강(앱도 자동 수행). 이미 있으면 무시된다.
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS started_at  TIMESTAMP;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS sub_query   TEXT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS exec_mode   TEXT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS staging_ddl TEXT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS insert_sql  TEXT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS rows_read        BIGINT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS read_wait_ms     BIGINT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS write_wait_ms    BIGINT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS finalize_wait_ms BIGINT;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS impala_done_at   TIMESTAMP;
ALTER TABLE public.task_history ADD COLUMN IF NOT EXISTS phases           JSONB;

CREATE INDEX IF NOT EXISTS idx_task_history_job_id ON public.task_history (job_id);
CREATE INDEX IF NOT EXISTS idx_task_history_task_id ON public.task_history (task_id);
CREATE INDEX IF NOT EXISTS idx_task_history_recorded_at ON public.task_history (recorded_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 4) executor_status — executor self-report 상태 (executor.self_report=true)
--    각 executor 가 자기 CPU/메모리/디스크/heartbeat 를 주기적으로 upsert 하고,
--    coordinator 는 이 테이블을 읽어 /executors·/cluster 를 구성한다(중복 폴링 제거).
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.executor_status (
    executor_id     TEXT PRIMARY KEY,
    executor_url    TEXT,          -- executor.advertise_url (HA에서 coordinator가 URL 키로 부하 뷰 구성)
    cpu_percent     DOUBLE PRECISION,
    memory_percent  DOUBLE PRECISION,
    memory_used_mb  DOUBLE PRECISION,
    memory_total_mb DOUBLE PRECISION,
    disk_percent         DOUBLE PRECISION,
    disk_used_gb         DOUBLE PRECISION,
    disk_total_gb        DOUBLE PRECISION,
    active_tasks         INTEGER,
    max_concurrent_tasks INTEGER,
    updated_at           TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul')
);
-- 구버전 보강(executor_url 추가). 이미 있으면 무시된다.
ALTER TABLE public.executor_status ADD COLUMN IF NOT EXISTS executor_url TEXT;


-- ─────────────────────────────────────────────────────────────────────────
-- 4b) executor_reservation — (Phase 3) 공유 예약: coordinator 가 dispatch 중인 task 를
--     executor 별로 예약해, 여러 coordinator 가 실시간 전역 부하(active_tasks + 예약)를
--     공유하게 한다. coordinator.executor_reservation=true 일 때만 사용. updated_at 으로
--     TTL(누수 방지: 죽은 coordinator 의 예약은 만료시켜 무시).
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.executor_reservation (
    executor_url   TEXT NOT NULL,
    coordinator_id TEXT NOT NULL,
    n              INTEGER NOT NULL DEFAULT 0,
    updated_at     TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul'),
    PRIMARY KEY (executor_url, coordinator_id)
);
CREATE INDEX IF NOT EXISTS idx_executor_reservation_updated_at
    ON public.executor_reservation (updated_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 4c) coordinator_status — (Phase 3) coordinator heartbeat. 죽은 coordinator 가 소유한
--     비종료 job 을 다른 coordinator 가 정합(FAILED)할 수 있도록, 각 coordinator 가 자기
--     생존을 주기적으로 upsert 한다. updated_at 신선도로 생존 판정.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.coordinator_status (
    coordinator_id TEXT PRIMARY KEY,
    updated_at     TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul')
);


-- ─────────────────────────────────────────────────────────────────────────
-- 5) executor_health_metrics — coordinator 가 기록하는 헬스/메트릭 (monitor.db_dsn)
--    monitor.record_interval_s 마다 executor 의 CPU·메모리·디스크 사용량을 append.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.executor_health_metrics (
    id              BIGSERIAL PRIMARY KEY,
    recorded_at     TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul'),
    executor_url    TEXT NOT NULL,
    healthy         BOOLEAN NOT NULL,
    cpu_percent     DOUBLE PRECISION,
    memory_percent  DOUBLE PRECISION,
    memory_used_mb  DOUBLE PRECISION,
    memory_total_mb DOUBLE PRECISION,
    disk_percent    DOUBLE PRECISION,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_executor_health_metrics_recorded_at
    ON public.executor_health_metrics (recorded_at);
CREATE INDEX IF NOT EXISTS idx_executor_health_metrics_url
    ON public.executor_health_metrics (executor_url);
