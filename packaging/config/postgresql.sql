-- ============================================================================
-- Distributed Query Executor — PostgreSQL 통합 스키마
-- ============================================================================
-- 앱은 각 테이블을 CREATE TABLE IF NOT EXISTS 로 자동 생성하지만, 사전 생성이나
-- 권한 관리를 원할 때 이 스크립트 하나로 전체 스키마를 만들 수 있다.
--
-- 적용:  psql "postgresql://user:pass@pg-host:5432/queryexec" -f postgresql.sql
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
CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    coordinator_id   TEXT,
    status           TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    data             JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs (updated_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 2) job_history — coordinator 의 job 단위 실행 이력 (history.db_dsn)
--    run() 안에서 상태 전이(시작=RUNNING, 종료=DONE/PARTIAL/FAILED)마다 append.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_history (
    id                 BIGSERIAL PRIMARY KEY,
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    created_at         TIMESTAMPTZ,
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    original_sql       TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_history_job_id ON job_history (job_id);
CREATE INDEX IF NOT EXISTS idx_job_history_recorded_at ON job_history (recorded_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 3) task_history — 각 executor 의 task 단위 실행 이력 (history.db_dsn)
--    상태 전이(QUEUED/READING/WRITING/DONE/FAILED)마다 append. 하나의 job_id 아래
--    N개 task 가 executor_id 별로 기록된다.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS task_history (
    id           BIGSERIAL PRIMARY KEY,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    job_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    username     TEXT,
    executor_id  TEXT,
    status       TEXT NOT NULL,
    rows_written BIGINT,
    error        TEXT,
    started_at   TIMESTAMPTZ,   -- task 가 READING 을 시작한 시각
    finished_at  TIMESTAMPTZ,   -- task 가 종료(DONE/FAILED/CANCELLED)된 시각
    sub_query    TEXT,          -- 이 task 가 실행한 SELECT sub-query 전문
    exec_mode    TEXT,          -- 적재 방식: copy | statement | stage_insert
    staging_ddl  TEXT,          -- stage_insert 의 staging(TEMP) 생성 DDL
    insert_sql   TEXT           -- stage_insert 의 INSERT 문(staging→target)
);

-- 구버전 테이블 보강(앱도 자동 수행). 이미 있으면 무시된다.
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS started_at  TIMESTAMPTZ;
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS sub_query   TEXT;
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS exec_mode   TEXT;
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS staging_ddl TEXT;
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS insert_sql  TEXT;

CREATE INDEX IF NOT EXISTS idx_task_history_job_id ON task_history (job_id);
CREATE INDEX IF NOT EXISTS idx_task_history_task_id ON task_history (task_id);
CREATE INDEX IF NOT EXISTS idx_task_history_recorded_at ON task_history (recorded_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 4) executor_status — executor self-report 상태 (executor.self_report=true)
--    각 executor 가 자기 CPU/메모리/디스크/heartbeat 를 주기적으로 upsert 하고,
--    coordinator 는 이 테이블을 읽어 /executors·/cluster 를 구성한다(중복 폴링 제거).
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS executor_status (
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
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 구버전 보강(executor_url 추가). 이미 있으면 무시된다.
ALTER TABLE executor_status ADD COLUMN IF NOT EXISTS executor_url TEXT;


-- ─────────────────────────────────────────────────────────────────────────
-- 4b) executor_reservation — (Phase 3) 공유 예약: coordinator 가 dispatch 중인 task 를
--     executor 별로 예약해, 여러 coordinator 가 실시간 전역 부하(active_tasks + 예약)를
--     공유하게 한다. coordinator.executor_reservation=true 일 때만 사용. updated_at 으로
--     TTL(누수 방지: 죽은 coordinator 의 예약은 만료시켜 무시).
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS executor_reservation (
    executor_url   TEXT NOT NULL,
    coordinator_id TEXT NOT NULL,
    n              INTEGER NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (executor_url, coordinator_id)
);
CREATE INDEX IF NOT EXISTS idx_executor_reservation_updated_at
    ON executor_reservation (updated_at);


-- ─────────────────────────────────────────────────────────────────────────
-- 4c) coordinator_status — (Phase 3) coordinator heartbeat. 죽은 coordinator 가 소유한
--     비종료 job 을 다른 coordinator 가 정합(FAILED)할 수 있도록, 각 coordinator 가 자기
--     생존을 주기적으로 upsert 한다. updated_at 신선도로 생존 판정.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS coordinator_status (
    coordinator_id TEXT PRIMARY KEY,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ─────────────────────────────────────────────────────────────────────────
-- 5) executor_health_metrics — coordinator 가 기록하는 헬스/메트릭 (monitor.db_dsn)
--    monitor.record_interval_s 마다 executor 의 CPU·메모리·디스크 사용량을 append.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS executor_health_metrics (
    id              BIGSERIAL PRIMARY KEY,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    ON executor_health_metrics (recorded_at);
CREATE INDEX IF NOT EXISTS idx_executor_health_metrics_url
    ON executor_health_metrics (executor_url);
