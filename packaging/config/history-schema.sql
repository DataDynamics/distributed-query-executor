-- coordinator 가 Job 실행 이력을 기록하는 테이블.
-- history.db_dsn(미설정 시 monitor.db_dsn)으로 지정한 PostgreSQL 에 생성된다.
-- run() 안에서 상태 전이(시작=RUNNING, 종료=DONE/PARTIAL/FAILED)마다 한 행씩 append 된다.
-- 앱이 CREATE TABLE IF NOT EXISTS 로 자동 생성하지만, 사전 생성/권한 관리를 원할 때 사용한다.

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
