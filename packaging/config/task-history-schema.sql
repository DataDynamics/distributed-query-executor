-- 각 executor 가 자신이 처리하는 task(=executor job) 실행 이력을 기록하는 테이블.
-- history.db_dsn(미설정 시 monitor.db_dsn)으로 지정한 PostgreSQL 에 생성된다.
-- 상태 전이(QUEUED/READING/WRITING/DONE/FAILED)마다 한 행씩 append 된다.
-- 하나의 job_id 아래 N개 task 가 executor_id 별로 기록된다.
-- 앱이 CREATE TABLE IF NOT EXISTS 로 자동 생성하지만, 사전 생성/권한 관리를 원할 때 사용한다.

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
    sub_query    TEXT           -- 이 task 가 실행한 sub-query 전문
);

-- 구버전 테이블 보강(앱도 자동 수행). 이미 있으면 무시된다.
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS started_at  TIMESTAMPTZ;
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;
ALTER TABLE task_history ADD COLUMN IF NOT EXISTS sub_query   TEXT;

CREATE INDEX IF NOT EXISTS idx_task_history_job_id ON task_history (job_id);
CREATE INDEX IF NOT EXISTS idx_task_history_task_id ON task_history (task_id);
CREATE INDEX IF NOT EXISTS idx_task_history_recorded_at ON task_history (recorded_at);
