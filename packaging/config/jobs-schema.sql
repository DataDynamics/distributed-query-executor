-- 멀티 coordinator 공유 Job 저장소(store.backend=postgres).
-- 어느 coordinator로 요청이 가도 조회/취소가 가능하도록 Job 스냅샷을 영속한다.
-- 앱이 CREATE TABLE IF NOT EXISTS 로 자동 생성하지만, 사전 생성/권한 관리 시 사용한다.

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
