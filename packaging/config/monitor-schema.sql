-- coordinator 가 executor 헬스/메트릭(CPU·메모리·디스크)을 기록하는 테이블.
-- monitor.db_dsn 으로 지정한 PostgreSQL 에 생성된다(앱이 IF NOT EXISTS 로 자동 생성하지만,
-- 사전 생성/권한 관리를 원할 때 이 스크립트를 사용한다).

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
