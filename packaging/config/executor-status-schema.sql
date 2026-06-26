-- executor self-report 상태 테이블(executor.self_report=true).
-- 각 executor 가 자기 CPU/메모리/디스크/heartbeat 를 주기적으로 upsert 하고,
-- coordinator 는 이 테이블을 읽어 /executors, /cluster 를 구성한다(중복 폴링 제거).
-- 앱이 CREATE TABLE IF NOT EXISTS 로 자동 생성하지만, 사전 생성/권한 관리 시 사용한다.

CREATE TABLE IF NOT EXISTS executor_status (
    executor_id     TEXT PRIMARY KEY,
    cpu_percent     DOUBLE PRECISION,
    memory_percent  DOUBLE PRECISION,
    memory_used_mb  DOUBLE PRECISION,
    memory_total_mb DOUBLE PRECISION,
    disk_percent    DOUBLE PRECISION,
    disk_used_gb    DOUBLE PRECISION,
    disk_total_gb   DOUBLE PRECISION,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
