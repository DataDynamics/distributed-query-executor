"""애플리케이션 설정.

설정은 설정 디렉터리(기본 /etc/query-executor)의 두 파일에서 로드된다:
1. config.properties - Java 스타일 key=value 변수 정의
2. config.yml - Spring Boot 스타일 ${variable:default} 을 사용하는 메인 YAML 설정

coordinator·executor 가 동일한 설정 파일을 공유하며, 각자 필요한 섹션만 사용한다.
(argus-catalog backend의 config 구조와 동일한 방식)
"""

import os
import socket
import uuid
from pathlib import Path

from core.config_loader import load_config

_CONFIG_DIR = Path(os.environ.get("QUERY_EXECUTOR_CONFIG_DIR", "/etc/query-executor"))
_yaml_path: Path = _CONFIG_DIR / "config.yml"
_properties_path: Path = _CONFIG_DIR / "config.properties"
_raw: dict = load_config(config_dir=_CONFIG_DIR)


def _get(section: str, key: str, default=None):
    return _raw.get(section, {}).get(key, default)


def _get_nested(section: str, subsection: str, key: str, default=None):
    return _raw.get(section, {}).get(subsection, {}).get(key, default)


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def _csv_list(value) -> list[str]:
    """쉼표 구분 문자열(또는 list)을 공백 제거된 문자열 리스트로 변환한다."""
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return []


class Settings:
    """config.yml + config.properties 에서 로드한 전역 애플리케이션 설정."""

    def __init__(self) -> None:
        self.app_name: str = _get("app", "name", "distributed-query-executor")
        self.debug: bool = _to_bool(_get("app", "debug", False))

        # 쿼리 파싱 기본 방언(요청에서 sql_dialect 로 재정의 가능)
        self.query_default_dialect: str = _get("query", "sql_dialect", "hive")

        # ───────── 로깅 (공통) ─────────
        self.log_level: str = _get("logging", "level", "INFO")
        self.log_dir: Path = Path(_get("logging", "dir", "logs"))
        self.log_rolling_type: str = _get_nested("logging", "rolling", "type", "daily")
        self.log_rolling_backup_count: int = int(
            _get_nested("logging", "rolling", "backup_count", 30)
        )
        self.coordinator_log_filename: str = _get_nested(
            "logging", "filename", "coordinator", "query-coordinator-server.log"
        )
        self.executor_log_filename: str = _get_nested(
            "logging", "filename", "executor", "query-executor-server.log"
        )

        self.config_dir: Path = _CONFIG_DIR
        self.config_yaml_path: Path = _yaml_path
        self.config_properties_path: Path = _properties_path

        # ───────── Coordinator ─────────
        self.coordinator_host: str = _get("coordinator", "host", "0.0.0.0")
        self.coordinator_port: int = int(_get("coordinator", "port", 8000))
        # executor 실행 방식: remote(HTTP 디스패치) | local(in-process 직접 실행)
        # 환경변수 COORDINATOR_EXECUTOR_MODE 로 즉시 토글 가능(로컬 검증용)
        self.executor_mode: str = (
            os.getenv("COORDINATOR_EXECUTOR_MODE")
            or _get("coordinator", "executor_mode", "remote")
        ).lower()
        # 멀티 coordinator 식별자(로그/공유 store 소유 표기). 미지정 시 host:port 기반.
        self.coordinator_id: str = (
            os.getenv("COORDINATOR_ID")
            or _get("coordinator", "id", "")
            or f"{socket.gethostname()}:{self.coordinator_port}"
        )

        # ───────── 공유 상태 저장소(멀티 coordinator) ─────────
        # store.backend: memory(기본, 단일) | postgres(공유). DSN 은 history.db_dsn 재사용.
        self.store_backend: str = (
            os.getenv("STORE_BACKEND") or _get("store", "backend", "memory")
        ).lower()
        self.store_table: str = _get("store", "table", "jobs")
        # / 모니터링 대시보드(읽기 전용). 비밀값은 마스킹되지만 운영 정보 노출에 주의.
        self.dashboard_enabled: bool = _to_bool(_get("dashboard", "enabled", True))
        # dispatcher/app 에서 사용하는 속성명과 호환되도록 별칭 유지
        self.executors: list[str] = _csv_list(_get("coordinator", "executors", ""))
        self.max_concurrent_jobs: int = int(
            _get("coordinator", "max_concurrent_jobs", 16)
        )
        # 실행 슬롯(max_concurrent_jobs)이 다 찼을 때 PENDING 으로 대기 가능한 job 수.
        # 실행+대기 합을 넘는 요청은 429 로 거부. 0 이하이면 무제한.
        self.max_pending_jobs: int = int(
            _get("coordinator", "max_pending_jobs", 100)
        )
        self.max_dispatch_concurrency: int = int(
            _get("coordinator", "max_dispatch_concurrency", 32)
        )
        self.poll_interval_s: float = float(
            _get("coordinator", "poll_interval_s", 1.0)
        )
        self.task_timeout_s: float = float(
            _get("coordinator", "task_timeout_s", 3600)
        )

        # ───────── Coordinator - executor 헬스 모니터링 & 메트릭 기록 ─────────
        self.monitor_enabled: bool = _to_bool(_get("monitor", "enabled", True))
        # executor /health·/metrics 폴링 간격(초)
        self.monitor_health_interval_s: float = float(
            _get("monitor", "health_interval_s", 10)
        )
        # PostgreSQL 기록 간격(초)
        self.monitor_record_interval_s: float = float(
            _get("monitor", "record_interval_s", 60)
        )
        # 기록 대상 PostgreSQL DSN(비어 있으면 DB 기록 비활성, 폴링만 수행)
        self.monitor_db_dsn: str = _get("monitor", "db_dsn", "")
        self.monitor_table: str = _get("monitor", "table", "executor_health_metrics")
        # /metrics 에서 사용량을 측정할 디스크 경로
        self.monitor_disk_path: str = _get("monitor", "disk_path", "/")

        # ───────── Coordinator - Job 실행 이력(PostgreSQL) ─────────
        # 비어 있으면 monitor.db_dsn 을 재사용. 둘 다 없으면 이력 기록 비활성.
        self.history_db_dsn: str = _get("history", "db_dsn", "") or self.monitor_db_dsn
        self.history_table: str = _get("history", "table", "job_history")
        # executor 가 기록하는 task 단위 이력 테이블(history_db_dsn 공유)
        self.task_history_table: str = _get("history", "task_table", "task_history")

        # ───────── Executor ─────────
        self.executor_host: str = _get("executor", "host", "0.0.0.0")
        # executor self-report(멀티 coordinator): executor가 자기 상태를 공유 DB에 기록
        self.executor_self_report: bool = _to_bool(
            os.getenv("EXECUTOR_SELF_REPORT") or _get("executor", "self_report", False)
        )
        self.executor_status_table: str = _get("executor", "status_table", "executor_status")
        self.executor_status_interval_s: float = float(
            _get("executor", "status_interval_s", 10)
        )
        # executor 자체 동시 task 상한(admission control). 0 이면 무제한.
        self.executor_max_concurrent_tasks: int = int(
            _get("executor", "max_concurrent_tasks", 8)
        )
        # Impala (source) — TLS + Kerberos(GSSAPI) 환경
        self.impala_host: str = _get_nested("executor", "impala", "host", "")
        self.impala_port: int = int(_get_nested("executor", "impala", "port", 21050))
        self.impala_database: str = _get_nested("executor", "impala", "database", "default")
        self.impala_auth_mechanism: str = _get_nested(
            "executor", "impala", "auth_mechanism", "GSSAPI"
        )
        self.impala_kerberos_service_name: str = _get_nested(
            "executor", "impala", "kerberos_service_name", "impala"
        )
        self.impala_use_ssl: bool = _to_bool(
            _get_nested("executor", "impala", "use_ssl", True)
        )
        self.impala_ca_cert: str = _get_nested("executor", "impala", "ca_cert", "")
        # GSSAPI 에서는 사용하지 않음(LDAP/PLAIN 인증일 때만)
        self.impala_user: str = _get_nested("executor", "impala", "user", "")
        self.impala_password: str = _get_nested("executor", "impala", "password", "")
        # Greenplum (target)
        self.greenplum_dsn: str = _get_nested("executor", "greenplum", "dsn", "")
        self.copy_batch_size: int = int(
            _get_nested("executor", "copy", "batch_size", 10000)
        )


def init_settings(
    yaml_path: str | None = None,
    properties_path: str | None = None,
) -> None:
    """사용자 지정 설정 파일 경로로 설정을 재초기화한다(테스트/임베디드 용)."""
    global _raw, _yaml_path, _properties_path
    if yaml_path:
        _yaml_path = Path(yaml_path)
    if properties_path:
        _properties_path = Path(properties_path)
    _raw = load_config(
        config_dir=_CONFIG_DIR,
        yaml_path=yaml_path,
        properties_path=properties_path,
    )
    settings.__init__()


settings = Settings()
