"""애플리케이션 설정.

설정은 설정 디렉터리(기본 /etc/query-executor)의 두 파일에서 로드된다:
1. config.properties - Java 스타일 key=value 변수 정의
2. config.yml - Spring Boot 스타일 ${variable:default} 을 사용하는 메인 YAML 설정

coordinator·executor 가 동일한 설정 파일을 공유하며, 각자 필요한 섹션만 사용한다.
(argus-catalog backend의 config 구조와 동일한 방식)
"""

import os
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
        # dispatcher/app 에서 사용하는 속성명과 호환되도록 별칭 유지
        self.executors: list[str] = _csv_list(_get("coordinator", "executors", ""))
        self.max_concurrent_jobs: int = int(
            _get("coordinator", "max_concurrent_jobs", 16)
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

        # ───────── Executor ─────────
        self.executor_host: str = _get("executor", "host", "0.0.0.0")
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
