"""애플리케이션 전역 설정 모듈.

이 모듈은 분산 쿼리 실행기(coordinator·executor)가 공유하는 모든 설정값을
한곳에 모아 ``Settings`` 객체로 노출한다. 설정은 설정 디렉터리
(기본 /etc/query-executor, 환경변수 QUERY_EXECUTOR_CONFIG_DIR 로 변경 가능)의
두 파일에서 로드된다:

1. config.properties - Java 스타일 key=value 변수 정의. config.yml 안의
   ``${변수}`` 자리표시자를 채우는 값들을 담는다.
2. config.yml - Spring Boot 스타일 ``${variable:default}`` 자리표시자를
   사용하는 메인 YAML 설정. 실제 섹션/계층 구조가 여기에 정의된다.

설계 의도:
  - coordinator 와 executor 는 역할이 다르지만 **동일한 설정 파일을 공유**하고,
    각자 자신에게 필요한 섹션(coordinator / executor / monitor / ...)만 읽는다.
    배포 시 설정 파일을 하나만 관리하면 되도록 하기 위함이다.
  - 일부 핵심 값은 **환경변수로 override** 할 수 있게 해 두었는데(예:
    COORDINATOR_EXECUTOR_MODE, STORE_BACKEND), 파일을 고치지 않고도 로컬 검증이나
    임시 토글이 가능하도록 하기 위함이다.
  - 모듈 하단에서 ``settings`` 싱글턴을 즉시 생성하므로, 다른 모듈은
    ``from core.config import settings`` 만으로 설정에 접근한다.

(argus-catalog backend의 config 구조와 동일한 방식)
"""

# Python 3.9 호환: PEP 604 (``X | None``) 유니언을 함수 시그니처에서 쓰므로,
# 어노테이션 평가를 지연(문자열화)시켜 3.9 에서도 런타임 오류 없이 동작하게 한다.
from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

from core.config_loader import load_config

# 설정 디렉터리는 환경변수로 우선 결정하고, 없으면 운영 기본 경로를 쓴다.
# 모듈 로드 시점에 한 번 확정되며, init_settings() 로 재로딩해도 디렉터리는 유지된다.
_CONFIG_DIR = Path(os.environ.get("QUERY_EXECUTOR_CONFIG_DIR", "/etc/query-executor"))
_yaml_path: Path = _CONFIG_DIR / "config.yml"
_properties_path: Path = _CONFIG_DIR / "config.properties"
# _raw 는 properties 치환까지 끝난 "원시 설정 dict"(YAML 계층 구조 그대로).
# Settings 가 이 dict 를 _get/_get_nested 로 읽어 타입이 정해진 속성으로 변환한다.
_raw: dict = load_config(config_dir=_CONFIG_DIR)


def _get(section: str, key: str, default=None):
    """최상위 ``section`` 아래의 ``key`` 값을 읽는다.

    섹션이나 키가 없으면 ``default`` 를 돌려준다. 설정 파일이 비어 있거나
    특정 섹션이 통째로 빠져 있어도 KeyError 없이 동작하도록, 단계마다
    빈 dict 로 폴백한다.
    """
    return _raw.get(section, {}).get(key, default)


def _get_nested(section: str, subsection: str, key: str, default=None):
    """``section -> subsection -> key`` 3단계 중첩 값을 읽는다.

    중간 어느 단계가 없어도 빈 dict 폴백으로 안전하게 ``default`` 를 반환한다.
    (예: logging.rolling.type, executor.impala.host 처럼 계층이 깊은 값)
    """
    return _raw.get(section, {}).get(subsection, {}).get(key, default)


def _to_bool(value) -> bool:
    """다양한 표현의 값을 불리언으로 정규화한다.

    YAML 은 ``true``/``false`` 를 bool 로 파싱하지만, properties 치환을 거친 값은
    문자열("true", "1", "yes" 등)로 들어올 수 있다. 두 경우를 모두 받아들이기 위해
    문자열일 때는 대표적인 참 표현만 True 로 인정하고, 그 외 타입은 bool() 로 위임한다.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def _csv_list(value) -> list[str]:
    """쉼표 구분 문자열(또는 list)을 공백 제거된 문자열 리스트로 변환한다.

    설정에서 executor 목록 등을 ``a, b, c`` 같은 한 줄 문자열로도, YAML 리스트로도
    적을 수 있게 하기 위한 헬퍼다. 어느 형태로 들어오든 빈 항목은 버리고
    앞뒤 공백을 제거한 문자열 리스트로 통일한다.
    """
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return []


def _kv_dict(value) -> dict[str, str]:
    """``K=V,K2=V2`` 형태의 문자열(또는 dict)을 {K: V} dict 로 변환한다.

    Impala query option 처럼 "옵션=값" 쌍을 쉼표로 나열한 한 줄 문자열을 받아 dict 로
    만든다. 각 항목은 첫 ``=`` 기준으로 key/value 를 나눈다(값에 ``=`` 가 들어가도 안전).
    이미 dict 면 문자열 키/값으로 정규화한다. 비어 있으면 빈 dict 를 돌려준다.
    """
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, str):
        out: dict[str, str] = {}
        for item in value.split(","):
            item = item.strip()
            if not item or "=" not in item:
                continue
            k, v = item.split("=", 1)
            k = k.strip()
            if k:
                out[k] = v.strip()
        return out
    return {}


class Settings:
    """config.yml + config.properties 에서 로드한 전역 애플리케이션 설정 컨테이너.

    생성자에서 ``_raw`` dict 의 각 값을 읽어 **타입이 확정된 인스턴스 속성**으로
    펼쳐 둔다. 이렇게 미리 변환해 두면 사용처에서는 dict 탐색이나 타입 캐스팅
    없이 ``settings.coordinator_port`` 처럼 바로 쓸 수 있다.

    __init__ 이 모든 속성 할당을 담당하므로, init_settings() 는 재로딩 후
    같은 인스턴스에 대해 __init__ 을 다시 호출하는 것만으로 설정을 갱신한다.
    섹션은 주석 구분선으로 app / logging / coordinator / monitor / history /
    executor 단위로 묶여 있다.
    """

    def __init__(self) -> None:
        # ───────── App (공통 기본) ─────────
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
        # WARNING 이상만 따로 모으는 별도 로그(운영 시 문제만 빠르게 추적). INFO 로그와 분리.
        self.log_warn_enabled: bool = _to_bool(
            _get_nested("logging", "warn", "enabled", True)
        )
        self.log_warn_level: str = _get_nested("logging", "warn", "level", "WARNING")
        # 메인 로그 파일명 stem 뒤에 붙는 접미사: foo.log → foo-warn.log
        self.log_warn_suffix: str = _get_nested("logging", "warn", "suffix", "-warn")

        self.config_dir: Path = _CONFIG_DIR
        self.config_yaml_path: Path = _yaml_path
        self.config_properties_path: Path = _properties_path

        # ───────── Coordinator ─────────
        self.coordinator_host: str = _get("coordinator", "host", "0.0.0.0")
        self.coordinator_port: int = int(_get("coordinator", "port", 8088))
        # executor 실행 방식: remote(HTTP 디스패치) | local(in-process 직접 실행)
        # 환경변수 COORDINATOR_EXECUTOR_MODE 로 즉시 토글 가능(로컬 검증용)
        self.executor_mode: str = (
            os.getenv("COORDINATOR_EXECUTOR_MODE")
            or _get("coordinator", "executor_mode", "remote")
        ).lower()
        # 멀티 coordinator 식별자(로그/공유 store 소유 표기). 미지정 시 host:port 기반.
        # 우선순위: 환경변수 COORDINATOR_ID > 설정 파일 값 > "호스트명:포트" 자동 생성.
        # 여러 coordinator 가 같은 공유 store 를 쓸 때 어느 인스턴스가 소유/처리 중인지
        # 구분하기 위한 값이라 충돌 없이 유일해야 한다. (or 연쇄로 빈 문자열도 폴백됨)
        self.coordinator_id: str = (
            os.getenv("COORDINATOR_ID")
            or _get("coordinator", "id", "")
            or f"{socket.gethostname()}:{self.coordinator_port}"
        )

        # ───────── 공유 상태 저장소(멀티 coordinator) ─────────
        # store.backend: memory(기본, 단일 프로세스) | postgres(여러 coordinator 공유).
        # postgres 일 때 접속 DSN 은 별도로 두지 않고 history.db_dsn 을 재사용한다
        # (job 이력과 같은 DB 에 상태를 두는 게 운영상 단순하기 때문).
        # 환경변수 STORE_BACKEND 로 파일 수정 없이 백엔드를 바꿀 수 있다.
        self.store_backend: str = (
            os.getenv("STORE_BACKEND") or _get("store", "backend", "memory")
        ).lower()
        self.store_table: str = _get("store", "table", "jobs")
        # 모니터링 대시보드(읽기 전용 웹 UI) 노출 여부.
        # 비밀값은 마스킹되어 표시되지만, 운영 정보가 외부에 드러날 수 있으므로
        # 외부 노출 환경에서는 비활성화를 고려한다.
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
        # executor 접속(connect) 전용 타임아웃(초). task_timeout_s(전체 read 타임아웃)와
        # 분리해, executor 가 죽어 응답이 없을 때 1시간씩 매달리지 않고 빠르게 실패하도록 한다.
        self.task_connect_timeout_s: float = float(
            _get("coordinator", "task_connect_timeout_s", 5.0)
        )
        # 연결 계열 실패 시 같은 executor 에 재시도하는 횟수(지수 백오프). 0 이면 재시도 없음.
        self.task_max_retries: int = int(
            _get("coordinator", "task_max_retries", 2)
        )
        # 재시도 지수 백오프의 기준 시간(초): 대기 = backoff * 2**시도횟수.
        self.task_retry_backoff_s: float = float(
            _get("coordinator", "task_retry_backoff_s", 0.5)
        )
        # 재시도를 모두 소진해도 연결 실패면, 다른 살아있는 executor 로 재배정(failover)할지 여부.
        self.task_failover: bool = _to_bool(
            _get("coordinator", "task_failover", True)
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
        # executor self-report 시 자기 상태를 기록할 테이블과 기록 주기(초).
        self.executor_status_table: str = _get("executor", "status_table", "executor_status")
        self.executor_status_interval_s: float = float(
            _get("executor", "status_interval_s", 10)
        )
        # executor 자체 동시 task 상한(admission control). 0 이면 무제한.
        self.executor_max_concurrent_tasks: int = int(
            _get("executor", "max_concurrent_tasks", 8)
        )
        # Impala (source) — 데이터를 읽어오는 원본. TLS + Kerberos(GSSAPI) 환경 기준 기본값.
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
        # Impala 쿼리 옵션(전역 기본값). "MEM_LIMIT=2g,REQUEST_POOL=etl" 형태 → dict.
        # 비어 있으면 {} 이고, 이 경우 impyla 에 configuration 을 넘기지 않고 그대로 실행한다.
        # 요청별 impala_query_options 가 있으면 이 전역값 위에 덮어쓴다.
        self.impala_query_options: dict[str, str] = _kv_dict(
            _get_nested("executor", "impala", "query_options", "")
        )
        # Greenplum (target) — 읽어온 데이터를 적재할 대상 DB 접속 DSN.
        self.greenplum_dsn: str = _get_nested("executor", "greenplum", "dsn", "")
        # source→target 복사 시 한 번에 처리할 행 수. 메모리 사용량과 처리량의 균형값.
        self.copy_batch_size: int = int(
            _get_nested("executor", "copy", "batch_size", 10000)
        )


def init_settings(
    yaml_path: str | None = None,
    properties_path: str | None = None,
) -> None:
    """사용자 지정 설정 파일 경로로 설정을 재초기화한다(테스트/임베디드 용).

    인자:
        yaml_path: 사용할 config.yml 경로. None 이면 기존 경로 유지.
        properties_path: 사용할 config.properties 경로. None 이면 기존 경로 유지.

    반환:
        없음. 모듈 전역 ``_raw`` 와 경로 변수, 그리고 싱글턴 ``settings`` 를
        제자리에서 갱신한다.

    동작:
        모듈 전역 상태를 직접 바꾸므로 global 선언이 필요하다. 새 _raw 를 로드한 뒤
        ``settings.__init__()`` 을 다시 호출해 **같은 settings 객체를 그대로 두고
        속성만 새 값으로 덮어쓴다.** 이렇게 하면 이미 ``settings`` 를 import 해 참조 중인
        다른 모듈들이 재초기화 이후에도 갱신된 값을 보게 된다(객체 교체 시 생길
        오래된 참조 문제를 피하기 위함).
    """
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


# 모듈 import 시점에 만들어지는 전역 싱글턴. 애플리케이션 전역에서
# `from core.config import settings` 로 공유한다.
settings = Settings()
