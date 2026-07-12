"""애플리케이션 전역 설정 모듈.

이 모듈은 분산 쿼리 실행기(coordinator·executor)가 공유하는 모든 설정값을
한곳에 모아 ``Settings`` 객체로 노출한다. 설정은 설정 디렉터리
(기본 /data1/query-executor/config, 환경변수 QUERY_EXECUTOR_CONFIG_DIR 로 변경 가능)의
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
import random
import uuid
from pathlib import Path

from core.config_loader import load_config, load_properties

# 설정 디렉터리는 환경변수로 우선 결정하고, 없으면 운영 기본 경로를 쓴다.
# 모듈 로드 시점에 한 번 확정되며, init_settings() 로 재로딩해도 디렉터리는 유지된다.
_CONFIG_DIR = Path(os.environ.get("QUERY_EXECUTOR_CONFIG_DIR", "/data1/query-executor/config"))
_yaml_path: Path = _CONFIG_DIR / "config.yml"
_properties_path: Path = _CONFIG_DIR / "config.properties"
# _raw 는 properties 치환까지 끝난 "원시 설정 dict"(YAML 계층 구조 그대로).
# Settings 가 이 dict 를 _get/_get_nested 로 읽어 타입이 정해진 속성으로 변환한다.
_raw: dict = load_config(config_dir=_CONFIG_DIR)
# _props 는 치환 전 raw properties(key=value flat). YAML 구조에 없는 **자유 정의 설정**을
# 프리픽스로 수집하는 데 쓴다(예: query.func.config.* → 커스텀 실행 함수에 넘길 dict).
_props: dict = load_properties(_properties_path)


def _collect_prefix(prefix: str) -> dict:
    """raw properties 에서 ``prefix`` 로 시작하는 키를 모아 접두어를 뗀 dict 로 반환한다.

    YAML 스키마를 손대지 않고 config.properties 한 줄만 추가해 임의 설정을 넘길 수 있게 한다.
    값은 모두 문자열이다(형변환은 소비하는 쪽 책임).
    """
    return {k[len(prefix):]: v for k, v in _props.items() if k.startswith(prefix)}


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


def _qualify_table(schema: str, table: str) -> str:
    """테이블명에 메타 저장소 스키마를 붙인다(``public.jobs`` 형태).

    앱 런타임 SQL(각 repo 의 ``self.table`` f-string)과 DDL 파일이 같은 스키마를 쓰도록,
    설정에서 읽은 기본 테이블명을 ``schema`` 로 한정한다. 단:
    - ``schema`` 가 비어 있으면 한정하지 않는다(검색 경로/기본 스키마에 위임).
    - 설정값이 이미 ``myschema.t`` 처럼 ``.`` 로 한정돼 있으면 중복 한정하지 않는다.
    """
    if not schema or "." in table:
        return table
    return f"{schema}.{table}"


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

        # ───────── 쿼리 템플릿 엔진 ─────────
        # SELECT/STAGING DDL/INSERT 를 서버 템플릿 파일로 런타임 생성(요청은 파라미터만 전달).
        self.template_enabled: bool = _to_bool(_get("template", "enabled", True))
        # 템플릿 루트 디렉터리(하위 <template_id>/manifest.yml + *.sql.j2). 개발 시
        # packaging/config/templates 를 QUERY_EXECUTOR_CONFIG_DIR 로 가리키면 그 아래를 쓴다.
        self.template_dir: str = _get(
            "template", "dir", str(_CONFIG_DIR / "templates")
        )
        # 파일 변경 자동 리로드(개발 편의). 운영에선 false 로 stat 비용 제거.
        self.template_auto_reload: bool = _to_bool(_get("template", "auto_reload", False))
        # 커스텀 함수 모듈(쉼표 구분 import 경로). 엔진 기동 시 import 되어 필터/글로벌 등록.
        self.template_func_modules: list[str] = _csv_list(
            _get("template", "func_modules", "")
        )
        # 렌더된 DDL/INSERT 조각을 단일 SQL 문으로 강제(다중 문 인젝션 방지).
        self.template_validate_ddl_single_stmt: bool = _to_bool(
            _get("template", "validate_ddl_single_stmt", True)
        )

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
        # 멀티 coordinator 식별자(로그/공유 store 소유 표기). 미지정 시 랜덤 생성.
        # 우선순위: 환경변수 COORDINATOR_ID > 설정 파일 값 > "coordinator-<랜덤숫자>" 자동 생성.
        # 여러 coordinator 가 같은 공유 store 를 쓸 때 어느 인스턴스가 소유/처리 중인지
        # 구분하기 위한 값이라 충돌 없이 유일해야 한다. (or 연쇄로 빈 문자열도 폴백됨)
        self.coordinator_id: str = (
            os.getenv("COORDINATOR_ID")
            or _get("coordinator", "id", "")
            or f"coordinator-{random.randint(100000, 999999)}"
        )

        # ───────── 공유 상태 저장소(멀티 coordinator) ─────────
        # store.backend: memory(기본, 단일 프로세스) | postgres(여러 coordinator 공유).
        # postgres 일 때 접속 DSN 은 별도로 두지 않고 history.db_dsn 을 재사용한다
        # (job 이력과 같은 DB 에 상태를 두는 게 운영상 단순하기 때문).
        # 환경변수 STORE_BACKEND 로 파일 수정 없이 백엔드를 바꿀 수 있다.
        self.store_backend: str = (
            os.getenv("STORE_BACKEND") or _get("store", "backend", "memory")
        ).lower()
        # 메타 저장소 스키마(jobs/*_history/executor_*/coordinator_status 공통). 기본 public.
        # 아래 모든 메타 테이블명을 이 스키마로 한정하므로, 앱 런타임 SQL 과 DDL(postgresql.sql/
        # warehousepg.sql)이 같은 스키마를 가리킨다. search_path 가 비표준이어도 안전하다.
        self.db_schema: str = _get("db", "schema", "public")
        self.store_table: str = _qualify_table(self.db_schema, _get("store", "table", "jobs"))
        # coordinator HA 보조 테이블(설정 키는 없고 스키마만 한정). app.py 에서 repo 에 주입.
        self.coordinator_status_table: str = _qualify_table(self.db_schema, "coordinator_status")
        self.reservation_table: str = _qualify_table(self.db_schema, "executor_reservation")
        # store.backend=file 일 때 스냅샷 파일 경로(비우면 로그 디렉터리 옆 jobs-state.json).
        self.store_path: str = _get("store", "path", "")
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
        # executor 선택 정책(failover 순서): round_robin(기본, 현행) | least_loaded | p2c.
        # least_loaded/p2c 면 HealthMonitor 스냅샷(헬스+active_tasks)을 보고 살아있는·한가한
        # 노드를 먼저 시도한다. HA(다중 coordinator)에서는 분산 스탬피드를 피하는 p2c 권장.
        self.executor_select: str = (
            os.getenv("COORDINATOR_EXECUTOR_SELECT")
            or _get("coordinator", "executor_select", "round_robin")
        ).lower()
        # ── Phase 3: HA(다중 coordinator) 헬스 기반 선택 고도화 ──
        # 부하 뷰 소스: auto(멀티=self_report 공유테이블, 단일=monitor) | monitor | self_report.
        self.executor_health_source: str = _get(
            "coordinator", "executor_health_source", "auto"
        ).lower()
        # 공유 예약(엄격 균형): true 면 executor_reservation 으로 dispatch 중 task 를 예약해
        # 여러 coordinator 가 실시간 전역 부하를 공유한다(active_tasks + 예약). 누수는 TTL 로 방지.
        self.executor_reservation: bool = _to_bool(
            _get("coordinator", "executor_reservation", False)
        )
        self.reservation_ttl_s: float = float(_get("coordinator", "reservation_ttl_s", 60))
        # coordinator 자기 heartbeat 주기/만료. 죽은 coordinator 소유 job 정합(orphan)에 쓰인다.
        self.heartbeat_interval_s: float = float(_get("coordinator", "heartbeat_interval_s", 10))
        self.coordinator_stale_s: float = float(_get("coordinator", "coordinator_stale_s", 30))
        # 죽은 coordinator 소유의 비종료 job 을 FAILED 로 정합하는 주기(초). 0 이면 비활성.
        self.orphan_reconcile_interval_s: float = float(
            _get("coordinator", "orphan_reconcile_interval_s", 30)
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
        self.monitor_table: str = _qualify_table(self.db_schema, _get("monitor", "table", "executor_health_metrics"))
        # /metrics 에서 사용량을 측정할 디스크 경로
        self.monitor_disk_path: str = _get("monitor", "disk_path", "/")

        # ───────── Coordinator - Job 실행 이력(PostgreSQL) ─────────
        # 비어 있으면 monitor.db_dsn 을 재사용. 둘 다 없으면 이력 기록 비활성.
        self.history_db_dsn: str = _get("history", "db_dsn", "") or self.monitor_db_dsn
        self.history_table: str = _qualify_table(self.db_schema, _get("history", "table", "job_history"))
        # executor 가 기록하는 task 단위 이력 테이블(history_db_dsn 공유)
        self.task_history_table: str = _qualify_table(self.db_schema, _get("history", "task_table", "task_history"))

        # ───────── Executor ─────────
        self.executor_host: str = _get("executor", "host", "0.0.0.0")
        # executor self-report(멀티 coordinator): executor가 자기 상태를 공유 DB에 기록
        self.executor_self_report: bool = _to_bool(
            os.getenv("EXECUTOR_SELF_REPORT") or _get("executor", "self_report", False)
        )
        # executor 가 self-report 시 함께 기록하는 자기 base URL(coordinator 가 도달하는 주소).
        # HA 에서 coordinator 가 executor_status 를 URL 키로 읽어 부하 뷰를 구성하게 한다.
        # 미설정 시 self-report 는 URL 없이 기록되고, coordinator 는 monitor 폴링으로 폴백한다.
        # coordinator.executors 의 해당 URL 과 정확히 일치시켜야 매칭된다.
        self.executor_advertise_url: str = (
            os.getenv("EXECUTOR_ADVERTISE_URL")
            or _get("executor", "advertise_url", "")
        )
        # local_stage: 이 executor 가 올라가 있는 GP 세그먼트 호스트명. coordinator 가 file://
        # URI(file://<hostname>/...)를 조립할 때 쓰며, gp_segment_configuration.hostname 과
        # 정확히 일치해야 세그먼트가 로컬 파일을 찾는다. 미설정 시 OS hostname 을 쓴다.
        self.executor_gp_hostname: str = (
            os.getenv("EXECUTOR_GP_HOSTNAME") or _get("executor", "gp_hostname", "")
        )
        # executor self-report 시 자기 상태를 기록할 테이블과 기록 주기(초).
        self.executor_status_table: str = _qualify_table(self.db_schema, _get("executor", "status_table", "executor_status"))
        self.executor_status_interval_s: float = float(
            _get("executor", "status_interval_s", 10)
        )
        # executor 자체 동시 task 상한(admission control). 0 이면 무제한.
        self.executor_max_concurrent_tasks: int = int(
            _get("executor", "max_concurrent_tasks", 8)
        )
        # 종료(SIGTERM) 시 진행 중 task 를 강제 중단하지 않고 완료를 기다리는 최대 시간(초).
        self.executor_shutdown_drain_timeout_s: float = float(
            _get("executor", "shutdown_drain_timeout_s", 25)
        )
        # Impala (source) — 데이터를 읽어오는 원본. TLS + LDAP 인증 기준 기본값.
        self.impala_host: str = _get_nested("executor", "impala", "host", "")
        self.impala_port: int = int(_get_nested("executor", "impala", "port", 21050))
        self.impala_database: str = _get_nested("executor", "impala", "database", "default")
        self.impala_auth_mechanism: str = _get_nested(
            "executor", "impala", "auth_mechanism", "LDAP"
        )
        self.impala_use_ssl: bool = _to_bool(
            _get_nested("executor", "impala", "use_ssl", True)
        )
        self.impala_ca_cert: str = _get_nested("executor", "impala", "ca_cert", "")
        # LDAP/PLAIN 인증일 때 사용하는 사용자/비밀번호
        self.impala_user: str = _get_nested("executor", "impala", "user", "")
        self.impala_password: str = _get_nested("executor", "impala", "password", "")
        # Impala 쿼리 옵션(전역 기본값). "MEM_LIMIT=2g,REQUEST_POOL=etl" 형태 → dict.
        # 비어 있으면 {} 이고, 이 경우 impyla 에 configuration 을 넘기지 않고 그대로 실행한다.
        # 요청별 impala_query_options 가 있으면 이 전역값 위에 덮어쓴다.
        self.impala_query_options: dict[str, str] = _kv_dict(
            _get_nested("executor", "impala", "query_options", "")
        )
        # 소스 엔진 선택: impala(기본) | trino. executor 가 SELECT 를 실행할 소스 DB 종류.
        # copy/stage_insert/local_stage 의 읽기 쪽과 /datasources 미리보기가 이 값을 따른다.
        self.source_type: str = str(
            _get_nested("executor", "source", "type", "impala")
        ).strip().lower() or "impala"
        # Trino (source) — source.type=trino 일 때 사용하는 접속 정보.
        # trino 파이썬 클라이언트(trino.dbapi.connect)에 그대로 전달할 값들이다.
        self.trino_host: str = _get_nested("executor", "trino", "host", "")
        self.trino_port: int = int(_get_nested("executor", "trino", "port", 8080))
        # Trino 는 인증이 없어도 user 헤더(X-Trino-User)가 필수라 기본값을 둔다.
        self.trino_user: str = _get_nested("executor", "trino", "user", "query-executor")
        # password 를 설정하면 BasicAuthentication 사용(클라이언트 제약상 https 필수).
        self.trino_password: str = _get_nested("executor", "trino", "password", "")
        self.trino_catalog: str = _get_nested("executor", "trino", "catalog", "hive")
        self.trino_schema: str = _get_nested("executor", "trino", "schema", "default")
        self.trino_http_scheme: str = str(
            _get_nested("executor", "trino", "http_scheme", "http")
        ).strip().lower() or "http"
        # TLS 검증: 비우면 기본(true), "true"/"false" 또는 CA 인증서 파일 경로를 지정할 수 있다.
        self.trino_verify: str = str(_get_nested("executor", "trino", "verify", "")).strip()
        # 세션 프로퍼티 전역 기본값. "query_max_run_time=1h,..." 형태 → dict.
        # Trino 클라이언트는 연결 단위로만 세션 프로퍼티를 받으므로 요청별 재정의는 없다
        # (요청별 impala_query_options 는 impala 소스에서만 적용).
        self.trino_session_properties: dict[str, str] = _kv_dict(
            _get_nested("executor", "trino", "session_properties", "")
        )
        # 커스텀 쿼리 실행 함수(query-execute 의 trino 경로 위임용). config.properties 에서
        # 자유 정의한다: query.func.module 은 dotted path(module:func / module.func),
        # query.func.config.* 는 함수에 넘길 설정 dict(host/port/user/... + 임의 파라미터, 문자열).
        # YAML 스키마가 아니라 raw properties 를 직접 읽어 키를 자유롭게 추가할 수 있게 한다.
        self.query_func_module: str = str(_props.get("query.func.module", "")).strip()
        self.query_func_config: dict = _collect_prefix("query.func.config.")
        # Greenplum (target) — 읽어온 데이터를 적재할 대상 DB 접속 DSN.
        self.greenplum_dsn: str = _get_nested("executor", "greenplum", "dsn", "")
        # source→target 복사 시 한 번에 처리할 행 수. 메모리 사용량과 처리량의 균형값.
        self.copy_batch_size: int = int(
            _get_nested("executor", "copy", "batch_size", 10000)
        )
        # copy 모드 사전검증: COPY 전에 SELECT 컬럼이 대상 테이블에 있는지 확인(불일치 조기 실패).
        self.copy_preflight: bool = _to_bool(
            _get_nested("executor", "copy", "preflight", True)
        )
        # COPY 파이프라인: Impala 읽기(fetch)와 Greenplum 쓰기(COPY)를 별도 스레드로 겹쳐
        # 실행할지 여부. true 면 리더 스레드가 배치를 큐에 채우고 메인 스레드가 COPY 로 흘려
        # 보내, 두 구간이 직렬이 아니라 병렬로 진행돼 벽시계가 줄어든다(둘이 비슷할수록 효과 큼).
        self.copy_pipeline: bool = _to_bool(
            _get_nested("executor", "copy", "pipeline", True)
        )
        # 파이프라인 큐 크기(배치 개수). 리더가 라이터보다 앞서 채워 둘 수 있는 배치 수의 상한 —
        # backpressure 로 메모리를 제한한다(총 버퍼 ≈ queue_size × batch_size 행).
        self.copy_queue_size: int = int(
            _get_nested("executor", "copy", "queue_size", 8)
        )
        # COPY 포맷: text(기본) | binary. binary 는 값을 문자열로 인코딩하지 않아 클라이언트
        # CPU(write_wait)를 줄일 수 있으나, 컬럼 타입을 정확히 알아야 한다(대상 테이블 카탈로그에서
        # 해석; 실패하면 자동으로 text 로 폴백). write_wait 이 병목일 때만 켜는 실험적 옵션.
        _fmt = str(_get_nested("executor", "copy", "format", "text")).strip().lower()
        self.copy_format: str = _fmt if _fmt in ("text", "binary") else "text"
        # Greenplum 커넥션 풀 최대 크기(executor 1대가 동시에 여는 GP 연결 상한).
        # 0/미설정이면 동시 task 당 1 연결이 되도록 executor.max_concurrent_tasks 와 맞춘다
        # (무제한(0)이면 8 로 폴백). 다운스트림 max_connections 보호의 직접 손잡이다.
        _pool_default = (
            self.executor_max_concurrent_tasks
            if self.executor_max_concurrent_tasks > 0
            else 8
        )
        self.greenplum_pool_max: int = int(
            _get_nested("executor", "greenplum", "pool_max", 0)
        ) or _pool_default

        # ───────── local_stage (file:// 세그먼트 로컬 스테이징) ─────────
        # executor 가 Impala 결과를 CSV 로 떨어뜨릴 로컬 디렉터리 루트. 모든 세그먼트 호스트가
        # 동일 경로를 쓰되, 그 안의 파일은 호스트마다 다르다(각자 자기 몫만 쓴다). job 별
        # 하위 디렉터리({local_dir}/{job_id}/)로 격리된다.
        self.stage_local_dir: str = _get_nested(
            "executor", "stage", "local_dir", "/data1/query-executor/stage"
        )
        # CSV 방언 — executor 의 write 와 GP file:// 외부테이블 FORMAT 'CSV'(...) 에 공통 적용된다.
        # 양쪽이 정확히 일치해야 하며(불일치 시 조용한 데이터 오염), 기본 구분자는 데이터에 잘
        # 나타나지 않는 backtick(`). 설정으로 바꿀 수 있다.
        self.stage_csv_delimiter: str = str(
            _get_nested("executor", "stage", "csv_delimiter", "`")
        ) or "`"
        self.stage_csv_null: str = str(_get_nested("executor", "stage", "csv_null", ""))
        self.stage_csv_quote: str = str(
            _get_nested("executor", "stage", "csv_quote", '"')
        ) or '"'
        # Phase 3 정리: 적재 성공 후 로컬 CSV 디렉터리와 외부테이블을 제거할지 여부.
        self.stage_cleanup: bool = _to_bool(
            _get_nested("executor", "stage", "cleanup", True)
        )
        # file:// 호스트 검증: Phase 2 전에 매핑된 세그먼트 호스트가 실제로
        # gp_segment_configuration 에 있는지 확인해 오타/불일치를 조기 실패시킬지 여부.
        self.stage_validate_hosts: bool = _to_bool(
            _get_nested("executor", "stage", "validate_hosts", True)
        )
        # 호스트당 최대 파일 수 상한. file:// 규칙상 "호스트당 파일 수 ≤ 그 호스트의 primary
        # 세그먼트 수(S_h)". 0 이면 gp_segment_configuration 의 S_h 를 그대로 상한으로 쓰고,
        # >0 이면 min(S_h, 이 값)으로 더 낮춘다(세그먼트보다 적게 쓰고 싶을 때).
        self.stage_max_files_per_host: int = int(
            _get_nested("executor", "stage", "max_files_per_host", 0)
        )
        # local_stage export 시 소스 커서의 값 형변환 여부. False(기본)면 형변환을 꺼
        # TIMESTAMP/DATE/DECIMAL 을 wire 문자열 그대로 받아 CSV 로 바로 쓴다(재파싱 비용 제거).
        # impala 는 impyla 의 convert_types, trino 는 legacy_primitive_types 로 동일하게 적용된다.
        # 특수 타입(BINARY 등)이 문자열화에 문제가 되면 true 로 되돌린다.
        self.stage_impala_convert_types: bool = _to_bool(
            _get_nested("executor", "stage", "impala_convert_types", False)
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
    global _raw, _props, _yaml_path, _properties_path
    if yaml_path:
        _yaml_path = Path(yaml_path)
    if properties_path:
        _properties_path = Path(properties_path)
    _raw = load_config(
        config_dir=_CONFIG_DIR,
        yaml_path=yaml_path,
        properties_path=properties_path,
    )
    # 자유 정의 프리픽스 수집을 위해 raw properties 도 함께 재로딩한다.
    _props = load_properties(_properties_path)
    settings.__init__()


# 모듈 import 시점에 만들어지는 전역 싱글턴. 애플리케이션 전역에서
# `from core.config import settings` 로 공유한다.
settings = Settings()
