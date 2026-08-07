"""CLI 도구가 공유하는 설정 어댑터다.

이 저장소에는 이미 `config/config.properties` + `config/config.yml` 이라는 설정 체계가
있고(:mod:`core.config_loader`), coordinator·executor 가 그 값을 그대로 쓴다. 그래서
CLI 도구가 별도의 YAML 을 또 두면 같은 접속 정보를 두 곳에 적어야 하고, 한쪽만 고쳐서
어긋나는 사고가 난다. 이 모듈은 그 사고를 막으려고 **기존 설정을 그대로 읽어** 도구가
기대하는 섹션(`impala`/`greenplum`/`s3`/`sql`) 모양으로 바꿔 준다.

값의 우선순위는 **명령행 인자 > 설정 파일 > 각 도구의 기본값** 이다. 설정에 값이 있어도
명령행으로 준 값이 언제나 이긴다.

읽어 오는 설정 디렉터리는 `QUERY_EXECUTOR_CONFIG_DIR` 이 정하며(코드 기본값은
`/data1/distributed-query-executor/config`, 개발 시에는 저장소의 `config/`),
``--config-dir`` 로 바꾸거나 ``--no-config`` 로 아예 읽지 않을 수 있다.

Greenplum 만 형태가 다르다. 이 저장소는 접속 정보를 host·port 로 쪼개지 않고
``greenplum.dsn`` 한 줄(``postgresql://user:pass@host:5432/db``)로 들고 있어서,
:func:`parse_dsn` 이 그 DSN 을 도구가 쓰는 키로 풀어 준다.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Optional, Sequence
from urllib.parse import unquote, urlparse

#: 도구가 참조하는 SQL 템플릿 디렉터리. 이 저장소는 서버 템플릿을 ``templates/`` 에
#: 두지만 그쪽은 manifest 기반이라 성격이 다르다. 여기서는 ``-f`` 로 읽을 평범한
#: ``.sql`` 파일 자리로 저장소 루트의 ``sql/`` 을 쓴다(없으면 그때 안내한다).
ROOT = Path(__file__).resolve().parents[2]

#: ``--query-file`` 에 이름만 줬을 때 찾아볼 기본 디렉터리
SQL_DIR = ROOT / "sql"


def config_dir() -> Path:
    """도구가 읽을 설정 디렉터리를 정한다.

    :mod:`core.config_loader` 와 같은 규칙이다. 환경변수를 먼저 보고, 없으면 운영
    기본 경로를 쓰되 그 경로가 없으면 저장소의 ``config/`` 로 떨어진다. 개발 트리에서
    아무 설정 없이 실행해도 저장소 설정이 잡히게 하려는 것이다.
    """
    env = os.environ.get("QUERY_EXECUTOR_CONFIG_DIR")
    if env:
        return Path(env)
    deploy = Path("/data1/distributed-query-executor/config")
    return deploy if (deploy / "config.yml").is_file() else ROOT / "config"


#: ``--help`` 에 보여줄 기본 설정 파일 경로
DEFAULT_CONFIG = config_dir() / "config.yml"


def add_config_arguments(parser: argparse.ArgumentParser) -> None:
    """``--config-dir`` 과 ``--no-config`` 를 파서에 붙인다."""
    group = parser.add_argument_group("설정")
    group.add_argument(
        "-c",
        "--config-dir",
        metavar="DIR",
        help=f"config.properties·config.yml 이 있는 디렉터리 (기본 {config_dir()})",
    )
    group.add_argument(
        "--no-config",
        action="store_true",
        help="설정 파일을 읽지 않고 명령행 인자만 씁니다",
    )


def resolve_config_path(args: argparse.Namespace) -> Optional[Path]:
    """실제로 읽을 설정 디렉터리를 정한다. 읽지 않을 상황이면 None 이다.

    ``--config-dir`` 로 직접 지정한 디렉터리에 ``config.yml`` 이 없으면 오타일 가능성이
    높으므로 오류를 낸다. 반대로 기본 디렉터리는 없어도 조용히 넘어간다 — 설정 없이
    명령행 인자만으로도 돌아야 하기 때문이다.
    """
    if getattr(args, "no_config", False):
        return None
    given = getattr(args, "config_dir", None)
    if given:
        path = Path(given)
        if not (path / "config.yml").is_file():
            raise SystemExit(f"설정 디렉터리에 config.yml 이 없습니다: {given}")
        return path
    default = config_dir()
    return default if (default / "config.yml").is_file() else None


def _load_settings(path: Path):
    """``path`` 를 설정 디렉터리로 삼아 :class:`core.config.Settings` 를 만든다.

    :mod:`core.config` 는 모듈을 import 하는 시점에 디렉터리를 확정하므로, 다른
    디렉터리를 읽으려면 환경변수를 바꿔 놓고 다시 로드해야 한다. 도구는 프로세스마다
    한 번만 읽으므로 이 방식으로 충분하다.
    """
    import importlib

    previous = os.environ.get("QUERY_EXECUTOR_CONFIG_DIR")
    os.environ["QUERY_EXECUTOR_CONFIG_DIR"] = str(path)
    try:
        import core.config as core_config

        importlib.reload(core_config)
        return core_config.settings
    finally:
        if previous is None:
            os.environ.pop("QUERY_EXECUTOR_CONFIG_DIR", None)
        else:
            os.environ["QUERY_EXECUTOR_CONFIG_DIR"] = previous


def parse_dsn(dsn: str) -> Dict[str, Any]:
    """``postgresql://user:pass@host:5432/db?sslmode=require`` 를 접속 키로 푼다.

    이 저장소는 Greenplum 접속을 DSN 한 줄로 들고 있는데, 도구는 host·port·database
    처럼 쪼갠 값을 받는다. libpq 가 이해하는 형태 중 URL 형식만 다룬다. 비어 있거나
    URL 이 아니면 빈 dict 를 돌려 도구가 명령행 인자를 요구하게 한다.
    """
    if not dsn or "://" not in dsn:
        return {}
    parsed = urlparse(dsn)
    out: Dict[str, Any] = {}
    if parsed.hostname:
        out["host"] = parsed.hostname
    if parsed.port:
        out["port"] = parsed.port
    if parsed.username:
        out["user"] = unquote(parsed.username)
    if parsed.password:
        out["password"] = unquote(parsed.password)
    database = (parsed.path or "").lstrip("/")
    if database:
        out["database"] = database
    # 쿼리 문자열의 sslmode·connect_timeout 만 본다. 나머지는 도구가 쓰지 않는다.
    for item in (parsed.query or "").split("&"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        if key in ("sslmode", "connect_timeout") and value:
            out[key] = value
    return out


def _impala_section(settings) -> Dict[str, Any]:
    """``executor.impala.*`` 를 impala 도구가 쓰는 키로 옮긴다."""
    return {
        "host": settings.impala_host or None,
        "port": settings.impala_port or None,
        "database": settings.impala_database or None,
        "user": settings.impala_user or None,
        "password": settings.impala_password or None,
        "auth_mechanism": settings.impala_auth_mechanism or None,
        # 이 저장소 설정에는 Kerberos 서비스명 항목이 없다. 필요하면 명령행으로 준다.
        "kerberos_service_name": None,
        "use_ssl": settings.impala_use_ssl,
        "ca_cert": settings.impala_ca_cert or None,
        "timeout": None,
        # 전역 Impala 쿼리 옵션(SET)을 셸 세션에도 그대로 적용한다.
        "session_settings": dict(settings.impala_query_options or {}) or None,
    }


def _greenplum_section(settings) -> Dict[str, Any]:
    """``executor.greenplum.dsn`` 을 gp 도구가 쓰는 키로 푼다."""
    parsed = parse_dsn(settings.greenplum_dsn)
    return {
        "host": parsed.get("host"),
        "port": parsed.get("port"),
        "database": parsed.get("database"),
        "user": parsed.get("user"),
        "password": parsed.get("password"),
        # search_path 로 쓸 스키마는 메타 저장소 스키마와 성격이 달라 비워 둔다.
        "schema": None,
        "sslmode": parsed.get("sslmode"),
        "connect_timeout": parsed.get("connect_timeout"),
        "session_sql": None,
    }


def _s3_section(settings) -> Dict[str, Any]:
    """``executor.s3.*`` 를 s3 도구가 쓰는 키로 옮긴다."""
    return {
        "bucket": settings.s3_bucket or None,
        "region": settings.s3_region or None,
        "access_key_id": settings.s3_access_key or None,
        "secret_access_key": settings.s3_secret_key or None,
        # 이 저장소 설정에는 세션 토큰 항목이 없다. 환경변수나 명령행으로 준다.
        "session_token": None,
        "client_endpoint_url": settings.s3_endpoint_url or None,
        "multipart_threshold": None,
    }


def _sql_section(_settings) -> Dict[str, Any]:
    """``.sql`` 템플릿 디렉터리. 설정 항목이 없어 저장소의 ``sql/`` 을 쓴다."""
    return {"dir": None}


#: 섹션 이름에서 변환 함수를 찾는 표. 도구가 요구하는 섹션은 이 넷뿐이다.
_SECTIONS = {
    "impala": _impala_section,
    "greenplum": _greenplum_section,
    "s3": _s3_section,
    "sql": _sql_section,
}


def load_section(
    path: Optional[Path],
    section: str,
    keys: Sequence[str],
    required: bool = False,
    path_keys: Sequence[str] = (),
) -> Dict[str, Any]:
    """설정에서 한 섹션을 읽어 ``keys`` 에 해당하는 값만 돌려준다.

    도구가 모르는 키는 걸러 내므로, 설정에 쓰지 않는 항목이 섞여 있어도 문제가 되지
    않는다. ``path`` 가 None 이면(``--no-config``) 전부 None 인 dict 를 돌려준다.

    ``path_keys`` 는 원본 도구와의 호환을 위해 받기만 하고 쓰지 않는다. 이 저장소의
    설정 로더가 경로를 이미 절대 경로로 확정해 주기 때문이다.
    """
    empty: Dict[str, Any] = {key: None for key in keys}
    if path is None:
        return empty

    builder = _SECTIONS.get(section)
    if builder is None:
        if required:
            raise SystemExit(f"알 수 없는 설정 섹션입니다: {section}")
        return empty

    settings = _load_settings(path)
    body = builder(settings)
    if required and not any(v for v in body.values()):
        raise SystemExit(
            f"{path/'config.properties'} 에 {section} 접속 정보가 비어 있습니다."
        )
    return {key: body.get(key) for key in keys}


def pick(cli: Any, config: Any, default: Any = None) -> Any:
    """명령행 > 설정 파일 > 기본값 순으로 고른다."""
    if cli is not None:
        return cli
    if config is not None:
        return config
    return default
