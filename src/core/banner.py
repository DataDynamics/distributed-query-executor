"""기동 배너(Spring Boot 스타일) — coordinator·executor 가 뜰 때 ASCII 아트 + 버전 표시.

Spring Boot 가 시작 시 콘솔에 ASCII 배너와 버전을 찍는 것을 흉내 낸다. 애플리케이션이
어떤 버전으로, 어떤 역할(coordinator/executor)로, 어느 포트에서 떴는지를 로그 파일을
열지 않고도 콘솔(부팅 로그)에서 바로 확인할 수 있게 한다.

## 커스터마이즈(Spring Boot `banner.txt` 방식)

설정 디렉터리(`QUERY_EXECUTOR_CONFIG_DIR`, 개발은 `config/`)에 ``banner.txt`` 를 두면
내장 기본 아트 대신 그 파일을 쓴다. 파일 안에서는 다음 자리표시자를 치환한다:
``${version}``(버전), ``${role}``(coordinator|executor), ``${port}``(수신 포트),
``${python}``(파이썬 버전). 파일이 없으면 아래 내장 블록체(ANSI Shadow) 배너를 쓴다.
"""

from __future__ import annotations

import logging
import os
import platform
from pathlib import Path

from core.version import version_string

# 내장 ASCII 아트(ANSI Shadow 스타일 "DQE" = Distributed Query Executor).
# 유니코드 블록 문자를 쓰므로 콘솔/로그는 UTF-8 을 전제한다(이 저장소 관례).
_ART = r"""
██████╗  ██████╗ ███████╗
██╔══██╗██╔═══██╗██╔════╝
██║  ██║██║   ██║█████╗
██║  ██║██║▄▄ ██║██╔══╝
██████╔╝╚██████╔╝███████╗
╚═════╝  ╚══▀▀═╝ ╚══════╝
"""


def _config_dir() -> Path:
    """배너 오버라이드(banner.txt)를 찾을 설정 디렉터리."""
    return Path(os.getenv("QUERY_EXECUTOR_CONFIG_DIR", "/data1/distributed-query-executor/config"))


def _substitute(text: str, mapping: dict[str, str]) -> str:
    """``${key}`` 자리표시자를 mapping 값으로 치환한다(간단한 문자열 치환)."""
    for key, val in mapping.items():
        text = text.replace("${" + key + "}", val)
    return text


def render_banner(role: str, port: int | None = None, *, config_dir: Path | None = None) -> str:
    """기동 배너 문자열을 만든다(콘솔 출력용, 끝에 개행 포함).

    인자:
        role: 애플리케이션 역할 라벨(``coordinator`` | ``executor``).
        port: 수신 포트(있으면 표시).
        config_dir: 배너 오버라이드(banner.txt) 를 찾을 디렉터리(기본: 설정 디렉터리).

    반환:
        출력할 배너 문자열. ``banner.txt`` 가 있으면 그 내용을(자리표시자 치환) 쓰고,
        없으면 내장 아트 아래에 "제품명 / 역할·포트 / 버전 / 파이썬" 정보 줄을 붙인다.
    """
    version = version_string()
    python = platform.python_version()
    where = f"{role}:{port}" if port else role
    mapping = {"version": version, "role": role, "port": str(port or ""), "python": python}

    cfg = config_dir or _config_dir()
    override = cfg / "banner.txt"
    if override.is_file():
        return _substitute(override.read_text(encoding="utf-8"), mapping).rstrip("\n") + "\n"

    # 내장 배너: 아트 + 정보 두 줄(Spring Boot 처럼 버전을 아트 하단에 표기).
    info = (
        f" Distributed Query Executor  (v{version})\n"
        f" :: {where} ::   Python {python}\n"
    )
    return _ART.lstrip("\n").rstrip("\n") + "\n" + info


def print_banner(role: str, port: int | None = None, *, config_dir: Path | None = None) -> None:
    """기동 배너를 표준출력(콘솔)에 찍는다. 실패해도 기동을 막지 않는다."""
    try:
        # Spring Boot 처럼 콘솔에 먼저 찍는다(파일 로깅과 별개로 부팅 로그에서 바로 보이게).
        print(render_banner(role, port, config_dir=config_dir), flush=True)
    except Exception:
        # 배너는 부가 기능 — 어떤 이유로든(인코딩·파일 등) 실패해도 서비스 기동은 계속한다.
        pass


def render_config_sources(properties_path: Path, yaml_path: Path) -> str:
    """로딩한 설정 파일들의 **절대 경로**(+ 존재 여부)를 보여주는 문자열을 만든다.

    config.properties / config.yml 을 실제로 어느 파일에서 읽었는지 배너 바로 아래에
    노출해, 경로 오지정·빈 디렉터리·QUERY_EXECUTOR_CONFIG_DIR 오설정 같은 로딩 문제를
    콘솔에서 바로 눈치챌 수 있게 한다. 파일이 없으면 경로 뒤에 경고 마커를 붙인다.

    인자:
        properties_path: 로딩한 config.properties 경로(``settings.config_properties_path``).
        yaml_path: 로딩한 config.yml 경로(``settings.config_yaml_path``).
    """
    def _line(label: str, p: Path) -> str:
        # resolve() 로 상대 경로(개발 시 QUERY_EXECUTOR_CONFIG_DIR=config 등)도 절대 경로로 만든다.
        abs_path = p.resolve()
        mark = "" if p.is_file() else "   ← 파일 없음(로딩 실패)!"
        return f"   {label:<17}: {abs_path}{mark}"

    return (
        " 로딩한 설정 파일(절대 경로):\n"
        + _line("config.properties", properties_path) + "\n"
        + _line("config.yml", yaml_path) + "\n"
    )


def print_config_sources(properties_path: Path, yaml_path: Path) -> None:
    """로딩한 설정 파일 경로를 표준출력(콘솔)에 찍는다. 실패해도 기동을 막지 않는다."""
    try:
        print(render_config_sources(properties_path, yaml_path), flush=True)
    except Exception:
        # 배너와 마찬가지로 부가 정보 — 실패해도 서비스 기동은 계속한다.
        pass


def log_startup(
    logger: logging.Logger,
    role: str,
    port: int | None,
    properties_path: Path,
    yaml_path: Path,
    *,
    config_dir: Path | None = None,
) -> None:
    """기동 배너 **전체**(아트 + 설정 파일 경로)를 로그 파일에도 한 번 남긴다.

    ``print_banner``/``print_config_sources`` 는 stdout 으로만 나가고, 런처가 이를
    ``logs/<name>.out`` 으로 리다이렉트한다(``bin/env.sh``). 그래서 ``.log`` 만 보는
    경우 배너를 볼 수 없다. 이 함수는 ``setup_logging`` 이후에 호출되어, 같은 배너를
    로그 파일에도 하나의 레코드로 남긴다.

    첫 줄은 grep 하기 좋은 요약(역할·버전·포트)이고, 이어서 배너 아트와 로딩한 설정
    파일의 절대 경로 블록이 붙는다. 부가 정보이므로 어떤 이유로든 실패해도 기동을 막지 않는다.
    """
    try:
        banner = render_banner(role, port, config_dir=config_dir)
        sources = render_config_sources(properties_path, yaml_path)
        logger.info(
            "%s 기동 (version=%s port=%s)\n%s\n%s",
            role, version_string(), port if port is not None else "",
            banner.rstrip("\n"), sources.rstrip("\n"),
        )
    except Exception:
        pass
