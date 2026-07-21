"""systemd 유닛(bin/coordinator.service, bin/executor@.service) 정합성 검증.

systemd 없이도 파일 자체를 파싱해, 런처 환경(bin/env.sh)과 어긋나지 않는지 확인한다
(경로·설정 디렉터리·PYTHONPATH·실행 커맨드·executor 인스턴스 %i). 배포 트리 경로가
바뀌면 여기서 드리프트가 잡힌다.
"""
from __future__ import annotations

import configparser
from pathlib import Path

BIN = Path(__file__).resolve().parent.parent / "bin"
APP_HOME = "/data1/distributed-query-executor"


def _unit(name: str) -> configparser.ConfigParser:
    # systemd 유닛도 INI 형식이라 configparser 로 파싱 가능. 단 systemd 는 같은 키를 여러 번
    # 쓸 수 있으므로(Environment=, After= 등) strict=False 로 중복을 허용한다(마지막 값 유지).
    cp = configparser.ConfigParser(interpolation=None, strict=False)
    cp.optionxform = str  # 대소문자 보존(Environment 등)
    cp.read(BIN / name, encoding="utf-8")
    return cp


def test_두_유닛_파일이_존재한다():
    assert (BIN / "coordinator.service").is_file()
    assert (BIN / "executor@.service").is_file()
    assert (BIN / "install-systemd.sh").is_file()


def test_coordinator_유닛_핵심_지시자():
    u = _unit("coordinator.service")
    svc = u["Service"]
    assert svc["ExecStart"] == f"{APP_HOME}/.venv/bin/python -m coordinator"
    assert svc["WorkingDirectory"] == APP_HOME
    assert svc["User"] == "gpadmin"
    assert svc["Type"] == "simple"
    assert u["Install"]["WantedBy"] == "multi-user.target"


def test_executor_템플릿_유닛은_포트_인스턴스를_쓴다():
    u = _unit("executor@.service")
    assert u["Service"]["ExecStart"] == f"{APP_HOME}/.venv/bin/python -m executor"
    # 포트/인스턴스 식별자는 systemd 인스턴스 이름 %i 로 주입된다. configparser 는 같은 키
    # (Environment)가 여러 줄이면 마지막만 남으므로 원문 텍스트로 확인한다.
    text = (BIN / "executor@.service").read_text(encoding="utf-8")
    assert "Environment=EXECUTOR_PORT=%i" in text
    assert "Environment=EXECUTOR_INSTANCE=%i" in text
    assert "Description=Distributed Query Executor - Executor (port %i)" in text


def test_유닛_환경이_env_sh_와_일치한다():
    # 런처 환경(env.sh)은 APP_HOME 변수로 경로를 만든다. APP_HOME 값과 파생 경로가
    # 유닛의 절대 경로와 같은 곳을 가리키는지 확인한다(드리프트 방지).
    env_sh = (BIN / "env.sh").read_text(encoding="utf-8")
    assert f'APP_HOME="{APP_HOME}"' in env_sh
    assert 'QUERY_EXECUTOR_CONFIG_DIR="$APP_HOME/config"' in env_sh
    assert 'PYTHONPATH="$APP_HOME/src:$APP_HOME' in env_sh
    for name in ("coordinator.service", "executor@.service"):
        text = (BIN / name).read_text(encoding="utf-8")
        assert f"Environment=QUERY_EXECUTOR_CONFIG_DIR={APP_HOME}/config" in text
        assert f"Environment=PYTHONPATH={APP_HOME}/src:{APP_HOME}" in text
