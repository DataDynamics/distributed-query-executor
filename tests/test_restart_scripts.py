"""restart-*.sh 런처 스크립트 정합성 검증.

실제 프로세스를 띄우지 않고 파일 구조만 확인한다. 존재 여부와 실행 권한, env.sh 를
source 하는지, 대응하는 stop/start 를 부르는지, 종료 대기 헬퍼(wait_pid_gone)를 쓰는지를
본다. 배포 트리(/data1)가 아니라 저장소에서 돌더라도 검증되도록 텍스트 기반으로 본다.

역할별 스크립트만 남기고 전체 제어 스크립트(restart.sh 등)는 없앴으므로, 여기서도
coordinator·executor 두 개만 확인한다.
"""
from __future__ import annotations

import os

from pathlib import Path

BIN = Path(__file__).resolve().parent.parent / "bin"

RESTART_SCRIPTS = ("restart-coordinator.sh", "restart-executor.sh")


def test_restart_스크립트가_존재하고_실행권한이_있다():
    for name in RESTART_SCRIPTS:
        p = BIN / name
        assert p.is_file(), f"{name} 없음"
        assert os.access(p, os.X_OK), f"{name} 실행권한 없음"


def test_env_sh_에_wait_pid_gone_헬퍼가_있다():
    text = (BIN / "env.sh").read_text(encoding="utf-8")
    assert "wait_pid_gone()" in text
    # 타임아웃 시 강제 종료로 포트를 확실히 비운다.
    assert "kill -9" in text


def test_restart_scripts_는_env_와_stop_start_를_엮는다():
    for name in RESTART_SCRIPTS:
        text = (BIN / name).read_text(encoding="utf-8")
        assert 'source "$DIR/env.sh"' in text
        assert "wait_pid_gone" in text          # 중지 후 종료 대기

    coord = (BIN / "restart-coordinator.sh").read_text(encoding="utf-8")
    assert "stop-coordinator.sh" in coord and "start-coordinator.sh" in coord

    execu = (BIN / "restart-executor.sh").read_text(encoding="utf-8")
    assert "stop-executor.sh" in execu and "start-executor.sh" in execu
    assert "EXECUTOR_PORTS" in execu            # 인자 없을 때의 폴백


def test_전체_제어_스크립트는_남아_있지_않다():
    """start/stop/restart.sh 를 지운 뒤 문서나 설치 스크립트에 참조가 남지 않게 한다."""
    for name in ("start.sh", "stop.sh", "restart.sh"):
        assert not (BIN / name).exists(), f"{name} 이 아직 남아 있다"


def test_systemd_자산은_bin_systemd_아래에_모여_있다():
    systemd = BIN / "systemd"
    for name in ("coordinator.service", "executor@.service", "install-systemd.sh"):
        assert (systemd / name).is_file(), f"bin/systemd/{name} 없음"
    # bin/ 최상위에는 유닛 파일이 남아 있지 않아야 한다.
    assert not list(BIN.glob("*.service"))
    assert os.access(systemd / "install-systemd.sh", os.X_OK)
