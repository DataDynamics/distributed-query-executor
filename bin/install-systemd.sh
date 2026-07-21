#!/usr/bin/env bash
# systemd 유닛(coordinator.service, executor@.service)을 설치·활성화한다.
# /etc 에 실제 파일을 두지 않는 이 프로젝트 관례에 맞춰 `systemctl link` 로 배포 트리 bin/ 의
# 유닛 파일에 심볼릭 링크만 만든다(/etc/systemd/system 에 링크). 런처 스크립트(start-*.sh)를
# 쓰지 않고 systemd 로 관리하고 싶을 때만 실행하면 된다.
#
# 사용:  sudo ./bin/install-systemd.sh [EXECUTOR_PORT...]   # 포트 생략 시 8087
#   예)  sudo ./bin/install-systemd.sh 8087 8086
#
# 제거:  sudo systemctl disable --now coordinator executor@8087
#        sudo systemctl disable coordinator.service executor@.service   # 링크 해제
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORTS=("$@")
[[ ${#PORTS[@]} -eq 0 ]] && PORTS=(8087)

if [[ $EUID -ne 0 ]]; then
    echo "root 권한으로 실행하세요: sudo $0 [PORT...]" >&2
    exit 1
fi

echo "==> 유닛 링크(/etc 에는 심볼릭 링크만)"
systemctl link "$DIR/coordinator.service"
systemctl link "$DIR/executor@.service"
systemctl daemon-reload

echo "==> coordinator 활성화/기동"
systemctl enable --now coordinator.service

echo "==> executor 인스턴스 활성화/기동: ${PORTS[*]}"
for p in "${PORTS[@]}"; do
    systemctl enable --now "executor@$p.service"
done

echo
echo "설치 완료. 상태/로그:"
echo "  systemctl status coordinator executor@${PORTS[0]}"
echo "  journalctl -u coordinator -f"
echo "  journalctl -u executor@${PORTS[0]} -f"
