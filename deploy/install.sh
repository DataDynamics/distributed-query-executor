#!/usr/bin/env bash
# RHEL 9.2용 설치 스크립트.
# 애플리케이션을 /opt/query-executor 에 배치하고, 전용 사용자/venv/systemd 유닛을 구성한다.
# 사용법:  sudo ./deploy/install.sh
set -euo pipefail

APP_USER="queryexec"
APP_DIR="/opt/query-executor"
CONF_DIR="/etc/query-executor"
PYTHON="${PYTHON:-python3.11}"

# 저장소 루트(이 스크립트의 상위 디렉터리)
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $EUID -ne 0 ]]; then
    echo "root 권한으로 실행하세요: sudo $0" >&2
    exit 1
fi

echo "==> Python 3.11 확인"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "$PYTHON 가 없습니다. 먼저 설치하세요: sudo dnf install -y python3.11 python3.11-pip python3.11-devel" >&2
    exit 1
fi

echo "==> 서비스 계정($APP_USER) 생성"
if ! id "$APP_USER" >/dev/null 2>&1; then
    useradd --system --home-dir "$APP_DIR" --shell /sbin/nologin "$APP_USER"
fi

echo "==> 애플리케이션 복사 -> $APP_DIR"
mkdir -p "$APP_DIR"
rsync -a --delete \
    --exclude '.venv' --exclude '.git' --exclude '__pycache__' \
    --exclude '.pytest_cache' --exclude '*.egg-info' \
    "$SRC_DIR"/ "$APP_DIR"/

echo "==> 가상환경 및 의존성 설치"
"$PYTHON" -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --upgrade pip
"$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
# executor를 실제 DB에 연결하려면 아래 주석을 해제:
# "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements-executor.txt"

chown -R "$APP_USER:$APP_USER" "$APP_DIR"

echo "==> 환경설정 파일 배치 -> $CONF_DIR"
mkdir -p "$CONF_DIR"
[[ -f "$CONF_DIR/coordinator.env" ]] || \
    cp "$APP_DIR/deploy/systemd/coordinator.env.example" "$CONF_DIR/coordinator.env"
[[ -f "$CONF_DIR/executor.env" ]] || \
    cp "$APP_DIR/deploy/systemd/executor.env.example" "$CONF_DIR/executor.env"
chmod 640 "$CONF_DIR"/*.env
chown root:"$APP_USER" "$CONF_DIR"/*.env

echo "==> systemd 유닛 설치"
cp "$APP_DIR/deploy/systemd/query-coordinator.service" /etc/systemd/system/
cp "$APP_DIR/deploy/systemd/query-executor@.service" /etc/systemd/system/
systemctl daemon-reload

echo
echo "설치 완료. 다음 명령으로 기동하세요:"
echo "  sudo systemctl enable --now query-executor@8001 query-executor@8002"
echo "  sudo systemctl enable --now query-coordinator"
echo
echo "설정 변경: $CONF_DIR/coordinator.env, $CONF_DIR/executor.env"
