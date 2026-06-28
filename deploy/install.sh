#!/usr/bin/env bash
# RHEL 9.2용 설치 스크립트.
# 애플리케이션을 /opt/query-executor 에 배치하고, 전용 사용자/venv/설정/systemd 유닛을 구성한다.
# 사용법:  sudo ./deploy/install.sh
set -euo pipefail

APP_USER="queryexec"
APP_DIR="/opt/query-executor"
CONF_DIR="/etc/query-executor"
LOG_DIR="/var/log/query-executor"
# RHEL 9.2 기본 Python 은 3.9 이다. 별도 설치 없이 시스템 python3.9 를 그대로 쓴다.
PYTHON="${PYTHON:-python3.9}"
# 에어갭(인터넷 차단) 설치 지원: WHEELHOUSE 에 미리 받아 둔 wheel 디렉터리를 지정하면
# PyPI 대신 그 디렉터리에서만(--no-index) 의존성을 설치한다.
WHEELHOUSE="${WHEELHOUSE:-}"
# executor 런타임 드라이버(impyla/gssapi 등)까지 설치하려면 INSTALL_EXECUTOR=1.
INSTALL_EXECUTOR="${INSTALL_EXECUTOR:-0}"

# 저장소 루트(이 스크립트의 상위 디렉터리)
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $EUID -ne 0 ]]; then
    echo "root 권한으로 실행하세요: sudo $0" >&2
    exit 1
fi

echo "==> Python 3.9 확인"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "$PYTHON 가 없습니다. 먼저 설치하세요: sudo dnf install -y python3 python3-devel rsync" >&2
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
    --exclude '.pytest_cache' --exclude '*.egg-info' --exclude 'logs' \
    "$SRC_DIR"/ "$APP_DIR"/

echo "==> 가상환경 및 의존성 설치"
"$PYTHON" -m venv "$APP_DIR/.venv"
PIP="$APP_DIR/.venv/bin/pip"
# 설치할 requirements 선택: executor 드라이버 포함 여부.
if [[ "$INSTALL_EXECUTOR" == "1" ]]; then
    REQ="$APP_DIR/requirements-executor.txt"
else
    REQ="$APP_DIR/requirements.txt"
fi
if [[ -n "$WHEELHOUSE" ]]; then
    # 에어갭: 미리 받아 둔 wheelhouse 에서만 설치(인터넷·PyPI 접근 없음).
    echo "    (에어갭 모드) wheelhouse=$WHEELHOUSE"
    "$PIP" install --no-index --find-links "$WHEELHOUSE" --upgrade pip
    "$PIP" install --no-index --find-links "$WHEELHOUSE" -r "$REQ"
else
    "$PIP" install --upgrade pip
    "$PIP" install -r "$REQ"
fi

chown -R "$APP_USER:$APP_USER" "$APP_DIR"

echo "==> 설정 파일 배치 -> $CONF_DIR (config.properties + config.yml)"
mkdir -p "$CONF_DIR"
if [[ ! -f "$CONF_DIR/config.properties" ]]; then
    cp "$APP_DIR/packaging/config/config.properties" "$CONF_DIR/config.properties"
    # 운영 로그 경로를 절대 경로로 설정(개발 기본값 logs -> /var/log/query-executor)
    sed -i "s|^log.dir=.*|log.dir=$LOG_DIR|" "$CONF_DIR/config.properties"
fi
[[ -f "$CONF_DIR/config.yml" ]] || cp "$APP_DIR/packaging/config/config.yml" "$CONF_DIR/config.yml"
chmod 640 "$CONF_DIR"/config.properties "$CONF_DIR"/config.yml
chown root:"$APP_USER" "$CONF_DIR"/config.properties "$CONF_DIR"/config.yml

echo "==> 로그 디렉터리 준비 -> $LOG_DIR"
mkdir -p "$LOG_DIR"
chown "$APP_USER:$APP_USER" "$LOG_DIR"

echo "==> systemd 유닛 설치"
cp "$APP_DIR/deploy/systemd/query-coordinator.service" /etc/systemd/system/
cp "$APP_DIR/deploy/systemd/query-executor@.service" /etc/systemd/system/
# Impala Kerberos 티켓 발급/갱신 유닛
cp "$APP_DIR/deploy/systemd/query-executor-kinit.service" /etc/systemd/system/
cp "$APP_DIR/deploy/systemd/query-executor-kinit.timer" /etc/systemd/system/
systemctl daemon-reload

echo
echo "설치 완료. 다음을 확인/수정하세요:"
echo "  - 설정:        $CONF_DIR/config.properties , $CONF_DIR/config.yml"
echo "  - Impala TLS:  $CONF_DIR/impala-ca.pem (CA 인증서 배치)"
echo "  - Kerberos:    $CONF_DIR/impala.keytab (keytab 배치, 소유자 queryexec, 권한 600)"
echo "                 query-executor-kinit.service 의 principal/keytab 경로 수정"
echo
echo "Kerberos 티켓 갱신 타이머 활성화:"
echo "  sudo systemctl enable --now query-executor-kinit.timer"
echo
echo "기동:"
echo "  sudo systemctl enable --now query-executor@8087 query-executor@8086"
echo "  sudo systemctl enable --now query-coordinator"
