#!/usr/bin/env bash
# RHEL 9.2용 설치 스크립트(에어갭 + /data1 단일 트리).
# 보안 정책상 /etc·/opt·/var 에 파일을 추가하지 않는다. 애플리케이션·설정·로그·런타임을
# 모두 /data1/query-executor 아래에 배치하고, systemd 시스템 유닛 대신 런처 스크립트로 구동한다.
# 사용법:  sudo ./deploy/install.sh
set -euo pipefail

# ── 경로(모두 /data1 아래) ──────────────────────────────────────────────
APP_USER="${APP_USER:-gpadmin}"            # 서비스 계정(없으면 생성, 홈=/data1)
APP_BASE="/data1"
APP_HOME="$APP_BASE/query-executor"
CONF_DIR="$APP_HOME/config"                # config.{properties,yml}, 인증서
LOG_DIR="$APP_HOME/logs"
RUN_DIR="$APP_HOME/run"                     # PID
BIN_DIR="$APP_HOME/bin"                     # 런처 스크립트
VENV="$APP_HOME/.venv"

# RHEL 9.2 기본 Python 은 3.9 이다. 별도 설치 없이 시스템 python3.9 를 그대로 쓴다.
PYTHON="${PYTHON:-python3.9}"
# 에어갭(인터넷 차단) 설치: WHEELHOUSE 에 미리 받아 둔 wheel 디렉터리를 지정하면
# PyPI 대신 그 디렉터리에서만(--no-index) 설치한다. 비우면 pip 기본 인덱스(Nexus 등) 사용.
WHEELHOUSE="${WHEELHOUSE:-}"
# executor 런타임 드라이버(impyla 등)까지 설치하려면 INSTALL_EXECUTOR=1.
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

echo "==> 서비스 계정($APP_USER) 생성(홈=$APP_BASE)"
if ! id "$APP_USER" >/dev/null 2>&1; then
    useradd --system --home-dir "$APP_BASE" --create-home --shell /sbin/nologin "$APP_USER"
fi
mkdir -p "$APP_BASE"

echo "==> 애플리케이션 복사 -> $APP_HOME"
mkdir -p "$APP_HOME"
rsync -a --delete \
    --exclude '.venv' --exclude '.git' --exclude '__pycache__' \
    --exclude '.pytest_cache' --exclude '*.egg-info' --exclude 'logs' \
    --exclude '/config' --exclude '/run' --exclude '/logs' \
    "$SRC_DIR"/ "$APP_HOME"/

echo "==> 가상환경 및 의존성 설치"
"$PYTHON" -m venv "$VENV"
PIP="$VENV/bin/pip"
if [[ "$INSTALL_EXECUTOR" == "1" ]]; then
    REQ="$APP_HOME/requirements-executor.txt"
else
    REQ="$APP_HOME/requirements.txt"
fi
if [[ -n "$WHEELHOUSE" ]]; then
    # WHEELHOUSE 는 콜론(:)으로 여러 디렉터리 지정 가능 → 다중 --find-links
    # 예) packaging/wheels/coordinator:packaging/wheels/executor
    echo "    (에어갭 모드) wheelhouse=$WHEELHOUSE"
    FIND_LINKS=()
    IFS=':' read -ra _WHS <<< "$WHEELHOUSE"
    for _w in "${_WHS[@]}"; do FIND_LINKS+=(--find-links "$_w"); done
    "$PIP" install --no-index "${FIND_LINKS[@]}" --upgrade pip
    "$PIP" install --no-index "${FIND_LINKS[@]}" -r "$REQ"
else
    "$PIP" install --upgrade pip
    "$PIP" install -r "$REQ"
fi

echo "==> 설정 파일 배치 -> $CONF_DIR"
mkdir -p "$CONF_DIR" "$LOG_DIR" "$RUN_DIR" "$BIN_DIR"
if [[ ! -f "$CONF_DIR/config.properties" ]]; then
    cp "$APP_HOME/packaging/config/config.properties" "$CONF_DIR/config.properties"
    # 로그 경로를 /data1 절대 경로로 고정(개발 기본값 logs -> $LOG_DIR)
    sed -i "s|^log.dir=.*|log.dir=$LOG_DIR|" "$CONF_DIR/config.properties"
fi
[[ -f "$CONF_DIR/config.yml" ]] || cp "$APP_HOME/packaging/config/config.yml" "$CONF_DIR/config.yml"

# Impala TLS 자리표시 파일(실제 파일로 교체할 것). Greenplum 은 TLS 미적용.
[[ -f "$CONF_DIR/impala-ca.pem" ]] || printf '# TLS CA 인증서(PEM) 자리표시 — 실제 Impala CA 로 교체할 것\n' > "$CONF_DIR/impala-ca.pem"

echo "==> 런처 스크립트 배치 -> $BIN_DIR"
cp "$APP_HOME"/deploy/bin/*.sh "$BIN_DIR"/
chmod +x "$BIN_DIR"/*.sh

echo "==> 소유권/권한"
chown -R "$APP_USER:$APP_USER" "$APP_BASE"
chmod 750 "$CONF_DIR"
chmod 640 "$CONF_DIR"/config.properties "$CONF_DIR"/config.yml "$CONF_DIR"/impala-ca.pem

cat <<EOF

설치 완료(/data1 트리). 다음을 확인/수정하세요:
  - 설정:        $CONF_DIR/config.properties , $CONF_DIR/config.yml
  - Impala TLS:  $CONF_DIR/impala-ca.pem        (실제 CA 인증서로 교체)

기동/중지/상태(전체):
  sudo -u $APP_USER $BIN_DIR/start.sh
  sudo -u $APP_USER $BIN_DIR/stop.sh
  sudo -u $APP_USER $BIN_DIR/status.sh

역할별(coordinator / executor 구분):
  sudo -u $APP_USER $BIN_DIR/start-coordinator.sh
  sudo -u $APP_USER $BIN_DIR/start-executor.sh [PORT...]   # 포트 생략 시 EXECUTOR_PORTS 전체
  sudo -u $APP_USER $BIN_DIR/stop-coordinator.sh
  sudo -u $APP_USER $BIN_DIR/stop-executor.sh  [PORT...]
  sudo -u $APP_USER $BIN_DIR/status-coordinator.sh
  sudo -u $APP_USER $BIN_DIR/status-executor.sh
EOF
