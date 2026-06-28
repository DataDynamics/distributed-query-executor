#!/usr/bin/env bash
# RHEL 9.2용 설치 스크립트(에어갭 + /appuser 단일 트리).
# 보안 정책상 /etc·/opt·/var 에 파일을 추가하지 않는다. 애플리케이션·설정·로그·런타임을
# 모두 /appuser/query-executor 아래에 배치하고, systemd 시스템 유닛 대신 런처 스크립트로 구동한다.
# 사용법:  sudo ./deploy/install.sh
set -euo pipefail

# ── 경로(모두 /appuser 아래) ──────────────────────────────────────────────
APP_USER="${APP_USER:-appuser}"            # 서비스 계정(없으면 생성, 홈=/appuser)
APP_BASE="/appuser"
APP_HOME="$APP_BASE/query-executor"
CONF_DIR="$APP_HOME/config"                # config.{properties,yml}, krb5.conf, 인증서/키탭
LOG_DIR="$APP_HOME/logs"
RUN_DIR="$APP_HOME/run"                     # PID, Kerberos ccache
BIN_DIR="$APP_HOME/bin"                     # 런처 스크립트
VENV="$APP_HOME/.venv"

# RHEL 9.2 기본 Python 은 3.9 이다. 별도 설치 없이 시스템 python3.9 를 그대로 쓴다.
PYTHON="${PYTHON:-python3.9}"
# 에어갭(인터넷 차단) 설치: WHEELHOUSE 에 미리 받아 둔 wheel 디렉터리를 지정하면
# PyPI 대신 그 디렉터리에서만(--no-index) 설치한다. 비우면 pip 기본 인덱스(Nexus 등) 사용.
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
    # 로그 경로를 /appuser 절대 경로로 고정(개발 기본값 logs -> $LOG_DIR)
    sed -i "s|^log.dir=.*|log.dir=$LOG_DIR|" "$CONF_DIR/config.properties"
fi
[[ -f "$CONF_DIR/config.yml" ]] || cp "$APP_HOME/packaging/config/config.yml" "$CONF_DIR/config.yml"

# Impala Kerberos+TLS 자리표시 파일(실제 파일로 교체할 것). Greenplum 은 TLS/Kerberos 미적용.
if [[ ! -f "$CONF_DIR/krb5.conf" ]]; then
    cat > "$CONF_DIR/krb5.conf" <<'KRB'
# Impala Kerberos 용 krb5 설정(KRB5_CONFIG 로 사용). 실제 realm/KDC 로 교체할 것.
[libdefaults]
    default_realm = EXAMPLE.LOCAL
    dns_lookup_realm = false
    dns_lookup_kdc = false
    rdns = false

[realms]
    EXAMPLE.LOCAL = {
        kdc = kdc.example.local
        admin_server = kdc.example.local
    }

[domain_realm]
    .example.local = EXAMPLE.LOCAL
    example.local = EXAMPLE.LOCAL
KRB
fi
[[ -f "$CONF_DIR/impala-ca.pem" ]] || printf '# TLS CA 인증서(PEM) 자리표시 — 실제 Impala CA 로 교체할 것\n' > "$CONF_DIR/impala-ca.pem"
[[ -f "$CONF_DIR/impala.keytab" ]] || printf '' > "$CONF_DIR/impala.keytab"

echo "==> 런처 스크립트 배치 -> $BIN_DIR"
cp "$APP_HOME"/deploy/bin/*.sh "$BIN_DIR"/
chmod +x "$BIN_DIR"/*.sh

echo "==> 소유권/권한"
chown -R "$APP_USER:$APP_USER" "$APP_BASE"
chmod 750 "$CONF_DIR"
chmod 640 "$CONF_DIR"/config.properties "$CONF_DIR"/config.yml "$CONF_DIR"/krb5.conf "$CONF_DIR"/impala-ca.pem
chmod 600 "$CONF_DIR"/impala.keytab

cat <<EOF

설치 완료(/appuser 트리). 다음을 확인/수정하세요:
  - 설정:        $CONF_DIR/config.properties , $CONF_DIR/config.yml
  - Impala TLS:  $CONF_DIR/impala-ca.pem        (실제 CA 인증서로 교체)
  - Kerberos:    $CONF_DIR/impala.keytab        (실제 keytab 으로 교체, 권한 600)
                 $CONF_DIR/krb5.conf            (realm/KDC 수정)
                 config.properties 의 impala.host / krb5.principal / keytab 경로 확인

Kerberos 티켓 발급(이후 cron 등으로 주기 갱신):
  sudo -u $APP_USER $BIN_DIR/kinit-renew.sh

기동/중지/상태:
  sudo -u $APP_USER $BIN_DIR/start.sh
  sudo -u $APP_USER $BIN_DIR/stop.sh
  sudo -u $APP_USER $BIN_DIR/status.sh
EOF
