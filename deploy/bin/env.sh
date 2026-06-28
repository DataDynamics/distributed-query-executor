#!/usr/bin/env bash
# 런처 공통 환경(/appuser 트리). start/stop/status/kinit-renew 가 source 한다.
# 시스템 디렉터리(/etc·/opt·/var)를 쓰지 않고 모든 경로를 /appuser 아래에 둔다.
APP_HOME="/appuser/query-executor"
VENV_PY="$APP_HOME/.venv/bin/python"
LOG_DIR="$APP_HOME/logs"
RUN_DIR="$APP_HOME/run"

# 소스 트리에서 직접 실행하므로(패키지 미설치) coordinator/executor 모듈을 찾도록 경로 지정
export PYTHONPATH="$APP_HOME${PYTHONPATH:+:$PYTHONPATH}"
cd "$APP_HOME"

# 설정 디렉터리(코드 기본값과 동일하지만 명시적으로 고정)
export QUERY_EXECUTOR_CONFIG_DIR="$APP_HOME/config"
# Impala Kerberos: 시스템 /etc/krb5.conf 대신 /appuser 아래 설정과 공유 ccache 사용
export KRB5_CONFIG="$APP_HOME/config/krb5.conf"
export KRB5CCNAME="FILE:$RUN_DIR/krb5cc"

# 기동할 executor 포트(config 의 coordinator.executors 와 일치시킬 것)
EXECUTOR_PORTS="${EXECUTOR_PORTS:-8087 8086}"
# coordinator health 확인용 포트(config 의 coordinator.port 와 일치)
COORDINATOR_PORT="${COORDINATOR_PORT:-8088}"

mkdir -p "$LOG_DIR" "$RUN_DIR"
