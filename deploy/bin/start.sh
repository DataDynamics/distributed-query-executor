#!/usr/bin/env bash
# coordinator + executor 들을 nohup 백그라운드로 기동하고 PID 를 run/ 에 기록한다.
# (systemd 시스템 유닛을 쓸 수 없는 환경용 경량 런처)
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

start_one() {
    local name="$1"; shift
    local pidf="$RUN_DIR/$name.pid"
    if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf" 2>/dev/null)" 2>/dev/null; then
        echo "이미 실행 중: $name (pid $(cat "$pidf"))"; return 0
    fi
    nohup "$@" >>"$LOG_DIR/$name.out" 2>&1 &
    local pid=$!
    echo "$pid" > "$pidf"
    echo "기동: $name (pid $pid)"
}

# executor 먼저 기동(coordinator 가 디스패치할 대상)
for port in $EXECUTOR_PORTS; do
    EXECUTOR_PORT="$port" EXECUTOR_INSTANCE="$port" \
        start_one "executor-$port" "$VENV_PY" -m executor
done
# coordinator
start_one "coordinator" "$VENV_PY" -m coordinator
