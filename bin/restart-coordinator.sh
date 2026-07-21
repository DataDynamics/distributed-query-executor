#!/usr/bin/env bash
# coordinator 만 재기동한다(중지 → 종료 대기 → 기동).
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

# 중지 전에 현재 PID 를 확보한다(stop 이 PID 파일을 지우므로).
pid="$(cat "$RUN_DIR/coordinator.pid" 2>/dev/null || true)"

"$DIR/stop-coordinator.sh"
wait_pid_gone "$pid"
"$DIR/start-coordinator.sh"
