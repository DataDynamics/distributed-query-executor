#!/usr/bin/env bash
# 전체 재기동: 모든 프로세스 중지 → 종료 대기 → 기동. 역할별 제어는 restart-coordinator.sh /
# restart-executor.sh 를 직접 쓴다(중지는 coordinator→executor, 기동은 executor→coordinator 순).
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

# 중지 전에 실행 중인 모든 PID 를 확보한다(stop 이 PID 파일을 지우므로).
pids=()
for pidf in "$RUN_DIR"/coordinator.pid "$RUN_DIR"/executor-*.pid; do
    [[ -f "$pidf" ]] || continue
    p="$(cat "$pidf" 2>/dev/null || true)"
    [[ -n "$p" ]] && pids+=("$p")
done

"$DIR/stop.sh"
for p in "${pids[@]}"; do wait_pid_gone "$p"; done
"$DIR/start.sh"
