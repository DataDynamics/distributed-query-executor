#!/usr/bin/env bash
# executor 를 재기동한다(중지 → 종료 대기 → 기동). 포트를 인자로 주면 그 포트만.
# 인자가 없으면 현재 실행 중인 executor 들을, 실행 중이 없으면 설정된 EXECUTOR_PORTS 를 대상으로 한다.
#   예) restart-executor.sh          # 실행 중 executor 전부(없으면 EXECUTOR_PORTS)
#       restart-executor.sh 8087     # 8087 만
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

PORTS=("$@")
if [[ ${#PORTS[@]} -eq 0 ]]; then
    # 인자 없음: 실행 중인 executor 포트들을 모은다(executor-<port>.pid).
    shopt -s nullglob
    running=()
    for pidf in "$RUN_DIR"/executor-*.pid; do
        name="$(basename "$pidf" .pid)"
        running+=("${name#executor-}")
    done
    shopt -u nullglob
    if [[ ${#running[@]} -gt 0 ]]; then
        PORTS=("${running[@]}")
    else
        PORTS=($EXECUTOR_PORTS)   # 실행 중이 없으면 설정된 포트 집합으로 기동
    fi
fi

# 중지 전에 대상 포트의 현재 PID 를 확보한다(stop 이 PID 파일을 지우므로).
pids=()
for port in "${PORTS[@]}"; do
    p="$(cat "$RUN_DIR/executor-$port.pid" 2>/dev/null || true)"
    [[ -n "$p" ]] && pids+=("$p")
done

"$DIR/stop-executor.sh" "${PORTS[@]}"
for p in "${pids[@]}"; do wait_pid_gone "$p"; done
"$DIR/start-executor.sh" "${PORTS[@]}"
