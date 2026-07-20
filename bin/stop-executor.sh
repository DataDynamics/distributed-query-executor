#!/usr/bin/env bash
# executor 를 중지한다. 포트를 인자로 주면 그 포트만, 없으면 실행 중인 executor 전부.
#   예) stop-executor.sh         # executor-*.pid 전부
#       stop-executor.sh 8086    # 8086 만
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

PORTS=("$@")
if [[ ${#PORTS[@]} -eq 0 ]]; then
    shopt -s nullglob
    for pidf in "$RUN_DIR"/executor-*.pid; do
        stop_proc "$(basename "$pidf" .pid)"
    done
    shopt -u nullglob
else
    for port in "${PORTS[@]}"; do stop_proc "executor-$port"; done
fi
