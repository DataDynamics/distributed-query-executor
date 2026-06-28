#!/usr/bin/env bash
# executor 를 기동한다. 포트를 인자로 주면 그 포트만, 없으면 EXECUTOR_PORTS 전체.
#   예) start-executor.sh            # 8087 8086 모두
#       start-executor.sh 8087       # 8087 만
#       start-executor.sh 8085 8084  # 임의 포트 추가 기동
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

PORTS=("$@")
[[ ${#PORTS[@]} -eq 0 ]] && PORTS=($EXECUTOR_PORTS)

for port in "${PORTS[@]}"; do
    EXECUTOR_PORT="$port" EXECUTOR_INSTANCE="$port" \
        start_proc "executor-$port" "$VENV_PY" -m executor
done
