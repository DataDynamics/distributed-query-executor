#!/usr/bin/env bash
# executor 프로세스 상태 + health(EXECUTOR_PORTS 기준).
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

shopt -s nullglob
found=0
for pidf in "$RUN_DIR"/executor-*.pid; do
    proc_status "$(basename "$pidf" .pid)"; found=1
done
shopt -u nullglob
[[ $found -eq 0 ]] && echo "executor: (실행 중 인스턴스 없음)"

for port in $EXECUTOR_PORTS; do
    health_check "executor-$port" "$port"
done
