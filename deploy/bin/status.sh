#!/usr/bin/env bash
# 프로세스 상태 + health 엔드포인트 확인.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

for pidf in "$RUN_DIR"/coordinator.pid "$RUN_DIR"/executor-*.pid; do
    [[ -e "$pidf" ]] || continue
    pid="$(cat "$pidf" 2>/dev/null)"; name="$(basename "$pidf" .pid)"
    if kill -0 "$pid" 2>/dev/null; then echo "$name: RUNNING (pid $pid)"; else echo "$name: STOPPED"; fi
done

curl -s -o /dev/null -w "coordinator health: %{http_code}\n" "http://127.0.0.1:$COORDINATOR_PORT/health" 2>/dev/null || true
for port in $EXECUTOR_PORTS; do
    curl -s -o /dev/null -w "executor $port health: %{http_code}\n" "http://127.0.0.1:$port/health" 2>/dev/null || true
done
