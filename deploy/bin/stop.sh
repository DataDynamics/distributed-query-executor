#!/usr/bin/env bash
# run/ 의 PID 파일을 이용해 coordinator + executor 들을 중지한다.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

for pidf in "$RUN_DIR"/coordinator.pid "$RUN_DIR"/executor-*.pid; do
    [[ -e "$pidf" ]] || continue
    pid="$(cat "$pidf" 2>/dev/null || true)"
    name="$(basename "$pidf" .pid)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" && echo "중지: $name (pid $pid)"
    else
        echo "미실행: $name"
    fi
    rm -f "$pidf"
done
