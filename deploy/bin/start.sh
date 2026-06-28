#!/usr/bin/env bash
# 전체 기동: executor(전부) 먼저 → coordinator. 역할별 제어는 start-executor.sh /
# start-coordinator.sh 를 직접 쓴다.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$DIR/start-executor.sh"
"$DIR/start-coordinator.sh"
