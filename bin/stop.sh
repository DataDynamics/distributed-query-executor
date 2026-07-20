#!/usr/bin/env bash
# 전체 중지: coordinator 먼저 → executor(전부). 역할별 제어는 stop-executor.sh /
# stop-coordinator.sh 를 직접 쓴다.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$DIR/stop-coordinator.sh"
"$DIR/stop-executor.sh"
