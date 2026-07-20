#!/usr/bin/env bash
# coordinator 프로세스 상태 + health.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

proc_status "coordinator"
health_check "coordinator" "$COORDINATOR_PORT"
