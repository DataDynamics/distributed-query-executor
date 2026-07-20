#!/usr/bin/env bash
# 전체 상태: coordinator + executor.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$DIR/status-coordinator.sh"
"$DIR/status-executor.sh"
