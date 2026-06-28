#!/usr/bin/env bash
# coordinator 만 중지한다.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

stop_proc "coordinator"
