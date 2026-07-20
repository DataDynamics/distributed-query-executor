#!/usr/bin/env bash
# coordinator 만 기동한다.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

start_proc "coordinator" "$VENV_PY" -m coordinator
