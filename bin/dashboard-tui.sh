#!/usr/bin/env bash
# coordinator 대시보드를 터미널에서 보는 읽기 전용 모니터(curses)를 띄운다.
# 배포 트리(/data1)와 소스 트리(개발) 양쪽에서 동작한다.
#   예) dashboard-tui.sh                                  # 설정에서 coordinator URL 유추
#       dashboard-tui.sh --url http://127.0.0.1:8088      # URL 직접 지정
#       dashboard-tui.sh --interval 1                     # 새로고침 주기(초)
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/.." && pwd)"

# 파이썬: 배포 트리 .venv 우선, 없으면 소스 트리 .venv, 없으면 python3.
if [[ -x "/data1/distributed-query-executor/.venv/bin/python" ]]; then
    PY="/data1/distributed-query-executor/.venv/bin/python"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
    PY="$ROOT/.venv/bin/python"
else
    PY="$(command -v python3)"
fi

# src 레이아웃이므로 PYTHONPATH 에 src 를 넣어야 coordinator/core 모듈을 찾는다.
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

# 설정 디렉터리 기본값(URL 유추에 사용): 환경변수 → 배포 config → 소스 config.
if [[ -z "${QUERY_EXECUTOR_CONFIG_DIR:-}" ]]; then
    if [[ -f "/data1/distributed-query-executor/config/config.yml" ]]; then
        export QUERY_EXECUTOR_CONFIG_DIR="/data1/distributed-query-executor/config"
    else
        export QUERY_EXECUTOR_CONFIG_DIR="$ROOT/config"
    fi
fi

exec "$PY" -m coordinator.tui "$@"
