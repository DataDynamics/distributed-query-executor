#!/usr/bin/env bash
# config.properties 를 편집하는 curses 설정 TUI 를 띄운다(스키마는 config.yml 에서 자동).
# 배포 트리(/data1)와 소스 트리(개발) 양쪽에서 동작한다.
#   예) config-tui.sh                         # 기본 설정 디렉터리
#       config-tui.sh --config-dir conf       # 특정 디렉터리
#       QUERY_EXECUTOR_CONFIG_DIR=conf config-tui.sh
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

# src 레이아웃이므로 PYTHONPATH 에 src 를 넣어야 core 모듈을 찾는다.
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

# 설정 디렉터리 기본값: 환경변수 → 배포 config → 소스 conf.
if [[ -z "${QUERY_EXECUTOR_CONFIG_DIR:-}" ]]; then
    if [[ -f "/data1/distributed-query-executor/config/config.yml" ]]; then
        export QUERY_EXECUTOR_CONFIG_DIR="/data1/distributed-query-executor/config"
    else
        export QUERY_EXECUTOR_CONFIG_DIR="$ROOT/conf"
    fi
fi

exec "$PY" -m core.config_tui "$@"
