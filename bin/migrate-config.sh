#!/usr/bin/env bash
# 업그레이드용 설정 마이그레이션: 기존 설치 경로의 config.properties 에서 운영자가 바꾼
# 값(새 기본값과 다르거나 새 파일에 없는 키)을 찾아, 새 버전 기본 설정(config/config.properties)
# 위에 얹어 다시 설치 경로에 기록한다(.bak 백업, 주석·순서 보존).
#   예) migrate-config.sh --dry-run             # 무엇이 적용될지 보고만
#       migrate-config.sh                       # 설치 설정 제자리 갱신
#       migrate-config.sh --old /path/a.properties --out /tmp/merged.properties
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

exec "$PY" -m core.config_migrate "$@"
