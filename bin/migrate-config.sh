#!/usr/bin/env bash
# 업그레이드용 마이그레이션: 새 버전 소스 트리를 기준으로 설치 트리의 운영자 자산을 반영한다.
#   - config/  : config.properties 는 운영자 변경분만 새 기본값 위에 병합, config.yml·스키마는
#                새 버전으로 교체(.bak 백업) → 새 버전이 추가한 설정 구조가 제대로 반영된다.
#   - templates/ : 예제 템플릿 반영(바뀐 파일 .bak), 운영자 추가 템플릿은 보존.
#   - customs/   : 커스텀 쿼리 함수 예제 반영, 운영자 모듈은 보존.
# 설치 트리에만 있는 파일(운영자 자산)은 절대 삭제하지 않는다.
#   예) migrate-config.sh --dry-run             # 무엇이 반영될지 보고만
#       migrate-config.sh                       # config+templates+customs 제자리 반영
#       migrate-config.sh --old /path/a.properties --out /tmp/merged.properties  # 단일 파일 병합(하위 호환)
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
