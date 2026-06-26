"""Executor 실행 진입점: 설정 기반으로 로깅 구성 후 uvicorn 기동.

포트는 인스턴스마다 다르므로 환경변수 EXECUTOR_PORT 로 지정한다(systemd 템플릿의 %i).
인스턴스 식별자(EXECUTOR_INSTANCE)가 있으면 로그 파일명에 포함해 인스턴스별로 분리한다.

사용:  EXECUTOR_PORT=8001 python -m executor
"""

import os
from pathlib import Path

import uvicorn

from core.config import settings
from core.logging import setup_logging


def _log_filename() -> str:
    """인스턴스 식별자가 있으면 로그 파일명에 끼워 넣는다."""
    base = settings.executor_log_filename
    instance = os.getenv("EXECUTOR_INSTANCE") or os.getenv("EXECUTOR_PORT")
    if not instance:
        return base
    stem, ext = Path(base).stem, Path(base).suffix or ".log"
    return f"{stem}-{instance}{ext}"


def main() -> None:
    setup_logging(
        program_name="query-executor-server",
        filename=_log_filename(),
    )
    port = int(os.getenv("EXECUTOR_PORT", "8001"))
    uvicorn.run(
        "executor.app:app",
        host=settings.executor_host,
        port=port,
        workers=1,  # 인메모리 Task 저장소 → 인스턴스당 단일 워커
        log_config=None,
    )


if __name__ == "__main__":
    main()
