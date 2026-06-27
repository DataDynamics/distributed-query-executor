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
    """인스턴스 식별자가 있으면 로그 파일명에 끼워 넣는다.

    한 호스트에서 여러 executor 인스턴스를 띄울 때 로그가 한 파일에 섞이지 않도록,
    EXECUTOR_INSTANCE(또는 EXECUTOR_PORT)를 파일명 stem 뒤에 붙인다
    (예: ``executor.log`` → ``executor-8001.log``). 식별자가 없으면 원본 파일명 그대로.
    """
    base = settings.executor_log_filename
    instance = os.getenv("EXECUTOR_INSTANCE") or os.getenv("EXECUTOR_PORT")
    if not instance:
        return base
    stem, ext = Path(base).stem, Path(base).suffix or ".log"
    return f"{stem}-{instance}{ext}"


def main() -> None:
    """로깅을 구성하고 uvicorn 으로 executor 앱을 기동한다.

    수신 포트는 EXECUTOR_PORT(기본 8001)로 인스턴스마다 달리 지정한다. 인메모리 task
    저장소를 쓰므로 워커는 인스턴스당 1개로 고정한다(여러 워커면 상태가 분산돼 깨진다).
    uvicorn 자체 로그 설정은 비활성(log_config=None)해 우리 로깅 구성을 그대로 쓴다.
    """
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
