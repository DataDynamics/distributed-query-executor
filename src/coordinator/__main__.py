"""Coordinator 실행 진입점: 설정 기반으로 로깅 구성 후 uvicorn 기동.

'python -m coordinator' 로 패키지를 실행하면 이 모듈의 main() 이 호출된다. main() 은
core.logging 으로 애플리케이션 로깅을 먼저 구성한 뒤, coordinator.app:app(FastAPI/ASGI 앱)을
uvicorn 으로 띄운다.

사용:  python -m coordinator
설정:  /data1/distributed-query-executor/config/config.{properties,yml} (QUERY_EXECUTOR_CONFIG_DIR 로 변경 가능)
"""

import logging
import sys

import uvicorn

from core.banner import print_banner
from core.config import settings
from core.logging import setup_logging
from core.version import version_string


def main() -> None:
    """로깅을 구성하고 uvicorn 으로 coordinator ASGI 앱을 기동한다(블로킹)."""
    # `--version`: 버전만 출력하고 종료(기동하지 않음).
    if "--version" in sys.argv[1:]:
        print(f"query-executor {version_string()}")
        return

    # Spring Boot 처럼 기동 배너를 콘솔에 먼저 찍는다(버전·역할 표시).
    print_banner("coordinator", settings.coordinator_port)

    # 앱 임포트보다 먼저 로깅을 세팅해, 기동 단계 로그도 동일한 포맷/핸들러로 남도록 한다.
    setup_logging(
        program_name="query-coordinator-server",
        filename=settings.coordinator_log_filename,
    )
    # 버전은 로그 파일에도 한 줄 남겨, 콘솔을 못 봐도 이력에서 확인 가능하게 한다.
    logging.getLogger(__name__).info(
        "coordinator 기동 — version=%s port=%s", version_string(), settings.coordinator_port
    )
    uvicorn.run(
        "coordinator.app:app",
        host=settings.coordinator_host,
        port=settings.coordinator_port,
        workers=1,  # 인메모리 Job 저장소 → 단일 워커
        log_config=None,  # 로깅은 core.logging 에서 구성
    )


if __name__ == "__main__":
    main()
