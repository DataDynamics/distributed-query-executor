"""Coordinator 실행 진입점: 설정 기반으로 로깅 구성 후 uvicorn 기동.

'python -m coordinator' 로 패키지를 실행하면 이 모듈의 main() 이 호출된다. main() 은
core.logging 으로 애플리케이션 로깅을 먼저 구성한 뒤, coordinator.app:app(FastAPI/ASGI 앱)을
uvicorn 으로 띄운다.

사용:  python -m coordinator
설정:  /appuser/query-executor/config/config.{properties,yml} (QUERY_EXECUTOR_CONFIG_DIR 로 변경 가능)
"""

import uvicorn

from core.config import settings
from core.logging import setup_logging


def main() -> None:
    """로깅을 구성하고 uvicorn 으로 coordinator ASGI 앱을 기동한다(블로킹)."""
    # 앱 임포트보다 먼저 로깅을 세팅해, 기동 단계 로그도 동일한 포맷/핸들러로 남도록 한다.
    setup_logging(
        program_name="query-coordinator-server",
        filename=settings.coordinator_log_filename,
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
