"""Executor 실행 진입점: 설정 기반으로 로깅 구성 후 uvicorn 기동.

포트는 인스턴스마다 다르므로 환경변수 EXECUTOR_PORT 로 지정한다(systemd 템플릿의 %i).
인스턴스 식별자(EXECUTOR_INSTANCE)가 있으면 로그 파일명에 포함해 인스턴스별로 분리한다.

사용:  EXECUTOR_PORT=8087 python -m executor
"""

import logging
import os
import sys
from pathlib import Path

import uvicorn

from core.banner import print_banner, print_config_sources
from core.config import settings
from core.logging import setup_logging
from core.version import version_string


def _log_filename() -> str:
    """인스턴스 식별자가 있으면 로그 파일명에 끼워 넣는다.

    한 호스트에서 여러 executor 인스턴스를 띄울 때 로그가 한 파일에 섞이지 않도록,
    EXECUTOR_INSTANCE(또는 EXECUTOR_PORT)를 파일명 stem 뒤에 붙인다
    (예: ``executor.log`` → ``executor-8087.log``). 식별자가 없으면 원본 파일명 그대로.
    """
    base = settings.executor_log_filename
    instance = os.getenv("EXECUTOR_INSTANCE") or os.getenv("EXECUTOR_PORT")
    if not instance:
        return base
    stem, ext = Path(base).stem, Path(base).suffix or ".log"
    return f"{stem}-{instance}{ext}"


def main() -> None:
    """로깅을 구성하고 uvicorn 으로 executor 앱을 기동한다.

    수신 포트는 EXECUTOR_PORT(기본 8087)로 인스턴스마다 달리 지정한다. 인메모리 task
    저장소를 쓰므로 워커는 인스턴스당 1개로 고정한다(여러 워커면 상태가 분산돼 깨진다).
    uvicorn 자체 로그 설정은 비활성(log_config=None)해 우리 로깅 구성을 그대로 쓴다.
    """
    # `--version`: 버전만 출력하고 종료(기동하지 않음).
    if "--version" in sys.argv[1:]:
        print(f"query-executor {version_string()}")
        return

    port = int(os.getenv("EXECUTOR_PORT", "8087"))
    # Spring Boot 처럼 기동 배너를 콘솔에 먼저 찍는다(버전·역할·포트 표시).
    print_banner("executor", port)
    # 배너 바로 아래에 실제 로딩한 설정 파일의 절대 경로(+ 존재 여부)를 찍어,
    # 설정이 제대로 로딩됐는지 콘솔에서 바로 확인할 수 있게 한다.
    print_config_sources(settings.config_properties_path, settings.config_yaml_path)

    setup_logging(
        program_name="query-executor-server",
        filename=_log_filename(),
    )
    logging.getLogger(__name__).info(
        "executor 기동 — version=%s port=%s config_properties=%s config_yaml=%s",
        version_string(), port,
        settings.config_properties_path.resolve(), settings.config_yaml_path.resolve(),
    )
    uvicorn.run(
        "executor.app:app",
        host=settings.executor_host,
        port=port,
        workers=1,  # 인메모리 Task 저장소 → 인스턴스당 단일 워커
        log_config=None,
        # 순수 파이썬 asyncio 루프 고정(uvloop 미사용). 커스텀 쿼리 함수(query.func.module)가
        # nest_asyncio 등으로 루프를 패치하는 라이브러리를 쓸 수 있는데, Cython 구현인
        # uvloop 루프는 패치가 불가능해 "can't patch loop of type uvloop.Loop" 로 죽는다.
        # executor 의 데이터 경로(Impala→GP)는 스레드에서 돌고 루프는 제어 트래픽만
        # 처리하므로(초당 수십 요청 이하) uvloop 를 포기해도 성능 차이는 무시할 수준이다.
        loop="asyncio",
    )


if __name__ == "__main__":
    main()
