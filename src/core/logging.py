"""일 단위 롤링 파일 핸들러를 사용하는 로깅 설정 모듈.

이 모듈은 coordinator/executor 공통의 파일 로깅을 구성한다. 핵심 기능은 세 가지다:

1. **일 단위 롤링**: 자정마다 로그 파일을 넘기고, 지난 파일은
   ``파일명_YYYYMMDD.log`` 형태로 이름을 바꿔 보관한다(backup_count 만큼 유지).
2. **job/task 컨텍스트 자동 주입**: contextvars 로 현재 처리 중인 job_id·task_id 를
   추적해, 같은 async 체인에서 찍히는 모든 로그에 ``[job_id][task_id]`` 를 자동으로
   붙인다. 로그를 호출할 때마다 식별자를 넘기지 않아도 되게 하기 위함이다.
3. **WARNING 분리 로그**: WARNING 이상만 모으는 별도 파일을 두어, 평상시 INFO
   잡음 속에서 문제를 빠르게 추적할 수 있게 한다.

(argus-catalog backend의 logging 구조와 동일한 방식: 파일명_YYYYMMDD.log 롤링)
"""

# Python 3.9 호환: PEP 604 (``X | None``) 유니언을 함수 시그니처에서 쓰므로,
# 어노테이션 평가를 지연(문자열화)시켜 3.9 에서도 런타임 오류 없이 동작하게 한다.
from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from core.config import settings as default_settings

# 현재 처리 중인 job_id / task_id (없으면 "-"). 로그에 [job_id][task_id] 로 자동 주입된다.
# ContextVar 를 쓰는 이유: async 태스크/스레드별로 값이 독립적으로 유지되어,
# 동시에 여러 job 을 처리해도 로그의 식별자가 서로 섞이지 않는다.
job_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("job_id", default="-")
task_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("task_id", default="-")


@contextmanager
def job_log_context(job_id: str, task_id: str | None = None):
    """``with`` 블록(및 그 안의 await 체인) 동안 로그에 job_id/task_id 를 붙이는 컨텍스트 매니저.

    인자:
        job_id: 이 블록 동안 로그에 표기할 job 식별자.
        task_id: task 식별자(선택). None 이면 기존 task_id 컨텍스트를 건드리지 않는다.

    동작:
        진입 시 ContextVar 에 값을 set 하고, 그때 받은 **토큰을 finally 에서 reset**
        한다. 토큰 기반 reset 은 중첩 사용 시 이전 값을 정확히 복원해 주므로,
        블록을 빠져나오면 바깥 컨텍스트의 식별자가 그대로 살아난다.
        task_id 가 None 이면 토큰을 만들지 않아(ttok=None) 기존 값을 보존한다.
        예외가 나도 식별자가 누수되지 않도록 reset 은 finally 에서 보장한다.
    """
    jtok = job_id_var.set(job_id)
    ttok = task_id_var.set(task_id) if task_id is not None else None
    try:
        yield
    finally:
        # set 의 역순으로 복원: 안쪽에서 바꾼 task_id 부터 되돌리고 job_id 를 되돌린다.
        if ttok is not None:
            task_id_var.reset(ttok)
        job_id_var.reset(jtok)


def with_log_context(fn, *args, **kwargs):
    """현재 로그 컨텍스트(job_id/task_id)를 **다른 스레드까지** 들고 가는 무인자 콜러블을 만든다.

    왜 필요한가: 블로킹 DB 호출은 ``loop.run_in_executor(None, ...)`` 로 스레드 풀에
    넘기는데, ``run_in_executor`` 는 ``asyncio.to_thread`` 와 달리 **contextvars 를
    복사하지 않는다**. 그대로 넘기면 워커 스레드는 기본값 컨텍스트에서 시작하므로,
    그 안에서 찍히는 로그(특히 :mod:`core.sqllog` 의 실행 SQL)가 ``[-][-]`` 로 남아
    "이 쿼리가 어느 job/task 의 것인가"를 잃는다.

    이 함수는 **호출 시점**(= 아직 코루틴 쪽 컨텍스트)에서 컨텍스트를 복사해 두고,
    워커 스레드에서는 그 복사본 안에서 ``fn`` 을 실행하는 클로저를 돌려준다::

        rows = await loop.run_in_executor(None, with_log_context(backend.move, sql, ...))

    인자:
        fn: 스레드에서 실행할 함수.
        *args, **kwargs: ``fn`` 에 그대로 전달할 인자.

    반환:
        ``run_in_executor`` 에 넘길 수 있는 무인자 콜러블(반환값은 ``fn`` 의 반환값).
    """
    ctx = contextvars.copy_context()
    return lambda: ctx.run(fn, *args, **kwargs)


# 메인 로그 한 줄 포맷: 레벨 / 시각(ms 포함) / PID / 서비스명(programname) /
# 파일:함수:줄번호 / [job_id][task_id] / 메시지. job_id·task_id 는 record factory 가
# 모든 LogRecord 에 주입하므로 포맷 문자열에서 바로 참조할 수 있다.
LOG_FORMAT = (
    "%(levelname)s %(asctime)s.%(msecs)03d %(process)d %(programname)s"
    " %(filename)s:%(funcName)s:%(lineno)d [%(job_id)s][%(task_id)s] - %(message)s"
)
# WARNING 전용 로그는 추적에 도움되도록 로거 이름(%(name)s)을 추가로 남긴다.
WARN_LOG_FORMAT = (
    "%(levelname)s %(asctime)s.%(msecs)03d %(process)d %(programname)s"
    " %(name)s %(filename)s:%(funcName)s:%(lineno)d"
    " [%(job_id)s][%(task_id)s] - %(message)s"
)
# asctime 의 날짜/시간 표기 형식(밀리초는 포맷 문자열에서 .%(msecs)03d 로 따로 붙임).
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _warn_filename(filename: str, suffix: str) -> str:
    """메인 로그 파일명에서 stem 과 확장자 사이에 suffix 를 끼운 WARNING 로그 파일명을 만든다.

    예: ``foo.log`` + suffix ``-warn`` → ``foo-warn.log``.
    확장자가 없는 파일명이 들어와도 일관되게 ``.log`` 를 보장한다.
    """
    p = Path(filename)
    return f"{p.stem}{suffix}{p.suffix or '.log'}"


class _ProgramNameFilter(logging.Filter):
    """모든 LogRecord 에 ``programname`` 속성을 채워 넣는 로깅 필터.

    로그 포맷의 ``%(programname)s`` 자리에 서비스 식별자(coordinator/executor 등)를
    찍기 위한 것이다. 표준 LogRecord 에는 이 속성이 없으므로 필터 단계에서 주입한다.
    필터 자체는 레코드를 거르지 않고 항상 통과(return True)시킨다.
    """

    def __init__(self, program_name: str) -> None:
        super().__init__()
        self.program_name = program_name

    def filter(self, record: logging.LogRecord) -> bool:
        # 레코드를 버리지 않고 속성만 덧붙인 뒤 통과시킨다.
        record.programname = self.program_name  # type: ignore[attr-defined]
        return True


class _DailyFileHandler(TimedRotatingFileHandler):
    """자정 기준 일 단위 롤링 + 사용자 지정 보관 파일명 규칙을 적용한 파일 핸들러.

    TimedRotatingFileHandler 기본 동작은 롤링 시 ``파일.log.YYYYMMDD`` 처럼
    확장자 뒤에 날짜를 붙인다. 여기서는 namer 를 교체해 ``파일_YYYYMMDD.log`` 형태
    (stem 과 확장자 사이에 날짜)로 바꾼다 — 확장자가 .log 로 유지돼야 로그 뷰어/
    수집기가 인식하기 쉽기 때문이다.
    """

    def __init__(self, log_dir: Path, filename: str, backup_count: int) -> None:
        log_file = log_dir / filename
        super().__init__(
            filename=str(log_file),
            when="midnight",      # 자정마다 롤링
            interval=1,
            backupCount=backup_count,  # 보관할 과거 파일 개수(초과분 자동 삭제)
            encoding="utf-8",
        )
        # 부모가 롤링 시 붙이는 날짜 접미사 형식. namer 에서 이 값을 다시 파싱해 쓴다.
        self.suffix = "%Y%m%d"
        self._log_dir = log_dir
        # 보관 파일명을 재조립할 때 쓸 원본 파일명의 stem/확장자.
        self._base_stem = Path(filename).stem
        self._base_ext = Path(filename).suffix or ".log"
        # 롤링된 파일의 최종 경로/이름을 결정하는 콜백을 우리 구현으로 교체.
        self.namer = self._namer

    def _namer(self, default_name: str) -> str:
        """롤링된 파일 이름을 ``stem_YYYYMMDD.확장자`` 형태로 재구성한다.

        default_name 은 부모가 만든 ``.../foo.log.20240101`` 같은 이름이다.
        오른쪽 끝의 ``.YYYYMMDD`` 만 떼어내 날짜로 쓰고, 나머지는 우리가 보관해 둔
        stem/확장자로 다시 조립한다. 예상 형태가 아니면(분리 실패) 원본을 그대로 둔다.
        """
        parts = default_name.rsplit(".", 1)
        if len(parts) == 2:
            date_suffix = parts[1]
            return str(self._log_dir / f"{self._base_stem}_{date_suffix}{self._base_ext}")
        return default_name


def setup_logging(program_name: str, filename: str, settings=default_settings) -> None:
    """루트 로거에 일 단위 롤링 파일 로깅을 구성한다(애플리케이션 기동 시 1회 호출).

    인자:
        program_name: 로그의 ``programname`` 필드(서비스 식별자, 예: coordinator/executor).
        filename: 메인 로그 파일명(예: query-coordinator-server.log).
        settings: 로깅 관련 설정 제공자. 기본은 전역 ``settings`` 이지만,
            테스트에서 다른 설정 객체를 주입할 수 있도록 인자로 빼 두었다.

    동작 개요:
        1) 로그 디렉터리를 생성한다.
        2) LogRecord 팩토리를 교체해 job_id/task_id 를 모든 레코드에 주입한다.
        3) 메인 파일 핸들러를 만들고 루트 로거에 단다(기존 핸들러는 비운다).
        4) 설정에 따라 WARNING 전용 핸들러를 추가한다.
        5) 시끄러운 서드파티 로거(uvicorn.access, httpx) 레벨을 조정한다.
    """
    log_dir = settings.log_dir
    # 로그 디렉터리가 없으면 만든다(이미 있으면 무시). 상위 경로까지 생성.
    log_dir.mkdir(parents=True, exist_ok=True)

    # 모든 LogRecord 에 job_id/task_id 속성을 주입한다.
    # 기존 팩토리를 감싸(_base_factory) 표준 레코드 생성 동작은 유지하면서,
    # 현재 컨텍스트의 식별자만 덧붙인다. 컨텍스트가 없으면 ContextVar 기본값 "-".
    _base_factory = logging.getLogRecordFactory()

    def _record_factory(*args, **kwargs):
        record = _base_factory(*args, **kwargs)
        record.job_id = job_id_var.get()
        record.task_id = task_id_var.get()
        return record

    logging.setLogRecordFactory(_record_factory)

    # 설정의 레벨 문자열("INFO" 등)을 logging 모듈 상수로 변환. 알 수 없는 값이면 INFO.
    # app.debug=true 면 상세 추적을 위해 강제로 DEBUG 로 낮춘다(logging.level 설정보다 우선).
    # 상세 로그(단계 전이·진행률 등)는 모두 DEBUG 이므로, 평상시(INFO)에는 찍히지 않아
    # 추가 IO 가 발생하지 않고, 필요할 때만 debug 모드/레벨로 켜서 파일로 흐름을 추적한다.
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    if getattr(settings, "debug", False):
        log_level = logging.DEBUG
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    program_filter = _ProgramNameFilter(program_name)

    file_handler = _DailyFileHandler(
        log_dir=log_dir,
        filename=filename,
        backup_count=settings.log_rolling_backup_count,
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(program_filter)

    root_logger = logging.getLogger()
    # 루트 레벨은 메인 로그와 WARNING 로그 중 더 낮은(더 많이 통과시키는) 쪽으로 맞춘다.
    # 루트 레벨이 핸들러 레벨보다 높으면 레코드가 핸들러에 닿기 전에 잘리므로,
    # 메인 레벨을 WARNING 보다 높게 설정해도 WARNING 전용 로그가 비지 않도록 보장한다.
    # (각 핸들러는 자기 setLevel 로 다시 한 번 필터링하므로 파일별 레벨은 따로 지켜진다)
    warn_level = getattr(logging, settings.log_warn_level.upper(), logging.WARNING)
    root_level = min(log_level, warn_level) if settings.log_warn_enabled else log_level
    root_logger.setLevel(root_level)
    # 중복 호출/재설정 시 핸들러가 누적돼 로그가 여러 번 찍히는 것을 막기 위해 먼저 비운다.
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)

    # WARNING 이상만 모으는 별도 롤링 파일(문제만 빠르게 추적). INFO 로그와 분리·강화 포맷.
    if settings.log_warn_enabled:
        warn_handler = _DailyFileHandler(
            log_dir=log_dir,
            filename=_warn_filename(filename, settings.log_warn_suffix),
            backup_count=settings.log_rolling_backup_count,
        )
        warn_handler.setLevel(warn_level)
        warn_handler.setFormatter(logging.Formatter(fmt=WARN_LOG_FORMAT, datefmt=DATE_FORMAT))
        warn_handler.addFilter(program_filter)
        root_logger.addHandler(warn_handler)

    # uvicorn/httpx 등 서드파티의 과도한 로그를 정리한다.
    # access 로그는 메인 레벨이 INFO 이하일 때만 INFO 로 남기고, 그보다 엄격하면
    # WARNING 으로 올려 요청 단위 잡음을 줄인다. httpx/httpcore 는 항상 WARNING 이상만 남긴다
    # (DEBUG 모드에서 httpcore 의 저수준 트레이스(_trace.py: send/receive/close 등)가 로그를
    # 뒤덮지 않도록. 앱의 상세 DEBUG 로그와 분리한다).
    logging.getLogger("uvicorn.access").setLevel(
        logging.INFO if log_level <= logging.INFO else logging.WARNING
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # 기동 시 유효 로그 레벨을 한 줄 남겨, 파일만 보고도 상세(DEBUG) 추적이 켜졌는지 알 수 있게 한다.
    logging.getLogger(__name__).info(
        "로깅 구성 완료: level=%s (debug=%s), dir=%s, file=%s",
        logging.getLevelName(log_level), getattr(settings, "debug", False),
        log_dir, filename,
    )
