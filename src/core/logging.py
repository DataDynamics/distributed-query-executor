"""일별 파일 롤링과 WARNING 로그 분리, job/task 컨텍스트 주입을 제공한다."""

# Python 3.9 호환: PEP 604 (``X | None``) 유니언을 함수 시그니처에서 쓰므로,
# 어노테이션 평가를 지연(문자열화)시켜 3.9 에서도 런타임 오류 없이 동작하게 한다.
from __future__ import annotations

import contextvars
import logging
import re
from datetime import datetime
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
    """블록과 await 체인에 식별자를 주입하고 예외가 나도 이전 값을 복원한다.

    ``task_id=None``이면 기존 task 컨텍스트를 유지한다.
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
    """호출 시점의 컨텍스트를 복사해 ``fn(*args, **kwargs)``를 실행할 콜러블을 만든다.

    컨텍스트를 복사하지 않는 ``run_in_executor``에 넘겨 스레드 로그의 job/task를 유지한다.
    반환값과 예외는 원래 함수의 것을 그대로 전달한다.
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
# asctime 의 날짜와 시간 표기 형식이다. 밀리초는 포맷 문자열에서 .%(msecs)03d 로 따로 붙인다.
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _warn_filename(filename: str, suffix: str) -> str:
    """확장자 앞에 suffix를 붙인다. 확장자가 없으면 .log를 쓴다."""
    p = Path(filename)
    return f"{p.stem}{suffix}{p.suffix or '.log'}"


class _ProgramNameFilter(logging.Filter):
    """모든 레코드에 서비스 식별자를 붙이고 통과시킨다."""

    def __init__(self, program_name: str) -> None:
        super().__init__()
        self.program_name = program_name

    def filter(self, record: logging.LogRecord) -> bool:
        # 레코드를 버리지 않고 속성만 덧붙인 뒤 통과시킨다.
        record.programname = self.program_name  # type: ignore[attr-defined]
        return True


class _DailyFileHandler(TimedRotatingFileHandler):
    """자정마다 ``stem_YYYYMMDD.확장자``로 롤링하고 보관 개수를 제한한다."""

    def __init__(self, log_dir: Path, filename: str, backup_count: int) -> None:
        log_file = log_dir / filename
        super().__init__(
            filename=str(log_file),
            when="midnight",      # 자정마다 롤링한다
            interval=1,
            backupCount=backup_count,  # 보관할 과거 파일 개수이며 넘치면 자동으로 지운다
            encoding="utf-8",
        )
        # 부모가 롤링 시 붙이는 날짜 접미사 형식. namer 에서 이 값을 다시 파싱해 쓴다.
        self.suffix = "%Y%m%d"
        self._log_dir = Path(self.baseFilename).parent
        # 보관 파일명을 재조립할 때 쓸 원본 파일명의 stem/확장자.
        self._base_stem = Path(filename).stem
        self._base_ext = Path(filename).suffix or ".log"
        # 롤링된 파일의 최종 경로와 이름을 정하는 콜백을 우리 구현으로 바꾼다.
        self.namer = self._namer

    def _namer(self, default_name: str) -> str:
        """부모의 날짜 접미사를 stem과 확장자 사이로 옮긴다."""
        parts = default_name.rsplit(".", 1)
        if len(parts) == 2:
            date_suffix = parts[1]
            return str(self._log_dir / f"{self._base_stem}_{date_suffix}{self._base_ext}")
        return default_name


    def getFilesToDelete(self) -> list[str]:
        """이 핸들러의 유효한 날짜 보관 파일만 오래된 순으로 골라 보관 한도를 맞춘다."""
        if self.backupCount <= 0:
            return []
        pattern = re.compile(
            re.escape(self._base_stem) + r"_([0-9]{8})" + re.escape(self._base_ext)
        )
        archives = []
        for path in self._log_dir.iterdir():
            match = pattern.fullmatch(path.name)
            if not match or not path.is_file() or path.is_symlink():
                continue
            try:
                datetime.strptime(match.group(1), "%Y%m%d")
            except ValueError:
                continue
            archives.append(str(path))
        archives.sort()
        return archives[:-self.backupCount]


def setup_logging(program_name: str, filename: str, settings=default_settings) -> None:
    """설정에 따라 루트 로거의 파일 핸들러를 구성하고 식별자 팩토리를 설치한다.

    기동 시 호출하며 기존 핸들러는 교체한다. WARNING 로그는 별도 레벨을 적용하고
    httpx/httpcore 로그는 WARNING 이상으로 제한한다.
    """
    log_dir = settings.log_dir
    # 로그 디렉터리가 없으면 상위 경로까지 만든다. 이미 있으면 그냥 넘어간다.
    log_dir.mkdir(parents=True, exist_ok=True)

    # 모든 LogRecord 에 job_id 와 task_id 속성을 주입한다. 기존 팩토리를 _base_factory 로
    # 감싸 표준 레코드 생성 동작은 유지하면서 현재 컨텍스트의 식별자만 덧붙인다. 컨텍스트가
    # 없으면 ContextVar 기본값인 "-" 가 들어간다.
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

    # WARNING 이상만 모으는 별도 롤링 파일을 둔다. INFO 로그와 분리하고 포맷을 강화해 문제만 빠르게 추적한다.
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
