"""일 단위 롤링 파일 핸들러를 사용하는 로깅 설정.

(argus-catalog backend의 logging 구조와 동일한 방식: 파일명_YYYYMMDD.log 롤링)
"""

import contextvars
import logging
from contextlib import contextmanager
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from core.config import settings as default_settings

# 현재 처리 중인 job_id / task_id (없으면 "-"). 로그에 [job_id][task_id] 로 자동 주입된다.
job_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("job_id", default="-")
task_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("task_id", default="-")


@contextmanager
def job_log_context(job_id: str, task_id: str | None = None):
    """이 블록(및 await 체인) 안의 모든 로그에 job_id(및 task_id)를 붙인다."""
    jtok = job_id_var.set(job_id)
    ttok = task_id_var.set(task_id) if task_id is not None else None
    try:
        yield
    finally:
        if ttok is not None:
            task_id_var.reset(ttok)
        job_id_var.reset(jtok)


LOG_FORMAT = (
    "%(levelname)s %(asctime)s.%(msecs)03d %(process)d %(programname)s"
    " %(filename)s:%(funcName)s:%(lineno)d [%(job_id)s][%(task_id)s] - %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class _ProgramNameFilter(logging.Filter):
    def __init__(self, program_name: str) -> None:
        super().__init__()
        self.program_name = program_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.programname = self.program_name  # type: ignore[attr-defined]
        return True


class _DailyFileHandler(TimedRotatingFileHandler):
    def __init__(self, log_dir: Path, filename: str, backup_count: int) -> None:
        log_file = log_dir / filename
        super().__init__(
            filename=str(log_file),
            when="midnight",
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self.suffix = "%Y%m%d"
        self._log_dir = log_dir
        self._base_stem = Path(filename).stem
        self._base_ext = Path(filename).suffix or ".log"
        self.namer = self._namer

    def _namer(self, default_name: str) -> str:
        parts = default_name.rsplit(".", 1)
        if len(parts) == 2:
            date_suffix = parts[1]
            return str(self._log_dir / f"{self._base_stem}_{date_suffix}{self._base_ext}")
        return default_name


def setup_logging(program_name: str, filename: str, settings=default_settings) -> None:
    """일 단위 롤링 파일 로깅을 구성한다.

    program_name: 로그의 programname 필드(서비스 식별자)
    filename: 로그 파일명(예: query-coordinator-server.log)
    """
    log_dir = settings.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    # 모든 LogRecord 에 job_id 속성을 주입(현재 컨텍스트의 job_id, 없으면 "-")
    _base_factory = logging.getLogRecordFactory()

    def _record_factory(*args, **kwargs):
        record = _base_factory(*args, **kwargs)
        record.job_id = job_id_var.get()
        record.task_id = task_id_var.get()
        return record

    logging.setLogRecordFactory(_record_factory)

    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
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
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)

    # uvicorn/httpx 등의 과도한 로그를 레벨에 맞춰 정리
    logging.getLogger("uvicorn.access").setLevel(
        logging.INFO if log_level <= logging.INFO else logging.WARNING
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
