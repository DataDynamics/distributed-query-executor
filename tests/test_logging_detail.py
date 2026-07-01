"""상세 로깅: 스레드로 넘어가도 job/task 로그 컨텍스트가 유지되고, 단계 전이가 DEBUG 로 남는지."""

from __future__ import annotations

import asyncio
import contextvars
import logging

from core.logging import job_id_var, job_log_context, task_id_var
from executor.models import Task


def _task():
    return Task(
        task_id="t1", job_id="j1", sub_query="SELECT 1", target_table="x",
        write_mode="append", partition_column="c", partition_values=[],
    )


def test_copy_context_propagates_job_task_to_worker_thread():
    """_run/LocalDispatcher 가 쓰는 copy_context→run_in_executor 패턴이 컨텍스트를 옮기는지."""
    async def run():
        seen: dict = {}
        with job_log_context("job_x", "task_y"):
            ctx = contextvars.copy_context()
            loop = asyncio.get_running_loop()

            def work():
                seen["job"] = job_id_var.get()
                seen["task"] = task_id_var.get()

            await loop.run_in_executor(None, lambda: ctx.run(work))
        return seen

    assert asyncio.run(run()) == {"job": "job_x", "task": "task_y"}


def test_on_stage_emits_debug_logs(caplog):
    """단계 시작/종료가 DEBUG 로 남고, 종료 로그에 소요/행수가 포함되는지."""
    t = _task()
    with caplog.at_level(logging.DEBUG, logger="executor.models"):
        t.on_stage("STREAM_COPY", "start")
        t.on_stage("STREAM_COPY", "end", {"rows": 5, "read_wait_ms": 1, "write_wait_ms": 2})
    msgs = [r.getMessage() for r in caplog.records]
    assert any("단계 시작 STREAM_COPY" in m for m in msgs)
    assert any("단계 종료 STREAM_COPY" in m and "행수=5" in m for m in msgs)


def test_debug_forces_debug_log_level(tmp_path):
    """app.debug=true 면 logging.level 설정과 무관하게 파일 핸들러가 DEBUG 로 열리는지."""
    from core.logging import setup_logging

    class _S:
        log_dir = tmp_path
        log_level = "INFO"      # INFO 로 두어도
        debug = True            # debug=true 면 DEBUG 로 내려가야 한다
        log_rolling_backup_count = 3
        log_warn_enabled = False
        log_warn_level = "WARNING"
        log_warn_suffix = "-warn"

    try:
        setup_logging("test", "detail-test.log", settings=_S())
        root = logging.getLogger()
        # 루트 핸들러 중 메인 파일 핸들러가 DEBUG 레벨로 설정됐는지 확인.
        levels = [h.level for h in root.handlers]
        assert logging.DEBUG in levels
    finally:
        logging.getLogger().handlers.clear()
