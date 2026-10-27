"""executor 인메모리 ``tasks`` 의 종료 task 정리를 검증한다.

query-run 까지 task 로 쌓이면서 지우지 않으면 메모리가 끝없이 자라므로, 새 task 를 접수할 때
보관 시간·개수 상한을 넘은 종료 task 를 지운다. 진행 중 task 와 끝난 지 얼마 안 된 task 는
coordinator 가 아직 폴링하거나 접수를 재생할 수 있어 지우지 않아야 한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import httpx

from core import config as core_config
from core.dbprobe import QueryResult
from executor.app import _prune_finished_tasks, create_app as create_executor_app
from executor.models import Task, TaskStatus

NOW = datetime(2026, 10, 27, 12, 0, 0)


def _task(tid: str, status: TaskStatus, finished_ago_s: float | None = None) -> Task:
    t = Task(
        task_id=tid, job_id="job_x", sub_query="SELECT 1", target_table="",
        write_mode="append", partition_column="", partition_values=[],
    )
    t.status = status
    if finished_ago_s is not None:
        t.finished_at = (NOW - timedelta(seconds=finished_ago_s)).isoformat()
    return t


def _store(*items: Task):
    tasks = {t.task_id: t for t in items}
    return tasks, {tid: {"task_id": tid} for tid in tasks}


def test_보관_시간이_지난_종료_task_만_지운다():
    tasks, reqs = _store(
        _task("old_done", TaskStatus.DONE, 7200),
        _task("old_failed", TaskStatus.FAILED, 4000),
        _task("recent", TaskStatus.DONE, 600),
        _task("running", TaskStatus.READING),
        _task("queued", TaskStatus.QUEUED),
    )
    removed = _prune_finished_tasks(tasks, reqs, now=NOW, retention_s=3600, max_finished=0)
    assert removed == ["old_done", "old_failed"]
    assert set(tasks) == {"recent", "running", "queued"}
    assert set(reqs) == set(tasks)  # 접수 재생용 요청 본문도 함께 지운다


def test_개수_상한을_넘으면_오래_끝난_것부터_지운다():
    tasks, reqs = _store(
        *(_task(f"d{i}", TaskStatus.DONE, 100 + i * 10) for i in range(5)),
        _task("running", TaskStatus.WRITING),
    )
    removed = _prune_finished_tasks(tasks, reqs, now=NOW, retention_s=0, max_finished=2)
    # d4(140초 전)·d3·d2 가 가장 오래 끝났다.
    assert removed == ["d2", "d3", "d4"]
    assert set(tasks) == {"d0", "d1", "running"}


def test_끝난_지_얼마_안_된_task_는_상한을_넘어도_남긴다():
    """coordinator 가 종료 상태를 아직 폴링하지 못했을 수 있으므로 60초 안쪽은 보존한다."""
    tasks, reqs = _store(*(_task(f"d{i}", TaskStatus.DONE, 5 + i) for i in range(5)))
    removed = _prune_finished_tasks(tasks, reqs, now=NOW, retention_s=1, max_finished=1)
    assert removed == []
    assert len(tasks) == 5


def test_두_기준을_모두_끄면_지우지_않는다():
    tasks, reqs = _store(_task("old", TaskStatus.CANCELLED, 10**6))
    assert _prune_finished_tasks(tasks, reqs, now=NOW, retention_s=0, max_finished=0) == []
    assert "old" in tasks


def test_종료_시각을_모르는_task_는_지우지_않는다():
    t = _task("weird", TaskStatus.DONE)
    t.finished_at = "not-a-time"
    tasks, reqs = _store(t, _task("none", TaskStatus.DONE))
    assert _prune_finished_tasks(tasks, reqs, now=NOW, retention_s=1, max_finished=1) == []


async def test_query_run_접수시_오래된_종료_task_가_정리된다(monkeypatch):
    import executor.app as ea

    s = core_config.settings
    monkeypatch.setattr(s, "executor_self_report", False, raising=False)
    monkeypatch.setattr(s, "impala_host", "impala-1", raising=False)
    monkeypatch.setattr(s, "impala_query_options", {}, raising=False)
    monkeypatch.setattr(s, "executor_task_retention_s", 3600, raising=False)
    monkeypatch.setattr(s, "executor_max_finished_tasks", 1000, raising=False)
    monkeypatch.setattr(ea, "now_dt", lambda: NOW)
    monkeypatch.setattr(
        ea, "run_impala_select",
        lambda dsn, sql, **kw: QueryResult(columns=["a"], rows=[[1]], row_count=1, truncated=False, elapsed_ms=1.0),
    )

    class _NoHistory:
        async def record(self, task):
            pass

    app = create_executor_app(task_history=_NoHistory())
    app.state.tasks["stale"] = _task("stale", TaskStatus.DONE, 7200)
    app.state.tasks["fresh"] = _task("fresh", TaskStatus.DONE, 30)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        r = await c.post("/query-run", json={"sql": "SELECT 1", "datasource": "impala"})
        assert r.status_code == 200, r.text
        ids = {t["task_id"] for t in (await c.get("/tasks")).json()["tasks"]}
    assert "stale" not in ids
    assert "fresh" in ids
    assert any(i.startswith("qr-") for i in ids)
