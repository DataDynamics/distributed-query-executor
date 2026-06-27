"""executor 연결 실패 시 재시도 + 다른 executor 로 failover 동작 테스트.

httpx.MockTransport 로 특정 executor 의 연결 실패(ConnectError)를 시뮬레이션하고,
HttpDispatcher 가 재시도/failover/실패를 올바르게 처리하는지 검증한다.
"""

from __future__ import annotations

import httpx

import coordinator.dispatcher as disp_mod
from coordinator.dispatcher import HttpDispatcher
from coordinator.models import Job, JobStatus, Task, TaskStatus

DOWN = "http://127.0.0.1:9001"   # 항상 연결 실패하는 executor
UP = "http://127.0.0.1:9002"     # 정상 executor


def _patch_client(monkeypatch, handler):
    """dispatcher 가 만드는 httpx.AsyncClient 가 MockTransport 를 쓰도록 바꾼다."""
    transport = httpx.MockTransport(handler)
    real = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real(*args, **kwargs)

    monkeypatch.setattr(disp_mod.httpx, "AsyncClient", factory)


def _settings(monkeypatch, *, executors, retries=1, failover=True):
    from coordinator.config import settings
    monkeypatch.setattr(settings, "executors", executors, raising=False)
    monkeypatch.setattr(settings, "task_max_retries", retries, raising=False)
    monkeypatch.setattr(settings, "task_retry_backoff_s", 0, raising=False)  # 테스트는 즉시
    monkeypatch.setattr(settings, "task_failover", failover, raising=False)
    monkeypatch.setattr(settings, "task_connect_timeout_s", 1.0, raising=False)
    monkeypatch.setattr(settings, "task_timeout_s", 5.0, raising=False)
    monkeypatch.setattr(settings, "poll_interval_s", 0, raising=False)
    monkeypatch.setattr(settings, "max_dispatch_concurrency", 4, raising=False)
    return settings


def _job(executor_url, write_mode="append"):
    job = Job(
        original_sql="SELECT 1 FROM t WHERE dt IN ('1')",
        partition_column="dt", target_table="public.t", write_mode=write_mode,
        parallelism=1, split_strategy="contiguous", failure_policy="fail_fast",
        exec_mode="copy",
    )
    job.tasks = [Task(job_id=job.job_id, executor_url=executor_url,
                      sub_query="SELECT 1 FROM t WHERE dt IN ('1')",
                      partition_values=["'1'"])]
    return job


def _down_up_handler(calls):
    """DOWN 은 연결 실패, UP 은 즉시 DONE 으로 응답하는 핸들러."""
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        calls.append((request.method, url))
        if url.startswith(DOWN):
            raise httpx.ConnectError("All connection attempts failed", request=request)
        if request.method == "POST":
            return httpx.Response(202, json={"task_id": "t", "status": "QUEUED"})
        return httpx.Response(200, json={"status": "DONE", "rows_written": 7, "error": None})
    return handler


async def test_failover_to_live_executor(monkeypatch):
    calls: list = []
    _patch_client(monkeypatch, _down_up_handler(calls))
    s = _settings(monkeypatch, executors=[DOWN, UP], retries=1)
    disp = HttpDispatcher(s)

    job = _job(DOWN)
    await disp.run(job)

    t = job.tasks[0]
    assert t.status == TaskStatus.DONE
    assert t.executor_url == UP          # 죽은 executor → 살아있는 executor 로 failover
    assert t.rows_written == 7
    assert job.status == JobStatus.DONE
    # DOWN 에 (1 + retries)=2회 시도 후 UP 로 넘어감
    down_posts = [c for c in calls if c[0] == "POST" and c[1].startswith(DOWN)]
    assert len(down_posts) == 2
    assert t.attempt == 3                # DOWN×2 + UP×1


async def test_all_executors_fail_marks_task_failed(monkeypatch):
    calls: list = []
    _patch_client(monkeypatch, _down_up_handler(calls))
    s = _settings(monkeypatch, executors=[DOWN], retries=1)  # failover 후보 없음
    disp = HttpDispatcher(s)

    job = _job(DOWN)
    await disp.run(job)

    t = job.tasks[0]
    assert t.status == TaskStatus.FAILED
    assert "connection" in (t.error or "").lower()
    assert job.status == JobStatus.FAILED


async def test_failover_disabled_does_not_switch(monkeypatch):
    calls: list = []
    _patch_client(monkeypatch, _down_up_handler(calls))
    s = _settings(monkeypatch, executors=[DOWN, UP], retries=1, failover=False)
    disp = HttpDispatcher(s)

    job = _job(DOWN)
    await disp.run(job)

    t = job.tasks[0]
    assert t.status == TaskStatus.FAILED        # failover 꺼짐 → UP 로 안 넘어감
    assert t.executor_url == DOWN
    assert not any(c[1].startswith(UP) for c in calls)


async def test_retry_succeeds_on_same_executor(monkeypatch):
    """첫 시도는 연결 실패하지만 재시도에서 성공하면 같은 executor 에서 완료된다."""
    state = {"posts": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            state["posts"] += 1
            if state["posts"] == 1:  # 첫 POST 만 실패
                raise httpx.ConnectError("temporary", request=request)
            return httpx.Response(202, json={"status": "QUEUED"})
        return httpx.Response(200, json={"status": "DONE", "rows_written": 3, "error": None})

    _patch_client(monkeypatch, handler)
    s = _settings(monkeypatch, executors=[UP], retries=2)
    disp = HttpDispatcher(s)

    job = _job(UP)
    await disp.run(job)

    t = job.tasks[0]
    assert t.status == TaskStatus.DONE
    assert t.executor_url == UP
    assert state["posts"] == 2  # 실패 1회 + 성공 1회
