"""executor graceful drain(종료 시 진행 중 task 안전 완료) 테스트.

- 드레이닝 중에는 신규 task 를 503 으로 거부한다.
- 종료(lifespan shutdown) 시 진행 중 task 의 완료를 기다린다(강제 중단하지 않음).
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from executor.app import create_app
from executor.backend import MockBackend


def _task_payload(task_id="t_drain", job_id="job_drain"):
    return {
        "task_id": task_id,
        "job_id": job_id,
        "sub_query": "SELECT 1",
        "target_table": "public.t",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["1"],
        "exec_mode": "copy",
    }


@pytest.fixture(autouse=True)
def _no_self_report(monkeypatch):
    from core.config import settings
    monkeypatch.setattr(settings, "executor_self_report", False, raising=False)


def test_rejects_new_tasks_while_draining():
    # lifespan 을 띄우지 않고(컨텍스트 매니저 미사용) 드레이닝 플래그만 세워 503 을 확인.
    app = create_app(backend=MockBackend())
    client = TestClient(app)
    app.state.drain["on"] = True
    resp = client.post("/tasks", json=_task_payload())
    assert resp.status_code == 503
    assert "draining" in resp.json()["detail"]


class _SlowBackend:
    """move 가 잠시 블로킹되는 백엔드 — 드레이닝이 완료를 기다리는지 검증용."""

    def __init__(self, delay: float):
        self.delay = delay
        self.completed = False

    def move(self, *a, on_progress=None, query_options=None, **k):
        time.sleep(self.delay)  # run_in_executor 스레드에서 블로킹
        self.completed = True
        return 7

    def execute(self, sql):  # pragma: no cover - 미사용
        return 0

    def stage_and_insert(self, *a, **k):  # pragma: no cover - 미사용
        return 0


def test_shutdown_drains_inflight_task(monkeypatch):
    from core.config import settings
    monkeypatch.setattr(settings, "executor_shutdown_drain_timeout_s", 5, raising=False)
    monkeypatch.setattr(settings, "executor_max_concurrent_tasks", 0, raising=False)
    backend = _SlowBackend(delay=0.4)
    app = create_app(backend=backend)

    with TestClient(app) as client:  # __enter__/__exit__ 가 lifespan 기동/종료를 돌린다
        resp = client.post("/tasks", json=_task_payload())
        assert resp.status_code == 202
        # 컨텍스트 종료(__exit__) 시 lifespan shutdown → 드레이닝이 진행 중 task 완료를 대기
    # 드레이닝이 완료를 기다렸으므로 백엔드 작업이 끝나 있어야 한다.
    assert backend.completed is True
    assert app.state.drain["on"] is True
