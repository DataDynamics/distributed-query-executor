"""Coordinator API 테스트용 공용 픽스처."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.job_store import JobStore
from coordinator.models import Job, JobStatus, TaskStatus


class FakeRunner:
    """디스패치된 Job을 기록하고, 네트워크 I/O 없이 DONE으로 표시한다."""

    def __init__(self) -> None:
        self.runs: list[Job] = []
        self.cancels: list[Job] = []

    async def run(self, job: Job) -> str:
        self.runs.append(job)
        for task in job.tasks:
            task.status = TaskStatus.DONE
            task.rows_written = 10
        job.status = JobStatus.DONE
        return job.job_id

    async def cancel(self, job: Job) -> None:
        self.cancels.append(job)
        job.cancel_requested = True
        for task in job.tasks:
            if task.status not in (TaskStatus.DONE, TaskStatus.FAILED):
                task.status = TaskStatus.CANCELLED
        job.status = JobStatus.CANCELLED


@pytest.fixture(autouse=True)
def _hermetic_settings(monkeypatch):
    """테스트를 머신의 /data1/query-executor/config(실 DB) 설정과 무관하게 유지한다."""
    from coordinator.config import settings
    monkeypatch.setattr(settings, "history_db_dsn", "", raising=False)
    monkeypatch.setattr(settings, "store_backend", "memory", raising=False)
    monkeypatch.setattr(settings, "executor_self_report", False, raising=False)
    # 주변 /etc 설정(coordinator.executors)에 의존하지 않도록 executor 목록도 비운다.
    # executor 가 필요한 테스트는 각자 설정/디스패처에 명시적으로 주입한다.
    monkeypatch.setattr(settings, "executors", [], raising=False)


@pytest.fixture
def runner() -> FakeRunner:
    return FakeRunner()


@pytest.fixture
def store() -> JobStore:
    return JobStore()


@pytest.fixture
def client(runner: FakeRunner, store: JobStore) -> TestClient:
    app = create_app(runner=runner, store=store)
    return TestClient(app)


@pytest.fixture
def valid_payload() -> dict:
    return {
        "sql": (
            "SELECT user_id, amount, dt FROM sales "
            "WHERE dt IN ('2026-01-01','2026-01-02','2026-01-03','2026-01-04') "
            "AND region = 'KR'"
        ),
        "partition_column": "dt",
        "target_table": "public.sales_mirror",
        "parallelism": 2,
    }
