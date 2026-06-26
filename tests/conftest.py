"""Shared test fixtures for the coordinator API."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.job_store import JobStore
from coordinator.models import Job, JobStatus, TaskStatus


class FakeRunner:
    """Records dispatched jobs and marks them DONE without any network I/O."""

    def __init__(self) -> None:
        self.runs: list[Job] = []

    async def run(self, job: Job) -> None:
        self.runs.append(job)
        for task in job.tasks:
            task.status = TaskStatus.DONE
            task.rows_written = 10
        job.status = JobStatus.DONE


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
