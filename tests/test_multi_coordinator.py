"""멀티 coordinator: 공유 store, executor self-report, 직렬화, admission control."""

from __future__ import annotations

import asyncio
import threading

import httpx
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.config import settings as core_settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.job_store import InMemoryJobStore, SqlJobStore, build_job_store
from coordinator.models import Job, JobStatus, Task, TaskStatus
from executor.backend import MockBackend
from executor.status import ExecutorStatusReporter


def _job(status=JobStatus.RUNNING, n=2):
    job = Job(
        original_sql="SELECT a, dt FROM t WHERE dt IN ('1','2')",
        partition_column="dt", target_table="public.t", write_mode="append",
        parallelism=n, split_strategy="contiguous", failure_policy="fail_fast",
        status=status,
    )
    job.tasks = [
        Task(job_id=job.job_id, executor_url=None, sub_query=f"SELECT {i}",
             partition_values=[f"'{i}'"])
        for i in range(n)
    ]
    return job


# ───────────────────── 직렬화 ─────────────────────

def test_job_record_roundtrip():
    job = _job(status=JobStatus.RUNNING, n=3)
    job.exec_mode = "stage_insert"
    job.staging_table = "stg"
    job.staging_ddl = "CREATE TEMP TABLE stg (a int)"
    job.insert_sql = "INSERT INTO t SELECT * FROM stg"
    job.tasks[0].status = TaskStatus.DONE
    job.tasks[0].rows_written = 7

    restored = Job.from_record(job.to_record())
    assert restored.job_id == job.job_id
    assert restored.exec_mode == "stage_insert"
    assert restored.staging_ddl == job.staging_ddl
    assert restored.insert_sql == job.insert_sql
    assert len(restored.tasks) == 3
    assert restored.tasks[0].status == TaskStatus.DONE
    assert restored.tasks[0].rows_written == 7
    assert restored.status == JobStatus.RUNNING


# ───────────────────── store 팩토리 ─────────────────────

class _S:
    store_backend = "memory"
    history_db_dsn = ""
    store_table = "jobs"
    coordinator_id = "c1"


def test_build_job_store_memory_default():
    assert isinstance(build_job_store(_S()), InMemoryJobStore)


def test_build_job_store_postgres_requires_dsn():
    s = _S(); s.store_backend = "postgres"; s.history_db_dsn = ""
    assert isinstance(build_job_store(s), InMemoryJobStore)  # DSN 없으면 폴백

    s2 = _S(); s2.store_backend = "postgres"; s2.history_db_dsn = "postgresql://u@h/db"
    store = build_job_store(s2)
    assert isinstance(store, SqlJobStore)
    assert store.coordinator_id == "c1"


# ───────────────────── 공유 store 로 cross-coordinator ─────────────────────

class _FakeRunner:
    def __init__(self):
        self.cancels = []

    async def run(self, job):
        return job.job_id

    async def cancel(self, job):
        self.cancels.append(job.job_id)
        for t in job.tasks:
            t.status = TaskStatus.CANCELLED
        job.cancel_requested = True


def test_two_coordinators_share_store_for_read_and_cancel():
    shared = InMemoryJobStore()
    # coordinator A, B 가 같은 store 를 공유
    a = TestClient(create_app(runner=_FakeRunner(), store=shared))
    b_runner = _FakeRunner()
    b = TestClient(create_app(runner=b_runner, store=shared))

    # A 가 직접 RUNNING job 을 store 에 적재(실행 중 가정)
    job = _job(status=JobStatus.RUNNING)
    shared.add(job)

    # B 에서 조회 가능해야 한다(공유 store)
    assert b.get(f"/jobs/{job.job_id}/status").json()["status"] == "RUNNING"
    # B 에서 취소 → 공유 플래그 + runner.cancel
    resp = b.post(f"/jobs/{job.job_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELLED"
    assert b_runner.cancels == [job.job_id]
    # A 에서도 취소됨이 보인다
    assert a.get(f"/jobs/{job.job_id}/status").json()["status"] == "CANCELLED"


# ───────────────────── dispatcher 가 store 에 스냅샷 저장 ─────────────────────

class _RecordingStore(InMemoryJobStore):
    def __init__(self):
        super().__init__()
        self.save_count = 0

    def save(self, job):
        self.save_count += 1
        super().save(job)


async def test_dispatcher_persists_snapshots():
    store = _RecordingStore()
    disp = LocalDispatcher(core_settings, backend=MockBackend(rows_per_value=2), store=store)
    job = _job(status=JobStatus.PENDING, n=2)
    await disp.run(job)
    # 시작 + task별 + 종료 → 여러 번 저장
    assert store.save_count >= 3
    assert job.status == JobStatus.DONE


async def test_dispatcher_observes_store_cancel_flag():
    # 공유 store 에 취소 플래그를 세우면 로컬 플래그가 없어도 취소로 관측
    store = InMemoryJobStore()
    disp = LocalDispatcher(core_settings, backend=MockBackend(), store=store)
    job = _job(status=JobStatus.RUNNING)
    store.add(job)
    store.request_cancel(job.job_id)
    assert disp._cancel_observed(job) is True


# ───────────────────── executor self-report ─────────────────────

def test_status_reporter_disabled_without_dsn():
    class _ES:
        history_db_dsn = ""
        executor_status_table = "executor_status"
        executor_status_interval_s = 10
        monitor_disk_path = "/"

    rep = ExecutorStatusReporter(_ES())
    assert rep.enabled is False
    assert rep.executor_id


def test_coordinator_uses_status_repo_when_self_report(monkeypatch):
    monkeypatch.setattr(core_settings, "executor_self_report", True, raising=False)
    monkeypatch.setattr(core_settings, "history_db_dsn", "postgresql://u@h/db", raising=False)
    app = create_app(runner=_FakeRunner(), store=InMemoryJobStore())
    assert app.state.status_repo is not None


# ───────────────────── executor admission control ─────────────────────

async def test_executor_admission_control_no_deadlock(monkeypatch):
    from executor.app import create_app as create_executor_app
    from core import config as core_config

    monkeypatch.setattr(core_config.settings, "executor_max_concurrent_tasks", 1, raising=False)
    app = create_executor_app(backend=MockBackend(rows_per_value=1))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        for i in range(3):
            await c.post("/tasks", json={
                "task_id": f"t{i}", "job_id": "j", "sub_query": "SELECT 1",
                "target_table": "t", "write_mode": "append",
                "partition_column": "dt", "partition_values": ["'1'"],
            })
        # 동시 1개 제한이어도 3개 모두 완료되어야 한다(데드락 없음)
        done = 0
        for _ in range(300):
            statuses = []
            for i in range(3):
                statuses.append((await c.get(f"/tasks/t{i}")).json()["status"])
            done = sum(1 for s in statuses if s == "DONE")
            if done == 3:
                break
            await asyncio.sleep(0.01)
        assert done == 3
