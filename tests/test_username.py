"""username 인자: 요청 → Job → executor → 이력 전파 테스트."""

from __future__ import annotations

import asyncio

import httpx

from coordinator.models import Job, JobStatus
from executor.app import create_app as create_executor_app


def test_create_job_accepts_username(client, store, valid_payload):
    payload = {**valid_payload, "username": "alice"}
    job_id = client.post("/jobs", json=payload).json()["job_id"]
    assert store.get(job_id).username == "alice"
    # /jobs 목록에도 노출
    rows = client.get("/jobs").json()["jobs"]
    assert any(r["job_id"] == job_id and r["username"] == "alice" for r in rows)


def test_username_optional_default_none(client, store, valid_payload):
    job_id = client.post("/jobs", json=valid_payload).json()["job_id"]
    assert store.get(job_id).username is None


def test_job_record_roundtrip_keeps_username():
    job = Job(
        original_sql="SELECT 1 FROM t WHERE dt IN ('1')", partition_column="dt",
        target_table="public.t", write_mode="append", parallelism=1,
        split_strategy="contiguous", failure_policy="fail_fast",
        username="bob", status=JobStatus.RUNNING,
    )
    assert Job.from_record(job.to_record()).username == "bob"


class _RecHistory:
    def __init__(self):
        self.users = []

    async def record(self, task):
        self.users.append(getattr(task, "username", None))


async def test_executor_receives_and_records_username():
    fake = _RecHistory()
    app = create_executor_app(task_history=fake)
    transport = httpx.ASGITransport(app=app)
    payload = {
        "task_id": "tU", "job_id": "j", "sub_query": "SELECT 1",
        "target_table": "t", "write_mode": "append", "partition_column": "dt",
        "partition_values": ["'1'"], "username": "carol",
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(100):
            st = (await c.get("/tasks/tU")).json()
            if st["status"] in ("DONE", "FAILED"):
                break
            await asyncio.sleep(0.01)
        assert st["status"] == "DONE"
    # task 상태 전이 이력이 모두 carol 로 기록
    assert fake.users and all(u == "carol" for u in fake.users)
