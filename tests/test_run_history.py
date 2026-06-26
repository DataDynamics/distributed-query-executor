"""run() 의 job_id 반환 + 이력 기록, 그리고 상태 확인 API 테스트."""

from __future__ import annotations

import pytest

from coordinator.config import settings as core_settings
from coordinator.dispatcher import HttpDispatcher
from coordinator.history import JobHistoryRepository
from coordinator.models import Job, JobStatus


def _job() -> Job:
    return Job(
        original_sql="SELECT 1 FROM t WHERE dt IN ('1')",
        partition_column="dt",
        target_table="public.t",
        write_mode="append",
        parallelism=1,
        split_strategy="contiguous",
        failure_policy="fail_fast",
    )


class _RecordingHistory:
    """이력 기록을 메모리에 모으는 가짜 저장소."""

    def __init__(self):
        self.records: list[str] = []

    async def record(self, job):
        self.records.append(job.status.value)


async def test_run_returns_job_id_and_records_history():
    history = _RecordingHistory()
    disp = HttpDispatcher(core_settings, history=history)
    job = _job()
    job.tasks = []  # 태스크 없음 → 네트워크 호출 없이 즉시 완료

    result = await disp.run(job)

    assert result == job.job_id            # run() 이 job_id 반환
    assert job.status == JobStatus.DONE
    assert job.started_at and job.finished_at
    # 시작(RUNNING)과 종료(DONE) 이력이 기록되어야 한다
    assert history.records == ["RUNNING", "DONE"]


async def test_history_repository_disabled_without_dsn():
    class _S:
        history_db_dsn = ""
        history_table = "job_history"

    repo = JobHistoryRepository(_S())
    assert repo.enabled is False
    # DSN 없으면 예외 없이 no-op
    await repo.record(_job())


# ───────────────────── 상태 확인 API ─────────────────────


def test_submit_returns_job_id_then_status_lookup(client, valid_payload):
    # 1) 제출 → job_id 반환
    resp = client.post("/jobs", json=valid_payload)
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    # 2) job_id 로 상태 확인 API 호출 → 현재 상태
    status = client.get(f"/jobs/{job_id}/status").json()
    assert status["job_id"] == job_id
    assert status["status"] in {"SPLITTING", "RUNNING", "DONE"}
    assert "progress_percent" in status
    assert "tasks" not in status  # 경량 뷰


def test_status_progress_percent_complete(client, valid_payload):
    job_id = client.post("/jobs", json=valid_payload).json()["job_id"]
    status = client.get(f"/jobs/{job_id}/status").json()
    # FakeRunner 가 모든 태스크 완료 처리 → 100%
    assert status["status"] == "DONE"
    assert status["progress_percent"] == 100.0


def test_status_unknown_job_404(client):
    assert client.get("/jobs/does-not-exist/status").status_code == 404
