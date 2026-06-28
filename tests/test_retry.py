"""실패 파티션만 재실행(POST /jobs/{id}/retry) 테스트."""

from __future__ import annotations

from coordinator.models import Job, JobStatus, Task, TaskStatus


def _partial_job() -> Job:
    """DONE 1 + FAILED 1 + CANCELLED 1 로 구성된 PARTIAL 작업."""
    job = Job(
        original_sql="SELECT a, dt FROM t WHERE dt IN ('1','2','3')",
        partition_column="dt",
        target_table="public.t",
        write_mode="append",
        parallelism=3,
        split_strategy="contiguous",
        failure_policy="best_effort",
        status=JobStatus.PARTIAL,
    )
    job.tasks = [
        Task(job_id=job.job_id, executor_url="http://x", sub_query="q1",
             partition_values=["1"], status=TaskStatus.DONE),
        Task(job_id=job.job_id, executor_url="http://x", sub_query="q2",
             partition_values=["2"], status=TaskStatus.FAILED, error="boom"),
        Task(job_id=job.job_id, executor_url="http://x", sub_query="q3",
             partition_values=["3"], status=TaskStatus.CANCELLED),
    ]
    return job


def test_retry_creates_new_job_with_only_failed_tasks(client, store, runner):
    job = _partial_job()
    store.add(job)

    resp = client.post(f"/jobs/{job.job_id}/retry")
    assert resp.status_code == 202
    body = resp.json()
    assert body["retry_of"] == job.job_id
    assert body["retried_tasks"] == 2  # FAILED + CANCELLED 만

    new = store.get(body["job_id"])
    assert new is not None and new.job_id != job.job_id
    assert new.retry_of == job.job_id
    # 성공(DONE) 파티션 '1' 은 빠지고, 실패/취소 '2','3' 만 재실행 대상.
    assert {tuple(t.partition_values) for t in new.tasks} == {("2",), ("3",)}
    # FakeRunner 가 background 로 실행 → DONE
    assert new.status == JobStatus.DONE
    assert runner.runs[-1].job_id == new.job_id


def test_retry_404_when_job_missing(client):
    assert client.post("/jobs/nope/retry").status_code == 404


def test_retry_409_when_job_not_terminal(client, store):
    job = _partial_job()
    job.status = JobStatus.RUNNING
    store.add(job)
    assert client.post(f"/jobs/{job.job_id}/retry").status_code == 409


def test_retry_409_when_no_failed_tasks(client, store):
    job = _partial_job()
    # 종료 상태지만 실패 task 가 하나도 없는 경우(인위적): FAILED 상태 + 전부 DONE.
    for t in job.tasks:
        t.status = TaskStatus.DONE
    job.status = JobStatus.FAILED
    store.add(job)
    resp = client.post(f"/jobs/{job.job_id}/retry")
    assert resp.status_code == 409
    assert "재실행할" in resp.json()["detail"]
