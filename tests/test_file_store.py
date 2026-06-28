"""FileJobStore(파일 영속) + 재기동 정합(reconcile) 테스트 — 단일 노드 크래시 복구."""

from __future__ import annotations

from coordinator.job_store import FileJobStore, reconcile_interrupted_jobs
from coordinator.models import Job, JobStatus, Task, TaskStatus


def _job(status: JobStatus, task_status: TaskStatus = TaskStatus.READING) -> Job:
    j = Job(
        original_sql="SELECT a, dt FROM t WHERE dt IN ('1')",
        partition_column="dt",
        target_table="public.t",
        write_mode="append",
        parallelism=1,
        split_strategy="contiguous",
        failure_policy="fail_fast",
        status=status,
    )
    j.tasks = [
        Task(job_id=j.job_id, executor_url=None, sub_query="q",
             partition_values=["1"], status=task_status)
    ]
    return j


def test_persists_and_reloads_from_file(tmp_path):
    path = tmp_path / "jobs-state.json"
    s1 = FileJobStore(path)
    job = _job(JobStatus.DONE, TaskStatus.DONE)
    s1.add(job)
    assert path.exists()

    # 새 인스턴스(=프로세스 재시작)가 파일에서 복원해야 한다.
    s2 = FileJobStore(path)
    got = s2.get(job.job_id)
    assert got is not None
    assert got.status == JobStatus.DONE
    assert len(got.tasks) == 1 and got.tasks[0].partition_values == ["1"]


def test_reconcile_marks_running_job_failed(tmp_path):
    path = tmp_path / "jobs-state.json"
    s1 = FileJobStore(path)
    job = _job(JobStatus.RUNNING, TaskStatus.READING)
    s1.add(job)

    # 재시작 시뮬레이션: 파일에서 다시 로드한 뒤 정합.
    s2 = FileJobStore(path)
    n = reconcile_interrupted_jobs(s2)
    assert n == 1
    got = s2.get(job.job_id)
    assert got.status == JobStatus.FAILED
    assert got.error and "재시작" in got.error
    # 진행 중이던 task 도 FAILED → retry 로 재실행 대상이 된다.
    assert got.tasks[0].status == TaskStatus.FAILED
    # 정합 결과가 파일에도 반영됐는지(또 다른 재로드로 확인).
    assert FileJobStore(path).get(job.job_id).status == JobStatus.FAILED


def test_reconcile_noop_for_terminal_jobs(tmp_path):
    path = tmp_path / "jobs-state.json"
    s = FileJobStore(path)
    s.add(_job(JobStatus.DONE, TaskStatus.DONE))
    s.add(_job(JobStatus.PARTIAL, TaskStatus.FAILED))
    assert reconcile_interrupted_jobs(s) == 0
