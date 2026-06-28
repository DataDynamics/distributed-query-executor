"""Phase 3 HA 로직 테스트: 예약 합산 / 고아 job 선별 / 죽은 coordinator 정합.

순수 로직이라 DB 없이 검증한다(SQL 실행 경로는 다른 DB 코드와 동일하게 통합 테스트 대상).
"""

from __future__ import annotations

from coordinator.ha import reconcile_orphaned_jobs, select_orphans
from coordinator.models import Job, JobStatus, Task, TaskStatus
from coordinator.reservation import merge_reservations


# ── 3-B: 예약 합산 ────────────────────────────────────────────────────────

def test_merge_reservations_adds_to_active_tasks():
    view = {"u1": {"executor_url": "u1", "active_tasks": 2, "healthy": True}}
    merged = merge_reservations(view, {"u1": 3})
    assert merged["u1"]["active_tasks"] == 5
    # 원본은 그대로(불변), 예약 없으면 동일 객체 반환
    assert view["u1"]["active_tasks"] == 2
    assert merge_reservations(view, {}) is view


# ── 3-C: 고아 job 선별(순수) ──────────────────────────────────────────────

def test_select_orphans_picks_non_terminal_with_dead_or_missing_owner():
    owned = [
        ("j1", "RUNNING", "dead"),     # 죽은 소유자 → 고아
        ("j2", "RUNNING", "alive"),    # 살아있는 소유자 → 제외
        ("j3", "DONE", "dead"),        # 종료 상태 → 제외
        ("j4", "SPLITTING", None),     # 소유자 없음 → 고아
    ]
    assert set(select_orphans(owned, {"alive"})) == {"j1", "j4"}


# ── 3-C: 정합(fake store/heartbeat) ───────────────────────────────────────

def _job(job_id, status, task_status=TaskStatus.READING):
    j = Job(
        original_sql="SELECT a, dt FROM t WHERE dt IN ('1')", partition_column="dt",
        target_table="public.t", write_mode="append", parallelism=1,
        split_strategy="contiguous", failure_policy="fail_fast", status=status, job_id=job_id,
    )
    j.tasks = [Task(job_id=job_id, executor_url=None, sub_query="q",
                    partition_values=["1"], status=task_status)]
    return j


class _FakeStore:
    def __init__(self, jobs, owners):
        self._jobs = jobs        # id -> Job
        self._owners = owners    # id -> coordinator_id
        self.saved = []

    def list_owned(self):
        return [(jid, j.status.value, self._owners.get(jid)) for jid, j in self._jobs.items()]

    def get(self, jid):
        return self._jobs.get(jid)

    def save(self, job):
        self.saved.append(job.job_id)


class _FakeHeartbeat:
    coordinator_id = "me"

    def __init__(self, fresh):
        self._fresh = set(fresh)

    def fresh_ids(self, stale_s):
        return set(self._fresh)


def test_reconcile_marks_only_dead_owner_non_terminal_jobs():
    jobs = {
        "j1": _job("j1", JobStatus.RUNNING),                        # dead owner → 정합
        "j2": _job("j2", JobStatus.RUNNING),                        # 자기 소유 → 보존
        "j3": _job("j3", JobStatus.DONE, TaskStatus.DONE),          # 종료 → 제외
    }
    owners = {"j1": "dead", "j2": "me", "j3": "dead"}
    store = _FakeStore(jobs, owners)
    n = reconcile_orphaned_jobs(store, _FakeHeartbeat(fresh=set()), stale_s=30)

    assert n == 1
    assert jobs["j1"].status == JobStatus.FAILED
    assert jobs["j1"].tasks[0].status == TaskStatus.FAILED      # 진행 중 task 도 FAILED → retry 대상
    assert jobs["j1"].error and "coordinator" in jobs["j1"].error
    assert jobs["j2"].status == JobStatus.RUNNING               # 자기 소유는 건드리지 않음
    assert store.saved == ["j1"]


def test_reconcile_respects_fresh_owner():
    jobs = {"j1": _job("j1", JobStatus.RUNNING)}
    store = _FakeStore(jobs, {"j1": "other"})
    # other 가 살아있으면 정합 대상 아님
    assert reconcile_orphaned_jobs(store, _FakeHeartbeat(fresh={"other"}), 30) == 0
    assert jobs["j1"].status == JobStatus.RUNNING


def test_reconcile_noop_without_list_owned():
    class Bare:
        pass
    assert reconcile_orphaned_jobs(Bare(), _FakeHeartbeat(set()), 30) == 0
