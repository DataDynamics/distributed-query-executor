"""동시 실행 슬롯 + 대기 큐 상한(admission control, 큐잉+큐상한) 테스트."""

from __future__ import annotations

from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.dispatcher import JobAdmission
from coordinator.job_store import InMemoryJobStore
from coordinator.models import Job


class _Cfg:
    """JobAdmission 용 최소 설정 더블."""

    def __init__(self, running: int, pending: int):
        self.max_concurrent_jobs = running
        self.max_pending_jobs = pending


# ── JobAdmission 단위 테스트 ──────────────────────────────────────────────

def test_capacity_is_running_plus_pending():
    adm = JobAdmission(_Cfg(running=2, pending=3))
    assert adm.capacity == 5


def test_unlimited_when_running_non_positive():
    adm = JobAdmission(_Cfg(running=0, pending=10))
    assert adm.capacity is None
    # 무제한: 계속 admit 되어야 한다
    for _ in range(50):
        assert adm.try_admit() is True


def test_try_admit_rejects_beyond_capacity_and_release_recovers():
    adm = JobAdmission(_Cfg(running=1, pending=1))  # capacity=2
    assert adm.try_admit() is True   # in-flight 1
    assert adm.try_admit() is True   # in-flight 2 (=capacity)
    assert adm.try_admit() is False  # 초과 → 거부
    assert adm.inflight == 2
    adm.release()                    # 슬롯 1 반납
    assert adm.inflight == 1
    assert adm.try_admit() is True   # 다시 수용 가능
    # release 는 0 미만으로 내려가지 않는다
    adm.release(); adm.release(); adm.release()
    assert adm.inflight == 0


# ── /jobs API admission 통합 테스트 ───────────────────────────────────────

class _SaturatingRunner:
    """run() 에서 in-flight 를 반납하지 않아 슬롯이 계속 점유된 상태를 만든다.

    (app 핸들러의 admit/거부 게이트만 결정적으로 검증하기 위한 더블.)
    """

    def __init__(self, running: int, pending: int):
        self.admission = JobAdmission(_Cfg(running, pending))
        self.ran: list[str] = []

    async def run(self, job: Job) -> str:
        self.ran.append(job.job_id)
        return job.job_id

    async def cancel(self, job: Job) -> None:  # pragma: no cover - 미사용
        pass


def _payload() -> dict:
    return {
        "sql": (
            "SELECT user_id, dt FROM sales "
            "WHERE dt IN ('2026-01-01','2026-01-02') AND region = 'KR'"
        ),
        "partition_column": "dt",
        "target_table": "public.sales_mirror",
        "parallelism": 2,
    }


def test_jobs_endpoint_rejects_over_capacity_with_429():
    runner = _SaturatingRunner(running=1, pending=1)  # capacity=2
    client = TestClient(create_app(runner=runner, store=InMemoryJobStore()))

    assert client.post("/jobs", json=_payload()).status_code == 202
    assert client.post("/jobs", json=_payload()).status_code == 202
    # 실행+대기 용량(2) 초과 → 429
    r = client.post("/jobs", json=_payload())
    assert r.status_code == 429
    assert r.headers.get("Retry-After") == "5"
    assert "capacity=2" in r.json()["detail"]
    # 거부된 요청은 디스패치되지 않는다
    assert len(runner.ran) == 2


def test_completed_job_frees_slot_real_dispatcher(monkeypatch):
    """실제 LocalDispatcher: job 이 완료되면 in-flight 가 반납되어 다음 job 을 받는다."""
    from coordinator.config import settings as cfg
    from coordinator.dispatcher import LocalDispatcher

    monkeypatch.setattr(cfg, "max_concurrent_jobs", 1, raising=False)
    monkeypatch.setattr(cfg, "max_pending_jobs", 0, raising=False)  # capacity=1
    monkeypatch.setattr(cfg, "greenplum_dsn", "", raising=False)    # → MockBackend

    store = InMemoryJobStore()
    runner = LocalDispatcher(cfg, store=store)
    client = TestClient(create_app(runner=runner, store=store))

    # 첫 job: 백그라운드에서 즉시 완료 → in-flight 반납
    assert client.post("/jobs", json=_payload()).status_code == 202
    assert runner.admission.inflight == 0
    # capacity 회복 → 두 번째도 수용된다(429 아님)
    assert client.post("/jobs", json=_payload()).status_code == 202
    assert runner.admission.inflight == 0
