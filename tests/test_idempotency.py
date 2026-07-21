"""요청 멱등(Idempotency-Key) 검증.

POST /jobs 에 ``Idempotency-Key`` 헤더를 주면 중복 제출(타임아웃 재시도 등)을 흡수해
같은 job 을 돌려주고 새로 만들지 않는다(중복 적재 방지). 같은 키를 다른 본문으로 쓰면
409, 키가 없으면 기존대로 매번 새 job. 저장소 레벨의 원자적 선점(claim_and_add)도 본다.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.dispatcher import JobAdmission
from coordinator.job_store import InMemoryJobStore
from coordinator.models import Job


class _Cfg:
    def __init__(self, running: int, pending: int):
        self.max_concurrent_jobs = running
        self.max_pending_jobs = pending


class _Runner:
    """슬롯을 정상 반납하는 무해한 러너(멱등 경로만 결정적으로 보기 위한 더블)."""

    def __init__(self):
        self.admission = JobAdmission(_Cfg(running=100, pending=100))
        self.ran: list[str] = []

    async def run(self, job: Job) -> str:
        self.ran.append(job.job_id)
        self.admission.release()  # 실제 run 처럼 실행 슬롯 반납
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


def _client(store=None):
    return TestClient(create_app(runner=_Runner(), store=store or InMemoryJobStore()))


# ── API 통합 ────────────────────────────────────────────────────────────────

def test_같은_키_재제출은_같은_job_을_재생한다():
    store = InMemoryJobStore()
    client = _client(store)
    h = {"Idempotency-Key": "k-1"}

    r1 = client.post("/jobs", json=_payload(), headers=h)
    assert r1.status_code == 202
    jid = r1.json()["job_id"]

    r2 = client.post("/jobs", json=_payload(), headers=h)
    assert r2.status_code == 200                                # 새로 만든 게 아니라 재생
    assert r2.json()["job_id"] == jid                           # 같은 job_id
    assert r2.headers.get("Idempotency-Replayed") == "true"     # 재생 표시
    assert len(store.list()) == 1                               # job 은 하나만 생성됨


def test_같은_키_다른_본문은_409():
    client = _client()
    h = {"Idempotency-Key": "k-2"}
    assert client.post("/jobs", json=_payload(), headers=h).status_code == 202
    p2 = _payload()
    p2["parallelism"] = 4                                       # 본문이 달라짐
    assert client.post("/jobs", json=p2, headers=h).status_code == 409


def test_키_없으면_매번_새_job():
    store = InMemoryJobStore()
    client = _client(store)
    a = client.post("/jobs", json=_payload()).json()["job_id"]
    b = client.post("/jobs", json=_payload()).json()["job_id"]
    assert a != b
    assert len(store.list()) == 2


def test_dry_run_은_멱등_대상이_아니다():
    store = InMemoryJobStore()
    client = _client(store)
    p = _payload()
    p["dry_run"] = True
    r = client.post("/jobs", json=p, headers={"Idempotency-Key": "k-dry"})
    assert r.status_code == 200 and r.json().get("dry_run") is True
    assert len(store.list()) == 0                               # dry_run 은 저장/선점 안 함


# ── 저장소 레벨 원자적 선점 ─────────────────────────────────────────────────

def _job(key: str) -> Job:
    return Job(
        original_sql="SELECT 1", partition_column="dt", target_table="t",
        write_mode="append", parallelism=1, split_strategy="contiguous",
        failure_policy="fail_fast", idempotency_key=key,
    )


def test_claim_and_add_는_같은_키를_한_번만_저장한다():
    store = InMemoryJobStore()
    j1 = _job("k")
    assert store.claim_and_add(j1) is None                      # 선점 성공(저장)
    j2 = _job("k")
    won = store.claim_and_add(j2)
    assert won is not None and won.job_id == j1.job_id          # 진 쪽은 기존 job 반환
    assert len(store.list()) == 1                               # job 은 하나
    assert store.get_by_idempotency_key("k").job_id == j1.job_id


def test_claim_and_add_키_없으면_그냥_저장():
    store = InMemoryJobStore()
    j = Job(original_sql="s", partition_column="dt", target_table="t",
            write_mode="append", parallelism=1, split_strategy="contiguous",
            failure_policy="fail_fast")
    assert store.claim_and_add(j) is None
    assert len(store.list()) == 1
