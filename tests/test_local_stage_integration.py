"""local_stage mock 통합 테스트 — GP·Impala 없이 "파일 루프 닫힘"까지 검증.

SCENARIO.md B-3 설계의 실증: `MockLocalStageBackend` 로 Phase 1(실 CSV write)이 쓴 파일을
Phase 2(file:// 경로 파싱 → read)가 그대로 읽어 target 에 넣는 루프를 닫아, 실제 코드 경로
(POST /jobs → split → 파일 예산 배분 → export → 배리어 → file:// 적재 → finalize)를 통과시킨다.
"""

from __future__ import annotations

from collections import Counter

from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.config import settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.job_store import JobStore
from coordinator.stage import host_of

from .helpers import MockLocalStageBackend


def _job_json():
    return {
        "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('d1','d2','d3','d4')",
        "partition_column": "dt",
        "target_table": "public.sales_mirror",
        "exec_mode": "local_stage",
        "parallelism": 4,
        "external_columns": "user_id bigint, amount numeric, dt date",
        "staging_table": "stg_sales",
        "insert_sql": "INSERT INTO public.sales_mirror SELECT * FROM stg_sales",
    }


def test_local_stage_mock_integration_closes_the_loop(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "executors",
                        ["http://seg1:8087", "http://seg2:8087"], raising=False)
    monkeypatch.setattr(settings, "stage_local_dir", str(tmp_path), raising=False)
    backend = MockLocalStageBackend(topology={"seg1": 2, "seg2": 2})
    runner = LocalDispatcher(settings, backend=backend)
    client = TestClient(create_app(runner=runner, store=JobStore()))

    # POST /jobs → 실제 검증/분할/out_path/예산배분/2-phase 전 과정을 태운다.
    resp = client.post("/jobs", json=_job_json())
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "DONE"
    assert body["total"] == 4

    # ① 파일 예산: 호스트당 ≤ S_h(2), 총 4파일
    detail = client.get(f"/jobs/{job_id}").json()
    per_host = Counter(host_of(t["executor_url"]) for t in detail["tasks"])
    assert per_host["seg1"] <= 2 and per_host["seg2"] <= 2
    assert sum(per_host.values()) == 4

    # ② 루프 닫힘: Phase 1 이 실제로 파일을 썼고, Phase 2 가 그 파일을 읽어 target 에 넣었다.
    exported_rows = sum(r for _, r in backend.exported)
    assert exported_rows == 4  # IN 값 4개 → 파일당 1행 → 총 4
    assert len(backend.target) == exported_rows

    # ③ host 매핑: Phase 2 URI 가 gp_hostname(seg1/seg2) 기반(URL 파싱 폴백으로 동일)
    assert "file://seg1/" in backend.loads[0] and "file://seg2/" in backend.loads[0]

    # ④ CSV 방언: 기본 backtick 구분자로 파일이 써졌다.
    first_file = backend.exported[0][0]
    assert "`" in open(first_file, encoding="utf-8").readline()


def test_local_stage_mock_integration_budget_exceeded_fails(monkeypatch, tmp_path):
    # 용량(seg1=1) < 파일 4 → 예산 초과로 FAILED, export/load 미실행.
    monkeypatch.setattr(settings, "executors", ["http://seg1:8087"], raising=False)
    monkeypatch.setattr(settings, "stage_local_dir", str(tmp_path), raising=False)
    backend = MockLocalStageBackend(topology={"seg1": 1})
    runner = LocalDispatcher(settings, backend=backend)
    client = TestClient(create_app(runner=runner, store=JobStore()))

    job_id = client.post("/jobs", json=_job_json()).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "FAILED"
    assert backend.exported == [] and backend.target == []
