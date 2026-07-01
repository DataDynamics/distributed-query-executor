"""local_stage mock 통합 테스트 — GP·Impala 없이 "파일 루프 닫힘"까지 검증.

SCENARIO.md B-3 설계의 실증: `MockLocalStageBackend` 로 Phase 1(실 CSV write)이 쓴 파일을
Phase 2(file:// 경로 파싱 → read)가 그대로 읽어 target 에 넣는 루프를 닫아, 실제 코드 경로
(POST /jobs → split → 파일 예산 배분 → export → 배리어 → file:// 적재 → finalize)를 통과시킨다.
"""

from __future__ import annotations

import csv
import os
import re

from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.config import settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.job_store import JobStore
from coordinator.stage import host_of
from executor.backend import MockBackend


class MockLocalStageBackend(MockBackend):
    """export=실 CSV 파일 write, load=file:// 경로 파싱→read→target 집계, 토폴로지 제공."""

    def __init__(self, topology=None):
        super().__init__()
        self.topology = dict(topology or {})
        self.target: list = []          # 인메모리 GP target
        self.exported: list = []        # (out_path, rows)
        self.loads: list = []           # external_ddl 기록

    def export_to_local_csv(self, sub_query, out_path, csv_options=None,
                            on_progress=None, query_options=None, on_stage=None):
        opts = csv_options or {}
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        vals = re.findall(r"'([^']*)'", sub_query)  # sub_query 의 IN 값 → 값당 1행
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=opts.get("delimiter", "`"),
                           quotechar=opts.get("quote", '"'), lineterminator="\n")
            for i, v in enumerate(vals):
                w.writerow([i, 1.0, v])
        self.exported.append((out_path, len(vals)))
        if on_progress:
            on_progress(len(vals))
        return len(vals)

    def load_external_csv(self, external_ddl, staging_ddl, staging_load_sql,
                          pre_delete_sql, insert_sql, cleanup_sqls=None, on_stage=None):
        self.loads.append(external_ddl)
        paths = re.findall(r"file://[^/]*(/[^']+)", external_ddl)  # host 뒤 경로만
        loaded = 0
        for p in paths:
            with open(p, newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f, delimiter="`"))
            self.target.extend(rows)
            loaded += len(rows)
        return loaded

    def segment_host_counts(self):
        return self.topology


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
    from collections import Counter
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
