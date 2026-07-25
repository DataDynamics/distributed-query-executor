"""s3_stage mock 통합 테스트 — GP·S3 없이 "S3 루프 닫힘"까지 검증.

docs/GUIDE.md(s3_stage 절) 설계의 실증: `MockS3StageBackend` 로 Phase 1(인메모리 S3 업로드)이
올린 객체를, 배리어 후 Phase 2(coordinator 가 조립한 PXF external_ddl 프리픽스 파싱 → read)가
그대로 읽어 target 에 넣는 루프를 닫아, 실제 코드 경로(POST /jobs → split → out_path(S3 키) →
export 디스패치 → 배리어 → _run_s3_load(외부테이블/INSERT 조립) → load → cleanup → finalize)를
통과시킨다. local_stage 의 test_local_stage_integration.py 에 대응한다.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from core import s3_stage as s3sql
from coordinator.app import create_app
from coordinator.config import settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.job_store import JobStore

from .helpers import MockS3StageBackend


def _job_json(**over):
    base = {
        "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('d1','d2','d3','d4')",
        "partition_column": "dt",
        "target_table": "public.sales_mirror",
        "exec_mode": "s3_stage",
        "parallelism": 4,
        "external_columns": "user_id bigint, amount numeric, dt date",
        "staging_table": "stg_sales",
        "insert_sql": "INSERT INTO public.sales_mirror SELECT * FROM stg_sales",
    }
    base.update(over)
    return base


def _s3_settings(monkeypatch):
    monkeypatch.setattr(settings, "s3_bucket", "mybkt", raising=False)
    monkeypatch.setattr(settings, "s3_prefix", "dqe-stage", raising=False)
    monkeypatch.setattr(settings, "s3_pxf_server", "s3srv", raising=False)
    monkeypatch.setattr(settings, "s3_pxf_profile", "s3:csv", raising=False)
    monkeypatch.setattr(settings, "s3_gp_location_template", "", raising=False)
    monkeypatch.setattr(settings, "s3_delete_on_cleanup", True, raising=False)
    monkeypatch.setattr(settings, "executors", [], raising=False)


def _client(backend, monkeypatch):
    _s3_settings(monkeypatch)
    store = JobStore()
    runner = LocalDispatcher(settings, backend=backend, store=store)
    return TestClient(create_app(runner=runner, store=store))


def test_s3_stage_mock_integration_closes_the_loop(monkeypatch):
    backend = MockS3StageBackend()
    client = _client(backend, monkeypatch)

    # POST /jobs → 실제 검증/분할/out_path(S3 키)/2-phase 전 과정을 태운다.
    resp = client.post("/jobs", json=_job_json())
    assert resp.status_code == 202, resp.text
    job_id = resp.json()["job_id"]

    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "DONE"
    assert body["total"] == 4

    # ① Phase 1: 각 task 가 job 프리픽스 아래 고유 S3 키로 업로드했다(총 4파일).
    prefix = s3sql.s3_job_prefix("dqe-stage", job_id)
    assert len(backend.uploaded_keys) == 4
    assert all(k.startswith(prefix) and k.endswith(".csv") for k in backend.uploaded_keys)

    # ② 루프 닫힘: Phase 2 가 job 프리픽스로 외부테이블을 만들어 Phase 1 이 올린 객체를
    #    그대로 읽어 target 에 넣었다(IN 값 4개 → 파일 합계 4행 == target 4행).
    assert len(backend.target) == 4

    # ③ Phase 2 SQL: coordinator 가 job 고유 외부테이블 + job 프리픽스 LOCATION 을 조립하고,
    #    insert_sql 의 staging 참조를 그 외부테이블 이름으로 치환했다.
    ext_ddl, pre_delete, insert_sql = backend.loads[0]
    ext_name = s3sql.external_table_name(job_id)
    assert f"CREATE EXTERNAL TABLE {ext_name}" in ext_ddl
    assert f"pxf://mybkt/{prefix}?PROFILE=s3:csv&SERVER=s3srv" in ext_ddl
    assert f"FROM {ext_name}" in insert_sql and "public.sales_mirror" in insert_sql

    # ④ CSV 방언: 기본 backtick 구분자로 객체가 써졌다(외부테이블 FORMAT 과 일치).
    first = backend.s3.get(backend.uploaded_keys[0])
    # Phase 3 에서 삭제됐을 수 있으니, 삭제 전 방언 검증은 target 존재로 갈음하고 프리픽스만 확인.
    assert backend.cleaned == [prefix]  # Phase 3 정리됨
    assert first is None  # 정리 후 인메모리 S3 에서 사라짐


def test_s3_stage_mock_integration_overwrite_pre_deletes(monkeypatch):
    backend = MockS3StageBackend()
    client = _client(backend, monkeypatch)
    job_id = client.post(
        "/jobs", json=_job_json(write_mode="overwrite_partitions")
    ).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "DONE"
    # Phase 2 에 멱등 선삭제(DELETE)가 조립됐다(job 전체 파티션 값 대상).
    _ext, pre_delete, _ins = backend.loads[0]
    assert pre_delete and pre_delete.startswith("DELETE FROM public.sales_mirror WHERE dt IN (")
    for v in ("'d1'", "'d2'", "'d3'", "'d4'"):
        assert v in pre_delete


def test_s3_stage_mock_integration_upload_failure_skips_phase2(monkeypatch):
    # Phase 1(업로드) 실패 → 배리어 후 Phase 2(적재) 건너뜀 → target 비고 job FAILED.
    backend = MockS3StageBackend(fail_export=True)
    client = _client(backend, monkeypatch)
    job_id = client.post("/jobs", json=_job_json()).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "FAILED"
    assert backend.loads == []      # Phase 2 미실행
    assert backend.target == []     # 적재 없음
    assert backend.cleaned == []    # 정리도 없음


def test_s3_stage_mock_integration_cleanup_disabled_keeps_objects(monkeypatch):
    backend = MockS3StageBackend()
    client = _client(backend, monkeypatch)
    monkeypatch.setattr(settings, "s3_delete_on_cleanup", False, raising=False)
    job_id = client.post("/jobs", json=_job_json()).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "DONE"
    # 적재는 정상이지만 delete_on_cleanup=false 라 Phase 3 를 건너뛰어 S3 객체가 남는다.
    assert len(backend.target) == 4
    assert backend.cleaned == []
    assert len(backend.s3) == 4  # 업로드한 4개 객체가 그대로 남음
    # CSV 방언: 기본 backtick 구분자로 객체가 써졌다(외부테이블 FORMAT 과 일치).
    assert "`" in next(iter(backend.s3.values()))
