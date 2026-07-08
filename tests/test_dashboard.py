"""/ 모니터링 대시보드 및 데이터 API(/jobs, /config, /info) 테스트."""

from __future__ import annotations

from coordinator.dashboard import mask_dsn, masked_config
from coordinator.config import settings as core_settings
from coordinator.models import Job, JobStatus


def test_mask_dsn_hides_password():
    assert mask_dsn("postgresql://user:secret@host:5432/db") == \
        "postgresql://user:***@host:5432/db"
    assert mask_dsn("") == ""


def test_masked_config_no_plaintext_secrets():
    rows = masked_config(core_settings)
    keys = {(r["section"], r["key"]) for r in rows}
    assert ("greenplum", "dsn") in keys
    assert ("coordinator", "executor_mode") in keys
    # 비밀번호가 평문으로 노출되지 않아야 한다
    for r in rows:
        val = str(r["value"])
        assert "secret" not in val.lower()


def test_dashboard_html_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "Query Coordinator 모니터링" in r.text


def test_config_endpoint(client):
    body = client.get("/config").json()
    assert "config" in body and isinstance(body["config"], list)
    assert any(r["section"] == "coordinator" for r in body["config"])


def test_info_endpoint(client):
    body = client.get("/info").json()
    assert body["version"]
    assert "coordinator_id" in body
    assert "jobs_by_status" in body
    assert "uptime_seconds" in body


def test_jobs_list_endpoint(client, store):
    # 다양한 상태의 job 적재
    for st in (JobStatus.RUNNING, JobStatus.DONE, JobStatus.SPLITTING):
        store.add(Job(
            original_sql="SELECT 1 FROM t WHERE dt IN ('1')",
            partition_column="dt", target_table="public.t", write_mode="append",
            parallelism=1, split_strategy="contiguous", failure_policy="fail_fast",
            status=st,
        ))
    body = client.get("/jobs").json()
    assert body["total"] == 3
    assert body["running"] == 1
    assert body["active"] == 2  # RUNNING + SPLITTING
    assert len(body["jobs"]) == 3
    # status 필터
    only_running = client.get("/jobs?status=running").json()
    assert all(j["status"] in ("RUNNING", "SPLITTING", "PENDING") for j in only_running["jobs"])


def test_history_endpoint_disabled_without_dsn(client):
    # 기본 설정엔 history.db_dsn 이 없어 비활성(빈 결과)
    body = client.get("/history?limit=20&offset=0").json()
    assert body["enabled"] is False
    assert body["rows"] == []
    assert body["total"] == 0


def test_history_clamps_limit(client):
    body = client.get("/history?limit=9999&offset=-5").json()
    assert body["limit"] <= 200
    assert body["offset"] == 0


def test_dashboard_has_history_tab(client):
    html = client.get("/").text
    assert 'data-tab="hist"' in html
    assert "실행 이력" in html


def test_dashboard_disabled(monkeypatch):
    from fastapi.testclient import TestClient
    from coordinator.app import create_app
    from coordinator.job_store import InMemoryJobStore
    from tests.conftest import FakeRunner

    monkeypatch.setattr(core_settings, "dashboard_enabled", False, raising=False)
    c = TestClient(create_app(runner=FakeRunner(), store=InMemoryJobStore()))
    assert c.get("/").status_code == 404      # 대시보드 비활성
    assert c.get("/jobs").status_code == 200   # /jobs 는 항상 제공


def test_dashboard_has_cancel_retry_actions(client):
    """처리중 탭의 취소 버튼과 이력 탭의 재실행 버튼(JS 핸들러)이 HTML 에 포함돼야 한다."""
    html = client.get("/").text
    assert "cancelJob(" in html      # 활성 job 취소 → POST /jobs/{id}/cancel
    assert "retryJob(" in html       # 실패 파티션 재실행 → POST /jobs/{id}/retry
    assert "postJSON" in html        # 액션 공용 헬퍼
