"""executor self-view 대시보드 엔드포인트(/, /tasks, /info, /config) 테스트."""

from __future__ import annotations

import asyncio

import httpx

from core.version import __version__
from executor.app import create_app as create_executor_app


def _client(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://x")


async def test_dashboard_root_serves_html():
    app = create_executor_app()
    async with _client(app) as c:
        r = await c.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "Query Executor" in r.text


async def test_tasks_list_counts_and_active_filter():
    app = create_executor_app()
    payload = {
        "task_id": "t-1",
        "job_id": "job-1",
        "sub_query": "SELECT 1 WHERE dt IN ('1')",
        "target_table": "public.t",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
    }
    async with _client(app) as c:
        await c.post("/tasks", json=payload)
        # 완료될 때까지 대기
        for _ in range(200):
            st = (await c.get("/tasks/t-1")).json()
            if st["status"] in ("DONE", "FAILED"):
                break
            await asyncio.sleep(0.01)

        full = (await c.get("/tasks")).json()
        assert full["total"] == 1
        # 종료된 task 는 active 집계에서 빠진다
        assert full["active"] == 0
        assert full["running"] == 0
        # view() 확장 필드가 노출된다
        row = full["tasks"][0]
        assert row["task_id"] == "t-1"
        assert "started_at" in row and "finished_at" in row
        assert row["exec_mode"] == "copy"

        # status=active 필터: 종료 task 는 제외된다
        only_active = (await c.get("/tasks?status=active")).json()
        assert only_active["tasks"] == []


async def test_info_and_config_endpoints():
    app = create_executor_app()
    async with _client(app) as c:
        info = (await c.get("/info")).json()
        assert "executor_id" in info
        assert info["version"] == __version__
        assert "tasks_by_status" in info

        conf = (await c.get("/config")).json()
        assert isinstance(conf["config"], list)
        # 비밀값(greenplum.dsn 등)이 평문으로 노출되지 않는다
        for row in conf["config"]:
            assert ":@" not in str(row["value"]) or "***" in str(row["value"])


async def test_config_endpoint_exposes_s3_stage_section():
    """executor 환경설정 탭에도 s3_stage 설정(s3.*)이 보여야 한다(Phase 1 업로드 대상)."""
    app = create_executor_app()
    async with _client(app) as c:
        rows = (await c.get("/config")).json()["config"]
    keys = {(r["section"], r["key"]) for r in rows}
    for key in ("bucket", "prefix", "external_schema", "endpoint_url",
                "access_key", "secret_key", "pxf_server", "delete_on_cleanup"):
        assert ("s3", key) in keys, key


async def test_config_endpoint_exposes_stage_section():
    """executor 환경설정 탭에 local_stage 설정(stage.*)과 gp_hostname 이 보여야 한다."""
    app = create_executor_app()
    async with _client(app) as c:
        rows = (await c.get("/config")).json()["config"]
    keys = {(r["section"], r["key"]) for r in rows}
    for key in ("local_dir", "csv_delimiter", "csv_null", "csv_quote", "cleanup",
                "validate_hosts", "max_files_per_host", "impala_convert_types"):
        assert ("stage", key) in keys, key
    assert ("executor", "gp_hostname") in keys


async def test_dashboard_has_cancel_action():
    """처리중 탭의 task 취소 버튼(JS 핸들러)이 HTML 에 포함돼야 한다."""
    app = create_executor_app()
    async with _client(app) as c:
        html = (await c.get("/")).text
    assert "cancelTask(" in html     # 활성 task 취소 → POST /tasks/{id}/cancel
    assert "postJSON" in html        # 액션 공용 헬퍼


async def test_task_detail_includes_sql():
    """/tasks/{id}/detail 은 목록 뷰에서 제외되는 실행 SQL 전문을 포함해야 한다.

    대시보드 처리중 탭의 task SQL 열람(coordinator 의 task 상세 조회와 대칭)이 사용한다.
    """
    app = create_executor_app()
    payload = {
        "task_id": "t-detail",
        "job_id": "job-1",
        "sub_query": "SELECT 1 WHERE dt IN ('1')",
        "target_table": "public.t",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
    }
    async with _client(app) as c:
        await c.post("/tasks", json=payload)
        for _ in range(200):
            st = (await c.get("/tasks/t-detail")).json()
            if st["status"] in ("DONE", "FAILED"):
                break
            await asyncio.sleep(0.01)
        # 목록/상태 조회에는 SQL 이 없어야 하고(경량 유지), detail 에는 있어야 한다
        assert "sub_query" not in st
        d = (await c.get("/tasks/t-detail/detail")).json()
        assert d["sub_query"] == "SELECT 1 WHERE dt IN ('1')"
        assert d["task_id"] == "t-detail"
        assert (await c.get("/tasks/no-such/detail")).status_code == 404


async def test_dashboard_has_datasource_tab_and_hist_filter():
    """데이터소스 탭과 실행 이력 필터 바가 executor 대시보드 HTML 에 포함돼야 한다."""
    app = create_executor_app()
    async with _client(app) as c:
        html = (await c.get("/")).text
        body = (await c.get("/history?status=FAILED&username=kim&job_id=abc")).json()
    assert 'data-tab="ds"' in html
    assert 'id="hist-filter"' in html
    assert "runDs(" in html
    # 이력 비활성(기본 설정)에서도 필터 파라미터가 422 없이 수용돼야 한다
    assert body["enabled"] is False
