"""coordinator/executor 의 /health, /metrics 및 executor 헬스 모니터 테스트."""

from __future__ import annotations

import httpx
from executor.app import create_app as create_executor_app
from fastapi.testclient import TestClient

from coordinator.monitor import HealthMonitor


def test_coordinator_health(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["service"] == "coordinator"


def test_coordinator_metrics_reports_cpu_memory_disk(client):
    body = client.get("/metrics").json()
    assert isinstance(body["cpu_percent"], (int, float))
    assert "percent" in body["memory"]
    assert "used_mb" in body["memory"]
    assert "percent" in body["disk"]


def test_coordinator_executors_endpoint_empty_by_default(client):
    # 기본 설정에는 executor가 없으므로 빈 목록
    body = client.get("/executors").json()
    assert body == {"executors": []}


def test_executor_health_and_metrics():
    ex = TestClient(create_executor_app())
    health = ex.get("/health").json()
    assert health["status"] == "ok"
    assert health["service"] == "executor"

    metrics = ex.get("/metrics").json()
    assert isinstance(metrics["cpu_percent"], (int, float))
    assert "percent" in metrics["memory"]
    assert "percent" in metrics["disk"]


def test_healthz_alias_still_works(client):
    assert client.get("/healthz").json() == {"status": "ok"}


# ----------------------------- Swagger / OpenAPI -----------------------------


def test_coordinator_swagger_and_openapi(client):
    assert client.get("/docs").status_code == 200          # Swagger UI
    assert client.get("/redoc").status_code == 200          # ReDoc
    schema = client.get("/openapi.json").json()
    assert schema["info"]["title"] == "Distributed Query Coordinator"
    # 주요 경로가 스키마에 포함되는지
    assert "/jobs" in schema["paths"]
    assert "/metrics" in schema["paths"]
    # 태그가 부여되었는지
    assert "Jobs" in {t for p in schema["paths"].values()
                      for op in p.values() for t in op.get("tags", [])}


def test_executor_swagger_and_openapi():
    ex = TestClient(create_executor_app())
    assert ex.get("/docs").status_code == 200
    schema = ex.get("/openapi.json").json()
    assert schema["info"]["title"] == "Distributed Query Executor"
    assert "/tasks" in schema["paths"]
    assert "/metrics" in schema["paths"]


class _MonitorSettings:
    """모니터 단위 테스트용 최소 설정."""

    executors = ["http://exec"]
    monitor_enabled = True
    monitor_health_interval_s = 10
    monitor_record_interval_s = 60
    monitor_db_dsn = ""
    monitor_table = "executor_health_metrics"


async def test_monitor_polls_executor_health_and_metrics():
    # coordinator가 executor의 /health·/metrics를 호출해 CPU/메모리 상태를 보유하는지 검증
    transport = httpx.ASGITransport(app=create_executor_app())
    monitor = HealthMonitor(_MonitorSettings())
    async with httpx.AsyncClient(transport=transport) as client:
        await monitor._poll_one(client, "http://exec")

    rec = monitor.executors["http://exec"]
    assert rec.healthy is True
    assert rec.cpu_percent is not None
    assert rec.memory_percent is not None
    assert rec.memory_used_mb is not None
    assert rec.error is None

    # 모니터 스냅샷이 PG 기록용 필드를 모두 갖추는지
    snap = monitor.snapshot()[0]
    assert snap["executor_url"] == "http://exec"
    assert {"cpu_percent", "memory_percent", "disk_percent"} <= snap.keys()
