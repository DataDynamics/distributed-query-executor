"""coordinator 의 executor 상세 프록시(GET /executors/{idx}/...) + index 주석 테스트.

TUI/대시보드가 coordinator 한 곳만 붙어 특정 executor 의 task/metrics 화면을 그릴 수 있게
하는 읽기 프록시. 실제 executor 없이 httpx.AsyncClient 를 가짜로 대체해 검증한다(datasource
프록시 테스트와 동일한 관례).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.config import settings


class _Resp:
    def __init__(self, status: int, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


def _fake_get_client(handler):
    """.get(url, params) 호출을 handler 로 넘기는 가짜 httpx.AsyncClient 클래스를 만든다."""

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, params=None):
            return handler(url, params)

    return _FakeClient


# ── 프록시 정상 경로 ────────────────────────────────────────────────────────

def test_proxy_tasks_forwards_with_params(client, monkeypatch):
    import coordinator.app as ca

    monkeypatch.setattr(settings, "executors", ["http://exec:8001/"], raising=False)
    captured: dict = {}

    def handler(url, params):
        captured["url"] = url
        captured["params"] = params
        return _Resp(200, {"tasks": [], "total": 0, "active": 0, "running": 0})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_get_client(handler))
    body = client.get("/executors/0/tasks?status=active&limit=5").json()

    # 슬래시 정규화 후 executor /tasks 로 포워딩, status/limit 전달.
    assert captured["url"] == "http://exec:8001/tasks"
    assert captured["params"] == {"status": "active", "limit": 5}
    assert body["proxied_to"] == "http://exec:8001/"


def test_proxy_metrics_forwards(client, monkeypatch):
    import coordinator.app as ca

    monkeypatch.setattr(settings, "executors", ["http://exec:8001"], raising=False)

    def handler(url, params):
        assert url == "http://exec:8001/metrics"
        return _Resp(200, {"cpu_percent": 1.0, "tasks": {"active": 0, "queued": 0, "max": 8}})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_get_client(handler))
    body = client.get("/executors/0/metrics").json()
    assert body["tasks"]["max"] == 8
    assert body["proxied_to"] == "http://exec:8001"


def test_proxy_task_detail_forwards(client, monkeypatch):
    import coordinator.app as ca

    monkeypatch.setattr(settings, "executors", ["http://exec:8001"], raising=False)

    def handler(url, params):
        assert url == "http://exec:8001/tasks/t-9/detail"
        return _Resp(200, {"task_id": "t-9", "sub_query": "SELECT 1"})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_get_client(handler))
    body = client.get("/executors/0/tasks/t-9/detail").json()
    assert body["task_id"] == "t-9"


# ── 경계/에러 ──────────────────────────────────────────────────────────────

def test_proxy_index_out_of_range_404(client, monkeypatch):
    # executor 목록이 비어 있으면 어떤 인덱스도 404(allowlist 밖).
    monkeypatch.setattr(settings, "executors", [], raising=False)
    r = client.get("/executors/0/tasks")
    assert r.status_code == 404


def test_proxy_propagates_executor_error(client, monkeypatch):
    import coordinator.app as ca

    monkeypatch.setattr(settings, "executors", ["http://exec:8001"], raising=False)

    def handler(url, params):
        return _Resp(503, {"detail": "draining — 종료 중"})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_get_client(handler))
    r = client.get("/executors/0/metrics")
    assert r.status_code == 503
    assert "draining" in r.json()["detail"]


# ── index 주석(드릴인 키) ───────────────────────────────────────────────────

def test_cluster_annotates_executor_index(runner, store, monkeypatch):
    # executor 를 설정한 채 앱을 만들면 /cluster 의 각 executor 엔트리에 설정목록 index 가 붙는다.
    monkeypatch.setattr(settings, "executors", ["http://e1:1", "http://e2:2"], raising=False)
    c = TestClient(create_app(runner=runner, store=store))
    body = c.get("/cluster?refresh=false").json()
    execs = body["executors"]
    assert len(execs) == 2
    by_url = {e["executor_url"]: e["index"] for e in execs}
    assert by_url == {"http://e1:1": 0, "http://e2:2": 1}


def test_executors_endpoint_annotates_index(runner, store, monkeypatch):
    monkeypatch.setattr(settings, "executors", ["http://only:9"], raising=False)
    c = TestClient(create_app(runner=runner, store=store))
    execs = c.get("/executors").json()["executors"]
    assert execs and execs[0]["index"] == 0
