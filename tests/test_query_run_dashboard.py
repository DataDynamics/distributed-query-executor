"""coordinator 가 결과 반환 실행(query-run)을 처리중·이력으로 보여 주는 경로를 검증한다.

- ``GET /query-runs`` 는 설정된 executor 전부의 /tasks 를 모아 exec_mode=query_run 만 돌려주고,
  응답하지 않는 executor 는 errors 에 적되 나머지는 돌려준다.
- ``GET /query-runs/history`` 는 task_history 를 exec_mode='query_run' 으로 한정해 task_id 별 최신
  한 건씩 읽는다(필터는 DISTINCT ON 바깥, RUNNING 은 처리중 상태 집합으로 풀린다).
- 대시보드 HTML 이 두 엔드포인트를 쓴다.
실제 executor·PostgreSQL 없이 httpx.AsyncClient 와 psycopg 를 가짜로 바꿔 검증한다.
"""

from __future__ import annotations

import sys
import types

import httpx
import pytest

from coordinator.config import settings
from coordinator.history import QueryRunHistoryRepository


class _Resp:
    def __init__(self, status: int, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


def _fake_get_client(handler):
    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, params=None, headers=None):
            return handler(url, params)

    return _FakeClient


def _task(task_id, exec_mode, status="READING", **kw):
    return {"task_id": task_id, "job_id": f"req-{task_id}", "exec_mode": exec_mode, "status": status,
            "rows_read": 3, "rows_written": 0, "phases": [], "started_at": "2026-10-21T10:00:00",
            "finished_at": None, "error": None, "username": None, "current_phase": "SOURCE_QUERY",
            **kw}


# ───────────────────────── GET /query-runs ─────────────────────────

def test_query_runs_는_모든_executor_의_query_run_task_만_모은다(client, monkeypatch):
    import coordinator.app as ca
    monkeypatch.setattr(settings, "executors", ["http://e1:8001", "http://e2:8001/"], raising=False)
    seen: list = []

    def handler(url, params):
        seen.append((url, params))
        if url.startswith("http://e1"):
            return _Resp(200, {"tasks": [_task("qr-a", "query_run"), _task("t-1", "copy")]})
        return _Resp(200, {"tasks": [_task("qr-b", "query_run", status="QUEUED",
                                           started_at=None)]})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_get_client(handler))
    body = client.get("/query-runs").json()
    assert sorted(seen) == [("http://e1:8001/tasks", {"status": "active"}),
                            ("http://e2:8001/tasks", {"status": "active"})]
    assert body["total"] == 2 and body["active"] == 2 and body["executors"] == 2 and body["errors"] == []
    by_id = {r["task_id"]: r for r in body["runs"]}
    assert set(by_id) == {"qr-a", "qr-b"}
    assert by_id["qr-a"]["executor_url"] == "http://e1:8001" and by_id["qr-a"]["executor_idx"] == 0
    assert by_id["qr-b"]["executor_url"] == "http://e2:8001/" and by_id["qr-b"]["executor_idx"] == 1
    # 시작한 것이 위로 온다.
    assert [r["task_id"] for r in body["runs"]] == ["qr-a", "qr-b"]


def test_query_runs_는_실패한_executor_를_errors_에_적고_나머지는_돌려준다(client, monkeypatch):
    import coordinator.app as ca
    monkeypatch.setattr(settings, "executors", ["http://e1:8001", "http://e2:8001"], raising=False)

    def handler(url, params):
        if url.startswith("http://e1"):
            raise httpx.ConnectError("refused")
        return _Resp(200, {"tasks": [_task("qr-b", "query_run")]})

    monkeypatch.setattr(ca.httpx, "AsyncClient", _fake_get_client(handler))
    body = client.get("/query-runs?status=").json()
    assert [r["task_id"] for r in body["runs"]] == ["qr-b"]
    assert body["errors"] == [{"executor_url": "http://e1:8001", "error": "refused"}]


def test_query_runs_는_executor_가_없으면_빈_목록이다(client, monkeypatch):
    monkeypatch.setattr(settings, "executors", [], raising=False)
    body = client.get("/query-runs").json()
    assert body == {"runs": [], "total": 0, "active": 0, "executors": 0, "errors": []}


# ───────────────────────── 이력 저장소 ─────────────────────────

class _FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append((sql, params))

    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, log):
        self._log = log

    def cursor(self):
        return _FakeCursor(self._log)

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture()
def fake_psycopg(monkeypatch):
    log: list = []
    mod = types.ModuleType("psycopg")
    mod.connect = lambda dsn, **kw: _FakeConn(log)  # meta_connect 가 connect_timeout 을 넘긴다
    monkeypatch.setitem(sys.modules, "psycopg", mod)
    return log


def _settings(**kw):
    base = {"history_db_dsn": "postgresql://u:p@h/db", "task_history_table": "public.task_history"}
    base.update(kw)
    return types.SimpleNamespace(**base)


def test_query_run_history_는_exec_mode_로_한정하고_필터를_바깥에_건다(fake_psycopg):
    repo = QueryRunHistoryRepository(_settings())
    out = repo.read(limit=10, offset=20, status="failed", username="kim", job_id="req-ab")
    assert out["enabled"] is True
    count_sql, count_params = fake_psycopg[0]
    page_sql, page_params = fake_psycopg[1]
    for sql in (count_sql, page_sql):
        assert "DISTINCT ON (task_id)" in sql
        assert "FROM public.task_history WHERE exec_mode = 'query_run'" in sql
        # 필터는 파생 테이블 바깥의 WHERE 에 붙는다.
        assert sql.index(") t WHERE status = %s AND username = %s AND job_id LIKE %s") > sql.index("DISTINCT ON")
    assert count_params == ("FAILED", "kim", "req-ab%")
    assert page_params == ("FAILED", "kim", "req-ab%", 10, 20)


def test_query_run_history_의_RUNNING_은_처리중_상태_집합이다(fake_psycopg):
    QueryRunHistoryRepository(_settings()).read(status="running")
    sql, params = fake_psycopg[0]
    assert "status = ANY(%s)" in sql and params == (["QUEUED", "READING", "WRITING"],)


def test_query_run_history_는_필터_없이_WHERE_를_덧붙이지_않는다(fake_psycopg):
    QueryRunHistoryRepository(_settings()).read()
    sql, params = fake_psycopg[0]
    assert sql.endswith(") t") and params == ()


def test_query_run_history_는_dsn_없으면_비활성이다():
    out = QueryRunHistoryRepository(_settings(history_db_dsn="")).read()
    assert out == {"enabled": False, "rows": [], "total": 0, "limit": 20, "offset": 0}


def test_query_runs_history_엔드포인트는_limit_를_clamp_한다(client, monkeypatch):
    import coordinator.app as ca
    captured = {}

    def fake_read(**kw):
        captured.update(kw)
        return {"enabled": True, "rows": [], "total": 0, "limit": kw["limit"], "offset": kw["offset"]}

    monkeypatch.setattr(ca.QueryRunHistoryRepository, "read", lambda self, **kw: fake_read(**kw))
    body = client.get("/query-runs/history?limit=999&offset=-5&status=DONE&job_id=req-").json()
    assert body["enabled"] is True
    assert captured == {"limit": 200, "offset": 0, "status": "DONE", "username": None, "job_id": "req-"}


# ───────────────────────── 대시보드 ─────────────────────────

def test_dashboard_는_query_run_처리중과_이력을_그린다(client):
    html = client.get("/").text
    assert '/query-runs?status=active' in html
    assert '/query-runs/history?limit=' in html
    assert "결과 반환 실행(query-run) 처리중" in html
    assert "결과 반환 실행(query-run) 이력" in html
    # SQL 상세는 executor 프록시로 받는다.
    assert "/executors/${idx}/tasks/${encodeURIComponent(taskId)}/detail" in html
