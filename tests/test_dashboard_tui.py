"""coordinator 대시보드 TUI 의 순수 로직(포매터·API 클라이언트 URL·기본 URL) 테스트.

curses UI 는 대화형이라 다루지 않고, JSON → 표시 문자열 변환과 API 경로/파라미터 구성처럼
회귀 위험이 큰 부분만 검증한다.
"""

from __future__ import annotations

from coordinator.tui import (
    CoordinatorApi,
    default_base_url,
    fmt_bar,
    fmt_cluster_summary,
    fmt_executor_detail,
    fmt_executor_rows,
    fmt_history_rows,
    fmt_job_detail,
    fmt_job_rows,
    fmt_pct,
    short_id,
)


# ── 작은 유틸 ──────────────────────────────────────────────────────────────

def test_fmt_pct_and_bar_and_short_id():
    assert fmt_pct(12.34) == "12.3%"
    assert fmt_pct(None) == "-"
    assert fmt_pct("x") == "-"
    assert fmt_bar(0, 10) == "░" * 10
    assert fmt_bar(100, 10) == "█" * 10
    assert fmt_bar(50, 10).count("█") == 5
    assert fmt_bar(999, 4) == "█" * 4   # 범위 밖 클램프
    assert short_id("abcdefghijklmnop", 8) == "abcdefgh"
    assert short_id(None) == ""


# ── 클러스터/executor ──────────────────────────────────────────────────────

def _cluster():
    return {
        "coordinator": {"status": "ok", "metrics": {"cpu_percent": 3.2, "memory_percent": 40.0, "disk_percent": 55.5}},
        "executors": [
            {"executor_url": "http://e1:8087", "healthy": True, "cpu_percent": 10.0, "memory_percent": 20.0, "index": 0},
            {"executor_url": "http://e2:8086", "healthy": False, "cpu_percent": None, "memory_percent": None, "index": 1},
        ],
        "executors_summary": {"total": 2, "healthy": 1, "unhealthy": 1},
        "jobs": {"running": 1, "active": 2, "total": 5, "by_status": {"DONE": 3, "RUNNING": 1}},
        "assignment_counts": {"http://e1:8087": 7},
        "executor_select": "p2c",
    }


def test_fmt_cluster_summary():
    lines = fmt_cluster_summary(_cluster())
    joined = "\n".join(lines)
    assert "coordinator: ok" in joined
    assert "total=2" in joined and "healthy=1" in joined
    assert "running=1" in joined and "DONE=3" in joined
    assert "p2c" in joined


def test_fmt_executor_rows_has_header_and_index_and_assigned():
    rows = fmt_executor_rows(_cluster())
    assert "executor" in rows[0] and "health" in rows[0]
    body = "\n".join(rows[1:])
    assert "up" in body and "down" in body
    assert "http://e1:8087" in body
    assert "7" in body            # assignment_counts 반영
    # 두 executor + 헤더
    assert len(rows) == 3


def test_fmt_executor_rows_empty():
    rows = fmt_executor_rows({"executors": []})
    assert "executor 없음" in "\n".join(rows)


# ── jobs ───────────────────────────────────────────────────────────────────

def test_fmt_job_rows_and_empty():
    body = {"jobs": [
        {"job_id": "job-123456789", "status": "RUNNING", "progress_percent": 50.0,
         "total_rows_written": 1234, "target_table": "public.t"},
    ]}
    rows = fmt_job_rows(body)
    assert "job_id" in rows[0]
    assert "RUNNING" in rows[1] and "50.0%" in rows[1] and "public.t" in rows[1]
    assert "job 없음" in "\n".join(fmt_job_rows({"jobs": []}))


def test_fmt_job_detail_lists_tasks():
    job = {
        "job_id": "j1", "status": "DONE", "progress_percent": 100.0,
        "target_table": "public.t", "exec_mode": "copy", "total_rows_written": 20,
        "tasks": [
            {"task_id": "t1", "status": "DONE", "rows_written": 10, "executor_url": "http://e1:8087"},
            {"task_id": "t2", "status": "DONE", "rows_written": 10, "executor_url": "http://e2:8086"},
        ],
    }
    lines = fmt_job_detail(job)
    joined = "\n".join(lines)
    assert "job_id: j1" in joined and "tasks (2)" in joined
    assert "t1" in joined and "http://e1:8087" in joined


# ── executor 상세(프록시 결과) ──────────────────────────────────────────────

def test_fmt_executor_detail():
    metrics = {"cpu_percent": 5.0, "memory_percent": 30.0, "disk_percent": 40.0,
               "tasks": {"active": 2, "queued": 1, "max": 8}, "gp_hostname": "seg1"}
    tasks = {"tasks": [{"task_id": "t1", "status": "READING", "rows_written": 0, "job_id": "j1"}], "total": 1}
    lines = fmt_executor_detail(metrics, tasks)
    joined = "\n".join(lines)
    assert "active=2" in joined and "max=8" in joined
    assert "gp_hostname: seg1" in joined
    assert "t1" in joined and "READING" in joined


# ── history ────────────────────────────────────────────────────────────────

def test_fmt_history_rows():
    body = {"history": [
        {"job_id": "h1", "status": "DONE", "username": "alice", "total_rows_written": 99, "finished_at": "2026-07-20T00:00:00"},
    ]}
    rows = fmt_history_rows(body)
    assert "job_id" in rows[0]
    assert "h1" in rows[1] and "alice" in rows[1] and "99" in rows[1]
    assert "이력 없음" in "\n".join(fmt_history_rows({"history": []}))


# ── API 클라이언트 경로/파라미터 ────────────────────────────────────────────

class _CapClient:
    """httpx.Client 대역: get 호출의 url/params 를 기록하고 지정 payload 반환."""

    calls: list = []

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, params=None):
        _CapClient.calls.append((url, params))
        return _Resp()


class _Resp:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


def test_api_builds_paths_and_params(monkeypatch):
    import coordinator.tui as tui
    import httpx

    _CapClient.calls = []
    monkeypatch.setattr(httpx, "Client", _CapClient)
    api = CoordinatorApi("http://host:8088/")

    api.cluster()
    api.jobs(status="active", limit=10)
    api.job("j1")
    api.executor_tasks(2)
    api.executor_metrics(2)
    api.history(limit=25)

    urls = [c[0] for c in _CapClient.calls]
    assert urls == [
        "http://host:8088/cluster",
        "http://host:8088/jobs",
        "http://host:8088/jobs/j1",
        "http://host:8088/executors/2/tasks",
        "http://host:8088/executors/2/metrics",
        "http://host:8088/history",
    ]
    # 파라미터: cluster refresh, jobs status/limit, history limit
    assert _CapClient.calls[0][1] == {"refresh": "true"}
    assert _CapClient.calls[1][1] == {"status": "active", "limit": 10}
    assert _CapClient.calls[5][1] == {"limit": 25}


def test_default_base_url_env(monkeypatch):
    monkeypatch.setenv("COORDINATOR_URL", "http://custom:9999")
    assert default_base_url() == "http://custom:9999"
