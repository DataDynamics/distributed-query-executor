"""coordinator 대시보드 TUI 의 순수 로직(포매터·API 클라이언트 URL·기본 URL) 테스트.

JSON → 표시 문자열 변환과 API 경로/파라미터 구성처럼 회귀 위험이 큰 부분을 검증한다. 키 입력
시나리오는 다루지 않지만 화면 그리기만은 가짜 curses 로 훑는데, 한글이 섞인 줄을 글자 수로
자르면 폭 80칸 터미널에서 화면을 넘겨 TUI 가 통째로 죽기 때문이다.
"""

from __future__ import annotations

import coordinator.tui as tui
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


# ── 폴링 상태와 커서 클램프 ────────────────────────────────────────────────

class _StubApi:
    """호출할 때마다 미리 준 줄 수만큼의 목록을 돌려주는 가짜 API 다."""

    def __init__(self):
        self.base_url = "http://127.0.0.1:8088"
        self.rows = 50

    def cluster(self):
        return {"executors": [{"index": i, "executor_url": f"http://e{i}"} for i in range(self.rows)],
                "assignment_counts": {}}


def test_목록이_짧아지면_커서와_스크롤을_당겨_온다():
    """폴링 중에 job 이 끝나 목록이 줄면 커서가 끝 너머에 남아 화면이 통째로 비었다."""
    app = tui.DashboardTUI(_StubApi(), interval=2.0)
    app.set_tab(2)                       # Executors 탭
    app.row = app.top = 45               # 아래쪽을 보던 중
    app.api.rows = 3                     # executor 가 줄었다
    app.refresh()
    assert app.row <= len(app.lines) - 1
    assert app.top <= len(app.lines) - 1
    # 클램프 뒤에는 화면에 그릴 줄이 실제로 남아 있어야 한다.
    assert app.lines[app.top:]


def test_refresh_는_갱신_시각을_남긴다():
    app = tui.DashboardTUI(_StubApi(), interval=2.0)
    assert app.refreshed_at == "-"
    app.refresh()
    assert app.refreshed_at != "-" and len(app.refreshed_at) == 8   # HH:MM:SS


def test_실패한_refresh_는_갱신_시각을_올리지_않는다():
    """화면이 언제 것인지 알려면 성공한 폴링만 시각을 갱신해야 한다."""
    class _Broken(_StubApi):
        def cluster(self):
            raise RuntimeError("연결 실패")

    app = tui.DashboardTUI(_Broken(), interval=2.0)
    app.set_tab(2)
    assert app.refreshed_at == "-"
    assert app.error is not None


def test_set_interval_은_범위_안으로_가둔다():
    app = tui.DashboardTUI(_StubApi(), interval=2.0)
    assert app.set_interval(0.1) == 0.5        # 너무 촘촘하면 coordinator 만 두드린다
    assert app.set_interval(999) == 60.0       # 너무 성기면 모니터가 아니다
    assert app.set_interval(3.0) == 3.0


# ── 화면 그리기(가짜 curses) ───────────────────────────────────────────────

class _FakeCurses:
    KEY_LEFT, KEY_RIGHT, KEY_UP, KEY_DOWN = 260, 261, 259, 258
    KEY_PPAGE, KEY_NPAGE, KEY_HOME, KEY_END, KEY_ENTER = 339, 338, 262, 360, 343
    A_BOLD = A_REVERSE = 1

    def color_pair(self, n):
        return 0


class _FakeScreen:
    """화면 밖으로 넘겨 쓰면 그 자리에서 실패한다(진짜 curses 는 예외를 던진다)."""

    def __init__(self, h=24, w=80):
        self.h, self.w, self.lines = h, w, {}

    def getmaxyx(self):
        return (self.h, self.w)

    def erase(self):
        self.lines.clear()

    def refresh(self):
        pass

    def addstr(self, y, x, s, attr=0):
        from core.textui import disp_width
        assert 0 <= y < self.h and 0 <= x < self.w, f"({y},{x}) 가 화면 밖"
        assert x + disp_width(s) <= self.w, f"y={y} 오른쪽 넘침({disp_width(s)}칸, 폭 {self.w})"
        self.lines[y] = self.lines.get(y, "") + s


def test_긴_한글_줄과_상태줄이_좁은_화면을_넘지_않는다():
    """상태 줄은 한글이라 글자 수로 자르면 80칸 터미널에서 폭의 두 배까지 밀렸다."""
    app = tui.DashboardTUI(_StubApi(), interval=2.0)
    app.lines = ["가" * 200, "x" * 300, "한글이 섞인 아주 긴 줄 " * 20]
    app.refreshed_at = "12:34:56"
    for size in ((24, 80), (24, 60), (10, 40)):
        for tab in range(5):
            for paused in (False, True):
                app.tab, app.row, app.top, app.paused = tab, 0, 0, paused
                app._draw(_FakeScreen(*size), _FakeCurses())
        app.error = "ConnectError: 연결이 거부되었습니다 " * 5
        app._draw(_FakeScreen(*size), _FakeCurses())
        app.error = None


def test_선택한_탭은_좁은_화면에서도_탭_바에_보인다():
    app = tui.DashboardTUI(_StubApi(), interval=2.0)
    app.lines = ["줄"]
    for width in (80, 50, 34):
        for tab in range(5):
            app.tab, app.row, app.top = tab, 0, 0
            screen = _FakeScreen(24, width)
            app._draw(screen, _FakeCurses())
            assert tui.TABS[tab][0] in screen.lines[1], f"w={width}: {tui.TABS[tab][0]} 이 탭 바에 없다"


def test_상태줄에_갱신_시각과_주기가_보인다():
    app = tui.DashboardTUI(_StubApi(), interval=2.0)
    app.lines = ["줄"]
    app.refreshed_at = "12:34:56"
    screen = _FakeScreen(24, 100)
    app._draw(screen, _FakeCurses())
    assert "12:34:56" in screen.lines[23] and "2초" in screen.lines[23]
    app.paused = True
    screen = _FakeScreen(24, 100)
    app._draw(screen, _FakeCurses())
    assert "정지" in screen.lines[23]
