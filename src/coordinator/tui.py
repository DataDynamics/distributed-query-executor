"""coordinator 대시보드를 터미널에서 보는 **읽기 전용** curses 모니터다.

coordinator 의 웹 대시보드가 쓰는 것과 동일한 JSON API 를 폴링해 클러스터/job/executor/
이력을 텍스트 UI 로 그린다(HTML 스크래핑 아님). 개별 executor 상세(task 목록/메트릭)는
coordinator 프록시(`GET /executors/{idx}/tasks`·`/metrics`)로 가져오므로, **coordinator 한
곳만** 붙어도 executor 화면까지 볼 수 있다(각 executor 에 직접 접속하지 않는다).

## 구성

* :class:`CoordinatorApi` — httpx 로 coordinator JSON API 를 호출하는 얇은 클라이언트.
* 순수 포매터(:func:`fmt_cluster_summary`/:func:`fmt_executor_rows`/:func:`fmt_job_rows`/
  :func:`fmt_executor_tasks` 등) — curses·네트워크 무관, 테스트 대상.
* curses UI: :class:`DashboardTUI` 와 :func:`run_tui`.
* 진입점: :func:`main` (`python -m coordinator.tui` 또는 `bin/dashboard-tui.sh`).

에어갭 전제라 외부 TUI 라이브러리 없이 표준 ``curses`` 만 쓴다(config-tui 와 동일 노선).
읽기 전용이므로 job 취소/재시도 같은 쓰기 동작은 하지 않는다(향후 옵션).
"""

from __future__ import annotations

import argparse
import os
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# CoordinatorApi — coordinator JSON API 클라이언트
# ─────────────────────────────────────────────────────────────────────────────


class CoordinatorApi:
    """coordinator REST API 를 호출하는 얇은 클라이언트다. 읽기 전용 GET 만 쓴다.

    각 호출마다 짧은 타임아웃의 httpx 클라이언트를 열어 JSON 을 돌려준다. 폴링 주기가
    길지 않아도(수 초) 모니터 용도엔 충분하다. 오류는 호출부(UI)가 잡아 배너로 표시한다.
    """

    def __init__(self, base_url: str, timeout: float = 3.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, params: dict | None = None) -> Any:
        import httpx  # 지연 import: 테스트에서 순수 포매터만 쓸 때 httpx 불필요

        with httpx.Client(timeout=self.timeout) as c:
            r = c.get(self.base_url + path, params=params or {})
            r.raise_for_status()
            return r.json()

    # 클러스터 상태와 집계를 가져온다.
    def cluster(self) -> Any:
        return self._get("/cluster", {"refresh": "true"})

    def info(self) -> Any:
        return self._get("/info")

    def config(self) -> Any:
        return self._get("/config")

    # jobs
    def jobs(self, status: str | None = None, limit: int = 0) -> Any:
        params: dict = {}
        if status:
            params["status"] = status
        if limit:
            params["limit"] = limit
        return self._get("/jobs", params)

    def job(self, job_id: str) -> Any:
        return self._get(f"/jobs/{job_id}")

    def history(self, limit: int = 50) -> Any:
        return self._get("/history", {"limit": limit})

    # executor 상세를 coordinator 프록시로 가져온다.
    def executor_metrics(self, idx: int) -> Any:
        return self._get(f"/executors/{idx}/metrics")

    def executor_tasks(self, idx: int) -> Any:
        return self._get(f"/executors/{idx}/tasks")


# ─────────────────────────────────────────────────────────────────────────────
# 순수 포매터(테스트 대상, curses/네트워크 무관)
# ─────────────────────────────────────────────────────────────────────────────


def fmt_pct(v: Any) -> str:
    """숫자를 ``12.3%`` 형태로 만든다. 값이 없으면 ``-`` 를 돌려준다."""
    try:
        return f"{float(v):.1f}%"
    except (TypeError, ValueError):
        return "-"


def fmt_bar(pct: Any, width: int = 10) -> str:
    """0~100 백분율을 ``█████░░░░░`` 막대로 만든다(범위 밖은 클램프)."""
    try:
        p = max(0.0, min(100.0, float(pct)))
    except (TypeError, ValueError):
        p = 0.0
    filled = int(round(p / 100.0 * width))
    return "█" * filled + "░" * (width - filled)


def short_id(s: Any, n: int = 10) -> str:
    """긴 식별자를 앞 n자로 자른다. 이미 짧으면 그대로 두고, None 이면 빈 문자열을 돌려준다."""
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[:n]


def fmt_cluster_summary(cluster: dict) -> list[str]:
    """`/cluster` JSON 에서 상단 요약 줄들을 만든다(coordinator 상태·executor·job 집계)."""
    coord = cluster.get("coordinator", {}) or {}
    cm = coord.get("metrics", {}) or {}
    es = cluster.get("executors_summary", {}) or {}
    jobs = cluster.get("jobs", {}) or {}
    by = jobs.get("by_status", {}) or {}
    by_str = "  ".join(f"{k}={v}" for k, v in sorted(by.items())) or "(없음)"
    return [
        f"coordinator: {coord.get('status', '?')}   "
        f"CPU {fmt_pct(cm.get('cpu_percent'))}  MEM {fmt_pct(cm.get('memory_percent'))}  "
        f"DISK {fmt_pct(cm.get('disk_percent'))}",
        f"executors: total={es.get('total', 0)}  "
        f"healthy={es.get('healthy', 0)}  unhealthy={es.get('unhealthy', 0)}   "
        f"정책={cluster.get('executor_select', '?')}",
        f"jobs: running={jobs.get('running', 0)}  active={jobs.get('active', 0)}  "
        f"total={jobs.get('total', 0)}   [{by_str}]",
    ]


def fmt_executor_rows(cluster: dict) -> list[str]:
    """`/cluster` 의 executor 목록을 표 행 문자열로 만든다. 헤더 한 줄과 executor 마다 한 줄이 나온다."""
    execs = cluster.get("executors", []) or []
    assign = cluster.get("assignment_counts", {}) or {}
    rows = [f"{'#':>2}  {'executor':<26} {'health':<7} {'CPU':>6} {'MEM':>6} {'assigned':>8}"]
    for e in execs:
        idx = e.get("index")
        idx_s = "-" if idx is None else str(idx)
        url = str(e.get("executor_url", "?"))
        url_s = url if len(url) <= 26 else "…" + url[-25:]
        health = "up" if e.get("healthy") else "down"
        cpu = fmt_pct(e.get("cpu_percent"))
        mem = fmt_pct(e.get("memory_percent"))
        assigned = assign.get(url, 0)
        rows.append(f"{idx_s:>2}  {url_s:<26} {health:<7} {cpu:>6} {mem:>6} {assigned:>8}")
    if len(rows) == 1:
        rows.append("  (executor 없음 — coordinator.executors 설정 확인)")
    return rows


def fmt_job_rows(jobs_body: dict) -> list[str]:
    """`/jobs` JSON 을 표 행 문자열로 만든다. 헤더 한 줄과 job 마다 한 줄이 나온다."""
    jobs = jobs_body.get("jobs", []) or []
    rows = [f"{'job_id':<12} {'status':<10} {'prog':>5} {'rows_w':>10}  target"]
    for j in jobs:
        rows.append(
            f"{short_id(j.get('job_id'), 12):<12} "
            f"{str(j.get('status', '?')):<10} "
            f"{fmt_pct(j.get('progress_percent')):>5} "
            f"{str(j.get('total_rows_written', 0)):>10}  "
            f"{str(j.get('target_table', ''))}"
        )
    if len(rows) == 1:
        rows.append("  (job 없음)")
    return rows


def fmt_job_detail(job: dict) -> list[str]:
    """단일 job(`/jobs/{id}`)의 상세 줄을 만든다. 헤더 필드에 이어 task 목록이 붙는다."""
    lines = [
        f"job_id: {job.get('job_id')}   status: {job.get('status')}   "
        f"progress: {fmt_pct(job.get('progress_percent'))}",
        f"target: {job.get('target_table', '')}   exec_mode: {job.get('exec_mode', '')}   "
        f"rows_written: {job.get('total_rows_written', 0)}",
    ]
    if job.get("error"):
        lines.append(f"error: {job.get('error')}")
    tasks = job.get("tasks", []) or []
    lines.append("")
    lines.append(f"tasks ({len(tasks)}):")
    lines.append(f"  {'task_id':<12} {'status':<10} {'rows_w':>10}  executor")
    for t in tasks:
        lines.append(
            f"  {short_id(t.get('task_id'), 12):<12} "
            f"{str(t.get('status', '?')):<10} "
            f"{str(t.get('rows_written', 0)):>10}  "
            f"{str(t.get('executor_url', ''))}"
        )
    return lines


def fmt_executor_detail(metrics: dict, tasks_body: dict) -> list[str]:
    """executor 상세를 만든다. 메트릭과 동시처리 요약에 이어, coordinator 프록시로 받은 보유 task 목록이 붙는다."""
    tk = metrics.get("tasks", {}) or {}
    lines = [
        f"CPU {fmt_pct(metrics.get('cpu_percent'))}  MEM {fmt_pct(metrics.get('memory_percent'))}  "
        f"DISK {fmt_pct(metrics.get('disk_percent'))}   "
        f"tasks active={tk.get('active', 0)} queued={tk.get('queued', 0)} max={tk.get('max', 0)}",
    ]
    if metrics.get("gp_hostname"):
        lines.append(f"gp_hostname: {metrics.get('gp_hostname')}")
    tasks = tasks_body.get("tasks", []) or []
    lines.append("")
    lines.append(f"tasks ({tasks_body.get('total', len(tasks))}):")
    lines.append(f"  {'task_id':<12} {'status':<10} {'rows_w':>10}  job")
    for t in tasks:
        lines.append(
            f"  {short_id(t.get('task_id'), 12):<12} "
            f"{str(t.get('status', '?')):<10} "
            f"{str(t.get('rows_written', 0)):>10}  "
            f"{short_id(t.get('job_id'), 12)}"
        )
    return lines


def fmt_history_rows(history_body: dict) -> list[str]:
    """`/history` JSON 을 표 행으로 만든다."""
    items = history_body.get("history") or history_body.get("items") or []
    rows = [f"{'job_id':<12} {'status':<10} {'user':<10} {'rows':>10}  finished"]
    for h in items:
        rows.append(
            f"{short_id(h.get('job_id'), 12):<12} "
            f"{str(h.get('status', '?')):<10} "
            f"{short_id(h.get('username'), 10):<10} "
            f"{str(h.get('total_rows_written', h.get('rows_written', 0))):>10}  "
            f"{str(h.get('finished_at', ''))}"
        )
    if len(rows) == 1:
        rows.append("  (이력 없음 — history.db_dsn 미설정이거나 기록 없음)")
    return rows


def fmt_info_lines(info: dict, config: dict | None) -> list[str]:
    """`/info`(+/config) 를 키:값 줄로. config 는 마스킹된 상태로 온다."""
    lines = [f"{k}: {v}" for k, v in info.items() if not isinstance(v, (dict, list))]
    cfg = (config or {}).get("config")
    if isinstance(cfg, list):
        lines.append("")
        lines.append("── config (마스킹) ──")
        for row in cfg:
            if isinstance(row, dict):
                sec = row.get("section", "")
                key = row.get("key", "")
                val = row.get("value", "")
                lines.append(f"  {sec}.{key} = {val}")
    return lines


# 탭을 (라벨, 내부 키) 로 정의한다. 여기 적힌 순서가 곧 화면의 탭 순서다.
TABS = [
    ("Cluster", "cluster"),
    ("Jobs", "jobs"),
    ("Executors", "executors"),
    ("History", "history"),
    ("Info", "info"),
]


def default_base_url() -> str:
    """설정에서 coordinator 접속 URL 을 유추한다(host 0.0.0.0 은 127.0.0.1 로).

    환경변수 ``COORDINATOR_URL`` 이 있으면 그것을 우선한다.
    """
    env = os.getenv("COORDINATOR_URL")
    if env:
        return env
    try:
        from core.config import settings

        host = settings.coordinator_host or "127.0.0.1"
        if host in ("0.0.0.0", "::"):
            host = "127.0.0.1"
        return f"http://{host}:{settings.coordinator_port}"
    except Exception:
        return "http://127.0.0.1:8088"


# ─────────────────────────────────────────────────────────────────────────────
# curses UI
# ─────────────────────────────────────────────────────────────────────────────


class DashboardTUI:
    """탭 기반 읽기 전용 모니터. 활성 탭/상세 뷰의 데이터만 폴링해 그린다."""

    def __init__(self, api: CoordinatorApi, interval: float = 2.0):
        self.api = api
        self.interval = interval
        self.tab = 0
        self.row = 0
        self.top = 0
        self.detail: tuple[str, Any] | None = None  # ("job", id) | ("executor", idx)
        self.data: Any = None            # 현재 뷰의 원본 JSON 이다
        self.error: str | None = None
        self.lines: list[str] = []       # 현재 뷰를 렌더한 줄들이다
        self.select_rows: list[Any] = [] # 드릴인할 수 있는 행의 키다(job_id 나 executor index)

    # ── 데이터 폴링 ──
    def refresh(self) -> None:
        """현재 뷰(탭 또는 상세)의 데이터를 다시 가져와 렌더 줄을 만든다."""
        self.error = None
        try:
            if self.detail is not None:
                self._refresh_detail()
            else:
                self._refresh_tab()
        except Exception as e:  # 네트워크·HTTP·파싱 오류는 배너로만 알리고 UI 는 그대로 둔다
            self.error = f"{type(e).__name__}: {e}"

    def _refresh_tab(self) -> None:
        key = TABS[self.tab][1]
        self.select_rows = []
        if key == "cluster":
            self.data = self.api.cluster()
            self.lines = fmt_cluster_summary(self.data) + [""] + fmt_executor_rows(self.data)
        elif key == "jobs":
            self.data = self.api.jobs()
            self.lines = fmt_job_rows(self.data)
            # 헤더인 0행을 뺀 각 job 을 드릴인 대상으로 매핑한다.
            self.select_rows = [None] + [j.get("job_id") for j in (self.data.get("jobs") or [])]
        elif key == "executors":
            self.data = self.api.cluster()
            self.lines = fmt_executor_rows(self.data)
            self.select_rows = [None] + [e.get("index") for e in (self.data.get("executors") or [])]
        elif key == "history":
            self.data = self.api.history()
            self.lines = fmt_history_rows(self.data)
        elif key == "info":
            info = self.api.info()
            try:
                cfg = self.api.config()
            except Exception:
                cfg = None
            self.lines = fmt_info_lines(info, cfg)

    def _refresh_detail(self) -> None:
        kind, key = self.detail
        if kind == "job":
            self.lines = fmt_job_detail(self.api.job(key))
        elif kind == "executor":
            metrics = self.api.executor_metrics(key)
            tasks = self.api.executor_tasks(key)
            self.lines = fmt_executor_detail(metrics, tasks)

    # ── 입력과 드릴인 ──
    def enter(self) -> None:
        """현재 선택한 행에서 상세로 들어간다. Jobs 와 Executors 탭에서만 동작한다."""
        if self.detail is not None:
            return
        key = TABS[self.tab][1]
        if key == "jobs" and 0 <= self.row < len(self.select_rows):
            jid = self.select_rows[self.row]
            if jid:
                self.detail = ("job", jid)
                self.row = self.top = 0
                self.refresh()
        elif key == "executors" and 0 <= self.row < len(self.select_rows):
            idx = self.select_rows[self.row]
            if idx is not None:
                self.detail = ("executor", idx)
                self.row = self.top = 0
                self.refresh()

    def back(self) -> bool:
        """상세에서 목록으로 돌아간다. 상세가 아니면 False 를 돌려주어 상위가 다르게 처리하게 한다."""
        if self.detail is not None:
            self.detail = None
            self.row = self.top = 0
            self.refresh()
            return True
        return False

    def set_tab(self, tab: int) -> None:
        self.tab = tab % len(TABS)
        self.detail = None
        self.row = self.top = 0
        self.refresh()

    # ── 렌더 루프 ──
    def run(self, stdscr: Any) -> None:
        import curses

        curses.curs_set(0)
        stdscr.keypad(True)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN, -1)
        curses.init_pair(2, curses.COLOR_GREEN, -1)
        curses.init_pair(3, curses.COLOR_RED, -1)
        curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_BLUE)

        stdscr.timeout(int(self.interval * 1000))  # 이 시간마다 getch 가 -1 을 돌려주어 자동으로 새로고침된다
        self.refresh()
        while True:
            self._draw(stdscr, curses)
            ch = stdscr.getch()
            if ch == -1:                      # 타임아웃이므로 자동으로 폴링한다
                self.refresh()
            elif ch in (ord("q"), 27) and self.detail is None:  # 최상위에서 q 나 ESC 를 누르면 종료한다
                if ch == 27 and self.back():
                    continue
                return
            elif ch == 27:                    # 상세 화면에서 ESC 를 누르면 뒤로 간다
                self.back()
            elif ch in (curses.KEY_LEFT,):
                if self.detail is not None:
                    self.back()
                else:
                    self.set_tab(self.tab - 1)
            elif ch in (curses.KEY_RIGHT, ord("\t")):
                if self.detail is None:
                    self.set_tab(self.tab + 1)
            elif ch == curses.KEY_UP:
                self.row = max(0, self.row - 1)
            elif ch == curses.KEY_DOWN:
                self.row = min(max(0, len(self.lines) - 1), self.row + 1)
            elif ch in (curses.KEY_ENTER, 10, 13):
                self.enter()
            elif ch == ord("r"):
                self.refresh()

    def _draw(self, stdscr: Any, curses: Any) -> None:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        # 타이틀과 탭 바를 그린다.
        title = f" Coordinator 모니터  {self.api.base_url} "
        stdscr.addstr(0, 0, title[: w - 1], curses.color_pair(1) | curses.A_BOLD)
        x = 0
        for i, (label, _) in enumerate(TABS):
            seg = f" {label} "
            if x + len(seg) >= w:
                break
            attr = curses.color_pair(4) if (i == self.tab and self.detail is None) else curses.color_pair(1)
            stdscr.addstr(1, x, seg, attr)
            x += len(seg) + 1

        # 본문을 스크롤하며 그린다. 상세 모드면 상세 줄을, 아니면 탭 줄을 쓴다.
        body = self.lines
        list_h = h - 4
        # Jobs 와 Executors 처럼 선택할 수 있는 목록에서만 커서를 하이라이트한다.
        selectable = self.detail is None and TABS[self.tab][1] in ("jobs", "executors")
        if self.row < self.top:
            self.top = self.row
        elif self.row >= self.top + list_h:
            self.top = self.row - list_h + 1

        y = 3
        for i in range(self.top, min(len(body), self.top + list_h)):
            if y >= h - 1:
                break
            line = body[i]
            attr = 0
            if selectable and i == self.row and i < len(self.select_rows) and self.select_rows[i] is not None:
                attr = curses.color_pair(4)
            stdscr.addstr(y, 0, line[: w - 1], attr)
            y += 1

        # 상태 줄을 그린다.
        if self.error:
            status = f" ⚠ {self.error} "
            stdscr.addstr(h - 1, 0, status[: w - 1], curses.color_pair(3) | curses.A_REVERSE)
        else:
            hint = ("↑↓ 이동  Enter 상세  ←/ESC 뒤로  r 새로고침  q 종료"
                    if self.detail is not None
                    else "←→ 탭  ↑↓ 이동  Enter 상세  r 새로고침  q 종료")
            stdscr.addstr(h - 1, 0, (" " + hint)[: w - 1], curses.A_REVERSE)
        stdscr.refresh()


def run_tui(base_url: str, interval: float = 2.0) -> None:
    """curses 모니터를 띄운다."""
    import curses

    api = CoordinatorApi(base_url)
    curses.wrapper(DashboardTUI(api, interval=interval).run)


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점이다. ``python -m coordinator.tui`` 나 ``bin/dashboard-tui.sh`` 로 실행한다."""
    import sys

    parser = argparse.ArgumentParser(
        prog="dashboard-tui",
        description="coordinator 대시보드를 터미널에서 보는 읽기 전용 모니터(curses).",
    )
    parser.add_argument(
        "--url", default=default_base_url(),
        help="coordinator 베이스 URL(기본: 설정/COORDINATOR_URL 에서 유추, %(default)s)",
    )
    parser.add_argument(
        "--interval", type=float, default=2.0, help="자동 새로고침 주기(초, 기본 %(default)s)",
    )
    args = parser.parse_args(argv)

    if not sys.stdout.isatty():
        print("TTY 가 아닙니다 — 대화형 터미널에서 실행하세요.", file=sys.stderr)
        return 2
    try:
        run_tui(args.url, interval=args.interval)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
