"""config_tui 의 순수 로직(스키마 파싱·diff-write·검증·마스킹·동시성 조정) 테스트.

스키마 추출과 파일 병합처럼 회귀 위험이 큰 부분을 검증하며, 실제 저장소 config/config.yml 을
그대로 파싱해 드리프트도 잡는다. 키 입력을 흉내 내는 대화형 시나리오는 다루지 않지만,
화면 그리기만은 가짜 curses(:class:`_FakeScreen`)로 한 번 훑는다 — 항목이 늘거나 이름이
길어졌을 때 열이 밀리거나 탭이 화면 밖으로 사라지는 사고가 실제로 나기 때문이다.
"""

from __future__ import annotations

from pathlib import Path

from core.config_tui import (
    CONCURRENCY_KEYS,
    CONCURRENCY_SECTION,
    INT_BOUNDS,
    SECTION_LABELS,
    ConfigTUI,
    Field,
    check_concurrency,
    concurrency_summary,
    display_value,
    infer_type,
    merge_properties_lines,
    parse_schema,
    step_value,
    validate,
    write_config,
)

_CONF = Path(__file__).resolve().parents[1] / "config"


# ── 스키마 파싱 ────────────────────────────────────────────────────────────

def _schema():
    return parse_schema((_CONF / "config.yml").read_text(encoding="utf-8"))


def test_parses_real_config_yml():
    fields = _schema()
    keys = {f.prop_key for f in fields}
    # 대표 키들이 섹션별로 잡혀야 한다.
    assert "coordinator.port" in keys
    assert "executor.max_concurrent_tasks" in keys
    assert "impala.host" in keys
    assert "stage.csv_delimiter" in keys
    # 상수 값(자리표시자 아님)은 항목이 아니다: logging.rolling.type=daily.
    assert "daily" not in keys


def test_default_and_help_extracted():
    by = {f.prop_key: f for f in _schema()}
    port = by["coordinator.port"]
    assert port.default == "8088"
    assert port.ftype == "int"
    assert port.section == "coordinator"
    # 줄 끝 주석이 짧은 설명으로 잡힌다.
    assert "포트" in port.help_inline or "수신" in port.help_inline


def test_enum_and_bool_typing():
    by = {f.prop_key: f for f in _schema()}
    assert by["store.backend"].ftype == "enum"
    assert by["store.backend"].enum == ["memory", "file", "postgres"]
    assert by["app.debug"].ftype == "bool"
    assert by["coordinator.executor_mode"].enum == ["remote", "local"]


def test_secret_flagged():
    by = {f.prop_key: f for f in _schema()}
    assert by["impala.password"].secret is True
    assert by["history.db_dsn"].secret is True
    assert by["coordinator.port"].secret is False


def test_nested_section_grouping():
    by = {f.prop_key: f for f in _schema()}
    # executor.impala.host → 최상위 섹션 executor, 하위 그룹 impala.
    f = by["impala.host"]
    assert f.section == "executor"
    assert f.group == "impala"


def test_infer_type():
    assert infer_type("true") == "bool"
    assert infer_type("false") == "bool"
    assert infer_type("16") == "int"
    assert infer_type("1.0") == "float"
    assert infer_type("hive") == "str"
    assert infer_type("") == "str"


# ── diff-write(주석·순서 보존) ─────────────────────────────────────────────

def test_merge_updates_in_place():
    existing = [
        "# 주석",
        "coordinator.port=8088",
        "app.debug=false",
    ]
    out = merge_properties_lines(existing, {"coordinator.port": "9090"})
    assert out == ["# 주석", "coordinator.port=9090", "app.debug=false"]


def test_merge_appends_new_key_with_marker():
    existing = ["coordinator.port=8088"]
    out = merge_properties_lines(existing, {"impala.host": "imp.example"})
    assert "coordinator.port=8088" in out
    assert "impala.host=imp.example" in out
    # 새 키는 마커 아래에 붙는다.
    assert any("config-tui" in line for line in out)


def test_merge_preserves_untouched_lines_and_colon_sep():
    existing = ["# c", "history.db_dsn: postgresql://u:p@h/db", "app.debug=false"]
    out = merge_properties_lines(existing, {"app.debug": "true"})
    # colon 구분 줄은 그대로, 대상만 교체.
    assert "history.db_dsn: postgresql://u:p@h/db" in out
    assert "app.debug=true" in out


def test_merge_marker_added_once():
    existing = ["coordinator.port=8088", "# ─── config-tui 로 추가된 설정 ───", "x.y=1"]
    out = merge_properties_lines(existing, {"a.b": "2"})
    assert sum(1 for line in out if "config-tui" in line) == 1


# ── 검증 ──────────────────────────────────────────────────────────────────

def _fields(*specs):
    out = []
    for key, default, ftype, enum in specs:
        out.append(Field(prop_key=key, section=key.split(".")[0], path=key.split("."),
                         default=default, has_default=True, ftype=ftype, enum=list(enum)))
    return out


def test_validate_int_and_port():
    fields = _fields(("coordinator.port", "8088", "int", []))
    assert any(sev == "error" for sev, *_ in validate(fields, {"coordinator.port": "abc"}))
    assert any("포트" in msg for sev, _, msg in validate(fields, {"coordinator.port": "99999"}))
    assert validate(fields, {"coordinator.port": "9090"}) == []


def test_validate_enum():
    fields = _fields(("store.backend", "memory", "enum", ["memory", "file", "postgres"]))
    assert any(sev == "error" for sev, *_ in validate(fields, {"store.backend": "mysql"}))
    # file 은 유효 enum 이고 postgres 처럼 DSN 조건도 없어 이슈가 없어야 한다.
    assert validate(fields, {"store.backend": "file"}) == []


def test_validate_postgres_requires_dsn_warn():
    fields = _fields(
        ("store.backend", "memory", "enum", ["memory", "file", "postgres"]),
        ("history.db_dsn", "", "str", []),
    )
    issues = validate(fields, {"store.backend": "postgres"})
    assert any(sev == "warn" and key == "history.db_dsn" for sev, key, _ in issues)


def test_validate_executor_url_warn():
    fields = _fields(("coordinator.executors", "", "str", []))
    issues = validate(fields, {"coordinator.executors": "http://a:1,bad-url"})
    assert any(key == "coordinator.executors" for _, key, _ in issues)


# ── 저장(백업·주석 보존) ───────────────────────────────────────────────────

def test_write_config_backs_up_and_updates(tmp_path):
    props = tmp_path / "config.properties"
    props.write_text("# 헤더\ncoordinator.port=8088\napp.debug=false\n", encoding="utf-8")
    path = write_config(tmp_path, {"coordinator.port": "9090", "impala.host": "imp"})
    assert path == props
    # 백업이 원본을 보존한다.
    bak = tmp_path / "config.properties.bak"
    assert "coordinator.port=8088" in bak.read_text(encoding="utf-8")
    # 새 파일은 제자리 갱신 + 새 키 추가 + 주석 보존.
    text = props.read_text(encoding="utf-8")
    assert "# 헤더" in text
    assert "coordinator.port=9090" in text
    assert "impala.host=imp" in text


def test_write_config_no_existing_file(tmp_path):
    path = write_config(tmp_path, {"coordinator.port": "9090"})
    assert path.read_text(encoding="utf-8").strip().endswith("coordinator.port=9090")
    assert not (tmp_path / "config.properties.bak").exists()


# ── 마스킹 표시 ────────────────────────────────────────────────────────────

def test_display_masks_secrets():
    dsn = Field(prop_key="history.db_dsn", section="history", path=["history", "db_dsn"],
                default="", has_default=True, secret=True)
    pw = Field(prop_key="impala.password", section="executor", path=["executor", "impala", "password"],
               default="", has_default=True, secret=True)
    assert display_value(dsn, "postgresql://u:secret@h/db") == "postgresql://u:***@h/db"
    assert display_value(pw, "hunter2") == "***"
    assert display_value(pw, "") == "(미설정)"


# ── 동시성 조정 ────────────────────────────────────────────────────────────

def test_동시성_키가_실제_스키마에_모두_있다():
    """동시성 탭과 범위 표가 config.yml 의 실제 키를 가리키는지 본다.

    설정 키 이름이 바뀌면 탭에서 조용히 사라지므로(에러 없이 빈 줄만 준다) 여기서 막는다.
    """
    keys = {f.prop_key for f in _schema()}
    assert not (set(CONCURRENCY_KEYS) - keys)
    assert not (set(INT_BOUNDS) - keys)


def test_step_value_는_스텝만큼_움직이고_범위에서_멈춘다():
    by = {f.prop_key: f for f in _schema()}
    tasks = by["executor.max_concurrent_tasks"]
    assert step_value(tasks, "8", 1) == "9"
    assert step_value(tasks, "8", -1) == "7"
    assert step_value(tasks, "0", -1) == "0"          # 하한(0)에서 더 내려가지 않는다
    assert step_value(tasks, "1024", 1) == "1024"     # 상한에서 멈춘다


def test_step_value_는_큰_스텝을_눈금에_맞춘다():
    """배치 크기처럼 스텝이 큰 항목은 어중간한 값에서 눈금으로 정렬한다."""
    batch = {f.prop_key: f for f in _schema()}["copy.batch_size"]
    assert step_value(batch, "10000", 1) == "11000"
    assert step_value(batch, "10500", 1) == "11000"
    assert step_value(batch, "10500", -1) == "10000"


def test_step_value_는_숫자가_아니면_손대지_않는다():
    by = {f.prop_key: f for f in _schema()}
    assert step_value(by["coordinator.executors"], "http://a:8087", 1) == "http://a:8087"
    assert step_value(by["dashboard.enabled"], "true", 1) == "true"
    # float 은 0.5 씩 움직이고 정수로 떨어지면 소수점을 남기지 않는다.
    assert step_value(by["coordinator.poll_interval_s"], "1.0", 1) == "1.5"
    assert step_value(by["coordinator.poll_interval_s"], "1.5", 1) == "2"


def test_동시성_요약이_유효_용량을_곱해_보여준다():
    lines = concurrency_summary({
        "coordinator.executors": "http://a:8087,http://b:8087",
        "coordinator.max_concurrent_jobs": "16",
        "coordinator.max_pending_jobs": "100",
        "executor.max_concurrent_tasks": "8",
        "greenplum.pool_max": "0",
        "copy.batch_size": "10000",
        "copy.queue_size": "8",
    })
    text = " ".join(lines)
    assert "116건까지 수용" in text            # 16 실행 + 100 대기
    assert "동시 16개" in text                 # executor 2대 × task 8개
    assert "GP 연결 최대 16개" in text         # pool_max=0 이면 task 수를 따라간다
    assert "80,000행" in text                  # queue 8 × batch 10000


def test_동시성_요약은_무제한과_미설정을_구분해_적는다():
    unlimited = concurrency_summary({"coordinator.max_concurrent_jobs": "0"})
    assert "무제한" in unlimited[0]
    no_exec = concurrency_summary({"coordinator.executors": ""})
    assert "계산할 수 없다" in " ".join(no_exec)


def test_check_concurrency_는_어긋난_조합을_경고한다():
    keys = [k for _, k, _ in check_concurrency({
        "coordinator.executors": "http://a:8087,http://b:8087",
        "coordinator.max_concurrent_jobs": "16",
        "coordinator.max_pending_jobs": "0",       # 대기 큐 없음
        "coordinator.max_dispatch_concurrency": "4",  # 플릿 용량 16보다 작다
        "executor.max_concurrent_tasks": "8",
        "greenplum.pool_max": "4",                 # 동시 task 8 보다 작다
    })]
    assert set(keys) == {
        "greenplum.pool_max",
        "coordinator.max_pending_jobs",
        "coordinator.max_dispatch_concurrency",
    }


def test_check_concurrency_는_맞는_조합에_침묵한다():
    assert check_concurrency({
        "coordinator.executors": "http://a:8087,http://b:8087",
        "coordinator.max_concurrent_jobs": "16",
        "coordinator.max_pending_jobs": "100",
        "coordinator.max_dispatch_concurrency": "32",
        "executor.max_concurrent_tasks": "8",
        "greenplum.pool_max": "0",
    }) == []


def test_validate_는_범위를_벗어난_숫자를_막는다():
    """max_dispatch_concurrency=0 은 세마포어가 0 이 되어 디스패치가 영원히 멈춘다."""
    fields = _schema()
    errs = [i for i in validate(fields, {"coordinator.max_dispatch_concurrency": "0"})
            if i[0] == "error"]
    assert errs and errs[0][1] == "coordinator.max_dispatch_concurrency"
    # 정상 범위는 통과한다.
    assert not [i for i in validate(fields, {"coordinator.max_dispatch_concurrency": "32"})
                if i[0] == "error"]


def test_동시성_탭이_맨_앞에_모든_손잡이를_모은다():
    fields = _schema()
    app = ConfigTUI(_CONF, fields, {})
    assert app.sections[0] == CONCURRENCY_SECTION
    assert [f.prop_key for f in app._visible()] == CONCURRENCY_KEYS


# ── 화면 그리기(가짜 curses) ───────────────────────────────────────────────

class _FakeCurses:
    """ConfigTUI._draw 가 쓰는 상수와 함수만 흉내 낸 curses 대역이다."""

    KEY_LEFT, KEY_RIGHT, KEY_UP, KEY_DOWN = 260, 261, 259, 258
    KEY_PPAGE, KEY_NPAGE, KEY_HOME, KEY_END, KEY_ENTER = 339, 338, 262, 360, 343
    A_BOLD = A_REVERSE = 1
    COLOR_CYAN = COLOR_YELLOW = COLOR_GREEN = COLOR_RED = COLOR_WHITE = COLOR_BLUE = 0

    def color_pair(self, n):
        return 0


class _FakeScreen:
    """폭·높이를 넘겨 쓰면 그 자리에서 실패하는 화면이다.

    진짜 curses 는 화면 밖에 쓰면 curses.error 를 던져 TUI 가 통째로 죽으므로,
    여기서도 관대하게 잘라 주지 않고 단언으로 잡는다.
    """

    def __init__(self, h=24, w=100):
        self.h, self.w, self.lines = h, w, {}

    def getmaxyx(self):
        return (self.h, self.w)

    def erase(self):
        self.lines.clear()

    def refresh(self):
        pass

    def addstr(self, y, x, s, attr=0):
        assert 0 <= y < self.h, f"y={y} 가 화면(h={self.h}) 밖"
        assert 0 <= x < self.w, f"x={x} 가 화면(w={self.w}) 밖"
        assert x + len(s) <= self.w, f"y={y} 에서 오른쪽 넘침(x={x}, len={len(s)}, w={self.w})"
        self.lines[y] = self.lines.get(y, "") + s


def test_모든_탭의_모든_행을_그려도_화면을_넘지_않는다():
    app = ConfigTUI(_CONF, _schema(), {})
    for size in ((24, 100), (10, 40), (8, 30)):
        screen = _FakeScreen(*size)
        for tab in range(len(app.sections)):
            app.tab, app.top = tab, 0
            for row in range(len(app._visible())):
                app.row = row
                app._draw(screen, _FakeCurses())


def test_선택한_탭은_좁은_화면에서도_탭_바에_보인다():
    """탭이 늘어 폭을 넘기면 앞을 잘라 내되 선택한 탭은 남겨야 한다."""
    app = ConfigTUI(_CONF, _schema(), {})
    last = len(app.sections) - 1
    for width in (100, 70, 50):
        for tab in (0, last):
            app.tab, app.row, app.top = tab, 0, 0
            screen = _FakeScreen(24, width)
            app._draw(screen, _FakeCurses())
            label = SECTION_LABELS.get(app.sections[tab], app.sections[tab])
            assert label in screen.lines[1], f"w={width} tab={tab}: '{label}' 이 탭 바에 없다"


def test_동시성_탭은_값_열이_밀리지_않고_요약을_함께_그린다():
    app = ConfigTUI(_CONF, _schema(), {})
    app.tab = app.sections.index(CONCURRENCY_SECTION)
    screen = _FakeScreen(24, 100)
    app._draw(screen, _FakeCurses())
    # 가장 긴 키에서도 값이 같은 열에서 시작한다.
    rows = [screen.lines[y] for y in range(3, 3 + len(CONCURRENCY_KEYS))]
    starts = {len(r) - len(r.split()[-1]) for r in rows if r.split()[-1] not in ("(미설정)",)}
    assert len(starts) == 1, f"값 열이 어긋난다: {rows}"
    # 유도값 요약이 함께 보인다.
    assert any("입구:" in line for line in screen.lines.values())
    assert any("copy 버퍼:" in line for line in screen.lines.values())
