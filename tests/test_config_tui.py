"""config_tui 의 순수 로직(스키마 파싱·diff-write·검증·마스킹) 테스트.

curses UI 는 대화형이라 여기서 다루지 않고, 스키마 추출과 파일 병합처럼 회귀
위험이 큰 부분만 검증한다. 실제 저장소 conf/config.yml 을 그대로 파싱해 드리프트도 잡는다.
"""

from __future__ import annotations

from pathlib import Path

from core.config_tui import (
    Field,
    display_value,
    infer_type,
    merge_properties_lines,
    parse_schema,
    validate,
    write_config,
)

_CONF = Path(__file__).resolve().parents[1] / "conf"


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
