"""config_migrate(설정 마이그레이션) 검증.

기존 설치본 config.properties 의 사용자 변경분(새 기본값과 다른 값 + 사용자 추가 키)을
새 기본 파일 위에 얹는 동작을 본다 — 주석·순서 보존, 백업, dry-run, CLI 오류 처리.
멀티 타깃 모드(config/templates/customs 트리 반영: 신규·갱신·병합·운영자 파일 보존)도 본다.
"""

from pathlib import Path

import pytest

from core.config_migrate import (
    build_plan,
    main,
    merge_files,
    migrate,
    migrate_all,
    migrate_tree,
)

# 새 버전 기본 파일: 주석/빈 줄 포함, 기본값이 바뀐 키(log.level)와 새 키(template.enabled) 포함.
NEW_TEMPLATE = """\
# ───────── 로깅 ─────────
log.level=INFO
log.dir=logs

# ───────── 템플릿 ─────────
template.enabled=true
"""

# 기존 설치본: 운영자가 log.level 을 바꿨고(query.func.* 는 직접 추가), log.dir 은 기본값 그대로.
OLD_INSTALLED = """\
log.level=DEBUG
log.dir=logs
query.func.module=customs.query_funcs.trino_runner:run
query.func.config.password=secret
"""


@pytest.fixture
def paths(tmp_path: Path) -> tuple[Path, Path]:
    old = tmp_path / "old" / "config.properties"
    new = tmp_path / "new" / "config.properties"
    old.parent.mkdir()
    new.parent.mkdir()
    old.write_text(OLD_INSTALLED, encoding="utf-8")
    new.write_text(NEW_TEMPLATE, encoding="utf-8")
    return old, new


def test_build_plan_은_변경_추가_동일_새키를_분류한다():
    plan = build_plan(
        old_props={"a": "1", "b": "2", "c": "3"},
        new_props={"a": "1", "b": "99", "d": "4"},
    )
    assert plan.same == ["a"]              # 값 동일 → 적용 불필요
    assert plan.changed == {"b": "2"}      # 새 기본값과 다름 → 보존
    assert plan.added == {"c": "3"}        # 새 파일에 없음 → 사용자 추가
    assert plan.new_keys == ["d"]          # 새 버전에서 생긴 키
    assert plan.to_apply == {"b": "2", "c": "3"}


def test_병합은_새_파일_베이스에_변경분만_얹는다(paths):
    old, new = paths
    plan, merged = merge_files(old, new)
    text = "\n".join(merged)

    assert plan.changed == {"log.level": "DEBUG"}
    assert "log.level=DEBUG" in text                      # 변경 값이 제자리 적용
    assert "log.dir=logs" in text                         # 동일 값은 새 파일 원문 유지
    assert "template.enabled=true" in text                # 새 키는 기본값으로 들어옴
    assert "# ───────── 로깅 ─────────" in text          # 새 파일 주석 보존
    # 사용자 추가 키는 끝쪽(마커 아래)에 붙는다.
    assert text.index("query.func.module=") > text.index("template.enabled=")
    assert "query.func.config.password=secret" in text


def test_migrate_는_out_에_기록하고_기존_파일을_백업한다(paths, capsys):
    old, new = paths
    migrate(old, new, out_path=old)

    backup = old.with_suffix(".properties.bak")
    assert backup.read_text(encoding="utf-8") == OLD_INSTALLED       # 원본 백업
    merged = old.read_text(encoding="utf-8")
    assert "log.level=DEBUG" in merged and "template.enabled=true" in merged
    # 보고에서 비밀값은 마스킹된다(파일에는 원본 기록).
    out = capsys.readouterr().out
    assert "*****" in out and "secret" not in out
    assert "query.func.config.password=secret" in merged


def test_dry_run_은_아무_파일도_쓰지_않는다(paths, capsys):
    old, new = paths
    migrate(old, new, out_path=old, dry_run=True)
    assert old.read_text(encoding="utf-8") == OLD_INSTALLED          # 원본 그대로
    assert not old.with_suffix(".properties.bak").exists()
    assert "dry-run" in capsys.readouterr().out


def test_main_은_파일_누락과_동일_경로를_오류로_거른다(paths, tmp_path, capsys):
    old, new = paths
    assert main(["--old", str(tmp_path / "없음.properties"), "--new", str(new)]) == 1
    assert main(["--old", str(old), "--new", str(old)]) == 1
    assert main(["--old", str(old), "--new", str(new), "--dry-run"]) == 0


def test_main_out_지정_시_별도_경로에_기록한다(paths, tmp_path):
    old, new = paths
    out = tmp_path / "merged.properties"
    assert main(["--old", str(old), "--new", str(new), "--out", str(out)]) == 0
    assert "log.level=DEBUG" in out.read_text(encoding="utf-8")
    assert old.read_text(encoding="utf-8") == OLD_INSTALLED          # 원본 무변경


# ── 멀티 타깃(디렉터리 트리) 마이그레이션 ───────────────────────────────────

def _mk(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def trees(tmp_path: Path) -> tuple[Path, Path]:
    """소스(새 버전)·설치(운영) 두 트리를 config/templates/customs 로 구성한다."""
    src, dep = tmp_path / "src", tmp_path / "deploy"
    # config: properties(운영자 변경) + yml(새 버전 구조 변경) + schema(동일)
    _mk(src / "config/config.properties", "log.level=INFO\nnew.key=1\n")
    _mk(dep / "config/config.properties", "log.level=DEBUG\nquery.func.config.pw=secret\n")
    _mk(src / "config/config.yml", "app:\n  new_section: ${new.key:0}\n")   # 새 버전 구조
    _mk(dep / "config/config.yml", "app:\n  old_only: 1\n")                 # 구버전
    _mk(src / "config/postgresql.sql", "CREATE TABLE t();\n")
    _mk(dep / "config/postgresql.sql", "CREATE TABLE t();\n")               # 동일
    # 운영자만 가진 인증서(보존돼야 함)
    _mk(dep / "config/impala-ca.pem", "CERT\n")
    # templates: 예제 1개 갱신 + 운영자 추가 템플릿 보존
    _mk(src / "templates/sales/select.sql.j2", "SELECT 1\n")
    _mk(dep / "templates/sales/select.sql.j2", "SELECT 0\n")               # 바뀜 → 갱신
    _mk(dep / "templates/mysite/select.sql.j2", "SELECT 99\n")             # 운영자 → 보존
    # customs(query function): 예제 신규 + 운영자 모듈 보존
    _mk(src / "customs/query_funcs/trino_runner.py", "def run(): pass\n")
    _mk(dep / "customs/query_funcs/my_runner.py", "def run(): 1\n")        # 운영자 → 보존
    return src, dep


def test_migrate_tree_config_병합_교체_보존(trees):
    src, dep = trees
    r = migrate_tree(src / "config", dep / "config",
                     merge_props=frozenset({"config.properties"}))
    # config.properties 는 병합(운영자 log.level=DEBUG 보존 + 새 키 반영)
    assert "config.properties" in r.merged
    merged = (dep / "config/config.properties").read_text(encoding="utf-8")
    assert "log.level=DEBUG" in merged and "new.key=1" in merged
    assert "query.func.config.pw=secret" in merged           # 운영자 추가 키 보존
    # config.yml 은 새 버전으로 교체(+백업)
    assert "config.yml" in r.updated
    assert "new_section" in (dep / "config/config.yml").read_text(encoding="utf-8")
    assert (dep / "config/config.yml.bak").read_text(encoding="utf-8") == "app:\n  old_only: 1\n"
    # 동일 schema 는 그대로, 운영자 인증서는 보존 대상으로 보고
    assert "postgresql.sql" in r.unchanged
    assert "impala-ca.pem" in r.operator_only


def test_migrate_tree_templates_and_customs_preserve_operator(trees):
    src, dep = trees
    rt = migrate_tree(src / "templates", dep / "templates")
    assert "sales/select.sql.j2" in rt.updated                # 예제 갱신
    assert "SELECT 1" in (dep / "templates/sales/select.sql.j2").read_text()
    assert "mysite/select.sql.j2" in rt.operator_only         # 운영자 템플릿 보존
    assert (dep / "templates/mysite/select.sql.j2").exists()  # 삭제 안 됨

    rc = migrate_tree(src / "customs", dep / "customs")
    assert "query_funcs/trino_runner.py" in rc.new            # 예제 신규 복사
    assert (dep / "customs/query_funcs/my_runner.py").exists()  # 운영자 모듈 보존


def test_migrate_tree_dry_run_writes_nothing(trees):
    src, dep = trees
    before = (dep / "config/config.yml").read_text(encoding="utf-8")
    r = migrate_tree(src / "config", dep / "config",
                     merge_props=frozenset({"config.properties"}), dry_run=True)
    assert r.change_count > 0                                 # 반영 예정은 집계됨
    assert (dep / "config/config.yml").read_text(encoding="utf-8") == before  # 그러나 안 씀
    assert not (dep / "config/config.yml.bak").exists()


def test_main_multi_target_mode(trees, capsys):
    src, dep = trees
    rc = main(["--deploy-base", str(dep), "--source-base", str(src)])
    assert rc == 0
    # 세 트리 모두 반영됐는지 결과 파일로 확인
    assert "log.level=DEBUG" in (dep / "config/config.properties").read_text()
    assert "new_section" in (dep / "config/config.yml").read_text()
    assert "SELECT 1" in (dep / "templates/sales/select.sql.j2").read_text()
    assert (dep / "customs/query_funcs/trino_runner.py").exists()
    assert (dep / "templates/mysite/select.sql.j2").exists()  # 운영자 보존
    out = capsys.readouterr().out
    assert "config" in out and "templates" in out and "customs" in out


def test_migrate_all_returns_three_trees(trees):
    src, dep = trees
    results = migrate_all(dep_base := dep, src, dry_run=True)  # noqa: F841
    labels = [rep.label for _, _, rep in results]
    assert labels == ["config", "templates", "customs"]
