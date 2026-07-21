"""config_migrate(설정 마이그레이션) 검증.

기존 설치본 config.properties 의 사용자 변경분(새 기본값과 다른 값 + 사용자 추가 키)을
새 기본 파일 위에 얹는 동작을 본다 — 주석·순서 보존, 백업, dry-run, CLI 오류 처리.
"""

from pathlib import Path

import pytest

from core.config_migrate import build_plan, main, merge_files, migrate

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
