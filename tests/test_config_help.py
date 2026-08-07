"""설정 항목별 안내(core.config_help)와 도움말 화면 구성 테스트.

안내가 코드에 있고 설정 구조는 config.yml 에 있으므로 둘이 어긋날 수 있다. 여기서는 모든
안내가 실제 존재하는 키를 가리키는지, 조정하기 까다로운 항목이 빠지지 않았는지를 본다.
"""

from __future__ import annotations

from pathlib import Path

from core.config_help import FIELD_HELP, RELATED, help_for, related_to
from core.config_tui import CONCURRENCY_KEYS, help_lines, parse_schema
from core.textui import disp_width

_CONF = Path(__file__).resolve().parents[1] / "config"


def _schema():
    return parse_schema((_CONF / "config.yml").read_text(encoding="utf-8"))


def test_안내는_모두_실제_설정_키를_가리킨다():
    """키 이름이 바뀌면 안내가 조용히 붕 뜨므로 여기서 막는다."""
    keys = {f.prop_key for f in _schema()}
    assert not (set(FIELD_HELP) - keys)
    assert not (set(RELATED) - keys)
    # '함께 보기'가 가리키는 상대 키도 실재해야 한다.
    for targets in RELATED.values():
        assert not (set(targets) - keys)


def test_동시성_항목에는_안내가_빠짐없이_있다():
    """조정 결과를 예측하기 가장 어려운 값들이라 안내가 반드시 있어야 한다."""
    missing = [k for k in CONCURRENCY_KEYS if not help_for(k)]
    assert missing == [], f"안내 없는 동시성 항목: {missing}"


def test_안내는_뜻과_쓰는_법을_모두_담는다():
    """한 문장짜리 되풀이가 아니라 판단에 쓸 내용이어야 한다."""
    for key, text in FIELD_HELP.items():
        assert len(text) > 40, f"{key} 안내가 너무 짧다"
        assert text.strip() == text
        assert "." in text or "다" in text        # 산문체 문장인지


def test_help_for_는_없는_키에_빈_문자열을_준다():
    assert help_for("없는.키") == ""
    assert related_to("없는.키") == []


def test_help_lines_는_현재값과_뜻과_사용법을_함께_싣는다():
    by = {f.prop_key: f for f in _schema()}
    lines = help_lines(by["executor.max_concurrent_tasks"], "12", 60)
    text = "\n".join(lines)
    assert lines[0] == "executor.max_concurrent_tasks"
    assert "현재 값: 12" in text
    assert "기본값: 8" in text
    assert "허용 범위: 0 ~ 1024" in text
    assert "■ 무엇인가" in text                    # config.yml 주석
    assert "■ 어떻게 쓰는가" in text               # config_help 안내
    assert "■ 함께 보기" in text
    assert "greenplum.pool_max" in text
    # 모든 줄이 요청한 폭 안에 들어와야 curses 에서 넘치지 않는다.
    assert all(disp_width(line) <= 60 for line in lines)


def test_help_lines_는_비밀값을_가린다():
    by = {f.prop_key: f for f in _schema()}
    text = "\n".join(help_lines(by["impala.password"], "hunter2", 60))
    assert "hunter2" not in text
    assert "***" in text


def test_help_lines_는_enum_후보를_보여준다():
    by = {f.prop_key: f for f in _schema()}
    text = "\n".join(help_lines(by["store.backend"], "memory", 60))
    assert "memory | file | postgres" in text


def test_안내가_없는_항목도_화면을_만든다():
    by = {f.prop_key: f for f in _schema()}
    # 안내를 따로 쓰지 않은 항목이라도 기본 정보는 나와야 한다.
    plain = next(f for f in by.values() if not help_for(f.prop_key))
    lines = help_lines(plain, "", 60)
    assert lines[0] == plain.prop_key
    assert any("기본값:" in line for line in lines)
