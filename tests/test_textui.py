"""터미널 문자 폭 계산(core.textui) 테스트.

한글은 한 글자가 두 칸을 차지하므로 ``line[: w - 1]`` 처럼 글자 수로 자르면 화면 폭의 두 배까지
밀려나고, 그렇게 넘친 문자열은 curses 맨 아랫줄에서 ``addwstr() returned ERR`` 로 TUI 를 죽인다.
여기서는 자르기·채우기·줄바꿈이 모두 **칸 수** 기준으로 도는지 확인한다.
"""

from __future__ import annotations

from core.textui import char_width, cut, disp_width, pad, wrap


def test_한글은_두_칸_영문은_한_칸이다():
    assert char_width("가") == 2
    assert char_width("a") == 1
    assert disp_width("가나다") == 6
    assert disp_width("abc") == 3
    assert disp_width("한글abc") == 7
    assert disp_width("") == 0


def test_화살표_같은_모호폭_문자는_한_칸으로_본다():
    """터미널마다 갈리는 폭이라 좁게 잡는다. 넘치는 쪽보다 덜 쓰는 쪽이 안전하다."""
    assert disp_width("←→↑↓") == 4


def test_cut_은_칸_수_기준으로_자른다():
    assert cut("가나다라", 4) == "가나"          # 4칸 = 두 글자
    assert cut("가나다라", 5) == "가나"          # 두 칸짜리가 걸치면 통째로 뺀다
    assert cut("abcdef", 4) == "abcd"
    assert cut("한글abc", 6) == "한글ab"
    assert cut("아무거나", 0) == ""
    assert disp_width(cut("한글이 섞인 긴 문장" * 20, 79)) <= 79


def test_pad_는_칸_수를_채운다():
    assert disp_width(pad("가나", 10)) == 10
    assert disp_width(pad("abc", 10)) == 10
    # 넘치면 자르되 결과 폭이 요청 폭을 넘지 않는다.
    assert disp_width(pad("가나다라마바사", 6)) == 6


def test_pad_로_맞춘_열은_한글이_섞여도_어긋나지_않는다():
    """f-문자열의 :<12 는 글자 수로 세어 한글 열을 밀어 놓는다. pad 는 칸 수로 센다."""
    keys = ("짧은키", "aaa", "조금더긴키", "a.b.c.d")
    # 값이 시작하는 화면 칸이 모든 행에서 같아야 한다.
    assert len({disp_width(pad(k, 12)) for k in keys}) == 1
    # 대조군: 글자 수로 맞추면 어긋난다.
    assert len({disp_width(f"{k:<12}") for k in keys}) > 1


def test_wrap_은_낱말을_끊지_않고_폭_안에_넣는다():
    text = "동시에 RUNNING 일 수 있는 job 수다. 슬롯이 다 차면 다음 job 은 PENDING 으로 줄을 선다."
    lines = wrap(text, 30)
    assert all(disp_width(line) <= 30 for line in lines)
    # 낱말이 쪼개지지 않았으므로 공백으로 다시 이으면 원문이 된다.
    assert " ".join(lines) == text


def test_wrap_은_폭보다_긴_낱말만_어쩔_수_없이_쪼갠다():
    lines = wrap("postgresql://user:password@some-very-long-host.example.com:5432/db", 20)
    assert all(disp_width(line) <= 20 for line in lines)
    assert "".join(lines).startswith("postgresql://")


def test_wrap_은_빈_문자열과_좁은_폭을_견딘다():
    assert wrap("", 40) == []
    assert wrap("가", 1) == ["가"]      # 한 칸에는 못 넣지만 삼키지는 않는다
