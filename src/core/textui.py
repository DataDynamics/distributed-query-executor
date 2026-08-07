"""터미널 UI 가 공유하는 문자 폭 계산과 자르기·줄바꿈 도구다.

## 왜 필요한가

파이썬 문자열의 길이와 터미널이 실제로 쓰는 칸 수가 다르다. 한글·한자·가나 같은 글자는
한 글자가 **두 칸**을 차지하므로, ``line[: w - 1]`` 처럼 글자 수로 자르면 화면 폭의 두 배까지
밀려난다. 그렇게 넘친 문자열을 curses 에 넘기면 중간 줄에서는 조용히 잘리지만 **맨 아랫줄에서는
``addwstr() returned ERR`` 로 예외가 나고 TUI 가 통째로 죽는다**. 이 저장소의 상태 줄과 설명 줄은
대부분 한글이라 폭 80칸 터미널에서 바로 걸리는 문제다.

그래서 두 TUI(:mod:`core.config_tui`, :mod:`coordinator.tui`)는 화면에 무언가를 쓰기 전에 항상
:func:`cut` 을 거친다. curses 와 무관한 순수 함수라 테스트로 직접 확인한다.
"""

from __future__ import annotations

import unicodedata

__all__ = ["char_width", "disp_width", "cut", "pad", "wrap"]


def char_width(ch: str) -> int:
    """글자 하나가 터미널에서 차지하는 칸 수를 돌려준다(0, 1, 2 중 하나).

    결합 문자(악센트 등)는 앞 글자에 얹히므로 0 이고, 동아시아 문자 중 ``W``(Wide)와
    ``F``(Fullwidth)는 2 다. ``A``(Ambiguous)로 분류되는 화살표나 기호는 터미널 설정에 따라
    1 이 되기도 2 가 되기도 하는데, 여기서는 1 로 본다 — 2 로 세면 대부분의 환경에서 멀쩡한
    문장이 지레 잘리기 때문이다(넘치는 쪽보다 조금 덜 쓰는 쪽이 안전하다).
    """
    if unicodedata.combining(ch):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def disp_width(text: str) -> int:
    """문자열이 터미널에서 차지하는 총 칸 수다."""
    return sum(char_width(c) for c in text)


def cut(text: str, cols: int) -> str:
    """``cols`` 칸 안에 들어가도록 뒤를 잘라 낸다.

    두 칸짜리 글자가 경계에 걸치면 그 글자는 통째로 뺀다(반 칸만 그릴 수는 없다).
    """
    if cols <= 0:
        return ""
    out, used = [], 0
    for ch in text:
        cw = char_width(ch)
        if used + cw > cols:
            break
        out.append(ch)
        used += cw
    return "".join(out)


def pad(text: str, cols: int) -> str:
    """``cols`` 칸을 채우도록 오른쪽에 공백을 붙인다(넘치면 자른다).

    한글이 섞인 열을 ``f"{s:<20}"`` 으로 맞추면 글자 수로 세어 열이 어긋나므로 이 함수를 쓴다.
    """
    text = cut(text, cols)
    return text + " " * (cols - disp_width(text))


def wrap(text: str, cols: int) -> list[str]:
    """문장을 ``cols`` 칸에 맞춰 여러 줄로 나눈다. 낱말 중간에서 끊지 않는다.

    낱말 하나가 폭보다 긴 경우(긴 경로나 DSN)에만 어쩔 수 없이 중간에서 자른다.
    """
    if cols <= 1:
        return [text] if text else []
    lines: list[str] = []
    line = ""
    for word in text.split(" "):
        candidate = word if not line else f"{line} {word}"
        if disp_width(candidate) <= cols:
            line = candidate
            continue
        if line:
            lines.append(line)
        # 낱말 자체가 한 줄보다 길면 폭 단위로 쪼개 넣는다.
        while disp_width(word) > cols:
            head = cut(word, cols)
            lines.append(head)
            word = word[len(head):]
        line = word
    if line:
        lines.append(line)
    return lines
