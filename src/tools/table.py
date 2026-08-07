"""조회 결과를 표나 CSV로 내보낸다.

두 쿼리 도구와 셸이 같은 모양으로 보여주도록 여기 모아둔다. 표는 한글이 섞여도
열이 맞도록 문자 개수가 아니라 표시 폭으로 채운다.
"""

import contextlib
import csv
import gzip
import sys
from typing import Any, Iterator, List, Optional, Sequence, TextIO, Tuple

from .progress import display_width, pad

#: 표로 보여줄 때 기본으로 출력할 최대 행 수. 0이면 제한 없음.
DEFAULT_MAX_ROWS = 100


def format_value(value: Any, null_string: str) -> str:
    if value is None:
        return null_string
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def render(
    columns: Sequence[str], rows: Sequence[Sequence[Any]], null_string: str = "NULL"
) -> str:
    """열을 맞춰 표 문자열을 만든다."""
    cells = [[format_value(v, null_string) for v in row] for row in rows]
    widths = [display_width(c) for c in columns]
    for row in cells:
        for i, text in enumerate(row):
            widths[i] = max(widths[i], display_width(text))

    lines = [" | ".join(pad(c, w) for c, w in zip(columns, widths)).rstrip()]
    lines.append("-+-".join("-" * w for w in widths))
    for row in cells:
        lines.append(" | ".join(pad(t, w) for t, w in zip(row, widths)).rstrip())
    return "\n".join(lines)


def fetch_limited(
    cursor: Any,
    max_rows: int,
    batch_size: int = 10_000,
    on_batch: Any = None,
) -> Tuple[List[Sequence[Any]], bool]:
    """표로 보여줄 만큼만 받는다. (행, 더 있는지) 를 돌려준다.

    100행을 보여주려고 수백만 행을 다 받을 이유가 없다. 한 행 더 받아보고 남아
    있으면 거기서 멈춘다. 그래서 **총 개수는 알 수 없고** "N행 이상"으로 말한다.
    정확한 개수가 필요하면 ``-o`` 로 파일에 받거나 ``--max-rows 0`` 을 준다.

    ``max_rows`` 가 0이면 끝까지 받는다.
    """
    rows: List[Sequence[Any]] = []
    limit = max_rows + 1 if max_rows > 0 else 0
    while True:
        size = batch_size if limit <= 0 else min(batch_size, limit - len(rows))
        if size <= 0:
            break
        batch = cursor.fetchmany(size)
        if not batch:
            break
        rows.extend(batch)
        if on_batch is not None:
            on_batch(len(rows))

    if max_rows > 0 and len(rows) > max_rows:
        return rows[:max_rows], True
    return rows, False


def print_result(
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    truncated: bool,
    null_string: str = "NULL",
    stream: Optional[TextIO] = None,
    vertical: bool = False,
) -> None:
    """표를 stdout 으로, 행 수 요약을 stderr 로 낸다.

    표는 데이터라 stdout 으로 가야 파이프로 넘길 수 있고, 요약은 보고라 stderr
    로 가야 그 데이터에 섞이지 않는다.
    """
    body, note = format_result(columns, rows, truncated, null_string, vertical)
    print(body, file=stream or sys.stdout)
    print(note, file=sys.stderr)


def format_result(
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    truncated: bool,
    null_string: str = "NULL",
    vertical: bool = False,
) -> Tuple[str, str]:
    """(화면에 낼 본문, 행 수 요약) 을 만든다.

    본문과 요약을 나눠 두면 셸이 본문만 페이저로 넘길 수 있다.
    """
    if rows:
        draw = render_vertical if vertical else render
        body = draw(columns, rows, null_string)
    else:
        body = " | ".join(columns) + "\n(0행)"

    if truncated:
        note = (
            f"{len(rows):,}행 이상 (앞 {len(rows):,}행만 표시 — "
            "전부 보려면 --max-rows 0, 파일로 받으려면 -o)"
        )
    else:
        note = f"{len(rows):,}행"
    return body, note


def render_vertical(
    columns: Sequence[str], rows: Sequence[Sequence[Any]], null_string: str = "NULL"
) -> str:
    """한 행을 여러 줄로 편다. 컬럼이 많아 가로로 넘칠 때 쓴다(psql 의 \\x)."""
    width = max((display_width(c) for c in columns), default=0)
    blocks = []
    for index, row in enumerate(rows, 1):
        header = f"-[ RECORD {index} ]"
        blocks.append(header + "-" * max(0, width + 12 - display_width(header)))
        for column, value in zip(columns, row):
            blocks.append(f"{pad(column, width)} | {format_value(value, null_string)}")
    return "\n".join(blocks)


@contextlib.contextmanager
def open_output(path: str, use_gzip: bool, encoding: str) -> Iterator[TextIO]:
    """CSV 출력 파일을 연다.

    csv 모듈은 자체적으로 개행을 제어하므로 newline='' 로 열어야 한다.
    그러지 않으면 윈도우에서 빈 줄이 하나씩 끼어든다.
    """
    if use_gzip:
        handle = gzip.open(path, "wt", encoding=encoding, newline="")
    else:
        handle = open(path, "w", encoding=encoding, newline="")
    try:
        yield handle
    finally:
        handle.close()


def write_csv(
    handle: TextIO,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    delimiter: str = "`",
    null_string: str = "",
    write_header: bool = True,
) -> int:
    """받아둔 결과를 CSV로 쓴다. 기본값은 백틱 구분, 따옴표 없음이다."""
    writer = csv.writer(
        handle,
        delimiter=delimiter,
        quoting=csv.QUOTE_NONE,
        quotechar=None,
        escapechar="\\",
    )
    if write_header:
        writer.writerow(columns)
    for row in rows:
        writer.writerow([null_string if v is None else v for v in row])
    return len(rows)
