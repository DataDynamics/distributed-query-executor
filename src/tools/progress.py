"""구간별 소요 시간과 진행 상황을 보고한다.

bin/ 아래 모든 스크립트가 같은 형식으로 보고하도록 여기 모아둔다. 작업이 끝나면
어느 구간에 시간을 썼는지 항상 표로 남기고, 오래 걸리는 구간에서는 진행 상황을
보여준다.

**출력은 전부 stderr 로 간다.** stdout 은 조회 결과 같은 데이터 몫이라, 파이프로
넘기거나 파일로 받을 때 보고가 섞이면 안 된다.

크론에서 도는 것을 전제로 만들었다.

- 진행 상황은 터미널이면 한 줄을 ``\\r`` 로 덮어쓰고, 아니면 줄을 새로 쓴다.
  크론 로그가 ``\\r`` 로 뒤덮이지 않는다.
- 터미널이 아니면 갱신 간격을 크게 잡는다. 몇 시간짜리 작업이어도 로그가 몇 줄
  늘어날 뿐이다.
- 갱신을 행 수가 아니라 **시간**으로 끊는다. 행이 몇 건이든 로그 양이 예측된다.
"""

import contextlib
import sys
import time
import unicodedata
from typing import Any, Dict, Iterator, List, Optional, Sequence, TextIO

#: 터미널일 때 진행 상황을 갱신하는 간격(초)
INTERACTIVE_INTERVAL = 0.2

#: 터미널이 아닐 때(크론, 로그 리다이렉트) 갱신 간격(초)
BATCH_INTERVAL = 30.0


def display_width(text: str) -> int:
    """터미널에서 차지하는 칸 수. 한글·한자는 두 칸을 쓴다."""
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def pad(text: str, width: int) -> str:
    """표시 폭 기준으로 오른쪽을 채운다(한글이 섞여도 열이 맞는다)."""
    return text + " " * max(0, width - display_width(text))


def human_bytes(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size) < 1024 or unit == "GB":
            return f"{size:,.1f}{unit}"
        size /= 1024
    return f"{size:,.1f}GB"


def human_seconds(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}초"
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}시간 {minutes}분 {secs}초"
    return f"{minutes}분 {secs}초"


def is_interactive(stream: TextIO = None) -> bool:
    """터미널에 붙어 있는지. 크론이면 False 다."""
    stream = stream or sys.stderr
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


class Progress:
    """오래 걸리는 작업의 진행 상황을 stderr 로 보여준다.

    ``update()`` 는 자주 불러도 된다. 실제 출력은 시간 간격으로 끊는다.
    """

    def __init__(
        self,
        label: str,
        total: Optional[int] = None,
        unit: str = "건",
        stream: TextIO = None,
        enabled: bool = True,
        interval: Optional[float] = None,
    ) -> None:
        self.label = label
        self.total = total
        self.unit = unit
        self.stream = stream or sys.stderr
        self.interactive = is_interactive(self.stream)
        self.enabled = enabled
        self.interval = (
            interval
            if interval is not None
            else (INTERACTIVE_INTERVAL if self.interactive else BATCH_INTERVAL)
        )
        self.count = 0
        self._started = time.monotonic()
        # 시작 시각을 기준으로 두면 첫 출력이 한 간격 뒤에 나온다. 금방 끝나는
        # 작업은 진행 상황을 아예 남기지 않고, 처리 속도도 0초로 나누지 않는다.
        self._last = self._started
        self._wrote = False

    def _line(self) -> str:
        elapsed = time.monotonic() - self._started
        rate = self.count / elapsed if elapsed > 0 else 0.0
        text = f"  {self.label} {self.count:,}{self.unit}"
        if self.total:
            share = self.count / self.total * 100
            text += f" / {self.total:,}{self.unit} ({share:.0f}%)"
        return f"{text}  {rate:,.0f}{self.unit}/s  {human_seconds(elapsed)}"

    def _emit(self, force: bool = False) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        if not force and now - self._last < self.interval:
            return
        self._last = now
        line = self._line()
        if self.interactive:
            # 한 줄을 덮어쓴다. 이전 줄이 더 길 수 있으니 공백으로 지운다.
            self.stream.write("\r" + line + "\033[K")
        else:
            self.stream.write(line + "\n")
        self.stream.flush()
        self._wrote = True

    def update(self, count: int) -> None:
        """지금까지 처리한 누적 개수를 알린다."""
        self.count = count
        self._emit()

    def advance(self, delta: int = 1) -> None:
        self.update(self.count + delta)

    def finish(self) -> None:
        """작업이 끝났을 때 줄을 정리한다.

        터미널에서는 덮어쓰던 줄을 지운다. 뒤이어 나올 요약과 겹치지 않게 한다.
        """
        if not self._wrote:
            return
        if self.interactive:
            self.stream.write("\r\033[K")
            self.stream.flush()

    def __enter__(self) -> "Progress":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.finish()


class PhaseTimer:
    """구간별 누적 시간을 재고 표로 정리한다.

    조회와 쓰기는 번갈아 일어나므로 한 번씩 재서는 의미가 없다. 구간 이름별로
    누적해서 "전체 중 어디에 시간을 썼는지"를 본다.
    """

    def __init__(self, order: Sequence[str] = ()) -> None:
        """
        Args:
            order: 보고서에 표시할 구간 순서. 미리 정해두면 실제로 처음 호출된
                순서(예: 헤더를 먼저 쓰느라 '쓰기'가 앞서는 경우)와 무관하게
                파이프라인 흐름대로 읽힌다.
        """
        self._elapsed: Dict[str, float] = {name: 0.0 for name in order}
        self._order: List[str] = list(order)
        self._started = time.monotonic()

    @contextlib.contextmanager
    def measure(self, name: str) -> Iterator[None]:
        if name not in self._elapsed:
            self._elapsed[name] = 0.0
            self._order.append(name)
        start = time.monotonic()
        try:
            yield
        finally:
            self._elapsed[name] += time.monotonic() - start

    @property
    def total(self) -> float:
        """프로그램이 시작한 뒤 지금까지 실제로 흐른 시간이다."""
        return time.monotonic() - self._started

    def report(self) -> str:
        # 한 번도 실행되지 않은 구간은 굳이 보여주지 않는다
        names = [n for n in self._order if self._elapsed[n] > 0]
        measured = sum(self._elapsed.values())
        total = self.total
        width = max((display_width(n) for n in names), default=0)
        width = max(width, display_width("기타"), display_width("합계"))

        lines = ["", "=== 구간별 소요 시간 ==="]
        for index, name in enumerate(names, 1):
            seconds = self._elapsed[name]
            share = seconds / total * 100 if total > 0 else 0.0
            lines.append(f"  {index}. {pad(name, width)}  {seconds:8.3f}초  {share:5.1f}%")

        # 측정 구간 밖에서 흘러간 시간(인자 처리, 파일 열기, 리포트 출력 등)
        other = total - measured
        if other > 0.001:
            share = other / total * 100 if total > 0 else 0.0
            lines.append(f"     {pad('기타', width)}  {other:8.3f}초  {share:5.1f}%")

        lines.append("  " + "─" * (width + 21))
        lines.append(f"     {pad('합계', width)}  {total:8.3f}초  100.0%")
        return "\n".join(lines)

    def print_report(self, stream: TextIO = None) -> None:
        """작업이 끝난 뒤 항상 부른다. stdout 은 데이터 몫이라 stderr 로 낸다.

        재어둔 구간이 하나도 없으면 내지 않는다. 패키지가 없거나 인자가 틀려서
        아무것도 못 해본 경우인데, "합계 0.000초" 만 있는 표는 알려주는 것이 없다.
        """
        if not any(seconds > 0 for seconds in self._elapsed.values()):
            return
        print(self.report(), file=stream or sys.stderr)


def add_progress_argument(parser: Any) -> None:
    """``--no-progress`` 를 파서에 붙인다."""
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="진행 상황을 출력하지 않습니다. 소요 시간 요약은 그대로 나옵니다.",
    )
