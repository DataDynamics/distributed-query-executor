"""이벤트 루프 정지 감시다. 루프가 오래 응답하지 않으면 그 순간 루프 스레드가 실행 중인 코드 위치를 남긴다.

coordinator 와 executor 는 단일 워커라 HTTP 처리·헬스 체크·task 폴링을 이벤트 루프 스레드 하나가
모두 맡는다. 그 스레드에서 블로킹 호출(동기 DB 연결, 큰 JSON 복원 등)이 돌면 그동안 모든 요청이
멈추고, 걸어 둔 httpx timeout 이 지나 "executor 다운 감지"나 "폴링 실패" 로 나타난다. CPU 는 놀고
있어 자원 지표로는 보이지 않으므로, 멈춘 순간의 스택을 남겨 어떤 호출이 루프를 막았는지 바로 알 수
있게 한다.

감시는 별도 데몬 스레드가 한다. ``call_soon_threadsafe`` 로 루프에 신호 콜백을 넣고 기준 시간 안에
실행되지 않으면 정지로 본다. 이 방식은 루프가 블로킹 호출에 묶인 경우뿐 아니라 콜백이 밀려 처리가
늦어진 경우도 잡는다. 정지 한 번에 경고는 두 번(멈춤 감지와 해소)만 남긴다.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
import traceback
from typing import Optional

from core.perf import perf

logger = logging.getLogger(__name__)

# 경고에 담을 루프 스레드 스택의 최대 프레임 수다(가장 최근 호출부터).
_STACK_LIMIT = 25


class LoopStallWatchdog:
    """이벤트 루프 정지를 감시하는 데몬 스레드다. 루프 스레드 안에서 ``start()`` 해야 한다.

    인자:
        threshold_s: 루프가 이 시간 넘게 신호 콜백을 처리하지 못하면 정지로 본다.
        interval_s: 정상일 때 다음 확인까지 쉬는 시간. 기본은 기준 시간과 같다.
    """

    def __init__(self, threshold_s: float, interval_s: Optional[float] = None) -> None:
        self.threshold_s = threshold_s
        self.interval_s = interval_s if interval_s is not None else threshold_s
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread_id: Optional[int] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # 감지한 정지 횟수다. 테스트와 디버깅에서 참조한다.
        self.stalls = 0

    def start(self) -> None:
        """현재 실행 중인 루프와 그 스레드를 기억하고 감시 스레드를 띄운다."""
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="loop-stall-watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """감시 스레드를 멈춘다. 이미 멈췄으면 아무것도 하지 않는다."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.threshold_s + self.interval_s + 1.0)
            self._thread = None

    def _loop_stack(self) -> str:
        """루프 스레드의 현재 스택을 가장 최근 호출 위주로 문자열로 만든다."""
        frame = sys._current_frames().get(self._loop_thread_id)
        if frame is None:
            return "    (스택을 얻지 못함)"
        lines = traceback.format_stack(frame, limit=_STACK_LIMIT)
        return "".join(lines).rstrip()

    def _run(self) -> None:
        loop = self._loop
        while not self._stop.is_set():
            answered = threading.Event()
            started = time.monotonic()
            try:
                loop.call_soon_threadsafe(answered.set)
            except RuntimeError:
                return  # 루프가 닫혔다(종료 중)
            if not answered.wait(self.threshold_s):
                self.stalls += 1
                perf.inc("loop.stalls")  # 성능 지표 로그의 구간별 정지 횟수
                # 멈춘 바로 그 순간의 스택이 원인을 가리키므로 기다리기 전에 먼저 잡는다.
                logger.warning(
                    "이벤트 루프가 %dms 넘게 응답하지 않음 — 루프 스레드 실행 위치:\n%s",
                    int(self.threshold_s * 1000), self._loop_stack(),
                )
                while not answered.wait(1.0):
                    if self._stop.is_set():
                        return
                logger.warning(
                    "이벤트 루프 정지 해소 — 약 %dms 동안 응답 없음",
                    int((time.monotonic() - started) * 1000),
                )
            self._stop.wait(self.interval_s)


def start_loop_watchdog(stall_ms: int) -> Optional[LoopStallWatchdog]:
    """설정값(ms)으로 감시를 시작해 돌려준다. 0 이하면 감시를 끄고 None 을 돌려준다."""
    if not stall_ms or stall_ms <= 0:
        return None
    dog = LoopStallWatchdog(stall_ms / 1000.0)
    dog.start()
    return dog
