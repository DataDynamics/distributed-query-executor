"""메타 저장소(PostgreSQL) I/O 를 asyncio 기본 스레드 풀과 떼어 놓는 전용 풀과 연결 헬퍼다.

coordinator 와 executor 는 긴 블로킹 작업을 asyncio 기본 스레드 풀(``min(32, CPU+4)``)로 넘긴다.
coordinator 의 Phase 2·3 GP INSERT, executor 의 task 적재 본체처럼 몇 분씩 걸리는 작업이다.
메타 저장소에 한 행을 쓰는 짧은 호출까지 같은 풀에 넣으면, 풀이 적재로 차 있는 동안 그 호출이 적재가
끝날 때까지 줄을 선다. 이력 기록은 실행 경로 한가운데서 await 되므로 job 의 슬롯 반납과 task 의 읽기
시작이 함께 늦어지고, heartbeat·self-report 가 밀리면 멀쩡한 coordinator·executor 가 죽은 것으로
판정된다. 그래서 메타 호출은 용도별 전용 풀에서 돌린다.

풀은 둘이다. ``store`` 는 job 저장소(저장·취소 플래그·건수 집계) 몫이고, ``meta`` 는 이력·heartbeat·
self-report·예약·메트릭 기록 몫이다. 둘을 나눈 것은 기록 쪽 DB 호출이 몰리거나 걸려도 job 상태 저장이
그 뒤에서 기다리지 않게 하기 위해서다.

연결은 :func:`meta_connect` 로 연다. libpq 기본값은 연결 대기 상한이 없어서, DB 가 응답하지 않으면
전용 풀의 스레드가 무한정 묶이고 결국 풀 전체가 멈춘다. ``db.connect_timeout_s`` 로 상한을 둔다.
"""

from __future__ import annotations

import asyncio
import contextvars
import threading
import time
from typing import Dict

from core.perf import InstrumentedThreadPoolExecutor, perf

# 풀 이름별 스레드 수다. 메타 호출은 ms 단위로 끝나므로 이 정도면 동시 디스패치가 몰려도 충분하고,
# DB 가 멈췄을 때 붙잡히는 연결 수에도 상한이 된다.
_POOL_WORKERS: Dict[str, int] = {"store": 8, "meta": 8}
_pools: Dict[str, InstrumentedThreadPoolExecutor] = {}
_pools_lock = threading.Lock()

# 설정을 읽지 못할 때 쓰는 연결 대기 상한(초)이다.
_DEFAULT_CONNECT_TIMEOUT_S = 5


def _executor(name: str) -> InstrumentedThreadPoolExecutor:
    """이름에 해당하는 전용 풀을 처음 쓸 때 만든다(프로세스당 하나).

    계측용 풀이라 성능 지표 로그에 ``pool.store.*``·``pool.meta.*``(사용 중·큐 길이·대기·실행 시간)가
    남는다. 실행 시간이 곧 메타 저장소 호출 한 건의 지연이다.
    """
    with _pools_lock:
        pool = _pools.get(name)
        if pool is None:
            pool = _pools[name] = InstrumentedThreadPoolExecutor(
                name, _POOL_WORKERS[name], thread_name_prefix=f"{name}-io",
            )
        return pool


async def _run(name: str, fn, *args):
    # run_in_executor 는 contextvars 를 넘기지 않으므로 복사해 넘겨야 [job_id][task_id] 로그 표시가 이어진다.
    ctx = contextvars.copy_context()
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor(name), lambda: ctx.run(fn, *args))


async def run_store_io(fn, *args):
    """job 저장소의 블로킹 호출을 store 전용 풀에서 실행한다."""
    return await _run("store", fn, *args)


async def run_meta_io(fn, *args):
    """이력·heartbeat·self-report·예약·메트릭 기록 같은 블로킹 메타 호출을 meta 전용 풀에서 실행한다."""
    return await _run("meta", fn, *args)


def _connect_timeout_s() -> int:
    """설정의 연결 대기 상한(초)을 읽는다. 설정을 불러올 수 없으면 기본값을 쓴다."""
    try:
        from core.config import settings
    except Exception:  # 설정 파일 없이 import 되는 도구·테스트에서도 동작하게 한다
        return _DEFAULT_CONNECT_TIMEOUT_S
    return int(getattr(settings, "db_connect_timeout_s", _DEFAULT_CONNECT_TIMEOUT_S) or 0)


def meta_connect(dsn: str):
    """메타 저장소 연결을 연결 대기 상한을 붙여 연다.

    DSN 에 ``connect_timeout`` 을 이미 적었으면 그 값을 존중해 덮어쓰지 않는다. 설정값이 0 이면
    상한 없이 연다(libpq 기본 동작). psycopg 는 선택 의존성이라 여기서 지연 임포트한다.
    """
    import psycopg

    timeout = _connect_timeout_s()
    kwargs = {"connect_timeout": timeout} if timeout > 0 and "connect_timeout" not in dsn else {}
    t0 = time.monotonic()
    try:
        conn = psycopg.connect(dsn, **kwargs)
    except Exception as exc:
        # 연결 실패는 성능 지표 로그에서 구간별로 센다. 대기 상한에 걸린 것은 따로 센다.
        perf.inc("meta_db.connect_errors")
        if type(exc).__name__ == "ConnectionTimeout" or "timeout" in str(exc).lower():
            perf.inc("meta_db.connect_timeouts")
        raise
    perf.observe("meta_db.connect_ms", (time.monotonic() - t0) * 1000)
    return conn
