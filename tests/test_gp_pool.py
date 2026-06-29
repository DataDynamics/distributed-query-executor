"""Greenplum 커넥션 풀(_GreenplumPool) 동작 검증 — 가짜 연결로 DB 없이 테스트."""

import threading
import time

import pytest

from executor.backend import _GreenplumPool


class FakeConn:
    """psycopg 연결 흉내: reset(DISCARD ALL)·close 호출을 기록한다."""

    _ids = iter(range(1, 10_000))

    def __init__(self, fail_reset=False):
        self.id = next(FakeConn._ids)
        self.autocommit = False
        self.closed = False
        self.discard_calls = 0
        self.rollbacks = 0
        self._fail_reset = fail_reset

    def rollback(self):
        self.rollbacks += 1

    def execute(self, sql):
        if sql == "DISCARD ALL":
            self.discard_calls += 1
            if self._fail_reset:
                raise RuntimeError("DISCARD ALL 미지원")

    def close(self):
        self.closed = True


def _factory(**kw):
    created = []

    def make():
        c = FakeConn(**kw)
        created.append(c)
        return c

    return make, created


def test_reuses_connection_and_resets_on_return():
    make, created = _factory()
    pool = _GreenplumPool("dsn", 2, connect=make)
    with pool.connection() as c1:
        first = c1
    # 반납 시 DISCARD ALL 로 초기화됐어야 한다.
    assert first.discard_calls == 1
    # 다음 호출은 같은 연결을 재사용(새로 만들지 않음).
    with pool.connection() as c2:
        assert c2 is first
    assert len(created) == 1
    assert first.discard_calls == 2


def test_discards_connection_on_error():
    make, created = _factory()
    pool = _GreenplumPool("dsn", 2, connect=make)
    with pytest.raises(ValueError):
        with pool.connection() as c:
            bad = c
            raise ValueError("작업 중 오류")
    # 오류 난 연결은 재사용하지 않고 닫는다.
    assert bad.closed is True
    # 다음 호출은 새 연결을 만든다.
    with pool.connection() as c2:
        assert c2 is not bad
    assert len(created) == 2


def test_discards_connection_when_reset_fails():
    make, created = _factory(fail_reset=True)
    pool = _GreenplumPool("dsn", 2, connect=make)
    with pool.connection() as c:
        first = c
    # DISCARD ALL 실패 → 재사용하지 않고 닫음.
    assert first.closed is True
    with pool.connection() as c2:
        assert c2 is not first


def test_bounded_concurrency_blocks_beyond_maxsize():
    make, created = _factory()
    pool = _GreenplumPool("dsn", 2, connect=make)
    started = threading.Semaphore(0)
    release = threading.Event()
    held = []

    def worker():
        with pool.connection():
            held.append(1)
            started.release()
            release.wait(2)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    # 두 슬롯이 모두 점유될 때까지 대기
    assert started.acquire(timeout=2)
    assert started.acquire(timeout=2)
    assert len(held) == 2

    # 세 번째는 슬롯이 없어 즉시 획득 불가(블로킹)해야 한다.
    got = threading.Event()

    def third():
        with pool.connection():
            got.set()

    t3 = threading.Thread(target=third)
    t3.start()
    assert got.wait(0.3) is False  # 아직 슬롯 없음 → 진입 못 함

    release.set()  # 두 작업 종료 → 슬롯 반납
    assert got.wait(2) is True     # 세 번째가 슬롯을 얻어 진입
    for t in threads:
        t.join(2)
    t3.join(2)
    # 동시 연결은 2개를 넘지 않았다(재사용 포함).
    assert len(created) <= 2


def test_close_closes_idle_connections():
    make, created = _factory()
    pool = _GreenplumPool("dsn", 2, connect=make)
    with pool.connection():
        pass
    pool.close()
    assert created[0].closed is True
