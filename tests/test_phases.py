"""세부 단계(phase) 타임라인 가시화 검증.

- core.phases.record_stage 의 시작/종료/소요/행수 기록 규칙(순수 로직).
- executor 를 통해 task 를 실행하면 view()/API 에 단계 타임라인·rows_read·조회완료 시각이
  실리는지(엔드투엔드, MockBackend).
- 실제 백엔드의 STREAM_COPY 읽기/쓰기 대기 분리 계측(_stream_to_copy/_copy_stats).
"""

from __future__ import annotations

import asyncio

import httpx

from core.phases import PHASE_LABELS, phase_of, record_stage
from executor.app import create_app as create_executor_app
from executor.backend import ImpalaToGreenplumBackend, MockBackend


# ─────────────────────────── 순수 로직 ───────────────────────────
def test_record_stage_start_end_and_duration():
    phases: list = []
    record_stage(phases, "STREAM_COPY", "start")
    assert phases[0]["finished_at"] is None and phases[0]["label"] == PHASE_LABELS["STREAM_COPY"]
    rows = record_stage(phases, "STREAM_COPY", "end",
                        {"rows": 100, "read_wait_ms": 30, "write_wait_ms": 10})
    assert rows == 100
    p = phase_of(phases, "STREAM_COPY")
    assert p["rows"] == 100
    assert p["finished_at"] is not None
    assert p["duration_ms"] is not None and p["duration_ms"] >= 0
    # rows 는 별도 필드로 빠지고 나머지는 extra 로 들어간다.
    assert p["extra"] == {"read_wait_ms": 30, "write_wait_ms": 10}


def test_record_stage_end_without_start_is_noop():
    phases: list = []
    assert record_stage(phases, "COMMIT", "end") is None
    assert phases == []


# ─────────────────────────── 엔드투엔드 ───────────────────────────
def _payload(task_id, **over):
    base = {
        "task_id": task_id,
        "job_id": "j",
        "sub_query": "SELECT a, dt FROM imp WHERE dt IN ('1')",
        "target_table": "public.target",
        "write_mode": "append",
        "partition_column": "dt",
        "partition_values": ["'1'"],
        "exec_mode": "stage_insert",
        "staging_table": "stg_t",
        "staging_ddl": "CREATE TEMP TABLE stg_t (a int, dt text)",
        "insert_sql": "INSERT INTO public.target (a, dt) SELECT a, dt FROM stg_t",
    }
    base.update(over)
    return base


async def _run(app, payload):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        await c.post("/tasks", json=payload)
        for _ in range(200):
            st = (await c.get(f"/tasks/{payload['task_id']}")).json()
            if st["status"] in ("DONE", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
        return st


async def test_stage_insert_task_exposes_phase_timeline():
    st = await _run(create_executor_app(backend=MockBackend(rows_per_value=13)),
                    _payload("tp"))
    assert st["status"] == "DONE"
    names = [p["name"] for p in st["phases"]]
    # QUEUE_WAIT(접수 대기) → IMPALA_SUBMIT → STAGING_DDL → STREAM_COPY → INSERT → COMMIT
    for expected in ("QUEUE_WAIT", "IMPALA_SUBMIT", "STAGING_DDL",
                     "STREAM_COPY", "INSERT", "COMMIT"):
        assert expected in names, names
    # STREAM_COPY 종료 시점이 Impala 조회 완료 시각으로 노출되고, 읽은 건수가 rows_read.
    assert st["rows_read"] == 13
    assert st["impala_done_at"] is not None
    # 모든 단계가 종료(finished_at)되어 소요가 계산돼 있다.
    for p in st["phases"]:
        assert p["finished_at"] is not None
        assert p["duration_ms"] is not None


async def test_copy_mode_task_exposes_stream_copy_phase():
    st = await _run(create_executor_app(backend=MockBackend(rows_per_value=7)),
                    _payload("tc", exec_mode="copy"))
    assert st["status"] == "DONE"
    names = [p["name"] for p in st["phases"]]
    assert "STREAM_COPY" in names and "STAGING_DDL" not in names
    assert st["rows_read"] == 7  # copy 모드는 읽은 행수 = 적재 행수


# ─────────────────── 실제 백엔드: 읽기/쓰기 대기 분리 계측 ───────────────────
class _FakeImpalaCursor:
    """description 과 fetchmany 를 흉내내는 Impala 커서(2배치 후 소진)."""

    def __init__(self):
        self.description = [("a",), ("dt",)]
        self._batches = [[(1, "x"), (2, "y")], [(3, "z")]]

    def fetchmany(self, size):
        return self._batches.pop(0) if self._batches else []


class _FakeCopy:
    def __init__(self):
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def write_row(self, row):
        self.rows.append(row)


class _FakeGpCursor:
    def __init__(self):
        self.copied = _FakeCopy()

    def copy(self, sql):
        return self.copied


def _mk_backend(pipeline):
    return ImpalaToGreenplumBackend(
        impala_dsn={}, greenplum_dsn="postgresql://x", batch_size=10,
        pipeline=pipeline, queue_size=4,
    )


def test_stream_serial_measures_read_and_write_and_stats():
    backend = _mk_backend(pipeline=False)
    cur = _FakeImpalaCursor()
    gp = _FakeGpCursor()
    seen = []
    loaded, read_wait, write_wait, finalize_wait, read_starve = backend._stream_to_copy(
        cur, gp, "COPY t (a, dt) FROM STDIN", on_progress=lambda n: seen.append(n)
    )
    assert loaded == 3  # 2 + 1
    assert gp.copied.rows == [(1, "x"), (2, "y"), (3, "z")]
    assert seen[-1] == 3  # 진행률 콜백이 누적 행수로 호출됨
    assert read_wait >= 0 and write_wait >= 0 and finalize_wait >= 0
    assert read_starve == 0.0  # 직렬 모드는 굶음 없음
    stats = backend._copy_stats(loaded, read_wait, write_wait, finalize_wait, read_starve)
    assert stats["rows"] == 3
    for k in ("read_wait_ms", "write_wait_ms", "read_starve_ms", "finalize_wait_ms"):
        assert k in stats
    assert stats["rows_per_sec"] >= 0


def test_stream_pipelined_reads_all_rows_in_order():
    """파이프라인 모드도 모든 행을 순서대로 COPY 로 흘려보내는지(리더 스레드 경유)."""
    backend = _mk_backend(pipeline=True)
    cur = _FakeImpalaCursor()
    gp = _FakeGpCursor()
    seen = []
    loaded, read_wait, write_wait, finalize_wait, read_starve = backend._stream_to_copy(
        cur, gp, "COPY t (a, dt) FROM STDIN", on_progress=lambda n: seen.append(n)
    )
    assert loaded == 3
    assert gp.copied.rows == [(1, "x"), (2, "y"), (3, "z")]  # 순서 보존
    assert seen[-1] == 3
    assert read_starve >= 0.0


def test_stream_pipelined_propagates_reader_error():
    """리더(Impala fetch)에서 난 예외가 라이터(호출부)로 전파되는지."""
    class _BoomCursor:
        description = [("a",)]

        def fetchmany(self, size):
            raise RuntimeError("impala 폭발")

    backend = _mk_backend(pipeline=True)
    gp = _FakeGpCursor()
    try:
        backend._stream_to_copy(_BoomCursor(), gp, "COPY t (a) FROM STDIN", on_progress=None)
        assert False, "예외가 전파되어야 한다"
    except RuntimeError as e:
        assert "impala 폭발" in str(e)


def test_stream_pipelined_writer_error_does_not_deadlock():
    """라이터(COPY write)가 중간에 죽어도 리더가 큐 put 에서 막혀 join 이 멈추지 않아야 한다.

    큐(=4)보다 많은 배치를 계속 내는 리더 + 첫 write_row 에서 터지는 라이터 조합으로,
    리더가 put backpressure 에 걸린 상태에서 라이터가 죽는 데드락 시나리오를 강제한다.
    """
    class _EndlessCursor:
        description = [("a",)]

        def fetchmany(self, size):
            return [(1,)] * size  # 절대 소진되지 않음 → 리더가 큐를 계속 채우려 함

    class _BoomCopy(_FakeCopy):
        def write_row(self, row):
            raise RuntimeError("copy 폭발")

    class _BoomGp:
        def copy(self, sql):
            return _BoomCopy()

    backend = _mk_backend(pipeline=True)
    try:
        backend._stream_to_copy(_EndlessCursor(), _BoomGp(), "COPY t (a) FROM STDIN", None)
        assert False, "라이터 예외가 전파되어야 한다"
    except RuntimeError as e:
        assert "copy 폭발" in str(e)  # 데드락 없이 예외로 종료(테스트가 끝나면 통과)
