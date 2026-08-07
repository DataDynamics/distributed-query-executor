"""이관(/jobs) 소스 엔진 선택 — SELECT 를 Impala 대신 커스텀 함수(Trino 등)로 읽는 경로.

job 의 ``datasource`` 가 impala(기본)가 아니면 executor backend 가
``query.func.<name>.connect`` 로 지정된 함수의 **DB-API 연결**로 SELECT 를 읽는다. 적재
(Greenplum)와 exec_mode 는 그대로다. 실제 Trino 없이 가짜 DB-API 연결로 검증한다.

핵심 회귀 방지 포인트:
  - datasource 가 impala/미지정이면 커스텀 경로를 절대 타지 않는다(기존 동작 보존).
  - 커스텀 소스는 ``fetchmany`` 스트리밍을 그대로 쓴다(limit 으로 잘리지 않는다).
  - impala_query_options(configuration=) 는 Impala 에만 적용된다.
  - 설정이 없으면 조용히 Impala 로 폴백하지 않고 명확히 실패한다.
"""

from __future__ import annotations

import pytest

from executor.backend import ImpalaToGreenplumBackend


class _FakeCursor:
    """DB-API 2.0 커서 흉내 — execute/description/fetchmany 만 구현."""

    def __init__(self, rows: list, recorder: dict):
        self._rows = list(rows)
        self._rec = recorder
        self.description = [("a", None), ("dt", None)]

    def execute(self, sql, *args, **kwargs):
        # configuration= 이 넘어오면 기록한다(Impala 전용 kwarg 가 새는지 확인용).
        self._rec["sql"] = sql
        self._rec["execute_kwargs"] = kwargs

    def fetchmany(self, n):
        batch, self._rows = self._rows[:n], self._rows[n:]
        return batch


class _FakeConn:
    """cursor(convert_types=...) 를 모르는 연결 — Trino 등 일반 DB-API 드라이버와 같다."""

    def __init__(self, rows: list, recorder: dict):
        self._rows = rows
        self._rec = recorder
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._rows, self._rec)

    def close(self):
        self.closed = True


@pytest.fixture
def recorder() -> dict:
    return {}


@pytest.fixture
def backend(recorder, monkeypatch):
    """trino datasource 에 가짜 connect 함수를 물린 backend."""
    conns: list = []

    def fake_connect(*, config):
        recorder["config"] = config
        conn = _FakeConn([[i, "2026-01-01"] for i in range(2500)], recorder)
        conns.append(conn)
        return conn

    be = ImpalaToGreenplumBackend(
        impala_dsn={"host": "impala.example"}, greenplum_dsn="", batch_size=1000,
        query_options={"MEM_LIMIT": "2g"},
        source_funcs={"trino": {"module": "", "connect": "pkg:connect",
                                "config": {"host": "trino.example"}}},
    )
    monkeypatch.setattr("executor.backend.load_dotted", lambda dotted: fake_connect)
    be._conns = conns
    return be


def test_custom_source_streams_all_rows(backend, recorder, tmp_path):
    """커스텀 소스도 fetchmany 스트리밍을 그대로 쓴다 — 미리보기 limit(10000)에 묶이지 않는다."""
    out = tmp_path / "t.csv"
    rows = backend.export_to_local_csv(
        "SELECT a, dt FROM t", str(out), {"delimiter": ",", "null": "", "quote": '"'},
        datasource="trino",
    )
    # batch_size(1000)로 3번 나눠 받아 2500행 전부 CSV 로 나갔다.
    assert rows == 2500
    assert len(out.read_text(encoding="utf-8").strip().splitlines()) == 2500
    # connect 에는 그 소스의 config 블록이 그대로 전달된다.
    assert recorder["config"] == {"host": "trino.example"}
    assert backend._conns[0].closed is True


def test_custom_source_does_not_receive_impala_query_options(backend, recorder, tmp_path):
    """configuration= 은 impyla 전용이라 커스텀 소스 커서에는 넘기지 않는다."""
    backend.export_to_local_csv(
        "SELECT a, dt FROM t", str(tmp_path / "t.csv"), None,
        query_options={"REQUEST_POOL": "etl"}, datasource="trino",
    )
    assert recorder["execute_kwargs"] == {}


@pytest.mark.parametrize("ds", [None, "", "impala", "source"])
def test_builtin_names_never_take_custom_path(backend, ds, tmp_path):
    """빈 값/impala/source 는 커스텀 함수를 타지 않고 기존 Impala 경로로 간다."""
    # impyla 는 설치돼 있지 않으므로 import 단계에서 실패하는 것이 곧 "Impala 경로로 갔다"는 증거다.
    with pytest.raises(Exception) as ei:
        backend.export_to_local_csv("SELECT 1", str(tmp_path / "x.csv"), None, datasource=ds)
    assert "trino" not in str(ei.value).lower()


def test_unconfigured_custom_source_fails_loudly(tmp_path):
    """connect 미설정이면 조용히 Impala 로 폴백하지 않고 명확한 오류로 실패한다."""
    be = ImpalaToGreenplumBackend(
        impala_dsn={"host": "impala.example"}, greenplum_dsn="",
        source_funcs={"trino": {"module": "pkg:run", "connect": "", "config": {}}},
    )
    with pytest.raises(ValueError) as ei:
        be.export_to_local_csv("SELECT 1", str(tmp_path / "x.csv"), None, datasource="trino")
    assert "query.func.trino.connect" in str(ei.value)


def test_unknown_custom_source_lists_configured_names(tmp_path):
    be = ImpalaToGreenplumBackend(
        impala_dsn={}, greenplum_dsn="",
        source_funcs={"trino": {"module": "", "connect": "pkg:connect", "config": {}}},
    )
    with pytest.raises(ValueError) as ei:
        be.export_to_local_csv("SELECT 1", str(tmp_path / "x.csv"), None, datasource="presto")
    assert "presto" in str(ei.value) and "trino" in str(ei.value)


# ─────────────── coordinator → task payload → executor 배선 ───────────────


def test_task_payload_carries_datasource(runner, store, monkeypatch, tmp_path):
    """/jobs 의 datasource 가 executor 로 가는 task 본문에 실린다(HTTP 디스패치 경로)."""
    from coordinator.config import settings as coord_settings
    from coordinator.dispatcher import HttpDispatcher
    from coordinator.models import Job, JobStatus, Task

    sent: list = []

    class _Resp:
        status_code = 202

        def raise_for_status(self):
            return None

    class _FakeClient:
        async def post(self, url, json=None):
            sent.append(json)
            return _Resp()

    job = Job(
        job_id="j1", original_sql="SELECT 1", partition_column="dt",
        target_table="public.t", write_mode="append", parallelism=1,
        split_strategy="contiguous", failure_policy="fail_fast",
        status=JobStatus.RUNNING, datasource="trino",
    )
    task = Task(task_id="t1", job_id="j1", sub_query="SELECT 1",
                partition_values=["'x'"], executor_url="http://exec1:8001")
    disp = HttpDispatcher(coord_settings, store=store)

    import asyncio
    asyncio.run(disp._start_task(_FakeClient(), job, task, "http://exec1:8001"))
    assert sent and sent[0]["datasource"] == "trino"


def test_executor_accepts_and_stores_datasource():
    """executor 의 POST /tasks 가 datasource 를 받아 Task 에 보관한다."""
    from executor.models import CreateTaskRequest

    req = CreateTaskRequest(
        task_id="t1", job_id="j1", sub_query="SELECT 1", target_table="public.t",
        partition_column="dt", datasource="trino",
    )
    assert req.datasource == "trino"
    # 이 필드를 안 보내는 구버전 coordinator 와도 호환된다(기본 None = Impala).
    old = CreateTaskRequest(
        task_id="t2", job_id="j1", sub_query="SELECT 1", target_table="public.t",
        partition_column="dt",
    )
    assert old.datasource is None


async def test_s3_stage_job_with_trino_source_end_to_end(monkeypatch):
    """s3_stage 이관을 datasource=trino 로 제출하면 Phase 1 export 에 그 값이 전달된다.

    사용자가 실제로 원하는 조합(템플릿 SELECT 는 Trino, 적재는 Greenplum, 모드는 s3_stage)이
    coordinator→dispatcher→backend 까지 끊기지 않고 이어지는지 확인한다. Phase 2/3(GP·S3)은
    소스와 무관하므로 그대로다.
    """
    import asyncio

    import httpx

    from coordinator.app import create_app
    from coordinator.config import settings as coord_settings
    from coordinator.dispatcher import LocalDispatcher
    from coordinator.job_store import JobStore
    from tests.test_s3_stage import _MockS3StageBackend, _job_payload, _s3_settings

    class _Recording(_MockS3StageBackend):
        def __init__(self):
            super().__init__()
            self.seen: list = []

        def export_to_s3(self, impala_select, key, job_id, task_id, csv_options=None,
                         on_progress=None, query_options=None, on_stage=None, datasource=None):
            self.seen.append(datasource)
            return super().export_to_s3(impala_select, key, job_id, task_id, csv_options,
                                        on_progress, query_options, on_stage)

    _s3_settings(monkeypatch)
    backend = _Recording()
    store = JobStore()
    disp = LocalDispatcher(coord_settings, backend=backend, store=store)
    app = create_app(runner=disp, store=store, settings=coord_settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        payload = {**_job_payload(parallelism=2), "datasource": "trino"}
        resp = await c.post("/jobs", json=payload)
        assert resp.status_code == 202, resp.text
        job_id = resp.json()["job_id"]
        for _ in range(300):
            st = (await c.get(f"/jobs/{job_id}/status")).json()
            if st["status"] in ("DONE", "PARTIAL", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
    assert st["status"] == "DONE", st
    # 모든 Phase 1 task 가 trino 소스로 읽었다.
    assert backend.seen == ["trino", "trino"]
    # Phase 2(GP PXF 외부테이블→target INSERT)는 소스와 무관하게 그대로 수행됐다.
    assert backend.phase2 is not None and backend.cleaned


async def test_s3_stage_job_without_datasource_keeps_impala_path(monkeypatch):
    """datasource 미지정 이관은 backend 호출에 인자를 붙이지 않는다(기존 동작 그대로)."""
    import asyncio

    import httpx

    from coordinator.app import create_app
    from coordinator.config import settings as coord_settings
    from coordinator.dispatcher import LocalDispatcher
    from coordinator.job_store import JobStore
    from tests.test_s3_stage import _MockS3StageBackend, _job_payload, _s3_settings

    _s3_settings(monkeypatch)
    # datasource kwarg 를 아예 모르는(구버전) 백엔드 — 붙여 보내면 TypeError 로 실패한다.
    backend = _MockS3StageBackend()
    store = JobStore()
    disp = LocalDispatcher(coord_settings, backend=backend, store=store)
    app = create_app(runner=disp, store=store, settings=coord_settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://x") as c:
        resp = await c.post("/jobs", json=_job_payload(parallelism=2))
        assert resp.status_code == 202, resp.text
        job_id = resp.json()["job_id"]
        for _ in range(300):
            st = (await c.get(f"/jobs/{job_id}/status")).json()
            if st["status"] in ("DONE", "PARTIAL", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
    assert st["status"] == "DONE", st
