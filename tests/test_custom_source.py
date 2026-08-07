"""이관(/jobs) 소스 엔진 선택 — Impala(커서) vs 커서 없는 커스텀 API.

manifest 의 ``datasource`` 로 SELECT 를 읽을 엔진을 고른다. ``impala``(기본)는 기존 impyla
커서 경로 그대로이고, 그 외 이름은 ``query.func.fetch_module`` 커스텀 API 로 읽는다 —
운영에서 DB-API 커서를 쓸 수 없는 사내 API(Trino 등)를 위한 경로다.

핵심 회귀 방지 포인트:
  - datasource 가 impala/미지정이면 커스텀 경로를 **절대** 타지 않는다(기존 동작 보존).
  - 커스텀 API 결과(DataFrame/records/columns-rows/튜플/청크)를 모두 정규화한다.
  - 결측(NaN/NaT/NA)이 CSV 에 문자열로 새지 않고 NULL 마커로 나간다.
  - 설정이 없으면 Impala 로 조용히 폴백하지 않고 명확히 실패한다.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from executor.backend import ImpalaToGreenplumBackend


class _FakeDF:
    """DataFrame 최소 인터페이스 더블(pandas 미설치 환경에서도 계약을 고정하기 위함)."""

    def __init__(self, columns, rows):
        self.columns = list(columns)
        self._rows = [tuple(r) for r in rows]

    class _ILoc:
        def __init__(self, df):
            self._df = df

        def __getitem__(self, sl):
            return type(self._df)(self._df.columns, self._df._rows[sl])

    @property
    def iloc(self):
        return _FakeDF._ILoc(self)

    def itertuples(self, index=False, name=None):
        return iter(self._rows)


def _backend(fetch_fn, monkeypatch, *, batch_size=2, fetch_module="pkg:fetch", config=None):
    """커스텀 소스만 설정된 backend(연결 객체 없음)."""
    be = ImpalaToGreenplumBackend(
        impala_dsn={"host": "impala.example"}, greenplum_dsn="", batch_size=batch_size,
        query_options={"MEM_LIMIT": "2g"},
        source_fetch_module=fetch_module,
        source_func_config=config if config is not None else {"token": "t"},
    )
    monkeypatch.setattr("executor.backend.load_dotted", lambda dotted: fetch_fn)
    return be


def _csv_lines(path):
    text = path.read_text(encoding="utf-8")
    return text.strip().splitlines() if text.strip() else []


_CSV = {"delimiter": ",", "null": "", "quote": '"'}


# ───────────────────── 커스텀 API 반환 형태 정규화 ─────────────────────


def test_custom_api_returns_columns_rows_tuple(monkeypatch, tmp_path):
    seen = {}

    def fetch(sql, *, config):
        seen.update(sql=sql, config=config)
        return (["a", "dt"], [(1, "2026-01-01"), (2, "2026-01-02")])

    out = tmp_path / "t.csv"
    rows = _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT a, dt FROM t", str(out), _CSV, datasource="trino")
    assert rows == 2
    assert _csv_lines(out) == ["1,2026-01-01", "2,2026-01-02"]
    # 계약대로 sql·config 를 받는다(limit 은 없다 — 이관은 전량이다).
    assert seen == {"sql": "SELECT a, dt FROM t", "config": {"token": "t"}}


def test_custom_api_returns_dataframe(monkeypatch, tmp_path):
    def fetch(sql, *, config):
        return _FakeDF(["a", "b"], [[1, "x"], [2, "y"], [3, "z"]])

    out = tmp_path / "t.csv"
    rows = _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), _CSV, datasource="trino")
    assert rows == 3 and _csv_lines(out) == ["1,x", "2,y", "3,z"]


def test_custom_api_returns_json_records(monkeypatch, tmp_path):
    """가장 흔한 JSON 응답 형태 — dict 목록. 컬럼 순서는 첫 행의 키 순서."""
    def fetch(sql, *, config):
        return [{"a": 1, "dt": "2026-01-01"}, {"a": 2, "dt": "2026-01-02"}]

    out = tmp_path / "t.csv"
    rows = _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), _CSV, datasource="trino")
    assert rows == 2 and _csv_lines(out) == ["1,2026-01-01", "2,2026-01-02"]


def test_custom_api_returns_columns_rows_dict(monkeypatch, tmp_path):
    def fetch(sql, *, config):
        return {"columns": ["a", "b"], "rows": [[1, 2]]}

    out = tmp_path / "t.csv"
    assert _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), _CSV, datasource="trino") == 1
    assert _csv_lines(out) == ["1,2"]


def test_custom_api_returns_data_key_dict(monkeypatch, tmp_path):
    """rows 대신 data 키를 쓰는 응답도 받는다."""
    def fetch(sql, *, config):
        return {"columns": ["a"], "data": [[7]]}

    out = tmp_path / "t.csv"
    assert _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), _CSV, datasource="trino") == 1
    assert _csv_lines(out) == ["7"]


def test_custom_api_streams_chunks(monkeypatch, tmp_path):
    """청크를 yield 하면 전량을 메모리에 올리지 않는다(대용량 권장형)."""
    produced = []

    def fetch(sql, *, config):
        def gen():
            for start in range(0, 10, 2):
                produced.append(start)
                yield (["a"], [(i,) for i in range(start, start + 2)])
        return gen()

    out = tmp_path / "t.csv"
    rows = _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), _CSV, datasource="trino")
    assert rows == 10 and _csv_lines(out) == [str(i) for i in range(10)]
    assert produced == [0, 2, 4, 6, 8]   # 순차 소비(한 번에 다 만들어지지 않음)


def test_custom_api_chunks_of_dataframes(monkeypatch, tmp_path):
    def fetch(sql, *, config):
        yield _FakeDF(["a"], [[1], [2]])
        yield _FakeDF(["a"], [[3]])

    out = tmp_path / "t.csv"
    assert _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), _CSV, datasource="trino") == 3
    assert _csv_lines(out) == ["1", "2", "3"]


def test_custom_api_empty_result(monkeypatch, tmp_path):
    out = tmp_path / "t.csv"
    rows = _backend(lambda sql, *, config: (["a"], []), monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), _CSV, datasource="trino")
    assert rows == 0 and _csv_lines(out) == []


def test_custom_api_unparseable_return_fails_clearly(monkeypatch, tmp_path):
    with pytest.raises(ValueError) as ei:
        _backend(lambda sql, *, config: 42, monkeypatch).export_to_local_csv(
            "SELECT 1", str(tmp_path / "x.csv"), _CSV, datasource="trino")
    assert "해석할 수 없습니다" in str(ei.value)


# ───────────────────── 결측값 정규화(CSV NULL) ─────────────────────


def test_missing_values_become_csv_null_marker(monkeypatch, tmp_path):
    """NaN/None 이 문자열 'nan' 으로 새면 외부테이블이 NULL 로 읽지 못한다."""
    def fetch(sql, *, config):
        return (["a", "b"], [(1, float("nan")), (None, "x")])

    out = tmp_path / "t.csv"
    _backend(fetch, monkeypatch).export_to_local_csv(
        "SELECT 1", str(out), {"delimiter": ",", "null": "\\N", "quote": '"'},
        datasource="trino")
    assert _csv_lines(out) == ["1,\\N", "\\N,x"]


# ───────────────────── 커서 경로 보존(회귀 방지) ─────────────────────


@pytest.mark.parametrize("ds", [None, "", "impala", "IMPALA", "source"])
def test_builtin_names_never_take_custom_path(monkeypatch, ds, tmp_path):
    """빈 값/impala/source 는 커스텀 함수를 타지 않고 기존 Impala 커서 경로로 간다.

    impyla 가 설치돼 있지 않으므로 import 단계 실패가 곧 "Impala 경로로 갔다"는 증거다.
    """
    called = []
    be = _backend(lambda sql, *, config: called.append(sql), monkeypatch)
    with pytest.raises(Exception) as ei:
        be.export_to_local_csv("SELECT 1", str(tmp_path / "x.csv"), _CSV, datasource=ds)
    assert called == []
    assert "fetch_module" not in str(ei.value)


def test_impala_query_options_not_passed_to_custom_api(monkeypatch, tmp_path):
    """configuration= 은 impyla 전용이라 커스텀 API 커서에 넘어가면 안 된다."""
    got = {}

    def fetch(sql, *, config):
        got["config"] = config
        return (["a"], [(1,)])

    be = _backend(fetch, monkeypatch)
    # 전역(MEM_LIMIT)·요청별 옵션이 있어도 커스텀 경로는 정상 동작해야 한다.
    be.export_to_local_csv("SELECT 1", str(tmp_path / "t.csv"), _CSV,
                           query_options={"REQUEST_POOL": "etl"}, datasource="trino")
    # 커스텀 함수는 접속 설정만 받는다(Impala 옵션은 전달되지 않는다).
    assert got["config"] == {"token": "t"}


# ───────────────────── 미설정 시 명확한 실패 ─────────────────────


def test_unconfigured_custom_source_fails_loudly(tmp_path):
    """fetch_module 미설정이면 Impala 로 조용히 폴백하지 않고 명확히 실패한다."""
    be = ImpalaToGreenplumBackend(
        impala_dsn={"host": "impala.example"}, greenplum_dsn="", source_fetch_module="")
    with pytest.raises(ValueError) as ei:
        be.export_to_local_csv("SELECT 1", str(tmp_path / "x.csv"), _CSV, datasource="trino")
    msg = str(ei.value)
    assert "query.func.fetch_module" in msg and "trino" in msg


# ───────────────── coordinator → task 본문 → backend 배선 ─────────────────


def test_task_payload_carries_datasource(store):
    """/jobs 의 datasource 가 executor 로 가는 task 본문에 실린다(HTTP 디스패치)."""
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
    asyncio.run(disp._start_task(_FakeClient(), job, task, "http://exec1:8001"))
    assert sent and sent[0]["datasource"] == "trino"


def test_executor_task_request_accepts_datasource():
    """POST /tasks 가 datasource 를 받고, 없으면 None(구버전 coordinator 호환)."""
    from executor.models import CreateTaskRequest

    req = CreateTaskRequest(task_id="t1", job_id="j1", sub_query="SELECT 1",
                            target_table="public.t", partition_column="dt",
                            datasource="trino")
    assert req.datasource == "trino"
    old = CreateTaskRequest(task_id="t2", job_id="j1", sub_query="SELECT 1",
                            target_table="public.t", partition_column="dt")
    assert old.datasource is None


# ───────────────── manifest datasource → job (s3_stage e2e) ─────────────────


def _ds_client(runner, store, monkeypatch, tmp_path, name, datasource):
    """datasource 만 다른 최소 이관 템플릿 하나를 가진 coordinator 클라이언트."""
    from fastapi.testclient import TestClient

    from coordinator.app import create_app
    from coordinator.config import settings

    d = tmp_path / name
    d.mkdir()
    (d / "manifest.yml").write_text(
        f"id: {name}\nexec_mode: copy\npartition_column: dt\ntarget_table: public.t\n"
        f"datasource: {datasource}\nfiles: {{select: s.j2}}\n", encoding="utf-8")
    (d / "s.j2").write_text("SELECT a, dt FROM t WHERE dt IN ('2026-01-01')\n", encoding="utf-8")
    monkeypatch.setattr(settings, "template_enabled", True, raising=False)
    monkeypatch.setattr(settings, "template_dir", str(tmp_path), raising=False)
    monkeypatch.setattr(settings, "template_auto_reload", False, raising=False)
    monkeypatch.setattr(settings, "template_func_modules", [], raising=False)
    return TestClient(create_app(runner=runner, store=store))


def test_manifest_datasource_flows_into_job(runner, store, monkeypatch, tmp_path):
    client = _ds_client(runner, store, monkeypatch, tmp_path, "trino_src", "trino")
    resp = client.post("/jobs", json={"template_id": "trino_src", "params": {}})
    assert resp.status_code == 202, resp.text
    assert runner.runs[0].datasource == "trino"


def test_request_datasource_beats_manifest(runner, store, monkeypatch, tmp_path):
    client = _ds_client(runner, store, monkeypatch, tmp_path, "trino_src2", "trino")
    resp = client.post("/jobs", json={
        "template_id": "trino_src2", "params": {}, "datasource": "impala"})
    assert resp.status_code == 202, resp.text
    assert runner.runs[0].datasource == "impala"


def test_default_datasource_is_source_type(runner, store, monkeypatch, tmp_path):
    """아무도 지정하지 않으면 서버 source.type(기본 impala)을 따른다."""
    import core.config as core_config

    monkeypatch.setattr(core_config.settings, "source_type", "impala", raising=False)
    client = _ds_client(runner, store, monkeypatch, tmp_path, "plain_src", "impala")
    assert client.post("/jobs", json={"template_id": "plain_src", "params": {}}).status_code == 202
    assert runner.runs[0].datasource == "impala"


async def test_s3_stage_job_with_custom_source_end_to_end(monkeypatch):
    """s3_stage 이관을 datasource=trino 로 제출하면 Phase 1 export 에 그 값이 전달된다.

    사용자가 원하는 조합(SELECT 는 커스텀 API, 적재는 Greenplum, 모드는 s3_stage)이
    coordinator→dispatcher→backend 까지 끊기지 않고 이어지는지 확인한다.
    """
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
        resp = await c.post("/jobs", json={**_job_payload(parallelism=2), "datasource": "trino"})
        assert resp.status_code == 202, resp.text
        job_id = resp.json()["job_id"]
        for _ in range(300):
            st = (await c.get(f"/jobs/{job_id}/status")).json()
            if st["status"] in ("DONE", "PARTIAL", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.01)
    assert st["status"] == "DONE", st
    assert backend.seen == ["trino", "trino"]
    # Phase 2(GP PXF 외부테이블→target INSERT)는 소스와 무관하게 그대로 수행됐다.
    assert backend.phase2 is not None and backend.cleaned


async def test_s3_stage_without_datasource_keeps_impala_call_signature(monkeypatch):
    """datasource 미지정이면 backend 호출에 인자를 아예 붙이지 않는다(기존 동작 그대로).

    _MockS3StageBackend 는 datasource kwarg 를 **모르는** 구버전 시그니처라, 붙여 보내면
    TypeError 로 실패한다 — 통과 자체가 영향 차단의 증거다.
    """
    from coordinator.app import create_app
    from coordinator.config import settings as coord_settings
    from coordinator.dispatcher import LocalDispatcher
    from coordinator.job_store import JobStore
    from tests.test_s3_stage import _MockS3StageBackend, _job_payload, _s3_settings

    _s3_settings(monkeypatch)
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
