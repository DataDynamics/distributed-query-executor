"""trino 셸(tools.trino_query)이 커스텀 실행 함수를 DB-API 커서처럼 감싸 쓰는지 검증한다."""

from __future__ import annotations

import sys
import types

import pytest

from core.dbprobe import QueryResult
from tools import trino_query as tq

CALLS: list = []


def fake_run(sql, *, config, limit):
    CALLS.append((sql, dict(config), limit))
    if sql.upper().startswith("SHOW TABLES"):
        return QueryResult(columns=["Table"], rows=[["orders"], ["sales"]], row_count=2, truncated=False,
                           elapsed_ms=1)
    if sql.upper().startswith("CREATE"):
        return QueryResult(columns=[], rows=[], row_count=0, truncated=False, elapsed_ms=1)
    rows = [[i, f"v{i}"] for i in range(5)]
    return QueryResult(columns=["id", "name"], rows=rows[:limit], row_count=min(5, limit),
                       truncated=limit < 5, elapsed_ms=1)


@pytest.fixture()
def fake_module(monkeypatch):
    mod = types.ModuleType("fake_trino_runner")
    mod.run = fake_run
    monkeypatch.setitem(sys.modules, "fake_trino_runner", mod)
    CALLS.clear()
    return "fake_trino_runner:run"


def test_커서_어댑터는_결과를_나눠_준다():
    conn = tq.FunctionConnection(fake_run, {"catalog": "hive", "schema": "s"}, limit=3)
    cur = conn.cursor()
    cur.execute("SELECT * FROM t")
    assert [d[0] for d in cur.description] == ["id", "name"]
    assert cur.fetchmany(2) == [[0, "v0"], [1, "v1"]] and cur.fetchall() == [[2, "v2"]]
    assert cur.truncated and conn.last_truncated
    cur.execute("CREATE TABLE x (a int)")
    assert cur.description is None


def test_USE_는_설정을_바꾸고_SET_SESSION_은_거절한다():
    seen = []
    CALLS.clear()
    conn = tq.FunctionConnection(fake_run, {"catalog": "hive", "schema": "s"}, 10, on_use=lambda c: seen.append(1))
    cur = conn.cursor()
    cur.execute("USE sales")
    assert conn.config == {"catalog": "hive", "schema": "sales"} and seen
    cur.execute('use "iceberg".raw;')
    assert conn.config["catalog"] == "iceberg" and conn.config["schema"] == "raw"
    with pytest.raises(ValueError, match="SET SESSION"):
        cur.execute("SET SESSION query_max_run_time = '1h'")
    assert CALLS == []  # 세션 명령은 실행 함수까지 가지 않는다


def test_명령행_값이_설정을_덮는다():
    parser = tq.build_parser()
    args = parser.parse_args(["--catalog", "c2", "--port", "9443", "--set", "verify=false", "-q", "x"])
    cfg = tq.build_config(args, {"config": {"host": "h", "catalog": "c1", "port": "8080"}})
    assert cfg == {"host": "h", "catalog": "c2", "port": "9443", "verify": "false"}
    bad = parser.parse_args(["--set", "novalue", "-q", "x"])
    with pytest.raises(SystemExit):
        tq.build_config(bad, {})


def test_엔진의_메타_명령과_프롬프트(fake_module):
    args = tq.build_parser().parse_args(["--max-rows", "0", "-q", "x"])
    cfg = {"catalog": "hive", "schema": "s"}
    engine = tq.make_engine(args, fake_run, cfg)
    assert engine.label == "hive.s"
    assert engine.list_tables_sql("ord%") == "SHOW TABLES LIKE 'ord%'"
    assert engine.ddl_sql("t") == "SHOW CREATE TABLE t"
    conn = engine.connect()
    assert conn.limit == tq.UNLIMITED  # --max-rows 0 은 제한 없음
    conn.cursor().execute("USE other")
    assert engine.label == "hive.other"


def test_배치_실행은_CSV_로_쓰고_잘림을_알린다(fake_module, tmp_path, capsys):
    out = tmp_path / "o.csv"
    code = tq.main(["--no-config", "--module", fake_module, "-q", "SELECT 1", "-o", str(out),
                    "--max-rows", "3", "--no-progress"])
    assert code == 0
    assert out.read_text().splitlines() == ["id,name", "0,v0", "1,v1", "2,v2"]
    err = capsys.readouterr().err
    assert "잘랐습니다" in err and CALLS[-1][2] == 3


def test_표_출력(fake_module, capsys):
    assert tq.main(["--no-config", "--module", fake_module, "-q", "SELECT 1", "--no-progress"]) == 0
    assert "v4" in capsys.readouterr().out


def test_없는_실행_함수는_알기_쉽게_실패한다():
    with pytest.raises(SystemExit, match="불러올 수 없습니다"):
        tq.load_function("no_such_mod_xyz:run")
    with pytest.raises(SystemExit, match="module:func"):
        tq.load_function("run")
