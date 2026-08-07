"""실행 SQL 로깅(core.sqllog) 검증.

확인하는 것은 네 가지다.
  1. 순수 포매터(collapse_sql/format_sql/format_params) — 한 줄 접기·절단·마스킹.
  2. log_sql 이 남기는 한 줄에 **datasource 가 반드시 들어간다**.
  3. 백엔드 실행 경로(소스 SELECT / GP 적재)가 실제로 로그를 남긴다 —
     Impala 커서든 커스텀 API 커서든 각자의 엔진 이름으로.
  4. 로그 레코드에 **job_id/task_id 가 붙는다**(스레드로 넘어가도 유지).
"""
import asyncio
import logging

from core.logging import job_log_context, job_id_var, task_id_var, with_log_context
from core.sqllog import collapse_sql, format_params, format_sql, log_sql, datasource_of


class _Cfg:
    """log_sql 이 읽는 설정만 갖춘 최소 더블."""

    def __init__(self, enabled=True, max_length=4000, params=True):
        self.log_sql_enabled = enabled
        self.log_sql_max_length = max_length
        self.log_sql_params = params


# ───────────────────────── 순수 포매터 ─────────────────────────

def test_collapse_sql_한줄로_접는다():
    assert collapse_sql("SELECT 1\n  FROM   t\n WHERE x=1") == "SELECT 1 FROM t WHERE x=1"


def test_collapse_sql_빈값():
    assert collapse_sql(None) == ""
    assert collapse_sql("   \n ") == ""


def test_format_sql_절단하고_생략_사실을_표기한다():
    out = format_sql("A" * 100, 10)
    assert out.startswith("A" * 10)
    # 원문이 더 길었다는 사실이 로그에 남아야 한다(재실행 가능한 전문으로 오해 금지).
    assert "총 100자 중 90자 절단" in out


def test_format_sql_상한이_0이면_절단하지_않는다():
    assert format_sql("A" * 100, 0) == "A" * 100


def test_format_sql_이_비밀값을_마스킹한다():
    out = format_sql("COPY t FROM 'postgresql://u:secret@h/db'", 4000)
    assert "secret" not in out
    assert "***" in out


def test_format_params():
    assert format_params(None, 100) == ""
    assert format_params(["2026-01-01", "2026-01-02"], 100) == "['2026-01-01', '2026-01-02']"


# ───────────────────────── log_sql 출력 ─────────────────────────

def test_log_sql_이_datasource_와_sql_을_남긴다(caplog):
    with caplog.at_level(logging.INFO, logger="core.sql"):
        log_sql("trino", "SELECT 1", phase="SOURCE_SELECT", settings=_Cfg())
    msg = caplog.records[-1].getMessage()
    assert "datasource=trino" in msg
    assert "phase=SOURCE_SELECT" in msg
    assert "SELECT 1" in msg


def test_log_sql_datasource_는_소문자로_정규화되고_빈값은_unknown(caplog):
    with caplog.at_level(logging.INFO, logger="core.sql"):
        log_sql("TRINO", "SELECT 1", settings=_Cfg())
        log_sql("", "SELECT 2", settings=_Cfg())
    assert "datasource=trino" in caplog.records[-2].getMessage()
    assert "datasource=unknown" in caplog.records[-1].getMessage()


def test_log_sql_params_는_설정으로_끌_수_있다(caplog):
    with caplog.at_level(logging.INFO, logger="core.sql"):
        log_sql("greenplum", "DELETE FROM t WHERE d IN (%s)", params=["d1"],
                settings=_Cfg(params=True))
        log_sql("greenplum", "DELETE FROM t WHERE d IN (%s)", params=["d1"],
                settings=_Cfg(params=False))
    assert "params=" in caplog.records[-2].getMessage()
    assert "params=" not in caplog.records[-1].getMessage()


def test_log_sql_enabled_false_면_아무것도_남기지_않는다(caplog):
    with caplog.at_level(logging.INFO, logger="core.sql"):
        log_sql("impala", "SELECT 1", settings=_Cfg(enabled=False))
    assert not [r for r in caplog.records if r.name == "core.sql"]


def test_log_sql_은_예외를_밖으로_올리지_않는다(caplog):
    class _Boom:
        log_sql_enabled = True

        @property
        def log_sql_max_length(self):
            raise RuntimeError("설정 읽기 실패")

    # 로깅 실패가 적재를 깨뜨리면 안 된다.
    log_sql("impala", "SELECT 1", settings=_Boom())


def test_datasource_of():
    class _Custom:
        _name = "Trino"

    assert datasource_of(_Custom()) == "trino"
    assert datasource_of(object()) == "impala"
    assert datasource_of(object(), default="source") == "source"


# ───────────────── job_id/task_id 주입 ─────────────────

def test_실행_sql_레코드에_job_task_id_가_주입된다(caplog):
    """setup_logging 이 설치하는 record factory 와 같은 방식으로 주입되는지 확인한다.

    포맷 문자열이 ``[%(job_id)s][%(task_id)s]`` 를 참조하므로, 레코드에 두 속성이
    실제로 붙는지가 핵심이다(안 붙으면 포맷 시점에 KeyError 로 로그가 깨진다).
    """
    base = logging.getLogRecordFactory()

    def _factory(*args, **kwargs):
        record = base(*args, **kwargs)
        record.job_id = job_id_var.get()
        record.task_id = task_id_var.get()
        return record

    logging.setLogRecordFactory(_factory)
    try:
        with caplog.at_level(logging.INFO, logger="core.sql"):
            with job_log_context("job_abc", "task_3"):
                log_sql("impala", "SELECT 1", settings=_Cfg())
        rec = [r for r in caplog.records if r.name == "core.sql"][-1]
        assert rec.job_id == "job_abc"
        assert rec.task_id == "task_3"
        # 실제 로그 파일 포맷으로 렌더해도 식별자가 그대로 나온다.
        rendered = logging.Formatter("[%(job_id)s][%(task_id)s] %(message)s").format(rec)
        assert rendered.startswith("[job_abc][task_3] SQL 실행 datasource=impala")
    finally:
        logging.setLogRecordFactory(base)


def test_job_log_context_안에서_실행_sql_로그에_식별자가_찍힌다():
    """레코드 factory 는 setup_logging 이 설치하므로, 여기서는 ContextVar 자체를 검증한다."""
    with job_log_context("job_1", "task_1"):
        assert job_id_var.get() == "job_1"
        assert task_id_var.get() == "task_1"
    # 블록을 벗어나면 기본값으로 복원된다.
    assert job_id_var.get() == "-"
    assert task_id_var.get() == "-"


def test_with_log_context_는_다른_스레드에서도_식별자를_유지한다():
    """run_in_executor 는 contextvars 를 복사하지 않으므로 이 헬퍼가 필요하다."""
    seen = {}

    def _work():
        seen["job"] = job_id_var.get()
        seen["task"] = task_id_var.get()

    async def _main():
        loop = asyncio.get_running_loop()
        with job_log_context("job_9", "task_9"):
            # 감싸지 않고 넘기면 워커 스레드는 기본 컨텍스트로 시작한다.
            await loop.run_in_executor(None, _work)
            bare = dict(seen)
            await loop.run_in_executor(None, with_log_context(_work))
            return bare, dict(seen)

    bare, wrapped = asyncio.run(_main())
    assert bare == {"job": "-", "task": "-"}
    assert wrapped == {"job": "job_9", "task": "task_9"}


def test_with_log_context_는_인자와_반환값을_그대로_전달한다():
    fn = with_log_context(lambda a, b=0: a + b, 3, b=4)
    assert fn() == 7


# ───────────────── 백엔드 실행 경로가 실제로 로그를 남기는가 ─────────────────

def _backend():
    from executor.backend import ImpalaToGreenplumBackend

    return ImpalaToGreenplumBackend(
        impala_dsn={"host": "h"}, greenplum_dsn="postgresql://x", batch_size=2,
    )


class _FakeCursor:
    """impyla 커서 흉내(로그 검증용 — description/fetchmany 만 필요)."""

    def __init__(self):
        self.executed = []
        self.description = [("a", None, None, None, None, None, None)]

    def execute(self, sql, *a, **k):
        self.executed.append(sql)

    def fetchmany(self, n=None):
        return []

    def close(self):
        pass


def test_소스_select_는_impala_로_기록된다(caplog):
    be = _backend()
    cur = _FakeCursor()
    with caplog.at_level(logging.INFO, logger="core.sql"):
        be._source_execute(cur, "SELECT * FROM src", None)
    msg = caplog.records[-1].getMessage()
    assert "datasource=impala" in msg
    assert "phase=SOURCE_SELECT" in msg
    assert "SELECT * FROM src" in msg


def test_커스텀_소스_select_는_그_엔진_이름으로_기록된다(caplog):
    """커서에서 datasource 를 추론하므로 _source_execute 시그니처를 바꾸지 않아도 된다."""
    from executor.backend import _FunctionCursor

    be = _backend()
    cur = _FunctionCursor(lambda sql, config=None: ([], []), {}, 10, "trino")
    with caplog.at_level(logging.INFO, logger="core.sql"):
        be._source_execute(cur, "SELECT * FROM hive.default.src", None)
    msg = caplog.records[-1].getMessage()
    assert "datasource=trino" in msg
    assert "SELECT * FROM hive.default.src" in msg


class _FakeGpCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 7

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeGpConn:
    def __init__(self, cur):
        self._cur = cur
        self.committed = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True


class _FakeGpPool:
    def __init__(self, conn):
        self._conn = conn

    def connection(self):
        from contextlib import contextmanager

        @contextmanager
        def _cm():
            yield self._conn

        return _cm()


def test_s3_phase2_의_모든_gp_sql_이_greenplum_으로_기록된다(caplog):
    be = _backend()
    cur = _FakeGpCursor()
    be._gp_pool = _FakeGpPool(_FakeGpConn(cur))
    with caplog.at_level(logging.INFO, logger="core.sql"):
        be.load_external_s3(
            "CREATE EXTERNAL TABLE s3ext_j1 (a int)",
            "DELETE FROM public.t WHERE dt IN ('d1')",
            "INSERT INTO public.t SELECT * FROM s3ext_j1",
            ["DROP EXTERNAL TABLE IF EXISTS s3ext_j1"],
        )
    msgs = [r.getMessage() for r in caplog.records if r.name == "core.sql"]
    # 실행한 SQL 4건이 모두 남고, 전부 greenplum 으로 표기된다.
    assert all("datasource=greenplum" in m for m in msgs)
    joined = " || ".join(msgs)
    for phase in ("S3_EXTERNAL_DDL", "DELETE", "INSERT", "CLEANUP"):
        assert f"phase={phase}" in joined
    for sql in ("CREATE EXTERNAL TABLE s3ext_j1", "DELETE FROM public.t",
                "INSERT INTO public.t", "DROP EXTERNAL TABLE IF EXISTS s3ext_j1"):
        assert sql in joined
    # 로그가 실행을 대체하지 않는다 — 실제로도 4건이 실행됐다.
    assert len(cur.executed) == 4


def test_local_stage_phase2_의_gp_sql_도_모두_기록된다(caplog):
    be = _backend()
    cur = _FakeGpCursor()
    be._gp_pool = _FakeGpPool(_FakeGpConn(cur))
    with caplog.at_level(logging.INFO, logger="core.sql"):
        be.load_external_csv(
            "CREATE EXTERNAL TABLE ext (a int)",
            "CREATE TABLE stg (a int)",
            "INSERT INTO stg SELECT * FROM ext",
            None,
            "INSERT INTO public.t SELECT * FROM stg",
            ["DROP EXTERNAL TABLE IF EXISTS ext"],
        )
    joined = " || ".join(r.getMessage() for r in caplog.records if r.name == "core.sql")
    for phase in ("STAGING_DDL", "PXF_EXTERNAL_DDL", "STAGE_LOAD", "INSERT", "CLEANUP"):
        assert f"phase={phase}" in joined
    assert "datasource=impala" not in joined  # 순수 GP 작업이라 소스는 등장하지 않는다


def test_statement_모드_실행도_기록된다(caplog):
    be = _backend()
    cur = _FakeGpCursor()
    be._gp_pool = _FakeGpPool(_FakeGpConn(cur))
    with caplog.at_level(logging.INFO, logger="core.sql"):
        be.execute("INSERT INTO public.t SELECT 1")
    msg = [r.getMessage() for r in caplog.records if r.name == "core.sql"][-1]
    assert "datasource=greenplum" in msg and "phase=STATEMENT" in msg
