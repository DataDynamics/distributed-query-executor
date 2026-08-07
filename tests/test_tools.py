"""운영자용 CLI 도구(`bin/gp-shell`·`bin/impala-shell`·`bin/s3-ops`) 검증.

실제 DB 나 S3 없이 돌아가는 부분만 본다. 접속이 필요한 코드는 이 저장소의 다른
테스트와 마찬가지로 다루지 않고, 대신 다음 네 가지를 확인한다.

  1. 설정 어댑터가 이 저장소의 config.properties 를 도구가 쓰는 섹션 모양으로 옮기는가.
  2. Greenplum DSN 한 줄을 host·port·user 등으로 정확히 푸는가(비밀번호 인코딩 포함).
  3. 표·CSV 출력과 SQL 문장 분리 같은 순수 함수가 기대대로 동작하는가.
  4. bin/ 래퍼가 세 도구를 실제로 띄우는가(--help 가 뜨는지).
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tools import appconfig, progress, shell, sqlfile, table
from tools import s3_ops

REPO = Path(__file__).resolve().parent.parent


# ───────────────────────── 설정 어댑터 ─────────────────────────

def test_parse_dsn_기본형():
    got = appconfig.parse_dsn("postgresql://etl:pw@gp-host:5433/dw")
    assert got == {
        "host": "gp-host", "port": 5433, "user": "etl",
        "password": "pw", "database": "dw",
    }


def test_parse_dsn_은_퍼센트_인코딩을_푼다():
    """비밀번호에 @ 나 : 가 있으면 DSN 에 인코딩돼 들어온다. 그대로 쓰면 접속이 실패한다."""
    got = appconfig.parse_dsn("postgresql://u%40corp:se%40cr%3At@h:5432/db")
    assert got["user"] == "u@corp"
    assert got["password"] == "se@cr:t"


def test_parse_dsn_은_sslmode_와_타임아웃만_읽는다():
    got = appconfig.parse_dsn(
        "postgresql://u@h/db?sslmode=require&connect_timeout=10&application_name=x"
    )
    assert got["sslmode"] == "require"
    assert got["connect_timeout"] == "10"
    assert "application_name" not in got


def test_parse_dsn_은_빈값이나_비URL_을_무시한다():
    assert appconfig.parse_dsn("") == {}
    assert appconfig.parse_dsn("host=gp port=5432") == {}


def _write_config(tmp_path, **overrides):
    """저장소 기본 설정을 복사한 뒤 몇 줄만 바꾼 임시 설정 디렉터리를 만든다."""
    cfg = tmp_path / "config"
    cfg.mkdir()
    for name in ("config.yml", "config.properties"):
        (cfg / name).write_text(
            (REPO / "config" / name).read_text(encoding="utf-8"), encoding="utf-8"
        )
    props = (cfg / "config.properties").read_text(encoding="utf-8")
    for key, value in overrides.items():
        key = key.replace("__", ".")
        # 주석 처리돼 있든 빈 값이든 같은 줄을 찾아 값을 채운다.
        props = props.replace(f"#{key}=", f"{key}=").replace(
            f"\n{key}=\n", f"\n{key}={value}\n"
        )
        if f"{key}={value}" not in props:
            props += f"\n{key}={value}\n"
    (cfg / "config.properties").write_text(props, encoding="utf-8")
    return cfg


def test_load_section_이_저장소_설정을_도구_섹션으로_옮긴다(tmp_path):
    cfg = _write_config(
        tmp_path,
        greenplum__dsn="postgresql://etl:pw@gp.example.com:5432/dw",
        impala__host="impala.example.com",
        s3__bucket="dw-stage",
    )
    args = argparse.Namespace(config_dir=str(cfg), no_config=False)
    path = appconfig.resolve_config_path(args)

    gp = appconfig.load_section(path, "greenplum", ("host", "port", "database", "user"))
    assert gp == {"host": "gp.example.com", "port": 5432, "database": "dw", "user": "etl"}

    im = appconfig.load_section(path, "impala", ("host", "port", "database"))
    assert im["host"] == "impala.example.com"
    assert im["port"] == 21050  # config.yml 의 기본값

    s3 = appconfig.load_section(path, "s3", ("bucket",))
    assert s3["bucket"] == "dw-stage"


def test_no_config_면_전부_None_이다(tmp_path):
    args = argparse.Namespace(config_dir=str(tmp_path), no_config=True)
    assert appconfig.resolve_config_path(args) is None
    got = appconfig.load_section(None, "greenplum", ("host", "user"))
    assert got == {"host": None, "user": None}


def test_config_dir_에_config_yml_이_없으면_오류다(tmp_path):
    """오타로 엉뚱한 디렉터리를 가리켰을 때 조용히 기본값으로 도는 것을 막는다."""
    args = argparse.Namespace(config_dir=str(tmp_path), no_config=False)
    with pytest.raises(SystemExit):
        appconfig.resolve_config_path(args)


def test_pick_은_명령행_설정_기본값_순이다():
    assert appconfig.pick("cli", "config", "default") == "cli"
    assert appconfig.pick(None, "config", "default") == "config"
    assert appconfig.pick(None, None, "default") == "default"


# ───────────────────────── 표·CSV 출력 ─────────────────────────

def test_render_는_한글_폭에_맞춰_열을_맞춘다():
    """한글은 터미널에서 두 칸을 차지한다. 문자 수로 채우면 열이 어긋난다."""
    out = table.render(["이름", "n"], [["가나다", 1], ["ab", 22]])
    lines = out.splitlines()
    # 구분자 '|' 가 모든 줄에서 같은 표시 위치에 와야 열이 맞은 것이다.
    positions = {progress.display_width(line.split("|")[0]) for line in lines if "|" in line}
    assert len(positions) == 1, out
    # '가나다'(6칸)가 가장 넓으므로 첫 열 폭이 6이고 뒤에 공백 하나가 붙는다.
    assert positions == {7}


def test_format_result_는_잘림을_숨기지_않는다():
    body, note = table.format_result(["a"], [[1], [2]], truncated=True)
    assert "2행 이상" in note and "--max-rows 0" in note
    body, note = table.format_result(["a"], [[1], [2]], truncated=False)
    assert note == "2행"


def test_format_result_는_빈_결과도_컬럼을_보여준다():
    body, note = table.format_result(["a", "b"], [], truncated=False)
    assert "a | b" in body and "(0행)" in body
    assert note == "0행"


class _FakeCursor:
    """fetchmany 만 흉내 내는 커서. 표 출력이 필요한 만큼만 받는지 본다."""

    def __init__(self, total):
        self._rows = [[i] for i in range(total)]
        self.fetched = 0

    def fetchmany(self, size):
        batch = self._rows[self.fetched : self.fetched + size]
        self.fetched += len(batch)
        return batch


def test_fetch_limited_는_보여줄_만큼만_받는다():
    """100행을 보려고 100만 행을 받지 않는다. 한 행 더 받아 잘림만 판정한다."""
    cur = _FakeCursor(1000)
    rows, truncated = table.fetch_limited(cur, max_rows=10, batch_size=4)
    assert len(rows) == 10 and truncated is True
    assert cur.fetched == 11  # 잘림 판정용 한 행만 더 받았다


def test_fetch_limited_는_0이면_끝까지_받는다():
    cur = _FakeCursor(25)
    rows, truncated = table.fetch_limited(cur, max_rows=0, batch_size=10)
    assert len(rows) == 25 and truncated is False


def test_write_csv_는_NULL_을_지정_문자열로_쓴다(tmp_path):
    out = tmp_path / "a.csv"
    with table.open_output(str(out), use_gzip=False, encoding="utf-8") as fp:
        table.write_csv(fp, ["a", "b"], [[1, None]], delimiter="`", null_string="")
    assert out.read_text(encoding="utf-8").splitlines() == ["a`b", "1`"]


# ───────────────────────── SQL 문장 분리 ─────────────────────────

def test_split_statements_는_세미콜론으로_자른다():
    done, rest = shell.split_statements("SELECT 1; SELECT 2; SELECT 3")
    assert done == ["SELECT 1", "SELECT 2"]
    assert rest.strip() == "SELECT 3"


def test_split_statements_는_문자열_안의_세미콜론을_자르지_않는다():
    done, rest = shell.split_statements("SELECT ';' AS x; SELECT 2;")
    assert done == ["SELECT ';' AS x", "SELECT 2"]


def test_is_complete():
    assert shell.is_complete("SELECT 1;")
    assert not shell.is_complete("SELECT 1")


# ───────────────────────── SQL 템플릿 ─────────────────────────

def test_parse_variables():
    assert sqlfile.parse_variables(["dt=2026-08-01", "n=3"]) == {"dt": "2026-08-01", "n": "3"}
    with pytest.raises(SystemExit):
        sqlfile.parse_variables(["dt"])


def test_render_query_는_정의되지_않은_변수를_오류로_본다():
    """빈 문자열로 조용히 치환되면 WHERE dt = '' 가 0건을 돌려주는 사고가 난다."""
    with pytest.raises(SystemExit):
        sqlfile.render_query("SELECT * FROM t WHERE dt = '{{ dt }}'", {}, "--query")


def test_render_query_는_변수를_채운다():
    got = sqlfile.render_query(
        "SELECT * FROM t WHERE dt = '{{ dt }}'", {"dt": "2026-08-01"}, "--query"
    )
    assert got == "SELECT * FROM t WHERE dt = '2026-08-01'"


# ───────────────────────── S3 경로·기간 파싱 ─────────────────────────

def test_parse_s3_uri():
    assert s3_ops.parse_s3_uri("s3://dw-stage/orders/a.csv") == ("dw-stage", "orders/a.csv")
    assert s3_ops.parse_s3_uri("s3://dw-stage") == ("dw-stage", "")


def test_resolve_target_은_스킴이_없으면_기본_버킷을_쓴다():
    assert s3_ops.resolve_target("orders/a.csv", "dw-stage") == ("dw-stage", "orders/a.csv")
    assert s3_ops.resolve_target("s3://other/a.csv", "dw-stage") == ("other", "a.csv")


def test_resolve_target_은_기본_버킷도_없으면_오류다():
    with pytest.raises(SystemExit):
        s3_ops.resolve_target("orders/a.csv", None)


def test_parse_duration():
    assert s3_ops.parse_duration("7d") == 7 * 86400
    assert s3_ops.parse_duration("30m") == 30 * 60
    with pytest.raises(SystemExit):
        s3_ops.parse_duration("일주일")


def test_parse_size():
    assert s3_ops.parse_size("64MB") == 64 * 1024 * 1024
    assert s3_ops.parse_size("8") == 8


# ───────────────────────── 진행 표시·구간 시간 ─────────────────────────

def test_human_bytes_와_human_seconds():
    assert progress.human_bytes(1536) == "1.5KB"
    assert progress.human_seconds(30) == "30.0초"
    assert progress.human_seconds(3661) == "1시간 1분 1초"


def test_phase_timer_는_구간을_누적한다():
    timer = progress.PhaseTimer(("읽기", "쓰기"))
    with timer.measure("읽기"):
        pass
    report = timer.report()
    assert "구간별 소요 시간" in report and "읽기" in report
    # 한 번도 재지 않은 구간은 보고서에 넣지 않는다.
    assert "쓰기" not in report


# ───────────────────────── bin/ 래퍼 ─────────────────────────

@pytest.mark.parametrize("name", ["gp-shell", "impala-shell", "s3-ops"])
def test_bin_래퍼가_도구를_띄운다(name):
    """PYTHONPATH·모듈 경로가 맞는지 --help 로 확인한다(접속은 하지 않는다)."""
    env = dict(os.environ, PYTHON=sys.executable, QUERY_EXECUTOR_CONFIG_DIR=str(REPO / "config"))
    proc = subprocess.run(
        [str(REPO / "bin" / name), "--help"],
        capture_output=True, text=True, env=env, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert f"bin/{name}" in proc.stdout
