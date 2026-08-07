"""Greenplum에 SQL을 실행한다. 결과가 있으면 표나 CSV로 보여준다.

접속 정보는 이 저장소의 config/config.properties(greenplum.dsn)에서 자동으로 읽는다. 명령행으로 준
값이 항상 우선하므로, 설정을 채워두면 쿼리만 주면 된다.

    pip install 'psycopg[binary]' PyYAML

    # sql/ 의 템플릿에 변수를 채워 실행
    bin/gp-shell -f order_summary.sql --var dt=2026-08-01

    # 결과를 CSV로 저장
    bin/gp-shell -q "SELECT * FROM staging.orders" -o orders.csv

    # DDL/DML. 기본은 실행 후 커밋한다.
    bin/gp-shell -q "TRUNCATE staging.orders"

    # 실제로 반영하지 않고 무엇이 바뀌는지만 확인 (실행 후 롤백)
    bin/gp-shell -f load_orders.sql --var dt=2026-08-01 --dry-run

SELECT 이든 DDL 이든 한 트랜잭션에서 실행하고, 성공하면 커밋한다. 중간에 실패하면
전부 롤백된다. ``--dry-run`` 은 실행은 하되 항상 롤백한다.

비밀번호는 명령행 인자로 받지 않는다. ps로 다른 사용자에게 노출되기 때문에
설정 파일(보통 ``${GP_PASSWORD}`` 참조), 환경변수, 대화형 입력으로만 받는다.
"""

import argparse
import contextlib
import csv
import getpass
import gzip
import os
import sys
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, TextIO

from . import appconfig
from . import progress
from . import sqlfile
from . import shell
from . import table
from .table import open_output, write_csv
from .progress import PhaseTimer, display_width, pad

#: 설정의 greenplum 섹션에서 읽어 쓰는 키. 나머지 키는 무시한다.
GREENPLUM_SETTINGS = (
    "host",
    "port",
    "database",
    "user",
    "password",
    "schema",
    "sslmode",
    "connect_timeout",
    "session_sql",
)

#: 보고서에 표시할 구간 순서 (실제 실행 흐름대로)
PHASES = ("Greenplum 접속", "세션 설정", "쿼리 실행 요청", "결과 수신", "결과 출력")

#: 결과를 나눠 받는 단위. 진행 상황을 보여주려면 한 번에 다 받으면 안 된다.
FETCH_SIZE = 10_000

#: 표로 보여줄 때 기본으로 출력할 최대 행 수. 0이면 제한 없음.
DEFAULT_MAX_ROWS = table.DEFAULT_MAX_ROWS


def import_psycopg() -> Any:
    """psycopg 를 가져온다. 없으면 설치 방법을 알려준다.

    이 저장소는 executor 백엔드와 같은 psycopg 3 을 쓴다(참고 저장소의 psycopg2 가
    아니다). 드라이버를 하나로 맞춰 두면 같은 DSN·같은 동작을 보장할 수 있다.
    """
    try:
        import psycopg  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "psycopg 가 설치되어 있지 않습니다.\n"
            "    pip install 'psycopg[binary]'\n"
            "  소스 빌드를 쓴다면 libpq-devel 을 먼저 설치하세요."
        ) from exc
    return psycopg


def load_greenplum_settings(args: argparse.Namespace) -> Dict[str, Any]:
    """설정 파일의 greenplum 섹션을 읽는다.

    ``--config`` 로 파일을 직접 지정했는데 greenplum 섹션이 없으면 오타일 가능성이
    높으므로 알린다. 기본 파일이라면 다른 스크립트용 설정만 들어 있을 수 있으니
    넘어간다.
    """
    path = appconfig.resolve_config_path(args)
    return appconfig.load_section(
        path, "greenplum", GREENPLUM_SETTINGS, required=bool(args.config_dir)
    )


def apply_config(
    args: argparse.Namespace,
    config: Dict[str, Any],
    parser: argparse.ArgumentParser,
    sql_config: Optional[Dict[str, Any]] = None,
) -> None:
    """설정 파일 값을 args 에 채운다. 명령행으로 준 값이 항상 우선한다."""
    args.host = appconfig.pick(args.host, config["host"])
    args.port = int(appconfig.pick(args.port, config["port"], 5432))
    args.database = appconfig.pick(args.database, config["database"])
    args.user = appconfig.pick(args.user, config["user"])
    args.schema = appconfig.pick(args.schema, config["schema"])
    args.sslmode = appconfig.pick(args.sslmode, config["sslmode"])
    args.timeout = appconfig.pick(args.timeout, config["connect_timeout"])
    args.sql_dir = sqlfile.resolve_sql_dir(args, sql_config)

    # 설정의 session_sql 을 --session-sql 보다 앞에 둬서 명령행이 뒤에 오게 한다
    session = config["session_sql"]
    if isinstance(session, str):
        session = [session]
    if isinstance(session, list):
        args.session_sql = [str(s) for s in session] + list(args.session_sql)

    args.config_password = config["password"] or None

    for name, flag in (("host", "--host"), ("database", "-d/--database"), ("user", "-u/--user")):
        if not getattr(args, name):
            parser.error(
                f"{name} 을(를) 알 수 없습니다. {flag} 를 주거나 "
                f"설정의 greenplum.{name} 을 채우세요."
            )


def resolve_password(args: argparse.Namespace) -> Optional[str]:
    """설정 파일, 환경변수, 대화형 입력 순으로 비밀번호를 얻는다.

    셋 다 없으면 None을 돌려준다. .pgpass 나 trust 인증을 쓰는 환경도 있어서,
    비밀번호가 없다는 이유만으로 막지는 않는다.
    """
    password = getattr(args, "config_password", None) or os.environ.get(args.password_env)
    if password:
        return password
    if sys.stdin.isatty() and not args.no_password_prompt:
        entered = getpass.getpass(f"{args.user}@{args.host} 비밀번호(없으면 Enter): ")
        return entered or None
    return None


def build_connect_kwargs(args: argparse.Namespace, password: Optional[str]) -> Dict[str, Any]:
    """psycopg.connect 인자를 만든다."""
    kwargs: Dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "dbname": args.database,
        "user": args.user,
    }
    # None을 그대로 넘기면 libpq가 빈 값으로 해석할 수 있어 값이 있을 때만 넣는다
    if password:
        kwargs["password"] = password
    if args.sslmode:
        kwargs["sslmode"] = args.sslmode
    if args.timeout is not None:
        kwargs["connect_timeout"] = int(args.timeout)
    return kwargs


def column_names(description: Sequence[Any]) -> List[str]:
    return [d[0] for d in description]


def fetch_rows(
    cursor: Any, reporter: Optional[progress.Progress] = None
) -> List[Sequence[Any]]:
    """결과를 나눠 받는다.

    fetchall 로 한 번에 받으면 진행 상황을 보여줄 수가 없다. 오래 걸리는 조회에서
    아무 반응 없이 멈춰 있는 것처럼 보이지 않도록 나눠 받으면서 알린다.
    """
    rows: List[Sequence[Any]] = []
    while True:
        batch = cursor.fetchmany(FETCH_SIZE)
        if not batch:
            break
        rows.extend(batch)
        if reporter is not None:
            reporter.update(len(rows))
    if reporter is not None:
        reporter.finish()
    return rows


def run(
    cursor: Any,
    sql: str,
    args: argparse.Namespace,
    timer: Optional[PhaseTimer] = None,
) -> int:
    """SQL을 실행하고 결과를 내보낸다. 종료 코드를 돌려준다."""
    timer = timer or PhaseTimer(PHASES)
    show_progress = not getattr(args, "no_progress", False)

    with timer.measure("쿼리 실행 요청"):
        cursor.execute(sql)

    if cursor.description is None:
        # INSERT/UPDATE/DELETE/DDL. rowcount 는 DDL 이면 -1 이다.
        affected = cursor.rowcount
        detail = f"{affected:,}행" if affected is not None and affected >= 0 else "완료"
        print(detail, file=sys.stderr)
        return 0

    columns = column_names(cursor.description)
    reporter = progress.Progress("받는 중", unit="행", enabled=show_progress)

    if args.output:
        # 파일로 받을 때는 끝까지 받아야 한다
        with timer.measure("결과 수신"):
            rows = fetch_rows(cursor, reporter)
        with timer.measure("결과 출력"):
            return write_output(columns, rows, args)

    # 표로 볼 때는 보여줄 만큼만 받는다. 100행을 보려고 수백만 행을 받지 않는다.
    with timer.measure("결과 수신"):
        rows, truncated = table.fetch_limited(
            cursor, args.max_rows, FETCH_SIZE, on_batch=reporter.update
        )
        reporter.finish()

    with timer.measure("결과 출력"):
        table.print_result(columns, rows, truncated, args.null_string or "NULL")
    return 0


def write_output(
    columns: Sequence[str], rows: Sequence[Sequence[Any]], args: argparse.Namespace
) -> int:
    """받은 결과를 CSV 파일로 쓴다."""
    with open_output(args.output, args.gzip, args.encoding) as handle:
        written = write_csv(
            handle,
            columns,
            rows,
            delimiter=args.delimiter,
            null_string=args.null_string,
            write_header=not args.no_header,
        )
    size = os.path.getsize(args.output)
    print(f"{args.output}  {progress.human_bytes(size)}  {written:,}행", file=sys.stderr)
    return 0


def quote_literal(value: str) -> str:
    """SQL 문자열 리터럴로 감싼다. 카탈로그 조회에 이름을 넣을 때 쓴다."""
    return "'" + value.replace("'", "''") + "'"


def make_engine(args: argparse.Namespace, psycopg: Any, password: Optional[str]) -> "shell.Engine":
    """대화형 셸이 쓸 엔진 어댑터. 접속 인자는 기존 것을 그대로 재사용한다."""

    # 붙어보기 전에는 상대가 Greenplum 인지 그냥 PostgreSQL 인지 알 수 없다.
    # 접속할 때 한 번 확인해두고 카탈로그 조회 SQL 을 다르게 만든다.
    capability = {"distributed_by": False}

    def connect() -> Any:
        return psycopg.connect(**build_connect_kwargs(args, password))

    def probe(conn: Any) -> None:
        """Greenplum 전용 함수가 있는지 본다.

        CASE 로 감싸도 소용없다. 없는 함수는 실행 전 해석 단계에서 걸리므로,
        분기에 넣어두기만 해도 문장 전체가 실패한다. SQL 을 만들기 전에 알아야 한다.
        """
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT 1 FROM pg_proc WHERE proname = 'pg_get_table_distributedby' LIMIT 1"
            )
            capability["distributed_by"] = cursor.fetchone() is not None
        except Exception:
            capability["distributed_by"] = False
        finally:
            cursor.close()

    def enable_autocommit(conn: Any) -> None:
        conn.autocommit = True
        probe(conn)
        if args.schema:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SET search_path TO {args.schema}")
                for statement in args.session_sql:
                    cursor.execute(statement)
            finally:
                cursor.close()

    def cancel(conn: Any, cursor: Any) -> None:
        # psycopg 의 cancel 은 다른 스레드에서 불러도 안전하도록 만들어졌다
        conn.cancel()

    def list_tables_sql(pattern: Optional[str]) -> str:
        where = "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"
        if pattern:
            where += f" AND tablename LIKE {quote_literal(pattern)}"
        return (
            "SELECT schemaname AS schema, tablename AS table, tableowner AS owner "
            f"FROM pg_catalog.pg_tables {where} ORDER BY 1, 2"
        )

    def _target_where(target: str, alias: str = "c") -> str:
        """``schema.table`` 을 카탈로그 조회 조건으로 바꾼다."""
        schema, _, name = target.rpartition(".")
        where = f"{alias}.relname = {quote_literal(name)}"
        if schema:
            where += f" AND n.nspname = {quote_literal(schema)}"
        return where

    def describe_sql(target: str) -> str:
        """컬럼 정보를 pg_catalog 에서 읽는다.

        information_schema 를 쓰면 **현재 사용자가 권한을 가진 테이블만** 보인다.
        남의 스키마를 들여다볼 때 결과가 조용히 비어서, 테이블이 없는 것인지
        권한이 없는 것인지 구분할 수 없다. psql 의 \\d 도 pg_catalog 를 쓴다.

        타입도 이쪽이 낫다. information_schema 는 varchar(50) 을 'character
        varying' 과 길이 컬럼으로 쪼개 놓지만, format_type 은 그대로 돌려준다.
        """
        return f"""
SELECT a.attnum                                        AS "번호",
       a.attname                                       AS "컬럼",
       format_type(a.atttypid, a.atttypmod)            AS "타입",
       CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE '' END AS "NULL",
       coalesce(pg_get_expr(ad.adbin, ad.adrelid), '') AS "기본값"
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
 WHERE {_target_where(target)}
 ORDER BY a.attnum
""".strip()

    def table_names_sql() -> str:
        # 자동완성용. 스키마를 붙인 이름과 안 붙인 이름을 모두 준다.
        # search_path 로 잡아둔 스키마는 짧게 치고 싶고, 다른 스키마는 붙여야 한다.
        visible = "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"
        return (
            f"SELECT tablename AS name FROM pg_catalog.pg_tables {visible} "
            "UNION "
            f"SELECT schemaname || '.' || tablename FROM pg_catalog.pg_tables {visible} "
            "ORDER BY 1"
        )

    def describe_extra_sql(target: str) -> Optional[str]:
        # 분산키는 컬럼 목록에 안 나온다. Greenplum 에서 가장 먼저 확인할 값이다.
        # 그냥 PostgreSQL 이면 분산키 자체가 없으므로 보여줄 것도 없다.
        if not capability["distributed_by"]:
            return None
        return (
            "SELECT '분산키: ' || pg_get_table_distributedby(c.oid) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE {_target_where(target)}"
        )

    def ddl_sql(target: str) -> str:
        """카탈로그에서 CREATE TABLE 문을 조립한다.

        Greenplum 에는 SHOW CREATE TABLE 이 없다. 컬럼, NOT NULL, 기본값, 저장
        옵션(appendonly 등), 분산키까지 맞춘다.

        제약조건·인덱스·파티션은 넣지 않는다. 그것까지 정확히 뽑으려면 pg_dump 를
        쓰는 편이 낫고, 어설프게 흉내내면 맞는 줄 알고 쓰다가 어긋난다.

        분산키는 Greenplum 에만 있다. 그냥 PostgreSQL 이면 그 줄을 빼고 만든다.
        """
        distributed = (
            "|| E'\\n' || pg_get_table_distributedby(c.oid) "
            if capability["distributed_by"]
            else ""
        )
        # 원시 문자열이어야 E'\n' 이 파이썬이 아니라 서버에서 해석된다
        return rf"""
SELECT 'CREATE TABLE ' || n.nspname || '.' || c.relname || E' (\n'
    || string_agg(
           '    ' || quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod)
           || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
           || CASE WHEN ad.adbin IS NOT NULL
                   THEN ' DEFAULT ' || pg_get_expr(ad.adbin, ad.adrelid) ELSE '' END,
           E',\n' ORDER BY a.attnum)
    || E'\n)'
    || CASE WHEN c.reloptions IS NOT NULL
            THEN E'\nWITH (' || array_to_string(c.reloptions, ', ') || ')' ELSE '' END
    {distributed}|| ';'
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
 WHERE {_target_where(target)}
 GROUP BY n.nspname, c.relname, c.oid, c.reloptions
""".strip()

    return shell.Engine(
        name="Greenplum",
        label=args.database,
        connect=connect,
        transactional=True,
        enable_autocommit=enable_autocommit,
        cancel=cancel,
        list_tables_sql=list_tables_sql,
        describe_sql=describe_sql,
        table_names_sql=table_names_sql,
        ddl_sql=ddl_sql,
        describe_extra_sql=describe_extra_sql,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        # 셸 래퍼로 들어오면 그 이름을 보여준다
        prog=os.environ.get("PROG_NAME", "bin/gp-shell"),
        description="Greenplum에 SQL을 실행하고 결과를 표나 CSV로 보여줍니다.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예시:\n"
            "  # sql/ 의 템플릿에 변수를 채워 실행\n"
            "  bin/gp-shell -f order_summary.sql -V dt=2026-08-01\n"
            "\n"
            "  # 결과를 CSV로 저장\n"
            "  bin/gp-shell -q \"SELECT * FROM staging.orders\" -o orders.csv\n"
            "\n"
            "  # DDL/DML (성공하면 커밋)\n"
            "  bin/gp-shell -q \"TRUNCATE staging.orders\"\n"
            "\n"
            "  # 실행은 하되 반영하지 않고 확인만 (항상 롤백)\n"
            "  bin/gp-shell -f load_orders.sql -V dt=2026-08-01 --dry-run\n"
            "\n"
            f"접속 정보는 {appconfig.DEFAULT_CONFIG} 의\n"
            "greenplum 섹션에서 자동으로 읽습니다. 아래 인자를 주면 그 값이 우선합니다.\n"
            "\n"
            "비밀번호는 인자로 받지 않습니다. 설정의 greenplum.password, 환경변수\n"
            "(기본 GP_PASSWORD), 대화형 입력 순으로 찾습니다.\n"
        ),
    )
    appconfig.add_config_arguments(parser)
    progress.add_progress_argument(parser)

    # 설정 파일에서 채울 수 있는 값은 기본값을 None 으로 둔다. 그래야 사용자가
    # 직접 준 값과 argparse 기본값을 구분해 우선순위를 매길 수 있다.
    conn = parser.add_argument_group("접속")
    conn.add_argument("--host", help="Greenplum 마스터 호스트")
    conn.add_argument("--port", type=int, help="기본 5432")
    conn.add_argument("-d", "--database", help="데이터베이스 이름")
    conn.add_argument("-u", "--user", help="접속 계정")
    conn.add_argument(
        "--password-env",
        default="GP_PASSWORD",
        metavar="ENV",
        help="비밀번호를 담은 환경변수 이름 (기본 GP_PASSWORD)",
    )
    conn.add_argument(
        "--no-password-prompt",
        action="store_true",
        help="비밀번호를 물어보지 않는다. .pgpass 나 trust 인증을 쓸 때.",
    )
    conn.add_argument("--schema", help="실행 전에 search_path 로 지정할 스키마")
    conn.add_argument("--sslmode", help="libpq sslmode (require, verify-full 등)")
    conn.add_argument("--timeout", type=int, help="접속 타임아웃(초)")

    # 대화형 셸에서는 쿼리를 나중에 받으므로 여기서 강제하지 않는다
    sqlfile.add_query_arguments(parser, required=False)

    session = parser.add_argument_group("세션")
    session.add_argument(
        "--session-sql",
        action="append",
        default=[],
        metavar="SQL",
        help="쿼리 전에 실행할 SET 문 등. 여러 번 지정 가능",
    )
    session.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="대화형 셸을 엽니다. 이때는 -q/-f 를 주지 않아도 됩니다.",
    )
    session.add_argument(
        "--dry-run",
        action="store_true",
        help="실행은 하되 커밋하지 않고 롤백합니다.",
    )
    session.add_argument(
        "--debug",
        action="store_true",
        help="실제로 서버에 보내는 SQL을 출력합니다.",
    )

    out = parser.add_argument_group("출력")
    out.add_argument("-o", "--output", help="결과를 CSV로 저장할 경로 (생략하면 표로 출력)")
    out.add_argument("--gzip", action="store_true", help="gzip으로 압축해 저장")
    out.add_argument("--delimiter", default="`", help="CSV 컬럼 구분자 (기본 `)")
    out.add_argument("--encoding", default="utf-8", help="CSV 파일 인코딩")
    out.add_argument("--no-header", action="store_true", help="CSV에 헤더 행을 쓰지 않음")
    out.add_argument(
        "--null-string",
        default="",
        help="NULL을 표시할 문자열 (CSV 기본: 빈 값, 표 기본: NULL)",
    )
    out.add_argument(
        "--max-rows",
        type=int,
        default=DEFAULT_MAX_ROWS,
        help=f"표로 출력할 최대 행 수 (기본 {DEFAULT_MAX_ROWS}, 0이면 제한 없음)",
    )
    return parser


def run_shell(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """대화형 셸을 연다. 접속 준비는 일반 실행과 같은 경로를 쓴다."""
    try:
        psycopg = import_psycopg()
    except ImportError as exc:
        print(f"오류: {exc}", file=sys.stderr)
        return 3

    password = resolve_password(args)
    print(f"접속: {args.user}@{args.host}:{args.port}/{args.database}", file=sys.stderr)
    try:
        return shell.Shell(make_engine(args, psycopg, password), args).run()
    except Exception as exc:
        print(f"\n접속 실패: {exc}", file=sys.stderr)
        return 4


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    # 인자 없이 실행하면 무엇을 할 수 있는지 보여준다. 오류가 아니므로 0으로 끝낸다.
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)
    apply_config(args, load_greenplum_settings(args), parser, sqlfile.load_sql_settings(args))

    # 셸은 구간별 소요 시간을 내지 않는다. 세션 전체를 재봐야 의미가 없고,
    # 문장별 시간은 셸 안에서 \timing 이 맡는다.
    if args.interactive:
        return run_shell(args, parser)

    timer = PhaseTimer(PHASES)
    try:
        if not (args.query or args.query_file):
            parser.error("-q/--query 또는 -f/--query-file 을 주세요. "
                         "대화형으로 쓰려면 -i/--interactive 입니다.")

        sql = sqlfile.read_query(args)
        if args.debug:
            print(f"--- 실행할 SQL ---\n{sql}\n------------------", file=sys.stderr)

        try:
            psycopg = import_psycopg()
        except ImportError as exc:
            print(f"오류: {exc}", file=sys.stderr)
            return 3

        password = resolve_password(args)
        print(
            f"접속: {args.user}@{args.host}:{args.port}/{args.database}",
            file=sys.stderr,
        )

        try:
            with timer.measure("Greenplum 접속"):
                conn = psycopg.connect(**build_connect_kwargs(args, password))
        except Exception as exc:
            print(f"\n접속 실패: {exc}", file=sys.stderr)
            return 4

        try:
            cursor = conn.cursor()
            try:
                with timer.measure("세션 설정"):
                    if args.schema:
                        cursor.execute(f"SET search_path TO {args.schema}")
                    for statement in args.session_sql:
                        cursor.execute(statement)

                code = run(cursor, sql, args, timer)
            except Exception as exc:
                conn.rollback()
                print(f"\n쿼리 실행 실패: {exc}", file=sys.stderr)
                return 5
            finally:
                cursor.close()

            if args.dry_run:
                conn.rollback()
                print("--dry-run 이므로 롤백했습니다. 반영되지 않았습니다.", file=sys.stderr)
            else:
                conn.commit()
            return code
        finally:
            conn.close()
    finally:
        # 성공하든 실패하든 어디에 시간을 썼는지는 항상 남긴다
        timer.print_report()


if __name__ == "__main__":
    raise SystemExit(main())
