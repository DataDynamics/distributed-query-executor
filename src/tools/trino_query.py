"""커스텀 실행 함수(기본 ``customs.query_funcs.trino_runner:run``)로 Trino 쿼리를 실행하는 SQL 셸이다.

executor 의 query-execute 는 Trino 를 직접 모르고, 설정 ``query.func.module`` 이 가리키는 함수에
``run(sql, *, config, limit) -> QueryResult`` 로 실행을 맡긴다. 이 도구도 같은 함수와 같은 설정
(``query.func.config.*``)을 그대로 불러 쓴다. 그래서 셸에서 되는 쿼리는 서비스에서도 되고, 사내
DataFrame API(``query.func.config.dataframe_module``)를 쓰는 배포처럼 Trino 드라이버 커서가 없는
경로도 그대로 동작한다.

공용 셸(:mod:`tools.shell`)과 표·CSV 출력 헬퍼는 DB-API 커서를 요구하므로, 실행 함수가 돌려준
``QueryResult`` 를 커서처럼 감싸는 어댑터(:class:`FunctionConnection`)를 둔다. 실행 함수는 호출마다
새로 접속하므로 서버 세션이 이어지지 않는다는 점이 다른 셸과 다르다.

- ``USE catalog.schema`` 는 셸이 가로채 이후 호출의 catalog·schema 설정을 바꾼다.
- ``SET SESSION`` 은 이어 갈 세션이 없어 거절한다. 필요하면 실행 함수 설정으로 넘긴다.
- 결과는 ``--max-rows`` 까지만 받는다(실행 함수의 ``limit``). 잘렸으면 화면과 파일 모두에 알린다.
- Ctrl+C 는 기다림만 멈춘다. 실행 함수가 커서를 드러내지 않아 서버 쪽 쿼리를 취소할 수 없다.

    bin/trino-shell                                   # 대화형
    bin/trino-shell --catalog hive --schema sales     # 설정 값 일부만 덮어쓰기
    PYTHONPATH=src:. python -m tools.trino_query -q "SELECT 1" -o out.csv
"""

from __future__ import annotations

import argparse
import getpass
import importlib
import os
import re
import sys
from typing import Any, Dict, List, Optional, Sequence

from . import appconfig
from . import progress
from . import shell
from . import sqlfile
from . import table
from .impala_query import export, human, show_table
from .progress import PhaseTimer
from .sqlfile import load_sql_settings, read_query

DEFAULT_MODULE = "customs.query_funcs.trino_runner:run"
PHASES = ("쿼리 실행 요청", "첫 배치 대기", "데이터 수신", "CSV 쓰기", "표 출력")

#: 실행 함수의 limit 은 정수여야 하므로, 제한 없음(--max-rows 0)은 이 값으로 넘긴다.
UNLIMITED = 1_000_000_000

_USE = re.compile(r"^\s*use\s+([A-Za-z0-9_\"`.]+)\s*;?\s*$", re.IGNORECASE)
_SET_SESSION = re.compile(r"^\s*(set|reset)\s+session\b", re.IGNORECASE)


def load_function(dotted: str):
    """``module:func`` 또는 ``module.func`` 경로의 실행 함수를 불러온다."""
    mod_path, sep, attr = dotted.partition(":")
    if not sep:
        mod_path, _, attr = dotted.rpartition(".")
    if not mod_path or not attr:
        raise SystemExit(f"잘못된 함수 경로입니다: {dotted!r} (module:func 형식)")
    try:
        fn = getattr(importlib.import_module(mod_path), attr)
    except ImportError as exc:
        raise SystemExit(
            f"실행 함수를 불러올 수 없습니다: {dotted} ({exc})\n"
            "  PYTHONPATH 에 저장소 루트(customs 패키지)가 있는지, 실행 함수가 쓰는 패키지"
            "(trino 등)가 설치됐는지 확인하세요."
        ) from exc
    except AttributeError as exc:
        raise SystemExit(f"실행 함수가 모듈에 없습니다: {dotted}") from exc
    if not callable(fn):
        raise SystemExit(f"실행 함수가 호출 가능하지 않습니다: {dotted}")
    return fn


class FunctionCursor:
    """실행 함수 한 번의 결과(QueryResult)를 DB-API 커서처럼 내주는 어댑터다."""

    arraysize = 1000

    def __init__(self, conn: "FunctionConnection") -> None:
        self._conn = conn
        self.description: Optional[List[tuple]] = None
        self.rowcount = -1
        self.truncated = False
        self._rows: List[Sequence[Any]] = []
        self._pos = 0

    def execute(self, sql: str, params: Any = None) -> None:
        self.description, self._rows, self._pos, self.truncated = None, [], 0, False
        self.rowcount = -1
        if params:
            raise ValueError("이 셸은 바인드 파라미터를 지원하지 않습니다")
        if self._conn.intercept(sql):
            return
        result = self._conn.fn(sql, config=dict(self._conn.config), limit=self._conn.limit)
        columns = list(getattr(result, "columns", None) or [])
        rows = list(getattr(result, "rows", None) or [])
        self.truncated = bool(getattr(result, "truncated", False))
        self._conn.last_truncated = self.truncated
        if columns:
            self.description = [(name, None, None, None, None, None, None) for name in columns]
            self._rows = rows
            self.rowcount = len(rows)

    def fetchmany(self, size: Optional[int] = None) -> List[Sequence[Any]]:
        n = size or self.arraysize
        batch = self._rows[self._pos:self._pos + n]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> List[Sequence[Any]]:
        rest = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        self._rows = []


class FunctionConnection:
    """실행 함수와 그 설정(config dict)을 묶은 연결 흉내다. 서버 세션은 없고 설정만 이어진다."""

    def __init__(self, fn, config: Dict[str, str], limit: int, on_use=None) -> None:
        self.fn = fn
        self.config = config
        self.limit = limit
        self.last_truncated = False
        self._on_use = on_use

    def cursor(self) -> FunctionCursor:
        return FunctionCursor(self)

    def intercept(self, sql: str) -> bool:
        """세션 명령을 셸이 대신 처리한다. 처리했으면 True 다."""
        m = _USE.match(sql)
        if m:
            parts = [p.strip('"`') for p in m.group(1).split(".")]
            if len(parts) == 1:
                self.config["schema"] = parts[0]
            elif len(parts) == 2:
                self.config["catalog"], self.config["schema"] = parts
            else:
                raise ValueError("USE 는 schema 또는 catalog.schema 형식입니다")
            if self._on_use:
                self._on_use(self)
            print(f"이후 쿼리는 {self.config.get('catalog', '?')}.{self.config['schema']} 에서 실행합니다.",
                  file=sys.stderr)
            return True
        if _SET_SESSION.match(sql):
            raise ValueError(
                "실행 함수가 호출마다 새로 접속하므로 SET SESSION 이 다음 쿼리에 이어지지 않습니다. "
                "세션 속성이 필요하면 실행 함수 설정(--set key=value)으로 넘기세요."
            )
        return False

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _quote_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def label_of(config: Dict[str, str]) -> str:
    catalog, schema = config.get("catalog"), config.get("schema")
    if catalog and schema:
        return f"{catalog}.{schema}"
    return schema or catalog or "trino"


def make_engine(args: argparse.Namespace, fn, config: Dict[str, str]) -> shell.Engine:
    """공용 셸이 쓸 엔진이다. 연결은 실행 함수 어댑터이고 메타 명령은 Trino SQL 로 바꾼다."""
    limit = args.max_rows if args.max_rows and args.max_rows > 0 else UNLIMITED
    holder: Dict[str, Any] = {}

    def on_use(conn: FunctionConnection) -> None:
        holder["engine"].label = label_of(conn.config)

    def connect() -> FunctionConnection:
        # 셸은 오류 뒤에 다시 연결하는데, USE 로 바꾼 catalog·schema 가 유지되도록 같은 dict 를 쓴다.
        return FunctionConnection(fn, config, limit, on_use=on_use)

    engine = shell.Engine(
        name="Trino",
        label=label_of(config),
        connect=connect,
        list_tables_sql=lambda pattern: f"SHOW TABLES LIKE {_quote_literal(pattern)}" if pattern else "SHOW TABLES",
        describe_sql=lambda target: f"DESCRIBE {target}",
        table_names_sql=lambda: "SHOW TABLES",
        ddl_sql=lambda target: f"SHOW CREATE TABLE {target}",
    )
    holder["engine"] = engine
    return engine


# ───────── 설정 ─────────

def load_trino_settings(args: argparse.Namespace) -> Dict[str, Any]:
    """설정의 ``query.func.module`` 과 ``query.func.config.*`` 를 읽는다."""
    path = appconfig.resolve_config_path(args)
    return appconfig.load_section(path, "trino", ("module", "config"))


def build_config(args: argparse.Namespace, loaded: Dict[str, Any]) -> Dict[str, str]:
    """설정 값 위에 명령행 값을 덮어 실행 함수에 넘길 config dict 를 만든다(값은 모두 문자열)."""
    config: Dict[str, str] = {k: str(v) for k, v in dict(loaded.get("config") or {}).items()}
    for key in ("host", "port", "user", "catalog", "schema", "http_scheme"):
        value = getattr(args, key, None)
        if value is not None:
            config[key] = str(value)
    for item in args.set:
        key, sep, value = item.partition("=")
        if not sep or not key.strip():
            raise SystemExit(f"--set 은 KEY=VALUE 형식입니다: {item}")
        config[key.strip()] = value
    if args.password_prompt:
        who = config.get("user", "")
        config["password"] = getpass.getpass(f"{who}@{config.get('host', 'trino')} 비밀번호: ")
    return config


def describe_target(module: str, config: Dict[str, str]) -> str:
    if (config.get("dataframe_module") or "").strip():
        return f"{module} → DataFrame API {config['dataframe_module']} ({label_of(config)})"
    host = config.get("host") or "?"
    return (f"{module} → {config.get('http_scheme', 'http')}://{host}:{config.get('port', '8080')} "
            f"({label_of(config)}, user={config.get('user', '')})")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=os.environ.get("PROG_NAME", "bin/trino-shell"),
        description="커스텀 실행 함수(query.func.module)로 Trino 쿼리를 실행해 표나 CSV 로 보여줍니다.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예시:\n"
            "  bin/trino-shell                                    # 대화형\n"
            "  bin/trino-shell --catalog hive --schema sales\n"
            "  bin/trino-shell -q \"SELECT * FROM t LIMIT 10\"\n"
            "  bin/trino-shell -f daily.sql -V dt=2026-10-01 -o out.csv --max-rows 0\n"
            "\n"
            f"실행 함수와 접속 정보는 {appconfig.DEFAULT_CONFIG} 의 query.func.module 과\n"
            "query.func.config.* 에서 읽습니다. 아래 인자를 주면 그 값이 우선합니다.\n"
        ),
    )
    appconfig.add_config_arguments(parser)
    progress.add_progress_argument(parser)

    conn = parser.add_argument_group("실행 함수와 접속(query.func.config.* 덮어쓰기)")
    conn.add_argument("--module", help=f"실행 함수(module:func). 기본은 설정의 query.func.module, 없으면 {DEFAULT_MODULE}")
    conn.add_argument("--host")
    conn.add_argument("--port", type=int)
    conn.add_argument("-u", "--user")
    conn.add_argument("--catalog")
    conn.add_argument("--schema")
    conn.add_argument("--http-scheme", choices=["http", "https"])
    conn.add_argument("--password-prompt", action="store_true",
                      help="비밀번호를 대화형으로 입력받아 config.password 로 넘긴다")
    conn.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                      help="실행 함수 설정 하나를 덮어쓴다(예: --set verify=false). 여러 번 지정 가능")

    sqlfile.add_query_arguments(parser, required=False)
    sess = parser.add_argument_group("세션")
    sess.add_argument("-i", "--interactive", action="store_true", help="대화형 셸을 엽니다")

    out = parser.add_argument_group("출력")
    out.add_argument("-o", "--output", help="저장할 CSV 경로 (생략하면 표로 출력)")
    out.add_argument("--gzip", action="store_true", help="gzip으로 압축해 저장")
    out.add_argument("--delimiter", default=",", help="컬럼 구분자 (기본 ,)")
    out.add_argument("--quote", action="store_true", help="값을 큰따옴표로 감싼다")
    out.add_argument("--escapechar", default="\\", help="따옴표를 쓰지 않을 때 이스케이프 문자 (기본 \\)")
    out.add_argument("--encoding", default="utf-8", help="파일 인코딩(엑셀이면 utf-8-sig)")
    out.add_argument("--no-header", action="store_true", help="헤더 행을 쓰지 않음")
    out.add_argument("--null-string", default="", help="NULL 표시 문자열 (기본 빈 값)")
    out.add_argument("--max-rows", type=int, default=table.DEFAULT_MAX_ROWS,
                     help=f"실행 함수에 넘길 최대 행 수 (기본 {table.DEFAULT_MAX_ROWS}, 0이면 제한 없음)")
    return parser


def _warn_truncated(conn: FunctionConnection, args: argparse.Namespace) -> None:
    if conn.last_truncated:
        print(f"주의: 실행 함수가 상위 {args.max_rows:,}행에서 결과를 잘랐습니다. "
              "전부 받으려면 --max-rows 를 늘리거나 0 으로 주세요.", file=sys.stderr)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        return 0
    args = parser.parse_args(argv)

    loaded = load_trino_settings(args)
    module = args.module or loaded.get("module") or DEFAULT_MODULE
    config = build_config(args, loaded)
    args.sql_dir = sqlfile.resolve_sql_dir(args, load_sql_settings(args))
    fn = load_function(module)
    print(f"실행: {describe_target(module, config)}", file=sys.stderr)

    engine = make_engine(args, fn, config)
    if args.interactive:
        try:
            return shell.Shell(engine, args).run()
        except Exception as exc:
            print(f"\n실행 실패: {exc}", file=sys.stderr)
            return 4

    if not (args.query or args.query_file):
        parser.error("-q/--query 또는 -f/--query-file 을 주세요. 대화형으로 쓰려면 -i/--interactive 입니다.")
    query = read_query(args)
    timer = PhaseTimer(PHASES)
    conn = engine.connect()
    cursor = conn.cursor()
    try:
        reporter = progress.Progress("읽는 중", unit="행", enabled=not args.no_progress)
        try:
            if not args.output:
                show_table(cursor, query, timer, batch_size=10_000, max_rows=args.max_rows,
                           null_string=args.null_string, reporter=reporter)
                _warn_truncated(conn, args)
                return 0
            with table.open_output(args.output, args.gzip, args.encoding) as handle:
                rows = export(cursor, query, handle, timer, batch_size=10_000, delimiter=args.delimiter,
                              null_string=args.null_string, write_header=not args.no_header,
                              reporter=reporter, quote=args.quote, escapechar=args.escapechar)
        except Exception as exc:
            print(f"\n쿼리 실행 실패: {exc}", file=sys.stderr)
            return 5
        _warn_truncated(conn, args)
        print(f"{args.output}  {human(os.path.getsize(args.output))}  {rows:,}행", file=sys.stderr)
        return 0
    finally:
        cursor.close()
        timer.print_report()


if __name__ == "__main__":
    raise SystemExit(main())
