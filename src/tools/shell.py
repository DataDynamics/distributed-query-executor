"""대화형 SQL 셸. beeline / sqlplus 처럼 붙어서 쿼리를 주고받는다.

엔진에 중립이다. 접속과 카탈로그 조회처럼 엔진마다 다른 부분만 :class:`Engine`
으로 받고, 나머지(입력 누적, 문장 분리, 메타 명령, 히스토리, 출력)는 여기서
처리한다. 실제 실행은 DB-API 커서로 하므로 impyla 든 psycopg2 든 같은 코드를 탄다.

터미널이 아니면(파이프, 크론) 프롬프트와 히스토리를 끄고 입력을 순서대로 실행한다.

    echo "SELECT 1;" | bin/gp-shell
    bin/gp-shell < script.sql
"""

import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from . import progress
from . import sqlfile
from . import table
from .progress import PhaseTimer

#: 히스토리를 두는 곳. 저장소 안에 두면 커밋 사고가 나고, 쿼리에는 값이 그대로 들어 있다.
HISTORY_DIR = os.path.join(os.path.expanduser("~"), ".impala-to-whpg")

#: 한 번에 받아오는 행 수
FETCH_SIZE = 10_000

#: PAGER 가 없을 때 쓸 기본값.
#: -F 는 한 화면에 들어가면 그냥 출력, -R 은 색, -S 는 긴 줄을 접지 않고 옆으로,
#: -X 는 끝난 뒤 화면을 지우지 않는다(결과가 스크롤백에 남는다).
DEFAULT_PAGER = "less -FRSX"

#: 자동완성에 쓸 SQL 키워드. 많이 넣을수록 방해가 되므로 자주 쓰는 것만 둔다.
KEYWORDS = (
    "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT",
    "JOIN", "LEFT JOIN", "INNER JOIN", "ON", "AS", "AND", "OR", "NOT", "NULL",
    "INSERT INTO", "UPDATE", "DELETE FROM", "CREATE TABLE", "DROP TABLE",
    "ALTER TABLE", "TRUNCATE", "DISTRIBUTED BY", "WITH", "UNION", "CASE",
    "WHEN", "THEN", "ELSE", "END", "COUNT", "SUM", "AVG", "MIN", "MAX",
)

#: 자동완성에 쓸 메타 명령 목록
META_NAMES = (
    "\\q", "\\?", "\\i", "\\set", "\\unset", "\\o", "\\timing", "\\x",
    "\\dt", "\\d", "\\ddl", "\\e", "\\paste", "\\pager", "\\begin", "\\commit", "\\rollback",
    ":paste",        # spark-shell 습관대로 치는 사람을 위해 이것도 완성한다
)


class Engine:
    """엔진마다 다른 부분만 담는다. 각 도구 모듈이 만들어 넘긴다."""

    def __init__(
        self,
        name: str,
        label: str,
        connect: Callable[[], Any],
        transactional: bool = False,
        enable_autocommit: Optional[Callable[[Any], None]] = None,
        cancel: Optional[Callable[[Any, Any], None]] = None,
        list_tables_sql: Optional[Callable[[Optional[str]], str]] = None,
        describe_sql: Optional[Callable[[str], str]] = None,
        table_names_sql: Optional[Callable[[], str]] = None,
        ddl_sql: Optional[Callable[[str], str]] = None,
        describe_extra_sql: Optional[Callable[[str], str]] = None,
    ) -> None:
        self.name = name                      # "Impala" / "Greenplum"
        self.label = label                    # 프롬프트에 쓸 대상 이름
        self._connect = connect
        self.transactional = transactional
        self._enable_autocommit = enable_autocommit
        self._cancel = cancel
        self.list_tables_sql = list_tables_sql
        self.describe_sql = describe_sql
        self.table_names_sql = table_names_sql
        self.ddl_sql = ddl_sql
        self.describe_extra_sql = describe_extra_sql

    def cancel(self, conn: Any, cursor: Any) -> bool:
        """실행 중인 문장을 서버에 취소해 달라고 요청한다. 요청이 받아들여지면 True 를 돌려준다."""
        if self._cancel is None:
            return False
        self._cancel(conn, cursor)
        return True

    def connect(self) -> Any:
        conn = self._connect()
        # 셸은 세션이 길다. 한 트랜잭션으로 묶어두면 잠금이 계속 유지되고,
        # 사용자는 문장마다 반영되기를 기대한다. 기본을 autocommit 으로 둔다.
        if self._enable_autocommit is not None:
            self._enable_autocommit(conn)
        return conn


# -- 문장 분리 ---------------------------------------------------------------------


#: PostgreSQL/Greenplum 의 달러 인용. $$ ... $$ 또는 $tag$ ... $tag$
DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


def split_statements(text: str) -> Tuple[List[str], str]:
    """``;`` 로 문장을 끊는다. (완성된 문장들, 남은 조각) 을 돌려준다.

    따옴표와 주석 안의 ``;`` 는 세지 않는다. 그러지 않으면
    ``SELECT ';'`` 한 줄이 두 문장으로 잘린다.

    Greenplum 의 달러 인용(``$$ ... $$``, ``$tag$ ... $tag$``)도 통째로 넘긴다.
    함수 정의처럼 그 안에
    ``;`` 가 들어가는 문장은 ``\\i`` 로 파일째 실행하는 편이 안전하다.
    """
    statements: List[str] = []
    start = 0
    i = 0
    quote: Optional[str] = None       # 현재 열려 있는 따옴표
    comment: Optional[str] = None     # "line" 또는 "block"
    dollar: Optional[str] = None      # 열려 있는 달러 인용 태그

    while i < len(text):
        ch = text[i]
        pair = text[i : i + 2]

        if dollar:
            if text.startswith(dollar, i):
                i += len(dollar) - 1
                dollar = None
        elif comment == "line":
            if ch == "\n":
                comment = None
        elif comment == "block":
            if pair == "*/":
                comment = None
                i += 1
        elif quote:
            if ch == quote:
                # 따옴표를 두 번 겹치면 이스케이프다 ('' 또는 "")
                if text[i + 1 : i + 2] == quote:
                    i += 1
                else:
                    quote = None
        elif pair == "--":
            comment = "line"
            i += 1
        elif pair == "/*":
            comment = "block"
            i += 1
        elif ch in ("'", '"'):
            quote = ch
        elif ch == "$" and DOLLAR_TAG.match(text, i):
            # 함수 본문에는 세미콜론이 여러 개 들어간다. 통째로 넘기지 않으면
            # CREATE FUNCTION 이 중간에서 잘린다.
            match = DOLLAR_TAG.match(text, i)
            dollar = match.group(0)
            i = match.end() - 1
        elif ch == ";":
            chunk = text[start : i].strip()
            if chunk:
                statements.append(chunk)
            start = i + 1
        i += 1

    return statements, text[start:]


def is_complete(buffer: str) -> bool:
    """버퍼가 문장 하나로 끝났는지. 프롬프트를 바꿀지 판단하는 데 쓴다."""
    statements, rest = split_statements(buffer)
    return bool(statements) and not rest.strip()


# -- 셸 ---------------------------------------------------------------------------


class Shell:
    def __init__(self, engine: Engine, args: Any) -> None:
        self.engine = engine
        self.args = args
        self.conn: Any = None
        self.variables: Dict[str, str] = sqlfile.parse_variables(getattr(args, "var", []))
        self.output: Optional[str] = None          # \o 로 지정한 CSV 경로
        self.timing = False
        self.vertical = False
        self.pager = "auto"                        # auto / on / off
        self._names: Optional[List[str]] = None    # 자동완성용 테이블 이름 캐시
        self.max_rows = getattr(args, "max_rows", table.DEFAULT_MAX_ROWS)
        self.interactive = sys.stdin.isatty() and progress.is_interactive()
        self.buffer = ""

    # -- 프롬프트 --
    def prompt(self) -> str:
        base = self.engine.label
        return f"{base}-> " if self.buffer.strip() else f"{base}=> "

    def history_path(self) -> str:
        return os.path.join(HISTORY_DIR, f"history-{self.engine.name.lower()}")

    def load_history(self) -> Any:
        """readline 이 있으면 히스토리를 붙인다. 없어도 셸은 돌아간다."""
        try:
            import readline
        except ImportError:
            return None
        os.makedirs(HISTORY_DIR, mode=0o700, exist_ok=True)
        path = self.history_path()
        try:
            readline.read_history_file(path)
        except (OSError, ValueError):
            pass
        readline.set_history_length(1000)

        # 기본 구분자에는 \\ 와 . 이 들어 있어 \\dt 나 schema.table 이 잘린다
        readline.set_completer_delims(" \t\n")
        readline.set_completer(self.complete)
        # libedit(맥 기본)과 GNU readline 의 바인딩 문법이 다르다
        if "libedit" in getattr(readline, "__doc__", "") or "":
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")
        return readline

    def table_names(self) -> List[str]:
        """자동완성에 쓸 테이블 이름. 처음 Tab 을 누를 때 한 번만 받아둔다.

        카탈로그 조회가 느릴 수 있어 매번 부르지 않는다. 새로 만든 테이블은
        ``\\dt`` 로 목록을 다시 받으면 캐시도 갱신된다.
        """
        if self._names is not None:
            return self._names
        self._names = []
        maker = self.engine.table_names_sql
        if maker is None:
            return self._names
        cursor = self.conn.cursor()
        try:
            cursor.execute(maker())
            self._names = [str(row[0]) for row in cursor.fetchall() if row and row[0]]
        except Exception:
            # 자동완성이 실패했다고 셸이 시끄러워질 이유는 없다
            self._recover()
        finally:
            cursor.close()
        return self._names

    def complete(self, text: str, state: int) -> Optional[str]:
        """readline 자동완성 콜백. state 0 일 때만 후보를 만든다."""
        if state == 0:
            self._matches = self.candidates(text)
        try:
            return self._matches[state]
        except IndexError:
            return None

    def candidates(self, text: str) -> List[str]:
        if text.startswith("\\") or text.startswith(":"):
            return sorted(c for c in META_NAMES if c.startswith(text))
        if not text:
            return []
        lowered = text.lower()
        names = [n for n in self.table_names() if n.lower().startswith(lowered)]
        keywords = [k for k in KEYWORDS if k.lower().startswith(lowered)]
        # 테이블 이름을 먼저 보여준다. 키워드는 외우고 있는 경우가 많다.
        return sorted(set(names)) + sorted(set(keywords))

    def save_history(self, readline: Any) -> None:
        if readline is None:
            return
        path = self.history_path()
        try:
            readline.write_history_file(path)
            # 쿼리에는 값이 그대로 들어 있다. 남이 읽지 못하게 한다.
            os.chmod(path, 0o600)
        except OSError:
            pass

    # -- 실행 --
    def execute(self, sql: str) -> None:
        rendered = sqlfile.render_query(sql, self.variables, "입력", warn_unused=False)
        self.run_sql(rendered)

    def run_sql(self, sql: str) -> Optional[int]:
        """문장을 실행한다. 결과가 있으면 보여준 행 수를 돌려준다."""
        timer = PhaseTimer(("실행 요청", "결과 출력"))
        shown: Optional[int] = None
        cursor = self.conn.cursor()
        try:
            with timer.measure("실행 요청"):
                outcome = self._run_cancellable(cursor, sql)
            if outcome is None:
                return None                             # 사용자가 취소했다

            columns, rows, truncated, affected = outcome
            if columns is None:
                detail = (
                    f"{affected:,}행" if affected is not None and affected >= 0 else "완료"
                )
                print(detail, file=sys.stderr)
            else:
                shown = len(rows)
                with timer.measure("결과 출력"):
                    self._emit(columns, rows, truncated)
        finally:
            cursor.close()

        if self.timing:
            timer.print_report()
        return shown

    def _run_cancellable(self, cursor: Any, sql: str) -> Optional[Tuple[Any, Any, bool, int]]:
        """문장을 별도 스레드에서 돌려 Ctrl-C 로 취소할 수 있게 한다.

        DB 드라이버는 실행 중 GIL 을 놓는다. 그래서 메인 스레드가 그대로 기다리면
        SIGINT 핸들러가 돌지 못해 Ctrl-C 가 먹지 않는다. 실행을 스레드로 넘기고
        메인은 짧게 끊어 기다리면 그 사이에 시그널을 받을 수 있다.

        취소는 서버에 요청하는 것이므로 즉시 끝나지 않을 수 있다.
        """
        box: Dict[str, Any] = {}

        def work() -> None:
            try:
                cursor.execute(sql)
                if cursor.description is None:
                    box["result"] = (None, [], False, getattr(cursor, "rowcount", -1))
                else:
                    columns = [d[0].split(".")[-1] for d in cursor.description]
                    if self.output:
                        rows, truncated = self._fetch_all(cursor), False
                    else:
                        rows, truncated = table.fetch_limited(
                            cursor, self.max_rows, FETCH_SIZE
                        )
                    box["result"] = (columns, rows, truncated, -1)
            except BaseException as exc:      # 스레드 밖으로 그대로 넘긴다
                box["error"] = exc

        worker = threading.Thread(target=work, daemon=True)
        worker.start()
        try:
            while worker.is_alive():
                worker.join(0.1)
        except KeyboardInterrupt:
            print("\n취소하는 중입니다...", file=sys.stderr)
            if not self.engine.cancel(self.conn, cursor):
                print(
                    "이 엔진은 취소를 지원하지 않습니다. 문장이 끝날 때까지 기다립니다.",
                    file=sys.stderr,
                )
            worker.join()
            self._recover()
            print("취소했습니다.", file=sys.stderr)
            return None

        if "error" in box:
            raise box["error"]
        return box["result"]

    def _fetch_all(self, cursor: Any) -> List[Sequence[Any]]:
        rows: List[Sequence[Any]] = []
        while True:
            batch = cursor.fetchmany(FETCH_SIZE)
            if not batch:
                break
            rows.extend(batch)
        return rows

    def _emit(self, columns: Sequence[str], rows: Sequence[Sequence[Any]], truncated: bool) -> None:
        if self.output:
            with table.open_output(self.output, False, "utf-8") as handle:
                written = table.write_csv(handle, columns, rows)
            print(f"{self.output}  {written:,}행", file=sys.stderr)
            return

        body, note = table.format_result(
            columns, rows, truncated, vertical=self.vertical
        )
        self.page(body)
        print(note, file=sys.stderr)

    def page(self, text: str) -> None:
        """긴 결과는 페이저로 넘긴다.

        화면에 들어가는 결과까지 페이저를 거치면 오히려 성가시므로, ``auto`` 에서는
        터미널 높이를 넘을 때만 쓴다. 터미널이 아니면(파이프, 크론) 쓰지 않는다.
        """
        if not self.interactive or self.pager == "off":
            print(text)
            return

        height = shutil.get_terminal_size(fallback=(80, 24)).lines
        if self.pager == "auto" and text.count("\n") + 2 < height:
            print(text)
            return

        command = os.environ.get("PAGER") or DEFAULT_PAGER
        try:
            proc = subprocess.Popen(
                shlex.split(command), stdin=subprocess.PIPE, text=True
            )
        except OSError as exc:
            print(f"페이저를 실행하지 못했습니다({command}): {exc}", file=sys.stderr)
            print(text)
            return
        try:
            proc.communicate(text + "\n")
        except BrokenPipeError:
            # 사용자가 페이저를 먼저 끝낸 경우다. 오류가 아니다.
            pass
        finally:
            if proc.poll() is None:
                proc.wait()

    # -- 메타 명령 --
    def meta(self, line: str) -> bool:
        """``\\`` 로 시작하는 메타 명령을 처리한다. 셸을 계속 돌려도 되면 True 를 돌려준다."""
        parts = line.strip().split(None, 2)
        name, rest = parts[0], parts[1:]

        if name in ("\\q", "\\quit"):
            return False
        if name in ("\\?", "\\h", "\\help"):
            print(HELP)
        elif name == "\\i":
            if not rest:
                print("사용법: \\i 파일.sql", file=sys.stderr)
            else:
                self.run_file(rest[0])
        elif name == "\\set":
            if not rest:
                for key, value in sorted(self.variables.items()):
                    print(f"{key} = {value}")
            elif len(rest) == 1:
                print("사용법: \\set 이름 값", file=sys.stderr)
            else:
                self.variables[rest[0]] = rest[1]
        elif name == "\\unset":
            if rest:
                self.variables.pop(rest[0], None)
        elif name == "\\o":
            self.output = rest[0] if rest else None
            where = self.output or "화면"
            print(f"결과를 {where} 으로 보냅니다.", file=sys.stderr)
        elif name == "\\timing":
            self.timing = not self.timing
            print(f"소요 시간 표시: {'켬' if self.timing else '끔'}", file=sys.stderr)
        elif name == "\\x":
            self.vertical = not self.vertical
            print(f"세로 출력: {'켬' if self.vertical else '끔'}", file=sys.stderr)
        elif name in ("\\dt", "\\d"):
            self.catalog(name, rest[0] if rest else None)
        elif name == "\\ddl":
            self.ddl(rest[0] if rest else None)
        elif name == "\\e":
            self.edit()
        elif name in ("\\paste", ":paste"):
            self.paste()
        elif name == "\\pager":
            choice = (rest[0] if rest else "").lower()
            if choice not in ("auto", "on", "off"):
                print(f"페이저: {self.pager}  (\\pager auto|on|off)", file=sys.stderr)
            else:
                self.pager = choice
                print(f"페이저: {self.pager}", file=sys.stderr)
        elif name in ("\\begin", "\\commit", "\\rollback"):
            self.transaction(name)
        else:
            print(f"모르는 명령입니다: {name}  (\\? 로 목록을 봅니다)", file=sys.stderr)
        return True

    def transaction(self, name: str) -> None:
        if not self.engine.transactional:
            print(f"{self.engine.name} 에는 트랜잭션이 없습니다.", file=sys.stderr)
            return
        cursor = self.conn.cursor()
        try:
            if name == "\\begin":
                cursor.execute("BEGIN")
                print("트랜잭션을 시작했습니다. \\commit 또는 \\rollback 으로 끝냅니다.",
                      file=sys.stderr)
            else:
                cursor.execute("COMMIT" if name == "\\commit" else "ROLLBACK")
                print("반영했습니다." if name == "\\commit" else "되돌렸습니다.",
                      file=sys.stderr)
        finally:
            cursor.close()

    def run_text_sql(self, sql: str) -> None:
        """결과를 표가 아니라 원문 그대로 낸다.

        CREATE TABLE 문처럼 그 자체가 여러 줄인 값은 표에 넣으면 오히려 읽기
        어렵다. 복사해서 바로 쓸 수 있어야 한다.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            if cursor.description is None:
                return
            lines = [str(row[0]) for row in cursor.fetchall() if row and row[0] is not None]
        finally:
            cursor.close()
        if lines:
            self.page("\n".join(lines))

    def ddl(self, target: Optional[str]) -> None:
        """``\\ddl 이름`` — 테이블 생성문을 보여준다."""
        if not target:
            print("사용법: \\ddl 테이블이름", file=sys.stderr)
            return
        if self.engine.ddl_sql is None:
            print(f"{self.engine.name} 에서는 지원하지 않습니다.", file=sys.stderr)
            return
        try:
            self.run_text_sql(self.engine.ddl_sql(target))
        except Exception as exc:
            print(f"오류: {type(exc).__name__}: {exc}", file=sys.stderr)
            self._recover()

    def catalog(self, name: str, target: Optional[str]) -> None:
        """``\\dt`` 는 테이블 목록을, ``\\d 이름`` 은 그 테이블의 컬럼 정보를 보여준다.

        엔진이 SQL 을 만들어 주면 평범한 문장처럼 실행한다. 카탈로그 조회 방식이
        Impala 와 Greenplum 이 전혀 다르지만, 셸이 알아야 할 것은 없다.
        """
        if name == "\\d" and target:
            maker, label = self.engine.describe_sql, f"{target} 의 컬럼"
        else:
            maker, label = self.engine.list_tables_sql, "테이블 목록"
            target = target if name == "\\dt" else None

        if maker is None:
            print(f"{self.engine.name} 에서는 지원하지 않습니다.", file=sys.stderr)
            return
        if name == "\\dt":
            self._names = None          # 목록을 다시 받으니 자동완성 캐시도 비운다
        print(f"-- {label}", file=sys.stderr)
        try:
            shown = self.run_sql(maker(target))
            if shown == 0:
                # 빈 표만 내면 없는 것인지 잘못 친 것인지 알 수 없다
                if name == "\\d" and target:
                    print(
                        f"{target} 을(를) 찾지 못했습니다. 스키마를 붙였는지, "
                        "이름 대소문자가 맞는지 확인하세요(\\dt 로 목록을 봅니다).",
                        file=sys.stderr,
                    )
                else:
                    print("테이블이 없습니다.", file=sys.stderr)
                return
            # 분산키처럼 컬럼 목록에 안 들어가는 정보를 뒤에 덧붙인다.
            # 엔진이 None 을 주면 그 서버에는 보여줄 것이 없다는 뜻이다.
            if name == "\\d" and target and self.engine.describe_extra_sql is not None:
                extra = self.engine.describe_extra_sql(target)
                if extra:
                    self.run_text_sql(extra)
        except Exception as exc:
            print(f"오류: {type(exc).__name__}: {exc}", file=sys.stderr)
            self._recover()

    def paste(self) -> None:
        """긴 쿼리를 통째로 붙여넣는다. spark-shell 의 ``:paste`` 와 같은 자리다.

        평소에는 ``;`` 가 나올 때까지 모으지만, 붙여넣는 글에는 세미콜론이 없을
        때가 많고 ``\\`` 로 시작하는 줄이 메타 명령으로 잡히기도 한다. 이 모드에서는
        둘 다 신경 쓰지 않고, 끝났다는 신호를 받은 뒤에 한꺼번에 해석한다.

        ``Ctrl-D`` 또는 ``\\.`` 만 있는 줄로 끝낸다. ``Ctrl-C`` 면 버린다.
        """
        if self.interactive:
            print("붙여넣고 Ctrl-D 로 끝냅니다. (\\. 만 있는 줄도 됩니다)", file=sys.stderr)

        lines: List[str] = []
        while True:
            try:
                lines.append(input("| " if self.interactive else ""))
            except EOFError:
                break
            except KeyboardInterrupt:
                print("\n붙여넣기를 취소했습니다.", file=sys.stderr)
                return
            if lines[-1].strip() == "\\.":
                lines.pop()
                break

        text = "\n".join(lines).strip()
        if not text:
            print("붙여넣은 내용이 없습니다.", file=sys.stderr)
            return

        statements, rest = split_statements(text)
        # 세미콜론 없이 끝났으면 남은 것을 통째로 한 문장으로 본다
        if rest.strip():
            statements.append(rest.strip())

        if self.interactive:
            print(f"{len(statements)}개 문장을 실행합니다.", file=sys.stderr)
        for statement in statements:
            try:
                self.execute(statement)
            except Exception as exc:
                print(f"오류: {type(exc).__name__}: {exc}", file=sys.stderr)
                self._recover()

    def edit(self) -> None:
        """``$EDITOR`` 로 문장을 편집한다. 저장하고 나오면 그대로 실행한다."""
        import tempfile

        editor = os.environ.get("EDITOR") or os.environ.get("VISUAL")
        if not editor:
            print("EDITOR 환경변수가 없습니다. 예: export EDITOR=vim", file=sys.stderr)
            return

        with tempfile.NamedTemporaryFile(
            "w+", suffix=".sql", encoding="utf-8", delete=False
        ) as handle:
            handle.write(self.buffer)
            path = handle.name
        try:
            subprocess.call([editor, path])
            with open(path, "r", encoding="utf-8") as handle:
                text = handle.read()
        finally:
            os.unlink(path)

        self.buffer = ""
        statements, rest = split_statements(text)
        # 세미콜론 없이 저장했으면 통째로 한 문장으로 본다
        if rest.strip():
            statements.append(rest.strip())
        for statement in statements:
            try:
                self.execute(statement)
            except Exception as exc:
                print(f"오류: {type(exc).__name__}: {exc}", file=sys.stderr)
                self._recover()

    def run_file(self, given: str) -> None:
        """``sql/`` 의 템플릿을 실행한다. ``\\set`` 으로 둔 변수가 그대로 채워진다."""
        path = sqlfile.resolve_query_file(given, getattr(self.args, "sql_dir", None))
        with open(path, "r", encoding="utf-8-sig") as fp:
            text = fp.read()
        rendered = sqlfile.render_query(text, self.variables, path, warn_unused=False)
        statements, rest = split_statements(rendered)
        if rest.strip():
            statements.append(rest)
        for statement in statements:
            print(f"-- {path}", file=sys.stderr)
            self.execute(statement)

    # -- 루프 --
    def feed(self, line: str) -> bool:
        """한 줄을 받아 처리한다. 셸을 계속 돌려도 되면 True 를 돌려준다."""
        stripped = line.strip()
        # spark-shell 습관대로 :paste 도 받는다. 나머지 : 명령은 없다.
        if not self.buffer.strip() and (
            stripped.startswith("\\") or stripped.split()[0:1] == [":paste"]
        ):
            return self.meta(line)

        self.buffer += line + "\n"
        statements, rest = split_statements(self.buffer)
        self.buffer = rest
        for statement in statements:
            try:
                self.execute(statement)
            except SystemExit:
                raise
            except Exception as exc:
                # 셸은 문장 하나가 실패했다고 끝나지 않는다
                print(f"오류: {type(exc).__name__}: {exc}", file=sys.stderr)
                self._recover()
        return True

    def _recover(self) -> None:
        """실패한 트랜잭션을 정리한다. 안 하면 이후 문장이 전부 거부된다."""
        if not self.engine.transactional:
            return
        try:
            self.conn.rollback()
        except Exception:
            pass

    def run(self) -> int:
        self.conn = self.engine.connect()
        readline = self.load_history() if self.interactive else None
        if self.interactive:
            print(
                f"{self.engine.name} 에 붙었습니다. \\? 로 도움말, \\q 로 종료합니다.",
                file=sys.stderr,
            )
        try:
            while True:
                try:
                    line = input(self.prompt()) if self.interactive else input()
                except EOFError:
                    break
                except KeyboardInterrupt:
                    # 입력 중 Ctrl-C 는 쓰던 문장만 버린다. 셸은 살아 있는다.
                    self.buffer = ""
                    print("^C", file=sys.stderr)
                    continue
                if not self.feed(line):
                    break

            if self.buffer.strip():
                print(
                    "세미콜론 없이 끝난 입력이 있어 실행하지 않았습니다:\n"
                    f"  {self.buffer.strip()[:60]}",
                    file=sys.stderr,
                )
        finally:
            self.save_history(readline)
            try:
                self.conn.close()
            except Exception:
                pass
        return 0


HELP = """\
메타 명령

  \\q                 종료 (Ctrl-D 도 같음)
  \\?                 이 도움말
  \\i 파일.sql        sql/ 의 템플릿 실행 (\\set 변수가 채워집니다)
  \\set               지금 설정된 변수 보기
  \\set 이름 값       템플릿 변수 지정
  \\unset 이름        변수 지우기
  \\o 파일.csv        결과를 CSV 파일로 보내기
  \\o                 결과를 화면으로 되돌리기
  \\timing            문장별 소요 시간 표시 켜기/끄기
  \\x                 세로 출력 켜기/끄기 (컬럼이 많을 때)
  \\dt [패턴]         테이블 목록
  \\d 이름            컬럼 정보 (Greenplum 은 분산키도)
  \\ddl 이름          테이블 생성문 (CREATE TABLE)
  \\e                 $EDITOR 로 편집한 뒤 실행
  \\paste (:paste)    긴 쿼리 붙여넣기. Ctrl-D 또는 \\. 로 끝냅니다.
  \\pager auto|on|off  긴 결과를 페이저로 넘길지
  \\begin             트랜잭션 시작 (Greenplum)
  \\commit            반영
  \\rollback          되돌리기

문장은 세미콜론(;) 으로 끝냅니다. 여러 줄로 이어 써도 됩니다.
기본은 문장마다 바로 반영(autocommit)이며, 묶으려면 \\begin 을 씁니다.
실행 중인 문장은 Ctrl-C 로 취소합니다.
Tab 을 누르면 메타 명령, 테이블 이름, SQL 키워드를 완성합니다."""
