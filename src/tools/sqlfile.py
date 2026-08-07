"""``sql/`` 의 .sql 파일을 찾아 읽고 Jinja 템플릿 변수를 채운다.

bin/ 아래 여러 스크립트가 같은 규칙으로 쿼리를 읽도록 여기 모아둔다. 어느
스크립트에서든 ``--query-file`` 은 이름만 주면 SQL 디렉터리에서 찾고, ``--var``
로 준 값이 ``{{ 변수 }}`` 자리에 들어간다.
"""

import argparse
import os
import sys
from typing import Dict, Optional, Sequence

from . import appconfig

#: 설정의 sql 섹션에서 읽어 쓰는 키
SQL_SETTINGS = ("dir",)
SQL_PATH_SETTINGS = ("dir",)


def add_query_arguments(
    parser: argparse.ArgumentParser, group_title: str = "쿼리", required: bool = True
) -> None:
    """``--query`` / ``--query-file`` / ``--var`` / ``--sql-dir`` 을 붙인다.

    ``required=False`` 는 대화형 셸처럼 쿼리를 나중에 받는 경우를 위한 것이다.
    그때는 호출부가 직접 검사해야 한다.
    """
    query = parser.add_argument_group(group_title)
    source = query.add_mutually_exclusive_group(required=required)
    source.add_argument("-q", "--query", help="실행할 SQL 문")
    source.add_argument(
        "-f",
        "--query-file",
        help=f"SQL 문이 담긴 .sql 파일. 이름만 주면 {appconfig.SQL_DIR} 에서 찾습니다.",
    )
    query.add_argument(
        "-V",
        "--var",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="쿼리 템플릿에 채울 변수 (예: --var dt=2026-08-01). 여러 번 지정 가능",
    )
    query.add_argument(
        "--sql-dir",
        metavar="DIR",
        help=f"이름만 준 --query-file 을 찾을 디렉터리 (설정의 sql.dir, 기본 {appconfig.SQL_DIR})",
    )


def load_sql_settings(args: argparse.Namespace) -> Dict[str, object]:
    """설정 파일의 sql 섹션을 읽는다.

    없어도 오류가 아니다. 적지 않으면 저장소의 sql/ 을 쓴다.
    """
    path = appconfig.resolve_config_path(args)
    return appconfig.load_section(path, "sql", SQL_SETTINGS, path_keys=SQL_PATH_SETTINGS)


def resolve_sql_dir(args: argparse.Namespace, sql_config: Optional[Dict[str, object]]) -> str:
    """SQL 디렉터리를 정한다. 명령행 > 설정 > 저장소의 sql/ 순.

    ``--sql-dir`` 은 셸에서 친 경로라 작업 디렉터리 기준으로 그대로 쓰고, 설정의
    ``sql.dir`` 은 설정 파일 위치 기준으로 이미 풀려서 넘어온다.
    """
    return appconfig.pick(
        args.sql_dir, (sql_config or {}).get("dir"), str(appconfig.SQL_DIR)
    )


def parse_variables(items: Sequence[str]) -> Dict[str, str]:
    """``--var KEY=VALUE`` 목록을 딕셔너리로 바꾼다."""
    variables: Dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"--var 는 KEY=VALUE 형식이어야 합니다: {item!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise SystemExit(f"--var 의 이름이 비어 있습니다: {item!r}")
        variables[key] = value
    return variables


def resolve_query_file(given: str, sql_dir: Optional[str] = None) -> str:
    """``--query-file`` 경로를 푼다.

    파일 이름만 주면 SQL 디렉터리에서 찾는다. 경로 구분자가 들어 있거나 작업
    디렉터리에 그 파일이 실제로 있으면 준 그대로 쓴다. 어느 쪽으로도 찾지 못하면
    그 디렉터리에 무엇이 있는지 함께 알려준다.
    """
    if os.path.isfile(given):
        return given

    directory = str(sql_dir) if sql_dir else str(appconfig.SQL_DIR)
    candidate = os.path.join(directory, given)
    if os.sep not in given and os.path.isfile(candidate):
        return candidate

    message = f"쿼리 파일을 찾을 수 없습니다: {given}"
    if not os.path.isdir(directory):
        raise SystemExit(f"{message}\n  SQL 디렉터리가 없습니다: {directory}")

    available = sorted(n for n in os.listdir(directory) if n.endswith(".sql"))
    if available:
        listing = "\n".join(f"    {name}" for name in available)
        message += f"\n  {directory} 에 있는 파일:\n{listing}"
    else:
        message += f"\n  {directory} 에 .sql 파일이 없습니다."
    raise SystemExit(message)


def render_query(
    sql: str, variables: Dict[str, str], source: str, warn_unused: bool = True
) -> str:
    """SQL을 Jinja 템플릿으로 보고 변수를 채운다.

    정의되지 않은 변수는 **오류** 다. Jinja 기본값은 빈 문자열로 조용히 치환하는
    것인데, SQL에서는 ``WHERE dt = ''`` 같은 문장이 조용히 만들어져 0건을 돌려주는
    편이 훨씬 위험하다. 오타를 바로 잡아내는 편이 낫다.

    HTML이 아니므로 자동 이스케이프는 켜지 않는다. 값은 SQL에 그대로 들어가므로,
    바깥에서 온 값을 넘긴다면 템플릿 쪽에서 따옴표와 검증을 책임져야 한다.
    """
    if "{{" not in sql and "{%" not in sql:
        # 템플릿 문법이 없으면 Jinja를 부르지 않는다. jinja2 미설치 환경에서도
        # 평범한 .sql 파일은 그대로 돌아간다.
        # 셸에서는 \set 해둔 변수가 있어도 평범한 문장을 계속 치는 것이 정상이라
        # 이 경고를 끈다. 한 번 실행하는 쪽에서는 오타를 잡아주는 신호가 된다.
        if variables and warn_unused:
            print(
                f"경고: {source} 에 템플릿 변수가 없어 --var 값이 쓰이지 않았습니다.",
                file=sys.stderr,
            )
        return sql

    try:
        from jinja2 import StrictUndefined, Template
        from jinja2 import TemplateError, UndefinedError
    except ImportError:
        raise SystemExit(
            "템플릿 변수를 쓰려면 Jinja2 가 필요합니다.\n"
            "    pip install Jinja2"
        )

    class OptionalUndefined(StrictUndefined):
        """``{% if x %}`` 로 물어보는 것은 되고, ``{{ x }}`` 로 출력하면 오류.

        StrictUndefined 는 참/거짓 판정까지 막아서 선택적 필터를 못 쓴다.
        여기서는 "값을 SQL에 넣는" 경우만 막으면 된다.
        """

        def __bool__(self) -> bool:
            return False

    try:
        return Template(sql, undefined=OptionalUndefined, keep_trailing_newline=True).render(
            **variables
        )
    except UndefinedError as exc:
        given = ", ".join(sorted(variables)) or "(없음)"
        raise SystemExit(
            f"{source} 의 템플릿 변수를 채우지 못했습니다: {exc}\n"
            f"  지금 준 변수: {given}\n"
            "  --var KEY=VALUE 로 지정하세요."
        )
    except TemplateError as exc:
        raise SystemExit(f"{source} 의 템플릿 문법이 잘못되었습니다: {exc}")


def read_query(args: argparse.Namespace) -> str:
    """``--query`` 또는 ``--query-file`` 에서 SQL을 읽는다.

    파일 내용은 템플릿을 채운 뒤 **그대로** 실행한다. 문장을 쪼개거나 세미콜론을
    떼어내지 않는다. 여러 줄로 이어진 쿼리도 줄바꿈째 그대로 넘어간다.

    파일을 읽을 때만 ``utf-8-sig`` 를 쓴다. 이건 SQL을 고치는 게 아니라 인코딩을
    제대로 해석하는 것이다. 윈도우 편집기로 저장한 .sql 앞머리에는 BOM(U+FEFF)이
    붙는데, utf-8로 읽으면 이 문자가 쿼리 첫 글자 앞에 남아 syntax error가 난다.
    utf-8-sig는 BOM이 있으면 벗기고 없으면 utf-8과 똑같이 동작한다.
    """
    variables = parse_variables(args.var)
    if args.query_file:
        path = resolve_query_file(args.query_file, getattr(args, "sql_dir", None))
        with open(path, "r", encoding="utf-8-sig") as fp:
            return render_query(fp.read(), variables, path)
    return render_query(args.query, variables, "--query")

