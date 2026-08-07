"""Impala에 쿼리를 실행해 표나 CSV로 보여주고, 구간별 소요 시간을 알려준다.

TLS(SSL) 위에서 LDAP 인증(auth_mechanism=PLAIN)으로 접속하는 구성을 전제로 한다.

접속 정보는 이 저장소의 config/config.properties(impala.*)에서 자동으로 읽는다. 명령행으로 준 값이
항상 우선하므로, 설정을 채워두면 쿼리만 주면 된다.

``-o`` 를 주면 CSV 파일로 쓰고, 없으면 표로 보여준다. 표로 볼 때는 ``--max-rows``
만큼만 받고 멈춘다. 100행을 보여주려고 수백만 행을 받아오면 배치로 나눠 읽어
메모리를 아끼는 의미가 없다.

    pip install impyla pure-sasl thrift-sasl PyYAML
    # "Failed building wheel for pure-sasl" 이 나면
    #     pip install --use-pep517 pure-sasl thrift-sasl

    # 표로 확인
    bin/impala-shell -f daily_orders.sql --var dt=2026-08-01

    # CSV 파일로 저장
    bin/impala-shell -f daily_orders.sql --var dt=2026-08-01 -o orders.csv

    # 설정을 무시하고 전부 명령행으로
    export IMPALA_PASSWORD='...'
    bin/impala-shell --no-config \
        --host impala.example.com \
        --user etl_user \
        --ca-cert /etc/ssl/certs/impala-ca.pem \
        --query-file daily_orders.sql --output orders.csv.gz --gzip

``--query-file`` 은 이름만 주면 저장소의 sql/ 에서 찾는다. .sql 파일은 Jinja
템플릿이라 ``{{ 변수 }}`` 자리에 ``--var`` 로 준 값이 들어간다.

외부 의존성은 impyla, PyYAML, Jinja2 이고 같은 디렉터리의 appconfig 모듈을 함께
쓴다. PyYAML 은 설정 파일을 읽을 때만, Jinja2 는 SQL에 템플릿 문법이 있을 때만
임포트하므로 둘 다 쓰지 않는다면 impyla 만 있어도 된다.

비밀번호는 명령행 인자로 받지 않는다. ps로 다른 사용자에게 노출되기 때문에
설정 파일(보통 ``${IMPALA_PASSWORD}`` 참조), 환경변수, 대화형 입력으로만 받는다.
"""

import argparse
import contextlib
import csv
import getpass
import gzip
import importlib.util
import os
import sys
import time
import unicodedata
from typing import Any, Dict, Iterator, List, Optional, Sequence, TextIO

from . import appconfig
from . import progress
from . import sqlfile
from . import shell
from . import table
from .table import open_output
from .progress import PhaseTimer, display_width, pad
from .sqlfile import (  # 다른 스크립트와 공유하는 쿼리 읽기
    load_sql_settings,
    parse_variables,
    read_query,
    render_query,
    resolve_query_file,
)

_INSTALL_HINT = (
    "impyla가 설치되어 있지 않습니다.\n"
    "    pip install impyla\n"
    "  LDAP/Kerberos 인증에는 pure-sasl, thrift-sasl도 필요합니다."
)


def _import_hint(exc: ImportError) -> str:
    """impyla 임포트 실패 원인을 짚어 준다.

    'impala' 라는 이름의 파일이나 디렉터리가 있으면 진짜 패키지를 가려버리는데,
    이때 나오는 "'impala' is not a package" 메시지만 봐서는 원인을 알기 어렵다.
    """
    try:
        spec = importlib.util.find_spec("impala")
    except (ImportError, ValueError):
        spec = None

    if spec is None:
        return _INSTALL_HINT
    if spec.submodule_search_locations is None:
        # 패키지가 아니라 단일 모듈로 잡혔다 = 같은 이름의 .py 파일이 가리고 있다
        return (
            f"'{spec.origin}' 파일이 impyla 패키지를 가리고 있습니다.\n"
            "  이 파일의 이름을 바꾸거나 다른 디렉터리로 옮긴 뒤 다시 실행하세요.\n"
            "  (파이썬은 현재 디렉터리를 먼저 뒤지므로, impala.py 라는 파일이 있으면\n"
            "   설치된 impyla 대신 그 파일을 가져옵니다.)"
        )
    if spec.origin is None:
        # __init__.py 없는 impala/ 디렉터리가 네임스페이스 패키지로 잡힌 경우
        return (
            f"{list(spec.submodule_search_locations)} 디렉터리가 impyla 패키지를 "
            "가리고 있습니다.\n  디렉터리 이름을 바꾸거나 다른 곳으로 옮긴 뒤 다시 "
            "실행하세요.\n  impyla가 아직 없다면 함께 설치하세요: pip install impyla"
        )
    return f"impyla를 불러오지 못했습니다: {exc}\n  {_INSTALL_HINT}"


def import_impala_dbapi() -> Any:
    """impyla의 ``dbapi`` 모듈을 가져온다. 실패하면 원인을 설명하는 오류를 낸다."""
    try:
        from impala import dbapi  # noqa: F401
    except ImportError as exc:
        raise ImportError(_import_hint(exc)) from exc
    return dbapi


class CyrusSASLClient:
    """Cyrus SASL(``sasl`` 패키지)을 impyla가 기대하는 인터페이스로 감싼다.

    impyla는 SASL 클라이언트로 ``puresasl`` 을 고정해서 쓴다. Cyrus SASL을 쓰려면
    ``impala.sasl_compat.PureSASLClient`` 를 이 클래스로 바꿔치기해야 한다.
    ``get_transport`` 가 함수 안에서 임포트하므로 접속 전에 바꾸면 반영된다.

    참고: ``sasl`` 패키지는 C 확장이라 Python 3.11 이상에서는 빌드가 되지 않는다
    (saslwrapper가 3.11에서 없어진 longintrepr.h 를 참조한다). 대부분의 환경에서는
    기본값인 puresasl 을 그대로 쓰면 된다.
    """

    def __init__(self, host: str, username: Any = None, password: Any = None,
                 service: Any = None) -> None:
        import sasl  # 없으면 호출부에서 안내 메시지를 낸다

        client = sasl.Client()
        client.setAttr("host", host)
        client.setAttr("service", service or "impala")
        if username is not None:
            client.setAttr("username", username)
        if password is not None:
            client.setAttr("password", password)
        client.init()
        self._client = client

    def start(self, mechanism: Any) -> Any:
        return self._client.start(mechanism)

    def step(self, challenge: Any) -> Any:
        return self._client.step(challenge)

    def encode(self, incoming: Any) -> Any:
        return self._client.encode(incoming)

    def decode(self, outgoing: Any) -> Any:
        return self._client.decode(outgoing)

    def getError(self) -> Any:
        return self._client.getError()


def use_cyrus_sasl() -> None:
    """impyla가 puresasl 대신 Cyrus SASL을 쓰도록 바꾼다."""
    if importlib.util.find_spec("sasl") is None:
        raise ImportError(
            "--sasl-backend sasl 을 쓰려면 Cyrus SASL 바인딩이 필요합니다.\n"
            "    pip install sasl        # libsasl2-dev 가 먼저 설치돼 있어야 합니다\n"
            "  다만 sasl 패키지는 Python 3.11 이상에서는 빌드되지 않습니다.\n"
            "  (saslwrapper가 3.11에서 없어진 longintrepr.h 를 참조합니다)\n"
            "  기본값인 --sasl-backend puresasl 을 쓰세요. impyla의 기본 동작입니다."
        )

    import impala.sasl_compat

    impala.sasl_compat.PureSASLClient = CyrusSASLClient


def check_auth_dependencies(auth_mechanism: str) -> None:
    """인증 방식에 필요한 SASL 패키지가 있는지 미리 확인한다.

    impyla는 ``auth_mechanism`` 이 NOSASL이 아니면 접속하는 순간에야
    ``thrift_sasl`` / ``puresasl`` 을 임포트한다. 그래서 패키지가 없으면 접속 직전에
    맥락 없는 ModuleNotFoundError가 튀어나온다. 여기서 미리 확인해 알려준다.
    """
    if (auth_mechanism or "NOSASL").upper() == "NOSASL":
        return

    missing = [
        name
        for name in ("thrift_sasl", "puresasl")
        if importlib.util.find_spec(name) is None
    ]
    if not missing:
        return

    raise ImportError(
        f"auth_mechanism={auth_mechanism} 인증에는 SASL 패키지가 필요한데 "
        f"{', '.join(missing)} 이(가) 없습니다.\n"
        "    pip install pure-sasl thrift-sasl\n"
        "  데비안/우분투에서 'Failed building wheel for pure-sasl' 이 나면:\n"
        "    pip install --use-pep517 pure-sasl thrift-sasl"
    )


#: 보고서에 표시할 구간 순서 (실제 실행 흐름대로)
PHASES = ("Impala 접속", "쿼리 실행 요청", "첫 배치 대기", "데이터 수신", "CSV 쓰기", "표 출력")


#: impyla에서 LDAP(사용자/비밀번호) 인증을 뜻하는 값
AUTH_MECHANISM = "PLAIN"

#: 인증이 필요 없는 방식
NO_AUTH = "NOSASL"


def build_connect_kwargs(args: argparse.Namespace, password: Optional[str]) -> Dict[str, Any]:
    """접속 인자를 만든다. 기본값은 TLS + LDAP.

    - auth_mechanism='PLAIN' 이 impyla에서 LDAP(사용자/비밀번호) 인증을 뜻한다.
      내부적으로는 SASL PLAIN으로 처리되며, 'LDAP' 을 줘도 같은 경로를 탄다.
    - use_ssl=True 로 TLS를 켜고, ca_cert로 서버 인증서를 검증한다.
      ca_cert를 주면 impyla가 check_hostname=True, CERT_REQUIRED 로 설정한다.
      주지 않으면 암호화는 되지만 인증서 검증은 하지 않는다.
    - 바이너리(21050)가 아니라 HTTP 엔드포인트(보통 28000)를 쓰는 환경이라면
      use_http_transport 를 켜야 한다. 안 그러면 Thrift가 EOF로 끊긴다.
    """
    kwargs: Dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "database": args.database,
        "auth_mechanism": args.auth_mechanism,
        "use_ssl": not args.no_ssl,
    }
    if args.auth_mechanism != NO_AUTH:
        kwargs["user"] = args.user
        kwargs["password"] = password
    if args.auth_mechanism == "GSSAPI":
        kwargs["kerberos_service_name"] = args.kerberos_service_name

    # None을 그대로 넘기면 impyla 버전에 따라 동작이 갈리므로 값이 있을 때만 넣는다
    if args.ca_cert:
        kwargs["ca_cert"] = args.ca_cert
    if args.timeout is not None:
        kwargs["timeout"] = args.timeout
    if args.http_transport:
        kwargs["use_http_transport"] = True
        kwargs["http_path"] = args.http_path
    return kwargs


def transport_error_hint(args: argparse.Namespace) -> str:
    """Thrift 전송 오류(EOF 등)가 났을 때 점검할 것들을 알려준다.

    "TSocket read 0 bytes" 나 "end of file" 은 서버가 핸드셰이크 도중 연결을
    끊었다는 뜻이다. 대부분 포트·전송 방식·TLS·인증 방식 중 하나가 서버 설정과
    어긋나서 생긴다. 메시지 자체로는 어느 쪽인지 알 수 없으므로 후보를 나열한다.
    """
    lines = [
        "서버가 핸드셰이크 도중 연결을 끊었습니다(EOF). 대개 아래 중 하나입니다.",
        "",
        f"  현재 설정: {args.host}:{args.port} "
        f"/ {'HTTP' if args.http_transport else '바이너리'} 전송"
        f" / TLS {'끄기' if args.no_ssl else '켜기'}"
        f" / 인증 {args.auth_mechanism}",
        "",
        "  1) 전송 방식과 포트가 맞는지",
        "     - 21050: 바이너리 HS2 (기본)",
        "     - 28000: HTTP HS2 → --http-transport 를 켜야 합니다",
        "     - 21000은 예전 beeswax, 25000은 웹 UI라 여기에 붙으면 EOF가 납니다",
    ]
    if not args.http_transport:
        lines.append("     지금 바이너리로 붙고 있으니 --http-transport 로 한 번 바꿔보세요.")
    else:
        lines.append(f"     지금 HTTP path는 '{args.http_path}' 입니다. 보통 cliservice 입니다.")

    lines += [
        "",
        "  2) TLS 설정이 서버와 맞는지",
    ]
    if args.no_ssl:
        lines.append("     지금 TLS를 끄고 있습니다. 서버가 TLS를 요구하면 --no-ssl 을 빼세요.")
    else:
        lines.append("     지금 TLS를 켜고 있습니다. 서버가 평문이면 --no-ssl 을 주세요.")

    lines += [
        "",
        "  3) 인증 방식이 서버와 맞는지",
        f"     지금 {args.auth_mechanism} 입니다. "
        "인증이 없는 서버면 --auth-mechanism NOSASL,",
        "     Kerberos면 GSSAPI 를 쓰세요.",
        "",
        "  4) 앞단에 로드밸런서나 프록시가 있다면 그쪽이 끊었을 수도 있습니다.",
        "",
        "  impala-shell 로 같은 조건이 되는지 먼저 확인하면 범위를 빨리 좁힐 수 있습니다.",
    ]
    return "\n".join(lines)



def query_error_hint(query: str, exc: Exception) -> str:
    """쿼리 실행이 문법 오류로 실패했을 때 짚어볼 것들을 알려준다."""
    message = str(exc).lower()
    if not any(k in message for k in ("syntax", "parseexception", "analysisexception")):
        return ""

    hints = [
        "SQL은 파일에 있는 그대로 서버에 보냅니다. --debug 로 실제로 보낸 내용을 볼 수 있습니다.",
    ]
    stripped = query.rstrip()
    if stripped.endswith(";"):
        hints.append(
            "끝에 세미콜론이 있습니다. Impala는 HS2로 받은 문장의 세미콜론을 "
            "거부할 수 있으니 빼고 다시 해보세요."
        )
    if ";" in stripped[:-1]:
        hints.append(
            "세미콜론이 문장 중간에도 있습니다. 파일에 문장이 여러 개면 "
            "한 번에 하나씩 나눠 실행해야 합니다."
        )
    return "\n".join(f"  - {h}" for h in hints)


def parse_session_settings(items: Sequence[str]) -> Dict[str, str]:
    """``KEY=VALUE`` 목록을 세션 설정 딕셔너리로 바꾼다."""
    settings: Dict[str, str] = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key.strip():
            raise SystemExit(f"--set 은 KEY=VALUE 형식이어야 합니다: {item!r}")
        settings[key.strip()] = value.strip()
    return settings


def resolve_password(args: argparse.Namespace) -> str:
    """설정 파일, 환경변수, 대화형 입력 순으로 비밀번호를 얻는다.

    설정의 impala.password 는 보통 ``${IMPALA_PASSWORD}`` 형태로 환경변수를
    가리킨다. 어느 경로든 명령행 인자로는 받지 않는다. ps 로 다른 사용자에게
    보이기 때문이다.
    """
    password = getattr(args, "config_password", None) or os.environ.get(args.password_env)
    if password:
        return password
    if sys.stdin.isatty():
        return getpass.getpass(f"{args.user}@{args.host} 비밀번호: ")
    raise SystemExit(
        f"비밀번호를 찾을 수 없습니다. 환경변수 {args.password_env} 를 설정하거나 "
        "설정의 impala.password 를 채우세요."
    )


def make_writer(
    handle: TextIO, delimiter: str, quote: bool, escapechar: Optional[str]
) -> Any:
    """CSV writer를 만든다.

    따옴표를 쓰지 않으면(기본) 값 안에 구분자나 줄바꿈이 들어 있을 때 이스케이프할
    문자가 필요하다. 없으면 csv 모듈이 그 행에서 예외를 내고 내보내기가 중단된다.
    구분자가 값에 등장하지 않는 한 이스케이프 문자는 출력에 나타나지 않는다.
    """
    if quote:
        return csv.writer(handle, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
    # quotechar=None 이어야 값 안의 큰따옴표를 건드리지 않는다.
    # 그냥 두면 따옴표를 쓰지 않는데도 " 앞에 이스케이프 문자가 붙는다.
    return csv.writer(
        handle,
        delimiter=delimiter,
        quoting=csv.QUOTE_NONE,
        quotechar=None,
        escapechar=escapechar or None,
    )


def show_table(
    cursor: Any,
    query: str,
    timer: PhaseTimer,
    batch_size: int,
    max_rows: int,
    null_string: str,
    reporter: Optional[progress.Progress] = None,
) -> None:
    """쿼리를 실행해 결과를 표로 보여준다.

    보여줄 만큼만 받고 멈춘다. 100행을 보려고 수백만 행을 받아오면 스트리밍으로
    메모리를 아끼는 의미가 없다.
    """
    with timer.measure("쿼리 실행 요청"):
        cursor.arraysize = batch_size
        cursor.execute(query)

    columns = [desc[0].split(".")[-1] for desc in (cursor.description or [])]

    with timer.measure("첫 배치 대기"):
        rows, truncated = table.fetch_limited(
            cursor,
            max_rows,
            batch_size,
            on_batch=reporter.update if reporter is not None else None,
        )
    if reporter is not None:
        reporter.finish()

    with timer.measure("표 출력"):
        table.print_result(columns, rows, truncated, null_string or "NULL")


def export(
    cursor: Any,
    query: str,
    handle: TextIO,
    timer: PhaseTimer,
    batch_size: int,
    delimiter: str,
    null_string: str,
    write_header: bool,
    reporter: Optional[progress.Progress] = None,
    quote: bool = False,
    escapechar: Optional[str] = "\\",
) -> int:
    """쿼리를 실행해 CSV로 쓰고 행 수를 돌려준다."""
    with timer.measure("쿼리 실행 요청"):
        cursor.arraysize = batch_size
        cursor.execute(query)

    columns = [desc[0].split(".")[-1] for desc in (cursor.description or [])]
    writer = make_writer(handle, delimiter, quote, escapechar)

    if write_header and columns:
        with timer.measure("CSV 쓰기"):
            writer.writerow(columns)

    total = 0
    first_batch = True
    while True:
        # 첫 배치는 Impala가 결과를 만들어내기까지 기다리는 시간이라 따로 잰다
        phase = "첫 배치 대기" if first_batch else "데이터 수신"
        with timer.measure(phase):
            rows = cursor.fetchmany(batch_size)
        first_batch = False

        if not rows:
            break

        with timer.measure("CSV 쓰기"):
            # csv 모듈은 None을 빈 문자열로 쓴다. NULL을 따로 표시하려면 치환한다.
            if null_string:
                rows = [[null_string if v is None else v for v in row] for row in rows]
            writer.writerows(rows)

        total += len(rows)
        if reporter is not None:
            reporter.update(total)

    if reporter is not None:
        reporter.finish()
    return total


def human(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size) < 1024 or unit == "GB":
            return f"{size:,.1f}{unit}"
        size /= 1024
    return f"{size:,.1f}GB"


#: 설정의 impala 섹션에서 읽어 쓰는 키. 나머지 키는 무시한다.
IMPALA_SETTINGS = (
    "host",
    "port",
    "database",
    "user",
    "password",
    "auth_mechanism",
    "kerberos_service_name",
    "use_ssl",
    "ca_cert",
    "timeout",
    "session_settings",
)

#: 파일 경로로 다루는 키. 상대 경로는 설정 파일 위치를 기준으로 푼다.
IMPALA_PATH_SETTINGS = ("ca_cert",)


def load_impala_settings(args: argparse.Namespace) -> Dict[str, Any]:
    """설정 파일의 impala 섹션을 읽는다.

    ``--config`` 로 파일을 직접 지정했는데 impala 섹션이 없으면 오타일 가능성이
    높으므로 알린다. 기본 파일이라면 다른 스크립트용 설정만 들어 있을 수 있으니
    넘어간다.
    """
    path = appconfig.resolve_config_path(args)
    return appconfig.load_section(
        path,
        "impala",
        IMPALA_SETTINGS,
        required=bool(args.config_dir),
        path_keys=IMPALA_PATH_SETTINGS,
    )


def apply_config(
    args: argparse.Namespace,
    config: Dict[str, Any],
    parser: argparse.ArgumentParser,
    sql_config: Optional[Dict[str, Any]] = None,
) -> None:
    """설정 파일 값을 args 에 채운다. 명령행으로 준 값이 항상 우선한다.

    설정에 없고 명령행에도 없는 값은 각 항목의 기본값으로 떨어진다. 접속에 반드시
    필요한 host 와 user 만 끝까지 비어 있으면 오류다.
    """
    args.host = appconfig.pick(args.host, config["host"])
    args.port = int(appconfig.pick(args.port, config["port"], 21050))
    args.database = appconfig.pick(args.database, config["database"], "default")
    args.user = appconfig.pick(args.user, config["user"])
    args.auth_mechanism = appconfig.pick(
        args.auth_mechanism, config["auth_mechanism"], AUTH_MECHANISM
    )
    args.kerberos_service_name = appconfig.pick(
        args.kerberos_service_name, config["kerberos_service_name"], "impala"
    )
    args.ca_cert = appconfig.pick(args.ca_cert, config["ca_cert"])
    args.timeout = appconfig.pick(args.timeout, config["timeout"])

    args.sql_dir = sqlfile.resolve_sql_dir(args, sql_config)

    # --no-ssl 은 store_true 라 "주지 않음"과 False 를 구분할 수 없다. 플래그를
    # 주지 않았을 때만 설정의 use_ssl 을 본다.
    if not args.no_ssl and config["use_ssl"] is False:
        args.no_ssl = True

    # 설정의 session_settings 를 --set 보다 앞에 둬서 명령행이 덮어쓰게 한다
    settings = config["session_settings"]
    if isinstance(settings, dict):
        args.set = [f"{k}={v}" for k, v in settings.items()] + list(args.set)

    # 비밀번호는 설정 > 환경변수 > 대화형 입력 순으로 찾는다
    args.config_password = config["password"] or None

    if args.auth_mechanism != NO_AUTH and not args.user:
        parser.error("사용자를 알 수 없습니다. -u/--user 를 주거나 설정의 impala.user 를 채우세요.")
    if not args.host:
        parser.error("호스트를 알 수 없습니다. --host 를 주거나 설정의 impala.host 를 채우세요.")

    # 없는 인증서를 그대로 넘기면 impyla가 알아보기 어려운 SSL 오류를 낸다.
    # 조용히 무시하면 검증 없이 접속하게 되므로, 여기서 경로를 짚어 알린다.
    if args.ca_cert and not args.no_ssl and not os.path.isfile(args.ca_cert):
        parser.error(
            f"CA 인증서를 찾을 수 없습니다: {args.ca_cert}\n"
            "  설정의 impala.ca_cert 경로를 고치거나, 그 위치에 인증서를 두세요.\n"
            "  인증서 검증 없이 접속하려면 impala.ca_cert 를 비우세요."
        )


def make_engine(args: argparse.Namespace, dbapi: Any, password: Optional[str]) -> "shell.Engine":
    """대화형 셸이 쓸 엔진 어댑터. 접속 인자는 기존 것을 그대로 재사용한다."""

    def connect() -> Any:
        conn = dbapi.connect(**build_connect_kwargs(args, password))
        settings = parse_session_settings(args.set)
        if settings:
            cursor = conn.cursor()
            try:
                for key, value in settings.items():
                    cursor.execute(f"SET {key}={value}")
            finally:
                cursor.close()
        return conn

    def cancel(conn: Any, cursor: Any) -> None:
        # impyla 커서는 진행 중인 작업을 서버에 취소 요청할 수 있다
        cursor.cancel_operation()

    def list_tables_sql(pattern: Optional[str]) -> str:
        return f"SHOW TABLES LIKE '{pattern}'" if pattern else "SHOW TABLES"

    def describe_sql(target: str) -> str:
        return f"DESCRIBE {target}"

    def ddl_sql(target: str) -> str:
        # Impala 는 생성문을 그대로 돌려준다. 조립할 필요가 없다.
        return f"SHOW CREATE TABLE {target}"

    def table_names_sql() -> str:
        # SHOW TABLES 는 현재 데이터베이스의 이름만 준다
        return "SHOW TABLES"

    # Impala 에는 트랜잭션이 없다. autocommit 을 켜고 말고 할 것이 없다.
    return shell.Engine(
        name="Impala",
        label=args.database or "impala",
        connect=connect,
        cancel=cancel,
        list_tables_sql=list_tables_sql,
        describe_sql=describe_sql,
        table_names_sql=table_names_sql,
        ddl_sql=ddl_sql,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        # 셸 래퍼로 들어오면 그 이름을 보여준다
        prog=os.environ.get("PROG_NAME", "bin/impala-shell"),
        description="Impala 쿼리 결과를 CSV로 저장하고 구간별 소요 시간을 표시합니다.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예시:\n"
            "  # sql/ 의 템플릿에 변수를 채워 표로 확인\n"
            "  bin/impala-shell -f daily_orders.sql -V dt=2026-08-01\n"
            "\n"
            "  # CSV 파일로 저장\n"
            "  bin/impala-shell -f daily_orders.sql -V dt=2026-08-01 -o orders.csv\n"
            "\n"
            "  # 쿼리를 직접 주고 gzip으로 저장\n"
            "  bin/impala-shell -q \"SELECT * FROM sales.orders\" -o orders.csv.gz --gzip\n"
            "\n"
            "  # 설정을 무시하고 접속 정보를 전부 명령행으로\n"
            "  bin/impala-shell --no-config --host impala.example.com -u etl_user \\\n"
            "                   -q \"SELECT 1\" -o out.csv\n"
            "\n"
            f"접속 정보는 {appconfig.DEFAULT_CONFIG} 의\n"
            "impala 섹션에서 자동으로 읽습니다. 아래 인자를 주면 그 값이 우선합니다.\n"
            "\n"
            "비밀번호는 인자로 받지 않습니다. 설정의 impala.password, 환경변수\n"
            "(기본 IMPALA_PASSWORD), 대화형 입력 순으로 찾습니다.\n"
        ),
    )
    appconfig.add_config_arguments(parser)
    progress.add_progress_argument(parser)

    # 설정 파일에서 채울 수 있는 값은 기본값을 None 으로 둔다. 그래야 사용자가
    # 직접 준 값과 argparse 기본값을 구분해 우선순위를 매길 수 있다.
    conn = parser.add_argument_group("접속 (TLS + LDAP)")
    conn.add_argument("--host", help="Impala 데몬 호스트")
    conn.add_argument("--port", type=int, help="기본 21050")
    conn.add_argument("-d", "--database")
    conn.add_argument("-u", "--user", help="LDAP 사용자")
    conn.add_argument(
        "--password-env",
        default="IMPALA_PASSWORD",
        metavar="ENV",
        help="비밀번호를 담은 환경변수 이름 (기본 IMPALA_PASSWORD)",
    )
    conn.add_argument("--ca-cert", help="서버 인증서 검증에 쓸 CA 인증서 경로")
    conn.add_argument("--timeout", type=int, help="접속 타임아웃(초)")
    conn.add_argument(
        "--auth-mechanism",
        choices=["PLAIN", "LDAP", "NOSASL", "GSSAPI"],
        help=f"PLAIN/LDAP=LDAP 인증(기본 {AUTH_MECHANISM}), NOSASL=인증 없음, GSSAPI=Kerberos",
    )
    conn.add_argument(
        "--kerberos-service-name",
        help="GSSAPI일 때 쓸 서비스명 (기본 impala)",
    )
    conn.add_argument("--no-ssl", action="store_true", help="TLS를 끄고 평문으로 접속")
    conn.add_argument(
        "--sasl-backend",
        default="puresasl",
        choices=["puresasl", "sasl"],
        help="SASL 구현. puresasl(기본, impyla 기본값) 또는 sasl(Cyrus, Python 3.10 이하만)",
    )
    conn.add_argument(
        "--debug",
        action="store_true",
        help="SASL 핸드셰이크를 포함한 impyla 디버그 로그 출력",
    )
    conn.add_argument(
        "--http-transport",
        action="store_true",
        help="HS2 HTTP 엔드포인트(보통 28000)로 접속. 바이너리로 붙어 EOF가 나면 이걸 켜세요.",
    )
    conn.add_argument(
        "--http-path",
        default="cliservice",
        help="HTTP 전송일 때의 경로 (기본 cliservice)",
    )

    # 대화형 셸에서는 쿼리를 나중에 받으므로 여기서 강제하지 않는다
    sqlfile.add_query_arguments(parser, required=False)
    query = parser.add_argument_group("세션")
    query.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="대화형 셸을 엽니다. 이때는 -q/-f 를 주지 않아도 됩니다.",
    )
    query.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="세션 설정 (예: --set MEM_LIMIT=8g). 여러 번 지정 가능",
    )

    out = parser.add_argument_group("출력")
    out.add_argument(
        "-o", "--output", help="저장할 CSV 경로 (생략하면 표로 출력)"
    )
    out.add_argument("--gzip", action="store_true", help="gzip으로 압축해 저장")
    out.add_argument("--delimiter", default="`", help="컬럼 구분자 (기본 `)")
    out.add_argument(
        "--quote",
        action="store_true",
        help='값을 큰따옴표로 감싼다. 기본은 끔(감싸지 않음).',
    )
    out.add_argument(
        "--escapechar",
        default="\\",
        help="따옴표를 쓰지 않을 때 값 안의 구분자를 이스케이프할 문자 (기본 \\). "
        "빈 문자열을 주면 이스케이프하지 않지만, 값에 구분자가 있으면 오류로 중단됩니다.",
    )
    out.add_argument(
        "--encoding",
        default="utf-8",
        help="파일 인코딩. 엑셀에서 한글이 깨지면 utf-8-sig를 쓰세요.",
    )
    out.add_argument("--no-header", action="store_true", help="헤더 행을 쓰지 않음")
    out.add_argument(
        "--null-string",
        default="",
        help="NULL을 표시할 문자열 (기본: 빈 값). 예: --null-string '\\N'",
    )
    out.add_argument(
        "--max-rows",
        type=int,
        default=table.DEFAULT_MAX_ROWS,
        help=f"표로 출력할 최대 행 수 (기본 {table.DEFAULT_MAX_ROWS}, 0이면 제한 없음)",
    )
    out.add_argument("--batch-size", type=int, default=50_000, help="fetchmany 크기")

    return parser


def run_shell(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """대화형 셸을 연다. 접속 준비는 일반 실행과 같은 경로를 쓴다."""
    try:
        dbapi = import_impala_dbapi()
        check_auth_dependencies(args.auth_mechanism)
        if args.sasl_backend == "sasl" and args.auth_mechanism != NO_AUTH:
            use_cyrus_sasl()
    except ImportError as exc:
        print(f"오류: {exc}", file=sys.stderr)
        return 3

    password = resolve_password(args) if args.auth_mechanism != NO_AUTH else None
    who = f"{args.user}@" if args.auth_mechanism != NO_AUTH else ""
    print(f"접속: {who}{args.host}:{args.port}", file=sys.stderr)
    try:
        return shell.Shell(make_engine(args, dbapi, password), args).run()
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
    apply_config(args, load_impala_settings(args), parser, load_sql_settings(args))

    # 셸은 구간별 소요 시간을 내지 않는다. 세션 전체를 재봐야 의미가 없고,
    # 문장별 시간은 셸 안에서 \timing 이 맡는다.
    if args.interactive:
        return run_shell(args, parser)

    timer = PhaseTimer(PHASES)
    try:
        if not (args.query or args.query_file):
            parser.error("-q/--query 또는 -f/--query-file 을 주세요. "
                         "대화형으로 쓰려면 -i/--interactive 입니다.")

        query = read_query(args)
        session_settings = parse_session_settings(args.set)
        password = resolve_password(args) if args.auth_mechanism != NO_AUTH else None
        connect_kwargs = build_connect_kwargs(args, password)

        # 지연 임포트: --help는 impyla 없이도 뜬다.
        # impyla가 없거나 impala.py 파일에 가려져 있으면 원인을 짚어 알려준다.
        if args.debug:
            # impyla와 thrift_sasl이 핸드셰이크 과정을 DEBUG로 남긴다
            import logging

            logging.basicConfig(
                level=logging.DEBUG, stream=sys.stderr, format="%(name)s %(levelname)s %(message)s"
            )
            # 서버로 보내는 SQL. 파일 내용 그대로다.
            print(f"--- 실행할 SQL ---\n{query}\n------------------", file=sys.stderr)

        try:
            dbapi = import_impala_dbapi()
            check_auth_dependencies(args.auth_mechanism)
            if args.sasl_backend == "sasl" and args.auth_mechanism != NO_AUTH:
                use_cyrus_sasl()
        except ImportError as exc:
            print(f"오류: {exc}", file=sys.stderr)
            return 3

        transport = "HTTP" if args.http_transport else "바이너리"
        tls = "평문" if args.no_ssl else "TLS"
        who = f"{args.user}@" if args.auth_mechanism != NO_AUTH else ""
        # HTTP 전송은 SASL이 아니라 HTTP 기본 인증 헤더를 쓴다
        sasl = ""
        if args.auth_mechanism != NO_AUTH and not args.http_transport:
            sasl = f", SASL {args.auth_mechanism}/{args.sasl_backend}"
        print(
            f"접속: {who}{args.host}:{args.port} ({tls}, {args.auth_mechanism}, {transport} 전송{sasl})",
            file=sys.stderr,
        )

        try:
            with timer.measure("Impala 접속"):
                conn = dbapi.connect(**connect_kwargs)
        except Exception as exc:
            # 전송 오류(EOF 등)는 메시지만으로 원인을 알기 어려워 점검 목록을 붙여준다.
            # impyla 설치에 따라 thrift와 thriftpy2 중 어느 쪽을 쓰는지 달라지므로
            # 모듈 경로로 잡지 않고 예외 이름으로 판별한다.
            if type(exc).__name__ != "TTransportException":
                raise
            print(f"\n접속 실패: {exc}\n", file=sys.stderr)
            print(transport_error_hint(args), file=sys.stderr)
            return 4

        try:
            cursor = conn.cursor()
            try:
                for key, value in session_settings.items():
                    cursor.execute(f"SET {key}={value}")

                reporter = progress.Progress(
                    "읽는 중", unit="행", enabled=not args.no_progress
                )
                if not args.output:
                    # -o 가 없으면 파일로 쓰지 않고 표로 보여준다
                    show_table(
                        cursor,
                        query,
                        timer,
                        batch_size=args.batch_size,
                        max_rows=args.max_rows,
                        null_string=args.null_string,
                        reporter=reporter,
                    )
                    return 0
                with open_output(args.output, args.gzip, args.encoding) as handle:
                    rows = export(
                        cursor,
                        query,
                        handle,
                        timer,
                        batch_size=args.batch_size,
                        delimiter=args.delimiter,
                        null_string=args.null_string,
                        write_header=not args.no_header,
                        reporter=reporter,
                        quote=args.quote,
                        escapechar=args.escapechar,
                    )
            except Exception as exc:
                hint = query_error_hint(query, exc)
                if not hint:
                    raise
                print(f"\n쿼리 실행 실패: {exc}\n", file=sys.stderr)
                print(hint, file=sys.stderr)
                return 5
            finally:
                cursor.close()
        finally:
            conn.close()

        size = os.path.getsize(args.output)
        elapsed = timer.total
        # 보고는 stdout 이 아니라 stderr 로 낸다. stdout 은 데이터 몫이다.
        print(f"{args.output}  {human(size)}  {rows:,}행", file=sys.stderr)
        if elapsed > 0:
            print(f"평균 {rows / elapsed:,.0f} rows/s", file=sys.stderr)
        return 0
    finally:
        # 성공하든 실패하든 어디에 시간을 썼는지는 항상 남긴다
        timer.print_report()


if __name__ == "__main__":
    raise SystemExit(main())
