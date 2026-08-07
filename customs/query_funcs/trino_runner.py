"""query-execute 커스텀 실행 함수 **예제** — Trino 로 SELECT 를 실행해 상위 N행을 반환.

이 파일은 "외부에서 제공하는 커스텀 함수" 의 참조 구현이다. executor 의 설정에서
``query.func.module`` 로 이 함수를 가리키고, ``query.func.config.*`` 로 접속 정보를 넘기면
query-execute 의 trino 경로가 이 함수에 실행을 위임한다(프레임워크는 Trino 를 직접 모른다).

설정 예(config.properties)::

    query.func.module=customs.query_funcs.trino_runner:run
    query.func.config.host=trino.example.com
    query.func.config.port=8080
    query.func.config.user=query-executor
    query.func.config.catalog=hive
    query.func.config.schema=default
    # 비밀번호(BasicAuth)를 쓰면 http_scheme=https 가 필수다. 자체서명 인증서면 verify=false
    # 로 TLS 검증을 끄거나 CA 번들 경로를 준다.
    query.func.config.password=secret
    query.func.config.http_scheme=https
    query.func.config.verify=false
    # 임의 파라미터도 자유롭게 추가할 수 있다(아래 run 에서 꺼내 쓴다):
    query.func.config.statement_timeout_s=60
    # 사내 인증 모듈의 login() 이 표준 입력(input()/getpass())으로 자격증명을 묻는
    # 대화형 함수라면 여기에 지정한다. executor 는 터미널 없는 데몬이라 그대로 두면
    # 프롬프트에서 EOFError/블록이 나므로, 이 예제가 입력을 config 값으로 대신 공급해
    # 비대화형으로 1회 호출하고 결과를 프로세스 수명 동안 캐시한다.
    query.func.config.login_module=mycorp.auth:login

계약:
    run(sql, *, config, limit) -> QueryResult
      - sql    : coordinator 가 템플릿을 렌더·검증한 SELECT.
      - config : query.func.config.* 를 모은 dict(값은 모두 문자열 — 여기서 형변환).
      - limit  : 반환 최대 행수.
      - 반환   : core.dbprobe.QueryResult(columns, rows, row_count, truncated, elapsed_ms).

이 예제는 표준 dbprobe 의 정형(_shape) 로직을 재사용해 ``fetchmany(limit+1)`` 로 truncated
를 판정한다. 조직 표준(게이트웨이/래퍼/커넥션 풀 등)이 있으면 이 함수 본문만 바꾸면 된다.

**커스텀 API 가 pandas DataFrame 을 돌려주는 경우**(사내 표준이 trino 드라이버가 아니라
"SQL → DataFrame" API 인 경우)는 설정 한 줄로 그 경로를 쓴다::

    query.func.config.dataframe_module=mycorp.trino_api:query

그러면 ``run`` 이 ``trino.dbapi`` 커서 대신 그 함수를 호출하고, 받은 DataFrame 을 그대로
``_shape`` 에 넘긴다(``_run_dataframe_api``). 커스텀 API 계약::

    query(sql: str, *, config: dict, limit: int) -> pandas.DataFrame

DataFrame 을 손으로 풀 필요가 없다 — ``_shape`` 가 컬럼명·행·truncated 를 뽑고, numpy
스칼라(``np.int64`` 등)와 결측(``NaN``/``NaT``/``pd.NA``)을 JSON 안전한 값으로 정규화한다.
pandas 는 이 저장소의 의존성이 아니라 덕타이핑으로 인식하므로, 쓰는 배포에만 설치하면 된다.
"""
from __future__ import annotations

import builtins
import getpass
import importlib
import io
import logging
import sys
import threading
import time

from core.dbprobe import QueryResult, _shape

# 커스텀 함수는 executor 프로세스 안에서 실행되므로, 표준 logging 을 쓰면 별도 설정 없이
# executor 의 로그 파일(executor-<포트>.log, WARNING 이상은 *-warn.log)에 그대로 남는다.
# print() 는 로그 파일에 남지 않으니 쓰지 말 것. 오류는 logger.exception() 으로 남기고
# 다시 raise 해야 스택 트레이스가 로그에 기록되고 executor 는 502 로 응답한다.
logger = logging.getLogger(__name__)

# ── 대화형 login() 의 비대화형 처리 ──────────────────────────────────────
# 사내 인증 모듈의 login() 이 input()/getpass.getpass() 로 자격증명을 묻는 경우를 위한
# 래퍼. executor 는 nohup 데몬(터미널 없음)이라 프롬프트가 뜨는 순간 EOFError 가 나거나
# 블록되므로, 호출 동안만 입력 함수를 config 값(user/password)으로 바꿔치기해 공급한다.
#
#  - 로그인 결과(세션/토큰)는 프로세스당 1회만 만들고 캐시한다 — 매 /query-run 마다
#    재로그인하지 않는다. 실패 시에는 캐시하지 않아 다음 요청에서 재시도된다.
#  - builtins.input/getpass/sys.stdin 교체는 프로세스 전역이므로, executor 가 task 를
#    동시 처리해도 안전하도록 락 안에서만 수행하고 finally 로 반드시 원복한다.
_login_lock = threading.Lock()
_login_session = None


def _login_noninteractive(config: dict):
    """config["login_module"] 이 가리키는 대화형 login() 을 비대화형으로 1회 호출한다.

    프롬프트에는 입력을 다음 순서로 공급한다: input() 1회차 → user, 2회차 → password,
    getpass() → 항상 password. 사내 login() 의 질문 순서가 다르면 answers 목록만 고친다.
    login_module 미설정이면 아무것도 하지 않는다(기존 BasicAuth 경로 그대로).
    """
    global _login_session
    dotted = (config.get("login_module") or "").strip()
    if not dotted:
        return None
    with _login_lock:
        if _login_session is not None:
            return _login_session

        # query.func.module 과 같은 규약(module:func / module.func)으로 로딩한다.
        mod_name, _, func_name = dotted.replace(":", ".").rpartition(".")
        login_fn = getattr(importlib.import_module(mod_name), func_name)

        user = config.get("user", "")
        password = config.get("password", "")
        answers = iter([user, password])

        orig = (builtins.input, getpass.getpass, sys.stdin)
        builtins.input = lambda prompt="": next(answers)
        getpass.getpass = lambda prompt="", stream=None: password
        # input() 을 직접 쓰지 않고 sys.stdin.readline() 등으로 읽는 구현까지 커버한다.
        sys.stdin = io.StringIO(f"{user}\n{password}\n")
        try:
            logger.info("커스텀 login 호출: %s (user=%s)", dotted, user)
            _login_session = login_fn()
            logger.info("커스텀 login 성공: %s", dotted)
        except Exception:
            # 스택 트레이스까지 로그에 남기고 다시 raise — executor 가 502 로 응답한다.
            logger.exception("커스텀 login 실패: %s (user=%s)", dotted, user)
            raise
        finally:
            builtins.input, getpass.getpass, sys.stdin = orig
        return _login_session


def _load_dotted(dotted: str):
    """``module:func`` 또는 ``module.func`` dotted path 로 함수를 import 한다.

    ``login_module`` 로딩과 같은 규약이다. 실패는 그대로 올려 executor 가 502 로 응답하게 한다.
    """
    mod_name, _, func_name = dotted.replace(":", ".").rpartition(".")
    if not mod_name or not func_name:
        raise ValueError(f"잘못된 함수 경로: {dotted!r} (module:func 형식이어야 함)")
    return getattr(importlib.import_module(mod_name), func_name)


def _run_dataframe_api(sql: str, *, config: dict, limit: int) -> QueryResult:
    """사내 커스텀 API 로 Trino SQL 을 실행하고, 받은 **DataFrame** 을 그대로 정형한다.

    ``query.func.config.dataframe_module`` 이 설정된 배포에서 이 경로가 쓰인다. 커스텀 API
    계약은 이 파일의 ``run`` 과 같은 모양이되 반환만 DataFrame 이다::

        def query(sql: str, *, config: dict, limit: int) -> pandas.DataFrame

    ``limit`` 은 API 가 서버측으로 밀어 넣을 수 있게 함께 넘기지만, **잘림 판정과 상한
    적용은 여기서 다시 한다**(``_shape``) — API 가 limit 을 무시해도 응답 크기가 보장된다.

    DataFrame 을 직접 풀어(``itertuples`` 등) 넘길 필요는 없다. ``_shape`` 가 컬럼명·행·
    truncated 를 뽑고, numpy 스칼라(np.int64 등)와 결측(NaN/NaT/pd.NA)을 JSON 안전한
    값(숫자/null)으로 정규화한다. 손으로 풀면 NaN 이 그대로 새어 표준 JSON 직렬화가 깨진다.

    인덱스에 의미가 있는 DataFrame 이면(예: ``set_index('dt')``) API 쪽에서
    ``reset_index()`` 로 컬럼화해 돌려줘야 한다 — SQL 결과셋에는 인덱스 개념이 없다.
    """
    dotted = (config.get("dataframe_module") or "").strip()
    started = time.perf_counter()
    logger.info("커스텀 DataFrame API 호출: %s (limit=%s) sql=%.200s", dotted, limit, sql)
    fn = _load_dotted(dotted)
    try:
        df = fn(sql, config=config, limit=limit)
    except Exception:
        logger.exception("커스텀 DataFrame API 실패: %s / sql=%.500s", dotted, sql)
        raise
    result = _shape(None, df, limit, started)
    logger.info("커스텀 DataFrame API 완료: %d행(truncated=%s) %.0fms",
                result.row_count, result.truncated, result.elapsed_ms)
    return result


def run(sql: str, *, config: dict, limit: int) -> QueryResult:
    """config 로 지정한 Trino 에 sql 을 실행해 상위 limit 행을 반환한다.

    실행 경로는 설정으로 갈린다:

    - ``query.func.config.dataframe_module`` 이 있으면 → **커스텀 API**(DataFrame 반환)
    - 없으면(기본) → 아래의 ``trino.dbapi`` 커서 경로
    """
    if (config.get("dataframe_module") or "").strip():
        return _run_dataframe_api(sql, config=config, limit=limit)

    import trino  # 지연 임포트(이 예제 함수를 쓰는 배포에만 trino 패키지가 필요)

    # 사내 대화형 login() 이 설정돼 있으면 먼저 비대화형으로 인증한다(1회 캐시).
    # 반환된 세션/토큰을 접속에 써야 하는 조직 표준이면 아래 kwargs 조립에 반영한다.
    _login_noninteractive(config)

    started = time.perf_counter()

    # config 값은 문자열이므로 여기서 형변환/기본값을 적용한다(자유 정의 파라미터도 여기서 해석).
    kwargs = {
        "host": config.get("host", ""),
        "port": int(config.get("port", 8080)),
        "user": config.get("user", "query-executor"),
        "catalog": config.get("catalog", "hive"),
        "schema": config.get("schema", "default"),
        "http_scheme": config.get("http_scheme", "http"),
    }
    password = config.get("password")
    if password:
        # BasicAuthentication 은 trino 클라이언트 제약상 https 에서만 허용된다.
        kwargs["auth"] = trino.auth.BasicAuthentication(kwargs["user"], password)

    # TLS 인증서 검증. 자체서명 인증서를 쓰는 사내 배포에서는 "false" 로 끄거나 CA 번들
    # 경로를 준다(값 문자열: false/true/no/0/1 또는 파일 경로). 기본은 검증 켜짐.
    verify = config.get("verify")
    if verify is not None:
        low = verify.strip().lower()
        if low in ("false", "0", "no", "off"):
            kwargs["verify"] = False
        elif low in ("true", "1", "yes", "on"):
            kwargs["verify"] = True
        elif low:
            kwargs["verify"] = verify  # CA 번들 파일 경로로 해석

    # 접속 대상을 로그로 남긴다 — 실패 시 어느 서버/카탈로그였는지 바로 확인할 수 있다.
    # 비밀값(password)은 절대 로그에 넣지 않는다.
    logger.info(
        "trino 접속: %s://%s:%s catalog=%s schema=%s user=%s",
        kwargs["http_scheme"], kwargs["host"], kwargs["port"],
        kwargs["catalog"], kwargs["schema"], kwargs["user"],
    )
    try:
        conn = trino.dbapi.connect(**kwargs)
    except Exception:
        logger.exception("trino 접속 실패: %s:%s", kwargs["host"], kwargs["port"])
        raise

    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
        except Exception:
            # 어떤 SQL 이 실패했는지 앞부분만 잘라 함께 남긴다(전문은 장황할 수 있음).
            logger.exception("trino 쿼리 실패: %.500s", sql)
            raise
        if cur.description is None:
            return _shape([], [], limit, started)
        columns = [d[0] for d in cur.description]
        raw = cur.fetchmany(limit + 1)          # limit+1 로 잘림(truncated) 판정
        result = _shape(columns, raw, limit, started)
        logger.info("trino 쿼리 완료: %d행(truncated=%s) %.0fms",
                    result.row_count, result.truncated, result.elapsed_ms)
        return result
    finally:
        conn.close()
