"""query-execute 커스텀 실행 함수 **예제** — Trino 로 SELECT 를 실행해 상위 N행을 반환.

이 파일은 "외부에서 제공하는 커스텀 함수" 의 참조 구현이다. executor 의 설정에서
``query.func.module`` 로 이 함수를 가리키고, ``query.func.config.*`` 로 접속 정보를 넘기면
query-execute 의 trino 경로가 이 함수에 실행을 위임한다(프레임워크는 Trino 를 직접 모른다).

설정 예(config.properties)::

    query.func.module=examples.query_funcs.trino_runner:run
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

계약:
    run(sql, *, config, limit) -> QueryResult
      - sql    : coordinator 가 템플릿을 렌더·검증한 SELECT.
      - config : query.func.config.* 를 모은 dict(값은 모두 문자열 — 여기서 형변환).
      - limit  : 반환 최대 행수.
      - 반환   : core.dbprobe.QueryResult(columns, rows, row_count, truncated, elapsed_ms).

이 예제는 표준 dbprobe 의 정형(_shape) 로직을 재사용해 ``fetchmany(limit+1)`` 로 truncated
를 판정한다. 조직 표준(게이트웨이/래퍼/커넥션 풀 등)이 있으면 이 함수 본문만 바꾸면 된다.
"""
from __future__ import annotations

import time

from core.dbprobe import QueryResult, _shape


def run(sql: str, *, config: dict, limit: int) -> QueryResult:
    """config 로 지정한 Trino 에 sql 을 실행해 상위 limit 행을 반환한다."""
    import trino  # 지연 임포트(이 예제 함수를 쓰는 배포에만 trino 패키지가 필요)

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

    conn = trino.dbapi.connect(**kwargs)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description is None:
            return _shape([], [], limit, started)
        columns = [d[0] for d in cur.description]
        raw = cur.fetchmany(limit + 1)          # limit+1 로 잘림(truncated) 판정
        return _shape(columns, raw, limit, started)
    finally:
        conn.close()
