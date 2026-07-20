"""local_stage(file:// 세그먼트 로컬 스테이징) Phase 2 SQL 조립.

coordinator 가 GP master 에 실행할 SQL 문자열을 **순수 함수**로 만든다(실 DB 없이 단위
테스트 가능). file:// 외부테이블 LOCATION·``FORMAT 'CSV'`` 절, staging 적재(INSERT),
멱등 선삭제(DELETE), 정리(DROP)를 각각 만든다. 실제 실행은 executor 백엔드의
``load_external_csv`` 가 담당한다(coordinator 가 자기 GP 백엔드로 호출).

배경(§17): executor 가 각 세그먼트 호스트 로컬 디스크에 쓴 CSV 파일을, GP 가 ``file://``
외부테이블로 **세그먼트별 로컬에서 병렬로** 읽어 staging 에 적재한 뒤 target 으로 INSERT 한다.
"""

from __future__ import annotations

from urllib.parse import urlparse


def host_of(executor_url: str | None, fallback: str = "") -> str:
    """executor base URL 에서 호스트명을 뽑는다(file:// URI 조립용).

    실제 배포에서는 executor 가 self-report 한 GP hostname(gp_segment_configuration 과 일치)을
    쓰는 것이 정확하지만, 골격에서는 배정된 ``executor_url`` 의 호스트를 그대로 사용한다.
    파싱 실패/미배정(로컬·목 모드)이면 ``fallback`` 을 반환한다.
    """
    if not executor_url:
        return fallback
    host = urlparse(executor_url).hostname
    return host or fallback


def external_table_name(job_id: str) -> str:
    """job 별 고유 외부테이블 이름(``ext_<job_id 안전화>``). 영숫자 외 문자는 ``_`` 로 치환."""
    safe = "".join(c if c.isalnum() else "_" for c in job_id)
    return f"ext_{safe}"


def csv_format_clause(csv_options: dict | None) -> str:
    """``FORMAT 'CSV' ( DELIMITER '`' NULL '' QUOTE '"' )`` 절을 만든다.

    executor 가 CSV 를 쓸 때 쓴 방언과 **정확히 일치**해야 하므로 같은 csv_options 를 쓴다.
    """
    opts = csv_options or {}
    delim = opts.get("delimiter", "`")
    null = opts.get("null", "")
    quote = opts.get("quote", '"')
    return f"FORMAT 'CSV' ( DELIMITER '{delim}' NULL '{null}' QUOTE '{quote}' )"


def build_external_ddl(
    external_table: str,
    external_columns: str,
    uris: list[tuple[str, str]],
    csv_options: dict | None,
) -> str:
    """``CREATE EXTERNAL TABLE`` DDL(파일 목록 LOCATION + CSV 포맷)을 만든다.

    인자:
        external_table  : 생성할 외부테이블 이름(job 별 고유).
        external_columns: 컬럼 정의 문자열(요청자 명시). 예: "user_id int, dt date".
        uris            : ``[(host, path), ...]`` — host 가 있으면 ``file://host/path``,
                          없으면(로컬/목 모드) ``file://path``. path 는 세그먼트 로컬 절대경로.
        csv_options     : CSV 방언(executor write 와 동일해야 함).
    """
    locs = []
    for host, path in uris:
        locs.append(f"'file://{host}{path}'" if host else f"'file://{path}'")
    location = ", ".join(locs)
    return (
        f"CREATE EXTERNAL TABLE {external_table} ({external_columns})\n"
        f"  LOCATION ({location})\n"
        f"  {csv_format_clause(csv_options)}"
    )


def build_staging_load(staging_table: str, external_table: str) -> str:
    """외부테이블 전체를 staging 힙 테이블로 적재하는 INSERT(세그먼트 로컬 병렬 read)."""
    return f"INSERT INTO {staging_table} SELECT * FROM {external_table}"


def build_pre_delete(
    target_table: str, partition_column: str, partition_values: list[str]
) -> str | None:
    """overwrite_partitions 멱등 선삭제 DELETE. 값이 없으면 None(선삭제 없음).

    ``partition_values`` 는 splitter 가 이미 방언 기준으로 렌더링한 SQL 리터럴 목록이므로
    그대로 IN 절에 결합한다(sub-query 생성과 동일한 방식).
    """
    if not partition_values:
        return None
    in_list = ", ".join(partition_values)
    return f"DELETE FROM {target_table} WHERE {partition_column} IN ({in_list})"


def build_cleanup(external_table: str) -> list[str]:
    """Phase 3 GP 정리 SQL(외부테이블 DROP). staging 은 사용자 테이블이라 건드리지 않는다."""
    return [f"DROP EXTERNAL TABLE IF EXISTS {external_table}"]


def budget_capacity(
    host_segments: dict, executors_by_host: dict, max_per_host: int = 0
) -> int:
    """수용 가능한 총 파일 수 = Σ 호스트별 유효 cap.

    유효 cap = min(S_h, max_per_host>0 시) 이며, **그 호스트에 executor 가 하나도 없으면
    0**(파일을 쓸 주체가 없으므로 제외)이다.
    """
    total = 0
    for host, s in host_segments.items():
        if not executors_by_host.get(host):
            continue
        total += int(s) if max_per_host <= 0 else min(int(s), max_per_host)
    return total


def plan_file_budget(
    num_files: int,
    host_segments: dict,
    executors_by_host: dict,
    max_per_host: int = 0,
) -> list[tuple[str, str]] | None:
    """``num_files`` 개 파일을 호스트당 예산(S_h, 또는 min(S_h, max_per_host))을 넘지 않게 배분.

    file:// 규칙("호스트당 파일 수 ≤ 그 호스트의 primary 세그먼트 수")을 그대로 구현한다.
    호스트를 라운드로빈으로 순회하며 각 호스트의 남은 예산이 있으면 파일을 하나씩 얹어, 파일을
    여러 호스트에 고르게 편다. 한 호스트 안에 executor 가 여럿이면 그 안에서도 라운드로빈해
    부담을 나눈다. executor 가 없는 호스트는 배분 대상에서 제외한다.

    인자:
        num_files        : 배분할 파일(=export task) 수.
        host_segments    : ``{hostname: S_h}`` (gp_segment_configuration 조회 결과).
        executors_by_host: ``{hostname: [executor_url, ...]}``.
        max_per_host     : >0 이면 호스트당 상한을 min(S_h, 이 값)으로 더 낮춘다(0=S_h).

    반환:
        길이 ``num_files`` 의 ``[(executor_url, hostname), ...]``. 총 예산이 부족하면
        (``num_files`` > Σ cap) **None**(호출자가 조기 실패 처리).
    """
    caps: dict = {}
    for host, s in host_segments.items():
        if not executors_by_host.get(host):
            continue  # 파일을 쓸 executor 가 없는 호스트는 제외
        cap = int(s) if max_per_host <= 0 else min(int(s), max_per_host)
        if cap > 0:
            caps[host] = cap
    if num_files > sum(caps.values()):
        return None  # 용량 초과 — file:// 규칙상 배치 불가

    hosts = sorted(caps)  # 결정적 순서
    assigned = {h: 0 for h in hosts}
    rr = {h: 0 for h in hosts}  # 호스트 내 executor 라운드로빈 인덱스
    plan: list[tuple[str, str]] = []
    hi = 0
    guard = 0
    max_iter = num_files * (len(hosts) + 1) + len(hosts) + 1  # 방어적 무한루프 차단
    while len(plan) < num_files and guard < max_iter:
        host = hosts[hi % len(hosts)]
        if assigned[host] < caps[host]:
            execs = executors_by_host[host]
            plan.append((execs[rr[host] % len(execs)], host))
            rr[host] += 1
            assigned[host] += 1
        hi += 1
        guard += 1
    return plan if len(plan) == num_files else None


def resolve_csv_options(job, settings) -> dict:
    """job 의 CSV 오버라이드와 설정 기본값을 합쳐 최종 CSV 방언 dict 를 만든다.

    executor payload(export)와 Phase 2(외부테이블 FORMAT) 양쪽이 **같은 함수**로 방언을
    해석하므로 항상 일치한다.
    """
    return {
        "delimiter": (job.csv_delimiter or settings.stage_csv_delimiter),
        "null": (job.csv_null if job.csv_null is not None else settings.stage_csv_null),
        "quote": (job.csv_quote or settings.stage_csv_quote),
    }
