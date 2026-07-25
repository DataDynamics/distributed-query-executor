"""local_stage(file:// 세그먼트 로컬 스테이징) Phase 2 SQL 조립.

coordinator 가 GP master 에 실행할 SQL 문자열을 **순수 함수**로 만든다(실 DB 없이 단위
테스트 가능). file:// 외부테이블 LOCATION·``FORMAT 'CSV'`` 절, staging 적재(INSERT),
멱등 선삭제(DELETE), 정리(DROP)를 각각 만든다. 실제 실행은 executor 백엔드의
``load_external_csv`` 가 담당한다(coordinator 가 자기 GP 백엔드로 호출).

배경(§17): executor 가 각 세그먼트 호스트 로컬 디스크에 쓴 CSV 파일을, GP 가 ``file://``
외부테이블로 **세그먼트별 로컬에서 병렬로** 읽어 staging 에 적재한 뒤 target 으로 INSERT 한다.
"""

from __future__ import annotations

import hashlib
import re
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


def _quote_ident(name: str) -> str:
    """``sql_ident`` 필터와 동일 규칙으로 식별자를 큰따옴표 인용한다.

    내부 ``"`` 는 ``""`` 로 이중화하고, 점(``.``)이 있으면 스키마 한정으로 보고 각 조각을
    따로 인용한다(예: ``public.stg`` → ``"public"."stg"``). 템플릿이 렌더한 토큰과
    정확히 일치시키기 위해 여기서 같은 규칙을 복제한다(stage.py 는 template_funcs 에
    의존하지 않는 순수 모듈이라 import 대신 규칙만 맞춘다).
    """
    parts = str(name).split(".")
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)


def unique_staging_name(base: str, task_id: str, max_len: int = 63) -> str:
    """``base`` 뒤에 ``task_id`` 를 붙여 **task 별 고유 staging 테이블명**을 만든다.

    왜 필요한가: 같은 job 의 여러 task 가 같은 이름으로 ``CREATE TEMP TABLE`` 을 하면,
    executor 의 GP 커넥션 풀이 세션을 재사용할 때 이전 task 의 TEMP 가 남아 다음
    ``CREATE`` 가 "relation already exists" 로 충돌할 수 있다(``DISCARD ALL`` 이 TEMP 를
    실제로 드롭하지 않는 GP 구성에서 특히). task_id 를 접미사로 붙여 이름을 분리하면
    이 충돌이 원천 차단된다.

    영숫자 외 문자는 ``_`` 로 치환하고, PostgreSQL/Greenplum 식별자 상한(기본 63바이트)을
    넘으면 base 를 잘라 뒤에 task_id 해시(8자)를 붙여 길이를 맞추면서 고유성을 유지한다.
    해시는 결정적이라 같은 task 재시도 시 항상 같은 이름이 나온다(멱등).
    """
    safe_id = "".join(c if c.isalnum() else "_" for c in str(task_id))
    name = f"{base}_{safe_id}"
    if len(name.encode("utf-8")) <= max_len:
        return name
    # 상한 초과: base 를 최대한 살리고 task_id 해시로 고유성 확보.
    h = hashlib.sha1(str(task_id).encode("utf-8")).hexdigest()[:8]
    keep = max_len - 1 - len(h)  # '_' + 해시 길이만큼 base 여유
    if keep < 1:
        return h[:max_len]
    return f"{base[:keep]}_{h}"


def rewrite_staging_name(
    sql: str | None, old_name: str, new_name: str, protect: tuple[str, ...] = ()
) -> str | None:
    """``sql`` 안의 **staging 식별자만** ``old_name`` → ``new_name`` 으로 바꾼다.

    치환 결과는 **큰따옴표로 감싸지 않은 bare 식별자**다(요청: 이름을 ``""`` 로 감싸지
    않는다). 템플릿이 ``{{ staging_table | sql_ident }}`` 로 렌더한 인용 토큰(``"old"``)이
    있으면 그 토큰째로 bare 이름으로 바꾸고(따옴표 제거), 따옴표 없이 쓴 경우는 단어경계
    bare 이름을 치환한다(컬럼/별칭 오탐을 줄이려 단어경계 정확 매치만).

    ``protect`` 는 치환에서 **건드리면 안 되는 이름**(예: INSERT 대상 테이블)이다. staging
    이름이 대상 테이블 이름의 일부와 겹칠 때(예: staging ``sales`` / target ``public.sales``)
    단어경계·부분문자열 치환이 대상까지 바꿔 버리는 것을 막는다. 보호 대상의 인용/비인용
    형태를 먼저 플레이스홀더로 잠갔다가 staging 치환 뒤 복원한다(가장 긴 형태부터 잠가
    부분 겹침을 피한다). staging 이름과 완전히 같은 보호 이름은 구분 불가라 잠그지 않는다.

    어느 staging 토큰도 없으면 원문을 그대로 돌려준다(무변경).
    """
    if not sql:
        return sql
    new = str(new_name)
    # 1) 보호 대상(target 등)을 플레이스홀더로 잠근다. staging 이름과 동일하면 구분 불가라 건너뛴다.
    forms: set[str] = set()
    for name in protect:
        if name and str(name) != str(old_name):
            forms.add(_quote_ident(name))  # "public"."target"
            forms.add(str(name))            # public.target
    placeholders: list[tuple[str, str]] = []
    masked = sql
    for i, form in enumerate(sorted(forms, key=len, reverse=True)):
        if form and form in masked:
            ph = f"\x00PROTECT{i}\x00"
            placeholders.append((ph, form))
            masked = masked.replace(form, ph)
    # 2) staging 이름만 치환(인용 토큰 우선, 아니면 단어경계 bare).
    quoted_old = _quote_ident(old_name)
    if quoted_old in masked:
        masked = masked.replace(quoted_old, new)
    else:
        pattern = r"\b" + re.escape(str(old_name)) + r"\b"
        masked = re.sub(pattern, new, masked)
    # 3) 보호 대상 복원.
    for ph, form in placeholders:
        masked = masked.replace(ph, form)
    return masked


def per_task_staging(
    staging_table: str,
    staging_ddl: str | None,
    insert_sql: str | None,
    task_id: str,
    *,
    enabled: bool = True,
    max_len: int = 63,
    target_table: str | None = None,
) -> tuple[str, str | None, str | None]:
    """stage_insert 용 **task 별 고유 staging** ``(이름, staging_ddl, insert_sql)`` 을 만든다.

    고유화 조건: ``enabled`` 이고 ``staging_ddl`` 이 있을 때만(=프레임워크가 이번 실행에
    staging 을 CREATE 하는 경우)이다. ``staging_ddl`` 이 비면 기존 영구 staging 테이블에
    곧장 COPY 하는 경로라 이름을 바꾸면 대상이 사라지므로 고유화하지 않는다(그 경로의
    격리는 요청자가 job·파티션별 고유 이름으로 보장해야 한다 — backend docstring 참고).

    task_id 접미사는 **staging 이름에만** 붙는다. ``target_table`` 을 넘기면 INSERT 대상
    테이블을 치환에서 보호해, staging 이름이 대상 이름의 일부와 겹쳐도(예: staging
    ``sales`` / target ``public.sales``) 대상에는 접미사가 붙지 않는다.

    반환은 항상 3-튜플이며, 고유화하지 않을 때는 입력을 그대로 돌려준다. job 원본은
    건드리지 않고 task 전용 사본만 만들어, 여러 task 가 각자 다른 staging 이름을 쓴다.
    """
    if not enabled or not staging_ddl:
        return staging_table, staging_ddl, insert_sql
    eff = unique_staging_name(staging_table, task_id, max_len)
    if eff == staging_table:
        return staging_table, staging_ddl, insert_sql
    protect = (target_table,) if target_table else ()
    ddl = rewrite_staging_name(staging_ddl, staging_table, eff, protect)
    ins = rewrite_staging_name(insert_sql, staging_table, eff, protect)
    return eff, ddl, ins


def per_task_external(
    staging_table: str,
    insert_sql: str | None,
    task_id: str,
    *,
    enabled: bool = True,
    max_len: int = 63,
    target_table: str | None = None,
) -> tuple[str, str | None]:
    """s3_stage 용 **task 별 고유 외부테이블(=staging) 이름**과 그에 맞춘 insert_sql 을 만든다.

    s3_stage 는 S3 외부테이블이 staging 역할을 겸한다. 외부테이블은 TEMP 가 아니라 GP
    카탈로그 전역이라, 같은 job 의 여러 task 가 같은 이름으로 CREATE EXTERNAL TABLE 을 하면
    동시에 충돌한다. 그래서 ``stage_insert`` 의 ``per_task_staging`` 과 같은 방식으로 이름에
    task_id 접미사를 붙여 고유화하고, insert_sql 의 소스 참조도 같은 이름으로 바꾼다
    (``target_table`` 은 치환에서 보호해 이름 겹침 시 대상이 바뀌지 않게 한다).

    ``staging_ddl`` 이 없다는 점만 ``per_task_staging`` 과 다르다(외부테이블 DDL 은 executor
    가 external_columns 로 생성하므로 여기서 다루지 않는다). 반환: ``(이름, insert_sql)``.
    """
    if not enabled:
        return staging_table, insert_sql
    eff = unique_staging_name(staging_table, task_id, max_len)
    if eff == staging_table:
        return staging_table, insert_sql
    protect = (target_table,) if target_table else ()
    ins = rewrite_staging_name(insert_sql, staging_table, eff, protect)
    return eff, ins


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
