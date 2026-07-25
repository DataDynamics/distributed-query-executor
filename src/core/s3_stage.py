"""s3_stage(S3 경유 스테이징) SQL/키 조립 — 순수 함수.

``s3_stage`` exec_mode 는 ``local_stage`` 와 같은 2-phase 다: Phase 1 에서 executor 가 Impala
SELECT 결과를 로컬 CSV 로 떨어뜨린 뒤 **S3 버킷에 업로드**하고(``export_to_s3``), 배리어 후
Phase 2 에서 **coordinator** 가 Greenplum master 에 job 프리픽스(``<prefix>/<job_id>/``)로 **PXF
외부테이블 하나**를 만들어 그 아래 모든 task CSV 를 세그먼트 병렬 read 해 target 에 INSERT 한다
(``load_external_s3``). ``local_stage`` 와 달리 S3 객체는 세그먼트 로컬이 아니라 위치 무관하게
읽히므로 executor 를 GP 세그먼트 호스트에 co-locate 할 필요가 없고 파일 예산 배분도 없다.

이 모듈은 GP master 에 실행할 SQL 문자열과 S3 객체 키/프리픽스/외부테이블 이름을 **순수
함수**로 만든다(실 DB/네트워크 없이 단위 테스트 가능). 실제 실행(업로드·GP 호출)은 backend 가
담당한다. ``coordinator/stage.py``(local_stage file:// 조립)의 S3 판이다.
"""

from __future__ import annotations


def s3_object_key(prefix: str, job_id: str, task_id: str) -> str:
    """이 task(Phase 1)가 업로드할 S3 객체 키(``<prefix>/<job_id>/<task_id>.csv``)를 만든다.

    task_id 가 유일하므로 동시 task 간 키 충돌이 없고, 같은 task 재시도 시에는 같은 키가
    나와(결정적) 이전 업로드를 덮어쓴다(멱등). ``prefix`` 앞뒤의 ``/`` 는 정규화한다.
    모든 task 키가 ``<prefix>/<job_id>/`` 아래에 모이므로, coordinator(Phase 2)는 그
    디렉터리 하나를 가리키는 외부테이블로 job 전체를 읽는다(``s3_job_prefix`` 참고).
    """
    pfx = str(prefix or "").strip("/")
    head = f"{pfx}/" if pfx else ""
    return f"{head}{job_id}/{task_id}.csv"


def s3_job_prefix(prefix: str, job_id: str) -> str:
    """job 의 모든 task 객체가 모이는 S3 디렉터리 프리픽스(``<prefix>/<job_id>/``).

    Phase 2 에서 coordinator 가 PXF 외부테이블 LOCATION 으로 이 디렉터리를 가리키면
    PXF 가 그 아래 모든 task CSV(``<task_id>.csv``)를 읽는다. 끝에 ``/`` 를 붙여 디렉터리임을
    명확히 한다. Phase 3 정리(객체 삭제)의 대상 프리픽스이기도 하다.
    """
    pfx = str(prefix or "").strip("/")
    head = f"{pfx}/" if pfx else ""
    return f"{head}{job_id}/"


def external_table_name(job_id: str) -> str:
    """job 별 고유 외부테이블 이름(``s3ext_<job_id 안전화>``). 영숫자 외 문자는 ``_`` 로 치환.

    외부테이블은 GP 카탈로그 전역이라 job 마다 고유해야 동시 실행 job 간 충돌이 없다.
    coordinator(Phase 2)가 이 이름으로 외부테이블을 만들고, insert_sql 의 staging 참조를
    이 이름으로 치환해 ``INSERT INTO target SELECT ... FROM <이 이름>`` 이 되게 한다.
    """
    safe = "".join(c if c.isalnum() else "_" for c in str(job_id))
    return f"s3ext_{safe}"


def csv_format_clause(csv_options: dict | None) -> str:
    """``FORMAT 'CSV' ( DELIMITER '`' NULL '' QUOTE '"' )`` 절을 만든다.

    executor 가 CSV 를 쓸 때 쓴 방언과 **정확히 일치**해야 하므로 같은 csv_options 를 쓴다.
    ``coordinator/stage.py`` 의 동명 함수와 규칙이 같다(core 는 coordinator 에 의존하지
    않으므로 규칙만 복제한다 — file:// 판과 CSV 방언을 공유).
    """
    opts = csv_options or {}
    delim = opts.get("delimiter", "`")
    null = opts.get("null", "")
    quote = opts.get("quote", '"')
    return f"FORMAT 'CSV' ( DELIMITER '{delim}' NULL '{null}' QUOTE '{quote}' )"


def build_s3_location(
    bucket: str,
    key: str,
    *,
    profile: str = "s3:csv",
    server: str = "",
    location_template: str = "",
) -> str:
    """PXF S3 외부테이블 LOCATION 문자열(``pxf://<bucket>/<key>?PROFILE=...&SERVER=...``)을 만든다.

    ``location_template`` 이 주어지면 그것을 raw override 로 쓰고(``{bucket}``/``{key}``/
    ``{profile}``/``{server}`` 치환), 아니면 표준 PXF 형태를 조립한다. PXF 는 S3 자격증명을
    세그먼트의 PXF SERVER 설정(``$PXF_BASE/servers/<server>/s3-site.xml``)에서 읽으므로,
    여기서 자격증명을 넣지 않는다(업로드용 자격증명과 경로가 분리된다).
    """
    if location_template:
        return location_template.format(
            bucket=bucket, key=key, profile=profile, server=server
        )
    query = f"PROFILE={profile}"
    if server:
        query += f"&SERVER={server}"
    return f"pxf://{bucket}/{key}?{query}"


def build_s3_external_ddl(
    external_table: str,
    external_columns: str,
    location: str,
    csv_options: dict | None,
) -> str:
    """S3(PXF) ``CREATE EXTERNAL TABLE`` DDL 을 만든다.

    인자:
        external_table  : 생성할 외부테이블 이름(job 별 고유 — ``external_table_name(job_id)``).
        external_columns: 컬럼 정의 문자열(요청자 명시). 예: "user_id int, dt date".
        location        : ``build_s3_location`` 이 만든 ``pxf://...`` LOCATION 문자열(job 프리픽스).
        csv_options     : CSV 방언(executor write 와 동일해야 함).

    외부테이블이 곧 staging 역할을 하므로(coordinator 가 Phase 2 에 생성), insert_sql 의 staging
    참조를 이 이름으로 치환해 ``INSERT INTO target SELECT ... FROM <external_table>`` 이 되게 한다.
    """
    return (
        f"CREATE EXTERNAL TABLE {external_table} ({external_columns})\n"
        f"  LOCATION ('{location}')\n"
        f"  {csv_format_clause(csv_options)}"
    )


def build_pre_delete(
    target_table: str, partition_column: str, partition_values: list[str]
) -> str | None:
    """overwrite_partitions 멱등 선삭제 DELETE. 값이 없으면 None(선삭제 없음).

    ``partition_values`` 는 splitter/fan-out 이 이미 방언 기준으로 렌더링한 SQL 리터럴
    목록이므로 그대로 IN 절에 결합한다(``coordinator/stage.py`` 와 동일 규칙).
    """
    if not partition_values:
        return None
    in_list = ", ".join(partition_values)
    return f"DELETE FROM {target_table} WHERE {partition_column} IN ({in_list})"


def build_cleanup_ddl(external_table: str) -> str:
    """외부테이블 정리 SQL(멱등 DROP). staging 을 겸하는 외부테이블만 지운다."""
    return f"DROP EXTERNAL TABLE IF EXISTS {external_table}"
