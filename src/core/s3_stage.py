"""s3_stage(S3 경유 스테이징) SQL/키 조립 — 순수 함수.

``s3_stage`` exec_mode 는 ``stage_insert`` 의 형제다: executor 가 Impala SELECT 결과를
로컬 CSV 로 떨어뜨린 뒤 **S3 버킷에 업로드**하고, Greenplum 이 그 객체를 **PXF 외부테이블**로
읽어 target 에 INSERT 한다. ``local_stage`` 와 달리 S3 객체는 세그먼트 로컬이 아니라 모든
세그먼트에서 위치 무관하게 읽히므로, executor 를 GP 세그먼트 호스트에 co-locate 할 필요가
없고 coordinator 의 파일 예산 배분/배리어/중앙 적재도 필요 없다. task 하나가 처음부터 끝까지
자체 완결한다(``stage_insert``/날짜 fan-out 과 같은 per-task 모델).

이 모듈은 GP master 에 실행할 SQL 문자열과 S3 객체 키를 **순수 함수**로 만든다(실 DB/네트워크
없이 단위 테스트 가능). 실제 실행(업로드·GP 호출)은 executor 백엔드의 ``stage_via_s3`` 가
담당한다. ``coordinator/stage.py``(local_stage file:// 조립)의 S3 판이다.
"""

from __future__ import annotations


def s3_object_key(prefix: str, job_id: str, task_id: str) -> str:
    """이 task 가 쓸 S3 객체 키(``<prefix>/<job_id>/<task_id>.csv``)를 만든다.

    task_id 가 유일하므로 동시 task 간 키 충돌이 없고, 같은 task 재시도 시에는 같은 키가
    나와(결정적) 이전 업로드를 덮어쓴다(멱등). ``prefix`` 앞뒤의 ``/`` 는 정규화한다.
    """
    pfx = str(prefix or "").strip("/")
    head = f"{pfx}/" if pfx else ""
    return f"{head}{job_id}/{task_id}.csv"


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
        external_table  : 생성할 외부테이블 이름(task 별 고유 — staging_table 을 겸한다).
        external_columns: 컬럼 정의 문자열(요청자 명시). 예: "user_id int, dt date".
        location        : ``build_s3_location`` 이 만든 ``pxf://...`` LOCATION 문자열.
        csv_options     : CSV 방언(executor write 와 동일해야 함).

    외부테이블이 곧 staging 역할을 하므로, insert_sql 은 이 이름을 소스로 참조한다
    (``INSERT INTO target SELECT ... FROM <external_table>``).
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
