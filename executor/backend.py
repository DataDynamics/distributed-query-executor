"""Impala 읽기 / Greenplum 쓰기 백엔드.

실제 백엔드는 impyla + psycopg 를 사용하며, coordinator 테스트(및 로컬 개발)에서 DB
드라이버가 필요 없도록 지연 임포트(lazy import)한다. 라이브 클러스터 없이 개발/통합
테스트를 할 수 있도록 MockBackend도 제공한다.
"""

from __future__ import annotations

import logging
from typing import Iterator, Protocol

logger = logging.getLogger(__name__)


class Backend(Protocol):
    """executor 가 사용하는 적재 백엔드의 구조적 인터페이스(덕 타이핑).

    세 가지 실행 모드를 메서드로 노출한다: ``move``(copy), ``execute``(statement),
    ``stage_and_insert``(stage_insert). 실제 구현(ImpalaToGreenplumBackend)과 테스트용
    가짜 구현(MockBackend)이 이 시그니처를 만족하면 ``app`` 에 주입할 수 있다.
    """

    def move(
        self,
        sub_query: str,
        target_table: str,
        write_mode: str,
        partition_column: str,
        partition_values: list[str],
        on_progress=None,
        query_options=None,
    ) -> int:
        """[copy 모드] 소스에서 sub_query를 읽어 target_table에 COPY 적재, 행 수 반환.

        query_options: 이 task 의 Impala 쿼리 옵션(SET). 전역 기본값 위에 병합된다.
        """
        ...

    def execute(self, sql: str) -> int:
        """[statement 모드] 대상 DB에서 sql(예: INSERT ... SELECT)을 실행, 영향받은 행 수 반환."""
        ...

    def stage_and_insert(
        self,
        impala_select: str,
        staging_table: str,
        staging_ddl: str,
        insert_sql: str,
        on_progress=None,
        query_options=None,
    ) -> int:
        """[stage_insert 모드] Impala 결과를 Greenplum staging 테이블에 COPY 적재 후,
        staging 을 소스로 하는 INSERT 를 실행한다. INSERT 영향 행 수를 반환."""
        ...


class MockBackend:
    """결정적인 행 수를 반환하고 실제 I/O는 하지 않음. 개발/테스트용.

    DB 드라이버(impyla/psycopg)나 라이브 클러스터 없이 coordinator·executor 의 흐름을
    검증할 수 있게, 입력에 따라 예측 가능한 행 수만 만들어 낸다.

    인자:
        rows_per_value: 파티션 값 1개당 가짜로 만들어 낼 행 수(기본 100).
    """

    def __init__(self, rows_per_value: int = 100):
        self.rows_per_value = rows_per_value

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None, query_options=None) -> int:
        # 파티션 값 개수 × rows_per_value 를 적재한 것으로 가정(값이 없으면 최소 1로 간주).
        total = max(1, len(partition_values)) * self.rows_per_value
        if on_progress:
            on_progress(total)
        return total

    def execute(self, sql: str) -> int:
        # statement 모드: 항상 rows_per_value 행이 영향받은 것으로 가정.
        return self.rows_per_value

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None, query_options=None) -> int:
        # stage_insert 모드: rows_per_value 행을 staging→target 으로 옮긴 것으로 가정.
        if on_progress:
            on_progress(self.rows_per_value)
        return self.rows_per_value


class ImpalaToGreenplumBackend:
    """실제 백엔드: impyla로 Impala에서 스트리밍, psycopg COPY로 Greenplum에 적재.

    impala_dsn 은 impyla connect() 에 그대로 전달된다. TLS + Kerberos 환경에서는
    아래 키를 포함한다:
      auth_mechanism='GSSAPI', kerberos_service_name='impala',
      use_ssl=True, ca_cert='/path/to/ca.pem'
    Kerberos 티켓은 OS 자격증명 캐시(KRB5CCNAME)를 사용하므로, 서비스 실행 전에
    keytab 으로 kinit 되어 있어야 한다(systemd kinit 서비스 참고).
    """

    def __init__(self, impala_dsn: dict, greenplum_dsn: str, batch_size: int = 10_000,
                 query_options: dict | None = None, copy_preflight: bool = True):
        self.impala_dsn = impala_dsn
        self.greenplum_dsn = greenplum_dsn
        self.batch_size = batch_size
        # Impala 쿼리 옵션 전역 기본값(SET). 요청별 옵션이 이 위에 병합된다.
        self.query_options: dict = query_options or {}
        # copy 모드 COPY 전에 SELECT 컬럼이 대상 테이블에 존재하는지 사전검증할지 여부.
        # 대용량 스트리밍을 시작하기 전에 컬럼 불일치를 잡아 빠르게 실패시킨다.
        self.copy_preflight = copy_preflight

    def _impala_execute(self, cur, sql: str, query_options) -> None:
        """Impala 커서로 sql 을 실행한다. 전역+요청별 옵션을 병합해 configuration 으로 넘긴다.

        병합 결과가 비어 있으면(둘 다 미지정) configuration 인자를 아예 넘기지 않고
        그대로 실행한다(요청자 의도: 옵션이 없으면 기본 동작 유지).
        """
        opts = {**self.query_options, **(query_options or {})}
        if opts:
            cur.execute(sql, configuration=opts)
        else:
            cur.execute(sql)

    def execute(self, sql: str) -> int:
        """statement 모드: 대상 Greenplum 에서 SQL(예: INSERT ... SELECT)을 그대로 실행.

        COPY를 쓰지 않으므로 컬럼 매핑은 SQL(INSERT 컬럼 목록/SELECT)이 책임진다.
        반환값은 cursor.rowcount(영향받은 행 수, 미지원 시 0).
        """
        import psycopg  # 지연 임포트

        with psycopg.connect(self.greenplum_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                affected = cur.rowcount
            conn.commit()
        return affected if affected and affected > 0 else 0

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None, query_options=None) -> int:
        """Impala SELECT → Greenplum staging(TEMP) COPY 적재 → staging→target INSERT.

        한 Greenplum 세션(연결) 안에서 CREATE TEMP TABLE → COPY → INSERT 를 수행하므로
        TEMP 테이블이 INSERT 시점까지 보이며, 세션 종료 시 자동 정리된다.
        SELECT(Impala)과 INSERT(Greenplum)이 서로 다른 엔진일 때의 표준 패턴.
        query_options 는 Impala SELECT 에만 적용된다(INSERT 는 Greenplum).
        반환: INSERT 영향 행 수(미지원 시 적재 행 수).
        """
        from impala.dbapi import connect as impala_connect  # 지연 임포트
        import psycopg  # 지연 임포트

        loaded = 0
        impala_conn = impala_connect(**self.impala_dsn)
        try:
            cur = impala_conn.cursor()
            self._impala_execute(cur, impala_select, query_options)
            columns = [d[0] for d in cur.description]

            with psycopg.connect(self.greenplum_dsn) as gp:
                with gp.cursor() as gp_cur:
                    gp_cur.execute(staging_ddl)  # CREATE TEMP TABLE <staging_table> (...)
                    copy_sql = f"COPY {staging_table} ({', '.join(columns)}) FROM STDIN"
                    with gp_cur.copy(copy_sql) as copy:
                        for batch in _batches(cur, self.batch_size):
                            for row in batch:
                                copy.write_row(row)
                            loaded += len(batch)
                            if on_progress:
                                on_progress(loaded)
                    gp_cur.execute(insert_sql)  # INSERT INTO target SELECT ... FROM staging
                    affected = gp_cur.rowcount
                gp.commit()
            return affected if affected and affected > 0 else loaded
        finally:
            impala_conn.close()

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None, query_options=None) -> int:
        """copy 모드: Impala 에서 sub_query 결과를 스트리밍해 Greenplum 에 COPY 적재한다.

        Impala 커서를 batch_size 단위로 fetch 하며 psycopg COPY(STDIN)로 흘려보내므로,
        전체 결과를 메모리에 모으지 않는다. COPY 컬럼 목록은 Impala 커서 description 에서
        얻은 컬럼 순서를 그대로 사용한다.

        write_mode 가 "overwrite_partitions" 이고 partition_values 가 있으면, 적재 전에
        해당 파티션 값들을 ``DELETE ... WHERE col IN (...)`` 로 먼저 지운다. 이 선삭제 덕분에
        같은 task 를 재실행해도 중복 적재되지 않아 멱등(idempotent)하다 — DELETE+COPY 가
        한 트랜잭션에서 commit 되므로, 재시도 시 이전 결과를 덮어쓰는 효과가 난다.

        반환: COPY 로 적재한 총 행 수.
        """
        from impala.dbapi import connect as impala_connect  # 지연 임포트
        import psycopg  # 지연 임포트

        rows_written = 0
        impala_conn = impala_connect(**self.impala_dsn)
        try:
            cur = impala_conn.cursor()
            self._impala_execute(cur, sub_query, query_options)
            columns = [d[0] for d in cur.description]

            with psycopg.connect(self.greenplum_dsn) as gp:
                with gp.cursor() as gp_cur:
                    # 사전검증(preflight): COPY 로 한 행도 흘려보내기 전에, Impala SELECT 가
                    # 내는 컬럼이 모두 대상 테이블에 존재하는지 확인한다. 불일치면 여기서
                    # 명확한 에러로 즉시 실패(런타임 COPY 오류로 대용량 읽기 후 깨지는 것 방지).
                    if self.copy_preflight:
                        target_cols = _target_columns(gp_cur, target_table)
                        _check_copy_columns(columns, target_cols, target_table)
                    if write_mode == "overwrite_partitions" and partition_values:
                        # 멱등성: 적재 대상 파티션을 먼저 삭제 → DELETE+COPY 가 같은 트랜잭션에
                        # 묶여 commit 되므로 재실행해도 중복 없이 해당 파티션만 새 데이터로 교체.
                        placeholders = ", ".join(["%s"] * len(partition_values))
                        gp_cur.execute(
                            f"DELETE FROM {target_table} "
                            f"WHERE {partition_column} IN ({placeholders})",
                            partition_values,
                        )
                    copy_sql = f"COPY {target_table} ({', '.join(columns)}) FROM STDIN"
                    with gp_cur.copy(copy_sql) as copy:
                        for batch in _batches(cur, self.batch_size):
                            for row in batch:
                                copy.write_row(row)
                            rows_written += len(batch)
                            if on_progress:
                                on_progress(rows_written)
                gp.commit()
            return rows_written
        finally:
            impala_conn.close()


def _split_schema_table(target_table: str) -> tuple[str, str]:
    """``schema.table`` 을 (schema, table) 로 분리한다(스키마 없으면 ('', table)).

    따옴표는 제거하고, 점이 여러 개면 마지막을 테이블, 그 앞을 스키마로 본다.
    """
    cleaned = target_table.replace('"', "").strip()
    parts = cleaned.split(".")
    if len(parts) >= 2:
        return ".".join(parts[:-1]), parts[-1]
    return "", parts[-1]


def _target_columns(gp_cur, target_table: str) -> list[str]:
    """information_schema 에서 대상 테이블의 컬럼명 목록을 읽는다(없으면 빈 리스트).

    스키마가 주어지면 (table_schema, table_name) 으로, 아니면 table_name 만으로 조회한다.
    조회가 비면(테이블 미존재/권한 없음 등) 빈 리스트를 돌려 사전검증을 건너뛰게 한다.
    """
    schema, table = _split_schema_table(target_table)
    if schema:
        gp_cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
    else:
        gp_cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
            (table,),
        )
    return [r[0] for r in gp_cur.fetchall()]


def _check_copy_columns(select_columns, target_columns, target_table: str) -> None:
    """copy 모드 COPY 전 컬럼 정합성 검사(순수 함수 — DB 없이 단위 테스트 가능).

    Impala SELECT 가 내는 각 컬럼명이 대상 테이블에 존재하는지(대소문자 무시) 확인한다.
    없는 컬럼이 있으면 ``ValueError`` 로 명확한 사유를 올려 조기 실패시킨다.
    ``target_columns`` 가 비어 있으면(대상 조회 실패 등) 오탐을 피하려 검사를 건너뛴다.
    """
    if not target_columns:
        return
    target_lc = {c.lower() for c in target_columns}
    missing = [c for c in select_columns if c.lower() not in target_lc]
    if missing:
        raise ValueError(
            f"COPY 사전검증 실패: 대상 테이블 {target_table} 에 없는 컬럼 {missing}. "
            f"대상 컬럼={sorted(target_columns)}. SELECT 출력 컬럼과 대상 스키마를 맞추세요."
        )


def _batches(cursor, size: int) -> Iterator[list]:
    """DB 커서를 ``size`` 행씩 묶어 순차적으로 내어주는 제너레이터.

    ``fetchmany`` 로 일정 크기만 가져와 메모리 사용을 일정하게 유지하며(스트리밍),
    더 가져올 행이 없으면(빈 결과) 중단한다.
    """
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        yield rows


def build_backend(settings) -> Backend:
    """설정에 따라 실제 백엔드 또는 MockBackend를 선택한다(coordinator·executor 공용).

    greenplum.dsn 이 설정되면 실제 백엔드(statement/stage_insert 가능, copy 는 impala.host 도 필요),
    아무 것도 없으면 MockBackend(실제 I/O 없음 — 로컬 검증용).
    """
    if settings.greenplum_dsn:
        impala_dsn: dict = {}
        if settings.impala_host:
            impala_dsn = {
                "host": settings.impala_host,
                "port": settings.impala_port,
                "database": settings.impala_database,
                "auth_mechanism": settings.impala_auth_mechanism,
                "use_ssl": settings.impala_use_ssl,
            }
            if settings.impala_ca_cert:
                impala_dsn["ca_cert"] = settings.impala_ca_cert
            if settings.impala_auth_mechanism.upper() == "GSSAPI":
                impala_dsn["kerberos_service_name"] = settings.impala_kerberos_service_name
            else:
                if settings.impala_user:
                    impala_dsn["user"] = settings.impala_user
                if settings.impala_password:
                    impala_dsn["password"] = settings.impala_password
        logger.info(
            "ImpalaToGreenplumBackend 사용 (impala=%s, batch=%s)",
            settings.impala_host or "(미설정 → statement 모드만)",
            settings.copy_batch_size,
        )
        return ImpalaToGreenplumBackend(
            impala_dsn=impala_dsn,
            greenplum_dsn=settings.greenplum_dsn,
            batch_size=settings.copy_batch_size,
            query_options=getattr(settings, "impala_query_options", None),
            copy_preflight=getattr(settings, "copy_preflight", True),
        )
    logger.warning("greenplum.dsn 미설정 → MockBackend 사용")
    return MockBackend()
