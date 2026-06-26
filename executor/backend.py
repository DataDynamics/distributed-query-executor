"""Impala 읽기 / Greenplum 쓰기 백엔드.

실제 백엔드는 impyla + psycopg 를 사용하며, coordinator 테스트(및 로컬 개발)에서 DB
드라이버가 필요 없도록 지연 임포트(lazy import)한다. 라이브 클러스터 없이 개발/통합
테스트를 할 수 있도록 MockBackend도 제공한다.
"""

from __future__ import annotations

from typing import Iterator, Protocol


class Backend(Protocol):
    def move(
        self,
        sub_query: str,
        target_table: str,
        write_mode: str,
        partition_column: str,
        partition_values: list[str],
        on_progress=None,
    ) -> int:
        """[copy 모드] 소스에서 sub_query를 읽어 target_table에 COPY 적재, 행 수 반환."""
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
    ) -> int:
        """[stage_insert 모드] Impala 결과를 Greenplum staging 테이블에 COPY 적재 후,
        staging 을 소스로 하는 INSERT 를 실행한다. INSERT 영향 행 수를 반환."""
        ...


class MockBackend:
    """결정적인 행 수를 반환하고 실제 I/O는 하지 않음. 개발/테스트용."""

    def __init__(self, rows_per_value: int = 100):
        self.rows_per_value = rows_per_value

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None) -> int:
        total = max(1, len(partition_values)) * self.rows_per_value
        if on_progress:
            on_progress(total)
        return total

    def execute(self, sql: str) -> int:
        return self.rows_per_value

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None) -> int:
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

    def __init__(self, impala_dsn: dict, greenplum_dsn: str, batch_size: int = 10_000):
        self.impala_dsn = impala_dsn
        self.greenplum_dsn = greenplum_dsn
        self.batch_size = batch_size

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

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None) -> int:
        """Impala SELECT → Greenplum staging(TEMP) COPY 적재 → staging→target INSERT.

        한 Greenplum 세션(연결) 안에서 CREATE TEMP TABLE → COPY → INSERT 를 수행하므로
        TEMP 테이블이 INSERT 시점까지 보이며, 세션 종료 시 자동 정리된다.
        SELECT(Impala)과 INSERT(Greenplum)이 서로 다른 엔진일 때의 표준 패턴.
        반환: INSERT 영향 행 수(미지원 시 적재 행 수).
        """
        from impala.dbapi import connect as impala_connect  # 지연 임포트
        import psycopg  # 지연 임포트

        loaded = 0
        impala_conn = impala_connect(**self.impala_dsn)
        try:
            cur = impala_conn.cursor()
            cur.execute(impala_select)
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

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None) -> int:
        from impala.dbapi import connect as impala_connect  # 지연 임포트
        import psycopg  # 지연 임포트

        rows_written = 0
        impala_conn = impala_connect(**self.impala_dsn)
        try:
            cur = impala_conn.cursor()
            cur.execute(sub_query)
            columns = [d[0] for d in cur.description]

            with psycopg.connect(self.greenplum_dsn) as gp:
                with gp.cursor() as gp_cur:
                    if write_mode == "overwrite_partitions" and partition_values:
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


def _batches(cursor, size: int) -> Iterator[list]:
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        yield rows
