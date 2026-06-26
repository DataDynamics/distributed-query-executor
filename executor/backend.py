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
        """소스에서 sub_query를 읽어 target_table에 쓰고, 적재된 행 수를 반환."""
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


class ImpalaToGreenplumBackend:
    """실제 백엔드: impyla로 Impala에서 스트리밍, psycopg COPY로 Greenplum에 적재."""

    def __init__(self, impala_dsn: dict, greenplum_dsn: str, batch_size: int = 10_000):
        self.impala_dsn = impala_dsn
        self.greenplum_dsn = greenplum_dsn
        self.batch_size = batch_size

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
