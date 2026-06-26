"""Impala-read / Greenplum-write backends.

The real backend uses impyla + psycopg and is imported lazily so the coordinator
test-suite (and local dev) does not require DB drivers. A MockBackend is provided
for development and integration tests without live clusters.
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
        """Read sub_query from source, write into target_table, return rows written."""
        ...


class MockBackend:
    """Returns a deterministic row count; no real I/O. For dev/tests."""

    def __init__(self, rows_per_value: int = 100):
        self.rows_per_value = rows_per_value

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None) -> int:
        total = max(1, len(partition_values)) * self.rows_per_value
        if on_progress:
            on_progress(total)
        return total


class ImpalaToGreenplumBackend:
    """Real backend: stream from Impala via impyla, COPY into Greenplum via psycopg."""

    def __init__(self, impala_dsn: dict, greenplum_dsn: str, batch_size: int = 10_000):
        self.impala_dsn = impala_dsn
        self.greenplum_dsn = greenplum_dsn
        self.batch_size = batch_size

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None) -> int:
        from impala.dbapi import connect as impala_connect  # lazy import
        import psycopg  # lazy import

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
