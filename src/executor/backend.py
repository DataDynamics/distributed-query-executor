"""Impala 읽기 / Greenplum 쓰기 백엔드.

실제 백엔드는 impyla + psycopg 를 사용하며, coordinator 테스트(및 로컬 개발)에서 DB
드라이버가 필요 없도록 지연 임포트(lazy import)한다. 라이브 클러스터 없이 개발/통합
테스트를 할 수 있도록 MockBackend도 제공한다.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from contextlib import contextmanager
from typing import Protocol

logger = logging.getLogger(__name__)


def _emit(on_stage, name: str, event: str, meta: dict | None = None) -> None:
    """단계 콜백을 안전하게 호출한다(콜백이 None 이면 아무것도 하지 않음).

    모니터링용 부가 기능이므로, 콜백에서 예외가 나더라도 본 적재 작업을 깨뜨리지 않게
    삼켜 로깅만 한다.
    """
    if on_stage is None:
        return
    try:
        on_stage(name, event, meta)
    except Exception:
        logger.warning("on_stage(%s,%s) 콜백 실패 — 무시", name, event, exc_info=True)


class _GreenplumPool:
    """executor 1대용 간단한 Greenplum(psycopg) 커넥션 풀 — 외부 의존성 없이 표준 라이브러리로 구현.

    왜 필요한가: 풀이 없으면 task 마다 새로 connect 하므로 (1) 동시 GP 연결 수가 제어되지
    않고(다운스트림 max_connections 압박), (2) task 마다 인증·핸드셰이크 비용을 다시 치른다.
    이 풀은 **동시 연결을 maxsize 로 제한**(BoundedSemaphore)하고, 유휴 연결을 재사용한다.

    재사용 안전성(중요): stage_insert 는 세션 전용 ``CREATE TEMP TABLE`` 을 쓰므로, 연결을
    그대로 재사용하면 이전 task 의 TEMP 가 남아 다음 ``CREATE TEMP TABLE`` 이 충돌한다.
    그래서 **반납 시 ``DISCARD ALL`` 로 세션을 초기화**(TEMP 테이블·GUC·준비문 등 제거)한다.
    초기화에 실패한 연결(드라이버/엔진 미지원·끊김 등)은 재사용하지 않고 닫아 버린다(안전 폴백).

    blocking psycopg 호출은 executor 가 ``to_thread`` 로 감싸 실행하므로, 스레드 안전한
    ``BoundedSemaphore`` + ``LifoQueue`` 조합으로 충분하다(이벤트 루프와 무관).
    """

    def __init__(self, dsn: str, maxsize: int, *, connect=None):
        self._dsn = dsn
        self._maxsize = max(1, int(maxsize))
        # 동시 "사용 중" 연결 수를 maxsize 로 제한. 슬롯이 없으면 반납될 때까지 대기.
        self._sema = threading.BoundedSemaphore(self._maxsize)
        # 유휴(반납되어 재사용 가능한) 연결 보관. LIFO 라 최근 쓴 연결을 우선 재사용(캐시 친화).
        self._idle: "queue.LifoQueue" = queue.LifoQueue()
        self._connect = connect or self._default_connect

    def _default_connect(self):
        import psycopg  # 지연 임포트(드라이버 선택 설치)

        return psycopg.connect(self._dsn)

    def _reset(self, conn) -> bool:
        """반납된 연결의 세션 상태를 깨끗이 비운다. 성공하면 True(재사용 가능)."""
        try:
            conn.rollback()  # 혹시 열린 트랜잭션이 있으면 정리(autocommit 전환 위해 선행)
            prev = conn.autocommit
            conn.autocommit = True  # DISCARD ALL 은 트랜잭션 블록 밖에서만 실행 가능
            try:
                conn.execute("DISCARD ALL")  # TEMP 테이블·SET·준비문 등 세션 상태 전부 제거
            finally:
                conn.autocommit = prev
            return True
        except Exception:
            logger.warning("GP 커넥션 초기화(DISCARD ALL) 실패 — 해당 연결은 폐기", exc_info=True)
            return False

    @staticmethod
    def _safe_close(conn) -> None:
        try:
            conn.close()
        except Exception:
            pass

    @contextmanager
    def connection(self):
        """풀에서 연결을 하나 빌려 준다(없으면 생성, maxsize 도달 시 반납 대기)."""
        self._sema.acquire()  # 동시 연결 상한 확보(필요 시 블로킹)
        conn = None
        try:
            try:
                conn = self._idle.get_nowait()  # 유휴 연결 재사용
            except queue.Empty:
                conn = self._connect()  # 없으면 새로 연결(슬롯을 이미 잡았으니 상한 내)
            try:
                yield conn
            except Exception:
                # 작업 중 오류 → 트랜잭션 상태가 불확실하므로 재사용하지 않고 폐기
                self._safe_close(conn)
                conn = None
                raise
            # 정상 종료 → 세션 초기화 후 유휴 풀에 반납(초기화 실패면 폐기)
            if self._reset(conn):
                self._idle.put(conn)
            else:
                self._safe_close(conn)
            conn = None
        finally:
            self._sema.release()

    def close(self) -> None:
        """풀에 남은 유휴 연결을 모두 닫는다(종료 시)."""
        while True:
            try:
                self._safe_close(self._idle.get_nowait())
            except queue.Empty:
                break


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
        on_stage=None,
    ) -> int:
        """[copy 모드] 소스에서 sub_query를 읽어 target_table에 COPY 적재, 행 수 반환.

        query_options: 이 task 의 Impala 쿼리 옵션(SET). 전역 기본값 위에 병합된다.
        on_stage: 세부 단계 경계 콜백 ``on_stage(name, event, meta=None)``. 모니터링용이며
            None 이면 계측을 생략한다(core.phases 참고).
        """
        ...

    def execute(self, sql: str, on_stage=None) -> int:
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
        on_stage=None,
    ) -> int:
        """[stage_insert 모드] Impala 결과를 Greenplum staging 테이블에 COPY 적재 후,
        staging 을 소스로 하는 INSERT 를 실행한다. INSERT 영향 행 수를 반환."""
        ...

    def export_to_local_csv(
        self,
        sub_query: str,
        out_path: str,
        csv_options=None,
        on_progress=None,
        query_options=None,
        on_stage=None,
    ) -> int:
        """[local_stage 1단계] Impala SELECT 결과를 로컬 CSV 파일 하나로 스트리밍 저장, 행수 반환.

        executor 가 자기 호스트 로컬 디스크(out_path)에 CSV 를 떨어뜨린다. 이후 Greenplum 이
        file:// 외부테이블로 이 파일을 세그먼트 로컬에서 병렬로 읽어 적재한다(2단계).
        """
        ...

    def load_external_csv(
        self,
        external_ddl: str,
        staging_ddl,
        staging_load_sql: str,
        pre_delete_sql,
        insert_sql: str,
        cleanup_sqls=None,
        on_stage=None,
    ) -> int:
        """[local_stage 2단계] file:// 외부테이블 → staging 적재 → target INSERT(coordinator 실행).

        coordinator 가 조립한 SQL 들을 한 GP 트랜잭션으로 실행하고, 커밋 후 cleanup 을
        best-effort 로 수행한다. INSERT 영향 행 수를 반환한다."""
        ...

    def segment_host_counts(self) -> dict:
        """[local_stage 파일 예산] gp_segment_configuration 의 호스트별 primary 세그먼트 수.

        ``{hostname: S_h}`` 를 반환한다. coordinator 가 file:// "호스트당 파일 수 ≤ S_h"
        규칙에 맞춰 파일을 호스트에 배분하는 근거다. 빈 dict 면 배분/검증을 생략한다(목)."""
        ...

    def segment_hosts(self) -> set:
        """[local_stage 검증] gp_segment_configuration 의 primary 세그먼트 호스트명 집합.

        coordinator 가 file:// URI 의 호스트가 실제 세그먼트 호스트인지 검증하는 데 쓴다.
        빈 집합을 반환하면 검증을 생략한다(목/조회 불가 시)."""
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

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None, query_options=None, on_stage=None) -> int:
        # 파티션 값 개수 × rows_per_value 를 적재한 것으로 가정(값이 없으면 최소 1로 간주).
        total = max(1, len(partition_values)) * self.rows_per_value
        # 실제 I/O 는 없지만, 대시보드/테스트에서 단계 타임라인이 보이도록 합성 이벤트를 방출한다.
        _emit(on_stage, "IMPALA_SUBMIT", "start")
        _emit(on_stage, "IMPALA_SUBMIT", "end")
        _emit(on_stage, "STREAM_COPY", "start")
        if on_progress:
            on_progress(total)
        _emit(on_stage, "STREAM_COPY", "end",
              {"rows": total, "read_wait_ms": 0, "write_wait_ms": 0,
               "read_starve_ms": 0, "finalize_wait_ms": 0, "rows_per_sec": total})
        _emit(on_stage, "COMMIT", "start")
        _emit(on_stage, "COMMIT", "end")
        return total

    def execute(self, sql: str, on_stage=None) -> int:
        # statement 모드: 항상 rows_per_value 행이 영향받은 것으로 가정.
        _emit(on_stage, "INSERT", "start")
        _emit(on_stage, "INSERT", "end", {"rows": self.rows_per_value})
        _emit(on_stage, "COMMIT", "start")
        _emit(on_stage, "COMMIT", "end")
        return self.rows_per_value

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None, query_options=None, on_stage=None) -> int:
        # stage_insert 모드: rows_per_value 행을 staging→target 으로 옮긴 것으로 가정.
        _emit(on_stage, "IMPALA_SUBMIT", "start")
        _emit(on_stage, "IMPALA_SUBMIT", "end")
        if staging_ddl:
            _emit(on_stage, "STAGING_DDL", "start")
            _emit(on_stage, "STAGING_DDL", "end")
        _emit(on_stage, "STREAM_COPY", "start")
        if on_progress:
            on_progress(self.rows_per_value)
        _emit(on_stage, "STREAM_COPY", "end",
              {"rows": self.rows_per_value, "read_wait_ms": 0, "write_wait_ms": 0,
               "read_starve_ms": 0, "finalize_wait_ms": 0, "rows_per_sec": self.rows_per_value})
        _emit(on_stage, "INSERT", "start")
        _emit(on_stage, "INSERT", "end", {"rows": self.rows_per_value})
        _emit(on_stage, "COMMIT", "start")
        _emit(on_stage, "COMMIT", "end")
        return self.rows_per_value

    def export_to_local_csv(self, sub_query, out_path, csv_options=None, on_progress=None, query_options=None, on_stage=None) -> int:
        # local_stage 1단계: 실제 파일은 만들지 않고 합성 단계 이벤트만 방출, rows_per_value 반환.
        total = self.rows_per_value
        _emit(on_stage, "IMPALA_SUBMIT", "start")
        _emit(on_stage, "IMPALA_SUBMIT", "end")
        _emit(on_stage, "EXPORT_WRITE", "start")
        if on_progress:
            on_progress(total)
        _emit(on_stage, "EXPORT_WRITE", "end", {"rows": total})
        return total

    def load_external_csv(self, external_ddl, staging_ddl, staging_load_sql, pre_delete_sql, insert_sql, cleanup_sqls=None, on_stage=None) -> int:
        # local_stage 2단계: 실제 GP 호출 없이 단계 이벤트만 방출하고 rows_per_value 반환.
        if staging_ddl:
            _emit(on_stage, "STAGING_DDL", "start")
            _emit(on_stage, "STAGING_DDL", "end")
        _emit(on_stage, "PXF_EXTERNAL_DDL", "start")
        _emit(on_stage, "PXF_EXTERNAL_DDL", "end")
        _emit(on_stage, "STAGE_LOAD", "start")
        _emit(on_stage, "STAGE_LOAD", "end", {"rows": self.rows_per_value})
        if pre_delete_sql:
            _emit(on_stage, "DELETE", "start")
            _emit(on_stage, "DELETE", "end")
        _emit(on_stage, "INSERT", "start")
        _emit(on_stage, "INSERT", "end", {"rows": self.rows_per_value})
        _emit(on_stage, "COMMIT", "start")
        _emit(on_stage, "COMMIT", "end")
        return self.rows_per_value

    def segment_host_counts(self) -> dict:
        # 목: 빈 dict → coordinator 의 파일 예산 배분/호스트 검증을 건너뛰게 한다.
        return {}

    def segment_hosts(self) -> set:
        # 호스트 집합은 호스트별 카운트의 키에서 파생(목이면 빈 집합).
        return set(self.segment_host_counts())


class ImpalaToGreenplumBackend:
    """실제 백엔드: Impala 에서 스트리밍해 psycopg COPY 로 Greenplum 에 적재.

    소스는 impyla(Impala) 하나다. impala_dsn 은 impyla ``connect()`` 에 그대로 전달된다
    (auth_mechanism='LDAP', user/password, use_ssl=True, ca_cert='/path/to/ca.pem').
    """

    def __init__(self, impala_dsn: dict, greenplum_dsn: str, batch_size: int = 10_000,
                 query_options: dict | None = None, copy_preflight: bool = True,
                 pool_max: int = 8, pipeline: bool = True, queue_size: int = 8,
                 copy_format: str = "text", stage_convert_types: bool = False):
        # Impala 소스 접속 dict(impyla connect 에 그대로 전달).
        self.impala_dsn = impala_dsn
        # local_stage export 의 impyla 커서에 넘길 convert_types 값. False 면 형변환을 꺼
        # TIMESTAMP/DATE/DECIMAL 을 wire 문자열 그대로 받아(재파싱 비용 제거) CSV 로 바로 쓴다.
        self.stage_convert_types = stage_convert_types
        self.greenplum_dsn = greenplum_dsn
        self.batch_size = batch_size
        # Impala 쿼리 옵션 전역 기본값(SET). 요청별 옵션이 이 위에 병합된다.
        self.query_options: dict = query_options or {}
        # copy 모드 COPY 전에 SELECT 컬럼이 대상 테이블에 존재하는지 사전검증할지 여부.
        # 대용량 스트리밍을 시작하기 전에 컬럼 불일치를 잡아 빠르게 실패시킨다.
        self.copy_preflight = copy_preflight
        # COPY 파이프라인: Impala 읽기와 GP 쓰기를 별도 스레드로 겹칠지 여부 + 큐 크기.
        self.pipeline = pipeline
        self.queue_size = max(1, int(queue_size))
        # COPY 포맷(text|binary). binary 는 텍스트 인코딩을 건너뛰어 클라이언트 CPU 를 줄인다.
        self.copy_format = copy_format if copy_format in ("text", "binary") else "text"
        # Greenplum 커넥션 풀: 동시 GP 연결을 pool_max 로 제한하고 연결을 재사용한다.
        # (Impala 쪽은 task 마다 새로 연결한다 — 읽기 커서가 연결에 묶여 풀링이 까다로움.)
        self._gp_pool = _GreenplumPool(greenplum_dsn, pool_max)

    def _source_connect(self):
        """Impala 소스 연결을 연다(impyla). 드라이버는 지연 임포트한다."""
        # 연결 실패 시 상위엔 드라이버 예외만 올라가므로, 어느 호스트로 붙었는지
        # backend 레벨에서 남긴다(연결/인증 문제 진단의 첫 단서).
        logger.debug(
            "Impala 연결 시도: host=%s port=%s",
            self.impala_dsn.get("host"), self.impala_dsn.get("port"),
        )
        from impala.dbapi import connect as impala_connect  # 지연 임포트

        return impala_connect(**self.impala_dsn)

    def _open_source_cursor(self, conn, convert_types: bool | None = None):
        """Impala 커서를 연다. ``convert_types=False`` 면 값 형변환을 꺼 서버 문자열 그대로 받는다.

        TIMESTAMP/DATE/DECIMAL 은 wire 에서 이미 문자열로 오는데, impyla 기본값은 이를
        datetime/Decimal 로 되돌려 파싱한다. CSV 로 다시 쓸 export 경로에서는 그 변환이
        순수 낭비이므로 ``cursor(convert_types=False)`` 로 꺼서 문자열 그대로 받는다
        (INT/DOUBLE/BOOL 은 네이티브라 영향 없음). 해당 kwarg 미지원 구버전이면 기본 커서로 폴백.
        """
        if convert_types is None:
            return conn.cursor()
        try:
            return conn.cursor(convert_types=convert_types)
        except TypeError:
            logger.warning("impyla cursor(convert_types=...) 미지원 — 기본 커서로 폴백")
            return conn.cursor()

    def _source_execute(self, cur, sql: str, query_options) -> None:
        """Impala 커서로 sql 을 실행한다. 전역+요청별 옵션을 configuration 으로 병합한다.

        병합 결과가 비어 있으면(둘 다 미지정) configuration 인자를 아예 넘기지 않고
        그대로 실행한다(요청자 의도: 옵션이 없으면 기본 동작 유지).
        """
        opts = {**self.query_options, **(query_options or {})}
        if opts:
            cur.execute(sql, configuration=opts)
        else:
            cur.execute(sql)

    def _build_copy(self, gp_cur, table: str, columns: list[str]) -> tuple[str, list | None]:
        """COPY SQL 과 (바이너리면) 컬럼 타입 목록을 만든다.

        - text 포맷(기본): ``COPY t (cols) FROM STDIN`` 을 돌려주고 types 는 None.
        - binary 포맷: ``... WITH (FORMAT BINARY)`` 를 만들고, psycopg 가 각 값을 올바른
          바이너리로 인코딩하도록 **대상 테이블 카탈로그에서 컬럼 타입(typname)을 해석**해
          SELECT 컬럼 순서에 맞춰 돌려준다. 하나라도 타입을 못 찾으면(불일치/미존재) 안전하게
          text 로 폴백한다(바이너리는 타입이 정확해야만 동작하므로).
        """
        col_list = ", ".join(columns)
        if self.copy_format == "binary":
            types = _resolve_copy_types(gp_cur, table, columns)
            if types:
                return f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT BINARY)", types
            logger.warning(
                "바이너리 COPY 타입 해석 실패 → 텍스트 COPY 로 폴백 (table=%s)", table
            )
        return f"COPY {table} ({col_list}) FROM STDIN", None

    def _stream_to_copy(self, cur, gp_cur, copy_sql: str, on_progress, types=None):
        """Impala 결과를 Greenplum COPY(STDIN)로 흘려보낸다. 설정에 따라 파이프라인/직렬 선택.

        types 가 있으면(바이너리 COPY) copy 진입 후 ``set_types`` 로 각 컬럼 타입을 지정한다.
        반환: (적재 행수, read_wait, write_wait, finalize_wait, read_starve) — 초 단위.
        각 구간의 의미와 진단법은 ``_copy_stats`` 주석 참고.
        """
        if self.pipeline:
            return self._stream_pipelined(cur, gp_cur, copy_sql, on_progress, types)
        return self._stream_serial(cur, gp_cur, copy_sql, on_progress, types)

    def _stream_serial(self, cur, gp_cur, copy_sql: str, on_progress, types=None):
        """직렬 스트리밍(한 스레드): fetch→write 를 번갈아 수행. read_starve 는 0.

        읽기·쓰기가 교차 실행되므로 구간별 시간을 따로 누적한다:
        - ``read_wait``  : ``fetchmany`` — Impala 가 행을 보내 줄 때까지 대기(소스 지연).
        - ``write_wait`` : ``copy.write_row`` — psycopg 송신 버퍼 인코딩+소켓 송신. 서버가
          소비를 못 따라와 버퍼가 차면 여기서 backpressure 로 막힌다.
        - ``finalize_wait`` : COPY 종료(``PQputCopyEnd``) — 입력이 끝난 뒤 서버가 남은 데이터를
          마저 ingest 완료할 때까지의 대기. 크면 서버(GP) 처리 병목.
        """
        loaded = 0
        read_wait = 0.0
        write_wait = 0.0
        t_end = time.monotonic()
        with gp_cur.copy(copy_sql) as copy:
            if types:
                copy.set_types(types)  # 바이너리 COPY: 각 컬럼의 PG 타입을 지정
            while True:
                t = time.monotonic()
                batch = cur.fetchmany(self.batch_size)   # Impala 읽기
                read_wait += time.monotonic() - t
                if not batch:
                    break
                t = time.monotonic()
                for row in batch:
                    copy.write_row(row)                  # Greenplum 쓰기(버퍼 인코딩+송신)
                write_wait += time.monotonic() - t
                loaded += len(batch)
                if on_progress:
                    on_progress(loaded)
            t_end = time.monotonic()
        finalize_wait = time.monotonic() - t_end
        return loaded, read_wait, write_wait, finalize_wait, 0.0

    def _stream_pipelined(self, cur, gp_cur, copy_sql: str, on_progress, types=None):
        """파이프라인 스트리밍(2스레드): 리더가 배치를 큐에 채우고, 라이터(현재 스레드)가 COPY.

        읽기(Impala fetch)와 쓰기(GP COPY)를 겹쳐 실행해 벽시계를 줄인다. 큐는 bounded 라
        한쪽이 느리면 자연히 backpressure 가 걸린다(메모리 ≈ queue_size × batch_size 행).

        연결 안전성: Impala 커서는 **리더 스레드만**, GP 커서/COPY 는 **라이터 스레드만** 만진다
        (한 연결을 두 스레드가 동시에 건드리지 않음). description 은 호출부에서 리더 시작 전에
        이미 읽었고, thread.start() 가 메모리 배리어라 안전하다.

        진단 지표(라이터 관점에서 벽시계를 분해):
        - ``read_starve`` : 라이터가 다음 배치를 기다리며 큐가 빌 때 막힌 시간 → **Impala 가
          못 따라와** 라이터가 굶는 시간. 크면 소스(Impala)가 병목.
        - ``write_wait``  : 라이터가 실제 ``write_row`` 에 쓴 시간 → GP 쓰기 비용.
        - ``finalize_wait`` : COPY 종료(서버 ingest 완료) 대기.
        대략 ``duration ≈ read_starve + write_wait + finalize`` 로, 어느 항이 큰지가 곧 병목이다.
        ``read_wait`` (리더의 순수 fetch 시간)은 참고용으로 함께 보고한다.
        """
        import queue as _queue
        import threading

        q: "_queue.Queue" = _queue.Queue(maxsize=self.queue_size)
        producer_err: list = []
        read_wait_box = [0.0]
        stop = threading.Event()  # 라이터가 죽으면 세워, put 에서 막힌 리더를 데드락 없이 깨운다.

        def _producer():
            # 리더 스레드: Impala 에서 배치를 읽어 큐에 넣는다(큐가 차면 put 에서 막힘=backpressure).
            rw = 0.0
            try:
                while not stop.is_set():
                    t = time.monotonic()
                    batch = cur.fetchmany(self.batch_size)
                    rw += time.monotonic() - t
                    if not batch:
                        break
                    # 큐가 차면 대기하되, 라이터가 죽어 stop 이 서면 빠져나온다(데드락 방지).
                    while not stop.is_set():
                        try:
                            q.put(batch, timeout=0.5)
                            break
                        except _queue.Full:
                            continue
            except BaseException as exc:  # noqa: BLE001 — 어떤 예외든 라이터로 전달해야 함
                producer_err.append(exc)
            finally:
                read_wait_box[0] = rw
                # 종료(또는 오류) 신호 — 라이터의 get 을 깨운다. 큐가 차 있으면(라이터가 이미
                # 떠났을 때) 건너뛴다(어차피 라이터는 더 이상 읽지 않음).
                try:
                    q.put_nowait(None)
                except _queue.Full:
                    pass

        thread = threading.Thread(target=_producer, name="copy-reader", daemon=True)
        thread.start()

        loaded = 0
        write_wait = 0.0
        read_starve = 0.0
        t_end = time.monotonic()
        try:
            with gp_cur.copy(copy_sql) as copy:
                if types:
                    copy.set_types(types)  # 바이너리 COPY: 각 컬럼의 PG 타입을 지정
                while True:
                    t = time.monotonic()
                    batch = q.get()               # 큐가 비면 대기 = Impala 를 기다리며 굶는 시간
                    read_starve += time.monotonic() - t
                    if batch is None:
                        break
                    t = time.monotonic()
                    for row in batch:
                        copy.write_row(row)
                    write_wait += time.monotonic() - t
                    loaded += len(batch)
                    if on_progress:
                        on_progress(loaded)
                t_end = time.monotonic()
            finalize_wait = time.monotonic() - t_end
        finally:
            # 라이터가 예외로 빠졌을 수 있으니 리더에 중단을 지시하고, put 에서 막혀 있으면
            # 큐를 비워 깨운 뒤 조인한다(데드락 방지 — join 이 영원히 대기하지 않게).
            stop.set()
            try:
                while True:
                    q.get_nowait()
            except _queue.Empty:
                pass
            thread.join()
        if producer_err:
            # 리더에서 난 오류를 라이터(호출부)로 올려 트랜잭션이 롤백되게 한다.
            raise producer_err[0]
        return loaded, read_wait_box[0], write_wait, finalize_wait, read_starve

    @staticmethod
    def _copy_stats(loaded: int, read_wait: float, write_wait: float,
                    finalize_wait: float = 0.0, read_starve: float = 0.0) -> dict:
        """STREAM_COPY 단계 종료 meta(행수/각 구간 대기 ms/초당 행수)를 만든다.

        직렬 모드는 read_wait+write_wait+finalize 가 곧 벽시계다. 파이프라인 모드는 읽기·쓰기가
        겹치므로 라이터 관점의 read_starve(=Impala 대기)+write_wait+finalize 가 벽시계에 가깝다.
        rows_per_sec 는 실제 벽시계 근사값(파이프라인이면 겹침 반영)으로 계산한다.
        """
        wall = max(read_starve + write_wait + finalize_wait,  # 파이프라인 벽시계 근사
                   read_wait + write_wait + finalize_wait)     # 직렬 벽시계
        return {
            "rows": loaded,
            "read_wait_ms": int(read_wait * 1000),
            "write_wait_ms": int(write_wait * 1000),
            "read_starve_ms": int(read_starve * 1000),
            "finalize_wait_ms": int(finalize_wait * 1000),
            "rows_per_sec": int(loaded / wall) if wall > 0 else 0,
        }

    def execute(self, sql: str, on_stage=None) -> int:
        """statement 모드: 대상 Greenplum 에서 SQL(예: INSERT ... SELECT)을 그대로 실행.

        COPY를 쓰지 않으므로 컬럼 매핑은 SQL(INSERT 컬럼 목록/SELECT)이 책임진다.
        반환값은 cursor.rowcount(영향받은 행 수, 미지원 시 0).
        """
        with self._gp_pool.connection() as conn:
            with conn.cursor() as cur:
                _emit(on_stage, "INSERT", "start")
                logger.debug("statement 실행: %s", sql)
                cur.execute(sql)
                affected = cur.rowcount
                _emit(on_stage, "INSERT", "end",
                      {"rows": affected if affected and affected > 0 else None})
            _emit(on_stage, "COMMIT", "start")
            conn.commit()
            _emit(on_stage, "COMMIT", "end")
        rows = affected if affected and affected > 0 else 0
        logger.debug("statement 완료: %s행 반영", rows)
        return rows

    def stage_and_insert(self, impala_select, staging_table, staging_ddl, insert_sql, on_progress=None, query_options=None, on_stage=None) -> int:
        """Impala SELECT → Greenplum staging(TEMP) COPY 적재 → staging→target INSERT.

        한 Greenplum 세션(연결) 안에서 CREATE TEMP TABLE → COPY → INSERT 를 수행하므로
        TEMP 테이블이 INSERT 시점까지 보인다. INSERT 직후(같은 트랜잭션) staging_table 을
        **명시적으로 DROP** 해 커밋 시점에 확정 정리하므로, 커넥션 풀이 세션을 재사용해도
        잔존 TEMP 로 인한 다음 task 의 "already exists" 가 발생하지 않는다(DISCARD ALL
        동작에 의존하지 않는다). staging 이름은 coordinator 가 task 마다 고유화해 보낸다.
        SELECT(Impala)과 INSERT(Greenplum)이 서로 다른 엔진일 때의 표준 패턴.
        query_options 는 Impala SELECT 에만 적용된다(INSERT 는 Greenplum).
        반환: INSERT 영향 행 수(미지원 시 적재 행 수).

        staging_ddl 이 비어 있으면 테이블 생성을 건너뛰고 **이미 존재하는** staging_table 에
        곧장 COPY 한다. 이 경우 staging_table 은 세션 임시(TEMP)가 아니므로, 여러 task 가
        같은 영구 테이블을 공유하면 COPY/INSERT 가 서로 간섭할 수 있다 — 호출자가 격리를
        보장해야 한다(예: job·파티션별 고유 staging_table 사용).
        """
        loaded = 0
        impala_conn = self._source_connect()
        try:
            cur = impala_conn.cursor()
            _emit(on_stage, "IMPALA_SUBMIT", "start")
            self._source_execute(cur, impala_select, query_options)
            columns = [d[0] for d in cur.description]
            _emit(on_stage, "IMPALA_SUBMIT", "end")

            with self._gp_pool.connection() as gp:
                with gp.cursor() as gp_cur:
                    if staging_ddl:
                        _emit(on_stage, "STAGING_DDL", "start")
                        gp_cur.execute(staging_ddl)  # CREATE TEMP TABLE <staging_table> (...)
                        _emit(on_stage, "STAGING_DDL", "end")
                    # staging_ddl 이 없으면 생성을 건너뛰고 기존 staging_table 에 그대로 COPY.
                    copy_sql, copy_types = self._build_copy(gp_cur, staging_table, columns)
                    logger.debug("stage_insert COPY 시작(pipeline=%s, format=%s): %s",
                                 self.pipeline, self.copy_format, copy_sql)
                    _emit(on_stage, "STREAM_COPY", "start")
                    loaded, read_wait, write_wait, finalize_wait, read_starve = \
                        self._stream_to_copy(cur, gp_cur, copy_sql, on_progress, copy_types)
                    # STREAM_COPY 종료 = Impala 조회 완료 시점, loaded = 읽은(=staging 적재) 건수.
                    _emit(on_stage, "STREAM_COPY", "end",
                          self._copy_stats(loaded, read_wait, write_wait,
                                           finalize_wait, read_starve))
                    logger.debug("stage_insert 적재 완료(%s행) → INSERT 실행: %s",
                                 loaded, insert_sql)
                    _emit(on_stage, "INSERT", "start")
                    gp_cur.execute(insert_sql)  # INSERT INTO target SELECT ... FROM staging
                    affected = gp_cur.rowcount
                    _emit(on_stage, "INSERT", "end",
                          {"rows": affected if affected and affected > 0 else loaded})
                    # 이번 실행에 우리가 만든(staging_ddl 있는) staging 을 같은 트랜잭션 안에서
                    # 드롭한다 → 커밋 시 확정되어, 커넥션 풀 재사용 시 잔존 TEMP 로 인한 다음
                    # task 의 "already exists" 를 원천 차단(DISCARD ALL 동작에 의존하지 않음).
                    # staging_ddl 이 없으면(기존 영구 테이블에 직접 COPY) 사용자 테이블이므로
                    # 절대 드롭하지 않는다.
                    if staging_ddl:
                        # staging_table 은 coordinator 가 CREATE/INSERT 와 같은 형태(따옴표
                        # 없는 bare 식별자)로 보낸 이름이라 그대로 DROP 한다.
                        gp_cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                        logger.debug("stage_insert staging 정리: DROP %s", staging_table)
                _emit(on_stage, "COMMIT", "start")
                gp.commit()
                _emit(on_stage, "COMMIT", "end")
            return affected if affected and affected > 0 else loaded
        finally:
            impala_conn.close()

    def export_to_local_csv(self, sub_query, out_path, csv_options=None, on_progress=None, query_options=None, on_stage=None) -> int:
        """local_stage 1단계: Impala SELECT 결과를 out_path 의 로컬 CSV 파일로 스트리밍 저장.

        impyla 커서를 batch_size 단위로 fetch 하며 표준 라이브러리 ``csv`` 로 한 줄씩 쓴다.
        전체 결과를 메모리에 올리지 않는다. CSV 방언(delimiter/null/quote)은 GP file:// 외부
        테이블의 ``FORMAT 'CSV'(...)`` 와 정확히 일치해야 하므로 coordinator 가 넘긴 값을 쓴다.
        NULL 은 지정된 null 문자열로, 나머지 값은 문자열화해 기록한다. 반환: 기록한 행 수.
        """
        import csv
        import os

        opts = csv_options or {}
        delimiter = opts.get("delimiter", "`")
        null_str = opts.get("null", "")
        quote = opts.get("quote", '"')

        # out_path 의 상위 디렉터리({local_dir}/{job_id}/)를 만들어 둔다(이미 있으면 통과).
        parent = os.path.dirname(out_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        written = 0
        impala_conn = self._source_connect()
        try:
            # convert_types=False 로 형변환을 꺼 timestamp/date/decimal 을 문자열 그대로 받는다.
            cur = self._open_source_cursor(impala_conn, convert_types=self.stage_convert_types)
            _emit(on_stage, "IMPALA_SUBMIT", "start")
            self._source_execute(cur, sub_query, query_options)
            _emit(on_stage, "IMPALA_SUBMIT", "end")
            _emit(on_stage, "EXPORT_WRITE", "start")
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(
                    f, delimiter=delimiter, quotechar=quote,
                    quoting=csv.QUOTE_MINIMAL, lineterminator="\n",
                )
                while True:
                    batch = cur.fetchmany(self.batch_size)
                    if not batch:
                        break
                    for row in batch:
                        writer.writerow([null_str if v is None else v for v in row])
                    written += len(batch)
                    if on_progress:
                        on_progress(written)
            _emit(on_stage, "EXPORT_WRITE", "end", {"rows": written})
            logger.debug("local_stage export 완료: %s행 → %s", written, out_path)
            return written
        finally:
            impala_conn.close()

    def load_external_csv(self, external_ddl, staging_ddl, staging_load_sql, pre_delete_sql, insert_sql, cleanup_sqls=None, on_stage=None) -> int:
        """local_stage 2단계: file:// 외부테이블 생성 → staging 적재 → target INSERT.

        coordinator 가 조립한 SQL 을 한 GP 트랜잭션으로 순서대로 실행한다:
          (staging_ddl?) → external_ddl → staging_load_sql → (pre_delete_sql?) → insert_sql
        커밋 뒤 cleanup_sqls(외부테이블 DROP 등)를 별도 트랜잭션에서 best-effort 로 수행한다
        (실패해도 적재 결과에는 영향이 없으므로 로깅만 한다). 반환: INSERT 영향 행 수.

        Impala 는 관여하지 않으므로(순수 GP 작업) coordinator 처럼 impala_dsn 이 없어도 동작한다.
        """
        affected = 0
        with self._gp_pool.connection() as gp:
            with gp.cursor() as cur:
                if staging_ddl:
                    _emit(on_stage, "STAGING_DDL", "start")
                    cur.execute(staging_ddl)  # CREATE TABLE staging (...) DISTRIBUTED BY ...
                    _emit(on_stage, "STAGING_DDL", "end")
                _emit(on_stage, "PXF_EXTERNAL_DDL", "start")
                cur.execute(external_ddl)  # CREATE EXTERNAL TABLE ext (...) LOCATION('file://...')
                _emit(on_stage, "PXF_EXTERNAL_DDL", "end")
                _emit(on_stage, "STAGE_LOAD", "start")
                cur.execute(staging_load_sql)  # INSERT INTO staging SELECT * FROM ext (세그먼트 로컬 병렬)
                loaded = cur.rowcount
                _emit(on_stage, "STAGE_LOAD", "end",
                      {"rows": loaded if loaded and loaded > 0 else None})
                if pre_delete_sql:
                    # overwrite_partitions 멱등: 최종 INSERT 전에 대상 파티션 선삭제.
                    _emit(on_stage, "DELETE", "start")
                    cur.execute(pre_delete_sql)
                    _emit(on_stage, "DELETE", "end",
                          {"rows": cur.rowcount if cur.rowcount and cur.rowcount > 0 else None})
                _emit(on_stage, "INSERT", "start")
                cur.execute(insert_sql)  # INSERT INTO target SELECT ... FROM staging
                affected = cur.rowcount
                _emit(on_stage, "INSERT", "end",
                      {"rows": affected if affected and affected > 0 else None})
            _emit(on_stage, "COMMIT", "start")
            gp.commit()
            _emit(on_stage, "COMMIT", "end")
        logger.debug("local_stage load 완료: file:// 외부테이블→staging→target INSERT %s행 커밋",
                     affected)
        # 정리(외부테이블 DROP 등)는 별도 트랜잭션 + best-effort. 실패해도 적재는 이미 커밋됨.
        if cleanup_sqls:
            _emit(on_stage, "CLEANUP", "start")
            try:
                with self._gp_pool.connection() as gp:
                    with gp.cursor() as cur:
                        for sql in cleanup_sqls:
                            cur.execute(sql)
                    gp.commit()
            except Exception:
                logger.warning("local_stage GP cleanup 실패 — 무시", exc_info=True)
            _emit(on_stage, "CLEANUP", "end")
        return affected if affected and affected > 0 else 0

    def segment_host_counts(self) -> dict:
        """gp_segment_configuration 에서 호스트별 primary(content>=0) 세그먼트 수 {host: S_h} 조회.

        coordinator 가 file:// "호스트당 파일 수 ≤ S_h" 규칙으로 파일을 호스트에 배분하고,
        호스트 존재 검증에도 쓴다. 조회 실패는 상위에서 배분/검증 생략으로 폴백하도록 예외를
        그대로 전파한다."""
        with self._gp_pool.connection() as gp:
            with gp.cursor() as cur:
                cur.execute(
                    "SELECT hostname, count(*) FROM gp_segment_configuration "
                    "WHERE content >= 0 GROUP BY hostname"
                )
                counts = {r[0]: int(r[1]) for r in cur.fetchall()}
                logger.debug("gp_segment_configuration 호스트별 primary 세그먼트 수: %s", counts)
                return counts

    def segment_hosts(self) -> set:
        """gp_segment_configuration 의 primary 세그먼트 호스트명 집합(호스트별 카운트 키에서 파생)."""
        return set(self.segment_host_counts())

    def move(self, sub_query, target_table, write_mode, partition_column, partition_values, on_progress=None, query_options=None, on_stage=None) -> int:
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
        rows_written = 0
        impala_conn = self._source_connect()
        try:
            cur = impala_conn.cursor()
            _emit(on_stage, "IMPALA_SUBMIT", "start")
            self._source_execute(cur, sub_query, query_options)
            columns = [d[0] for d in cur.description]
            _emit(on_stage, "IMPALA_SUBMIT", "end")

            with self._gp_pool.connection() as gp:
                with gp.cursor() as gp_cur:
                    # 사전검증(preflight): COPY 로 한 행도 흘려보내기 전에, Impala SELECT 가
                    # 내는 컬럼이 모두 대상 테이블에 존재하는지 확인한다. 불일치면 여기서
                    # 명확한 에러로 즉시 실패(런타임 COPY 오류로 대용량 읽기 후 깨지는 것 방지).
                    if self.copy_preflight:
                        _emit(on_stage, "PREFLIGHT", "start")
                        target_cols = _target_columns(gp_cur, target_table)
                        _check_copy_columns(columns, target_cols, target_table)
                        _emit(on_stage, "PREFLIGHT", "end")
                    if write_mode == "overwrite_partitions" and partition_values:
                        # 멱등성: 적재 대상 파티션을 먼저 삭제 → DELETE+COPY 가 같은 트랜잭션에
                        # 묶여 commit 되므로 재실행해도 중복 없이 해당 파티션만 새 데이터로 교체.
                        _emit(on_stage, "DELETE", "start")
                        placeholders = ", ".join(["%s"] * len(partition_values))
                        gp_cur.execute(
                            f"DELETE FROM {target_table} "
                            f"WHERE {partition_column} IN ({placeholders})",
                            partition_values,
                        )
                        _emit(on_stage, "DELETE", "end",
                              {"rows": gp_cur.rowcount if gp_cur.rowcount and gp_cur.rowcount > 0 else None})
                    copy_sql, copy_types = self._build_copy(gp_cur, target_table, columns)
                    logger.debug("copy COPY 시작(pipeline=%s, format=%s): %s",
                                 self.pipeline, self.copy_format, copy_sql)
                    _emit(on_stage, "STREAM_COPY", "start")
                    rows_written, read_wait, write_wait, finalize_wait, read_starve = \
                        self._stream_to_copy(cur, gp_cur, copy_sql, on_progress, copy_types)
                    _emit(on_stage, "STREAM_COPY", "end",
                          self._copy_stats(rows_written, read_wait, write_wait,
                                           finalize_wait, read_starve))
                    # 완료 요약(행수 + 성능 지표)을 로그로도 남긴다 — 대시보드 없이 로그만으로도
                    # 병목(read_starve=리더 대기, write_wait=GP 쓰기 대기)을 짚을 수 있게 한다.
                    logger.debug(
                        "copy 완료: %s행 (read_starve=%dms, write_wait=%dms)",
                        rows_written, int(read_starve * 1000), int(write_wait * 1000),
                    )
                _emit(on_stage, "COMMIT", "start")
                gp.commit()
                _emit(on_stage, "COMMIT", "end")
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


def _resolve_copy_types(gp_cur, table: str, columns: list[str]) -> list | None:
    """바이너리 COPY 용으로 각 SELECT 컬럼의 PG 타입명(typname)을 대상 테이블에서 해석한다.

    ``pg_attribute``/``pg_type`` 을 ``table::regclass`` 로 조회해 {컬럼명(소문자): typname}
    맵을 만들고, SELECT 컬럼 순서대로 타입 리스트를 만든다. 한 컬럼이라도 타입을 못 찾으면
    None 을 돌려 호출부가 텍스트 COPY 로 폴백하게 한다(바이너리는 타입이 완전해야 안전).

    temp staging 테이블도 같은 세션이면 search_path(pg_temp)로 ``regclass`` 가 해석된다.
    조회 자체가 실패하면(권한/구문 등) None 을 돌려 안전하게 텍스트로 되돌린다.
    """
    try:
        gp_cur.execute(
            "SELECT a.attname, t.typname FROM pg_attribute a "
            "JOIN pg_type t ON t.oid = a.atttypid "
            "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped",
            (table,),
        )
        typmap = {name.lower(): typ for name, typ in gp_cur.fetchall()}
    except Exception:
        logger.warning("바이너리 COPY 타입 조회 실패 (table=%s)", table, exc_info=True)
        return None
    types: list = []
    for col in columns:
        typ = typmap.get(col.replace('"', "").lower())
        if not typ:
            return None
        types.append(typ)
    return types


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


def build_impala_dsn(settings) -> dict:
    """settings 로부터 ``impala.dbapi.connect(**dsn)`` 에 넘길 접속 dict 를 만든다.

    impala.host 가 비어 있으면 빈 dict 를 반환한다(Impala 미사용 — statement 모드 전용).
    인증: LDAP/PLAIN 이면 ``user``/``password`` 를 채운다.
    ``build_backend`` 와 ``/datasources`` 연결 테스트 엔드포인트가 공유한다.
    """
    if not settings.impala_host:
        return {}
    dsn: dict = {
        "host": settings.impala_host,
        "port": settings.impala_port,
        "database": settings.impala_database,
        "auth_mechanism": settings.impala_auth_mechanism,
        "use_ssl": settings.impala_use_ssl,
    }
    if settings.impala_ca_cert:
        dsn["ca_cert"] = settings.impala_ca_cert
    if settings.impala_user:
        dsn["user"] = settings.impala_user
    if settings.impala_password:
        dsn["password"] = settings.impala_password
    return dsn


def build_backend(settings) -> Backend:
    """설정에 따라 실제 백엔드 또는 MockBackend를 선택한다(coordinator·executor 공용).

    greenplum.dsn 이 설정되면 실제 백엔드(statement/stage_insert 가능, copy 는 impala.host
    도 필요), 아무 것도 없으면 MockBackend(실제 I/O 없음 — 로컬 검증용).
    """
    if settings.greenplum_dsn:
        source_dsn: dict = build_impala_dsn(settings)
        logger.info(
            "ImpalaToGreenplumBackend 사용 (impala=%s, batch=%s, pipeline=%s)",
            source_dsn.get("host") or "(미설정 → statement 모드만)",
            settings.copy_batch_size,
            getattr(settings, "copy_pipeline", True),
        )
        return ImpalaToGreenplumBackend(
            impala_dsn=source_dsn,
            greenplum_dsn=settings.greenplum_dsn,
            batch_size=settings.copy_batch_size,
            query_options=getattr(settings, "impala_query_options", None),
            copy_preflight=getattr(settings, "copy_preflight", True),
            pool_max=getattr(settings, "greenplum_pool_max", 8),
            pipeline=getattr(settings, "copy_pipeline", True),
            queue_size=getattr(settings, "copy_queue_size", 8),
            copy_format=getattr(settings, "copy_format", "text"),
            stage_convert_types=getattr(settings, "stage_impala_convert_types", False),
        )
    logger.warning("greenplum.dsn 미설정 → MockBackend 사용")
    return MockBackend()
