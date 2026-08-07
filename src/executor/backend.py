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

from core.config import is_custom_source
from core.dbprobe import _is_missing
from core.sqllog import datasource_of, log_sql

# 적재 대상(그리고 local_stage/s3_stage 의 외부테이블·staging)이 도는 엔진 이름.
# 실행 SQL 로그의 datasource 표기에 쓴다 — 소스(impala/trino 등)와 구분하기 위한 상수.
GP_DATASOURCE = "greenplum"

logger = logging.getLogger(__name__)


# dotted path → 로드된 호출가능 객체 캐시. 커스텀 함수는 프로세스 수명 동안 고정이라
# 매 task 마다 importlib 를 돌 필요가 없다.
_dotted_cache: dict = {}


def load_dotted(dotted: str):
    """``module:func`` 또는 ``module.func`` dotted path 로 호출가능 객체를 import 한다."""
    fn = _dotted_cache.get(dotted)
    if fn is not None:
        return fn
    import importlib

    mod_path, sep, attr = dotted.partition(":")
    if not sep:  # ':' 가 없으면 마지막 '.' 을 함수명 경계로 본다
        mod_path, _, attr = dotted.rpartition(".")
    if not mod_path or not attr:
        raise ValueError(f"잘못된 함수 경로: {dotted!r} (module:func 형식이어야 함)")
    try:
        module = importlib.import_module(mod_path)
        fn = getattr(module, attr)
    except Exception as exc:
        raise ValueError(f"커스텀 소스 함수 로드 실패: {dotted} ({exc})") from exc
    if not callable(fn):
        raise ValueError(f"커스텀 소스 함수가 호출 가능하지 않습니다: {dotted}")
    _dotted_cache[dotted] = fn
    return fn


# ───────────────── 커서 없는 소스(커스텀 API) 어댑터 ─────────────────
# Trino 처럼 운영에서 DB-API 커서를 쓸 수 없고 "SQL 을 주면 JSON/DataFrame 을 돌려주는"
# 사내 API 만 있는 소스를 위한 계층. 읽기 루프(export_to_local_csv)가 소스에 요구하는 것은
# **컬럼명(description) · 행 배치(fetchmany) · 종료(close)** 셋뿐이므로, 커스텀 API 결과를
# 그 셋만 제공하는 얇은 객체로 감싸면 **읽기 루프를 한 줄도 고치지 않고** 태울 수 있다.


def _looks_like_columns(obj) -> bool:
    """``(columns, rows)`` 형태의 첫 원소인지(=문자열 컬럼명 목록인지) 판정한다.

    데이터 행도 튜플이라 형태가 겹칠 수 있으므로 **전부 문자열인 비어있지 않은 시퀀스**
    일 때만 컬럼 목록으로 본다.
    """
    return (
        isinstance(obj, (list, tuple))
        and len(obj) > 0
        and all(isinstance(c, str) for c in obj)
    )


def _clean_row(values) -> tuple:
    """행 하나의 결측값을 ``None`` 으로 정규화한다.

    **왜 필요한가**: DataFrame 의 ``NaN``/``NaT`` 를 CSV writer 로 그대로 넘기면 NULL 마커
    대신 문자열 ``"nan"``/``"NaT"`` 가 기록되어, 외부테이블이 그 컬럼을 NULL 로 읽지 못한다
    (Impala 커서 경로는 애초에 None 을 주므로 이 문제가 없다). 값 자체는 건드리지 않는다 —
    CSV 로 쓸 것이라 미리보기용 ``_json_safe`` 변환은 하지 않는다(불필요한 비용/표현 변경).
    """
    return tuple(None if _is_missing(v) else v for v in values)


def _one_chunk(obj):
    """단일 결과 형태면 ``(컬럼명|None, 행 목록)``, 아니면 ``None`` 을 반환한다.

    커스텀 API 가 돌려줄 수 있는 형태를 모두 받는다(문서화된 계약):

    - **pandas DataFrame**
    - **records**: ``[{"a": 1, "dt": "..."}, ...]`` — 흔한 JSON 응답 형태
    - **columns/rows dict**: ``{"columns": [...], "rows": [[...]]}`` (``data`` 키도 허용)
    - **``(columns, rows)`` 튜플**
    """
    from core.dbprobe import _is_dataframe  # 지연 임포트(모듈 로드 순환 방지)

    if _is_dataframe(obj):
        cols = [str(c) for c in obj.columns]
        return cols, [_clean_row(r) for r in obj.itertuples(index=False, name=None)]
    if isinstance(obj, dict) and "columns" in obj:
        cols = [str(c) for c in obj.get("columns") or []]
        raw = obj.get("rows")
        if raw is None:
            raw = obj.get("data") or []
        return cols, [_clean_row(r) for r in raw]
    if isinstance(obj, tuple) and len(obj) == 2 and _looks_like_columns(obj[0]):
        return [str(c) for c in obj[0]], [_clean_row(r) for r in obj[1]]
    if isinstance(obj, (list, tuple)):
        if not obj:
            return [], []
        if isinstance(obj[0], dict):  # records — 컬럼 순서는 첫 행의 키 순서
            cols = [str(k) for k in obj[0].keys()]
            return cols, [_clean_row([d.get(c) for c in cols]) for d in obj]
    return None


def _normalize_fetch_result(result) -> tuple[list, object]:
    """커스텀 fetch 함수의 반환을 ``(컬럼명 목록, 배치 이터레이터)`` 로 정규화한다.

    단일 결과(위 :func:`_one_chunk` 형태)면 배치 하나짜리로 감싸고, 그 형태가 아니면
    **청크를 yield 하는 이터러블**로 본다(각 청크가 다시 단일 결과 형태). 청크 형태를
    지원해 두면, 나중에 사내 API 에 페이징이 생겼을 때 **프레임워크 수정 없이** 전량
    메모리 적재에서 스트리밍으로 전환된다. 컬럼명은 첫 청크에서 확정한다.
    """
    single = _one_chunk(result)
    if single is not None:
        cols, rows = single
        return (cols or []), iter([rows])

    try:
        it = iter(result)
    except TypeError as exc:
        raise ValueError(
            f"커스텀 소스 함수 반환 형태를 해석할 수 없습니다: {type(result).__name__}. "
            "DataFrame / [{{컬럼: 값}}, ...] / {{'columns': [...], 'rows': [...]}} / "
            "(columns, rows) 중 하나이거나, 그 형태의 청크를 yield 해야 합니다."
        ) from exc

    first = next(it, None)
    if first is None:
        return [], iter(())
    parsed = _one_chunk(first)
    if parsed is None:
        raise ValueError(
            f"커스텀 소스 함수의 청크 형태를 해석할 수 없습니다: {type(first).__name__}."
        )
    cols, first_rows = parsed

    def _batches():
        yield first_rows
        for chunk in it:
            got = _one_chunk(chunk)
            if got is None:
                raise ValueError(
                    f"커스텀 소스 함수의 청크 형태를 해석할 수 없습니다: {type(chunk).__name__}."
                )
            yield got[1]

    return (cols or []), _batches()


class _FunctionCursor:
    """커스텀 API 결과를 **커서처럼** 보이게 감싸는 어댑터(DB-API 커서가 없는 소스용).

    읽기 루프가 쓰는 세 가지만 제공한다: ``execute`` → ``description`` → ``fetchmany``.
    실행은 커서 생성 시점이 아니라 ``execute`` 시점에 일어나 Impala 커서와 순서가 같고,
    따라서 ``IMPALA_SUBMIT``/``EXPORT_WRITE`` 단계 경계도 그대로 맞는다.
    """

    def __init__(self, fn, config: dict, batch_size: int, name: str):
        self._fn = fn
        self._config = dict(config or {})
        self._batch_size = max(1, int(batch_size or 1))
        self._name = name
        self.description = None
        self._batches = None
        self._buf: list = []

    def execute(self, sql: str, *args, **kwargs) -> None:
        """커스텀 함수를 호출해 결과를 배치 이터레이터로 준비한다.

        ``configuration=`` 같은 impyla 전용 kwarg 가 넘어와도 무시한다 — Impala 쿼리 옵션은
        커스텀 소스에 의미가 없으므로 무시가 올바른 동작이다(``_source_execute`` 무변경).
        """
        result = self._fn(sql, config=dict(self._config))
        cols, batches = _normalize_fetch_result(result)
        self._batches = batches
        self._buf = []
        # DB-API description 은 7-튜플이고 읽기 루프는 [0](컬럼명)만 쓴다.
        self.description = [(c, None, None, None, None, None, None) for c in cols]
        logger.debug("커스텀 소스 실행: datasource=%s 컬럼=%s", self._name, cols)

    def fetchmany(self, size: int | None = None) -> list:
        """요청한 행 수만큼 배치에서 꺼내 돌려준다(소진되면 빈 목록 → 루프 종료)."""
        n = max(1, int(size or self._batch_size))
        out: list = []
        while len(out) < n:
            if self._buf:
                take = n - len(out)
                out.extend(self._buf[:take])
                self._buf = self._buf[take:]
                continue
            if self._batches is None:
                break
            nxt = next(self._batches, None)
            if nxt is None:  # 이터레이터 소진
                break
            self._buf = list(nxt)
        return out

    def close(self) -> None:
        self._batches = None
        self._buf = []


class _FunctionConnection:
    """``_FunctionCursor`` 를 내주는 최소 커넥션 어댑터(실제 연결은 없다).

    읽기 경로는 ``conn.cursor(...)`` 와 ``conn.close()`` 만 쓰므로 이 둘만 제공한다.
    ``cursor`` 는 임의 kwarg(``convert_types`` 등 impyla 전용)를 받아 무시하므로
    ``_open_source_cursor`` 도 손대지 않는다.
    """

    def __init__(self, fn, config: dict, batch_size: int, name: str):
        self._fn, self._config, self._batch_size, self._name = fn, config, batch_size, name

    def cursor(self, **_kwargs) -> _FunctionCursor:
        return _FunctionCursor(self._fn, self._config, self._batch_size, self._name)

    def close(self) -> None:
        return None


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
                log_sql(GP_DATASOURCE, "DISCARD ALL", phase="SESSION_RESET")
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
        datasource=None,
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

    def export_to_s3(
        self,
        impala_select: str,
        key: str,
        job_id: str,
        task_id: str,
        csv_options=None,
        on_progress=None,
        query_options=None,
        on_stage=None,
        datasource=None,
    ) -> int:
        """[s3_stage Phase 1] Impala SELECT 결과를 로컬 CSV 로 export 후 S3(``key``)에 업로드.

        executor 가 GP 를 건드리지 않는 순수 Phase 1 이다. 로컬 임시 CSV 는 업로드 후 삭제한다.
        외부테이블 생성/INSERT(Phase 2)는 coordinator 가 배리어 후 수행한다. 반환: export 행수."""
        ...

    def load_external_s3(
        self,
        external_ddl: str,
        pre_delete_sql,
        insert_sql: str,
        cleanup_sqls=None,
        on_stage=None,
    ) -> int:
        """[s3_stage Phase 2] PXF 외부테이블 생성 → (선삭제) → target INSERT(coordinator 실행).

        S3 객체를 세그먼트가 직접 병렬 read 하므로 staging heap 없이 external→target 으로 곧장
        INSERT 한다. 커밋 후 cleanup(외부테이블 DROP)은 best-effort. INSERT 영향 행 수 반환."""
        ...

    def cleanup_s3_prefix(self, prefix: str) -> int:
        """[s3_stage Phase 3] S3 프리픽스 아래 객체를 모두 삭제한다(job 스테이징 정리). 삭제 수 반환."""
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

    def export_to_local_csv(self, sub_query, out_path, csv_options=None, on_progress=None, query_options=None, on_stage=None, datasource=None) -> int:
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

    def export_to_s3(self, impala_select, key, job_id, task_id, csv_options=None,
                     on_progress=None, query_options=None, on_stage=None, datasource=None) -> int:
        # s3_stage Phase 1: 실제 파일/업로드 없이 단계 이벤트만 방출하고 rows_per_value 반환.
        total = self.rows_per_value
        _emit(on_stage, "IMPALA_SUBMIT", "start")
        _emit(on_stage, "IMPALA_SUBMIT", "end")
        _emit(on_stage, "EXPORT_WRITE", "start")
        if on_progress:
            on_progress(total)
        _emit(on_stage, "EXPORT_WRITE", "end", {"rows": total})
        _emit(on_stage, "S3_UPLOAD", "start")
        _emit(on_stage, "S3_UPLOAD", "end", {"rows": total})
        return total

    def load_external_s3(self, external_ddl, pre_delete_sql, insert_sql, cleanup_sqls=None,
                         on_stage=None) -> int:
        # s3_stage Phase 2: 실제 GP 호출 없이 단계 이벤트만 방출하고 rows_per_value 반환.
        _emit(on_stage, "S3_EXTERNAL_DDL", "start")
        _emit(on_stage, "S3_EXTERNAL_DDL", "end")
        if pre_delete_sql:
            _emit(on_stage, "DELETE", "start")
            _emit(on_stage, "DELETE", "end")
        _emit(on_stage, "INSERT", "start")
        _emit(on_stage, "INSERT", "end", {"rows": self.rows_per_value})
        _emit(on_stage, "COMMIT", "start")
        _emit(on_stage, "COMMIT", "end")
        return self.rows_per_value

    def cleanup_s3_prefix(self, prefix: str) -> int:
        # 목: 삭제할 객체 없음(0).
        return 0

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
                 copy_format: str = "text", stage_convert_types: bool = False,
                 s3_config: dict | None = None, s3_client=None,
                 source_fetch_module: str = "", source_func_config: dict | None = None):
        # Impala 소스 접속 dict(impyla connect 에 그대로 전달).
        self.impala_dsn = impala_dsn
        # 커서 없는 커스텀 소스(사내 API) 실행 함수와 그 설정. 비어 있으면(기본) 모든 읽기가
        # 예전처럼 Impala 커서로 간다 — job 의 datasource 가 impala 가 아닐 때만 쓰인다.
        self.source_fetch_module: str = str(source_fetch_module or "").strip()
        self.source_func_config: dict = dict(source_func_config or {})
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
        # s3_stage 설정(버킷/프리픽스/PXF 서버·프로파일 등)과 업로더. s3_client 를 주입하면
        # 그걸 쓰고(테스트용 가짜), 없으면 첫 사용 때 설정으로 boto3 클라이언트를 지연 생성한다.
        self.s3_config: dict = dict(s3_config or {})
        self._s3_client = s3_client

    def _get_s3_client(self):
        """s3_stage 용 S3 클라이언트를 지연 생성해 반환한다(미구성이면 명확히 실패)."""
        if self._s3_client is None:
            from executor.s3_client import build_s3_client  # 지연 임포트(옵션 의존성)

            self._s3_client = build_s3_client(self.s3_config)
        if self._s3_client is None:
            raise ValueError(
                "s3_stage 모드는 s3.bucket 설정이 필요합니다(현재 미설정). "
                "config 의 s3.* 를 채우세요."
            )
        return self._s3_client

    def _source_connect(self, datasource: str | None = None):
        """소스 연결을 연다. 기본은 Impala(impyla 커서)이고, datasource 가 지정되면 커스텀 API.

        - ``impala``/``source``/미지정 → impyla 연결(기존 경로 그대로).
        - 그 외 이름 → ``query.func.fetch_module`` 커스텀 함수를 커서처럼 감싼
          :class:`_FunctionConnection`. **DB-API 커서가 없는 소스**(사내 API)를 위한 통로다.

        설정이 없으면 조용히 Impala 로 폴백하지 않고 명확히 실패한다 — Trino 로 읽는 줄 알고
        Impala 를 읽어 엉뚱한 데이터를 적재하는 사고를 막기 위해서다.
        """
        if is_custom_source(datasource):
            name = str(datasource).strip().lower()
            if not self.source_fetch_module:
                raise ValueError(
                    f"datasource={name} 의 소스 실행 함수가 설정되지 않았습니다. "
                    "executor 설정에 query.func.fetch_module=<module:func> 를 지정하세요"
                    "(계약: fetch(sql, *, config) -> DataFrame | records | "
                    "{'columns':[...], 'rows':[...]} | (columns, rows), limit 없이 전량)."
                )
            logger.debug("커스텀 소스 사용: datasource=%s func=%s",
                         name, self.source_fetch_module)
            return _FunctionConnection(
                load_dotted(self.source_fetch_module),
                self.source_func_config, self.batch_size, name,
            )
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

        모든 소스 읽기(copy·stage_insert·local_stage·s3_stage)가 이 한 곳을 지나므로
        실행 SQL 로깅도 여기서 한다. datasource 는 커서에서 추론한다 — 커스텀 소스
        어댑터면 그 이름(trino 등), impyla 커서면 impala. 덕분에 시그니처를 바꾸지 않아
        기존 호출부·테스트 더블이 그대로 동작한다.
        """
        opts = {**self.query_options, **(query_options or {})}
        log_sql(datasource_of(cur), sql, phase="SOURCE_SELECT")
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
                log_sql(GP_DATASOURCE, sql, phase="STATEMENT")
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
        한 Greenplum 세션(연결) 안에서 (DROP →) CREATE TEMP TABLE → COPY → INSERT 를
        수행하므로 TEMP 테이블이 INSERT 시점까지 보이며, 세션 종료 시 자동 정리된다. CREATE
        직전 ``DROP TABLE IF EXISTS`` 로 선삭제하는데, 풀에서 재사용한 연결에 이전 task 의
        TEMP staging 이 남아 있으면(일부 GP 는 DISCARD ALL 이 TEMP 를 안 떨군다) CREATE 가
        "already exists" 로 실패하기 때문이다.
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
                        # 생성 전 항상 선삭제한다. 풀에서 재사용한 GP 연결에 이전 task 의 TEMP
                        # staging 이 남아 있으면 CREATE TEMP TABLE 이 "already exists" 로 실패하기
                        # 때문이다. 반납 시 DISCARD ALL 로 세션을 비우지만, 일부 Greenplum/
                        # WarehousePG 는 DISCARD 로 TEMP 를 실제로 떨구지 않아(예외 없이 no-op)
                        # TEMP 가 살아남는다 — 이때 stage_insert/날짜 fan-out 처럼 같은 staging_table
                        # 이름을 여러 task 가 재사용하면 두 번째 task 부터 충돌한다. 선삭제로 세션
                        # 상태와 무관하게 멱등적으로 재생성한다. search_path 상 pg_temp 가 우선이라
                        # 동명의 영구 테이블은 건드리지 않고 이 세션의 TEMP 만 떨군다.
                        drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
                        log_sql(GP_DATASOURCE, drop_sql, phase="STAGING_DDL", target=staging_table)
                        gp_cur.execute(drop_sql)
                        log_sql(GP_DATASOURCE, staging_ddl, phase="STAGING_DDL", target=staging_table)
                        gp_cur.execute(staging_ddl)  # CREATE TEMP TABLE <staging_table> (...)
                        _emit(on_stage, "STAGING_DDL", "end")
                    # staging_ddl 이 없으면 생성을 건너뛰고 기존 staging_table 에 그대로 COPY.
                    copy_sql, copy_types = self._build_copy(gp_cur, staging_table, columns)
                    log_sql(GP_DATASOURCE, copy_sql, phase="STREAM_COPY", target=staging_table)
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
                    log_sql(GP_DATASOURCE, insert_sql, phase="INSERT")
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
                        drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
                        log_sql(GP_DATASOURCE, drop_sql, phase="CLEANUP", target=staging_table)
                        gp_cur.execute(drop_sql)
                        logger.debug("stage_insert staging 정리: DROP %s", staging_table)
                _emit(on_stage, "COMMIT", "start")
                gp.commit()
                _emit(on_stage, "COMMIT", "end")
            return affected if affected and affected > 0 else loaded
        finally:
            impala_conn.close()

    def export_to_local_csv(self, sub_query, out_path, csv_options=None, on_progress=None, query_options=None, on_stage=None, datasource=None) -> int:
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
        impala_conn = self._source_connect(datasource)
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
                    log_sql(GP_DATASOURCE, staging_ddl, phase="STAGING_DDL")
                    cur.execute(staging_ddl)  # CREATE TABLE staging (...) DISTRIBUTED BY ...
                    _emit(on_stage, "STAGING_DDL", "end")
                _emit(on_stage, "PXF_EXTERNAL_DDL", "start")
                log_sql(GP_DATASOURCE, external_ddl, phase="PXF_EXTERNAL_DDL")
                cur.execute(external_ddl)  # CREATE EXTERNAL TABLE ext (...) LOCATION('file://...')
                _emit(on_stage, "PXF_EXTERNAL_DDL", "end")
                _emit(on_stage, "STAGE_LOAD", "start")
                log_sql(GP_DATASOURCE, staging_load_sql, phase="STAGE_LOAD")
                cur.execute(staging_load_sql)  # INSERT INTO staging SELECT * FROM ext (세그먼트 로컬 병렬)
                loaded = cur.rowcount
                _emit(on_stage, "STAGE_LOAD", "end",
                      {"rows": loaded if loaded and loaded > 0 else None})
                if pre_delete_sql:
                    # overwrite_partitions 멱등: 최종 INSERT 전에 대상 파티션 선삭제.
                    _emit(on_stage, "DELETE", "start")
                    log_sql(GP_DATASOURCE, pre_delete_sql, phase="DELETE")
                    cur.execute(pre_delete_sql)
                    _emit(on_stage, "DELETE", "end",
                          {"rows": cur.rowcount if cur.rowcount and cur.rowcount > 0 else None})
                _emit(on_stage, "INSERT", "start")
                log_sql(GP_DATASOURCE, insert_sql, phase="INSERT")
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
                            log_sql(GP_DATASOURCE, sql, phase="CLEANUP")
                            cur.execute(sql)
                    gp.commit()
            except Exception:
                logger.warning("local_stage GP cleanup 실패 — 무시", exc_info=True)
            _emit(on_stage, "CLEANUP", "end")
        return affected if affected and affected > 0 else 0

    def export_to_s3(self, impala_select, key, job_id, task_id, csv_options=None,
                     on_progress=None, query_options=None, on_stage=None, datasource=None) -> int:
        """s3_stage Phase 1: Impala SELECT 결과를 로컬 CSV 로 export → S3 업로드 → 로컬 삭제.

        executor 가 GP 를 건드리지 않는 순수 Phase 1 이다(외부테이블 생성·INSERT 는
        coordinator 가 배리어 후 Phase 2 에서 수행한다 — local_stage 와 같은 구조). ``key`` 는
        coordinator 가 확정한 S3 객체 키(``<prefix>/<job_id>/<task_id>.csv``)이고, 로컬 임시
        CSV 는 ``{local_tmp_dir}/{job_id}/{task_id}.csv`` 에 잠깐 썼다가 업로드 후 지운다.

        단계: IMPALA_SUBMIT→EXPORT_WRITE(export 가 방출) → S3_UPLOAD(업로드+로컬 삭제).
        반환: export 한 행 수. 로컬 임시 파일은 finally 에서 항상 정리한다.
        """
        import os

        local_root = self.s3_config.get("local_tmp_dir") or "/tmp"
        out_path = os.path.join(local_root, job_id, f"{task_id}.csv")
        try:
            # 1) Impala SELECT → 로컬 임시 CSV(IMPALA_SUBMIT/EXPORT_WRITE 이벤트는 export 가 방출).
            rows = self.export_to_local_csv(
                impala_select, out_path, csv_options, on_progress,
                query_options=query_options, on_stage=on_stage,
                # 커스텀 소스일 때만 인자를 붙인다 — impala 면 호출이 예전과 완전히 동일.
                **({"datasource": datasource} if is_custom_source(datasource) else {}),
            )
            # 2) 로컬 CSV → S3 업로드 후 로컬 파일 즉시 삭제(로컬 디스크를 비운다).
            _emit(on_stage, "S3_UPLOAD", "start")
            self._get_s3_client().upload(out_path, key)
            _emit(on_stage, "S3_UPLOAD", "end", {"rows": rows})
            logger.debug("s3_stage export 완료: %s행 → s3(key=%s)", rows, key)
            return rows
        finally:
            try:
                if os.path.exists(out_path):
                    os.remove(out_path)
            except OSError:
                logger.warning("s3_stage 로컬 임시 CSV 삭제 실패: %s", out_path, exc_info=True)

    def load_external_s3(self, external_ddl, pre_delete_sql, insert_sql, cleanup_sqls=None,
                         on_stage=None) -> int:
        """s3_stage Phase 2: PXF 외부테이블 생성 → (선삭제) → target INSERT(coordinator 실행).

        coordinator 가 조립한 SQL 을 한 GP 트랜잭션으로 실행한다:
          external_ddl → (pre_delete_sql?) → insert_sql
        외부테이블이 staging 을 겸하므로(S3 객체를 세그먼트가 직접 병렬 read) staging heap 없이
        external→target 으로 곧장 INSERT 한다(local_stage 의 external→staging→target 2단계와
        다름). 커밋 뒤 cleanup_sqls(외부테이블 DROP)를 별도 트랜잭션에서 best-effort 로 수행한다.
        반환: INSERT 영향 행 수. Impala 는 관여하지 않으므로 impala_dsn 이 없어도 동작한다.
        """
        affected = 0
        with self._gp_pool.connection() as gp:
            with gp.cursor() as cur:
                _emit(on_stage, "S3_EXTERNAL_DDL", "start")
                log_sql(GP_DATASOURCE, external_ddl, phase="S3_EXTERNAL_DDL")
                cur.execute(external_ddl)  # CREATE EXTERNAL TABLE ext (...) LOCATION('pxf://...')
                _emit(on_stage, "S3_EXTERNAL_DDL", "end")
                if pre_delete_sql:
                    # overwrite_partitions 멱등: 최종 INSERT 전에 대상 파티션 선삭제.
                    _emit(on_stage, "DELETE", "start")
                    log_sql(GP_DATASOURCE, pre_delete_sql, phase="DELETE")
                    cur.execute(pre_delete_sql)
                    _emit(on_stage, "DELETE", "end",
                          {"rows": cur.rowcount if cur.rowcount and cur.rowcount > 0 else None})
                _emit(on_stage, "INSERT", "start")
                log_sql(GP_DATASOURCE, insert_sql, phase="INSERT")
                cur.execute(insert_sql)  # INSERT INTO target SELECT ... FROM ext (세그먼트 병렬 read)
                affected = cur.rowcount
                _emit(on_stage, "INSERT", "end",
                      {"rows": affected if affected and affected > 0 else None})
            _emit(on_stage, "COMMIT", "start")
            gp.commit()
            _emit(on_stage, "COMMIT", "end")
        logger.debug("s3_stage Phase 2 완료: pxf 외부테이블→target INSERT %s행 커밋", affected)
        # 정리(외부테이블 DROP)는 별도 트랜잭션 + best-effort. 실패해도 적재는 이미 커밋됨.
        if cleanup_sqls:
            _emit(on_stage, "CLEANUP", "start")
            try:
                with self._gp_pool.connection() as gp:
                    with gp.cursor() as cur:
                        for sql in cleanup_sqls:
                            log_sql(GP_DATASOURCE, sql, phase="CLEANUP")
                            cur.execute(sql)
                    gp.commit()
            except Exception:
                logger.warning("s3_stage GP cleanup 실패 — 무시", exc_info=True)
            _emit(on_stage, "CLEANUP", "end")
        return affected if affected and affected > 0 else 0

    def cleanup_s3_prefix(self, prefix: str) -> int:
        """s3_stage Phase 3: S3 프리픽스(``<prefix>/<job_id>/``) 아래 객체를 모두 삭제한다.

        Phase 2 적재가 끝난 뒤 job 의 스테이징 객체를 정리한다. best-effort 이며(실패해도
        적재는 이미 커밋됨) 삭제한 객체 수를 반환한다. S3 미구성이면 0."""
        return self._get_s3_client().delete_prefix(prefix)

    def segment_host_counts(self) -> dict:
        """gp_segment_configuration 에서 호스트별 primary(content>=0) 세그먼트 수 {host: S_h} 조회.

        coordinator 가 file:// "호스트당 파일 수 ≤ S_h" 규칙으로 파일을 호스트에 배분하고,
        호스트 존재 검증에도 쓴다. 조회 실패는 상위에서 배분/검증 생략으로 폴백하도록 예외를
        그대로 전파한다."""
        with self._gp_pool.connection() as gp:
            with gp.cursor() as cur:
                seg_sql = (
                    "SELECT hostname, count(*) FROM gp_segment_configuration "
                    "WHERE content >= 0 GROUP BY hostname"
                )
                log_sql(GP_DATASOURCE, seg_sql, phase="CATALOG")
                cur.execute(seg_sql)
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
                        delete_sql = (
                            f"DELETE FROM {target_table} "
                            f"WHERE {partition_column} IN ({placeholders})"
                        )
                        log_sql(GP_DATASOURCE, delete_sql, phase="DELETE",
                                target=target_table, params=partition_values)
                        gp_cur.execute(delete_sql, partition_values)
                        _emit(on_stage, "DELETE", "end",
                              {"rows": gp_cur.rowcount if gp_cur.rowcount and gp_cur.rowcount > 0 else None})
                    copy_sql, copy_types = self._build_copy(gp_cur, target_table, columns)
                    log_sql(GP_DATASOURCE, copy_sql, phase="STREAM_COPY", target=target_table)
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
        sql = ("SELECT column_name FROM information_schema.columns "
               "WHERE table_schema=%s AND table_name=%s")
        params = (schema, table)
    else:
        sql = "SELECT column_name FROM information_schema.columns WHERE table_name=%s"
        params = (table,)
    log_sql(GP_DATASOURCE, sql, phase="PREFLIGHT", target=target_table, params=params)
    gp_cur.execute(sql, params)
    return [r[0] for r in gp_cur.fetchall()]


def _resolve_copy_types(gp_cur, table: str, columns: list[str]) -> list | None:
    """바이너리 COPY 용으로 각 SELECT 컬럼의 PG 타입명(typname)을 대상 테이블에서 해석한다.

    ``pg_attribute``/``pg_type`` 을 ``table::regclass`` 로 조회해 {컬럼명(소문자): typname}
    맵을 만들고, SELECT 컬럼 순서대로 타입 리스트를 만든다. 한 컬럼이라도 타입을 못 찾으면
    None 을 돌려 호출부가 텍스트 COPY 로 폴백하게 한다(바이너리는 타입이 완전해야 안전).

    temp staging 테이블도 같은 세션이면 search_path(pg_temp)로 ``regclass`` 가 해석된다.
    조회 자체가 실패하면(권한/구문 등) None 을 돌려 안전하게 텍스트로 되돌린다.
    """
    type_sql = (
        "SELECT a.attname, t.typname FROM pg_attribute a "
        "JOIN pg_type t ON t.oid = a.atttypid "
        "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped"
    )
    try:
        log_sql(GP_DATASOURCE, type_sql, phase="CATALOG", target=table, params=(table,))
        gp_cur.execute(type_sql, (table,))
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


def build_s3_config(settings) -> dict:
    """settings 로부터 s3_stage 용 설정 dict 를 만든다(업로드 + PXF 외부테이블 조립).

    ``bucket`` 이 비어 있으면 s3_stage 를 쓰지 않는 배포이므로 클라이언트는 지연 생성 시
    None 이 되고(사용 시 명확한 오류), 다른 exec_mode 에는 영향이 없다. 로컬 임시 CSV 는
    local_stage 와 같은 루트(``stage.local_dir``)를 재사용한다.
    """
    return {
        "bucket": getattr(settings, "s3_bucket", ""),
        "prefix": getattr(settings, "s3_prefix", "dqe-stage"),
        "endpoint_url": getattr(settings, "s3_endpoint_url", ""),
        "region": getattr(settings, "s3_region", ""),
        "access_key": getattr(settings, "s3_access_key", ""),
        "secret_key": getattr(settings, "s3_secret_key", ""),
        "use_ssl": getattr(settings, "s3_use_ssl", True),
        "pxf_server": getattr(settings, "s3_pxf_server", ""),
        "pxf_profile": getattr(settings, "s3_pxf_profile", "s3:csv"),
        "gp_location_template": getattr(settings, "s3_gp_location_template", ""),
        "delete_on_cleanup": getattr(settings, "s3_delete_on_cleanup", True),
        "local_tmp_dir": getattr(settings, "stage_local_dir", "/tmp"),
    }


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
            s3_config=build_s3_config(settings),
            # 커서 없는 커스텀 소스(job.datasource 가 impala 가 아닐 때) 실행 함수·설정.
            source_fetch_module=getattr(settings, "query_func_fetch_module", ""),
            source_func_config=getattr(settings, "query_func_config", None),
        )
    logger.warning("greenplum.dsn 미설정 → MockBackend 사용")
    return MockBackend()
