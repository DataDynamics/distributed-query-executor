"""Impala 쿼리 옵션(전역 + 요청별) 병합 및 configuration 전달 테스트.

핵심: 전역 impala.query_options 위에 요청별 옵션을 병합하고, 둘 다 비어 있으면
impyla 에 configuration 인자를 아예 넘기지 않고 그대로 실행한다.
"""

from __future__ import annotations

from core.config import _kv_dict
from executor.backend import ImpalaToGreenplumBackend

_UNSET = object()


class _FakeCursor:
    """execute 호출 시 configuration 이 넘어왔는지(또는 미전달인지) 기록하는 가짜 커서."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, configuration=_UNSET):
        self.calls.append({"sql": sql, "configuration": configuration})

    @property
    def last(self):
        return self.calls[-1]


def _backend(global_opts):
    return ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn="x", query_options=global_opts)


# ── _kv_dict 파서 ──────────────────────────────────────────────────────────

def test_kv_dict_parses_string():
    assert _kv_dict("MEM_LIMIT=2g,REQUEST_POOL=etl") == {"MEM_LIMIT": "2g", "REQUEST_POOL": "etl"}


def test_kv_dict_empty_and_dict_passthrough():
    assert _kv_dict("") == {}
    assert _kv_dict(None) == {}
    assert _kv_dict({"A": 1}) == {"A": "1"}  # 값은 문자열로 정규화


def test_kv_dict_value_with_equals():
    # 값에 '=' 가 들어가도 첫 '=' 기준으로만 분리한다.
    assert _kv_dict("OPT=a=b") == {"OPT": "a=b"}


# ── 병합 + configuration 전달 ───────────────────────────────────────────────

def test_no_options_omits_configuration():
    cur = _FakeCursor()
    _backend({})._source_execute(cur, "SELECT 1", None)
    assert cur.last["configuration"] is _UNSET  # configuration 인자 미전달


def test_global_only_applied():
    cur = _FakeCursor()
    _backend({"MEM_LIMIT": "2g"})._source_execute(cur, "SELECT 1", None)
    assert cur.last["configuration"] == {"MEM_LIMIT": "2g"}


def test_request_only_applied():
    cur = _FakeCursor()
    _backend({})._source_execute(cur, "SELECT 1", {"REQUEST_POOL": "etl"})
    assert cur.last["configuration"] == {"REQUEST_POOL": "etl"}


def test_request_merges_over_global():
    cur = _FakeCursor()
    _backend({"MEM_LIMIT": "2g", "REQUEST_POOL": "default"})._source_execute(
        cur, "SELECT 1", {"REQUEST_POOL": "etl"}
    )
    # 전역 MEM_LIMIT 유지 + 요청별 REQUEST_POOL 이 전역값을 덮어씀
    assert cur.last["configuration"] == {"MEM_LIMIT": "2g", "REQUEST_POOL": "etl"}
