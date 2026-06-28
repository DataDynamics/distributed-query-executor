"""copy 모드 COPY 사전검증(preflight) 단위 테스트.

실 DB 없이 순수 함수(_check_copy_columns / _split_schema_table)만 검증한다.
"""

from __future__ import annotations

import pytest

from executor.backend import _check_copy_columns, _split_schema_table


def test_split_schema_table():
    assert _split_schema_table("public.sales") == ("public", "sales")
    assert _split_schema_table("sales") == ("", "sales")
    assert _split_schema_table('"sch"."tbl"') == ("sch", "tbl")


def test_preflight_ok_when_all_columns_present():
    # 모든 SELECT 컬럼이 대상에 존재 → 통과(예외 없음)
    _check_copy_columns(["id", "dt"], ["id", "dt", "extra"], "public.t")


def test_preflight_is_case_insensitive():
    _check_copy_columns(["ID", "DT"], ["id", "dt"], "public.t")


def test_preflight_raises_on_missing_column():
    with pytest.raises(ValueError) as ei:
        _check_copy_columns(["id", "missing_col"], ["id", "dt"], "public.t")
    msg = str(ei.value)
    assert "missing_col" in msg
    assert "public.t" in msg


def test_preflight_skips_when_target_unknown():
    # 대상 컬럼 조회가 비면(테이블 미존재/권한 없음 등) 오탐 방지 위해 검사를 건너뛴다.
    _check_copy_columns(["a", "b"], [], "public.t")
