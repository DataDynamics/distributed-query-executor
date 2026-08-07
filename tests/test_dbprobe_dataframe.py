"""``core.dbprobe._shape`` 의 pandas DataFrame 지원 + 값 정형(JSON 안전) 검증.

커스텀 실행 함수가 커서 대신 **DataFrame** 을 돌려받는 사내 API/게이트웨이를 쓰는 경우를
위해 ``_shape`` 가 DataFrame 을 그대로 받는다. 함께 ``_json_safe`` 가 numpy 스칼라와
결측(NaN/NaT/pd.NA)을 정규화한다 — 이게 없으면 DataFrame 을 받아도 출력이 깨진다.

pandas 는 이 프로젝트의 의존성이 아니므로(쓰는 배포에만 설치) 판정은 덕타이핑이다.
그래서 이 파일은 **pandas 없이도** 도는 가짜 DataFrame 으로 계약을 고정하고, pandas 가
설치된 환경에서만 실제 DataFrame 으로 한 번 더 검증한다(numpy 스칼라·NaN/NaT 포함).
"""

from __future__ import annotations

import json
import time

import pytest

from core.dbprobe import _is_dataframe, _json_safe, _shape


class _FakeDF:
    """DataFrame 의 최소 인터페이스(columns / iloc[:n] / itertuples)만 흉내낸 더블."""

    def __init__(self, columns, rows):
        self.columns = list(columns)
        self._rows = [tuple(r) for r in rows]

    class _ILoc:
        def __init__(self, df):
            self._df = df

        def __getitem__(self, sl):
            # 하위 클래스(계측 더블)를 보존해야 슬라이스 뒤에도 계측이 살아 있다.
            return type(self._df)(self._df.columns, self._df._rows[sl])

    @property
    def iloc(self):
        return _FakeDF._ILoc(self)

    def itertuples(self, index=False, name=None):
        return iter(self._rows)


class _FakeNumpyScalar:
    """numpy 스칼라 덕타이핑(dtype + tolist)만 흉내낸 더블 — np.int64 등에 대응."""

    def __init__(self, value):
        self._value = value
        self.dtype = "int64"

    def tolist(self):
        return self._value


# ───────────────────────── _is_dataframe(덕타이핑) ─────────────────────────


def test_is_dataframe_detects_duck_typed_frame():
    assert _is_dataframe(_FakeDF(["a"], [[1]])) is True


@pytest.mark.parametrize("obj", [None, [], [(1, 2)], (1, 2), {"a": 1}, "abc", 3])
def test_is_dataframe_rejects_plain_sequences(obj):
    """일반 시퀀스/스칼라를 DataFrame 으로 오탐하면 기존 커서 경로가 깨진다."""
    assert _is_dataframe(obj) is False


# ───────────────────────── _shape(DataFrame 입력) ─────────────────────────


def test_shape_accepts_dataframe():
    df = _FakeDF(["user_id", "dt"], [[1, "2026-01-01"], [2, "2026-01-02"]])
    res = _shape(None, df, 100, time.perf_counter())
    assert res.columns == ["user_id", "dt"]
    assert res.rows == [[1, "2026-01-01"], [2, "2026-01-02"]]
    assert res.row_count == 2 and res.truncated is False


def test_shape_dataframe_truncates_at_limit():
    df = _FakeDF(["a"], [[i] for i in range(10)])
    res = _shape(None, df, 3, time.perf_counter())
    assert res.rows == [[0], [1], [2]]
    assert res.row_count == 3 and res.truncated is True


def test_shape_dataframe_exactly_limit_is_not_truncated():
    df = _FakeDF(["a"], [[i] for i in range(3)])
    res = _shape(None, df, 3, time.perf_counter())
    assert res.row_count == 3 and res.truncated is False


def test_shape_dataframe_only_materializes_limit_plus_one():
    """큰 DataFrame 이 와도 limit+1 행만 잘라 변환한다(전량 변환하면 미리보기가 죽는다)."""
    seen = {}

    class _Counting(_FakeDF):
        def itertuples(self, index=False, name=None):
            seen["rows"] = len(self._rows)
            return super().itertuples(index, name)

    _shape(None, _Counting(["a"], [[i] for i in range(100_000)]), 5, time.perf_counter())
    assert seen["rows"] == 6  # limit(5) + 1


def test_shape_dataframe_empty_keeps_columns():
    res = _shape(None, _FakeDF(["a", "b"], []), 100, time.perf_counter())
    assert res.columns == ["a", "b"] and res.rows == [] and res.row_count == 0


def test_shape_dataframe_normalizes_non_string_column_names():
    res = _shape(None, _FakeDF([0, 1], [[1, 2]]), 100, time.perf_counter())
    assert res.columns == ["0", "1"]


def test_shape_explicit_columns_override_dataframe():
    """컬럼명을 갈아끼우고 싶으면 columns 를 명시한다(명시값 우선)."""
    df = _FakeDF(["a", "b"], [[1, 2]])
    res = _shape(["x", "y"], df, 100, time.perf_counter())
    assert res.columns == ["x", "y"] and res.rows == [[1, 2]]


def test_shape_cursor_path_unchanged():
    """기존 커서 경로(컬럼 목록 + 튜플 목록)는 그대로 동작한다."""
    res = _shape(["a", "b"], [(1, "x"), (2, "y"), (3, "z")], 2, time.perf_counter())
    assert res.columns == ["a", "b"]
    assert res.rows == [[1, "x"], [2, "y"]]
    assert res.truncated is True


# ───────────────────────── _json_safe(pandas/numpy 값) ─────────────────────────


def test_json_safe_numpy_scalar_stays_a_number():
    """np.int64 는 파이썬 int 를 상속하지 않는다 — tolist() 로 낮추지 않으면 문자열이 된다."""
    assert _json_safe(_FakeNumpyScalar(42)) == 42
    assert _json_safe(_FakeNumpyScalar(42)) != "42"


def test_json_safe_missing_values_become_null():
    """NaN/NaT/NA 는 JSON 표현이 없으므로 null 로 떨군다."""
    assert _json_safe(float("nan")) is None
    assert _json_safe(None) is None

    # 판정은 타입 "이름" 으로 하므로(import 없이) 더블도 이름을 정확히 맞춘다.
    class NaTType:   # pandas.NaT
        pass

    class NAType:    # pandas.NA
        pass

    assert _json_safe(NaTType()) is None
    assert _json_safe(NAType()) is None


def test_json_safe_infinities_become_null():
    """inf/-inf 도 표준 JSON 이 아니다(json.dumps 는 Infinity 리터럴을 낸다)."""
    assert _json_safe(float("inf")) is None
    assert _json_safe(float("-inf")) is None
    assert _json_safe(1.5) == 1.5


def test_json_safe_keeps_existing_conversions():
    """기존 변환 규칙(bytes/list/dict/기타→str)은 그대로 유지된다."""
    from decimal import Decimal

    assert _json_safe(b"\x01\x02") == "\\x0102"
    assert _json_safe([1, None, "x"]) == [1, None, "x"]
    assert _json_safe({"k": 1}) == {"k": 1}
    assert _json_safe(Decimal("1.50")) == "1.50"
    assert _json_safe(True) is True and _json_safe(7) == 7


def test_shape_output_is_strict_json_serializable():
    """정형 결과는 표준 JSON(allow_nan=False)으로 직렬화돼야 한다 — 클라이언트 호환."""
    df = _FakeDF(["a", "b"], [[float("nan"), _FakeNumpyScalar(7)],
                              [float("inf"), 1.5]])
    res = _shape(None, df, 10, time.perf_counter())
    assert res.rows == [[None, 7], [None, 1.5]]
    json.dumps(res.to_dict(), allow_nan=False)  # 예외가 나면 실패


# ───────── trino_runner: 커스텀 API(DataFrame 반환) 분기 ─────────


def test_trino_runner_uses_dataframe_api_when_configured(monkeypatch):
    """dataframe_module 이 설정되면 trino 드라이버 대신 커스텀 API 로 간다.

    trino 패키지가 없어도 이 경로는 동작해야 한다(드라이버를 아예 타지 않으므로).
    """
    from customs.query_funcs import trino_runner

    seen = {}

    def fake_api(sql, *, config, limit):
        seen.update(sql=sql, config=config, limit=limit)
        return _FakeDF(["a", "b"], [[1, float("nan")], [2, 2.5]])

    monkeypatch.setattr(trino_runner, "_load_dotted", lambda dotted: fake_api)
    res = trino_runner.run(
        "SELECT a, b FROM t",
        config={"dataframe_module": "mycorp.api:query", "host": "h"},
        limit=10,
    )
    assert res.columns == ["a", "b"]
    assert res.rows == [[1, None], [2, 2.5]]     # NaN → null
    assert res.row_count == 2 and res.truncated is False
    # 커스텀 API 는 sql·config·limit 을 그대로 받는다(limit 서버측 푸시다운 가능).
    assert seen["sql"] == "SELECT a, b FROM t" and seen["limit"] == 10
    assert seen["config"]["host"] == "h"


def test_trino_runner_dataframe_api_applies_limit(monkeypatch):
    """커스텀 API 가 limit 을 무시하고 더 돌려줘도 _shape 가 상한을 강제한다."""
    from customs.query_funcs import trino_runner

    monkeypatch.setattr(
        trino_runner, "_load_dotted",
        lambda dotted: (lambda sql, *, config, limit: _FakeDF(["a"], [[i] for i in range(50)])),
    )
    res = trino_runner.run("SELECT 1", config={"dataframe_module": "m:f"}, limit=3)
    assert res.row_count == 3 and res.truncated is True


def test_trino_runner_without_dataframe_module_uses_driver(monkeypatch):
    """dataframe_module 이 없으면 기존 trino.dbapi 경로 그대로(하위 호환)."""
    from customs.query_funcs import trino_runner

    called = []
    monkeypatch.setattr(trino_runner, "_load_dotted",
                        lambda dotted: called.append(dotted))
    # trino 패키지가 없으므로 드라이버 경로로 갔다면 ImportError 가 난다 = 분기 증거.
    with pytest.raises(ImportError):
        trino_runner.run("SELECT 1", config={"host": "h"}, limit=10)
    assert called == []


def test_trino_runner_bad_dotted_path():
    from customs.query_funcs import trino_runner

    with pytest.raises(ValueError):
        trino_runner._load_dotted("nomodulesep")


# ───────────────────────── 실제 pandas(설치된 환경에서만) ─────────────────────────
# pandas 는 선택 의존성이라(쓰는 배포에만 설치) 모듈 단위 importorskip 을 쓰면 위의
# 덕타이핑 테스트까지 통째로 건너뛴다. 아래 블록만 조건부로 건너뛴다.

try:  # pragma: no cover - 설치 여부에 따라 갈린다
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

requires_pandas = pytest.mark.skipif(pd is None, reason="pandas 미설치(선택 의존성)")


@requires_pandas
def test_real_dataframe_end_to_end():
    df = pd.DataFrame({
        "user_id": [1, 2, 3],                 # int64 → 파이썬 int
        "amount": [1.5, float("nan"), 3.0],   # NaN → null
        "dt": pd.to_datetime(["2026-01-01", "2026-01-02", None]),  # NaT → null
        "flag": [True, False, True],          # bool_ → 파이썬 bool
    })
    res = _shape(None, df, 2, time.perf_counter())
    assert res.columns == ["user_id", "amount", "dt", "flag"]
    assert res.row_count == 2 and res.truncated is True
    assert res.rows[0][0] == 1 and isinstance(res.rows[0][0], int)
    assert res.rows[1][1] is None                      # NaN
    assert res.rows[0][3] is True and isinstance(res.rows[0][3], bool)
    json.dumps(res.to_dict(), allow_nan=False)


@requires_pandas
def test_real_dataframe_nat_and_na_become_null():
    df = pd.DataFrame({"dt": pd.to_datetime([None]), "s": pd.array([None], dtype="string")})
    res = _shape(None, df, 10, time.perf_counter())
    assert res.rows == [[None, None]]


@requires_pandas
def test_real_dataframe_empty_keeps_columns():
    res = _shape(None, pd.DataFrame(columns=["a", "b"]), 10, time.perf_counter())
    assert res.columns == ["a", "b"] and res.rows == []


@requires_pandas
def test_real_dataframe_preserves_int_dtype_in_mixed_frame():
    """itertuples 는 컬럼별 dtype 을 보존한다(.values 는 혼합 시 float 로 업캐스트)."""
    df = pd.DataFrame({"i": [1, 2], "f": [1.5, 2.5], "s": ["a", "b"]})
    res = _shape(None, df, 10, time.perf_counter())
    assert res.rows == [[1, 1.5, "a"], [2, 2.5, "b"]]
    assert isinstance(res.rows[0][0], int) and not isinstance(res.rows[0][0], bool)


@requires_pandas
def test_real_json_safe_numpy_types():
    import numpy as np

    assert _json_safe(np.int64(7)) == 7 and isinstance(_json_safe(np.int64(7)), int)
    assert _json_safe(np.bool_(True)) is True
    assert _json_safe(np.array([1, 2, 3])) == [1, 2, 3]
    assert _json_safe(np.float64("nan")) is None
    assert _json_safe(np.float64(1.5)) == 1.5
