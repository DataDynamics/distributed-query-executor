"""응답 JSON 의 _at 시각 표기(yyyy-MM-dd HH:mm:ss.sss) 변환 검증."""

import re
from datetime import datetime, timedelta, timezone

from core.timeutil import KST, format_at_fields, now_dt, now_iso, to_display

# 표기 형식: 'YYYY-MM-DD HH:MM:SS.mmm' (밀리초 3자리)
_FMT = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$")


def test_to_display_naive_kst_passthrough():
    # 타임존 없는 KST naive 값은 그대로(밀리초 절삭) 표기된다.
    assert to_display("2026-06-29T16:01:11.123456") == "2026-06-29 16:01:11.123"


def test_to_display_no_fraction_pads_millis():
    assert to_display("2026-06-29T16:01:11") == "2026-06-29 16:01:11.000"


def test_to_display_utc_converted_to_kst():
    # 과거 UTC(+00:00) 값이 들어오면 KST(+9)로 환산해서 표기한다.
    assert to_display("2026-06-29T07:01:11.123456+00:00") == "2026-06-29 16:01:11.123"


def test_to_display_kst_aware_keeps_wallclock():
    # +09:00 붙은 값 → KST 환산 → 같은 벽시계
    dt = datetime(2026, 6, 29, 16, 1, 11, 123456, tzinfo=timezone(timedelta(hours=9)))
    assert to_display(dt) == "2026-06-29 16:01:11.123"


def test_now_iso_and_now_dt_are_naive_kst():
    # 생성 헬퍼는 타임존이 없어야(naive) 한다.
    assert now_dt().tzinfo is None
    assert "+" not in now_iso() and "Z" not in now_iso()
    # KST 기준이므로 UTC now 보다 약 9시간 앞선다(여유 있게 8~10시간 범위로 검증).
    utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    delta = now_dt() - utc_naive
    assert timedelta(hours=8) < delta < timedelta(hours=10)


def test_to_display_none_and_empty():
    assert to_display(None) is None
    assert to_display("") is None


def test_to_display_unparseable_passthrough():
    assert to_display("not-a-date") == "not-a-date"


def test_format_at_fields_recurses_and_skips_non_at():
    src = {
        "created_at": "2026-06-29T07:01:11.999999+00:00",
        "n": 1,
        "last_checked": "keep-me",  # _at 아님 → 변환 안 함
        "tasks": [{"started_at": "2026-06-29T07:01:11.5+00:00", "x": 2}],
    }
    out = format_at_fields(src)
    assert _FMT.match(out["created_at"])
    assert out["n"] == 1
    assert out["last_checked"] == "keep-me"
    assert _FMT.match(out["tasks"][0]["started_at"])
    assert out["tasks"][0]["x"] == 2


def test_api_status_view_at_fields_formatted(client, store):
    # 작업을 제출하고 상태 조회 응답의 _at 필드가 표기 형식인지 확인.
    payload = {
        "sql": "SELECT a, dt FROM imp WHERE dt IN ('1','2')",
        "partition_column": "dt",
        "target_table": "public.target",
        "parallelism": 2,
    }
    job_id = client.post("/jobs", json=payload).json()["job_id"]
    body = client.get(f"/jobs/{job_id}").json()
    assert _FMT.match(body["created_at"]), body["created_at"]
