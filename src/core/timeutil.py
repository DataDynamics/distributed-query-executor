"""시각을 만들고 표기하는 헬퍼다. 전 구간이 KST(Asia/Seoul) 기준이며 타임존 없는 naive 값을 쓴다.

이 시스템은 한국 단일 리전 환경이라 UTC·타임존이 필요 없다. 그래서 앱이 만드는 모든
시각은 **타임존 정보가 없는 KST 벽시계(naive)** 로 생성·저장·표기한다.

- 생성: ``now_iso()`` / ``now_dt()`` 가 KST naive 값을 돌려준다.
- 저장: DB 컬럼은 ``TIMESTAMP``(타임존 없음)이고, 앱이 naive KST 문자열을 그대로 넣는다.
        DB 가 채우는 기본값/``now()`` 도 ``now() AT TIME ZONE 'Asia/Seoul'`` 로 KST naive 다.
- 표기: ``to_display()`` 가 ``yyyy-MM-dd HH:mm:ss.sss``(밀리초 3자리) 문자열로 바꾼다.

과거에 +00:00(UTC) 같은 타임존이 붙은 값이 들어와도 KST 로 환산해 표기하도록 방어한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

# 한국 표준시(UTC+9)다. 표기와 환산의 기준이 되는 단일 타임존이다.
KST = timezone(timedelta(hours=9), name="KST")


def now_dt() -> datetime:
    """현재 KST 시각을 **타임존 없는(naive)** datetime 으로 반환한다."""
    return datetime.now(KST).replace(tzinfo=None)


def now_iso() -> str:
    """현재 KST 시각을 타임존 없는 ISO 문자열로 반환한다(started_at/finished_at 기록용).

    예: ``2026-06-29T16:01:11.123456`` (오프셋 접미사 없음).
    """
    return now_dt().isoformat()


def to_display(value):
    """ISO 문자열 또는 datetime 을 ``yyyy-MM-dd HH:mm:ss.sss``(KST) 문자열로 변환한다.

    - None/빈 값이면 None 을 돌려준다(응답에서 null 로 표기).
    - 타임존이 붙어 있으면 KST 로 환산한 뒤 타임존을 떼고 표기한다(과거 UTC 값 방어).
    - 파싱할 수 없는 문자열은 원본을 그대로 돌려준다(안전 폴백).
    - 밀리초는 마이크로초를 1000 으로 나눠 3자리로 자른다(반올림 없이 절삭).
    """
    if not value:
        return None
    dt = value
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return value
    if not isinstance(dt, datetime):
        return value
    if dt.tzinfo is not None:
        dt = dt.astimezone(KST).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def format_at_fields(obj):
    """응답 객체(dict/list)를 재귀적으로 훑어 키가 ``_at`` 로 끝나는 스칼라 값을 표기 변환한다.

    중첩된 dict/list(예: status_view 의 ``tasks`` 목록, /cluster 의 ``executors``)까지 모두
    처리한다. ``_at`` 로 끝나지 않는 키(예: ``last_checked``, ``age_s``)는 건드리지 않는다.
    원본을 변형하지 않고 새 객체를 만들어 돌려준다.
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k.endswith("_at") and not isinstance(v, (dict, list)):
                out[k] = to_display(v)
            else:
                out[k] = format_at_fields(v)
        return out
    if isinstance(obj, list):
        return [format_at_fields(x) for x in obj]
    return obj
