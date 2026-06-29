"""응답 JSON 의 시각 표기 헬퍼.

내부 저장·DB(TIMESTAMPTZ)·정렬은 ISO8601(UTC, 예: ``2026-06-29T07:01:11.123456+00:00``)
형식을 그대로 유지하고, **API 응답으로 내보낼 때만** 사람이 보기 좋은
``yyyy-MM-dd HH:mm:ss.sss``(밀리초 3자리, 공백 구분, 타임존 접미사 없음)로 바꾼다.

이렇게 출력 경계에서만 변환하므로, 저장값의 의미(UTC 절대시각)나 문자열 정렬, DB 컬럼
타입에는 영향을 주지 않는다. 표기 시각은 항상 UTC 기준 벽시계(wall-clock)다.
"""

from __future__ import annotations

from datetime import datetime, timezone


def to_display(value):
    """ISO 문자열 또는 datetime 을 ``yyyy-MM-dd HH:mm:ss.sss`` 문자열로 변환한다.

    - None/빈 값이면 None 을 돌려준다(응답에서 null 로 표기).
    - tz 정보가 있으면 UTC 로 환산한 뒤 표기한다(없으면 그대로 UTC 로 간주).
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
        dt = dt.astimezone(timezone.utc)
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
