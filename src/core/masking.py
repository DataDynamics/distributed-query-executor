"""민감값 마스킹 유틸 — coordinator·executor 대시보드가 공유한다.

DSN 같은 접속 문자열을 화면(/config 탭)이나 응답에 노출할 때 자격증명이 새지
않도록 비밀번호 부분만 가린다. 원래 두 대시보드 모듈에 같은 함수가 복제돼
있었는데, 드리프트를 막기 위해 core 로 승격했다(기존 임포트 경로는 각 모듈이
재수출해 호환 유지).
"""

from __future__ import annotations

import re


def mask_dsn(dsn: str | None) -> str:
    """DSN 의 비밀번호를 마스킹한다: ``scheme://user:pass@host`` → ``scheme://user:***@host``.

    ``user:password@`` 패턴의 비밀번호 부분만 ``***`` 로 치환한다. 사용자명/호스트 등
    나머지는 그대로 둔다. 값이 비어 있으면 빈 문자열을 돌려준다.

    Args:
        dsn: 마스킹할 DSN 문자열(없을 수 있음).

    Returns:
        비밀번호가 가려진 DSN 문자열(입력이 없으면 "", 자격증명이 없으면 원본 그대로).
    """
    if not dsn:
        return ""
    return re.sub(r"://([^:/@]+):([^@]+)@", r"://\1:***@", dsn)
