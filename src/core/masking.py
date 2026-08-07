"""민감값 마스킹 유틸 — coordinator·executor 대시보드가 공유한다.

DSN 같은 접속 문자열을 화면(/config 탭)이나 응답에 노출할 때 자격증명이 새지
않도록 비밀번호 부분만 가린다. 원래 두 대시보드 모듈에 같은 함수가 복제돼
있었는데, 드리프트를 막기 위해 core 로 승격했다(기존 임포트 경로는 각 모듈이
재수출해 호환 유지).
"""

from __future__ import annotations

import re

# 요청·응답 본문이나 헤더 같은 자유 텍스트에서 흔한 자격증명 패턴을 가리는 정규식이다.
# 1) DSN 이나 URL 의 userinfo 비밀번호를 가린다. scheme://user:pass@host 는 user:***@host 가 된다.
_DSN_CRED_RE = re.compile(r"://([^:/@\s]+):([^@\s]+)@")
# 2) key=value / "key": "value" 형태의 비밀 필드(대소문자 무시). JSON·폼·properties 공통.
#    값은 따옴표 안이거나(쉼표·중괄호·따옴표 전까지) 공백/구분자 전까지로 본다.
_SECRET_FIELD_RE = re.compile(
    r'(?P<key>["\']?(?:password|passwd|pwd|secret|token|api[_-]?key|auth[_-]?token)["\']?'
    r'\s*[:=]\s*)(?P<q>["\']?)(?P<val>[^"\',\s}&]+)',
    re.IGNORECASE,
)

# 값을 통째로 가릴 민감 HTTP 헤더 목록이며 이름은 소문자를 기준으로 본다.
SENSITIVE_HEADERS = frozenset(
    {"authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key"}
)


def mask_dsn(dsn: str | None) -> str:
    """DSN 의 비밀번호를 마스킹한다. ``scheme://user:pass@host`` 는 ``scheme://user:***@host`` 가 된다.

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


def mask_text(text: str | None) -> str:
    """자유 텍스트(요청/응답 본문 등)에서 흔한 자격증명을 **best-effort** 로 가린다.

    HTTP 요청/응답 본문을 DEBUG 로그로 남길 때 비밀번호·토큰·DSN 이 평문으로 새지
    않도록, 아래 두 패턴을 치환한다. 구조 파싱이 아니라 정규식 기반이라 완벽하진 않지만,
    운영에서 흔한 노출(접속 DSN·`password`/`token` 필드)을 줄이는 것이 목적이다.

    1. DSN 과 URL 의 userinfo 비밀번호를 가린다(``user:pass@`` 가 ``user:***@`` 가 된다).
    2. ``password``/``token``/``secret``/``api_key`` 등 키의 값(따옴표 유무 무관).

    Args:
        text: 마스킹할 텍스트(없을 수 있음).

    Returns:
        비밀값이 ``***`` 로 가려진 텍스트(입력이 없으면 "").
    """
    if not text:
        return ""
    text = _DSN_CRED_RE.sub(r"://\1:***@", text)
    text = _SECRET_FIELD_RE.sub(lambda m: f"{m.group('key')}{m.group('q')}***", text)
    return text


def mask_header_value(name: str, value: str) -> str:
    """민감 HTTP 헤더면 값을 통째로 가리고, 아니면 본문 마스킹 규칙을 적용한다.

    ``Authorization``/``Cookie`` 같은 헤더는 값 전체가 자격증명이므로 ``***`` 로 덮고,
    그 외 헤더는 값 안에 섞인 DSN·비밀 필드만 :func:`mask_text` 로 가린다.
    """
    if name.lower() in SENSITIVE_HEADERS:
        return "***"
    return mask_text(value)
