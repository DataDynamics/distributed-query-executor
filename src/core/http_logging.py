"""HTTP 요청/응답 DEBUG 로깅 미들웨어 (coordinator·executor 공용).

로그 레벨이 **DEBUG 일 때만** 각 HTTP 요청/응답을 ``core.http`` 로거로 남긴다. 별도의
켜기 스위치 없이, 애플리케이션 로그 레벨을 DEBUG 로 내리면(``app.debug=true`` 또는
``log.level=DEBUG``) 자동으로 기록되고, 그렇지 않으면 미들웨어가 즉시 통과해 오버헤드가
사실상 없다(`logging.http.enabled=false` 로 DEBUG 여도 끌 수 있다).

## 왜 ASGI 미들웨어인가

Starlette ``BaseHTTPMiddleware`` 에서 요청 본문을 미리 읽으면 다운스트림 핸들러가 본문을
못 읽는 잘 알려진 문제가 있다. 그래서 여기서는 **순수 ASGI 미들웨어**로, ``receive``/
``send`` 콜러블을 감싸 메시지를 **엿보기만** 한다(스트림을 소비하지 않고 그대로 전달).
따라서 핸들러의 본문 읽기에 영향을 주지 않으면서 요청/응답 본문을 로깅용으로 복사할 수
있다. 복사본은 ``max_body`` 바이트까지만 보관하고(메모리 방어), 원본 메시지는 항상
그대로 흘려보낸다 — 스트리밍/대용량 응답도 로그만 절단될 뿐 정상 전달된다.

## 안전장치

- **마스킹**: 본문·헤더의 DSN·비밀 필드(:func:`core.masking.mask_text`), 민감 헤더 값
  전체(:func:`core.masking.mask_header_value`).
- **절단**: 본문은 ``max_body`` 로 자르고 초과 표시. 비-텍스트(바이너리) 본문은 크기만.
- **잡음 제어**: health/metrics/정적/docs 등은 기본 제외(대시보드 3초 폴링·정적파일 홍수 방지).

순수 포매팅 함수(:func:`format_body`/:func:`format_headers`/:func:`is_excluded`)는 I/O
무관이라 테스트 대상이다(``tests/test_http_logging.py``).
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Iterable

from core.masking import mask_header_value, mask_text

# 요청/응답 로그 전용 로거. 이름이 따로라 WARNING 전용 로그에 로거명이 찍히고,
# 필요하면 이 로거만 레벨을 조정할 수 있다. 루트 핸들러(메인 .log)로 흘러가며 DEBUG 라
# WARNING 전용 로그(*-warn.log)에는 남지 않는다.
logger = logging.getLogger("core.http")

# 본문을 텍스트로 디코드해 로깅할 content-type 접두/부분 일치 목록. 그 외는 바이너리 취급.
_TEXTUAL_CONTENT_TYPES = (
    "application/json",
    "application/x-www-form-urlencoded",
    "application/xml",
    "text/",
    "+json",
    "+xml",
)

# 잡음 경로 기본 제외 목록(설정으로 덮어쓸 수 있음).
DEFAULT_EXCLUDE_PATHS = (
    "/health",
    "/metrics",
    "/assets",
    "/favicon.ico",
    "/docs",
    "/redoc",
    "/openapi.json",
)


def is_excluded(path: str, excludes: Iterable[str]) -> bool:
    """``path`` 가 제외 목록에 해당하는지 판정한다(정확 일치 또는 ``prefix/`` 하위)."""
    for p in excludes:
        if not p:
            continue
        if path == p or path.startswith(p.rstrip("/") + "/"):
            return True
    return False


def _is_textual(content_type: str) -> bool:
    """content-type 이 텍스트로 디코드해 로깅할 만한 유형인지 판정한다."""
    ct = (content_type or "").lower()
    return any(marker in ct for marker in _TEXTUAL_CONTENT_TYPES)


def _find_header(headers: Iterable[tuple[bytes, bytes]] | None, name: bytes) -> str:
    """ASGI 헤더 목록(list[tuple[bytes,bytes]])에서 ``name`` 헤더 값을 찾는다(없으면 "")."""
    if not headers:
        return ""
    name = name.lower()
    for key, val in headers:
        if key.lower() == name:
            return val.decode("latin-1")
    return ""


def format_body(raw: bytes, content_type: str, *, over_limit: bool, max_body: int) -> str:
    """본문 바이트를 로그 한 줄 문자열로 만든다(텍스트면 마스킹·개행 압축, 아니면 크기만).

    Args:
        raw: 보관한 본문 바이트(최대 ``max_body``).
        content_type: 본문 content-type(텍스트 판정용).
        over_limit: 원본이 보관분보다 커서 절단됐는지 여부.
        max_body: 절단 기준 바이트(표시용).
    """
    if not _is_textual(content_type):
        # 바이너리(또는 알 수 없는 유형): 내용 대신 크기만 남긴다.
        more = "+" if over_limit else ""
        return f"<{len(raw)}{more} bytes {content_type or 'binary'}>"
    text = raw.decode("utf-8", errors="replace")
    text = mask_text(text)
    # 여러 줄 본문을 한 줄로 압축(로그 가독성). 연속 공백도 한 칸으로.
    text = " ".join(text.split())
    suffix = f" …(잘림, {max_body}B 초과)" if over_limit else ""
    return text + suffix


def format_headers(
    headers: Iterable[tuple[bytes, bytes]] | None, *, mask: bool = True
) -> str:
    """ASGI 헤더 목록을 ``k=v; k=v`` 문자열로 만든다(민감 헤더 값은 마스킹)."""
    if not headers:
        return ""
    parts = []
    for key, val in headers:
        k = key.decode("latin-1")
        v = val.decode("latin-1")
        parts.append(f"{k}={mask_header_value(k, v) if mask else v}")
    return "; ".join(parts)


class HttpLoggingMiddleware:
    """DEBUG 레벨일 때만 HTTP 요청/응답을 ``core.http`` 로거로 남기는 ASGI 미들웨어.

    요청 도착 즉시 ``→`` 한 줄(메서드·경로·쿼리·client)을 남겨 요청 유입/행(hang)을
    바로 볼 수 있게 하고, 처리가 끝나면 ``←`` 한 줄(상태·소요시간)을 남긴다. 두 줄은
    짧은 요청 id(``rid``)로 상관된다. 본문 로깅이 켜져 있으면 요청/응답 본문을 별도 줄로
    남긴다(마스킹·절단).
    """

    def __init__(
        self,
        app,
        *,
        enabled: bool = True,
        bodies: bool = True,
        max_body: int = 2048,
        headers: bool = False,
        exclude_paths: Iterable[str] = DEFAULT_EXCLUDE_PATHS,
    ) -> None:
        self.app = app
        self.enabled = enabled
        self.bodies = bodies
        self.max_body = max(0, int(max_body))
        self.log_headers = headers
        self.exclude_paths = tuple(exclude_paths)

    def _active(self) -> bool:
        """이번 요청을 로깅할지: 설정 on + 로그 레벨이 DEBUG 일 때만."""
        return self.enabled and logger.isEnabledFor(logging.DEBUG)

    async def __call__(self, scope, receive, send):
        # HTTP 가 아니거나(웹소켓·lifespan), 비활성/비-DEBUG, 제외 경로면 그대로 통과.
        if scope.get("type") != "http" or not self._active():
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if is_excluded(path, self.exclude_paths):
            await self.app(scope, receive, send)
            return

        rid = uuid.uuid4().hex[:8]
        method = scope.get("method", "-")
        query = scope.get("query_string", b"").decode("latin-1")
        client = scope.get("client")
        client_str = f"{client[0]}:{client[1]}" if client else "-"
        headers_in = scope.get("headers")
        req_ct = _find_header(headers_in, b"content-type")

        # 요청 라인(도착 즉시). 본문은 아직 읽히지 않았으므로 여기선 메타데이터만.
        qs = f"?{query}" if query else ""
        logger.debug("→ [%s] %s %s%s client=%s", rid, method, path, qs, client_str)
        if self.log_headers:
            logger.debug("  [%s] req-headers %s", rid, format_headers(headers_in))

        # ── receive 래핑: 요청 본문 청크를 max_body 까지만 복사(원본은 그대로 전달) ──
        req_body = bytearray()
        req_over = False

        async def recv():
            nonlocal req_over
            message = await receive()
            if self.bodies and message.get("type") == "http.request":
                chunk = message.get("body", b"")
                if chunk:
                    room = self.max_body - len(req_body)
                    if room > 0:
                        req_body.extend(chunk[:room])
                    if len(chunk) > max(room, 0):
                        req_over = True
            return message

        # ── send 래핑: 응답 status/headers 와 본문 청크(최대 max_body)를 복사 ──
        resp = {"status": None, "headers": None}
        resp_body = bytearray()
        resp_over = False

        async def snd(message):
            nonlocal resp_over
            mtype = message.get("type")
            if mtype == "http.response.start":
                resp["status"] = message.get("status")
                resp["headers"] = message.get("headers")
            elif mtype == "http.response.body" and self.bodies:
                chunk = message.get("body", b"")
                if chunk:
                    room = self.max_body - len(resp_body)
                    if room > 0:
                        resp_body.extend(chunk[:room])
                    if len(chunk) > max(room, 0):
                        resp_over = True
            await send(message)

        start = time.monotonic()
        try:
            await self.app(scope, recv, snd)
        except Exception:
            # 핸들러 예외 시에도 종료 라인을 남기고(디버깅) 예외는 그대로 전파.
            dur = (time.monotonic() - start) * 1000
            logger.debug("← [%s] %s %s EXC %.1fms", rid, method, path, dur)
            raise

        dur = (time.monotonic() - start) * 1000
        # 요청 본문(있으면).
        if self.bodies and req_body:
            logger.debug(
                "  [%s] req-body %s", rid,
                format_body(bytes(req_body), req_ct, over_limit=req_over, max_body=self.max_body),
            )
        # 응답 라인.
        logger.debug("← [%s] %s %s %s %.1fms", rid, method, path, resp["status"], dur)
        if self.log_headers and resp["headers"]:
            logger.debug("  [%s] resp-headers %s", rid, format_headers(resp["headers"]))
        if self.bodies and resp_body:
            resp_ct = _find_header(resp["headers"], b"content-type")
            logger.debug(
                "  [%s] resp-body %s", rid,
                format_body(bytes(resp_body), resp_ct, over_limit=resp_over, max_body=self.max_body),
            )


def install_http_logging(app, settings) -> None:
    """앱에 HTTP 로깅 미들웨어를 등록한다(설정값으로 구성). ``create_app`` 에서 호출.

    설정(``logging.http.*``)을 읽어 미들웨어를 붙인다. DEBUG 게이팅은 미들웨어가
    요청마다 스스로 판단하므로, 여기서는 항상 등록하되 값만 넘긴다.
    """
    app.add_middleware(
        HttpLoggingMiddleware,
        enabled=getattr(settings, "log_http_enabled", True),
        bodies=getattr(settings, "log_http_bodies", True),
        max_body=getattr(settings, "log_http_max_body", 2048),
        headers=getattr(settings, "log_http_headers", False),
        exclude_paths=getattr(settings, "log_http_exclude_paths", DEFAULT_EXCLUDE_PATHS)
        or DEFAULT_EXCLUDE_PATHS,
    )
