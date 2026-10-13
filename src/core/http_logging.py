"""HTTP 요청과 응답을 남기는 순수 ASGI 미들웨어이며 coordinator 와 executor 가 함께 쓴다.

## 무엇을 어느 레벨로 남기나

요청 하나가 끝나면 ``core.http`` 로거에 **레코드 하나**를 남긴다. 머리줄에 메서드·경로·상태·
소요시간·client·요청 식별자를 적고, 그 아래 줄에 요청·응답 본문(과 선택적으로 헤더)을 붙인다.
레벨은 요청 종류로 정한다.

- **API 호출**(POST·PUT·PATCH·DELETE)과 **실패 응답**(상태 400 이상)은 ``logging.http.level``
  (기본 INFO)로 남긴다. 운영 기본 레벨에서도 무엇을 요청받아 무엇을 돌려줬는지가 남는다.
- **성공한 조회**(GET·HEAD·OPTIONS)는 ``logging.http.read_level``(기본 DEBUG)로 남긴다.
  대시보드와 클라이언트의 상태 폴링이 몇 초마다 들어오므로 INFO 로 남기면 로그가 묻힌다.
- 요청이 **도착한 순간**의 한 줄(``→``)은 DEBUG 로만 남긴다. 응답이 오지 않는 요청(hang)을
  좇을 때 쓴다.

## 요청 식별자

미들웨어는 요청마다 짧은 식별자를 정해 :data:`core.logging.request_id_var` 에 넣는다. 그러면
job 컨텍스트가 없는 그 요청 안의 모든 로그(템플릿 렌더·실행 SQL 등)의 job_id 자리에
``req-<id>`` 가 찍힌다. 식별자는 응답 헤더 ``X-Request-ID`` 로 돌려주고, 요청에 같은 헤더가
있으면 그 값을 이어 쓴다. coordinator 가 executor 를 부를 때 이 헤더를 실어 보내므로, 한
요청이 두 프로세스에 남긴 로그를 같은 식별자로 grep 할 수 있다. 식별자는 로깅이 꺼져 있어도
붙인다(헤더 하나와 ContextVar 하나라 비용이 사실상 없다).

## 왜 ASGI 미들웨어인가

Starlette ``BaseHTTPMiddleware`` 에서 요청 본문을 미리 읽으면 다운스트림 핸들러가 본문을
못 읽는 잘 알려진 문제가 있다. 그래서 ``receive``/``send`` 콜러블을 감싸 메시지를 **엿보기만**
한다(스트림을 소비하지 않고 그대로 전달). 복사본은 ``max_body`` 바이트까지만 보관하고(메모리
방어), 원본 메시지는 항상 그대로 흘려보낸다. 스트리밍과 대용량 응답도 로그만 잘릴 뿐 정상
전달된다.

## 안전장치

- **마스킹**: 본문·헤더의 DSN·비밀 필드(:func:`core.masking.mask_text`), 민감 헤더 값
  전체(:func:`core.masking.mask_header_value`).
- **절단**: 본문은 ``max_body`` 로 자르고 초과 표시. 비-텍스트(바이너리) 본문은 크기만.
- **잡음 제어**: health/metrics/정적/docs 등은 기본 제외하며, 제외 경로에는 식별자도 붙이지 않는다.

순수 포매팅 함수(:func:`format_body`/:func:`format_headers`/:func:`is_excluded`/
:func:`completion_level`)는 I/O 무관이라 테스트 대상이다(``tests/test_http_logging.py``).
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from typing import Iterable

from core.logging import request_id_var
from core.masking import mask_header_value, mask_text

# 요청/응답 로그 전용 로거. 이름이 따로라 WARNING 전용 로그에 로거명이 찍히고,
# 필요하면 이 로거만 레벨을 조정할 수 있다. 루트 핸들러(메인 .log)로 흘러가며 DEBUG 라
# WARNING 전용 로그(*-warn.log)에는 남지 않는다.
logger = logging.getLogger("core.http")

# 본문을 텍스트로 디코드해 남길 content-type 목록이며 접두 또는 부분 일치로 본다. 그 밖은 바이너리로 취급한다.
_TEXTUAL_CONTENT_TYPES = (
    "application/json",
    "application/x-www-form-urlencoded",
    "application/xml",
    "text/",
    "+json",
    "+xml",
)

# 잡음이 되는 경로의 기본 제외 목록이며 설정으로 덮어쓸 수 있다.
DEFAULT_EXCLUDE_PATHS = (
    "/health",
    "/metrics",
    "/assets",
    "/favicon.ico",
    "/docs",
    "/redoc",
    "/openapi.json",
)


# 요청 식별자를 주고받는 헤더 이름이다. coordinator → executor 호출에도 같은 이름을 쓴다.
REQUEST_ID_HEADER = "X-Request-ID"

# 받아들일 외부 요청 식별자의 모양이다. 로그 줄과 헤더에 그대로 찍히므로 줄바꿈이나 공백이
# 섞인 값(로그 위조)은 버리고 새로 만든다.
_REQUEST_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,64}")

# 성공해도 상태를 바꾸지 않는 조회 메서드다. 이 메서드의 성공 응답은 read_level 로 남긴다.
_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def completion_level(method: str, status, *, level: int, read_level: int) -> int:
    """완료 레코드의 로그 레벨을 정한다.

    실패 응답(400 이상)이나 응답 없이 끝난 요청(``status`` 가 None)은 조회든 아니든 ``level`` 로
    남긴다. 성공한 조회(GET·HEAD·OPTIONS)는 폴링 잡음이므로 ``read_level`` 이고, 그 밖의 API
    호출은 ``level`` 이다.
    """
    if status is None or int(status) >= 400:
        return level
    return read_level if method.upper() in _READ_METHODS else level


def resolve_request_id(headers: Iterable[tuple[bytes, bytes]] | None) -> str:
    """요청 헤더의 ``X-Request-ID`` 를 이어 쓰고, 없거나 모양이 이상하면 새로 만든다."""
    incoming = _find_header(headers, REQUEST_ID_HEADER.lower().encode()).strip()
    if incoming and _REQUEST_ID_RE.fullmatch(incoming):
        return incoming
    return uuid.uuid4().hex[:12]


def outgoing_headers() -> dict:
    """현재 요청 식별자를 다른 서비스 호출에 실어 보낼 헤더 dict 를 만든다(요청 밖이면 빈 dict)."""
    rid = request_id_var.get()
    return {REQUEST_ID_HEADER: rid} if rid != "-" else {}


def _level_value(value, default: int) -> int:
    """설정의 레벨 문자열(``"INFO"`` 등)이나 숫자를 logging 상수로 바꾼다(모르는 값은 기본값)."""
    if isinstance(value, int):
        return value
    resolved = logging.getLevelName(str(value or "").strip().upper())
    return resolved if isinstance(resolved, int) else default


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
    # 여러 줄 본문을 한 줄로 압축해 로그를 읽기 쉽게 한다. 연속 공백도 한 칸으로 접는다.
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
    """요청마다 식별자를 붙이고, 끝나면 요청·응답을 레코드 하나로 ``core.http`` 로거에 남기는 ASGI 미들웨어다.

    레벨 규칙은 모듈 docstring 과 :func:`completion_level` 을 따른다. 완료 레벨이 로거에서
    꺼져 있으면 본문을 복사하지 않아 오버헤드가 없다.
    """

    def __init__(
        self,
        app,
        *,
        enabled: bool = True,
        bodies: bool = True,
        max_body: int = 8192,
        headers: bool = False,
        exclude_paths: Iterable[str] = DEFAULT_EXCLUDE_PATHS,
        level=logging.INFO,
        read_level=logging.DEBUG,
    ) -> None:
        self.app = app
        self.enabled = enabled
        self.bodies = bodies
        self.max_body = max(0, int(max_body))
        self.log_headers = headers
        self.exclude_paths = tuple(exclude_paths)
        self.level = _level_value(level, logging.INFO)
        self.read_level = _level_value(read_level, logging.DEBUG)

    async def __call__(self, scope, receive, send):
        # 웹소켓이나 lifespan 처럼 HTTP 가 아니거나 제외 경로면 손대지 않고 통과시킨다.
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if is_excluded(path, self.exclude_paths):
            await self.app(scope, receive, send)
            return

        headers_in = scope.get("headers")
        rid = resolve_request_id(headers_in)
        rid_header = (REQUEST_ID_HEADER.lower().encode(), rid.encode("latin-1"))
        token = request_id_var.set(rid)
        try:
            # 두 레벨 중 하나라도 로거에서 켜져 있어야 본문을 복사한다. 완료 레벨은 응답을 받아야
            # 정해지므로 여기서는 둘 중 하나가 켜졌는지만 보고, 실제 기록 여부는 완료 시점에 다시 본다.
            logging_on = self.enabled and (
                logger.isEnabledFor(self.level) or logger.isEnabledFor(self.read_level)
            )
            if not logging_on:
                async def snd_plain(message):
                    if message.get("type") == "http.response.start":
                        message = _with_header(message, rid_header)
                    await send(message)

                await self.app(scope, receive, snd_plain)
                return
            await self._logged_call(scope, receive, send, rid, rid_header, headers_in, path)
        finally:
            request_id_var.reset(token)

    async def _logged_call(self, scope, receive, send, rid, rid_header, headers_in, path):
        """본문을 엿보며 앱을 호출하고, 끝나면 완료 레코드를 남긴다."""
        method = scope.get("method", "-")
        query = scope.get("query_string", b"").decode("latin-1")
        client = scope.get("client")
        client_str = f"{client[0]}:{client[1]}" if client else "-"
        req_ct = _find_header(headers_in, b"content-type")
        qs = f"?{query}" if query else ""

        # 요청이 도착하자마자 한 줄을 남긴다(DEBUG). 응답이 오지 않는 요청을 좇을 때 쓴다.
        logger.debug("→ %s %s%s client=%s req_id=%s", method, path, qs, client_str, rid)

        # receive 를 감싸 요청 본문 청크를 max_body 까지만 복사한다. 원본은 그대로 전달한다.
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

        # send 를 감싸 응답의 status·headers 와 본문 청크를 max_body 까지 복사하고 식별자 헤더를 붙인다.
        resp = {"status": None, "headers": None}
        resp_body = bytearray()
        resp_over = False

        async def snd(message):
            nonlocal resp_over
            mtype = message.get("type")
            if mtype == "http.response.start":
                message = _with_header(message, rid_header)
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
        failed = False
        try:
            await self.app(scope, recv, snd)
        except Exception:
            # 핸들러에서 예외가 나도 완료 레코드를 남기고, 예외 자체는 그대로 전파한다.
            failed = True
            raise
        finally:
            dur = (time.monotonic() - start) * 1000
            status = "EXC" if failed else resp["status"]
            lvl = completion_level(method, None if failed else resp["status"],
                                   level=self.level, read_level=self.read_level)
            if logger.isEnabledFor(lvl):
                lines = [f"HTTP {method} {path}{qs} → {status} {dur:.1f}ms client={client_str} req_id={rid}"]
                if self.log_headers:
                    lines.append(f"  req-headers  {format_headers(headers_in)}")
                if self.bodies and req_body:
                    lines.append("  req-body  " + format_body(
                        bytes(req_body), req_ct, over_limit=req_over, max_body=self.max_body))
                if self.log_headers and resp["headers"]:
                    lines.append(f"  resp-headers {format_headers(resp['headers'])}")
                if self.bodies and resp_body:
                    resp_ct = _find_header(resp["headers"], b"content-type")
                    lines.append("  resp-body " + format_body(
                        bytes(resp_body), resp_ct, over_limit=resp_over, max_body=self.max_body))
                logger.log(lvl, "%s", "\n".join(lines))


def _with_header(message: dict, header: tuple[bytes, bytes]) -> dict:
    """응답 시작 메시지에 헤더를 하나 덧붙인 사본을 만든다. 앱이 이미 같은 헤더를 넣었으면 그대로 둔다."""
    headers = list(message.get("headers") or [])
    if any(k.lower() == header[0] for k, _ in headers):
        return message
    headers.append(header)
    return {**message, "headers": headers}


def install_http_logging(app, settings) -> None:
    """설정값으로 구성한 HTTP 로깅 미들웨어를 앱에 등록한다. ``create_app`` 이 호출한다.

    설정(``logging.http.*``)을 읽어 미들웨어를 붙인다. 레벨 게이팅은 미들웨어가 요청마다
    스스로 판단하므로 항상 등록한다(요청 식별자는 로깅이 꺼져 있어도 붙어야 한다).
    """
    app.add_middleware(
        HttpLoggingMiddleware,
        enabled=getattr(settings, "log_http_enabled", True),
        bodies=getattr(settings, "log_http_bodies", True),
        max_body=getattr(settings, "log_http_max_body", 8192),
        headers=getattr(settings, "log_http_headers", False),
        exclude_paths=getattr(settings, "log_http_exclude_paths", DEFAULT_EXCLUDE_PATHS)
        or DEFAULT_EXCLUDE_PATHS,
        level=getattr(settings, "log_http_level", "INFO"),
        read_level=getattr(settings, "log_http_read_level", "DEBUG"),
    )
