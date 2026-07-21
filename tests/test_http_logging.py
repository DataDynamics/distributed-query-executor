"""HTTP 요청/응답 DEBUG 로깅 미들웨어(core.http_logging) 검증.

순수 포매팅/마스킹 함수는 직접 호출로, 미들웨어는 최소 FastAPI 앱에 붙여 ASGI 로 호출해
검증한다. 핵심 확인 포인트:
- DEBUG 레벨일 때만 요청/응답이 기록되고, 아니면(또는 enabled=false) 아무것도 안 남는다.
- 미들웨어가 본문을 '엿보기'만 하므로 다운스트림 핸들러의 본문 읽기가 깨지지 않는다.
- 본문·헤더의 비밀값이 마스킹되고, 잡음 경로(/health 등)는 제외된다.
"""
from __future__ import annotations

import logging

import httpx
from fastapi import FastAPI, Request

from core.http_logging import (
    DEFAULT_EXCLUDE_PATHS,
    format_body,
    format_headers,
    install_http_logging,
    is_excluded,
)
from core.masking import mask_header_value, mask_text


# ── 순수 함수: 마스킹 ────────────────────────────────────────────────────────

def test_mask_text_masks_dsn_and_secret_fields():
    assert mask_text("postgresql://u:secretpw@h:5432/db") == "postgresql://u:***@h:5432/db"
    out = mask_text('{"password": "hunter2", "token": "abc123", "keep": 1}')
    assert "hunter2" not in out and "abc123" not in out
    assert "***" in out and "keep" in out


def test_mask_text_empty():
    assert mask_text(None) == "" and mask_text("") == ""


def test_mask_header_value():
    assert mask_header_value("Authorization", "Bearer xyz") == "***"
    assert mask_header_value("Cookie", "sid=abc") == "***"
    # 민감하지 않은 헤더는 본문 마스킹 규칙만(여기선 변화 없음).
    assert mask_header_value("Content-Type", "application/json") == "application/json"


# ── 순수 함수: 제외 경로 / 본문·헤더 포매팅 ──────────────────────────────────

def test_is_excluded():
    assert is_excluded("/health", DEFAULT_EXCLUDE_PATHS)
    assert is_excluded("/assets/app.js", DEFAULT_EXCLUDE_PATHS)
    assert is_excluded("/openapi.json", DEFAULT_EXCLUDE_PATHS)
    assert not is_excluded("/jobs", DEFAULT_EXCLUDE_PATHS)
    # prefix 오탐 방지: /health 로 시작하는 다른 경로는 제외되지 않는다.
    assert not is_excluded("/healthz", DEFAULT_EXCLUDE_PATHS)


def test_format_body_textual_masks_and_compacts():
    out = format_body(b'{\n  "password": "x"\n}', "application/json", over_limit=False, max_body=2048)
    assert "***" in out and "\n" not in out  # 마스킹 + 한 줄 압축


def test_format_body_truncation_marker():
    out = format_body(b"abcdef", "text/plain", over_limit=True, max_body=6)
    assert "잘림" in out and "6B" in out


def test_format_body_binary_shows_size_only():
    out = format_body(b"\x00\x01\x02", "application/octet-stream", over_limit=True, max_body=2048)
    assert "bytes" in out and "3" in out  # 내용 대신 크기만


def test_format_headers_masks_sensitive():
    headers = [(b"authorization", b"Bearer secret"), (b"content-type", b"application/json")]
    out = format_headers(headers)
    assert "authorization=***" in out and "secret" not in out
    assert "content-type=application/json" in out


# ── 미들웨어 통합(최소 앱) ───────────────────────────────────────────────────

class _FakeSettings:
    """install_http_logging 이 읽는 설정 속성만 담은 가짜 설정."""

    def __init__(self, **kw):
        self.log_http_enabled = kw.get("enabled", True)
        self.log_http_bodies = kw.get("bodies", True)
        self.log_http_max_body = kw.get("max_body", 2048)
        self.log_http_headers = kw.get("headers", False)
        self.log_http_exclude_paths = kw.get("exclude_paths", list(DEFAULT_EXCLUDE_PATHS))


def _build_app(**settings_kw) -> FastAPI:
    app = FastAPI()

    @app.post("/echo")
    async def echo(request: Request):
        # 다운스트림에서 본문을 읽는다 — 미들웨어가 본문을 소비했다면 여기서 깨진다.
        data = await request.json()
        return {"got": data}

    @app.get("/health")
    async def health():
        return {"ok": True}

    install_http_logging(app, _FakeSettings(**settings_kw))
    return app


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://x")


def _http_msgs(caplog) -> str:
    return "\n".join(r.getMessage() for r in caplog.records if r.name == "core.http")


async def test_logs_request_response_and_keeps_body_intact(caplog):
    app = _build_app()
    with caplog.at_level(logging.DEBUG, logger="core.http"):
        async with _client(app) as c:
            r = await c.post("/echo", json={"sql": "SELECT 1", "password": "hunter2"})
    # 다운스트림 핸들러가 본문을 정상적으로 읽어 echo 했다.
    assert r.status_code == 200
    assert r.json() == {"got": {"sql": "SELECT 1", "password": "hunter2"}}
    msgs = _http_msgs(caplog)
    assert "→" in msgs and "POST /echo" in msgs      # 요청 라인
    assert "←" in msgs and "200" in msgs             # 응답 라인
    assert "SELECT 1" in msgs                          # 본문 로깅됨
    assert "hunter2" not in msgs and "***" in msgs     # 비밀값 마스킹(요청·응답 양쪽)


async def test_excluded_path_not_logged(caplog):
    app = _build_app()
    with caplog.at_level(logging.DEBUG, logger="core.http"):
        async with _client(app) as c:
            await c.get("/health")
    assert _http_msgs(caplog) == ""


async def test_no_logs_when_level_not_debug(caplog):
    app = _build_app()
    # core.http 로거를 INFO 로 두면(=DEBUG 아님) 미들웨어가 통과만 한다.
    with caplog.at_level(logging.INFO, logger="core.http"):
        async with _client(app) as c:
            r = await c.post("/echo", json={"x": 1})
    assert r.status_code == 200
    assert _http_msgs(caplog) == ""


async def test_enabled_false_disables_even_in_debug(caplog):
    app = _build_app(enabled=False)
    with caplog.at_level(logging.DEBUG, logger="core.http"):
        async with _client(app) as c:
            await c.post("/echo", json={"x": 1})
    assert _http_msgs(caplog) == ""


async def test_bodies_false_logs_lines_without_body(caplog):
    app = _build_app(bodies=False)
    with caplog.at_level(logging.DEBUG, logger="core.http"):
        async with _client(app) as c:
            await c.post("/echo", json={"secretkey": "v"})
    msgs = _http_msgs(caplog)
    assert "POST /echo" in msgs          # 요청/응답 라인은 남고
    assert "req-body" not in msgs        # 본문 줄은 없다
    assert "resp-body" not in msgs
