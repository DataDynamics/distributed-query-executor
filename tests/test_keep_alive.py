"""HTTP 서버의 keep-alive 유지 시간이 클라이언트(httpx)의 유휴 연결 만료보다 긴지 검증한다.

둘이 같으면(uvicorn 기본 5초 = httpx 기본 5초) 클라이언트가 연결을 재사용하는 순간 서버가 닫아
"Server disconnected without sending a response" 가 난다. 서버 루프가 바쁠수록 잦아진다.
"""

from __future__ import annotations

import httpx

from core import config as core_config


def _httpx_keepalive_expiry() -> float:
    return httpx.Limits().keepalive_expiry or 0.0


def test_서버_keep_alive_가_클라이언트_만료보다_길다():
    s = core_config.settings
    assert s.coordinator_keep_alive_s > _httpx_keepalive_expiry()
    assert s.executor_keep_alive_s > _httpx_keepalive_expiry()


def test_런처가_keep_alive_설정을_uvicorn_에_넘긴다(monkeypatch):
    import uvicorn

    import coordinator.__main__ as cm
    import executor.__main__ as em

    seen = []
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: seen.append(kw.get("timeout_keep_alive")))
    monkeypatch.setattr(core_config.settings, "coordinator_keep_alive_s", 41, raising=False)
    monkeypatch.setattr(core_config.settings, "executor_keep_alive_s", 42, raising=False)
    for mod in (cm, em):
        monkeypatch.setattr(mod, "setup_logging", lambda *a, **kw: None, raising=False)
        monkeypatch.setattr(mod, "log_startup", lambda *a, **kw: None, raising=False)
    cm.main()
    em.main()
    assert seen == [41, 42]
