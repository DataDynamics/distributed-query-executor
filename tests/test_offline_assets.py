"""에어갭(인터넷 차단) 내장 에셋 회귀 테스트.

런타임에 외부 CDN/폰트 서버로 나가는 요청이 없어야 한다. 확인 항목:

- ``/docs``(Swagger UI)·``/redoc`` 가 외부 CDN 이 아니라 ``/assets/...`` 로컬
  에셋을 참조한다.
- ``/assets/`` 로 마운트된 정적 파일(swagger-ui·redoc·폰트 woff2)이 실제로 200 으로
  서빙된다.
- 대시보드·docs HTML 에 외부 호스트(fonts.googleapis.com, cdn.jsdelivr.net 등)가
  남아 있지 않다.

coordinator·executor 두 앱을 같은 기준으로 검사한다(공용 ``core/webassets``).
"""
from __future__ import annotations

import httpx
import pytest

from coordinator.app import create_app as create_coordinator_app
from executor.app import create_app as create_executor_app

# 코드/HTML 어디에도 남으면 안 되는 외부 런타임 호스트들.
_EXTERNAL_HOSTS = (
    "fonts.googleapis.com",
    "fonts.gstatic.com",
    "cdn.jsdelivr.net",
    "unpkg.com",
    "cdnjs.cloudflare.com",
    "fastapi.tiangolo.com",
)


def _async_client(app) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://x")


def _make_app(which: str):
    return create_coordinator_app() if which == "coordinator" else create_executor_app()


@pytest.mark.parametrize("which", ["coordinator", "executor"])
async def test_docs_use_local_assets(which: str):
    async with _async_client(_make_app(which)) as c:
        docs = await c.get("/docs")
        redoc = await c.get("/redoc")
        assert docs.status_code == 200
        assert redoc.status_code == 200
        # 로컬 내장 에셋을 참조해야 한다.
        assert "/assets/swagger-ui-bundle.js" in docs.text
        assert "/assets/swagger-ui.css" in docs.text
        assert "/assets/redoc.standalone.js" in redoc.text


@pytest.mark.parametrize("which", ["coordinator", "executor"])
async def test_static_assets_served(which: str):
    async with _async_client(_make_app(which)) as c:
        for path in (
            "/assets/fonts.css",
            "/assets/swagger-ui-bundle.js",
            "/assets/swagger-ui.css",
            "/assets/redoc.standalone.js",
            "/assets/favicon.png",
            "/assets/fonts/roboto-condensed-400-latin.woff2",
            "/assets/fonts/roboto-condensed-600-latin.woff2",
        ):
            r = await c.get(path)
            assert r.status_code == 200, f"{which} {path} -> {r.status_code}"
            assert r.content, f"{which} {path} 비어 있음"


@pytest.mark.parametrize("which", ["coordinator", "executor"])
async def test_no_external_references(which: str):
    async with _async_client(_make_app(which)) as c:
        bodies = {
            "dashboard": (await c.get("/")).text,
            "docs": (await c.get("/docs")).text,
            "redoc": (await c.get("/redoc")).text,
            "fonts.css": (await c.get("/assets/fonts.css")).text,
        }
        for tag, body in bodies.items():
            for host in _EXTERNAL_HOSTS:
                assert host not in body, f"{which} {tag} 에 외부 참조({host}) 잔존"
