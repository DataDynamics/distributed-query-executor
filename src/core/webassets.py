"""에어갭 환경을 위해 내장한 웹 정적 에셋을 서빙하고 오프라인 Swagger UI 와 ReDoc 을 등록한다.

이 프로젝트는 인터넷이 차단된 에어갭 환경에 배포된다. 따라서 런타임에 외부
CDN/폰트 서버에서 가져오던 리소스를 모두 저장소에 내장하고(``core/static/``)
앱이 직접 서빙한다. 내장 대상:

- Swagger UI(``/docs``)·ReDoc(``/redoc``) — FastAPI 기본값은 jsdelivr CDN 에서
  ``swagger-ui-bundle.js`` / ``swagger-ui.css`` / ``redoc.standalone.js`` 를 로드한다.
- 대시보드 폰트(Roboto Condensed) — 기존엔 Google Fonts(fonts.googleapis.com)에서
  로드. ``core/static/fonts.css`` + ``core/static/fonts/*.woff2`` 로 대체.

두 FastAPI 앱(coordinator·executor)이 동일하게 쓰도록 공용 헬퍼로 둔다. 사용법:

    app = FastAPI(..., docs_url=None, redoc_url=None)  # 기본(CDN) docs 비활성
    mount_static(app)            # /assets 에 core/static 마운트
    register_offline_docs(app)   # 내장 에셋으로 /docs·/redoc 재등록
"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

# 내장 에셋 디렉터리. 패키징 시 이 디렉터리가 함께 배포되어야 한다.
STATIC_DIR = Path(__file__).resolve().parent / "static"

# 앱이 정적 에셋을 노출하는 URL 접두사. fonts.css 내부 url(...) 과 docs 라우트가
# 이 경로를 가정하므로 바꾸려면 fonts.css 도 함께 고칠 것.
STATIC_MOUNT = "/assets"


def mount_static(app: FastAPI) -> None:
    """``core/static`` 을 ``/assets`` 로 마운트한다(폰트·swagger·redoc 에셋)."""
    app.mount(STATIC_MOUNT, StaticFiles(directory=str(STATIC_DIR)), name="assets")


def register_offline_docs(app: FastAPI) -> None:
    """내장 에셋만으로 동작하는 ``/docs``(Swagger UI)·``/redoc`` 를 등록한다.

    FastAPI 기본 docs 라우트는 외부 CDN 을 참조하므로, 앱 생성 시
    ``docs_url=None, redoc_url=None`` 로 끄고 이 함수로 대체한다. openapi 스키마
    경로(``app.openapi_url``)는 그대로 사용한다.
    """
    openapi_url = app.openapi_url or "/openapi.json"
    favicon = f"{STATIC_MOUNT}/favicon.png"

    @app.get("/docs", include_in_schema=False)
    async def swagger_ui_html() -> HTMLResponse:  # noqa: D401 (라우트 핸들러)
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=f"{app.title} - Swagger UI",
            swagger_js_url=f"{STATIC_MOUNT}/swagger-ui-bundle.js",
            swagger_css_url=f"{STATIC_MOUNT}/swagger-ui.css",
            swagger_favicon_url=favicon,
        )

    @app.get("/redoc", include_in_schema=False)
    async def redoc_html() -> HTMLResponse:  # noqa: D401 (라우트 핸들러)
        # with_google_fonts=False: ReDoc 기본 HTML 이 삽입하는 Google Fonts
        # <link> 를 제거해 외부 폰트 요청을 막는다.
        return get_redoc_html(
            openapi_url=openapi_url,
            title=f"{app.title} - ReDoc",
            redoc_js_url=f"{STATIC_MOUNT}/redoc.standalone.js",
            redoc_favicon_url=favicon,
            with_google_fonts=False,
        )
