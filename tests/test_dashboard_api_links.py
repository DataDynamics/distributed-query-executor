"""대시보드 API·링크 탭이 실제로 열리는 주소만 가리키는지 검증한다.

탭은 브라우저에서 /openapi.json 을 읽어 그리므로, 여기서는 탭 배선과 함께 공용 스크립트가 고정으로
걸어 둔 바로가기 경로가 두 앱의 스키마나 문서 라우트에 실제로 있는지를 본다. 라우트 이름이 바뀌어
링크가 죽는 회귀를 막으려는 것이다.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app as create_coordinator_app
from executor.app import create_app as create_executor_app

COMMON_JS = Path(__file__).resolve().parents[1] / "src" / "core" / "static" / "dashboard-common.js"


def _links(name: str) -> list[str]:
    """공용 스크립트의 링크 배열(API_DOC_LINKS·API_OPS_LINKS)에 적힌 경로를 꺼낸다."""
    js = COMMON_JS.read_text(encoding="utf-8")
    block = re.search(r"const %s = \[(.*?)\];" % name, js, re.S).group(1)
    return re.findall(r"path:'([^']+)'", block)


@pytest.fixture(params=["coordinator", "executor"])
def client(request):
    app = create_coordinator_app() if request.param == "coordinator" else create_executor_app()
    with TestClient(app) as c:
        yield c


def test_탭과_헤더_링크가_배선돼_있다(client):
    html = client.get("/").text
    assert 'data-tab="api"' in html and 'id="p-api"' in html and 'id="hdr-links"' in html
    assert "api:loadApi" in html and "initHeaderLinks()" in html


def test_문서_링크는_항상_열린다(client):
    for path in _links("API_DOC_LINKS"):
        assert client.get(path).status_code == 200, path


def test_운영_링크는_스키마에_있으면_열린다(client):
    paths = client.get("/openapi.json").json()["paths"]
    ops = _links("API_OPS_LINKS")
    present = [p for p in ops if p in paths and "get" in paths[p]]
    # 기본 설정에서 Prometheus·메트릭·헬스는 두 앱 모두에 있어야 한다.
    assert {"/metrics/prometheus", "/metrics", "/health", "/info", "/config"} <= set(present)
    for path in present:
        assert client.get(path).status_code < 500, path


def test_coordinator_는_executor_주소와_http_sd_를_보여_준다():
    with TestClient(create_coordinator_app()) as c:
        html = c.get("/").text
        assert "/metrics/prometheus" in html and "Executor 주소" in html
        assert "/prometheus/targets" in c.get("/openapi.json").json()["paths"]
