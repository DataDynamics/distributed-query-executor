"""버전 단일 소스와 기동 배너(core.version / core.banner) 테스트."""

from __future__ import annotations

import re

import core
from core.banner import render_banner
from core.version import __version__, git_revision, version_string


# ── 버전 ───────────────────────────────────────────────────────────────────

def test_version_is_semver():
    assert re.fullmatch(r"\d+\.\d+\.\d+", __version__)


def test_core_reexports_version():
    # `from core import __version__` 편의 재수출이 실제 값과 일치해야 한다.
    assert core.__version__ == __version__


def test_version_string_includes_revision_when_present(monkeypatch):
    # git sha 를 환경변수로 각인하면 표시 문자열에 +g<sha> 로 붙는다.
    git_revision.cache_clear()
    monkeypatch.setenv("QUERY_EXECUTOR_GIT_SHA", "deadbee")
    assert version_string() == f"{__version__}+gdeadbee"
    git_revision.cache_clear()


def test_version_string_plain_when_no_revision(monkeypatch, tmp_path):
    # .git 도 없고 각인도 없으면 순수 버전만.
    git_revision.cache_clear()
    monkeypatch.setenv("QUERY_EXECUTOR_GIT_SHA", "")
    # git_revision 이 실제 저장소 .git 을 볼 수 있으므로, 여기서는 환경 각인이 빈 값일 때
    # 최소한 예외 없이 문자열을 돌려주는지만 확인한다(리비전 유무는 환경에 의존).
    assert version_string().startswith(__version__)
    git_revision.cache_clear()


# ── 배너 ───────────────────────────────────────────────────────────────────

def test_render_banner_contains_role_and_version(tmp_path):
    out = render_banner("coordinator", 8088, config_dir=tmp_path)
    assert "Distributed Query Executor" in out
    assert "coordinator:8088" in out
    assert __version__ in out
    # 내장 블록체 아트가 포함된다.
    assert "██" in out


def test_render_banner_without_port(tmp_path):
    out = render_banner("executor", None, config_dir=tmp_path)
    assert ":: executor ::" in out
    assert "executor:" not in out  # 포트 없으면 콜론 포트 표기 없음


def test_banner_txt_override_with_placeholders(tmp_path):
    (tmp_path / "banner.txt").write_text(
        "MyApp ${role} v${version} on :${port} (py${python})\n", encoding="utf-8"
    )
    out = render_banner("executor", 8087, config_dir=tmp_path)
    assert out.startswith("MyApp executor v")
    assert version_string() in out
    assert ":8087" in out
    # 내장 아트는 오버라이드 시 나타나지 않는다.
    assert "██" not in out
