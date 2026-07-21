"""trino_runner 의 대화형 login() 비대화형 처리(_login_noninteractive) 검증.

사내 인증 모듈의 login() 이 input()/getpass()/sys.stdin 으로 자격증명을 묻는 경우,
executor(터미널 없는 데몬) 안에서 config 값으로 입력을 대신 공급해 1회 호출·캐시하는지,
그리고 전역 패치(builtins.input 등)가 호출 후 반드시 원복되는지를 본다.
"""

import builtins
import getpass
import logging
import sys
import types

import pytest

import customs.query_funcs.trino_runner as tr

CONFIG = {"login_module": "fake_auth:login", "user": "svc-user", "password": "svc-pass"}


@pytest.fixture(autouse=True)
def fake_auth_module(monkeypatch):
    """input()/getpass()를 쓰는 가짜 사내 login 모듈을 fake_auth 로 주입하고 캐시를 초기화한다."""
    mod = types.ModuleType("fake_auth")
    mod.calls = []

    def login():
        # 실제 사내 모듈처럼 표준 입력으로 사용자/비밀번호를 묻는다.
        user = input("Username: ")
        password = getpass.getpass("Password: ")
        mod.calls.append((user, password))
        return {"token": f"{user}/{password}"}

    mod.login = login
    monkeypatch.setitem(sys.modules, "fake_auth", mod)
    monkeypatch.setattr(tr, "_login_session", None)
    yield mod


def test_login_은_config_자격증명을_입력으로_공급한다(fake_auth_module):
    session = tr._login_noninteractive(CONFIG)
    assert fake_auth_module.calls == [("svc-user", "svc-pass")]
    assert session == {"token": "svc-user/svc-pass"}


def test_login_결과는_캐시되어_재호출하지_않는다(fake_auth_module):
    first = tr._login_noninteractive(CONFIG)
    second = tr._login_noninteractive(CONFIG)
    assert first is second
    assert len(fake_auth_module.calls) == 1


def test_호출_후_전역_입력_함수들이_원복된다(fake_auth_module):
    orig = (builtins.input, getpass.getpass, sys.stdin)
    tr._login_noninteractive(CONFIG)
    assert (builtins.input, getpass.getpass, sys.stdin) == orig


def test_login_module_미설정이면_아무것도_하지_않는다(fake_auth_module):
    orig = (builtins.input, getpass.getpass, sys.stdin)
    assert tr._login_noninteractive({"user": "u", "password": "p"}) is None
    assert fake_auth_module.calls == []
    assert (builtins.input, getpass.getpass, sys.stdin) == orig


def test_login_실패_시_원복되고_캐시되지_않아_재시도된다(fake_auth_module, caplog):
    orig = (builtins.input, getpass.getpass, sys.stdin)
    boom = {"n": 0}

    def flaky_login():
        boom["n"] += 1
        if boom["n"] == 1:
            raise RuntimeError("인증 서버 일시 오류")
        return {"token": "ok"}

    fake_auth_module.login = flaky_login

    with pytest.raises(RuntimeError), caplog.at_level(logging.ERROR):
        tr._login_noninteractive(CONFIG)
    # 실패해도 전역 패치는 원복되고, 세션은 캐시되지 않는다.
    assert (builtins.input, getpass.getpass, sys.stdin) == orig
    assert tr._login_session is None
    # 실패는 스택 트레이스와 함께 로그에 남는다(executor 로그 파일에서 확인 가능).
    failed = [r for r in caplog.records if "커스텀 login 실패" in r.getMessage()]
    assert failed and failed[0].exc_info is not None
    assert "인증 서버 일시 오류" in caplog.text

    # 다음 호출에서 재시도되어 성공한다.
    assert tr._login_noninteractive(CONFIG) == {"token": "ok"}
    assert boom["n"] == 2


def test_login_성공도_로그에_남는다(fake_auth_module, caplog):
    with caplog.at_level(logging.INFO):
        tr._login_noninteractive(CONFIG)
    messages = [r.getMessage() for r in caplog.records]
    assert any("커스텀 login 호출" in m for m in messages)
    assert any("커스텀 login 성공" in m for m in messages)
    # 비밀번호는 로그에 남지 않는다(사용자명은 추적용으로 허용).
    assert "svc-pass" not in caplog.text


def test_sys_stdin_readline_로_읽는_구현도_동작한다(fake_auth_module):
    def stdin_login():
        user = sys.stdin.readline().strip()
        password = sys.stdin.readline().strip()
        fake_auth_module.calls.append((user, password))
        return {"token": "stdin"}

    fake_auth_module.login = stdin_login
    assert tr._login_noninteractive(CONFIG) == {"token": "stdin"}
    assert fake_auth_module.calls == [("svc-user", "svc-pass")]
