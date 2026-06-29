"""Impala 인증 기본값(LDAP) 및 auth_mechanism 별 impala_dsn 구성 검증."""

from core.config import Settings
from executor.backend import build_backend


def _settings(**over):
    s = Settings()
    s.greenplum_dsn = "postgresql://u@h/db"  # 실제 백엔드 선택용
    s.impala_host = "impala.example"
    for k, v in over.items():
        setattr(s, k, v)
    return s


def test_default_auth_mechanism_is_ldap():
    assert Settings().impala_auth_mechanism == "LDAP"


def test_ldap_dsn_has_user_password_no_kerberos():
    s = _settings(impala_auth_mechanism="LDAP", impala_user="svc", impala_password="pw")
    dsn = build_backend(s).impala_dsn
    assert dsn["auth_mechanism"] == "LDAP"
    assert dsn["user"] == "svc" and dsn["password"] == "pw"
    assert "kerberos_service_name" not in dsn


def test_gssapi_dsn_has_kerberos_no_user_password():
    s = _settings(
        impala_auth_mechanism="GSSAPI",
        impala_kerberos_service_name="impala",
        impala_user="svc",
        impala_password="pw",
    )
    dsn = build_backend(s).impala_dsn
    assert dsn["auth_mechanism"] == "GSSAPI"
    assert dsn["kerberos_service_name"] == "impala"
    assert "user" not in dsn and "password" not in dsn
