"""실행 이력 검색 필터(status/username/job_id) 테스트.

실제 PostgreSQL 없이, ``read()`` 내부의 지연 임포트(``import psycopg``)를 가짜 모듈로
바꿔치기해 저장소가 조립하는 SQL/바인드 파라미터를 검증한다. 확인 항목:

- 필터는 DISTINCT ON 파생 테이블 **바깥**(job/task 별 최신 상태 기준)에 WHERE 로 걸린다.
- 값은 전부 바인드 파라미터로 전달된다(SQL 주입 차단). status 는 대문자 정규화,
  job_id 는 전방일치(prefix LIKE).
- total(count) 쿼리에도 같은 필터가 적용된다(페이저 어긋남 방지).
- 필터 미지정 시 WHERE 가 붙지 않는다(기존 동작 유지).
"""

from __future__ import annotations

import sys
import types

import pytest

from coordinator.history import JobHistoryRepository
from executor.history import TaskHistoryRepository


class _FakeCursor:
    """execute 된 (sql, params) 를 기록만 하는 가짜 커서."""

    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append((sql, params))

    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, log):
        self._log = log

    def cursor(self):
        return _FakeCursor(self._log)

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture()
def fake_psycopg(monkeypatch):
    """sys.modules 의 psycopg 를 가짜로 바꿔 read() 의 지연 임포트를 가로챈다."""
    log: list[tuple[str, tuple]] = []
    mod = types.ModuleType("psycopg")
    mod.connect = lambda dsn: _FakeConn(log)
    monkeypatch.setitem(sys.modules, "psycopg", mod)
    return log


def _settings(**kw):
    base = {"history_db_dsn": "postgresql://u:p@h/db"}
    base.update(kw)
    return types.SimpleNamespace(**base)


def test_job_history_filters_applied_outside_distinct_on(fake_psycopg):
    repo = JobHistoryRepository(_settings(history_table="public.job_history"))
    out = repo.read(limit=10, offset=5, status="failed", username="kim", job_id="job-1")
    assert out["enabled"] is True
    count_sql, count_params = fake_psycopg[0]
    page_sql, page_params = fake_psycopg[1]
    # 필터는 파생 테이블(t) 바깥에 걸린다 — WHERE 가 서브쿼리 닫는 괄호 뒤에 있어야 한다
    for sql in (count_sql, page_sql):
        assert ") t WHERE " in sql
        assert "status = %s" in sql and "username = %s" in sql and "job_id LIKE %s" in sql
    # 값은 바인드 파라미터로: status 대문자 정규화 + job_id 전방일치
    assert count_params == ("FAILED", "kim", "job-1%")
    assert page_params == ("FAILED", "kim", "job-1%", 10, 5)


def test_job_history_no_filters_keeps_plain_query(fake_psycopg):
    repo = JobHistoryRepository(_settings(history_table="public.job_history"))
    repo.read(limit=20, offset=0)
    count_sql, count_params = fake_psycopg[0]
    page_sql, page_params = fake_psycopg[1]
    assert "WHERE" not in count_sql.split(") t")[1]  # 파생 테이블 바깥에 WHERE 없음
    assert count_params == ()
    assert page_params == (20, 0)


def test_task_history_filters_keep_executor_scope(fake_psycopg):
    repo = TaskHistoryRepository(_settings(task_history_table="public.task_history"))
    repo.read(limit=7, offset=3, status="done", job_id="job-9")
    count_sql, count_params = fake_psycopg[0]
    page_sql, page_params = fake_psycopg[1]
    # executor_id 스코프(파생 테이블 안) + 검색 필터(바깥) 둘 다 유지돼야 한다
    for sql in (count_sql, page_sql):
        assert "WHERE executor_id = %s" in sql
        assert ") t WHERE " in sql and "status = %s" in sql and "job_id LIKE %s" in sql
        assert "username" not in sql.split(") t")[1]  # 미지정 필터는 빠짐
    assert count_params == (repo.executor_id, "DONE", "job-9%")
    assert page_params == (repo.executor_id, "DONE", "job-9%", 7, 3)


def test_history_endpoint_accepts_filter_params(client):
    """이력 비활성(기본 설정) 상태에서도 필터 파라미터가 422 없이 수용돼야 한다."""
    body = client.get("/history?status=FAILED&username=kim&job_id=abc").json()
    assert body["enabled"] is False
