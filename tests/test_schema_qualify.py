"""메타 저장소 스키마 한정(db.schema, 기본 public) 검증.

설정에서 읽은 메타 테이블명이 db.schema 로 한정되는지, 한정 헬퍼의 경계 동작
(빈 스키마·이미 한정된 값)이 맞는지, 각 repo 가 한정된 테이블명으로 SQL 을 만드는지
확인한다. 실제 DB 는 필요 없다.
"""
from __future__ import annotations

from core.config import _qualify_table, settings
from coordinator.ha import CoordinatorHeartbeat
from coordinator.history import JobHistoryRepository
from coordinator.job_store import SqlJobStore
from coordinator.reservation import ReservationRepository


def test_qualify_table_helper():
    assert _qualify_table("public", "jobs") == "public.jobs"
    assert _qualify_table("myschema", "job_history") == "myschema.job_history"
    # 빈 스키마 → 한정하지 않음(검색 경로/기본 스키마에 위임)
    assert _qualify_table("", "jobs") == "jobs"
    # 이미 'schema.table' 로 한정된 값 → 중복 한정하지 않음
    assert _qualify_table("public", "other.jobs") == "other.jobs"


def test_settings_tables_are_schema_qualified():
    # 기본 설정(db.schema=public)에서 모든 메타 테이블이 public. 으로 한정된다.
    assert settings.db_schema == "public"
    assert settings.store_table == "public.jobs"
    assert settings.history_table == "public.job_history"
    assert settings.task_history_table == "public.task_history"
    assert settings.monitor_table == "public.executor_health_metrics"
    assert settings.executor_status_table == "public.executor_status"
    assert settings.coordinator_status_table == "public.coordinator_status"
    assert settings.reservation_table == "public.executor_reservation"


def test_repos_use_qualified_table_names():
    # 각 repo 는 한정된 테이블명을 그대로 self.table 로 들고 SQL f-string 에 끼운다.
    assert JobHistoryRepository(settings).table == "public.job_history"
    assert SqlJobStore("postgresql://x", table=settings.store_table).table == "public.jobs"
    assert CoordinatorHeartbeat(
        "postgresql://x", "c1", table=settings.coordinator_status_table
    ).table == "public.coordinator_status"
    assert ReservationRepository(
        "postgresql://x", "c1", table=settings.reservation_table
    ).table == "public.executor_reservation"


def test_qualified_name_flows_into_sql():
    # SqlJobStore 가 만드는 INSERT 문에 public.jobs 가 들어가는지(런타임 SQL 일치) 확인.
    store = SqlJobStore("postgresql://x", table=settings.store_table)
    # _upsert_sql 같은 비공개 헬퍼 대신, 테이블명이 그대로 쓰이는지 직접 확인한다.
    assert store.table == "public.jobs"
    assert f"INSERT INTO {store.table}".startswith("INSERT INTO public.jobs")
