"""메타 저장소 보관 정책(coordinator/retention.py)을 실제 DB 없이 검증한다."""

from __future__ import annotations

import types

import pytest

from coordinator import retention
from coordinator.retention import RetentionCleaner, RetentionTarget, build_retention_cleaner, build_targets


def _settings(**kw):
    base = dict(
        history_db_dsn="postgresql://h/meta", monitor_db_dsn="postgresql://h/mon",
        history_table="public.job_history", task_history_table="public.task_history",
        monitor_table="public.executor_health_metrics", store_table="public.jobs",
        store_backend="postgres",
        history_retention_days=90, monitor_retention_days=30, store_retention_days=0,
        retention_interval_s=3600,
    )
    base.update(kw)
    return types.SimpleNamespace(**base)


def test_기본_설정은_이력과_메트릭만_정리하고_jobs_는_남긴다():
    targets = {t.name: t for t in build_targets(_settings())}
    assert sorted(targets) == ["executor_health_metrics", "job_history", "task_history"]
    assert targets["job_history"].days == 90 and targets["job_history"].dsn == "postgresql://h/meta"
    assert targets["executor_health_metrics"].days == 30
    assert targets["executor_health_metrics"].dsn == "postgresql://h/mon"


def test_보관_일수가_0_이거나_DSN_이_없으면_그_대상을_뺀다():
    assert [t.name for t in build_targets(_settings(history_retention_days=0))] == ["executor_health_metrics"]
    assert [t.name for t in build_targets(_settings(monitor_db_dsn=""))] == ["job_history", "task_history"]
    assert build_targets(_settings(history_db_dsn="", monitor_db_dsn="")) == []


def test_jobs_정리는_postgres_store_에서_종료된_job_만_고른다():
    targets = {t.name: t for t in build_targets(_settings(store_retention_days=14))}
    jobs = targets["jobs"]
    assert jobs.key_col == "job_id" and jobs.time_col == "updated_at"
    assert "status IN (%s, %s, %s, %s)" in jobs.delete_sql()
    assert jobs.delete_params(100) == (14, "DONE", "PARTIAL", "FAILED", "CANCELLED", 100)
    # 공유 store 가 아니면 jobs 테이블이 없으므로 켜도 무시한다.
    assert "jobs" not in {t.name for t in build_targets(_settings(store_retention_days=14, store_backend="memory"))}


def test_삭제문은_KST_기준_보관기간과_배치_상한을_건다():
    t = RetentionTarget("job_history", "dsn", "public.job_history", "id", "recorded_at", 90)
    assert t.delete_sql() == (
        "DELETE FROM public.job_history WHERE id = ANY(ARRAY(SELECT id FROM public.job_history "
        "WHERE recorded_at < (now() AT TIME ZONE 'Asia/Seoul') - make_interval(days => %s) LIMIT %s))"
    )
    assert t.delete_params(5000) == (90, 5000)


def test_정리기는_주기가_0_이거나_대상이_없으면_만들지_않는다():
    assert build_retention_cleaner(_settings(retention_interval_s=0)) is None
    assert build_retention_cleaner(_settings(history_db_dsn="", monitor_db_dsn="")) is None
    assert isinstance(build_retention_cleaner(_settings()), RetentionCleaner)


class _Cur:
    def __init__(self, log, counts):
        self.log, self.counts = log, counts
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        self.log.append(params)
        self.rowcount = self.counts.pop(0)


class _Conn:
    def __init__(self, log, counts):
        self.log, self.counts = log, counts
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _Cur(self.log, self.counts)

    def commit(self):
        self.log.append("commit")


async def test_배치가_꽉_차면_다음_배치를_이어서_지운다(monkeypatch):
    log: list = []
    counts = [3, 3, 1]  # 배치 3 기준: 꽉 찬 두 번 뒤 덜 찬 한 번에서 멈춘다
    monkeypatch.setattr(retention, "meta_connect", lambda dsn: _Conn(log, counts))
    t = RetentionTarget("job_history", "dsn", "public.job_history", "id", "recorded_at", 90)
    assert await retention.purge(t, batch=3) == 7
    assert log == [(90, 3), "commit", (90, 3), "commit", (90, 3), "commit"]  # 배치마다 따로 커밋


async def test_한_테이블이_실패해도_다른_테이블은_정리한다(monkeypatch, caplog):
    calls = []

    async def fake_purge(target, batch=retention._BATCH):
        calls.append(target.name)
        if target.name == "job_history":
            raise RuntimeError("db down")
        return 5

    monkeypatch.setattr(retention, "purge", fake_purge)
    cleaner = build_retention_cleaner(_settings())
    result = await cleaner.run_once()
    assert calls == ["job_history", "task_history", "executor_health_metrics"]
    assert result == {"task_history": 5, "executor_health_metrics": 5}
    assert any("보관 기간 정리 실패" in r.getMessage() for r in caplog.records)


async def test_정리기는_멈추면_루프를_정리한다(monkeypatch):
    ran = []

    async def fake_run_once(self):
        ran.append(1)
        return {}

    monkeypatch.setattr(RetentionCleaner, "run_once", fake_run_once)
    cleaner = RetentionCleaner([], interval_s=3600, initial_delay_s=0)
    cleaner.start()
    for _ in range(5):
        await __import__("asyncio").sleep(0)
    await cleaner.stop()
    assert ran == [1] and cleaner._task is None


def test_설정_파일의_기본값():
    from core.config import settings

    assert settings.history_retention_days >= 0 and settings.monitor_retention_days >= 0
    assert settings.store_retention_days == 0  # 종료된 job 삭제는 명시적으로 켤 때만
    assert settings.retention_interval_s == 3600


@pytest.mark.parametrize("key", [
    "history.retention_days", "history.retention_interval_s", "monitor.retention_days", "store.retention_days",
])
def test_보관_설정에는_도움말이_있다(key):
    from core.config_help import FIELD_HELP

    assert key in FIELD_HELP
