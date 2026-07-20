"""보완한 로깅의 동작을 검증한다 — 단순 로그 한 줄이 아니라 **조건 로직**이 있는 부분만.

* monitor 의 상태 전이 기반 로깅(up↔down 순간에만 WARNING/INFO, 반복 실패는 DEBUG).
* coordinator 검증 실패(422) 시 서버 로그에 사유가 남는지(진단 사각지대 해소).
"""

from __future__ import annotations

import logging

from coordinator.monitor import HealthMonitor


class _MonitorSettings:
    executors = ["http://exec"]
    monitor_enabled = True
    monitor_health_interval_s = 10
    monitor_record_interval_s = 60
    monitor_db_dsn = ""
    monitor_table = "executor_health_metrics"


class _OkResp:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "cpu_percent": 1.0,
            "memory": {"percent": 2.0, "used_mb": 1, "total_mb": 2},
            "disk": {"percent": 3.0, "used_gb": 1, "total_gb": 2},
            "tasks": {"active": 0, "max": 8},
        }


class _SeqClient:
    """_poll_one 이 호출하는 async client.get 대역. fail=True 면 예외를 던진다."""

    def __init__(self, fail: bool):
        self.fail = fail

    async def get(self, url):
        if self.fail:
            raise RuntimeError("connection refused")
        return _OkResp()


def _msgs(caplog):
    return [r.getMessage() for r in caplog.records]


async def test_monitor_logs_only_on_state_transitions(caplog):
    monitor = HealthMonitor(_MonitorSettings())
    url = "http://exec"

    # 1) 첫 폴링 실패 → WARNING "다운 감지"(첫 관측은 알린다).
    with caplog.at_level(logging.DEBUG, logger="coordinator.monitor"):
        await monitor._poll_one(_SeqClient(fail=True), url)
    assert any("다운 감지" in m for m in _msgs(caplog))
    assert monitor.executors[url].healthy is False
    caplog.clear()

    # 2) 반복 실패 → WARNING 없음(잡음 억제), DEBUG "여전히 다운".
    with caplog.at_level(logging.DEBUG, logger="coordinator.monitor"):
        await monitor._poll_one(_SeqClient(fail=True), url)
    assert not any(r.levelno == logging.WARNING for r in caplog.records)
    assert any("여전히 다운" in m for m in _msgs(caplog))
    caplog.clear()

    # 3) 복구(성공) → INFO "복구됨".
    with caplog.at_level(logging.DEBUG, logger="coordinator.monitor"):
        await monitor._poll_one(_SeqClient(fail=False), url)
    assert any("복구됨" in m for m in _msgs(caplog))
    assert monitor.executors[url].healthy is True


async def test_monitor_first_success_is_quiet(caplog):
    # 첫 폴링이 성공이면 "복구됨"(down→up 전이)이 아니므로 조용해야 한다(스퓨리어스 방지).
    monitor = HealthMonitor(_MonitorSettings())
    with caplog.at_level(logging.DEBUG, logger="coordinator.monitor"):
        await monitor._poll_one(_SeqClient(fail=False), "http://exec")
    assert not any("복구됨" in m for m in _msgs(caplog))


def test_validation_failure_is_logged(client, caplog):
    # 422 로 거부되는 요청은 서버 로그에도 사유가 남아야 한다(진단 사각지대 해소).
    with caplog.at_level(logging.WARNING, logger="coordinator.app"):
        r = client.post("/jobs", json={
            "sql": "SELECT count(*) FROM t WHERE dt IN ('1')",
            "partition_column": "dt",
            "target_table": "public.t",
        })
    assert r.status_code == 422
    msgs = _msgs(caplog)
    assert any("검증 실패" in m for m in msgs)
    # error_code 도 로그에 포함(분기 원인 파악).
    assert any("UNSUPPORTED_AGGREGATE" in m for m in msgs)
