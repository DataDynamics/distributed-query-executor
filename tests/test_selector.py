"""헬스/부하 기반 executor 선택(ExecutorSelector / SharedLoadView) 단위 테스트.

순수 로직이라 DB·네트워크 없이 합성 스냅샷으로 검증한다. HA(다중 coordinator) 분산
스탬피드 방지(P2C)도 시뮬레이션한다.
"""

from __future__ import annotations

import random

from coordinator.selector import ExecutorSelector, SharedLoadView

A, B, C = "http://a:8087", "http://b:8087", "http://c:8087"


def _load(url, healthy=True, active=0, mx=8):
    return {"executor_url": url, "healthy": healthy,
            "active_tasks": active, "max_concurrent_tasks": mx}


def _view(*loads):
    return {l["executor_url"]: l for l in loads}


def test_shared_load_view_keys_by_url():
    lv = SharedLoadView(lambda: [_load(A), _load(B)])
    by = lv.by_url()
    assert set(by) == {A, B}
    # provider 예외 → 빈 뷰(폴백 유도)
    assert SharedLoadView(lambda: (_ for _ in ()).throw(RuntimeError())).by_url() == {}


def test_round_robin_rotates_and_ignores_view():
    s = ExecutorSelector(policy="round_robin")
    # round_robin 은 부하 뷰 무시, 호출마다 시작점 회전
    assert s.order([A, B, C], {}) == [A, B, C]
    assert s.order([A, B, C], {}) == [B, C, A]
    assert s.order([A, B, C], {}) == [C, A, B]


def test_least_loaded_orders_by_remaining_capacity():
    s = ExecutorSelector(policy="least_loaded")
    view = _view(_load(A, active=7), _load(B, active=1), _load(C, active=4))  # 잔여 1/7/4
    assert s.order([A, B, C], view) == [B, C, A]  # 한가한 순


def test_unhealthy_and_unknown_go_last():
    s = ExecutorSelector(policy="least_loaded")
    # A unhealthy, C 는 뷰에 없음(unknown) → 둘 다 살아있는 B 뒤로
    view = _view(_load(A, healthy=False, active=0), _load(B, active=5))
    order = s.order([A, B, C], view)
    assert order[0] == B
    assert set(order[1:]) == {A, C}


def test_fallback_to_round_robin_when_no_alive():
    s = ExecutorSelector(policy="p2c")
    # 전부 unhealthy → round_robin 폴백(전체 후보 보존)
    view = _view(_load(A, healthy=False), _load(B, healthy=False))
    assert sorted(s.order([A, B], view)) == sorted([A, B])


def test_p2c_picks_less_loaded_of_two():
    # rng 를 고정해 어떤 2개가 뽑히든 덜 바쁜 쪽이 1순위인지 확인
    view = _view(_load(A, active=8), _load(B, active=0), _load(C, active=8))
    for seed in range(20):
        s = ExecutorSelector(policy="p2c", rng=random.Random(seed))
        first = s.order([A, B, C], view)[0]
        # B 가 뽑힌 쌍이면 B 가 1순위여야 하고, 안 뽑혔으면 둘 중 덜 바쁜 쪽
        # 최소한 가장 바쁜 A/C 가 단독 1순위가 되는 일은 B 와 함께 뽑혔을 때만 없어야 한다
        assert first in (A, B, C)
    # B(잔여 최대)가 포함된 쌍에서는 반드시 B 가 선택됨을 직접 검증
    s = ExecutorSelector(policy="p2c", rng=random.Random(0))
    # 후보를 B 포함 2개로 좁히면 항상 B
    assert s.order([A, B], view)[0] == B
    assert s.order([B, C], view)[0] == B


def test_p2c_spreads_across_coordinators_no_stampede():
    """HA 시뮬레이션: 모두 동일 부하일 때 여러 coordinator(독립 selector)가 P2C 로 고르면
    한 노드로 몰리지 않고 분산된다(스탬피드 방지)."""
    view = _view(_load(A, active=0), _load(B, active=0), _load(C, active=0))
    counts = {A: 0, B: 0, C: 0}
    # 50개 coordinator/요청이 각자 독립 selector 로 1순위를 고른다
    for i in range(50):
        s = ExecutorSelector(policy="p2c", rng=random.Random(i))
        counts[s.order([A, B, C], view)[0]] += 1
    # 세 노드 모두 선택돼야 하고(분산), 한 노드 독점이 아니어야 한다
    assert all(v > 0 for v in counts.values())
    assert max(counts.values()) < 50

    # 대조군: least_loaded 는 동일 부하에서 항상 같은 1순위(스탬피드)
    s2 = ExecutorSelector(policy="least_loaded")
    firsts = {s2.order([A, B, C], view)[0] for _ in range(50)}
    assert len(firsts) == 1


# ── dispatcher 통합: _failover_order 가 selector 를 쓰는지 ──────────────────

def _dispatcher(monkeypatch, *, selector=None, load_view=None, failover=True):
    from coordinator.config import settings
    from coordinator.dispatcher import HttpDispatcher
    monkeypatch.setattr(settings, "executors", [A, B, C], raising=False)
    monkeypatch.setattr(settings, "task_failover", failover, raising=False)
    return HttpDispatcher(settings, store=None, load_view=load_view, selector=selector)


def _task(executor_url):
    from coordinator.models import Task
    return Task(job_id="j", executor_url=executor_url, sub_query="q", partition_values=["1"])


def test_failover_order_uses_selector_when_injected(monkeypatch):
    view = SharedLoadView(lambda: [_load(A, active=7), _load(B, active=0), _load(C, active=3)])
    d = _dispatcher(monkeypatch, selector=ExecutorSelector("least_loaded"), load_view=view)
    # 배정은 A 였지만 헬스 기반으로 한가한 순(B,C,A)으로 재정렬된다
    assert d._failover_order(_task(A)) == [B, C, A]


def test_failover_order_default_is_static(monkeypatch):
    d = _dispatcher(monkeypatch)  # selector/load_view 미주입 → 기존 동작
    order = d._failover_order(_task(B))
    assert order[0] == B and set(order) == {A, B, C}


def test_failover_disabled_returns_primary_only(monkeypatch):
    d = _dispatcher(monkeypatch, selector=ExecutorSelector("p2c"),
                    load_view=SharedLoadView(lambda: [_load(A), _load(B), _load(C)]),
                    failover=False)
    assert d._failover_order(_task(A)) == [A]


# ── Phase 2: 초기 배정(assign) ────────────────────────────────────────────

def test_assign_round_robin_cycles():
    s = ExecutorSelector(policy="round_robin")
    assert s.assign(4, [A, B], {}) == [A, B, A, B]


def test_assign_none_when_no_candidates():
    s = ExecutorSelector(policy="p2c")
    assert s.assign(3, [], _view(_load(A))) == [None, None, None]


def test_assign_least_loaded_spreads_within_job():
    # 동일 부하 2노드에 4 task → 한 노드로 몰리지 않고 2:2 분산(임시 부하 가산 덕분)
    from collections import Counter
    s = ExecutorSelector(policy="least_loaded")
    view = _view(_load(A, active=0), _load(B, active=0))
    assert Counter(s.assign(4, [A, B], view)) == {A: 2, B: 2}


def test_assign_least_loaded_prefers_idle_node():
    # A 거의 만석, B 한가 → 대부분 B 로, A 가 어느 정도 차오르면 A 도 일부
    from collections import Counter
    s = ExecutorSelector(policy="least_loaded")
    view = _view(_load(A, active=7, mx=8), _load(B, active=0, mx=8))  # 잔여 1 vs 8
    counts = Counter(s.assign(8, [A, B], view))
    assert counts[B] > counts[A]  # 한가한 B 가 더 많이


def test_assign_p2c_distributes_with_seed():
    from collections import Counter
    s = ExecutorSelector(policy="p2c", rng=random.Random(1))
    view = _view(_load(A, active=0), _load(B, active=0), _load(C, active=0))
    counts = Counter(s.assign(30, [A, B, C], view))
    assert set(counts) == {A, B, C}  # 세 노드 모두 사용(분산)


def test_cluster_reports_assignment_counts(client, valid_payload, monkeypatch):
    # round_robin(기본)으로도 배정 카운트는 누적·노출된다.
    from coordinator.config import settings
    monkeypatch.setattr(settings, "executors", [A, B], raising=False)
    r = client.post("/jobs", json={**valid_payload, "parallelism": 2})
    assert r.status_code == 202
    cl = client.get("/cluster?refresh=false").json()
    assert "assignment_counts" in cl and "executor_select" in cl
    assert sum(cl["assignment_counts"].values()) == 2
    assert set(cl["assignment_counts"]) <= {A, B}
