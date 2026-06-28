"""헬스/부하 기반 executor 선택(Phase 1: P2C + failover order).

HA(다중 coordinator)에서는 여러 coordinator 가 **독립적으로**, 공유되지만 약간 stale 한
부하 뷰를 보고 동시에 결정한다. 이런 분산 환경에서 단순 "least-loaded" 는 모두 같은
'가장 한가한' 노드로 몰리는 **스탬피드**를 일으키므로, 기본 정책은
**Power-of-Two-Choices(P2C)** 다(무작위 2개 중 덜 바쁜 쪽 선택 → 결정이 탈상관되어 herding
억제). 무상태·무락이라 HA 친화적이다.

구성:
- ``SharedLoadView`` : executor_url 을 키로 하는 헬스/부하 스냅샷 어댑터(monitor.snapshot 등).
- ``ExecutorSelector`` : 후보 URL 목록을 정책(round_robin/least_loaded/p2c)에 따라 **시도 순서**로
  정렬하는 순수 로직(DB 없이 단위 테스트 가능). 데이터가 없거나 살아있는 후보가 없으면
  안전하게 round_robin 으로 폴백한다.
"""

from __future__ import annotations

import random
from typing import Callable, Optional


class SharedLoadView:
    """executor 부하/헬스 스냅샷을 ``executor_url`` 키 dict 로 제공하는 어댑터.

    ``provider()`` 는 각 executor 의 헬스 dict(최소 ``executor_url``·``healthy``·
    ``active_tasks``·``max_concurrent_tasks``)를 담은 리스트를 돌려준다. 보통
    ``HealthMonitor.snapshot`` 을 주입한다(URL 키라 디스패치 대상과 바로 매칭됨).
    조회 실패는 빈 뷰로 처리해 선택기가 round_robin 으로 폴백하게 한다.
    """

    def __init__(self, provider: Callable[[], list]):
        self._provider = provider

    def by_url(self) -> dict:
        try:
            items = self._provider() or []
        except Exception:
            return {}
        out: dict = {}
        for it in items:
            url = it.get("executor_url")
            if url:
                out[url] = it
        return out


def _remaining(load: dict) -> float:
    """잔여 용량(클수록 한가). ``max_concurrent_tasks`` 가 0/None(무제한)이면
    ``-active_tasks`` 로 두어 active 가 적을수록 우선되게 한다."""
    active = load.get("active_tasks") or 0
    mx = load.get("max_concurrent_tasks") or 0
    if mx and mx > 0:
        return mx - active
    return -active


class ExecutorSelector:
    """후보 executor URL 들을 정책에 따라 **시도 순서**로 정렬한다(순수 로직).

    policy:
      - ``round_robin`` : 호출마다 시작점을 1칸 회전(기존 동작과 동일한 균등 분산).
      - ``least_loaded`` : 살아있는 후보를 잔여 용량 큰 순으로(가장 한가한 노드 먼저).
      - ``p2c`` : 살아있는 후보 중 무작위 2개를 뽑아 덜 바쁜 쪽을 1순위로(HA 권장).

    살아있는(healthy) 후보가 없거나 부하 뷰가 비면 round_robin 으로 폴백한다.
    healthy 가 아닌(unknown/unhealthy) 후보는 failover 안전을 위해 **맨 뒤**에 남긴다.
    """

    def __init__(self, policy: str = "round_robin", rng: Optional[random.Random] = None):
        self.policy = policy
        self._rng = rng or random.Random()
        self._rr = 0

    def order(self, candidates: list, view: dict) -> list:
        cand = [c for c in candidates if c]
        if not cand:
            return []
        if self.policy == "round_robin" or not view:
            return self._round_robin(cand)

        alive = [c for c in cand if view.get(c, {}).get("healthy")]
        rest = [c for c in cand if c not in alive]  # unknown/unhealthy → failover 꼬리
        if not alive:
            return self._round_robin(cand)  # 헬스 정보상 살아있는 후보가 없으면 폴백

        if self.policy == "p2c":
            first = self._p2c_first(alive, view)
            tail = sorted(
                [c for c in alive if c != first],
                key=lambda c: _remaining(view[c]), reverse=True,
            )
            ordered_alive = [first] + tail
        else:  # least_loaded
            ordered_alive = sorted(
                alive, key=lambda c: _remaining(view[c]), reverse=True
            )
        return ordered_alive + rest

    def _round_robin(self, cand: list) -> list:
        n = len(cand)
        start = self._rr % n
        self._rr = (self._rr + 1) % n
        return cand[start:] + cand[:start]

    def _p2c_first(self, alive: list, view: dict) -> str:
        """살아있는 후보 중 무작위 2개를 뽑아 잔여 용량이 큰(덜 바쁜) 쪽을 고른다."""
        if len(alive) == 1:
            return alive[0]
        a, b = self._rng.sample(alive, 2)
        return a if _remaining(view[a]) >= _remaining(view[b]) else b
