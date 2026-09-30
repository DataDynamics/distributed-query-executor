"""서버 자원을 주기적으로 수집한다. ``GET /cluster`` 한 번으로 coordinator 와 모든 executor 를 본다.

``/cluster`` 는 coordinator 자신의 CPU·메모리와, coordinator 가 알고 있는 executor 별 CPU·메모리·
실행 중 task 수를 한 응답에 담는다. 그래서 도구는 executor 주소를 따로 알 필요가 없다.
coordinator 가 여럿이면 각자의 ``/cluster`` 에서 자기 메트릭을 받고, executor 는 첫 coordinator
응답의 것을 쓴다(모두 같은 executor 를 보고 있으므로).

표본에는 수집 당시의 단계(phase)를 붙인다. ``baseline`` 은 부하를 걸기 전, ``load`` 는 VU 가 도는
동안, ``drain`` 은 제출을 멈추고 남은 job 을 기다리는 동안이다. 기준선이 있어야 "부하로 얼마나
올랐는지"를 볼 수 있다.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

from .client import CoordinatorClient
from .stats import summarize

logger = logging.getLogger(__name__)

PHASES = ("baseline", "load", "drain")


def _num(value: Any) -> Optional[float]:
    return float(value) if isinstance(value, (int, float)) else None


def extract_servers(cluster: Dict[str, Any], coord_name: str,
                    include_executors: bool = True) -> Dict[str, Dict[str, Any]]:
    """``/cluster`` 응답을 ``{서버이름: {cpu, mem, active_tasks, max_tasks, healthy}}`` 로 편다.

    executor 레코드는 모니터 폴링판과 self-report 판이 있는데 둘 다 ``cpu_percent``·
    ``memory_percent`` 를 평평하게 담으므로 같은 코드로 읽는다.
    """
    out: Dict[str, Dict[str, Any]] = {}
    coord = (cluster.get("coordinator") or {}).get("metrics") or {}
    out[coord_name] = {
        "role": "coordinator",
        "cpu": _num(coord.get("cpu_percent")),
        "mem": _num((coord.get("memory") or {}).get("percent")),
        "active_tasks": None,
        "max_tasks": None,
        "healthy": True,
    }
    if include_executors:
        for e in cluster.get("executors") or []:
            name = e.get("executor_url") or e.get("executor_id") or f"executor-{len(out)}"
            out[name] = {
                "role": "executor",
                "cpu": _num(e.get("cpu_percent")),
                "mem": _num(e.get("memory_percent")),
                "active_tasks": _num(e.get("active_tasks")),
                "max_tasks": _num(e.get("max_concurrent_tasks")),
                "healthy": bool(e.get("healthy")),
            }
    return out


class ResourceSampler:
    """``interval`` 초마다 ``/cluster`` 를 불러 표본을 쌓는다."""

    def __init__(self, client: CoordinatorClient, interval: float,
                 clock: Callable[[], float], refresh: bool = True) -> None:
        self.client = client
        self.interval = interval
        self.clock = clock          # 테스트 시작 기준 경과 초를 주는 함수
        self.refresh = refresh
        self.phase = "baseline"
        self.samples: List[Dict[str, Any]] = []
        self.errors = 0
        self.last_error: Optional[str] = None
        multi = len(client.base_urls) > 1
        self._names = {
            base: (f"coordinator@{base.split('://', 1)[-1]}" if multi else "coordinator")
            for base in client.base_urls
        }

    def latest(self) -> Optional[Dict[str, Any]]:
        return self.samples[-1] if self.samples else None

    async def sample_once(self) -> None:
        t = self.clock()
        servers: Dict[str, Dict[str, Any]] = {}
        jobs: Dict[str, Any] = {}
        for i, base in enumerate(self.client.base_urls):
            try:
                cluster = await self.client.cluster(base, refresh=self.refresh)
            except Exception as exc:  # 수집 실패는 부하 테스트를 멈출 이유가 아니다
                self.errors += 1
                self.last_error = f"{base}: {type(exc).__name__}: {exc}"
                logger.debug("자원 수집 실패 %s", self.last_error)
                continue
            servers.update(extract_servers(cluster, self._names[base], include_executors=(i == 0)
                                           or not servers))
            if not jobs:
                jobs = cluster.get("jobs") or {}
        if servers:
            self.samples.append({
                "t": t,
                "phase": self.phase,
                "servers": servers,
                "jobs_active": jobs.get("active"),
                "jobs_running": jobs.get("running"),
            })

    async def run(self, stop: asyncio.Event) -> None:
        """``stop`` 이 설정될 때까지 주기적으로 수집한다. 수집 시간만큼 간격을 줄여 주기를 지킨다."""
        while not stop.is_set():
            started = time.monotonic()
            await self.sample_once()
            wait = max(0.0, self.interval - (time.monotonic() - started))
            try:
                await asyncio.wait_for(stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass

    def summary(self) -> Dict[str, Any]:
        """서버별·단계별로 CPU·메모리 평균/p95/최대와 task 최대치를 낸다."""
        per: Dict[str, Dict[str, Dict[str, List[float]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list)))
        roles: Dict[str, str] = {}
        unhealthy: Dict[str, int] = defaultdict(int)
        for s in self.samples:
            for name, m in s["servers"].items():
                roles[name] = m["role"]
                bucket = per[name][s["phase"]]
                for key in ("cpu", "mem", "active_tasks", "max_tasks"):
                    if m.get(key) is not None:
                        bucket[key].append(m[key])
                if not m.get("healthy"):
                    unhealthy[name] += 1

        servers = []
        # coordinator 를 먼저, 그 뒤 executor 를 이름순으로 낸다.
        for name in sorted(roles, key=lambda n: (roles[n] != "coordinator", n)):
            phases = {}
            for phase in PHASES:
                b = per[name].get(phase)
                if not b:
                    continue
                cpu, mem = summarize(b["cpu"]), summarize(b["mem"])
                phases[phase] = {
                    "samples": max(cpu["count"], mem["count"]),
                    "cpu_avg": cpu["mean"], "cpu_p95": cpu["p95"], "cpu_max": cpu["max"],
                    "mem_avg": mem["mean"], "mem_max": mem["max"],
                    "active_tasks_max": max(b["active_tasks"]) if b["active_tasks"] else None,
                    "max_tasks": max(b["max_tasks"]) if b["max_tasks"] else None,
                }
            servers.append({
                "name": name,
                "role": roles[name],
                "unhealthy_samples": unhealthy.get(name, 0),
                "phases": phases,
            })

        peak_jobs = [s["jobs_active"] for s in self.samples
                     if s["phase"] == "load" and isinstance(s.get("jobs_active"), (int, float))]
        return {
            "interval": self.interval,
            "samples": len(self.samples),
            "errors": self.errors,
            "last_error": self.last_error,
            "servers": servers,
            "jobs_active_max": max(peak_jobs) if peak_jobs else None,
            "cpu_peak": self._peak("cpu"),
        }

    def _peak(self, key: str) -> Optional[Dict[str, Any]]:
        """부하 구간에서 CPU 가 가장 높았던 서버와 시점이다. 포화 시점을 가늠하는 데 쓴다."""
        best = None
        for s in self.samples:
            if s["phase"] != "load":
                continue
            for name, m in s["servers"].items():
                v = m.get(key)
                if v is not None and (best is None or v > best["value"]):
                    best = {"server": name, "value": v, "t": s["t"]}
        return best

    def rows(self) -> List[Dict[str, Any]]:
        """CSV 용으로 표본을 (시각, 서버) 한 행씩 펼친다."""
        out = []
        for s in self.samples:
            for name, m in s["servers"].items():
                out.append({
                    "t": round(s["t"], 3), "phase": s["phase"], "server": name,
                    "role": m["role"], "cpu_percent": m.get("cpu"),
                    "memory_percent": m.get("mem"), "active_tasks": m.get("active_tasks"),
                    "max_tasks": m.get("max_tasks"), "healthy": m.get("healthy"),
                    "jobs_active": s.get("jobs_active"),
                })
        return out
