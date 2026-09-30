"""부하 테스트 결과를 모으고 요약한다. 지연 백분위와 초 단위 시계열을 만든다.

VU 들은 한 이벤트 루프에서 돌기 때문에 락 없이 같은 :class:`Collector` 에 기록한다.
표본은 전부 보관한 뒤 끝에서 정렬해 백분위를 낸다. job 하나가 수십 초씩 걸리는 이 도구의 부하
규모에서는 표본이 많아야 수십만 개라 근사 히스토그램을 쓸 이유가 없고, 정확한 값이 더 낫다.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Sequence

#: 요청 결과 분류다. accepted 는 async 요청이 접수된 것(이후 폴링으로 완료를 본다)이고, ok 는 sync
#: 요청이 성공 응답을 받은 것(받는 순간 완료)이다. unexpected_status 는 sync 에서 2xx 이지만 성공
#: 코드로 지정하지 않은 응답, missing_id 는 async 에서 2xx 인데 응답에 id 가 없는 경우다.
SUBMIT_OUTCOMES = (
    "accepted", "ok", "rejected_429", "client_error", "server_error", "conn_error",
    "unexpected_status", "missing_id",
)

#: 오류 판정에 들어가는 요청 분류. 429 는 용량 초과라는 정상적인 신호라 따로 센다.
SUBMIT_ERRORS = ("client_error", "server_error", "conn_error", "unexpected_status", "missing_id")

#: 도구가 판정하는 async 종료 분류다. 서버가 준 종료 상태(DONE·FAILED 등)는 폴링 설정에 따라
#: 달라지므로 문자열 그대로 센다. POLL_TIMEOUT 은 --job-timeout 안에 끝나지 않은 것, LOST 는 폴링
#: 중 요청이 사라진 것(404), ABANDONED 는 테스트를 멈출 때 drain 시간 안에 끝나지 않아 도구가 손을
#: 뗀 것이다. ABANDONED 는 도구가 끊은 것이라 오류율에 넣지 않는다.
TOOL_OUTCOMES = ("POLL_TIMEOUT", "LOST", "ABANDONED")


def percentile(sorted_values: Sequence[float], p: float) -> Optional[float]:
    """정렬된 표본의 p 백분위(nearest-rank)다. 표본이 없으면 None 이다."""
    if not sorted_values:
        return None
    if p <= 0:
        return sorted_values[0]
    k = int(math.ceil(p / 100.0 * len(sorted_values))) - 1
    return sorted_values[min(max(k, 0), len(sorted_values) - 1)]


def summarize(values: Iterable[float]) -> Dict[str, Optional[float]]:
    """표본을 건수·평균·p50/p90/p95/p99·최소·최대로 요약한다."""
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return {"count": 0, "mean": None, "min": None, "p50": None, "p90": None,
                "p95": None, "p99": None, "max": None}
    return {
        "count": len(vals),
        "mean": sum(vals) / len(vals),
        "min": vals[0],
        "p50": percentile(vals, 50),
        "p90": percentile(vals, 90),
        "p95": percentile(vals, 95),
        "p99": percentile(vals, 99),
        "max": vals[-1],
    }


class Collector:
    """요청과 async 완료 이벤트를 기록한다. 시각은 테스트 시작 기준 경과 초다.

    "완료"는 결과가 확정된 요청이다. sync 는 성공 응답을 받은 순간, async 는 서버가 종료 상태를 준
    순간이다. 그 가운데 성공 응답이나 성공 상태만 "성공"으로 센다.
    """

    #: 초 단위 버킷에 남기는 카운터 이름(CSV 열 순서이기도 하다)
    BUCKET_FIELDS = (
        "submitted", "accepted", "rejected_429", "request_errors",
        "completed", "success", "failed", "rows",
    )

    def __init__(self) -> None:
        self.submit_counts: Counter = Counter()
        self.job_counts: Counter = Counter()      # async 종료 분류(서버 상태 + TOOL_OUTCOMES)
        self.job_success = 0
        self.job_failed = 0                       # 성공이 아닌 종료 + POLL_TIMEOUT + LOST
        self.job_terminal = 0                     # 서버가 종료 상태를 준 async 요청 수
        self.submit_latency: List[float] = []     # sync 는 곧 응답 지연이다
        # e2e 는 제출부터 종료 확인까지 도구가 잰 시간, queue/run 은 서버 타임스탬프로 잰 시간이다.
        self.e2e_success: List[float] = []
        self.e2e_all: List[float] = []
        self.queue_time: List[float] = []
        self.run_time: List[float] = []
        self.rows_written = 0
        self.poll_requests = 0
        self.poll_errors = 0
        self.cancel_sent = 0
        self.errors: Counter = Counter()
        self.buckets: Dict[int, Counter] = defaultdict(Counter)
        # 초별 VU 수와 진행 중 요청 수는 컨트롤러가 틱마다 덮어쓴다(그 초의 마지막 값).
        self.vus_at: Dict[int, int] = {}
        self.inflight_at: Dict[int, int] = {}
        self.first_accept_at: Optional[float] = None
        self.first_reject_at: Optional[float] = None
        self.first_reject_vus: Optional[int] = None

    # ── 기록 ──

    def record_submit(self, t: float, outcome: str, latency: Optional[float],
                      error: Optional[str] = None, vus: Optional[int] = None) -> None:
        """요청 한 건을 기록한다. latency 는 응답을 받은 경우에만 있다(연결 오류면 None).

        sync 요청의 ok 는 받는 순간이 완료이므로 완료·성공으로도 함께 센다.
        """
        self.submit_counts[outcome] += 1
        if latency is not None:
            self.submit_latency.append(latency)
        b = self.buckets[int(t)]
        b["submitted"] += 1
        if outcome in ("accepted", "ok"):
            b["accepted"] += 1
            if self.first_accept_at is None:
                self.first_accept_at = t
            if outcome == "ok":
                b["completed"] += 1
                b["success"] += 1
        elif outcome == "rejected_429":
            b["rejected_429"] += 1
            if self.first_reject_at is None:
                self.first_reject_at = t
                self.first_reject_vus = vus
        else:
            b["request_errors"] += 1
        if error:
            self.errors[_short(error)] += 1

    def record_job(self, t: float, outcome: str, success: bool, e2e: Optional[float],
                   queue: Optional[float] = None, run: Optional[float] = None,
                   rows: Optional[int] = None, error: Optional[str] = None) -> None:
        """async 요청 하나의 종료를 기록한다. outcome 은 서버 상태이거나 TOOL_OUTCOMES 중 하나다."""
        self.job_counts[outcome] += 1
        terminal = outcome not in TOOL_OUTCOMES
        b = self.buckets[int(t)]
        if terminal:
            self.job_terminal += 1
            b["completed"] += 1
            if e2e is not None:
                self.e2e_all.append(e2e)
        if success:
            self.job_success += 1
            b["success"] += 1
            if e2e is not None:
                self.e2e_success.append(e2e)
        elif outcome != "ABANDONED":
            self.job_failed += 1
            b["failed"] += 1
        if queue is not None and queue >= 0:
            self.queue_time.append(queue)
        if run is not None and run >= 0:
            self.run_time.append(run)
        if rows:
            self.rows_written += int(rows)
            b["rows"] += int(rows)
        if error:
            self.errors[_short(f"[{outcome}] {error}")] += 1

    def record_gauges(self, t: float, vus: int, inflight: int) -> None:
        self.vus_at[int(t)] = vus
        self.inflight_at[int(t)] = inflight

    # ── 조회 ──

    def completed_between(self, start: float, end: float) -> int:
        """[start, end) 초 구간에 완료된 요청 수다. 진행 줄의 순간 TPS 에 쓴다."""
        return sum(self.buckets[s]["completed"] for s in range(int(start), int(end))
                   if s in self.buckets)

    @property
    def accepted(self) -> int:
        return self.submit_counts["accepted"] + self.submit_counts["ok"]

    @property
    def completed(self) -> int:
        return self.job_terminal + self.submit_counts["ok"]

    @property
    def succeeded(self) -> int:
        return self.job_success + self.submit_counts["ok"]

    @property
    def failed(self) -> int:
        return self.job_failed + sum(self.submit_counts[o] for o in SUBMIT_ERRORS)

    def timeline(self, until: float) -> List[Dict[str, int]]:
        """0 초부터 ``until`` 초까지 빈 초 없이 채운 초 단위 시계열이다."""
        last = max([int(math.ceil(until))] + [k + 1 for k in self.buckets])
        rows = []
        vus = inflight = 0
        for s in range(last):
            b = self.buckets.get(s, Counter())
            vus = self.vus_at.get(s, vus)
            inflight = self.inflight_at.get(s, inflight)
            row = {"second": s, "vus": vus, "inflight": inflight}
            row.update({f: int(b.get(f, 0)) for f in self.BUCKET_FIELDS})
            rows.append(row)
        return rows

    def summary(self, elapsed: float, load_elapsed: float) -> Dict:
        """보고서용 요약이다.

        요청 TPS 는 부하를 건 구간(``load_elapsed``)으로, 완료 TPS 는 마지막 완료까지 포함한 전체
        경과 시간(``elapsed``)으로 나눈다. drain 구간까지 넣으면 요청이 없는 시간으로 요청 TPS 가
        깎이기 때문이다.
        """
        submitted = sum(self.submit_counts.values())
        denom_submit = load_elapsed if load_elapsed > 0 else None
        denom_done = elapsed if elapsed > 0 else None
        attempts = submitted - self.submit_counts["rejected_429"]
        # 종료 분류는 많이 나온 순으로, 도구 판정 분류는 뒤에 둔다.
        server = sorted(((k, v) for k, v in self.job_counts.items() if k not in TOOL_OUTCOMES),
                        key=lambda kv: -kv[1])
        tool = [(k, self.job_counts[k]) for k in TOOL_OUTCOMES if self.job_counts[k]]
        return {
            "submitted": submitted,
            "accepted": self.accepted,
            "completed": self.completed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "submit_outcomes": {o: self.submit_counts[o] for o in SUBMIT_OUTCOMES},
            "job_outcomes": dict(server + tool),
            "submit_tps": submitted / denom_submit if denom_submit else None,
            "accept_tps": self.accepted / denom_submit if denom_submit else None,
            "complete_tps": self.completed / denom_done if denom_done else None,
            "success_tps": self.succeeded / denom_done if denom_done else None,
            "rows_written": self.rows_written,
            "rows_per_sec": self.rows_written / denom_done if denom_done else None,
            # 오류율은 429 를 뺀 시도(실제로 처리하려 한 요청) 대비다.
            "error_rate": self.failed / attempts if attempts > 0 else 0.0,
            "reject_rate": self.submit_counts["rejected_429"] / submitted if submitted else 0.0,
            "latency": {
                "submit": summarize(self.submit_latency),
                "e2e_success": summarize(self.e2e_success),
                "e2e_all": summarize(self.e2e_all),
                "queue": summarize(self.queue_time),
                "run": summarize(self.run_time),
            },
            "poll_requests": self.poll_requests,
            "poll_errors": self.poll_errors,
            "cancel_sent": self.cancel_sent,
            "first_accept_at": self.first_accept_at,
            "first_reject_at": self.first_reject_at,
            "first_reject_vus": self.first_reject_vus,
            "top_errors": self.errors.most_common(10),
        }


def _short(text: str, limit: int = 200) -> str:
    """오류 메시지를 한 줄로 접고 잘라 같은 오류끼리 묶이게 한다."""
    s = " ".join(str(text).split())
    return s if len(s) <= limit else s[: limit - 1] + "…"
