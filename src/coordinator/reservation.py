"""Phase 3-B 의 공유 예약이다. 여러 coordinator 가 실시간 전역 부하를 공유하려고 예약을 남긴다.

HA 에서 P2C/least_loaded 만으로는 heartbeat 간격 동안 여러 coordinator 가 같은 한가한
노드로 몰릴 수 있다(분산 스탬피드). 이를 더 강하게 막으려면, 각 coordinator 가 dispatch 중인
task 를 ``executor_reservation`` 테이블에 **예약**하고, 선택 시 부하를
``active_tasks + 예약수`` 로 보면 된다(effective load). 죽은 coordinator 의 예약이 영구
누수되지 않도록 **TTL**(updated_at 신선도)로 만료시킨다.

구성:
- ``merge_reservations`` : 부하 뷰의 active_tasks 에 예약수를 합산하는 순수 함수(테스트 용이).
- ``ReservationRepository`` : executor_reservation 테이블에 reserve/release/load_by_url.
- ``ReservingLoadView`` : SharedLoadView 를 감싸 by_url() 에서 예약을 합산(드롭인 교체).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def merge_reservations(view: dict, reserved: dict) -> dict:
    """URL 별 부하 뷰의 ``active_tasks`` 에 URL 별 예약 수를 더한 새 뷰를 만든다.

    예약이 없으면 원본을 그대로 돌려준다(불필요한 복사 회피). 뷰에 없는 url 의 예약은
    무시한다(헬스 정보가 없으면 어차피 선택 대상이 아님).
    """
    if not reserved:
        return view
    out: dict = {}
    for url, load in view.items():
        r = reserved.get(url, 0)
        if r:
            merged = dict(load)
            merged["active_tasks"] = (merged.get("active_tasks") or 0) + r
            out[url] = merged
        else:
            out[url] = load
    return out


class ReservationRepository:
    """executor_reservation 공유 테이블에 (executor_url, coordinator_id) 별 예약수를 관리한다.

    reserve/release 는 자기 coordinator 행만 원자적으로 증감하고, load_by_url 은 **TTL 이내**
    예약만 url 별로 합산해 돌려준다(죽은 coordinator 의 예약은 만료되어 무시). 모든 DB 작업은
    실패해도 작업 진행을 막지 않도록 예외를 로깅만 하고 삼킨다(예약은 최적화이지 정확성 요건이
    아니다 — executor 의 실제 self-report 가 결국 진실).
    """

    def __init__(self, dsn: str, coordinator_id: str, table: str = "executor_reservation"):
        self.dsn = dsn
        self.coordinator_id = coordinator_id
        self.table = table

    def _conn(self):
        import psycopg  # 드라이버가 없을 수 있어 지연 임포트한다
        return psycopg.connect(self.dsn)

    def reserve(self, url: str) -> None:
        sql = (
            f"INSERT INTO {self.table} (executor_url, coordinator_id, n, updated_at) "
            "VALUES (%s, %s, 1, (now() AT TIME ZONE 'Asia/Seoul')) "
            "ON CONFLICT (executor_url, coordinator_id) DO UPDATE SET "
            f"n = {self.table}.n + 1, updated_at = (now() AT TIME ZONE 'Asia/Seoul')"
        )
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (url, self.coordinator_id))
                conn.commit()
        except Exception:
            logger.exception("예약 reserve 실패 url=%s", url)

    def release(self, url: str) -> None:
        sql = (
            f"UPDATE {self.table} SET n = GREATEST(n - 1, 0), updated_at = (now() AT TIME ZONE 'Asia/Seoul') "
            "WHERE executor_url = %s AND coordinator_id = %s"
        )
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (url, self.coordinator_id))
                conn.commit()
        except Exception:
            logger.exception("예약 release 실패 url=%s", url)

    def load_by_url(self, ttl_s: float) -> dict:
        sql = (
            f"SELECT executor_url, SUM(n) FROM {self.table} "
            "WHERE (now() AT TIME ZONE 'Asia/Seoul') - updated_at <= make_interval(secs => %s) "
            "GROUP BY executor_url"
        )
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (float(ttl_s),))
                    rows = cur.fetchall()
        except Exception:
            logger.exception("예약 load 실패")
            return {}
        return {r[0]: int(r[1] or 0) for r in rows if r[0]}


class ReservingLoadView:
    """SharedLoadView 를 감싸, by_url() 에서 예약수를 active_tasks 에 합산해 돌려준다.

    selector 입장에선 일반 부하 뷰와 동일한 인터페이스(by_url)라 드롭인 교체된다.
    """

    def __init__(self, base, reservations: ReservationRepository, ttl_s: float):
        self.base = base
        self.reservations = reservations
        self.ttl_s = ttl_s

    def by_url(self) -> dict:
        view = self.base.by_url()
        reserved = self.reservations.load_by_url(self.ttl_s)
        return merge_reservations(view, reserved)
