"""local_stage HTTP 멀티프로세스 통합 하니스 (docs/SCENARIO.md B-4).

executor 를 실제 HTTP 서버(uvicorn 스레드)로 2대 띄우고, coordinator 는 HttpDispatcher 로
그들에게 POST /tasks → 폴링 → /metrics 로 gp_hostname 수집 → GP file:// 적재(주입된 목) →
/stage/{job}/cleanup 팬아웃까지 **실제 HTTP 경로**를 태운다. 모든 파일은 단일 테스트 머신의
공유 파일시스템(tmp)에 쓰이고, gp_hostname 만 "seg" 로 흉내 내어 URL 호스트(127.0.0.1)와
분리한다(목 load 는 host 를 무시하고 경로로 읽으므로 1머신에서 멀티 호스트 매핑을 재현).

한 호스트에 executor 2대가 있는 경우(= 사용자 요구 "segment 1개당 executor 여러 개")도
함께 검증한다: 파일 예산이 두 executor 에 라운드로빈된다.
"""

from __future__ import annotations

import os
import socket
import threading
import time
from collections import Counter

import pytest
import uvicorn
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.config import settings
from coordinator.dispatcher import HttpDispatcher
from coordinator.job_store import JobStore
from coordinator.stage import host_of
from executor.app import create_app as create_executor_app

from .helpers import MockLocalStageBackend


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _ThreadedServer:
    """uvicorn 앱을 데몬 스레드에서 띄우고 준비될 때까지 기다리는 헬퍼."""

    def __init__(self, app, port: int):
        config = uvicorn.Config(app, host="127.0.0.1", port=port,
                                log_level="warning", lifespan="on")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        for _ in range(250):  # 최대 ~5초 대기
            if self.server.started:
                return
            time.sleep(0.02)
        raise RuntimeError("executor 서버가 기동되지 않음")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


@pytest.fixture
def http_cluster(monkeypatch, tmp_path):
    """executor 2대(HTTP) + 공유 설정을 준비하고, 각 executor 의 export 백엔드를 돌려준다."""
    # 두 executor 는 같은 호스트("seg")를 흉내내되 URL 호스트는 127.0.0.1 로 다르다.
    monkeypatch.setattr(settings, "stage_local_dir", str(tmp_path), raising=False)
    monkeypatch.setattr(settings, "executor_gp_hostname", "seg", raising=False)
    monkeypatch.setattr(settings, "poll_interval_s", 0.02, raising=False)
    monkeypatch.setattr(settings, "executor_self_report", False, raising=False)
    monkeypatch.setattr(settings, "dashboard_enabled", False, raising=False)

    exec_backends = [MockLocalStageBackend(), MockLocalStageBackend()]
    servers = []
    urls = []
    for be in exec_backends:
        port = _free_port()
        srv = _ThreadedServer(create_executor_app(backend=be), port)
        srv.start()
        servers.append(srv)
        urls.append(f"http://127.0.0.1:{port}")

    monkeypatch.setattr(settings, "executors", urls, raising=False)
    try:
        yield urls, exec_backends
    finally:
        for srv in servers:
            srv.stop()


def _job_json():
    return {
        "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('d1','d2','d3','d4')",
        "partition_column": "dt",
        "target_table": "public.sales_mirror",
        "exec_mode": "local_stage",
        "parallelism": 4,
        "external_columns": "user_id bigint, amount numeric, dt date",
        "staging_table": "stg_sales",
        "insert_sql": "INSERT INTO public.sales_mirror SELECT * FROM stg_sales",
    }


def test_local_stage_http_end_to_end(http_cluster, tmp_path):
    urls, exec_backends = http_cluster

    # coordinator: HttpDispatcher + 주입된 Phase 2(GP file:// 적재) 목 백엔드.
    stage_backend = MockLocalStageBackend(topology={"seg": 4})
    disp = HttpDispatcher(settings)
    disp._stage_backend = stage_backend
    client = TestClient(create_app(runner=disp, store=JobStore()))

    resp = client.post("/jobs", json=_job_json())
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "DONE", body
    assert body["total"] == 4

    # ① 실제 HTTP 디스패치: 두 executor 모두 export 를 수행(멀티 executor per host).
    assert all(be.exported for be in exec_backends)
    assert sum(len(be.exported) for be in exec_backends) == 4  # 파일 4개가 두 대에 분산

    # ② gp_hostname 수집(/metrics): 파일 예산이 "seg" 호스트로 묶여 두 executor 에 라운드로빈.
    per_host = Counter(host_of(t["executor_url"]) for t in body["tasks"])
    # (URL 호스트는 127.0.0.1 이지만 host_of 로 본 것이므로 여기선 URL 호스트 분포만 확인)
    assert sum(per_host.values()) == 4

    # ③ Phase 2 URI 는 executor 가 /metrics 로 보고한 gp_hostname("seg") 기반(URL 호스트 아님).
    ddl = stage_backend.loads[0]
    assert "file://seg/" in ddl
    assert "127.0.0.1" not in ddl

    # ④ 파일 루프 닫힘: 두 executor 가 쓴 파일을 coordinator Phase 2 가 읽어 target 에 집계.
    exported_rows = sum(r for be in exec_backends for _, r in be.exported)
    assert exported_rows == 4
    assert len(stage_backend.target) == exported_rows

    # ⑤ cleanup 팬아웃: /stage/{job}/cleanup 으로 로컬 job 디렉터리가 삭제됨.
    assert not os.path.isdir(os.path.join(str(tmp_path), job_id))
