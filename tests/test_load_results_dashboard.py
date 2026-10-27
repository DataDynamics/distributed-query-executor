"""coordinator 대시보드의 부하 테스트 기록 목록(GET /load-tests)과 본문 조회를 검증한다.

기록 파일은 실제 기록 도구(tools.load.resultfile)로 만들어, 도구가 쓰는 머리 형식과 대시보드가 읽는 형식이
어긋나는 회귀를 함께 막는다.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from coordinator import load_results
from coordinator.app import create_app
from tools.load import resultfile


def _write_record(directory, started, exit_code=0, run_id="r1"):
    result = {
        "meta": {"run_id": run_id, "started_at": started.isoformat(timespec="seconds"),
                 "finished_at": started.isoformat(timespec="seconds"), "load_elapsed": 10.0,
                 "total_elapsed": 12.0, "stop_reason": "지정 시간 종료", "coordinators": ["http://coord:8088"],
                 "scenario": "async POST /jobs | 0s:2 → 10s:2", "body": {"sql": "SELECT 1"}},
        "summary": {"submitted": 11, "complete_tps": 0.92, "error_rate": 0.0,
                    "submit_outcomes": {"rejected_429": 0}},
        "timeline": [{"second": 0, "vus": 2}],
    }
    text = resultfile.render(result, summary="[처리량]\n  제출 11건", command="bin/load-test run --vus 2",
                             cwd="/tmp", exit_code=exit_code, exit_reason="정상 종료" if exit_code == 0 else "중단",
                             files={}, params={"vus": 2})
    return resultfile.write(text, started, directory)


@pytest.fixture
def client(runner, store, tmp_path, monkeypatch):
    from coordinator.config import settings
    monkeypatch.setattr(settings, "log_dir", tmp_path / "logs", raising=False)
    with TestClient(create_app(runner=runner, store=store)) as c:
        yield c, tmp_path / "logs" / "test_results"


def test_기록이_없으면_빈_목록과_위치를_알려_준다(client):
    c, d = client
    body = c.get("/load-tests").json()
    assert body == {"dir": str(d), "exists": False, "total": 0, "rows": []}


def test_목록은_최신순이고_머리_개요를_담는다(client):
    c, d = client
    _write_record(d, datetime(2026, 10, 28, 9, 0, 0), run_id="old")
    _write_record(d, datetime(2026, 10, 28, 10, 0, 0), exit_code=130, run_id="new")
    (d / "notes.txt").write_text("기록 파일이 아님", encoding="utf-8")   # 모양이 다른 파일은 빠진다
    body = c.get("/load-tests").json()
    assert body["total"] == 2 and [r["name"] for r in body["rows"]] == ["20261028-100000.txt", "20261028-090000.txt"]
    new, old = body["rows"]
    assert new["run_id"] == "new" and new["exit_code"] == 130 and old["exit_code"] == 0
    assert old["target"] == "http://coord:8088" and old["command"] == "bin/load-test run --vus 2"
    assert "완료 TPS 0.92" in old["headline"] and old["scenario"].startswith("async POST /jobs")
    assert c.get("/load-tests?limit=1&offset=1").json()["rows"][0]["run_id"] == "old"


def test_본문은_원본과_JSON_을_뺀_화면용_두_가지다(client):
    c, d = client
    path = _write_record(d, datetime(2026, 10, 28, 9, 0, 0))
    full = c.get(f"/load-tests/{path.name}")
    assert full.status_code == 200 and full.headers["content-type"].startswith("text/plain")
    assert "[결과 JSON]" in full.text and full.text == path.read_text(encoding="utf-8")
    short = c.get(f"/load-tests/{path.name}?full=false").text
    assert "[테스트 결과]" in short and '"timeline"' not in short and "'원본' 링크" in short


@pytest.mark.parametrize("name", ["..%2F..%2Fetc%2Fpasswd", "x.txt", "20261028-090000.log"])
def test_기록_파일_모양이_아닌_이름은_거절한다(client, name):
    c, _ = client
    assert c.get(f"/load-tests/{name}").status_code in (400, 404)


def test_없는_기록은_404_이고_디렉터리_밖은_읽지_않는다(client, tmp_path):
    c, d = client
    d.mkdir(parents=True)
    assert c.get("/load-tests/20261028-090000.txt").status_code == 404
    with pytest.raises(ValueError):
        load_results.read_result(d, "../20261028-090000.txt")


def test_대시보드에_탭이_있다(client):
    c, _ = client
    html = c.get("/").text
    assert 'data-tab="lt"' in html and 'id="p-lt"' in html and "lt:loadLt" in html
