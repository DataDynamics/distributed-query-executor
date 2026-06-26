"""dry_run: executor 호출 없이 생성된 쿼리만 반환/로깅."""

from __future__ import annotations


def _payload(**over):
    base = {
        "sql": "SELECT a, dt FROM sales WHERE dt IN ('1','2','3','4')",
        "partition_column": "dt",
        "target_table": "public.t",
        "parallelism": 2,
        "dry_run": True,
    }
    base.update(over)
    return base


def test_dry_run_returns_plan_without_dispatch(client, runner, store):
    resp = client.post("/jobs", json=_payload())
    assert resp.status_code == 200
    body = resp.json()
    assert body["dry_run"] is True
    assert body["task_count"] == 2
    assert len(body["tasks"]) == 2
    # 각 task 에 생성된 sub_query 포함
    for t in body["tasks"]:
        assert "dt IN" in t["sub_query"]
        assert "partition_values" in t
    # executor 디스패치/작업 저장이 일어나지 않아야 한다
    assert runner.runs == []
    assert store.list() == []


def test_dry_run_split_values(client):
    body = client.post("/jobs", json=_payload(parallelism=2)).json()
    vals = [t["partition_values"] for t in body["tasks"]]
    assert vals == [["'1'", "'2'"], ["'3'", "'4'"]]


def test_dry_run_with_statement_wrapper(client):
    body = client.post("/jobs", json=_payload(
        exec_mode="statement",
        wrapper_query="INSERT INTO public.t SELECT * FROM ({{SUBQUERY}}) s",
    )).json()
    for t in body["tasks"]:
        assert t["sub_query"].startswith("INSERT INTO public.t SELECT * FROM (")


def test_dry_run_with_stage_insert(client):
    body = client.post("/jobs", json=_payload(
        exec_mode="stage_insert",
        staging_table="stg",
        staging_ddl="CREATE TEMP TABLE stg (a int, dt text)",
        wrapper_query="INSERT INTO public.t (a, dt) SELECT a, dt FROM stg",
    )).json()
    assert body["exec_mode"] == "stage_insert"
    t = body["tasks"][0]
    # sub_query 는 분할된 SELECT, staging/insert 정보 동봉
    assert t["sub_query"].startswith("SELECT a, dt FROM sales")
    assert t["staging_table"] == "stg"
    assert "CREATE TEMP TABLE" in t["staging_ddl"]
    assert t["insert_sql"].startswith("INSERT INTO public.t")


def test_dry_run_still_validates(client):
    # dry_run 이어도 검증은 동일하게 수행 → 잘못된 쿼리는 422
    resp = client.post("/jobs", json=_payload(
        sql="SELECT count(*) FROM t WHERE dt IN ('1')"))
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "UNSUPPORTED_AGGREGATE"


def test_non_dry_run_still_dispatches(client, runner):
    # dry_run=false(기본)는 기존대로 202 + 디스패치
    resp = client.post("/jobs", json=_payload(dry_run=False))
    assert resp.status_code == 202
    assert "job_id" in resp.json()
    assert len(runner.runs) == 1
