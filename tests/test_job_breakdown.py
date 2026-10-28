"""job 구간 분석(``coordinator.breakdown``)과 소스 조회 단계 라벨 검증.

- 벽시계 구간(슬롯 대기·디스패치 대기·task 실행·마감)이 job·task 시각에서 올바르게 나뉘는지.
- task 단계별 평균은 같은 이름 단계를 task 안에서 합친 뒤 그 단계를 거친 task 수로 나누는지.
- Trino 로 읽은 job 의 IMPALA_SUBMIT 단계가 "Trino 조회 제출" 로 보이는지(기록 시점과 조회 시점 모두).
- ``GET /jobs/{id}/breakdown`` 이 저장소의 job 으로 응답하고, 없으면 404 인지.
"""

from __future__ import annotations

from coordinator.breakdown import job_breakdown
from coordinator.models import Job, JobStatus, Task, TaskStatus
from core.phases import phase_label, record_stage, relabel_phases, source_name


def _ph(name, start, end, label=None):
    """HH:MM:SS 두 개로 단계 레코드 하나를 만든다(같은 날짜)."""
    s, e = f"2026-10-28T10:{start}", f"2026-10-28T10:{end}"
    from core.phases import _dur_ms
    return {"name": name, "label": label or name, "started_at": s, "finished_at": e,
            "duration_ms": _dur_ms(s, e), "rows": None, "extra": None}


def _job_view(**kw):
    base = {
        "job_id": "job_x", "status": "DONE", "exec_mode": "copy", "datasource": None,
        "created_at": "2026-10-28T10:00:00", "started_at": "2026-10-28T10:00:10",
        "finished_at": "2026-10-28T10:02:00",
        "tasks": [
            # t1: 대기 5s, 조회 10s, COPY 40s(재시도로 두 번 열려 20+20)
            {"task_id": "t1", "phases": [
                _ph("QUEUE_WAIT", "00:30", "00:35"), _ph("IMPALA_SUBMIT", "00:35", "00:45"),
                _ph("STREAM_COPY", "00:45", "01:05"), _ph("STREAM_COPY", "01:05", "01:25")]},
            # t2: 대기 15s, 조회 30s, COPY 60s
            {"task_id": "t2", "phases": [
                _ph("QUEUE_WAIT", "00:40", "00:55"), _ph("IMPALA_SUBMIT", "00:55", "01:25"),
                _ph("STREAM_COPY", "01:25", "01:50")]},
        ],
    }
    base.update(kw)
    return base


def test_구간은_슬롯_디스패치_task_마감으로_나뉜다():
    b = job_breakdown(_job_view())
    seg = {s["key"]: s["ms"] for s in b["segments"]}
    assert seg == {"slot_wait": 10_000, "dispatch_wait": 20_000, "tasks": 80_000, "finalize": 10_000}
    assert b["longest_segment"] == "tasks"
    assert b["total_ms"] == 120_000
    # task 처리 시간은 첫 단계 시작부터 마지막 단계 종료까지다(t1 55s, t2 70s).
    assert b["task_avg_ms"] == 62_500 and b["task_max_ms"] == 70_000


def test_단계별_평균은_task_안에서_합친_뒤_task_수로_나눈다():
    b = job_breakdown(_job_view())
    ph = {p["name"]: p for p in b["phases"]}
    assert [p["name"] for p in b["phases"]] == ["QUEUE_WAIT", "IMPALA_SUBMIT", "STREAM_COPY"]
    assert ph["QUEUE_WAIT"]["avg_ms"] == 10_000
    assert ph["IMPALA_SUBMIT"]["avg_ms"] == 20_000 and ph["IMPALA_SUBMIT"]["slowest_task"] == "t2"
    # t1 의 STREAM_COPY 두 구간(20s+20s)은 합쳐 40s 한 건이다. t2 는 25s.
    assert ph["STREAM_COPY"]["n"] == 2 and ph["STREAM_COPY"]["avg_ms"] == 32_500
    assert ph["STREAM_COPY"]["max_ms"] == 40_000 and ph["STREAM_COPY"]["min_ms"] == 25_000
    assert b["longest_phase"] == "STREAM_COPY"
    assert round(sum(p["share_pct"] for p in b["phases"])) == 100


def test_스테이징_모드는_마감을_phase2_적재로_부른다():
    b = job_breakdown(_job_view(exec_mode="s3_stage"))
    assert b["segments"][-1]["label"] == "Phase 2 적재·정리"


def test_시각이_없거나_시계가_어긋나도_깨지지_않는다():
    # 단계 기록이 없고 executor 시계가 coordinator 보다 앞서 디스패치 대기가 음수가 되는 경우다.
    b = job_breakdown(_job_view(started_at="2026-10-28T10:00:50", tasks=[{"task_id": "t", "phases": []}]))
    assert b["phases"] == [] and b["longest_phase"] is None and b["task_avg_ms"] is None
    seg = {s["key"]: s["ms"] for s in b["segments"]}
    assert seg["dispatch_wait"] is None and seg["tasks"] is None and seg["finalize"] == 70_000
    skew = job_breakdown(_job_view(started_at="2026-10-28T10:00:40"))
    assert {s["key"]: s["ms"] for s in skew["segments"]}["dispatch_wait"] == 0


def test_소스_조회_단계_라벨은_datasource_를_따른다():
    assert source_name(None) == "Impala" and source_name("source") == "Impala"
    assert source_name("trino") == "Trino" and source_name("my_api") == "my_api"
    assert phase_label("IMPALA_SUBMIT") == "Impala 조회 제출"
    assert phase_label("IMPALA_SUBMIT", "trino") == "Trino 조회 제출"
    assert phase_label("STREAM_COPY", "trino") == "스트리밍 COPY"
    phases: list = []
    record_stage(phases, "IMPALA_SUBMIT", "start", datasource="trino")
    assert phases[0]["label"] == "Trino 조회 제출"
    # 예전 기록은 조회할 때 바로잡고, 원본은 건드리지 않는다.
    old = [{"name": "IMPALA_SUBMIT", "label": "Impala 조회 제출"}]
    assert relabel_phases(old, "trino")[0]["label"] == "Trino 조회 제출"
    assert old[0]["label"] == "Impala 조회 제출"
    # 구간 분석도 job 의 datasource 로 라벨을 붙인다.
    b = job_breakdown(_job_view(datasource="trino"))
    assert {p["name"]: p["label"] for p in b["phases"]}["IMPALA_SUBMIT"] == "Trino 조회 제출"


def test_executor_task_는_자기_datasource_로_라벨을_붙인다():
    from executor.models import Task as ExecTask
    t = ExecTask(job_id="j", task_id="t", sub_query="SELECT 1", target_table="x", write_mode="append",
                 partition_column="dt", partition_values=["1"], datasource="trino")
    t.on_stage("IMPALA_SUBMIT", "start")
    assert t.phases[0]["label"] == "Trino 조회 제출"


def _stored_job(datasource=None):
    job = Job(
        original_sql="SELECT 1 FROM t WHERE dt IN ('1')",
        partition_column="dt", target_table="public.t", write_mode="append",
        parallelism=1, split_strategy="contiguous", failure_policy="fail_fast",
        status=JobStatus.DONE, datasource=datasource,
        created_at="2026-10-28T10:00:00", started_at="2026-10-28T10:00:10",
        finished_at="2026-10-28T10:02:00",
    )
    task = Task(job_id=job.job_id, executor_url="http://e1:8001", sub_query="SELECT 1",
                partition_values=["1"], status=TaskStatus.DONE)
    task.phases = [_ph("IMPALA_SUBMIT", "00:30", "00:45", "Impala 조회 제출"),
                   _ph("STREAM_COPY", "00:45", "01:50", "스트리밍 COPY")]
    job.tasks = [task]
    return job


def test_breakdown_엔드포인트는_저장소의_job_으로_응답한다(client, store):
    job = _stored_job(datasource="trino")
    store.add(job)
    body = client.get(f"/jobs/{job.job_id}/breakdown").json()
    assert body["source"] == "store" and body["longest_phase"] == "STREAM_COPY"
    assert body["longest_segment"] == "tasks"
    assert body["tasks"][0]["phases"][0]["label"] == "Trino 조회 제출"
    # 작업 상세 조회도 같은 라벨로 바로잡혀 나온다.
    detail = client.get(f"/jobs/{job.job_id}").json()
    assert detail["tasks"][0]["phases"][0]["label"] == "Trino 조회 제출"


def test_breakdown_엔드포인트는_없는_job_에_404(client):
    # 이력 DSN 이 없으면 복원할 곳도 없으므로 404 다.
    assert client.get("/jobs/job_nope/breakdown").status_code == 404


def test_대시보드_이력_탭에_구간_분석_링크가_있다(client):
    html = client.get("/").text
    assert "구간 분석" in html and "/breakdown" in html and "renderBreakdown" in html
