"""stage_insert: task 별 고유 staging 이름(task_id 접미사) + 종료 시 DROP.

배경: 같은 job 의 여러 task 가 같은 이름으로 CREATE TEMP TABLE 을 하면, executor 의 GP
커넥션 풀이 세션을 재사용할 때 이전 task 의 TEMP 가 남아 "already exists" 로 충돌할 수
있다. coordinator 가 디스패치 시점에 staging 이름을 task 별로 고유화하고, 실 backend 는
INSERT 직후 같은 트랜잭션에서 DROP 해 이 문제를 없앤다.

순수 함수(coordinator/stage.py)와 LocalDispatcher 배선(실 DB 없이 레코더)로 검증한다.
"""

from __future__ import annotations

from coordinator import stage as stage_sql
from coordinator.config import settings as core_settings
from coordinator.dispatcher import LocalDispatcher
from coordinator.models import Job, JobStatus, Task


# ───────────────────── 순수 함수: 이름 생성/치환 ─────────────────────


def test_unique_staging_name_basic_and_sanitize():
    # task_id 를 접미사로 붙이고, 영숫자 외 문자는 '_' 로 치환한다.
    assert stage_sql.unique_staging_name("stg", "t_0") == "stg_t_0"
    assert stage_sql.unique_staging_name("stg_sales", "t-ab.12") == "stg_sales_t_ab_12"


def test_unique_staging_name_respects_identifier_limit():
    # 63바이트 상한을 넘으면 base 를 잘라 뒤에 task_id 해시(8자)를 붙인다(결정적·고유).
    base = "s" * 60
    name = stage_sql.unique_staging_name(base, "t_deadbeef_task_1234567890")
    assert len(name.encode("utf-8")) <= 63
    # 같은 입력은 항상 같은 이름(재시도 멱등).
    assert name == stage_sql.unique_staging_name(base, "t_deadbeef_task_1234567890")
    # 다른 task_id 는 다른 이름.
    other = stage_sql.unique_staging_name(base, "t_deadbeef_task_9999999999")
    assert other != name


def test_rewrite_staging_name_quoted_token_becomes_bare():
    # 템플릿이 인용한 형태("old") 를 찾아 **따옴표 없는 bare** 새 이름으로 치환한다.
    ddl = 'CREATE TEMP TABLE "stg_t" (a int)'
    assert stage_sql.rewrite_staging_name(ddl, "stg_t", "stg_t_x") == \
        "CREATE TEMP TABLE stg_t_x (a int)"


def test_rewrite_staging_name_bare_fallback_and_wordboundary():
    # raw-SQL 로 따옴표 없이 쓴 경우 단어경계 bare 이름을 치환한다.
    sql = "INSERT INTO target SELECT * FROM stg_t"
    assert stage_sql.rewrite_staging_name(sql, "stg_t", "stg_t_x") == \
        "INSERT INTO target SELECT * FROM stg_t_x"
    # 부분 문자열은 건드리지 않는다(단어경계).
    assert stage_sql.rewrite_staging_name("FROM stg_table", "stg", "stg_x") == \
        "FROM stg_table"


def test_rewrite_staging_name_no_match_passthrough():
    # 이름이 SQL 어디에도 없으면 원문 그대로(무변경).
    sql = 'INSERT INTO target SELECT * FROM "other"'
    assert stage_sql.rewrite_staging_name(sql, "stg_t", "stg_t_x") == sql


# ───────────────────── per_task_staging 조합 ─────────────────────


def test_per_task_staging_uniquifies_ddl_and_insert_consistently():
    ddl = 'CREATE TEMP TABLE "stg_t" (a int)'
    ins = 'INSERT INTO "public"."target" (a) SELECT a FROM "stg_t"'
    name, out_ddl, out_ins = stage_sql.per_task_staging("stg_t", ddl, ins, "t_5")
    assert name == "stg_t_t_5"
    # DDL·INSERT 양쪽이 같은 고유 이름으로 일관 치환되어야 한다(핵심 계약).
    assert "CREATE TEMP TABLE stg_t_t_5 (a int)" == out_ddl
    assert "FROM stg_t_t_5" in out_ins
    # 따옴표로 감싸지 않는다(bare 식별자).
    assert '"stg_t_t_5"' not in out_ddl and '"stg_t_t_5"' not in out_ins
    assert '"stg_t"' not in out_ddl and '"stg_t"' not in out_ins
    # INSERT 대상 테이블에는 task_id 가 붙지 않고 이름도 그대로 유지된다(staging 만 치환).
    assert '"public"."target"' in out_ins
    assert "target_t_5" not in out_ins


def test_per_task_staging_protects_target_when_names_overlap_bare():
    # staging 이름이 target 이름의 일부와 겹쳐도(sales ⊂ public.sales) 대상엔 접미사가
    # 붙지 않아야 한다(bare 형태).
    ddl = "CREATE TEMP TABLE sales (a int)"
    ins = "INSERT INTO public.sales (a) SELECT a FROM sales"
    name, out_ddl, out_ins = stage_sql.per_task_staging(
        "sales", ddl, ins, "t_9", target_table="public.sales"
    )
    assert name == "sales_t_9"
    assert out_ddl == "CREATE TEMP TABLE sales_t_9 (a int)"
    # 대상 테이블은 그대로, staging 만 치환.
    assert "INSERT INTO public.sales (a)" in out_ins
    assert "FROM sales_t_9" in out_ins
    assert "public.sales_t_9" not in out_ins


def test_per_task_staging_protects_target_when_names_overlap_quoted():
    # 인용 형태에서도 "public"."sales" 안의 "sales" 를 건드리지 않는다.
    ddl = 'CREATE TEMP TABLE "sales" (a int)'
    ins = 'INSERT INTO "public"."sales" (a) SELECT a FROM "sales"'
    name, out_ddl, out_ins = stage_sql.per_task_staging(
        "sales", ddl, ins, "t_9", target_table="public.sales"
    )
    assert name == "sales_t_9"
    assert out_ddl == "CREATE TEMP TABLE sales_t_9 (a int)"
    # 대상 테이블 인용 토큰은 원문 그대로.
    assert '"public"."sales"' in out_ins
    assert "FROM sales_t_9" in out_ins


def test_per_task_staging_disabled_passthrough():
    ddl = 'CREATE TEMP TABLE "stg_t" (a int)'
    assert stage_sql.per_task_staging("stg_t", ddl, None, "t_5", enabled=False) == \
        ("stg_t", ddl, None)


def test_per_task_staging_without_ddl_passthrough():
    # staging_ddl 이 없으면(기존 영구 테이블에 직접 COPY) 이름을 바꾸지 않는다.
    assert stage_sql.per_task_staging("stg_t", None, "INSERT ... FROM stg_t", "t_5") == \
        ("stg_t", None, "INSERT ... FROM stg_t")


# ───────────────────── LocalDispatcher 배선(실 DB 없음) ─────────────────────


class _StageInsertRecorder:
    """stage_and_insert 호출 인자를 task 별로 기록하는 레코더 백엔드."""

    def __init__(self):
        self.calls = []  # [(staging_table, staging_ddl, insert_sql), ...]

    def stage_and_insert(self, impala_select, staging_table, staging_ddl,
                         insert_sql, on_progress=None, query_options=None, on_stage=None):
        self.calls.append((staging_table, staging_ddl, insert_sql))
        if on_progress:
            on_progress(1)
        return 1


def _stage_insert_job(n: int) -> Job:
    job = Job(
        original_sql="SELECT a, dt FROM imp WHERE dt IN ('1','2','3')",
        partition_column="dt",
        target_table="public.target",
        write_mode="append",
        parallelism=n,
        split_strategy="contiguous",
        failure_policy="fail_fast",
        exec_mode="stage_insert",
        staging_table="stg_t",
        staging_ddl='CREATE TEMP TABLE "stg_t" (a int, dt text)',
        insert_sql='INSERT INTO public.target (a, dt) SELECT a, dt FROM "stg_t"',
    )
    job.tasks = [
        Task(job_id=job.job_id, executor_url="local", sub_query=f"SELECT {i}",
             partition_values=[f"'{i + 1}'"])
        for i in range(n)
    ]
    return job


async def test_local_dispatch_assigns_unique_staging_per_task():
    backend = _StageInsertRecorder()
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _stage_insert_job(n=3)
    task_ids = [t.task_id for t in job.tasks]
    await disp.run(job)

    assert job.status == JobStatus.DONE
    assert len(backend.calls) == 3
    names = [c[0] for c in backend.calls]
    # task 마다 서로 다른 staging 이름이 배정된다.
    assert len(set(names)) == 3
    for (staging_table, staging_ddl, insert_sql), tid in zip(backend.calls, task_ids):
        expected = f"stg_t_{tid}"  # task_id 는 't_...' 라 sanitize 무변경
        assert staging_table == expected
        # DDL·INSERT 도 같은 고유 이름으로, 따옴표 없이(bare) 치환되어 전달된다.
        assert f"CREATE TEMP TABLE {expected} " in staging_ddl
        assert f"FROM {expected}" in insert_sql
        assert f'"{expected}"' not in staging_ddl and '"stg_t"' not in staging_ddl
        # INSERT 대상 테이블은 원래 이름 그대로 — task_id 접미사가 붙지 않는다.
        assert "INSERT INTO public.target " in insert_sql
        assert "public.target_" not in insert_sql
    # job 원본은 건드리지 않는다(공유 상태 오염 방지).
    assert job.staging_table == "stg_t"
    assert '"stg_t"' in job.staging_ddl


async def test_local_dispatch_passthrough_when_disabled(monkeypatch):
    backend = _StageInsertRecorder()
    monkeypatch.setattr(core_settings, "stage_unique_staging", False, raising=False)
    disp = LocalDispatcher(core_settings, backend=backend)
    job = _stage_insert_job(n=2)
    await disp.run(job)
    assert job.status == JobStatus.DONE
    # 비활성화면 모든 task 가 원래 이름을 그대로 쓴다.
    assert {c[0] for c in backend.calls} == {"stg_t"}
