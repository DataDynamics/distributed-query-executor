# CLAUDE.md

이 저장소에서 작업하는 Claude Code(및 기타 에이전트)를 위한 안내다. 개요와 빠른 시작은
[README.md](README.md), 설계 심화는 [docs/DESIGN.md](docs/DESIGN.md), 실행 모드별 사용법은
[docs/GUIDE.md](docs/GUIDE.md), 배포는 [docs/DEPLOY.md](docs/DEPLOY.md)를 참고한다.

문서는 독자별로 갈린다. `docs/USER.md`(사용자 — API 사용법·모드 선택·오류 코드)와
`docs/OPERATOR.md`(운영자 — 일상 점검·장애 추적·용량 조정·설정)가 각 역할의 문서이고, GUIDE·
INTERFACE·INTEGRATION·DEPLOY·PERFORMANCE·DESIGN 은 특정 주제의 심화 문서다.
`docs/INTERFACE.md` 는 클라이언트가 coordinator 를 부를 때 쓰는 REST 인터페이스 명세로, 엔드포인트
아홉 개의 요청·응답 JSON 을 엑셀로 옮길 수 있는 평면 표(부모필드명·입출력 구분 열)로 적은 것이다.
엔드포인트를 추가하거나 요청·응답 필드를 바꾸면 이 문서와 짝을 이루는 `docs/INTERFACE.xlsx` 를
함께 고친다(엑셀은 md 를 파싱해 엔드포인트별 시트로 만든 것이다).

**이 두 문서는 의도적으로 self-contained 문서다.** 링크를 따라다니지 않고 하나만 읽어도 일을 끝낼 수
있도록 다른 문서의 내용을 가져와 담았으므로, 심화 문서와 겹치는 서술이 있는 것이 정상이다. 중복을
없애겠다고 본문을 링크로 바꾸지 않는다(문서 안에 다른 문서로 가는 링크를 두지 않는 것이 규칙이다).
대신 **겹치는 사실을 바꿀 때는 양쪽을 함께 고친다** — 오류 코드를 추가하면 USER.md 의 오류 절에,
운영에 영향을 주는 설정을 바꾸면 OPERATOR.md 에, 실행 모드의 필수 필드를 바꾸면 USER.md 와 GUIDE.md
양쪽에 반영한다.

## 프로젝트 개요

Coordinator 한 대와 Executor N대로 이루어진 분산 쿼리 실행기다. Impala `SELECT` 한 건을 파티션
컬럼의 `IN` 목록 기준으로 N분할해 병렬로 읽고, 각 executor 가 자기 몫을 Greenplum 에 적재한다
(Impala → Greenplum 이관). 데이터는 coordinator 를 거치지 않고 executor 가 직접 흘려보내며,
coordinator 로는 상태와 row count 만 흐른다.

## 명령어

```bash
# 가상환경(파이썬 3.9+, RHEL 9.2 기본 python3). 이 저장소는 .venv 를 사용한다.
python3.9 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt        # coordinator + 테스트 의존성

# 테스트 (실제 DB 불필요 — MockBackend/FakeRunner 사용). pandas 선택 테스트는 미설치 시 skip.
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest tests/test_admission.py -q # 특정 파일만

# 로컬 실행 (저장소 기본 설정 사용). 소스가 src/ 아래라 PYTHONPATH=src 필요(pytest 는 conftest.py 가 처리).
PYTHONPATH=src QUERY_EXECUTOR_CONFIG_DIR=config EXECUTOR_PORT=8001 .venv/bin/python -m executor &
PYTHONPATH=src QUERY_EXECUTOR_CONFIG_DIR=config .venv/bin/python -m coordinator

# local 모드(별도 executor 없이 coordinator 안에서 직접 실행)
COORDINATOR_EXECUTOR_MODE=local PYTHONPATH=src QUERY_EXECUTOR_CONFIG_DIR=config .venv/bin/python -m coordinator
```

셸에 `python` 이 없을 수 있으니(pyenv) 항상 `.venv/bin/python` 을 명시적으로 쓴다.

## 아키텍처

```
src/
  core/        # 공용: 설정 로더/설정/로깅/메트릭 (coordinator·executor 공유)
  coordinator/ # FastAPI: 검증(parser) → 분할(splitter) → admission → 디스패치 → 상태 추적
  executor/    # FastAPI: 소스 읽기 → Greenplum 적재(backend), task 상태 노출
  tools/       # 운영자용 CLI(gp-shell·impala-shell·s3-ops·load-test). 서비스와 별개로 사람이 직접 쓴다
bin/           # 런처·설치 스크립트 + 운영자 CLI 래퍼(/data1 배포 트리와 공용)
  systemd/     # systemd 유닛(coordinator.service·executor@.service) + install-systemd.sh
config/        # config.properties + config.yml 기본값 + 스키마(postgresql.sql / warehousepg.sql)
templates/     # 쿼리 템플릿(<template_id>/manifest.yml + *.sql.j2)
customs/       # 사이트 커스텀 코드(customs.query_funcs.* — src 밖 최상위 패키지)
tests/         # pytest (검증/라이프사이클/admission/대시보드/로깅)
```

소스는 src 레이아웃이다. 패키지 임포트명은 `core`/`coordinator`/`executor`/`tools` 그대로이고
(`pyproject.toml` 의 `package-dir`), 실행할 때는 `PYTHONPATH=src` 가 필요하다. 테스트는 루트
`conftest.py` 가 `src/` 와 저장소 루트를 sys.path 에 넣어 주므로 신경 쓰지 않아도 된다.

### 요청 흐름

`POST /jobs` 가 들어오면 먼저 멱등 사전확인을 한다. `Idempotency-Key` 헤더가 있으면 기존 job 을
재생하거나 409 로 거절한다. 이어서 parser 검증과 splitter 분할을 동기로 수행하고(실패하면 즉시
4xx), admission 의 `try_admit` 으로 수용 여부를 판단한다(초과하면 429). 통과하면 Job 을
SPLITTING 상태로 만들면서 `store.claim_and_add` 로 멱등 키를 원자적으로 선점하고, 백그라운드
`run()` 이 실행 슬롯을 기다렸다가(PENDING) RUNNING 으로 올라간다. 그 뒤 executor 들에게
`POST /tasks` 를 병렬 디스패치하고 상태를 polling 하다가, `finalize_job` 이 종료 상태를
DONE/PARTIAL/FAILED/CANCELLED 중 하나로 집계한다.

### 멱등성 두 층위

**요청 멱등**은 중복 제출을 흡수한다. 같은 `Idempotency-Key` 로 같은 본문이 오면 기존 job 을
200 + `Idempotency-Replayed` 로 재생하고, 본문이 다르면 409 를 준다. Job 에는
`idempotency_key` 와 본문 sha256 인 `request_fingerprint` 를 저장하며, 선점은
`store.claim_and_add` 가 원자적으로 처리한다(InMemory 는 프로세스 락, Sql 은 JSONB
`data->>'idempotency_key'` 조회에 postgresql.sql 의 부분 UNIQUE 인덱스를 backstop 으로 둔다.
WarehousePG 는 분산키 제약 때문에 best-effort 다). 관련 함수는 app.py 의
`_request_fingerprint` 와 `_idempotent_replay_response` 다.

**데이터 멱등**은 `write_mode:overwrite_partitions` 가 담당한다. 적재 전에 담당 파티션 값을
DELETE와 INSERT를 같은 트랜잭션에서 수행한다(`stage.py`/`backend.py`). 소스 범위와 삭제 범위가
일치하고 같은 파티션의 실행이 겹치지 않을 때 재실행 중복을 막는다. `append`는 멱등하지 않다.

## 핵심 모듈

### coordinator

**`dispatcher.py` 는 동시성의 중심이다.** `JobAdmission` 이 실행 슬롯과 대기 큐를 관리하며 초과
시 429 를 내고, `_DispatcherBase` 가 PENDING → 슬롯 대기 → RUNNING → 종료의 수명주기와 in-flight
반납·취소 감지를 맡는다. 원격 실행은 `HttpDispatcher`, in-process 실행은 `LocalDispatcher` 이며
하위 클래스는 `_execute(job)` 하나만 구현하면 된다.

**`parser.py`** 는 sqlglot 으로 SELECT 를 검증한다. `strict_validation=true` 면 단순 SELECT 만
받고, `false`면 JOIN·서브쿼리·GROUP BY 같은 복합 쿼리도 허용한다. 분할 기준 `IN`은 해당
SELECT의 WHERE에서 반드시 적용되는 양의 조건이어야 한다. OR·NOT·값 표현식에 포함되면 거절한다.

**`splitter.py`** 는 IN 값을 N등분하고(contiguous/round_robin) 원문 포맷을 보존한 채 치환한다.
여기에 **날짜 fan-out**(DESIGN §18.8)이 붙어 있다. `/jobs` 에 `task_params`(구간의 두 끝을 담은
파라미터 이름 두 개)를 주면 IN 분할 대신 하루를 task 하나로 펼친다(`app.py` 의 `_build_fanout`,
`_compute_task_offsets` — IN 파싱과 split 을 우회한다).

구간은 각 파라미터의 값과 `sign` 에서 도출하는데, 여기서 **`sign` 은 값의 부호가 아니라 SQL
연산자의 방향**이라는 점이 중요하다. Impala 의 `interval` 은 절대값만 받기 때문에
`- interval 7 day` 처럼 방향이 SQL 문에 박히기 때문이다. 템플릿에는 `<name>_sign` 으로 노출되고
`sql_sign` 필터가 `+`/`-` 외의 값을 막는다. task 마다 두 파라미터를 같은 날로 좁혀 렌더하므로
BETWEEN 이 하루로 붕괴하고 값은 언제나 절대값이 된다. `task_bound` 는 `point`(기본,
`(d,d)` — BETWEEN 이나 `=` 처럼 양끝 포함)와 `pair`(`(d,d+1)` — half-open `>=`/`<`) 중 고른다.

부호 변수를 쓰지 않는 템플릿은 Jinja2 AST 검사로 422(`TEMPLATE_MISSING_SIGN_VAR`)를 낸다. 막지
않으면 각 task 가 의도보다 넓은 구간을 읽어 **조용히 중복 적재**되기 때문이다. INSERT 와 staging
task 는 날짜와 무관하므로 한 번만 렌더해 job 전체가 공유한다. 이 기능은 stage_insert와 s3_stage에서 지원하며
append로 적재하므로(프레임워크가 대상에 DELETE 를 하지 않는다) 멱등이 필요하면 대상을 미리
비우거나 날짜별 물리 테이블을 쓴다. 예제는 `templates/daily_sales_interval/`(interval + sign)과
`templates/daily_sales/`(날짜 리터럴)다.

**`template.py` 와 `template_funcs.py` 는 쿼리 템플릿 엔진이다.** 클라이언트가 SQL 전문 대신
`template_id` 와 `params` 를 보내면 서버 템플릿(`template.dir/<id>/manifest.yml` + `*.sql.j2`)을
Jinja2 `SandboxedEnvironment` 로 렌더해 SELECT·STAGING DDL·INSERT 를 만들고 기존 요청 필드에
주입한다. 그 뒤의 parser → splitter → dispatch 경로는 전혀 바뀌지 않는다. 커스텀 함수는
`@template_filter`/`@template_global` 레지스트리로 등록하며(내장으로 `sql_str`·`sql_in`·
`sql_ident`·`sql_num`·`date_range` 를 제공한다) 설정 `template.func_modules` 로 확장한다.
`template_id` 를 주지 않으면 예전 raw-SQL 방식 그대로 동작한다. 예제는
`templates/sales_migration/` 이고 자세한 설계는 DESIGN 에 있다.

**`stage.py`** 는 `local_stage`(file:// 세그먼트 로컬 스테이징)의 Phase 2 SQL 을 조립하는 순수
함수 모음이다. `file://` 외부테이블 DDL, staging 적재, 멱등 DELETE, 정리 SQL 을 만들고
`plan_file_budget` 으로 호스트당 파일 수가 세그먼트 수 이하가 되도록 배분하며, executor_url 에서
gp_hostname 을 유도한다.

**`job_store.py`** 는 단일 coordinator용 `InMemoryJobStore`·`FileJobStore`와
멀티 coordinator용 `SqlJobStore`(JSONB)를 제공한다.

### 결과 반환 실행(`POST /query-execute`)

같은 템플릿을 `render_query()` 로 select 부분만 렌더해 실행하고 상위 N행을 동기로 돌려주는
preview 용 API다. coordinator 는 `/jobs` 와 같은 정책으로 가장 한가한 executor 를 골라
프록시하므로 클라이언트는 executor 의 존재를 모른다. greenplum 과 history 만 coordinator 가 직접
실행한다. `params` 는 이름-값 항목 배열이고, 응답의 `executed_by` 에 실제 실행한 executor 가
담긴다(직접 실행이면 null).

`POST /query-count` 는 같은 렌더·검증·라우팅(`_render_template_select`·`_execute_select`)을 공유하고
SQL 만 `parser.build_count_query()` 로 `SELECT COUNT(*) ... FROM (<원문>) qc_src` 로 감싸 실행해 건수
하나(`row_count`)를 돌려준다. 원문을 sqlglot 으로 재생성하지 않고 텍스트로 감싸므로 미리보기와 같은
쿼리를 센다. 이 응답의 `row_count` 는 총 건수라 query-execute 의 `row_count`(반환 행 수)와 뜻이 다르다.
예제는 `templates/order_count/` 로, 조인한 두 테이블의 겹치는 컬럼에 별칭을 붙여 감쌌을 때 출력 컬럼
이름이 충돌하지 않게 하는 모습을 보여 준다. 같은 쿼리의 Trino 판은 `templates/order_count_trino/` 로, manifest 의
`datasource`·`sql_dialect` 만으로 Trino 실행과 검증 방언이 정해지는 예다.

소스 실행은 impala·trino·source 구분 없이 **executor 의 `/query-run` 하나로 통일**돼 있고, 경로는
executor 가 `datasource` 로 고른다(`is_custom_source` — 이관의 소스 읽기와 같은 기준). `impala`·
`source`·미지정은 task 와 같은 impyla 접속(`build_impala_dsn` + `run_impala_select`)으로 직접
실행하고 `impala.host` 미설정이면 커스텀 함수로 폴백하지 않고 400 이다. 그 외 이름만 executor 가
`query.func.module`(dotted path, importlib 로 로딩) 함수를 `run(sql, config, limit)`
으로 호출하며, `config` 는 `query.func.config.*` 를 프리픽스로 모은 자유 설정 dict 다
(`core/config.py` 의 `_collect_prefix` 가 raw properties 기반으로 모으므로 YAML 과 무관하다).
참조 구현과 설정은 docs/GUIDE.md 와 `customs/query_funcs/trino_runner.py` 에 있다. 임의 SQL
미리보기(`/datasources/{name}/query`)는 운영 점검용으로 별개이며 built-in 으로 남겨 두었다.

### 이관 소스 엔진 선택(`datasource`)

template manifest 최상위의 `datasource` 로 `/jobs` 의 SELECT 를 읽을 엔진을 고른다. 우선순위는
요청 > manifest > `source.type` 이다. coordinator 가 `Job.datasource` 로 확정해 모든 task 에
실어 보내고, executor 의 `_source_connect(datasource)` 가 분기한다. 빈 값·`impala`·`source` 는
기존 impyla 커서를 쓰고, 그 외 이름은 `query.func.fetch_module` 커스텀 API 로 간다. 운영에서
DB-API 를 쓸 수 없는 사내 API 를 위한 경로라 커서가 없다.

**핵심 트릭은 어댑터다.** 읽기 루프가 소스에 요구하는 것은 `description`·`fetchmany`·`close`
셋뿐이라, 커스텀 API 결과를 `_FunctionCursor`/`_FunctionConnection` 으로 감싸면 CSV export 루프가
전혀 바뀌지 않는다. `_open_source_cursor` 와 `_source_execute` 도 손대지 않았는데, 어댑터가
impyla 전용 kwarg 를 삼켜 주기 때문이다. 반환값은 DataFrame, records, `{columns, rows}`,
`(columns, rows)` 와 그 청크 이터러블을 모두 받는다(`_normalize_fetch_result`). `NaN`/`NaT` 는
`_clean_row` 가 `None` 으로 바꿔 CSV NULL 마커로 나가게 하는데, 이걸 안 하면 문자열 `"nan"` 이
그대로 적재된다.

**`run()` 과 계약이 다르다는 점을 반드시 지킨다.** `run` 은 `limit` 으로 자르는 미리보기라 이관에
쓰면 잘린 결과가 조용히 적재된다. `fetch_module` 은 전량 반환이 계약이다
(`trino_runner.fetch_all`). 판정은 `core.config.is_custom_source` 한 곳에서만 하고, 설정이 없으면
Impala 로 폴백하지 않고 명확히 실패시킨다.

하위 호환의 핵심은 impala 이거나 미지정이면 backend 호출에 `datasource` kwarg 를 **아예 붙이지
않는 것**이다(`_src_kw`). 그래서 기존 백엔드와 테스트 더블이 무변경으로 동작한다. 적용 범위는
`export_to_local_csv` 하나(= s3_stage + local_stage)이고 copy 와 stage_insert 는 건드리지 않았다.
예제는 `templates/sales_migration_s3_trino/` 다. 다만 청크를 쓰지 않으면 task 결과가 전량 메모리에
올라가므로 `parallelism` 으로 완화한다.

### executor

**`backend.py`** 에는 실제 백엔드인 `ImpalaToGreenplumBackend`(소스 impyla → psycopg)와
`MockBackend` 가 있다. 소스 접속은 `_source_connect`·`_open_source_cursor`·`_source_execute` 에
모여 있고, 스트리밍과 적재 로직은 이와 무관하게 공유한다. 요청별 `impala_query_options` 는 SET
으로 병합된다. GP 연결은 표준 라이브러리 기반 `_GreenplumPool` 로 재사용하되 반납할 때
`DISCARD ALL` 로 세션을 초기화해, stage_insert 의 TEMP 테이블이 다음 task 와 충돌하지 않게 한다.

`exec_mode` 는 다섯 가지다. `copy` 는 psycopg COPY 로 바로 넣고, `statement` 는 받은 INSERT 를
그대로 실행하며, `stage_insert` 는 TEMP 테이블을 거친다. `local_stage` 와 `s3_stage` 는 둘 다
2-phase 구조로, executor 가 Phase 1 에서 CSV 만 만들고 **Phase 2 의 외부테이블 생성과 target
INSERT 는 coordinator 가 중앙에서 수행**한다(executor 는 GP 에 붙지 않는다).

`s3_stage` 는 Phase 1 에서 executor 가 `export_to_s3` 로 소스 → 로컬 CSV → S3 업로드까지 하고,
barrier 뒤 Phase 2 에서 coordinator 가 `load_external_s3` 로 job 프리픽스(`<prefix>/<job_id>/`)를
가리키는 PXF 외부테이블 하나를 만들어 target 으로 INSERT 한 뒤, Phase 3 에서 S3 를 정리한다.
외부테이블이 staging 을 겸하므로 heap staging 없이 external → target 으로 곧장 넣는다. insert_sql
의 staging 참조는 job 고유 외부테이블 `s3ext_<job_id>` 로 치환되고(`s3_stage.external_table_name`),
설정 `s3.external_schema` 를 주면 스키마 한정(`dwtemp.s3ext_<job_id>`)으로 만들어진다(CREATE·
INSERT 치환·DROP 이 같은 이름을 공유하며, 비우면 예전처럼 search_path 를 따른다).

두 스테이징 모드의 결정적 차이는 co-location 제약이다. `local_stage` 는 executor 와 GP 세그먼트가
같은 호스트에 있어야 하지만, `s3_stage` 는 S3 가 위치와 무관하므로 co-locate 가 필요 없다(DESIGN
§17.1). export 할 때는 impyla `convert_types=False` 로 형변환을 꺼서 timestamp/date 를 wire 문자열
그대로 받아 CSV 에 쓴다(재파싱 비용 제거). S3 업로드·삭제는 `executor/s3_client.py` (boto3 지연
임포트), SQL 조립은 `core/s3_stage.py` 의 순수 함수가 맡고 `s3.*` 설정은 coordinator 와 executor 가
공유한다.

**`app.py`** 는 task 상태머신(QUEUED → READING → WRITING → DONE/FAILED/CANCELLED)과
`executor.max_concurrent_tasks` 세마포어를 담당한다.

### 로깅

**`core/logging.py`** 는 일별 로그 보관, `[job_id][task_id]` 컨텍스트와 WARNING 전용 파일을
관리한다. `run_in_executor`는 컨텍스트를 복사하지 않으므로 백엔드 호출을
`with_log_context(fn, *args)`로 감싸 식별자를 유지한다. job 컨텍스트가 없으면 job_id 자리에
요청 식별자 `req-<id>` 를 대신 찍는다(`request_id_var`·`current_job_label`).

**`core/sqllog.py`** 는 SQL을 마스킹·길이 제한 후 INFO로 기록한다. 기본 형식(`logging.sql.format=
multiline`)은 머리줄 아래에 원문 줄을 `    | ` 를 붙여 그대로 남기는 레코드 하나이고, `oneline` 은
한 줄로 접는다. `log_template_render` 는 coordinator 가 템플릿 렌더 직후(검증 전) 조각별 SQL 을,
`log_sql_parts` 는 dry-run 계획을 같은 형식으로 남긴다. `logging.sql.enabled=false`로 끌 수 있고,
로거와 핸들러의 레벨 필터를 따른다. `datasource_of(cursor)`로 소스를 판별하며 소스 읽기, GP 실행,
미리보기를 계측한다. 검증은 `tests/test_sql_logging.py`에 있다.

**`core/http_logging.py`** 는 요청 하나를 머리줄과 본문을 담은 레코드 하나로 남기는 순수 ASGI
미들웨어다. API 호출(POST 등)과 실패 응답은 `logging.http.level`(INFO), 성공한 조회는
`read_level`(DEBUG)로 남긴다(`completion_level`). 요청마다 `X-Request-ID` 를 정해 응답 헤더로
돌려주고(들어온 값은 이어 쓴다) `request_id_var` 에 넣으며, coordinator 는 executor 호출에
`outgoing_headers()` 로 실어 보낸다. 원본은 그대로 전달하고 본문 복사본만 `max_body`까지 보관한다.
본문·헤더는 `core.masking`으로 가리며 health·metrics·정적 파일·docs는 기본 제외한다. 두 앱의
`install_http_logging`으로 등록하며 순수 포매팅 함수는 별도로 테스트한다.

### 데이터소스 미리보기(`core/dbprobe.py`)

임의 SQL 을 Impala·Greenplum·history DB 에 실행해 상위 N행을 JSON 안전 형태로 돌려주는 공용
로직이다. `fetchmany` 로 잘라 truncated 를 표시하고, PostgreSQL 은 커밋 없이 닫아 implicit
rollback 시킨다. 두 앱의 `GET /datasources` 와 `POST /datasources/{name}/query` 가 이를 호출하며,
executor 는 소스에 직접 접속하지만 coordinator 는 history 와 greenplum 만 직접 처리하고 impala 는
요청 본문의 `executor_url` 로 executor 에 프록시한다(coordinator 에는 소스 드라이버가 없다).

정형 함수 `_shape` 는 커스텀 실행 함수들이 함께 쓰는 도구라(`customs/query_funcs/*` 가 import
한다) 커서 결과뿐 아니라 **pandas DataFrame** 도 받는다. `_is_dataframe` 이 덕타이핑이라 pandas 는
의존성이 아니다. DataFrame 은 `iloc[:limit+1]` 로 자른 뒤 `itertuples(index=False)` 로 변환하는데,
`.values` 를 쓰면 혼합 dtype 이 업캐스트되어 int 가 float 이 되기 때문이다.

여기서 조심할 함정이 `_json_safe` 다. numpy 스칼라를 `tolist()` 로 낮추지 않으면 `np.int64` 가
`'7'`, `np.bool_` 가 `'True'` 로 새는데, `np.float64` 는 float 하위형이라 우연히 통과해서
**컬럼 dtype 별로 결과 타입이 갈린다**. NaN·NaT·`pd.NA`·inf 는 표준 JSON 에 표현이 없으므로
`null` 로 떨구며, 이 규칙은 모든 데이터소스에 적용된다. `trino_runner` 는
`query.func.config.dataframe_module` 이 설정되면 trino 드라이버 대신 그 커스텀
API(`query(sql, config, limit=None) -> DataFrame`)를 호출한다.

미리보기 `run`은 `limit`으로 결과를 제한하고, 이관 `fetch_all`은 전량을 읽는다.
`_call_dataframe_api`는 함수 시그니처를 확인해 `limit`을 받는 함수에만 전달한다.
커스텀 함수는 `limit=None`을 상한 없음으로 처리해야 한다. 이관 결과를 자르면 일부만 적재된다.

### 운영 도구

**`core/config_tui.py`** 는 `config.yml`에서 항목·기본값·설명을 읽어
`config.properties`를 편집하는 curses TUI다(`bin/config-tui.sh`). 변경 값만 저장하며
주석·순서를 보존하고, 저장 전 `.bak`을 만들며 비밀값은 가린다.

`ENUM_CHOICES`는 선택값을, `INT_BOUNDS`는 최소·최대·증감 폭을 정의한다. 범위 위반은
저장을 막는다. `CONCURRENCY_KEYS`는 동시성 항목을 한 탭에 모으며, `check_concurrency`는
GP 풀·task·디스패치 한도 사이의 불일치를 확인한다. `max_dispatch_concurrency=0`은
디스패치를 멈추므로 허용하지 않는다.

**`core/config_help.py`** 는 `?` 키로 여는 설정별 설명과 조정 기준이다.
`tests/test_config_help.py`는 안내에 사용한 키가 설정에 존재하는지 확인한다.

**`core/textui.py`** 는 한글을 포함한 표시 폭을 계산한다. 화면 출력에는 `cut()`,
열 정렬에는 `pad()`를 사용한다. 문자 수 기준으로 자르면 화면 경계를 넘어 curses 오류가
발생할 수 있다.

**`coordinator/tui.py`** 는 JSON API를 폴링하는 읽기 전용 모니터다(`bin/dashboard-tui.sh`).
Executor 상세는 coordinator 프록시를 거치며, 설정 목록의 index로만 대상을 선택한다.
목록 갱신 후 `clamp()`로 커서 범위를 보정한다. 스페이스는 폴링 정지, `+`/`-`는 주기 조정이다.
두 TUI는 표준 라이브러리 curses를 사용하며 순수 로직과 화면 경계를 별도로 테스트한다.

설치 스크립트는 `config/`·`templates/`·`customs/`를 최초 한 번만 복사한다. 업그레이드 시
운영자가 변경분을 비교해 반영한다. 새 설정은 `config.properties`의 값뿐 아니라
`config.yml`의 구조에도 있어야 읽힌다.

### 운영자 CLI(`src/tools/`)

서비스와 별개로 사람이 터미널에서 직접 쓰는 도구 셋이다. 이관 중에 무엇이 들어갔는지 바로
확인하거나(SQL 셸) 스테이징 객체를 정리할 때(S3 조작) 쓴다.
[DataDynamics/impala-to-whpg](https://github.com/DataDynamics/impala-to-whpg) 의 같은 이름 도구를
이 저장소에 맞춰 옮겨 온 것이다.

- **`bin/gp-shell`** 은 Greenplum 대화형 SQL 셸이다(`tools/gp_query.py --interactive`).
- **`bin/impala-shell`** 은 Impala 대화형 SQL 셸이다(`tools/impala_query.py --interactive`).
- **`bin/s3-ops`** 는 S3 객체를 올리고 내리고 지운다(`ls`·`upload`·`download`·`head`·`cp`·`mv`·
  `rm`·`rmdir`·`exists`·`mkdir`·`buckets`).
- **`bin/load-test`** 는 coordinator API 부하 테스트 도구다(`tools/load/`, `run`·`plan`·`wizard`).

**`tools/appconfig.py`** 는 공용 설정을 CLI의 `impala`·`greenplum`·`s3`·`sql` 섹션으로
변환한다. 값은 명령행 > 설정 > 기본값 순이다. `--config-dir`로 위치를 바꾸고
`--no-config`로 읽기를 생략한다. Greenplum은 psycopg 3을 사용한다.

`bin/gp-shell`과 `bin/impala-shell`은 대화형 실행용이다. 배치는
`PYTHONPATH=src .venv/bin/python -m tools.gp_query -q "SELECT 1"`처럼 실행한다.
GP의 `--dry-run`은 배치 전용이며 대화형 모드와 함께 주면 접속 전에 거절한다.
`--session-sql`은 `--schema`와 무관하게 접속마다 실행한다.

S3의 같은 버킷 내 재귀 복사·이동은 원본과 대상 프리픽스가 겹치면 거절한다.
다운로드는 전체 계획의 경로를 확인해 대상 디렉터리 밖으로 나가는 파일을 거절한다.

**`tools/load/`** 는 coordinator HTTP API로 부하를 준다. VU는 요청 완료 후 다음 요청을
보내며, `async`는 종료 상태까지 폴링하고 `sync`는 응답으로 완료를 판단한다.
`PollSpec`으로 폴링 규칙을 지정한다. 제출 TPS와 완료 TPS를 구분하고, 요청 오류와 실행
실패를 오류율에 포함하며 429는 별도로 집계한다. `/cluster`로 서버 자원을 수집하고
부하 전 baseline과 비교한다.

요청 본문은 `scenario.render_body`가 요청마다 `${...}`를 치환한다. 값끼리 짝이 맞아야 하는
템플릿 파라미터(시작일·종료일 등)는 `--data` 데이터 파일(CSV·JSONL·JSON·YAML)의 한 행에서
`${data:<컬럼>}`으로 채운다(`DataFeed`, sequential|random). 한 요청 안의 `${data:..}`는 같은 행을
보며, 시작 전에 모든 행으로 렌더해 컬럼 누락을 거절한다.

VU 감소와 drain 중에는 진행 중인 작업을 기다린다. drain 시간을 넘기면 취소하고
`ABANDONED`로 집계한다. `wizard.py`는 실행 인자를 만들고 `cli.main`을 호출한다.
화면은 `LiveRenderer`가 갱신하며 비터미널에서는 30초 간격으로 요약한다.

`analysis.py`는 VU 수준별 과도구간을 제외하고 완료 TPS와 무릎점을 계산한다.
`saturation`은 계단 부하를 만들며, 콘솔·SVG·선택적 PNG(Pillow)로 결과를 표시한다.
검증은 `tests/test_load_tool.py`, 사용법은 `src/tools/load/README.md`에 있다.

**`core/version.py`** 의 `__version__`이 버전의 단일 출처다. `pyproject.toml`도 이 값을 읽는다.
**`core/banner.py`** 는 버전·역할·포트와 실제 설정 파일의 절대 경로를 표시한다.
배너는 런처의 `.out`과 서비스 `.log`에 남으며 `config/banner.txt`가 있으면 우선 사용한다.
검증은 `tests/test_banner_version.py`에 있다.

## 설정

`config.properties`(Java 스타일 key=value)의 값으로 `config.yml` 의 `${변수:기본값}` 자리표시자를
치환해 로드한다(`src/core/config_loader.py`). 설정 디렉터리 기본값은
`/data1/distributed-query-executor/config` 이고 환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 바꾼다
(개발 시에는 `config`).

새 설정을 추가할 때 주의할 점이 하나 있다. `src/core/config.py` 의 `_get("section","key")` 는
**YAML 의 섹션 구조**를 따라 읽으므로, placeholder 이름(`${coordinator.x}`)이 아니라 실제 YAML
중첩 위치가 섹션과 일치해야 값이 반영된다. coordinator 키는 반드시 `coordinator:` 아래에 둔다.

주요 설정은 다음과 같다.

- **동시성** — `coordinator.max_concurrent_jobs`(실행 슬롯, 기본 16)와
  `coordinator.max_pending_jobs`(대기 큐, 기본 100)의 합을 넘는 요청은 429 로 거절한다. 그 아래로
  `coordinator.max_dispatch_concurrency`(task 디스패치 32),
  `executor.max_concurrent_tasks`(executor 당 8), `greenplum.pool_max`(GP 커넥션 풀, 0 이면 동시
  task 수와 동일)가 다층으로 걸린다.
- **HTTP 로깅** — `logging.http.{enabled,level,read_level,bodies,max_body,headers,exclude_paths}`
  (기본 on/INFO/DEBUG/on/8192/off/health·metrics·정적·docs). API 호출과 실패 응답은 `level`, 성공한
  조회(폴링)는 `read_level` 로 남긴다. `X-Request-ID` 는 로깅이 꺼져 있어도 붙는다.
- **실행 SQL 로깅** — `logging.sql.{enabled,format,max_length,params}`(기본 on/multiline/20000/on).
  INFO로 기록하며 로거·핸들러의 레벨 필터를 따른다. 템플릿 렌더 로그도 같은 설정을 따른다.
- **멀티 coordinator** — `store.backend=postgres` 와 공유 `history.db_dsn`,
  `executor.self_report=true` 를 함께 켠다.
- **백엔드 선택** — `build_backend` 는 **`greenplum.dsn` 하나만** 본다. 비어 있으면 경고를 남기고
  `MockBackend` 로 떨어지고, 있으면 실제 백엔드를 쓴다. `impala.host` 가 비어도 실제 백엔드로 뜨되
  소스를 읽을 수 없어 `statement` 모드만 동작한다(기동 로그에 `(미설정 → statement 모드만)`). query-execute 의 소스 실행은 이와 별개로 `/query-run` 커스텀 함수에
  위임한다(예제 `trino_runner`, 의존성 `trino` 는 이 예제 전용이라 requirements-executor.txt 에
  있다).
- **템플릿 엔진** — `template.dir`(템플릿 루트, 개발 시 `templates`), `template.enabled`,
  `template.auto_reload`(개발 편의), `template.func_modules`(커스텀 함수 모듈),
  `template.validate_ddl_single_stmt`. 의존성은 `Jinja2`(requirements.txt)다.

## 관례와 주의점

**주석과 문서는 한글로 쓴다.** 코드 주석과 docstring 은 "무엇을·왜" 중심으로 상세히 쓰되, 로그·
예외 메시지·SQL·HTML 같은 문자열 리터럴은 주석이 아니라 코드이므로 구분한다. 서술은 개조식이
아니라 **산문체**로 쓴다(명사 종결이나 화살표 나열 대신 완결된 문장).

**용어 두 가지를 구분한다.** `방언`은 **SQL dialect** 하나만 가리킨다(`query.sql_dialect`,
sqlglot 의 `hive`/`trino`/`postgres`). 문서나 모듈에서 처음 나올 때는 설정 키와 이어지도록
`방언(dialect)` 으로 병기하고, 그 뒤로는 `방언` 만 쓴다. 반면 CSV 의 delimiter·null·quote 는
방언이 아니라 **`CSV 형식`** 이라고 쓴다 — GP 쪽 SQL 이 실제로 `FORMAT 'CSV'(...)` 이고, 한
저장소에서 `방언` 이 두 뜻으로 쓰이면 executor 의 write 설정과 파서 설정이 뒤섞여 읽힌다.

**대시보드는 빌드 도구 없이 인라인 HTML 문자열로 되어 있다**(`src/coordinator/dashboard.py` 와
`src/executor/dashboard.py` 의 `DASHBOARD_HTML`). 이 문자열 안의 HTML/CSS/JS 를 고칠 때는 따옴표와
중괄호를 깨뜨리지 않도록 주의하고, 수정 뒤 `import` 로 무결성을 확인한다. 두 대시보드가 공유하는
스타일과 JS 헬퍼(포맷터·표·타임라인·모달·페이저·탭 배선·esc 이스케이프)는
`src/core/static/dashboard-common.css`/`dashboard-common.js` 에 있고 `/assets` 로 서빙된다. 공통
룩앤필과 동작은 이 두 파일만 고치고 페이지 문자열에 복사본을 되살리지 않는다
(`tests/test_offline_assets.py` 가 회귀를 막는다). 서버가 준 임의 문자열을 innerHTML 로 뿌릴 때는
반드시 `esc()`/`fmt()` 를 거친다.

**에어갭 환경이 전제이므로 웹 에셋은 모두 내장한다.** 런타임에 외부 CDN 이나 폰트로 나가면 안
된다. Swagger UI(`/docs`), ReDoc(`/redoc`), 대시보드 폰트(Roboto Condensed)는 전부
`src/core/static/` 에 vendoring 했고 `src/core/webassets.py`(`mount_static`/
`register_offline_docs`)가 `/assets` 로 서빙한다. 두 앱은 `FastAPI(docs_url=None,
redoc_url=None)` 로 CDN 기반 기본 docs 를 끄고 이 헬퍼로 다시 등록한다. 대시보드 HTML 에
`fonts.googleapis.com` 같은 외부 `<link>` 를 다시 넣지 않는다(역시
`tests/test_offline_assets.py` 가 막는다).

**상태가 인메모리이므로 단일 워커로 돌린다.** coordinator 와 executor 모두 `workers=1` 이고,
처리량 확장은 executor 인스턴스 수로 한다.

비동기 디스패처에서 블로킹 DB 호출(impyla/psycopg)은 `run_in_executor` 나 `to_thread` 로 감싸
이벤트 루프를 막지 않는다. 새 기능에는 `tests/` 에 테스트를 추가하되, 실제 DB 없이
`MockBackend`/`FakeRunner` 로 검증한다.

**메타 테이블은 모두 스키마 한정된다.** jobs·job_history·task_history·executor_status·
executor_health_metrics·coordinator_status·executor_reservation 의 테이블명은 `db.schema`(기본
`public`)로 한정되며, `src/core/config.py` 의 `_qualify_table()` 이 설정에서 읽은 이름을
`public.<t>` 로 만든다(이미 `.` 로 한정된 값은 그대로 둔다). 각 repo 의 `self.table` f-string 이
이 값을 그대로 쓰므로 앱 런타임 SQL 과 두 DDL 파일이 같은 스키마를 가리킨다. 테이블이나 스키마를
바꾸면 **설정과 DDL 두 파일을 함께** 고친다.

**메타 저장소 스키마는 두 벌이다.** `config/postgresql.sql`(PostgreSQL)과
`config/warehousepg.sql`(WarehousePG/Greenplum 7 = PG12)이며, 테이블이나 컬럼을 바꾸면 **두 파일을
함께** 고쳐야 한다. WarehousePG 판은 테이블마다 `DISTRIBUTED BY` 가 붙고 PK 가 분산키를 포함해야
하며, history 와 metrics 는 대리 PK 를 빼고 `job_id`/`executor_url` 로 co-locate 한다. 앱이 쓰는
SQL(`ON CONFLICT`·`JSONB`·`DISTINCT ON`)은 GP7 = PG12 라 양쪽에서 공통으로 동작한다.

## Git / PR

커밋 메시지는 한글로 쓰고, 사용자가 명시적으로 요청할 때만 커밋하거나 푸시한다. 기본 브랜치는
`main` 이고 원격은 `DataDynamics/distributed-query-executor` 다.
