# CLAUDE.md

이 저장소에서 작업하는 Claude Code(및 기타 에이전트)를 위한 안내다. 개요와 빠른 시작은
[README.md](README.md), 설계 심화는 [docs/DESIGN.md](docs/DESIGN.md), 실행 모드별 사용법은
[docs/GUIDE.md](docs/GUIDE.md), 배포는 [docs/DEPLOY.md](docs/DEPLOY.md)를 참고한다.

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

# 테스트 (실제 DB 불필요 — MockBackend/FakeRunner 사용). 현재 626개(+pandas 미설치 시 5 skip).
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
  tools/       # 운영자용 CLI(gp-shell·impala-shell·s3-ops). 서비스와 별개로 사람이 직접 쓴다
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
DELETE 한 뒤 넣으므로(`stage.py`/`backend.py`) 재실행해도 중복되지 않는다. `append` 는 멱등하지
않다.

## 핵심 모듈

### coordinator

**`dispatcher.py` 는 동시성의 중심이다.** `JobAdmission` 이 실행 슬롯과 대기 큐를 관리하며 초과
시 429 를 내고, `_DispatcherBase` 가 PENDING → 슬롯 대기 → RUNNING → 종료의 수명주기와 in-flight
반납·취소 감지를 맡는다. 원격 실행은 `HttpDispatcher`, in-process 실행은 `LocalDispatcher` 이며
하위 클래스는 `_execute(job)` 하나만 구현하면 된다.

**`parser.py`** 는 sqlglot 으로 SELECT 를 검증한다. `strict_validation=true` 면 단순 SELECT 만
받고, `false` 면 JOIN·서브쿼리·GROUP BY 같은 복합 쿼리도 허용하면서 파티션 `IN` 절을 트리 어디에
있든 찾아낸다.

**`splitter.py`** 는 IN 값을 N등분하고(contiguous/round_robin) 원문 포맷을 보존한 채 치환한다.
여기에 **날짜 fan-out**(DESIGN §18.8)이 붙어 있다. `/jobs` 에 `task_params`(구간의 두 끝을 담은
파라미터 이름 두 개)를 주면 IN 분할 대신 하루를 task 하나로 펼친다(`app.py` 의 `_build_fanout`,
`_compute_task_offsets` — IN 파싱과 split 을 우회한다).

구간은 각 파라미터의 값과 `sign` 에서 도출하는데, 여기서 **`sign` 은 값의 부호가 아니라 SQL
연산자의 방향**이라는 점이 중요하다. Impala 의 `interval` 은 절대값만 받기 때문에
`- interval 7 day` 처럼 방향이 SQL 문에 박히기 때문이다. 템플릿에는 `<name>_sign` 으로 노출되고
`sql_sign` 필터가 `+`/`-` 외의 값을 막는다. task 마다 두 파라미터를 같은 날로 좁혀 렌더하므로
BETWEEN 이 하루로 붕괴하고 값은 언제나 절대값이 된다. `task_bound` 는 `point`(기본,
`(d,d)` — BETWEEN 이나 `=` 처럼 양끝 포함)와 `pair`(`(d,d+1)` — 반열림 `>=`/`<`) 중 고른다.

부호 변수를 쓰지 않는 템플릿은 Jinja2 AST 검사로 422(`TEMPLATE_MISSING_SIGN_VAR`)를 낸다. 막지
않으면 각 task 가 의도보다 넓은 구간을 읽어 **조용히 중복 적재**되기 때문이다. INSERT 와 staging
조각은 날짜와 무관하므로 한 번만 렌더해 job 전체가 공유한다. 이 기능은 stage_insert 전용이고
append 로 적재하므로(프레임워크가 대상에 DELETE 를 하지 않는다) 멱등이 필요하면 대상을 미리
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

**`job_store.py`** 는 단일 coordinator 용 `InMemoryJobStore` 와 멀티 coordinator 용
`SqlJobStore`(JSONB)를 제공한다.

### 결과 반환 실행(`POST /query-execute`)

같은 템플릿을 `render_query()` 로 select 조각만 렌더해 실행하고 상위 N행을 동기로 돌려주는
미리보기성 API다. coordinator 는 `/jobs` 와 같은 정책으로 가장 한가한 executor 를 골라
프록시하므로 클라이언트는 executor 의 존재를 모른다. greenplum 과 history 만 coordinator 가 직접
실행한다. `params` 는 이름-값 항목 배열이고, 응답의 `executed_by` 에 실제 실행한 executor 가
담긴다(직접 실행이면 null).

소스 실행은 impala·trino·source 구분 없이 **executor 의 `/query-run` 하나로 통일**돼 있다.
executor 는 `query.func.module`(dotted path, importlib 로 로딩) 함수를 `run(sql, config, limit)`
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
배리어 뒤 Phase 2 에서 coordinator 가 `load_external_s3` 로 job 프리픽스(`<prefix>/<job_id>/`)를
가리키는 PXF 외부테이블 하나를 만들어 target 으로 INSERT 한 뒤, Phase 3 에서 S3 를 정리한다.
외부테이블이 staging 을 겸하므로 heap staging 없이 external → target 으로 곧장 넣는다. insert_sql
의 staging 참조는 job 고유 외부테이블 `s3ext_<job_id>` 로 치환되고(`s3_stage.external_table_name`),
설정 `s3.external_schema` 를 주면 스키마 한정(`dwtemp.s3ext_<job_id>`)으로 만들어진다(CREATE·
INSERT 치환·DROP 이 같은 이름을 공유하며, 비우면 예전처럼 search_path 를 따른다).

두 스테이징 모드의 결정적 차이는 배치 제약이다. `local_stage` 는 executor 와 GP 세그먼트가 같은
호스트에 있어야 하지만, `s3_stage` 는 S3 가 위치와 무관하므로 co-locate 가 필요 없다(DESIGN
§17.1). export 할 때는 impyla `convert_types=False` 로 형변환을 꺼서 timestamp/date 를 wire
문자열 그대로 받아 CSV 에 쓴다(재파싱 비용 제거). S3 업로드·삭제는 `executor/s3_client.py`(boto3
지연 임포트), SQL 조립은 `core/s3_stage.py` 의 순수 함수가 맡고 `s3.*` 설정은 coordinator 와
executor 가 공유한다.

**`app.py`** 는 task 상태머신(QUEUED → READING → WRITING → DONE/FAILED/CANCELLED)과
`executor.max_concurrent_tasks` 세마포어를 담당한다.

### 로깅

**`core/logging.py`** 는 일 단위 롤링, `[job_id][task_id]` 컨텍스트 주입, WARNING 전용
로그(`*-warn.log`) 분리를 담당한다. 식별자는 `contextvars`(`job_id_var`/`task_id_var`)와 record
factory 로 모든 레코드에 자동으로 붙는다. 여기에 `with_log_context(fn, *args)` 가 있는데, 이
컨텍스트를 워커 스레드까지 들고 가는 콜러블을 만든다. `loop.run_in_executor` 는
`asyncio.to_thread` 와 달리 contextvars 를 복사하지 않아서, 감싸지 않으면 백엔드 스레드의
로그(특히 실행 SQL)가 `[-][-]` 로 남아 어느 job 의 쿼리인지 잃는다.

**`core/sqllog.py`** 는 실행 SQL 로깅이다(`core.sql` 로거). 데이터소스에 던지는 모든 SQL 을
`SQL 실행 datasource=<엔진> phase=<단계> [target=…] | <SQL> [| params=…]` 한 줄로 남긴다. HTTP
로깅과 달리 DEBUG 를 요구하지 않고 **INFO 로 항상** 기록하는데, 운영 기본 레벨에서 "무엇을 읽어
무엇을 적재했나"가 비어 있으면 사고 추적이 불가능하기 때문이다(끄려면
`logging.sql.enabled=false`). SQL 은 마스킹 → 공백 접기(한 줄 = 한 레코드 유지) → `max_length`
절단 순으로 가공하고, 잘리면 `… (총 N자 중 M자 절단)` 을 붙여 전문이 아님을 숨기지 않는다.
datasource 는 `datasource_of(cursor)` 가 커서에서 추론하므로(커스텀 어댑터는 `_name`, impyla
커서는 속성이 없어 `impala`) `_source_execute` 시그니처를 바꾸지 않고도 소스별 표기가 갈린다.
계측 지점은 소스 SELECT 전부(`_source_execute` 한 곳), GP 쪽 실행문 전부, `dbprobe` 미리보기,
executor 의 `POST /query-run` 이다. 검증은 `tests/test_sql_logging.py` 에 있다.

**`core/http_logging.py`** 는 HTTP 요청/응답을 DEBUG 로 남기는 미들웨어다(`core.http` 로거).
`logger.isEnabledFor(DEBUG)` 가드로 DEBUG 가 아니면 즉시 통과하므로 오버헤드가 사실상 없다.
BaseHTTPMiddleware 가 아닌 **순수 ASGI 미들웨어**로 `receive`/`send` 를 엿보기만 하기 때문에
다운스트림 본문 읽기를 깨지 않으며, 본문 복사본은 `max_body` 까지만 보관하고 원본은 그대로
전달한다. 본문과 헤더는 `core.masking` 으로 마스킹하고 health·metrics·정적·docs 같은 잡음 경로는
기본 제외한다. 등록은 두 `create_app` 의 `install_http_logging(app, settings)` 이고, 순수 함수는
`tests/test_http_logging.py` 에서 검증한다.

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
API(`query(sql, config, limit) -> DataFrame`)를 호출한다.

### 운영 도구

**`core/config_tui.py`** 는 config.properties 를 편집하는 curses 설정 TUI 다(`python -m
core.config_tui`, `bin/config-tui.sh`). `config.yml` 을 파싱해 항목·기본값·설명·enum 을 자동
추출하므로 스키마를 하드코딩하지 않으며, 바꾼 값만 주석과 순서를 보존해 diff-write 한다(저장 전
`.bak` 백업, 비밀값 마스킹).

**`coordinator/tui.py`** 는 대시보드의 읽기 전용 curses 모니터다(`python -m coordinator.tui`,
`bin/dashboard-tui.sh`). 웹 대시보드와 같은 JSON API(`/cluster`·`/jobs`·`/history`·`/info`)를
폴링해 그리며 HTML 을 스크래핑하지 않는다. 개별 executor 상세는 coordinator 프록시(app.py 의
`_proxy_executor_get`)로 가져오므로 coordinator 한 곳만 붙어도 executor 화면까지 볼 수 있는데,
executor 를 설정 목록의 index 로만 지정하는 allowlist 방식이라 SSRF 가 막힌다(`/datasources`
프록시와 같은 관례). 두 TUI 모두 순수 로직이 curses 와 무관해 테스트할 수 있고, 에어갭을 고려해
표준 라이브러리 curses 만 쓴다.

업그레이드 때 설정을 반영하는 자동화는 두지 않는다. config/·templates/·customs/ 는 install.sh 의
rsync 에서 제외되고 최초 1회만 시딩되므로 재설치만으로는 새 버전의 변경이 들어가지 않는데, 이를
`diff` 로 확인해 운영자가 직접 옮긴다(절차는 docs/DEPLOY.md). 여기서 놓치기 쉬운 것이
**`config.yml` 을 교체해야 새 버전이 추가한 설정 구조가 반영된다**는 점이다. config.yml 은 값이
아니라 `${변수:기본값}` 자리를 담은 구조라, 자리가 없으면 properties 에 값을 적어도 조용히
무시된다.

### 운영자 CLI(`src/tools/`)

서비스와 별개로 사람이 터미널에서 직접 쓰는 도구 셋이다. 이관 중에 무엇이 들어갔는지 바로
확인하거나(SQL 셸) 스테이징 객체를 정리할 때(S3 조작) 쓴다.
[DataDynamics/impala-to-whpg](https://github.com/DataDynamics/impala-to-whpg) 의 같은 이름 도구를
이 저장소에 맞춰 옮겨 온 것이다.

- **`bin/gp-shell`** 은 Greenplum 대화형 SQL 셸이다(`tools/gp_query.py --interactive`).
- **`bin/impala-shell`** 은 Impala 대화형 SQL 셸이다(`tools/impala_query.py --interactive`).
- **`bin/s3-ops`** 는 S3 객체를 올리고 내리고 지운다(`ls`·`upload`·`download`·`head`·`cp`·`mv`·
  `rm`·`rmdir`·`exists`·`mkdir`·`buckets`).

**핵심은 설정을 새로 만들지 않은 것이다.** 원본은 자체 `conf/config.yaml` 을 읽지만, 그대로 옮기면
같은 접속 정보를 두 곳에 적어야 하고 한쪽만 고쳐서 어긋나는 사고가 난다. 그래서
`tools/appconfig.py` 가 이 저장소의 `config.properties`/`config.yml` 을 읽어 도구가 기대하는
섹션(`impala`/`greenplum`/`s3`/`sql`) 모양으로 바꿔 준다. Greenplum 만 형태가 달라서
(이 저장소는 `greenplum.dsn` 한 줄로 들고 있다) `parse_dsn` 이 DSN 을 host·port·user 로 풀어 준다.
우선순위는 명령행 인자 > 설정 > 기본값이고, `--config-dir` 로 다른 디렉터리를, `--no-config` 로
설정 무시를 고를 수 있다.

드라이버도 서비스 쪽과 맞췄다. 원본의 psycopg2 대신 executor 백엔드가 쓰는 **psycopg 3** 을 쓴다.
impyla 와 boto3 는 이미 `requirements-executor.txt` 에 있으므로 추가 의존성이 없다.

`bin/` 래퍼는 대화형 셸만 노출하지만 모듈 자체는 한 번 실행도 지원한다. 배치로 쓰려면
`PYTHONPATH=src python -m tools.gp_query -q "SELECT 1" -o out.csv` 처럼 직접 부른다.

**`core/version.py` 와 `core/banner.py`** 는 버전 단일 소스와 기동 배너를 맡는다. 버전은
`version.py` 의 `__version__` 한 줄이 유일한 출처이고 `pyproject.toml` 이 `dynamic` + `attr` 로
그 값을 읽으므로 버전을 올릴 때는 이 한 줄만 고친다. 배너는 ASCII 아트와 버전·역할·포트에 더해
`print_config_sources` 로 **실제 로딩한 `config.properties`·`config.yml` 의 절대 경로**를 찍어,
설정이 제대로 잡혔는지 콘솔에서 바로 확인하게 한다(파일이 없으면 `← 파일 없음(로딩 실패)!` 마커가
붙는다). 배너는 `.out` 과 `.log` 양쪽에 남는다. stdout 은 런처(`bin/env.sh`)가 `logs/<name>.out`
으로 리다이렉트하고, `setup_logging` 뒤에 `banner.log_startup()` 이 같은 내용을 `.log` 에도 한
레코드로 남긴다(첫 줄은 grep 용 `<role> 기동 (version=… port=…)` 요약이다). 설정 디렉터리에
`banner.txt` 가 있으면 그쪽을 우선한다. 순수 렌더 함수는 `tests/test_banner_version.py` 에서
검증한다.

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
- **HTTP 로깅** — `logging.http.{enabled,bodies,max_body,headers,exclude_paths}`(기본 on/on/
  2048/off/health·metrics·정적·docs). 별도 스위치가 아니라 **로그 레벨이 DEBUG 일 때만** 기록되며,
  `enabled=false` 로 DEBUG 여도 끌 수 있다.
- **실행 SQL 로깅** — `logging.sql.{enabled,max_length,params}`(기본 on/4000/on). HTTP 로깅과 달리
  로그 레벨과 무관하게 INFO 로 항상 남는다.
- **멀티 coordinator** — `store.backend=postgres` 와 공유 `history.db_dsn`,
  `executor.self_report=true` 를 함께 켠다.
- **백엔드 선택** — `impala.host` 와 `greenplum.dsn` 이 둘 다 있으면 실제 백엔드를, 하나라도 비면
  `MockBackend` 를 쓴다. query-execute 의 소스 실행은 이와 별개로 `/query-run` 커스텀 함수에
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
