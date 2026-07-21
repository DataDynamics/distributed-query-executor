# CLAUDE.md

이 저장소에서 작업하는 Claude Code(및 기타 에이전트)를 위한 안내. 자세한 사용법은
[README.md](README.md), 설계는 [DESIGN.md](docs/DESIGN.md), 배포는 [packaging/README.md](packaging/README.md) 참고.

## 프로젝트 개요

Coordinator + N Executor 구조의 분산 쿼리 실행기. 하나의 Impala `SELECT` 를 파티션 컬럼의
`IN` 목록 기준으로 N분할해 병렬로 읽고, 각 executor 가 결과를 Greenplum 에 적재한다
(**Impala → Greenplum 이관**). 데이터는 coordinator 를 거치지 않고 executor 가 직접 흘려보내며,
coordinator 로는 상태와 row count 만 흐른다.

## 명령어

```bash
# 가상환경(파이썬 3.9+, RHEL 9.2 기본 python3). 이 저장소는 .venv 를 사용한다.
python3.9 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt        # coordinator + 테스트 의존성

# 테스트 (실제 DB 불필요 — MockBackend/FakeRunner 사용). 현재 441개.
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest tests/test_admission.py -q # 특정 파일만

# 로컬 실행 (저장소 기본 설정 사용). 소스가 src/ 아래라 PYTHONPATH=src 필요(pytest 는 conftest.py 가 처리).
PYTHONPATH=src QUERY_EXECUTOR_CONFIG_DIR=config EXECUTOR_PORT=8001 .venv/bin/python -m executor &
PYTHONPATH=src QUERY_EXECUTOR_CONFIG_DIR=config .venv/bin/python -m coordinator

# local 모드(별도 executor 없이 coordinator 안에서 직접 실행)
COORDINATOR_EXECUTOR_MODE=local PYTHONPATH=src QUERY_EXECUTOR_CONFIG_DIR=config .venv/bin/python -m coordinator
```

> 셸에 `python` 이 없을 수 있다(pyenv). 항상 `.venv/bin/python` 을 명시적으로 쓴다.

## 아키텍처

```
src/
  core/        # 공용: 설정 로더/설정/로깅/메트릭 (coordinator·executor 공유)
  coordinator/ # FastAPI: 검증(parser) → 분할(splitter) → admission → 디스패치 → 상태 추적
  executor/    # FastAPI: Impala 읽기 → Greenplum 적재(backend), task 상태 노출
bin/           # 런처·설치 스크립트(install/start/stop/status·env·check-prereqs·config-tui·dashboard-tui·migrate-config — /data1 배포 트리와 공용)
config/        # config.properties + config.yml 기본값 + 스키마(postgresql.sql / warehousepg.sql)
templates/     # 쿼리 템플릿(<template_id>/manifest.yml + *.sql.j2) — config 와 같은 레벨의 별도 디렉터리
customs/       # 사이트 커스텀 코드(customs.query_funcs.* — 커스텀 쿼리 함수, src 밖 최상위 패키지)
packaging/     # 배포·패키징: README.md(배포 안내) + wheels/(에어갭 휠 번들 py39·py311). 설치는 bin/install.sh
tests/         # pytest (coordinator·executor 검증/라이프사이클/admission/대시보드)
```

소스는 src 레이아웃이다: 패키지 임포트명은 그대로 `core`/`coordinator`/`executor` 이고
(`pyproject.toml` 의 `package-dir`), 실행 시 `PYTHONPATH=src` 가 필요하다(테스트는
루트 `conftest.py` 가 `src/` 와 저장소 루트를 sys.path 에 넣는다).

요청 흐름: `POST /jobs` → parser 검증 + splitter 분할(동기, 실패 시 즉시 4xx) →
admission `try_admit`(초과 시 429) → Job 생성(SPLITTING) → 백그라운드 `run()` 이 슬롯
대기(PENDING) 후 RUNNING → executor 에 `POST /tasks` 병렬 디스패치 + polling → `finalize_job`
으로 종료 상태 집계(DONE/PARTIAL/FAILED/CANCELLED).

### 핵심 모듈
- `src/coordinator/dispatcher.py` — **동시성의 중심**. `JobAdmission`(실행 슬롯 + 대기 큐, 429),
  `_DispatcherBase`(PENDING→슬롯대기→RUNNING→종료 + in-flight 반납 + 취소 감지),
  `HttpDispatcher`(원격), `LocalDispatcher`(in-process). 하위 클래스는 `_execute(job)` 만 구현.
- `src/coordinator/parser.py` — sqlglot 검증. `strict_validation=true`(단순 SELECT) vs
  `false`(JOIN/서브쿼리/GROUP BY 등 복합 쿼리에서 파티션 `IN` 절을 트리 어디서든 탐색).
- `src/coordinator/template.py` + `template_funcs.py` — **쿼리 템플릿 엔진**. 클라이언트가 SQL
  전문 대신 `template_id`+`params` 를 보내면, 서버 템플릿(`template.dir/<id>/manifest.yml` +
  `*.sql.j2`)을 Jinja2 `SandboxedEnvironment` 로 렌더해 SELECT/STAGING DDL/INSERT 를 만들고
  기존 요청 필드에 주입한다(이후 parser→splitter→dispatch 무변경). 커스텀 함수는
  `@template_filter`/`@template_global` 레지스트리(내장 `sql_str`/`sql_in`/`sql_ident`/`sql_num`/
  `date_range`) + 설정 `template.func_modules` 로 확장. `template_id` 미지정 시 기존 raw-SQL
  방식 그대로(하위 호환). 예제: `templates/sales_migration/`. 자세히는 DESIGN §18.
  **결과 반환 실행**(`POST /query-execute`, DESIGN §18.7): 같은 템플릿을 `render_query()`(select
  조각만 렌더)로 SELECT 만 만들어 실행하고 결과(상위 N행)를 동기 반환한다. coordinator 가 `/jobs` 와
  동일 정책으로 **가장 한가한 executor 를 골라 프록시**(클라이언트는 executor 를 모름), greenplum/
  history 는 직접 실행. `params` 는 이름-값 항목 배열. 응답에 `executed_by`(실행 executor, 직접이면 null).
  **소스 실행은 executor `/query-run`(커스텀 함수)로 통일** — impala/trino/source 구분 없이 coordinator 가
  `/query-run` 하나로 프록시(greenplum/history 만 coordinator 직접). executor 가 `query.func.module`(dotted
  path, importlib 로딩) 함수를 `run(sql, config, limit)` 로 호출. `config` 는 `query.func.config.*` 를
  프리픽스로 모은 자유 설정 dict(`src/core/config.py` 의 `_collect_prefix`, raw properties 기반 — YAML 무관).
  참조 구현·설정은 QUERY.md / `customs/query_funcs/trino_runner.py`. 임의 SQL 미리보기(`/datasources/
  {name}/query`)는 별개 운영 점검용으로 built-in 유지.
- `src/coordinator/splitter.py` — IN 값 N등분(contiguous/round_robin), 원문 포맷 보존 치환.
  **날짜 태스크 컬럼 fan-out**(DESIGN §18.8): `/jobs` 에 `task_column`+`task_range`(오늘 기준 상대
  일수, 양끝 포함)를 주면 IN 분할 대신 **날짜=1 task** 로 펼친다(`app.py` `_build_fanout`/`_compute_task_dates`,
  IN 파싱·split 우회). 날짜별 SELECT 만 `render_query` 로 렌더, INSERT/staging 은 날짜 독립이라 1회
  렌더해 job 공유. stage_insert 전용이며 **append** 적재(프레임워크는 대상에 DELETE 안 함 — 멱등이
  필요하면 대상 선비우기/날짜별 물리 테이블). 예제: `templates/daily_sales/`.
- `src/coordinator/stage.py` — **`local_stage`(file:// 세그먼트 로컬 스테이징) Phase 2 SQL 조립**(순수
  함수): `file://` 외부테이블 DDL·staging 적재·멱등 DELETE·정리 SQL, 파일 예산 배분
  (`plan_file_budget`, 호스트당 ≤ S_h), executor_url→gp_hostname 유도. 자세히는 DESIGN §17.
- `src/coordinator/job_store.py` — `InMemoryJobStore`(단일) / `SqlJobStore`(멀티 coordinator, JSONB).
- `src/executor/backend.py` — `ImpalaToGreenplumBackend`(소스 impyla → psycopg) + `MockBackend`.
  소스는 Impala 전용이다. 소스 접속은 `_source_connect`/`_open_source_cursor`/`_source_execute` 에
  모여 있고 스트리밍/적재 로직은 이와 무관하게 공유한다. 요청별 `impala_query_options` 는 SET 으로 병합된다.
  GP 연결은 `_GreenplumPool`(표준 라이브러리 기반)로 재사용하며, 반납 시 `DISCARD ALL` 로
  세션을 초기화해 stage_insert 의 TEMP 테이블이 다음 task 와 충돌하지 않게 한다.
  `exec_mode`: `copy`(COPY) / `statement`(INSERT 그대로 실행) / `stage_insert`(TEMP 경유) /
  `local_stage`(executor 가 로컬 CSV export → coordinator 가 `file://` 외부테이블로 세그먼트
  로컬 병렬 read → target INSERT, 2-phase). local_stage 는 executor 를 GP 세그먼트 호스트에
  co-locate 해야 한다(DESIGN §17). export fetch 는 형변환을 꺼 timestamp/date 를 wire 문자열
  그대로 받아 CSV 로 쓴다(재파싱 비용 제거 — impyla `convert_types=False`).
- `src/executor/app.py` — task 상태머신(QUEUED→READING→WRITING→DONE/FAILED/CANCELLED),
  `executor.max_concurrent_tasks` 세마포어.
- `src/core/logging.py` — 일 단위 롤링 + `[job_id][task_id]` 컨텍스트 주입 + **WARNING 전용
  로그(`*-warn.log`) 분리**.
- `src/core/http_logging.py` — **HTTP 요청/응답 DEBUG 로깅 미들웨어**(`core.http` 로거). 로그
  레벨이 DEBUG 일 때만(`logger.isEnabledFor(DEBUG)` 가드) 각 요청/응답을 남기고, 아니면 즉시
  통과(오버헤드 ~0). **순수 ASGI 미들웨어**(BaseHTTPMiddleware 아님) — `receive`/`send` 를 감싸
  엿보기만 해 다운스트림 본문 읽기를 깨지 않고, 본문 복사본은 `max_body` 까지만 보관(원본은 그대로
  전달 → 스트리밍/대용량도 로그만 절단). 본문·헤더는 마스킹(`core.masking` 의 `mask_text`/
  `mask_header_value`), 잡음 경로(health/metrics/정적/docs) 기본 제외. 두 `create_app` 이
  `install_http_logging(app, settings)` 로 등록. 순수 함수(`format_body`/`format_headers`/
  `is_excluded`)는 테스트 대상(`tests/test_http_logging.py`).
- `src/core/dbprobe.py` — **데이터소스 SELECT 미리보기/연결 테스트 공용 로직**. 임의 SQL 을
  Impala/Greenplum/history DB 에 실행해 상위 N행을 JSON 안전 형태로 반환(`fetchmany` 로
  잘라 truncated 표시, PostgreSQL 은 커밋 없이 닫아 implicit rollback). 두 앱의
  `GET /datasources` + `POST /datasources/{name}/query` 엔드포인트가 이를 호출한다.
  executor 는 소스들을 직접 접속하고, coordinator 는 history/greenplum 만 직접·impala 는
  요청 본문 `executor_url` 로 executor 에 프록시한다(coordinator 에는 소스 드라이버가 없음).
- `src/core/config_tui.py` — **config.properties 편집용 curses 설정 TUI**(`python -m
  core.config_tui`, `bin/config-tui.sh`). `config.yml` 을 파싱해 항목·기본값·설명·enum 을 자동
  추출(스키마 하드코딩 없음)하고, 바꾼 값만 주석·순서를 보존해 diff-write 한다(저장 전 `.bak`
  백업, 비밀값 마스킹). 순수 로직은 curses 무관(테스트 대상), 에어갭 stdlib curses.
- `src/coordinator/tui.py` — **coordinator 대시보드의 읽기 전용 curses 모니터**(`python -m
  coordinator.tui`, `bin/dashboard-tui.sh`). 웹 대시보드와 같은 JSON API(`/cluster`·`/jobs`·
  `/history`·`/info`)를 폴링해 텍스트 UI 로 그린다(HTML 스크래핑 아님). 개별 executor 상세는
  **coordinator 프록시**(`GET /executors/{idx}/tasks`·`/metrics`·`/tasks/{tid}/detail`, app.py
  `_proxy_executor_get`)로 가져와 **coordinator 한 곳만** 붙어도 executor 화면까지 본다(executor 는
  설정목록 index=allowlist 로만 지정 → SSRF 방지, `/datasources` 프록시와 동일 관례). `/cluster`·
  `/executors` 엔트리에 드릴인 키 `index` 를 붙인다(`_annotate_executor_index`). 순수 포매터는
  curses 무관(테스트 대상), config-tui 와 같은 에어갭 stdlib curses.
- `src/core/config_migrate.py` — **업그레이드용 설정 마이그레이션**(`python -m core.config_migrate`,
  `bin/migrate-config.sh`). 기존 설치 경로의 config.properties 에서 운영자 변경분(새 기본값과
  다른 값 + 새 파일에 없는 키)을 찾아 새 기본 config/config.properties 위에 얹어 기록한다
  (`merge_properties_lines` 재사용 — 주석·순서 보존, 없는 키는 마커 아래 추가, `.bak` 백업,
  `--dry-run` 보고, 비밀값은 보고에서 마스킹).
- `src/core/version.py` + `src/core/banner.py` — **버전 단일 소스 + 기동 배너**. 버전은
  `version.py` 의 `__version__` **한 줄이 유일 소스**이고, `pyproject.toml` 이 `dynamic`
  + `attr` 로 그 값을 읽는다(버전 올릴 때 이 한 줄만 수정). `banner.py` 는 coordinator/
  executor 가 뜰 때 Spring Boot 식 ASCII 배너(버전·역할·포트)를 stdout 에 찍고, 이어서
  `print_config_sources` 로 **실제 로딩한 `config.properties`·`config.yml` 의 절대 경로**(+ 파일
  없으면 `← 파일 없음(로딩 실패)!` 마커)를 찍어 설정 로딩 여부를 콘솔에서 바로 확인하게 한다
  (경로는 `settings.config_properties_path`/`config_yaml_path`). 설정 디렉터리에 `banner.txt` 가
  있으면 그걸 우선(자리표시자 `${version}`/`${role}`/`${port}`/`${python}` 치환).
  `python -m coordinator|executor --version` 은 버전만 출력하고 종료. 배포 트리엔 `.git` 이
  없어 git sha 는 있으면 `+g<sha>` 로 붙이는 best-effort(환경변수 `QUERY_EXECUTOR_GIT_SHA` 로 각인 가능).
  **배너는 `.out` 과 `.log` 양쪽에 남는다**: stdout `print()` 는 런처(`bin/env.sh`)가
  `logs/<name>.out` 으로 리다이렉트하고, `setup_logging` 뒤 `banner.log_startup()` 이 같은 배너
  전체(아트 + 버전 + 설정 절대 경로)를 `.log` 에도 한 레코드로 남긴다(첫 줄은 grep 용 요약
  `<role> 기동 (version=… port=…)`). 순수 렌더 함수(`render_banner`/`render_config_sources`)는
  I/O 무관이라 테스트 대상(`tests/test_banner_version.py`).

## 설정

`config.properties`(Java 스타일 key=value)의 값으로 `config.yml` 의 `${변수:기본값}`
자리표시자를 치환해 로드한다(`src/core/config_loader.py`). 설정 디렉터리는
`/data1/distributed-query-executor/config`(환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 변경, 개발 시 `config`).

- `src/core/config.py` 의 `_get("section","key")` 는 **YAML 의 섹션 구조**를 따라 읽는다. 새 설정을
  추가할 때 placeholder 이름(`${coordinator.x}`)이 아니라 **실제 YAML 중첩 위치**가 섹션과
  일치해야 값이 반영된다(coordinator 키는 `coordinator:` 아래에 둘 것).
- 동시성: `coordinator.max_concurrent_jobs`(실행 슬롯 16) + `coordinator.max_pending_jobs`(대기
  큐 100) → 합 초과 시 429 / `coordinator.max_dispatch_concurrency`(task 디스패치 32) /
  `executor.max_concurrent_tasks`(executor당 8) / `greenplum.pool_max`(GP 커넥션 풀, 0=동시 task 수와 동일).
- HTTP 로깅: `logging.http.{enabled,bodies,max_body,headers,exclude_paths}`(기본 on/on/2048/off/
  health·metrics·정적·docs). **로그 레벨이 DEBUG 일 때만** 요청/응답이 기록된다(별도 스위치 아님 —
  `enabled=false` 로 DEBUG 여도 끌 수 있음). 자세히는 `src/core/http_logging.py`.
- 멀티 coordinator: `store.backend=postgres` + 공유 `history.db_dsn`, `executor.self_report=true`.
- 백엔드: `impala.host` + `greenplum.dsn` 둘 다 있으면 실제 백엔드, 아니면 `MockBackend`
  (소스는 Impala 전용). query-execute 의 소스 실행은 별개로 `/query-run` 커스텀 함수에 위임한다(예제
  `trino_runner`, 의존성 `trino` 는 이 예제용 — requirements-executor.txt).
- 템플릿 엔진: `template.dir`(템플릿 루트, 개발 `templates`) / `template.enabled` /
  `template.auto_reload`(개발 편의) / `template.func_modules`(커스텀 함수 모듈) /
  `template.validate_ddl_single_stmt`. 의존성 `Jinja2`(requirements.txt).

## 관례 / 주의점

- **주석/문서는 한글**. 코드 주석과 docstring 은 "무엇을·왜" 중심으로 상세히 쓴다(로그·예외·
  SQL·HTML 같은 문자열 리터럴은 코드이므로 주석이 아니다 — 구분할 것).
- **대시보드는 빌드 도구 없는 인라인 HTML 문자열**(`src/coordinator/dashboard.py`,
  `src/executor/dashboard.py` 의 `DASHBOARD_HTML`). 이 문자열 내부 HTML/CSS/JS 를 수정할 때 따옴표/
  중괄호를 깨뜨리지 않도록 주의하고, 수정 후 `import` 로 무결성을 확인한다.
  두 대시보드가 공유하는 스타일/JS 헬퍼(포맷터·표·타임라인·모달·페이저·탭 배선·esc 이스케이프)는
  `src/core/static/dashboard-common.css`/`dashboard-common.js` 에 있다(`/assets` 서빙). 공통 룩앤필/
  동작은 이 두 파일만 고치고, 페이지 문자열에 복사본을 되살리지 말 것(회귀 방지:
  `tests/test_offline_assets.py`). 서버가 준 임의 문자열을 innerHTML 로 뿌릴 땐 `esc()`/`fmt()` 를 거친다.
- **에어갭(외부 차단) 전제 → 웹 에셋은 내장**. 런타임에 외부 CDN/폰트로 나가면 안 된다.
  Swagger UI(`/docs`)·ReDoc(`/redoc`)·대시보드 폰트(Roboto Condensed)는 모두
  `src/core/static/` 에 vendoring 하고 `src/core/webassets.py`(`mount_static`/`register_offline_docs`)
  가 `/assets` 로 서빙한다. 두 앱은 `FastAPI(docs_url=None, redoc_url=None)` 로 기본
  CDN docs 를 끄고 이 헬퍼로 재등록한다. 대시보드 HTML 에 `fonts.googleapis.com` 등
  외부 `<link>` 를 다시 넣지 말 것(회귀 방지: `tests/test_offline_assets.py`).
- **인메모리 상태 → 단일 워커**. coordinator·executor 모두 `workers=1`. 처리량 확장은
  executor 인스턴스 수로 한다.
- 비동기 디스패처에서 블로킹 DB 호출(impyla/psycopg)은 `run_in_executor`/`to_thread` 로 감싸
  이벤트 루프를 막지 않는다.
- 새 기능은 `tests/` 에 테스트를 추가한다. 실제 DB 없이 `MockBackend`/`FakeRunner` 로 검증.
- **메타 테이블 스키마 한정**: 모든 메타 테이블(jobs/job_history/task_history/executor_status/
  executor_health_metrics/coordinator_status/executor_reservation)명은 `db.schema`(기본 `public`)로
  한정된다. `src/core/config.py` 의 `_qualify_table()` 이 설정에서 읽은 테이블명을 `public.<t>` 로 만들고
  (이미 `.` 한정된 값은 그대로), 각 repo 의 `self.table` f-string 이 이를 그대로 쓴다 — 앱 런타임
  SQL 과 두 DDL 파일이 같은 스키마를 가리킨다. 테이블/스키마를 바꾸면 **설정·DDL 두 파일을 함께** 고친다.
- **메타 저장소 스키마는 두 벌**: `config/postgresql.sql`(PostgreSQL) 과
  `warehousepg.sql`(WarehousePG/Greenplum 7=PG12). 테이블/컬럼을 바꾸면 **두 파일을 함께**
  고친다. WarehousePG 판은 테이블마다 `DISTRIBUTED BY` 가 붙고(PK 는 분산키를 포함해야 함),
  history/metrics 는 대리 PK 를 빼 `job_id`/`executor_url` 로 co-locate 한다. 앱 SQL(`ON
  CONFLICT`·`JSONB`·`DISTINCT ON`)은 GP7=PG12 라 양쪽 공통으로 동작한다.

## Git / PR

- 커밋 메시지는 한글. 사용자가 명시적으로 요청할 때만 커밋/푸시한다.
- 기본 브랜치는 `main`. 원격: `DataDynamics/distributed-query-executor`.
