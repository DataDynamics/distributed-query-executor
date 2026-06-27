# CLAUDE.md

이 저장소에서 작업하는 Claude Code(및 기타 에이전트)를 위한 안내. 자세한 사용법은
[README.md](README.md), 설계는 [DESIGN.md](DESIGN.md), 배포는 [deploy/README.md](deploy/README.md) 참고.

## 프로젝트 개요

Coordinator + N Executor 구조의 분산 쿼리 실행기. 하나의 Impala `SELECT` 를 파티션 컬럼의
`IN` 목록 기준으로 N분할해 병렬로 읽고, 각 executor 가 결과를 Greenplum 에 적재한다
(**Impala → Greenplum 이관**). 데이터는 coordinator 를 거치지 않고 executor 가 직접 흘려보내며,
coordinator 로는 상태와 row count 만 흐른다.

## 명령어

```bash
# 가상환경(파이썬 3.11+). 이 저장소는 .venv 를 사용한다.
python3.11 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt        # coordinator + 테스트 의존성

# 테스트 (실제 DB 불필요 — MockBackend/FakeRunner 사용). 현재 178개.
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest tests/test_admission.py -q # 특정 파일만

# 로컬 실행 (저장소 기본 설정 사용)
QUERY_EXECUTOR_CONFIG_DIR=packaging/config EXECUTOR_PORT=8001 .venv/bin/python -m executor &
QUERY_EXECUTOR_CONFIG_DIR=packaging/config .venv/bin/python -m coordinator

# local 모드(별도 executor 없이 coordinator 안에서 직접 실행)
COORDINATOR_EXECUTOR_MODE=local QUERY_EXECUTOR_CONFIG_DIR=packaging/config .venv/bin/python -m coordinator
```

> 셸에 `python` 이 없을 수 있다(pyenv). 항상 `.venv/bin/python` 을 명시적으로 쓴다.

## 아키텍처

```
core/         # 공용: 설정 로더/설정/로깅/메트릭 (coordinator·executor 공유)
coordinator/  # FastAPI: 검증(parser) → 분할(splitter) → admission → 디스패치 → 상태 추적
executor/     # FastAPI: Impala 읽기 → Greenplum 적재(backend), task 상태 노출
packaging/config/  # config.properties + config.yml 기본값 + 스키마(*.sql)
tests/        # pytest (coordinator·executor 검증/라이프사이클/admission/대시보드)
```

요청 흐름: `POST /jobs` → parser 검증 + splitter 분할(동기, 실패 시 즉시 4xx) →
admission `try_admit`(초과 시 429) → Job 생성(SPLITTING) → 백그라운드 `run()` 이 슬롯
대기(PENDING) 후 RUNNING → executor 에 `POST /tasks` 병렬 디스패치 + polling → `finalize_job`
으로 종료 상태 집계(DONE/PARTIAL/FAILED/CANCELLED).

### 핵심 모듈
- `coordinator/dispatcher.py` — **동시성의 중심**. `JobAdmission`(실행 슬롯 + 대기 큐, 429),
  `_DispatcherBase`(PENDING→슬롯대기→RUNNING→종료 + in-flight 반납 + 취소 감지),
  `HttpDispatcher`(원격), `LocalDispatcher`(in-process). 하위 클래스는 `_execute(job)` 만 구현.
- `coordinator/parser.py` — sqlglot 검증. `strict_validation=true`(단순 SELECT) vs
  `false`(JOIN/서브쿼리/GROUP BY 등 복합 쿼리에서 파티션 `IN` 절을 트리 어디서든 탐색).
- `coordinator/splitter.py` — IN 값 N등분(contiguous/round_robin), 원문 포맷 보존 치환.
- `coordinator/job_store.py` — `InMemoryJobStore`(단일) / `SqlJobStore`(멀티 coordinator, JSONB).
- `executor/backend.py` — `ImpalaToGreenplumBackend`(impyla→psycopg) + `MockBackend`.
  `exec_mode`: `copy`(COPY) / `statement`(INSERT 그대로 실행) / `stage_insert`(TEMP 경유).
- `executor/app.py` — task 상태머신(QUEUED→READING→WRITING→DONE/FAILED/CANCELLED),
  `executor.max_concurrent_tasks` 세마포어.
- `core/logging.py` — 일 단위 롤링 + `[job_id][task_id]` 컨텍스트 주입 + **WARNING 전용
  로그(`*-warn.log`) 분리**.

## 설정

`config.properties`(Java 스타일 key=value)의 값으로 `config.yml` 의 `${변수:기본값}`
자리표시자를 치환해 로드한다(`core/config_loader.py`). 설정 디렉터리는
`/etc/query-executor/`(환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 변경, 개발 시 `packaging/config`).

- `core/config.py` 의 `_get("section","key")` 는 **YAML 의 섹션 구조**를 따라 읽는다. 새 설정을
  추가할 때 placeholder 이름(`${coordinator.x}`)이 아니라 **실제 YAML 중첩 위치**가 섹션과
  일치해야 값이 반영된다(coordinator 키는 `coordinator:` 아래에 둘 것).
- 동시성: `coordinator.max_concurrent_jobs`(실행 슬롯 16) + `coordinator.max_pending_jobs`(대기
  큐 100) → 합 초과 시 429 / `coordinator.max_dispatch_concurrency`(task 디스패치 32) /
  `executor.max_concurrent_tasks`(executor당 8).
- 멀티 coordinator: `store.backend=postgres` + 공유 `history.db_dsn`, `executor.self_report=true`.
- 백엔드: `impala.host` + `greenplum.dsn` 둘 다 있으면 실제 백엔드, 아니면 `MockBackend`.

## 관례 / 주의점

- **주석/문서는 한글**. 코드 주석과 docstring 은 "무엇을·왜" 중심으로 상세히 쓴다(로그·예외·
  SQL·HTML 같은 문자열 리터럴은 코드이므로 주석이 아니다 — 구분할 것).
- **대시보드는 빌드 도구 없는 인라인 HTML 문자열**(`coordinator/dashboard.py`,
  `executor/dashboard.py` 의 `DASHBOARD_HTML`). 이 문자열 내부 HTML/CSS/JS 를 수정할 때 따옴표/
  중괄호를 깨뜨리지 않도록 주의하고, 수정 후 `import` 로 무결성을 확인한다.
- **인메모리 상태 → 단일 워커**. coordinator·executor 모두 `workers=1`. 처리량 확장은
  executor 인스턴스 수로 한다.
- 비동기 디스패처에서 블로킹 DB 호출(impyla/psycopg)은 `run_in_executor`/`to_thread` 로 감싸
  이벤트 루프를 막지 않는다.
- 새 기능은 `tests/` 에 테스트를 추가한다. 실제 DB 없이 `MockBackend`/`FakeRunner` 로 검증.

## Git / PR

- 커밋 메시지는 한글. 사용자가 명시적으로 요청할 때만 커밋/푸시한다.
- 기본 브랜치는 `main`. 원격: `DataDynamics/distributed-query-executor`.
