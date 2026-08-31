# 프로그램 명세서

이 문서는 이 시스템을 이루는 **핵심 프로그램 열세 개가 각각 무슨 일을 어떤 순서로 하는지**를 적어 둔
것이다. 프로그램마다 무엇을 하는 물건인지(개요)와, 그 안에서 벌어지는 일을 번호 순서대로 따라갈 수
있게 한 처리 로직을 담았다. 처리 로직에는 실제 코드 대신 **의사코드(pseudo code)** 를 실었다.
파이썬을 모르는 사람도 순서를 읽을 수 있어야 하고, 코드가 조금 바뀌어도 문서가 곧바로 낡지 않게 하기
위해서다.

전체 구성 요소를 모두 담지는 않았다. 요청 하나가 들어와 데이터가 옮겨지고 끝날 때까지 **반드시
거쳐 가는 길목**과, 그 길목이 어긋났을 때 사고가 나는 자리를 골랐다. 대시보드나 운영자 CLI 처럼
사람이 보는 화면, 그리고 얇은 조회 API 는 뺐다.

## 읽는 법

프로그램마다 먼저 개요 표가 나오고, 그 아래에 처리 로직이 번호 순서대로 이어진다. 개요 표의 항목은
다음과 같다.

| 항목 | 뜻 |
|---|---|
| 프로그램 ID | 이 문서 안에서 프로그램을 가리키는 번호. `CO` 는 coordinator, `EX` 는 executor, `CM` 은 공용이다 |
| 프로그램명 | 사람이 부르는 이름 |
| 구분 | 어느 프로세스 안에서 도는지 |
| 소스 파일 | 실제 코드가 있는 파일 |
| 주요 함수 | 그 파일에서 이 명세가 다루는 함수 |

**의사코드를 읽는 규칙**은 간단하다. `IF`·`ELSE`·`FOR`·`WHILE` 은 그대로 조건과 반복이고,
`RETURN` 은 값을 돌려주며 끝내는 것, `THROW` 는 오류를 내며 중단하는 것이다. `→` 는 "그 결과로 이런
일이 벌어진다"는 뜻이고, `//` 로 시작하는 줄은 설명이다. 대문자로 적은 이름은 실제 함수나 상태값을
가리킨다.

**같은 내용을 엑셀로도 두었다.** `program-spec.xlsx` 는 프로그램마다 시트를 하나씩 두고, 한
프로그램이 **한 줄**이 되도록 담았다. 프로그램 ID·프로그램명·구분·소스 파일·주요 함수·개요·처리
로직이 그 줄의 칸들이고, 처리 로직 칸 하나에 모든 단계가 일련번호 순서로 설명과 의사코드까지 함께
들어 있다. 맨 앞 개요 시트에서 전체 목록을 본다.

처리 로직 칸은 내용이 길어 화면에서는 아래쪽이 잘려 보인다. 엑셀의 행 높이 상한이 정해져 있기
때문이며 값이 없어진 것은 아니다. 칸을 고르면 수식 입력줄에서 전문을 볼 수 있고, 인쇄와 다른
문서로 붙여넣기에도 전문이 그대로 따라간다. **내용을 고쳤다면 이 문서와 엑셀을 함께 갱신한다.**

## 프로그램 목록

| NO | 프로그램 ID | 프로그램명 | 구분 | 소스 파일 |
|---|---|---|---|---|
| 1 | `PG-CO-001` | 이관 작업 생성 API | coordinator | `src/coordinator/app.py` |
| 2 | `PG-CO-002` | SQL 검증기 | coordinator | `src/coordinator/parser.py` |
| 3 | `PG-CO-003` | 쿼리 분할기 | coordinator | `src/coordinator/splitter.py` |
| 4 | `PG-CO-004` | 쿼리 템플릿 엔진 | coordinator | `src/coordinator/template.py` |
| 5 | `PG-CO-005` | 동시 실행 수용 제어 | coordinator | `src/coordinator/dispatcher.py` |
| 6 | `PG-CO-006` | 작업 실행 디스패처 | coordinator | `src/coordinator/dispatcher.py` |
| 7 | `PG-CO-007` | S3 스테이징 2단계 적재 | coordinator | `src/coordinator/dispatcher.py` |
| 8 | `PG-CO-008` | 작업 저장소 | coordinator | `src/coordinator/job_store.py` |
| 9 | `PG-CO-009` | executor 헬스 모니터 | coordinator | `src/coordinator/monitor.py` |
| 10 | `PG-EX-001` | 태스크 접수와 상태 관리 | executor | `src/executor/app.py` |
| 11 | `PG-EX-002` | 소스→Greenplum 적재 백엔드 | executor | `src/executor/backend.py` |
| 12 | `PG-CM-001` | 설정 로더 | 공용 | `src/core/config_loader.py` |
| 13 | `PG-CM-002` | 실행 SQL 로깅 | 공용 | `src/core/sqllog.py` |

---

## PG-CO-001 이관 작업 생성 API

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-001` |
| 프로그램명 | 이관 작업 생성 API |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/app.py` |
| 주요 함수 | `create_job`, `_create_job` |

**개요**

클라이언트가 `POST /jobs` 로 보낸 이관 요청을 받아 실제로 실행할 수 있는 작업으로 만들어 내는
프로그램이다. 이 시스템의 모든 이관은 여기서 시작한다.

여기서 하는 일은 크게 넷이다. 같은 요청이 두 번 들어왔는지 먼저 확인하고, 보낸 SQL 이 규칙에 맞는지
검사하며, 그것을 여러 조각으로 나누고, 지금 받아도 되는 양인지 판단해 받아들인다. **네 가지를 모두
응답을 주기 전에 끝낸다는 점이 중요하다.** 잘못된 요청이 백그라운드로 넘어간 뒤에 실패하면
클라이언트는 한참 뒤에야 그 사실을 알게 되기 때문이다.

실제 이관은 응답을 준 뒤에 백그라운드에서 진행되므로, 이 프로그램은 작업 번호만 돌려주고 끝난다.

**처리 로직**

**1. 작업 번호 발급과 로그 컨텍스트 설정**

가장 먼저 작업 번호를 만들고 로그 컨텍스트에 묶는다. 검증에 실패해 작업이 저장되지 않더라도 이
요청에서 찍히는 모든 로그에 같은 번호가 붙어야 나중에 추적할 수 있기 때문이다.

```
job_id = NEW_JOB_ID()                 // job_ + uuid4 앞 12자
idempotency_key = 요청헤더["Idempotency-Key"]
WITH 로그컨텍스트(job_id):
    RETURN 아래 2~10 단계 수행
```

**2. 멱등 사전확인**

멱등 키가 있으면 같은 키로 만든 작업이 이미 있는지 먼저 본다. 있으면 검증도 분할도 하지 않고 곧바로
기존 작업을 돌려준다. 지문은 템플릿을 적용하기 전의 원본 요청으로 계산해야 재시도할 때마다 같은
값이 나온다.

```
fingerprint = SHA256(요청본문)  IF idempotency_key 있음
IF idempotency_key 있음 AND NOT dry_run THEN
    existing = STORE.GET_BY_IDEMPOTENCY_KEY(idempotency_key)
    IF existing 있음 THEN
        IF existing.fingerprint != fingerprint THEN
            THROW 409  // 같은 키에 다른 본문
        RETURN 200 + 헤더 Idempotency-Replayed=true + existing.job_id
```

**3. SQL 확보 — 날짜 fan-out / 템플릿 / raw 중 하나**

SQL 을 어디서 얻을지 세 갈래로 갈린다. `task_params` 가 있으면 날짜 구간을 하루 단위로 펼쳐 조각을
직접 만들고, `template_id` 가 있으면 서버 템플릿을 렌더해 요청 필드를 채우며, 둘 다 없으면
클라이언트가 보낸 SQL 을 그대로 쓴다.

```
IF 요청.task_params 있음 THEN
    fanout = TRUE
    sub_queries = BUILD_FANOUT(요청, 템플릿엔진, 오늘)   // 하루 = task 1개
ELSE IF 요청.template_id 있음 THEN
    IF 템플릿엔진 == NULL THEN THROW 400
    APPLY_TEMPLATE(요청, 템플릿엔진)   // sql·staging_ddl·insert_sql 등을 요청에 주입
```

**4. 필수 필드 검증**

raw 방식이든 템플릿 방식이든 이 시점에는 세 필드가 모두 채워져 있어야 한다.

```
missing = [n FOR n IN (sql, partition_column, target_table) IF 요청[n] 비어있음]
IF missing 있음 THEN
    THROW 422 MISSING_REQUIRED_FIELDS
```

**5. SQL 검증과 분할(fan-out 이 아닐 때)**

검증과 분할을 동기로 수행한다. 여기서 나는 오류는 백그라운드로 넘어가지 않고 곧바로 클라이언트에
422 로 간다.

```
IF NOT fanout THEN
    dialect = 요청.sql_dialect OR 설정.기본방언
    parsed = VALIDATE_AND_PARSE(sql, partition_column, dialect, strict_validation)
    sub_queries = SPLIT(parsed, parallelism, split_strategy)
```

**6. 실행 모드별 필수 필드 검증**

고른 `exec_mode` 가 요구하는 필드가 채워졌는지 본다. 모드마다 요구가 다르므로 여기서 갈라 검사한다.

```
CASE exec_mode
  stage_insert : staging_table·wrapper_query 필요 → 없으면 422
  local_stage  : external_columns·insert_sql 필요 → 없으면 422
  s3_stage     : external_columns·insert_sql·staging_table 필요 → 없으면 422
  copy         : wrapper_query 가 SELECT 가 아니면 422
```

**7. executor 배정**

조각마다 어느 executor 가 맡을지 정한다. 헬스·부하 기반 정책이 켜져 있으면 가장 한가한 쪽부터,
아니면 순번대로 돌아가며 배정하고 배정 횟수를 누적해 쏠림을 관측할 수 있게 한다.

```
executor_urls = ASSIGN_EXECUTORS(조각수, 설정.executors)
FOR url IN executor_urls:
    assign_counts[url] += 1
```

**8. 모의 실행 분기**

`dry_run` 이면 여기서 끝난다. executor 를 부르지도 않고 저장하지도 않으며, 만들어질 SQL 계획만
200 으로 돌려준다. 부작용이 없으므로 수용 판단보다 앞에 둔다.

```
IF dry_run THEN
    plan = [{executor_url, partition_values, sub_query, (모드별 SQL)} FOR 각 조각]
    로그로 각 조각의 SQL 을 남긴다
    RETURN 200 {dry_run:TRUE, exec_mode, task_count, tasks:plan}
```

**9. 수용 판단(admission)**

지금 받아도 되는 양인지 본다. 실행 슬롯과 대기 큐를 합한 용량을 넘으면 즉시 429 로 거절한다. 여기서
거절만 하고 대기는 디스패처가 맡는다.

```
IF NOT ADMISSION.TRY_ADMIT() THEN
    THROW 429 + 헤더 Retry-After: 5
// 성공했으면 in-flight 를 1 점유한 상태다. 아래에서 실패하면 반드시 되돌려야 한다.
```

**10. 작업 저장과 백그라운드 예약**

Job 과 Task 를 만들어 저장하고 실행을 백그라운드로 예약한 뒤 202 를 돌려준다. 멱등 키가 있으면
저장이 곧 선점이라, 동시에 들어온 같은 키의 요청 둘 중 하나만 작업을 만든다.

```
TRY:
    job = NEW Job(status=SPLITTING, idempotency_key, fingerprint, 요청 필드들)
    FOR idx, (조각, url) IN 조각목록:
        task = NEW Task(job_id, url, 조각.sql, 조각.partition_values)
        task.out_path = OUT_PATH(idx, task)   // local_stage 는 로컬 경로, s3_stage 는 S3 키
        job.tasks 에 추가
    IF idempotency_key 있음 THEN
        winner = STORE.CLAIM_AND_ADD(job)     // 원자적 선점
        IF winner 있음 THEN                    // 경쟁에서 졌다
            IF winner.fingerprint != fingerprint THEN THROW 409
            ADMISSION.RELEASE()               // 우리는 실행하지 않으므로 슬롯 반납
            RETURN 200 + winner.job_id
    ELSE
        STORE.ADD(job)
    BACKGROUND.ADD(디스패처.RUN, job)
CATCH 모든예외:
    ADMISSION.RELEASE()   // 실행이 예약되지 못했으므로 점유한 슬롯을 되돌린다
    RETHROW
RETURN 202 {job_id}
```

---

## PG-CO-002 SQL 검증기

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-002` |
| 프로그램명 | SQL 검증기 |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/parser.py` |
| 주요 함수 | `validate_and_parse`, `validate_select_query`, `find_partition_in` |

**개요**

클라이언트가 보낸 SQL 이 이 시스템이 다룰 수 있는 모양인지 검사하고, 나눌 기준이 되는 `IN` 절을 찾아
내는 프로그램이다. 검사는 글자를 훑는 것이 아니라 `sqlglot` 으로 SQL 을 구문 트리로 만들어서 한다.
문자열 검사는 주석이나 따옴표에 쉽게 속지만 트리는 속지 않기 때문이다.

**검사의 목적은 두 가지다.** 하나는 안전이다. `SELECT` 가 아닌 문장이나 세미콜론으로 이어 붙인 여러
문장을 막아 의도치 않은 삭제나 변경이 실행되지 않게 한다. 다른 하나는 분할 가능성이다. 나눌 기준
컬럼의 `IN` 목록이 없으면 애초에 병렬로 나눌 수가 없다.

엄격 검증을 켜면 단순 `SELECT` 만 받고, 끄면 JOIN 이나 GROUP BY 가 있는 복합 쿼리도 받되 `IN` 절만
찾아낸다.

**처리 로직**

**1. 입력 확인**

빈 쿼리와 빈 파티션 컬럼을 먼저 거른다. 뒤 단계에서 엉뚱한 오류로 번지는 것을 막기 위해서다.

```
IF sql 이 비었음 THEN THROW PARSE_ERROR
IF partition_column 이 비었음 THEN THROW MISSING_PARTITION_COLUMN
```

**2. 구문 트리 생성**

지정된 방언(dialect)으로 SQL 을 파싱한다. 파싱 자체가 실패하면 방언이 맞지 않는 경우가 대부분이다.

```
TRY:
    statements = SQLGLOT.PARSE(sql, read=dialect) 중 NULL 아닌 것
CATCH:
    THROW PARSE_ERROR "SQL 파싱 실패"
IF statements 가 비었음 THEN THROW PARSE_ERROR
```

**3. 단일 문·SELECT 여부 확인**

여러 문장이 이어져 있으면 인젝션 위험이 있으므로 거절하고, 최상위 문이 `SELECT` 가 아니어도
거절한다.

```
IF COUNT(statements) > 1 THEN THROW MULTIPLE_STATEMENTS
stmt = statements[0]
IF stmt 가 SELECT 가 아님 THEN THROW NOT_A_SELECT
```

**4. 엄격 모드 추가 검사**

엄격 모드에서만 복합 구문을 거절한다. 결과가 달라질 수 있는 구문을 1단계에서는 받지 않겠다는
뜻이며, 끄면 이 블록을 통째로 건너뛴다.

```
IF strict THEN
    IF stmt 에 DISTINCT 있음 THEN THROW UNSUPPORTED_DISTINCT
    IF stmt 에 GROUP BY 있음 THEN THROW UNSUPPORTED_GROUP_BY
    IF stmt 에 HAVING 있음   THEN THROW UNSUPPORTED_HAVING
    IF stmt 에 JOIN 있음     THEN THROW UNSUPPORTED_JOIN
    IF stmt 에 집계함수 있음  THEN THROW UNSUPPORTED_AGGREGATE
    IF stmt 에 WHERE 없음    THEN THROW NO_PARTITION_IN_CLAUSE
```

**5. 파티션 IN 절 탐색**

트리 어디에 있든 파티션 컬럼의 `IN` 노드를 찾는다. 엄격 모드는 최상위 WHERE 에 있어야 하지만,
느슨한 모드는 중첩 서브쿼리 안에 있어도 찾아낸다.

```
in_node = FIND_PARTITION_IN(stmt, partition_column)
IF in_node == NULL THEN THROW NO_PARTITION_IN_CLAUSE
```

**6. IN 절 자체의 제약 검사**

찾은 `IN` 이 나눌 수 있는 모양인지 본다. 세 가지 모두 나눌 수 없는 경우다.

```
IF in_node 가 NOT IN 임        THEN THROW NEGATED_IN
IF in_node 값목록이 비었음      THEN THROW EMPTY_IN_LIST
IF in_node 안에 서브쿼리 있음   THEN THROW SUBQUERY_IN_CLAUSE
```

**7. 파싱 결과 반환**

원문과 트리, 컬럼과 방언을 함께 담아 돌려준다. 분할기가 원문 포맷을 보존한 채 값만 바꾸려면 원문과
트리가 모두 필요하다.

```
RETURN ParsedQuery(sql=원문, expression=stmt,
                   partition_column, dialect, in_values=in_node 의 값들)
```

---

## PG-CO-003 쿼리 분할기

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-003` |
| 프로그램명 | 쿼리 분할기 |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/splitter.py` |
| 주요 함수 | `split`, `_chunk`, `_value_span`, `wrap` |

**개요**

검증을 통과한 SELECT 하나를 여러 개의 SELECT 로 나누는 프로그램이다. 이 시스템이 빨라지는 이유가
바로 여기에 있다. 파티션 컬럼의 `IN` 목록에 값이 100개 있고 4조각으로 나누라고 하면, 값을 25개씩
나눠 가진 SELECT 네 개를 만든다.

**나눌 때 원문 포맷을 보존한다는 점이 이 프로그램의 특징이다.** 구문 트리를 다시 문자열로 만들면
줄바꿈과 별칭, 힌트 주석 같은 것이 사라져 원래 쿼리와 눈에 띄게 달라진다. 그래서 원문에서 값 목록이
놓인 위치만 찾아 그 구간만 바꿔치기한다. 위치를 찾지 못할 때만 트리를 다시 문자열로 만드는 방법으로
물러선다.

**처리 로직**

**1. 값 목록 확보**

파싱 결과에서 `IN` 노드를 다시 찾아 값 표현식 목록을 꺼낸다.

```
src_in = FIND_PARTITION_IN(parsed.expression, parsed.partition_column)
value_exprs = src_in 의 값 표현식 목록
```

**2. 값 분배(버킷 나누기)**

값을 요청한 개수만큼의 버킷으로 나눈다. 값보다 버킷이 많으면 빈 조각이 생기므로 값 개수를 상한으로
눌러 둔다.

```
n = MAX(1, MIN(parallelism, COUNT(value_exprs)))
IF strategy == "round_robin" THEN
    i 번째 값을 (i MOD n) 번 버킷에 넣는다     // 번갈아 분배
ELSE                                          // contiguous(기본)
    size, rem = DIVMOD(COUNT(values), n)
    앞에서부터 연속 구간으로 자르되, 나머지 rem 개는 앞쪽 버킷에 하나씩 더 준다
buckets = 빈 버킷을 제거한 결과
```

**3. 원문에서 값 구간 찾기**

원문 문자열에서 값 목록이 시작하고 끝나는 위치를 한 번만 계산한다. 버킷마다 같은 구간을 쓰므로
반복문 밖에서 구한다.

```
span = VALUE_SPAN(parsed.sql, partition_column)   // (시작, 끝) 또는 NULL
IF span == NULL THEN
    로그: 포맷 보존 실패 → AST 재직렬화로 폴백
```

**4. 버킷마다 sub-query 생성**

버킷의 값들을 방언에 맞는 문자열로 만든 뒤, 원문의 값 구간만 갈아 끼운다. 구간을 못 찾았을 때만 트리
복제 방식으로 만든다.

```
FOR bucket IN buckets:
    rendered = [값.SQL(dialect) FOR 값 IN bucket]
    IF span != NULL THEN
        sub_sql = 원문[:시작] + JOIN(rendered, ", ") + 원문[끝:]    // 포맷 보존
    ELSE
        cloned = parsed.expression 복제
        FIND_PARTITION_IN(cloned).값목록 = bucket
        sub_sql = cloned.SQL(dialect)                              // 포맷 미보존
    결과에 SubQuery(sql=sub_sql, partition_values=rendered) 추가
RETURN 결과
```

**5. 래퍼 적용(선택)**

요청이 래퍼 쿼리를 주면 각 조각을 그 안에 끼워 넣는다. 자리표시자가 없으면 조각이 사라져 조용히
잘못된 SQL 이 실행되므로 반드시 막는다.

```
IF wrapper 있음 THEN
    IF placeholder NOT IN wrapper THEN THROW WRAPPER_PLACEHOLDER_MISSING
    sub_sql = wrapper.REPLACE(placeholder, sub_sql)
```

---

## PG-CO-004 쿼리 템플릿 엔진

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-004` |
| 프로그램명 | 쿼리 템플릿 엔진 |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/template.py` |
| 주요 함수 | `render`, `render_query`, `load_manifest`, `_resolve_params` |

**개요**

클라이언트가 SQL 전문을 보내는 대신 **템플릿 이름과 값만 보내도 되게** 해 주는 프로그램이다. 서버에
미리 등록해 둔 템플릿 파일을 값으로 채워 SELECT 와 스테이징 DDL, INSERT 를 만들어 낸다.

**이렇게 하면 두 가지가 좋아진다.** SQL 이 서버에 있으므로 클라이언트를 고치지 않고도 쿼리를 바꿀 수
있고, 값이 SQL 문법에 맞게 이스케이프되어 들어가므로 인젝션을 막을 수 있다. 렌더는 Jinja2 의
샌드박스 환경에서 하고, 값이 비어 있으면 조용히 빈 문자열이 되는 대신 오류를 내도록(StrictUndefined)
해 두었다.

렌더가 끝난 뒤의 검증·분할·디스패치 경로는 SQL 을 직접 보낸 경우와 완전히 같다.

**처리 로직**

**1. manifest 로드**

템플릿 디렉터리에서 `manifest.yml` 을 읽는다. 파일 이름이 경로를 벗어나지 못하도록 식별자를 먼저
검사하고, 수정 시각을 보고 캐시를 쓸지 다시 읽을지 정한다.

```
VALIDATE_ID(template_id)     // 영숫자·_·- 만 허용 → 아니면 TEMPLATE_ID_INVALID
path = 템플릿루트 / template_id / "manifest.yml"
IF path 없음 THEN THROW TEMPLATE_NOT_FOUND
IF 캐시에 있고 mtime 동일 THEN RETURN 캐시본
manifest = PARSE_MANIFEST(path)   // 실패하면 TEMPLATE_MANIFEST_ERROR
```

**2. exec_mode 확정**

무슨 조각을 렌더해야 하는지는 실행 모드가 정한다. 우선순위는 요청이 가장 높고 manifest 가 다음이며
기본값은 `copy` 다.

```
exec_mode = 요청.exec_mode OR manifest.defaults.exec_mode OR "copy"
roles = 모드별_필요조각[exec_mode]
IF roles == NULL THEN THROW TEMPLATE_EXEC_MODE
```

**3. 파라미터 검증과 기본값 채우기**

manifest 가 선언한 파라미터마다 필수 여부와 타입을 확인하고, 없으면 기본값을 채운다.

```
FOR spec IN manifest.params:
    IF spec.name NOT IN params THEN
        IF spec.required THEN THROW TEMPLATE_PARAM_ERROR
        params[spec.name] = spec.default
    params[spec.name] = COERCE(spec, params[spec.name])   // 타입 변환 실패도 같은 오류
ctx = params
```

**4. 스칼라 값 노출**

`target_table` 처럼 SQL 이 아닌 값들도 템플릿이 참조할 수 있도록 컨텍스트에 넣는다. 같은 이름의
파라미터가 있으면 파라미터가 이긴다.

```
effective = {**manifest.defaults, **요청스칼라}
FOR k, v IN effective:
    ctx.SETDEFAULT(k, v)
ctx["exec_mode"] = exec_mode          // 확정값으로 덮어쓴다
ctx.SETDEFAULT("template_id", template_id)
```

**5. 조각 렌더**

모드가 요구하는 필수 조각을 모두 렌더하고, 선택 조각은 파일이 있을 때만 렌더한다.

```
FOR role IN roles.required:
    fname = manifest.files[role]
    IF fname 없음 THEN THROW TEMPLATE_MISSING_ROLE
    out[role] = RENDER_FILE(template_id, fname, ctx)   // 실패 시 TEMPLATE_RENDER_ERROR
FOR role IN roles.optional:
    out[role] = RENDER_FILE(...) IF manifest.files[role] 있음 ELSE NULL
```

**6. 렌더 결과 검증**

빈 SELECT 를 조기에 잡고, 파서를 타지 않는 DDL 과 INSERT 는 여기서 다중 문을 막는다.

```
IF out["select"] 이 비었음 THEN THROW TEMPLATE_RENDER_ERROR
IF 설정.validate_ddl_single_stmt THEN
    FOR role IN ("staging_ddl", "insert"):
        IF out[role] 에 문장이 2개 이상 THEN THROW TEMPLATE_MULTIPLE_STATEMENTS
RETURN RenderResult(exec_mode, select, wrapper, staging_ddl, insert,
                    external_columns, defaults)
```

**7. 날짜 fan-out 전용 검사(호출 측)**

날짜 fan-out 을 쓸 때는 템플릿이 부호 변수를 반드시 참조해야 한다. 쓰지 않으면 조각마다 의도보다
넓은 구간을 읽어 **조용히 중복 적재**되므로 렌더 전에 막는다.

```
IF fanout 모드 THEN
    vars = REFERENCED_VARIABLES(template_id, role="select")   // Jinja2 AST 검사
    FOR name IN task_params:
        IF (name + "_sign") NOT IN vars THEN
            THROW 422 TEMPLATE_MISSING_SIGN_VAR
```

---

## PG-CO-005 동시 실행 수용 제어

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-005` |
| 프로그램명 | 동시 실행 수용 제어(admission) |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/dispatcher.py` |
| 주요 함수 | `JobAdmission.try_admit`, `release`, `slot` |

**개요**

한 번에 처리할 작업 수를 제한해 시스템이 무너지지 않게 지키는 프로그램이다. 크기가 작지만 이것이
없으면 요청이 몰릴 때 대기 목록이 끝없이 쌓여 메모리가 차고, 뒤에 있는 원본과 목적지 데이터베이스가
함께 무너진다.

**두 개의 숫자로 관리한다.** 하나는 동시에 실행할 수 있는 작업 수(실행 슬롯)이고 다른 하나는 줄을 서
기다릴 수 있는 작업 수(대기 큐)다. 둘을 합한 것이 이 시스템이 한 번에 품을 수 있는 총량이고, 그것을
넘는 요청은 받지 않고 429 로 돌려보낸다.

**거절과 대기의 역할을 나눈 것이 설계의 핵심이다.** 이 프로그램은 받을지 말지만 정하고, 받은 뒤
줄을 세우는 일은 디스패처가 한다. 그래서 API 응답이 빨리 나가고 대기는 뒤에서 조용히 이루어진다.

**처리 로직**

**1. 초기화**

설정에서 두 숫자를 읽어 세마포어와 카운터를 만든다. 실행 슬롯이 0 이하면 무제한으로 본다.

```
max_running = 설정.max_concurrent_jobs OR 0
max_pending = 설정.max_pending_jobs OR 0
sem = SEMAPHORE(max_running) IF max_running > 0 ELSE NULL
inflight = 0
```

**2. 용량 계산**

용량은 실행 슬롯과 대기 큐의 합이다. 무제한 모드는 상한이 없음을 NULL 로 표현한다.

```
FUNCTION capacity():
    IF max_running <= 0 THEN RETURN NULL      // 무제한
    RETURN max_running + MAX(0, max_pending)
```

**3. 수용 시도(비차단)**

기다리지 않고 즉시 판단한다. 여유가 있으면 카운터를 1 늘리고 받아들이며, 가득 찼으면 카운터를
건드리지 않고 거절한다.

```
FUNCTION try_admit():
    cap = capacity()
    IF cap != NULL AND inflight >= cap THEN
        RETURN FALSE          // 호출 측이 429 로 변환한다
    inflight = inflight + 1
    RETURN TRUE
```

**4. 슬롯 반납**

받아들였던 자리를 하나 돌려준다. 0 아래로 내려가지 않게 막아 두어 두 번 반납해도 망가지지 않는다.

```
FUNCTION release():
    IF inflight > 0 THEN inflight = inflight - 1
```

**5. 실행 슬롯 확보(대기)**

실제 실행 직전에 부르는 부분이다. 빈 슬롯이 없으면 날 때까지 기다리고, 블록을 빠져나가며 자동으로
반납한다. 기다리는 동안 작업은 밖에서 PENDING 으로 보인다.

```
CONTEXT slot():
    IF sem == NULL THEN
        YIELD                 // 무제한이면 대기 없이 통과
    ELSE
        WITH sem:             // 빈 슬롯이 날 때까지 대기
            YIELD             // 블록을 빠져나가면 자동 반납
```

---

## PG-CO-006 작업 실행 디스패처

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-006` |
| 프로그램명 | 작업 실행 디스패처 |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/dispatcher.py` |
| 주요 함수 | `_DispatcherBase.run`, `HttpDispatcher._execute`, `_run_task`, `_poll`, `finalize_job` |

**개요**

접수된 작업 하나를 실제로 끝까지 몰고 가는 프로그램이며 이 시스템에서 가장 복잡한 자리다. 조각들을
executor 들에게 나눠 보내고, 진행 상황을 물어보며 기다리고, 실패하면 다른 executor 로 넘기고,
마지막에 작업 전체의 상태를 판정한다.

**데이터는 여기를 지나가지 않는다.** coordinator 는 "이 SQL 을 실행해 저 테이블에 넣어라"라고 시키고
"몇 건 넣었다"는 답만 받는다. 수억 건의 행은 executor 와 데이터베이스 사이에서만 흐른다.

원격 executor 를 부르는 방식과 coordinator 안에서 직접 실행하는 방식 두 가지가 있는데, 수명주기를
다루는 부분은 공통이고 조각을 실제로 실행하는 부분만 갈린다.

**처리 로직**

**1. PENDING 으로 표시하고 슬롯 대기**

먼저 대기 상태로 저장한다. 슬롯이 없으면 여기서 기다리며, 그동안 조회하는 쪽에는 PENDING 으로
보인다.

```
job.status = PENDING
STORE.SAVE(job)
IF inflight > max_running > 0 THEN
    로그: 실행 슬롯 대기(inflight, 슬롯수, 큐크기)
WITH ADMISSION.SLOT():        // 빈 슬롯이 날 때까지 대기
    2~7 단계 수행
```

**2. 대기 중 취소 확인**

슬롯을 잡은 직후 다시 취소를 확인한다. 기다리는 사이에 취소됐다면 헛되이 실행하지 않는다.

```
IF CANCEL_OBSERVED(job) THEN
    FINALIZE_JOB(job)                  // CANCELLED 로 확정
    job.finished_at = NOW()
    STORE.SAVE(job); HISTORY.RECORD(job)
    RETURN job.job_id
```

**3. RUNNING 전이**

실행 상태로 올리고 시작 시각과 시작 이력을 남긴다.

```
job.status = RUNNING
job.started_at = NOW()
STORE.SAVE(job); HISTORY.RECORD(job)
로그: 실행 시작 (exec_mode, 조각수, target, write_mode)
```

**4. local_stage 파일 배치 계획(해당 모드만)**

`local_stage` 는 세그먼트 호스트마다 읽을 수 있는 파일 수에 한계가 있어, 조각을 호스트별 예산에 맞춰
미리 배분한다. 배분에 실패하면 실행하지 않고 실패로 마감한다.

```
IF NOT PLAN_LOCAL_STAGE(job) THEN
    // 예산 초과로 배치 불가 → 5~7 단계를 건너뛰고 마감으로 간다
    GOTO 7
```

**5. 조각 병렬 실행**

모든 조각을 동시에 시작한다. HTTP 클라이언트 하나를 공유해 연결을 재사용하고, 실제 동시 실행 수는
디스패치 세마포어가 제한한다. 접속 타임아웃은 짧게, 읽기 타임아웃은 길게 잡는다. 죽은 executor 에
오래 매달리지 않으면서도 오래 걸리는 조각은 기다려 주기 위해서다.

```
timeout = TIMEOUT(read=설정.task_timeout_s, connect=설정.task_connect_timeout_s)
WITH HTTP_CLIENT(timeout) AS client:
    GATHER(RUN_TASK(client, job, t) FOR t IN job.tasks)

FUNCTION RUN_TASK(client, job, task):
    WITH 디스패치세마포어:                       // max_dispatch_concurrency 로 제한
        IF CANCEL_OBSERVED(job) THEN
            task.status = CANCELLED; RETURN
        예약: RESERVE(task.executor_url)        // 다른 coordinator 의 선택에 반영
        TRY:
            DISPATCH_WITH_FAILOVER(client, job, task)
        CATCH exc:
            task.status = FAILED; task.error = exc   // 이 조각만 격리해 실패
        FINALLY:
            예약해제: RELEASE(task.executor_url)
```

**6. 조각 하나의 전송·재시도·폴링**

조각을 후보 executor 순서대로 시도한다. 시작 전 연결 실패는 재시도해도 안전하므로 지수 백오프로
다시 시도하고, 소진하면 다음 executor 로 넘긴다. 시작한 뒤에는 끝날 때까지 상태를 물어본다.

```
후보 = FAILOVER_ORDER(task)          // 배정 executor 우선, 헬스 기반이면 한가한 순
FOR url IN 후보:
    TRY:
        RETRY(최대 task_max_retries, 지수백오프):
            POST url + "/tasks"  {task_id, job_id, sub_query, target_table, ...}
        POLL(client, job, task)      // 아래 반복
        RETURN                       // 종료 상태에 도달
    CATCH 연결오류:
        CONTINUE                     // 다음 executor 로 failover
task.status = FAILED

FUNCTION POLL(client, job, task):
    transient = 0
    WHILE task.status NOT IN (DONE, FAILED, CANCELLED):
        IF CANCEL_OBSERVED(job) THEN task.status = CANCELLED; RETURN
        SLEEP(설정.poll_interval_s)
        TRY:
            resp = GET url + "/tasks/" + task.task_id
            transient = 0
        CATCH 일시오류:
            transient += 1
            IF transient > task_max_retries THEN RETHROW    // 상위가 failover 판단
            CONTINUE
        task.status = resp.status
        task.rows_written / rows_read / current_phase / phases 갱신
```

**7. 배리어 뒤 2단계 적재(스테이징 모드만)**

`_execute` 가 돌아왔다는 것은 모든 조각이 종료 상태라는 뜻이며, 이것이 자연스러운 배리어가 된다. 그
뒤에 coordinator 가 중앙에서 2단계 적재를 수행한다. 다른 모드에서는 아무 일도 하지 않는다.

```
RUN_STAGE_LOAD(job)     // local_stage: file:// 외부테이블 → staging → target
RUN_S3_LOAD(job)        // s3_stage: PXF 외부테이블 → target (PG-CO-007 참고)
```

**8. 최종 상태 판정과 마감**

실행이 성공했든 예외로 끝났든 반드시 여기를 지난다. 판정에는 순서가 있어 취소가 가장 앞선다.

```
FINALLY:
    FUNCTION FINALIZE_JOB(job):
        failed = [t FOR t IN job.tasks IF t.status == FAILED]
        IF job.cancel_requested THEN         job.status = CANCELLED
        ELSE IF failed 가 없음 THEN           job.status = DONE
        ELSE IF failure_policy == "best_effort"
                AND exec_mode != "local_stage" THEN
                                             job.status = PARTIAL
        ELSE
            job.status = FAILED
            job.error = 실패 조각들의 사유를 이어 붙인 것
    job.finished_at = NOW()
    STORE.SAVE(job); HISTORY.RECORD(job)
    로그: 종료 요약(status, 완료수, 적재행수, 소요시간)
```

**9. 슬롯 반납**

가장 바깥에서 반드시 슬롯을 되돌린다. 대기 중 취소든 정상 종료든 예외든 모든 경로가 이곳을 지나야
용량이 새지 않는다.

```
FINALLY:
    ADMISSION.RELEASE()
RETURN job.job_id
```

---

## PG-CO-007 S3 스테이징 2단계 적재

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-007` |
| 프로그램명 | S3 스테이징 2단계 적재 |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/dispatcher.py`, `src/core/s3_stage.py` |
| 주요 함수 | `_run_s3_load`, `build_s3_external_ddl`, `build_pre_delete`, `external_table_name` |

**개요**

`s3_stage` 모드에서 **모든 executor 가 CSV 를 S3 에 올린 뒤** coordinator 가 중앙에서 한 번에
Greenplum 에 넣는 프로그램이다.

**왜 중앙에서 하는가**를 알아 두면 이해가 쉽다. executor 마다 따로 넣으면 목적지 테이블에 여러
트랜잭션이 동시에 붙어 경합이 생기고, 일부만 성공한 상태를 되돌리기도 어렵다. 그래서 executor 는
S3 에 파일만 올리고, 목적지에 넣는 일은 coordinator 가 한 트랜잭션으로 처리한다.

**외부테이블 하나가 job 전체를 덮는다는 점이 요령이다.** 조각마다 외부테이블을 만들지 않고, 작업
번호로 묶인 S3 경로 하나를 가리키는 외부테이블을 만든다. 그러면 그 아래 모든 조각의 CSV 를
세그먼트들이 병렬로 읽어 간다.

**처리 로직**

**1. 수행 여부 판단**

이 모드가 아니거나 취소됐거나 업로드가 하나라도 실패했으면 아무것도 하지 않는다. 일부만 올라간
상태로 목적지에 넣으면 데이터가 빠진 채 성공으로 보이기 때문이다.

```
IF job.exec_mode != "s3_stage" THEN RETURN
IF CANCEL_OBSERVED(job) THEN RETURN
IF job.tasks 중 FAILED 가 하나라도 있음 THEN
    로그 경고: 업로드 실패 → Phase 2 건너뜀
    RETURN
```

**2. 외부테이블 이름과 S3 위치 조립**

작업마다 고유한 외부테이블 이름을 만든다. 설정에 스키마가 있으면 스키마까지 한정한다. 생성과 치환과
삭제가 모두 이 한 이름을 쓰므로 세 곳이 어긋나지 않는다.

```
csv_options = RESOLVE_CSV_OPTIONS(job, 설정)
ext    = EXTERNAL_TABLE_NAME(job.job_id, 설정.s3_external_schema)  // 예: dwtemp.s3ext_<job_id>
prefix = S3_JOB_PREFIX(설정.s3_prefix, job.job_id)                 // <prefix>/<job_id>/
location = BUILD_S3_LOCATION(버킷, prefix, PXF프로파일, PXF서버)
```

**3. 외부테이블 DDL 생성**

컬럼 정의와 위치, CSV 형식을 붙여 외부테이블 생성문을 만든다.

```
external_ddl = "CREATE EXTERNAL TABLE " + ext
             + " (" + job.external_columns + ")"
             + " LOCATION ('" + location + "')"
             + " FORMAT 'CSV' (" + csv_options + ")"
```

**4. INSERT 문의 스테이징 참조 치환**

템플릿이 만든 INSERT 는 스테이징 테이블 이름을 참조한다. 그 이름을 이번 작업의 외부테이블 이름으로
바꿔야 `INSERT INTO target SELECT ... FROM <외부테이블>` 이 된다.

```
insert_sql = REWRITE_STAGING_NAME(job.insert_sql, job.staging_table, ext,
                                  보호대상=(job.target_table,))
```

**5. 멱등 선삭제 여부 결정**

넣기 전에 해당 파티션을 지울지 정한다. 요청이 지정했으면 그것을 따르고, 없으면 적재 방식을 따른다.
지우고 넣으므로 같은 작업을 다시 돌려도 중복되지 않는다.

```
do_delete = job.pre_delete IF job.pre_delete != NULL
            ELSE (job.write_mode == "overwrite_partitions")
IF do_delete THEN
    values = 모든 조각의 partition_values 를 합친 것
    pre_delete = "DELETE FROM " + target + " WHERE " + col + " IN (" + values + ")"
ELSE
    pre_delete = NULL
```

**6. 한 트랜잭션으로 적재**

조립한 SQL 들을 GP 마스터에서 한 트랜잭션으로 실행한다. 블로킹 호출이므로 스레드로 넘기되 로그
컨텍스트를 함께 넘겨 어느 작업의 SQL 인지 남게 한다.

```
TRY:
    rows = RUN_IN_THREAD(백엔드.LOAD_EXTERNAL_S3,
                         external_ddl, pre_delete, insert_sql, cleanup)
    // 백엔드 안에서: CREATE EXTERNAL → (DELETE) → INSERT → COMMIT → DROP EXTERNAL
    로그: Phase 2 적재 완료 (target 반영 rows 행)
CATCH exc:
    job.error = "s3_stage Phase 2 실패: " + exc
    job.tasks[0].status = FAILED       // 업로드는 됐지만 적재가 실패 → 작업 실패로 확정
    STORE.SAVE(job)
    RETURN
```

**7. S3 정리(Phase 3)**

목적지에 다 들어갔으므로 S3 에 올린 스테이징 객체를 지운다. 남겨 두면 보관 비용이 계속 늘고 다음
실행과 섞일 위험이 있다.

```
CLEANUP_S3(job)        // <prefix>/<job_id>/ 아래 객체 일괄 삭제(best-effort)
STORE.SAVE(job)
```

---

## PG-CO-008 작업 저장소

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-008` |
| 프로그램명 | 작업 저장소 |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/job_store.py` |
| 주요 함수 | `InMemoryJobStore`, `SqlJobStore`, `claim_and_add`, `reconcile_interrupted_jobs` |

**개요**

작업과 그 조각들의 현재 모습을 보관하는 프로그램이다. 두 가지 구현이 있는데 coordinator 를 한 대만
쓰면 메모리에 두는 것으로 충분하고, 여러 대를 띄우면 PostgreSQL 을 함께 봐야 한다.

**가장 중요한 기능은 멱등 키 선점이다.** 같은 키를 가진 요청 둘이 정확히 같은 순간에 들어와도 작업은
하나만 만들어져야 한다. 사전 확인만으로는 두 요청이 모두 "없다"고 판단하는 순간이 생기므로, 저장하는
순간에 원자적으로 선점하는 절차를 따로 둔다.

여러 대를 쓸 때는 작업의 모습을 통째로 JSON 한 칸에 넣는다. 조각까지 별도 표로 나누면 갱신할 때마다
여러 표를 맞춰야 하는데, 작업 하나를 통째로 읽고 쓰는 편이 단순하고 어긋날 여지가 없기 때문이다.

**처리 로직**

**1. 저장소 선택**

설정을 보고 어느 구현을 쓸지 정한다. 공유 저장소를 쓰려면 접속 정보가 반드시 있어야 한다.

```
FUNCTION BUILD_JOB_STORE(설정):
    IF 설정.store_backend == "postgres" THEN
        IF 설정.history_db_dsn 이 비었음 THEN THROW 설정오류
        RETURN SqlJobStore(dsn, table, coordinator_id)
    RETURN InMemoryJobStore()
```

**2. 작업 저장(메모리 구현)**

프로세스 락으로 감싸 여러 스레드가 동시에 건드려도 깨지지 않게 한다.

```
FUNCTION ADD(job):
    WITH 락:
        jobs[job.job_id] = job
```

**3. 멱등 키 원자적 선점**

이 절차가 중복 작업을 막는 마지막 방어선이다. 락 안에서 다시 확인하고, 이미 있으면 그 작업을
돌려준다. 돌려받은 쪽은 자기 작업을 버린다.

```
FUNCTION CLAIM_AND_ADD(job):
    WITH 락:                                  // Sql 구현은 트랜잭션으로 같은 일을 한다
        existing = 같은 idempotency_key 를 가진 작업 찾기
        IF existing 있음 THEN
            RETURN existing                   // 경쟁에서 졌다
        jobs[job.job_id] = job
        RETURN NULL                           // 선점 성공
```

**4. 작업 저장(공유 저장소 구현)**

작업 전체를 JSON 으로 직렬화해 한 행으로 넣거나 갱신한다. 기록한 coordinator 도 함께 남겨 어느
인스턴스가 들고 있는지 알 수 있게 한다.

```
FUNCTION SAVE(job):
    data = job.TO_RECORD()                    // 조각 목록까지 포함한 전체 스냅샷
    INSERT INTO jobs (job_id, coordinator_id, status, data, updated_at)
    VALUES (...)
    ON CONFLICT (job_id) DO UPDATE SET ...
```

**5. 취소 플래그 기록**

취소는 다른 coordinator 가 실행 중인 작업에도 닿아야 하므로, 공유 저장소에 플래그를 남긴다. 실행
중인 쪽이 폴링하다가 이 값을 보고 멈춘다.

```
FUNCTION REQUEST_CANCEL(job_id):
    UPDATE jobs SET cancel_requested = TRUE WHERE job_id = ?
    RETURN 갱신된 행이 있는가
```

**6. 중단된 작업 정합**

프로세스가 죽으면 실행 중이던 작업이 RUNNING 인 채로 남는다. 기동할 때 그런 작업을 찾아 실패로
정리해, 영원히 끝나지 않는 작업이 목록에 남지 않게 한다.

```
FUNCTION RECONCILE_INTERRUPTED_JOBS(store):
    n = 0
    FOR job IN store.LIST_OWNED():            // 내 coordinator_id 로 기록된 것만
        IF job.status IN (PENDING, SPLITTING, RUNNING) THEN
            job.status = FAILED
            job.error  = "coordinator 재기동으로 중단됨"
            job.finished_at = NOW()
            store.SAVE(job); n += 1
    RETURN n
```

---

## PG-CO-009 executor 헬스 모니터

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CO-009` |
| 프로그램명 | executor 헬스 모니터 |
| 구분 | coordinator |
| 소스 파일 | `src/coordinator/monitor.py` |
| 주요 함수 | `HealthMonitor.start`, `_health_loop`, `_poll_one`, `_record_loop` |

**개요**

등록된 executor 들이 살아 있는지, 자원을 얼마나 쓰고 있는지를 주기적으로 확인해 두는 프로그램이다.
확인해 둔 값은 두 곳에 쓰인다. 하나는 대시보드와 조회 API 이고, 다른 하나는 조각을 어느 executor
에게 맡길지 정하는 판단이다.

**한 대가 죽어도 다른 대의 확인이 멈추지 않아야 한다.** 그래서 확인 중에 나는 오류는 모두 잡아
기록만 하고 반복은 계속 돈다. 가용성이 정확성보다 앞서는 자리이기 때문이다.

**로그 잡음을 줄이는 장치가 하나 있다.** 죽은 executor 를 매 주기 경고로 남기면 로그가 금세
묻히므로, 살아 있다가 죽은 순간과 다시 살아난 순간만 눈에 띄게 남긴다.

**처리 로직**

**1. 시작**

설정이 꺼져 있거나 등록된 executor 가 없으면 아무 반복도 띄우지 않는다. 확인 반복은 항상 띄우고,
기록 반복은 접속 정보가 있을 때만 띄운다.

```
IF NOT 설정.monitor_enabled THEN 로그 후 RETURN
IF executors 가 비었음 THEN 로그 후 RETURN
백그라운드 시작: HEALTH_LOOP()
IF 설정.monitor_db_dsn 있음 THEN
    백그라운드 시작: RECORD_LOOP()
```

**2. 확인 반복**

모든 executor 를 동시에 확인한 뒤 설정된 간격만큼 쉰다. HTTP 클라이언트 하나를 반복 전체에서
재사용해 연결을 아낀다.

```
WITH HTTP_CLIENT(timeout=5s) AS client:
    WHILE TRUE:
        GATHER(POLL_ONE(client, url) FOR url IN executors)
        SLEEP(설정.monitor_health_interval_s)
```

**3. 한 대 확인**

살아 있는지 먼저 묻고, 그다음 자원 사용량을 받아 메모리 레코드에 덮어쓴다. 성공하든 실패하든 확인
시각은 먼저 남긴다.

```
FUNCTION POLL_ONE(client, url):
    rec = executors[url]
    was_healthy = rec.healthy;  first = (rec.last_checked == NULL)
    rec.last_checked = NOW()
    TRY:
        GET url + "/health"    → 실패하면 예외
        md = GET url + "/metrics"
        rec.healthy = TRUE
        rec.cpu_percent    = md.cpu_percent
        rec.memory_*       = md.memory.*
        rec.disk_*         = md.disk.*
        rec.active_tasks   = md.tasks.active
        rec.max_concurrent_tasks = md.tasks.max
        rec.error = NULL
        IF NOT was_healthy AND NOT first THEN 로그 INFO: 복구됨
    CATCH exc:
        rec.healthy = FALSE;  rec.error = exc
        IF was_healthy OR first THEN 로그 WARNING: 다운 감지
        ELSE                        로그 DEBUG: 여전히 다운
```

**4. 기록 반복**

간격만큼 쉰 뒤 현재 스냅샷을 데이터베이스에 쌓는다. 먼저 자고 나서 기록하므로 기동 직후가 아니라 한
주기 뒤부터 쌓인다. 기록이 실패해도 반복은 죽지 않는다.

```
WHILE TRUE:
    SLEEP(설정.monitor_record_interval_s)
    TRY:
        RUN_IN_THREAD(WRITE_PG, SNAPSHOT())
    CATCH:
        로그: 메트릭 기록 실패    // 반복은 계속된다
```

**5. 일괄 기록**

스냅샷의 각 행을 메트릭 표에 한 번에 넣는다. 표는 앱이 만들지 않고 미리 만들어져 있어야 한다.

```
FUNCTION WRITE_PG(snapshot):
    rows = [(executor_url, healthy, cpu, mem%, mem_used, mem_total, disk%, error)
            FOR r IN snapshot]
    WITH DB연결:
        EXECUTEMANY(INSERT INTO executor_health_metrics ..., rows)
        COMMIT
```

**6. 즉시 확인**

대시보드가 새로 고칠 때 지금 값을 보고 싶어 하면 반복을 기다리지 않고 한 번 확인해 돌려준다.

```
FUNCTION POLL_NOW():
    IF executors 가 비었음 THEN RETURN []
    WITH HTTP_CLIENT(timeout=5s) AS client:
        GATHER(POLL_ONE(client, url) FOR url IN executors)
    RETURN SNAPSHOT()
```

---

## PG-EX-001 태스크 접수와 상태 관리

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-EX-001` |
| 프로그램명 | 태스크 접수와 상태 관리 |
| 구분 | executor |
| 소스 파일 | `src/executor/app.py` |
| 주요 함수 | `create_task`, `_run`, `cancel_task`, `get_task` |

**개요**

coordinator 가 보낸 조각 하나를 받아 실제로 실행하고, 그 진행 상태를 물어보면 알려 주는
프로그램이다. executor 쪽의 입구에 해당한다.

**접수와 실행을 분리한 것이 핵심이다.** 요청을 받으면 곧바로 202 를 돌려주고 실제 실행은
백그라운드로 넘긴다. 이관은 몇 시간이 걸릴 수 있어서 응답을 붙들고 있을 수 없기 때문이다.
coordinator 는 이후 상태를 물어보며 진행을 따라간다.

**상태는 다섯 단계로 흐른다.** 대기(QUEUED)에서 시작해 읽기(READING)와 쓰기(WRITING)를 거쳐
완료(DONE)로 끝나며, 중간에 실패하면 FAILED, 취소되면 CANCELLED 가 된다. 그 안쪽의 더 잘게 나뉜
단계는 별도로 기록해 두어 어디서 시간이 걸렸는지 볼 수 있게 한다.

**처리 로직**

**1. 접수**

조각을 받아 저장하고 곧바로 응답한다. 이때 대기 단계를 열어 두어 슬롯을 기다린 시간이 측정되게
한다.

```
FUNCTION CREATE_TASK(요청):
    task = NEW Task(요청의 모든 필드)
    task.status = QUEUED
    task.on_stage("QUEUE_WAIT", "start")     // 슬롯 대기 시작
    tasks[task.task_id] = task
    HISTORY.RECORD(task)
    백그라운드 시작: RUN_WITH_SEMAPHORE(task)
    RETURN 202 {task_id, status:"QUEUED"}
```

**2. 동시 실행 제한**

이 executor 가 한 번에 처리할 조각 수를 세마포어로 제한한다. 제한이 없으면 원본과 목적지 양쪽에
연결이 몰려 오히려 느려진다.

```
FUNCTION RUN_WITH_SEMAPHORE(task):
    IF 세마포어 있음 THEN
        WITH 세마포어:                        // executor.max_concurrent_tasks
            RUN(task)
    ELSE
        RUN(task)
```

**3. 실행 전 취소 확인**

슬롯을 기다리는 사이에 취소됐으면 실행하지 않는다. 열려 있던 대기 단계는 지금 시각으로 닫아 소요
시간이 계속 늘지 않게 한다.

```
IF task.cancel_requested THEN
    task.status = CANCELLED;  task.finished_at = NOW()
    CLOSE_OPEN_PHASES(task.phases)
    HISTORY.RECORD(task);  RETURN
```

**4. READING·WRITING 전이**

대기 단계를 닫고 읽기로 올린 뒤, 백엔드를 부르기 직전에 쓰기로 올린다. 상태가 바뀔 때마다 이력을 한
행씩 남긴다.

```
task.on_stage("QUEUE_WAIT", "end")
task.status = READING;  task.started_at = NOW();  HISTORY.RECORD(task)
ctx = 현재 로그컨텍스트 복사          // 워커 스레드에서도 [job][task] 가 붙게 한다
task.status = WRITING;  HISTORY.RECORD(task)
```

**5. 실행 모드별 백엔드 호출**

모드에 따라 부르는 함수가 갈린다. 어느 쪽이든 블로킹 드라이버이므로 워커 스레드로 넘겨 이벤트 루프를
막지 않는다.

```
CASE task.exec_mode
  "statement"   : rows = THREAD(백엔드.EXECUTE, task.sub_query)
  "stage_insert": rows = THREAD(백엔드.STAGE_AND_INSERT, sub_query, staging_table,
                                staging_ddl, insert_sql, progress)
  "local_stage" : rows = THREAD(백엔드.EXPORT_TO_LOCAL_CSV, sub_query, out_path,
                                csv_options, progress)         // Phase 1 만
  "s3_stage"    : rows = THREAD(백엔드.EXPORT_TO_S3, sub_query, out_path,
                                job_id, task_id, csv_options, progress)  // Phase 1 만
  기본("copy")  : rows = THREAD(백엔드.MOVE, sub_query, target_table, write_mode,
                                partition_column, partition_values, progress)
```

**6. 진행률 수집**

백엔드가 배치를 넣을 때마다 부르는 콜백으로 누적 행수를 갱신한다. 매 배치마다 로그를 남기면 파일이
커지므로 일정 간격으로만 남긴다.

```
FUNCTION PROGRESS(n):
    task.rows_written = n
    IF DEBUG 이고 n - 마지막기록 >= 로그간격 THEN
        마지막기록 = n
        로그 DEBUG: 누적 n행 적재
```

**7. 종료 처리**

실행 중에 취소됐으면 완료가 아니라 취소로 끝낸다. 정상이면 완료로 표시하고 적재 행수를 남긴다.

```
task.rows_written = rows
IF task.cancel_requested THEN
    task.status = CANCELLED;  task.finished_at = NOW()
    CLOSE_OPEN_PHASES(task.phases);  HISTORY.RECORD(task);  RETURN
task.status = DONE;  task.finished_at = NOW()
로그 INFO: task 완료 (rows 행 적재)
HISTORY.RECORD(task)
```

**8. 실패 처리**

어느 단계에서 나든 예외는 여기서 잡는다. 밖으로 던지지 않는데, 백그라운드 작업이라 던져도 받을
사람이 없기 때문이다.

```
CATCH exc:
    task.status = FAILED;  task.error = exc;  task.finished_at = NOW()
    CLOSE_OPEN_PHASES(task.phases)      // 열린 단계를 지금 시각으로 마감
    로그 EXCEPTION: task 실패
    HISTORY.RECORD(task)
```

**9. 취소 접수**

취소는 즉시 죽이는 것이 아니라 플래그를 세우는 방식이다. 실행 중인 코드가 안전한 지점에서 그 값을
보고 스스로 멈춘다.

```
FUNCTION CANCEL_TASK(task_id):
    task = tasks[task_id]
    IF task 없음 THEN THROW 404
    IF task.status IN (DONE, FAILED, CANCELLED) THEN RETURN 현재상태
    task.cancel_requested = TRUE
    RETURN {task_id, status, cancel_requested:TRUE}
```

---

## PG-EX-002 소스→Greenplum 적재 백엔드

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-EX-002` |
| 프로그램명 | 소스→Greenplum 적재 백엔드 |
| 구분 | executor |
| 소스 파일 | `src/executor/backend.py` |
| 주요 함수 | `move`, `_stream_to_copy`, `_GreenplumPool`, `export_to_s3` |

**개요**

데이터를 실제로 옮기는 프로그램이며, 이 시스템에서 유일하게 대량의 행을 직접 만지는 자리다. 원본에서
읽어 목적지에 넣는 일을 한다.

**전체를 메모리에 모으지 않는다는 점이 가장 중요하다.** 원본 커서에서 정해진 크기만큼 꺼내
목적지로 흘려보내는 일을 번갈아 반복한다. 수억 건을 다 읽고 나서 넣으려 하면 메모리가 먼저 터진다.

**다시 실행해도 안전하게 만드는 장치**도 여기에 있다. 적재 방식이 덮어쓰기면 넣기 전에 담당 파티션을
먼저 지운다. 지우기와 넣기가 한 트랜잭션이라 중간에 실패해도 원래 상태로 돌아간다.

목적지 연결은 풀에 담아 재사용하되, 돌려줄 때 세션을 깨끗이 비운다. 임시 테이블이 다음 조각에
남아 있으면 충돌하기 때문이다.

**처리 로직**

**1. 원본 접속과 쿼리 제출**

원본에 붙어 조각 SQL 을 제출하고, 컬럼 이름 목록을 커서에서 받아 온다. 이 컬럼 순서가 뒤에서 그대로
쓰인다.

```
conn = SOURCE_CONNECT()
cur  = conn.CURSOR()
STAGE("IMPALA_SUBMIT", start)
SOURCE_EXECUTE(cur, sub_query, query_options)   // 요청별 SET 옵션을 전역 위에 병합
columns = [d[0] FOR d IN cur.description]
STAGE("IMPALA_SUBMIT", end)
```

**2. 목적지 연결 확보**

풀에서 연결을 빌린다. 빈 연결이 없으면 반납될 때까지 기다리므로 동시 연결 수가 저절로 제한된다.

```
WITH GP_POOL.CONNECTION() AS gp:
    WITH gp.CURSOR() AS gp_cur:
        3~6 단계 수행
```

**3. 컬럼 사전검증**

한 행도 보내기 전에 원본 컬럼이 목적지에 모두 있는지 확인한다. 대용량을 다 읽은 뒤 런타임 오류로
깨지는 것을 막기 위해서다.

```
IF 설정.copy_preflight THEN
    STAGE("PREFLIGHT", start)
    target_cols = TARGET_COLUMNS(gp_cur, target_table)
    IF columns 중 target_cols 에 없는 것이 있음 THEN THROW 명확한 오류
    STAGE("PREFLIGHT", end)
```

**4. 멱등 선삭제**

덮어쓰기 방식이면 담당 파티션을 먼저 지운다. 값은 반드시 바인드 파라미터로 넘겨 인젝션을 막는다.

```
IF write_mode == "overwrite_partitions" AND partition_values 있음 THEN
    STAGE("DELETE", start)
    delete_sql = "DELETE FROM " + target_table
               + " WHERE " + partition_column + " IN (" + 자리표시자들 + ")"
    LOG_SQL(delete_sql, phase="DELETE", params=partition_values)
    gp_cur.EXECUTE(delete_sql, partition_values)
    STAGE("DELETE", end, {rows: 지운 행수})
```

**5. 스트리밍 COPY**

읽기와 쓰기를 번갈아 반복하며 흘려보낸다. 파이프라인 방식이면 읽는 스레드를 따로 두어 읽는 동안에도
쓰기가 멈추지 않게 한다.

```
copy_sql = BUILD_COPY(gp_cur, target_table, columns)
STAGE("STREAM_COPY", start)
WITH gp_cur.COPY(copy_sql) AS copy:
    WHILE TRUE:
        batch = cur.FETCHMANY(batch_size)      // 원본에서 한 묶음
        IF batch 가 비었음 THEN BREAK
        FOR row IN batch:
            copy.WRITE_ROW(CLEAN(row))         // NaN·NaT 는 NULL 로 바꾼다
        rows_written += COUNT(batch)
        ON_PROGRESS(rows_written)
STAGE("STREAM_COPY", end, {rows, read_wait_ms, write_wait_ms, rows_per_sec})
```

**6. 커밋과 정리**

지우기와 넣기를 함께 커밋한다. 원본 연결은 성공이든 실패든 반드시 닫는다.

```
STAGE("COMMIT", start)
gp.COMMIT()
STAGE("COMMIT", end)
RETURN rows_written
FINALLY:
    conn.CLOSE()
```

**7. 연결 반납과 세션 초기화**

풀에 돌려줄 때 세션 상태를 모두 지운다. 지우지 못한 연결은 재사용하지 않고 닫아 버린다.

```
FUNCTION RELEASE(conn):
    TRY:
        conn.ROLLBACK()
        conn.autocommit = TRUE                 // DISCARD ALL 은 트랜잭션 밖에서만 된다
        conn.EXECUTE("DISCARD ALL")            // 임시 테이블·SET·준비문 제거
        유휴목록에 반납
    CATCH:
        로그 경고 후 conn.CLOSE()               // 안전 폴백
```

**8. S3 내보내기(s3_stage 1단계)**

`s3_stage` 에서는 목적지에 붙지 않는다. 로컬 CSV 로 내린 뒤 S3 에 올리고 로컬 파일을 지운다.

```
STAGE("EXPORT_WRITE", start)
rows = 원본 결과를 로컬 CSV 파일로 스트리밍 저장     // 형변환을 꺼서 재파싱 비용 제거
STAGE("EXPORT_WRITE", end, {rows})
STAGE("S3_UPLOAD", start)
S3.UPLOAD(로컬파일, 버킷, key)
로컬파일 삭제
STAGE("S3_UPLOAD", end, {rows})
RETURN rows
```

---

## PG-CM-001 설정 로더

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CM-001` |
| 프로그램명 | 설정 로더 |
| 구분 | 공용 |
| 소스 파일 | `src/core/config_loader.py` |
| 주요 함수 | `load_config`, `load_properties`, `_resolve_value`, `_resolve_dict` |

**개요**

설정 파일 두 개를 읽어 하나의 설정 값 묶음으로 만드는 프로그램이다. coordinator 와 executor 가 모두
기동할 때 가장 먼저 부른다.

**파일을 둘로 나눈 이유**를 알아 두면 이 프로그램이 왜 이렇게 생겼는지 이해된다. `config.yml` 은
설정의 **구조**를 담고 `config.properties` 는 그 **값**을 담는다. 구조에는 `${변수:기본값}` 모양의
자리표시자가 들어 있고, 값 파일이 그 자리를 채운다. 새 버전을 설치할 때 구조 파일은 교체하고 값
파일은 그대로 두면 되므로, 운영자가 손으로 옮겨야 하는 것이 줄어든다.

**여기서 놓치기 쉬운 함정이 하나 있다.** 구조 파일에 자리가 없으면 값 파일에 아무리 적어도 조용히
무시된다. 새 버전이 추가한 설정을 쓰려면 구조 파일을 반드시 교체해야 하는 이유다.

**처리 로직**

**1. 파일 경로 결정**

명시한 경로가 있으면 그것을 쓰고, 없으면 설정 디렉터리와 파일명을 합쳐 만든다.

```
base_dir  = config_dir OR 기본설정디렉터리
props_file = properties_path OR (base_dir / "config.properties")
yaml_file  = yaml_path       OR (base_dir / "config.yml")
```

**2. properties 읽기**

Java 스타일의 `키=값` 파일을 읽는다. 주석과 빈 줄은 건너뛰고, 값에 있는 앞뒤 공백은 떼어 낸다.

```
FUNCTION LOAD_PROPERTIES(path):
    IF path 없음 THEN RETURN {}       // 없어도 오류가 아니다. 기본값으로 동작한다
    FOR line IN 파일:
        line = TRIM(line)
        IF line 이 비었거나 "#" 또는 "!" 로 시작 THEN CONTINUE
        key, value = line 을 첫 "=" 또는 ":" 에서 나눈 것
        props[TRIM(key)] = TRIM(value)
    RETURN props
```

**3. YAML 읽기**

구조 파일을 읽는다. 비어 있으면 치환할 것이 없으므로 빈 묶음을 돌려주고 끝낸다.

```
raw_config = LOAD_YAML(yaml_file)
IF raw_config 가 비었음 THEN RETURN {}
```

**4. 자리표시자 치환**

값 하나를 만날 때마다 `${변수:기본값}` 모양을 찾아 바꾼다. properties 에 값이 있으면 그것을 쓰고,
없으면 기본값을 쓰며, 기본값도 없으면 빈 문자열이 된다.

```
FUNCTION RESOLVE_VALUE(value, props):
    FOR 각 "${...}" 조각 IN value:
        이름, 기본값 = 조각을 첫 ":" 에서 나눈 것
        치환값 = props[이름] IF 이름 IN props ELSE (기본값 OR "")
        조각을 치환값으로 바꾼다
    RETURN 바뀐 문자열
```

**5. 재귀 치환**

구조 전체를 재귀로 훑으며 문자열 값마다 치환한다. 치환한 결과는 **문자열 그대로 둔다.** 숫자나
참·거짓으로 바꾸는 일은 여기서 하지 않고, 나중에 설정 항목을 꺼내는 쪽이 필요한 타입으로 읽는다.
목록은 문자열 원소만 치환하고 숫자·참거짓 원소는 건드리지 않는다.

```
FUNCTION RESOLVE_DICT(data, props):
    FOR k, v IN data:
        IF v 가 문자열 THEN out[k] = RESOLVE_VALUE(v, props)
        ELSE IF v 가 dict THEN out[k] = RESOLVE_DICT(v, props)      // 한 단계 더 들어간다
        ELSE IF v 가 list THEN
            out[k] = [RESOLVE_VALUE(x, props) IF x 가 문자열 ELSE x FOR x IN v]
        ELSE out[k] = v                                             // 숫자·참거짓·NULL 은 그대로
    RETURN out
```

**6. 결과 반환**

치환이 끝난 묶음을 돌려준다. 이후 `Settings` 가 이 묶음을 섹션 구조 그대로 읽으면서 숫자·참거짓
항목을 그때 필요한 타입으로 바꾼다. 자리표시자 이름이 아니라 **YAML 의 중첩 위치**가 섹션과
일치해야 값이 반영된다.

```
RETURN RESOLVE_DICT(raw_config, props)
```

---

## PG-CM-002 실행 SQL 로깅

| 항목 | 내용 |
|---|---|
| 프로그램 ID | `PG-CM-002` |
| 프로그램명 | 실행 SQL 로깅 |
| 구분 | 공용 |
| 소스 파일 | `src/core/sqllog.py` |
| 주요 함수 | `log_sql`, `format_sql`, `format_params`, `datasource_of` |

**개요**

데이터베이스에 던지는 모든 SQL 을 로그에 한 줄씩 남기는 프로그램이다. 사고가 났을 때 "무엇을 읽어
무엇을 넣었는가"를 되짚을 수 있는 유일한 단서가 된다.

**이 로그는 상세 로그 수준과 무관하게 항상 남는다.** HTTP 로그는 상세 수준일 때만 남기지만 실행 SQL
은 운영 기본 수준에서도 남긴다. 사고는 대개 상세 수준을 켜기 전에 일어나고, 그때 이 기록이 비어
있으면 추적이 불가능하기 때문이다. 꼭 꺼야 한다면 설정으로 끌 수 있다.

**한 줄에 한 SQL 이라는 규칙**을 지킨다. 여러 줄로 쓰인 SQL 을 그대로 남기면 로그 한 건이 여러 줄이
되어 검색과 집계가 어려워진다. 그래서 줄바꿈과 연속 공백을 하나로 접는다.

**처리 로직**

**1. 기록 여부 판단**

설정으로 꺼져 있으면 아무 일도 하지 않는다.

```
FUNCTION LOG_SQL(datasource, sql, phase, target=NULL, params=NULL):
    enabled, max_length, params_on = RESOLVE(설정)
    IF NOT enabled THEN RETURN
```

**2. 민감값 마스킹**

비밀번호처럼 남으면 안 되는 값을 먼저 가린다. 자르기 전에 가려야 잘린 뒤에도 새지 않는다.

```
sql = MASK(sql)
```

**3. 공백 접기**

줄바꿈과 연속 공백을 공백 하나로 바꿔 한 줄로 만든다.

```
FUNCTION COLLAPSE_SQL(sql):
    IF sql 이 비었음 THEN RETURN ""
    RETURN 연속된 공백문자를 " " 하나로 바꾼 뒤 앞뒤 공백 제거
```

**4. 길이 제한과 절단 표시**

너무 긴 SQL 은 잘라 낸다. 다만 잘랐다는 사실을 반드시 남긴다. 전문인 줄 알고 읽으면 잘못 판단하기
때문이다.

```
FUNCTION FORMAT_SQL(sql, max_length):
    one_line = COLLAPSE_SQL(sql)
    IF max_length <= 0 OR LEN(one_line) <= max_length THEN RETURN one_line
    RETURN one_line[:max_length] + " … (총 " + LEN(one_line) + "자 중 "
                                 + (LEN(one_line) - max_length) + "자 절단)"
```

**5. 파라미터 정리**

바인드 파라미터도 함께 남기되 같은 규칙으로 자른다. 설정으로 끄면 남기지 않는다.

```
IF params_on AND params 있음 THEN
    params_text = FORMAT_PARAMS(params, max_length)   // 마스킹 → 한 줄 → 절단
ELSE
    params_text = NULL
```

**6. 데이터소스 이름 추론**

어느 엔진에 던진 SQL 인지는 커서에서 알아낸다. 커스텀 어댑터는 자기 이름을 들고 있고, 기본 커서는
그런 속성이 없으므로 기본값을 쓴다. 덕분에 함수 서명을 바꾸지 않고도 엔진별 표기가 갈린다.

```
FUNCTION DATASOURCE_OF(cursor, default="impala"):
    IF cursor 에 _name 속성 있음 THEN RETURN cursor._name
    RETURN default
```

**7. 한 줄로 기록**

정해진 형식으로 한 줄을 남긴다. 어느 엔진에 어느 단계에서 무엇을 던졌는지가 한눈에 보이는 형식이다.

```
줄 = "SQL 실행 datasource=" + datasource + " phase=" + phase
IF target 있음 THEN 줄 += " target=" + target
줄 += " | " + sql_text
IF params_text 있음 THEN 줄 += " | params=" + params_text
LOGGER("core.sql").INFO(줄)
```

계측하는 자리는 넷이다. 원본에 던지는 SELECT 전부, 목적지에 던지는 실행문 전부, 데이터소스
미리보기, 그리고 executor 의 결과 반환 실행이다.
