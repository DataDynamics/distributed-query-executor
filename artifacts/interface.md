# 인터페이스 정의서

이 문서는 **밖에서 이 시스템에 말을 거는 방법**을 하나하나 적어 둔 것이다. 이 시스템은 REST API 로
말을 건다. 즉 정해진 주소로 HTTP 요청을 보내면 JSON 으로 답이 온다. 여기서는 그 주소마다 **무엇을
보내야 하고 무엇이 돌아오는지**를 항목 단위로 풀어 적었다.

크게 세 묶음이다. 데이터를 옮기는 **작업(Jobs)** 인터페이스, 결과를 바로 받아 보는 **쿼리(Query)**
인터페이스, 그리고 지금 시스템이 어떤 상태인지 들여다보는 **모니터링(Monitoring)** 인터페이스다.
연동 프로그램을 만드는 사람은 앞의 두 묶음을, 운영 화면이나 감시 도구를 붙이는 사람은 마지막
묶음을 주로 본다.

**먼저 하나만 분명히 해 두자.** 여기 적힌 주소로는 **옮겨지는 데이터가 흐르지 않는다.** 수억 건의
행은 executor 가 원본에서 읽어 목적지 테이블로 곧장 보내며, 여기 있는 API 로는 "몇 번 작업이 지금
어디까지 갔고 몇 건을 넣었나" 같은 이야기만 오간다. 그래서 응답이 아무리 커도 몇 킬로바이트를
넘지 않는다.

말을 거는 상대는 **coordinator 한 대**다. executor 도 같은 모양의 API 를 갖고 있지만 그것은
coordinator 가 쓰는 내부 통로이므로, 밖에서 붙는 프로그램은 coordinator 만 알면 된다. 필요하면
coordinator 가 대신 executor 에게 물어봐 준다.

마지막으로 같은 내용을 엑셀로도 두었다. `interface.xlsx` 는 인터페이스마다 시트를 하나씩 두고
여기와 같은 항목을 담으며, 맨 앞 개요 시트에서 전체 목록을 볼 수 있다. 검토 의견을 직접 달거나
사내 표준 양식에 옮겨 붙일 때 쓴다. **내용을 고쳤다면 이 문서와 엑셀을 함께 갱신한다.**

엑셀에는 여기 없는 열이 둘 더 있는데, 표 대신 시트 하나에 요청과 응답이 함께 담기므로 그것을 가르는
`구분` 과, 계층을 걸러 볼 수 있도록 `필드명(영문)` 오른쪽에 붙인 `부모필드명` 이다. 이 문서가
`tasks[].phases[].name` 처럼 한 칸에 적는 경로를 엑셀에서는 필드명 `name` 과 부모필드명 `phases` 로
나눠 담으며, 3단계 이상이어도 **바로 위 부모 하나만** 적는다. 최상위 필드는 `-` 다.

## 읽는 법

인터페이스마다 먼저 개요 표가 나오고, 그 뒤에 보내는 항목(요청)과 받는 항목(응답)의 표가 이어진다.
항목 표의 열이 무슨 뜻인지부터 정리해 둔다.

| 열 | 뜻 |
|---|---|
| `NO` | 그 표 안에서 항목이 놓인 순서 |
| `필드명(한글)` | 그 항목이 무엇인지 우리말로 옮긴 이름 |
| `필드명(영문)` | 실제로 JSON 에 적히는 이름. 프로그램은 이 이름을 쓴다 |
| `데이터 타입` | 그 자리에 들어갈 값의 종류 |
| `길이` | 값이 얼마나 길 수 있는지 |
| `설명` | 무엇을 담는 자리인지, 그리고 지켜야 할 규칙 |

**데이터 타입**은 JSON 이 쓰는 다섯 가지에 하나를 더한 것이다. `string` 은 글자, `integer` 는 정수,
`number` 는 소수점이 있는 수, `boolean` 은 참/거짓, `object` 는 항목이 여럿 묶인 덩어리,
`array` 는 값이 여러 개 줄지어 있는 목록이다. 여기에 `enum` 을 더 썼는데, 이것은 글자이긴 하지만
**미리 정해 둔 값 중에서만 골라야 하는 자리**라는 뜻이다.

**길이**는 타입에 따라 읽는 법이 다르다. 글자는 최대 몇 자까지 들어가는지를 적었고, 상한이 없으면
`가변` 이라고 적었다. 숫자는 길이 대신 **허용 범위**를 적었고 범위가 따로 없으면 `-` 다. 참/거짓과
덩어리, 목록은 길이라는 말이 어울리지 않으므로 모두 `-` 다.

**비어 있어도 되는 항목**은 설명 첫머리에 `[선택]` 이라고 적었다. 아무 표시가 없으면 반드시 채워야
하는 항목이다. 응답 쪽의 `[선택]` 은 "상황에 따라 `null` 로 올 수 있다"는 뜻이다.

## 공통 규약

### 주소와 형식

coordinator 는 기본적으로 `8088` 포트에서 듣는다. 이 문서의 모든 주소는 그 앞에
`http://<coordinator 주소>:8088` 이 붙는다고 보면 된다.

주고받는 내용은 **모두 JSON 이고 글자는 UTF-8** 이다. 본문을 보내는 요청(`POST`)에는
`Content-Type: application/json` 을 붙인다. 별도의 로그인 절차는 없다 — 사내망 안에서만 쓰는 것을
전제로 만들었기 때문이다. 밖에 노출해야 한다면 앞단에 인증을 두는 장치를 따로 세운다.

### 시각과 식별자를 적는 규칙

시각을 담는 항목은 이름이 모두 `_at` 으로 끝나고 값은 `yyyy-MM-dd HH:mm:ss.sss` 모양의 글자다.
`2026-08-20 14:32:07.418` 처럼 나온다. **시간대 표시가 없는 한국 시각**이니 따로 변환하지 않는다.
`last_checked` 처럼 `_at` 으로 끝나지 않는 몇몇 항목은 예외적으로 ISO 8601 모양 그대로 나온다.

작업 식별자는 `job_` 뒤에 12자리 임의 글자가 붙은 16자다(`job_3f9c2a1b7d4e`). 작업 조각인 태스크
식별자는 `t_` 뒤에 12자리가 붙은 14자다(`t_9a1c4e8b2d70`). 둘 다 서버가 만들어 주므로 보내는
쪽에서 지어낼 일은 없다.

### 응답 코드

| 코드 | 언제 나오나 | 본문에 들어 있는 것 |
|---|---|---|
| `200` | 조회가 잘 됐을 때. 모의 실행이었거나 이미 만든 작업을 다시 돌려줄 때도 이 코드다 | 각 인터페이스의 응답 |
| `202` | 작업을 접수했을 때. 실행은 아직 시작되지 않았다 | `job_id` |
| `400` | 템플릿 기능이 꺼져 있거나 접속 정보가 없어 처리할 수 없을 때 | `detail` |
| `404` | 찾는 작업·태스크·데이터소스가 없을 때 | `detail` |
| `409` | 이미 끝난 작업을 취소하려 했거나, 재실행할 대상이 없거나, 같은 멱등 키에 다른 본문이 왔을 때 | `detail` |
| `422` | 보낸 내용이 규칙에 맞지 않을 때 | `error_code`, `message` |
| `429` | 동시에 처리할 수 있는 작업 수를 넘겼을 때 | `detail`, 그리고 `Retry-After` 헤더 |
| `502` | coordinator 가 executor 나 데이터베이스에 닿지 못했을 때 | `detail` |

`400`·`404`·`409`·`429`·`502` 의 본문은 `{"detail": "사람이 읽을 설명"}` 한 줄이다. 반면 `422` 는
프로그램이 원인을 갈라 볼 수 있도록 **코드를 함께** 준다.

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 오류 코드 | `error_code` | string | 최대 40 | 실패 사유를 나타내는 고정 코드. 프로그램은 이 값으로 분기한다 |
| 2 | 오류 메시지 | `message` | string | 가변 | 사람이 읽을 설명. 무엇을 어떻게 고쳐야 하는지가 들어 있다 |

주요 `error_code` 는 다음과 같다. 앞의 두 묶음은 보낸 SQL 자체의 문제이고, `TEMPLATE_` 로 시작하는
것은 템플릿 문제, `TASK_` 로 시작하는 것은 날짜 fan-out 설정 문제다.

| 코드 | 무슨 뜻인가 |
|---|---|
| `NOT_A_SELECT` | `SELECT` 가 아닌 문장을 보냈다 |
| `MULTIPLE_STATEMENTS` | 세미콜론으로 여러 문장을 이어 보냈다 |
| `PARSE_ERROR` | SQL 을 해석할 수 없다. 방언(dialect) 설정이 맞는지 본다 |
| `MISSING_PARTITION_COLUMN` | 나눌 기준 컬럼을 지정하지 않았다 |
| `NO_PARTITION_IN_CLAUSE` | 그 컬럼의 `IN` 목록이 SQL 안에 없다 |
| `EMPTY_IN_LIST` | `IN` 목록이 비어 있어 나눌 것이 없다 |
| `NEGATED_IN` | `NOT IN` 은 나눌 수 없다 |
| `SUBQUERY_IN_CLAUSE` | `IN` 안에 서브쿼리가 있어 값을 셀 수 없다 |
| `UNSUPPORTED_JOIN` 외 | 엄격 검증에서 막히는 복합 구문이다. `strict_validation` 을 끄면 통과한다 |
| `MISSING_REQUIRED_FIELDS` | `sql`·`partition_column`·`target_table` 중 빈 것이 있다 |
| `STAGE_INSERT_REQUIRES_FIELDS` 외 | 고른 실행 모드가 요구하는 항목이 비었다 |
| `TEMPLATE_NOT_FOUND` | 그런 이름의 템플릿이 서버에 없다 |
| `TEMPLATE_PARAM_ERROR` | 템플릿이 요구하는 파라미터가 빠졌거나 타입이 맞지 않는다 |
| `TEMPLATE_MISSING_SIGN_VAR` | 날짜 fan-out 인데 템플릿이 부호 변수를 쓰지 않는다 |
| `TASK_RANGE_EMPTY` | 날짜 구간이 비어 만들 태스크가 없다 |
| `TASK_RANGE_TOO_LARGE` | 날짜 구간이 너무 넓어 태스크가 과도하게 생긴다 |

### 같은 요청을 두 번 보내도 안전하게 하기

네트워크가 끊겨 응답을 못 받았을 때 그냥 다시 보내면 같은 데이터를 두 번 넣을 위험이 있다. 이를
막으려고 **작업 생성 요청에만** 멱등 키를 쓸 수 있게 해 두었다.

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 멱등 키 | `Idempotency-Key` | string | 가변 | [선택] 요청 헤더. 재시도에도 같은 값을 쓰면 작업이 하나만 만들어진다 |
| 2 | 재생 표시 | `Idempotency-Replayed` | string | 4 | 응답 헤더. 이미 만든 작업을 다시 돌려줄 때만 `true` 로 붙는다 |

같은 키에 **같은 본문**이 오면 서버는 기존 작업을 `200` 으로 다시 돌려주고 위 응답 헤더를 붙인다.
같은 키에 **다른 본문**이 오면 실수로 보았다고 판단해 `409` 로 거절한다.

## 인터페이스 목록

모두 열아홉 개다. `동기` 는 답이 올 때 일이 이미 끝나 있다는 뜻이고, `비동기` 는 접수만 받고 실제
일은 뒤에서 계속된다는 뜻이다.

| NO | 인터페이스 ID | 인터페이스명 | 메서드 | URI | 방식 |
|---|---|---|---|---|---|
| 1 | `IF-JOB-001` | 이관 작업 생성 | POST | `/jobs` | 비동기 |
| 2 | `IF-JOB-002` | 작업 진행률 조회 | GET | `/jobs/{job_id}/status` | 동기 |
| 3 | `IF-JOB-003` | 작업 상세 조회 | GET | `/jobs/{job_id}` | 동기 |
| 4 | `IF-JOB-004` | 작업 결과 조회 | GET | `/jobs/{job_id}/result` | 동기 |
| 5 | `IF-JOB-005` | 태스크 상세 조회 | GET | `/jobs/{job_id}/tasks/{task_id}` | 동기 |
| 6 | `IF-JOB-006` | 작업 취소 | POST | `/jobs/{job_id}/cancel` | 동기 |
| 7 | `IF-JOB-007` | 실패 파티션 재실행 | POST | `/jobs/{job_id}/retry` | 비동기 |
| 8 | `IF-JOB-008` | 작업 목록 조회 | GET | `/jobs` | 동기 |
| 9 | `IF-JOB-009` | 실행 이력 조회 | GET | `/history` | 동기 |
| 10 | `IF-JOB-010` | 템플릿 목록 조회 | GET | `/templates` | 동기 |
| 11 | `IF-QRY-001` | 템플릿 쿼리 실행 | POST | `/query-execute` | 동기 |
| 12 | `IF-QRY-002` | 데이터소스 SELECT 미리보기 | POST | `/datasources/{name}/query` | 동기 |
| 13 | `IF-QRY-003` | 데이터소스 목록 조회 | GET | `/datasources` | 동기 |
| 14 | `IF-MON-001` | 헬스 체크 | GET | `/health` | 동기 |
| 15 | `IF-MON-002` | 시스템 메트릭 조회 | GET | `/metrics` | 동기 |
| 16 | `IF-MON-003` | 클러스터 상태 조회 | GET | `/cluster` | 동기 |
| 17 | `IF-MON-004` | executor 상태 목록 조회 | GET | `/executors` | 동기 |
| 18 | `IF-MON-005` | executor 메트릭 조회 | GET | `/executors/{idx}/metrics` | 동기 |
| 19 | `IF-MON-006` | 런타임 정보 조회 | GET | `/info` | 동기 |

---

# 작업(Jobs) 인터페이스

데이터를 옮기는 일을 맡기고 그 진행을 지켜보는 묶음이다. 순서로 보면 `IF-JOB-001` 로 일을 맡기고,
`IF-JOB-002` 로 진행률을 지켜보다가, 끝나면 `IF-JOB-004` 로 몇 건이 들어갔는지 확인한다. 일부가
실패했다면 `IF-JOB-007` 로 실패한 몫만 다시 돌린다.

## IF-JOB-001 이관 작업 생성

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-001` |
| 인터페이스명 | 이관 작업 생성 |
| 메서드 | `POST` |
| URI | `/jobs` |
| 방식 | 비동기(접수 후 `202`) |
| 설명 | 옮길 SELECT 를 검증하고 여러 조각으로 나눈 뒤 executor 들에게 나눠 준다 |

이 시스템에서 가장 중요한 인터페이스다. 보내는 방법이 두 가지인데, SQL 전문을 그대로 보내는
방식과 서버에 등록해 둔 템플릿 이름과 값만 보내는 방식이다. `template_id` 를 채우면 템플릿
방식이고, 비우면 SQL 방식이다.

**보내자마자 돌아오는 것은 작업 번호뿐이다.** 실제 이관은 그 뒤에 진행되므로, 얼마나 진행됐는지는
`IF-JOB-002` 로 따로 물어봐야 한다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 멱등 키 | `Idempotency-Key` | string | 가변 | [선택] 헤더로 보낸다. 재시도해도 작업이 하나만 생기게 한다 |
| 2 | 템플릿 식별자 | `template_id` | string | 가변 | [선택] 서버에 등록된 템플릿 이름. 주면 SQL 을 서버가 만든다 |
| 3 | 템플릿 파라미터 | `params` | object 또는 array | - | [선택] 템플릿에 넣을 값. `{이름: 값}` 이거나 `{name, value, sign}` 목록이다 |
| 4 | 원본 SELECT 문 | `sql` | string | 가변 | 옮길 데이터를 읽는 SELECT. 템플릿을 쓰면 비워도 된다 |
| 5 | 파티션 컬럼 | `partition_column` | string | 가변 | 이 컬럼의 `IN` 목록을 기준으로 일을 나눈다 |
| 6 | 대상 테이블 | `target_table` | string | 가변 | 읽은 데이터를 넣을 Greenplum 테이블. `스키마.테이블` 로 적는다 |
| 7 | 날짜 분할 파라미터 | `task_params` | array | - | [선택] 구간의 두 끝을 담은 파라미터 이름 두 개. 주면 하루를 태스크 하나로 펼친다 |
| 8 | 구간 경계 방식 | `task_bound` | enum | 5 | [선택] `point`(기본, 양끝 포함) 또는 `pair`(뒤끝 제외) |
| 9 | 날짜 표기 형식 | `task_date_format` | string | 가변 | [선택] 태스크에 넣을 날짜의 모양. 기본은 `%Y-%m-%d` |
| 10 | 요청 사용자 | `username` | string | 가변 | [선택] 누가 시켰는지. 이력 조회에서 이 값으로 찾는다 |
| 11 | 적재 방식 | `write_mode` | enum | 20 | [선택] `append`(기본, 덧붙임) 또는 `overwrite_partitions`(같은 파티션을 지우고 넣음) |
| 12 | 사전 삭제 여부 | `pre_delete` | boolean | - | [선택] `s3_stage` 에서 넣기 전 삭제를 강제하거나 건너뛴다. 비우면 `write_mode` 를 따른다 |
| 13 | 분할 수 | `parallelism` | integer | 1~128 | [선택] 몇 조각으로 나눌지. 기본 4 |
| 14 | 분할 전략 | `split_strategy` | enum | 11 | [선택] `contiguous`(기본, 붙어 있는 값끼리) 또는 `round_robin`(번갈아 가며) |
| 15 | 실패 처리 정책 | `failure_policy` | enum | 11 | [선택] `fail_fast`(기본, 하나 실패하면 중단) 또는 `best_effort`(끝까지 진행) |
| 16 | 실행 모드 | `exec_mode` | enum | 12 | [선택] `copy`(기본)·`statement`·`stage_insert`·`local_stage`·`s3_stage` 중 하나 |
| 17 | 스테이징 테이블 | `staging_table` | string | 가변 | [선택] 중간에 거쳐 갈 테이블 이름. `stage_insert`·`s3_stage` 에서 쓴다 |
| 18 | 스테이징 생성문 | `staging_ddl` | string | 가변 | [선택] 그 테이블을 만드는 `CREATE` 문. 비우면 이미 있는 것을 쓴다 |
| 19 | 모의 실행 여부 | `dry_run` | boolean | - | [선택] `true` 면 실제로 옮기지 않고 만들어질 SQL 만 돌려준다 |
| 20 | SQL 방언 | `sql_dialect` | string | 가변 | [선택] SQL 을 해석할 문법. `hive`·`impala`·`trino` 등 |
| 21 | 엄격 검증 여부 | `strict_validation` | boolean | - | [선택] 기본 `true` 로 단순 SELECT 만 받는다. `false` 면 JOIN 같은 복합 쿼리도 받는다 |
| 22 | 래퍼 쿼리 | `wrapper_query` | string | 가변 | [선택] 나뉜 조각을 감쌀 바깥 쿼리. 자리표시자 위치에 조각이 들어간다 |
| 23 | 래퍼 자리표시자 | `wrapper_placeholder` | string | 가변 | [선택] 위 쿼리에서 조각이 들어갈 자리. 기본 `{{SUBQUERY}}` |
| 24 | Impala 쿼리 옵션 | `impala_query_options` | object | - | [선택] 이 작업에만 적용할 Impala 설정. 예: `{"MEM_LIMIT": "2g"}` |
| 25 | 소스 엔진 | `datasource` | string | 가변 | [선택] 어디서 읽을지. `impala` 또는 사내 API 이름 |
| 26 | 외부테이블 컬럼 정의 | `external_columns` | string | 가변 | [선택] 스테이징 모드에서 CSV 를 읽을 컬럼 정의. 순서가 CSV 와 같아야 한다 |
| 27 | 대상 INSERT 문 | `insert_sql` | string | 가변 | [선택] 스테이징에서 대상으로 옮기는 `INSERT` 문 |
| 28 | 로컬 내보내기 경로 | `export_local_dir` | string | 가변 | [선택] `local_stage` 에서 CSV 를 만들 디렉터리 |
| 29 | CSV 구분자 | `csv_delimiter` | string | 1 | [선택] CSV 의 칸 구분 문자 |
| 30 | CSV NULL 표기 | `csv_null` | string | 가변 | [선택] CSV 에서 빈 값을 나타낼 글자 |
| 31 | CSV 인용부호 | `csv_quote` | string | 1 | [선택] CSV 에서 값을 감쌀 문자 |

**응답 항목(`202`)**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 접수된 작업의 번호. 이 값으로 이후 모든 조회를 한다 |

**응답 항목(`dry_run=true` 일 때 `200`)**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 모의 실행 표시 | `dry_run` | boolean | - | 언제나 `true` 다. 실제로는 아무것도 실행되지 않았다는 뜻이다 |
| 2 | 실행 모드 | `exec_mode` | enum | 12 | 이 계획이 어떤 모드로 실행될지 |
| 3 | 파티션 컬럼 | `partition_column` | string | 가변 | 나눌 기준으로 쓴 컬럼 |
| 4 | 대상 테이블 | `target_table` | string | 가변 | 넣을 대상 테이블 |
| 5 | 태스크 수 | `task_count` | integer | - | 몇 조각으로 나뉘었는지 |
| 6 | 태스크 계획 목록 | `tasks` | array | - | 조각별 계획. 아래 항목이 조각마다 하나씩 들어 있다 |
| 7 | 담당 executor 주소 | `tasks[].executor_url` | string | 가변 | 그 조각을 맡을 executor |
| 8 | 담당 파티션 값 | `tasks[].partition_values` | array | - | 그 조각이 읽을 파티션 값 목록 |
| 9 | 조각 SELECT 문 | `tasks[].sub_query` | string | 가변 | 그 조각이 실제로 던질 SELECT |
| 10 | 스테이징 테이블 | `tasks[].staging_table` | string | 가변 | [선택] 스테이징을 쓰는 모드에서만 나온다 |
| 11 | 스테이징 생성문 | `tasks[].staging_ddl` | string | 가변 | [선택] `stage_insert` 에서만 나온다 |
| 12 | 대상 INSERT 문 | `tasks[].insert_sql` | string | 가변 | [선택] 스테이징을 쓰는 모드에서만 나온다 |
| 13 | 외부테이블 컬럼 정의 | `tasks[].external_columns` | string | 가변 | [선택] `s3_stage` 에서만 나온다 |

## IF-JOB-002 작업 진행률 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-002` |
| 인터페이스명 | 작업 진행률 조회 |
| 메서드 | `GET` |
| URI | `/jobs/{job_id}/status` |
| 방식 | 동기 |
| 설명 | 작업이 지금 어디까지 갔는지 가볍게 확인한다 |

**주기적으로 물어볼 때 쓰는 인터페이스다.** 조각 하나하나의 사정은 빼고 전체 상태와 진행률만
담기 때문에 응답이 짧다. 몇 초에 한 번씩 불러도 부담이 없다. 조각별 사정까지 봐야 하면
`IF-JOB-003` 을 쓴다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 주소에 넣는다. 작업 생성 때 받은 값이다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 조회한 작업의 번호 |
| 2 | 작업 상태 | `status` | enum | 9 | `PENDING`·`SPLITTING`·`RUNNING`·`DONE`·`PARTIAL`·`FAILED`·`CANCELLED` 중 하나 |
| 3 | 진행률 | `progress_percent` | number | 0~100 | 끝난 조각의 비율. 소수점 한 자리다 |
| 4 | 완료 조각 수 | `completed` | integer | - | 끝난 조각 수. 실패와 취소도 "더 할 일이 없다"는 뜻에서 여기 포함된다 |
| 5 | 전체 조각 수 | `total` | integer | - | 이 작업이 몇 조각으로 나뉘었는지 |
| 6 | 적재 행 수 | `total_rows_written` | integer | - | 지금까지 대상 테이블에 넣은 행 수의 합 |
| 7 | 오류 메시지 | `error` | string | 가변 | [선택] 실패했을 때만 사유가 들어 있다 |
| 8 | 취소 요청 여부 | `cancel_requested` | boolean | - | 취소가 접수됐는지. `true` 면 곧 멈춘다 |
| 9 | 접수 시각 | `created_at` | string | 23 | 요청이 접수된 시각 |
| 10 | 시작 시각 | `started_at` | string | 23 | [선택] 실제로 실행이 시작된 시각. 대기 중이면 비어 있다 |
| 11 | 종료 시각 | `finished_at` | string | 23 | [선택] 끝난 시각. 진행 중이면 비어 있다 |

일곱 가지 상태의 뜻은 이렇다. `PENDING` 은 자리가 나기를 기다리는 중, `SPLITTING` 은 방금 접수돼
조각을 나누는 중, `RUNNING` 은 실제로 옮기는 중이다. 끝난 뒤에는 넷으로 갈리는데 `DONE` 은 모두
성공, `PARTIAL` 은 일부만 성공, `FAILED` 는 전부 실패, `CANCELLED` 는 사람이 취소한 것이다.

## IF-JOB-003 작업 상세 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-003` |
| 인터페이스명 | 작업 상세 조회 |
| 메서드 | `GET` |
| URI | `/jobs/{job_id}` |
| 방식 | 동기 |
| 설명 | 작업 전체 상태에 더해 조각(태스크) 하나하나의 상태까지 함께 돌려준다 |

`IF-JOB-002` 가 주는 것에 조각별 상세를 얹은 인터페이스다. **어느 조각이 느린지, 어느 조각이
실패했는지**를 봐야 할 때 쓴다. 조각이 128개면 응답도 그만큼 길어지므로 반복 폴링에는 맞지 않다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 주소에 넣는다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 조회한 작업의 번호 |
| 2 | 작업 상태 | `status` | enum | 9 | `IF-JOB-002` 와 같은 일곱 가지 |
| 3 | 완료 조각 수 | `completed` | integer | - | 끝난 조각 수 |
| 4 | 전체 조각 수 | `total` | integer | - | 전체 조각 수 |
| 5 | 진행률 | `progress_percent` | number | 0~100 | 끝난 조각의 비율 |
| 6 | 적재 행 수 | `total_rows_written` | integer | - | 대상 테이블에 넣은 행 수의 합 |
| 7 | 조회 행 수 | `total_rows_read` | integer | - | 원본에서 읽은 행 수의 합. 적재 수와 다르면 중간에 걸러진 것이 있다는 뜻이다 |
| 8 | 단계별 조각 수 | `phase_summary` | object | - | 진행 중인 조각이 어느 단계에 몇 개씩 있는지. 예: `{"STREAM_COPY": 3}` |
| 9 | 오류 메시지 | `error` | string | 가변 | [선택] 작업 전체의 실패 사유 |
| 10 | 취소 요청 여부 | `cancel_requested` | boolean | - | 취소가 접수됐는지 |
| 11 | 접수 시각 | `created_at` | string | 23 | 요청이 접수된 시각 |
| 12 | 시작 시각 | `started_at` | string | 23 | [선택] 실행이 시작된 시각 |
| 13 | 종료 시각 | `finished_at` | string | 23 | [선택] 끝난 시각 |
| 14 | 원본 작업 식별자 | `retry_of` | string | 16 | [선택] 재실행으로 만들어진 작업이면 원래 작업의 번호 |
| 15 | 조각 목록 | `tasks` | array | - | 조각별 상태. 아래 항목이 조각마다 하나씩 들어 있다 |
| 16 | 태스크 식별자 | `tasks[].task_id` | string | 14 | 조각의 번호 |
| 17 | 담당 executor 주소 | `tasks[].executor_url` | string | 가변 | 이 조각을 맡은 executor |
| 18 | 태스크 상태 | `tasks[].status` | enum | 9 | `QUEUED`·`READING`·`WRITING`·`DONE`·`FAILED`·`CANCELLED` 중 하나 |
| 19 | 적재 행 수 | `tasks[].rows_written` | integer | - | 이 조각이 넣은 행 수 |
| 20 | 조회 행 수 | `tasks[].rows_read` | integer | - | 이 조각이 읽은 행 수 |
| 21 | 현재 단계 | `tasks[].current_phase` | string | 가변 | [선택] 지금 하고 있는 세부 단계의 이름 |
| 22 | 원본 읽기 완료 시각 | `tasks[].impala_done_at` | string | 23 | [선택] 원본에서 다 읽어 낸 시각 |
| 23 | 단계 기록 | `tasks[].phases` | array | - | 거쳐 온 단계들의 기록. 아래 여섯 항목이 단계마다 들어 있다 |
| 24 | 단계 이름 | `tasks[].phases[].name` | string | 가변 | 단계의 코드 이름 |
| 25 | 단계 표시명 | `tasks[].phases[].label` | string | 가변 | 화면에 보여 줄 우리말 이름 |
| 26 | 단계 시작 시각 | `tasks[].phases[].started_at` | string | 23 | 그 단계가 시작된 시각 |
| 27 | 단계 종료 시각 | `tasks[].phases[].finished_at` | string | 23 | [선택] 그 단계가 끝난 시각. 진행 중이면 비어 있다 |
| 28 | 단계 소요 시간 | `tasks[].phases[].duration_ms` | integer | - | [선택] 그 단계에 걸린 시간(밀리초) |
| 29 | 단계 처리 행 수 | `tasks[].phases[].rows` | integer | - | [선택] 그 단계에서 다룬 행 수 |
| 30 | 시도 횟수 | `tasks[].attempt` | integer | - | 이 조각을 몇 번째 시도하고 있는지 |
| 31 | 담당 파티션 값 | `tasks[].partition_values` | array | - | 이 조각이 맡은 파티션 값 목록 |
| 32 | 오류 메시지 | `tasks[].error` | string | 가변 | [선택] 이 조각이 실패한 사유 |

## IF-JOB-004 작업 결과 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-004` |
| 인터페이스명 | 작업 결과 조회 |
| 메서드 | `GET` |
| URI | `/jobs/{job_id}/result` |
| 방식 | 동기 |
| 설명 | 끝난 작업이 몇 건을 넣었는지 조각별로 정리해 돌려준다 |

작업이 끝난 뒤 **원본과 대상의 건수를 맞춰 볼 때** 쓴다. 조각별 건수가 함께 나오므로 특정 파티션만
비어 있는 경우를 바로 짚어 낼 수 있다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 주소에 넣는다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 조회한 작업의 번호 |
| 2 | 작업 상태 | `status` | enum | 9 | 끝난 상태. 보통 `DONE`·`PARTIAL`·`FAILED` 중 하나다 |
| 3 | 적재 행 수 | `total_rows_written` | integer | - | 대상 테이블에 넣은 전체 행 수 |
| 4 | 조각별 결과 | `per_task` | array | - | 조각별 적재 건수 목록 |
| 5 | 태스크 식별자 | `per_task[].task_id` | string | 14 | 조각의 번호 |
| 6 | 조각 적재 행 수 | `per_task[].rows_written` | integer | - | 그 조각이 넣은 행 수 |

## IF-JOB-005 태스크 상세 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-005` |
| 인터페이스명 | 태스크 상세 조회 |
| 메서드 | `GET` |
| URI | `/jobs/{job_id}/tasks/{task_id}` |
| 방식 | 동기 |
| 설명 | 조각 하나의 상태에 실제로 던진 SELECT 문 전문을 더해 돌려준다 |

**조각 하나가 왜 실패했는지 파고들 때** 쓴다. `IF-JOB-003` 의 조각 항목과 내용이 같고 거기에
`sub_query` 하나가 더 붙는데, 이 값이 그 조각이 원본에 실제로 던진 SELECT 전문이다. 이것을 그대로
복사해 손으로 실행해 보면 실패 원인이 대개 드러난다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 주소에 넣는다 |
| 2 | 태스크 식별자 | `task_id` | string | 14 | 주소에 넣는다. `IF-JOB-003` 의 조각 목록에서 얻는다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 태스크 식별자 | `task_id` | string | 14 | 조회한 조각의 번호 |
| 2 | 담당 executor 주소 | `executor_url` | string | 가변 | 이 조각을 맡은 executor |
| 3 | 태스크 상태 | `status` | enum | 9 | `QUEUED`·`READING`·`WRITING`·`DONE`·`FAILED`·`CANCELLED` 중 하나 |
| 4 | 적재 행 수 | `rows_written` | integer | - | 이 조각이 넣은 행 수 |
| 5 | 조회 행 수 | `rows_read` | integer | - | 이 조각이 읽은 행 수 |
| 6 | 현재 단계 | `current_phase` | string | 가변 | [선택] 지금 하고 있는 세부 단계 |
| 7 | 원본 읽기 완료 시각 | `impala_done_at` | string | 23 | [선택] 원본에서 다 읽어 낸 시각 |
| 8 | 단계 기록 | `phases` | array | - | 거쳐 온 단계 기록. 항목 구성은 `IF-JOB-003` 과 같다 |
| 9 | 시도 횟수 | `attempt` | integer | - | 몇 번째 시도인지 |
| 10 | 담당 파티션 값 | `partition_values` | array | - | 이 조각이 맡은 파티션 값 목록 |
| 11 | 오류 메시지 | `error` | string | 가변 | [선택] 실패 사유 |
| 12 | 조각 SELECT 문 | `sub_query` | string | 가변 | 이 조각이 원본에 실제로 던진 SELECT 전문 |

## IF-JOB-006 작업 취소

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-006` |
| 인터페이스명 | 작업 취소 |
| 메서드 | `POST` |
| URI | `/jobs/{job_id}/cancel` |
| 방식 | 동기 |
| 설명 | 진행 중인 작업을 멈춘다. 각 executor 에 취소가 전달되고 작업은 `CANCELLED` 가 된다 |

보낼 본문은 없다. 주소에 작업 번호만 담으면 된다. 응답은 `IF-JOB-002` 와 같은 모양이며, 그때의
`status` 는 이미 `CANCELLED` 로 바뀐 상태다.

**이미 끝난 작업은 취소할 수 없다.** `DONE`·`FAILED`·`CANCELLED` 인 작업에 취소를 걸면 `409` 로
거절한다. 되돌릴 것이 없기 때문이다.

**취소해도 이미 들어간 데이터는 남는다.** 취소는 앞으로 할 일을 멈추는 것이지 지금까지 넣은 것을
되돌리는 것이 아니다. 되돌려야 한다면 `write_mode` 를 `overwrite_partitions` 로 두고 다시 돌리거나
대상 테이블을 직접 정리한다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 주소에 넣는다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 취소한 작업의 번호 |
| 2 | 작업 상태 | `status` | enum | 9 | 취소가 반영돼 `CANCELLED` 로 나온다 |
| 3 | 진행률 | `progress_percent` | number | 0~100 | 멈춘 시점까지의 진행률 |
| 4 | 완료 조각 수 | `completed` | integer | - | 멈춘 시점까지 끝난 조각 수 |
| 5 | 전체 조각 수 | `total` | integer | - | 전체 조각 수 |
| 6 | 적재 행 수 | `total_rows_written` | integer | - | 멈추기 전까지 넣은 행 수 |
| 7 | 오류 메시지 | `error` | string | 가변 | [선택] 오류로 멈춘 것이 아니면 비어 있다 |
| 8 | 취소 요청 여부 | `cancel_requested` | boolean | - | 언제나 `true` 다 |
| 9 | 접수 시각 | `created_at` | string | 23 | 요청이 접수된 시각 |
| 10 | 시작 시각 | `started_at` | string | 23 | [선택] 실행이 시작된 시각 |
| 11 | 종료 시각 | `finished_at` | string | 23 | [선택] 멈춘 시각 |

## IF-JOB-007 실패 파티션 재실행

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-007` |
| 인터페이스명 | 실패 파티션 재실행 |
| 메서드 | `POST` |
| URI | `/jobs/{job_id}/retry` |
| 방식 | 비동기(접수 후 `202`) |
| 설명 | 끝난 작업에서 실패하거나 취소된 조각만 모아 새 작업으로 다시 돌린다 |

**성공한 조각은 건드리지 않는다.** 이미 잘 들어간 파티션을 다시 읽으면 시간도 낭비고 중복 위험도
있기 때문이다. 그래서 실패와 취소 조각만 새 작업으로 복제한다. 원래 작업은 그대로 남고 **새 작업
번호가 발급된다.**

되돌릴 수 있는 상태는 `PARTIAL`·`FAILED`·`CANCELLED` 셋뿐이다. 아직 진행 중인 작업이나 전부
성공한 작업에 걸면 `409` 로 거절한다. 다시 돌릴 조각이 하나도 없을 때도 마찬가지다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 식별자 | `job_id` | string | 16 | 주소에 넣는다. 다시 돌릴 원래 작업의 번호다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 새 작업 식별자 | `job_id` | string | 16 | 새로 만들어진 작업의 번호. 이후 조회는 이 번호로 한다 |
| 2 | 원본 작업 식별자 | `retry_of` | string | 16 | 어느 작업을 다시 돌린 것인지 |
| 3 | 재실행 조각 수 | `retried_tasks` | integer | - | 다시 돌리는 조각이 몇 개인지 |

## IF-JOB-008 작업 목록 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-008` |
| 인터페이스명 | 작업 목록 조회 |
| 메서드 | `GET` |
| URI | `/jobs` |
| 방식 | 동기 |
| 설명 | 지금 coordinator 가 들고 있는 작업들을 최신순으로 돌려준다 |

**여기 나오는 것은 메모리에 남아 있는 작업뿐이다.** coordinator 를 다시 띄우면 목록이 비워지므로,
지난 기록을 보려면 `IF-JOB-009` 를 쓴다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 상태 필터 | `status` | string | 가변 | [선택] 그 상태의 작업만 본다. `running` 이나 `active` 를 주면 처리 중인 것을 묶어서 본다 |
| 2 | 조회 개수 | `limit` | integer | - | [선택] 최대 몇 건까지 볼지. 기본 100 이고 0 이하면 전부 본다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 작업 목록 | `jobs` | array | - | 작업 한 건이 아래 항목들로 이루어진다 |
| 2 | 작업 식별자 | `jobs[].job_id` | string | 16 | 작업의 번호 |
| 3 | 작업 상태 | `jobs[].status` | enum | 9 | 일곱 가지 상태 중 하나 |
| 4 | 요청 사용자 | `jobs[].username` | string | 가변 | [선택] 누가 시켰는지 |
| 5 | 진행률 | `jobs[].progress_percent` | number | 0~100 | 끝난 조각의 비율 |
| 6 | 완료 조각 수 | `jobs[].completed` | integer | - | 끝난 조각 수 |
| 7 | 전체 조각 수 | `jobs[].total` | integer | - | 전체 조각 수 |
| 8 | 적재 행 수 | `jobs[].total_rows_written` | integer | - | 넣은 행 수의 합 |
| 9 | 조회 행 수 | `jobs[].total_rows_read` | integer | - | 읽은 행 수의 합 |
| 10 | 단계별 조각 수 | `jobs[].phase_summary` | object | - | 진행 중인 조각이 어느 단계에 몇 개씩 있는지 |
| 11 | 실행 모드 | `jobs[].exec_mode` | enum | 12 | 이 작업이 쓰는 실행 모드 |
| 12 | 파티션 컬럼 | `jobs[].partition_column` | string | 가변 | 나눌 기준으로 쓴 컬럼 |
| 13 | 대상 테이블 | `jobs[].target_table` | string | 가변 | 넣을 대상 테이블 |
| 14 | 접수 시각 | `jobs[].created_at` | string | 23 | 요청이 접수된 시각 |
| 15 | 시작 시각 | `jobs[].started_at` | string | 23 | [선택] 실행이 시작된 시각 |
| 16 | 종료 시각 | `jobs[].finished_at` | string | 23 | [선택] 끝난 시각 |
| 17 | 원본 SELECT 문 | `jobs[].original_sql` | string | 가변 | 나누기 전의 원래 SELECT |
| 18 | 오류 메시지 | `jobs[].error` | string | 가변 | [선택] 실패 사유 |
| 19 | 전체 작업 수 | `total` | integer | - | 필터와 무관하게 지금 들고 있는 작업 수 |
| 20 | 실행 중 작업 수 | `running` | integer | - | `RUNNING` 인 작업 수 |
| 21 | 처리 중 작업 수 | `active` | integer | - | 실행 중에 분할 중과 대기 중까지 더한 수 |
| 22 | 대기 중 작업 수 | `pending` | integer | - | 자리가 나기를 기다리는 작업 수 |
| 23 | 동시 실행 한도 | `max_concurrent_jobs` | integer | - | 한 번에 실행할 수 있는 작업 수 |
| 24 | 대기 큐 한도 | `max_pending_jobs` | integer | - | 줄 서서 기다릴 수 있는 작업 수 |

마지막 네 항목을 함께 보면 **지금 얼마나 여유가 있는지** 알 수 있다. `running` 이 한도에 닿고
`pending` 까지 차오르고 있다면 새 요청이 `429` 로 거절될 때가 가까워진 것이다.

## IF-JOB-009 실행 이력 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-009` |
| 인터페이스명 | 실행 이력 조회 |
| 메서드 | `GET` |
| URI | `/history` |
| 방식 | 동기 |
| 설명 | 데이터베이스에 쌓아 둔 지난 실행 기록을 조건으로 걸러 페이지 단위로 돌려준다 |

`IF-JOB-008` 이 지금 메모리에 있는 것을 보여 준다면, 이쪽은 **다시 띄운 뒤에도 남는 기록**을
보여 준다. 한 작업에 상태가 바뀔 때마다 여러 줄이 쌓이지만, 여기서는 작업마다 **가장 최근 한 줄만**
골라 돌려주므로 목록에 같은 작업이 여러 번 나오지 않는다.

이력 저장이 설정돼 있지 않으면 오류 대신 `enabled` 가 `false` 인 빈 결과가 온다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 조회 개수 | `limit` | integer | 1~200 | [선택] 한 페이지에 몇 건을 볼지. 기본 20 |
| 2 | 건너뛸 개수 | `offset` | integer | 0 이상 | [선택] 앞에서 몇 건을 건너뛸지. 기본 0 |
| 3 | 상태 필터 | `status` | string | 가변 | [선택] 그 최종 상태인 것만 본다. 대소문자는 가리지 않는다 |
| 4 | 사용자 필터 | `username` | string | 가변 | [선택] 그 사용자가 시킨 것만 본다 |
| 5 | 작업 번호 필터 | `job_id` | string | 가변 | [선택] 번호가 이 글자로 시작하는 것만 본다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 이력 사용 여부 | `enabled` | boolean | - | 이력 저장이 켜져 있는지. `false` 면 나머지는 비어 있다 |
| 2 | 이력 목록 | `rows` | array | - | 기록 한 건이 아래 항목들로 이루어진다 |
| 3 | 기록 시각 | `rows[].recorded_at` | string | 23 | 이 기록이 남은 시각 |
| 4 | 작업 식별자 | `rows[].job_id` | string | 16 | 작업의 번호 |
| 5 | 요청 사용자 | `rows[].username` | string | 가변 | [선택] 누가 시켰는지 |
| 6 | 작업 상태 | `rows[].status` | enum | 9 | 그 작업의 최종 상태 |
| 7 | 파티션 컬럼 | `rows[].partition_column` | string | 가변 | 나눌 기준으로 쓴 컬럼 |
| 8 | 대상 테이블 | `rows[].target_table` | string | 가변 | 넣은 대상 테이블 |
| 9 | 완료 조각 수 | `rows[].completed_tasks` | integer | - | 끝난 조각 수 |
| 10 | 전체 조각 수 | `rows[].total_tasks` | integer | - | 전체 조각 수 |
| 11 | 적재 행 수 | `rows[].total_rows_written` | integer | - | 넣은 행 수의 합 |
| 12 | 오류 메시지 | `rows[].error` | string | 가변 | [선택] 실패 사유 |
| 13 | 시작 시각 | `rows[].started_at` | string | 23 | [선택] 실행이 시작된 시각 |
| 14 | 종료 시각 | `rows[].finished_at` | string | 23 | [선택] 끝난 시각 |
| 15 | 원본 SELECT 문 | `rows[].original_sql` | string | 가변 | 나누기 전의 원래 SELECT |
| 16 | 전체 건수 | `total` | integer | - | 같은 조건으로 세었을 때의 전체 건수. 페이지를 넘길 때 쓴다 |
| 17 | 조회 개수 | `limit` | integer | 1~200 | 실제로 적용된 페이지 크기 |
| 18 | 건너뛴 개수 | `offset` | integer | 0 이상 | 실제로 적용된 시작 위치 |

## IF-JOB-010 템플릿 목록 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-JOB-010` |
| 인터페이스명 | 템플릿 목록 조회 |
| 메서드 | `GET` |
| URI | `/templates` |
| 방식 | 동기 |
| 설명 | 서버에 등록된 쿼리 템플릿과 각 템플릿이 요구하는 파라미터를 돌려준다 |

템플릿을 쓰려면 **어떤 템플릿이 있고 무엇을 넣어야 하는지**를 먼저 알아야 한다. 이 인터페이스가
그것을 알려 준다. 여기서 받은 파라미터 목록을 그대로 `IF-JOB-001` 이나 `IF-QRY-001` 의 `params` 로
채우면 된다. 보낼 항목은 없다.

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 템플릿 사용 여부 | `enabled` | boolean | - | 템플릿 기능이 켜져 있는지. `false` 면 목록은 비어 있다 |
| 2 | 템플릿 목록 | `templates` | array | - | 템플릿 하나가 아래 항목들로 이루어진다 |
| 3 | 템플릿 식별자 | `templates[].template_id` | string | 가변 | 요청에 적을 템플릿 이름 |
| 4 | 템플릿 설명 | `templates[].description` | string | 가변 | [선택] 이 템플릿이 무엇을 하는지 |
| 5 | 기본 실행 모드 | `templates[].exec_mode` | enum | 12 | [선택] 요청이 지정하지 않으면 쓰일 실행 모드 |
| 6 | 기본 파티션 컬럼 | `templates[].partition_column` | string | 가변 | [선택] 요청이 지정하지 않으면 쓰일 파티션 컬럼 |
| 7 | 파라미터 목록 | `templates[].params` | array | - | 이 템플릿이 요구하는 값들 |
| 8 | 파라미터 이름 | `templates[].params[].name` | string | 가변 | 요청의 `params` 에 적을 이름 |
| 9 | 파라미터 타입 | `templates[].params[].type` | string | 가변 | `string`·`int`·`date` 처럼 넣을 값의 종류 |
| 10 | 필수 여부 | `templates[].params[].required` | boolean | - | `true` 면 반드시 채워야 한다 |
| 11 | 기본값 | `templates[].params[].default` | 값에 따라 다름 | - | [선택] 비웠을 때 대신 쓰일 값 |

---

# 쿼리(Query) 인터페이스

데이터를 옮기지 않고 **결과를 그 자리에서 받아 보는** 묶음이다. 옮기기 전에 원본이 어떻게 생겼는지
확인하거나, 옮긴 뒤에 대상 테이블이 제대로 채워졌는지 들여다볼 때 쓴다.

**돌려주는 행 수에는 상한이 있다.** 최대 1만 행이고 기본은 100행이다. 미리 보는 용도이지 데이터를
받아 가는 용도가 아니기 때문이다. 대량으로 옮겨야 한다면 `IF-JOB-001` 을 쓴다.

## IF-QRY-001 템플릿 쿼리 실행

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-QRY-001` |
| 인터페이스명 | 템플릿 쿼리 실행 |
| 메서드 | `POST` |
| URI | `/query-execute` |
| 방식 | 동기 |
| 설명 | 서버 템플릿을 값으로 채워 SELECT 를 만들고 실행해 상위 몇 행을 바로 돌려준다 |

**보내는 쪽은 SQL 을 쓰지 않는다.** 템플릿 이름과 값만 보내면 서버가 SQL 을 만들어 실행한다.
어떤 템플릿에 어떤 값이 필요한지는 `IF-JOB-010` 으로 미리 확인한다.

**어디서 실행되는지는 신경 쓰지 않아도 된다.** 원본 쪽 엔진은 coordinator 가 가장 한가한 executor
를 골라 대신 물어보고, Greenplum 과 이력 데이터베이스는 coordinator 가 직접 실행한다. 실제로 누가
실행했는지는 응답의 `executed_by` 에 담겨 오고, coordinator 가 직접 했다면 비어 있다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 템플릿 식별자 | `template_id` | string | 가변 | 실행할 템플릿 이름 |
| 2 | 파라미터 목록 | `params` | array | - | [선택] 템플릿에 넣을 값들 |
| 3 | 파라미터 이름 | `params[].name` | string | 가변 | 템플릿이 요구하는 이름 |
| 4 | 파라미터 값 | `params[].value` | 값에 따라 다름 | - | 넣을 값 |
| 5 | 연산자 방향 | `params[].sign` | enum | 1 | [선택] `+` 또는 `-`. 값의 부호가 아니라 SQL 에 들어갈 연산자 방향이다 |
| 6 | 데이터소스 | `datasource` | string | 가변 | [선택] 어디에 물어볼지. `impala`·`trino`·`greenplum`·`history` 중 하나 |
| 7 | 조회 행 수 | `limit` | integer | 1~10000 | [선택] 최대 몇 행을 받을지. 기본 100 |
| 8 | SQL 방언 | `sql_dialect` | string | 가변 | [선택] 만들어진 SELECT 를 검사할 문법 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 템플릿 식별자 | `template_id` | string | 가변 | 실행한 템플릿 이름 |
| 2 | 데이터소스 | `datasource` | string | 가변 | 실제로 물어본 곳 |
| 3 | 실행 SQL | `sql` | string | 가변 | 템플릿에서 만들어진 SELECT 전문. 감사와 확인에 쓴다 |
| 4 | 조회 행 수 | `limit` | integer | 1~10000 | 실제로 적용된 상한 |
| 5 | 실행 executor 주소 | `executed_by` | string | 가변 | [선택] 대신 실행한 executor. coordinator 가 직접 했으면 비어 있다 |
| 6 | 컬럼 목록 | `columns` | array | - | 결과의 컬럼 이름들. 값의 순서와 짝이 맞는다 |
| 7 | 결과 행 목록 | `rows` | array | - | 행 하나가 값 목록 하나다. 컬럼 순서대로 들어 있다 |
| 8 | 반환 행 수 | `row_count` | integer | - | 실제로 돌려준 행 수 |
| 9 | 잘림 여부 | `truncated` | boolean | - | `true` 면 상한에 걸려 잘렸다는 뜻이다 |
| 10 | 소요 시간 | `elapsed_ms` | integer | - | 쿼리에 걸린 시간(밀리초) |

**`truncated` 를 꼭 확인한다.** 이 값이 `true` 인데 그대로 건수를 세면 실제보다 적게 센다. 값이
비었을 때도 조심할 것이 하나 있는데, JSON 이 표현하지 못하는 값(숫자가 아닌 실수 등)은 모두
`null` 로 바뀌어 나온다는 점이다.

## IF-QRY-002 데이터소스 SELECT 미리보기

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-QRY-002` |
| 인터페이스명 | 데이터소스 SELECT 미리보기 |
| 메서드 | `POST` |
| URI | `/datasources/{name}/query` |
| 방식 | 동기 |
| 설명 | 지정한 데이터소스에 SQL 을 직접 실행해 상위 몇 행을 돌려준다 |

`IF-QRY-001` 과 달리 **SQL 을 그대로 보낸다.** 접속이 살아 있는지 확인하거나 대상 테이블을 빠르게
들여다볼 때 쓰는 **운영 점검용** 인터페이스다.

**Impala 는 `executor_url` 을 함께 보내야 한다.** coordinator 에는 원본 쪽 드라이버가 없어서
executor 를 거쳐야 하기 때문이다. 빠뜨리면 `400` 이 온다. Greenplum 과 이력 데이터베이스는
coordinator 가 직접 실행하므로 그냥 보내면 된다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 데이터소스 이름 | `name` | string | 가변 | 주소에 넣는다. `impala`·`greenplum`·`history` 중 하나다 |
| 2 | 실행 SQL | `sql` | string | 가변 | [선택] 실행할 SELECT. 기본은 접속만 확인하는 `SELECT 1` 이다 |
| 3 | 조회 행 수 | `limit` | integer | 1~10000 | [선택] 최대 몇 행을 받을지. 기본 100 |
| 4 | executor 주소 | `executor_url` | string | 가변 | [선택] 주면 그 executor 를 거쳐 실행한다. Impala 는 반드시 준다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 데이터소스 이름 | `datasource` | string | 가변 | 실제로 실행한 데이터소스의 이름 |
| 2 | 조회 행 수 | `limit` | integer | 1~10000 | 실제로 적용된 상한 |
| 3 | 컬럼 목록 | `columns` | array | - | 결과의 컬럼 이름들 |
| 4 | 결과 행 목록 | `rows` | array | - | 행 하나가 값 목록 하나다 |
| 5 | 반환 행 수 | `row_count` | integer | - | 실제로 돌려준 행 수 |
| 6 | 잘림 여부 | `truncated` | boolean | - | `true` 면 상한에 걸려 잘렸다 |
| 7 | 소요 시간 | `elapsed_ms` | integer | - | 쿼리에 걸린 시간(밀리초) |
| 8 | 경유 executor 주소 | `proxied_to` | string | 가변 | [선택] executor 를 거쳤을 때만 그 주소가 담긴다 |

## IF-QRY-003 데이터소스 목록 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-QRY-003` |
| 인터페이스명 | 데이터소스 목록 조회 |
| 메서드 | `GET` |
| URI | `/datasources` |
| 방식 | 동기 |
| 설명 | 미리보기를 걸 수 있는 데이터소스와, 그중 접속 정보가 채워진 것이 무엇인지 알려 준다 |

`IF-QRY-002` 를 쓰기 전에 **무엇을 부를 수 있는지** 확인하는 인터페이스다. 보낼 항목은 없다.
`local` 은 coordinator 가 직접 부를 수 있는 것이고, `via_executor` 는 executor 를 거쳐야 하는
것이며, `executors` 는 그때 `executor_url` 로 쓸 수 있는 주소 목록이다.

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 직접 실행 가능 목록 | `local` | array | - | coordinator 가 직접 부를 수 있는 데이터소스 |
| 2 | 데이터소스 이름 | `local[].name` | string | 가변 | `history` 또는 `greenplum` |
| 3 | 접속 정보 설정 여부 | `local[].configured` | boolean | - | `false` 면 접속 정보가 비어 있어 부를 수 없다 |
| 4 | executor 경유 목록 | `via_executor` | array | - | executor 를 거쳐야 부를 수 있는 데이터소스 이름들 |
| 5 | executor 주소 목록 | `executors` | array | - | 설정에 등록된 executor 주소들 |

---

# 모니터링(Monitoring) 인터페이스

지금 시스템이 어떤 상태인지 들여다보는 묶음이다. 감시 도구를 붙이거나 운영 화면을 만들 때 쓴다.
이 묶음은 모두 조회만 하므로 무엇을 부르든 시스템 상태가 바뀌지 않는다.

가볍게는 `IF-MON-001` 로 살아 있는지만 확인하고, 자원이 얼마나 남았는지는 `IF-MON-002`, 전체
그림은 `IF-MON-003` 하나로 본다.

## IF-MON-001 헬스 체크

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-MON-001` |
| 인터페이스명 | 헬스 체크 |
| 메서드 | `GET` |
| URI | `/health` |
| 방식 | 동기 |
| 설명 | coordinator 프로세스가 살아 있는지 확인한다 |

**가장 가벼운 인터페이스다.** 데이터베이스에도 executor 에도 묻지 않고 곧바로 답하므로 감시
도구가 몇 초에 한 번씩 불러도 부담이 없다. 다만 가볍다는 것은 뒤집으면 **프로세스가 떠 있다는 것
말고는 아무것도 보장하지 않는다**는 뜻이다. 데이터베이스가 끊겼는지까지 보려면 `IF-MON-003` 을
쓴다. 보낼 항목은 없다.

같은 답을 주는 `/healthz` 주소도 있다. 쿠버네티스처럼 그 이름을 기대하는 도구를 위해 남겨 둔
별칭인데, 이쪽은 `status` 하나만 돌려준다.

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 상태 | `status` | string | 2 | 살아 있으면 언제나 `ok` 다 |
| 2 | 역할 | `service` | string | 11 | `coordinator` 로 고정이다 |
| 3 | 버전 | `version` | string | 가변 | 지금 떠 있는 소프트웨어 버전 |

## IF-MON-002 시스템 메트릭 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-MON-002` |
| 인터페이스명 | 시스템 메트릭 조회 |
| 메서드 | `GET` |
| URI | `/metrics` |
| 방식 | 동기 |
| 설명 | coordinator 가 떠 있는 서버의 CPU·메모리·디스크 사용량을 돌려준다 |

**이것은 coordinator 서버의 상태이지 executor 의 상태가 아니다.** executor 쪽은 `IF-MON-004` 나
`IF-MON-005` 로 본다. 보낼 항목은 없다.

CPU 사용률을 재느라 이 호출은 0.1초 정도 머문다는 점만 알아 두면 된다. 아주 촘촘히 부르는 용도는
아니다.

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | CPU 사용률 | `cpu_percent` | number | 0~100 | 최근 짧은 구간의 CPU 사용률 |
| 2 | 메모리 정보 | `memory` | object | - | 메모리 사용 현황 |
| 3 | 전체 메모리 | `memory.total_mb` | number | - | 서버에 달린 전체 메모리(MB) |
| 4 | 사용 메모리 | `memory.used_mb` | number | - | 지금 쓰고 있는 메모리(MB) |
| 5 | 메모리 사용률 | `memory.percent` | number | 0~100 | 메모리 사용 비율 |
| 6 | 디스크 정보 | `disk` | object | - | 디스크 사용 현황 |
| 7 | 측정 경로 | `disk.path` | string | 가변 | 어느 경로를 기준으로 쟀는지 |
| 8 | 전체 용량 | `disk.total_gb` | number | - | 그 경로가 속한 디스크의 전체 용량(GB) |
| 9 | 사용 용량 | `disk.used_gb` | number | - | 지금 쓰고 있는 용량(GB) |
| 10 | 디스크 사용률 | `disk.percent` | number | 0~100 | 디스크 사용 비율 |

## IF-MON-003 클러스터 상태 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-MON-003` |
| 인터페이스명 | 클러스터 상태 조회 |
| 메서드 | `GET` |
| URI | `/cluster` |
| 방식 | 동기 |
| 설명 | coordinator 와 모든 executor 의 상태, 그리고 작업 현황을 한 번에 돌려준다 |

**운영 화면을 만든다면 이 하나로 대부분이 채워진다.** 누가 살아 있고 자원은 얼마나 쓰고 있으며
작업은 몇 건이 돌고 있는지가 한 응답에 모두 담긴다.

`refresh` 를 `true`(기본)로 두면 물어보는 그 자리에서 executor 들을 즉시 확인하므로 값이 가장
최신이지만 응답이 조금 느리다. 촘촘히 새로 고치는 화면이라면 `false` 로 두어 직전에 확인해 둔
값을 쓰는 편이 낫다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 즉시 확인 여부 | `refresh` | boolean | - | [선택] `true`(기본)면 지금 확인하고, `false` 면 직전 확인 값을 쓴다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | coordinator 정보 | `coordinator` | object | - | coordinator 자신의 상태 |
| 2 | 역할 | `coordinator.service` | string | 11 | `coordinator` 로 고정이다 |
| 3 | 상태 | `coordinator.status` | string | 2 | 답을 했다는 것 자체가 살아 있다는 뜻이라 언제나 `ok` 다 |
| 4 | 자원 사용량 | `coordinator.metrics` | object | - | `IF-MON-002` 와 같은 구성의 CPU·메모리·디스크 |
| 5 | executor 목록 | `executors` | array | - | executor 하나가 아래 항목들로 이루어진다 |
| 6 | executor 주소 | `executors[].executor_url` | string | 가변 | 그 executor 의 주소 |
| 7 | 정상 여부 | `executors[].healthy` | boolean | - | 마지막 확인에서 답을 했는지 |
| 8 | 확인 시각 | `executors[].last_checked` | string | 가변 | 마지막으로 확인해 본 시각 |
| 9 | CPU 사용률 | `executors[].cpu_percent` | number | 0~100 | [선택] 그 서버의 CPU 사용률 |
| 10 | 메모리 사용률 | `executors[].memory_percent` | number | 0~100 | [선택] 메모리 사용 비율 |
| 11 | 사용 메모리 | `executors[].memory_used_mb` | number | - | [선택] 쓰고 있는 메모리(MB) |
| 12 | 전체 메모리 | `executors[].memory_total_mb` | number | - | [선택] 전체 메모리(MB) |
| 13 | 디스크 사용률 | `executors[].disk_percent` | number | 0~100 | [선택] 디스크 사용 비율 |
| 14 | 사용 용량 | `executors[].disk_used_gb` | number | - | [선택] 쓰고 있는 용량(GB) |
| 15 | 전체 용량 | `executors[].disk_total_gb` | number | - | [선택] 전체 용량(GB) |
| 16 | 진행 중 태스크 수 | `executors[].active_tasks` | integer | - | [선택] 그 executor 가 지금 처리 중인 조각 수 |
| 17 | 동시 처리 한도 | `executors[].max_concurrent_tasks` | integer | - | [선택] 한 번에 처리할 수 있는 조각 수 |
| 18 | 오류 메시지 | `executors[].error` | string | 가변 | [선택] 확인에 실패했을 때의 사유 |
| 19 | executor 번호 | `executors[].index` | integer | - | [선택] `IF-MON-005` 에서 쓸 순번. 설정 목록에 없으면 비어 있다 |
| 20 | executor 요약 | `executors_summary` | object | - | executor 수를 정상 여부로 나눈 요약 |
| 21 | 전체 수 | `executors_summary.total` | integer | - | 확인 대상 executor 수 |
| 22 | 정상 수 | `executors_summary.healthy` | integer | - | 답을 한 executor 수 |
| 23 | 비정상 수 | `executors_summary.unhealthy` | integer | - | 답을 하지 않은 executor 수 |
| 24 | 작업 현황 | `jobs` | object | - | 지금 들고 있는 작업의 수 |
| 25 | 실행 중 작업 수 | `jobs.running` | integer | - | 실제로 옮기고 있는 작업 수 |
| 26 | 처리 중 작업 수 | `jobs.active` | integer | - | 실행 중에 분할 중과 대기 중까지 더한 수 |
| 27 | 전체 작업 수 | `jobs.total` | integer | - | 상태와 무관한 전체 수 |
| 28 | 상태별 작업 수 | `jobs.by_status` | object | - | 상태 이름마다 몇 건인지. 예: `{"RUNNING": 2, "DONE": 5}` |
| 29 | executor 배정 횟수 | `assignment_counts` | object | - | 기동 뒤 executor 마다 조각을 몇 번 맡겼는지. 쏠림을 본다 |
| 30 | executor 선택 정책 | `executor_select` | string | 가변 | 어느 executor 에게 맡길지 정하는 방식의 이름 |

## IF-MON-004 executor 상태 목록 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-MON-004` |
| 인터페이스명 | executor 상태 목록 조회 |
| 메서드 | `GET` |
| URI | `/executors` |
| 방식 | 동기 |
| 설명 | 감시 루프가 확인해 둔 executor 별 상태를 돌려준다 |

`IF-MON-003` 에서 executor 부분만 떼어 낸 것이라고 보면 된다. **여기서는 즉시 확인하지 않고
직전에 확인해 둔 값을 그대로 준다.** 그래서 응답이 빠르지만 값은 확인 주기만큼 과거의 것이다.
보낼 항목은 없다.

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | executor 목록 | `executors` | array | - | executor 하나가 아래 항목들로 이루어진다 |
| 2 | executor 주소 | `executors[].executor_url` | string | 가변 | 그 executor 의 주소 |
| 3 | 정상 여부 | `executors[].healthy` | boolean | - | 마지막 확인에서 답을 했는지 |
| 4 | 확인 시각 | `executors[].last_checked` | string | 가변 | 마지막으로 확인해 본 시각 |
| 5 | CPU 사용률 | `executors[].cpu_percent` | number | 0~100 | [선택] 그 서버의 CPU 사용률 |
| 6 | 메모리 사용률 | `executors[].memory_percent` | number | 0~100 | [선택] 메모리 사용 비율 |
| 7 | 사용 메모리 | `executors[].memory_used_mb` | number | - | [선택] 쓰고 있는 메모리(MB) |
| 8 | 전체 메모리 | `executors[].memory_total_mb` | number | - | [선택] 전체 메모리(MB) |
| 9 | 디스크 사용률 | `executors[].disk_percent` | number | 0~100 | [선택] 디스크 사용 비율 |
| 10 | 사용 용량 | `executors[].disk_used_gb` | number | - | [선택] 쓰고 있는 용량(GB) |
| 11 | 전체 용량 | `executors[].disk_total_gb` | number | - | [선택] 전체 용량(GB) |
| 12 | 진행 중 태스크 수 | `executors[].active_tasks` | integer | - | [선택] 지금 처리 중인 조각 수 |
| 13 | 동시 처리 한도 | `executors[].max_concurrent_tasks` | integer | - | [선택] 한 번에 처리할 수 있는 조각 수 |
| 14 | 오류 메시지 | `executors[].error` | string | 가변 | [선택] 확인에 실패했을 때의 사유 |
| 15 | executor 번호 | `executors[].index` | integer | - | [선택] `IF-MON-005` 에서 쓸 순번 |

## IF-MON-005 executor 메트릭 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-MON-005` |
| 인터페이스명 | executor 메트릭 조회 |
| 메서드 | `GET` |
| URI | `/executors/{idx}/metrics` |
| 방식 | 동기 |
| 설명 | 지정한 executor 의 자원 사용량과 동시 처리 현황을 coordinator 를 거쳐 가져온다 |

**executor 에 직접 붙지 않고 coordinator 에게 대신 물어보는 통로다.** 감시 도구가 coordinator 한
곳만 알면 되게 하려는 것이고, 동시에 아무 주소나 대신 부르게 하지 않으려는 안전장치이기도 하다.
그래서 executor 를 주소가 아니라 **설정 목록에서의 순번**으로만 지정한다. 그 순번은 `IF-MON-003`
이나 `IF-MON-004` 응답의 `index` 에서 얻는다. 범위를 벗어난 번호를 주면 `404` 다.

같은 방식으로 그 executor 의 기본 정보(`/executors/{idx}/info`), 보유한 조각 목록
(`/executors/{idx}/tasks`), 조각 하나의 상세(`/executors/{idx}/tasks/{task_id}/detail`)도 가져올
수 있다. 응답에는 모두 어느 executor 에게 물었는지가 `proxied_to` 로 함께 담긴다.

**요청 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | executor 번호 | `idx` | integer | 0 이상 | 주소에 넣는다. 설정 목록에서의 순번이다 |

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | CPU 사용률 | `cpu_percent` | number | 0~100 | 그 executor 서버의 CPU 사용률 |
| 2 | 메모리 정보 | `memory` | object | - | `IF-MON-002` 와 같은 구성이다 |
| 3 | 디스크 정보 | `disk` | object | - | `IF-MON-002` 와 같은 구성이다 |
| 4 | 태스크 현황 | `tasks` | object | - | 조각 처리 현황 |
| 5 | 진행 중 태스크 수 | `tasks.active` | integer | - | 지금 처리 중인 조각 수 |
| 6 | 대기 중 태스크 수 | `tasks.queued` | integer | - | 자리가 나기를 기다리는 조각 수 |
| 7 | 동시 처리 한도 | `tasks.max` | integer | - | 이 executor 가 한 번에 처리할 수 있는 조각 수 |
| 8 | 세그먼트 호스트명 | `gp_hostname` | string | 가변 | [선택] 같은 서버에 있는 Greenplum 세그먼트의 이름 |
| 9 | 경유 executor 주소 | `proxied_to` | string | 가변 | 실제로 물어본 executor 의 주소 |

## IF-MON-006 런타임 정보 조회

| 항목 | 내용 |
|---|---|
| 인터페이스 ID | `IF-MON-006` |
| 인터페이스명 | 런타임 정보 조회 |
| 메서드 | `GET` |
| URI | `/info` |
| 방식 | 동기 |
| 설명 | 지금 떠 있는 coordinator 의 버전과 주요 설정, 그리고 누적 현황을 돌려준다 |

**"지금 이 서버가 어떤 모습으로 떠 있는가"를 확인하는 인터페이스다.** 배포한 버전이 맞는지,
설정을 바꾼 것이 반영됐는지, 언제부터 떠 있었는지를 한 번에 본다. 보낼 항목은 없다.

이 인터페이스는 대시보드가 켜져 있을 때만 열린다. 설정에서 대시보드를 끄면 `404` 가 된다.

**응답 항목**

| NO | 필드명(한글) | 필드명(영문) | 데이터 타입 | 길이 | 설명 |
|---|---|---|---|---|---|
| 1 | 버전 | `version` | string | 가변 | 지금 떠 있는 소프트웨어 버전 |
| 2 | coordinator 식별자 | `coordinator_id` | string | 가변 | 이 coordinator 를 가리키는 이름. 여러 대일 때 구분에 쓴다 |
| 3 | 실행 방식 | `executor_mode` | string | 가변 | `http`(executor 를 따로 두는 방식) 또는 `local`(혼자 다 하는 방식) |
| 4 | 작업 저장 방식 | `store_backend` | string | 가변 | 작업을 메모리에 두는지 데이터베이스에 두는지 |
| 5 | 자가 보고 사용 여부 | `executor_self_report` | boolean | - | executor 가 자기 상태를 직접 기록하는 방식인지 |
| 6 | executor 선택 정책 | `executor_select` | string | 가변 | 조각을 어느 executor 에게 맡길지 정하는 방식 |
| 7 | 헬스 정보 출처 | `executor_health_source` | string | 가변 | executor 상태를 어디서 읽는지 |
| 8 | 예약 사용 여부 | `executor_reservation` | boolean | - | 배정할 때 자리를 미리 잡아 두는지 |
| 9 | 기동 시각 | `started_at` | string | 가변 | 이 프로세스가 뜬 시각 |
| 10 | 가동 시간 | `uptime_seconds` | number | - | 뜬 뒤로 흐른 시간(초) |
| 11 | 전체 작업 수 | `jobs_total` | integer | - | 지금 들고 있는 작업 수 |
| 12 | 설정된 executor 수 | `executors_configured` | integer | - | 설정에 등록된 executor 수 |
| 13 | 동시 실행 한도 | `max_concurrent_jobs` | integer | - | 한 번에 실행할 수 있는 작업 수 |
| 14 | 대기 큐 한도 | `max_pending_jobs` | integer | - | 줄 서서 기다릴 수 있는 작업 수 |
| 15 | 디스패치 동시 한도 | `max_dispatch_concurrency` | integer | - | 조각을 한 번에 몇 개까지 내보낼지 |
| 16 | 상태별 작업 수 | `jobs_by_status` | object | - | 상태 이름마다 몇 건인지 |
