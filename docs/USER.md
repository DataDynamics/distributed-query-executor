# 사용자 가이드 (Coordinator API 사용법)

이 문서는 분산 쿼리 실행기에 작업을 맡기는 사람을 위한 것이다. Impala 같은 소스에서 데이터를 읽어
Greenplum 으로 옮기는 일을 HTTP 로 요청하고, 진행 상황을 확인하고, 실패했을 때 무엇을 고쳐야
하는지를 다룬다. 서버를 설치하고 돌보는 쪽 이야기는 [운영자 가이드](OPERATOR.md)에 있다.

알아 둘 것은 coordinator 한 곳만 상대하면 된다는 점이다. 뒤에 executor 가 몇 대 있든 데이터가
어느 경로로 흐르든, 요청과 조회는 모두 coordinator 의 `8088` 포트로 보낸다.

처음 읽는다면 아래 순서가 편하다. 먼저 [첫 작업 제출](#첫-작업-제출)로 한 건을 넣어 보고,
[exec_mode 고르기](#exec_mode-고르기)에서 자기 상황에 맞는 실행 방식을 고른 뒤,
[진행 상황 확인](#진행-상황-확인)으로 끝날 때까지 기다리는 방법을 익히면 기본은 끝난다. SQL 대신
템플릿을 쓰려면 [템플릿으로 요청하기](#템플릿으로-요청하기)를, 요청이 거절당했다면
[오류 대처](#오류-대처)를 본다. 모드별 전체 예제는 [실행 모드 사용 가이드](GUIDE.md)에, C# 클라이언트
코드는 [연동 가이드](INTEGRATION.md)에 따로 있다.

---

## 어떤 일을 대신 해 주는가

`SELECT` 한 건을 주면 그것을 파티션 컬럼의 `IN` 목록 기준으로 여러 조각으로 나눠 동시에 읽고, 각
조각을 나눠 맡은 executor 가 곧바로 Greenplum 에 적재한다. 조각을 나누는 것도 병렬로 읽는 것도
적재하는 것도 서버가 하므로, 요청하는 쪽은 무엇을 어디로 옮길지만 알려 주면 된다.

데이터 자체는 coordinator 를 거치지 않는다. executor 가 소스에서 읽어 대상으로 바로 흘려보내고
coordinator 에는 진행 상태와 적재 행 수만 올라온다. 그래서 작업이 아무리 커도 coordinator 의 응답은
가볍지만, 반대로 작업 결과를 HTTP 응답으로 돌려받을 수는 없다. 옮긴 데이터는 대상 테이블에 있다.

작업은 비동기다. `POST /jobs` 는 접수만 하고 `job_id` 를 즉시 돌려주며 실제 실행은 그 뒤에
백그라운드로 진행되므로, 제출과 완료 확인은 언제나 두 단계다.

---

## 첫 작업 제출

가장 단순한 형태는 SQL 과 분할 기준 컬럼, 대상 테이블 세 가지다.

```bash
curl -s localhost:8088/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-01-01','2026-01-02') AND region='KR'",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "parallelism": 2
}'
# → {"job_id": "..."}  (202 Accepted)
```

여기서 `partition_column` 은 `IN` 목록으로 나눌 기준 컬럼이다. 위 요청은 `dt IN ('2026-01-01',
'2026-01-02')` 를 두 조각으로 갈라 각각 하루씩 읽는다. 그러므로 이 컬럼에 대한 `IN` 절이 SQL 안에
반드시 있어야 하고, 값이 `parallelism` 보다 적으면 그만큼만 나뉜다.

자주 쓰는 나머지 필드를 훑어보면 이렇다. `parallelism` 은 몇 조각으로 나눌지를 1에서 128 사이로
정하며 기본값은 4다. `split_strategy` 는 값을 잇달아 묶을지(`contiguous`, 기본) 번갈아 나눌지
(`round_robin`)를 고르고, `write_mode` 를 `overwrite_partitions` 로 두면 적재하기 전에 그 조각이 맡은
파티션을 먼저 지운다. `failure_policy` 는 한 조각이 실패했을 때 전체를 실패로 볼지(`fail_fast`, 기본)
나머지는 계속할지(`best_effort`)를 정한다. 이력에 요청자를 남기려면 `username` 을 채운다.

처음 만드는 요청이라면 `dry_run` 을 켜 보는 편이 좋다. 실제로는 아무것도 옮기지 않으면서 분할이
의도대로 됐는지, 각 조각의 SQL 이 어떻게 생겼는지를 먼저 확인할 수 있다.

### 같은 요청을 두 번 보내지 않으려면

네트워크가 끊겨 응답을 못 받았을 때 그냥 다시 보내면 같은 작업이 두 번 돌 수 있다. 이를 막으려면
`Idempotency-Key` 헤더에 요청마다 고유한 값을 실어 보낸다.

```bash
curl -s localhost:8088/jobs \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: sales-mirror-2026-01-02' \
  -d '{...}'
```

같은 키로 같은 본문이 다시 오면 서버는 새 작업을 만들지 않고 원래 작업을 그대로 돌려준다(`200` 과
함께 `Idempotency-Replayed: true` 헤더가 붙는다). 같은 키인데 본문이 다르면 `409` 로 거절하는데,
키를 재사용하다 엉뚱한 작업을 덮어쓰는 사고를 막기 위해서다.

---

## exec_mode 고르기

같은 "읽어서 넣는다"라도 데이터가 흐르는 경로는 여러 가지다. 대개는 기본값으로 충분하고, 양이
커지거나 소스와 대상이 서로 다른 엔진일 때 다른 모드를 고른다.

기본값인 `copy` 는 소스에서 읽어 Greenplum 에 곧바로 COPY 한다. 가장 단순하고 대부분의 경우에
맞는다. 적재하면서 변환이나 가공이 필요하면 `stage_insert` 를 쓰는데, staging 테이블에 COPY 한 뒤
그것을 읽어 target 으로 INSERT 하므로 그 사이에 원하는 변형을 넣을 수 있다. 소스와 대상이 같은
엔진이라 옮길 필요조차 없다면 `statement` 로 INSERT 문을 그대로 실행시킨다.

아주 큰 이관에는 남은 두 모드가 있다. `local_stage` 는 executor 가 CSV 를 자기 호스트에 떨어뜨리고
Greenplum 세그먼트가 그 파일을 직접 읽으며, `s3_stage` 는 CSV 를 S3 에 올린 뒤 PXF 로 읽는다. 이 둘이
빠른 이유는 데이터가 executor 프로세스를 한 줄씩 통과하지 않고 모든 세그먼트가 파일을 나눠 동시에
읽기 때문이다. 대신 준비할 것이 많다. `local_stage` 는 executor 가 GP 세그먼트 호스트에 함께 있어야
하고, `s3_stage` 는 그 제약이 없는 대신 버킷과 PXF 설정이 필요하다. 어느 쪽이 가능한 환경인지는
운영자에게 확인한다.

모드마다 요청에 더 넣어야 하는 필드가 다르다. 필요한 필드가 빠지면 `STAGE_INSERT_REQUIRES_FIELDS`
처럼 무엇이 부족한지 알려 주는 오류가 돌아온다. 모드별 전체 예제와 필수 필드 목록은
[실행 모드 사용 가이드](GUIDE.md)에 있다.

작업을 다시 돌렸을 때 데이터가 두 벌로 쌓이지 않게 하려면 `write_mode` 를 `overwrite_partitions` 로
둔다. 적재하기 전에 그 작업이 맡은 파티션 값을 먼저 지우므로 몇 번을 돌려도 결과가 같다. 기본값인
`append` 는 그런 정리를 하지 않으므로 재실행하면 그만큼 더 쌓인다.

---

## 템플릿으로 요청하기

SQL 전문을 매번 만들어 보내는 대신, 서버에 등록된 템플릿에 값만 넘길 수 있다. 쿼리가 서버에서
관리되므로 요청하는 쪽이 테이블 구조나 적재 규칙을 알 필요가 없고, 쿼리가 바뀌어도 호출하는 코드는
그대로다.

```bash
curl -s localhost:8088/jobs -H 'content-type: application/json' -d '{
  "template_id": "sales_migration",
  "params": {"start_date": "2026-01-01", "end_date": "2026-01-07", "region": "KR"},
  "parallelism": 4
}'
```

쓸 수 있는 템플릿은 `GET /templates` 로 확인한다. `template_id` 를 주면 `sql`·`staging_ddl`·
`insert_sql` 같은 SQL 필드는 렌더 결과가 채우므로 생략해도 되고, `partition_column` 이나
`target_table` 처럼 템플릿이 기본값을 갖고 있는 값도 넣지 않으면 그 기본값을 쓴다. 요청에 명시하면
요청이 이긴다.

`params` 는 이름과 값을 짝지은 dict 로 주면 되지만, 값에 부호를 함께 실어야 하는 템플릿에서는 배열
형태를 쓴다.

```json
"params": [
  {"name": "from_date_no", "value": 7, "sign": "-"},
  {"name": "to_date_no",   "value": 0, "sign": "+"}
]
```

여기서 `sign` 은 값의 부호가 아니라 SQL 에 들어갈 연산자의 방향이다. `- interval 7 day` 처럼 방향이
SQL 문에 박히고 값은 절대값이어야 하는 경우가 있어 둘을 나눠 받는다.

### 하루를 한 조각으로 나누기

기간을 다루는 이관에서는 `IN` 목록 대신 날짜별로 나누는 편이 자연스럽다. `task_params` 에 구간의 두
끝을 담은 파라미터 이름 두 개를 지목하면 하루를 조각 하나로 펼쳐 실행한다.

```json
{
  "template_id": "daily_sales_interval",
  "params": [
    {"name": "from_date_no", "value": 7, "sign": "-"},
    {"name": "to_date_no",   "value": 0, "sign": "+"}
  ],
  "task_params": ["from_date_no", "to_date_no"]
}
```

각 조각이 받는 구간의 모양은 `task_bound` 로 고른다. 기본값 `point` 는 `(d, d)` 로 양끝을 포함하는
비교(`BETWEEN` 이나 `=`)에 쓰고, `pair` 는 `(d, d+1)` 로 반열림 비교(`>=` 와 `<`)에 쓴다. 템플릿이
어느 쪽을 전제로 쓰였는지에 맞춰야 한다.

이 방식은 `stage_insert` 전용이며 `append` 로 쌓는다. 다시 돌려도 안전해야 한다면 대상을 미리
비우거나 날짜별로 테이블을 나눠 쓴다.

---

## 진행 상황 확인

제출과 완료는 별개이므로 `job_id` 로 상태를 물어본다. 조회는 두 가지인데, 반복해서 물어볼 때는 조각
목록이 빠진 가벼운 쪽을 쓴다.

```bash
curl -s localhost:8088/jobs/$JOB_ID/status   # 가볍다 — 폴링에는 이쪽
curl -s localhost:8088/jobs/$JOB_ID          # 조각(task) 목록까지 함께
```

`/status` 응답에는 `status` 와 `progress_percent`, `completed`/`total`, `total_rows_written`, `error`
가 들어 있다.

상태는 `PENDING` 으로 시작한다. 접수는 됐지만 실행 슬롯을 기다리는 중이라는 뜻이다. 슬롯을 잡으면
`SPLITTING` 으로 넘어가 쿼리를 검증하고 조각으로 나누며, 그다음 `RUNNING` 에서 executor 들이 조각을
실행한다. 종료 상태는 넷이다. `DONE` 은 모든 조각이 성공한 것이고, `PARTIAL` 은 일부만 성공한
것으로 `best_effort` 정책에서 나온다. `FAILED` 는 실패, `CANCELLED` 는 취소다. 폴링은 상태가 이 넷
중 하나가 될 때까지 이어 가면 된다.

```bash
while :; do
  S=$(curl -s localhost:8088/jobs/$JOB_ID/status | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  case "$S" in DONE|PARTIAL|FAILED|CANCELLED) echo "종료: $S"; break;; esac
  sleep 3
done
```

폴링 간격은 몇 초면 충분하다. 큰 이관은 수십 분씩 걸리므로 1초 미만으로 조이면 서버만 두드리게
된다.

작업이 끝난 뒤 조각별로 몇 행이 들어갔는지는 `GET /jobs/{job_id}/result` 로 본다. 어떤 조각이 어떤
SQL 을 실행했는지까지 보려면 `GET /jobs/{job_id}/tasks/{task_id}` 를 쓴다.

### 취소와 재실행

```bash
curl -s -X POST localhost:8088/jobs/$JOB_ID/cancel   # 진행 중인 작업 중단
curl -s -X POST localhost:8088/jobs/$JOB_ID/retry    # 실패한 조각만 다시
```

취소는 각 executor 로 전파되어 실행 중인 조각을 멈춘다. 이미 종료된 작업을 취소하려 하면 `409` 다.

재실행은 실패하거나 취소된 조각만 모아 새 작업으로 돌린다. 이미 성공한 조각은 건너뛰므로 중복 적재
걱정 없이 눌러도 된다. 새 `job_id` 가 `202` 와 함께 돌아오며, 다시 돌릴 조각이 없거나 작업이 아직
끝나지 않았으면 `409` 다.

---

## 결과를 바로 받아 보기

이관이 아니라 지금 이 쿼리가 무엇을 돌려주는지 보고 싶을 때는 `POST /query-execute` 를 쓴다.
템플릿의 SELECT 조각만 렌더해 실행하고 상위 몇 행을 동기로 돌려준다.

```bash
curl -s localhost:8088/query-execute -H 'content-type: application/json' -d '{
  "template_id": "sales_migration",
  "params": [{"name": "region", "value": "KR"}],
  "limit": 100
}'
```

`limit` 은 1에서 10000 사이이며 기본값은 100이다. 미리보기라 결과가 잘리므로 이관에 쓰면 잘린
데이터가 그대로 적재된다. 옮기는 일은 반드시 `POST /jobs` 로 한다.

---

## 오류 대처

검증에 걸린 요청은 `422` 와 함께 `error_code` 를 돌려준다. 코드를 보면 무엇을 고쳐야 하는지 바로 알
수 있다.

```json
{"error_code": "NO_PARTITION_IN_CLAUSE", "message": "..."}
```

가장 흔한 것은 쿼리를 나눌 수 없는 경우다. `NO_PARTITION_IN_CLAUSE` 는 `partition_column` 으로 지정한
컬럼의 `IN` 절이 SQL 에 없다는 뜻이고, `MISSING_PARTITION_COLUMN` 은 그 필드 자체를 주지 않은
것이다. `IN` 절이 있어도 값이 비었으면 `EMPTY_IN_LIST` 가 나오고, `NOT IN` 처럼 부정형이거나
(`NEGATED_IN`) `IN (SELECT ...)` 처럼 서브쿼리면(`SUBQUERY_IN_CLAUSE`) 나눌 기준이 없어 역시
거절된다. 값 목록으로 바꿔 주면 된다.

쿼리 자체가 받아들여지지 않는 경우도 있다. `NOT_A_SELECT` 는 SELECT 가 아니라는 뜻이고,
`MULTIPLE_STATEMENTS` 는 여러 문장을 한 번에 보냈다는 뜻이다. `PARSE_ERROR` 는 SQL 을 해석하지
못한 것인데, 문법이 맞는데도 나온다면 방언이 달라서일 수 있으므로 `sql_dialect` 를 지정해 본다.

`UNSUPPORTED_JOIN` 과 `UNSUPPORTED_GROUP_BY`, `UNSUPPORTED_HAVING`, `UNSUPPORTED_DISTINCT`,
`UNSUPPORTED_AGGREGATE` 는 모두 원인이 하나다. 기본값인 `strict_validation: true` 가 단순한 SELECT
만 받는데 JOIN 이나 GROUP BY 가 섞인 쿼리를 보냈다는 뜻이다. `false` 로 두면 복합 쿼리도 받으면서
파티션 `IN` 절을 쿼리 어디에 있든 찾아 나눈다.

필드가 모자란 경우는 메시지가 무엇이 빠졌는지 짚어 준다. `MISSING_REQUIRED_FIELDS` 는 공통 필수
필드가, `STAGE_INSERT_REQUIRES_FIELDS`·`LOCAL_STAGE_REQUIRES_FIELDS`·`S3_STAGE_REQUIRES_FIELDS` 는
그 모드에만 필요한 필드가 빠진 것이다. 무엇을 채워야 하는지는 [실행 모드 사용 가이드](GUIDE.md)에
모드별로 정리돼 있다.

템플릿 쪽 오류도 비슷하게 읽으면 된다. `TEMPLATE_NOT_FOUND` 는 그런 템플릿이 없다는 뜻이라
`GET /templates` 로 이름을 확인하고, `TEMPLATE_PARAM_ERROR` 는 템플릿이 요구하는 파라미터가 빠진
것이다. 날짜 fan-out 에서 구간이 너무 넓으면 `TASK_RANGE_TOO_LARGE` 가 나오므로 기간을 좁힌다.

### 422 말고 다른 응답

`429` 는 잘못이 아니라 줄을 서 달라는 뜻이다. 실행 슬롯과 대기 큐가 모두 찼을 때 나오며,
`Retry-After` 헤더가 알려 주는 만큼(기본 5초) 기다렸다 다시 보내면 대개 통과한다. 자동 재시도를
넣을 때는 [멱등 키](#같은-요청을-두-번-보내지-않으려면)를 함께 쓰는 편이 안전하다.

`409` 는 이미 끝난 작업을 취소하거나 재실행하려 했을 때, 또는 같은 멱등 키로 다른 본문을 보냈을 때
나온다. 상태를 먼저 확인하면 어느 쪽인지 가려진다. `404` 는 그런 `job_id` 가 없다는 뜻이다.

### 실행 중에 실패했을 때

검증을 통과한 뒤 실패하면 `422` 가 아니라 작업 상태로 나타난다. `GET /jobs/{job_id}` 의 `error`
필드에 이유가 있고, 조각별로 어디서 어떻게 실패했는지는 같은 응답의 task 목록에서 본다. `PARTIAL`
이면 일부만 들어간 것이므로 `retry` 로 나머지를 채운다.

원인이 서버 쪽으로 보이면 — 소스 접속 실패나 대상 테이블 없음, 권한 부족 같은 것들이다 — 운영자에게
`job_id` 를 알려 주는 편이 빠르다. 서버 로그에는 작업과 조각 식별자가 모든 줄에 붙어 있고 실제로
실행한 SQL 도 함께 남아 있어서, 그 하나만으로 관련된 기록을 전부 모을 수 있다.

---

## 실수하기 쉬운 것들

`IN` 목록의 값 개수가 곧 병렬도의 한계다. `parallelism` 을 32로 줘도 `IN` 값이 셋이면 세 조각으로만
나뉜다. 더 잘게 나누고 싶으면 나눌 값을 늘리거나 날짜 fan-out 을 쓴다.

`append` 는 다시 돌리면 그만큼 쌓인다. 재실행이 예상되는 작업이라면 처음부터
`write_mode: overwrite_partitions` 로 만들어 두는 편이 낫다.

타임아웃이 났다고 그냥 다시 보내면 작업이 두 번 만들어질 수 있다. 멱등 키를 쓰거나 이미
만들어졌는지 먼저 확인한다.

`/query-execute` 는 미리보기라 `limit` 으로 잘린다. 이관에 쓰지 않는다.

결과는 HTTP 응답에 담기지 않는다. 데이터는 executor 가 대상으로 직접 보내므로 확인은 대상
테이블에서 하고, API 가 돌려주는 것은 상태와 행 수뿐이다.

---

## 더 볼 것

모드별 전체 요청 예제와 필수 필드는 [실행 모드 사용 가이드](GUIDE.md)에 있고, 폴링과 취소·재시도를
포함한 C# 클라이언트 코드는 [연동 가이드](INTEGRATION.md)에 있다. 서버가 어떻게 돌아가는지, 느릴 때
운영자가 무엇을 보는지가 궁금하면 [운영자 가이드](OPERATOR.md)를 읽는다. 모든 API 를 직접 호출해
보려면 `http://<coordinator>:8088/docs` 의 Swagger UI 를 쓴다.
