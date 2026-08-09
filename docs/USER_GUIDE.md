# 사용자 가이드 (Coordinator API 사용법)

이 문서는 **분산 쿼리 실행기에 작업을 맡기는 사람**을 위한 것이다. Impala 같은 소스에서
데이터를 읽어 Greenplum 으로 옮기는 일을 HTTP 로 요청하고, 진행 상황을 확인하고, 실패했을 때
무엇을 고쳐야 하는지를 다룬다. 서버를 설치하고 돌보는 쪽 이야기는
[운영자 가이드](OPERATIONS.md)에 있다.

알아 둘 것은 coordinator 한 곳만 상대하면 된다는 점이다. 뒤에 executor 가 몇 대 있든, 데이터가
어느 경로로 흐르든 요청과 조회는 모두 coordinator 의 `8088` 포트로 보낸다.

문서를 처음 읽는다면 아래 순서가 편하다.

| 무엇을 하려는가 | 어디를 보는가 |
| --- | --- |
| 처음 한 건 넣어 보기 | [첫 작업 제출](#첫-작업-제출) |
| 실행 방식 고르기 | [exec_mode 고르기](#exec_mode-고르기) |
| SQL 대신 템플릿 쓰기 | [템플릿으로 요청하기](#템플릿으로-요청하기) |
| 끝날 때까지 기다리기 | [진행 상황 확인](#진행-상황-확인) |
| 실패 원인 찾기 | [오류 대처](#오류-대처) |
| 모드별 상세 예제 | [실행 모드 사용 가이드](GUIDE.md) |
| C# 에서 호출하기 | [C# 연동 가이드](INTEGRATION.md) |

---

## 어떤 일을 대신 해 주는가

`SELECT` 한 건을 주면 그것을 파티션 컬럼의 `IN` 목록 기준으로 여러 조각으로 나눠 동시에 읽고,
각 조각을 나눠 맡은 executor 가 곧바로 Greenplum 에 적재한다. 조각을 나누는 것도, 병렬로 읽는
것도, 적재하는 것도 서버가 한다. 요청하는 쪽은 **무엇을 어디로 옮길지**만 알려 주면 된다.

데이터 자체는 coordinator 를 거치지 않는다. executor 가 소스에서 읽어 대상으로 바로 흘려보내고
coordinator 에는 진행 상태와 적재 행 수만 올라온다. 그래서 작업이 아무리 커도 coordinator 응답은
가볍고, 작업 결과를 HTTP 응답으로 돌려받지는 않는다(결과는 대상 테이블에 있다).

작업은 **비동기**다. `POST /jobs` 는 접수만 하고 `job_id` 를 즉시 돌려주며, 실제 실행은 그 뒤에
백그라운드로 진행된다. 그러므로 제출과 완료 확인은 언제나 두 단계다.

---

## 첫 작업 제출

가장 단순한 형태는 SQL·분할 기준 컬럼·대상 테이블 세 가지다.

```bash
curl -s localhost:8088/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-01-01','2026-01-02') AND region='KR'",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "parallelism": 2
}'
# → {"job_id": "..."}  (202 Accepted)
```

`partition_column` 은 **`IN` 목록으로 나눌 기준 컬럼**이다. 위 요청은 `dt IN ('2026-01-01',
'2026-01-02')` 를 두 조각으로 갈라 각각 하루씩 읽는다. 그래서 이 컬럼에 대한 `IN` 절이 SQL 안에
반드시 있어야 하고, 값이 `parallelism` 보다 적으면 그만큼만 나뉜다.

자주 쓰는 나머지 필드는 다음과 같다.

| 필드 | 기본값 | 무엇을 정하는가 |
| --- | --- | --- |
| `parallelism` | `4` | 몇 조각으로 나눌지(1~128). `IN` 값 개수를 넘지 못한다 |
| `split_strategy` | `contiguous` | 값을 잇달아 묶을지(`contiguous`) 번갈아 나눌지(`round_robin`) |
| `write_mode` | `append` | `overwrite_partitions` 면 적재 전에 담당 파티션을 지운다 |
| `failure_policy` | `fail_fast` | 한 조각이 실패하면 전체를 실패로 볼지(`fail_fast`), 나머지는 계속할지(`best_effort`) |
| `exec_mode` | `copy` | 데이터를 어떤 경로로 옮길지([아래](#exec_mode-고르기)) |
| `username` | 없음 | 이력에 남길 요청자 이름 |
| `dry_run` | `false` | `true` 면 실행하지 않고 만들어질 쿼리만 돌려준다 |

`dry_run` 은 특히 처음 만드는 요청에서 값이 있다. 실제로 아무것도 옮기지 않으면서 분할이
의도대로 됐는지, 각 조각의 SQL 이 어떻게 생겼는지를 먼저 볼 수 있다.

### 같은 요청을 두 번 보내지 않으려면

네트워크가 끊겨 응답을 못 받았을 때 그냥 다시 보내면 같은 작업이 두 번 돌 수 있다. 이를 막으려면
`Idempotency-Key` 헤더에 요청마다 고유한 값을 실어 보낸다.

```bash
curl -s localhost:8088/jobs \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: sales-mirror-2026-01-02' \
  -d '{...}'
```

같은 키로 **같은 본문**이 다시 오면 서버는 새 작업을 만들지 않고 원래 작업을 그대로 돌려준다
(`200` + `Idempotency-Replayed: true` 헤더). 같은 키인데 **본문이 다르면** `409` 로 거절하는데,
키를 재사용하다 엉뚱한 작업을 덮어쓰는 사고를 막기 위해서다.

---

## exec_mode 고르기

같은 "읽어서 넣는다"라도 데이터가 흐르는 경로는 여러 가지다. 대개는 기본값으로 충분하고, 양이
커지거나 소스와 대상이 서로 다른 엔진일 때 다른 모드를 고른다.

| exec_mode | 어떻게 옮기는가 | 언제 쓰는가 |
| --- | --- | --- |
| `copy` (기본) | 소스에서 읽어 Greenplum 에 곧바로 COPY | 대부분의 경우. 가장 단순하다 |
| `stage_insert` | staging 테이블에 COPY 한 뒤 target 으로 INSERT | 적재하며 변환·가공이 필요할 때 |
| `statement` | 받은 INSERT 문을 대상에서 그대로 실행 | 소스와 대상이 같은 엔진이라 옮길 필요가 없을 때 |
| `local_stage` | CSV 로 떨어뜨린 뒤 GP 세그먼트가 직접 읽음 | 아주 큰 이관. executor 가 세그먼트 호스트에 함께 있어야 한다 |
| `s3_stage` | CSV 를 S3 에 올린 뒤 GP 가 PXF 로 읽음 | 아주 큰 이관인데 executor 와 세그먼트를 같이 둘 수 없을 때 |

뒤의 두 모드가 빠른 이유는 데이터가 executor 프로세스를 한 줄씩 통과하지 않고, Greenplum 의 모든
세그먼트가 파일을 나눠 동시에 읽기 때문이다. 대신 준비할 것이 많다 — `local_stage` 는 executor 와
GP 세그먼트가 같은 호스트에 있어야 하고, `s3_stage` 는 버킷과 PXF 설정이 필요하다. 어느 쪽이
가능한지는 운영자에게 확인한다.

모드마다 요청에 더 넣어야 하는 필드가 다르다. 필요한 필드가 빠지면
`STAGE_INSERT_REQUIRES_FIELDS` 처럼 무엇이 빠졌는지 알려 주는 오류가 돌아온다. 모드별 전체 예제와
필드 목록은 [실행 모드 사용 가이드](GUIDE.md)에 있다.

### 다시 실행해도 안전하게

작업을 다시 돌렸을 때 데이터가 두 벌로 쌓이지 않게 하려면 `write_mode` 를 `overwrite_partitions`
로 둔다. 적재하기 전에 이 작업이 맡은 파티션 값을 먼저 지우므로 몇 번을 돌려도 결과가 같다.
기본값인 `append` 는 그런 정리를 하지 않으므로 재실행하면 그만큼 더 쌓인다.

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

`params` 는 이름-값 dict 로 주면 되지만, 값에 **부호(연산자 방향)** 를 함께 실어야 하는 템플릿에서는
배열 형태를 쓴다.

```json
"params": [
  {"name": "from_date_no", "value": 7, "sign": "-"},
  {"name": "to_date_no",   "value": 0, "sign": "+"}
]
```

여기서 `sign` 은 값의 부호가 아니라 **SQL 에 들어갈 연산자의 방향**이다. `- interval 7 day` 처럼
방향이 SQL 문에 박히고 값은 절대값이어야 하는 경우가 있어 둘을 나눠 받는다.

### 하루를 한 조각으로 나누기 (날짜 fan-out)

기간을 다루는 이관에서는 `IN` 목록 대신 **날짜별로** 나누는 편이 자연스럽다. `task_params` 에
구간의 두 끝을 담은 파라미터 이름 두 개를 지목하면, 하루를 조각 하나로 펼쳐 실행한다.

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

`task_bound` 로 각 조각이 받는 구간의 모양을 고른다. 기본값 `point` 는 `(d, d)` 로 양끝을 포함하는
비교(`BETWEEN`, `=`)용이고, `pair` 는 `(d, d+1)` 로 반열림 비교(`>=` 와 `<`)용이다. 템플릿이 어느
쪽을 전제로 쓰였는지에 맞춰야 한다.

이 방식은 `stage_insert` 전용이며 `append` 로 쌓는다. 다시 돌려도 안전해야 한다면 대상을 미리
비우거나 날짜별로 테이블을 나눠 쓴다.

---

## 진행 상황 확인

제출과 완료는 별개이므로 `job_id` 로 상태를 물어본다. 조회는 두 가지가 있다.

```bash
curl -s localhost:8088/jobs/$JOB_ID/status   # 가볍다 — 폴링에는 이쪽
curl -s localhost:8088/jobs/$JOB_ID          # 조각(task) 목록까지 함께
```

`/status` 응답에는 `status`·`progress_percent`·`completed`/`total`·`total_rows_written`·`error` 가
들어 있다. 반복해서 물어볼 때는 조각 목록이 없는 이쪽을 쓴다.

상태는 다음과 같이 흐른다.

| 상태 | 뜻 |
| --- | --- |
| `PENDING` | 접수됐고 실행 슬롯을 기다리는 중 |
| `SPLITTING` | 쿼리를 검증하고 조각으로 나누는 중 |
| `RUNNING` | 조각들이 executor 에서 실행 중 |
| `DONE` | 모든 조각 성공 (종료) |
| `PARTIAL` | 일부만 성공 (종료, `best_effort` 에서 나온다) |
| `FAILED` | 실패 (종료) |
| `CANCELLED` | 취소됨 (종료) |

뒤의 네 가지가 종료 상태이므로, 폴링은 상태가 그중 하나가 될 때까지 이어 가면 된다.

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

취소는 각 executor 로 전파되어 실행 중인 조각을 멈춘다. 이미 종료된 작업을 취소하면 `409` 다.

재실행은 **실패하거나 취소된 조각만** 모아 새 작업으로 돌린다. 이미 성공한 조각은 건너뛰므로
중복 적재 걱정 없이 눌러도 된다. 새 `job_id` 가 돌아오며(`202`), 다시 돌릴 조각이 없거나 작업이
아직 끝나지 않았으면 `409` 다.

---

## 결과를 바로 받아 보기

이관이 아니라 **지금 이 쿼리가 무엇을 돌려주는지** 보고 싶을 때는 `POST /query-execute` 를 쓴다.
템플릿의 SELECT 조각만 렌더해 실행하고 상위 몇 행을 동기로 돌려준다.

```bash
curl -s localhost:8088/query-execute -H 'content-type: application/json' -d '{
  "template_id": "sales_migration",
  "params": [{"name": "region", "value": "KR"}],
  "limit": 100
}'
```

`limit` 은 1~10000 이며 기본 100 이다. 미리보기라 **결과가 잘린다** — 이관에 쓰면 잘린 데이터가
그대로 적재되므로, 옮기는 일은 반드시 `POST /jobs` 로 한다.

---

## 오류 대처

검증에 걸린 요청은 `422` 와 함께 `error_code` 를 돌려준다. 코드를 보면 무엇을 고쳐야 하는지
바로 알 수 있다.

```json
{"error_code": "NO_PARTITION_IN_CLAUSE", "message": "..."}
```

자주 만나는 코드는 다음과 같다.

| error_code | 무엇이 잘못됐나 | 어떻게 고치나 |
| --- | --- | --- |
| `NOT_A_SELECT` | SELECT 가 아니다 | 이관 소스는 SELECT 여야 한다 |
| `MULTIPLE_STATEMENTS` | 여러 문장을 한 번에 보냈다 | 한 문장만 보낸다 |
| `PARSE_ERROR` | SQL 을 해석하지 못했다 | 문법을 확인하고, 방언이 다르면 `sql_dialect` 를 지정한다 |
| `NO_PARTITION_IN_CLAUSE` | 분할 기준 컬럼의 `IN` 절이 없다 | `partition_column` 에 대한 `IN (...)` 을 넣는다 |
| `MISSING_PARTITION_COLUMN` | `partition_column` 을 안 줬다 | 필드를 채운다 |
| `EMPTY_IN_LIST` | `IN` 목록이 비어 있다 | 나눌 값을 넣는다 |
| `NEGATED_IN` | `NOT IN` 은 나눌 수 없다 | 긍정형 `IN` 으로 바꾼다 |
| `SUBQUERY_IN_CLAUSE` | `IN (SELECT ...)` 은 나눌 수 없다 | 값 목록으로 바꾼다 |
| `UNSUPPORTED_JOIN` 외 | 복합 쿼리를 엄격 모드로 보냈다 | `strict_validation: false` 로 보낸다 |
| `MISSING_REQUIRED_FIELDS` | 필수 필드가 없다 | 메시지가 가리키는 필드를 채운다 |
| `STAGE_INSERT_REQUIRES_FIELDS` | 그 모드에 필요한 필드가 없다 | 모드별 필수 필드를 채운다([GUIDE](GUIDE.md)) |
| `TEMPLATE_NOT_FOUND` | 그런 템플릿이 없다 | `GET /templates` 로 확인한다 |
| `TEMPLATE_PARAM_ERROR` | 템플릿이 요구하는 파라미터가 빠졌다 | 메시지가 가리키는 `params` 를 채운다 |
| `TASK_RANGE_TOO_LARGE` | fan-out 구간이 너무 넓다 | 기간을 좁힌다 |

`UNSUPPORTED_JOIN`·`UNSUPPORTED_GROUP_BY`·`UNSUPPORTED_HAVING`·`UNSUPPORTED_DISTINCT`·
`UNSUPPORTED_AGGREGATE` 는 모두 같은 원인이다. 기본값인 `strict_validation: true` 는 단순한
SELECT 만 받는데, JOIN 이나 GROUP BY 가 섞인 쿼리를 보냈다는 뜻이다. `false` 로 두면 복합 쿼리도
받으면서 파티션 `IN` 절을 쿼리 어디에 있든 찾아 나눈다.

### 422 말고 다른 응답

| 응답 | 뜻 | 어떻게 하나 |
| --- | --- | --- |
| `429` | 서버가 받을 수 있는 양을 넘었다 | `Retry-After` 초만큼 기다렸다 다시 보낸다 |
| `409` | 이미 끝난 작업을 취소·재실행하려 했거나, 같은 멱등 키로 다른 본문을 보냈다 | 상태를 먼저 확인한다 |
| `404` | 그런 `job_id` 가 없다 | 식별자를 확인한다 |

`429` 는 잘못이 아니라 **줄을 서 달라는 뜻**이다. 실행 슬롯과 대기 큐가 모두 찼을 때 나오며,
`Retry-After` 헤더(기본 5초)만큼 기다렸다 재시도하면 대개 통과한다. 자동 재시도를 넣을 때는
[멱등 키](#같은-요청을-두-번-보내지-않으려면)를 함께 쓰는 편이 안전하다.

### 실행 중에 실패했을 때

검증을 통과한 뒤 실패하면 `422` 가 아니라 작업 상태로 나타난다. `GET /jobs/{job_id}` 의 `error`
필드에 이유가 있고, 조각별로 어디서 어떻게 실패했는지는 같은 응답의 task 목록에서 본다.
`PARTIAL` 이면 일부만 들어간 것이므로 `retry` 로 나머지를 채운다.

원인이 서버 쪽(소스 접속 실패, 대상 테이블 없음, 권한 부족)으로 보이면 운영자에게 `job_id` 를
알려 주는 편이 빠르다. 서버 로그에는 작업·조각 식별자와 실제로 실행한 SQL 이 함께 남아 있다.

---

## 실수하기 쉬운 것들

**`IN` 목록이 곧 병렬도의 한계다.** `parallelism: 32` 를 줘도 `IN` 값이 3개면 3조각으로만 나뉜다.
더 잘게 나누고 싶으면 나눌 값을 늘리거나 날짜 fan-out 을 쓴다.

**`append` 는 다시 돌리면 쌓인다.** 재실행이 예상되는 작업이라면 처음부터
`write_mode: overwrite_partitions` 로 만든다.

**타임아웃 재시도는 작업을 두 번 만들 수 있다.** 응답을 못 받았다고 그냥 다시 보내지 말고 멱등
키를 쓰거나, 이미 만들어졌는지 먼저 확인한다.

**`/query-execute` 는 미리보기다.** `limit` 으로 잘리므로 이관에 쓰지 않는다.

**결과는 HTTP 응답에 없다.** 데이터는 executor 가 대상으로 직접 보내므로, 확인은 대상 테이블에서
한다. API 가 돌려주는 것은 상태와 행 수뿐이다.

---

## 더 볼 것

- [실행 모드 사용 가이드](GUIDE.md) — 모드별 전체 요청 예제와 필수 필드
- [C# 연동 가이드](INTEGRATION.md) — 폴링·취소·재시도를 포함한 클라이언트 코드
- [운영자 가이드](OPERATIONS.md) — 서버가 어떻게 돌아가는지, 느릴 때 무엇을 보는지
- Swagger UI — `http://<coordinator>:8088/docs` 에서 모든 API 를 직접 호출해 볼 수 있다
