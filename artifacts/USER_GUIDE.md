# 통합 사용자 가이드

이 문서는 **큰 데이터를 옮기는 일을 맡기는 사람**을 위한 것이다. 함께 설치된 터미널 도구로
데이터베이스와 S3 를 직접 들여다보는 방법도 담았다.

## 이 시스템이 무엇인지부터

한마디로 **큰 데이터를 한 데이터베이스에서 다른 데이터베이스로 빠르게 옮겨 주는 도구**다.

옮길 데이터가 수억 건이라고 해 보자. 직접 `SELECT` 를 던져 결과를 받아 넣으면 데이터가 한 줄기로만
흐르니 몇 시간이 걸린다. 이 시스템은 그 `SELECT` 하나를 **여러 조각으로 쪼개** 여러 대의 서버가
동시에 읽어 동시에 넣는다.

## 두 갈래를 먼저 구분하자

이 문서에는 성격이 다른 두 가지가 나온다. 이것만 구분해 두면 나머지는 쉽다.

**서비스** — 큰 데이터를 옮기는 일을 대신해 주는 쪽이다. HTTP 로 "이걸 옮겨 달라"고 요청하면 알아서
나눠 처리한다. 여러분은 **`8088` 포트 한 곳만 상대하면 된다.**

**터미널 도구** — 사람이 직접 두드리는 쪽이다(`bin/gp-shell`·`bin/impala-shell`·`bin/s3-ops`). 이관
결과가 제대로 들어갔는지 확인하거나, 중간 파일로 남은 것을 정리할 때 쓴다.

둘은 **같은 설정 파일에서 접속 정보를 읽는다.** 그래서 같은 값을 두 번 적을 필요가 없다.

이 문서 하나만 읽어도 일이 끝나도록 필요한 내용을 모두 담았다. 설치와 접속 정보 설정, 용량 조정처럼
**서버를 돌보는 쪽 이야기**는 같은 디렉터리의 운영자 가이드에 있다.

![분산 쿼리 실행기 전체 구성](images/architecture.svg)

그림에서 눈여겨볼 것은 **데이터가 coordinator 를 지나가지 않는다**는 점이다. 점선으로 오가는 것은
요청과 상태뿐이고, 실제 데이터는 executor 가 원본에서 읽어 목적지로 곧장 흘려보낸다.

이것이 이 시스템이 빠른 이유이자, 뒤에 나오는 여러 규칙의 배경이다.

---

# 1장. 이관 작업 맡기기

## 어떤 일을 대신 해 주는가

`SELECT` 한 건을 주면 시스템이 이렇게 처리한다.

1. 그 SELECT 를 **여러 조각으로 나눈다.** 기준은 여러분이 알려 준 컬럼의 `IN` 목록이다
2. 조각을 나눠 맡은 executor 들이 **동시에** 읽는다
3. 각 executor 가 자기 몫을 곧바로 목적지에 넣는다

나누는 것도, 동시에 읽는 것도, 넣는 것도 서버가 한다. **요청하는 쪽은 무엇을 어디로 옮길지만 알려
주면 된다.**

여기서 꼭 알아 둘 것이 둘 있다.

**첫째, 결과 데이터를 HTTP 응답으로 받을 수 없다.** 데이터는 executor 가 목적지로 곧장 보내고,
coordinator 에는 진행 상태와 건수만 올라온다. **옮긴 데이터는 목적지 테이블에 있다.** 그래서 작업이
아무리 커도 응답은 가볍다.

**둘째, 제출과 완료 확인은 언제나 두 단계다.** 요청하면 접수만 하고 작업 번호를 즉시 돌려준다. 실제
실행은 그 뒤에 진행되므로, **끝났는지는 따로 물어봐야 한다.**

결과를 그 자리에서 받아야 하는 미리보기 조회만 예외인데, 그것은 별도 API 이며 뒤에서 다룬다.

## 첫 작업 제출

가장 단순한 형태는 SQL 과 분할 기준 컬럼, 대상 테이블 세 가지다.

```bash
curl -s localhost:8088/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-01-01','2026-01-02') AND region='KR'",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "parallelism": 2
}'
# → 202 {"job_id": "job_ab12cd34"}
```

**`partition_column` 이 가장 중요한 필드다.** 이 컬럼의 `IN` 목록을 기준으로 SQL 을 나누기 때문이다.

위 예에서는 `dt IN ('2026-01-01','2026-01-02')` 를 두 조각으로 갈라 각각 하루씩 읽는다.

여기서 두 가지를 지켜야 한다. **이 컬럼에 대한 `IN` 절이 SQL 안에 반드시 있어야 한다.** 그리고
**값의 개수가 나누려는 수보다 적으면 그만큼만 나뉜다.** 값이 2개인데 10조각으로 나누라고 해도
2조각이 된다.

자주 쓰는 나머지 필드는 이렇다.

| 필드 | 하는 일 | 기본값 |
|---|---|---|
| `parallelism` | 몇 조각으로 나눌지 (1~128) | 4 |
| `split_strategy` | 값을 잇달아 묶을지(`contiguous`) 번갈아 나눌지(`round_robin`) | `contiguous` |
| `write_mode` | `overwrite_partitions` 로 두면 넣기 전에 그 자리를 먼저 지운다 | `append` |
| `failure_policy` | 한 조각이 실패하면 전체를 실패로 볼지(`fail_fast`) 나머지는 계속할지(`best_effort`) | `fail_fast` |
| `username` | 누가 요청했는지. 기록에 남으므로 채워 두는 편이 좋다 | — |

SQL 해석과 관련된 필드도 둘 있다.

**`sql_dialect`** 는 SQL 을 어느 데이터베이스의 문법으로 볼지 정한다. 기본값은 `hive` 다.

**`strict_validation`** 은 기본값이 `true` 라 **단순한 SELECT 만 받는다.** JOIN 이나 서브쿼리, GROUP
BY 가 섞인 복잡한 쿼리를 보내려면 `false` 로 둔다. 그러면 파티션 조건을 쿼리 어디에 있든 찾아
나눈다.

원본이 Impala 라면 그 작업에만 적용할 옵션을 `impala_query_options` 로 넘길 수 있다(예:
`{"MEM_LIMIT": "2g"}`). **이 옵션은 읽는 쪽에만 붙고 넣는 쪽에는 영향을 주지 않는다.**

**처음 만드는 요청이라면 `dry_run` 을 켜 보기를 권한다.** 실제로는 아무것도 옮기지 않으면서 **어떻게
나눌 계획인지를 돌려준다.** 조각마다 어떤 SQL 이 만들어지는지 눈으로 확인할 수 있다.

### 같은 요청을 두 번 보내지 않으려면

**흔히 겪는 상황부터 말하자.** 요청을 보냈는데 네트워크가 끊겨 응답을 못 받았다. 그냥 다시 보내면
어떻게 될까. **같은 이관이 두 번 돌아 데이터가 두 배로 들어갈 수 있다.**

이를 막으려면 `Idempotency-Key` 라는 헤더에 **요청마다 고유한 값**을 실어 보낸다. 날짜나 업무
이름처럼 그 요청을 특정할 수 있는 값이면 된다.

```bash
curl -s localhost:8088/jobs \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: sales-mirror-2026-01-02' \
  -d '{...}'
```

그러면 서버가 이렇게 처리한다.

| 상황 | 서버의 응답 |
|---|---|
| 같은 키 + 같은 내용 | 새로 만들지 않고 **원래 작업을 그대로 돌려준다.** `200` 과 함께 `Idempotency-Replayed: true` 헤더가 붙는다 |
| 같은 키 + 다른 내용 | `409` 로 **거절한다** |
| 처음 보는 키 | 정상 접수. `202` 를 돌려준다 |

두 번째 줄을 거절하는 이유는, **키를 재사용하다 엉뚱한 작업을 덮어쓰는 사고**를 막기 위해서다.

## exec_mode 고르기

같은 "읽어서 넣는다"라도 **데이터가 지나는 길은 여러 가지다.**

요청의 모양은 모든 방식이 똑같고, **어떤 길로 갈지는 `exec_mode` 라는 필드 하나가 정한다.** 같은
SQL 과 같은 목적지를 두고 이 값만 바꾸면 처리 방식이 갈린다.

다섯 가지를 나란히 늘어놓으면 무엇이 다른지가 한눈에 들어온다.

![exec_mode 다섯 가지 경로](images/exec-modes.svg)

표로 정리하면 이렇다.

| 방식 | 어떻게 넣나 | 더 필요한 필드 | 다시 돌려도 안전한가 |
|---|---|---|---|
| `copy` | 읽은 결과를 목적지에 곧장 넣는다 | 없음 | `overwrite_partitions` 로 가능 |
| `statement` | 받은 SQL 을 목적지가 스스로 실행한다 | — | 문장이 정하기 나름 |
| `stage_insert` | 임시 테이블을 거쳐 넣는다 | `staging_table` · `wrapper_query` | **불가능**(언제나 덧붙임) |
| `local_stage` | CSV 파일을 만들어 목적지가 읽게 한다 | `staging_table` · `external_columns` · `insert_sql` | `overwrite_partitions` 로 가능 |
| `s3_stage` | 같은 방식인데 파일을 S3 에 둔다 | 위와 같음 | `overwrite_partitions` 로 가능 |

뒤의 둘은 **거의 같고, 파일을 어디에 두느냐만 다르다.** 그 차이가 서버를 어디에 둘 수 있느냐를
가른다.

**기본값은 `copy` 이며 대부분의 경우에 맞는다.** 템플릿을 쓰면 템플릿에 적힌 값이 기본이 되고,
요청에 직접 적으면 그쪽이 이긴다.

**방식마다 더 넣어야 하는 필드가 다르다.** 빠뜨리면 접수 시점에 `422` 로 걸러지므로, 데이터가 반쯤
들어간 상태로 실패하는 일은 없다.

### copy — 가장 단순한 경로

**가장 단순하고 가장 흔히 쓰는 방식이다.** 원본에서 읽은 결과를 목적지에 곧바로 넣는다.

추가로 넣을 필드가 없다. `sql`·`partition_column`·`target_table` 세 가지만 있으면 되고, 앞의 "첫
작업 제출" 예가 그대로 이 방식이다.

나뉜 조각 쿼리를 무언가로 감싸야 한다면 `wrapper_query` 에 감쌀 쿼리를 두고, 조각이 들어갈 자리에
`{{SUBQUERY}}` 라고 적는다.

**다시 돌려도 안전하게 만들 수 있다.** `write_mode` 를 `overwrite_partitions` 로 두면 된다.

### statement — 옮기지 않고 대상에서 실행

**데이터를 옮기지 않고, 받은 SQL 을 목적지가 스스로 실행하게 하는 방식이다.**

언제 쓸까. 원본과 목적지가 **같은 데이터베이스**이거나, 목적지가 원본을 **직접 읽을 수 있는**
경우다. 그럴 때는 데이터를 executor 로 끌어올 이유가 없다.

이 방식에서 executor 는 SQL 을 던지고 결과를 기다리기만 한다. **데이터가 executor 를 전혀 지나지
않는다.**

### stage_insert — staging 을 거치는 표준 이관

**임시 테이블을 한 번 거쳐 넣는 방식이다.** 원본에서 읽은 결과를 먼저 임시 테이블에 넣고, 그다음 그
임시 테이블에서 최종 테이블로 옮긴다.

언제 쓸까. 원본과 목적지의 **컬럼 이름이나 구조가 달라서 중간에 손을 봐야 할 때**, 또는 넣는 SQL
자체가 복잡할 때다. 두 단계로 나누면 그 사이에 원하는 처리를 넣을 수 있다.

```jsonc
POST /jobs
{
  "exec_mode": "stage_insert",
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-07-01','2026-07-02')",
  "partition_column": "dt",
  "target_table": "public.sales",
  "staging_table": "stg_sales",                     // 필수
  "staging_ddl": "CREATE TEMP TABLE stg_sales (user_id bigint, amount numeric, dt date)",  // 선택
  "wrapper_query": "INSERT INTO public.sales (user_id, amount, dt) SELECT * FROM stg_sales", // 필수
  "parallelism": 4
}
```

넣는 SQL 은 `wrapper_query` 필드에 담는다. **`copy` 방식과 다른 점이 있으니 헷갈리지 말자.**
여기서는 `{{SUBQUERY}}` 를 쓰지 않고, **임시 테이블에서 읽어 최종 테이블에 넣는 완성된 문장**을
적는다.

`staging_table` 과 `wrapper_query` 가 둘 다 없으면 `422 STAGE_INSERT_REQUIRES_FIELDS` 로 거절된다.

**`staging_ddl` 은 선택이다.** 비워 두면 테이블을 만들지 않고 이미 있는 것을 쓴다. 다만 이때는
**여러 조각이 같은 임시 테이블을 함께 쓰지 않도록 주의해야 한다.** 함께 쓰면 서로의 데이터가 섞인다.

**TEMP 로 만들어 두는 것을 권한다.** TEMP 테이블은 접속마다 따로 만들어지므로 조각끼리 저절로
격리되고, 조각이 실패해도 함께 사라져 깨끗한 상태에서 다시 시작할 수 있다. `CREATE TEMP TABLE ...
(LIKE 대상테이블)` 이 가장 안전하다. 읽어 온 컬럼과 임시 테이블 컬럼은 **이름·개수·순서가 모두
같아야 한다.**

**여기에 반드시 알아 둘 제약이 있다. 이 방식은 `write_mode` 를 적용하지 않는다.** 언제나 덧붙이기만
하므로 **같은 날짜를 두 번 실행하면 데이터가 중복된다.**

다시 돌려도 안전해야 한다면 **사람이 준비해야 한다.** 목적지 테이블을 미리 비우거나, 날짜별로
테이블을 따로 두는 것이 흔한 방법이다.

### local_stage — 세그먼트가 로컬 파일을 병렬로 읽음

**아주 큰 데이터를 넣을 때 쓰는 방식이다.** 두 단계로 나뉜다.

**1단계** — executor 가 원본에서 읽은 데이터를 CSV 파일로 만들어 목적지 서버의 로컬 디스크에 둔다.

**2단계** — coordinator 가 그 파일들을 목적지가 읽을 수 있게 연결해 주면, **목적지의 여러 서버가
각자 자기 파일을 동시에 읽어 넣는다.**

왜 빠를까. 앞의 `copy` 방식은 목적지의 대표 서버 한 대로 밀어 넣지만, 이 방식은 **목적지의 모든
서버가 동시에 일한다.**

**대신 제약이 하나 있다. executor 를 목적지 서버와 같은 자리에 두어야 한다.** 파일을 로컬 디스크에
두기 때문이다. 쓸 수 있는 환경인지 운영자에게 먼저 확인한다.

```jsonc
POST /jobs
{
  "exec_mode": "local_stage",
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-06-01','2026-06-02','2026-06-03','2026-06-04') AND region='KR'",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "write_mode": "overwrite_partitions",
  "parallelism": 4,
  "external_columns": "user_id bigint, amount numeric, dt date",
  "staging_table": "stg_sales",
  "staging_ddl": "CREATE TEMP TABLE stg_sales (user_id bigint, amount numeric, dt date) DISTRIBUTED BY (user_id)",
  "insert_sql": "INSERT INTO public.sales_mirror (user_id, amount, dt) SELECT user_id, amount, dt FROM stg_sales"
}
```

**필수 필드가 셋이다.** `staging_table`·`external_columns`·`insert_sql` 중 하나라도 빠지면 `422
LOCAL_STAGE_REQUIRES_FIELDS` 로 거절된다.

이 중 **`external_columns` 를 특히 조심해야 한다.** 파일을 읽을 때 쓸 컬럼 정의인데, **CSV 의 컬럼
순서, 곧 SELECT 가 내는 순서와 타입이 정확히 같아야 한다.** 어긋나면 오류 없이 데이터만 밀려
들어간다.

`staging_ddl` 은 선택이고, TEMP 로 두면 끝날 때 저절로 정리되어 다시 돌리기 깔끔하다.

**이 방식은 다시 돌려도 안전하게 만들 수 있다.** `overwrite_partitions` 로 두면 넣기 전에 그 자리를
지운다.

### s3_stage — S3 를 거치므로 co-location 제약이 없음

**앞의 `local_stage` 와 구조가 같고, 파일을 어디에 두느냐만 다르다.** 로컬 디스크가 아니라 S3 에
둔다.

1단계에서 각 executor 가 결과를 CSV 로 만들어 **S3 에 올리고**, 모두 끝나기를 기다린 뒤 2단계에서
coordinator 가 그 객체들을 목적지가 읽게 연결해 넣는다.

**가장 큰 장점은 executor 를 아무 데나 둘 수 있다는 것이다.** S3 는 위치와 상관없이 읽히기 때문이다.
`local_stage` 의 제약이 없다.

```jsonc
POST /jobs
{
  "exec_mode": "s3_stage",
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-06-01','2026-06-02','2026-06-03','2026-06-04') AND region='KR'",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "write_mode": "overwrite_partitions",
  "parallelism": 4,
  "external_columns": "user_id bigint, amount numeric, dt date",
  "staging_table": "stg_sales_s3",
  "insert_sql": "INSERT INTO public.sales_mirror (user_id, amount, dt) SELECT user_id, amount, dt FROM stg_sales_s3"
}
```

필수 필드는 `local_stage` 와 같다. 빠지면 `422 S3_STAGE_REQUIRES_FIELDS` 다.

**다만 `staging_ddl` 은 주지 않는다.** 임시 테이블을 따로 만들지 않고 S3 파일을 곧바로 읽어 넣기
때문이다. `staging_table` 은 `insert_sql` 의 `FROM` 에 적을 이름일 뿐이고, coordinator 가 2단계에서
**그 작업 전용 이름으로 바꿔 준다.**

이 방식에는 `pre_delete` 라는 옵션이 하나 더 있다. **"넣기 전에 지울까 말까"를 직접 정하는 값**이다.

| 값 | 동작 |
|---|---|
| 지정하지 않음(기본) | `write_mode` 를 따른다 |
| `true` | `write_mode` 와 무관하게 **반드시 지운다** |
| `false` | `write_mode` 와 무관하게 **지우지 않는다** |

목적지가 이미 비어 있어 지우는 시간을 아끼고 싶을 때, 또는 반대로 덧붙이는 방식인데도 중복을 막고
싶을 때 쓴다.

## 템플릿으로 요청하기

SQL 전문을 매번 만들어 보내는 대신, **서버에 미리 등록해 둔 쿼리에 값만 넘기는** 방법이 있다.

**이렇게 하면 좋은 점이 둘이다.** 요청하는 쪽이 테이블 구조나 적재 규칙을 몰라도 되고, **쿼리가
바뀌어도 호출하는 코드는 그대로 둘 수 있다.**

```bash
curl -s localhost:8088/jobs -H 'content-type: application/json' -d '{
  "template_id": "sales_migration",
  "params": {"start_dt": "2026-07-01", "end_dt": "2026-07-07", "regions": ["KR"]},
  "parallelism": 4
}'
```

**쓸 수 있는 템플릿 목록은 `GET /templates` 로 확인한다.**

템플릿 이름을 주면 **SQL 관련 필드는 적지 않아도 된다.** 템플릿이 만들어 채우기 때문이다. 옮기는
방식이나 목적지 테이블처럼 템플릿이 기본값을 갖고 있는 값도 마찬가지다.

**요청에 직접 적으면 그쪽이 이긴다.** 그러므로 대부분은 템플릿에 맡기고 필요한 것만 덮어쓰면 된다.

템플릿이 어떻게 생겼는지 한 번 보아 두면 값을 채우기 쉽다. 아래는 날짜 구간을 받아 목록을 자동으로
만들어 주는 예다.

```yaml
# manifest.yml
id: sales_migration
description: 일별 매출 Impala→Greenplum 이관(날짜 구간 파라미터로 IN 목록 자동 생성)
exec_mode: stage_insert
partition_column: dt
target_table: public.sales
staging_table: stg_sales
strict_validation: false
params:
  - {name: start_dt, type: date, required: true}
  - {name: end_dt,   type: date, required: true}
  - {name: regions,  type: list, required: false, default: []}
files:
  select:      select.sql.j2
  staging_ddl: staging_ddl.sql.j2
  insert:      insert.sql.j2
```

```sql
-- select.sql.j2
SELECT user_id, amount, region, dt
FROM sales
WHERE dt IN ( {{ date_range(start_dt, end_dt) | sql_in }} )
{%- if regions %}
  AND region IN ( {{ regions | sql_in }} )
{%- endif %}
```

**값은 두 가지 형태로 줄 수 있다.** 이름과 값을 짝지은 단순한 형태가 기본이고, **방향(부호)까지 함께
실어야 하는 템플릿에서는 배열 형태**를 쓴다. 방향은 배열 형태에서만 표현할 수 있다.

```json
"params": [
  {"name": "from_date_no", "value": 7, "sign": "-"},
  {"name": "to_date_no",   "value": 0, "sign": "+"}
]
```

**`sign` 은 값의 부호가 아니라 SQL 에 들어갈 연산자의 방향이다.** 헷갈리기 쉬운 부분이니 짚어 둔다.

왜 이런 것이 필요할까. Impala 에서 날짜를 계산할 때는 `current_date() - interval 7 day` 처럼
**방향이 문장에 직접 박힌다.** 숫자 7 자체는 양수든 음수든 `7 day` 로만 들어가므로, **값만으로는
"7일 전"인지 "7일 뒤"인지 알 수 없다.**

그래서 방향을 따로 받는다. 생략하면 값 자체의 부호를 쓰므로, **`value: -7` 과 `value: 7, sign:
"-"` 은 같은 뜻이다.**

### 하루를 한 task 로 나누기

기간을 다루는 이관에서는 목록으로 나누는 것보다 **날짜별로 나누는 편이 자연스럽다.**

`task_params` 에 **구간의 시작과 끝을 담은 값 이름 두 개**를 지목하면, 하루를 조각 하나로 펼쳐
실행한다.

```jsonc
{
  "template_id": "daily_sales_interval",
  "params": [
    {"name": "from_date_no", "value": 7, "sign": "-"},   // → 오프셋 -7
    {"name": "to_date_no",   "value": 1, "sign": "+"},   // → 오프셋 +1
    {"name": "region",       "value": "KR"}
  ],
  "task_params": ["from_date_no", "to_date_no"],
  "task_bound": "point"
}
```

예를 들어 오늘이 2026-07-22 이고 구간이 `[-7, +1]` 이라면, 2026-07-15 부터 2026-07-23 까지이므로
**조각이 9개** 만들어진다.

**이 방식에서는 `partition_column`·`parallelism`·`split_strategy` 를 쓰지 않는다.** 조각 수는 날짜
수가 정하기 때문이다.

조각마다 coordinator 가 **시작과 끝을 같은 날로 좁혀** 준다. 그래서 원래 구간을 조회하던 조건이
**하루짜리 조건으로 줄어든다.** 다섯 번째 조각이라면 "3일 전부터 3일 전까지"가 되는 식이다.

**`task_bound` 는 조각 하나가 받는 구간의 모양을 정한다.** 무엇을 골라야 할지는 **쿼리에서 날짜를
어떻게 비교하는지**가 결정한다.

| 쿼리의 비교 방식 | 골라야 할 값 | 위 예에서 조각 수 |
|---|---|---|
| `BETWEEN a AND b` 나 `= a` (양 끝 포함), DATE 컬럼 | `point` (기본값) | 9개 |
| `>= a AND < b` (끝을 포함하지 않음), TIMESTAMP 컬럼 | `pair` | 8개 |

**잘못 고르면 오류 없이 데이터만 틀어진다.** 이 점이 위험하다.

- 양 끝 포함 비교에 `pair` 를 주면 → **경계 날짜가 두 조각에 겹쳐 중복해서 들어간다**
- 끝을 포함하지 않는 비교에 `point` 를 주면 → **자정 정각 데이터만 읽혀 사실상 0건이 된다**

**가장 안전한 방법은 템플릿에 이 값을 못 박아 두는 것이다.** 그러면 요청하는 쪽이 컬럼 타입을 몰라도
된다. 템플릿을 만드는 사람에게 그렇게 해 달라고 요청하는 편이 좋다.

**이 기능은 `stage_insert` 와 `s3_stage` 에서만 쓸 수 있다.** 다른 방식에서는 `422` 로 거절된다.

그리고 **넣는 방식이 언제나 덧붙이기**이므로, 다시 돌려도 안전해야 한다면 목적지를 미리 비우거나
날짜별로 테이블을 나눠 써야 한다.

## 진행 상황 확인

**제출과 완료는 별개다.** 그러므로 작업 번호로 상태를 물어봐야 한다.

작업 하나가 어떤 상태를 지나가는지 먼저 보아 두면, **어디서 그만 물어봐도 되는지**가 분명해진다.

![작업 하나가 지나가는 상태](images/job-lifecycle.svg)

조회는 두 가지인데, 반복해서 물어볼 때는 task 목록이 빠진 가벼운 쪽을 쓴다.

```bash
curl -s localhost:8088/jobs/$JOB_ID/status   # 가볍다 — 폴링에는 이쪽
curl -s localhost:8088/jobs/$JOB_ID          # task 목록까지 함께
```

경량 응답은 이렇게 생겼다.

```json
{
  "job_id": "job_3f9c2a1b7d4e",
  "status": "RUNNING",
  "progress_percent": 50.0,
  "completed": 2,
  "total": 4,
  "total_rows_written": 18234,
  "error": null,
  "cancel_requested": false,
  "created_at": "2026-06-29 07:01:11.123",
  "started_at": "2026-06-29 07:01:11.456",
  "finished_at": null
}
```

이름이 `_at` 로 끝나는 필드는 모두 시각이다. `yyyy-MM-dd HH:mm:ss.sss` 형식이고, **아직 일어나지
않은 일이면 `null` 이다.**

상태는 이렇게 흐른다.

| 상태 | 뜻 | 끝났나 |
|---|---|---|
| `PENDING` | 접수는 됐지만 자기 차례를 기다리는 중 | 아니오 |
| `SPLITTING` | 쿼리를 검사하고 조각으로 나누는 중 | 아니오 |
| `RUNNING` | executor 들이 조각을 실행하는 중 | 아니오 |
| `DONE` | **모든 조각이 성공했다** | 예 |
| `PARTIAL` | 일부만 성공했다 | 예 |
| `FAILED` | 실패했다 | 예 |
| `CANCELLED` | 취소됐다 | 예 |

아래 넷에 도달하면 **더 이상 바뀌지 않으므로 그만 물어봐도 된다.**

**여기서 가장 중요한 것: 온전한 성공은 `DONE` 하나뿐이다.**

`PARTIAL` 은 **일부 데이터가 빠진 상태**다. 성공으로 다루면 안 된다. 이것은 `best_effort` 정책을
골랐을 때 나온다.

```bash
while :; do
  S=$(curl -s localhost:8088/jobs/$JOB_ID/status | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  case "$S" in DONE|PARTIAL|FAILED|CANCELLED) echo "종료: $S"; break;; esac
  sleep 3
done
```

**얼마나 자주 물어보면 좋을까. 1초에서 3초면 무난하다.** 큰 이관은 수십 분씩 걸리므로 1초보다 자주
물어도 얻는 것 없이 서버만 두드리게 된다.

**여기서 초보자가 자주 겪는 함정이 있다.** HTTP 클라이언트에 타임아웃을 걸 때, **작업 전체가 걸리는
시간을 그 타임아웃으로 묶으면 안 된다.** 작업이 수십 분 걸릴 수 있어 정상 작업도 끊긴다.

**개별 호출의 타임아웃은 짧게 두고**, 작업 전체의 시간 제한은 별도 수단으로 건다.

### 결과 확인

`DONE` 에 도달하면 `GET /jobs/{job_id}/result` 로 전체 적재 행 수와 task 별 행 수를 가져온다.

```json
{
  "job_id": "job_3f9c2a1b7d4e",
  "status": "DONE",
  "total_rows_written": 40567,
  "per_task": [
    { "task_id": "t_a1b2c3d4e5f6", "rows_written": 10120 },
    { "task_id": "t_b2c3d4e5f6a1", "rows_written": 10010 }
  ]
}
```

**조각마다의 상태나 오류까지 보려면** `GET /jobs/{job_id}` 를 호출한다. 위 정보에 더해 조각마다
상태와 건수, 시도 횟수, 오류 메시지를 담은 목록이 함께 온다.

```json
{
  "job_id": "job_3f9c2a1b7d4e",
  "status": "PARTIAL",
  "completed": 3,
  "total": 4,
  "progress_percent": 75.0,
  "total_rows_written": 30360,
  "error": "1개 파티션 실패",
  "retry_of": null,
  "tasks": [
    { "task_id": "t_a1b2c3d4e5f6", "executor_url": "http://10.0.0.11:8087", "status": "DONE",
      "rows_written": 10120, "attempt": 0, "partition_values": ["A"], "error": null },
    { "task_id": "t_d4e5f6a1b2c3", "executor_url": "http://10.0.0.12:8086", "status": "FAILED",
      "rows_written": 0, "attempt": 2, "partition_values": ["D"], "error": "greenplum connection refused" }
  ]
}
```

`attempt` 는 **그 조각이 몇 번 다시 시도됐는지**를 알려 준다. 이 값이 크다면 그 조각이 반복해서
실패하고 있다는 뜻이다.

어떤 조각이 **어떤 SQL 을 실행했는지**까지 보려면 `GET /jobs/{job_id}/tasks/{task_id}` 를 쓴다.

### 취소와 재실행

```bash
curl -s -X POST localhost:8088/jobs/$JOB_ID/cancel   # 진행 중인 작업 중단
curl -s -X POST localhost:8088/jobs/$JOB_ID/retry    # 실패한 task 만 다시
```

**취소**를 요청하면 각 executor 로 전달되어 실행 중인 조각을 멈추고 작업을 `CANCELLED` 로 표시한다.
**이미 끝난 작업을 취소하려 하면 `409` 가 온다.** 멈출 것이 없기 때문이다.

여기서 알아 둘 것이 있다. **취소해도 이미 들어간 데이터는 되돌아가지 않는다.** 목적지를 확인해야
한다.

**재실행**은 `PARTIAL`·`FAILED`·`CANCELLED` 로 끝난 작업에서 **실패하거나 취소된 조각만 모아** 새
작업으로 돌린다.

**이미 성공한 조각은 아예 담기지 않으므로, 데이터가 중복될 걱정 없이 눌러도 된다.**

```json
{ "job_id": "job_7a8b9c0d1e2f", "retry_of": "job_3f9c2a1b7d4e", "retried_tasks": 1 }
```

**새 작업 번호가 `202` 와 함께 돌아온다.** 그 번호로 다시 상태를 물어보면 된다.

다시 돌릴 조각이 없거나 작업이 아직 끝나지 않았으면 `409` 가 온다. **이것은 정상적인 거부 신호이지
오류가 아니다.** 당황할 필요 없다.

## 결과를 바로 받아 보기

이관이 아니라 **"이 쿼리가 지금 무엇을 돌려주는지"** 만 보고 싶을 때는 `POST /query-execute` 를
쓴다.

등록해 둔 쿼리의 조회 부분만 실행해 **위쪽 몇 줄을 그 자리에서 돌려준다.** 기다렸다 물어볼 필요가
없다.

```bash
curl -s localhost:8088/query-execute -H 'content-type: application/json' -d '{
  "template_id": "sales_migration",
  "params": [{"name": "region", "value": "KR"}],
  "limit": 100
}'
```

**여기서 `params` 는 언제나 배열이다.** 이관 요청에서는 객체 형태도 받으므로 헷갈리기 쉽다. 이
API 에서는 배열만 받는다.

`limit` 은 1에서 10000 사이이고 기본값은 100이다. `datasource` 로 어느 데이터베이스에서 실행할지
고를 수 있다.

응답에는 실행한 SQL 과 컬럼, 줄 목록, 건수, 잘렸는지 여부, 걸린 시간이 담긴다. 여기에
**`executed_by` 라는 값**이 하나 더 있다.

쿼리는 coordinator 가 **가장 한가한 executor 에게 맡겨** 실행한다. 그래서 이 값으로 **실제 어디서
돌았는지** 알 수 있다. coordinator 가 직접 실행했다면 `null` 이다. 요청하는 쪽이 실행할 곳을 지정할
수는 없다.

**여기서 반드시 알아야 할 것이 있다. 이 결과는 정해진 줄 수에서 잘린 미리보기다.**

**이관에 쓰면 잘린 데이터가 그대로 들어간다.** 옮기는 일은 반드시 `POST /jobs` 로 한다.

## 오류 대처

오류는 **성격이 다른 두 갈래**다. 어느 쪽인지 먼저 가리면 대처가 쉬워진다.

| 갈래 | 언제 | 어디에 나타나나 |
|---|---|---|
| 요청이 거부됨 | 접수 단계 | HTTP 상태 코드(`422`·`429`·`409`·`404`) |
| 실행 중 실패 | 접수된 뒤 | 상태 조회 결과의 `status` 와 `error` |

앞엣것은 **아무것도 실행되지 않은 상태**이고, 뒤엣것은 **일부가 이미 실행됐을 수 있는 상태**다.

### 요청이 거부될 때

검증에 걸린 요청은 `422` 와 함께 `error_code` 를 돌려준다.

```json
{"error_code": "NO_PARTITION_IN_CLAUSE", "message": "..."}
```

**`422` 응답이 두 가지 형태로 온다는 점에 주의한다.** 프로그램을 만든다면 둘 다 처리해야 한다.

**첫째 형태** — 이 시스템이 판단해서 거절한 경우다. `error_code` 와 `message` 를 담고 있어 **코드로
원인을 나눌 수 있다.**

**둘째 형태** — 요청의 모양 자체가 틀린 경우다(예: `sql` 을 아예 빠뜨렸거나 `parallelism` 이 허용
범위를 벗어남). 이때는 `detail` 이라는 배열 형태로 온다.

```json
{ "detail": [ { "loc": ["body", "parallelism"], "msg": "...", "type": "..." } ] }
```

자주 만나는 오류 코드를 **갈래별로** 묶어 두면 외우지 않아도 된다.

**갈래 1 — 쿼리를 나눌 수 없다** (가장 흔하다)

| 코드 | 뜻 | 어떻게 고치나 |
|---|---|---|
| `NO_PARTITION_IN_CLAUSE` | 지정한 컬럼의 `IN` 절이 SQL 에 없다 | SQL 에 `IN` 절을 넣는다 |
| `MISSING_PARTITION_COLUMN` | `partition_column` 필드를 주지 않았다 | 필드를 채운다 |
| `EMPTY_IN_LIST` | `IN` 절은 있는데 값이 비었다 | 값을 넣는다 |
| `NEGATED_IN` | `NOT IN` 이라 나눌 기준이 없다 | 값 목록으로 바꾼다 |
| `SUBQUERY_IN_CLAUSE` | `IN (SELECT ...)` 이라 값을 알 수 없다 | 값 목록으로 바꾼다 |

**갈래 2 — 쿼리 자체를 받아들일 수 없다**

| 코드 | 뜻 |
|---|---|
| `NOT_A_SELECT` | SELECT 가 아니다 |
| `MULTIPLE_STATEMENTS` | 여러 문장을 한 번에 보냈다 |
| `PARSE_ERROR` | SQL 을 해석하지 못했다 |

`PARSE_ERROR` 는 **문법이 분명히 맞는데도 나올 수 있다.** 그럴 때는 문법 종류가 달라서일 수 있으니
`sql_dialect` 를 지정해 본다.

**갈래 3 — 필드가 모자라다**

`MISSING_REQUIRED_FIELDS` 는 공통 필수 필드가 빠진 것이고,
`STAGE_INSERT_REQUIRES_FIELDS`·`LOCAL_STAGE_REQUIRES_FIELDS`·`S3_STAGE_REQUIRES_FIELDS` 는 **그
방식에만 필요한 필드**가 빠진 것이다. 무엇을 채워야 하는지는 앞의 "exec_mode 고르기" 표에 있다.

**갈래 4 — 템플릿 문제**

`TEMPLATE_NOT_FOUND` 는 그런 템플릿이 없다는 뜻이니 `GET /templates` 로 이름을 확인한다.
`TEMPLATE_PARAM_ERROR` 는 필요한 값이 빠진 것이고, `TEMPLATE_RENDER_ERROR` 는 SQL 을 만드는 도중
실패한 것이다.

**갈래 5 — 날짜별로 나누기 관련**

| 코드 | 뜻 |
|---|---|
| `FANOUT_REQUIRES_TEMPLATE` | 템플릿 없이 쓰려 했다 |
| `FANOUT_REQUIRES_STAGE_INSERT` | 지원하지 않는 방식에서 쓰려 했다 |
| `TASK_PARAMS_INVALID` | 지목한 이름이 형식에 안 맞거나 값 목록에 없다 |
| `TASK_PARAM_NOT_NUMERIC` | 값이 정수가 아니다 |
| `TASK_RANGE_EMPTY` / `TASK_RANGE_TOO_LARGE` | 구간이 비었거나 너무 넓다 — 기간을 조정한다 |
| `TEMPLATE_MISSING_SIGN_VAR` | 템플릿이 방향 변수를 쓰지 않는다 |

마지막 것을 왜 거절할까. **막지 않으면 각 조각이 의도보다 넓은 구간을 읽어 데이터가 조용히
중복된다.** 오류 없이 성공으로 끝나므로 알아채기 어렵다. 그래서 아예 접수하지 않는다.

**갈래 6 — 복잡한 쿼리를 거절함**

`UNSUPPORTED_JOIN`·`UNSUPPORTED_GROUP_BY`·`UNSUPPORTED_HAVING`·`UNSUPPORTED_DISTINCT`·
`UNSUPPORTED_AGGREGATE` 는 **원인이 모두 하나다.**

기본 설정이 **단순한 SELECT 만 받도록** 되어 있는데 JOIN 이나 GROUP BY 가 섞인 쿼리를 보낸 것이다.
**`strict_validation` 을 `false` 로 두면** 복잡한 쿼리도 받으면서 파티션 조건을 쿼리 어디에 있든
찾아 나눈다.

### 422 말고 다른 응답

**`429` 는 잘못이 아니라 "줄을 서 달라"는 뜻이다.** 실행 자리와 대기 줄이 모두 찼을 때 나온다.

`Retry-After` 헤더가 알려 주는 만큼(기본 5초) 기다렸다 다시 보내면 대개 통과한다. **곧바로 다시
보내면 거부만 반복되므로 반드시 이 헤더를 지킨다.** 자동 재시도를 넣는다면 앞서 다룬 중복 방지
열쇠를 함께 쓰는 편이 안전하다.

**`409` 는 두 경우에 나온다.** 이미 끝난 작업을 취소하거나 재실행하려 했을 때, 또는 같은 중복 방지
열쇠로 다른 내용을 보냈을 때다. 상태를 먼저 확인하면 어느 쪽인지 가려진다.

**`404` 는 그런 작업 번호가 없다는 뜻이다.** 다만 여기에 함정이 있다.

coordinator 를 여러 대 두었는데 **상태 저장소를 공유하지 않았다면**, 제출한 서버와 물어본 서버가
달라 **멀쩡한 작업에도 `404` 가 날 수 있다.** 이 경우는 운영자에게 저장소 공유 설정을 확인해 달라고
한다.

### 실행 중에 실패했을 때

검사를 통과한 뒤 실패하면 **HTTP 오류가 아니라 작업 상태로 나타난다.**

전체 상태 조회의 `error` 필드에 한 줄 요약이 있고, **어느 조각이 왜 실패했는지**는 조각 목록의
`error` 에 담긴다. `PARTIAL` 이면 일부만 들어간 것이므로 재실행으로 나머지를 채운다.

**방식별로 자주 나오는 실패**를 알아 두면 대처가 빠르다.

| 증상 | 원인 | 대처 |
|---|---|---|
| `local_stage` 에서 "파일 예산 초과" | 나누려는 조각 수가 목적지 서버가 감당할 수 있는 수보다 많다 | `parallelism` 을 낮춘다 |
| "gp_segment_configuration 에 없습니다" | executor 의 호스트 이름 설정이 실제와 다르다 | **운영자가 고쳐야 한다** |
| CSV 를 읽다 칸이 어긋남 | 데이터 안에 칸 구분자로 쓰는 문자가 들어 있다 | 구분자를 바꾼다 |
| `s3_stage` 의 2단계 실패 | 여러 가지 | S3 객체가 남아 있으므로 원인을 고쳐 재실행한다 |

마지막 경우에 **무엇이 남아 있는지는 뒤에서 다루는 `bin/s3-ops ls` 로 직접 볼 수 있다.**

**원인이 서버 쪽으로 보이면**(원본 접속 실패, 목적지 테이블 없음, 권한 부족 같은 것) **운영자에게
작업 번호를 알려 주는 편이 빠르다.** 서버 로그에는 모든 줄에 작업 번호가 붙어 있고 실행한 SQL 도
함께 남아 있어서, **번호 하나로 관련 기록을 전부 모을 수 있다.**

## 실수하기 쉬운 것들

처음 쓰는 사람이 가장 자주 겪는 것들을 모아 두었다.

**1. `202` 를 완료로 읽는다.** 가장 흔한 오해다. 그것은 **"받았다"는 뜻일 뿐**이므로 반드시 상태를
물어 종료를 확인해야 한다. 물어볼 때는 **가벼운 쪽**을 쓴다. 조각 목록까지 담긴 전체 조회는 작업이
끝난 뒤 한 번이나 원인을 찾을 때만 부르면 충분하다.

**2. 옮긴 데이터를 HTTP 응답으로 받으려 한다.** 데이터는 executor 가 목적지로 직접 보내므로 **응답에
담기지 않는다.** 확인은 목적지 테이블에서 해야 하고, API 가 돌려주는 것은 상태와 건수뿐이다.

**3. 결과를 보고 싶어 미리보기 API 로 이관한다.** 같은 종류의 실수다. 그쪽은 **정해진 줄 수에서
잘리므로**, 옮기는 데 쓰면 잘린 데이터가 그대로 들어간다.

**4. 나누는 수를 실제보다 크게 잡는다.** `IN` 목록의 값 개수가 곧 상한이다. 32로 줘도 값이 셋이면
**세 조각으로만 나뉜다.** 더 잘게 나누려면 나눌 값을 늘리거나 날짜별로 나누는 방식을 쓴다.

**5. 다시 돌렸을 때를 생각하지 않는다.** 덧붙이는 방식은 다시 돌린 만큼 그대로 쌓인다. 중복이
곤란하다면 **`write_mode: overwrite_partitions` 를 지원하는
방식**(`copy`·`local_stage`·`s3_stage`)을 고르고 그 값으로 제출한다.

**여기서 `stage_insert` 는 이 옵션을 아예 적용하지 않는다는 점을 특히 조심한다.**

**6. 응답을 못 받아 그냥 다시 보낸다.** 그러면 같은 작업이 두 번 만들어질 수 있다. 중복 방지 열쇠를
쓰거나, 이미 만들어졌는지 먼저 확인한다.

## API 한눈에 보기

작업 하나를 다루는 데 쓰는 것이 여섯이다.

| 무엇을 할 때 | 부를 것 |
|---|---|
| 제출한다 | `POST /jobs` → `202` 와 작업 번호 |
| **진행 상황을 반복해서 물어본다** | `GET /jobs/{job_id}/status` (가볍다) |
| 조각 목록까지 전체를 본다 | `GET /jobs/{job_id}` |
| 끝난 뒤 결과 요약을 본다 | `GET /jobs/{job_id}/result` |
| 조각 하나를 자세히 본다 | `GET /jobs/{job_id}/tasks/{task_id}` |
| 중단한다 | `POST /jobs/{job_id}/cancel` |
| 실패분만 다시 돌린다 | `POST /jobs/{job_id}/retry` |

그 밖에 작업 목록은 `GET /jobs`, 지난 기록은 `GET /history`, 쓸 수 있는 템플릿은 `GET /templates` 로
본다. 결과를 바로 받는 실행은 `POST /query-execute` 이고, 서버가 살아 있는지는 `GET /health` 와 `GET
/cluster` 로 확인한다.

**직접 눌러 보며 익히고 싶다면** `http://<coordinator>:8088/docs` 를 브라우저로 연다. 모든 API 를
화면에서 호출해 볼 수 있다.

**`http://<coordinator>:8088/`** 에 접속하면 작업 진행 상황을 보여 주는 화면이 나온다.

---

# 2장. 터미널에서 직접 다루기

이관을 맡기는 것과 **별개로**, 사람이 터미널에서 직접 두드리는 도구 셋이 함께 설치돼 있다.

**언제 쓸까.** 이관 결과가 제대로 들어갔는지 목적지 테이블을 바로 확인할 때, 원본에서 표본을 뽑아 볼
때, 중간 파일로 남은 S3 객체를 들여다보고 정리할 때다.

| 도구 | 하는 일 |
|---|---|
| `bin/gp-shell` | Greenplum 에 붙어 SQL 을 주고받는 대화형 셸 |
| `bin/impala-shell` | 같은 일을 Impala 에 대해 한다 |
| `bin/s3-ops` | S3 객체를 올리고 내리고 복사·이동·삭제하고 목록과 내용을 본다 |

## 실행 방법과 설정

이 도구들은 **어디서든 쓸 수 있게 설치된 명령이 아니라**, 설치 디렉터리 안의 스크립트를 직접 부르는
방식이다.

다만 스크립트가 자기 위치를 기준으로 최상위를 찾기 때문에 **어느 디렉터리에서 실행해도 같은 코드와
같은 설정을 읽는다.** 설치 위치가 `/data1/distributed-query-executor` 라면 어디서든
`/data1/distributed-query-executor/bin/gp-shell` 로 부르면 된다.

**접속 정보는 서비스와 같은 설정 파일에서 자동으로 읽는다.** coordinator·executor 가 쓰는
`config/config.properties` 의 `greenplum.dsn`·`impala.*`·`s3.*` 를 그대로 재사용하므로, 같은 값을 두
곳에 적어 한쪽만 고쳐 어긋나는 사고가 없다. 어느 디렉터리를 읽을지는 환경변수
`QUERY_EXECUTOR_CONFIG_DIR` 이 정하고, `--config-dir` 로 그때만 바꾸거나 `--no-config` 로 아예 읽지
않고 명령행 인자만 쓸 수도 있다. 값의 우선순위는 언제나 **명령행 인자 > 설정 파일 > 도구 기본값**
이라, `--host` 하나만 덮어 다른 클러스터에 붙여 보는 식으로 쓸 수 있다.

```bash
bin/gp-shell                                     # 설정대로 접속
bin/gp-shell --host other-gp.example.com         # 개별 값만 덮어쓰기
bin/gp-shell --config-dir /path/to/config        # 다른 설정 디렉터리
echo "SELECT 1;" | bin/gp-shell                  # 파이프로 넘겨도 된다
```

`bin/` 의 셸 래퍼는 대화형 셸을 연다. **한 번 실행하고 끝내는 배치 용도라면 모듈을 직접 부른다.**
아래 두 줄이 같은 도구의 비대화형 얼굴이고, 이 장에서 소개하는 `-q`·`-f`·`-o` 같은 옵션은 모두
이쪽에서 쓴다.

```bash
PYTHONPATH=src python -m tools.gp_query     -q "SELECT 1"
PYTHONPATH=src python -m tools.impala_query -f daily_orders.sql -V dt=2026-08-01 -o orders.csv
```

인자 없이 실행하면 사용법과 예시를 보여 준다(`--help` 와 같다). 셸만 예외라 인자 없이 실행하면
도움말 대신 바로 접속한다. 아래 예제들은 지면을 줄이려고 `PYTHONPATH=src` 를 생략한 곳이 있는데,
배포 트리의 `.venv` 파이썬으로 부를 때도 이 환경변수는 언제나 필요하다.

### 비밀번호는 어디서 오는가

**비밀번호는 명령행 인자로 받지 않는다.** `ps` 로 같은 서버의 다른 사용자에게 그대로 보이기
때문이다. 설정 파일의 값(`greenplum.dsn` 안의 비밀번호, `impala.password`), 환경변수(기본
`GP_PASSWORD`·`IMPALA_PASSWORD`, `--password-env` 로 이름 변경 가능), 대화형 입력 순으로 찾는다.
셋 다 없어도 Greenplum 은 일단 접속을 시도하는데 `.pgpass` 나 trust 인증을 쓰는 환경이 있기
때문이며, 그런 환경에서 프롬프트가 뜨는 것이 곤란하면 `--no-password-prompt` 를 준다.

준비가 됐는지는 한 줄로 확인한다.

```bash
PYTHONPATH=src python -m tools.gp_query -q "SELECT 1"
```

## 쿼리 실행하기

가장 짧은 형태는 `-q` 로 쿼리를 직접 주는 것이다. 결과가 있으면 표로 보여 준다.

```bash
python -m tools.gp_query -q "SELECT order_dt, status, count(*) FROM staging.orders GROUP BY 1, 2"
```

```
order_dt   | status   | order_cnt | amount_sum
-----------+----------+-----------+-----------
2026-08-01 | 결제완료 | 12        | 10.50
2026-08-01 | 취소     | NULL      | NULL
2행
```

`-o` 로 파일 이름을 주면 표 대신 CSV 파일로 쓴다. `--gzip` 을 함께 주면 압축해 저장한다.

```bash
python -m tools.impala_query -q "SELECT * FROM sales.orders" -o orders.csv
python -m tools.impala_query -q "SELECT * FROM sales.orders" -o orders.csv.gz --gzip
```

**표는 기본 100행까지만 보여 주고 멈춘다.** 그리고 보여줄 만큼만 받고 연결을 끊기 때문에 총 개수를
알 수 없어 `100행 이상` 이라고만 표시한다. 100행을 보려고 수백만 행을 받아오지 않으려는 것이다.
정확한 개수가 필요하면 `--max-rows 0` 으로 전부 받거나 `-o` 로 파일에 받는다. 화면에 더 보고 싶을
때는 `--max-rows 500` 처럼 숫자를 올린다.

Greenplum 쪽은 SELECT 뿐 아니라 DDL 과 DML 도 실행한다. **한 트랜잭션에서 실행하고 성공하면
커밋하며, 중간에 실패하면 전부 롤백된다.** 지우거나 바꾸는 문장이 실제로 무엇을 건드릴지 먼저 보고
싶다면 `--dry-run` 을 쓴다. 실행은 하되 **항상 롤백** 하므로 결과 행 수는 볼 수 있고 반영은 되지
않는다.

```bash
python -m tools.gp_query -q "TRUNCATE staging.orders"
python -m tools.gp_query -f load_orders.sql --var dt=2026-08-01 --dry-run
# 1,204행
# --dry-run 이므로 롤백했습니다. 반영되지 않았습니다.
```

### SQL 파일과 변수

여러 줄짜리 쿼리를 매번 따옴표 안에 넣는 것은 금방 지겨워진다. `.sql` 파일로 두고 이름만 넘기면
된다. 경로 구분자가 없는 이름을 주면 기본 SQL 디렉터리(저장소 루트의 `sql/`, `--sql-dir` 로 변경)
에서 찾고, 없는 이름을 주면 어떤 파일이 있는지 나열해 준다. 이 디렉터리는 사람이 쓰는 평범한 SQL
파일 자리라, 서버가 렌더하는 `templates/` 의 manifest 기반 템플릿과는 성격이 다르다.

```bash
python -m tools.impala_query -f daily_orders.sql -V dt=2026-08-01 -o orders.csv
```

`.sql` 파일은 Jinja 템플릿이라 `{{ 변수 }}` 자리에 `-V` 또는 `--var KEY=VALUE` 로 준 값이 들어간다.
변수는 여러 번 지정할 수 있다.

```sql
-- sql/daily_orders.sql
SELECT order_id, customer_id, amount
  FROM sales.orders
 WHERE order_dt = '{{ dt }}'
{%- if status %}
   AND status = '{{ status }}'
{%- endif %}
```

`{{ }}` 로 참조한 변수를 주지 않으면 **오류로 멈춘다.** Jinja 의 기본 동작은 빈 문자열로 조용히
채우는 것인데, 그러면 `WHERE order_dt = ''` 같은 문장이 만들어져 0건을 돌려준다. 그 편이 훨씬
위험해서 그렇게 두지 않았다. 반대로 `{% if var %}` 안에 넣은 조건은 변수를 주지 않으면 그 블록이
통째로 빠지므로, 선택적인 필터를 만들 때 쓰면 된다. 기본값을 템플릿에 적어 두고 싶다면
`{{ var | default("2026-08-01") }}` 형태를 쓴다.

값이 SQL 에 그대로 들어가므로 따옴표는 템플릿 쪽에서 책임진다. 위 예제가 `'{{ dt }}'` 처럼 따옴표를
감싸 둔 이유다. 변수가 의도대로 들어갔는지 확신이 서지 않으면 `--debug` 를 붙인다. **템플릿을 채운
뒤 실제로 서버에 보내는 SQL** 을 그대로 보여 준다.

파일 내용은 템플릿을 채운 뒤 그대로 실행한다. 문장을 쪼개거나 세미콜론을 떼어내지 않으므로 여러
줄로 이어진 쿼리도 주석도 작성한 그대로 서버에 전달되고, 윈도우 편집기가 붙이는 BOM 과 CRLF 만 읽는
단계에서 정리한다. BOM 이 남아 있으면 쿼리 첫 글자 앞에 보이지 않는 문자가 끼어 원인을 알기 어려운
문법 오류가 나기 때문이다.

### CSV 파일의 모양

기본 구분자는 **백틱**(`` ` ``)이고 값을 따옴표로 감싸지 않는다. 쉼표나 큰따옴표가 값에 섞여 있어도
그대로 나가는데, 백틱은 일반적인 텍스트에 거의 나타나지 않아 따옴표 없이도 안전하기 때문이다.
이 기본값은 서비스의 스테이징 CSV 구분자(`stage.csv_delimiter`)와도 같다.

```
order_id`name`amount`order_dt
1`김철수`10.50`2026-08-01
```

바꾸고 싶다면 `--delimiter ,` 로 쉼표를, `--delimiter $'\t'` 로 탭을 쓴다. Impala 쪽 도구에는
`--quote` 와 `--escapechar` 도 있어서 필요할 때 값을 큰따옴표로 감싸거나 이스케이프 방식을 바꿀 수
있다(Greenplum 쪽에는 없다). 헤더가 필요 없으면
`--no-header`, NULL 을 특정 문자열로 쓰려면 `--null-string '\N'`, 엑셀에서 한글이 깨지면
`--encoding utf-8-sig` 를 쓴다. Greenplum 외부 테이블로 읽힐 파일을 만드는 중이라면 그쪽 `FORMAT`
절과 맞춰야 하는데, 흔한 조합은 이렇다.

```bash
python -m tools.impala_query -f daily_orders.sql -V dt=2026-08-01 \
    -o orders.csv.gz --gzip --delimiter $'\t' --null-string '\N' --no-header
```

한 가지 주의할 것이 있다. **따옴표를 쓰지 않으면 값 안의 줄바꿈이 레코드를 깨뜨린다.** 자유 입력
텍스트처럼 줄바꿈이 들어갈 수 있는 컬럼을 뽑는다면 쿼리에서 `regexp_replace` 로 걷어내는 편이
안전하다.

## 대화형 셸

이것저것 둘러보며 작업할 때는 **셸이 훨씬 편하다.** 매번 명령을 다시 치지 않아도 되기 때문이다.
설정의 접속 정보를 그대로 쓰므로 **아무 인자 없이 바로 열린다.**

```
dw=> SELECT order_dt, status, count(*)
dw->   FROM staging.orders
dw->  GROUP BY 1, 2;
order_dt   | status   | count
-----------+----------+------
2026-08-01 | 결제완료 | 12
2행
dw=>
```

문장은 세미콜론으로 끝낸다. 여러 줄로 이어 쓰면 프롬프트가 `->` 로 바뀌고, 따옴표나 주석 안의
세미콜론은 문장 끝으로 세지 않는다. `$$ ... $$` 로 인용된 함수 본문도 통째로 넘기므로
`CREATE FUNCTION` 을 그대로 붙여 넣을 수 있다.

`\?` 를 치면 메타 명령 목록이 나온다. 자주 쓰게 되는 것은 몇 개 안 된다. `\q` 로 나가고(`Ctrl-D` 도
같다), `\dt` 로 테이블 목록을, `\d 테이블` 로 컬럼 정보를 보고, `\ddl 테이블` 로 생성문을 본다.
`\timing` 은 문장별 소요 시간 표시를 켜고 끄며, `\x` 는 컬럼이 많을 때 세로 출력으로 바꾼다.
`\o 파일.csv` 로 결과를 파일에 받다가 `\o` 만 치면 다시 화면으로 돌아온다.

`\set` 과 `\i` 를 합치면 SQL 디렉터리의 템플릿을 셸에서 그대로 쓸 수 있다. 변수를 한 번 정해 두면
`--var` 로 준 것과 같은 값이 채워진다.

```
dw=> \set dt 2026-08-01
dw=> \i order_summary.sql
```

**셸은 기본이 autocommit 이다.** 문장마다 바로 반영된다. 세션이 길어지는데 한 트랜잭션으로 묶어
두면 잠금이 계속 유지되기 때문인데, 덕분에 `VACUUM` 처럼 트랜잭션 블록 안에서 못 도는 문장도
셸에서는 실행된다. 묶고 싶으면 `\begin` 으로 시작해 `\commit` 이나 `\rollback` 으로 끝낸다.

문장 하나가 실패해도 셸은 끝나지 않는다. Greenplum 이면 실패한 트랜잭션을 정리해 주므로 이후 문장이
계속 거부되는 상황도 생기지 않는다. 오래 걸리는 문장은 `Ctrl-C` 로 취소한다. 서버에 취소를 요청하는
것이라 즉시 끝나지 않을 수 있지만 셸은 그대로 살아 있다.

`Tab` 을 누르면 메타 명령과 테이블 이름, SQL 키워드를 완성한다. **테이블 이름이 키워드보다 먼저
나온다.** 키워드는 외우고 있지만 테이블 이름은 그렇지 않기 때문이다. 테이블 목록은 처음 `Tab` 을
누를 때 한 번만 받아 두므로, 방금 만든 테이블이 안 보이면 `\dt` 를 한 번 실행한다. 목록과 함께
캐시도 갱신된다.

결과가 터미널 높이를 넘으면 페이저로 넘어간다. `$PAGER` 가 있으면 그것을, 없으면 `less -FRSX` 를
쓴다. **나갈 때는 `q`** 다. `Space` 와 `b` 로 한 화면씩 오르내리고, 컬럼이 많아 오른쪽이 잘릴 때는
좌우 화살표로 스크롤하며, `g` 와 `G` 로 맨 위와 맨 아래로 간다. `/단어` 로 검색하고 `n` 으로 다음을
찾는다. 매번 페이저를 거치는 것이 성가시면 `\pager off` 로 끈다.

다른 데서 복사해 온 긴 쿼리는 `\paste` 로 통째로 붙여 넣는 편이 안전하다(`spark-shell` 습관대로
`:paste` 라고 쳐도 된다). `Ctrl-D` 나 `\.` 만 있는 줄로 끝낸다. 붙여넣기 모드에서는 세미콜론이
없어도
되고, `\` 로 시작하는 줄이 메타 명령으로 잡히지 않는다.

파이프로도 쓴다. 터미널이 아니면 프롬프트와 히스토리를 끄고 순서대로 실행한다. 히스토리는
`~/.impala-to-whpg/` 아래에 엔진별로 남는다(디렉터리 이름은 이 도구가 유래한 저장소 이름을 그대로
쓴다). 쿼리에 값이 그대로 들어 있어서 저장소 안에 두지 않는다.

## S3 에 파일 올리고 내리기

`s3-ops` 는 `git` 처럼 **하위 명령**을 붙여 쓰는 구조다(`ls`·`upload`·`rm` 같은 것들).

**여기서 순서를 조심한다.** 접속에 관한 옵션은 모든 하위 명령에 공통이라 **하위 명령 앞에** 와야
한다.

설정에 기본 버킷이 지정돼 있으면 `s3://` 를 빼고 키만 줘도 된다.

```bash
bin/s3-ops ls       s3://dw-stage/dqe-stage/
bin/s3-ops ls       dqe-stage/job_abc123/            # 설정의 기본 버킷 사용
bin/s3-ops upload   orders.csv s3://dw-stage/orders/
bin/s3-ops upload   ./out/ s3://dw-stage/orders/2026-08-01/ --recursive
bin/s3-ops download s3://dw-stage/orders/2026-08-01/ ./out/ --recursive
bin/s3-ops cp       s3://dw-stage/orders/a.csv s3://dw-stage/archive/
bin/s3-ops mv       s3://dw-stage/orders/2026-08-01/ s3://dw-stage/archive/2026-08-01/ -r
bin/s3-ops rm       s3://dw-stage/orders/orders.csv --yes
bin/s3-ops rmdir    s3://dw-stage/dqe-stage/job_abc123/ --yes
```

올린 파일이 제대로 생겼는지 확인할 때 `head` 가 유용하다. 앞부분만 `Range` 로 받아 보여 주므로 큰
파일도 부담이 없고 `.gz` 는 알아서 푼다. **구분자와 NULL 표기, 인코딩이 의도대로인지** 여기서 바로
드러나므로, `s3_stage` 작업이 Phase 2 에서 깨졌을 때 가장 먼저 볼 자리이기도 하다.

```bash
bin/s3-ops head s3://dw-stage/dqe-stage/job_abc123/t_0.csv -n 3
```

`ls --summary` 는 목록 대신 개수와 크기 분포를 낸다. Greenplum 외부 테이블로 읽을 파일이 세그먼트
수만큼 고르게 나뉘었는지 볼 때 쓴다. `ls --dirs` 는 파일 대신 한 단계 아래 디렉터리만 보여 주므로
실행 단위가 몇 개 남았는지 훑기 좋다.

```
파일 3개, 합계 2.1KB
최소 547.0B / 평균 706.0B / 최대 888.0B
```

`cp` 와 `mv` 는 **서버측 복사** 라 파일을 받아서 다시 올리지 않는다. 큰 파일도 네트워크를 타지
않으니
적재가 끝난 prefix 를 `archive/` 로 옮겨 두는 용도로 편하다. S3 에는 이동이 없어서 `mv` 는 복사 후
원본을 지우는데, **복사가 끝난 것만 지우므로** 중간에 실패해도 아직 복사되지 않은 원본은 남는다.

오래된 것만 골라내려면 `--older-than` 을 쓴다. 단위는 `m`(분), `h`(시간), `d`(일), `w`(주)이고
단위를
빼면 시간이다. 먼저 `ls` 로 확인한 뒤 `rmdir` 로 지우는 순서를 권한다.

```bash
bin/s3-ops ls    s3://dw-stage/dqe-stage/ --older-than 7d
bin/s3-ops rmdir s3://dw-stage/dqe-stage/ --older-than 7d --yes
```

삭제는 되돌릴 수 없어서 안전장치가 몇 겹 있다. `--yes` 없이 실행하면 지울 목록을 보여 주고 물어보며,
터미널이 아니면 아예 거부한다. `-n` 또는 `--dry-run` 으로 무엇을 지울지 확인만 할 수도 있다. `rmdir`
에 prefix 가 비어 있으면(`s3://버킷/`) 버킷 전체 삭제를 막기 위해 거부한다. 다운로드도 마찬가지로
이미 있는 로컬 파일은 건너뛰며, 덮어쓰려면 `--force` 를 줘야 한다.

알아 둘 것이 하나 있다. **S3 에는 디렉터리가 없다.** 키가 `a/b/c.csv` 인 오브젝트가 있을 뿐이고
콘솔이 슬래시를 보고 폴더처럼 보여 주는 것이다. 그래서 `mkdir` 은 콘솔에서 빈 폴더로 보이게 하는 빈
오브젝트를 만들 뿐이고, 파일을 올리기 전에 상위 디렉터리를 미리 만들 필요는 없다. 반대로 `rmdir` 은
그 prefix 로 시작하는 오브젝트를 **전부** 지운다.

`exists` 는 결과를 종료 코드로 알려 줘서 셸 조건문에 바로 쓸 수 있다. 있으면 `0`, 없으면 `1`, 권한이
없으면 `5` 다. 없음과 권한 없음을 구분하는 것이 중요한데, 그래야 권한 문제를 데이터 문제로 착각하지
않는다.

```bash
if bin/s3-ops exists -q s3://dw-stage/orders/2026-08-01/_DONE; then
    echo "적재 완료 표시가 있습니다"
fi
```

8MB 가 넘는 파일은 여러 task 로 나뉘어 올라간다. 계정에 멀티파트 권한이 없으면 작은 파일은 되는데
큰 파일만 실패하는 일이 생기는데, 그때는 단일 요청으로 한 번 더 시도하므로 대개는 그냥 올라간다.
아래 메시지가 보인다면 그 경로를 탄 것이다. 5GB 가 넘는 파일에서 계속 실패한다면 운영자에게 멀티파트
권한을 요청해야 한다.

```
멀티파트 업로드가 거부되었습니다: ... (AccessDenied)
  단일 PutObject 로 다시 시도합니다. ...
```

`buckets` 로 계정의 버킷 목록을 볼 수 있지만, 이 목록을 볼 권한 없이 특정 버킷만 쓰는 계정도 흔하다.
**목록이 비어 있다고 버킷이 없는 것은 아니니** 그럴 때는 `exists` 로 개별 접근을 확인한다.

## 시간이 어디로 갔는지 읽기

작업이 끝나면 구간별 소요 시간 표가 나온다. 성공하든 실패하든 나오므로 실패했을 때 어디까지 갔는지도
알 수 있다.

```
=== 구간별 소요 시간 ===
  1. Impala 접속        0.412초    2.1%
  2. 쿼리 실행 요청     1.203초    6.1%
  3. 첫 배치 대기       8.442초   42.6%
  4. 데이터 수신        6.120초   30.9%
  5. CSV 쓰기           3.640초   18.4%
     기타              0.002초    0.0%
  ───────────────────────────────────
     합계             19.817초  100.0%
```

느리다고 느낄 때 이 표부터 본다. 첫 배치 대기가 길면 서버가 결과를 만드는 데 시간을 쓴 것이라 쿼리를
손봐야 하고, 데이터 수신이 길면 네트워크나 데이터 양의 문제이며, CSV 쓰기가 길면 로컬 디스크다. 어느
쪽인지 모른 채 쿼리만 붙잡고 있는 일을 줄여 준다. 오래 걸리는 구간에서는 진행 상황도 함께 보여 준다.

**이 보고는 전부 stderr 로 나간다.** stdout 은 조회 결과 몫이라 파이프로 넘기거나 파일로 받을 때
섞이지 않는다. 진행 상황만 끄고 싶으면 `--no-progress` 를 준다(소요 시간 요약은 그대로 나온다).

```bash
python -m tools.gp_query -q "SELECT * FROM t" > data.txt     # data.txt 에는 표만 들어간다
python -m tools.gp_query -q "SELECT * FROM t" 2> /dev/null   # 보고만 버린다
```

## 막혔을 때

**끝날 때 남기는 숫자로 원인을 가른다.** 자동 실행에 걸어 둘 때 특히 유용하다.

| 숫자 | 뜻 | 다시 시도할 가치 |
|---|---|---|
| `0` | 성공 | — |
| `1` | 대상이 없거나 취소함 | 대개 정상 |
| `2` | 인자가 잘못됨 | 없음 |
| `3` | 파이썬 패키지 없음 | **없음 — 환경 문제** |
| `4` | 접속 실패 | **없음 — 설정 문제** |
| `5` | 실행 실패나 권한 없음 | 사람이 봐야 한다 |

**`3` 이나 `4` 가 반복된다면 데이터 문제가 아니라 환경이나 접속 설정 문제다.** 몇 번을 다시 시도해도
같은 결과가 나온다.

접속이 안 되면 먼저 설정 디렉터리가 의도한 곳인지 본다. 도구는 `QUERY_EXECUTOR_CONFIG_DIR` 이나
기본 배포 경로를 읽으므로, 개발 트리와 배포 트리가 함께 있는 서버에서는 엉뚱한 설정을 읽고 있을 수
있다. `--config-dir` 로 명시해 보면 금방 갈린다.

`TSocket read 0 bytes` 나 `end of file` 은 인증 실패가 아니라 포트나 접속 방식이 서버 설정과
어긋났다는 신호다. Impala 는 21050 이 바이너리 HS2 이고 28000 은 HTTP HS2 라 `--http-transport` 가
필요하며, 21000 이나 25000 에 붙으면 핸드셰이크 도중 끊긴다. 대개 사용자가 고칠 수 있는 문제가
아니므로 운영자에게 알린다.

패키지가 없다는 오류는 메시지에 설치 명령이 함께 나오므로 그대로 따라가면 된다. `'impala' is not a
package` 라는 조금 이상한 메시지는 지금 있는 디렉터리에 `impala.py` 라는 파일이 있어 라이브러리를
가리고 있다는 뜻이므로 파일 이름을 바꾸면 해결된다.

쿼리 결과가 이상할 때는 `--debug` 로 실제로 서버에 보낸 SQL 을 확인하는 것이 가장 빠르다. 변수가
엉뚱하게 채워졌거나 조건 블록이 통째로 빠진 것이 여기서 드러난다. 결과가 0건인데 이유를 모르겠다면
템플릿 변수를 의심한다. 값을 주지 않으면 오류가 나도록 해 두었지만, `{% if %}` 블록 안의 조건은
변수를 주지 않았을 때 조용히 빠지므로 의도와 다른 쿼리가 될 수 있다.

---

# 3장. 두 축을 함께 쓰기

여기까지 읽었다면 두 갈래를 각각 쓸 수 있다. **이제 둘을 함께 쓰는 실제 흐름을 보자.**

이관은 대개 세 걸음으로 이어진다. **작업을 넣고 → 끝날 때까지 기다리고 → 목적지 테이블에서 실제로
확인한다.**

세 번째 걸음을 빠뜨리지 않는 것이 중요하다. API 가 돌려주는 건수는 **이 시스템이 센 값**이므로, 정말
몇 건이 들어갔는지는 목적지에서 세어 보는 것이 확실하다.

끝난 뒤 무엇을 해야 하는지는 **종료 상태가 정한다.**

![제출하고, 기다리고, 실제로 확인하기](images/verify-loop.svg)

```bash
# 1) 이관 작업 제출
JOB=$(curl -s localhost:8088/jobs -H 'content-type: application/json' -d @job.json \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')

# 2) 종료까지 폴링
while :; do
  S=$(curl -s localhost:8088/jobs/$JOB/status | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  case "$S" in DONE|PARTIAL|FAILED|CANCELLED) echo "종료: $S"; break;; esac
  sleep 3
done

# 3) 대상 테이블에서 직접 확인 (API 의 row count 와 대조)
PYTHONPATH=src python -m tools.gp_query -q \
  "SELECT dt, count(*) FROM public.sales_mirror WHERE dt IN ('2026-06-01','2026-06-02') GROUP BY 1 ORDER BY 1"
```

`s3_stage` 로 돌렸는데 Phase 2 에서 실패했다면 S3 에 중간 산출물이 남아 있다. 무엇이 올라갔는지
`ls --summary` 로 개수와 크기를 보고, 형식이 의심되면 `head` 로 앞부분을 열어 본다. 원인을 고쳐 다시
돌린 뒤에는 남은 prefix 를 정리한다.

```bash
bin/s3-ops ls   s3://dw-stage/dqe-stage/$JOB/ --summary
bin/s3-ops head s3://dw-stage/dqe-stage/$JOB/t_0.csv -n 3
bin/s3-ops rmdir s3://dw-stage/dqe-stage/$JOB/ --yes
```

옮기기 전에 원본이 무엇인지 확인하고 싶을 때는 소스 쪽 셸을 먼저 두드려 본다. 특히 `IN` 목록이 몇
개인지가 곧 task 수의 상한이므로, 분할이 예상대로 되는지는 이 한 줄로 미리 가늠할 수 있다.

```bash
PYTHONPATH=src python -m tools.impala_query -q \
  "SELECT dt, count(*) FROM sales WHERE dt BETWEEN '2026-06-01' AND '2026-06-04' GROUP BY dt ORDER BY dt"
```
