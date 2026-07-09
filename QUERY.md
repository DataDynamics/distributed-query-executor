# QUERY.md — 결과 반환 실행 (`POST /query-execute`)

서버에 보관된 **쿼리 템플릿**을 파라미터로 렌더해 `SELECT` 를 만들고, 지정한 데이터소스에
실행해 **결과(상위 N행)를 동기로 돌려받는** API 다. 데이터를 옮기는 이관(`POST /jobs`)과
달리 결과가 coordinator 를 거쳐 클라이언트로 반환되는 **미리보기성 실행**이다.

- 클라이언트는 SQL 전문이 아니라 **`template_id` + `params`(이름-값 항목 배열)** 만 보낸다.
- 클라이언트는 **어떤 executor 가 실행하는지 몰라도 된다** — impala/trino 소스는 coordinator 가
  `/jobs` 와 동일한 정책으로 가장 한가한 executor 를 골라 프록시한다(연결 실패 시 failover).
- 자세한 설계는 [DESIGN.md §18.7](DESIGN.md), 엔진 규약은 [DESIGN.md §18](DESIGN.md) 참고.

---

## 예제 템플릿: `order_search`

`packaging/config/templates/order_search/` 에 포함된 query-execute 전용 예제다. **WHERE 절에
지역 `IN` 목록(N개)** 과 **주문일 `BETWEEN` 날짜 구간**을 조합해 주문을 조회한다.

### 파라미터 스키마 (`manifest.yml`)

| 이름 | 타입 | 필수 | 기본값 | 설명 |
|---|---|---|---|---|
| `regions` | list | ✅ | — | `region IN (...)` 에 전개될 지역 코드 목록(N개) |
| `start_dt` | date | ✅ | — | `order_dt BETWEEN` 시작일(YYYY-MM-DD) |
| `end_dt` | date | ✅ | — | `order_dt BETWEEN` 종료일(YYYY-MM-DD) |
| `min_amount` | number | ✕ | `0` | 최소 주문금액(0 이면 조건 생략) |

### 템플릿 (`select.sql.j2`)

```sql
SELECT order_id, region, order_dt, amount
FROM orders
WHERE region IN ( {{ regions | sql_in }} )
  AND order_dt BETWEEN {{ start_dt | sql_str }} AND {{ end_dt | sql_str }}
{%- if min_amount %}
  AND amount >= {{ min_amount | sql_num }}
{%- endif %}
ORDER BY order_dt, order_id
```

파라미터는 반드시 필터를 거쳐 안전한 리터럴로 렌더된다(SQL 인젝션 방지): `regions` 는
`sql_in`(콤마 구분 리터럴, 빈 목록은 안전한 `NULL`), 날짜 경계는 `sql_str`(작은따옴표
이스케이프), `min_amount` 는 `sql_num`(숫자만 허용, 비숫자는 렌더 실패 → 422).

---

## Request JSON

```jsonc
POST /query-execute
Content-Type: application/json

{
  "template_id": "order_search",
  "params": [
    { "name": "regions",    "value": ["KR", "US", "JP"] },  // IN 목록(N개)
    { "name": "start_dt",   "value": "2026-01-01" },        // BETWEEN 시작
    { "name": "end_dt",     "value": "2026-03-31" },        // BETWEEN 종료
    { "name": "min_amount", "value": 1000 }                 // 선택(생략 가능)
  ],
  "datasource": "trino",   // 생략 시 서버 source.type. 이관은 Impala, 실행은 Trino 로 나눌 때 명시
  "limit": 100             // 반환 최대 행수(1~10000, 기본 100)
}
```

> **이관 소스와 분리**: `datasource` 를 생략하면 전역 `source.type` 을 따른다. "이관(`/jobs`)은
> Impala, query-execute 는 Trino" 처럼 나누려면 `source.type=impala` 로 두고 요청에
> `"datasource": "trino"` 를 명시한다. 이때 executor 에 `impala.*` 와 `trino.*` 접속 정보가
> **둘 다** 설정돼 있어야 한다.

### 위 요청이 렌더하는 SQL

```sql
SELECT order_id, region, order_dt, amount
FROM orders
WHERE region IN ( 'KR', 'US', 'JP' )
  AND order_dt BETWEEN '2026-01-01' AND '2026-03-31'
  AND amount >= 1000.0
ORDER BY order_dt, order_id
```

---

## Response JSON

```jsonc
{
  "template_id": "order_search",
  "datasource": "trino",
  "sql": "SELECT order_id, region, order_dt, amount FROM orders WHERE region IN ( 'KR', 'US', 'JP' ) AND order_dt BETWEEN '2026-01-01' AND '2026-03-31' AND amount >= 1000.0 ORDER BY order_dt, order_id",
  "columns": ["order_id", "region", "order_dt", "amount"],
  "rows": [
    [10001, "KR", "2026-01-03", 25000],
    [10002, "US", "2026-01-05", 18000]
  ],
  "row_count": 2,
  "truncated": false,   // limit 초과로 잘렸는지
  "limit": 100,
  "elapsed_ms": 42.7
}
```

- `columns`/`rows`/`row_count`/`truncated`/`elapsed_ms` 는 데이터소스 미리보기(`/datasources`)와
  동일한 shape 이다.
- `sql` 은 감사·재현용으로 렌더된 SELECT 를 그대로 싣는다. **어떤 executor 가 실행했는지는
  응답에 노출하지 않는다**(서버 로그에만 기록).

---

## curl 예시

```bash
curl -s localhost:8088/query-execute -H 'content-type: application/json' -d '{
  "template_id": "order_search",
  "params": [
    {"name": "regions",    "value": ["KR", "US", "JP"]},
    {"name": "start_dt",   "value": "2026-01-01"},
    {"name": "end_dt",     "value": "2026-03-31"},
    {"name": "min_amount", "value": 1000}
  ],
  "datasource": "trino",
  "limit": 100
}'
```

`min_amount` 를 빼면 금액 조건 없이 지역·날짜 구간만으로 조회한다:

```bash
curl -s localhost:8088/query-execute -H 'content-type: application/json' -d '{
  "template_id": "order_search",
  "params": [
    {"name": "regions",  "value": ["KR"]},
    {"name": "start_dt", "value": "2026-01-01"},
    {"name": "end_dt",   "value": "2026-01-31"}
  ],
  "datasource": "trino"
}'
```

---

## 오류 응답

렌더/검증 실패는 `422 + error_code`(이관 `/jobs` 와 동일 규약)로 돌려준다.

| 상황 | 상태 | error_code / detail |
|---|---|---|
| 필수 파라미터 누락(예: `end_dt`) | 422 | `TEMPLATE_PARAM_ERROR` |
| 같은 `name` 중복 | 422 | `DUPLICATE_PARAM` |
| 없는 템플릿 | 422 | `TEMPLATE_NOT_FOUND` |
| `min_amount` 에 비숫자 전달 | 422 | `TEMPLATE_RENDER_ERROR` |
| 템플릿 엔진 비활성(`template.enabled=false`) | 404 | — |
| impala/trino 인데 executor 미설정 | 400 | `executor 가 설정되지 않았습니다` |
| 데이터소스 접속/SQL 오류 | 502 | 원인 메시지 |

---

## 사용 가능한 템플릿 조회

```bash
curl -s localhost:8088/templates
# {"enabled": true, "templates": [
#   {"template_id": "order_search", "description": "주문 조회(query-execute 예제) ...",
#    "params": [{"name": "regions", "type": "list", "required": true, ...}, ...]}, ...]}
```
