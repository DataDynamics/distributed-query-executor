# ER 다이어그램

이 문서는 이 시스템이 무엇을 기억하고 그것을 어떤 테이블로 담는지를 그림으로 정리한 것이다. 두
단계로 나눠 그렸다. 앞의 논리 ER 은 테이블 모양을 잊고 개념과 관계만 보고, 뒤의 물리 ER 은 실제
DDL 이 만드는 테이블과 컬럼, 인덱스를 그대로 보여 준다. 물리 쪽은 저장소를 어디에 두느냐에 따라
스키마가 갈리므로 PostgreSQL 판과 WarehousePG 판을 따로 두었다.

먼저 알아 둘 것이 하나 있다. 여기 나오는 테이블은 **이관하는 데이터가 아니라 이관을 관리하는
메타데이터**다. 실제로 옮겨지는 행은 executor 가 소스에서 읽어 Greenplum 의 대상 테이블로 곧장
보내므로 이 스키마를 지나가지 않는다. 그래서 볼륨이 크지 않고, 단일 coordinator 로 쓸 때는 이
테이블들이 아예 없어도 서비스가 돈다.

그림은 `images/` 아래에 SVG 와 PNG 를 함께 둔다. 문서에는 SVG 를 넣고, SVG 를 받지 않는 곳에서는
같은 이름의 PNG(가로 두 배 해상도)를 쓴다.

---

## 논리 ER — 무엇을 기억해야 하는가

개념은 일곱이다. 요청을 받아 Job 을 만들고 소유하는 Coordinator, 요청 하나에 해당하는 Job, 그
Job 을 나눈 실행 단위인 Task, 그 Task 를 실제로 돌리는 Executor, 그리고 상태가 바뀔 때마다 쌓이는
기록인 JobHistoryEvent 와 TaskHistoryEvent, 마지막으로 여러 coordinator 가 자리를 나눠 잡기 위한
Reservation 이다.

![논리 ER — 무엇을 기억해야 하는가](images/er-logical.svg)

여기서 눈여겨볼 관계가 셋이다. 첫째로 Job 과 Task 는 1 대 N 이며, Task 하나는 반드시 Job 하나에
속한다. 파티션 값 목록을 나눈 결과가 곧 Task 이므로 Job 없이 Task 만 존재할 수 없다. 둘째로 이력은
현재 상태가 아니라 사건이다. 같은 Job 이나 Task 에 대해 상태 전이마다 한 줄씩 쌓이므로 관계가 1 대
N 이고, 지금 상태를 알고 싶으면 가장 최근 행을 골라야 한다. 셋째로 Reservation 은 Executor 와
Coordinator 를 잇는 교차 개념이라 두 식별자가 함께 키가 된다.

Task 가 개념으로는 독립인데 물리에서는 독립 테이블이 아니라는 점이 이 시스템의 특징이다. 그 이유는
바로 다음 절에서 다룬다.

## 물리 ER — PostgreSQL 메타 저장소

`config/postgresql.sql` 이 만드는 일곱 테이블이다. 앱도 기동할 때 `CREATE TABLE IF NOT EXISTS` 로
만들지만, 권한을 좁히거나 미리 만들어 두려면 이 파일을 서비스 기동 전에 적용한다.

![물리 ER — PostgreSQL 메타 저장소](images/er-physical.svg)

가장 먼저 눈에 띄는 것은 **Task 테이블이 없다**는 점이다. Job 의 스냅샷 전체가 `jobs.data` 라는
JSONB 문서 한 칸에 들어가고 Task 는 그 안의 배열로 담긴다. 이렇게 둔 이유는 저장소의 역할이 조회가
아니라 복원이기 때문이다. 어느 coordinator 가 받아도 같은 Job 을 이어서 다루려면 스냅샷을 통째로
읽고 쓰는 편이 단순하고, 조각조각 정규화해 두면 매 상태 전이마다 여러 테이블을 함께 갱신해야 한다.
대신 검색이 필요한 값만 컬럼으로 꺼내 두었다. `status` 와 `updated_at` 이 그것이고, 요청 멱등 키는
JSONB 안에 있지만 그 표현식에 부분 UNIQUE 인덱스를 걸어 여러 coordinator 가 같은 키로 동시에
제출해도 job 이 하나만 생기게 막는다.

두 번째로 알아 둘 것은 **외래키 제약이 없다**는 점이다. 그림의 점선은 값으로만 이어지는 논리 관계다.
이력 테이블은 append 전용이고 job 저장소와 다른 DSN 에 있을 수도 있어서, 제약을 걸면 기록이 본체를
기다리게 되고 이력만 따로 보관하거나 오래된 행을 지우는 일도 어려워진다. 그래서 정합은 애플리케이션이
지키고 DB 는 빠르게 쌓기만 한다.

세 번째는 테이블마다 쓰는 설정이 다르다는 점이다. `jobs` 는 `store.backend=postgres` 를 켰을 때,
`job_history` 와 `task_history`, `executor_status` 는 `history.db_dsn` 을, `executor_health_metrics`
는 `monitor.db_dsn` 을 따른다. 보통은 하나의 DSN 에 몰아 두지만, 메트릭만 다른 DB 로 뺄 수도 있다.
어느 쪽이든 앱은 테이블을 만들어 주더라도 스키마를 옮겨 주지는 않으므로, 여러 DB 를 쓴다면 그 DB 마다
같은 DDL 을 적용해야 한다.

마지막으로 `task_history` 가 유독 넓은 이유는 성능 진단 때문이다. `read_wait_ms` 와
`read_starve_ms`, `write_wait_ms`, `finalize_wait_ms` 가 task 하나의 시간을 네 갈래로 쪼개 담고,
`phases` JSONB 에 단계별 타임라인이 들어간다. 이관이 느릴 때 어느 구간이 병목인지를 이 컬럼들로
가른다.

## 물리 ER — WarehousePG(Greenplum 7) 판

메타 저장소를 WarehousePG 나 Greenplum 7 에 둘 때는 `config/warehousepg.sql` 을 대신 적용한다.
테이블과 컬럼은 같고 MPP 규칙에 맞춰 두 가지가 달라진다.

![물리 ER — WarehousePG 판](images/er-physical-wpg.svg)

하나는 모든 테이블에 분산키를 명시한다는 것이다. 지정하지 않으면 첫 컬럼이나 랜덤으로 잡혀 데이터가
한쪽으로 몰릴 수 있다. 기존 기본키가 그대로 분산키로 적합한 테이블은 그것을 쓰고, 이력과 메트릭은
`job_id` 나 `executor_url` 로 나눠 같은 job 의 행이 한 세그먼트에 모이게 한다.

다른 하나는 이력과 메트릭 세 테이블에서 대리키 기본키를 뺀 것이다. 앱이 그 `id` 를 읽지 않는데도
기본키로 두면 분산키가 `(id)` 로 강제되어 job 단위 조회와 중복 제거가 전 세그먼트로 흩어진다.
`id` 는 정렬과 디버깅에 쓸 IDENTITY 컬럼으로만 남긴다.

앱 코드는 어느 쪽이든 그대로 동작한다. 쓰는 SQL 이 `ON CONFLICT` 와 JSONB, `DISTINCT ON` 정도이고
Greenplum 7 은 PostgreSQL 12 엔진이라 모두 지원하기 때문이다. 다만 `executor_status` 와
`coordinator_status`, `executor_reservation` 은 수 초 간격의 단일 행 UPSERT 라 MPP 의 강점과는
반대되는 워크로드다. 볼륨이 작아 동작은 하지만, 성능이 중요하면 메타 저장소는 일반 PostgreSQL 에
두고 WarehousePG 는 데이터 적재 대상으로만 쓰는 편이 낫다.

## 스키마를 고칠 때

테이블이나 컬럼을 바꾸면 반드시 두 파일을 함께 고친다. `config/postgresql.sql` 과
`config/warehousepg.sql` 이 그 둘이고, 한쪽만 고치면 저장소 종류에 따라 서비스가 뜨지 않는다.
테이블 이름은 `db.schema` 설정으로 한정되므로 스키마를 옮길 때는 설정과 DDL 을 함께 맞춰야 한다.

보존 기간은 운영 쪽 몫이다. `job_history` 와 `task_history`, `executor_health_metrics` 는 계속
쌓이기만 하고 앱이 지우지 않으므로, 기간을 정해 오래된 행을 지우는 일은 따로 걸어 둔다.
