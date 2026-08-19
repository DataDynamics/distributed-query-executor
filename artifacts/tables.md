# 테이블 명세서

이 문서는 `config/postgresql.sql` 이 만드는 메타 테이블 일곱 개를 컬럼 단위로 적어 둔 것이다.
스키마를 옮기거나 권한을 좁힐 때, 이력 테이블을 직접 조회할 때, 그리고 보존 정책을 세울 때 이
문서 하나만 보면 되도록 컬럼마다 타입과 제약, 기본값, 무엇을 담는지, 실제로 어떤 값이 들어가는지를
함께 적었다.

여기 있는 테이블은 이관하는 데이터가 아니라 **이관을 관리하는 메타데이터**다. 실제로 옮겨지는 행은
executor 가 소스에서 읽어 Greenplum 의 대상 테이블로 곧장 보내므로 이 스키마를 지나가지 않는다.

## 읽는 법

`NO` 는 테이블 안에서 컬럼이 놓인 순서이고 DDL 의 정의 순서와 같다. `컬럼ID` 는 실제 컬럼
이름이고 `컬럼명` 은 그 뜻을 우리말로 옮긴 이름이다. `길이` 는 가변 길이
타입이면 "가변", 고정 길이면 바이트 수를 적었다. `KEY` 는 기본키를 `PK`, 유일 인덱스를 `UQ`,
조회용 인덱스를 `IDX` 로 표시한다. `NOT NULL` 이 `Y` 면 값이 반드시 있어야 하고, 기본키 컬럼은
표기와 무관하게 언제나 `Y` 다.

모든 시각 컬럼은 **타임존 없는 KST TIMESTAMP** 다. 이 시스템은 한국 단일 리전이라 UTC 변환을 두지
않았고, 기본값도 `now() AT TIME ZONE 'Asia/Seoul'` 로 KST 기준 현재 시각을 넣는다. 앱이 직접 넣는 시각도
같은 규칙의 문자열이다. `TEXT` 는 길이 제한을 두지 않으므로 SQL 전문처럼 긴 값도 그대로 들어간다.

테이블 이름은 `db.schema` 설정으로 한정된다. 기본값이 `public` 이라 이 문서도 `public.` 을 붙여
적었고, 스키마를 바꾸면 설정과 DDL 두 파일을 함께 고쳐야 한다.

---

## public.jobs

여러 coordinator 가 공유하는 Job 저장소다. `store.backend=postgres` 일 때만 쓰며, 어느 coordinator
로 요청이 가도 같은 job 을 조회하고 취소할 수 있도록 Job 스냅샷을 통째로 영속한다. Task 는 별도
테이블이 없고 이 테이블의 `data` 안에 배열로 담긴다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `job_id` | 작업 식별자 | TEXT | 가변 | Y | PK | | `job_` 접두사에 uuid4 앞 12자를 붙인 값 | `job_3f9c2a1b7d4e` |
| 2 | `coordinator_id` | 기록한 coordinator | TEXT | 가변 | N | | | 이 행을 마지막으로 쓴 coordinator. HA 에서 고아 job 정합에 쓴다 | `coordinator-482913` |
| 3 | `status` | 작업 상태 | TEXT | 가변 | N | IDX | | PENDING · SPLITTING · RUNNING · DONE · PARTIAL · FAILED · CANCELLED | `RUNNING` |
| 4 | `cancel_requested` | 취소 요청 여부 | BOOLEAN | 1B | Y | | `FALSE` | 취소가 접수되면 true. 각 단계가 이 값을 보고 멈춘다 | `false` |
| 5 | `updated_at` | 갱신 시각 | TIMESTAMP | 8B | Y | IDX | `now() AT TIME ZONE 'Asia/Seoul'` | 이 행을 마지막으로 저장한 시각 | `2026-06-29 07:01:14.882` |
| 6 | `data` | 작업 스냅샷 | JSONB | 가변 | Y | UQ | | Job 전체 직렬화. tasks 배열까지 포함해 손실 없이 복원된다 | `{"job_id":"job_3f9c2a1b7d4e","status":"RUNNING","exec_mode":"copy","tasks":[...]}` |

인덱스는 셋이다. `idx_jobs_status` 와 `idx_jobs_updated_at` 은 목록 조회용이고,
`uq_jobs_idempotency_key` 는 `(data->>'idempotency_key')` 표현식에 건 **부분 유일 인덱스**다. 키가
없는 job 은 인덱스에서 빠지도록 `WHERE data->>'idempotency_key' IS NOT NULL` 조건을 달았고, 이것이
여러 coordinator 가 같은 키로 동시에 제출해도 job 이 하나만 생기게 하는 마지막 방어선이다.

`data` 안에는 `job_id`·`status`·`username`·`original_sql`·`target_table`·`exec_mode`·`write_mode`·
`idempotency_key`·`request_fingerprint`·`retry_of`·`cancel_requested` 와 task 스냅샷 배열이 들어간다.

## public.job_history

coordinator 가 남기는 job 단위 실행 이력이다. `history.db_dsn` 을 설정했을 때만 기록하며, 상태가
바뀔 때마다 한 행씩 덧붙이는 append 전용 로그라 job 하나가 여러 행으로 남는다. 지금 상태를 보려면
`job_id` 별 최신 행을 골라야 한다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `id` | 일련번호 | BIGSERIAL | 8B | Y | PK | `nextval` | 대리키. 앱은 읽지 않고 정렬과 디버깅에만 쓴다 | `10482` |
| 2 | `recorded_at` | 기록 시각 | TIMESTAMP | 8B | Y | IDX | `now() AT TIME ZONE 'Asia/Seoul'` | 이 이력 행을 남긴 시각 | `2026-06-29 07:31:52.104` |
| 3 | `job_id` | 작업 식별자 | TEXT | 가변 | Y | IDX | | 대상 job. jobs.job_id 와 같은 값이지만 외래키 제약은 없다 | `job_3f9c2a1b7d4e` |
| 4 | `username` | 제출자 | TEXT | 가변 | N | | | 요청의 username. 대시보드에서 누가 낸 작업인지 보여 준다 | `etl_user` |
| 5 | `status` | 그때의 상태 | TEXT | 가변 | Y | | | 이 행을 남긴 시점의 job 상태 | `DONE` |
| 6 | `partition_column` | 분할 기준 컬럼 | TEXT | 가변 | N | | | IN 목록으로 나눈 기준 컬럼 | `dt` |
| 7 | `target_table` | 적재 대상 | TEXT | 가변 | N | | | 데이터를 넣은 Greenplum 테이블 | `public.sales_mirror` |
| 8 | `parallelism` | 요청 병렬도 | INTEGER | 4B | N | | | 요청이 지정한 분할 수(1~128) | `4` |
| 9 | `total_tasks` | 전체 task 수 | INTEGER | 4B | N | | | 실제로 만들어진 task 개수 | `4` |
| 10 | `completed_tasks` | 완료 task 수 | INTEGER | 4B | N | | | 그 시점까지 성공한 task 수 | `4` |
| 11 | `total_rows_written` | 누적 적재 행 수 | BIGINT | 8B | N | | | 모든 task 의 적재 행 수 합 | `40567` |
| 12 | `error` | 오류 메시지 | TEXT | 가변 | N | | | 실패했을 때의 한 줄 요약. 정상이면 NULL | `1개 파티션 실패` |
| 13 | `created_at` | 접수 시각 | TIMESTAMP | 8B | N | | | job 이 만들어진 시각 | `2026-06-29 07:01:11.123` |
| 14 | `started_at` | 실행 시작 시각 | TIMESTAMP | 8B | N | | | 실행 슬롯을 잡아 RUNNING 이 된 시각 | `2026-06-29 07:01:11.456` |
| 15 | `finished_at` | 종료 시각 | TIMESTAMP | 8B | N | | | 종료 상태에 도달한 시각. 진행 중이면 NULL | `2026-06-29 07:31:52.098` |
| 16 | `original_sql` | 원본 SQL | TEXT | 가변 | N | | | 분할하기 전의 SELECT 전문 | `SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-06-01','2026-06-02')` |

## public.task_history

각 executor 가 직접 남기는 task 단위 실행 이력이다. 같은 `history.db_dsn` 을 쓰므로 executor
호스트에서도 그 DB 에 닿아야 한다. 상태 전이마다 한 행씩 쌓이고, 이관이 느릴 때 어느 구간이
병목인지 가르는 네 갈래 시간이 여기에 담긴다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `id` | 일련번호 | BIGSERIAL | 8B | Y | PK | `nextval` | 대리키. 앱은 읽지 않는다 | `88213` |
| 2 | `recorded_at` | 기록 시각 | TIMESTAMP | 8B | Y | IDX | `now() AT TIME ZONE 'Asia/Seoul'` | 이 이력 행을 남긴 시각 | `2026-06-29 07:12:03.771` |
| 3 | `job_id` | 작업 식별자 | TEXT | 가변 | Y | IDX | | 이 task 가 속한 job | `job_3f9c2a1b7d4e` |
| 4 | `task_id` | task 식별자 | TEXT | 가변 | Y | IDX | | `t_` 접두사에 uuid4 앞 12자를 붙인 값 | `t_a1b2c3d4e5f6` |
| 5 | `username` | 제출자 | TEXT | 가변 | N | | | 요청의 username | `etl_user` |
| 6 | `executor_id` | 실행 executor | TEXT | 가변 | N | | | 호스트명과 포트를 이은 인스턴스 식별자 | `dqe-exec01:8087` |
| 7 | `status` | 그때의 상태 | TEXT | 가변 | Y | | | QUEUED · READING · WRITING · DONE · FAILED · CANCELLED | `WRITING` |
| 8 | `rows_written` | 적재 행 수 | BIGINT | 8B | N | | | 이 task 가 대상에 넣은 행 수 | `10120` |
| 9 | `error` | 오류 메시지 | TEXT | 가변 | N | | | 실패 사유. 정상이면 NULL | `greenplum connection refused` |
| 10 | `started_at` | 읽기 시작 시각 | TIMESTAMP | 8B | N | | | task 가 READING 으로 들어간 시각 | `2026-06-29 07:01:12.004` |
| 11 | `finished_at` | 종료 시각 | TIMESTAMP | 8B | N | | | DONE·FAILED·CANCELLED 에 도달한 시각 | `2026-06-29 07:12:03.769` |
| 12 | `sub_query` | 실행한 SELECT | TEXT | 가변 | N | | | 이 task 가 맡은 분할 쿼리 전문 | `SELECT user_id, amount, dt FROM sales WHERE dt IN ('2026-06-01')` |
| 13 | `exec_mode` | 적재 방식 | TEXT | 가변 | N | | | copy · statement · stage_insert · local_stage · s3_stage | `copy` |
| 14 | `staging_ddl` | staging 생성 DDL | TEXT | 가변 | N | | | stage_insert 계열에서 쓴 staging 테이블 생성문 | `CREATE TEMP TABLE stg_sales (user_id bigint, amount numeric, dt date)` |
| 15 | `insert_sql` | 최종 INSERT 문 | TEXT | 가변 | N | | | staging 에서 target 으로 넣은 문장 | `INSERT INTO public.sales_mirror SELECT * FROM stg_sales` |
| 16 | `rows_read` | 소스 조회 행 수 | BIGINT | 8B | N | | | 소스에서 읽어들인 행 수 | `10120` |
| 17 | `read_wait_ms` | 소스 읽기 시간 | BIGINT | 8B | N | | | 소스에서 결과를 읽는 데 쓴 누적 시간 | `8442` |
| 18 | `write_wait_ms` | 인코딩·송신 시간 | BIGINT | 8B | N | | | 값을 인코딩해 Greenplum 으로 보낸 누적 시간 | `6120` |
| 19 | `read_starve_ms` | 배치 대기 시간 | BIGINT | 8B | N | | | 쓰는 쪽이 다음 배치를 기다린 시간. 크면 소스가 병목이다 | `1320` |
| 20 | `finalize_wait_ms` | COPY 마감 대기 | BIGINT | 8B | N | | | COPY 종료와 서버 적재 완료를 기다린 시간. 크면 GP 가 병목이다 | `3640` |
| 21 | `impala_done_at` | 소스 조회 완료 시각 | TIMESTAMP | 8B | N | | | 스트리밍이 끝나 소스 조회가 확정된 시각 | `2026-06-29 07:11:58.221` |
| 22 | `phases` | 단계 타임라인 | JSONB | 가변 | N | | | 단계별 시작·종료·소요·행 수를 담은 배열 | `[{"name":"STREAM_COPY","started_at":"...","duration_ms":19817,"rows":10120}]` |

## public.executor_status

executor 가 자기 상태를 주기적으로 스스로 올리는 테이블이다. `executor.self_report=true` 일 때만
쓰며, coordinator 는 이 테이블을 읽어 대시보드와 executor 선택에 쓰므로 중복 폴링이 사라진다.
같은 `executor_id` 행을 UPSERT 하므로 행 수는 executor 대수만큼만 유지된다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `executor_id` | executor 식별자 | TEXT | 가변 | Y | PK | | 호스트명과 포트를 이은 인스턴스 식별자 | `dqe-exec01:8087` |
| 2 | `executor_url` | 광고 URL | TEXT | 가변 | N | | | `executor.advertise_url`. coordinator.executors 의 URL 과 맞춰야 부하 뷰가 이어진다 | `http://10.0.0.11:8087` |
| 3 | `cpu_percent` | CPU 사용률 | DOUBLE PRECISION | 8B | N | | | 백분율 | `37.2` |
| 4 | `memory_percent` | 메모리 사용률 | DOUBLE PRECISION | 8B | N | | | 백분율 | `48.5` |
| 5 | `memory_used_mb` | 사용 메모리 | DOUBLE PRECISION | 8B | N | | | MB 단위 | `7864.0` |
| 6 | `memory_total_mb` | 전체 메모리 | DOUBLE PRECISION | 8B | N | | | MB 단위 | `16384.0` |
| 7 | `disk_percent` | 디스크 사용률 | DOUBLE PRECISION | 8B | N | | | `monitor.disk_path` 기준 백분율 | `61.3` |
| 8 | `disk_used_gb` | 사용 디스크 | DOUBLE PRECISION | 8B | N | | | GB 단위 | `184.2` |
| 9 | `disk_total_gb` | 전체 디스크 | DOUBLE PRECISION | 8B | N | | | GB 단위 | `300.0` |
| 10 | `active_tasks` | 실행 중 task 수 | INTEGER | 4B | N | | | 지금 이 executor 가 돌리고 있는 task 수 | `3` |
| 11 | `max_concurrent_tasks` | 동시 task 상한 | INTEGER | 4B | N | | | `executor.max_concurrent_tasks` 설정값 | `8` |
| 12 | `updated_at` | 보고 시각 | TIMESTAMP | 8B | Y | | `now() AT TIME ZONE 'Asia/Seoul'` | 마지막 self-report 시각. 신선도로 생존을 판단한다 | `2026-06-29 07:31:40.512` |

## public.executor_reservation

여러 coordinator 가 같은 executor 를 동시에 고르는 쏠림을 줄이려고, 디스패치하는 동안 자리를 미리
잡아 두는 테이블이다. `coordinator.executor_reservation=true` 일 때만 쓴다. executor 와 coordinator
를 잇는 교차 테이블이라 두 컬럼이 함께 기본키가 된다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `executor_url` | 예약 대상 | TEXT | 가변 | Y | PK | | 자리를 잡아 둔 executor 의 URL | `http://10.0.0.11:8087` |
| 2 | `coordinator_id` | 예약한 coordinator | TEXT | 가변 | Y | PK | | 예약을 건 coordinator 식별자 | `coordinator-482913` |
| 3 | `n` | 예약 수 | INTEGER | 4B | Y | | `0` | 그 coordinator 가 이 executor 에 잡아 둔 자리 수 | `2` |
| 4 | `updated_at` | 갱신 시각 | TIMESTAMP | 8B | Y | IDX | `now() AT TIME ZONE 'Asia/Seoul'` | TTL 판정에 쓴다. 오래되면 죽은 coordinator 의 예약으로 보고 무시한다 | `2026-06-29 07:31:44.203` |

## public.coordinator_status

coordinator 가 자기 생존을 알리는 heartbeat 테이블이다. 신호가 끊긴 coordinator 가 쥐고 있던 job 은
다른 coordinator 가 거둬 정합할 수 있어야 하므로, 판정 기준은 오직 `updated_at` 의 신선도다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `coordinator_id` | coordinator 식별자 | TEXT | 가변 | Y | PK | | 설정값이나 자동 생성된 식별자 | `coordinator-482913` |
| 2 | `updated_at` | 생존 신호 시각 | TIMESTAMP | 8B | Y | | `now() AT TIME ZONE 'Asia/Seoul'` | `coordinator.heartbeat_interval_s` 마다 갱신한다 | `2026-06-29 07:31:45.880` |

## public.executor_health_metrics

coordinator 가 폴링한 executor 자원 사용량을 남기는 테이블이다. `monitor.db_dsn` 을 설정했을 때만
쓰고 `monitor.record_interval_s` 마다 한 행씩 덧붙인다. 살아 있는지를 넘어 추세를 보려는 용도이므로
append 전용이고, 다른 테이블과 다른 DB 에 둘 수도 있다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `id` | 일련번호 | BIGSERIAL | 8B | Y | PK | `nextval` | 대리키. 앱은 읽지 않는다 | `55021` |
| 2 | `recorded_at` | 기록 시각 | TIMESTAMP | 8B | Y | IDX | `now() AT TIME ZONE 'Asia/Seoul'` | 이 측정을 남긴 시각 | `2026-06-29 07:31:00.000` |
| 3 | `executor_url` | 대상 executor | TEXT | 가변 | Y | IDX | | 폴링한 executor 의 URL | `http://10.0.0.11:8087` |
| 4 | `healthy` | 헬스 여부 | BOOLEAN | 1B | Y | | | 헬스 체크 성공 여부. false 면 배분에서 빠진 상태다 | `true` |
| 5 | `cpu_percent` | CPU 사용률 | DOUBLE PRECISION | 8B | N | | | 백분율 | `37.2` |
| 6 | `memory_percent` | 메모리 사용률 | DOUBLE PRECISION | 8B | N | | | 백분율 | `48.5` |
| 7 | `memory_used_mb` | 사용 메모리 | DOUBLE PRECISION | 8B | N | | | MB 단위 | `7864.0` |
| 8 | `memory_total_mb` | 전체 메모리 | DOUBLE PRECISION | 8B | N | | | MB 단위 | `16384.0` |
| 9 | `disk_percent` | 디스크 사용률 | DOUBLE PRECISION | 8B | N | | | `monitor.disk_path` 기준 백분율 | `61.3` |
| 10 | `error` | 오류 메시지 | TEXT | 가변 | N | | | 폴링에 실패했을 때의 사유. 정상이면 NULL | `Connection refused` |

---

## 함께 알아 둘 것

**외래키 제약은 걸지 않는다.** `job_history.job_id` 와 `task_history.job_id` 는 `jobs.job_id` 를
가리키지만 값으로만 이어질 뿐이다. 이력은 append 전용이고 job 저장소와 다른 DSN 에 있을 수도 있어
제약을 걸면 기록이 본체를 기다리게 되고, 이력만 따로 보관하거나 오래된 행을 지우기도 어려워진다.
정합은 애플리케이션이 지키고 DB 는 빠르게 쌓기만 한다.

**이력 테이블은 현재 상태가 아니라 사건을 담는다.** 같은 job 이나 task 에 대해 상태 전이마다 행이
쌓이므로, 지금 상태를 보려면 `DISTINCT ON (job_id)` 처럼 최신 행만 골라야 한다. 대시보드와 API 도
같은 방식으로 읽는다.

**앱이 지우지 않는다.** `job_history`·`task_history`·`executor_health_metrics` 는 계속 자라기만
하므로 보존 기간을 정해 오래된 행을 지우는 일은 운영 쪽에서 걸어 둔다. 반대로 `jobs`·
`executor_status`·`coordinator_status`·`executor_reservation` 은 UPSERT 라 행 수가 대상 수만큼만
유지된다.

**WarehousePG 에 둘 때는 스키마가 조금 다르다.** `config/warehousepg.sql` 을 대신 적용하며, 모든
테이블에 분산키를 명시하고 `job_history`·`task_history`·`executor_health_metrics` 세 테이블에서는
대리키 기본키를 빼 `id` 를 IDENTITY 컬럼으로만 남긴다. 컬럼 구성과 타입은 이 문서와 같다.

**스키마를 고칠 때는 두 파일을 함께 고친다.** `config/postgresql.sql` 과 `config/warehousepg.sql`
이 그 둘이고, 한쪽만 고치면 저장소 종류에 따라 서비스가 뜨지 않는다.
