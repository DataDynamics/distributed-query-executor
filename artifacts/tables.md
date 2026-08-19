# 테이블 명세서

이 문서는 이 시스템이 쓰는 표 일곱 개에 **어떤 칸이 있고 그 칸에 무엇이 들어가는지**를 하나하나 적어
둔 것이다. `config/postgresql.sql` 이라는 파일이 이 표들을 만든다.

다음 세 가지 일을 할 때 이 문서 하나만 보면 되도록 만들었다. 표를 다른 자리로 옮기거나 권한을 좁힐
때, 기록을 직접 조회해야 할 때, 그리고 얼마나 오래 보관할지 정할 때다. 그래서 칸마다 타입과 규칙,
기본값, 무엇을 담는지, 실제로 어떤 값이 들어가는지를 함께 적었다.

**먼저 하나만 분명히 해 두자.** 여기 있는 표는 옮기는 데이터가 아니라 **옮기는 일을 관리하는
데이터**다. 실제로 옮겨지는 수억 건의 행은 executor 가 원본에서 읽어 목적지 테이블로 곧장 보내므로,
여기 나오는 표를 단 한 번도 지나가지 않는다. 여기 있는 것은 "몇 번 작업이 언제 시작해 몇 건을 넣고
끝났는가" 같은 기록뿐이다.

## 읽는 법

각 표의 열이 무슨 뜻인지부터 정리해 둔다.

| 열 | 뜻 |
|---|---|
| `NO` | 그 표 안에서 칸이 놓인 순서. 실제 정의 순서와 같다 |
| `컬럼ID` | 실제 칸 이름. SQL 을 쓸 때 그대로 적는 이름이다 |
| `컬럼명` | 그 칸의 뜻을 우리말로 옮긴 이름 |
| `길이` | 길이가 정해지지 않은 타입이면 "가변", 정해진 타입이면 바이트 수 |
| `KEY` | 기본키는 `PK`, 값이 겹치면 안 되는 것은 `UQ`, 빨리 찾기 위한 것은 `IDX` |
| `NOT NULL` | `Y` 면 값이 반드시 있어야 한다. 기본키는 표기와 무관하게 언제나 `Y` 다 |

**시각을 적는 규칙**을 알아 두면 좋다. 시각을 담는 칸은 모두 **시간대 정보가 없는 한국
시각**이다. 이 시스템은 한국에서만 쓰이므로 세계 표준시로 바꿔 저장했다가 볼 때 되돌리는 절차를 두지
않았다. 데이터베이스가 자동으로 채우는 기본값도 한국 시각 기준 지금이고, 앱이 직접 넣는 값도 같은
규칙이다.

**`TEXT` 는 길이 제한이 없다.** 그래서 SQL 문 전체처럼 아주 긴 값도 잘리지 않고 그대로 들어간다.

**표 이름 앞에 붙는 `public.` 은 설정으로 바뀔 수 있다.** 기본값이 `public` 이라 이 문서도 그렇게
적었다. 이 이름을 바꾸려면 설정과 DDL 두 파일을 함께 고쳐야 한다.

마지막으로 같은 내용을 엑셀로도 두었다. `tables.xlsx` 는 표마다 시트를 하나씩 두고 여기와 같은 열을
담으며, 맨 앞 개요 시트에서 표 목록과 칸 수를 볼 수 있다. 검토 의견을 직접 달거나 사내 표준 양식에
옮겨 붙일 때 쓴다. **값을 고쳤다면 이 문서와 엑셀을 함께 갱신한다.**

---

## public.jobs

**언제 쓰나** — coordinator 를 여러 대 띄울 때 그들이 함께 보는 작업 저장소다. 설정에서
`store.backend=postgres` 로 켰을 때만 쓴다. 한 대만 쓴다면 이 표가 없어도 된다.

**왜 필요한가** — 요청이 어느 coordinator 로 가더라도 같은 작업을 조회하고 취소할 수 있어야 하기
때문이다. 그래서 작업의 모습을 통째로 여기에 저장해 둔다.

**Task 표가 따로 없다.** 작업 조각들은 이 표의 `data` 칸 안에 목록으로 담긴다. 작업의 모습을 통째로
읽고 쓰는 편이 단순하기 때문이다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `job_id` | 작업 식별자 | TEXT | 가변 | Y | PK | | `job_` 접두사에 uuid4 앞 12자를 붙인 값 | `job_3f9c2a1b7d4e` |
| 2 | `coordinator_id` | 기록한 coordinator | TEXT | 가변 | N | | | 이 행을 마지막으로 쓴 coordinator. HA 에서 고아 job 정합에 쓴다 | `coordinator-482913` |
| 3 | `status` | 작업 상태 | TEXT | 가변 | N | IDX | | PENDING · SPLITTING · RUNNING · DONE · PARTIAL · FAILED · CANCELLED | `RUNNING` |
| 4 | `cancel_requested` | 취소 요청 여부 | BOOLEAN | 1B | Y | | `FALSE` | 취소가 접수되면 true. 각 단계가 이 값을 보고 멈춘다 | `false` |
| 5 | `updated_at` | 갱신 시각 | TIMESTAMP | 8B | Y | IDX | `now() AT TIME ZONE 'Asia/Seoul'` | 이 행을 마지막으로 저장한 시각 | `2026-06-29 07:01:14.882` |
| 6 | `data` | 작업 스냅샷 | JSONB | 가변 | Y | UQ | | Job 전체 직렬화. tasks 배열까지 포함해 손실 없이 복원된다 | `{"job_id":"job_3f9c2a1b7d4e","status":"RUNNING","exec_mode":"copy","tasks":[...]}` |

빨리 찾기 위한 장치가 셋 있다. 둘(`idx_jobs_status`·`idx_jobs_updated_at`)은 목록을 뽑을 때 쓰는
평범한 것이다. 나머지 하나(`uq_jobs_idempotency_key`)가 특별한데, **중복 요청을 막는 열쇠 값에
"겹치면 안 된다"는 규칙을 걸어 둔 것**이다. 열쇠가 없는 작업은 이 규칙에서 빠지도록 조건을 달았다.
여러 coordinator 가 같은 열쇠로 동시에 요청해도 작업이 하나만 생기게 하는 마지막 방어선이다.

`data` 칸 안에는 이런 것들이 들어간다. 작업 번호(`job_id`)와 상태(`status`), 요청한
사람(`username`), 원래 SQL(`original_sql`), 목적지 테이블(`target_table`), 옮기는
방식(`exec_mode`)과 넣는 방식(`write_mode`), 중복 방지 열쇠(`idempotency_key`)와 그
지문(`request_fingerprint`), 재실행이면 원래 작업 번호(`retry_of`), 취소 요청
여부(`cancel_requested`), 그리고 조각들의 목록이다.

## public.job_history

**무엇을 담나** — coordinator 가 남기는 작업 단위 기록이다. 설정에서 기록할 데이터베이스를 지정했을
때만 남는다.

여기서 꼭 알아 둘 것이 있다. 이 표는 **덮어쓰지 않고 계속 쌓기만 한다.** 상태가 바뀔 때마다 한 줄씩
덧붙으므로 작업 하나가 여러 줄로 남는다. 그래서 지금 상태를 보려면 그 작업의 **가장 최근 줄**을
골라야 한다.

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

**무엇을 담나** — 작업 조각 하나하나의 기록이다. coordinator 가 아니라 **각 executor 가 직접 쓴다.**

그래서 놓치기 쉬운 준비물이 있다. executor 가 있는 서버에서도 그 데이터베이스에 닿을 수 있어야 한다.
닿지 못하면 작업 단위 기록은 남는데 조각 단위 기록만 비게 된다.

이 표에는 **이관이 느릴 때 어디가 문제인지 가리는 네 갈래 시간**이 담긴다. 원본에서 읽느라 쓴 시간,
다음 데이터를 기다리느라 쓴 시간, 목적지에 넣느라 쓴 시간, 마무리를 기다리느라 쓴 시간이다. 가장 큰
값이 곧 병목이다.

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

**무엇을 담나** — 각 executor 가 "나는 지금 CPU 를 얼마나 쓰고 몇 개를 돌리고 있다"를 주기적으로
스스로 적어 두는 표다. 설정에서 켰을 때만 쓴다.

**왜 이렇게 하나** — coordinator 가 executor 마다 일일이 물어보지 않아도 되기 때문이다.
coordinator 가 여러 대여도 각자 따로 물어보지 않는다.

같은 executor 는 늘 같은 줄을 고쳐 쓰므로 **줄 수가 executor 대수만큼만 유지된다.** 자라지 않는다.

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

**무엇을 담나** — coordinator 여러 대가 같은 순간에 같은 executor 를 고르는 쏠림을 줄이기 위한
예약표다. 일감을 보내는 동안 자리를 미리 잡아 둔다. 설정에서 켰을 때만 쓴다.

executor 와 coordinator 를 **잇는** 표라, 어느 한쪽만으로는 줄 하나를 특정할 수 없다. 그래서 두 칸이
함께 기본키가 된다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `executor_url` | 예약 대상 | TEXT | 가변 | Y | PK | | 자리를 잡아 둔 executor 의 URL | `http://10.0.0.11:8087` |
| 2 | `coordinator_id` | 예약한 coordinator | TEXT | 가변 | Y | PK | | 예약을 건 coordinator 식별자 | `coordinator-482913` |
| 3 | `n` | 예약 수 | INTEGER | 4B | Y | | `0` | 그 coordinator 가 이 executor 에 잡아 둔 자리 수 | `2` |
| 4 | `updated_at` | 갱신 시각 | TIMESTAMP | 8B | Y | IDX | `now() AT TIME ZONE 'Asia/Seoul'` | TTL 판정에 쓴다. 오래되면 죽은 coordinator 의 예약으로 보고 무시한다 | `2026-06-29 07:31:44.203` |

## public.coordinator_status

**무엇을 담나** — 각 coordinator 가 "나 살아 있다"를 주기적으로 남기는 표다. heartbeat 라고 부른다.

**왜 필요한가** — coordinator 하나가 죽으면 그가 쥐고 있던 작업이 영영 진행되지 않기 때문이다. 다른
coordinator 가 그걸 알아채고 대신 정리하려면 누가 죽었는지 알 방법이 필요하다.

판정 기준은 오직 **마지막으로 적은 시각이 얼마나 최근인가** 하나다. 정해진 시간 동안 갱신되지 않으면
죽은 것으로 본다.

| NO | 컬럼ID | 컬럼명 | 타입 | 길이 | NOT NULL | KEY | DEFAULT | 설명 | 예제값 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `coordinator_id` | coordinator 식별자 | TEXT | 가변 | Y | PK | | 설정값이나 자동 생성된 식별자 | `coordinator-482913` |
| 2 | `updated_at` | heartbeat 시각 | TIMESTAMP | 8B | Y | | `now() AT TIME ZONE 'Asia/Seoul'` | `coordinator.heartbeat_interval_s` 마다 갱신한다 | `2026-06-29 07:31:45.880` |

## public.executor_health_metrics

**무엇을 담나** — coordinator 가 물어본 executor 의 자원 사용량을 시간순으로 쌓아 두는 표다.
설정에서 기록할 데이터베이스를 지정했을 때만 쓰고, 정해진 주기마다 한 줄씩 덧붙는다.

앞의 `executor_status` 가 "지금 어떤가"라면 이 표는 **"어떻게 변해 왔는가"** 를 본다. 그래서
덮어쓰지 않고 계속 쌓는다.

주의할 점은 **이관을 하지 않아도 시간에 비례해 쌓인다**는 것이다. 주기가 60초이고 executor 가 3대면
하루에 4,320줄이다. 다른 표와 다른 데이터베이스에 둘 수도 있다.

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

**표 사이에 강제 규칙이 걸려 있지 않다.** 기록 표의 작업 번호는 작업 저장소의 작업 번호를
가리키지만, "반드시 있어야 한다"고 데이터베이스에 강제해 두지는 않았다. 기록은 계속 쌓기만 하는 데다
본체와 다른 데이터베이스에 있을 수도 있어서, 강제해 두면 기록이 본체를 기다리게 되고 오래된 줄을
지우기도 어려워지기 때문이다. 앞뒤를 맞추는 일은 애플리케이션이 하고, 데이터베이스는 빠르게 쌓기만
한다.

**기록 표는 "지금"이 아니라 "그때"를 담는다.** 상태가 바뀔 때마다 줄이 쌓이므로, 지금 상태를 보려면
가장 최근 줄만 골라야 한다. 화면과 API 도 같은 방식으로 읽는다.

**앱이 오래된 줄을 지워 주지 않는다.** `job_history`·`task_history`·`executor_health_metrics` 세
표는 계속 자라기만 한다. 보존 기간을 정해 지우는 일은 운영 쪽에서 걸어 두어야 한다. 반대로
`jobs`·`executor_status`·`coordinator_status`·`executor_reservation` 은 같은 줄을 고쳐 쓰므로 줄
수가 늘지 않는다.

**WarehousePG 에 둘 때는 조금 다르다.** `config/warehousepg.sql` 을 대신 적용한다. 모든 표에
데이터를 나눌 기준을 지정하고, 기록 표 셋에서는 일련번호 기본키를 뺀다. 칸 구성과 타입은 이 문서와
같다.

**고칠 때는 두 파일을 함께 고친다.** `config/postgresql.sql` 과 `config/warehousepg.sql` 이 그
둘이다. 한쪽만 고치면 어느 데이터베이스를 쓰느냐에 따라 서비스가 뜨지 않는다.
