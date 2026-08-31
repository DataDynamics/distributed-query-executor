# 단위 테스트 결과서

이 문서는 **중요한 기능이 제대로 도는지 하나씩 확인한 기록**이다. 단위 테스트이므로 실제 Impala 나
WarehousePG 에 붙지 않고, 각 케이스가 자기 데이터를 만들어 함수를 직접 부른다. 그래서 데이터베이스가
없어도 언제든 다시 돌릴 수 있고, 결과가 사람이나 환경에 따라 달라지지 않는다.

확인 대상은 요청 하나가 들어와 데이터가 옮겨지고 끝날 때까지 **반드시 거쳐 가는 길목**으로 골랐다.
SQL 을 검증하고 나누는 곳, 템플릿을 렌더하는 곳, 동시에 받을 양을 정하는 곳, 끝난 뒤
상태를 판정하는 곳처럼 잘못되면 조용히 틀린 결과가 나가는 자리다.

| NO | 기능 | 케이스 수 |
|---|---|---|
| 1 | SQL 검증 | 7 |
| 2 | 쿼리 분할 | 5 |
| 3 | 템플릿 엔진 | 4 |
| 4 | 동시 실행 수용 | 3 |
| 5 | 종료 상태 판정 | 4 |
| 6 | 요청 멱등 | 1 |
| 7 | 설정 로더 | 2 |
| 8 | 실행 SQL 로깅 | 2 |
| 9 | S3 스테이징 | 3 |
| 10 | JSON 안전 변환 | 1 |
| 11 | 단계 타임라인 | 1 |
| | **합계** | **33** |

**실행 환경과 시점은 다음과 같다.** 아래 결과는 이 시점에 실제로 실행해 얻은 것을 그대로 옮긴
것이며, 지어낸 값이 없다.

| 항목 | 내용 |
|---|---|
| 실행 일시 | 2026-08-31 10:28 |
| 실행 환경 | Python 3.11.15, 외부 데이터베이스 없음 |
| 실행 방법 | 각 케이스가 자체 데이터를 만들어 대상 함수를 직접 호출 |
| 판정 방법 | 케이스마다 미리 정한 합격 조건이 실제 결과에 들어 있는지로 기계가 판정 |
| 대상 | `src/coordinator`, `src/core` 의 핵심 함수 |

**결과 요약은 33건 중 성공 33건, 실패 0건이다.**

## 읽는 법

표의 열은 다음과 같다. `판정` 은 예상과 실제를 견주어 매긴 것으로, 결과서에 반드시 있어야 해서
덧붙였다.

| 열 | 뜻 |
|---|---|
| `NO` | 표 안에서의 순서 |
| `기능` | 어느 기능을 확인한 케이스인지 |
| `케이스 ID` | 케이스 문서의 같은 번호와 짝을 이룬다 |
| `테스트 케이스명` | 무엇을 확인했는지 한 줄로 |
| `테스트 데이터` | 확인을 위해 넣은 값 |
| `실제 결과` | 실행해서 실제로 나온 결과 |
| `판정` | 예상과 같으면 `성공`, 다르면 `실패` |

## 테스트 결과

| NO | 기능 | 케이스 ID | 테스트 케이스명 | 테스트 데이터 | 실제 결과 | 판정 |
|---|---|---|---|---|---|---|
| 1 | SQL 검증 | `UT-PAR-001` | 단순 SELECT 는 검증을 통과한다 | SQL=SELECT … WHERE dt IN ('d1'…'d10') AND region='KR' / partition_column=dt / strict=true | 통과, partition_column=dt, IN 값 10개 인식 | 성공 |
| 2 | SQL 검증 | `UT-PAR-002` | SELECT 가 아닌 문은 거부한다 | SQL=DELETE FROM sales WHERE dt IN ('d1') / partition_column=dt | QueryValidationError(NOT_A_SELECT) 발생 | 성공 |
| 3 | SQL 검증 | `UT-PAR-003` | 세미콜론으로 이은 다중 문은 거부한다 | SQL=SELECT … IN ('d1'); DROP TABLE sales / partition_column=dt | QueryValidationError(MULTIPLE_STATEMENTS) 발생 | 성공 |
| 4 | SQL 검증 | `UT-PAR-004` | 파티션 IN 절이 없으면 거부한다 | SQL=SELECT a FROM sales WHERE region='KR' / partition_column=dt | QueryValidationError(NO_PARTITION_IN_CLAUSE) 발생 | 성공 |
| 5 | SQL 검증 | `UT-PAR-005` | NOT IN 은 분할할 수 없으므로 거부한다 | SQL=SELECT a FROM sales WHERE dt NOT IN ('d1','d2') / partition_column=dt | QueryValidationError(NEGATED_IN) 발생 | 성공 |
| 6 | SQL 검증 | `UT-PAR-006` | 엄격 모드에서 JOIN 은 거부한다 | SQL=SELECT … FROM sales s JOIN dim d ON … WHERE dt IN ('d1') / strict=true | QueryValidationError(UNSUPPORTED_JOIN) 발생 | 성공 |
| 7 | SQL 검증 | `UT-PAR-007` | 느슨한 모드에서는 JOIN 이 있어도 통과한다 | 위와 같은 SQL / strict=false | 통과, partition_column=dt | 성공 |
| 8 | 쿼리 분할 | `UT-SPL-001` | 연속 분할은 값을 앞에서부터 균등하게 나눈다 | IN 값 10개 / parallelism=3 / strategy=contiguous | 조각 3개, 값 개수 [4, 3, 3] | 성공 |
| 9 | 쿼리 분할 | `UT-SPL-002` | 라운드로빈 분할은 값을 번갈아 나눈다 | IN 값 10개 / parallelism=3 / strategy=round_robin | 조각 3개, 첫 조각=["'d1'", "'d4'", "'d7'", "'d10'"] | 성공 |
| 10 | 쿼리 분할 | `UT-SPL-003` | 값보다 큰 분할 수는 값 개수로 줄인다 | IN 값 10개 / parallelism=50 | 조각 10개, 빈 조각 0개 | 성공 |
| 11 | 쿼리 분할 | `UT-SPL-004` | 분할해도 원문 SQL 의 앞뒤 형태를 보존한다 | IN 값 10개 / parallelism=2 | 접두 일치=True, 접미 일치=True | 성공 |
| 12 | 쿼리 분할 | `UT-SPL-005` | 래퍼 쿼리의 자리표시자에 조각이 치환된다 | sub_sql=SELECT 1 / wrapper=INSERT INTO t SELECT * FROM ({{SUBQUERY}}) x | INSERT INTO t SELECT * FROM (SELECT 1) x | 성공 |
| 13 | 템플릿 엔진 | `UT-TPL-001` | 파라미터로 SELECT 조각을 렌더한다 | template_id=sales_migration / params={start_dt:2026-01-01, end_dt:2026-01-03} | 생성됨, SELECT 로 시작=True, 날짜 3개 포함=True | 성공 |
| 14 | 템플릿 엔진 | `UT-TPL-002` | 필수 파라미터가 빠지면 거부한다 | template_id=sales_migration / params={start_dt 만 전달} | TemplateError(TEMPLATE_PARAM_ERROR) 발생 | 성공 |
| 15 | 템플릿 엔진 | `UT-TPL-003` | 없는 템플릿 이름은 거부한다 | template_id=no_such_template / params={} | TemplateError(TEMPLATE_NOT_FOUND) 발생 | 성공 |
| 16 | 템플릿 엔진 | `UT-TPL-004` | 문자열 파라미터의 작은따옴표를 이스케이프한다 | sql_str 필터 입력 = O'Brien | 'O''Brien' | 성공 |
| 17 | 동시 실행 수용 | `UT-ADM-001` | 용량은 실행 슬롯과 대기 큐의 합이다 | max_concurrent_jobs=2 / max_pending_jobs=3 | capacity=5 | 성공 |
| 18 | 동시 실행 수용 | `UT-ADM-002` | 용량을 넘으면 수용하지 않는다 | max_concurrent_jobs=2 / max_pending_jobs=0 / try_admit 3회 | [True, True, False] | 성공 |
| 19 | 동시 실행 수용 | `UT-ADM-003` | 슬롯을 반납하면 다시 수용한다 | max_concurrent_jobs=1 / try_admit → try_admit → release → try_admit | [True, False], 반납 후 True | 성공 |
| 20 | 종료 상태 판정 | `UT-FIN-001` | 모든 task 가 성공하면 DONE 이다 | task 3개 모두 DONE / failure_policy=fail_fast | status=DONE | 성공 |
| 21 | 종료 상태 판정 | `UT-FIN-002` | 일부 실패 + best_effort 는 PARTIAL 이다 | task 3개 중 1개 FAILED / failure_policy=best_effort / exec_mode=copy | status=PARTIAL | 성공 |
| 22 | 종료 상태 판정 | `UT-FIN-003` | 일부 실패 + fail_fast 는 FAILED 이다 | task 3개 중 1개 FAILED / failure_policy=fail_fast | status=FAILED, error='t_5ad899373ff8: 적재 실패' | 성공 |
| 23 | 종료 상태 판정 | `UT-FIN-004` | 취소 요청은 실패보다 우선한다 | task 3개 중 1개 FAILED / cancel_requested=true | status=CANCELLED | 성공 |
| 24 | 요청 멱등 | `UT-STO-001` | 같은 멱등 키로 두 번 등록하면 하나만 생성된다 | Idempotency-Key=KEY-1 로 job A 등록 후 job B 등록 | 1회차 선점=성공, 2회차 반환=job_aaaaaaaaaaaa, 저장 건수=1 | 성공 |
| 25 | 설정 로더 | `UT-CFG-001` | properties 값으로 자리표시자를 치환한다 | properties={coordinator.port: 9999} / yaml={port: ${coordinator.port:8088}} | port='9999' (str) | 성공 |
| 26 | 설정 로더 | `UT-CFG-002` | properties 에 값이 없으면 기본값을 쓴다 | properties={} / yaml={port: ${coordinator.port:8088}} | port='8088' (str) | 성공 |
| 27 | 실행 SQL 로깅 | `UT-LOG-001` | 여러 줄 SQL 을 한 줄로 접는다 | SQL='SELECT a\n  FROM t\n WHERE x = 1' | SELECT a FROM t WHERE x = 1 | 성공 |
| 28 | 실행 SQL 로깅 | `UT-LOG-002` | 긴 SQL 은 자르고 잘렸음을 표시한다 | SQL=60자 문자열 / max_length=20 | AAAAAAAAAAAAAAAAAAAA… (총 60자 중 40자 절단) | 성공 |
| 29 | S3 스테이징 | `UT-S3-001` | S3 객체 키를 job·task 기준으로 만든다 | prefix=stage / job_id=job_abc / task_id=t_001 | stage/job_abc/t_001.csv | 성공 |
| 30 | S3 스테이징 | `UT-S3-002` | 외부테이블 이름을 스키마로 한정한다 | job_id=job_abc / s3.external_schema=dwtemp | dwtemp.s3ext_job_abc | 성공 |
| 31 | S3 스테이징 | `UT-S3-003` | 멱등 선삭제 SQL 을 만든다 | target=public.sales / partition_column=dt / values=['2026-01-01','2026-01-02'] | DELETE FROM public.sales WHERE dt IN ('2026-01-01', '2026-01-02') | 성공 |
| 32 | JSON 안전 변환 | `UT-JSN-001` | 표준 JSON 에 없는 값은 null 로 떨군다 | 값 = float('nan') | None | 성공 |
| 33 | 단계 타임라인 | `UT-PHS-001` | 단계 시작과 종료를 기록하고 소요를 계산한다 | phases=[] / STREAM_COPY start → end(rows=1000) | 단계 1건, name=STREAM_COPY, rows=1000, duration_ms=채워짐 | 성공 |
