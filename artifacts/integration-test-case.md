# 통합 테스트 케이스

이 문서는 **여러 구성 요소를 실제로 엮어 돌려 본 기록**이다. 함수 하나를 따로 부르는 단위 테스트와
달리, coordinator 를 그대로 띄우고 REST API 로 요청을 넣어 **검증 → 분할 → 수용 판단 → 디스패치 →
적재 → 상태 집계**까지 실제 경로를 통과시킨다.

외부 Impala·WarehousePG·S3 에는 붙지 않는다. 대신 저장소가 갖춘 Mock 백엔드가 CSV 파일과 인메모리
S3 로 **루프를 닫아** 준다. 파일을 실제로 쓰고 그 파일을 다시 읽어 대상에 넣는 데까지 확인하므로,
데이터베이스 없이도 2단계 적재가 이어지는지를 볼 수 있다.

| NO | 확인 영역 | 케이스 수 |
|---|---|---|
| 1 | 작업 실행 전 과정 | 5 |
| 2 | 템플릿 연동 | 2 |
| 3 | 요청 멱등 | 2 |
| 4 | 과부하 보호 | 1 |
| 5 | 취소 | 2 |
| 6 | 재실행 | 2 |
| 7 | 상태 조회 | 3 |
| 8 | 결과 조회 | 1 |
| 9 | 목록 조회 | 1 |
| 10 | local_stage 2단계 적재 | 1 |
| 11 | s3_stage 2단계 적재 | 1 |
| 12 | 모니터링 | 2 |
| | **합계** | **23** |

케이스는 **잘 되는 길과 막혀야 하는 길을 함께** 넣었다. 제출부터 완료까지 이어지는 정상 흐름과
더불어, 잘못된 SQL·필수 항목 누락·용량 초과·이미 끝난 작업 취소처럼 거절되어야 하는 요청도 확인
대상이다. 실제로 돌린 결과는 짝을 이루는 `integration-test-result.md` 에 있다.

## 읽는 법

| 열 | 뜻 |
|---|---|
| `NO` | 표 안에서의 순서 |
| `확인 영역` | 어느 흐름을 확인하는 케이스인지 |
| `케이스 ID` | 케이스를 가리키는 고유 번호. 결과서가 이 번호로 짝을 맞춘다 |
| `테스트 케이스명` | 무엇을 확인하는지 한 줄로 |
| `테스트 데이터` | 그 확인을 위해 넣는 요청과 설정 |
| `예상 결과` | 올바르게 동작한다면 나와야 하는 결과 |

## 테스트 케이스

| NO | 확인 영역 | 케이스 ID | 테스트 케이스명 | 테스트 데이터 | 예상 결과 |
|---|---|---|---|---|---|
| 1 | 작업 실행 전 과정 | `IT-JOB-001` | 작업을 제출하면 분할·적재를 거쳐 완료된다 | SQL=IN 값 4개 / partition_column=dt / target=public.sales_mirror / parallelism=2 | 202 로 접수되고 status=DONE, 완료 2/2, 적재 행수가 집계됨 |
| 2 | 작업 실행 전 과정 | `IT-JOB-002` | 분할 수만큼 task 가 생기고 파티션 값이 나뉜다 | 같은 SQL / parallelism=4 | task 4개, 파티션 값 4개가 중복 없이 나뉨 |
| 3 | 작업 실행 전 과정 | `IT-JOB-003` | 모의 실행은 저장하지 않고 계획만 돌려준다 | 같은 SQL / dry_run=true | 200, dry_run=true, task_count=2, 저장된 작업 0건 |
| 4 | 작업 실행 전 과정 | `IT-JOB-004` | SELECT 가 아닌 SQL 은 접수 단계에서 막힌다 | SQL=DELETE FROM sales WHERE dt IN ('d1') | 422, error_code=NOT_A_SELECT |
| 5 | 작업 실행 전 과정 | `IT-JOB-005` | 필수 필드가 없으면 접수 단계에서 막힌다 | partition_column·target_table 없이 SQL 만 전송 | 422, error_code=MISSING_REQUIRED_FIELDS |
| 6 | 템플릿 연동 | `IT-JOB-006` | 템플릿 이름과 값만 보내도 작업이 완료된다 | template_id=sales_migration / params={start_dt:2026-01-01, end_dt:2026-01-03} | 202, status=DONE, task 3개, 생성된 SQL 에 날짜가 들어감 |
| 7 | 요청 멱등 | `IT-IDM-001` | 같은 키·같은 본문은 기존 작업을 재생한다 | Idempotency-Key=IT-KEY-1 로 같은 본문 2회 전송 | 1회차 202, 2회차 200 + Idempotency-Replayed, 같은 job_id, 저장 1건 |
| 8 | 요청 멱등 | `IT-IDM-002` | 같은 키에 다른 본문이 오면 거절한다 | Idempotency-Key=IT-KEY-2 로 target_table 만 바꿔 2회 전송 | 409 거절 |
| 9 | 과부하 보호 | `IT-ADM-001` | 용량을 넘긴 요청은 즉시 거절한다 | max_concurrent_jobs=1 / max_pending_jobs=0 / 슬롯이 찬 상태에서 제출 | 429 + Retry-After 헤더 |
| 10 | 취소 | `IT-CAN-001` | 진행 중 작업을 취소하면 CANCELLED 로 바뀐다 | 실행이 끝나지 않은 작업에 POST /jobs/{id}/cancel | 200, status=CANCELLED, cancel_requested=true |
| 11 | 취소 | `IT-CAN-002` | 이미 끝난 작업은 취소할 수 없다 | 완료된(DONE) 작업에 POST /jobs/{id}/cancel | 409 거절 |
| 12 | 재실행 | `IT-RTY-001` | 실패한 task 만 새 작업으로 다시 돌린다 | task 4개 중 1개가 실패해 PARTIAL 로 끝난 작업에 POST /jobs/{id}/retry | 202, 새 job_id 발급, retry_of 가 원본을 가리키고 재실행 task 1개 |
| 13 | 재실행 | `IT-RTY-002` | 재실행할 대상이 없으면 거절한다 | 모두 성공한(DONE) 작업에 POST /jobs/{id}/retry | 409 거절 |
| 14 | 상태 조회 | `IT-QRY-001` | 진행률 조회는 태스크 목록 없이 가볍게 응답한다 | 완료된 작업에 GET /jobs/{id}/status | status=DONE, 진행률 100%, tasks 키가 없음 |
| 15 | 상태 조회 | `IT-QRY-002` | 상세 조회는 태스크와 단계 기록까지 돌려준다 | 완료된 작업에 GET /jobs/{id} | tasks 2개, 각 task 에 상태·단계 기록·적재 행수가 담김 |
| 16 | 결과 조회 | `IT-QRY-003` | 결과 조회의 조각별 합이 전체 적재 행수와 같다 | 완료된 작업에 GET /jobs/{id}/result | per_task 2건, 조각별 합계 = total_rows_written |
| 17 | 상태 조회 | `IT-QRY-004` | 없는 작업 번호는 404 로 답한다 | GET /jobs/job_nosuchjob01 | 404, detail=job not found |
| 18 | 목록 조회 | `IT-QRY-005` | 작업 목록과 용량 정보를 함께 돌려준다 | 작업 3건 제출 후 GET /jobs | 목록 3건, total=3, 실행 슬롯·대기 큐 값이 함께 담김 |
| 19 | 템플릿 연동 | `IT-QRY-006` | 쓸 수 있는 템플릿 목록을 돌려준다 | GET /templates | enabled=true, sales_migration 이 목록에 있음 |
| 20 | local_stage 2단계 적재 | `IT-STG-001` | CSV 를 만들고 그 파일을 읽어 대상에 넣는다 | SQL=IN 값 4개(다른 리터럴 없음) / exec_mode=local_stage / parallelism=4 / executor 2대 | status=DONE, CSV 4개 생성, Phase 2 가 그 파일을 읽어 4행 적재 |
| 21 | s3_stage 2단계 적재 | `IT-STG-002` | S3 에 올리고 그 객체를 읽어 넣은 뒤 정리한다 | SQL=IN 값 4개(다른 리터럴 없음) / exec_mode=s3_stage / parallelism=4 / bucket=mybkt | status=DONE, 업로드 4건, Phase 2 가 4행 적재, Phase 3 뒤 남은 객체 0건 |
| 22 | 모니터링 | `IT-MON-001` | 헬스 체크가 서비스와 버전을 돌려준다 | GET /health | status=ok, service=coordinator, version 있음 |
| 23 | 모니터링 | `IT-MON-002` | 시스템 메트릭이 CPU·메모리·디스크를 돌려준다 | GET /metrics | cpu_percent 와 memory·disk 하위 항목이 모두 채워짐 |
