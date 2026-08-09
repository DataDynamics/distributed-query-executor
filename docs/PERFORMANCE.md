# 성능 · 확장(Scale Out) · 고가용성(HA) 가이드

이 문서는 처리량을 어떻게 늘리고(Scale Out, 수평 확장), 장애가 나도 서비스가 멈추지 않게
어떻게 견디며(HA, 고가용성), 그 과정에서 어떤 파라미터를 어떤 기준으로 잡아야 하는지를 운영자
눈높이에서 정리합니다. 값을 정하는 **근거**가 이 문서라면, 그 값을 실제로 언제 어떻게 손보는지는
[운영자 가이드](OPERATOR.md)에 있습니다. 설계 배경은 [DESIGN.md](DESIGN.md), 설정 적용 방법은
[README.md](../README.md) 와 [DEPLOY.md](DEPLOY.md) 를 참고하세요.

한 가지 약속만 기억하면 됩니다. 모든 파라미터는 `config/config.properties` 에 자바 스타일
`key=value` 로 적고, 이 값이 `config.yml` 의 `${변수:기본값}` 자리표시자를 채웁니다. 아래 표의
기본값이란 `config.yml` 에서 콜론(`:`) 뒤에 적힌 값이며, 설정을 주지 않으면 이 값이 쓰입니다.

---

## 0. 큰 그림 — 데이터 평면과 제어 평면 분리

성능을 이해하는 출발점은 하나입니다. **데이터가 coordinator 를 거치지 않는다.** 소스(Impala
커서 또는 커서 없는 커스텀 API)에서 읽은 실제 행은 각 executor 가 흘려보내고, coordinator 에게는
진행 상태와 처리한 행 수(row count)만 옵니다(제어 평면).

행이 Greenplum 으로 들어가는 경로는 `exec_mode` 에 따라 둘로 갈립니다. `copy`·`statement`·
`stage_insert` 는 executor 가 GP 에 직접 붙어 적재하고, `local_stage`·`s3_stage` 는 executor 가
스테이지(세그먼트 로컬 파일 또는 S3)에 CSV 만 떨어뜨린 뒤 **coordinator 가 외부테이블 DDL 과
target INSERT 를 중앙에서 실행**합니다. 후자에서도 행 자체는 스테이지에서 GP 세그먼트로 직접
병렬 유입되며 coordinator 는 SQL 만 보냅니다 — 데이터 평면은 여전히 coordinator 를 비껴갑니다.

데이터가 coordinator 를 통과하지 않으므로 처리량 천장은 coordinator 가 아니라 executor 의 수와 그
뒤의 Impala·Greenplum 용량으로 정해집니다. coordinator 를 아무리 키워도 처리량은 늘지 않습니다.

![0. 큰 그림 — 데이터 평면과 제어 평면 분리](images/performance-01.svg)

`(데이터)` 로 표시한 선은 모두 소스·executor·스테이지·Greenplum 사이에서만 오가고, coordinator
에 걸린 선은 전부 `(제어)` — task 디스패치와 Phase 2 SQL 뿐입니다. 결론은 둘입니다. 처리량을 늘리려면 executor 수 또는 executor 당 동시 task 수를 늘리고(Scale Out),
coordinator 는 처리량이 아니라 가용성을 위해 늘립니다(HA).

---

## 1. Scale Out (수평 확장)

확장의 축은 셋입니다. 효과가 크고 안전한 순서대로 ① executor 인스턴스 수, ② executor 한 대의
동시 task 수, ③ coordinator 수입니다. 어느 축을 키워도 전체 처리량 천장은 모든 executor 의 동시
task 수 합을 넘지 못하고, 그마저 Impala 슬롯과 Greenplum 동시 COPY 한도 안에 머물러야 합니다.

**축 1 — executor 인스턴스 추가(주력).** 가장 안전하고 확실합니다. 새 포트로 executor 를 하나 더
띄우고 그 URL 을 `coordinator.executors` 목록에 콤마로 이어 추가하면 됩니다(런처의
`EXECUTOR_PORTS` 로 여러 개를 한꺼번에 띄울 수 있음).

```properties
coordinator.executors=http://10.0.0.11:8087,http://10.0.0.11:8086,http://10.0.0.12:8087
```

한 `SELECT` 가 몇 갈래로 병렬 처리되는지는 분할된 task 수(파티션 컬럼 `IN` 값을 N등분)로 정해지고,
그 task 들이 executor 풀에 나누어 배정됩니다. executor 가 많을수록 한 job 의 task 가 더 넓게
퍼져 빨리 끝나고, executor 를 서로 다른 물리 노드에 흩어 놓으면 NIC·CPU·디스크 대역폭이 합산됩니다.

**축 2 — executor당 동시 task.** `executor.max_concurrent_tasks`(기본 8)가 한 executor 가 동시에
처리하는 task 수를 제한하는 세마포어입니다. task 하나는 Impala 읽기 + Greenplum 적재 한 묶음이며,
이 값을 올리면 노드당 처리량은 늘지만 메모리·DB 커넥션·다운스트림 부하도 비례해 함께 늘어납니다.

**축 3 — coordinator 추가.** 데이터가 coordinator 를 거치지 않으므로 목적은 처리량보다 가용성(HA)과
입구 QPS 분산입니다. 여러 대를 띄우려면 각자의 상태를 공유 PostgreSQL 로 외부화해야 하며, 이는
[2장](#2-고가용성-ha)에서 다룹니다.

한 가지 주의점이 있습니다. 과부하를 막는 admission 한도는 coordinator 인스턴스마다 각자의 메모리
안에서 관리됩니다.

> ⚠️ **admission 한도는 coordinator 인스턴스별(인메모리)** 이다. coordinator 를 2대로 늘리면
> 시스템 전체 동시 job 한도는 `max_concurrent_jobs × 2` 로 **합산**된다. 다운스트림 보호를 위해
> coordinator 수를 늘릴 땐 인스턴스별 한도를 그만큼 낮춰야 총량이 유지된다.

---

## 2. 고가용성 (HA)

coordinator 가 한 대뿐이면 그 한 대가 죽는 순간 시스템 전체가 멈춥니다(SPOF, 단일 장애점). 이를
없애려면 coordinator 를 여러 대 띄우되 각자의 상태를 공유 PostgreSQL(`history.db_dsn`)로
외부화해, 여러 coordinator 가 같은 그림을 보게 만들어야 합니다.

중앙 스케줄러는 없습니다. 대신 각 coordinator 가 공유 DB 에 적힌 부하 상황(실시간보다 살짝 늦은,
약간 stale 한 뷰)을 보고 독립적으로 결정합니다.

![2. 고가용성 (HA)](images/performance-02.svg)

두 coordinator 가 같은 공유 PostgreSQL 을 바라보고, executor 는 자기 상태를 그 DB 에 직접
보고하며(self-report/heartbeat), 어느 coordinator 든 살아 있는 executor 누구에게나 task 를 보낼 수
있는 구조입니다.

### 2.1 HA 를 켜는 최소 설정

네 가지만 설정하면 됩니다. 상태를 공유 PostgreSQL 로 옮기고(1), executor 가 자기 상태·주소를
스스로 보고하게 하고(2), coordinator 가 부하를 보고 똑똑하게 고르게 하고(3), 죽은 coordinator 의
job 을 다른 coordinator 가 수습하게(4) 만듭니다.

```properties
# 1) 공유 저장소 외부화
store.backend=postgres
history.db_dsn=postgresql://user:pass@pg-host:5432/queryexec
# 2) executor 가 자기 상태·URL 을 직접 보고(중복 폴링 제거 + URL 키 부하 뷰)
executor.self_report=true
executor.advertise_url=http://10.0.0.11:8087     # coordinator.executors 의 URL 과 일치
# 3) 헬스 기반 분산 선택(분산 스탬피드 방지)
coordinator.executor_select=p2c
coordinator.executor_health_source=auto
# 4) 죽은 coordinator 소유 job 자동 정합
coordinator.orphan_reconcile_interval_s=30
```

> 스키마는 앱이 자동 생성하지 않는다. 기동 전에 `config/postgresql.sql`
> (WarehousePG/Greenplum 7 이면 `warehousepg.sql`)을 먼저 적용한다.

### 2.2 두 종류의 장애와 대응

**(a) executor 장애.** coordinator 가 task 를 보내려다 연결이 실패하면 곧장 포기하지 않고 같은
executor 로 다시 시도합니다. 재시도 횟수는 `task_max_retries`, 간격은 지수 백오프(2배씩 증가)를
씁니다. 끝내 안 되면 `task_failover=true` 일 때만 다른 executor 로 재배정하며, 새 후보는
`executor_select` 정책(p2c 라면 살아 있는·한가한 쪽)을 따릅니다. 재배정받은 executor 는 task 를
수락해 READING → WRITING → DONE 으로 정상 진행합니다.

**(b) coordinator 장애.** 각 coordinator 는 살아 있다는 heartbeat 를 `coordinator_status` 테이블에
`heartbeat_interval_s` 마다 남깁니다. 어떤 job 의 소유 coordinator 가 `coordinator_stale_s` 를
넘도록 heartbeat 를 남기지 않아 stale 로 판단되면, 그 job 이 미완료일 경우 다른 coordinator 가
`orphan_reconcile_interval_s` 주기로 감지해 `FAILED` 로 정리(reconcile)합니다. 이렇게 정리된 job 은
이후 `retry` 로 이어 갈 수 있습니다. 상태 조회·취소는 공유 `jobs` 테이블을 근거로 처리되므로 어느
coordinator 로 라우팅되든 동일하게 응답합니다.

### 2.3 왜 P2C(Power-of-Two-Choices)인가

heartbeat 는 일정 간격으로만 갱신되므로 각 coordinator 가 보는 부하 뷰는 살짝 옛 정보(stale)입니다.
이 상태에서 단순히 "가장 한가한 노드를 고르는" `least_loaded` 를 쓰면, 모든 coordinator 가 똑같은
옛 정보를 보고 똑같은 노드 한 곳으로 일제히 몰리는 herding(떼몰림, 스탬피드)이 벌어집니다.

P2C 는 살아 있는 후보 중 무작위로 2개만 뽑아 그중 덜 바쁜 쪽을 고릅니다. 이 랜덤화가 각
coordinator 의 결정을 서로 무관하게(탈상관) 만들어 쏠림을 막고, 별도 상태나 락이 필요 없어 HA
환경에 잘 어울립니다.

여기에 `executor_reservation=true` 를 더하면 균형이 더 엄격해집니다. task 디스패치 동안 그 task 를
`executor_reservation` 테이블에 TTL 을 걸어 예약해 두면, heartbeat 갱신 전이라도 다른 coordinator 가
"실제 도는 task(`active_tasks`) + 예약분"을 실시간 부하로 보고 판단합니다. 예약은
`(executor_url, coordinator_id)` 단위로 관리되고 TTL 만료 시 사라지므로, coordinator 가 죽어도 예약이
영영 남아 부하를 부풀리는 누수가 없습니다.

| 선택 정책 | 동작 | 권장 상황 |
|---|---|---|
| `round_robin`(기본) | 순번대로. 부하 무시 | 단일 coordinator, 균질 부하 |
| `least_loaded` | 가장 한가한 노드 | **단일** coordinator + 불균질 부하 |
| `p2c` | 무작위 2개 중 덜 바쁜 쪽 | **멀티** coordinator(HA) 권장 |

---

## 3. 성능 파라미터 레퍼런스

성능 관련 파라미터를 주제별로 묶은 참고용 표 모음입니다. 필요할 때 사전처럼 들춰 보면 됩니다.

### 3.1 동시성 / Admission (3층 과부하 방어)

과부하 방어는 세 층위(L1·L2·L3)로 겹겹이 이루어집니다(층위별 그림은 [DESIGN.md §10](DESIGN.md)).
"범위" 칸은 그 한도가 coordinator 별인지 executor 별인지를 뜻합니다.

| 파라미터 | 기본 | 층위 / 의미 | 범위 |
|---|---|---|---|
| `coordinator.max_concurrent_jobs` | 16 | L1 실행 슬롯. 동시에 RUNNING 가능한 job 수. `<=0` 무제한 | coordinator별 |
| `coordinator.max_pending_jobs` | 100 | L1 대기 큐. 슬롯이 차면 PENDING 대기. **실행+대기 합 초과 → 429** | coordinator별 |
| `coordinator.max_dispatch_concurrency` | 32 | L2 한 coordinator 가 모든 job 통틀어 동시에 띄우는 task 수 | coordinator별 |
| `executor.max_concurrent_tasks` | 8 | L3 executor 한 대의 동시 task 수. `0` 무제한 | executor별 |

L1 은 job 단위 입구 통제로, 동시 RUNNING job 을 실행 슬롯으로 제한하고 슬롯이 차면 대기 큐에
PENDING 으로 세우며 실행+대기 합마저 넘으면 429 로 거절합니다. L2 는 한 coordinator 가 모든 job 을
통틀어 띄우는 task 수 상한, L3 는 executor 한 대의 동시 task 수 상한입니다.

### 3.2 폴링 / 타임아웃 / Failover

coordinator 가 task 진행을 확인하고(폴링), 응답이 없을 때 얼마나 기다리며(타임아웃), 실패 시
어떻게 재시도·재배정할지(failover)를 정하는 값들입니다.

| 파라미터 | 기본 | 의미 |
|---|---|---|
| `coordinator.poll_interval_s` | 1.0 | task 상태 폴링 간격(초). 작을수록 반응 빠름·부하↑ |
| `coordinator.task_timeout_s` | 3600 | task HTTP 전체(read) 타임아웃. 가장 긴 분할 task 예상시간보다 길게 |
| `coordinator.task_connect_timeout_s` | 5.0 | **접속** 전용 타임아웃. 죽은 executor 를 빨리 실패시켜 failover 가속 |
| `coordinator.task_max_retries` | 2 | 연결 실패 시 같은 executor 재시도 횟수(지수 백오프) |
| `coordinator.task_retry_backoff_s` | 0.5 | 백오프 기준: `backoff × 2^시도` |
| `coordinator.task_failover` | true | 재시도 소진 시 다른 executor 로 재배정 |

타임아웃은 두 종류입니다. `task_timeout_s` 는 task 전체가 끝날 때까지, `task_connect_timeout_s` 는
연결을 맺는 순간까지만 기다립니다. 후자를 짧게 두면 죽은 executor 를 빨리 가려내 failover 를 앞당깁니다.

### 3.3 HA / 선택 / 정합

2장의 HA 동작을 세밀하게 조정하는 값들입니다.

| 파라미터 | 기본 | 의미 |
|---|---|---|
| `coordinator.executor_select` | round_robin | 선택 정책: round_robin / least_loaded / p2c |
| `coordinator.executor_health_source` | auto | 부하 뷰 소스: auto(멀티=self_report, 단일=monitor) / monitor / self_report |
| `coordinator.executor_reservation` | false | 공유 TTL 예약(엄격 균형) |
| `coordinator.reservation_ttl_s` | 60 | 예약 만료(죽은 coordinator 누수 방지). heartbeat 의 수 배 |
| `coordinator.heartbeat_interval_s` | 10 | coordinator 자기 생존 heartbeat 주기 |
| `coordinator.coordinator_stale_s` | 30 | coordinator 생존 판정 임계. heartbeat 의 2~3배 |
| `coordinator.orphan_reconcile_interval_s` | 30 | 죽은 coordinator 소유 job 정합 주기. `0` 비활성 |
| `executor.self_report` | false | executor 가 자기 상태를 공유 DB 에 직접 기록 |
| `executor.status_interval_s` | 10 | self-report(heartbeat) 주기 |
| `executor.shutdown_drain_timeout_s` | 25 | SIGTERM 시 진행 중 task 완료를 기다리는 최대 시간(graceful drain) |

`shutdown_drain_timeout_s` 의 graceful drain 은 종료 신호(SIGTERM)를 받았을 때 곧바로 멈추지 않고
진행 중이던 task 가 끝날 때까지 잠시 기다리는 "부드러운 마무리"를 뜻합니다.

### 3.4 모니터링

executor 의 건강 상태와 메트릭을 얼마나 자주 확인·기록할지를 정합니다.

| 파라미터 | 기본 | 의미 |
|---|---|---|
| `monitor.enabled` | true | executor 헬스/메트릭 모니터링 |
| `monitor.health_interval_s` | 10 | 헬스 체크 주기 |
| `monitor.record_interval_s` | 60 | 메트릭 DB 기록 주기 |
| `monitor.db_dsn` | (빈값) | 메트릭 기록 DSN. 비우면 폴링만 하고 기록 생략 |

`monitor.db_dsn` 을 비워 두면 헬스 체크는 계속하되 결과를 DB 에 남기지 않습니다.

### 3.5 백엔드 처리량 (executor → Greenplum)

executor 가 Greenplum 으로 데이터를 실제로 밀어 넣을 때의 처리량 관련 값들입니다.

| 파라미터 | 기본 | 의미 |
|---|---|---|
| `greenplum.pool_max` | 0 | GP 커넥션 풀 크기(동시 GP 연결 상한). 0=`executor.max_concurrent_tasks` 와 동일 |
| `copy.batch_size` | 10000 | COPY 배치 크기(행). 클수록 처리량↑·메모리↑ |
| `copy.preflight` | true | COPY 전 컬럼 사전검증(불일치 조기 실패) |
| `copy.pipeline` | true | Impala 읽기와 GP COPY 를 별도 스레드로 겹쳐 실행(벽시계 단축) |
| `copy.queue_size` | 8 | 파이프라인 큐 크기(배치 개수). 메모리 ≈ `queue_size × batch_size` 행 |
| `copy.format` | text | COPY 포맷 `text`\|`binary`. binary 는 인코딩 CPU 절감(타입 해석 실패 시 text 폴백) |
| `impala.query_options` | (빈값) | Impala SET 전역 기본값. 예: `MEM_LIMIT=2g,REQUEST_POOL=etl` |
| `query.sql_dialect` | hive | 파싱 기본 방언(dialect). 요청에서 재정의 가능 |

### 3.6 SELECT→COPY 병목 진단·튜닝 (executor 단일 task 관점)

한 task 의 `SELECT → COPY` 가 느릴 때는 **먼저 원인을 측정하고 그다음 손댑니다.** 대시보드의 단계
타임라인(STREAM_COPY 행)과 `task_history` 컬럼이 벽시계를 네 갈래로 쪼개 보여 줍니다.

| 지표(컬럼) | 의미 | 이게 지배적이면 |
|---|---|---|
| `read_wait_ms` | 리더의 Impala `fetchmany` 순수 시간 | 참고용(아래 `read_starve` 로 병목 판단) |
| `read_starve_ms` | (파이프라인) 라이터가 **다음 배치를 기다린** 시간 | **Impala(소스)가 병목** — 읽기가 못 따라옴 |
| `write_wait_ms` | 라이터의 `write_row`(인코딩+송신) 시간 | **클라이언트 인코딩/네트워크** 병목 |
| `finalize_wait_ms` | COPY 종료(서버 ingest 완료) 대기 | **Greenplum COPY 처리**(마스터 단일 스트림) 병목 |

파이프라인 모드에서 벽시계 ≈ `read_starve + write_wait + finalize` 이므로 셋 중 가장 큰 항이 곧
병목입니다. 처방은 병목별로 다릅니다.

- **`read_starve` 가 지배하면 Impala 가 느린 것입니다.** 파티션 분할(`parallelism`)을 늘려 여러
  executor 가 서로 다른 파티션을 동시에 읽게 하는 것이 가장 효과가 큽니다. 이어서
  `copy.batch_size` 를 올려(예: 10k → 50k) fetch 왕복을 줄이고,
  `impala.query_options`(`MEM_LIMIT`, `REQUEST_POOL`)를 조정하거나 스캔 대상을 줄여 Impala 자체를
  튜닝합니다.
- **`write_wait` 가 지배하면 클라이언트의 인코딩과 전송이 병목입니다.** `copy.format=binary` 로
  텍스트 인코딩 CPU 를 줄이고(실패하면 자동으로 text 로 폴백합니다), executor 와 GP 사이 네트워크를
  점검한 뒤 `copy.batch_size` 를 올립니다.
- **`finalize_wait` 가 지배하면 Greenplum 의 COPY 처리가 병목입니다.** 한 스트림이 마스터로 몰리는
  구조라, `parallelism` 을 늘려 여러 executor 가 동시에 COPY 하게 하는 것이 가장 효과적입니다
  (동시 GP 연결은 `greenplum.pool_max` 로 조절합니다). 대상 테이블의 인덱스·트리거·분산키
  (`DISTRIBUTED BY`)도 함께 재검토합니다.

튜닝은 느린 task 하나의 STREAM_COPY 지표에서 지배 항을 찾고, 위 처방을 **하나씩** 적용해 다시
측정하는 식으로 진행합니다. `read_starve` 와 `write_wait` 가 비슷하면 이미 파이프라인이 잘 겹치는
상태이므로, 다음 수는 수평 확장(`parallelism`↑ + executor 추가, §1)입니다.

> `copy.pipeline=false` 로 두면 읽기·쓰기가 직렬 실행돼 `read_wait`/`write_wait` 가 순수 벽시계로
> 나뉩니다. 파이프라인이 의심스러울 때 원인 격리용으로 잠깐 꺼서 비교하면 유용합니다.

### 3.7 최후의 수단 — PXF 세그먼트 병렬 로딩 (COPY 마스터 병목 우회)

파이프라인·바이너리·`batch_size`·수평 확장을 다 해도 `finalize_wait`(GP 서버 ingest)가 계속
지배적이라면, 병목은 **COPY STDIN 이 Greenplum 마스터 한 노드로 몰리는 구조** 자체입니다. executor 를
아무리 늘려도 각자 마스터로 COPY 하므로 마스터가 최종 천장이 됩니다. 정석은 데이터 평면을 "우리가
밀어넣기(push COPY)"에서 "GP 가 당겨오기(pull)"로 바꾸는 것입니다.

**PXF(Platform Extension Framework)** 는 GP 의 병렬 외부 데이터 프레임워크로, 모든 세그먼트가 외부
소스(HDFS/Hive/오브젝트 스토어)를 직접 병렬로 읽어 마스터를 데이터 경로에서 빼냅니다. 이 프로젝트는
이미 `exec_mode=statement` 로 이 패턴을 **코드 변경 없이** 수용합니다.

```sql
-- 분할(splitter)은 그대로: 파티션 IN 버킷이 wrapper_query 에 채워진다.
-- SELECT 대상을 COPY 스트림이 아니라 PXF 외부 테이블로 둔다 → 세그먼트 병렬 읽기.
INSERT INTO gp_target (c1, c2, dt)
SELECT c1, c2, dt FROM pxf_ext_source
WHERE dt IN ('2026-07-01','2026-07-02');   -- ← 각 task 의 파티션 버킷
```

`exec_mode=statement`, `wrapper_query` 에 위 INSERT…SELECT(placeholder 로 파티션 IN 치환)를 두면
COPY 도 executor 를 통한 스트리밍도 전혀 없고 executor 는 SQL 제출+폴링만 합니다. 멱등성(overwrite)이
필요하면 앞에 `DELETE FROM target WHERE dt IN(...)` 를 붙이거나 stage 후 스왑합니다. 두 변형이 있습니다.

| 변형 | 방식 | 특징 |
|---|---|---|
| **A. 원본 직접** | PXF `Hive`/`hdfs:parquet` 프로파일로 Impala 원본 파일을 바로 읽기 | export 단계 없음(가장 단순). 파티션 커밋 상태가 파일로 안정적이어야 함 |
| **B. export 후 로딩** | ① Impala `INSERT OVERWRITE staging_hive_tbl SELECT …`(Parquet 병렬 쓰기) → ② PXF 로 그 경로 로딩 → ③ 정리 | 읽기·쓰기 양쪽 병렬(단일 스트림 0). 스냅샷·포맷 통제 확실하나 이동 부품 많음 |

도입 전 확인할 것은 코드보다 운영입니다. PXF 설치·구성(`pxf cluster init/start`)으로 GP 운영
의존성이 늘고, **모든 GP 세그먼트가 HDFS(NameNode/DataNode)·오브젝트 스토어에 직접 도달**해야 하므로
망분리/에어갭에선 방화벽·라우팅이 실제 관문입니다. 타입 매핑은 PXF 프로파일이 대부분 처리하며, 행
단위 진행률은 사라지지만(적재를 GP 가 함) 단계는 SUBMIT/INSERT/COMMIT 로 추적됩니다.

도입은 먼저 **코드 0줄 파일럿**(지금의 `statement` 모드로 `INSERT…SELECT FROM pxf_ext` 를 돌려 기존
COPY 경로와 처리량 비교)으로 검증하고, 효과가 확인되면 외부 테이블·export·정리 라이프사이클을
캡슐화한 `exec_mode=pxf` 를 1급 지원으로 검토하는 순서를 권합니다.

> 언제 쓰나: `finalize_wait` 가 벽이고 executor 수평 확장으로도 안 풀릴 때가 명확한 신호다. 반대로
> `read_starve`(Impala) 가 지배적이면 변형 B(Impala 병렬 export)가, 데이터량이 크지 않거나 PXF
> 설치·망 개방이 어려우면 기존 경로의 `parallelism`↑ 가 비용 대비 낫다. 구조적 한계라면
> `exec_mode=local_stage`(DESIGN §17)도 대안이다 — executor 가 GP 세그먼트 호스트 로컬 CSV 로
> export 하고 GP 가 `file://` 외부테이블로 세그먼트별 파일을 병렬 read 하므로 단일 소켓 병목이
> 사라진다(단, executor 를 GP 세그먼트 호스트에 co-locate 해야 함).

---

## 4. 값을 정하는 기준 (Sizing)

앞 장이 "어떤 파라미터가 있는가"라면 이 장은 "그 값을 얼마로 잡는가"입니다.

### 4.1 황금률 — 천장은 coordinator 가 아니라 다운스트림이다

전체 처리량 천장은 coordinator 가 아니라 그 뒤의 다운스트림(Impala·Greenplum)에서 정해집니다. 동시
처리 가능한 task 수의 실효 상한은 다음 세 값 중 최솟값입니다.

```
유효 동시 task ≈ min(
    Σ executor.max_concurrent_tasks   (= executor 수 × executor당 동시 task),
    Greenplum 이 견디는 동시 COPY 세션 수,
    Impala 동시 쿼리 슬롯(REQUEST_POOL 한도)
)
```

executor 를 아무리 많이 띄워도 Greenplum 동시 COPY 세션이나 Impala 쿼리 슬롯이 더 작으면 거기서
막힙니다. 그래서 순서를 거꾸로 잡습니다. 먼저 다운스트림이 안전하게 견디는 한도를 확정하고, 그
한도를 executor 풀에 나누어 분배합니다. coordinator 쪽(`max_dispatch_concurrency`)은 이 한도보다
넉넉히 크게 잡아 자신이 병목이 되지 않게 합니다.

### 4.2 파라미터별 산정 기준

**`executor.max_concurrent_tasks`** 는 노드 한 대 기준으로 대략
`min(코어수, 안전한 GP 동시 COPY ÷ executor 수, 메모리 ÷ task당 메모리)` 로 잡습니다. task 하나는
Impala 커넥션 하나, Greenplum 커넥션 하나, `copy.batch_size` 만큼의 버퍼를 씁니다. 메모리가 빡빡하면
이 값을 먼저 줄이는 것이 좋습니다.

**`greenplum.pool_max`** 는 executor 가 재사용하는 GP 커넥션 풀 크기로, **Greenplum 의
`max_connections` 를 직접 보호하는 손잡이**입니다. 풀이 동시 연결을 이 개수로 제한하고 유휴 연결을
재사용합니다(stage_insert 의 세션 전용 TEMP 테이블은 반납 시 `DISCARD ALL` 로 비워져 재사용이 안전).
기본값 0 이면 풀 크기가 `executor.max_concurrent_tasks` 와 같아져 "동시 task 당 GP 연결 하나"가
됩니다. 클러스터 전체 동시 GP 연결은 `Σ greenplum.pool_max` 이므로 이 합이 Greenplum 허용 세션 수를
넘지 않게 잡습니다. 동시 task 수보다 작게 두면 task 가 연결을 기다리며 throttle 되고, 크게 둬 봐야
동시 task 수가 천장이라 의미가 없습니다.

**`coordinator.max_dispatch_concurrency`** 는 모든 executor 동시 task 수 합
(`Σ executor.max_concurrent_tasks`) 이상으로 둡니다(기본 32). 너무 작으면 executor 가 노는데도
coordinator 가 task 를 충분히 못 띄워 오히려 coordinator 가 병목이 됩니다.

**`coordinator.max_concurrent_jobs`** 와 **`max_pending_jobs`** 는 입구 보호용입니다. 동시 job 수 ×
평균 분할 task 수가 앞서 구한 유효 동시 task 를 크게 넘지 않게 잡습니다. 대기 큐는 버스트를 잠시
흡수하는 완충이며, 길수록 429 거절은 줄지만 대기 지연이 늘어 오래된 요청이 쌓입니다. 멀티
coordinator 면 이 값들을 인스턴스 수만큼 나눠 총량을 맞춥니다.

> ⚠️ **커스텀 API 소스(`datasource`)의 메모리 특성은 다릅니다.** Impala 커서 경로는
> `fetchmany` 로 진짜 스트리밍이라 메모리가 배치 크기에 묶이지만, 커서 없는 커스텀 API
> (`query.func.fetch_module`)가 결과를 한 번에 돌려주면 **task 하나의 결과 전체가 executor
> 메모리에 올라갑니다**. 이때는 `parallelism` 을 늘려 task 당 파티션을 잘게 쪼개는 것이 1차
> 완화책이고, 근본 해결은 사내 API 에 페이징을 넣어 **청크를 yield** 하는 것입니다(프레임워크는
> 이미 청크 형태를 받으므로 코드 수정 없이 스트리밍으로 전환됩니다). 자세히는
> [DESIGN.md](DESIGN.md) §17.11 을 보세요.

**`copy.batch_size`** 는 처리량과 메모리·트랜잭션 크기의 줄다리기입니다. 행이 넓거나 메모리가
빠듯하면 낮추고(2000~5000), 좁고 넉넉하면 올립니다(20000 이상). 바꾼 뒤 `monitor` 메트릭으로 메모리
사용량을 보며 조정합니다.

**`task_timeout_s`** 는 가장 큰 단일 분할 task 예상 실행시간에 여유를 더해 잡습니다. 너무 짧으면
정상 task 가 타임아웃으로 실패하고, 너무 길면 hang 을 뒤늦게 감지합니다. **`task_connect_timeout_s`**
는 연결 순간만의 타임아웃이라 짧게(기본 5초) 둘수록 죽은 노드를 빨리 걸러 failover 를 앞당깁니다.

### 4.3 HA 타이밍 파라미터의 관계 (불변식)

HA 에서 타이밍 값은 반드시 지켜야 하는 순서 관계가 있습니다. 핵심은, 장애 감지·정합이 헬스 신호
갱신보다 느려야 잠깐 신호가 늦은 것을 죽음으로 오판하지 않는다는 것입니다.

```
status_interval_s  ≤  heartbeat_interval_s  ＜  coordinator_stale_s  ≤  orphan_reconcile 주기
        (10)                  (10)                    (30)
heartbeat_interval_s  ＜  reservation_ttl_s
        (10)                    (60)
```

즉 보고·heartbeat 주기는 짧게, "죽음" 판정 임계(`coordinator_stale_s`)는 그보다 넉넉히 길게, 주인
잃은 job 정리 주기는 그보다 길거나 같게, 예약 유효기간(`reservation_ttl_s`)은 heartbeat 보다 길게
둡니다. 구체적으로:

- `coordinator_stale_s` 는 `heartbeat_interval_s` 의 2~3배로 둡니다. 신호를 한두 번 놓쳐도 살아
  있다고 봐 주기 위해서이며, 너무 작으면 잠깐의 GC 나 지연만으로 멀쩡한 coordinator 의 job 을
  빼앗습니다.
- `reservation_ttl_s` 는 heartbeat 의 몇 배로 둡니다. 너무 짧으면 예약이 일찍 풀려 균형 효과가
  사라지고, 너무 길면 죽은 coordinator 의 예약이 남아 부하를 부풀려 보게 됩니다.
- 장애를 더 빨리 감지하려면 위 부등식 순서를 깨지 않은 채 관련 값들을 한 세트로 함께 줄입니다.
  하나만 줄이면 부등식이 깨져 오탐이 생깁니다.

### 4.4 튜닝 절차(권장)

1. 다운스트림 안전 한도(Greenplum 동시 COPY, Impala 풀)부터 확정한다.
2. `executor 수 × executor.max_concurrent_tasks` 가 그 한도에 맞도록 분배한다.
3. `max_dispatch_concurrency` 를 그 합 이상으로, `max_concurrent_jobs`/`max_pending_jobs` 로
   입구를 보호한다(멀티 coordinator 면 나눠서).
4. 부하를 걸고 `/metrics`·대시보드·`monitor` 메트릭(CPU/메모리/디스크, active_tasks)을 보며 병목
   지점(executor 메모리? Greenplum? coordinator 디스패치?)을 찾아 **한 번에 한 값씩** 조정한다.
5. HA 면 4.3 부등식을 깨지 않는 선에서 감지 속도를 맞춘다.

특히 4번의 "한 번에 한 값씩"을 지키세요. 여러 값을 동시에 바꾸면 어떤 변경이 효과를 냈는지 알 수
없어 튜닝이 미궁에 빠집니다.

---

## 5. 빠른 참조 — 목적별 추천 출발점

흔한 목적별로 어디서부터 손대면 좋은지입니다. 어디까지나 출발점이므로 적용 후에는 4.4 절차대로
메트릭을 보며 다듬으세요.

| 목적 | 핵심 설정 |
|---|---|
| 단일 노드 처리량 ↑ | `executor.max_concurrent_tasks` ↑, executor 인스턴스 추가 |
| 전체 처리량 ↑ | executor 노드 분산 + `max_dispatch_concurrency` 동반 ↑ |
| SELECT→COPY 병목 진단 | 타임라인 STREAM_COPY 지표(`read_starve`/`write_wait`/`finalize_wait`) → §3.6 처방 |
| COPY 인코딩(write_wait) 절감 | `copy.format=binary`(타입 해석 실패 시 text 폴백) |
| GP 마스터 COPY 병목(finalize_wait) 우회 | PXF 세그먼트 병렬 로딩(`exec_mode=statement` + PXF 외부 테이블) → §3.7 |
| 가용성(SPOF 제거) | 멀티 coordinator + `store.backend=postgres` + `self_report=true` + `executor_select=p2c` + `orphan_reconcile_interval_s>0` |
| 멀티 coordinator 부하 균형 강화 | `executor_reservation=true`(+ `reservation_ttl_s` 적정) |
| 다운스트림 보호(과부하 차단) | `max_concurrent_jobs`/`max_pending_jobs` 를 다운스트림 한도에 맞춰 하향 |
| 빠른 장애 감지 | `task_connect_timeout_s` ↓, HA 타이밍 세트(4.3) 동반 ↓ |
