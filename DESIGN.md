# Distributed Query Executor — 설계 문서

> Coordinator + N Executor 구조로 하나의 **Impala `SELECT` 쿼리**를
> 파티션 컬럼의 `IN` 조건 기준으로 N분할하여 병렬로 읽고,
> 그 결과를 **Greenplum 테이블에 적재**하는 데이터 이관 API.

> 외부 애플리케이션(예: C#)에서 이 API 를 호출해 작업을 실행하고 완료·에러를 확인하는
> 구체적 방법과 JSON 예시는 [INTEGRATION.md](INTEGRATION.md) 를 참고하세요.

---

## 1. 개요 (Overview)

이 시스템은 한마디로 "큰 조회 한 건을 여러 일꾼에게 나눠 시키는 데이터 이관기"입니다. 처음 접하는 분을 위해 먼저 큰 그림부터 그려 보겠습니다.

우리가 다루는 데이터는 Impala라는 분석용 데이터베이스 안에 들어 있고, 이것을 Greenplum이라는 또 다른 데이터베이스로 옮기는 것이 이 시스템의 일입니다(이 옮기는 작업을 **Impala → Greenplum 이관**이라고 부릅니다). 그런데 데이터가 워낙 크기 때문에 한 대가 통째로 읽으면 너무 느립니다. 그래서 이 시스템은 하나의 큰 Impala `SELECT` 문을 여러 일꾼(executor)에게 나누어 주고, 각 일꾼이 자기 몫을 병렬로 읽은 다음 자기가 읽은 데이터를 직접 Greenplum에 적재하도록 만들었습니다.

그렇다면 일을 "어떤 기준으로" 나눌까요? 바로 쿼리의 `WHERE <partition_column> IN (v1, v2, ...)` 조건에 들어 있는 IN 값 리스트를 N등분합니다. 여기서 **파티션 컬럼(partition column)**이란 데이터를 날짜나 지역처럼 일정한 구간으로 나누어 저장할 때 그 기준이 되는 컬럼을 말합니다. 예를 들어 날짜 100일치를 IN으로 나열했다면, 그 100개의 날짜를 네 묶음으로 쪼개 네 일꾼에게 25일씩 맡기는 식입니다.

아래는 이 개요에서 기억해 둘 만한 핵심 설정값들입니다. 지금 다 외울 필요는 없고, 뒤 절에서 하나씩 다시 만나게 됩니다.

- **목적**: 하나의 큰 Impala `SELECT`를 여러 executor로 나누어 병렬로 읽고, 각 executor가 자신이 읽은 데이터를 Greenplum에 적재한다. (**Impala → Greenplum 이관**)
- **분할 기준**: `WHERE <partition_column> IN (v1, v2, ...)` 의 IN 값 리스트를 N등분.
- **소스(read) 방언**: 기본 **Impala**(`sqlglot`의 `hive` 방언). 요청에서 `sql_dialect`로 재정의 가능(`impala`, `postgres` 등).
- **타깃(write)**: **Greenplum**(PostgreSQL 기반) → psycopg `COPY` 또는 INSERT.
- **검증 범위**: 기본은 단순 `SELECT`(strict). `strict_validation=false`로 **JOIN·서브쿼리·GROUP BY 등 복합 쿼리**도 허용(파티션 `IN` 절을 트리 어디서든 탐색).
- **적재 방식**(`exec_mode`): `copy`(기본) / `statement` / `stage_insert` 세 가지.
- **응답 모델**: Job 기반 **비동기** API (job_id 발급 후 polling).
- **동시성**: 입구의 admission control(동시 job 슬롯 + 대기 큐)과 디스패치/executor 단의 task 동시성 상한으로 다층 제어.
- **운영 형태**: 단일 또는 **멀티 coordinator**(공유 PostgreSQL), executor **N대**. 별도 executor 없이 검증하는 **local 모드**도 지원.

위 항목 중 "방언(dialect)"이라는 말이 낯설 수 있는데, 이것은 같은 SQL이라도 데이터베이스마다 문법이 조금씩 다르기 때문에 어느 데이터베이스의 문법으로 해석할지를 정해 주는 설정입니다. 또 "비동기 API"란 요청을 보내자마자 결과가 바로 나오는 것이 아니라, 먼저 접수증(job_id)을 받아 두고 나중에 그 번호로 진행 상황을 물어보는(polling) 방식을 뜻합니다.

### 핵심 특징

이 시스템을 다른 흔한 데이터 파이프라인과 구별 짓는 네 가지 성질이 있습니다. 각각이 왜 그런지 함께 풀어 보겠습니다.

1. **결과 병합 불필요**: 각 executor가 서로 다른 파티션 값 집합을 담당해 Greenplum에 **독립적으로 적재**한다. 행이 겹치지 않으므로 merge가 없다. Job의 "결과"는 적재된 **row count 집계 + 상태**다. 다시 말해, 일꾼들이 맡은 날짜 묶음이 서로 겹치지 않게 나눴기 때문에, 나중에 결과를 합쳐서 중복을 제거하는 골치 아픈 과정이 아예 필요 없습니다.
2. **양방향 상태 추적**: Coordinator와 Executor **모두** 자신의 작업이 진행 중인지/완료인지 안다. 두 계층 모두 PostgreSQL 이력에 기록한다. 즉 지휘자도 일꾼도 각자 "내가 지금 무엇을 하고 있는지"를 알고 있고, 그 기록을 공통의 장부에 남깁니다.
3. **쿼리문 보관**: Coordinator는 원본 SQL과 각 executor에게 보낸 **sub-query 전문(全文)** 을 Job 안에 보관한다(감사·디버깅·대시보드 표시). 나중에 "이 일꾼은 정확히 어떤 쿼리를 받았지?"를 따져 볼 수 있도록 보낸 쿼리를 통째로 저장해 둔다는 뜻입니다.
4. **데이터는 coordinator를 거치지 않는다**: executor가 Impala→Greenplum로 직접 흘려보내고, coordinator로는 **상태와 row count만** 흐른다. 대량의 데이터가 지휘자 한 곳으로 몰리면 그곳이 병목이 되므로, 실제 데이터는 일꾼이 직접 목적지로 보내고 지휘자에게는 "몇 행 넣었는지"와 "성공/실패" 같은 가벼운 정보만 올려보냅니다.

---

## 2. 전체 아키텍처

앞 절에서 말한 "지휘자"가 바로 **coordinator**이고, "일꾼"이 **executor**입니다. coordinator는 요청을 받는 창구이자 전체를 지휘하는 한 대의 서비스이고, executor는 실제로 데이터를 읽고 적재하는 여러 대의 일꾼 서비스입니다. 아래 다이어그램은 이 둘과 주변 데이터베이스들이 어떻게 연결되는지를 한눈에 보여 줍니다. 화살표에 붙은 동그라미 번호(①~⑤)가 일이 흘러가는 순서입니다.

```mermaid
flowchart TB
    Client([Client])
    Impala[(Impala<br/>source)]
    GP[(Greenplum<br/>target)]
    PG[(PostgreSQL<br/>이력·공유 store·상태·메트릭)]

    subgraph Coordinator["Coordinator (FastAPI)"]
        direction TB
        API["REST API + 대시보드(/)"]
        Parser["Parser (sqlglot)<br/>검증 + 파티션 IN 탐지"]
        Splitter["Splitter<br/>IN 목록 N분할 + wrapper"]
        Admission["JobAdmission<br/>동시 슬롯 + 대기 큐(429)"]
        Dispatcher["Dispatcher<br/>비동기 디스패치/polling"]
        Monitor["HealthMonitor<br/>executor /health·/metrics 폴링"]
        JobStore[("JobStore<br/>memory | file | postgres")]
    end

    subgraph Executors["Executor Pool (N개, 독립 서비스)"]
        direction LR
        E1["Executor :8087<br/>/tasks · /metrics · 대시보드(/)"]
        E2["Executor :8086"]
        E3["Executor :800N"]
    end

    Client -- "① SELECT + partition_column (+ exec_mode/wrapper)" --> API
    API --> Parser --> Splitter --> Admission --> Dispatcher
    Dispatcher <--> JobStore
    Dispatcher -- "② POST /tasks (sub-query 전문)" --> E1 & E2 & E3
    Monitor -- "주기 폴링" --> E1 & E2 & E3

    E1 & E2 & E3 -- "③ read (TLS+LDAP)" --> Impala
    E1 & E2 & E3 -- "④ COPY/INSERT 적재" --> GP

    Dispatcher -- "job_history + 공유 jobs store" --> PG
    Monitor -- "executor_health_metrics" --> PG
    E1 & E2 & E3 -- "task_history + executor_status(self-report)" --> PG
    Client -- "⑤ GET /jobs/{id}/status" --> API
```

그림을 말로 풀면 이렇습니다. 클라이언트가 쿼리를 보내면(①) coordinator의 API가 받아서 Parser로 검사하고 Splitter로 잘게 나눈 뒤, Admission으로 받아들일지 판단하고 Dispatcher가 각 executor에게 일을 보냅니다(②). 일을 받은 executor들은 Impala에서 데이터를 읽고(③) 곧바로 Greenplum에 적재합니다(④). 그동안 클라이언트는 접수증으로 진행 상황을 물어볼 수 있습니다(⑤). PostgreSQL은 이 모든 과정의 이력과 상태를 적어 두는 공통 장부 역할을 합니다.

### 설계상의 중요한 결정

위 구조가 왜 이렇게 생겼는지, 네 가지 핵심 결정을 짚어 보겠습니다.

첫째, **Executor는 read+write를 모두 수행하는 상태 보유 독립 서비스**입니다. 즉 일꾼은 단순한 계산기가 아니라, Impala에서 sub-query로 읽어 Greenplum에 적재하는 일을 끝까지 책임지고, 자기가 맡은 task의 상태를 인메모리(프로세스 안의 기억)에 들고 있다가 REST API(`/tasks`)와 자체 대시보드로 보여 줍니다.

둘째, **결과 데이터는 coordinator를 거치지 않습니다.** 만약 모든 데이터가 지휘자 한 곳으로 모였다면 그곳의 메모리와 네트워크가 금세 막혔을 것입니다. 그래서 데이터는 일꾼이 직접 목적지로 보내고, Coordinator로는 **상태와 row count만** 흐르게 했습니다.

셋째, **Coordinator는 분할한 sub-query 전문을 Task 레코드에 저장**합니다. 나중에 무슨 일이 있었는지 추적할 수 있도록, 각 일꾼에게 보낸 쿼리를 통째로 보관해 둡니다.

넷째, **상태와 이력은 PostgreSQL로 외부화**할 수 있습니다. 이렇게 하면 coordinator를 여러 대로 늘리거나 한 번 재시작한 뒤에도 과거 작업을 조회할 수 있습니다. 만약 이 외부 저장소를 설정하지 않으면 인메모리로만 동작하며, 프로세스가 꺼지면 기록도 함께 사라집니다.

---

## 3. 컴포넌트

이제 coordinator와 executor 각각이 어떤 부품들로 이루어져 있는지 들여다보겠습니다. 아래 두 표는 "어떤 부품이 무슨 일을 맡는가"를 정리한 것으로, 이 시스템의 부품 목록이라고 보면 됩니다. 표의 굵은 글씨가 부품 이름이고, 오른쪽이 그 책임, 괄호 안 파일명은 실제 코드 위치입니다.

### 3.1 Coordinator

coordinator 쪽 부품들을 먼저 봅시다. 요청을 받는 API부터, 쿼리를 검사·분할하는 부품, 과부하를 막는 부품, 실제로 일을 뿌리는 부품, 그리고 상태와 이력을 적어 두는 부품까지가 한 묶음으로 협력합니다.

| 컴포넌트 | 책임 |
|---|---|
| **API Layer** | 작업 제출/조회/취소, 클러스터 상태, 대시보드(`coordinator/app.py`) |
| **Parser** | sqlglot로 SQL 파싱, partition column의 `IN(...)` 노드 탐색, strict/lenient 모드 검증(`parser.py`) |
| **Splitter** | IN 값 리스트를 `parallelism`개로 분할 → sub-query N개 재작성(원문 포맷 보존, `splitter.py`) |
| **JobAdmission** | 동시 실행 슬롯 + 대기 큐 상한(과부하 시 429). `dispatcher.py` |
| **Dispatcher** | sub-query를 executor에 분배, task 단위 동시성(Semaphore) 제어, 상태 polling, 종료 집계(`dispatcher.py`) |
| **JobStore** | Job·Task 상태 + **sub-query 전문 저장**. `memory`(휘발) / `file`(단일 노드 **파일 영속 → 크래시 복구**) / `postgres`(공유, JSONB) — `job_store.py` |
| **JobHistory** | job 단위 실행 이력 PostgreSQL 기록·조회(`history.py`) |
| **HealthMonitor** | executor `/health`·`/metrics` 주기 폴링, 메트릭 PostgreSQL 기록(`monitor.py`) |
| **Dashboard** | 인라인 HTML 모니터링 UI(`/`) + 설정 마스킹(`dashboard.py`) |

표에 나온 용어 몇 가지를 풀어 두겠습니다. **Parser**는 들어온 SQL의 문법을 검사하고 어디에 파티션 IN 절이 있는지 찾아내는 부품이고, **Splitter**는 그 IN 값을 `parallelism`(병렬 처리 개수)만큼 쪼개 여러 개의 sub-query로 다시 써 주는 부품입니다. **JobAdmission**의 "admission"은 입장 통제라는 뜻으로, 한꺼번에 너무 많은 작업이 몰리지 않도록 입구에서 막아 주는 역할입니다. **JobStore**의 세 가지 모드 중 `memory`는 프로세스가 꺼지면 사라지는 휘발성, `file`은 파일로 저장해 크래시 후 복구가 되는 방식, `postgres`는 여러 coordinator가 함께 보는 공유 저장소입니다.

### 3.2 Executor (N개, 각각 독립 프로세스/서비스)

이번에는 일꾼 쪽입니다. executor는 한 대만 있는 게 아니라 N대가 각자 독립된 프로세스로 떠 있으며, 저마다 task를 받아 처리하고 자기 상태를 관리합니다.

| 컴포넌트 | 책임 |
|---|---|
| **Task API** | `POST /tasks`(수신·실행 시작), `GET /tasks`·`/tasks/{id}`(상태), `/cancel`, `/metrics` — `executor/app.py` |
| **Backend** | `ImpalaToGreenplumBackend`(impyla read → psycopg COPY/INSERT) + `MockBackend`. copy 모드는 COPY 전 **컬럼 사전검증(preflight)** 으로 불일치 조기 실패(`backend.py`) |
| **Task Store** | 받은 task 상태(QUEUED→READING→WRITING→DONE/FAILED/CANCELLED) + 누적 `rows_written`(인메모리 dict) |
| **TaskHistory** | task 단위 상태 전이 이력 PostgreSQL 기록·조회(`history.py`) |
| **StatusReporter** | 자기 상태(CPU/메모리/동시 task)를 공유 DB에 self-report(`status.py`) |
| **동시 task 상한** | `executor.max_concurrent_tasks` 세마포어(admission control) |
| **Graceful drain** | 종료(SIGTERM) 시 신규 task 거부(503) + 진행 중 task 를 `shutdown_drain_timeout_s` 내에서 완료 대기(`app.py` lifespan) |
| **Dashboard** | remote 모드에서 `/`에 노출되는 self-view 대시보드(`dashboard.py`) |

여기서 **Backend**는 실제로 Impala에서 읽고 Greenplum에 쓰는 손발에 해당합니다. 실전용 `ImpalaToGreenplumBackend` 외에 실제 데이터베이스 없이 테스트할 수 있는 `MockBackend`도 있다는 점을 기억해 두면 좋습니다. **세마포어(semaphore)**라는 말이 나오는데, 이것은 "동시에 들어올 수 있는 인원수를 정해 둔 출입증" 같은 장치로, 한 일꾼이 동시에 처리하는 task 수를 일정 개수로 제한합니다. **Graceful drain(우아한 비우기)**은 일꾼을 끌 때 갑자기 일을 끊지 않고, 새 일은 거절하되 하던 일은 정해진 시간 안에서 마무리하게 두는 종료 방식입니다.

---

## 4. 데이터 흐름 (Impala → Executor → Greenplum)

이제 한 명의 일꾼 입장에서 데이터가 실제로 어떻게 흘러가는지 따라가 봅시다. 일꾼은 Impala에서 자기 몫의 파티션을 읽어 들여 약간의 변환을 거친 뒤 Greenplum에 적재합니다. 아래 그림이 그 한 줄기 흐름입니다.

```mermaid
flowchart LR
    subgraph src[Impala]
        P1[(partition v1..vk)]
        P2[(partition vk+1..)]
    end
    subgraph ex[Executor k]
        R[impyla cursor<br/>배치 fetch] --> B[배치 변환] --> W[psycopg COPY/INSERT]
    end
    subgraph dst[Greenplum]
        T[(target_table)]
    end
    P1 --> R
    P2 --> R
    W --> T
```

이 흐름에서 중요한 점 세 가지를 짚어 보겠습니다.

먼저, executor는 sub-query 결과를 **스트리밍(배치 fetch)** 합니다. 즉 결과를 한꺼번에 메모리에 다 올려놓지 않고, 한 묶음씩 끊어서 읽어 흘려보내기 때문에 데이터가 아무리 커도 메모리가 터지지 않습니다.

다음으로, 적재는 `COPY`로 배치 단위로 수행합니다. 한 행씩 여러 번 INSERT 하는 것보다 한 묶음을 통째로 밀어 넣는 COPY가 훨씬 빠르기 때문입니다. 상황에 따라서는 INSERT나 staging(임시 테이블)을 경유하는 방식도 쓸 수 있는데, 이 선택지는 §9에서 자세히 다룹니다.

마지막으로, 각 executor는 **서로 다른 파티션 값 집합**을 담당하므로 Greenplum에 동시에 써도 서로 충돌하지 않고, 그래서 나중에 결과를 병합할 필요도 없습니다.

- executor는 sub-query 결과를 **스트리밍(배치 fetch)** 하여 메모리에 전체를 올리지 않는다.
- 적재는 `COPY`로 배치 단위 수행(INSERT 다건보다 훨씬 빠름). `exec_mode`에 따라 INSERT/staging 경유도 가능(§9).
- 각 executor가 **서로 다른 파티션 값 집합**을 담당 → Greenplum 쓰기 충돌 없음 → 병합 불필요.

---

## 5. 데이터 모델

이 시스템이 머릿속에 어떤 정보를 들고 있는지, 즉 데이터 모델을 살펴봅시다. 핵심은 두 가지 구조입니다. 하나는 작업 전체를 나타내는 **Job**이고, 다른 하나는 그 작업을 잘게 나눈 하나하나의 일감인 **Task**입니다. 하나의 Job 아래에 여러 개의 Task가 매달리는 관계입니다. 특히 이 모델은 **원본 쿼리와 각 executor로 보낸 sub-query 전문을 모두 저장**한다는 점이 특징인데, 이는 나중에 무슨 일이 있었는지 감사하고 디버깅하기 위해서입니다.

```mermaid
classDiagram
    class Job {
        +str job_id
        +str original_sql        // 원본 쿼리 전문
        +str partition_column
        +str target_table        // Greenplum 적재 대상
        +str write_mode          // append | overwrite_partitions
        +str exec_mode           // copy | statement | stage_insert | local_stage(§17)
        +int parallelism
        +str split_strategy      // contiguous | round_robin
        +str failure_policy      // fail_fast | best_effort
        +str username            // 제출자(이력/대시보드 표시)
        +str staging_table       // stage_insert 전용
        +str staging_ddl         // stage_insert 전용(선택 — 없으면 생성 건너뜀)
        +str insert_sql          // stage_insert INSERT 문
        +bool cancel_requested
        +str retry_of            // 재실행으로 생성된 job이면 원본 job_id
        +JobStatus status
        +int total_rows_written  // 모든 task 합산
        +datetime created_at
        +datetime started_at
        +datetime finished_at
        +str error
        +Task[] tasks
    }

    class Task {
        +str task_id
        +str job_id
        +str executor_url        // 어느 executor로 보냈는지
        +str sub_query           // ★ 보낸 sub-query 전문
        +list partition_values   // 이 task가 담당한 IN 값들
        +TaskStatus status
        +int rows_written        // 이 task가 적재한 행 수
        +int attempt
        +str error
    }

    Job "1" o-- "N" Task
```

위 그림에서 Job은 작업 전체에 대한 정보(원본 SQL, 파티션 컬럼, 적재 대상 테이블, 병렬도, 전체 상태와 적재된 총 행 수 등)를 담고, Task는 잘게 나뉜 일감 하나에 대한 정보(어느 executor로 보냈는지, 보낸 sub-query 전문, 이 일감이 담당한 IN 값들, 적재한 행 수 등)를 담습니다. 두 구조를 잇는 `Job "1" o-- "N" Task` 표시는 "Job 하나에 Task가 여럿 달린다"는 뜻입니다.

> 진행률은 `completed / total`(완료=성공·실패·취소 모두 포함)로 계산한다. `progress_percent`·`completed`·`total`은 Job에서 파생된다.

진행률을 계산할 때 "완료"에는 성공뿐 아니라 실패나 취소까지 모두 포함된다는 점에 주의하세요. 즉 진행률은 "끝난 일감이 전체 중 얼마나 되는가"이지 "성공한 일감이 얼마나 되는가"가 아닙니다.

---

## 6. 상태 머신 (양방향 추적)

작업이 시작부터 끝까지 어떤 단계를 거치는지를 **상태 머신**으로 정리합니다. 상태 머신이란 "지금 어떤 상태에 있고, 어떤 일이 생기면 어떤 상태로 넘어가는가"를 그림으로 나타낸 것입니다. 앞서 1절에서 말한 "양방향 상태 추적"이 여기서 구체화되는데, coordinator는 Job의 상태를, executor는 Task의 상태를 각자 관리합니다.

### 6.1 Coordinator — Job 상태

먼저 coordinator가 보는 Job의 일생입니다. 작업이 만들어져서 슬롯을 기다리고, 실행되고, 끝나는 흐름을 따라가 보세요.

```mermaid
stateDiagram-v2
    [*] --> SPLITTING: POST /jobs (검증+분할 완료, 작업 생성)
    SPLITTING --> PENDING: 백그라운드 run() — 실행 슬롯 대기
    PENDING --> RUNNING: admission 슬롯 확보 → 디스패치
    RUNNING --> DONE: 모든 Task DONE
    RUNNING --> PARTIAL: 일부 Task FAILED (정책=best_effort)
    RUNNING --> FAILED: Task FAILED (정책=fail_fast)
    PENDING --> CANCELLED: 대기 중 취소
    RUNNING --> CANCELLED: 실행 중 취소
    DONE --> [*]
    FAILED --> [*]
    PARTIAL --> [*]
    CANCELLED --> [*]
```

이 그림을 이야기로 풀면 이렇습니다.

검증과 분할은 `POST /jobs` 요청을 처리하는 그 자리에서 **동기로**(즉 요청에 응답하기 전에 바로) 끝납니다. 그래서 문제가 있으면 곧장 4xx 오류로 거절되고, 통과하면 작업은 `SPLITTING` 상태로 만들어진 뒤 곧 백그라운드의 `run()`이 이어받습니다.

이어서 `run()`은 admission의 실행 슬롯이 빌 때까지 작업을 `PENDING`(대기 줄)에 세워 두었다가, 슬롯을 잡으면 `RUNNING`으로 넘깁니다. 만약 입구에서 이미 용량이 꽉 찼다면 애초에 작업이 만들어지지도 않고 `429`로 거절되는데, 이 입장 통제 이야기는 §10에서 자세히 다룹니다.

작업이 끝나면 최종 상태는 `finalize_job()`이 하위 task들을 모아 보고 결정합니다. 그 판단 순서는 다음과 같습니다. 취소가 있었으면 취소가 우선이고, 실패한 task가 하나도 없으면 DONE, 실패가 있어도 best_effort 정책이면 PARTIAL, 그 외에는 FAILED입니다.

또한 이 시스템은 **재기동 정합(크래시 복구)**을 지원합니다. 영속 저장소(`file`/`postgres`)를 쓰는 경우, coordinator가 다시 켜질 때 `reconcile_interrupted_jobs()`가 아직 끝나지 않은(PENDING/SPLITTING/RUNNING) 채로 남아 있던 job을 `FAILED`로 정리합니다. 프로세스가 죽으면서 그 job을 실행하던 루프도 함께 사라졌기 때문입니다. 이때 진행 중이던 task도 FAILED로 표시되어 나중에 `retry`(재실행)의 대상이 됩니다.

그리고 **실패 파티션 재실행**도 가능합니다. 이미 끝난 job에 `POST /jobs/{id}/retry`를 보내면, FAILED/CANCELLED였던 task만 모은 **새 job**(원본을 가리키는 `retry_of` 표시가 붙습니다)이 SPLITTING부터 똑같은 흐름으로 다시 실행됩니다.

- 검증/분할은 `POST /jobs` 핸들러에서 **동기로** 끝나므로(실패 시 즉시 4xx), 작업은 `SPLITTING`으로 생성되고 곧 백그라운드 `run()`이 받는다.
- `run()`은 admission 실행 슬롯이 빌 때까지 job을 `PENDING`(대기 큐)으로 두었다가, 슬롯을 잡으면 `RUNNING`으로 전이한다. (입구에서 용량 초과면 애초에 `429`로 거부되어 작업이 생성되지 않는다 — §10)
- 최종 상태는 `finalize_job()`이 하위 task를 집계해 결정한다: 취소 우선 → 실패 없음=DONE → best_effort=PARTIAL → 그 외=FAILED.
- **재기동 정합(크래시 복구)**: 영속 저장소(`file`/`postgres`)면 기동 시 `reconcile_interrupted_jobs()`가 비종료(PENDING/SPLITTING/RUNNING)로 남은 job을 `FAILED`로 정합한다(실행 루프가 사라졌으므로). 진행 중이던 task도 FAILED로 표시돼 `retry` 대상이 된다.
- **실패 파티션 재실행**: 종료된 job에 `POST /jobs/{id}/retry` → FAILED/CANCELLED task만 담은 **새 job**(`retry_of`=원본)이 SPLITTING부터 동일 흐름으로 실행된다.

### 6.2 Task 상태 (Coordinator 미러 ↔ Executor 원본)

이번에는 일감 하나, 즉 Task의 일생입니다. Task의 "진짜" 상태는 그 일을 직접 하는 executor가 들고 있고, coordinator는 그것을 폴링으로 따라 적는 사본(미러)을 갖습니다.

```mermaid
stateDiagram-v2
    [*] --> QUEUED: POST /tasks 수신
    QUEUED --> READING: Impala sub-query 실행
    READING --> WRITING: 배치 fetch → Greenplum 적재
    WRITING --> DONE: 적재 완료
    READING --> FAILED: Impala 읽기 에러
    WRITING --> FAILED: Greenplum 적재 에러
    QUEUED --> CANCELLED: 시작 전 취소
    WRITING --> CANCELLED: 실행 중 취소(작업 완료 후 마감)
    DONE --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

이 두 계층이 어떻게 협력하는지 정리하면 다음과 같습니다.

**Executor** 쪽은 위 상태와 누적된 `rows_written`(지금까지 적재한 행 수)을 인메모리에 기록하고, `GET /tasks/{id}`로 외부에 보여 줍니다. 상태가 바뀔 때마다 `task_history`라는 이력 테이블에 한 줄씩 덧붙입니다.

**Coordinator** 쪽은 Dispatcher가 주기적으로 폴링하여 각 Task의 상태와 row count를 자기 쪽에 따라 적습니다(미러링). 그리고 Job 전체의 상태는 이 Task들을 모아 보고 결정합니다.

끝으로 `started_at`과 `finished_at` 시각은 executor가 READING 단계에 들어가고 빠져나가는 시점에 기록하는데, 이 값은 대시보드에서 각 task가 얼마나 걸렸는지를 보여 주는 데 쓰입니다.

- **Executor**: 위 상태 + 누적 `rows_written`을 인메모리에 기록, `GET /tasks/{id}`로 노출. 상태 전이마다 `task_history`에 append.
- **Coordinator**: Dispatcher가 polling으로 각 Task 상태/row count를 미러링. Job 상태는 Task 집계로 결정.
- `started_at`/`finished_at`은 executor가 READING 진입·종료 시점에 기록(대시보드 소요 시간 표시).

---

## 7. 요청 처리 시퀀스

지금까지 부품과 상태를 따로따로 봤다면, 이제는 하나의 요청이 시작부터 끝까지 어떻게 처리되는지 시간 순서대로 따라가 봅시다. 아래 시퀀스 다이어그램은 클라이언트, coordinator, 각종 저장소와 데이터베이스가 주고받는 메시지를 위에서 아래로 시간 순으로 나열한 것입니다.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant CO as Coordinator
    participant JS as JobStore
    participant EX as Executor k (N개)
    participant IM as Impala
    participant GP as Greenplum
    participant PG as PostgreSQL

    C->>CO: POST /jobs {sql, partition_column, target_table, exec_mode, ...}
    CO->>CO: Parser 검증 + Splitter 분할 (+ wrapper 적용)
    CO->>CO: admission.try_admit() — 용량 초과면 429
    CO->>JS: Job 생성(SPLITTING) · 각 Task에 sub-query 전문 저장
    CO-->>C: 202 {job_id}

    Note over CO,PG: 백그라운드 run(job) — 슬롯 대기(PENDING) 후 RUNNING
    CO->>PG: job_history (RUNNING)

    par 각 executor 병렬 디스패치 (max_dispatch_concurrency)
        CO->>EX: POST /tasks {task_id, sub_query, exec_mode, ...}
        EX->>PG: task_history (QUEUED→READING→WRITING)
        EX->>IM: sub-query 실행(배치 스트리밍)
        IM-->>EX: rows
        EX->>GP: COPY/INSERT 적재
        EX->>PG: task_history (DONE, rows_written)
        EX-->>CO: 상태/행수 (polling)
    end

    CO->>JS: 모든 task 종료 → finalize_job (DONE/PARTIAL/FAILED/CANCELLED)
    CO->>PG: job_history (최종 상태)

    C->>CO: GET /jobs/{job_id}/status
    CO-->>C: {status, progress_percent, completed/total, total_rows_written}
```

흐름을 말로 풀면 이렇습니다. 클라이언트가 쿼리와 옵션을 담아 `POST /jobs`를 보내면, coordinator는 그 자리에서 Parser로 검증하고 Splitter로 분할한 뒤(필요하면 wrapper를 입힙니다) admission으로 받아들일지 판단합니다. 용량이 넘으면 여기서 429로 거절합니다. 통과하면 Job을 `SPLITTING`으로 만들고 각 Task에 sub-query 전문을 저장한 다음, 곧바로 접수증(`202 {job_id}`)을 돌려줍니다. 클라이언트는 더 이상 기다리지 않아도 됩니다.

그 뒤로는 백그라운드에서 `run(job)`이 슬롯을 기다렸다가 RUNNING으로 넘어가고, 여러 executor에게 task를 **병렬로** 뿌립니다(이 동시 디스패치 개수가 `max_dispatch_concurrency`로 제한됩니다). 각 executor는 Impala에서 읽고 Greenplum에 적재하며, 그 진행 상황을 task_history에 적고 coordinator의 폴링에 응답합니다. 모든 task가 끝나면 `finalize_job`이 최종 상태를 정하고, 클라이언트는 `GET /jobs/{job_id}/status`로 결과(상태, 진행률, 완료/전체 개수, 총 적재 행 수)를 받아 봅니다.

> **모니터링은 별개 루프**: Coordinator는 `monitor.health_interval_s`마다 각 executor `/health`·`/metrics`를 폴링하고(`GET /executors`), `monitor.record_interval_s`마다 `executor_health_metrics`에 기록한다. (executor self-report 모드면 coordinator 폴링 대신 executor가 직접 기록 — §12)

한 가지 덧붙이면, 위 작업 처리와는 별개로 건강 상태를 살피는 모니터링 루프가 따로 돕니다. coordinator는 `monitor.health_interval_s` 간격으로 각 executor의 `/health`와 `/metrics`를 들여다보고, `monitor.record_interval_s` 간격으로 그 결과를 `executor_health_metrics`에 기록합니다. 다만 executor가 self-report 모드라면 coordinator가 일일이 물어보는 대신 executor가 직접 기록하는데, 이 이야기는 §12에서 이어집니다.

---

## 8. 쿼리 분할 (Splitting)

이제 이 시스템의 심장이라 할 수 있는 "쿼리를 어떻게 잘게 나누는가"를 자세히 봅시다. 핵심 아이디어는 단순합니다. 쿼리의 `IN (...)` 안에 들어 있는 값들을 여러 묶음으로 나누고, 각 묶음만 남긴 작은 쿼리(sub-query)를 새로 만들어 일꾼들에게 주는 것입니다.

### 입력 예시 (Impala source)

예를 들어 다음과 같은 쿼리가 들어왔다고 합시다. 날짜(`dt`)가 파티션 컬럼이고, IN 절에 여러 날짜가 나열되어 있습니다.

```sql
SELECT user_id, amount, dt
FROM sales
WHERE dt IN ('2026-01-01','2026-01-02', ... ,'2026-06-25')   -- partition_column = dt
  AND region = 'KR'
```

### 절차

이 쿼리를 나누는 과정은 다음 다섯 단계를 차례로 거칩니다.

1. **파싱**: `sqlglot.parse_one(sql, read=<dialect>)` → AST. 즉 글자 그대로의 SQL 문자열을 컴퓨터가 다루기 쉬운 나무 모양 구조(AST, 추상 구문 트리)로 바꿉니다.
2. **IN 절 탐색**: `partition_column`의 `IN` 노드를 찾는다. 테이블 한정자(`A.dt`)·대소문자는 무시. 다시 말해 `A.dt`처럼 테이블 이름이 앞에 붙어 있거나 대소문자가 다르더라도 같은 컬럼으로 알아봅니다.
3. **검증**: 이 단계에서 쿼리가 분할에 적합한지를 확인합니다. 검사 강도는 `strict_validation` 설정에 따라 둘로 갈립니다.
   - `strict_validation=true`(기본): `GROUP BY`/집계/`DISTINCT`/`JOIN`/`NOT IN`/서브쿼리 IN/IN 누락을 안정적 에러 코드로 거부.
   - `strict_validation=false`(lenient): 복합 쿼리 허용. 트리 어디에 있든 파티션 `IN`을 찾아 그 절만 분할.
4. **값 분할**: IN 값 `[v1..vM]`를 `parallelism`개 청크로 분할 (`contiguous` 기본 / skew 심하면 `round_robin`). 여기서 청크(chunk)란 나눠진 한 묶음을 말하고, `contiguous`는 앞에서부터 연속으로 잘라 주는 방식, `round_robin`은 카드를 돌리듯 한 개씩 번갈아 나눠 주는 방식입니다. 한쪽에 데이터가 쏠리는 편향(skew)이 심할 때는 `round_robin`이 균형을 더 잘 맞춰 줍니다.
5. **sub-query 재작성**: 각 청크로 **IN 절의 값 목록 구간만** 문자열 치환해 N개의 완전한 SQL 생성(원문 포맷 보존, 폴백으로 AST 재생성). 즉 원래 쿼리의 모양은 그대로 두고 IN 안의 값 목록 부분만 바꿔 끼워, 일꾼 수만큼의 완성된 쿼리를 만들어 냅니다. 만약 단순 문자열 치환이 어려우면 AST로부터 다시 만들어 내는 방법으로 대체(폴백)합니다.

아래 그림은 이 과정을 한눈에 보여 줍니다. 원본 SQL을 파싱해 파티션 IN 절이 있는지 확인하고, 없으면 4xx로 거절, 있으면 값을 N등분해 여러 sub-query로 만든 뒤 각 Task에 전문을 저장하는 흐름입니다.

```mermaid
flowchart LR
    Q[원본 SQL] --> P[AST 파싱] --> F{파티션 IN 절 발견?}
    F -- No --> R[4xx 거부]
    F -- Yes --> S[IN 값 N등분]
    S --> G1[sub-query 1<br/>IN v1..vk]
    S --> G2[sub-query 2<br/>IN vk+1..]
    S --> G3[sub-query N]
    G1 & G2 & G3 --> ST[(Task에 전문 저장)]
```

> **lenient 결과 보존 가정**: 분할 기준 컬럼이 출력 행을 분할하는 위치(주로 소스 스캔 필터)에 있어야 한다. 분할 기준 위에서 집계/DISTINCT 하면 결과가 달라질 수 있다.

마지막으로 한 가지 주의가 있습니다. `strict_validation=false`(lenient) 모드로 복합 쿼리를 나눌 때는 한 가지 전제가 지켜져야 합니다. 분할 기준 컬럼이 출력되는 행을 실제로 나누는 위치, 즉 주로 소스를 읽어 들이는 필터 자리에 있어야 한다는 것입니다. 만약 그 분할 기준보다 위에서 집계나 DISTINCT가 일어난다면, 데이터를 나눠 처리한 결과가 한꺼번에 처리한 결과와 달라질 수 있으니 조심해야 합니다.

---

## 9. 적재 방식 (`exec_mode`)

같은 "적재"라도 상황에 따라 가장 알맞은 방법이 다릅니다. 그래서 이 시스템은 `exec_mode`라는 설정으로 세 가지 적재 방식을 골라 쓸 수 있게 했습니다. 아래 표는 각 방식이 무엇을 하고 어떤 경우에 적합한지를 정리한 것입니다. 소스(읽는 곳)와 타깃(쓰는 곳)이 같은 데이터베이스인지, 다른 엔진인지에 따라 선택이 갈린다는 점에 주목하며 읽으면 좋습니다.

| `exec_mode` | 동작 | 적합한 경우 |
|---|---|---|
| `copy` (기본) | Impala에서 sub-query를 **읽어** Greenplum에 `COPY FROM STDIN` 배치 적재. **사전검증(preflight)**: COPY 전에 SELECT 컬럼이 대상 테이블에 있는지 확인(`copy.preflight`, 기본 on) | 소스(Impala)/타깃(Greenplum)이 다른 엔진. COPY는 대상 테이블 컬럼과 정확히 일치해야 하며, wrapper는 **행을 반환하는 SELECT** 여야 한다 |
| `statement` | wrapper로 감싼 SQL(예: `INSERT ... SELECT`)을 대상 DB에서 **그대로 실행** | 소스/타깃이 같은 DB(Greenplum). INSERT 컬럼 목록이 매핑을 담당 |
| `stage_insert` | (선택적으로 `staging_ddl`로 staging 테이블 생성 →) Impala SELECT 결과를 Greenplum **staging에 COPY** → staging을 `FROM`으로 하는 **INSERT 실행** | SELECT은 Impala, INSERT은 Greenplum처럼 서로 다른 엔진을 INSERT로 연결 |
| `local_stage` | 각 executor가 세그먼트 호스트 **로컬 디스크에 CSV 파일**로 export → GP가 `file://` 외부테이블로 **세그먼트별 로컬 파일을 병렬 read**해 staging 적재 → target INSERT. 2-phase(배리어). 자세히는 **§17** | executor를 **GP 세그먼트 호스트에 co-locate**한 대량 이관. `copy`의 단일 COPY 소켓 병목을 세그먼트 병렬 read로 대체 |

표를 풀어 설명하면 이렇습니다. 기본값인 `copy`는 Impala에서 읽은 데이터를 Greenplum에 통째로 밀어 넣는 가장 빠른 방식으로, 소스와 타깃 엔진이 서로 다를 때 잘 맞습니다. 다만 COPY는 컬럼이 대상 테이블과 정확히 들어맞아야 하므로, COPY를 시작하기 전에 SELECT의 컬럼들이 대상 테이블에 실제로 있는지 미리 확인하는 **사전검증(preflight)**을 합니다(이 검증은 `copy.preflight` 설정으로 켜고 끌 수 있으며 기본은 켜짐입니다). `statement`는 `INSERT ... SELECT` 같은 문장을 대상 DB에서 그대로 실행하는 방식으로, 소스와 타깃이 같은 Greenplum일 때 적합합니다. `stage_insert`는 Impala에서 읽은 결과를 일단 Greenplum의 staging 테이블에 COPY로 넣어 둔 다음, 그 staging 테이블을 바탕으로 INSERT를 실행하는 방식으로, 서로 다른 엔진을 INSERT 문으로 이어 주고 싶을 때 씁니다. 이때 staging 테이블을 만드는 `staging_ddl`은 **선택**입니다. 주면 COPY 전에 그 DDL(보통 `CREATE TEMP TABLE`)로 테이블을 만들고, 생략하면 테이블 생성을 건너뛰고 이미 존재하는 `staging_table`을 그대로 씁니다(이 경우 영구 테이블을 여러 task가 공유하지 않도록 격리에 유의).

**write_mode**(`copy`/적재 공통):

적재 방식과는 별개로, 데이터를 "어떻게 쌓을지"를 정하는 `write_mode`가 있습니다. 아래 표가 그 두 가지입니다.

| 모드 | 동작 |
|---|---|
| `append` | 단순 COPY로 누적 |
| `overwrite_partitions` | task별 담당 `partition_values`에 대해 같은 트랜잭션에서 먼저 `DELETE WHERE <partition_column> IN (chunk)` 후 COPY → **재실행 멱등성** 확보 |

`append`는 그냥 기존 데이터 위에 새 데이터를 덧붙이는 방식입니다. 반면 `overwrite_partitions`는 각 task가 맡은 파티션 값들에 대해 같은 트랜잭션 안에서 먼저 해당 구간을 `DELETE`로 지운 뒤 COPY로 새로 채웁니다. 이렇게 하면 같은 task를 다시 실행해도 결과가 똑같아지는 성질, 즉 **멱등성(idempotency)**이 확보됩니다. 멱등성이란 "같은 일을 여러 번 해도 한 번 한 것과 결과가 같다"는 뜻으로, 재실행이 안전해진다는 점에서 매우 중요합니다.

이와 관련해 몇 가지 보충 사항이 있습니다.

먼저 트랜잭션은 task 단위입니다. 그래서 어떤 task가 중간에 실패하면 그 task만 rollback(되돌리기)되고 다른 task에는 영향이 없습니다. 또한 각 task는 서로 겹치지 않는(disjoint) 파티션 집합만 다루므로 executor들 사이에 쓰기 충돌이 생기지 않습니다.

다음으로 **wrapper_query**라는 개념이 있습니다. 이것은 분할된 각 sub-query를 한 번 더 감싸는 바깥 쿼리로, `wrapper_placeholder`(기본 `{{SUBQUERY}}`)라고 적힌 자리에 안쪽 sub-query가 끼워집니다. 다만 `stage_insert`에서는 placeholder 대신 임시 테이블 이름을 참조하는 INSERT를 둡니다.

끝으로 **Impala 쿼리 옵션(SET)**을 전달할 수 있습니다. 전역 설정 `impala.query_options`에 요청별 `impala_query_options`를 얹어(전역 위에 병합) impyla의 `configuration`으로 넘깁니다. 이 옵션은 copy와 stage_insert가 수행하는 Impala SELECT에만 적용되며, 둘 다 비어 있으면 `configuration` 없이 그대로 실행합니다.

- 트랜잭션은 task 단위. 실패 시 해당 task만 rollback, 다른 task 무영향.
- 각 task가 disjoint한 partition 집합만 다루므로 executor 간 쓰기 충돌 없음.
- **wrapper_query**: 분할된 각 sub-query를 감싸는 쿼리. `wrapper_placeholder`(기본 `{{SUBQUERY}}`) 자리에 치환. `stage_insert`에서는 placeholder 대신 staging 테이블명을 참조하는 INSERT를 둔다.
- **Impala 쿼리 옵션(SET)**: 전역 `impala.query_options` + 요청별 `impala_query_options`(전역 위에 병합)를 impyla `configuration`으로 전달한다. copy·stage_insert의 Impala SELECT에만 적용되며, 둘 다 비면 `configuration` 없이 그대로 실행한다.

> 결과 데이터는 coordinator를 통과하지 않으므로 "결과 병합(merge)" 단계가 없다. Coordinator는 `rows_written`만 합산한다.

다시 한번 강조하면, 결과 데이터는 coordinator를 거치지 않기 때문에 따로 결과를 병합하는 단계가 없습니다. coordinator가 하는 일은 각 task가 보고한 `rows_written`을 더하는 것뿐입니다.

---

## 10. 동시성 모델 (admission control)

서버는 한 번에 처리할 수 있는 양이 정해져 있습니다. 그 한계를 넘는 요청이 한꺼번에 밀려들면 모두가 함께 느려지거나 무너집니다. 이를 막기 위해 이 시스템은 **admission control(입장 통제)**을 세 개의 층위로 두어, 입구부터 가장 안쪽까지 단계적으로 과부하를 거릅니다. 아래 그림이 그 세 층입니다.

```mermaid
flowchart TB
    subgraph L1["입구: Job admission (coordinator 인스턴스별)"]
        direction LR
        Slots["실행 슬롯<br/>max_concurrent_jobs (16)"]
        Queue["대기 큐<br/>max_pending_jobs (100)"]
        Reject["초과 → 429"]
    end
    subgraph L2["디스패치: Task 동시성 (job 실행 중)"]
        Disp["max_dispatch_concurrency (32)<br/>per-dispatcher Semaphore"]
    end
    subgraph L3["executor: Task 동시성 (executor별)"]
        ExSem["executor.max_concurrent_tasks (8)<br/>Semaphore"]
    end
    L1 --> L2 --> L3
```

세 층을 차례로 설명하겠습니다.

**Level 1 — Job admission(`JobAdmission`)**은 가장 바깥 입구입니다. 여기에는 `max_concurrent_jobs`개의 실행 슬롯과 `max_pending_jobs`개의 대기 줄이 있습니다. 슬롯이 비어 있으면 작업은 곧장 RUNNING이 되고, 슬롯이 꽉 차 있으면 PENDING으로 줄을 섭니다. 그런데 실행 중인 것과 대기 중인 것을 합한 수(capacity)마저 넘는 요청이 오면, 그때는 `429 Too Many Requests`로 거절하면서 `Retry-After`(잠시 후 다시 시도하라)를 함께 알려 줍니다. 참고로 `max_concurrent_jobs`를 0 이하로 두면 무제한이 되며, 이 한도는 **coordinator 인스턴스마다 인메모리로** 따로 셉니다. 그래서 coordinator가 여러 대면 한도도 그만큼 합산됩니다.

**Level 2 — Task 디스패치 동시성**은 한 coordinator가 동시에 띄울 수 있는 executor task의 총수를 `max_dispatch_concurrency` 세마포어로 제한합니다(모든 job을 통틀어서 셉니다).

**Level 3 — Executor admission**은 가장 안쪽으로, executor 한 대가 동시에 실행하는 task 수를 `executor.max_concurrent_tasks` 세마포어로 제한합니다. 여러 coordinator가 한 executor에게 일을 몰아주더라도 이 마지막 방어선이 그 합산 부하를 막아 줍니다.

그 밖에 입출력 처리 방식도 함께 알아 두면 좋습니다. **Coordinator I/O**는 `httpx.AsyncClient`와 `asyncio.gather`로 executor를 비동기로(코루틴 동시성) 호출합니다. 반면 **Executor 내부**에서 쓰는 impyla와 psycopg는 동기 라이브러리라서, 그대로 부르면 이벤트 루프가 멈춰 버립니다. 그래서 이들을 `run_in_executor(thread_pool, ...)`로 감싸 별도 스레드에서 돌려 이벤트 루프가 막히지 않게 합니다.

- **Level 1 — Job admission (`JobAdmission`)**: `max_concurrent_jobs`개의 실행 슬롯 + `max_pending_jobs`개의 대기 큐. 슬롯이 비면 즉시 RUNNING, 차면 PENDING으로 줄을 세우고, **실행+대기 합(capacity)을 넘는 요청은 `429 Too Many Requests`(`Retry-After`)로 거부**한다. `max_concurrent_jobs<=0`이면 무제한. 이 한도는 **coordinator 인스턴스별(인메모리)** 이라 멀티 coordinator에선 합산된다.
- **Level 2 — Task 디스패치 동시성**: `max_dispatch_concurrency` 세마포어로 한 coordinator가 동시에 띄우는 executor task 수를 제한(모든 job 통틀어).
- **Level 3 — Executor admission**: `executor.max_concurrent_tasks` 세마포어로 executor 한 대가 동시에 실행하는 task 수를 제한(여러 coordinator의 합산 부하 방어).
- **Coordinator I/O**: `httpx.AsyncClient` + `asyncio.gather`로 executor 비동기 호출(코루틴 동시성).
- **Executor 내부**: impyla/psycopg는 동기 → `run_in_executor(thread_pool, ...)`로 감싸 이벤트 루프 비차단.

> 적정값 산정: 실제 천장은 coordinator 코어가 아니라 **Greenplum 동시 COPY 허용량·Impala 동시 쿼리 슬롯·executor 풀 합**이다. 다운스트림 용량에 맞춰 `executor.max_concurrent_tasks`를 분배하고, `max_dispatch_concurrency`는 그 이상으로 두어 coordinator가 병목이 되지 않게 한다.

이 값들을 어떻게 정하면 좋을지에 대한 조언으로 이 절을 맺습니다. 진짜 한계는 coordinator의 CPU 코어 수가 아니라, 그 아래에 있는 Greenplum이 동시에 받아 줄 수 있는 COPY 수, Impala의 동시 쿼리 슬롯, 그리고 executor 풀 전체의 처리 능력입니다. 그러니 이 하류(downstream) 용량에 맞춰 `executor.max_concurrent_tasks`를 나눠 정하고, `max_dispatch_concurrency`는 그보다 넉넉히 두어 coordinator 자신이 병목이 되지 않도록 하는 것이 좋습니다.

---

## 11. API 명세

이제 실제로 이 시스템을 어떻게 호출하는지, 즉 API 명세를 봅시다. coordinator와 executor가 각각 자기만의 엔드포인트(요청을 받는 주소)를 제공합니다. 보통은 coordinator API만 쓰면 되고, executor API는 일꾼 내부를 들여다보거나 디버깅할 때 유용합니다.

### 11.1 Coordinator API

가장 중요한 요청은 작업을 제출하는 `POST /jobs`입니다. 아래는 그 요청 본문의 예시인데, 각 필드 옆 주석이 무엇을 뜻하는지 설명합니다.

```http
POST /jobs
{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN (...) AND region='KR'",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "write_mode": "overwrite_partitions",   // | "append"
  "exec_mode": "copy",                     // | "statement" | "stage_insert"
  "parallelism": 4,
  "split_strategy": "contiguous",          // | "round_robin"
  "failure_policy": "fail_fast",           // | "best_effort"
  "strict_validation": true,               // false면 복합 쿼리 허용
  "sql_dialect": "hive",                   // 선택(기본 서버 설정)
  "wrapper_query": null,                   // 선택(분할 sub-query 감싸기)
  "username": null,                        // 선택(이력/대시보드 표시)
  "impala_query_options": null,            // 선택. Impala SET 옵션, 전역 위에 병합. 예: {"MEM_LIMIT":"2g"}
  "dry_run": false                         // true면 executor 미호출, 생성 쿼리만 반환
}
→ 202 { "job_id": "a1b2c3" }
→ 429 { "detail": "동시 실행/대기 job 한도 초과(capacity=...)" }   // admission 초과
→ 4xx { ... }                                                      // 검증 실패(에러 코드)
```

응답은 세 가지로 갈립니다. 정상 접수되면 `202`와 함께 접수증인 `job_id`를 받고, 입구가 꽉 찼으면 `429`로 거절되며, 쿼리 검증에 실패하면 `4xx`와 에러 코드를 받습니다. 여기서 `dry_run`을 `true`로 두면 실제로 executor를 호출하지 않고 어떤 쿼리들이 만들어질지만 미리 확인할 수 있어, 분할 결과를 점검할 때 편리합니다.

coordinator가 제공하는 전체 엔드포인트 목록은 아래 표와 같습니다. 왼쪽이 호출 주소, 오른쪽이 그 용도입니다.

| 엔드포인트 | 설명 |
|---|---|
| `POST /jobs` | 작업 제출 → `{job_id}`. `dry_run=true`면 쿼리 미리보기(200, 미저장) |
| `GET /jobs` | 작업 목록(상태 필터/limit). 대시보드 "처리중인 Query" |
| `GET /jobs/{id}/status` | **진행 상태/진행률**(경량, 태스크 제외) |
| `GET /jobs/{id}` | 전체 상태(태스크 목록 포함) |
| `GET /jobs/{id}/result` | 적재 결과 요약(`total_rows_written`, per-task) |
| `GET /jobs/{id}/tasks/{task_id}` | 태스크 상세(**sub-query 전문 포함**, 감사/디버깅) |
| `POST /jobs/{id}/cancel` | 작업 취소(각 executor에 전파). 이미 종료면 409 |
| `POST /jobs/{id}/retry` | **실패 파티션만 재실행**: 종료된 job의 FAILED/CANCELLED task만 새 job으로 복제·디스패치(`retry_of`로 추적) → 새 `job_id`(202). 대상 없으면 409 |
| `GET /history` | 과거 실행 이력(PostgreSQL `job_history`, job_id별 최신 1건, 페이징) |
| `GET /executors` | executor 헬스/메트릭 상태 |
| `GET /cluster` | coordinator+executor health/metrics + 실행 중 job 수 한 번에 |
| `GET /health`·`/healthz`·`/metrics` | 헬스 체크/시스템 메트릭 |
| `GET /`·`/config`·`/info` | 대시보드 HTML / 설정(마스킹) / 요약 (`dashboard.enabled`로 토글) |

이 가운데 진행 상황만 가볍게 보고 싶으면 `GET /jobs/{id}/status`를, task 목록까지 전부 보고 싶으면 `GET /jobs/{id}`를 쓰면 됩니다. 어떤 task에 정확히 어떤 sub-query가 갔는지는 `GET /jobs/{id}/tasks/{task_id}`로 확인할 수 있어 감사와 디버깅에 유용합니다.

### 11.2 Executor API

executor 쪽에서 가장 핵심이 되는 것은 일감을 받는 `POST /tasks`입니다. coordinator가 이 요청으로 sub-query와 적재 정보를 일꾼에게 넘깁니다.

```http
POST /tasks
{ "task_id":"t1", "job_id":"a1b2c3",
  "sub_query":"SELECT ... WHERE dt IN (...)",
  "target_table":"public.sales_mirror", "write_mode":"overwrite_partitions",
  "partition_column":"dt", "partition_values":["'2026-01-01'", ...],
  "exec_mode":"copy", "username":null,
  "staging_table":null, "staging_ddl":null, "insert_sql":null }
→ 202 { "task_id":"t1", "status":"QUEUED" }
```

executor가 제공하는 엔드포인트는 아래 표와 같습니다. 대체로 task의 접수·조회·취소와 자기 상태를 보여 주는 것들입니다.

| 엔드포인트 | 설명 |
|---|---|
| `POST /tasks` | sub-query 수신·비동기 실행 시작 |
| `GET /tasks` | 이 executor 보유 task 목록(상태 필터). self-view 대시보드 |
| `GET /tasks/{id}` | task 상태(`status`, `rows_written`, `error`, `started_at`/`finished_at` 등) |
| `GET /tasks/{id}/result` | 적재 행수 |
| `POST /tasks/{id}/cancel` | task 취소(협조적) |
| `GET /health`·`/healthz`·`/metrics` | 헬스/메트릭(+ 동시 처리 현황) |
| `GET /`·`/history`·`/config`·`/info` | self-view 대시보드 / task 이력 / 설정 / 요약 |

---

## 12. 멀티 coordinator & 상태 외부화

지금까지는 coordinator가 한 대인 것처럼 이야기했지만, 실제로는 여러 대를 둘 수 있습니다. coordinator를 여러 대로 늘리면 한 대가 죽어도 서비스가 이어지고(고가용성), 더 많은 요청을 받을 수 있습니다. 이를 가능하게 하는 열쇠가 공유 PostgreSQL(`history.db_dsn`)인데, 이를 통해 두 가지를 외부로 빼냅니다(외부화). 아래 표가 그 두 설정입니다.

| 설정 | 효과 |
|---|---|
| `store.backend=postgres` | **공유 Job 저장소**(`jobs` 테이블, JSONB). 어느 coordinator로 조회/취소가 가도 동작 |
| `executor.self_report=true` | **executor가 자기 상태를 직접 기록**(`executor_status`). coordinator는 읽기만 → 중복 폴링/기록 제거 |

표를 풀어 설명하고 거기에 따라오는 동작들을 정리하면 다음과 같습니다.

첫째, `store.backend=postgres`로 Job 저장소를 공유하면 모든 coordinator가 같은 `jobs` 테이블을 보게 됩니다. 그래서 상태 조회, 결과 확인, 취소 요청이 어느 coordinator로 들어가도 똑같이 응답할 수 있습니다. 작업을 실제로 실행 중인 디스패처는 그 진행 스냅샷을 주기적으로 저장소에 적어 둡니다.

둘째, 이 공유 저장소 덕분에 **cross-coordinator 취소(다른 지휘자가 맡은 작업의 취소)**도 됩니다. 다른 coordinator가 소유한 작업이라도 공유 저장소에 `cancel_requested` 플래그를 세워 두면, 실제 소유자인 coordinator가 폴링하다가 그 신호를 보고 작업을 중단합니다.

셋째, `executor.self_report=true`로 두면 **executor의 생존 여부(liveness)**를 다르게 판단합니다. 이 모드에서 executor는 `status_interval_s`마다 `executor_status`에 자기 상태를 upsert(있으면 갱신, 없으면 삽입)하는 심장 박동(heartbeat)을 남기고, coordinator는 그 기록의 `updated_at`이 얼마나 신선한지를 보고 살아 있는지를 판정합니다.

넷째, 이력은 두 계층으로 나뉘어 기록됩니다. 하나의 `job_id` 아래 여러 task가 생기므로, job 단위는 coordinator가 `job_history`에, task 단위는 각 executor가 `task_history`에(누가 한 일인지 `executor_id`로 구분) 각각 적습니다. 작업을 제출할 때 `username`을 함께 넘기면 두 테이블 모두에 그 사용자가 기록됩니다.

- **상태 조회/결과/취소**가 공유 `jobs` 테이블 기반이라 아무 coordinator로 라우팅돼도 응답한다. 디스패처는 실행 중 스냅샷을 주기적으로 store에 저장한다.
- **cross-coordinator 취소**: 다른 coordinator 소유 작업도 `cancel_requested` 플래그를 공유 store에 세우면 소유 coordinator가 polling 중 감지해 중단한다.
- **executor liveness**: self-report 모드면 executor가 `status_interval_s`마다 `executor_status`에 upsert(heartbeat)하고, coordinator는 `updated_at` 신선도로 liveness를 판정한다.
- **이력 2계층**: 하나의 `job_id` 아래 N개 task가 생기므로 `job_history`(coordinator, job 단위) + `task_history`(각 executor, task 단위, `executor_id`로 식별)로 기록. 제출 시 `username`을 넘기면 두 테이블 모두 기록된다.

### HA 헬스 기반 선택 & 정합 (Phase 3)

coordinator가 여러 대일 때 생기는 까다로운 문제가 하나 있습니다. 어느 executor에게 일을 줄지를 여러 coordinator가 각자 정해야 하는데, 이들이 서로 조율하지 않으면 같은 곳에 몰릴 수 있다는 점입니다. 이 절은 그 문제를 중앙 관리자 없이, 즉 한 곳이 죽으면 전체가 멈추는 단일 장애점(SPOF) 없이 분산해서 푸는 방법을 다룹니다. 핵심 아이디어는 **여러 coordinator가 독립적으로, 공유된(약간 오래된) 부하 현황을 보고** 각자 판단하게 하는 것입니다. 아래 표가 이를 켜는 설정들입니다.

| 설정 | 효과 |
|---|---|
| `coordinator.executor_health_source=auto` | HA(self_report)면 **공유 `executor_status`(URL 키)** 를 부하 뷰로, 단일이면 monitor 폴링. executor는 `executor.advertise_url`로 자기 URL을 함께 self-report |
| `coordinator.executor_select=p2c` | **Power-of-Two-Choices**: 살아있는 후보 무작위 2개 중 덜 바쁜 쪽 — 랜덤화로 결정을 탈상관시켜 **분산 스탬피드** 억제(무상태·무락) |
| `coordinator.executor_reservation=true` | **TTL 보호 공유 예약**: dispatch 중 task를 `executor_reservation`에 예약 → 다른 coordinator가 `active_tasks + 예약`을 실시간 부하로 봄(엄격 균형). 죽은 coordinator의 예약은 `reservation_ttl_s`로 만료 |
| `coordinator.orphan_reconcile_interval_s` | **죽은 coordinator 정합**: 각 coordinator가 `coordinator_status`에 heartbeat하고, 소유자가 stale(`coordinator_stale_s`)인 비종료 job을 주기적으로 `FAILED`로 정합 → `retry`로 재개 |

표에 나온 용어들을 풀어 보겠습니다. **P2C(Power-of-Two-Choices, 두 후보 중 선택)**란 살아 있는 executor 중에서 무작위로 둘만 뽑아 그중 덜 바쁜 쪽에 일을 주는 방법입니다. **failover(장애 시 다른 곳으로 넘기기)**는 일을 맡기려던 곳이 응답하지 않을 때 다른 살아 있는 곳으로 옮겨 주는 동작이고, **TTL(time to live, 유효 기간)**은 어떤 정보가 일정 시간이 지나면 자동으로 사라지게 하는 장치입니다.

왜 가장 한가한 곳을 고르지 않고 굳이 P2C를 쓰는지 궁금할 수 있습니다. 그 이유는 다음과 같습니다.

**왜 P2C인가**: 만약 모든 coordinator가 그저 "가장 한가한 노드"를 고른다면, 부하 현황이 갱신되는 heartbeat 간격 동안 모두가 똑같이 한 노드를 한가하다고 보고 그곳으로 우르르 몰립니다(분산 herding, 떼지어 몰리는 현상). P2C는 무작위 두 후보 중 덜 바쁜 쪽을 고르므로 이 쏠림을 흩뜨려 줍니다. 분산 부하분산의 표준 해법으로 통하며, 상태나 잠금(lock)이 필요 없어 고가용성 환경에 잘 맞습니다.

또 하나, 예약이 영영 남아 새는 일을 어떻게 막는지도 짚어 둡니다.

**예약 누수 방지**: 예약은 `(executor_url, coordinator_id)` 쌍별로 기록되고 TTL이 지나면 만료됩니다. 그래서 어떤 coordinator가 갑자기 죽어도 그 예약이 영구히 남아 새지 않습니다. 게다가 결국에는 executor가 직접 보고하는 실제 `active_tasks`가 진실이므로, 예약은 잠깐의 치우침(bias)을 줄 뿐 오래 가지 않습니다.

- **왜 P2C인가**: heartbeat 간격 동안 단순 least-loaded는 모든 coordinator가 같은 한가한 노드로 몰린다(분산 herding). P2C는 분산 부하분산의 표준 해법으로, 무상태/무락이라 HA에 적합하다.
- **예약 누수 방지**: 예약은 `(executor_url, coordinator_id)`별로 기록되고 TTL로 만료되므로, coordinator가 죽어도 예약이 영구 누수되지 않는다. 또한 executor의 실제 self-report `active_tasks`가 결국 진실이라 예약은 짧은 bias일 뿐이다.

> 단일 coordinator면 기본값(`store.backend=memory`, `executor.self_report=false`, `executor_select=round_robin`) 그대로 두면 된다.

마지막으로 안심하셔도 되는 점 하나. 위 모든 이야기는 coordinator를 여러 대 둘 때의 고급 주제입니다. coordinator가 한 대뿐이라면 기본값(`store.backend=memory`, `executor.self_report=false`, `executor_select=round_robin`)을 그대로 두기만 하면 됩니다.

---

## 13. Local 모드

보통은 coordinator와 executor가 서로 다른 프로세스로 떠 있고 HTTP로 일을 주고받습니다. 그런데 개발하거나 검증할 때는 일꾼 프로세스를 따로 띄우는 것이 번거로울 수 있습니다. 이를 위한 것이 **local 모드**입니다.

`coordinator.executor_mode=local`(또는 환경변수 `COORDINATOR_EXECUTOR_MODE=local`)로 두면, executor 프로세스 없이 **coordinator 안에서 백엔드를 직접 호출**합니다(이 역할을 하는 것이 `LocalDispatcher`입니다). 덕분에 HTTP 디스패치나 원격 호출 없이도 실제 적재 동작까지 한 프로세스 안에서 검증할 수 있습니다. 만약 `greenplum.dsn`이 설정되어 있지 않으면 실제 입출력을 하지 않는 `MockBackend`로 자연스럽게 대체(폴백)됩니다. 중요한 점은, 이렇게 모드를 바꿔도 admission·상태·이력의 흐름은 remote와 똑같이 동작한다는 것입니다.

| `executor_mode` | 동작 |
|---|---|
| `remote` (기본) | executor 서비스에 HTTP(`POST /tasks`)로 디스패치 |
| `local` | coordinator 프로세스 안에서 백엔드를 직접 호출 |

---

## 14. 모니터링 & 대시보드

시스템이 잘 돌고 있는지 사람이 눈으로 확인할 수 있어야 합니다. 이 시스템은 메트릭, 대시보드, 로깅 세 가지로 그 가시성을 제공합니다.

먼저 **시스템 메트릭**입니다. coordinator와 executor 두 서비스 모두 `/metrics`에서 CPU·메모리·디스크 사용량과 동시 처리 현황을 내보냅니다. coordinator의 `HealthMonitor`는 각 executor를 폴링해 그 결과를 `/executors`와 `/cluster`로 모아 보여 주고, `monitor.db_dsn`이 설정되어 있으면 `executor_health_metrics` 테이블에도 기록합니다.

다음으로 두 종류의 **대시보드**가 있습니다. **coordinator 대시보드(`/`)**는 빌드 도구가 필요 없는 인라인 HTML로 되어 있고 3초마다 화면을 갱신하며, 처리중인 Query, 실행 이력, Executor, 환경설정, 그외 정보 탭으로 나뉩니다. **executor self-view 대시보드(`/`)**는 remote 모드에서 각 executor 프로세스가 자기 task와 메트릭, 이력을 스스로 보여 주는 화면입니다(처리중 Task, 실행 이력, 환경설정, 그외 정보). local 모드에서는 executor 프로세스가 따로 없으므로 자연히 coordinator 화면만 보이게 됩니다.

끝으로 **로깅**입니다. 로그는 `/data1/query-executor/logs`에 하루 단위로 롤링(날짜가 바뀌면 파일을 새로 만드는 방식)되어 쌓입니다. 모든 로그에는 어떤 작업·일감에 관한 것인지 알 수 있도록 `[job_id][task_id]` 컨텍스트가 자동으로 붙습니다. 특히 WARNING 이상의 경고는 별도의 `*-warn.log` 파일로 분리해(로거 이름까지 담은 강화 포맷으로) 운영 중에 문제만 빠르게 추적할 수 있게 했습니다.

- **시스템 메트릭**: 두 서비스 모두 `/metrics`(CPU/메모리/디스크 + 동시 처리). coordinator `HealthMonitor`가 executor를 폴링해 `/executors`·`/cluster`로 제공하고 `monitor.db_dsn` 설정 시 `executor_health_metrics`에 기록.
- **coordinator 대시보드(`/`)**: 인라인 HTML(빌드 불필요), 3초 폴링. 탭 — 처리중인 Query / 실행 이력 / Executor / 환경설정 / 그외 정보.
- **executor self-view 대시보드(`/`)**: remote 모드의 각 executor 프로세스가 자기 task/메트릭/이력을 노출(처리중 Task / 실행 이력 / 환경설정 / 그외 정보). local 모드에선 executor 프로세스가 없으므로 자연히 coordinator 화면만 보인다.
- **로깅**: `/data1/query-executor/logs`에 일 단위 롤링. 모든 로그에 `[job_id][task_id]` 컨텍스트 자동 주입. **WARNING 이상은 `*-warn.log`로 분리**(로거 이름 포함 강화 포맷)해 운영 중 문제만 빠르게 추적.

---

## 15. 실패 처리

분산 시스템에서는 무언가가 반드시 어딘가에서 실패합니다. 그래서 "실패했을 때 어떻게 행동하는가"가 시스템의 신뢰도를 좌우합니다. 아래 표는 일어날 수 있는 여러 상황과 그때의 처리 방식을 정리한 것으로, 이 시스템의 실패 대응 매뉴얼이라 할 수 있습니다. 왼쪽이 상황, 오른쪽이 그 대처입니다.

| 상황 | 처리 |
|---|---|
| 일부 task 실패 | `failure_policy`: `fail_fast`(Job FAILED) / `best_effort`(Job PARTIAL, 성공 task 적재 유지) |
| 적재 중 실패 | task 트랜잭션 rollback → 부분 적재 잔존 없음 |
| 과부하 | admission이 입구에서 `429`로 거부(`Retry-After`) → 클라이언트 재시도 |
| executor 동시 처리 full | executor 가 `POST /tasks`를 **202로 즉시 접수**하고 task 를 `QUEUED`로 내부 대기(세마포어). 에러 아님 — coordinator 는 폴링하며 기다린다(백프레셔) |
| executor 연결 실패 | **연결 계열 실패(`TransportError`/5xx)는 같은 executor 에 `task_max_retries`회 지수 백오프 재시도** → 소진 시 **다른 살아있는 executor 로 failover**(`task_failover`). 시작 전이라 항상 안전 |
| 실행 중 executor 유실 | 폴링 중 연결 끊김: **멱등(`overwrite_partitions`)이고 후보가 남았을 때만** 다른 executor 로 재실행. `append`는 중복 적재 위험이 있어 재배정하지 않고 FAILED |
| 취소 | Job cancel → 비종료 task의 executor에 `POST /tasks/{id}/cancel` 전파. 협조적 취소(QUEUED는 즉시, 실행 중은 현재 작업 후 `CANCELLED` 마감) |
| 타임아웃 | **접속은 `task_connect_timeout_s`(짧게), 전체는 `task_timeout_s`** 로 분리 적용 → 죽은 executor 에 오래 매달리지 않는다 |
| COPY 컬럼 불일치 | copy 모드 **사전검증(preflight)** 이 대용량 스트리밍 전에 SELECT↔대상 컬럼 불일치를 잡아 명확한 에러로 조기 실패(`copy.preflight=false`로 끌 수 있음) |
| 실패 파티션 재처리 | `POST /jobs/{id}/retry` 로 종료된 job의 **FAILED/CANCELLED task만** 새 job으로 재실행. copy 모드는 멱등(실패 task는 미커밋 / `overwrite_partitions` 선삭제)이라 안전 |
| coordinator 재시작 | `store.backend=file`(또는 postgres)이면 재기동 시 **중단된 job을 FAILED로 정합**(`reconcile_interrupted_jobs`) → `retry`로 실패 파티션만 재개 |
| executor 종료(SIGTERM) | **graceful drain**: 신규 task는 503으로 거부하고, 진행 중 task는 `shutdown_drain_timeout_s` 내에서 완료를 기다린 뒤 종료(강제 중단 안 함) |

표에서 특히 헷갈리기 쉬운 두 가지를 짚어 두겠습니다. "executor 연결 실패"와 "실행 중 executor 유실"은 비슷해 보이지만 다릅니다. 일을 보내려는데 연결이 안 되는 것(시작 전)은 아직 아무것도 적재하지 않았으니 언제든 다른 곳으로 옮겨도 안전합니다. 반면 일이 진행되던 도중에 연결이 끊긴 것(시작 후)은, 혹시 일부가 이미 적재됐을 수 있으므로 멱등이 보장되는 `overwrite_partitions`일 때만 다른 executor로 다시 맡깁니다. `append`는 다시 맡기면 같은 데이터가 두 번 들어갈 위험이 있어 재배정하지 않고 그냥 FAILED로 둡니다. 또 "백프레셔(backpressure)"란 일꾼이 바쁠 때 새 일을 에러로 떨구지 않고 잠시 줄 세워 두면, coordinator가 폴링하며 기다려 주는 식으로 부하가 자연스럽게 조절되는 것을 말합니다.

> 멱등성: `overwrite_partitions`는 task별 담당 파티션을 먼저 DELETE 후 COPY 하므로 같은 sub-query 재실행이 안전하다. executor 가 task 를 정상 접수해 `FAILED`로 보고한 **백엔드 오류는 재시도 대상이 아니다**(재시도해도 같은 결과). 재시도/failover 는 **연결 계열 실패에만** 발동한다.

마지막으로 재시도에 관한 중요한 원칙을 정리합니다. `overwrite_partitions`는 각 task가 맡은 파티션을 먼저 지우고 다시 채우므로 같은 sub-query를 다시 돌려도 안전합니다. 다만 executor가 task를 정상적으로 받아 실행했는데 백엔드(Impala/Greenplum)에서 난 오류로 `FAILED`를 보고한 경우는 다시 시도해도 같은 결과가 나오므로 자동 재시도 대상이 아닙니다. 자동 재시도와 failover는 오직 연결 계열의 실패에서만 작동합니다.

---

## 16. 기술 스택

이 시스템이 어떤 도구들로 만들어졌는지 한자리에 모았습니다. 각 영역마다 무엇을, 왜 골랐는지를 함께 떠올리며 보면 앞 절들의 설계 결정과 자연스럽게 연결됩니다.

| 영역 | 선택 |
|---|---|
| 언어/프레임워크 | Python 3.9+(RHEL 9.2 기본), **FastAPI**(coordinator·executor 공통) |
| SQL 파싱 | **sqlglot**(기본 `read="hive"`, 요청별 방언 재정의) |
| Impala 읽기 | **impyla**(HiveServer2, TLS+LDAP) + 배치 fetch |
| Greenplum 쓰기 | **psycopg** `COPY FROM STDIN` / INSERT |
| Coordinator↔Executor | **httpx**(AsyncClient) |
| 동시성 | asyncio + Semaphore(admission/디스패치) + thread pool(동기 DB 호출 래핑) |
| 상태/이력 저장 | 인메모리 dict / **파일 영속(JSON 스냅샷, 단일 노드 크래시 복구)** / **PostgreSQL**(`jobs`/`job_history`/`task_history`/`executor_status`/`executor_health_metrics`) |
| 대시보드 | 인라인 HTML + vanilla JS(빌드 도구 없음) |
| 배포 | /data1 트리 + 런처 스크립트로 coordinator 1 + executor N(`deploy/README.md`) |

요약하면, coordinator와 executor 모두 Python 3.9 이상에서 FastAPI로 만들어졌고, SQL 분석에는 sqlglot, Impala 읽기에는 impyla, Greenplum 쓰기에는 psycopg, 둘 사이의 통신에는 httpx를 씁니다. 동시성은 asyncio와 세마포어, 그리고 동기 DB 호출을 감싸는 스레드 풀로 다루며, 상태와 이력은 앞서 본 대로 인메모리·파일·PostgreSQL 중에서 고를 수 있습니다. 대시보드는 빌드 도구 없는 인라인 HTML과 순수 자바스크립트로 되어 있고, 배포는 런처 스크립트로 coordinator 한 대와 executor 여러 대를 띄우는 방식입니다(자세한 내용은 [deploy/README.md](deploy/README.md)).

---

## 17. 세그먼트 로컬 스테이징 파이프라인 (`local_stage`, `file://` 기반)

지금까지 본 `copy`/`stage_insert`는 executor가 Impala에서 읽은 데이터를 **자기 클라이언트 소켓 하나로** Greenplum에 `COPY`로 밀어 넣습니다. 데이터가 아주 클 때는 이 단일 소켓이 GP 진입점에서 직렬화되어 병목이 됩니다(자세한 진단은 [PERFORMANCE.md](PERFORMANCE.md) 참고). executor를 N대로 늘려도 각자 단일 COPY라 GP 쪽 진입 지점에서 다시 줄을 서게 됩니다.

이 절의 `local_stage` 모드는 그 병목을 **적재의 병렬성을 Greenplum 세그먼트로 옮겨** 해소합니다. 핵심 발상은 이렇습니다. executor를 **각 Greenplum 세그먼트 호스트 위에 함께 배치(co-locate)** 하고, 각 executor가 자기 몫의 Impala 결과를 **자기 호스트의 로컬 디스크에 CSV 파일**로 떨어뜨립니다. 그런 다음 Greenplum이 `file://` 외부테이블로 그 파일들을 읽는데, 이때 **각 세그먼트는 오직 자기 호스트의 로컬 파일만** 읽습니다. 즉 적재 시 네트워크를 타고 데이터가 오가는 셔플이 전혀 없고, 모든 세그먼트가 동시에 자기 로컬 디스크에서 읽어 들입니다. 이것이 "같은 디렉터리 경로, 노드마다 다른 파일"이라는 구조의 정체입니다.

> 왜 PXF가 아니라 `file://`인가: PXF의 `file:*` 프로파일은 **모든 GP 호스트에 동일하게 마운트된 공유 파일시스템**(NFS 등)을 전제하고, 파일을 세그먼트에 임의 분배한다. 그래서 "호스트마다 로컬 디스크에 서로 다른 파일"이라는 우리 구조와는 맞지 않는다(A 세그먼트가 B 호스트에만 있는 파일을 배정받아 못 읽는 상황이 생긴다). 내장 **`file://` 프로토콜**은 URI마다 **그 호스트의 primary 세그먼트 하나**를 그 로컬 파일에 배정하므로 이 구조와 정확히 일치한다.

### 17.1 토폴로지 — 세 계층의 분리

이 모드는 앞 절들과 배치(deployment)가 다릅니다. 특히 **coordinator는 GP master가 아니며**, executor는 **반드시 세그먼트 호스트 위**에 있어야 합니다.

| 계층 | 위치 | 역할 |
|---|---|---|
| **Coordinator** | GP master와 분리된 **독립 컨트롤 노드** | Impala SELECT 검증·분할 → export task 팬아웃 → 배리어 → **GP master에 클라이언트로 접속**해 `file://` 외부테이블 load SQL 실행 → cleanup 지시. 대량 데이터는 통과하지 않는다 |
| **Executor** | **각 GP 세그먼트 호스트에 co-locate**(호스트당 여러 개 가능) | coordinator가 지정한 partition 슬라이스를 impyla로 읽어 **자기 호스트 로컬 디스크의 지정된 파일 경로**에 CSV write. cleanup 때 자기 로컬 파일 삭제 |
| **GP Master** | 별개 노드 | coordinator가 던진 외부테이블 DDL/INSERT를 받아 세그먼트에 분배. 각 세그먼트는 `file://호스트/...`로 **자기 호스트 로컬 CSV**를 읽는다 |

coordinator가 export 팬아웃과 GP load를 모두 지휘하지만, 실제 DB 분배는 GP master가 하고 coordinator는 그 master에 **클라이언트로 접속**할 뿐이라는 점이 중요합니다(coordinator는 이미 greenplum에 직접 접속하는 경로를 갖고 있습니다). 즉 "coordinator가 master 역할처럼 보이지만 master는 아니다"가 이 배치의 핵심입니다.

```mermaid
flowchart TB
    Client([Client])
    Impala[(Impala<br/>source)]
    GPM[(Greenplum<br/>Master)]

    subgraph Ctrl["Coordinator (독립 컨트롤 노드 · master 아님)"]
        API["REST API + 분할 + 2-phase 오케스트레이션"]
    end

    subgraph Seg1["Segment Host h1"]
        Ex1["Executor(들)"]
        D1[["local dir<br/>{job}/f0.csv, f1.csv ..."]]
        SG1["primary seg 0..S1"]
    end
    subgraph Seg2["Segment Host h2"]
        Ex2["Executor(들)"]
        D2[["local dir<br/>{job}/fk.csv ..."]]
        SG2["primary seg 0..S2"]
    end

    Client -- "SELECT + partition_column + local_stage" --> API
    API -- "① export task (sub-query + out_path)" --> Ex1 & Ex2
    Ex1 & Ex2 -- "② impyla read" --> Impala
    Ex1 --> D1
    Ex2 --> D2
    API -- "④ file:// 외부테이블 + INSERT (배리어 후 1회)" --> GPM
    SG1 -- "⑤ 로컬 read" --> D1
    SG2 -- "⑤ 로컬 read" --> D2
    GPM --- SG1 & SG2
```

### 17.2 `file://` 규칙과 파일 레이아웃 계획

이 파이프라인의 뼈대는 `file://` 프로토콜의 두 규칙이 결정합니다.

1. 각 URI `file://<hostname>/<path>`는 **그 호스트의 primary 세그먼트 하나**가 그 파일 하나를 읽게 배정한다.
2. **호스트당 파일 수는 그 호스트의 primary 세그먼트 수(S_h)를 넘을 수 없다.**

그래서 coordinator는 다음과 같이 파일 배치를 계획합니다.

1. **참여 호스트 목록** `H = {h1..hk}` 확정 — executor가 self-report한 GP hostname을 `gp_segment_configuration.hostname`과 대조해 검증한다.
2. **호스트별 파일 예산** `S_h` 산정 — `SELECT hostname, count(*) FROM gp_segment_configuration WHERE content>=0 GROUP BY hostname`.
3. **총 파일 수** `F = Σ S_h` → Impala IN 리스트를 `F`개의 **disjoint** 슬라이스로 분할(기존 splitter 재사용, `parallelism=F`).
4. 각 슬라이스를 `(호스트 h, 파일 인덱스 i)`에 배정하고, **그 호스트 위의 executor 중 하나**에 "이 sub-query를 `{local_dir}/{job_id}/f{i}.csv`로 써라"고 디스패치한다.
5. 모든 파일이 준비되면 URI 목록을 조립한다: `file://h1/{dir}/{job}/f0.csv`, `file://h1/.../f1.csv`, …, `file://hk/.../f{F-1}.csv`.

**호스트당 executor가 여러 개**인 경우: 그 호스트의 파일 예산 `S_h`를 executor들에게 나눠 배정합니다(예: `S_h=8`, executor 2개 → 각 4파일). 파일이 그 호스트 로컬 디스크에 있기만 하면 어느 executor가 썼는지는 무관하고, **호스트별 총 파일 수가 `S_h`를 넘지 않게** 하는 것만 지키면 됩니다. 그 결과 **Phase 1(export) 병렬도는 executor 수**로, **Phase 2(load) 병렬도는 파일 수(=참여 세그먼트 수)**로 각각 독립적으로 최대화됩니다.

**정합성**: 슬라이스가 disjoint(splitter 보장)하고 각 파일이 URI 하나에만 참조되므로, 전체 데이터는 정확히 한 번 읽힙니다(중복·누락 없음). 어느 호스트 파일에 어떤 파티션이 담기든 무관합니다 — staging→target INSERT에서 target의 분배키(`DISTRIBUTED BY`)로 다시 분배되기 때문입니다.

### 17.3 Job 라이프사이클 — 2-phase(배리어)

`local_stage`는 지금까지의 "디스패치 → 상태 집계"와 달리, **모든 export가 끝나기를 기다리는 배리어**와 그 뒤의 **job 단위 GP load 단계**가 추가됩니다. 이것이 이 모드가 요구하는 신규 오케스트레이션입니다.

```mermaid
stateDiagram-v2
    [*] --> EXPORTING: export task F개 팬아웃(호스트별 executor가 로컬 CSV write)
    EXPORTING --> LOADING: 모든 export DONE (배리어)
    LOADING --> INSERTING: file:// 외부테이블 → staging 적재(세그먼트 로컬 병렬 read)
    INSERTING --> CLEANUP: target INSERT 커밋
    CLEANUP --> [*]: 외부테이블 DROP · staging 정리 · 각 호스트 로컬 파일 삭제
    EXPORTING --> FAILED: export 실패(정책)
    LOADING --> FAILED: 외부테이블/적재 오류
    INSERTING --> FAILED: INSERT 오류
```

**Phase 2에서 coordinator가 GP master에 실행하는 SQL**(한 트랜잭션):

```sql
CREATE EXTERNAL TABLE ext_{job} (<external_columns>)
  LOCATION ('file://h1/{dir}/{job}/f0.csv', 'file://h1/{dir}/{job}/f1.csv', ...,
            'file://hk/{dir}/{job}/f{N}.csv')
  FORMAT 'CSV' ( DELIMITER '`' NULL '' QUOTE '"' );      -- ← 구분자 기본 backtick(설정 가능)

INSERT INTO staging_{job} SELECT * FROM ext_{job};        -- 각 세그먼트가 자기 로컬 파일 병렬 read
-- write_mode=overwrite_partitions면 같은 트랜잭션에서 선삭제:
-- DELETE FROM target WHERE <partition_column> IN (...);
INSERT INTO target SELECT ... FROM staging_{job};         -- 변환/분배키/멱등
DROP EXTERNAL TABLE ext_{job};                            -- staging 은 TRUNCATE 또는 DROP
```

- **Phase 1 — Export(병렬)**: `F`개 export task를 호스트별 executor에 디스패치. 각 executor는 impyla 배치 읽기(§4의 스트리밍)를 그대로 쓰되, sink만 `COPY` 대신 **로컬 CSV writer**다. 완료되면 배리어에서 합류한다.
- **Phase 2 — Load(1회)**: coordinator가 GP master에 위 SQL을 실행. 데이터는 coordinator를 통과하지 않고, 세그먼트가 로컬 파일을 직접 읽는다.
- **Phase 3 — Cleanup**: 각 executor에 로컬 `{job_id}` 디렉터리 삭제를 지시(`stage.cleanup=false`면 디버깅용으로 보존).

### 17.4 요청 필드 — 스키마는 **명시** 방식

`local_stage`는 `stage_insert`와 같은 계약을 따릅니다. 즉 외부테이블 컬럼 정의와 최종 INSERT 문을 **요청자가 명시**합니다(자동 추론은 두지 않음 — 컬럼 타입/캐스팅을 요청자가 통제).

| 필드 | 필수 | 설명 |
|---|---|---|
| `exec_mode="local_stage"` | ✓ | 이 파이프라인 선택 |
| `external_columns` | ✓ | `file://` 외부테이블 컬럼 정의(예: `"user_id int, amount numeric, dt date"`). CSV 컬럼 순서와 일치해야 함 |
| `staging_table` | ✓ | 적재 대상 staging 힙 테이블(job별 고유 권장). `staging_ddl` 미지정 시 이미 존재해야 함 |
| `staging_ddl` | 선택 | staging 생성 DDL(보통 `CREATE TABLE ... DISTRIBUTED BY (...)`). 없으면 생성 건너뜀 |
| `insert_sql` | ✓ | `INSERT INTO <target> SELECT ... FROM <staging_table>` — 변환/컬럼 매핑 담당 |
| `partition_column` | ✓ | 분할 기준(§8). `overwrite_partitions` 선삭제에도 사용 |
| `export_local_dir` | 선택 | 로컬 저장 경로 오버라이드(기본 `stage.local_dir`, 모든 호스트 동일) |
| `csv_delimiter` 등 | 선택 | CSV 방언 오버라이드(기본은 아래 설정값) |

> LOCATION의 `file://` URI, `FORMAT 'CSV'(...)` 절, 파일 인덱스는 **coordinator가 조립**하므로 요청자가 신경 쓸 필요가 없습니다. 요청자는 컬럼 정의(`external_columns`)와 최종 INSERT(`insert_sql`)만 책임집니다.

### 17.5 CSV 방언 — 기본 구분자 backtick(`` ` ``)

executor가 쓰는 CSV의 방언과 GP 외부테이블 `FORMAT 'CSV'(...)`의 방언은 **정확히 일치**해야 합니다. 어긋나면 오류 없이 데이터가 조용히 오염될 수 있으므로, 양쪽을 **설정 단일 소스**에서 강제합니다. 기본 컬럼 구분자는 데이터에 잘 나타나지 않는 **backtick(`` ` ``)** 이며, 설정으로 바꿀 수 있습니다.

| 설정 | 기본값 | 의미 |
|---|---|---|
| `stage.csv_delimiter` | `` ` `` (backtick) | 컬럼 구분자(1바이트). executor write 와 외부테이블 `DELIMITER` 에 공통 적용 |
| `stage.csv_null` | `` (빈 문자열) | NULL 표현. 외부테이블 `NULL` 절과 일치 |
| `stage.csv_quote` | `"` | 인용 문자(`FORMAT 'CSV'`) |

### 17.6 실패 처리 · 멱등성 · 정리

- **job 전용 네임스페이스**: `{local_dir}/{job_id}/` + `staging_{job_id}` + `ext_{job_id}` → 재실행(`retry`)해도 충돌 없음.
- **export 재시도**: 실패한 export task를 재실행하면 같은 호스트의 같은 파일명을 덮어쓰므로 파일 단위 멱등이다.
- **Phase 2 원자성**: 외부테이블 적재 → (선삭제) → target INSERT를 **한 GP 트랜잭션**으로 묶는다. `overwrite_partitions`는 `DELETE ... WHERE <partition_column> IN (...)` 선삭제로 §9의 멱등 패턴을 그대로 따른다.
- **정리 시점**: target INSERT가 성공 커밋된 뒤에만 외부테이블 DROP·staging 정리·로컬 파일 삭제.
- **부분 실패**: export 실패 → 해당 파일 폐기 후 재시도. 외부테이블 read 중 타입 불일치 → `external_columns` 정의가 방어선.

### 17.7 제약 · 전제

- **포맷은 CSV/TEXT**(Parquet 아님). `file://`·내장 프로토콜은 텍스트 계열만 지원한다(Parquet 로컬 읽기는 공유 FS + PXF 전용).
- **hostname 매칭**: executor가 self-report하는 GP hostname이 `gp_segment_configuration.hostname`과 정확히 일치해야 URI가 파일을 찾는다.
- **파일 권한**: 외부테이블 read는 세그먼트 postgres 프로세스 사용자(보통 gpadmin)로 로컬 파일을 연다 → executor가 쓴 파일이 그 사용자에게 읽기 가능해야 한다(공유 umask/소유권 합의).
- **호스트당 파일 수 ≤ 세그먼트 수** — coordinator가 `S_h`로 상한을 강제한다.
- **mirror failover**: primary 세그먼트가 다른 호스트의 mirror로 넘어가면 그 로컬 파일이 없으므로 load가 실패한다 → 재시도 정책 대상.
- **배포 변경**: 이 모드는 executor가 세그먼트 호스트에 co-locate되어야 하므로, `remote` 배치(독립 executor 풀)와 배포 형태가 다르다([deploy/README.md](deploy/README.md)에 별도 배치로 기술).

### 17.8 설정 키

| 설정(프로퍼티) | 기본값 | 의미 |
|---|---|---|
| `stage.local_dir` | `/data1/query-executor/stage` | 로컬 CSV 저장 루트(모든 호스트 동일 경로) |
| `stage.csv_delimiter` / `stage.csv_null` / `stage.csv_quote` | `` ` `` / `` / `"` | CSV 방언(§17.5) |
| `stage.files_per_host` | `0`(자동=`S_h`) | 호스트당 파일 수 상한. 0이면 세그먼트 수로 자동 |
| `stage.cleanup` | `true` | Phase 3에서 로컬 파일/외부테이블/staging 정리 여부 |
| `executor.gp_hostname` | OS hostname | executor가 self-report할 GP 세그먼트 hostname(`gp_segment_configuration`과 일치) |

### 17.9 구현 매핑(코드 통합 지점, 구현됨)

- **exec_mode 확장**: `"local_stage"`를 `CreateJobRequest`/`Job`/`CreateTaskRequest`의 `exec_mode`에 추가.
- **executor(Phase 1)**: `_run`의 exec_mode 분기에 `local_stage` 갈래 → 백엔드 `export_to_local_csv(sub_query, out_path, csv_options, ...)`. `move`의 impyla 배치 읽기 루프를 재사용하고 sink만 표준 `csv` 로컬 writer로 교체(기본 구분자 backtick). 로컬 정리용 `POST /stage/{job_id}/cleanup` 엔드포인트 신설.
- **host 매핑(gp_hostname)**: executor 가 `_gp_hostname()`(`executor.gp_hostname` 설정 우선, 없으면 OS hostname)을 `/metrics`·`/info`로 보고. coordinator 의 `_resolve_hosts()`가 이를 수집해(HttpDispatcher는 `/metrics` 조회+URL별 캐시, 실패 시 URL 호스트 폴백) `file://` URI 의 호스트로 쓴다 — HTTP URL 호스트(IP/별칭) ≠ GP 호스트명 문제를 바로잡는다.
- **파일 예산 배분(`files_per_host ≤ S_h`)**: Phase 1 디스패치 전 `_plan_local_stage()`가 `backend.segment_host_counts()`(`{host: S_h}`)와 executor→host 매핑으로 `stage.plan_file_budget()`을 돌려, 각 파일(=export task)을 호스트당 `S_h`(또는 `min(S_h, stage.max_files_per_host)`)를 넘지 않게 배분하고 `executor_url`/`out_path`를 재확정한다(호스트 내 executor 라운드로빈). 총 파일 수가 예산(Σ S_h)을 넘으면 배치 불가 → job FAILED. 토폴로지/executor 미상(목·로컬)이면 기존 배정 유지.
- **호스트 검증**: Phase 2 직전 `backend.segment_hosts()`(`gp_segment_configuration`)로 매핑된 호스트가 실재하는지 확인(`stage.validate_hosts`, 기본 on). 없으면 load 전에 조기 실패.
- **coordinator(Phase 2·3)**: 디스패처 `run()`의 배리어(`_execute` 반환) 뒤 `_run_stage_load()`가 GP master에 외부테이블 DDL→staging 적재→(멱등 선삭제)→target INSERT를 한 트랜잭션으로 실행하고 `_cleanup_stage()`로 각 executor 로컬 파일을 정리한다. `file://` URI·`FORMAT 'CSV'(...)`·파일 인덱스 조립은 `coordinator/stage.py`(순수 함수)가 담당. `finalize_job`은 local_stage를 원자 적재로 보아 실패 시 정책 무관 FAILED.
- **테스트**: `tests/test_local_stage.py` — stage.py 순수 함수, executor 라우팅/cleanup/metrics, LocalDispatcher 2-phase e2e, gp_hostname 매핑·검증까지 실 DB·실 디스크 없이 검증.

---

## 18. 쿼리 템플릿 엔진 (Query Template Engine)

지금까지는 클라이언트가 **완성된 SQL 전문**(SELECT·STAGING DDL·INSERT)을 JSON 에 담아 보냈습니다. 이 방식은 클라이언트마다 SQL 조립 로직이 흩어지고, 쿼리를 바꾸려면 모든 클라이언트를 다시 배포해야 하며, 표준화·감사가 어렵다는 문제가 있었습니다. 이를 해결하기 위해, **SQL 을 서버의 템플릿 파일로 보관하고 클라이언트는 파라미터만 보내는** 템플릿 엔진을 추가했습니다.

### 18.1 핵심 아이디어

한마디로 "쿼리는 서버에 두고, 클라이언트는 값만 보낸다"입니다. `POST /jobs` 처리 초입에서 coordinator 가 `template_id` 로 지정된 서버 템플릿을 `params` 로 **런타임 렌더링**해 완성된 SQL 을 만들고, 그 결과를 기존 요청 필드(`sql`/`staging_ddl`/`insert_sql`/`external_columns`/`wrapper_query`)에 그대로 주입합니다. 그 뒤의 검증(parser)·분할(splitter)·디스패치 파이프라인은 **하나도 바뀌지 않습니다** — 렌더는 얇은 선행 단계일 뿐입니다.

```mermaid
flowchart LR
    C([Client: template_id + params]) --> R[템플릿 렌더<br/>coordinator/template.py]
    T[(서버 템플릿 파일<br/>manifest.yml + *.sql.j2)] --> R
    F[커스텀 함수<br/>template_funcs.py] --> R
    R -->|sql/staging_ddl/insert_sql 주입| V[validate_and_parse]
    V --> S[split] --> D[dispatch]
```

파티션 `IN` 분할과도 자연스럽게 합성됩니다: 템플릿이 `WHERE dt IN ( {{ date_range(start_dt, end_dt) | sql_in }} )` 처럼 IN 목록을 만들고, splitter 가 그 목록을 N분할합니다.

### 18.2 템플릿 저장 구조

`template.dir`(기본 `/data1/query-executor/config/templates`, 개발 시 `packaging/config/templates`) 아래 **`<template_id>/` 디렉터리 하나가 하나의 이관 시나리오**입니다.

```
<template_dir>/sales_migration/
  manifest.yml          # 메타 + 파라미터 스키마 + 조각 파일 매핑
  select.sql.j2         # SELECT (파티션 IN 포함)
  staging_ddl.sql.j2    # (선택) staging DDL
  insert.sql.j2         # (선택) staging→target INSERT
```

`manifest.yml` 은 실행 스칼라 기본값(`exec_mode`·`partition_column`·`target_table`·`staging_table`·`write_mode` 등), 파라미터 스키마(`params`: 이름/타입/필수/기본값), role→파일 매핑(`files`)을 담습니다. manifest 스칼라는 **요청이 명시하면 요청이 이기고, 없으면 기본값**이 쓰이므로(요청 `model_fields_set` 로 구분), 클라이언트는 `template_id`+`params` 만으로도 완전한 작업을 만들 수 있습니다.

`exec_mode` 별 필요한 조각(role):

| exec_mode | 필수 role | 선택 role |
|---|---|---|
| `copy` / `statement` | `select` | `wrapper` |
| `stage_insert` | `select`, `insert` | `staging_ddl` |
| `local_stage` | `select`, `insert`, `external_columns` | `staging_ddl` |

> `stage_insert` 는 관례상 렌더된 INSERT 를 `wrapper_query` 에 싣고(executor 계약), `local_stage` 는 `insert_sql` 에 싣습니다 — §9·§17 의 기존 필드 계약을 그대로 따릅니다.

### 18.3 엔진과 커스텀 함수

- **엔진**(`coordinator/template.py`, `TemplateEngine`): Jinja2 `SandboxedEnvironment` + `StrictUndefined`(미정의 변수 즉시 실패, 위험 속성 접근 차단), `autoescape=False`(HTML 이 아닌 SQL). 단일 워커 전제라 in-process 캐시가 안전하며, `template.auto_reload` 로 개발 중 변경을 반영합니다. `create_app` 에서 1개 생성해 주입합니다.
- **커스텀 함수**(`coordinator/template_funcs.py`): `@template_filter`/`@template_global` 데코레이터로 등록하는 레지스트리. 내장 SQL 안전 필터(`sql_str`·`sql_in`·`sql_num`·`sql_ident`)와 도메인 글로벌(`date_range`)을 제공합니다. 설정 `template.func_modules`(쉼표 구분 import 경로)에 모듈을 지정하면 엔진 기동 시 import 되어 앱 코드 수정 없이 함수를 추가할 수 있습니다.

### 18.4 보안

템플릿 파일은 **서버 신뢰 자산**, 파라미터는 **비신뢰 입력**입니다. 두 층으로 방어합니다.

1. **경로 탈출 차단**: `template_id` 는 영숫자/`_`/`-` 만 허용(`TEMPLATE_ID_INVALID`).
2. **SQL 인젝션 방지**: 파라미터는 반드시 `sql_str`/`sql_in`/`sql_ident`/`sql_num` 필터를 거쳐 이스케이프·검증합니다(`sql_num` 은 비숫자 거부, `sql_in` 은 빈 목록을 안전한 `NULL` 로).
3. **렌더 후 재검증**: 렌더된 SELECT 는 이후에도 `validate_and_parse` 를 통과해야 하므로, 다중 문/비-SELECT 인젝션은 기존 검증(`MULTIPLE_STATEMENTS`/`NOT_A_SELECT`)에서 걸러집니다.
4. **DDL/INSERT 단일 문 검사**: parser 를 타지 않는 DDL/INSERT 조각은 `template.validate_ddl_single_stmt`(기본 on)로 `;` 다중 문을 차단합니다(`TEMPLATE_MULTIPLE_STATEMENTS`).

### 18.5 API·감사·재현

- `POST /jobs`: `template_id`+`params` 를 받으면 렌더 후 기존 흐름으로 실행. `dry_run=true` 면 렌더된 SQL 계획만 반환(실행·저장 없음). 렌더/검증 실패는 `422 + error_code`(`TEMPLATE_NOT_FOUND`/`TEMPLATE_PARAM_ERROR`/`TEMPLATE_RENDER_ERROR` 등).
- `GET /templates`: 사용 가능한 템플릿 목록(설명·기본 exec_mode·파라미터 스키마)을 반환 — 클라이언트가 이 스키마를 보고 `params` 를 구성합니다.
- **감사·재현**: `Job` 에 `template_id`·`template_params` 를 저장하고, 렌더된 SELECT 전문은 `original_sql` 에 그대로 보관합니다. retry 는 이미 저장된 sub_query 를 재사용하므로 **재렌더 없이** 동작합니다.

### 18.6 하위 호환

`template_id` 를 주지 않으면 기존 raw-SQL 방식이 **완전히 그대로** 동작합니다(`sql`/`partition_column`/`target_table` 을 요청에 직접 담는 방식). 두 방식 모두 공통 필수 필드(`sql`·`partition_column`·`target_table`)가 렌더/병합 후 비어 있으면 `422 MISSING_REQUIRED_FIELDS` 로 거부합니다.

**테스트**: `tests/test_template.py` — 커스텀 함수/인젝션 이스케이프, 엔진 렌더(파라미터 검증·exec_mode 별 조각·단일 문 검사·경로 탈출), API 통합(예제 템플릿 `sales_migration`), 하위 호환까지 실 DB 없이 검증.

### 18.7 결과 반환 실행 (`POST /query-execute`)

`POST /jobs` 가 **이관**(Impala→Greenplum, 결과가 coordinator 를 거치지 않음)인 반면, `POST /query-execute` 는 같은 템플릿을 렌더한 SELECT 를 실행해 **결과(상위 N행)를 클라이언트에 동기로 돌려주는** 미리보기성 실행입니다. 사실상 §18 템플릿 엔진과 `/datasources` 미리보기(`core/dbprobe.py`)를 합친 것입니다.

- **요청**: `template_id` + `params`(이름-값 항목 **배열** `[{name, value}, ...]`) + `datasource`(선택, 미지정 시 `source.type`) + `limit`(1~10000). 배열은 내부에서 `{name: value}` dict 로 접혀 기존 렌더 경로(`ParamSpec` 검증·`sql_in` 이스케이프)를 그대로 탑니다. 같은 이름이 두 번 오면 `422 DUPLICATE_PARAM`.
- **렌더**: `TemplateEngine.render_query()` 가 **`select` 조각만** 렌더합니다(이관용 `render()` 와 달리 exec_mode 별 insert/staging 조각을 요구하지 않아 어떤 템플릿이든 동작). 렌더된 SELECT 는 `validate_select_query()` 로 **단일 행 반환 SELECT** 인지 검증합니다(다중 문·비-SELECT 차단; 값은 이미 템플릿 필터로 이스케이프되지만 구조 방어를 한 겹 더 둠).
- **실행 라우팅 — 클라이언트는 executor 를 지정하지 않는다(2갈래로 통일)**: `greenplum`/`history`(메타/타깃 DB)는 coordinator 가 직접(psycopg) 실행하고, **그 외 소스(`impala`/`trino`/`source`)는 datasource 종류와 무관하게 executor 의 `POST /query-run` 하나로 통일 위임**합니다(초기엔 impala 만 built-in 을 타는 비대칭이 있었으나 제거). 대상 executor 는 클라이언트가 지정하지 않고, coordinator 가 **`/jobs` 디스패치와 동일한 선택 정책**(`coordinator.executor_select` — `least_loaded`/`p2c` 면 '가장 한가한', 기본 `round_robin` 이면 회전)으로 고릅니다. 연결(transport) 실패 시 다음 executor 로 failover 하며(SELECT 는 멱등), executor 가 도달 후 돌려준 4xx/5xx(SQL 오류·함수 미설정)는 확정 응답이므로 failover 없이 그대로 전달합니다. 실제 실행한 executor 는 응답 `executed_by` 로 관측할 수 있습니다(직접 실행이면 null).
- **소스 실행 = 커스텀 함수 위임(`/query-run`)**: query-execute 의 소스 실행은 executor 가 소스(Trino 등)를 **직접 접속하지 않고**, 설정으로 지정한 외부 Python 함수에 위임합니다(`run_trino_select` 직접 호출 제거). executor 의 `POST /query-run` 이 `query.func.module`(dotted path, `importlib` 로딩·캐시)로 함수를 찾아 `run(sql, config=<query.func.config.* dict>, limit)` 를 호출하고, 반환된 `QueryResult`(또는 동일 키 dict)를 그대로 응답합니다. **설정은 config.properties 에서 자유 정의** — `query.func.config.<키>=<값>` 을 프리픽스로 모아(코드/`config.yml` 수정 없이) 함수에 dict 로 넘깁니다(값은 문자열, 형변환은 함수 책임; `core/config.py` 의 `_collect_prefix`). 미설정 시 400, 로드/실행 실패 시 502. 참조 구현: `examples/query_funcs/trino_runner.py`. (임의 SQL 미리보기 `/datasources/{name}/query`(§B 운영 점검용)와 이관 `_source_connect` 의 소스 접속은 별개로 built-in 유지.)
- **응답**: `{template_id, datasource, sql(감사용 렌더 SQL), columns, rows, row_count, truncated, limit, elapsed_ms, executed_by}` — `columns`/`rows`/… 는 `dbprobe.QueryResult` shape 과 동일하고, `executed_by` 는 실제 실행 executor URL(직접 실행이면 null)입니다. `datasource` 는 coordinator 가 확정값으로 싣습니다(`/query-run` 응답엔 datasource 가 없어 보정).
- **이관 소스와의 분리**: `datasource` 를 생략하면 전역 `source.type` 을 기본으로 쓰지만, 요청에 명시하면 그 소스로 라우팅됩니다. 이 덕분에 "이관(`/jobs`)은 Impala 읽기 → Greenplum 적재, query-execute 는 Trino(커스텀 함수) 실행" 처럼 기능별로 소스를 나눌 수 있습니다 — `source.type=impala` 로 두고 query-execute 요청에 `datasource:"trino"` 를 명시하면 됩니다.
- **경계**: 결과가 coordinator 메모리를 거치므로 `limit`(≤10000)으로 응답 크기를 강제하는 **미리보기 규모 전용**입니다. 대량 이관은 계속 `/jobs`. 또한 executor 의 `/query-run`·`/datasources/{name}/query` 는 task 세마포어(`max_concurrent_tasks`)를 거치지 않으므로, 무거운 사용이 예상되면 별도 동시성 가드를 후속으로 고려합니다.

**테스트**: `tests/test_query_execute.py`(render_query·coordinator 직접 실행·trino→/query-run 프록시·failover·오류 전파·에러 경로) + `tests/test_datasource_query.py`(executor `/query-run` 커스텀 함수 호출·dict 반환·미설정 400·함수 예외 502·`_load_query_func` 로딩·`_collect_prefix` 자유 설정 수집)를 실 DB 없이 검증.

### 18.8 날짜 태스크 컬럼 fan-out (`/jobs`, IN 분할 대체)

기본 분할(§8)은 파티션 컬럼의 `IN` 값 목록을 N등분합니다. 여기에 더해, `/jobs`(이관)는 **날짜 태스크 컬럼 기반 fan-out** 모드를 지원합니다 — `IN` 을 쓰지 않고 **날짜 하나 = task 하나**로 펼쳐, executor 마다 하루치를 맡깁니다(일별 배치 이관에 자연스러운 모델).

- **요청**: 템플릿 stage_insert 에 `task_column`(날짜 컬럼) + `task_range`(오늘 기준 상대 일수, **양끝 포함**)를 추가합니다. 예: `task_column:"dt", task_range:[-7,0]` → 오늘 포함 8일. `partition_column`/`parallelism`/`split_strategy` 는 이 모드에서 쓰이지 않습니다(task 수 = 날짜 수).
- **분할**(`coordinator/app.py` `_build_fanout`, IN 파싱·`split` 우회): 서버 오늘(KST) 기준으로 날짜 목록을 만들고(`_compute_task_dates`), **날짜마다 SELECT 조각만** `render_query()` 로 렌더해(컨텍스트에 `task_column`·`task_date` 주입) 하루치 sub-query 를 만듭니다. `INSERT`/`staging_ddl` 은 **날짜 독립**이라 대표 날짜로 **1회** 렌더해 job-level 로 공유합니다(§18.5 의 per-task `sub_query` / job-level 나머지 plumbing 그대로). 각 task 의 `partition_values` 에는 그 날짜를 담습니다(관측/표시용).
- **적재 방식**: stage_insert 는 **append** 입니다(그 날짜 SELECT → staging(TEMP) COPY → target INSERT). 하루 단위 재실행 멱등이 필요하면 대상 테이블을 job 밖에서 미리 비우거나(TRUNCATE 등) 날짜별 물리 테이블을 씁니다 — 프레임워크는 대상에 DELETE 를 하지 않습니다.
- **템플릿 계약**: SELECT 조각은 `WHERE {{ task_column | sql_ident }} = {{ task_date | sql_str }}` 처럼 하루치를 조회하고, INSERT/staging 은 날짜를 참조하지 않습니다. 예제: `packaging/config/templates/daily_sales/`.
- **예시**(today=2026-07-10, `[-7,0]`): `2026-07-03 … 2026-07-10` = 8 task, executor 당 1일. 각 task: 그 날짜 SELECT → staging(TEMP) COPY → INSERT.

**테스트**: `tests/test_task_fanout.py` — `_compute_task_dates`(양끝 포함·역순·포맷·오류), dry-run(날짜별 1 task·날짜별 partition_values·IN 없음·INSERT 공유), 템플릿/exec_mode/range 검증(422).

---

## 19. 향후 확장

마지막으로, 지금은 없지만 앞으로 더할 만한 기능들을 적어 둡니다. 이 목록은 시스템이 어느 방향으로 발전하려 하는지를 보여 줍니다. 각 항목을 한 줄로 풀어 설명하면 다음과 같습니다.

- **실행 중 즉시 취소**: 지금은 진행 중인 작업을 곧장 끊기 어려운데, 백엔드 커서를 취소(`cursor.cancel()`)하고 트랜잭션을 rollback해 진행 중이던 Impala 읽기와 COPY를 즉시 멈출 수 있게 한다.
- **헬스 기반 executor 선택**(Phase 1·2·3 구현 완료): `coordinator.executor_select=least_loaded|p2c`로 **초기 배정**과 **failover 순서**를 헬스/부하 기반으로 정한다(HA는 분산 스탬피드를 피하는 **P2C** 권장). HA 고도화로 **공유 self-report(URL 키 부하 뷰)·TTL 보호 공유 예약·죽은 coordinator 소유 job 정합**까지 지원한다 — §12 참고(`coordinator/selector.py`·`reservation.py`·`ha.py`).
- **append 모드 재실행 안전화**: 현재 폴링 중 유실은 멱등(`overwrite_partitions`)일 때만 재배정한다. task 단위 staging+swap 등으로 `append`도 안전 재실행 가능하게.
- **callback 기반 상태 전파**: polling 대신 executor→coordinator 콜백으로 부하 제거.
- **집계/GROUP BY 쿼리 지원**: 소스 측 사전 집계 후 적재 또는 적재 후 재집계.
- **IN 절 자동 합성**: IN이 없을 때 Impala `SHOW PARTITIONS`로 값 조회 후 합성.
- **read/write 파이프라이닝 및 COPY 병렬도 튜닝**으로 throughput 최적화.

위 항목 가운데 "헬스 기반 executor 선택"은 이미 Phase 1·2·3에 걸쳐 구현이 끝났으며, 그 자세한 동작은 §12에서 다뤘습니다. 나머지는 아직 구상 단계의 확장 방향으로 이해하면 됩니다.
