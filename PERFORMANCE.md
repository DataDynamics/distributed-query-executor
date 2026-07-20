# 성능 · 확장(Scale Out) · 고가용성(HA) 가이드

이 문서는 처리량을 어떻게 늘리고(이를 "Scale Out", 우리말로 수평 확장이라고 부릅니다),
장애가 나도 서비스가 멈추지 않게 어떻게 견디며(이를 "HA", High Availability 즉 고가용성이라고
합니다), 그 과정에서 어떤 파라미터를 어떤 기준으로 잡아야 하는지를 운영하는 사람의 눈높이에서
차근차근 풀어 설명합니다. 시스템의 구조나 상태머신 같은 설계 배경이 궁금하다면
[DESIGN.md](DESIGN.md) 를, 설정을 실제로 적용하는 방법이 궁금하다면
[README.md](README.md) 와 [packaging/README.md](packaging/README.md) 를 함께 보면 좋습니다.

처음 시작하기 전에 한 가지 약속만 기억해 두면 됩니다. 이 시스템의 모든 파라미터는
`conf/config.properties` 라는 파일에 자바 스타일의 `key=value` 형태로 적습니다.
이 값들은 `config.yml` 안에 있는 `${변수:기본값}` 모양의 자리표시자(빈칸)를 채우는 방식으로
읽힙니다. 그래서 아래 표에 나오는 기본값이란, 바로 `config.yml` 안에서 콜론(`:`) 뒤에 적혀 있는
값을 가리킵니다. 설정을 따로 주지 않으면 이 기본값이 그대로 쓰입니다.

---

## 0. 큰 그림 — 데이터 평면과 제어 평면 분리

성능을 이해하려면 가장 먼저 머릿속에 새겨야 할 사실이 하나 있습니다. 바로 데이터가
coordinator 를 거치지 않는다는 점입니다. Impala 에서 읽어 온 실제 행 데이터는 각 executor 가
직접 Greenplum 으로 흘려보냅니다. 이렇게 진짜 데이터가 오가는 길을 데이터 평면이라고 부릅니다.
반면 coordinator 에게는 "지금 어디까지 진행됐는지" 같은 상태와 처리한 행 수(row count) 정도만
오갑니다. 이렇게 지시와 보고만 오가는 길을 제어 평면이라고 합니다.

이 구분이 왜 중요할까요? 데이터가 coordinator 를 통과하지 않기 때문에, 시스템이 한 번에
처리할 수 있는 양의 한계(처리량 천장)는 coordinator 가 아니라 executor 의 수와, 그 뒤에 있는
Impala·Greenplum 의 용량으로 정해지기 때문입니다. 즉 coordinator 를 아무리 키워도 처리량은
늘지 않습니다.

```mermaid
flowchart LR
    Client["클라이언트<br/>POST /jobs"] --> Coord["Coordinator<br/>검증·분할·디스패치·상태추적<br/>(제어 평면)"]
    Coord -- "POST /tasks (제어)" --> E1["Executor 1"]
    Coord -- "POST /tasks (제어)" --> E2["Executor 2"]
    Coord -- "POST /tasks (제어)" --> EN["Executor N"]
    Impala[("Impala<br/>source")] -. "읽기 (데이터)" .-> E1
    Impala -. "읽기 (데이터)" .-> E2
    Impala -. "읽기 (데이터)" .-> EN
    E1 == "적재 (데이터)" ==> GP[("Greenplum<br/>target")]
    E2 == "적재 (데이터)" ==> GP
    EN == "적재 (데이터)" ==> GP
```

위 그림에서 점선(읽기)과 굵은 화살표(적재)가 모두 executor 와 데이터베이스 사이에서만
오가고, coordinator 를 비껴가는 모습을 눈으로 확인할 수 있습니다. 여기서 우리가 얻는 결론은
딱 두 가지입니다.

- 처리량을 늘리고 싶다면 일을 실제로 처리하는 일꾼인 executor 의 수를 늘리거나, 한 executor 가
  동시에 처리하는 task 의 수를 늘리면 됩니다. 이것이 바로 Scale Out(수평 확장)입니다.
- coordinator 는 처리량을 위해서가 아니라 가용성(한 대가 죽어도 서비스가 살아 있게)을 위해
  늘립니다. 데이터가 coordinator 로 흐르지 않으니, coordinator 를 늘려도 처리량 목적은 아닙니다.
  이것이 HA(고가용성)입니다.

---

## 1. Scale Out (수평 확장)

처리량을 늘리는 길, 즉 확장의 축은 세 가지가 있습니다. 보통은 효과가 크고 안전한 순서대로
살펴보는데, ① executor 인스턴스 수를 늘리는 것을 먼저 보고, 그 다음 ② executor 한 대가
동시에 처리하는 task 수를, 마지막으로 ③ coordinator 수를 봅니다.

```mermaid
flowchart TB
    subgraph Axis1["축 1: executor 인스턴스 수 ↑ (주력)"]
        direction LR
        a1["executor:8087"] ~~~ a2["executor:8086"] ~~~ a3["executor:8085 …"]
    end
    subgraph Axis2["축 2: executor당 동시 task ↑"]
        b1["executor.max_concurrent_tasks<br/>8 → 12 → …"]
    end
    subgraph Axis3["축 3: coordinator 수 ↑ (주로 HA, 입구 처리량 보조)"]
        c1["coordinator A"] ~~~ c2["coordinator B"]
    end
    Axis1 --> Note["천장 = Σ executor 동시 task,<br/>단 Impala 슬롯·Greenplum 동시 COPY 한도 이내"]
    Axis2 --> Note
```

그림 오른쪽의 메모가 중요한 힌트를 줍니다. 아무리 축 1과 축 2를 키워도 전체 처리량의 천장은
모든 executor 의 동시 task 수를 합한 값을 넘지 못하며, 그마저도 Impala 슬롯과 Greenplum 의
동시 COPY 한도 안에 머물러야 한다는 뜻입니다. 이제 각 축을 하나씩 살펴보겠습니다.

### 1.1 축 1 — executor 인스턴스 추가 (주력)

세 가지 중 가장 안전하고 효과가 확실한 방법입니다. 하는 일은 간단합니다. 새 포트로 executor
프로세스를 하나 더 띄우고, 그 주소(URL)를 `coordinator.executors` 목록에 추가해 주면 됩니다.
런처를 쓴다면 `EXECUTOR_PORTS` 값을 지정해 여러 executor 를 한꺼번에 띄울 수 있습니다.
목록은 다음처럼 콤마로 이어 적습니다.

```properties
coordinator.executors=http://10.0.0.11:8087,http://10.0.0.11:8086,http://10.0.0.12:8087
```

이렇게 했을 때 어떤 일이 벌어지는지 두 가지로 설명할 수 있습니다. 먼저, 하나의 `SELECT`
쿼리가 몇 갈래로 병렬 처리되는지는 분할된 task 수로 정해집니다. 분할이란 파티션 컬럼의 `IN`
값 목록을 N개로 등분하는 것을 말하며, 이렇게 나뉜 task 들이 executor 풀(executor 들의 모임)에
나누어 배정됩니다. 그래서 executor 가 많을수록 한 job 의 task 들이 더 넓게 퍼져 더 빨리
끝납니다. 다음으로, 여러 executor 를 서로 다른 물리 노드(서버)에 흩어 놓으면 각 노드가 가진
네트워크 카드(NIC)·CPU·디스크의 대역폭이 합산되어, 한 대에 몰아 놓을 때보다 전체 처리량이
커집니다.

### 1.2 축 2 — executor당 동시 task

두 번째 축은 executor 한 대가 동시에 몇 개의 task 를 처리하느냐입니다. 이를 조절하는 값이
`executor.max_concurrent_tasks`(기본 8)인데, 이것은 일종의 세마포어(동시에 들어갈 수 있는
인원을 제한하는 문지기) 역할을 합니다. 여기서 task 하나란 Impala 에서 읽고 Greenplum 으로
적재하는 한 묶음의 작업을 말합니다. 이 값을 올리면 노드 한 대가 내는 처리량은 늘지만, 그만큼
메모리 사용량, 데이터베이스 커넥션 수, 그리고 다운스트림(Impala·Greenplum)에 가하는 부하도
비례해서 함께 늘어난다는 점을 잊으면 안 됩니다.

### 1.3 축 3 — coordinator 추가

세 번째 축은 coordinator 를 늘리는 것입니다. 앞서 강조했듯 데이터가 coordinator 를 거치지
않기 때문에, coordinator 를 늘리는 목적은 처리량 자체보다는 가용성(HA)을 높이고 요청이
들어오는 입구의 처리 능력(QPS, 초당 요청 수)을 분산하는 데 있습니다. 다만 coordinator 를 여러
대 띄우려면 각자가 따로 들고 있던 상태를 공유 PostgreSQL 로 끄집어내 한곳에 모아야 합니다.
이 부분은 [2장](#2-고가용성-ha)에서 자세히 다룹니다.

여기서 한 가지 주의할 점이 있습니다. 과부하를 막는 admission(요청을 받아들일지 거절할지
판정하는 입구 통제) 한도는 coordinator 인스턴스마다 따로, 각자의 메모리 안에서 관리됩니다.

> ⚠️ **admission 한도는 coordinator 인스턴스별(인메모리)** 이다. coordinator 를 2대로
> 늘리면 시스템 전체 동시 job 한도는 `max_concurrent_jobs × 2` 로 **합산**된다. 다운스트림
> 보호를 위해 coordinator 수를 늘릴 땐 인스턴스별 한도를 그만큼 낮춰야 총량이 유지된다.

쉽게 말해, coordinator 가 2대가 되면 각자가 자기 한도만큼 받아들이므로 시스템 전체가 받아들이는
양은 두 배가 됩니다. 다운스트림을 보호하려고 정해 둔 총량을 그대로 유지하고 싶다면, coordinator
대수를 늘릴 때 인스턴스마다의 한도를 그만큼 낮춰 줘야 합니다.

---

## 2. 고가용성 (HA)

coordinator 가 한 대뿐이면, 그 한 대가 죽는 순간 시스템 전체가 멈춥니다. 이렇게 하나만
고장 나도 전체가 멈추는 지점을 SPOF(Single Point Of Failure, 단일 장애점)라고 부릅니다.
이 SPOF 를 없애려면 coordinator 를 여러 대 띄우되, 각자 들고 있던 상태를 공유
PostgreSQL(`history.db_dsn` 으로 지정)로 외부화해야 합니다. 외부화란 상태를 프로세스 안의
메모리가 아니라 바깥의 공용 데이터베이스에 두어, 여러 coordinator 가 같은 그림을 보게 만드는
것을 말합니다.

이 시스템에는 중앙에서 모든 것을 지휘하는 스케줄러가 없습니다. 대신 각 coordinator 가 공유
데이터베이스에 적힌 부하 상황(다만 실시간보다 살짝 늦은, 그래서 약간 stale 한 — 즉 최신이
아닌 — 뷰)을 보고 스스로 독립적으로 결정을 내립니다.

```mermaid
flowchart TB
    LB["로드밸런서 / VIP"] --> CA["Coordinator A"]
    LB --> CB["Coordinator B"]
    CA <--> PG[("공유 PostgreSQL<br/>jobs · *_history<br/>executor_status<br/>executor_reservation<br/>coordinator_status")]
    CB <--> PG
    EX1["Executor 1"] -- "self-report (heartbeat)" --> PG
    EX2["Executor 2"] -- "self-report (heartbeat)" --> PG
    CA -. "P2C 선택·디스패치" .-> EX1
    CA -.-> EX2
    CB -.-> EX1
    CB -.-> EX2
```

그림을 보면 두 coordinator(A, B)가 모두 같은 공유 PostgreSQL 을 바라보고, executor 들은
자기 상태를 그 데이터베이스에 직접 보고하며(이 보고를 self-report 또는 heartbeat 라고 합니다),
어느 coordinator 든 살아 있는 executor 누구에게나 task 를 보낼 수 있는 구조임을 알 수 있습니다.

### 2.1 HA 를 켜는 최소 설정

HA 를 켜려면 다음 네 가지만 설정해 주면 됩니다. 각 줄 위의 주석이 무엇을 위한 설정인지
알려 줍니다.

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

이 설정을 풀어 보면, 먼저 상태를 공유 PostgreSQL 로 옮기고(1번), 각 executor 가 자기 상태와
주소를 스스로 보고하게 하며(2번), coordinator 가 일을 맡길 때 부하를 보고 똑똑하게 고르도록
하고(3번), 마지막으로 죽은 coordinator 가 떠맡고 있던 job 을 다른 coordinator 가 알아서
수습하게(4번) 만드는 것입니다. 여기에 한 가지 준비 작업이 필요합니다.

> 스키마는 앱이 자동 생성하지 않는다. 기동 전에 `conf/postgresql.sql`
> (WarehousePG/Greenplum 7 이면 `warehousepg.sql`)을 먼저 적용한다.

즉 애플리케이션이 데이터베이스 테이블을 알아서 만들어 주지 않으므로, 시스템을 띄우기 전에
위 SQL 파일을 손수 적용해 두어야 합니다.

### 2.2 두 종류의 장애와 대응

이 시스템에서 일어날 수 있는 장애는 크게 두 갈래입니다. executor 가 죽는 경우와 coordinator 가
죽는 경우인데, 둘은 대응 방식이 다릅니다.

먼저 **(a) executor 장애** 입니다. coordinator 가 task 를 보내려는데 연결이 실패하면, 곧장
포기하지 않고 같은 executor 로 다시 시도합니다. 이때 재시도 횟수는 `task_max_retries` 로
정해지고, 실패할수록 다시 시도하기까지의 간격을 점점 늘리는 지수 백오프(exponential backoff,
재시도 간격을 2배씩 키워 가는 방식)를 씁니다. 그래도 끝내 안 되면, `task_failover=true` 인
경우에 한해 다른 executor 로 task 를 재배정합니다. 여기서 failover 란 한 노드가 죽었을 때 그
일을 살아 있는 다른 노드로 넘기는 것을 말합니다. 이때 어떤 executor 를 새로 고를지는
`executor_select` 정책을 따릅니다.

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant E1 as Executor 1 (장애)
    participant E2 as Executor 2
    C->>E1: POST /tasks
    E1--xC: 연결 실패
    C->>E1: 재시도 (backoff×2^n, 최대 task_max_retries)
    E1--xC: 여전히 실패
    Note over C: task_failover=true → 다른 후보 선택<br/>(p2c: 살아있는·한가한 쪽)
    C->>E2: POST /tasks (재배정)
    E2-->>C: 수락 → READING → WRITING → DONE
```

위 시퀀스를 시간 순서대로 따라가 보면, coordinator 가 Executor 1 에 task 를 보냈다가 연결에
실패하고, 백오프를 두며 몇 번 더 시도하다가, 그래도 안 되자 Executor 2 를 새 후보로 골라
재배정하고, Executor 2 가 이를 받아 정상적으로 읽기(READING)와 쓰기(WRITING)를 거쳐
완료(DONE)에 이르는 흐름을 한눈에 볼 수 있습니다.

다음은 **(b) coordinator 장애** 입니다. 각 coordinator 는 자기가 살아 있다는 신호를
`coordinator_status` 테이블에 주기적으로(`heartbeat_interval_s` 마다) 남깁니다. 이 신호를
heartbeat(심장 박동)라고 부릅니다. 만약 어떤 job 의 소유자(그 job 을 맡고 있던 coordinator)가
일정 시간(`coordinator_stale_s`) 넘게 heartbeat 를 남기지 않아 "죽은 것 같다(stale)"고
판단되면, 그 job 이 아직 끝나지 않았을 경우 다른 coordinator 가 `orphan_reconcile_interval_s`
주기로 이를 감지해 `FAILED` 로 정리합니다. 여기서 정합(reconcile)이란 주인을 잃고 떠도는
job 의 상태를 일관되게 맞춰 주는 작업을 말합니다. 이렇게 정리된 job 은 이후 `retry` 로 다시
이어 갈 수 있습니다. 한편 상태 조회나 취소 요청은 공유 `jobs` 테이블을 근거로 처리되므로,
어느 coordinator 로 요청이 라우팅되든 똑같이 응답할 수 있습니다.

### 2.3 왜 P2C(Power-of-Two-Choices)인가

heartbeat 는 일정 간격으로만 갱신되므로, 그 사이 각 coordinator 가 보는 부하 뷰는 살짝
옛날 정보(stale)입니다. 이 상태에서 단순히 "가장 한가한 노드를 고른다"는 `least_loaded`
방식을 쓰면 문제가 생깁니다. 모든 coordinator 가 똑같이 옛 정보를 보고 똑같은 "가장 한가한"
노드 한 곳으로 일제히 몰리는 일이 벌어지기 때문입니다. 이렇게 여럿이 한 곳으로 우르르 몰리는
현상을 herding(떼몰림) 또는 스탬피드라고 부릅니다.

P2C(Power-of-Two-Choices, 두 선택지 중 더 나은 쪽 고르기)는 이 문제를 영리하게 피합니다.
살아 있는 후보 중에서 무작위로 2개만 뽑은 다음, 그 둘 가운데 덜 바쁜 쪽을 고릅니다. 무작위로
2개를 뽑는다는 점이 핵심인데, 이 랜덤화가 각 coordinator 의 결정을 서로 무관하게(탈상관)
만들어 한 곳으로 쏠리는 것을 막아 줍니다. 게다가 별도의 상태나 락(잠금)이 필요 없어
여러 coordinator 가 따로 노는 HA 환경에 잘 어울립니다.

여기에 `executor_reservation=true` 를 더하면 균형이 한층 더 엄격해집니다. task 를 디스패치하는
동안 그 task 를 `executor_reservation` 테이블에 TTL(Time To Live, 일정 시간이 지나면 자동으로
사라지는 유효기간)을 걸어 미리 예약(reservation)해 둡니다. 그러면 heartbeat 가 아직 갱신되기
전이라도 다른 coordinator 가 "현재 실제로 도는 task(`active_tasks`)에 예약분까지 더한 값"을
실시간 부하로 보고 판단할 수 있습니다. 이 예약은 `(executor_url, coordinator_id)` 단위로
관리되고 TTL 이 지나면 저절로 만료되므로, 설령 어떤 coordinator 가 죽더라도 예약이 영영 남아
부하를 부풀리는 누수가 생기지 않습니다.

아래 표는 executor 를 고르는 세 가지 정책을 비교한 것입니다. 왼쪽부터 정책 이름, 동작 방식,
그리고 어떤 상황에서 권장되는지를 보여 줍니다. 단일 coordinator 인지 멀티 coordinator 인지,
부하가 고른지 들쭉날쭉한지에 따라 골라 쓰면 됩니다.

| 선택 정책 | 동작 | 권장 상황 |
|---|---|---|
| `round_robin`(기본) | 순번대로. 부하 무시 | 단일 coordinator, 균질 부하 |
| `least_loaded` | 가장 한가한 노드 | **단일** coordinator + 불균질 부하 |
| `p2c` | 무작위 2개 중 덜 바쁜 쪽 | **멀티** coordinator(HA) 권장 |

---

## 3. 성능 파라미터 레퍼런스

이 장은 성능과 관련된 파라미터들을 주제별로 묶어 정리한 참고용 표 모음입니다. 각 표는 왼쪽부터
파라미터 이름, 기본값, 그리고 그 의미를 보여 줍니다. 처음에는 전부 외울 필요 없이, 필요한 값을
찾아볼 때 사전처럼 들춰 보면 됩니다.

### 3.1 동시성 / Admission (3층 과부하 방어)

요청이 한꺼번에 몰려와도 시스템이 무너지지 않도록, 과부하 방어는 세 개의 층위로 겹겹이
이루어집니다(층위별 자세한 그림은 [DESIGN.md §10](DESIGN.md) 에 있습니다). 아래 표에서 "층위"
칸의 L1·L2·L3 가 바로 그 세 단계이고, "범위" 칸은 그 한도가 coordinator 마다 따로 적용되는지
executor 마다 따로 적용되는지를 알려 줍니다.

| 파라미터 | 기본 | 층위 / 의미 | 범위 |
|---|---|---|---|
| `coordinator.max_concurrent_jobs` | 16 | L1 실행 슬롯. 동시에 RUNNING 가능한 job 수. `<=0` 무제한 | coordinator별 |
| `coordinator.max_pending_jobs` | 100 | L1 대기 큐. 슬롯이 차면 PENDING 대기. **실행+대기 합 초과 → 429** | coordinator별 |
| `coordinator.max_dispatch_concurrency` | 32 | L2 한 coordinator 가 모든 job 통틀어 동시에 띄우는 task 수 | coordinator별 |
| `executor.max_concurrent_tasks` | 8 | L3 executor 한 대의 동시 task 수. `0` 무제한 | executor별 |

표를 풀어 보면, L1 은 job 단위의 입구 통제입니다. 동시에 실행(RUNNING)할 수 있는 job 수를
실행 슬롯(`max_concurrent_jobs`)으로 제한하고, 슬롯이 다 차면 들어온 job 을 대기
큐(`max_pending_jobs`)에 PENDING 상태로 세워 둡니다. 실행 중인 것과 대기 중인 것의 합마저
넘어서면 더는 받지 않고 429(Too Many Requests) 응답으로 거절합니다. L2 는 한 coordinator 가
모든 job 을 통틀어 동시에 띄울 수 있는 task 수의 상한이고, L3 는 executor 한 대가 동시에
처리하는 task 수의 상한입니다.

### 3.2 폴링 / 타임아웃 / Failover

다음 표는 coordinator 가 task 의 진행 상황을 확인하고(폴링), 응답이 없을 때 얼마나 기다리며
(타임아웃), 실패했을 때 어떻게 다시 시도하고 다른 노드로 넘길지(failover)를 정하는
파라미터들입니다.

| 파라미터 | 기본 | 의미 |
|---|---|---|
| `coordinator.poll_interval_s` | 1.0 | task 상태 폴링 간격(초). 작을수록 반응 빠름·부하↑ |
| `coordinator.task_timeout_s` | 3600 | task HTTP 전체(read) 타임아웃. 가장 긴 분할 task 예상시간보다 길게 |
| `coordinator.task_connect_timeout_s` | 5.0 | **접속** 전용 타임아웃. 죽은 executor 를 빨리 실패시켜 failover 가속 |
| `coordinator.task_max_retries` | 2 | 연결 실패 시 같은 executor 재시도 횟수(지수 백오프) |
| `coordinator.task_retry_backoff_s` | 0.5 | 백오프 기준: `backoff × 2^시도` |
| `coordinator.task_failover` | true | 재시도 소진 시 다른 executor 로 재배정 |

여기서 폴링(polling)이란 coordinator 가 일정 간격마다 task 에게 "다 됐니?" 하고 상태를 물어보는
것을 말합니다. 폴링 간격이 짧을수록 변화를 빨리 알아채지만 그만큼 부하가 늘어납니다. 또
타임아웃에는 두 종류가 있다는 점에 유의하세요. `task_timeout_s` 는 task 전체가 끝날 때까지
기다리는 시간이고, `task_connect_timeout_s` 는 연결을 맺는 그 순간까지만 기다리는 시간이라,
이미 죽은 executor 를 빨리 가려내 failover 를 앞당기는 데 쓰입니다.

### 3.3 HA / 선택 / 정합

다음 표는 2장에서 다룬 HA 동작을 세밀하게 조정하는 파라미터들입니다. executor 를 어떻게 고를지,
부하 정보를 어디서 읽을지, heartbeat 와 예약과 정합의 주기를 각각 얼마로 할지를 정합니다.

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

표 맨 아래의 `shutdown_drain_timeout_s` 에 나오는 graceful drain 이라는 말은, 종료 신호
(SIGTERM)를 받았을 때 곧바로 멈추지 않고 이미 진행 중이던 task 들이 끝날 때까지 잠시 기다려
주는 "부드러운 마무리"를 뜻합니다.

### 3.4 모니터링

다음 표는 executor 의 건강 상태와 메트릭(지표)을 얼마나 자주 확인하고 기록할지를 정하는
파라미터들입니다.

| 파라미터 | 기본 | 의미 |
|---|---|---|
| `monitor.enabled` | true | executor 헬스/메트릭 모니터링 |
| `monitor.health_interval_s` | 10 | 헬스 체크 주기 |
| `monitor.record_interval_s` | 60 | 메트릭 DB 기록 주기 |
| `monitor.db_dsn` | (빈값) | 메트릭 기록 DSN. 비우면 폴링만 하고 기록 생략 |

맨 아래 `monitor.db_dsn` 을 비워 두면 헬스 체크는 계속하되 그 결과를 데이터베이스에 남기지는
않습니다. 기록까지 남기고 싶다면 여기에 접속 정보(DSN)를 채워 주면 됩니다.

### 3.5 백엔드 처리량 (executor → Greenplum)

마지막 표는 executor 가 Greenplum 으로 데이터를 실제로 밀어 넣을 때의 처리량과 관련된
파라미터들입니다.

| 파라미터 | 기본 | 의미 |
|---|---|---|
| `greenplum.pool_max` | 0 | GP 커넥션 풀 크기(동시 GP 연결 상한). 0=`executor.max_concurrent_tasks` 와 동일 |
| `copy.batch_size` | 10000 | COPY 배치 크기(행). 클수록 처리량↑·메모리↑ |
| `copy.preflight` | true | COPY 전 컬럼 사전검증(불일치 조기 실패) |
| `copy.pipeline` | true | Impala 읽기와 GP COPY 를 별도 스레드로 겹쳐 실행(벽시계 단축) |
| `copy.queue_size` | 8 | 파이프라인 큐 크기(배치 개수). 메모리 ≈ `queue_size × batch_size` 행 |
| `copy.format` | text | COPY 포맷 `text`\|`binary`. binary 는 인코딩 CPU 절감(타입 해석 실패 시 text 폴백) |
| `impala.query_options` | (빈값) | Impala SET 전역 기본값. 예: `MEM_LIMIT=2g,REQUEST_POOL=etl` |
| `query.sql_dialect` | hive | 파싱 기본 방언(요청에서 재정의 가능) |

여기서 배치(batch)란 행을 한 번에 묶어 보내는 단위를 말합니다. `copy.batch_size` 를 크게 하면
한 번에 더 많은 행을 보내 처리량이 오르지만, 그만큼 메모리도 더 쓴다는 트레이드오프가 있습니다.
또 `copy.preflight` 를 켜 두면 COPY 를 시작하기 전에 컬럼이 서로 맞는지 미리 검사해, 어긋남이
있을 때 일찌감치 실패시켜 줍니다.

### 3.6 SELECT→COPY 병목 진단·튜닝 (executor 단일 task 관점)

한 task 의 `SELECT → COPY` 가 느릴 때, **먼저 원인을 측정하고 그다음에 손댑니다.** 대시보드의
단계 타임라인(STREAM_COPY 행)과 `task_history` 컬럼이 벽시계를 네 갈래로 쪼개 보여 줍니다.

| 지표(컬럼) | 의미 | 이게 지배적이면 |
|---|---|---|
| `read_wait_ms` | 리더의 Impala `fetchmany` 순수 시간 | 참고용(아래 `read_starve` 로 병목 판단) |
| `read_starve_ms` | (파이프라인) 라이터가 **다음 배치를 기다린** 시간 | **Impala(소스)가 병목** — 읽기가 못 따라옴 |
| `write_wait_ms` | 라이터의 `write_row`(인코딩+송신) 시간 | **클라이언트 인코딩/네트워크** 병목 |
| `finalize_wait_ms` | COPY 종료(서버 ingest 완료) 대기 | **Greenplum COPY 처리**(마스터 단일 스트림) 병목 |

파이프라인 모드에서 벽시계 ≈ `read_starve + write_wait + finalize` 이므로, 셋 중 가장 큰 항이
곧 병목입니다. 병목별 처방:

- **`read_starve` 지배(= Impala 가 느림)**
  - 파티션 분할(`parallelism`)을 늘려 **여러 executor 가 서로 다른 파티션을 동시에** 읽게 한다(최우선).
  - `copy.batch_size` 를 키워 fetch 왕복 횟수를 줄인다(예: 10k→50k). 메모리와 트레이드오프.
  - Impala 쪽 튜닝: `impala.query_options`(`MEM_LIMIT`, `REQUEST_POOL`), 스캔 대상 축소.
- **`write_wait` 지배(= 클라이언트 인코딩/전송)**
  - `copy.format=binary` 로 텍스트 인코딩 CPU 를 줄인다(타입 해석 실패 시 자동 text 폴백).
  - executor↔GP 네트워크 대역/지연 점검, `copy.batch_size` 상향.
- **`finalize_wait` 지배(= Greenplum COPY 처리)**
  - 한 스트림이 마스터로 몰리는 구조라 **`parallelism` 을 늘려 여러 executor 가 동시에 COPY** 하게
    하는 것이 가장 효과적(GP 세그먼트 병렬 활용). `greenplum.pool_max` 로 동시 GP 연결을 조절.
  - 대상 테이블 인덱스/트리거/분산키(`DISTRIBUTED BY`) 재검토.

**튜닝 절차(권장)**
1. 느린 task 하나의 STREAM_COPY 지표를 본다 → 지배 항을 찾는다.
2. 위 표의 해당 처방을 **하나씩** 적용하고 다시 측정한다(한 번에 하나만 바꿔 효과를 분리).
3. `read_starve` 와 `write_wait` 가 비슷하다면 이미 파이프라인이 잘 겹치는 상태 → **수평 확장
   (`parallelism`↑ + executor 추가)** 이 가장 확실한 다음 수. (§1 Scale Out 참고)

> `copy.pipeline=false` 로 두면 읽기·쓰기가 직렬 실행돼 `read_wait`/`write_wait` 가 순수 벽시계로
> 나뉩니다. 파이프라인이 의심스러울 때 원인 격리를 위해 잠깐 꺼서 비교하는 용도로 유용합니다.

### 3.7 최후의 수단 — PXF 세그먼트 병렬 로딩 (COPY 마스터 병목 우회)

파이프라인·바이너리·`batch_size`·수평 확장을 다 해도 **`finalize_wait`(GP 서버 ingest)가 계속
지배적**이라면, 병목은 **COPY STDIN 이 Greenplum 마스터 한 노드로 몰리는 구조** 자체입니다.
executor 를 아무리 늘려도 각자 마스터로 COPY 하므로 마스터가 최종 천장이 됩니다. 이때의 정석은
데이터 평면을 **"우리가 밀어넣기(push COPY)"에서 "GP 가 당겨오기(pull)"로** 바꾸는 것입니다.

**PXF(Platform Extension Framework)** 는 GP 의 병렬 외부 데이터 프레임워크로, **모든 세그먼트가
외부 소스(HDFS/Hive/오브젝트 스토어)를 직접 병렬로** 읽어 들입니다 → 마스터가 데이터 경로에서
빠집니다. 이 프로젝트는 이미 **`exec_mode=statement`** 로 이 패턴을 **코드 변경 없이** 수용합니다.

```sql
-- 분할(splitter)은 그대로: 파티션 IN 버킷이 wrapper_query 에 채워진다.
-- SELECT 대상을 COPY 스트림이 아니라 PXF 외부 테이블로 둔다 → 세그먼트 병렬 읽기.
INSERT INTO gp_target (c1, c2, dt)
SELECT c1, c2, dt FROM pxf_ext_source
WHERE dt IN ('2026-07-01','2026-07-02');   -- ← 각 task 의 파티션 버킷
```

- **exec_mode**: `statement`, **wrapper_query**: 위 INSERT…SELECT(placeholder 로 파티션 IN 치환).
- COPY 도, 우리 executor 를 통한 스트리밍도 **전혀 없다**. executor 는 SQL 제출+폴링만 한다.
- 멱등성(overwrite): `DELETE FROM target WHERE dt IN(...)` 를 앞에 붙이거나 stage 후 스왑.

두 가지 변형:

| 변형 | 방식 | 특징 |
|---|---|---|
| **A. 원본 직접** | PXF `Hive`/`hdfs:parquet` 프로파일로 Impala 원본 파일을 바로 읽기 | export 단계 없음(가장 단순). 파티션 커밋 상태가 파일로 안정적이어야 함 |
| **B. export 후 로딩** | ① Impala `INSERT OVERWRITE staging_hive_tbl SELECT …`(Parquet 병렬 쓰기) → ② PXF 로 그 경로 로딩 → ③ 정리 | 읽기·쓰기 양쪽 병렬(단일 스트림 0). 스냅샷·포맷 통제 확실하나 이동 부품 많음 |

**도입 전 반드시 확인(코드보다 운영이 관건)**

- **PXF 설치·구성**: `pxf cluster init/start`. GP 운영 의존성 추가.
- **네트워크**: 지금은 executor 만 Impala 와 통신하지만, PXF 는 **모든 GP 세그먼트가 HDFS
  (NameNode/DataNode)·오브젝트 스토어에 직접 도달**해야 한다. 망분리/에어갭에선 방화벽·라우팅이
  실제 관문(가장 큰 선행 과제).
- **타입 매핑**: Hive/Parquet→GP 는 PXF 프로파일이 대부분 처리(엣지 타입만 확인).
- **가시성**: 행 단위 진행률은 사라진다(적재를 GP 가 함). 단계는 여전히 SUBMIT/INSERT/COMMIT 로 추적.

**권장 도입 순서**

1. **파일럿(코드 0줄)**: GP 에 PXF 를 올리고 외부 테이블 하나를 만든 뒤, **지금의 `statement`
   모드로** `INSERT…SELECT FROM pxf_ext` 를 돌려 처리량을 기존 COPY 경로와 비교한다.
2. 효과가 확인되면 **1급 지원**으로 검토: 외부 테이블(및 선택적 Impala export) 생성→INSERT→정리
   라이프사이클을 캡슐화한 `exec_mode=pxf` 를 추가하고, PXF 프로파일/서버/경로를 설정화한다.

> 언제 쓰나: **`finalize_wait` 가 벽이고 executor 수평 확장으로도 안 풀릴 때**가 명확한 신호다.
> 반대로 `read_starve`(Impala) 가 지배적이면 변형 B(Impala 병렬 export)가, 데이터량이 크지 않거나
> PXF 설치·망 개방이 어려우면 기존 경로의 `parallelism`↑ 가 비용 대비 낫다.
> **구조적 한계(`finalize_wait` 이 계속 지배)라면 적재 방식 자체를 바꾼다.** `parallelism` 을 더
> 올려도 결국 각 executor 의 **단일 COPY 스트림이 GP 마스터로 몰리는 것**이 천장일 수 있습니다.
> 이때는 `exec_mode=local_stage`(DESIGN §17)가 적재 병렬성을 **GP 세그먼트로 이동**시켜 이 병목을
> 없앱니다 — executor 가 세그먼트 호스트 로컬 CSV 로 export 하고, GP 가 `file://` 외부테이블로
> **세그먼트별 로컬 파일을 병렬 read** 하므로 단일 소켓 병목이 사라집니다(단, executor 를 GP 세그먼트
> 호스트에 co-locate 해야 함).

---

## 4. 값을 정하는 기준 (Sizing)

앞 장이 "어떤 파라미터가 있는가"를 보여 주는 사전이라면, 이 장은 "그래서 그 값을 얼마로 잡아야
하는가"를 알려 주는 실전 가이드입니다. 이렇게 적절한 크기를 정하는 일을 사이징(sizing)이라고
부릅니다.

### 4.1 황금률 — 천장은 coordinator 가 아니라 다운스트림이다

사이징의 출발점이자 가장 중요한 원칙은, 전체 처리량의 천장이 coordinator 가 아니라 그 뒤에 있는
다운스트림(Impala·Greenplum)에서 정해진다는 것입니다. 시스템 전체가 동시에 처리할 수 있는
task 수의 실효 상한은 다음 세 값 중 가장 작은 값으로 결정됩니다.

```
유효 동시 task ≈ min(
    Σ executor.max_concurrent_tasks   (= executor 수 × executor당 동시 task),
    Greenplum 이 견디는 동시 COPY 세션 수,
    Impala 동시 쿼리 슬롯(REQUEST_POOL 한도)
)
```

이 식이 말하는 바는 분명합니다. executor 를 아무리 많이 띄워도, Greenplum 이 받아 줄 수 있는
동시 COPY 세션 수나 Impala 의 동시 쿼리 슬롯이 더 작다면 거기서 막힌다는 뜻입니다. 그래서
순서를 거꾸로 잡아야 합니다. 먼저 다운스트림(Greenplum 동시 COPY, Impala 풀)이 안전하게 견디는
한도를 확정한 다음, 그 한도를 executor 풀에 나누어 분배합니다. 반대로 coordinator
쪽(`max_dispatch_concurrency`)은 이 한도보다 넉넉하게 크게 잡아, coordinator 자신이 병목이 되지
않도록 합니다.

### 4.2 파라미터별 산정 기준

이제 주요 파라미터를 하나씩 어떤 기준으로 정하면 되는지 살펴보겠습니다.

먼저 **`executor.max_concurrent_tasks`** 는 노드 한 대를 기준으로 잡습니다. 대략
`min(코어수, 안전한 Greenplum 동시 COPY ÷ executor 수, 메모리 ÷ task당 메모리)` 정도가
기준이 됩니다. task 하나는 Impala 커넥션 하나, Greenplum 커넥션 하나, 그리고 `copy.batch_size`
만큼의 버퍼를 잡아먹습니다. 그래서 메모리가 빡빡한 환경이라면 다른 값보다 이 값을 먼저 줄이는
것이 좋습니다.

executor 는 Greenplum 연결을 **커넥션 풀**로 재사용합니다. 예전에는 task 마다 새로 연결을
맺어 동시 연결 수가 제어되지 않고 인증·핸드셰이크 비용도 매번 치렀지만, 지금은 풀이 동시
연결을 **`greenplum.pool_max`** 개로 제한하고 유휴 연결을 다시 씁니다(stage_insert 의 세션
전용 TEMP 테이블은 반납 시 `DISCARD ALL` 로 비워져 재사용이 안전합니다). 기본값은 0이며,
이때 풀 크기는 `executor.max_concurrent_tasks` 와 같아져 "동시 task 당 GP 연결 하나"가 됩니다.
**Greenplum 의 `max_connections` 를 직접 보호하는 손잡이**가 바로 이 값입니다. 클러스터 전체
동시 GP 연결은 `Σ executor.greenplum.pool_max` 이므로, 이 합이 Greenplum 이 허용하는 동시 세션
수를 넘지 않게 잡습니다. 동시 task 수보다 작게 두면 task 가 연결을 기다리며 추가로 throttle 되고,
크게 두어 봐야 동시 task 수가 천장이라 의미가 없습니다.

다음으로 **`coordinator.max_dispatch_concurrency`** 는 모든 executor 의 동시 task 수를 합한
값(`Σ executor.max_concurrent_tasks`) 이상으로 둡니다(기본 32). 이 값이 너무 작으면 executor 가
놀고 있는데도 coordinator 가 task 를 충분히 띄우지 못해 오히려 coordinator 가 병목이 됩니다.

그 다음 **`coordinator.max_concurrent_jobs`** 와 **`max_pending_jobs`** 는 입구를 보호하는
용도입니다. 동시 job 수에 평균 분할 task 수를 곱한 값이 앞서 구한 "유효 동시 task"를 크게
넘지 않도록 잡습니다. 대기 큐(`max_pending_jobs`)는 갑자기 몰리는 요청(버스트)을 잠시 흡수하는
완충 역할을 합니다. 큐가 길수록 429 로 거절되는 일은 줄지만, 대신 대기 지연이 늘어 오래된
요청이 줄줄이 쌓이게 됩니다. 멀티 coordinator 환경이라면, 이 값들을 인스턴스 수만큼 나누어
총량을 맞춰야 한다는 점을 기억하세요.

이어서 **`copy.batch_size`** 는 처리량과 메모리·트랜잭션 크기 사이의 줄다리기입니다. 행이 넓거나
executor 메모리가 빠듯하면 값을 낮추고(예: 2000~5000), 행이 좁고 메모리가 넉넉하면 올립니다
(20000 이상). 값을 바꾼 뒤에는 `monitor` 메트릭으로 executor 메모리 사용량을 확인하며 조정합니다.

또 **`task_timeout_s`** 는 가장 큰 단일 분할 task 의 예상 실행시간에 여유를 더해 잡습니다.
너무 짧으면 멀쩡히 돌고 있던 정상 task 가 타임아웃으로 실패하고, 너무 길면 진짜로 멈춰 버린
(hang) task 를 뒤늦게야 감지하게 됩니다.

끝으로 **`task_connect_timeout_s`** 는 짧게 잡습니다(기본 5초). 이 값은 연결을 맺는 동안만의
타임아웃이라서, 짧게 둘수록 죽은 노드를 빨리 걸러 내 failover 를 앞당겨 줍니다. 네트워크가
유난히 느린 환경이 아니라면 굳이 키울 이유가 별로 없습니다.

### 4.3 HA 타이밍 파라미터의 관계 (불변식)

HA 환경에서 여러 타이밍 값을 잡을 때는 반드시 지켜야 하는 순서 관계가 있습니다. 핵심 직관은
이렇습니다. 장애를 감지하고 정합하는 일이 헬스(생존 신호) 갱신보다 느려야, 잠깐 신호가 늦은
것을 두고 멀쩡한 노드를 죽었다고 잘못 판단하는 오탐을 막을 수 있습니다. 이 관계를 부등식으로
나타내면 다음과 같습니다. 항상 성립해야 한다는 뜻에서 불변식이라고 부릅니다.

```
status_interval_s  ≤  heartbeat_interval_s  ＜  coordinator_stale_s  ≤  orphan_reconcile 주기
        (10)                  (10)                    (30)
heartbeat_interval_s  ＜  reservation_ttl_s
        (10)                    (60)
```

이 부등식을 말로 풀면, executor 가 자기 상태를 보고하는 주기와 coordinator 가 생존 신호를 남기는
주기는 짧게 두되, "죽었다"고 판정하는 임계(`coordinator_stale_s`)는 그보다 넉넉히 길게, 그리고
주인 잃은 job 을 정리하는 주기는 그보다 더 길거나 같게 둔다는 뜻입니다. 또 예약의
유효기간(`reservation_ttl_s`)은 heartbeat 주기보다 길어야 합니다. 구체적인 기준은 다음과 같습니다.

- `coordinator_stale_s` 는 `heartbeat_interval_s` 의 2~3배로 잡습니다. 신호를 한두 번 놓쳐도
  아직 살아 있다고 봐 주기 위해서입니다. 이 값이 너무 작으면, 잠깐의 GC(가비지 컬렉션)나
  지연만으로도 멀쩡한 coordinator 의 job 을 빼앗아 버립니다.
- `reservation_ttl_s` 는 heartbeat 의 수 배로 잡습니다. 너무 짧으면 예약이 일찍 풀려 균형을
  맞춰 주던 효과가 사라지고, 너무 길면 죽은 coordinator 가 남긴 예약이 오래 남아 부하를 실제보다
  부풀려 보게 됩니다.
- 반대로 장애를 더 빨리 감지하고 싶다면, 위 부등식의 순서를 깨지 않은 채 관련된 값들을 한
  세트로 함께 줄여야 합니다. 어느 하나만 줄이면 부등식이 깨져 오탐이 생깁니다.

### 4.4 튜닝 절차(권장)

지금까지의 내용을 실제로 적용할 때는 다음 순서를 따르기를 권합니다. 각 단계는 앞 단계의 결과
위에 쌓이므로 순서대로 밟는 것이 중요합니다.

1. 다운스트림 안전 한도(Greenplum 동시 COPY, Impala 풀)부터 확정한다.
2. `executor 수 × executor.max_concurrent_tasks` 가 그 한도에 맞도록 분배한다.
3. `max_dispatch_concurrency` 를 그 합 이상으로, `max_concurrent_jobs`/`max_pending_jobs` 로
   입구를 보호한다(멀티 coordinator 면 나눠서).
4. 부하를 걸고 `/metrics`·대시보드·`monitor` 메트릭(CPU/메모리/디스크, active_tasks)을 보며
   병목 지점(executor 메모리? Greenplum? coordinator 디스패치?)을 찾아 한 번에 한 값씩 조정한다.
5. HA 면 4.3 의 부등식을 깨지 않는 선에서 감지 속도를 맞춘다.

특히 4번 단계의 "한 번에 한 값씩"이라는 원칙을 꼭 지키세요. 여러 값을 동시에 바꾸면 어떤 변경이
효과를 냈는지 알 수 없어 튜닝이 미궁에 빠집니다.

---

## 5. 빠른 참조 — 목적별 추천 출발점

마지막으로, 흔히 마주치는 목적별로 어디서부터 손대면 좋은지를 한 표에 모았습니다. 왼쪽 칸에서
지금 내가 이루고 싶은 목적을 찾고, 오른쪽 칸의 핵심 설정을 출발점 삼아 조정해 나가면 됩니다.
어디까지나 출발점이므로, 적용 후에는 4.4 의 튜닝 절차대로 메트릭을 보며 다듬어 가세요.

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
