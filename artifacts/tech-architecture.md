# 기술 아키텍처

이 문서는 분산 쿼리 실행기를 어떤 인프라 위에 어떻게 올리고 운영하는지를 정리한 기술 아키텍처
정의서다. 표준 양식의 목차를 따르되 이 시스템에 없는 항목은 빼고 실제 구성요소로 바꿨다. Web
Server 와 WAS 가 따로 없고 우리가 만든 Distributed Query Executor 가 그 자리를 대신하므로 해당
목차를 그것으로 교체했고, DBMS 는 메타 저장소로 쓰는 PostgreSQL 을 기준으로 적었다.

아직 정하지 않았거나 이 시스템에 해당하지 않는 절은 비워 두되, 왜 비었는지는 한 줄로 남긴다.
누락과 구분해야 리뷰가 가능하기 때문이다. 표기는 셋이다.

> **작성 예정** — 값이 정해지면 채운다. 채울 자리를 알 수 있게 표 골격만 두었다.

> **해당 없음** — 이 시스템의 구성에서는 쓰지 않는 항목이다.

> **[선택]** — 표준 양식에서 선택 항목이라 이번 범위에서는 작성하지 않는다.

---

## 1. 기술 인프라 구성

### 1.1 하드웨어 구성

> **작성 예정** — 서버 사양과 대수가 확정되면 채운다.

| 구분 | 용도 | 대수 | CPU | 메모리 | 디스크 | 비고 |
|---|---|---|---|---|---|---|
| Coordinator 서버 | 요청 접수·분할·상태 집계 | | | | | |
| Executor 서버 | 소스 읽기·적재 실행 | | | | | |
| PostgreSQL 서버 | 메타 저장소 | | | | | |

사양을 잡을 때 기억할 것이 하나 있다. 데이터는 coordinator 를 지나지 않고 executor 가 소스에서
읽어 대상으로 곧장 보내므로, 자원이 필요한 쪽은 언제나 executor 다. coordinator 는 상태와 행 수만
다루므로 요구가 가볍다.

### 1.2 소프트웨어 구성

운영체제와 런타임은 RHEL 9.2 기본 파이썬을 그대로 쓴다. 별도 파이썬을 올리지 않는 것은 에어갭
환경에서 관리 대상을 늘리지 않기 위해서다.

| 구분 | 소프트웨어 | 버전 | 설치 위치 | 비고 |
|---|---|---|---|---|
| 운영체제 | RHEL | 9.2 | — | 서비스 계정 `gpadmin` |
| 런타임 | Python | 3.9 이상 | `/data1/distributed-query-executor/.venv` | OS 기본 파이썬 사용 |
| 애플리케이션 | Distributed Query Executor | 저장소 버전 | `/data1/distributed-query-executor` | coordinator·executor 공용 트리 |
| 웹 프레임워크 | FastAPI · Uvicorn | 0.110~0.128 · 0.29~0.39 | `.venv` | 두 서비스 모두 단일 워커 |
| SQL 파서 | sqlglot | 30.x | `.venv` | SELECT 검증과 분할 |
| 템플릿 엔진 | Jinja2 | 3.1.x | `.venv` | 서버 측 쿼리 템플릿 |
| 메타 저장소 | PostgreSQL | 12 이상 | 별도 서버 | job·이력·상태 테이블 |
| DB 드라이버 | psycopg | 3.2 이상 | `.venv` | 메타 저장소와 적재 대상 공용 |
| 소스 드라이버 | impyla · thrift-sasl · pure-sasl | 0.19 이상 | executor `.venv` | Impala 접속과 LDAP 인증 |
| 오브젝트 스토리지 | boto3 | 1.34 이상 | executor `.venv` | `s3_stage` 에서만 사용 |
| 시스템 패키지 | gcc · gcc-c++ · make · python3-devel · cyrus-sasl-devel | — | OS | impyla SASL 빌드에 필요 |

서비스 관리는 런처 스크립트(`bin/start-*.sh`)를 기본으로 하고, systemd 로 돌릴 때는 저장소가 함께
배포하는 유닛을 `systemctl link` 로 등록한다. 웹 에셋과 API 문서는 모두 트리 안에 넣어 두어
런타임에 외부로 나가지 않는다.

### 1.3 네트워크 구성

> **작성 예정** — 대역과 방화벽 정책, 망 분리 구조가 확정되면 구성도와 함께 채운다. 서비스가 여는
> 포트와 통신 방향은 1.5 구성요소별 매핑에 정리해 두었다.

| 구간 | 출발지 | 목적지 | 포트 | 프로토콜 | 방화벽 정책 |
|---|---|---|---|---|---|
| 사용자 → Coordinator | | | | | |
| Coordinator → Executor | | | | | |
| Executor → 소스 | | | | | |
| Executor → 적재 대상 | | | | | |
| 서비스 → PostgreSQL | | | | | |

### 1.4 구성요소별 정의

표준 양식의 Web Server 와 WAS Server 자리에는 이 시스템의 실제 구성요소인 Distributed Query
Executor 가 온다. 하나의 애플리케이션이지만 역할이 뚜렷이 갈리므로 coordinator 와 executor 를
나눠 적는다.

#### 1.4.1 사용자

작업을 맡기는 쪽은 사람이 아니라 대개 배치 스케줄러나 업무 시스템이다. HTTP 클라이언트로
coordinator 의 `8088` 포트에 작업을 제출하고 상태를 폴링하며, executor 의 존재를 알 필요가 없다.
브라우저로 같은 포트에 접속하면 읽기 전용 대시보드를 볼 수 있고, 터미널만 있는 환경에서는 같은
API 를 읽는 curses 모니터를 쓴다. 운영자는 여기에 더해 `bin/gp-shell`·`bin/impala-shell`·
`bin/s3-ops` 로 소스와 대상, 스테이징 저장소를 직접 다룬다.

#### 1.4.2 Distributed Query Executor — Coordinator

요청을 받아 검증하고 나누고 배분하는 제어 평면이다. `POST /jobs` 가 들어오면 멱등 키를 확인하고,
템플릿을 쓰는 요청이면 서버에서 SQL 을 렌더한 뒤 파서로 검증하고 파티션 `IN` 목록 기준으로
task 를 만든다. 이어서 admission 이 수용 여부를 판단해 넘치면 `429` 로 거절하고, 통과하면 job 을
만들어 `202` 를 돌려준 뒤 백그라운드에서 실행한다.

실행 중에는 각 executor 에 task 를 병렬로 디스패치하고 상태를 폴링하며, 종료되면 DONE·PARTIAL·
FAILED·CANCELLED 중 하나로 집계한다. `local_stage` 와 `s3_stage` 에서는 모든 executor 가 파일을
만든 뒤의 Phase 2, 즉 외부테이블 생성과 target INSERT 를 coordinator 가 중앙에서 수행한다.
대시보드와 API 문서, 헬스와 메트릭 조회도 이 프로세스가 제공한다. 상태를 메모리에 두므로 단일
워커로 실행하고, 처리량 확장은 이쪽이 아니라 executor 수로 한다.

#### 1.4.3 Distributed Query Executor — Executor

실제로 데이터를 옮기는 데이터 평면이다. `POST /tasks` 로 task 를 받아 큐에 넣고, 소스에서 읽어
(READING) 대상에 적재하는(WRITING) 동안 상태를 스스로 관리한다. 소스 읽기는 impyla 커서를 쓰고,
커서가 없는 사내 API 는 커서처럼 감싸는 어댑터를 거치므로 읽기 루프는 같다. 적재는 `exec_mode` 에
따라 갈려서 COPY 로 곧장 넣거나, staging 을 거치거나, CSV 파일을 만들어 Greenplum 이 외부테이블로
당겨 읽게 한다.

한 대가 동시에 실행하는 task 수는 `executor.max_concurrent_tasks` 로 제한하고, Greenplum 연결은
풀로 재사용하되 반납할 때 세션을 초기화한다. 포트를 달리해 한 호스트에 여러 인스턴스를 띄울 수
있으며, `local_stage` 를 쓸 때는 Greenplum 세그먼트와 같은 호스트에 두어야 한다.

#### 1.4.4 DBMS — PostgreSQL

메타 저장소다. 옮기는 데이터가 아니라 이관을 관리하는 정보만 담으므로 볼륨이 크지 않다. 공유 Job
저장소(`jobs`)와 실행 이력(`job_history`·`task_history`), executor 자기 보고 상태
(`executor_status`), coordinator heartbeat(`coordinator_status`), 예약(`executor_reservation`),
헬스 메트릭(`executor_health_metrics`) 일곱 테이블로 이루어진다.

세 가지를 기억해 둔다. 첫째, 단일 coordinator 로 쓸 때는 이 저장소가 없어도 서비스가 돈다. 상태를
메모리에 두기 때문이며, 이력만 남기고 싶으면 `history.db_dsn` 만 설정한다. 둘째, coordinator 를
여러 대 띄우려면 이 저장소가 필수다. 그러지 않으면 작업을 접수한 인스턴스만 그 상태를 알아
사용자가 자기 작업을 조회하지 못한다. 셋째, 앱이 테이블을 만들어 주기는 하지만 권한을 좁히거나
미리 만들어 두려면 `config/postgresql.sql` 을 서비스 기동 전에 적용한다. 컬럼 단위 명세는 같은
디렉터리의 `tables.md` 에 있다.

#### 1.4.5 연계 시스템 — 소스와 적재 대상

소스는 Impala 를 기본으로 하고, 템플릿의 `datasource` 로 Trino 나 사내 API 를 고를 수 있다.
executor 만 소스에 접속하며 coordinator 는 관여하지 않는다. 적재 대상은 Greenplum 계열이고
`greenplum.dsn` 한 줄로 지정한다. 이 값이 비어 있으면 아무것도 읽고 쓰지 않는 백엔드로 뜨므로,
새 노드를 올린 뒤에는 기동 로그를 확인한다. `s3_stage` 를 쓴다면 여기에 오브젝트 스토리지와
Greenplum 쪽 PXF 구성이 더해진다.

### 1.5 구성요소별 매핑

구성요소와 프로세스, 포트, 설치 경로, 설정 키의 대응이다. 배치 서버 열은 하드웨어가 확정되면
채운다.

| 구성요소 | 프로세스 · systemd 유닛 | 포트 | 설치 경로 | 주요 설정 키 | 배치 서버 |
|---|---|---|---|---|---|
| Coordinator | `python -m coordinator` · `coordinator.service` | 8088 | `/data1/distributed-query-executor` | `coordinator.*`, `store.backend`, `history.db_dsn` | |
| Executor | `python -m executor` · `executor@<port>.service` | 8087 · 8086 … | 같음 | `executor.*`, `impala.*`, `greenplum.dsn`, `copy.*` | |
| 메타 저장소 | PostgreSQL 인스턴스 | 5432 | 별도 서버 | `history.db_dsn`, `monitor.db_dsn`, `db.schema` | |
| 소스 | Impala(HS2) · Trino · 사내 API | 21050 · 28000 | 기존 시스템 | `impala.*`, `query.func.*` | |
| 적재 대상 | Greenplum · WarehousePG | 5432 | 기존 시스템 | `greenplum.dsn`, `s3.*`, `stage.*` | |
| 운영자 도구 | `bin/gp-shell` · `bin/impala-shell` · `bin/s3-ops` | — | 같은 트리 | 서비스와 같은 `config.properties` | |

포트는 설정으로 바꿀 수 있고, 외부로 열어야 하는 것은 coordinator 의 `8088` 하나다. executor 포트는
같은 망 안에서 coordinator 만 호출하므로 외부에 노출할 이유가 없다.

### 1.6 용량 산정 [선택]

> **[선택]** — 이번 범위에서는 작성하지 않는다. 산정에 쓰는 계산식과 조정 순서는 운영자 가이드의
> 동시성과 용량 절에 정리돼 있다.

#### 1.6.1 개요

#### 1.6.2 Coordinator

#### 1.6.3 Executor

### 1.7 구성요소간 프로토콜 [선택]

> **[선택]** — 이번 범위에서는 작성하지 않는다.

### 1.8 구성요소간 처리 흐름 [선택]

> **[선택]** — 이번 범위에서는 작성하지 않는다. 요청 하나가 도는 순서는 같은 디렉터리의
> 시퀀스 다이어그램 문서에 있다.

---

## 2. 운영 아키텍처

### 2.1 백업/복구 방안

#### 2.1.1 백업 구성도

무엇을 어디로 백업하고 무엇을 백업하지 않는지를 한 장으로 정리하면 이렇다.

![백업 구성도](images/backup-topology.svg)

#### 2.1.2 구성요소

백업 대상은 성격이 다른 셋으로 나뉜다. 첫째는 **운영자가 만든 자산**이다. 설정과 인증서, 쿼리
템플릿, 사이트 커스텀 함수가 여기에 속하며 설치 스크립트가 덮지 않고 최초 한 번만 넣어 두는 것들
이라 잃으면 손으로 다시 만들어야 한다. 둘째는 **메타 저장소**로, 작업 상태와 실행 이력이 담긴
PostgreSQL 이다. 셋째는 **로그**이며, 사고를 추적할 때만 필요하고 날짜별로 갈려 보존 기간이 지나면
지워진다.

반대로 백업하지 않아도 되는 것이 있다. 애플리케이션 코드와 가상환경은 설치 스크립트로 다시 만들
수 있고, 스테이징 CSV 와 S3 객체는 실행 중에만 존재하는 중간 산출물이다. 무엇보다 **옮긴 데이터는
적재 대상에 있으므로 이 시스템의 백업 범위가 아니다.** 대상 Greenplum 의 백업 정책은 그쪽 운영
기준을 따른다.

#### 2.1.3 백업 대상

| 대상 | 경로 · 위치 | 주기 | 방식 | 보존 | 잃었을 때 |
|---|---|---|---|---|---|
| 설정과 인증서 | `/data1/distributed-query-executor/config/` | 변경 시 | 파일 복사 | 3세대 이상 | 접속 정보와 TLS 인증서를 다시 채워야 한다 |
| 쿼리 템플릿 | `/data1/distributed-query-executor/templates/` | 변경 시 | 파일 복사 | 3세대 이상 | 등록된 템플릿을 쓰는 요청이 모두 실패한다 |
| 사이트 커스텀 함수 | `/data1/distributed-query-executor/customs/` | 변경 시 | 파일 복사 | 3세대 이상 | 커스텀 소스와 템플릿 함수가 동작하지 않는다 |
| 메타 저장소 | PostgreSQL 데이터베이스 | 일 1회 | `pg_dump` | 기관 기준 | 진행 중 작업과 실행 이력을 잃는다 |
| 애플리케이션 로그 | `/data1/distributed-query-executor/logs/` | 필요 시 | 파일 복사 | 30일(`log.backup_count`) | 지난 사고를 추적할 수 없다 |
| 스테이징 CSV · S3 객체 | `stage.local_dir` · `s3.prefix` | — | 백업하지 않음 | — | 실행 중에만 쓰는 중간 산출물이라 영향이 없다 |
| 애플리케이션 코드 · 가상환경 | 배포 트리 · `.venv` | — | 백업하지 않음 | — | `install.sh` 로 다시 만든다 |

설정과 템플릿, 커스텀 함수는 세 디렉터리를 함께 묶어 두는 편이 편하다. 새 버전을 설치해도 이 셋은
덮이지 않으므로, 업그레이드 직전에 한 번 더 받아 두면 되돌릴 자리가 생긴다.

```bash
D=/data1/distributed-query-executor
tar czf "/backup/dqe-assets-$(date +%Y%m%d).tar.gz" -C "$D" config templates customs
```

#### 2.1.4 DB 데이터 백업/복구 방안

메타 저장소는 PostgreSQL 이므로 논리 백업으로 충분하다. 볼륨이 작고 복구 시점을 초 단위로 맞출
이유가 없기 때문이다. 하루 한 번 데이터베이스 전체를 받아 두고, 오래된 이력은 보존 기간을 정해
지운다. 앱은 오래된 행을 지우지 않으므로 이 정리는 운영 쪽에서 걸어 둔다.

```bash
PG="postgresql://user:pass@pg-host:5432/queryexec"
pg_dump -Fc "$PG" -f "/backup/queryexec-$(date +%Y%m%d).dump"     # 백업
```

복구는 세 걸음이다. 먼저 서비스를 멈춘다. 복구 중에 coordinator 가 살아 있으면 옛 상태 위에 새
상태가 겹쳐 쓰이기 때문이다. 그다음 데이터베이스를 되돌리고, 마지막으로 서비스를 다시 띄운다.

```bash
sudo -u gpadmin /data1/distributed-query-executor/bin/stop-coordinator.sh
pg_restore -d "$PG" --clean --if-exists "/backup/queryexec-20260629.dump"
sudo -u gpadmin /data1/distributed-query-executor/bin/start-coordinator.sh
```

빈 데이터베이스에서 새로 시작하는 경우라면 백업을 되돌리는 대신 스키마만 적용하면 된다. 앱도
기동할 때 테이블을 만들지만, 부분 유일 인덱스처럼 무결성에 관계된 것까지 갖추려면 배포된 DDL 을
쓰는 편이 안전하다.

```bash
psql "$PG" -f /data1/distributed-query-executor/config/postgresql.sql
```

복구 뒤에는 상태가 어긋나 있을 수 있다는 점을 감안한다. 백업 시점에 RUNNING 이었던 job 은 그
사이에 이미 끝났거나 중단됐을 수 있으므로, 복구 직후에는 진행 중으로 남은 작업을 확인하고 필요하면
실패분만 다시 돌린다.

### 2.2 보안 방안

**구간별 전송 보안은 소스 쪽에 걸린다.** executor 와 Impala 사이는 TLS 를 켜고 CA 인증서로 서버를
검증하며 LDAP 으로 인증한다. 반면 Greenplum 은 인증이나 TLS 없이 일반 DSN 으로 붙으므로, 이 구간은
망 분리와 방화벽으로 보호한다는 전제가 깔린다. 사용자와 coordinator 사이, coordinator 와 executor
사이는 평문 HTTP 이며 같은 망 안의 통신으로 본다. 외부에 열어야 하는 것은 coordinator 의 `8088`
하나이므로 방화벽은 그것만 연다.

**비밀은 설정 파일에 두고 노출 경로를 막는다.** 접속 정보는 `config.properties` 한 곳에 있고 서비스
계정만 읽도록 소유권과 권한을 좁힌다. 운영자 CLI 는 비밀번호를 명령행 인자로 받지 않는데, `ps` 로
같은 서버의 다른 사용자에게 그대로 보이기 때문이다. 설정 파일과 환경변수, 대화형 입력 순으로만
받는다. 대시보드의 환경설정 화면과 로그는 DSN 의 비밀번호와 비밀 항목을 마스킹해 내보낸다.

**노출면은 읽기 전용으로 묶는다.** 대시보드와 터미널 모니터는 조회만 하고 설정을 바꾸지 않으며,
coordinator 가 executor 화면을 대신 가져다줄 때는 임의 URL 을 프록시하지 않고 설정 목록의 인덱스로만
지정받는다. 임의 주소를 넣어 내부망을 훑는 요청을 막기 위해서다. 대시보드가 필요 없으면
`dashboard.enabled=false` 로 끈다.

**감사는 실행 SQL 로 한다.** 어떤 데이터소스에 어떤 문장을 던졌는지가 로그 레벨과 무관하게 항상
남고, 모든 줄에 작업과 task 식별자가 붙는다. 사고가 났을 때 식별자 하나로 coordinator 와 executor
의 기록을 이어 볼 수 있다. HTTP 본문까지 남기는 상세 로깅은 DEBUG 에서만 켜지며, 본문과 헤더는
마스킹을 거친다.

**에어갭을 전제로 한다.** Swagger UI 와 대시보드 폰트를 포함한 웹 에셋을 모두 트리 안에 넣어 두어
런타임에 외부로 나가지 않고, 설치도 미리 받아 둔 휠 묶음으로 한다.

계정과 권한은 최소로 잡는다. 서비스는 `gpadmin` 으로 돌고, 소스와 대상 계정은 이관에 필요한 스키마와
테이블 권한만 받는다. `s3_stage` 를 쓴다면 버킷 접근 권한도 필요한 접두사로 좁히되, 8MB 가 넘는
파일을 올릴 때 필요한 멀티파트 권한을 함께 주지 않으면 큰 파일만 실패하는 증상이 나온다.

### 2.3 가용성 방안

#### 2.3.1 네트워크 이중화

> **해당 없음** — 네트워크 구성이 확정되지 않았고 이중화도 이 시스템이 관여하는 범위가 아니다.
> 회선과 스위치 이중화는 기관의 인프라 표준을 따른다.

#### 2.3.2 Distributed Query Executor 이중화

표준 양식의 Web Server 와 WAS 이중화 자리다. 이 시스템은 두 층에서 서로 다른 방식으로 가용성을
확보한다.

**executor 는 여러 대를 두는 것이 곧 이중화다.** coordinator 가 task 를 배분할 때 살아 있고 한가한
노드를 먼저 고르고, 연결에 실패하면 짧게 쉬었다가 몇 번 다시 걸어 본 뒤 다음 후보로 넘긴다
(`coordinator.task_failover`). 한 대가 죽어도 나머지가 일을 나눠 받으므로 작업은 계속되고 용량만
줄어든다. 다만 `local_stage` 는 executor 와 Greenplum 세그먼트가 짝지어 있어 다른 곳으로 넘어가면
그 짝이 깨지므로, 이 모드에서는 failover 가 도는 것 자체가 배치나 호스트명 설정을 확인하라는
신호다.

**coordinator 는 상태를 공유해야 여러 대를 둘 수 있다.** 기본 구성은 상태를 프로세스 메모리에 두
므로, 그대로 두 대를 로드밸런서 뒤에 세우면 작업을 접수한 인스턴스만 그 상태를 알아 사용자가 자기
작업을 조회하지 못한다. 그래서 Job 저장소와 이력을 공유 PostgreSQL 로 외부화하고, 각 coordinator 는
자기 생존을 주기적으로 알린다. 신호가 끊긴 coordinator 가 쥐고 있던 작업은 다른 coordinator 가 거둬
정합한다.

```properties
store.backend=postgres                 # Job 저장소를 공유 PostgreSQL 로
history.db_dsn=postgresql://user:pass@pg-host:5432/queryexec
executor.self_report=true              # executor 가 자기 상태를 직접 기록
coordinator.executor_select=p2c        # 여러 coordinator 에서의 쏠림 완화
```

여러 대를 띄울 때 지켜야 하는 순서 관계가 있다. 장애를 판정하는 임계는 생존 신호 주기보다 넉넉히
길어야 잠깐 늦은 것을 죽음으로 오판하지 않는다. `coordinator_stale_s` 를 `heartbeat_interval_s` 의
두세 배로 두고, 예약 TTL 은 heartbeat 의 몇 배로 잡는다. 감지를 빠르게 하려면 이 값들을 한 세트로
함께 줄인다.

한 가지 더 기억할 것은 입구 한도가 인스턴스마다 따로 적용된다는 점이다. coordinator 를 두 대로
늘리면 전체 수용량도 두 배가 되므로, 다운스트림이 감당할 수 있는 총량에 맞춰 값을 나눠 잡는다.

#### 2.3.3 DB 이중화

> **해당 없음** — 메타 저장소의 이중화는 이 시스템이 관여하는 범위가 아니다. PostgreSQL 의 복제
> 구성은 기관의 DB 운영 표준을 따르며, 이 시스템은 저장소가 잠시 끊겨도 단일 coordinator 구성으로
> 계속 동작한다.

#### 2.3.4 디스크 이중화

> **해당 없음** — 디스크 이중화는 서버 인프라의 몫이다. 이 시스템이 디스크에 남기는 것은 로그와
> 스테이징 중간 산출물뿐이며, 둘 다 잃어도 재실행으로 복구된다.
