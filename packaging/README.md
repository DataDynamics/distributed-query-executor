# 배포 가이드 (RHEL 9.2, /data1 단일 트리)

이 문서는 분산 쿼리 실행기를 RHEL 9.2 서버에 처음 올려 보는 분을 위한 안내입니다. 배포란 결국 우리가 만든 프로그램을 서버에 옮겨 두고, 항상 같은 자리에서 같은 방식으로 실행되게 만드는 일입니다. 이 실행기는 명령을 받아 일을 나눠 주는 한 대의 coordinator(조율자)와, 실제로 데이터를 읽고 적재하는 여러 대의 executor(실행기)로 이루어져 있습니다. 그래서 우리는 coordinator 1개와 executor 다수를 함께 운영하도록 구성합니다.

설정을 다루는 방식도 미리 알아 두면 좋습니다. 이 프로젝트는 **`config.properties` + `config.yml`** 방식을 씁니다. 즉, 값은 `config.properties` 에 적어 두고, 그 값을 `config.yml` 이라는 본문 설정 파일이 가져다 채워 넣는 구조입니다.

> **보안 정책**: `/etc`·`/opt`·`/var` 에 파일을 추가하지 않는다. 애플리케이션·설정·로그·
> 런타임을 모두 **`/data1/distributed-query-executor`** 아래에 두고, systemd 시스템 유닛 대신
> **런처 스크립트(`bin/`)** 로 구동한다.

이 정책이 왜 중요한지 한마디 덧붙이면, 시스템 공용 디렉터리를 건드리지 않기 때문에 권한 다툼이나 다른 소프트웨어와의 충돌 없이, 모든 것이 한 폴더 안에 깔끔하게 모여 있게 됩니다. 그래서 백업도 이동도 제거도 쉬워집니다.

## 구성 파일

처음 배포할 때는 어떤 파일이 무슨 역할을 하는지부터 손에 익히는 것이 좋습니다. 아래 표는 배포에 쓰이는 스크립트와 설정 파일의 목록이며, 왼쪽 칸은 파일 이름, 오른쪽 칸은 그 파일이 하는 일입니다. 특히 `bin/` 아래의 스크립트들은 서비스를 켜고 끄고 상태를 보는 "리모컨" 같은 것이라고 생각하면 됩니다.

| 파일 | 설명 |
|---|---|
| `bin/start.sh` / `stop.sh` / `status.sh` | **전체**(coordinator + executor) 기동/중지/상태(nohup + PID) |
| `bin/start-coordinator.sh` / `stop-…` / `status-…` | **coordinator 만** 제어 |
| `bin/start-executor.sh` / `stop-…` / `status-…` | **executor 만** 제어(포트 인자 선택, 생략 시 전체) |
| `bin/check-prereqs.sh` | **사전 점검**: OS 패키지(rpm) + 파이썬 휠(.venv) 설치 여부 확인(설치는 안 함) |
| `bin/env.sh` | 런처 공통 환경 + 헬퍼 함수(경로·포트) |
| `../conf/config.properties` | Java 스타일 key=value 변수 정의 |
| `../conf/config.yml` | `${변수:기본값}` 치환을 쓰는 메인 YAML 설정 |
| `install.sh` | 사용자/디렉터리/venv/설정/런처를 한 번에 구성하는 설치 스크립트 |

표만으로는 전체 그림이 잘 안 그려질 수 있으니, 배포가 끝난 뒤 서버에서 무엇이 어디에 놓이는지를 산문으로 풀어 두겠습니다. 모든 것은 앞서 말한 한 그루의 디렉터리 나무 아래에 정리됩니다. 애플리케이션의 본체와 파이썬 가상환경(`.venv`, 이 프로젝트만을 위한 격리된 파이썬 실행 환경)은 **앱 홈**인 `/data1/distributed-query-executor` 에 자리 잡습니다. 설정 파일들은 그 아래 **설정 디렉터리**인 `/data1/distributed-query-executor/config` 에 모이며, 필요하면 환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 위치를 바꿀 수 있습니다. 프로그램이 남기는 기록인 **로그**는 `/data1/distributed-query-executor/logs` 에 쌓이는데, 하루 단위로 파일이 갈라지는 일 단위 롤링 방식이라 `파일명_YYYYMMDD.log` 형태의 이름을 갖습니다. 마지막으로 프로세스 ID 파일처럼 실행 중에만 의미가 있는 것들은 **런타임** 폴더인 `/data1/distributed-query-executor/run` 에 둡니다.

executor 는 한 대만 띄우는 것이 아니라 **포트별로 여러 인스턴스**를 띄울 수 있습니다. 예를 들어 `EXECUTOR_PORTS="8087 8086"` 처럼 지정하면 두 개의 executor 가 각각의 포트에서 동시에 일합니다. 그런데 여기서 한 가지 중요한 원칙이 있습니다. coordinator 와 executor 는 둘 다 자신의 상태를 **프로세스 메모리**에 담아 두기 때문에, 인스턴스 하나는 반드시 **단일 워커**로 실행해야 합니다. 그래서 더 많은 일을 처리하고 싶다면 한 프로세스 안의 워커 수를 늘리는 것이 아니라, **executor 인스턴스 수**를 늘리는 방식으로 확장합니다.

> **`local_stage`(file:// 세그먼트 로컬 스테이징) 배치는 다르다.** 기본 `copy`/`stage_insert` 모드는 executor 를 어디에 두든 상관없지만, `local_stage`(DESIGN §17)는 executor 가 읽은 데이터를 **자기 호스트 로컬 디스크의 CSV** 로 떨어뜨리고 GP 세그먼트가 그 로컬 파일을 `file://` 로 직접 읽습니다. 그래서 이 모드를 쓰려면 executor 를 **각 Greenplum 세그먼트 호스트에 co-locate**(한 호스트당 하나 이상) 해야 하며, 추가로 (1) `stage.local_dir` 을 모든 세그먼트 호스트에 **동일 경로**로 두고 GP 세그먼트 프로세스(보통 `gpadmin`)가 read 가능하도록 권한을 맞추고, (2) `executor.gp_hostname` 을 그 호스트의 `gp_segment_configuration.hostname` 과 일치시키며(미설정 시 OS hostname), (3) coordinator 의 `greenplum.dsn` 은 GP master 를 가리켜야 합니다(Phase 2 적재·검증·토폴로지 조회). 운영 시나리오는 [SCENARIO.md](../SCENARIO.md) 참고.

## 빠른 설치 (스크립트 사용)

이제 실제로 설치해 보겠습니다. 다행히 손이 많이 가는 일은 `install.sh` 한 스크립트가 대신 해 주므로, 우리가 직접 칠 명령은 몇 줄뿐입니다. 아래 단계를 위에서 아래로 순서대로 따라가면 됩니다.

```bash
# 0) (최초 1회) Python 3.9(RHEL 9.2 기본) + rsync 설치
sudo dnf install -y python3 python3-pip python3-devel rsync

# 1) 저장소 루트에서 실행 (에어갭이면 WHEELHOUSE/INSTALL_EXECUTOR 지정)
sudo ./packaging/install.sh
#   에어갭 예: sudo WHEELHOUSE=/path/wheels INSTALL_EXECUTOR=1 ./packaging/install.sh

# 2) 설정 확인/수정
sudo vi /data1/distributed-query-executor/config/config.properties   # executors, impala.*, greenplum.dsn 등

# 3) 서비스 기동 (executor 2개 + coordinator)
sudo -u gpadmin /data1/distributed-query-executor/bin/start.sh
sudo -u gpadmin /data1/distributed-query-executor/bin/status.sh
```

각 단계를 말로 풀면 이렇습니다. 0번은 최초 한 번만 하면 되는 준비로, RHEL 9.2 에 기본으로 들어 있는 Python 3.9 와 파일 복사에 쓰이는 rsync 를 설치합니다. 1번이 핵심인데, 저장소 루트에서 `install.sh` 를 실행하면 설치가 한 번에 이루어집니다. 만약 외부 인터넷이 막혀 있는 환경, 즉 **에어갭**(망 분리되어 외부 네트워크에 연결되지 않은 폐쇄망)이라면 미리 받아 둔 파이썬 휠 묶음의 경로를 `WHEELHOUSE` 로 알려 주고, executor 드라이버까지 함께 설치하려면 `INSTALL_EXECUTOR=1` 을 붙여 줍니다. 2번에서는 설치된 설정 파일을 열어 우리 환경에 맞게 고칩니다(executor 목록, Impala 접속 정보, Greenplum DSN 등). 마지막 3번에서 서비스를 띄우고 상태를 확인하면 설치가 마무리됩니다.

그렇다면 `install.sh` 는 우리 대신 정확히 무엇을 해 주는 걸까요? 다음과 같은 일들을 차례로 처리합니다.

- 서비스 계정 `gpadmin` 생성(홈 `/data1`)
- 앱을 `/data1/distributed-query-executor` 로 복사(`.venv`/`.git`/`logs`/`config`/`run` 제외)
- `/data1/distributed-query-executor/.venv` 가상환경 + 의존성 설치(`WHEELHOUSE` 지정 시 오프라인)
- `conf/*` 를 `config/` 로 배치(없을 때만), 로그 경로를 `/data1/distributed-query-executor/logs` 로 설정
- TLS 자리표시 파일 생성(`config/impala-ca.pem`)
- 런처 스크립트를 `bin/` 으로 배치, 소유권/권한 설정

## 사전 점검 (check-prereqs.sh)

설치를 시작하기 전이나 끝낸 후에 "필요한 것이 다 갖춰졌는지"를 미리 확인하고 싶을 때가 있습니다. 그럴 때 쓰는 것이 `check-prereqs.sh` 입니다. 이 스크립트는 **OS 패키지**와 **파이썬 휠**(파이썬 패키지를 미리 빌드해 둔 설치 파일)이 제대로 준비되었는지 **확인만** 하고, 무언가를 설치하지는 않습니다. 결과를 종료코드로도 알려 주는데, 모든 항목이 충족되면 `0`, 하나라도 빠지면 `1` 을 돌려줍니다. 그래서 자동화 파이프라인이나 배포 전 점검 게이트의 통과/실패 판정에도 그대로 끼워 넣을 수 있습니다.

```bash
# OS 패키지(rpm) + 휠(.venv) 점검
./bin/check-prereqs.sh

# 한쪽만 점검
OS_ONLY=1     ./bin/check-prereqs.sh   # OS 패키지만
WHEELS_ONLY=1 ./bin/check-prereqs.sh   # 휠만
```

위 명령들이 무엇을 들여다보는지 이어서 설명하겠습니다. 먼저 **OS 패키지** 점검은 `rpm -q` 명령으로 빌드에 쓰이는 도구들과 SASL 관련 의존성이 깔려 있는지 확인합니다. 구체적으로는 `gcc gcc-c++ make python3-devel python3 python3-pip cyrus-sasl-devel` 가 대상입니다. 다음으로 **파이썬 휠** 점검은 `packaging/wheels/py<버전>/` 폴더에 들어 있는 `.whl`·`.tar.gz` 파일 이름에서 패키지 이름과 버전을 뽑아낸 뒤, 실제 `.venv` 에 설치된 목록과 하나하나 대조합니다. 그 결과는 일치하면 `[OK]`, 아직 설치되지 않았으면 `[MISSING]`, 버전이 어긋나면 `[VER ?]`(이쪽은 실패가 아니라 경고일 뿐) 로 표시됩니다. 마지막으로 점검에 쓰이는 경로는 환경변수로 바꿀 수 있습니다. 검사할 파이썬은 `VENV_PY` 로, 휠 묶음의 루트는 `WHEELS_ROOT` 로 지정하며, 실제 배포 대상 서버에서는 보통 `VENV_PY=/data1/distributed-query-executor/.venv/bin/python` 처럼 그 서버의 가상환경 파이썬을 가리키도록 둡니다.

## 설정 항목 (config.properties)

설치가 끝났다면 이제 우리 환경에 맞게 설정을 손볼 차례입니다. 아래는 `config.properties` 의 주요 항목을 모아 둔 것이며, 각 줄 끝의 주석이 그 값이 무엇을 뜻하는지 알려 줍니다. 처음에는 전부 이해하려 애쓰기보다, coordinator 의 주소와 executor 목록, 그리고 Impala·Greenplum 접속 정보부터 채운다는 마음으로 읽으면 됩니다.

```properties
# Coordinator
coordinator.host=0.0.0.0
coordinator.port=8088
coordinator.executors=http://127.0.0.1:8087,http://127.0.0.1:8086
coordinator.id=                        # 멀티 coordinator 식별자(미지정 시 host:port)
coordinator.executor_mode=remote       # remote(HTTP 디스패치) | local(in-process 직접 실행)

# 동시성/큐잉(admission control)
coordinator.max_concurrent_jobs=16     # 동시에 RUNNING 가능한 job 수(실행 슬롯)
coordinator.max_pending_jobs=100       # 슬롯이 차면 PENDING 으로 대기 가능한 job 수
                                        #  → 실행+대기 합을 넘는 요청은 429(Retry-After)로 거부
coordinator.max_dispatch_concurrency=32 # 동시 task 디스패치 상한(코루틴 동시성)

# Executor 동시 task 상한(executor 1대 기준, admission control)
executor.max_concurrent_tasks=8

# Executor - Impala (source). 비어 있으면 MockBackend 사용. 기본 TLS + LDAP 인증
impala.host=
impala.port=21050
impala.database=default
impala.auth_mechanism=LDAP        # LDAP(기본) | PLAIN | NOSASL
impala.use_ssl=true
impala.ca_cert=/data1/distributed-query-executor/config/impala-ca.pem
impala.user=                      # LDAP 바인드 사용자
impala.password=                  # LDAP 비밀번호

# Executor - Greenplum (target). 비어 있으면 MockBackend 사용. TLS 미적용(일반 DSN)
greenplum.dsn=
copy.batch_size=10000
```

여기서 한 가지 꼭 기억할 점이 있습니다. 이 실행기는 Impala 에서 데이터를 읽어 Greenplum 으로 옮기는 일을 하는데, 그 두 곳의 접속 정보가 모두 채워져야만 진짜로 동작합니다.

> `impala.host` 와 `greenplum.dsn` 이 **모두** 설정되면 실제 `ImpalaToGreenplumBackend`
> 가 동작하고, 하나라도 비어 있으면 `MockBackend`(실제 I/O 없음)로 폴백한다.
> 실제 연결 시에는 `requirements-executor.txt` 도 설치해야 한다(impyla, psycopg, SASL).

동시성 값을 어느 정도로 잡아야 할지도 처음에는 헷갈리기 쉽습니다. 핵심은 진짜 한계가 coordinator 의 성능이 아니라 그 뒤에 있는 데이터베이스들의 수용량이라는 점입니다.

> **동시성 적정값**: 실제 천장은 coordinator 코어가 아니라 Greenplum 동시 COPY 허용량·
> Impala 동시 쿼리 슬롯·executor 풀 합이다. 다운스트림 용량에 맞춰
> `executor.max_concurrent_tasks` 를 분배하고, `max_dispatch_concurrency` 는 그 이상으로
> 두어 coordinator 가 병목이 되지 않게 한다.

## Impala TLS + 인증 (LDAP)

> **기본 인증은 LDAP 입니다.** `impala.auth_mechanism=LDAP`(기본값)이면 `impala.user`/
> `impala.password` 에 LDAP 바인드 자격증명만 채우면 되고, 비밀번호 보호를 위해
> `impala.use_ssl=true` + `impala.ca_cert` 로 TLS 를 함께 쓰는 것을 권장합니다.

이 부분은 보안 접속이 걸려 있는 Impala 에 연결할 때만 필요합니다. 먼저 큰 그림을 잡고 갑시다. 데이터의 원천인 Impala 에 실제로 접속하는 쪽은 executor 이고, coordinator 는 여기에 관여하지 않습니다. 그리고 보안 방식이 양쪽이 다릅니다. **Impala 에만 TLS(통신 암호화)와 LDAP 인증**이 적용되고, 데이터를 적재하는 **Greenplum 은 TLS 없이 일반 DSN** 으로 접속합니다.

설정은 아래 순서대로 진행합니다. 각 단계의 주석에 무엇을 하는지 적어 두었습니다.

```bash
# 0) 시스템 패키지 (RHEL 9.2)
sudo dnf install -y cyrus-sasl-devel gcc gcc-c++ make python3-devel

# 1) executor 드라이버 + SASL 설치(설치 시 INSTALL_EXECUTOR=1 했으면 생략)
sudo /data1/distributed-query-executor/.venv/bin/pip install -r /data1/distributed-query-executor/requirements-executor.txt

# 2) TLS CA 인증서 배치(임의 파일명 가능 — config.properties 의 impala.ca_cert 와 일치시킬 것)
sudo cp impala-ca.pem /data1/distributed-query-executor/config/impala-ca.pem
sudo chown -R gpadmin:gpadmin /data1/distributed-query-executor/config

# 3) config.properties 의 impala.user / impala.password(LDAP 바인드 자격증명) 설정
sudo vi /data1/distributed-query-executor/config/config.properties
```

## 멀티 coordinator & 실행 이력 (PostgreSQL)

여기서는 한 단계 더 나아간 구성을 다룹니다. 기본 설정에서는 coordinator 가 한 대뿐이고 모든 상태를 자기 메모리 안에만 둡니다. 처음에는 이 단순한 형태로 충분합니다. 하지만 **여러 대의 coordinator** 를 함께 두어 가용성을 높이고 싶거나, 서버를 재시작해도 **실행 이력이 사라지지 않도록 영속**하고 싶다면, 모두가 함께 바라볼 공유 PostgreSQL 을 설정합니다. 핵심은 모든 coordinator 와 executor 가 동일한 DSN(데이터베이스 접속 문자열)을 공유한다는 것입니다.

```properties
# 모든 coordinator/executor 공통
history.db_dsn=postgresql://user:pass@pg-host:5432/queryexec
history.table=job_history
history.task_table=task_history

# 멀티 coordinator: Job 저장소를 공유 PostgreSQL 로 (기본 memory)
store.backend=postgres
store.table=jobs

# executor 가 자기 상태를 공유 DB에 직접 기록(coordinator 중복 폴링 제거)
executor.self_report=true
executor.status_table=executor_status
executor.status_interval_s=10
```

위 설정 항목들이 각각 어떤 효과를 내는지 풀어 보겠습니다. 첫째, **공유 Job 저장소**(`store.backend=postgres`)를 켜면 작업 상태가 `jobs` 테이블에 JSONB 형태로 저장됩니다. 덕분에 어느 coordinator 로 조회하거나 취소 요청을 보내도 똑같이 동작하며, 한 coordinator 에서 낸 취소가 다른 coordinator 에까지 미치는 cross-coordinator 취소도 플래그 공유를 통해 이루어집니다. 둘째, 이력은 **2계층**으로 나뉘어 기록됩니다. 작업의 시작과 종료(`run()`)는 coordinator 가 `job_history` 에 남기고, 각 task 의 상태 변화는 executor 가 `executor_id` 를 포함해 `task_history` 에 남깁니다. 작업을 제출할 때 `username` 을 함께 넘기면 두 테이블 모두에 사용자가 기록되어 대시보드의 "사용자" 컬럼에 나타납니다. 이때 task 이력은 executor 가 직접 쓰므로 **executor 호스트에도 PG 자격증명이 필요**하다는 점을 잊지 마세요. 셋째, **executor 의 생존 확인**(liveness)을 위해 `self_report=true` 로 두면 각 executor 가 `executor_status` 테이블에 heartbeat(살아 있다는 신호)를 주기적으로 갱신(upsert)하고, coordinator 는 그 행의 `updated_at` 이 얼마나 최근인지(신선도)를 보고 살아 있는지 판단합니다. 끝으로 한 가지 주의할 점은, 동시 실행을 제한하는 admission 한도(`max_concurrent_jobs`·`max_pending_jobs`)가 **coordinator 인스턴스마다 따로(인메모리)** 적용된다는 것입니다. 그래서 coordinator 를 여러 대 띄우면 전체 한도는 인스턴스 수만큼 곱해져 합산됩니다.

다음 경고는 멀티 coordinator 구성에서 가장 흔히 발을 헛디디는 지점이니 꼭 짚고 넘어가야 합니다.

⚠️ **앱은 스키마를 자동 생성하지 않는다.** 서비스 기동 **전에** 통합 스키마 한 파일로 전체
테이블(jobs/job_history/task_history/executor_status/executor_health_metrics)을 먼저 만든다
(안 하면 "relation does not exist"로 실패):

```bash
PG="postgresql://user:pass@pg-host:5432/queryexec"
psql "$PG" -f /data1/distributed-query-executor/conf/postgresql.sql
```

반대로, 이런 고급 구성이 필요 없는 분도 많을 것입니다. 그래서 단순한 경우의 권장값을 함께 적어 둡니다.

> 단일 coordinator면 기본값(`store.backend=memory`, `executor.self_report=false`) 그대로 둔다.
> 이력만 남기고 싶으면 `history.db_dsn` 만 설정해도 된다(저장소/ self-report 는 끄고).

메타 저장소를 일반 PostgreSQL 이 아니라 WarehousePG 나 Greenplum 7 에 두려는 경우를 위한 안내도 있습니다. WarehousePG 는 Greenplum 계열의 MPP(대규모 병렬 처리) 데이터베이스인데, 데이터를 여러 노드에 나눠 분산하는 특성이 있어 스키마를 조금 다르게 만들어 줘야 합니다.

> **WarehousePG / Greenplum 7 에 메타 저장소를 둘 때**는 `postgresql.sql` 대신
> [`warehousepg.sql`](../conf/warehousepg.sql) 을 적용한다(테이블마다 `DISTRIBUTED BY`
> 지정, history/metrics 는 대리 PK 를 빼고 `job_id`/`executor_url` 로 co-locate). 앱 코드는
> 그대로다(`ON CONFLICT`·`JSONB`·`DISTINCT ON` 모두 GP7=PG12 에서 지원). 다만 heartbeat/예약은
> 고빈도 단일행 UPSERT 라 MPP 와 맞지 않으므로, 성능이 중요하면 이 메타 저장소는 PostgreSQL 에
> 두고 WarehousePG 는 데이터 적재 대상(`greenplum.dsn`)으로만 쓰는 편이 낫다.
> ```bash
> psql "$PG" -f /data1/distributed-query-executor/conf/warehousepg.sql
> ```

## 운영 명령

서비스를 일단 띄우고 나면, 그 다음부터는 날마다 상태를 살피고 로그를 들여다보고 가끔 재시작하는 운영 작업이 이어집니다. 자주 쓰는 명령들을 한곳에 모아 두었으니, 필요할 때 골라 쓰면 됩니다. 맨 앞의 `B=...` 줄은 긴 경로를 매번 치지 않으려고 `$B` 라는 짧은 이름에 담아 두는 것입니다.

```bash
B=/data1/distributed-query-executor/bin
# 상태(프로세스 + health) — 전체 / 역할별
sudo -u gpadmin $B/status.sh
sudo -u gpadmin $B/status-coordinator.sh
sudo -u gpadmin $B/status-executor.sh

# 파일 로그(일 단위 롤링)
tail -f /data1/distributed-query-executor/logs/query-coordinator-server.log
tail -f /data1/distributed-query-executor/logs/query-executor-server-8087.log

# WARNING 이상만 모은 전용 로그(문제 추적용, *-warn.log)
tail -f /data1/distributed-query-executor/logs/query-coordinator-server-warn.log
tail -f /data1/distributed-query-executor/logs/query-executor-server-8087-warn.log

# 재시작(전체) / 중지
sudo -u gpadmin $B/stop.sh && sudo -u gpadmin $B/start.sh

# 역할별 제어(coordinator / executor 따로)
sudo -u gpadmin $B/stop-coordinator.sh        # coordinator 만 중지
sudo -u gpadmin $B/start-executor.sh 8086     # executor 8086 만 기동/재기동
sudo -u gpadmin $B/stop-executor.sh  8086     # executor 8086 만 중지

# executor 인스턴스 추가(포트 8003): config.properties 의 executors 에 추가 후
sudo -u gpadmin $B/start-executor.sh 8003     # 또는 전체: EXECUTOR_PORTS="8087 8086 8003" $B/start.sh
```

몇 가지는 처음 보면 의아할 수 있어 덧붙입니다. 로그가 두 종류라는 점에 주목하세요. 일반 로그 외에 `*-warn.log` 라는 별도 파일이 있는데, 여기에는 WARNING 이상의 메시지만 따로 모입니다. 그래서 평소에는 일반 로그를 보다가, 문제를 추적할 때는 경고 로그만 빠르게 훑으면 원인에 더 빨리 다가갈 수 있습니다. 또 한 가지, executor 를 새로 한 대 늘리고 싶다면 무작정 스크립트만 실행하면 안 되고, 먼저 `config.properties` 의 executor 목록에 그 포트를 추가한 뒤에 해당 executor 를 기동해야 coordinator 가 새 인스턴스를 인식합니다.

## 동작 확인

설치와 기동을 마쳤다면, 정말 잘 살아 있는지 직접 두드려 보고 싶을 것입니다. 아래 명령들로 헬스 상태와 메트릭을 조회하고, 실제로 작은 작업을 하나 제출해 끝까지 도는지 확인할 수 있습니다.

```bash
# 헬스 / 메트릭(CPU·메모리·디스크)
curl -s localhost:8088/health
curl -s localhost:8088/metrics
curl -s localhost:8087/health
curl -s localhost:8087/metrics
# coordinator가 보유한 executor 헬스/메트릭 상태 + 클러스터 통합 상태
curl -s localhost:8088/executors
curl -s localhost:8088/cluster

# 모니터링 대시보드(브라우저): coordinator http://<host>:8088/
#   remote 모드면 각 executor 도 자기 화면 제공: http://<host>:8087/
# Swagger UI / OpenAPI 스키마
#   http://<host>:8088/docs , http://<host>:8087/docs
curl -s localhost:8088/openapi.json | head -c 200

curl -s localhost:8088/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('\''2026-01-01'\'','\''2026-01-02'\'') AND region='\''KR'\''",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "parallelism": 2
}'
```

명령줄이 익숙하지 않다면 브라우저를 열어도 됩니다. coordinator 의 `http://<host>:8088/` 에 접속하면 사람이 보기 좋은 모니터링 대시보드가 나오고, remote 모드에서는 각 executor 도 `http://<host>:8087/` 처럼 자기 화면을 따로 제공합니다. API 를 직접 살펴보고 싶다면 `/docs` 경로의 Swagger UI 를 이용하면 됩니다. 마지막 `curl ... /jobs` 명령은 실제 작업을 하나 제출하는 예시인데, `sales` 테이블에서 두 날짜의 데이터를 읽어 `public.sales_mirror` 로 2분할 병렬 적재하라는 뜻입니다. 이 한 번이 성공하면 전체 경로가 살아 있다는 좋은 신호입니다.

## 방화벽(firewalld)

서버 안에서는 잘 돌더라도, 바깥에서 coordinator 에 접근하려면 방화벽에 길을 터 줘야 할 수 있습니다. 외부에서 coordinator(8088)에 접근해야 한다면 다음과 같이 8088 포트를 영구적으로 열고 방화벽 설정을 다시 읽어 들입니다.

```bash
sudo firewall-cmd --permanent --add-port=8088/tcp
sudo firewall-cmd --reload
```

반면 executor 포트(8087, 8086 ...)는 보통 coordinator 와 같은 호스트 안에서만 주고받는 내부 통신이므로, 굳이 외부로 열어 둘 필요가 없습니다.

## 헬스/메트릭 모니터링

운영을 오래 하다 보면 단순히 살아 있는지를 넘어, 시간에 따라 자원을 얼마나 쓰는지 추세를 보고 싶어집니다. 그 토대가 되는 것이 헬스와 메트릭 기능입니다. 두 서비스는 모두 살아 있는지를 알려 주는 `/health`(liveness) 와 CPU·메모리·디스크 사용량을 알려 주는 `/metrics` 를 제공합니다. 그리고 coordinator 는 `monitor.health_interval_s` 마다 모든 executor 의 `/health`·`/metrics` 를 폴링해 그 상태를 자기 안에 보유하며(이는 `GET /executors` 로 조회할 수 있습니다), `monitor.record_interval_s` 마다 그 사용량을 PostgreSQL 테이블(`monitor.table`)에 차곡차곡 기록합니다.

이 동작을 제어하는 설정은 다음과 같습니다(`config.properties`).

```properties
monitor.enabled=true
monitor.health_interval_s=10
monitor.record_interval_s=60
# 기록 대상 PostgreSQL DSN. 비어 있으면 폴링만 하고 DB 기록은 하지 않는다.
monitor.db_dsn=postgresql://user:pass@pg-host:5432/monitoring
monitor.table=executor_health_metrics
monitor.disk_path=/
```

여기서도 앞서 말한 원칙이 똑같이 적용됩니다. 메트릭을 담는 `executor_health_metrics` 테이블 역시 앱이 알아서 만들어 주지 않으므로, 통합 스키마인 `conf/postgresql.sql` 을 **먼저 적용**해야 합니다. 만약 `monitor.db_dsn` 이 다른 데이터베이스를 가리킨다면 그 데이터베이스에도 동일하게 스키마를 적용해 주어야 합니다.

```bash
psql "postgresql://user:pass@pg-host:5432/monitoring" -f /data1/distributed-query-executor/conf/postgresql.sql

# 최근 기록 조회
psql ... -c "SELECT recorded_at, executor_url, healthy, cpu_percent, memory_percent
             FROM executor_health_metrics ORDER BY recorded_at DESC LIMIT 20;"
```

## 참고

마지막으로, 앞에서 군데군데 나왔지만 운영 내내 기억해 두면 좋은 핵심 원칙 세 가지를 다시 정리합니다.

- coordinator·executor 모두 상태를 **프로세스 메모리**에 두므로 인스턴스당 **단일 워커**로
  실행한다. 처리량 확장은 워커가 아니라 **executor 인스턴스 수**로 한다.
- coordinator를 **다중 인스턴스**로 띄우려면 `store.backend=postgres` + 공유 `history.db_dsn`
  으로 Job 저장소/이력을 PostgreSQL에 외부화한다(위 "멀티 coordinator & 실행 이력" 참고).
- 별도 executor 프로세스 없이 동작을 검증하려면 `coordinator.executor_mode=local`(또는
  `COORDINATOR_EXECUTOR_MODE=local`)로 coordinator 안에서 백엔드를 직접 실행한다.
