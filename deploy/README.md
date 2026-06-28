# 배포 가이드 (RHEL 9.2, /appuser 단일 트리)

coordinator 1개와 executor 다수를 운영하기 위한 구성이다.
설정은 **`config.properties` + `config.yml`** 방식을 쓴다.

> **보안 정책**: `/etc`·`/opt`·`/var` 에 파일을 추가하지 않는다. 애플리케이션·설정·로그·
> 런타임을 모두 **`/appuser/query-executor`** 아래에 두고, systemd 시스템 유닛 대신
> **런처 스크립트(`bin/`)** 로 구동한다.

## 구성 파일

| 파일 | 설명 |
|---|---|
| `bin/start.sh` / `stop.sh` / `status.sh` | coordinator + executor 기동/중지/상태(nohup + PID) |
| `bin/kinit-renew.sh` | Impala Kerberos 티켓 발급/갱신(keytab → 공유 ccache) |
| `bin/env.sh` | 런처 공통 환경(경로·`KRB5_CONFIG`·`KRB5CCNAME`·포트) |
| `../packaging/config/config.properties` | Java 스타일 key=value 변수 정의 |
| `../packaging/config/config.yml` | `${변수:기본값}` 치환을 쓰는 메인 YAML 설정 |
| `install.sh` | 사용자/디렉터리/venv/설정/런처를 한 번에 구성하는 설치 스크립트 |

- **앱 홈**: `/appuser/query-executor` (소스 + `.venv`)
- **설정 디렉터리**: `/appuser/query-executor/config` (환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 변경 가능)
- **로그**: `/appuser/query-executor/logs` (일 단위 롤링, `파일명_YYYYMMDD.log`)
- **런타임**: `/appuser/query-executor/run` (PID, Kerberos ccache)
- **executor는 포트별로 여러 인스턴스**를 띄운다(`EXECUTOR_PORTS="8087 8086"`).
- coordinator·executor 모두 상태를 **프로세스 메모리**에 두므로 인스턴스당 **단일 워커**로 실행한다. 처리량 확장은 워커가 아니라 **executor 인스턴스 수**로 한다.

## 빠른 설치 (스크립트 사용)

```bash
# 0) (최초 1회) Python 3.9(RHEL 9.2 기본) + rsync 설치
sudo dnf install -y python3 python3-pip python3-devel rsync

# 1) 저장소 루트에서 실행 (에어갭이면 WHEELHOUSE/INSTALL_EXECUTOR 지정)
sudo ./deploy/install.sh
#   에어갭 예: sudo WHEELHOUSE=/path/wheels INSTALL_EXECUTOR=1 ./deploy/install.sh

# 2) 설정 확인/수정
sudo vi /appuser/query-executor/config/config.properties   # executors, impala.*, greenplum.dsn 등

# 3) 서비스 기동 (executor 2개 + coordinator)
sudo -u appuser /appuser/query-executor/bin/start.sh
sudo -u appuser /appuser/query-executor/bin/status.sh
```

`install.sh`가 하는 일:
- 서비스 계정 `appuser` 생성(홈 `/appuser`)
- 앱을 `/appuser/query-executor` 로 복사(`.venv`/`.git`/`logs`/`config`/`run` 제외)
- `/appuser/query-executor/.venv` 가상환경 + 의존성 설치(`WHEELHOUSE` 지정 시 오프라인)
- `packaging/config/*` 를 `config/` 로 배치(없을 때만), 로그 경로를 `/appuser/query-executor/logs` 로 설정
- Kerberos+TLS 자리표시 파일 생성(`config/krb5.conf`·`impala-ca.pem`·`impala.keytab`)
- 런처 스크립트를 `bin/` 으로 배치, 소유권/권한 설정

## 설정 항목 (config.properties)

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

# Executor - Impala (source). 비어 있으면 MockBackend 사용. TLS + Kerberos(GSSAPI)
impala.host=
impala.port=21050
impala.database=default
impala.auth_mechanism=GSSAPI
impala.kerberos_service_name=impala
impala.use_ssl=true
impala.ca_cert=/appuser/query-executor/config/impala-ca.pem

# Executor - Greenplum (target). 비어 있으면 MockBackend 사용. TLS/Kerberos 미적용(일반 DSN)
greenplum.dsn=
copy.batch_size=10000
```

> `impala.host` 와 `greenplum.dsn` 이 **모두** 설정되면 실제 `ImpalaToGreenplumBackend`
> 가 동작하고, 하나라도 비어 있으면 `MockBackend`(실제 I/O 없음)로 폴백한다.
> 실제 연결 시에는 `requirements-executor.txt` 도 설치해야 한다(impyla, psycopg, SASL/GSSAPI).

> **동시성 적정값**: 실제 천장은 coordinator 코어가 아니라 Greenplum 동시 COPY 허용량·
> Impala 동시 쿼리 슬롯·executor 풀 합이다. 다운스트림 용량에 맞춰
> `executor.max_concurrent_tasks` 를 분배하고, `max_dispatch_concurrency` 는 그 이상으로
> 두어 coordinator 가 병목이 되지 않게 한다.

## Impala TLS + Kerberos

executor만 Impala에 접속한다(coordinator는 무관). **Impala 에만 TLS + Kerberos(GSSAPI)**
가 적용되고, **Greenplum 은 TLS/Kerberos 없이 일반 DSN** 으로 접속한다. 시스템
`/etc/krb5.conf` 대신 `/appuser/query-executor/config/krb5.conf` 를 `KRB5_CONFIG` 로 사용한다.

```bash
# 0) 시스템 패키지 (RHEL 9.2) — gssapi 빌드에 필요
sudo dnf install -y krb5-workstation krb5-devel cyrus-sasl-devel cyrus-sasl-gssapi \
    gcc gcc-c++ make python3-devel

# 1) executor 드라이버 + SASL/GSSAPI 설치(설치 시 INSTALL_EXECUTOR=1 했으면 생략)
sudo /appuser/query-executor/.venv/bin/pip install -r /appuser/query-executor/requirements-executor.txt

# 2) TLS CA 인증서 배치(임의 파일명 가능 — config.properties 의 impala.ca_cert 와 일치시킬 것)
sudo cp impala-ca.pem /appuser/query-executor/config/impala-ca.pem

# 3) Kerberos keytab 배치 (appuser 만 읽도록 600)
sudo cp impala.keytab /appuser/query-executor/config/impala.keytab

# 4) krb5.conf 의 realm/KDC, kinit-renew.sh 의 principal/keytab 확인
sudo vi /appuser/query-executor/config/krb5.conf
sudo chown -R appuser:appuser /appuser/query-executor/config
sudo chmod 600 /appuser/query-executor/config/impala.keytab

# 5) 티켓 발급(이후 cron 으로 주기 갱신: 예 0 */4 * * *)
sudo -u appuser KRB5_PRINCIPAL=svc-query@EXAMPLE.LOCAL /appuser/query-executor/bin/kinit-renew.sh
```

동작 방식:
- `bin/kinit-renew.sh` 가 keytab 으로 `/appuser/query-executor/run/krb5cc`
  공유 자격증명 캐시에 티켓을 발급한다(`KRB5_CONFIG`·`KRB5CCNAME` 는 `bin/env.sh` 가 export).
- executor 프로세스도 동일한 `KRB5CCNAME=FILE:/appuser/query-executor/run/krb5cc` 를 상속한다.
- 만료 방지를 위해 `kinit-renew.sh` 를 cron 으로 주기 실행한다(appuser crontab).
- 티켓 확인: `sudo -u appuser KRB5CCNAME=FILE:/appuser/query-executor/run/krb5cc klist`

## 멀티 coordinator & 실행 이력 (PostgreSQL)

기본은 단일 coordinator + 인메모리다. **여러 coordinator**를 두거나 **실행 이력을 영속**하려면
공유 PostgreSQL을 설정한다(모든 coordinator·executor가 같은 DSN을 공유).

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

- **공유 Job 저장소(`store.backend=postgres`)**: `jobs` 테이블(JSONB)에 상태를 두어, 어느
  coordinator로 조회/취소가 가도 동작한다. cross-coordinator 취소도 플래그 공유로 동작.
- **2계층 이력**: `run()` 시작/종료는 `job_history`(coordinator), 각 task 상태 전이는
  `task_history`(executor, `executor_id` 포함)에 기록된다. 제출 시 `username` 을 넘기면 두
  테이블 모두에 기록된다(대시보드 "사용자" 컬럼). **executor 호스트에도 PG 자격증명이 필요**하다.
- **executor liveness**: `self_report=true` 면 executor 가 `executor_status` 에 heartbeat 를
  upsert 하고, coordinator 는 `updated_at` 신선도로 살아있음을 판정한다.
- admission 한도(`max_concurrent_jobs`/`max_pending_jobs`)는 **coordinator 인스턴스별**(인메모리)
  이므로, 멀티 coordinator 에선 인스턴스 수만큼 합산된다.

⚠️ **앱은 스키마를 자동 생성하지 않는다.** 서비스 기동 **전에** 통합 스키마 한 파일로 전체
테이블(jobs/job_history/task_history/executor_status/executor_health_metrics)을 먼저 만든다
(안 하면 "relation does not exist"로 실패):

```bash
PG="postgresql://user:pass@pg-host:5432/queryexec"
psql "$PG" -f /appuser/query-executor/packaging/config/postgresql.sql
```

> 단일 coordinator면 기본값(`store.backend=memory`, `executor.self_report=false`) 그대로 둔다.
> 이력만 남기고 싶으면 `history.db_dsn` 만 설정해도 된다(저장소/ self-report 는 끄고).

## 운영 명령

```bash
B=/appuser/query-executor/bin
# 상태(프로세스 + health)
sudo -u appuser $B/status.sh

# 파일 로그(일 단위 롤링)
tail -f /appuser/query-executor/logs/query-coordinator-server.log
tail -f /appuser/query-executor/logs/query-executor-server-8087.log

# WARNING 이상만 모은 전용 로그(문제 추적용, *-warn.log)
tail -f /appuser/query-executor/logs/query-coordinator-server-warn.log
tail -f /appuser/query-executor/logs/query-executor-server-8087-warn.log

# 재시작(전체) / 중지
sudo -u appuser $B/stop.sh && sudo -u appuser $B/start.sh

# executor 인스턴스 추가(포트 8003): config.properties 의 executors 에 추가 후
sudo -u appuser EXECUTOR_PORTS="8087 8086 8003" $B/start.sh
```

## 동작 확인

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

## 방화벽(firewalld)

외부에서 coordinator(8088)에 접근해야 한다면:

```bash
sudo firewall-cmd --permanent --add-port=8088/tcp
sudo firewall-cmd --reload
```

executor 포트(8087, 8086 ...)는 보통 coordinator와 같은 호스트 내부 통신이므로 외부 개방이 불필요하다.

## 헬스/메트릭 모니터링

- 두 서비스 모두 `/health`(liveness), `/metrics`(CPU·메모리·디스크) 를 제공한다.
- coordinator는 `monitor.health_interval_s` 마다 모든 executor의 `/health`·`/metrics` 를
  폴링해 상태를 보유하고(`GET /executors` 로 조회), `monitor.record_interval_s` 마다
  PostgreSQL 테이블(`monitor.table`)에 CPU/메모리/디스크 사용량을 기록한다.

설정(config.properties):

```properties
monitor.enabled=true
monitor.health_interval_s=10
monitor.record_interval_s=60
# 기록 대상 PostgreSQL DSN. 비어 있으면 폴링만 하고 DB 기록은 하지 않는다.
monitor.db_dsn=postgresql://user:pass@pg-host:5432/monitoring
monitor.table=executor_health_metrics
monitor.disk_path=/
```

테이블(`executor_health_metrics`)도 앱이 자동 생성하지 않으므로, 통합 스키마
`packaging/config/postgresql.sql` 을 **먼저 적용**한다(`monitor.db_dsn` 이 다른 DB면 그 DB에도 적용):

```bash
psql "postgresql://user:pass@pg-host:5432/monitoring" -f /appuser/query-executor/packaging/config/postgresql.sql

# 최근 기록 조회
psql ... -c "SELECT recorded_at, executor_url, healthy, cpu_percent, memory_percent
             FROM executor_health_metrics ORDER BY recorded_at DESC LIMIT 20;"
```

## 참고

- coordinator·executor 모두 상태를 **프로세스 메모리**에 두므로 인스턴스당 **단일 워커**로
  실행한다. 처리량 확장은 워커가 아니라 **executor 인스턴스 수**로 한다.
- coordinator를 **다중 인스턴스**로 띄우려면 `store.backend=postgres` + 공유 `history.db_dsn`
  으로 Job 저장소/이력을 PostgreSQL에 외부화한다(위 "멀티 coordinator & 실행 이력" 참고).
- 별도 executor 프로세스 없이 동작을 검증하려면 `coordinator.executor_mode=local`(또는
  `COORDINATOR_EXECUTOR_MODE=local`)로 coordinator 안에서 백엔드를 직접 실행한다.
