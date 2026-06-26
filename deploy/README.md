# systemd 배포 가이드 (RHEL 9.2)

coordinator 1개와 executor 다수를 systemd 서비스로 운영하기 위한 구성이다.
설정은 argus-catalog backend와 동일하게 **`config.properties` + `config.yml`** 방식을 쓴다.

## 구성 파일

| 파일 | 설명 |
|---|---|
| `systemd/query-coordinator.service` | coordinator 서비스 유닛 |
| `systemd/query-executor@.service` | executor **템플릿** 유닛(인스턴스 이름 = 포트) |
| `../packaging/config/config.properties` | Java 스타일 key=value 변수 정의 |
| `../packaging/config/config.yml` | `${변수:기본값}` 치환을 쓰는 메인 YAML 설정 |
| `install.sh` | 사용자/디렉터리/venv/설정/유닛을 한 번에 구성하는 설치 스크립트 |

- **설정 디렉터리**: `/etc/query-executor/` (환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 변경 가능)
- **로그**: `/var/log/query-executor/` (일 단위 롤링, `파일명_YYYYMMDD.log`)
- **executor는 템플릿 유닛**이라 포트별로 여러 인스턴스를 띄운다: `query-executor@8001`, `query-executor@8002` ...
- coordinator·executor 모두 상태를 **프로세스 메모리**에 두므로 인스턴스당 **단일 워커**로 실행한다. 처리량 확장은 워커가 아니라 **executor 인스턴스 수**로 한다.

## 빠른 설치 (스크립트 사용)

```bash
# 0) (최초 1회) Python 3.11 + rsync 설치
sudo dnf install -y python3.11 python3.11-pip python3.11-devel rsync

# 1) 저장소 루트에서 실행
sudo ./deploy/install.sh

# 2) 설정 확인/수정
sudo vi /etc/query-executor/config.properties   # executors, impala.*, greenplum.dsn 등

# 3) 서비스 기동 (executor 2개 + coordinator)
sudo systemctl enable --now query-executor@8001 query-executor@8002
sudo systemctl enable --now query-coordinator
```

`install.sh`가 하는 일:
- 서비스 계정 `queryexec` 생성
- 앱을 `/opt/query-executor` 로 복사(`.venv`/`.git`/`logs` 제외)
- `/opt/query-executor/.venv` 가상환경 + `requirements.txt` 설치
- `packaging/config/*` 를 `/etc/query-executor/` 로 배치(없을 때만), 운영 로그 경로를 `/var/log/query-executor` 로 설정
- `/var/log/query-executor` 생성 후 소유권 설정
- systemd 유닛 설치 후 `daemon-reload`

## 설정 항목 (config.properties)

```properties
# Coordinator
coordinator.host=0.0.0.0
coordinator.port=8000
coordinator.executors=http://127.0.0.1:8001,http://127.0.0.1:8002
coordinator.max_concurrent_jobs=16
coordinator.max_dispatch_concurrency=32

# Executor - Impala (source). 비어 있으면 MockBackend 사용
impala.host=
impala.port=21050
impala.database=default
impala.user=
impala.password=
impala.auth_mechanism=PLAIN
impala.use_ssl=false

# Executor - Greenplum (target). 비어 있으면 MockBackend 사용
greenplum.dsn=
copy.batch_size=10000
```

> `impala.host` 와 `greenplum.dsn` 이 **모두** 설정되면 실제 `ImpalaToGreenplumBackend`
> 가 동작하고, 하나라도 비어 있으면 `MockBackend`(실제 I/O 없음)로 폴백한다.
> 실제 연결 시에는 `requirements-executor.txt` 도 설치해야 한다(impyla, psycopg).

## 운영 명령

```bash
# 상태/로그(저널)
systemctl status query-coordinator
journalctl -u query-coordinator -f
journalctl -u query-executor@8001 -f

# 파일 로그(일 단위 롤링)
tail -f /var/log/query-executor/query-coordinator-server.log
tail -f /var/log/query-executor/query-executor-server-8001.log

# 재시작 / 중지
sudo systemctl restart query-coordinator
sudo systemctl stop query-executor@8002

# executor 인스턴스 추가(포트 8003): config.properties 의 executors 에 추가 후
sudo systemctl enable --now query-executor@8003
sudo systemctl restart query-coordinator
```

## 동작 확인

```bash
curl -s localhost:8000/healthz
curl -s localhost:8001/healthz

curl -s localhost:8000/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('\''2026-01-01'\'','\''2026-01-02'\'') AND region='\''KR'\''",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "parallelism": 2
}'
```

## 방화벽(firewalld)

외부에서 coordinator(8000)에 접근해야 한다면:

```bash
sudo firewall-cmd --permanent --add-port=8000/tcp
sudo firewall-cmd --reload
```

executor 포트(8001, 8002 ...)는 보통 coordinator와 같은 호스트 내부 통신이므로 외부 개방이 불필요하다.

## 참고

- coordinator를 다중 인스턴스로 띄우려면 Job 저장소를 Redis 등으로 교체해야 한다(현재 인메모리).
