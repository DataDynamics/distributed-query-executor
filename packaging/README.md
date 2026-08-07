# 배포 가이드 (RHEL 9.2, /data1 단일 트리)

분산 쿼리 실행기를 RHEL 9.2 서버에 처음 올리는 분을 위한 안내다. 이 실행기는 명령을 받아 일을 나눠 주는 한 대의 coordinator 와, 실제로 데이터를 읽고 적재하는 여러 대의 executor 로 이루어진다. 그래서 coordinator 1개와 executor 다수를 함께 운영한다. 설정은 **`config.properties` + `config.yml`** 방식이다 — 값은 `config.properties` 에 적고, 그 값을 `config.yml` 본문 설정이 가져다 채운다.

> **보안 정책**: `/etc`·`/opt`·`/var` 에 파일을 추가하지 않는다. 애플리케이션·설정·로그·
> 런타임을 모두 **`/data1/distributed-query-executor`** 아래에 두고, 기본은 **런처 스크립트(`bin/`)**
> 로 구동한다. systemd 를 쓰려면 `bin/systemd/` 의 유닛(`coordinator.service`·`executor@.service`)을
> `systemctl link` 로 설치한다(`bin/systemd/install-systemd.sh`, /etc 에는 심볼릭 링크만 생성).

시스템 공용 디렉터리를 건드리지 않으므로 권한 다툼이나 다른 소프트웨어와의 충돌 없이 모든 것이 한 폴더에 모인다. 그만큼 백업·이동·제거도 쉬워진다.

## 구성 파일

배포에 쓰이는 스크립트와 설정 파일은 다음과 같다. `bin/` 아래 스크립트들은 서비스를 켜고 끄고 상태를 보는 리모컨이라고 생각하면 된다.

| 파일 | 설명 |
|---|---|
| `bin/start-coordinator.sh` / `stop-…` / `restart-…` / `status-…` | **coordinator 만** 제어(nohup + PID) |
| `bin/start-executor.sh` / `stop-…` / `restart-…` / `status-…` | **executor 만** 제어(포트 인자 선택, 생략 시 전체) |
| `bin/status.sh` | 전체(coordinator + executor) 상태를 한 번에 조회 |
| `bin/gp-shell` / `impala-shell` / `s3-ops` | 운영자용 CLI(SQL 셸, S3 조작) |
| `bin/systemd/` | systemd 유닛(`coordinator.service`·`executor@.service`)과 `install-systemd.sh` |
| `bin/check-prereqs.sh` | **사전 점검**: OS 패키지(rpm) + 파이썬 휠(.venv) 설치 여부 확인(설치는 안 함) |
| `bin/env.sh` | 런처 공통 환경 + 헬퍼 함수(경로·포트) |
| `../config/config.properties` | Java 스타일 key=value 변수 정의 |
| `../config/config.yml` | `${변수:기본값}` 치환을 쓰는 메인 YAML 설정 |
| `bin/install.sh` | 사용자/디렉터리/venv/설정/런처를 한 번에 구성하는 설치 스크립트 |

배포가 끝나면 서버 위에 하나의 디렉터리 나무가 선다. 애플리케이션 본체와 파이썬 가상환경(`.venv`)은 **앱 홈**인 `/data1/distributed-query-executor` 에 자리 잡고, 설정 파일은 그 아래 `config` 에 모인다(환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 위치 변경 가능). 로그는 `logs` 에 일 단위로 롤링되어 `파일명_YYYYMMDD.log` 형태로 쌓이고, PID 파일 등 실행 중에만 의미 있는 것은 `run` 에 둔다.

executor 는 **포트별로 여러 인스턴스**를 띄울 수 있다. 예컨대 `EXECUTOR_PORTS="8087 8086"` 이면 두 개가 각 포트에서 동시에 일한다. 다만 coordinator·executor 모두 상태를 **프로세스 메모리**에 두므로 인스턴스 하나는 반드시 **단일 워커**로 실행하고, 처리량은 워커가 아니라 **executor 인스턴스 수**로 확장한다.

> **`local_stage`(file:// 세그먼트 로컬 스테이징) 배치는 다르다.** 기본 `copy`/`stage_insert` 모드는 executor 를 어디에 두든 상관없지만, `local_stage`(DESIGN 참고)는 executor 가 읽은 데이터를 **자기 호스트 로컬 디스크의 CSV** 로 떨어뜨리고 GP 세그먼트가 그 파일을 `file://` 로 직접 읽는다. 그래서 executor 를 **각 Greenplum 세그먼트 호스트에 co-locate**(호스트당 하나 이상) 해야 하며, 추가로 (1) `stage.local_dir` 을 모든 세그먼트 호스트에 **동일 경로**로 두고 GP 세그먼트 프로세스(보통 `gpadmin`)가 read 가능하게 권한을 맞추고, (2) `executor.gp_hostname` 을 그 호스트의 `gp_segment_configuration.hostname` 과 일치시키며(미설정 시 OS hostname), (3) coordinator 의 `greenplum.dsn` 은 GP master 를 가리켜야 한다(Phase 2 적재·검증·토폴로지 조회). 운영 시나리오는 [docs/GUIDE.md](../docs/GUIDE.md) 의 `local_stage` 절 참고.

> **`s3_stage`(S3 경유 스테이징) 배치.** `local_stage` 와 **같은 2-phase**(Phase 1 executor 업로드 → Phase 2 coordinator PXF 적재)지만 스테이징 매체가 세그먼트 로컬 파일이 아니라 **S3 객체**라 **co-locate 가 필요 없다**(executor 를 어디에 두든 됨). 대신 (1) executor 설정 `s3.bucket`·`s3.prefix`·`s3.endpoint_url`(온프렘 S3 호환이면)·업로드 자격증명(`s3.access_key`/`s3.secret_key` 또는 boto3 기본 체인)을 채우고 — 업로드는 `boto3`(에어갭 wheel 번들에 포함), (2) **GP 세그먼트에 PXF 를 설치·기동**하고 **S3 SERVER 프로파일**(`$PXF_BASE/servers/<server>/s3-site.xml` 에 S3 자격증명·엔드포인트)을 구성한 뒤 그 서버 이름을 `s3.pxf_server` 로 지정한다(업로드용과 GP 읽기용 자격증명이 분리된다), (3) **coordinator 의 `greenplum.dsn`** 은 GP master 를 가리킨다(Phase 2 외부테이블 생성·INSERT — 이 작업은 coordinator 가 중앙에서 하므로 coordinator 도 같은 `s3.*` 설정을 읽는다). executor 는 GP 를 직접 쓰지 않지만 실백엔드 선택을 위해 `greenplum.dsn` 이 있어야 한다(연결은 lazy). 운영 시나리오는 [docs/GUIDE.md](../docs/GUIDE.md) 의 `s3_stage` 절 참고.

## 빠른 설치 (스크립트 사용)

손 가는 일은 `install.sh` 가 대신 하므로 직접 칠 명령은 몇 줄뿐이다. 위에서 아래로 순서대로 따라간다.

```bash
# 0) (최초 1회) Python 3.9(RHEL 9.2 기본) + rsync 설치
sudo dnf install -y python3 python3-pip python3-devel rsync

# 1) 저장소 루트에서 실행 (에어갭이면 WHEELHOUSE/INSTALL_EXECUTOR 지정)
sudo ./bin/install.sh
#   에어갭 예: sudo WHEELHOUSE=/path/wheels INSTALL_EXECUTOR=1 ./bin/install.sh

# 2) 설정 확인/수정
sudo vi /data1/distributed-query-executor/config/config.properties   # executors, impala.*, greenplum.dsn 등

# 3) 서비스 기동 (executor 를 먼저, 그다음 coordinator)
sudo -u gpadmin /data1/distributed-query-executor/bin/start-executor.sh
sudo -u gpadmin /data1/distributed-query-executor/bin/start-coordinator.sh
sudo -u gpadmin /data1/distributed-query-executor/bin/status.sh
```

0번은 최초 한 번만 하는 준비로 RHEL 9.2 기본 Python 3.9 와 rsync 를 설치한다. 1번이 핵심이라 저장소 루트에서 `install.sh` 를 실행하면 설치가 한 번에 끝난다. 외부 네트워크가 막힌 **에어갭**이라면 미리 받아 둔 휠 묶음 경로를 `WHEELHOUSE` 로 알려 주고, executor 드라이버까지 함께 설치하려면 `INSTALL_EXECUTOR=1` 을 붙인다(자세히는 아래 "오프라인 설치"). 2번에서 설정을 우리 환경에 맞게 고치고, 3번에서 서비스를 띄우고 상태를 확인하면 끝난다.

`install.sh` 가 대신 해 주는 일은 다음과 같다.

- 서비스 계정 `gpadmin` 을 홈 `/data1` 으로 만든다.
- 앱을 `/data1/distributed-query-executor` 로 복사한다. 이때 `.venv`·`.git`·`logs`·`run` 과 운영자 자산인 `config`·`templates`·`customs` 는 제외한다.
- `/data1/distributed-query-executor/.venv` 에 가상환경을 만들고 의존성을 설치한다. `WHEELHOUSE` 를 지정하면 오프라인으로 설치한다.
- 설정·스키마(`config/`), 템플릿(`templates/`), 커스텀 함수(`customs/`)를 소스에서 배치하되 **아직 없을 때만** 넣고, 로그 경로를 `/data1/distributed-query-executor/logs` 로 맞춘다.
- TLS 자리표시 파일 `config/impala-ca.pem` 을 만든다.
- 런처 스크립트를 `bin/` 에 배치하고 소유권과 권한을 설정한다.

> **업그레이드 시 자산 반영**: `config/`·`templates/`·`customs/` 는 모두 운영자가 편집·추가하는 자산이라 rsync 에서 제외되고 "없을 때만" 시딩된다. 그래서 재설치해도 운영자 편집·인증서·직접 추가한 템플릿·커스텀 함수는 보존되지만, **새 버전이 추가·변경한 기본값·설정 구조·예제도 자동으로 반영되지 않는다.** 이때 `bin/migrate-config.sh` 가 세 트리를 파일별 전략으로 반영한다:
>
> | 대상 | 전략 |
> |---|---|
> | `config/config.properties` | 운영자 변경분만 새 기본값 위에 **병합**(값·주석·순서 보존, `.bak`) |
> | `config/config.yml`·스키마(`*.sql`) | **새 버전으로 교체**(`.bak` 백업) |
> | `templates/`·`customs/` | 예제는 새 버전 반영(바뀐 파일 `.bak`), **운영자 추가 파일은 보존** |
>
> `config.yml` 을 교체하는 이유가 중요하다 — config.yml 은 값이 아니라 `${변수:기본값}` **구조**라, 새 버전이 추가한 설정은 config.yml 에 자리(placeholder)가 생겨야 실제로 읽힌다(운영자 값은 properties 에 있으므로 안전, config.yml 을 직접 고쳤다면 `.bak` 에서 확인·복원).
>
> **어디서 실행하나**: 설치 트리에는 "새 버전 원본"이 없으므로 **새로 내려받은 소스 트리에서** 실행해, 그 트리를 새 버전 기준(`--source-base`)으로 삼고 설치 트리(`--deploy-base`)에 반영한다. 기본값은 소스 트리 = 이 도구가 속한 트리, 설치 트리 = `$QUERY_EXECUTOR_CONFIG_DIR` 의 부모(미설정 시 `/data1/distributed-query-executor`)라, 보통은 환경변수만 지정하면 된다.
>
> ```bash
> # 새 버전 소스 트리로 이동해서 실행(install.sh 재실행으로 코드는 이미 갱신된 뒤).
> cd <새-버전-소스-트리>
> # 1) 무엇이 반영될지 먼저 확인(비밀값은 마스킹, 파일은 안 씀)
> QUERY_EXECUTOR_CONFIG_DIR=/data1/distributed-query-executor/config \
>   bin/migrate-config.sh --dry-run
> # 2) 실제 반영(config+templates+customs 제자리, 바뀐 파일은 .bak 백업)
> QUERY_EXECUTOR_CONFIG_DIR=/data1/distributed-query-executor/config \
>   bin/migrate-config.sh
> ```
>
> 트리 루트를 직접 지정하려면 `--deploy-base`(설치)·`--source-base`(새 소스)를 쓴다. config.properties 한 파일만 병합하던 예전 방식(`--old`/`--new`/`--out`)도 하위 호환으로 남아 있다. 반영 후 서비스를 재기동하면 적용된다.

## 오프라인 설치 (에어갭 휠 번들)

인터넷이 막힌 서버에서는 `pip` 이 패키지를 내려받을 곳(PyPI)이 없다. 그래서 미리 받아 둔 설치 파일을 한곳에 모아 두고 `pip` 에게 "인터넷 대신 이 폴더에서 찾아라"라고 알려 주는 방식으로 설치한다. 이 설치 파일은 대부분 **휠(wheel)** — 파이썬 패키지를 미리 빌드해 하나로 묶은 형식이라 컴파일 없이 빠르게 설치된다. 휠 묶음을 모아 둔 폴더를 흔히 **WHEELHOUSE**(휠 창고)라 부르고, 이 저장소는 `packaging/wheels/` 아래에 그 번들을 함께 담고 있다.

번들은 **파이썬 버전별로 두 벌**이다: `packaging/wheels/py39`(RHEL 9.2 기본 python3.9, 바이너리 휠 `cp39` 태그)와 `packaging/wheels/py311`(`dnf install python3.11` 로 설치하는 Python 3.11, `cp311` 태그). 두 벌은 같은 패키지·같은 버전이고 C 확장 휠의 ABI 태그만 다르다(`exceptiongroup`·`tomli` 는 3.11 표준 라이브러리에 흡수된 백포트라 `py311/` 에는 없다). 리눅스 배포판 간 시스템 라이브러리 차이는 **manylinux** 표준으로 흡수한다 — glibc ≤ 2.28 에서 빌드된 휠은 그보다 높은 버전에서도 돌고, RHEL 9.2 의 glibc 2.34 와도 호환된다.

각 버전 디렉터리에는 필요한 휠이 **한 폴더에 전부** 들어 있다: coordinator 런타임 의존성(`requirements.txt`, Jinja2 포함), executor 드라이버(impyla·thrift·SASL·trino)와 Cython, pytest·pytest-asyncio 등 테스트 의존성, 설치 부트스트랩(`pip`·`setuptools`·`wheel`)까지 모두다. 무엇이 실제로 설치될지는 폴더가 아니라 **requirements 파일이 결정**한다 — pip 은 `-r` 목록에 있는 패키지만 골라 설치하므로, coordinator 만 설치하면 executor 드라이버나 pytest 휠은 폴더에 있어도 무시된다.

가장 간단한 길은 `bin/install.sh` 에 `WHEELHOUSE` 로 휠 폴더 위치를 알려 주는 것이다. 그러면 스크립트가 알아서 `--no-index`(인터넷 저장소를 보지 말라는 뜻)와 `--find-links` 를 붙여 설치한다. `WHEELHOUSE` 에는 배포 대상 파이썬 버전의 디렉터리 하나만 지정하면 되고, coordinator 만 설치하든 executor 까지 설치하든 같은 폴더를 쓴다(무엇이 설치될지는 `INSTALL_EXECUTOR` 가 정하는 requirements 파일이 결정).

```bash
# coordinator 만
sudo WHEELHOUSE=packaging/wheels/py39 ./bin/install.sh

# executor 포함
sudo WHEELHOUSE=packaging/wheels/py39 INSTALL_EXECUTOR=1 ./bin/install.sh
```

스크립트를 거치지 않고 `pip` 을 직접 부르려면 손으로 옵션을 붙여도 된다. `--no-index` 로 인터넷을 끊고, `--find-links` 로 pip 이 이 폴더에서만 휠을 찾게 한 뒤, `-r` 로 설치할 목록 파일을 지정한다.

```bash
# 수동 설치 예시
pip install --no-index \
  --find-links packaging/wheels/py39 \
  -r requirements-executor.txt
```

Python 3.11 로 배포할 때는 경로의 `py39` 를 `py311` 로 바꾸면 된다. 다만 가상환경 자체도 3.11 로 만들어야 하므로 `install.sh` 에는 `PYTHON=python3.11` 을 함께 준다.

```bash
# Python 3.11 + executor 포함 설치
sudo PYTHON=python3.11 WHEELHOUSE=packaging/wheels/py311 \
     INSTALL_EXECUTOR=1 ./bin/install.sh
```

> 완전한 에어갭이 아니라 사내 **Nexus PyPI 프록시**(외부 PyPI 를 사내에서 대신 받아다 캐시해 주는 저장소)가 있으면 이 번들 대신 `pip install -i <nexus>/simple` 도 가능하다. 이 번들은 그런 프록시조차 없는 완전 오프라인 설치용 폴백이다.

## 사전 점검 (check-prereqs.sh)

설치 전후로 "필요한 것이 다 갖춰졌는지" 확인하고 싶을 때 쓴다. **OS 패키지**와 **파이썬 휠**이 준비되었는지 **확인만** 하고 설치는 하지 않는다. 결과는 종료코드로도 알려 주므로(모두 충족 `0`, 하나라도 빠지면 `1`) 자동화 파이프라인의 통과/실패 게이트에도 끼워 넣을 수 있다.

```bash
# OS 패키지(rpm) + 휠(.venv) 점검
./bin/check-prereqs.sh

# 한쪽만 점검
OS_ONLY=1     ./bin/check-prereqs.sh   # OS 패키지만
WHEELS_ONLY=1 ./bin/check-prereqs.sh   # 휠만
```

**OS 패키지** 점검은 `rpm -q` 로 빌드 도구와 SASL 의존성(`gcc gcc-c++ make python3-devel python3 python3-pip cyrus-sasl-devel`)이 깔려 있는지 본다. **파이썬 휠** 점검은 `packaging/wheels/py<버전>/` 의 `.whl`·`.tar.gz` 이름에서 패키지명·버전을 뽑아 실제 `.venv` 설치 목록과 대조해 `[OK]`/`[MISSING]`/`[VER ?]`(경고, 실패 아님)로 표시한다. 검사 대상 경로는 환경변수로 바꾼다 — 파이썬은 `VENV_PY`, 휠 번들 루트는 `WHEELS_ROOT` 이고, 배포 서버에서는 보통 `VENV_PY=/data1/distributed-query-executor/.venv/bin/python` 처럼 그 서버의 가상환경을 가리킨다.

## 설정 항목 (config.properties)

설치가 끝났으면 환경에 맞게 설정을 손본다. 처음에는 전부 이해하려 애쓰기보다 coordinator 주소와 executor 목록, Impala·Greenplum 접속 정보부터 채운다는 마음으로 읽으면 된다.

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

이 실행기는 Impala 에서 읽어 Greenplum 으로 옮기므로 두 곳의 접속 정보가 **모두** 채워져야 진짜로 동작한다.

> `impala.host` 와 `greenplum.dsn` 이 **모두** 설정되면 실제 `ImpalaToGreenplumBackend`
> 가 동작하고, 하나라도 비어 있으면 `MockBackend`(실제 I/O 없음)로 폴백한다.
> 실제 연결 시에는 `requirements-executor.txt` 도 설치해야 한다(impyla, psycopg, SASL).

동시성 값의 진짜 천장은 coordinator 성능이 아니라 뒤에 있는 데이터베이스의 수용량이다.

> **동시성 적정값**: 실제 천장은 coordinator 코어가 아니라 Greenplum 동시 COPY 허용량·
> Impala 동시 쿼리 슬롯·executor 풀 합이다. 다운스트림 용량에 맞춰
> `executor.max_concurrent_tasks` 를 분배하고, `max_dispatch_concurrency` 는 그 이상으로
> 두어 coordinator 가 병목이 되지 않게 한다.

## Impala TLS + 인증 (LDAP)

> **기본 인증은 LDAP 이다.** `impala.auth_mechanism=LDAP`(기본값)이면 `impala.user`/
> `impala.password` 에 LDAP 바인드 자격증명만 채우면 되고, 비밀번호 보호를 위해
> `impala.use_ssl=true` + `impala.ca_cert` 로 TLS 를 함께 쓰는 것을 권장한다.

이 절은 보안 접속이 걸린 Impala 에 연결할 때만 필요하다. 데이터 원천인 Impala 에 실제로 접속하는 쪽은 executor 이고 coordinator 는 관여하지 않는다. 보안 방식은 양쪽이 다르다 — **Impala 에만 TLS + LDAP** 가 적용되고, 적재 대상인 **Greenplum 은 TLS 없이 일반 DSN** 으로 접속한다.

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

기본 설정에서는 coordinator 가 한 대뿐이고 모든 상태를 자기 메모리에만 둔다 — 처음엔 이걸로 충분하다. 하지만 **여러 대의 coordinator** 로 가용성을 높이거나 재시작해도 **실행 이력이 남도록 영속**하고 싶다면, 모두가 함께 바라볼 공유 PostgreSQL 을 설정한다. 핵심은 모든 coordinator·executor 가 동일한 DSN 을 공유한다는 것이다.

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

**공유 Job 저장소**(`store.backend=postgres`)를 켜면 작업 상태가 `jobs` 테이블에 JSONB 로 저장되어 어느 coordinator 로 조회·취소해도 똑같이 동작하고, 취소는 플래그 공유를 통해 다른 coordinator 에까지 미친다. 이력은 **2계층**으로 나뉜다 — 작업 시작·종료(`run()`)는 coordinator 가 `job_history` 에, 각 task 의 상태 변화는 executor 가 `executor_id` 를 포함해 `task_history` 에 남긴다(작업 제출 시 `username` 을 넘기면 두 테이블에 사용자가 기록되어 대시보드 "사용자" 컬럼에 나타난다). task 이력은 executor 가 직접 쓰므로 **executor 호스트에도 PG 자격증명이 필요**하다. **생존 확인**을 위해 `self_report=true` 로 두면 각 executor 가 `executor_status` 에 heartbeat 를 주기적으로 upsert 하고, coordinator 는 그 행 `updated_at` 의 신선도로 살아 있는지 판단한다. 끝으로 admission 한도(`max_concurrent_jobs`·`max_pending_jobs`)는 **coordinator 인스턴스마다 따로(인메모리)** 적용되므로, 여러 대를 띄우면 전체 한도는 인스턴스 수만큼 곱해진다.

⚠️ **앱은 스키마를 자동 생성하지 않는다.** 서비스 기동 **전에** 통합 스키마 한 파일로 전체 테이블(jobs/job_history/task_history/executor_status/executor_health_metrics)을 먼저 만든다(안 하면 "relation does not exist"로 실패):

```bash
PG="postgresql://user:pass@pg-host:5432/queryexec"
psql "$PG" -f /data1/distributed-query-executor/config/postgresql.sql
```

> 단일 coordinator면 기본값(`store.backend=memory`, `executor.self_report=false`) 그대로 둔다.
> 이력만 남기고 싶으면 `history.db_dsn` 만 설정해도 된다(저장소/self-report 는 끄고).

메타 저장소를 WarehousePG 나 Greenplum 7 에 두려면 스키마를 조금 다르게 만들어야 한다. 이들은 Greenplum 계열 MPP 라 데이터를 여러 노드에 분산하기 때문이다.

> **WarehousePG / Greenplum 7 에 메타 저장소를 둘 때**는 `postgresql.sql` 대신
> [`warehousepg.sql`](../config/warehousepg.sql) 을 적용한다(테이블마다 `DISTRIBUTED BY`
> 지정, history/metrics 는 대리 PK 를 빼고 `job_id`/`executor_url` 로 co-locate). 앱 코드는
> 그대로다(`ON CONFLICT`·`JSONB`·`DISTINCT ON` 모두 GP7=PG12 에서 지원). 다만 heartbeat/예약은
> 고빈도 단일행 UPSERT 라 MPP 와 맞지 않으므로, 성능이 중요하면 이 메타 저장소는 PostgreSQL 에
> 두고 WarehousePG 는 데이터 적재 대상(`greenplum.dsn`)으로만 쓰는 편이 낫다.
> ```bash
> psql "$PG" -f /data1/distributed-query-executor/config/warehousepg.sql
> ```

## 운영 명령

서비스를 띄운 뒤에는 날마다 상태를 살피고 로그를 보고 가끔 재시작하는 운영이 이어진다. 자주 쓰는 명령을 모았다. 맨 앞의 `B=...` 는 긴 경로를 짧은 이름에 담아 두는 것이다.

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

# 역할별 제어(coordinator / executor 따로). 중지는 coordinator 부터, 기동은 executor 부터.
sudo -u gpadmin $B/stop-coordinator.sh        # coordinator 만 중지
sudo -u gpadmin $B/restart-executor.sh        # executor 전체 재기동(중지→종료 대기→기동)
sudo -u gpadmin $B/start-executor.sh 8086     # executor 8086 만 기동
sudo -u gpadmin $B/stop-executor.sh  8086     # executor 8086 만 중지

# executor 인스턴스 추가(포트 8003): config.properties 의 executors 에 추가 후
sudo -u gpadmin $B/start-executor.sh 8003     # 또는 전체: EXECUTOR_PORTS="8087 8086 8003" $B/start-executor.sh
```

로그는 두 종류다 — 일반 로그 외에 `*-warn.log` 가 WARNING 이상만 따로 모으므로, 문제를 추적할 때는 경고 로그만 빠르게 훑으면 원인에 더 빨리 다가간다. 또 executor 를 새로 늘릴 때는 스크립트만 실행하면 안 되고, 먼저 `config.properties` 의 executor 목록에 그 포트를 추가해야 coordinator 가 새 인스턴스를 인식한다.

## 동작 확인

설치·기동을 마쳤으면 실제로 두드려 본다. 헬스와 메트릭을 조회하고 작은 작업을 하나 제출해 끝까지 도는지 확인한다.

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

명령줄이 익숙하지 않으면 브라우저로 coordinator 의 `http://<host>:8088/` 에 접속하면 모니터링 대시보드가 나오고, remote 모드에서는 각 executor 도 `http://<host>:8087/` 처럼 자기 화면을 준다. API 를 살펴보려면 `/docs` 의 Swagger UI 를 쓴다. 마지막 `curl ... /jobs` 는 `sales` 에서 두 날짜를 읽어 `public.sales_mirror` 로 2분할 병렬 적재하라는 예시로, 이 한 번이 성공하면 전체 경로가 살아 있다는 좋은 신호다.

## 방화벽(firewalld)

서버 안에서는 돌더라도 바깥에서 coordinator(8088)에 접근하려면 방화벽에 길을 터 줘야 할 수 있다.

```bash
sudo firewall-cmd --permanent --add-port=8088/tcp
sudo firewall-cmd --reload
```

executor 포트(8087, 8086 ...)는 보통 coordinator 와 같은 호스트 안 내부 통신이므로 외부로 열 필요가 없다.

## 헬스/메트릭 모니터링

단순히 살아 있는지를 넘어 자원 사용 추세를 보고 싶어지는 때가 온다. 두 서비스는 모두 살아 있는지를 알려 주는 `/health`(liveness) 와 CPU·메모리·디스크 사용량을 알려 주는 `/metrics` 를 제공한다. coordinator 는 `monitor.health_interval_s` 마다 모든 executor 의 `/health`·`/metrics` 를 폴링해 그 상태를 보유하고(`GET /executors` 로 조회), `monitor.record_interval_s` 마다 그 사용량을 PostgreSQL 테이블(`monitor.table`)에 기록한다.

```properties
monitor.enabled=true
monitor.health_interval_s=10
monitor.record_interval_s=60
# 기록 대상 PostgreSQL DSN. 비어 있으면 폴링만 하고 DB 기록은 하지 않는다.
monitor.db_dsn=postgresql://user:pass@pg-host:5432/monitoring
monitor.table=executor_health_metrics
monitor.disk_path=/
```

여기서도 같은 원칙이다 — `executor_health_metrics` 테이블도 앱이 알아서 만들지 않으므로 통합 스키마 `config/postgresql.sql` 을 **먼저 적용**한다. `monitor.db_dsn` 이 다른 DB 를 가리키면 그 DB 에도 동일하게 적용한다.

```bash
psql "postgresql://user:pass@pg-host:5432/monitoring" -f /data1/distributed-query-executor/config/postgresql.sql

# 최근 기록 조회
psql ... -c "SELECT recorded_at, executor_url, healthy, cpu_percent, memory_percent
             FROM executor_health_metrics ORDER BY recorded_at DESC LIMIT 20;"
```

## 참고

운영 내내 기억해 두면 좋은 핵심 원칙 세 가지다.

- coordinator·executor 모두 상태를 **프로세스 메모리**에 두므로 인스턴스당 **단일 워커**로 실행한다. 처리량 확장은 워커가 아니라 **executor 인스턴스 수**로 한다.
- coordinator를 **다중 인스턴스**로 띄우려면 `store.backend=postgres` + 공유 `history.db_dsn` 으로 Job 저장소/이력을 PostgreSQL에 외부화한다(위 "멀티 coordinator & 실행 이력" 참고).
- 별도 executor 프로세스 없이 동작을 검증하려면 `coordinator.executor_mode=local`(또는 `COORDINATOR_EXECUTOR_MODE=local`)로 coordinator 안에서 백엔드를 직접 실행한다.
