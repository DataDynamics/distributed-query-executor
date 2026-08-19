# 통합 운영자 가이드

이 문서는 분산 쿼리 실행기(Distributed Query Executor)를 서버에 올려 두고 여러 사람이 쓰게 만드는
쪽, 그리고 그 이후에 생기는 일들을 책임지는 쪽을 위한 것이다. 원래 두 저장소에 나뉘어 있던 운영
지식을 하나로 합쳤다. 서비스(coordinator + executor)를 설치하고 설정하고 돌보는 이야기가 앞쪽이고,
같은 트리에 함께 설치되는 터미널 도구(`bin/gp-shell`·`bin/impala-shell`·`bin/s3-ops`)를 여러 사람이
안전하게 쓰게 만드는 이야기가 뒤쪽이다.

급한 상황이라면 바로 해당 장으로 간다. 작업이 429 로 거절되고 있으면 "작업이 밀릴 때"를, 이관이 너무
느리면 "느릴 때"를, 실패한 작업을 좇고 있으면 "실패를 추적할 때"를 본다. 평상시라면 "매일 보는
것"부터 읽으면 된다. 도구를 실제로 쓰는 사람에게 건네줄 문서는 같은 디렉터리의 사용자 가이드다.

---

# 1장. 무엇이 어떻게 돌아가는가

운영 판단을 하려면 구조를 한 문단쯤은 알고 있어야 한다.

coordinator 한 대가 요청을 받아 SQL 을 task 로 나누고 executor 여러 대에 나눠 준다. 여기서 중요한
것은 데이터가 coordinator 를 지나가지 않는다는 점이다. executor 가 소스에서 읽어 Greenplum 으로
곧장 보내고 coordinator 로는 상태와 행 수만 올라온다. 그래서 coordinator 가 병목이 되는 일은 드물고,
처리량을 늘릴 때 손대는 것은 거의 언제나 executor 쪽이다.

두 서비스 모두 상태를 프로세스 메모리에 들고 있어 단일 워커로 돈다. 워커 수를 늘리는 방식의 확장은
하지 않으며, 늘릴 것은 executor 인스턴스 수다.

과부하 방어는 세 층으로 겹겹이 이루어진다. 첫째 층은 작업 단위 admission control 이다. 동시에
RUNNING 일 수 있는 작업 수를 실행 슬롯으로 제한하고, 슬롯이 차면 대기 큐에 세우며, 실행과 대기의
합마저 넘으면 429 로 거절한다. 둘째 층은 한 coordinator 가 모든 작업을 통틀어 동시에 띄우는 task
수의 상한이고, 셋째 층은 executor 한 대가 동시에 실행하는 task 수의 상한이다. 이 셋이 각각 어떤
설정에 대응하는지는 "동시성과 용량"에서 다룬다.

![분산 쿼리 실행기 전체 구성](images/architecture.svg)

터미널 도구는 이 구조 밖에 있다. 서비스와 같은 설정 파일을 읽지만 프로세스도 수명도 따로이고, 사람이
친 명령이 그대로 Impala·Greenplum·S3 로 나간다. 그래서 자격증명과 권한을 어떻게 나눠 줄지는 서비스와
별개로 한 번 더 정해야 한다.

---

# 2장. 설치와 배포

## 빠른 설치

손 가는 일은 `install.sh` 가 대신하므로 직접 칠 명령은 몇 줄뿐이다. 위에서 아래로 순서대로 따라간다.

```bash
# 0) (최초 1회) Python 3.9(RHEL 9.2 기본) + rsync 설치
sudo dnf install -y python3 python3-pip python3-devel rsync

# 1) 저장소 루트에서 실행 (에어갭이면 WHEELHOUSE/INSTALL_EXECUTOR 지정)
sudo ./bin/install.sh
#   에어갭 예: sudo WHEELHOUSE=/path/wheels INSTALL_EXECUTOR=1 ./bin/install.sh

# 2) 설정 확인/수정
sudo vi /data1/distributed-query-executor/config/config.properties

# 3) 서비스 기동 (executor 를 먼저, 그다음 coordinator)
sudo -u gpadmin /data1/distributed-query-executor/bin/start-executor.sh
sudo -u gpadmin /data1/distributed-query-executor/bin/start-coordinator.sh
sudo -u gpadmin /data1/distributed-query-executor/bin/status-coordinator.sh
sudo -u gpadmin /data1/distributed-query-executor/bin/status-executor.sh
```

`install.sh` 는 서비스 계정 `gpadmin` 을 만들고, 앱을 `/data1/distributed-query-executor` 로
복사하고, 그 아래 `.venv` 에 의존성을 설치하고, 설정·템플릿·커스텀 함수를 **아직 없을 때만** seeding
하고, 런처 스크립트와 권한을 정리한다. `config/` · `templates/` · `customs/` 를 rsync 에서 제외하는
이유는 운영자가 채운 값과 인증서, 직접 추가한 템플릿을 지우지 않기 위해서다. 그 대가로 새 버전이
바꾼 기본값도 자동으로 들어오지 않으므로, 업그레이드 절차는 마지막 장에서 따로 다룬다.

설치 전후로 필요한 것이 갖춰졌는지 확인하고 싶으면 사전 점검 스크립트를 쓴다. OS 패키지와 파이썬
휠이 준비됐는지 확인만 하고 설치는 하지 않으며, 종료 코드로도 알려 주므로 자동화에 끼워 넣을 수 있다.

```bash
./bin/check-prereqs.sh
OS_ONLY=1     ./bin/check-prereqs.sh   # OS 패키지만
WHEELS_ONLY=1 ./bin/check-prereqs.sh   # 휠만
```

에어갭 환경이 전제이므로 웹 에셋은 모두 내장돼 있다. Swagger UI 와 대시보드 폰트까지 트리 안에서
서빙하므로 런타임에 외부로 나가지 않으며, 휠도 미리 받아 둔 묶음을 `WHEELHOUSE` 로 지정해 설치한다.

## 어떤 의존성이 어디에 필요한가

coordinator 는 기본 의존성만으로 뜬다. executor 는 소스와 대상 드라이버가 필요하므로
`requirements-executor.txt` 를 함께 설치하는데(`INSTALL_EXECUTOR=1`), impyla 의 SASL 빌드에는 시스템
패키지가 필요해 `cyrus-sasl-devel`·`gcc`·`gcc-c++`·`make`·`python3-devel` 이 깔려 있어야 한다.
터미널 도구는 여기에 얹혀 간다. `bin/gp-shell` 은 executor 백엔드와 같은 psycopg 3 을 쓰고,
`bin/impala-shell` 은 impyla 를, `bin/s3-ops` 는 boto3 를 쓰는데 셋 다 이미 executor 의존성에 있어서
추가로 설치할 것이 없다. 반대로 coordinator 만 올린 노드에서는 impala·S3 도구가 패키지 없음(종료
코드 3)으로 끝나는 것이 정상이다.

## systemd 로 돌리기

`bin/systemd/` 에 coordinator 와 executor 유닛이 있고 `install-systemd.sh` 가 배치한다. executor 는
포트별 인스턴스(`executor@8087.service`)로 뜬다. 여기서 하나만 기억하면 되는데, executor 는 SIGTERM
을 받으면 진행 중인 task 가 끝나기를 `executor.shutdown_drain_timeout_s`(기본 25초)만큼 기다렸다
내려가므로 **`TimeoutStopSec` 이 이보다 길어야** 그 대기가 의미를 갖는다.

---

# 3장. 설정

설정은 두 파일로 나뉜다. 값은 `config.properties` 에 자바 스타일 `key=value` 로 적고, 그 값이
`config.yml` 의 `${변수:기본값}` placeholder 를 채워 최종 설정이 된다. 설정 디렉터리는 기본적으로
`/data1/distributed-query-executor/config` 이고 환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 바꾼다.
바꾼 뒤에는 서비스를 재기동해야 반영된다.

손으로 고쳐도 되지만 터미널 설정 편집기를 쓰면 항목마다 무엇인지와 어떤 범위인지를 함께 볼 수 있다.

```bash
bin/config-tui.sh
```

첫 화면이 동시성 탭이다. 처리량을 좌우하는 값들이 섹션을 넘어 한자리에 모여 있고, `+` 와 `-` 로
올리고 내리면 화면 아래에서 실제 용량이 곧바로 다시 계산된다.

```
 입구: 동시 16건 실행 + 100건 대기 = 116건까지 수용(초과 429)
 플릿: executor 2대 × task 8개 = 동시 16개, GP 연결 최대 16개(pool_max 자동)
 copy 버퍼: 8 × 10,000행 ≈ task 당 최대 80,000행을 메모리에 보관
```

어떤 값인지 확실하지 않으면 `?` 를 누른다. 그 항목이 무엇을 정하는지, 얼마로 두어야 하는지, 함께
보아야 할 설정이 무엇인지가 한 화면에 나온다. 저장할 때 값들 사이가 어긋나면 경고로 알려 주고, 아예
동작을 멈추게 하는 값은 저장 자체를 막는다. 저장 전에 `.bak` 로 원본을 백업하고 바꾼 값만 제자리에서
갱신하므로 주석과 순서는 그대로 남는다.

## 처음에 채우는 항목

설치 직후라면 coordinator 주소와 executor 목록, 그리고 소스·대상 접속 정보부터 채운다.

```properties
# Coordinator
coordinator.host=0.0.0.0
coordinator.port=8088
coordinator.executors=http://127.0.0.1:8087,http://127.0.0.1:8086
coordinator.id=                        # 멀티 coordinator 식별자(미지정 시 자동 생성)
coordinator.executor_mode=remote       # remote(HTTP 디스패치) | local(in-process 직접 실행)

# 동시성/큐잉(admission control)
coordinator.max_concurrent_jobs=16     # 동시에 RUNNING 가능한 job 수(실행 슬롯)
coordinator.max_pending_jobs=100       # 슬롯이 차면 PENDING 으로 대기 가능한 job 수
coordinator.max_dispatch_concurrency=32 # 동시 task 디스패치 상한

# Executor 동시 task 상한(executor 1대 기준)
executor.max_concurrent_tasks=8

# Executor - Impala (source). 기본 TLS + LDAP 인증
impala.host=
impala.port=21050
impala.database=default
impala.auth_mechanism=LDAP        # LDAP(기본) | PLAIN | NOSASL
impala.use_ssl=true
impala.ca_cert=/data1/distributed-query-executor/config/impala-ca.pem
impala.user=                      # LDAP 바인드 사용자
impala.password=                  # LDAP 비밀번호

# Executor - Greenplum (target). 비어 있으면 MockBackend. TLS 미적용(일반 DSN)
greenplum.dsn=
copy.batch_size=10000
```

여기서 보안 방식이 양쪽에서 다르다는 점을 헷갈리지 않는다. TLS 와 LDAP 인증은 소스인 Impala 에만
적용되고, 적재 대상인 Greenplum 은 인증이나 TLS 없이 일반 `postgresql://` DSN 으로 붙는다. Impala 에
실제로 접속하는 쪽은 executor 이고 coordinator 는 관여하지 않는다.

보안 접속이 걸린 Impala 를 쓴다면 CA 인증서를 배치하고 자격증명을 채운다.

```bash
sudo cp impala-ca.pem /data1/distributed-query-executor/config/impala-ca.pem
sudo chown -R gpadmin:gpadmin /data1/distributed-query-executor/config
```

`impala.auth_mechanism=LDAP` 이 기본값이므로 `impala.user` 와 `impala.password` 에 LDAP 바인드
자격증명만 채우면 되고, 비밀번호 보호를 위해 TLS 를 함께 쓰는 것을 권한다. 포트와 전송 방식은
클러스터 구성에 달렸는데, 21050 이 바이너리 HS2 로 기본값이고 28000 은 HTTP HS2 다. 21000(예전
beeswax)이나 25000(웹 UI)에 붙으면 핸드셰이크 도중 서버가 연결을 끊어 EOF 오류가 난다.

## 백엔드 선택이 조용히 갈리는 지점

`build_backend` 는 **`greenplum.dsn` 하나만** 본다. 비어 있으면 경고를 남기고 `MockBackend` 로
떨어지는데, 이 상태에서는 작업이 성공했다고 보고되지만 실제로는 아무것도 읽고 쓰지 않는다. 반면
`impala.host` 만 비어 있으면 실제 백엔드로 뜨되 소스를 읽을 수 없어 `statement` 모드만 동작하고,
기동 로그에 `impala=(미설정 → statement 모드만)` 으로 남는다. 둘 다 기동 로그로 판별되므로 새 노드를
올린 직후에는 배너와 경고 로그를 한 번 확인하는 습관을 들이는 편이 좋다.

## 로깅 설정

로그 레벨과 무관하게 항상 남는 것과 DEBUG 에서만 남는 것이 갈린다. 실행 SQL(`log.sql.*`)은 감사와
사고 추적의 1차 근거라 INFO 로 항상 기록하고, HTTP 요청·응답(`log.http.*`)은 양이 커서 DEBUG 일
때만 기록한다. 둘 다 `enabled=false` 로 끌 수 있지만, SQL 로깅을 끄면 "무엇을 읽어 무엇을
적재했나"가 로그에서 사라지므로 특별한 이유가 없으면 켜 둔다.

```properties
log.level=INFO
log.dir=/data1/distributed-query-executor/logs
log.backup_count=30
log.sql.enabled=true
log.sql.max_length=4000
log.http.enabled=true      # 실제 기록은 log.level=DEBUG 일 때만
log.http.max_body=2048
```

## 터미널 도구도 같은 설정을 읽는다

`bin/gp-shell`·`bin/impala-shell`·`bin/s3-ops` 는 별도 설정 파일을 두지 않고 위의
`config.properties` 를 그대로 읽어 자기 형태로 바꿔 쓴다. 같은 접속 정보를 두 곳에 적어 한쪽만
고쳐서 어긋나는 사고를 막기 위해서다. Greenplum 만 형태가 달라서, 이 저장소가 한 줄로 들고 있는
`greenplum.dsn` 을 도구가 host·port·user 로 풀어 쓴다. 즉 **운영자가 `greenplum.dsn` 하나를 고치면
서비스와 도구가 함께 따라온다.**

읽을 디렉터리는 `QUERY_EXECUTOR_CONFIG_DIR` 이 정하고, 도구에 `--config-dir` 로 덮거나
`--no-config` 로 무시하게 할 수 있다. 개발 트리와 배포 트리가 같은 서버에 있으면 사용자가 엉뚱한
설정을 읽는 일이 생기므로, 공용 서버라면 프로파일에 이 환경변수를 박아 두는 편이 안전하다.

---

# 4장. 매일 보는 것과 로그 읽기

## 매일 보는 것

한 번에 전체를 보려면 클러스터 통합 상태가 가장 빠르다.

```bash
curl -s localhost:8088/cluster        # coordinator + 전체 executor + job 집계
curl -s localhost:8088/health         # 살아 있는지만
curl -s localhost:8088/metrics        # CPU·메모리·디스크
curl -s localhost:8088/executors      # coordinator 가 보유한 executor 헬스·메트릭
```

브라우저를 쓸 수 있으면 `http://<coordinator>:8088/` 의 대시보드가 같은 내용을 보여 준다. remote
모드라면 각 executor 도 `http://<host>:8087/` 처럼 자기 화면을 준다. 터미널만 있다면 읽기 전용
모니터를 띄운다.

```bash
bin/dashboard-tui.sh                                  # 설정에서 coordinator 주소를 유추한다
bin/dashboard-tui.sh --url http://host:8088 --interval 5
```

모니터는 스스로 갱신하는데, 목록을 읽는 동안 화면이 바뀌어 거슬리면 스페이스로 잠시 세우고 `+` 와
`-` 로 주기를 바꾼다. 상태 줄의 갱신 시각을 보면 지금 화면이 언제 것인지 알 수 있으므로, 화면이
그대로일 때 멈춘 것인지 조용한 것인지 구분된다. `Enter` 로 job 이나 executor 상세로 들어가고 `ESC`
로 나온다.

살펴야 할 것은 세 가지다. 먼저 executor 가 다 살아 있는지 본다. `/cluster` 의 `executors_summary`
에서 `unhealthy` 가 0이 아니면 그 executor 는 배분에서 빠진 상태다. 한 대가 죽어도 나머지가 일을
나눠 받으므로 작업은 계속되지만 그만큼 용량이 줄어 있다. 다음으로 작업이 쌓이고 있지 않은지 본다.
`jobs.running` 이 실행 슬롯에 붙어 있고 대기가 함께 늘고 있으면 곧 429 가 나기 시작한다. 마지막으로
디스크가 남아 있는지 본다. `local_stage` 나 `s3_stage` 를 쓴다면 CSV 가 잠시 쌓이는데, 적재에 실패한
작업은 그것을 정리하지 못하고 남길 수 있으므로 스테이징 경로를 가끔 들여다본다.

## 로그 읽기

로그는 날짜 단위로 갈리며 두 벌이다.

```bash
L=/data1/distributed-query-executor/logs
tail -f $L/query-coordinator-server.log        # 전체
tail -f $L/query-coordinator-server-warn.log   # WARNING 이상만
tail -f $L/query-executor-server-8087.log
tail -f $L/query-executor-server-8087-warn.log
```

`*-warn.log` 는 경고와 오류만 모아 둔 것이라 문제를 좇을 때는 이쪽부터 훑는 편이 빠르다.

모든 로그 줄에는 작업과 task 식별자가 붙는다. 그래서 사용자가 `job_id` 를 알려 주면 그것만으로
관련된 줄을 전부 모을 수 있고, coordinator 와 executor 의 로그를 같은 식별자로 이어 볼 수 있다.

```bash
grep 'a1b2c3d4' $L/query-coordinator-server.log $L/query-executor-server-*.log
```

실행한 SQL 도 전부 기록된다. 로그 레벨과 무관하게 INFO 로 남으므로 평소 설정에서도 무엇을 읽어
무엇을 적재했는지가 비어 있지 않다.

```
SQL 실행 datasource=impala phase=SELECT | SELECT user_id, ... WHERE dt IN ('2026-01-01')
SQL 실행 datasource=greenplum phase=INSERT target=public.sales | INSERT INTO ...
```

`datasource` 로 어느 엔진에 던진 문장인지 갈리므로 소스에서 못 읽은 것인지 대상에 못 넣은 것인지가
한눈에 구분된다. Trino 로 읽을 줄 알았던 쿼리가 Impala 로 나간 것 같은 사고도 이 표시 하나로
판별된다. 아주 긴 SQL 은 잘리는데, 잘린 경우에는 전문이 아니라는 표시가 함께 남는다.

HTTP 요청과 응답까지 보려면 로그 레벨을 DEBUG 로 내린다. 다만 양이 크게 늘므로 문제를 좇는 동안만
쓰고 되돌린다.

---

# 5장. 동시성과 용량

값을 얼마로 잡을지는 이 장 하나로 판단할 수 있다. 어느 층에 어떤 파라미터가 붙어 있는지부터 그림으로
잡아 두면 어디를 올려야 할지가 분명해진다.

![과부하 방어는 세 층으로 겹친다](images/admission.svg)

## ceiling 은 coordinator 가 아니라 downstream 이다

전체 처리량의 ceiling 은 coordinator 가 아니라 그 뒤의 Impala 와 Greenplum 이 정한다. 동시에 처리할
수 있는 task 수의 effective limit 은 다음 세 값 중 가장 작은 것이다.

```
유효 동시 task ≈ min(
    Σ executor.max_concurrent_tasks,      (= executor 수 × executor 당 동시 task)
    Greenplum 이 견디는 동시 COPY 세션 수,
    Impala 동시 쿼리 슬롯(REQUEST_POOL 한도)
)
```

executor 를 아무리 많이 띄워도 Greenplum 의 동시 COPY 세션이나 Impala 의 쿼리 슬롯이 더 작으면
거기서 막힌다. 그래서 순서를 거꾸로 잡는다. 먼저 downstream 이 안전하게 견디는 한도를 확정하고, 그
한도를 executor 풀에 나누어 분배한 뒤, coordinator 쪽 디스패치 상한은 그보다 넉넉히 크게 잡아
스스로 병목이 되지 않게 한다.

## 항목별로 정하는 기준

`executor.max_concurrent_tasks` 는 노드 한 대 기준으로 대략 코어 수와, 안전한 GP 동시 COPY 를
executor 수로 나눈 값과, 메모리를 task 하나가 쓰는 메모리로 나눈 값 중 가장 작은 것으로 잡는다.
task 하나는 소스 커넥션 하나와 Greenplum 커넥션 하나, 그리고 `copy.batch_size` 만큼의 버퍼를 쓴다.
메모리가 빡빡하면 이 값을 가장 먼저 줄인다.

`greenplum.pool_max` 는 executor 가 재사용하는 GP 커넥션 풀의 크기로, Greenplum 의
`max_connections` 를 직접 보호하는 파라미터다. 기본값 0이면 풀 크기가 `executor.max_concurrent_tasks`
와 같아져 동시 task 하나당 연결 하나가 된다. 클러스터 전체의 동시 GP 연결은 이 값을 executor 수만큼
곱한 것이므로, 그 합이 Greenplum 이 허용하는 세션 수를 넘지 않게 잡는다. 동시 task 수보다 작게 두면
task 가 연결을 기다리며 처리량만 깎이고, 크게 둬 봐야 동시 task 수가 ceiling 이라 의미가 없다.

`coordinator.max_dispatch_concurrency` 는 모든 executor 의 동시 task 수 합 이상으로 둔다(기본 32).
너무 작으면 executor 가 노는데도 task 를 충분히 띄우지 못해 오히려 coordinator 가 병목이 된다. 이
값은 0으로 두면 안 된다. 세마포어가 0이 되어 디스패치가 영원히 멈춘다.

`coordinator.max_concurrent_jobs` 와 `max_pending_jobs` 는 admission 보호용이다. 동시 작업 수에 평균
분할 task 수를 곱한 값이 앞서 구한 유효 동시 task 수를 크게 넘지 않게 잡는다. 대기 큐는 잠깐 몰리는
요청을 흡수하는 buffer 이며, 길수록 429 거절은 줄지만 대기 지연이 늘어 오래된 요청이 쌓인다. 다만
0으로 두면 buffer 가 아예 사라져 슬롯을 넘는 요청이 곧바로 429 가 된다는 점에 주의한다. coordinator
를 여러 대 두면 이 값들이 인스턴스마다 따로 적용되므로 인스턴스 수만큼 나눠 총량을 맞춘다.

`copy.batch_size` 는 처리량과 메모리의 trade-off 다. 행이 넓거나 메모리가 빠듯하면 2000에서 5000
사이로 낮추고, 좁고 넉넉하면 20000 이상으로 올린다. 파이프라인을 쓴다면 `copy.queue_size` 를 곱한
만큼이 task 하나가 메모리에 들고 있는 최대 행 수다.

`coordinator.task_timeout_s` 는 가장 큰 단일 task 의 예상 실행 시간에 여유를 더해 잡는다. 너무 짧으면
정상 task 가 타임아웃으로 실패하고 너무 길면 멈춘 task 를 뒤늦게 감지한다. 반면
`task_connect_timeout_s` 는 연결을 맺는 순간만의 타임아웃이라 짧게(기본 5초) 둘수록 죽은 노드를 빨리
걸러 failover 를 앞당긴다.

한 가지 예외가 있다. 커서 없는 커스텀 API 를 소스로 쓰는 경우 메모리 특성이 다르다. Impala 커서
경로는 진짜 스트리밍이라 메모리가 배치 크기에 묶이지만, 커스텀 API 가 결과를 한 번에 돌려주면 task
하나의 결과 전체가 executor 메모리에 올라간다. 이때는 `parallelism` 을 늘려 task 당 파티션을 잘게
쪼개는 것이 1차 완화책이고, 근본 해결은 그 API 에 페이징을 넣어 청크를 넘겨주게 하는 것이다.
프레임워크는 이미 청크 형태를 받으므로 코드 수정 없이 스트리밍으로 전환된다.

## 튜닝하는 순서

downstream 의 안전 한도부터 확정한다. 그다음 executor 수와 executor 당 동시 task 수의 곱이 그 한도에
맞도록 분배하고, 디스패치 상한을 그 합 이상으로 두며, admission 값으로 과부하를 막는다. 그 상태에서
부하를 걸고 메트릭을 보며 병목 지점을 찾아 값을 조정한다.

여기서 반드시 지킬 것은 한 번에 한 값씩 바꾸는 것이다. 여러 값을 동시에 바꾸면 어떤 변경이 효과를
냈는지 알 수 없어 튜닝이 미궁에 빠진다.

---

# 6장. 문제가 생겼을 때

## 작업이 밀릴 때

사용자가 `429` 를 본다는 것은 실행 슬롯과 대기 큐가 모두 찼다는 뜻이다. 고장이 아니라 설계된
방어선이므로, 먼저 어느 층이 좁은지 가려낸다.

```bash
curl -s localhost:8088/cluster    # jobs.running / jobs.active 와 executor 부하를 함께 본다
```

executor 는 한가한데 429 가 난다면 admission 이 좁은 것이다. `coordinator.max_concurrent_jobs` 로
동시 실행 슬롯을, `coordinator.max_pending_jobs` 로 대기 큐를 올린다. executor 가 이미 포화라면
admission 만 넓혀 봐야 대기만 길어진다. 이때는 executor 를 늘리거나 `executor.max_concurrent_tasks`
를 올리는데, 후자는 앞 장의 기준대로 소스와 대상의 여력을 함께 봐야 한다. 디스패치가 병목일 수도
있다. `coordinator.max_dispatch_concurrency` 가 fleet 전체 용량, 그러니까 executor 수에 동시 task
수를 곱한 값보다 작으면 executor 가 놀아도 task 가 나가지 못한다. 설정 TUI 의 동시성 탭이 이
어긋남을 경고로 짚어 준다.

## 느릴 때

먼저 어디가 느린지 가른다. 소스에서 못 읽고 있는지, 대상에 못 넣고 있는지, 그 사이가 막혔는지를
나눠야 손댈 곳이 정해진다. executor 상세에서 task 가 `READING` 에 오래 머물면 소스 쪽이고 `WRITING`
이면 Greenplum 쪽이다. 대시보드나 `GET /executors/{idx}/metrics` 로 볼 수 있고, 로그의 SQL 기록에서
`datasource` 를 봐도 같은 판단을 할 수 있다.

### task 하나를 네 갈래로 쪼개 보기

더 정확히 짚으려면 대시보드의 단계 타임라인과 `task_history` 테이블이 wall-clock time 을 네 갈래로
나눠 준다.

![task 하나의 wall-clock time 을 네 갈래로 나눠 본다](images/task-timing.svg)

`read_wait_ms` 는 소스에서 결과를 읽는 순수 시간이고, `read_starve_ms` 는 쓰는 쪽이 다음 배치를
기다린 시간이며, `write_wait_ms` 는 인코딩과 송신에 쓴 시간, `finalize_wait_ms` 는 COPY 가 서버에서
끝나기를 기다린 시간이다. 파이프라인 모드에서 wall-clock time 은 대략 뒤의 세 항의 합이므로 그중
가장 큰 것이 곧 병목이다.

`read_starve` 가 지배적이면 소스가 느린 것이다. `parallelism` 을 늘려 여러 executor 가 서로 다른
파티션을 동시에 읽게 하는 것이 가장 효과가 크고, 이어서 `copy.batch_size` 를 올려 왕복을 줄이거나
`impala.query_options` 로 전용 풀과 메모리 상한을 조정한다. 이관이 Impala 의 다른 작업과 자원을
다투고 있다면 이 옵션으로 서로 밀어내지 않게 할 수 있다.

`write_wait` 가 지배적이면 executor 쪽의 인코딩과 전송이 병목이다. `copy.format=binary` 로 텍스트
인코딩 CPU 를 줄이고(타입 해석에 실패하면 자동으로 text 로 fallback 한다), executor 와 GP 사이
네트워크를 점검한 뒤 `copy.batch_size` 를 올린다.

`finalize_wait` 가 지배적이면 Greenplum 의 COPY 처리가 병목이다. 한 스트림이 마스터로 몰리는 구조라
`parallelism` 을 늘려 여러 executor 가 동시에 COPY 하게 하는 것이 가장 효과적이고, 동시 GP 연결은
`greenplum.pool_max` 로 조절한다. 대상 테이블의 인덱스와 트리거, 분산키도 함께 재검토한다.

원인을 격리하고 싶으면 `copy.pipeline=false` 로 잠깐 꺼 본다. 읽기와 쓰기가 직렬로 돌아 `read_wait`
와 `write_wait` 가 순수 wall-clock time 으로 나뉘므로 비교하기 쉽다. `read_starve` 와 `write_wait`
가 비슷하다면 이미 파이프라인이 잘 겹치는 상태이므로 다음 수는 수평 확장이다.

### 경로 자체를 바꿔야 할 때

파이프라인과 배치 크기, 수평 확장을 다 해도 `finalize_wait` 가 계속 지배적이라면 병목은 COPY 가
Greenplum 마스터 한 노드로 몰리는 구조 자체다. executor 를 아무리 늘려도 각자 마스터로 COPY 하므로
마스터가 최종 ceiling 이 된다. 이때는 data plane 을 우리가 push 하는 방식에서 GP 가 pull 하는
방식으로 바꾼다.

`local_stage` 나 `s3_stage` 로 옮기면 Greenplum 의 모든 세그먼트가 파일을 나눠 동시에 읽으므로 단일
소켓 병목이 사라진다. 어느 쪽이 가능한지는 co-location 제약이 정하는데, `local_stage` 는 executor 와
GP 세그먼트가 같은 호스트에 있어야 하고 `s3_stage` 는 그 제약이 없는 대신 버킷과 PXF 설정이 필요하다
(9장에서 다룬다).

세 번째 길도 있다. `exec_mode=statement` 로 두고 PXF 외부테이블을 읽는 `INSERT … SELECT` 를 넘기면
COPY 도 executor 를 통한 스트리밍도 전혀 없이 GP 가 스스로 읽는다. 이 방식은 코드를 한 줄도 고치지
않고 시험해 볼 수 있으므로, 먼저 파일럿으로 기존 경로와 처리량을 비교해 보는 편이 좋다. 다만 PXF 를
설치하고 구성해야 하고 모든 GP 세그먼트가 원본 저장소에 직접 도달해야 하므로, 망분리 환경에서는
방화벽과 라우팅이 실제 관문이 된다.

## 실패를 추적할 때

사용자가 `job_id` 를 들고 오면 순서는 이렇다. 먼저 작업 상태를 본다.

```bash
curl -s localhost:8088/jobs/$JOB_ID | python3 -m json.tool
```

`error` 에 이유가 있고 `tasks` 배열에서 어느 task 가 어느 executor 에서 실패했는지, 몇 번
재시도됐는지 보인다. `PARTIAL` 이면 일부만 들어간 것이므로 사용자에게 `POST /jobs/{id}/retry` 를
안내한다. 이미 성공한 task 는 건너뛰므로 중복 적재 걱정은 없다.

그다음 그 식별자로 로그를 모은다. 실행한 SQL 이 함께 남아 있으므로 소스 쿼리가 실패했는지 적재
문장이 실패했는지가 드러난다.

```bash
grep "$JOB_ID" $L/query-coordinator-server.log $L/query-executor-server-*.log | less
```

마지막으로 실제 엔진에 직접 물어본다. 로그의 SQL 을 그대로 손으로 실행해 보는 것이 가장 확실하고,
여기가 터미널 도구가 가장 요긴한 자리다.

```bash
bin/impala-shell        # 소스 쪽 확인
bin/gp-shell            # 대상 쪽 확인 — 테이블이 있는지, 권한이 있는지
bin/s3-ops ls s3://<버킷>/<프리픽스>/$JOB_ID/    # s3_stage 중간 산출물 확인
```

### 자주 나오는 원인

가장 먼저 의심할 것은 executor 에 닿지 못한 경우다. coordinator 는 연결에 실패하면 몇 번 재시도했다가
다른 executor 로 넘기므로(`coordinator.task_failover`), 로그에 연결 실패가 반복된다면 그 executor 의
프로세스와 포트를 확인한다. 다만 `local_stage` 는 executor 와 세그먼트가 짝지어 있어 다른 곳으로
넘어가면 그 짝이 깨지므로, 이 모드에서는 failover 가 도는 것 자체가 이미 신호다.

접속이 멀쩡하다면 다음은 스키마가 어긋난 경우다. 대상 테이블이나 컬럼이 SELECT 결과와 맞지 않는 일이
흔한데, `copy.preflight` 가 켜져 있으면 COPY 를 시작하기 전에 걸러 주지만 꺼져 있으면 데이터를 반쯤
밀어 넣다 실패한다. 비슷하게 `stage_insert` 에서 TEMP 테이블이 `already exists` 로 부딪힌다면
`coordinator.stage_unique_staging` 이 꺼져 있는지 본다. GP 연결을 풀에서 재사용하는 구조라 이름이
같으면 앞 작업의 TEMP 가 그대로 남아 있기 때문이다.

증상이 아예 다른 경우도 있다. 작업은 성공이라는데 대상에 데이터가 없다면 MockBackend 를 의심한다.

```bash
grep MockBackend $L/query-executor-server-*-warn.log
# greenplum.dsn 미설정 → MockBackend 사용
```

### local_stage 와 s3_stage 의 실패

이 두 모드는 2단계로 돌기 때문에 실패 지점이 더 나뉜다.

`local_stage` 에서 "파일 예산 초과"가 뜨면 요청의 `parallelism` 이 호스트별 세그먼트 수의 합보다
크다는 뜻이다. 값을 낮추거나 executor 호스트를 늘린다. "gp_segment_configuration 에 없습니다"는
`executor.gp_hostname` 이 실제 세그먼트 호스트명과 다르다는 뜻이므로, executor 의 `/metrics` 가
보고하는 값과 `SELECT DISTINCT hostname FROM gp_segment_configuration` 결과를 대조한다. Phase 2 에서
파일을 못 읽는다면 세그먼트 호스트에서 그 파일이 실제로 있는지, GP 세그먼트 프로세스가 읽을 권한이
있는지, `stage.local_dir` 이 모든 호스트에 같은 경로로 있는지 확인한다. CSV 파싱이 어긋난다면
데이터에 구분자로 쓰는 문자가 들어 있을 수 있으므로 `stage.csv_delimiter` 를 데이터에 없는 문자로
바꾼다.

`s3_stage` 에서 Phase 1 업로드가 실패하면 자격증명과 엔드포인트를 본다. `s3.bucket` 이 아예 설정되지
않았다면 그 취지의 예외가 난다. Phase 2 에서 실패하면 PXF SERVER 와 프로파일, GP 쪽 권한을 확인한다.
이때 S3 객체는 남아 있으므로 `bin/s3-ops head` 로 실제 파일 모양을 확인한 뒤 원인을 고쳐 재실행할 수
있다.

두 모드 모두 디버깅 중에는 정리를 꺼 두면 중간 산출물을 들여다볼 수 있다. `stage.cleanup=false` 또는
`s3.delete_on_cleanup=false` 로 두되, 끝난 뒤 되돌리지 않으면 디스크와 버킷이 계속 찬다.

---

# 7장. 기동·중지와 규모 조정

## 기동·중지·재기동

중지는 coordinator 부터 하고 기동은 executor 부터 한다. 받아 줄 곳이 없는 상태에서 요청을 받지 않기
위해서다.

```bash
B=/data1/distributed-query-executor/bin
sudo -u gpadmin $B/status-coordinator.sh      # 프로세스 + health
sudo -u gpadmin $B/status-executor.sh

sudo -u gpadmin $B/stop-coordinator.sh
sudo -u gpadmin $B/restart-executor.sh        # 전체 재기동
sudo -u gpadmin $B/start-executor.sh 8086     # 특정 포트만
sudo -u gpadmin $B/stop-executor.sh  8086
```

executor 는 SIGTERM 을 받으면 진행 중인 task 가 끝나기를 기다렸다 내려간다. 기다리는 시간은
`executor.shutdown_drain_timeout_s` 이며 기본값은 25초다. 재기동 중에 task 가 잘리는 것이 곤란하다면
평소 task 하나가 걸리는 시간보다 넉넉히 잡아 둔다.

기동할 때 콘솔과 로그에 찍히는 배너에는 실제로 읽은 설정 파일의 절대 경로가 나온다. 설정을 바꿨는데
반영이 안 된 것 같으면 여기부터 본다. 엉뚱한 디렉터리를 읽고 있는 경우가 많다.

기동 뒤에는 실제로 두드려 확인한다. 헬스와 메트릭을 조회하고 작은 작업을 하나 넣어 끝까지 도는지 보면
전체 경로가 살아 있다는 좋은 신호가 된다. 서버 밖에서 coordinator 에 접근해야 한다면 방화벽을 연다.
executor 포트는 보통 같은 호스트 안의 내부 통신이라 외부로 열 필요가 없다.

```bash
curl -s localhost:8088/health
curl -s localhost:8087/health
sudo firewall-cmd --permanent --add-port=8088/tcp && sudo firewall-cmd --reload
```

## executor 늘리고 줄이기

처리량 확장은 executor 인스턴스 수로 한다. 순서가 중요하다. 먼저 `coordinator.executors` 에 새 URL 을
추가한다. 이 목록에 없으면 프로세스를 띄워도 coordinator 가 일을 주지 않는다. 그다음 새 인스턴스를
띄운다.

```bash
sudo -u gpadmin $B/start-executor.sh 8003
# 또는 전체를 한 번에: EXECUTOR_PORTS="8087 8086 8003" $B/start-executor.sh
```

executor 목록은 기동할 때 읽으므로 coordinator 를 재기동한다. 마지막으로
`curl -s localhost:8088/cluster` 에서 새 executor 가 healthy 로 잡히는지 확인한다. 줄일 때는
역순이다. `coordinator.executors` 에서 빼고 coordinator 를 재기동해 새 task 가 가지 않게 한 뒤, 그
executor 에서 돌던 task 가 끝나기를 기다렸다 내린다.

늘린 뒤에는 함께 움직여야 하는 값이 있다. executor 가 늘면 fleet 전체 용량이 커지므로
`coordinator.max_dispatch_concurrency` 가 그보다 작지 않은지 보고, Greenplum 의 `max_connections` 가
executor 수에 `pool_max` 를 곱한 만큼을 감당하는지 확인한다. 설정 TUI 의 동시성 탭이 이 곱셈을 풀어
보여 준다.

`local_stage` 를 쓴다면 새 executor 도 GP 세그먼트 호스트 위에 있어야 하고 `executor.gp_hostname` 을
그 호스트명과 정확히 맞춰야 한다. 또한 `stage.local_dir` 이 모든 세그먼트 호스트에 같은 경로로
존재하고, executor 프로세스가 쓸 수 있으며 GP 세그먼트 프로세스가 읽을 수 있어야 한다.

---

# 8장. 여러 대로 늘리기와 이력 남기기

## coordinator 를 여러 대 두기

기본 설정에서는 coordinator 가 한 대뿐이고 모든 상태를 자기 메모리에만 둔다. 처음에는 이것으로
충분하지만, 가용성을 높이거나 재시작해도 실행 이력이 남게 하려면 모두가 함께 바라볼 공유 PostgreSQL
을 설정한다. 핵심은 모든 coordinator 와 executor 가 같은 DSN 을 공유한다는 것이다.

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

공유 작업 저장소를 켜면 상태가 `jobs` 테이블에 JSONB 로 저장되어 어느 coordinator 로 조회하거나
취소해도 똑같이 동작한다. 이 설정을 하지 않은 채 로드밸런서 뒤에 여러 대를 두면, 작업을 접수한
인스턴스만 그 상태를 알기 때문에 제출과 폴링이 서로 다른 인스턴스로 가면 사용자가 404 를 받는다.

이력은 두 계층으로 나뉜다. 작업의 시작과 종료는 coordinator 가 `job_history` 에 남기고, 각 task 의
상태 변화는 executor 가 `task_history` 에 남긴다. task 이력은 executor 가 직접 쓰므로 executor
호스트에서도 이 DB 에 닿아야 한다. 사용자가 작업을 낼 때 `username` 을 채우면 두 테이블에 함께
기록되어 대시보드에서 누가 낸 작업인지 보인다.

여기서 반드시 지킬 것은 앱이 스키마를 자동으로 만들지 않는다는 점이다. 서비스를 띄우기 전에 통합
스키마를 먼저 적용하지 않으면 "relation does not exist" 로 실패한다.

```bash
PG="postgresql://user:pass@pg-host:5432/queryexec"
psql "$PG" -f /data1/distributed-query-executor/config/postgresql.sql
```

메타 저장소를 WarehousePG 나 Greenplum 7 에 둘 때는 `postgresql.sql` 대신 `warehousepg.sql` 을
적용한다. 분산 데이터베이스라 테이블마다 분산키를 지정해야 하기 때문이다. 앱 코드는 그대로 동작한다.
다만 heartbeat 와 예약은 잦은 단일 행 갱신이라 MPP 와 잘 맞지 않으므로, 성능이 중요하면 메타
저장소는 일반 PostgreSQL 에 두고 WarehousePG 는 데이터 적재 대상으로만 쓰는 편이 낫다.

단일 coordinator 라면 기본값 그대로 두면 되고, 이력만 남기고 싶으면 `history.db_dsn` 만 설정해도
된다. 참고로 admission 한도는 coordinator 인스턴스마다 따로 적용되므로, 여러 대를 띄우면 전체 한도가
그 수만큼 곱해진다는 점을 감안해 값을 나눠 잡는다.

### HA 타이밍 값의 순서 관계

여러 대를 띄운다면 타이밍 값들 사이에 반드시 지켜야 하는 순서가 있다. 핵심은 장애를 판정하는 임계가
heartbeat 를 보내는 주기보다 넉넉히 길어야 잠깐 신호가 늦은 것을 죽음으로 오판하지 않는다는 것이다.

```
status_interval_s  ≤  heartbeat_interval_s  ＜  coordinator_stale_s  ≤  orphan_reconcile 주기
       (10)                   (10)                     (30)
heartbeat_interval_s  ＜  reservation_ttl_s
       (10)                     (60)
```

`coordinator_stale_s` 는 `heartbeat_interval_s` 의 두세 배로 둔다. 신호를 한두 번 놓쳐도 살아 있다고
봐 주기 위해서이며, 너무 작으면 잠깐의 지연만으로 멀쩡한 coordinator 의 작업을 빼앗는다.
`reservation_ttl_s` 는 heartbeat 의 몇 배로 두는데, 너무 짧으면 예약이 일찍 풀려 균형 효과가 사라지고
너무 길면 죽은 coordinator 의 예약이 남아 부하를 부풀려 보이게 한다. 장애를 더 빨리 감지하고 싶다면
위 부등식 순서를 깨지 않은 채 관련 값들을 한 세트로 함께 줄인다. 하나만 줄이면 부등식이 깨져 오탐이
생긴다.

부하 배분과 관련해서는 `coordinator.executor_select` 를 본다. `round_robin` 은 단순히 돌아가며 주므로
executor 성능이 고르고 task 길이가 비슷할 때 무난하고, `least_loaded` 는 가장 한가한 곳을 고르지만
여러 coordinator 가 같은 판단을 동시에 내려 한쪽으로 몰릴 수 있다. 그래서 멀티 coordinator 에서는 두
곳만 비교해 덜 바쁜 쪽을 고르는 `p2c` 를 권한다. 배분이 실제로 쏠린다면
`coordinator.executor_reservation` 을 켜서 디스패치하는 동안 자리를 미리 잡아 둔다.

## 모니터링 기록 남기기

살아 있는지를 넘어 자원 사용 추세를 보고 싶다면 메트릭을 DB 에 쌓는다. coordinator 는
`monitor.health_interval_s` 마다 모든 executor 의 헬스와 메트릭을 폴링해 상태를 보유하고,
`monitor.record_interval_s` 마다 그 사용량을 PostgreSQL 에 기록한다.

```properties
monitor.enabled=true
monitor.health_interval_s=10
monitor.record_interval_s=60
monitor.db_dsn=postgresql://user:pass@pg-host:5432/monitoring
monitor.table=executor_health_metrics
monitor.disk_path=/
```

`monitor.db_dsn` 을 비워 두면 헬스 체크는 계속하되 결과를 DB 에 남기지 않는다. 여기서도 같은
원칙이라 테이블을 앱이 만들지 않으므로 통합 스키마를 먼저 적용한다. 다른 DB 를 가리킨다면 그 DB 에도
똑같이 적용한다. `monitor.enabled` 를 끄면 대시보드의 executor 상태가 갱신되지 않고 `least_loaded` 나
`p2c` 선택도 판단 근거를 잃으므로, 특별한 이유가 없으면 켜 둔다.

```bash
psql "postgresql://user:pass@pg-host:5432/monitoring" -f /data1/distributed-query-executor/config/postgresql.sql
psql ... -c "SELECT recorded_at, executor_url, healthy, cpu_percent, memory_percent
             FROM executor_health_metrics ORDER BY recorded_at DESC LIMIT 20;"
```

메타 테이블은 모두 `db.schema`(기본 `public`)로 한정된다. 스키마나 테이블 이름을 바꾸면 **설정과 DDL
두 파일을 함께** 고쳐야 하고, PostgreSQL 판과 WarehousePG 판이 두 벌이므로 둘 다 손봐야 한다.

---

# 9장. S3 스테이징과 PXF 준비

`s3_stage` 가 어떤 순서로 도는지를 먼저 보아 두면 무엇을 준비해야 하는지가 저절로 갈린다.

![s3_stage 는 barrier 를 사이에 둔 3단계로 돈다](images/s3-stage-phases.svg)

`s3_stage` 는 executor 와 GP 세그먼트를 같은 호스트에 두지 않아도 되는 대신, 운영자가 미리 갖춰
두어야 할 것이 두 가지다. executor 가 객체를 올릴 **S3 접근**과, Greenplum 이 그 객체를 읽을 **PXF
외부테이블 경로**다. 둘은 자격증명 체계가 서로 다르다는 점이 핵심이다. 올리는 쪽은 이 저장소의
`s3.*` 설정을 쓰고, 읽는 쪽은 GP 세그먼트에 배포된 PXF SERVER 설정을 쓴다.

## 서비스 쪽 S3 설정

```properties
s3.bucket=dw-stage
s3.prefix=dqe-stage
s3.endpoint_url=          # MinIO·Ceph 같은 S3 호환이면 지정, AWS 면 비운다
s3.region=ap-northeast-2
s3.access_key=
s3.secret_key=
s3.pxf_server=s3srv       # GP 읽기: PXF SERVER 이름만 준다
s3.pxf_profile=s3:csv
s3.external_schema=dwtemp # 지정하면 dwtemp.s3ext_<job_id> 로 만들고 지운다(스키마는 미리 생성)
s3.delete_on_cleanup=true
```

`s3.access_key`/`s3.secret_key` 를 비우면 boto3 의 기본 자격증명 체인이 그대로 동작하므로, EC2
인스턴스 프로파일이나 IAM 역할을 쓰는 환경이라면 아예 적지 않는 것이 가장 깔끔하다. 같은 `s3.*` 를
coordinator 와 executor, 그리고 `bin/s3-ops` 가 함께 쓰므로 값은 한 곳에만 있으면 된다.
`s3.external_schema` 를 지정했다면 그 스키마는 운영자가 미리 만들어 두어야 한다.

## IAM 권한을 좁게 주기

무엇을 호출하는지 알면 정책을 좁힐 수 있다. 목록과 조회에는 `s3:ListBucket` 과 `s3:GetObject`,
업로드에는 `s3:PutObject`, 삭제에는 `s3:DeleteObject` 가 필요하고, 버킷 안 복사·이동은 `GetObject`
와 `PutObject` 조합이다(`mv` 는 복사 후 삭제라 삭제 권한도 함께 필요하다).

여기서 가장 자주 사고가 나는 곳이 **멀티파트 업로드**다. 8MB 가 넘는 파일은 boto3 가 알아서 여러
task 로 나눠 올리는데, 이때는 `PutObject` 가 아니라 `CreateMultipartUpload` 부터 나간다.
`s3:PutObject` 만 열어 둔 계정에서는 작은 파일은 잘 올라가는데 큰 파일만 `AccessDenied` 로 실패하는,
진단하기 까다로운 증상이 된다. 도구는 멀티파트 개시가 거부되면 단일 `PutObject` 로 한 번 더 시도해
이 상황을 어느 정도 흡수하지만, 단일 `PutObject` 는 5GB 까지만 올라가므로 그보다 큰 파일을 다루는
계정이라면 멀티파트 네 개 권한(`s3:CreateMultipartUpload`·`s3:UploadPart`·
`s3:CompleteMultipartUpload`·`s3:AbortMultipartUpload`)을 받는 것 외에 방법이 없다.

`buckets` 하위 명령은 `s3:ListAllMyBuckets` 를 요구한다. 이 권한 없이 특정 버킷만 쓰는 계정은 아주
흔하므로 **목록이 비어 있다고 버킷이 없는 것은 아니다.** 사용자에게 미리 알려 주고, 확인이 필요하면
`exists` 로 개별 접근을 시험하게 한다.

## GP 쪽 PXF 구성

**가장 먼저 할 일은 확장 등록이다.** `pxf` 는 Greenplum 내장 프로토콜이 아니라 확장으로 등록해야
하는 사용자 정의 프로토콜이라, 이 단계를 건너뛰면 외부테이블을 만들 때 `protocol "pxf" does not
exist` 로 실패한다. 내장 `s3` 프로토콜은 등록 없이 바로 쓸 수 있어서 그쪽만 써 봤다면 놓치기 쉽다.

```bash
bin/gp-shell -d dw            # 적재 대상 데이터베이스로 접속
```

```sql
CREATE EXTENSION pxf;
SELECT extname, extversion FROM pg_extension WHERE extname = 'pxf';
GRANT SELECT ON PROTOCOL pxf TO etl;   -- 슈퍼유저가 아닌 계정이 외부테이블을 만들려면 필요
```

**확장은 데이터베이스마다 따로 설치해야 한다.** `postgres` 에 설치했다고 `dw` 에서 쓸 수 있는 것이
아니다. `could not open extension control file` 이 나면 확장 파일이 아직 `$GPHOME` 에 배포되지 않은
상태이므로 `pxf cluster register` 후 `pxf cluster restart` 를 하고 다시 시도한다. 권한이 없으면
`permission denied for protocol pxf` 가 나는데, `does not exist` 와는 다른 오류이므로 메시지로
구분한다.

그다음 서버 디렉터리를 만들고 자격증명을 채운다. **여기서 정한 디렉터리 이름이 곧 `s3.pxf_server`
값이자 LOCATION 의 `SERVER=` 값이다.**

```bash
mkdir -p "$PXF_BASE/servers/s3srv"
cp "$PXF_HOME/templates/s3-site.xml" "$PXF_BASE/servers/s3srv/"
# fs.s3a.access.key / fs.s3a.secret.key / fs.s3a.endpoint 를 채운다
# MinIO 등이면 fs.s3a.path.style.access=true 가 필요할 수 있다
chmod 600 "$PXF_BASE/servers/s3srv/s3-site.xml"
pxf cluster sync
```

자격증명이 평문으로 들어가므로 권한을 좁힌다. EC2 에서 IAM 역할을 쓴다면 키를 비우고
`fs.s3a.aws.credentials.provider` 를 인스턴스 프로파일 제공자로 지정한다. 버킷마다 자격증명이 다르면
`servers/s3-prod` , `servers/s3-dev` 처럼 디렉터리를 나누고 작업마다 다른 `s3.pxf_server` 를 쓰면
된다.

구성이 끝났는지 확인하는 가장 빠른 길은 손으로 외부테이블을 하나 만들어 읽어 보는 것이다. 서비스가
Phase 2 에서 만드는 것과 같은 모양이다.

```sql
CREATE EXTERNAL TABLE dwtemp.ext_check (
    user_id bigint, amount numeric, dt date
)
LOCATION ('pxf://dw-stage/dqe-stage/job_probe/?PROFILE=s3:csv&SERVER=s3srv')
FORMAT 'CSV';

SELECT count(*) FROM dwtemp.ext_check;
```

prefix 는 디렉터리처럼 동작해 그 아래 파일을 세그먼트가 나눠 읽고, `.gz` 는 확장자를 보고 알아서
풀어 읽는다. 파일이 세그먼트 수만큼 고르게 나뉘었는지는 `bin/s3-ops ls --summary` 로 개수와 크기
분포를 보면 된다. 한 파일이 지나치게 크면 그 파일을 읽는 세그먼트만 오래 걸린다.

---

# 10장. 적재 대상 테이블 설계 — 분산키

적재가 느리거나 특정 세그먼트만 붐빈다면 대상 테이블의 분산키를 의심한다. `DISTRIBUTED BY` 를
생략하면 Greenplum 이 첫 컬럼이나 기본키로 정하는데, 이 기본값이 항상 좋은 선택은 아니라 큰 테이블은
한 번 확인하는 편이 좋다.

볼 것은 세 가지다. 먼저 고유값 개수가 세그먼트 수의 최소 10배, 가능하면 100배 이상이어야 값이 고르게
흩어진다. 다음으로 NULL 비율을 보는데, NULL 은 전부 한 세그먼트로 몰리므로 0에 가까울수록 좋다.
마지막으로 최빈값의 비중이 세그먼트 수의 역수를 넘으면 그 값 하나만으로 이미 편중이 확정된다.

`ANALYZE` 만 되어 있으면 데이터를 읽지 않고 통계로 후보를 훑을 수 있다. 여러 줄짜리라 `bin/gp-shell`
에서 `\paste` 로 붙여 넣는 편이 편하다.

```sql
WITH seg AS (
    SELECT count(*)::numeric AS n FROM gp_segment_configuration WHERE content >= 0 AND role = 'p'
), tab AS (
    SELECT c.reltuples::numeric AS rows FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND c.relname = 'sales_mirror'
)
SELECT s.attname AS column_name,
       CASE WHEN s.n_distinct < 0 THEN round(-s.n_distinct * tab.rows)
            ELSE round(s.n_distinct::numeric) END              AS est_ndv,
       round(s.null_frac::numeric, 4)                          AS null_frac,
       round(coalesce(s.most_common_freqs[1], 0)::numeric, 4)  AS top_value_frac,
       seg.n::int                                              AS segments
  FROM pg_stats s CROSS JOIN seg CROSS JOIN tab
 WHERE s.schemaname = 'public' AND s.tablename = 'sales_mirror'
 ORDER BY est_ndv DESC;
```

`n_distinct` 가 음수면 "전체 행 수 대비 비율"이라는 뜻이라 위처럼 행 수를 곱해 환산해야 한다. 통계가
없으면 결과가 비거나 부정확하니 `ANALYZE` 를 먼저 돌린다. 이미 만든 테이블의 실제 편중은 바로 볼 수
있고, 전체 스캔이 필요한 `gp_toolkit` 뷰는 한가한 시간에 돌린다.

```sql
SELECT gp_segment_id, count(*) AS rows FROM public.sales_mirror GROUP BY 1 ORDER BY 2 DESC;

SELECT skcnamespace, skcrelname, round(skccoeff::numeric, 2) AS skew_coeff
  FROM gp_toolkit.gp_skew_coefficients
 WHERE skcnamespace NOT IN ('pg_catalog', 'information_schema')
 ORDER BY skccoeff DESC LIMIT 20;
```

분배가 고른 것만으로 좋은 분산키가 되지는 않는다. 우선순위는 균등 분배, 자주 조인하는 테이블과 조인
키 일치(분산키가 다르면 조인마다 세그먼트 간 재분배가 생긴다), 단일 값 조회가 잦은 컬럼 회피 순이다.
분산키 컬럼은 `UPDATE` 대상이 될 수 없다는 점(값이 바뀌면 저장 세그먼트가 달라진다), 마땅한 후보가
없으면 `DISTRIBUTED RANDOMLY` 도 선택지라는 점, 복합키는 2개를 넘기지 않는 편이 낫다는 점도 함께
기억한다. 이미 만든 테이블은 `ALTER TABLE … SET DISTRIBUTED BY (…)` 로 데이터를 유지한 채 바꿀 수
있지만 전체 재분배가 일어나므로 큰 테이블에서는 시간이 걸린다.

---

# 11장. 터미널 도구를 여럿이 쓰게 만들기

## 자격증명을 어디에 둘 것인가

원칙은 셋이다.

첫째, **비밀번호는 명령행 인자로 받지 않는다.** `ps` 로 같은 서버의 다른 사용자에게 그대로 보이기
때문에 애초에 `--password` 옵션이 없다. 도구는 설정 파일(`greenplum.dsn` 안의 비밀번호,
`impala.password`), 환경변수(`GP_PASSWORD`·`IMPALA_PASSWORD`), 대화형 입력 순으로 찾는다. `.pgpass`
나 trust 인증을 쓰는 환경이라면 `--no-password-prompt` 를 함께 쓰게 안내한다.

둘째, S3 자격증명도 같다. `s3-ops` 에는 `--access-key` 와 `--secret-key` 가 있지만 **공용 서버에서는
쓰지 않는다.** 설정 파일이나 IAM 역할을 쓰는 편이 안전하고, 인스턴스 프로파일을 쓰는 환경이라면
`s3.access_key`/`s3.secret_key` 를 아예 비워 두는 것이 가장 깔끔하다.

셋째, 비밀은 크론 항목이 아니라 권한을 좁힌 파일에서 읽어 오게 한다. crontab 은 다른 사용자가 읽을
수 있는 경우가 있고 한 줄로 길게 늘어져 관리하기 어렵다.

```bash
#!/bin/bash
set -euo pipefail
export IMPALA_PASSWORD="$(cat /etc/etl/impala.pw)"   # chmod 600, 소유자는 실행 계정
```

대화형 셸의 히스토리도 자격증명은 아니지만 값이 그대로 남는 자리다. 셸은 히스토리를 홈 디렉터리
아래 `~/.impala-to-whpg/` 에 엔진별로 두고 디렉터리는 700, 파일은 600 으로 만든다. 저장소 안이 아니라
홈에 두는 것은 실수로 커밋되는 일을 막기 위해서인데, 공용 계정을 여럿이 나눠 쓰는 서버라면 이 파일이
사실상 공유된다는 점을 염두에 둔다.

## 크론에 걸기

crontab 항목은 한 줄이어야 하고 백슬래시로 줄을 이을 수 없으며 `%` 는 개행으로 해석된다. 그래서
명령이 조금만 길어져도 crontab 에 직접 쓰는 것은 한계에 부딪힌다. 셸 스크립트로 감싸고 crontab 에는
그 스크립트만 걸어 둔다. 파이썬 인터프리터를 고정해야 한다면 `PYTHON` 환경변수로 지정하는데, 크론은
`PATH` 가 좁아 이 지정이 사실상 필수인 경우가 많다.

```bash
#!/bin/bash
# /srv/etl/daily-orders.sh
set -euo pipefail
export IMPALA_PASSWORD="$(cat /etc/etl/impala.pw)"
dt="$(date -d yesterday +%Y-%m-%d)"
D=/data1/distributed-query-executor

PYTHONPATH=$D/src $D/.venv/bin/python -m tools.impala_query \
    -f daily_orders.sql -V "dt=$dt" -o "/data/orders-$dt.csv.gz" \
    --gzip --delimiter $'\t' --null-string '\N' --no-header
$D/bin/s3-ops upload "/data/orders-$dt.csv.gz" "s3://dw-stage/orders/$dt/"
```

```cron
0 3 * * * /srv/etl/daily-orders.sh >> /var/log/etl.log 2>&1
0 4 * * * /data1/distributed-query-executor/bin/s3-ops rmdir s3://dw-stage/dqe-stage/ --older-than 7d --yes >> /var/log/etl.log 2>&1
```

크론에서 흔히 문제가 되는 것들은 도구 쪽에서 이미 처리해 두었다. 작업 디렉터리가 임의로 잡히는 문제는
설정과 SQL, 인증서 경로를 스크립트 위치 기준으로 잡아 해결했고, `LANG` 이 없어 파이썬이 출력 인코딩을
`ascii` 로 잡는 문제는 래퍼가 `PYTHONIOENCODING=utf-8` 을 넣어 막는다(이게 없으면 한글을 출력하는
순간 `UnicodeEncodeError` 로 죽는다). 출력이 버퍼에 갇혀 작업이 끝난 뒤에야 한꺼번에 나오는 문제는
`PYTHONUNBUFFERED=1` 로 막았고, 덕분에 중간에 죽어도 어디까지 갔는지 로그에 남는다. 진행 상황도
터미널이 아니면 `\r` 로 덮어쓰는 대신 줄을 새로 쓰고 갱신 간격을 0.2초에서 30초로 늘리므로, 몇
시간짜리 작업이라도 로그가 몇 줄 늘어날 뿐이다. 프롬프트에서 멈추는 사고도 막혀 있어 터미널이 아니면
비밀번호도 삭제 확인도 묻지 않고, `s3-ops` 의 삭제는 `--yes` 없이는 거부한다. 크론에 삭제를 걸 때는
처음 한 번은 `--dry-run` 으로 무엇이 지워질지 확인하는 습관을 권한다.

## 실패를 알아채기

실패 판별은 종료 코드로 한다. `0` 은 성공, `1` 은 대상이 없거나 취소, `2` 는 인자 오류, `3` 은 필요한
파이썬 패키지 없음, `4` 는 접속 실패, `5` 는 실행 실패나 권한 없음이다. 이 구분이 쓸모 있는 이유는
원인별로 대응이 다르기 때문이다. `3` 이나 `4` 가 반복되면 데이터 문제가 아니라 환경이나 접속 설정
문제라 재시도해 봐야 소용이 없고, `5` 는 쿼리 자체나 권한 문제라 사람이 봐야 하며, `1` 은 대개 정상
범주에 가깝다(지울 것이 없었다는 뜻일 수 있다). `exists` 가 이 원칙을 가장 잘 보여 주는데, 있으면
`0`, 없으면 `1`, 권한이 없으면 `5` 로 끝난다. `403` 을 "없음"으로 처리하면 권한 문제를 데이터 문제로
착각해 엉뚱한 곳을 파게 된다.

보고와 데이터의 분리도 운영에서 중요하다. **구간별 소요 시간 요약과 진행 상황을 포함한 모든 보고는
stderr 로 나간다.** stdout 은 조회 결과 몫이라 파이프로 넘기거나 파일로 받을 때 섞이지 않는다. 로그에
둘 다 남기려면 `>> log 2>&1`, 데이터만 받으려면 `> data.txt`, 보고를 버리려면 `2> /dev/null` 이다.

구간별 소요 시간 표는 성공하든 실패하든 나오므로 성능 문제를 신고받았을 때 첫 번째로 볼 자료다. 첫
배치 대기가 길면 소스 쪽 문제이고, 데이터 수신이 길면 네트워크, CSV 쓰기가 길면 로컬 디스크다.

## 접속이 안 될 때

접속 단계에서 `TSocket read 0 bytes` 나 `end of file` 이 나오면 서버가 핸드셰이크 도중 연결을
끊었다는 뜻이다. 인증 실패가 아니라 포트·전송 방식·TLS·인증 방식 중 하나가 서버 설정과 어긋난 경우가
대부분이며, 도구가 현재 설정과 함께 점검 목록을 출력하므로 위에서부터 하나씩 맞춰 본다.

```bash
PYTHONPATH=src python -m tools.impala_query --port 28000 --http-transport -q "SELECT 1"  # HTTP HS2
PYTHONPATH=src python -m tools.impala_query --no-ssl -q "SELECT 1"                        # 서버가 평문일 때
PYTHONPATH=src python -m tools.impala_query --auth-mechanism NOSASL -q "SELECT 1"         # 인증 없는 서버
```

`--debug` 의 뜻은 도구마다 다르다. `impala_query` 는 실행할 SQL 과 SASL 핸드셰이크 로그를,
`gp_query` 는 템플릿을 채운 뒤 실제로 보내는 SQL 을, `s3_ops` 는 오류가 났을 때 전체 스택 트레이스를
낸다.

---

# 12장. 정기적으로 살필 것과 업그레이드

## 정기 점검

디스크부터 본다. `local_stage` 와 `s3_stage` 의 스테이징 경로, 그리고 로그 디렉터리가 대상이다.
적재에 실패한 작업은 중간 산출물을 정리하지 못하고 남기므로 가끔 들여다본다. 로그는 날짜별로 갈리며
`log.backup_count` 만큼만 남는다.

이력 DB 도 계속 자란다. `history.db_dsn` 을 설정했다면 `job_history` 와 `task_history` 가 쌓이는데
앱이 지우지 않으므로 보존 기간을 정해 오래된 행을 지우는 일은 운영 쪽에서 한다. 메트릭 테이블도
마찬가지다.

S3 를 쓴다면 `s3.delete_on_cleanup` 이 꺼져 있는지 확인한다. 꺼져 있으면 객체가 계속 남으므로,
보관이 필요해서 껐다면 수명주기 정책을 따로 거는 편이 낫다. 남은 것을 눈으로 확인하고 정리하는 데는
도구가 그대로 쓸모 있다.

```bash
bin/s3-ops ls    s3://dw-stage/dqe-stage/ --dirs
bin/s3-ops ls    s3://dw-stage/dqe-stage/ --older-than 7d
bin/s3-ops rmdir s3://dw-stage/dqe-stage/ --older-than 7d --yes
```

## 업그레이드할 때

새 버전을 설치해도 `config/` 와 `templates/`, `customs/` 는 덮이지 않는다. 운영자가 손으로 넣은 값과
직접 추가한 템플릿, 인증서를 지우지 않기 위해서인데, 그래서 새 버전이 추가하거나 바꾼 기본값과 예제도
저절로 들어오지 않는다. 직접 옮겨야 한다.

```bash
NEW=<새-버전-소스-트리>
CONF=/data1/distributed-query-executor/config

# 1) 무엇이 달라졌는지 본다(새로 생긴 설정 키 확인)
diff -u "$CONF/config.properties" "$NEW/config/config.properties"
diff -u "$CONF/config.yml"        "$NEW/config/config.yml"

# 2) config.yml 과 스키마는 새 버전으로 교체(백업 후)
sudo -u gpadmin cp -a "$CONF/config.yml" "$CONF/config.yml.bak"
sudo -u gpadmin cp -a "$NEW/config/config.yml" "$CONF/config.yml"
sudo -u gpadmin cp -a "$NEW/config/"*.sql "$CONF/"

# 3) 1번 diff 에서 확인한 새 키만 config.properties 에 손으로 추가한다
sudo -u gpadmin vi "$CONF/config.properties"
```

여기서 놓치기 쉬운 것이 `config.yml` 이다. 이 파일은 값이 아니라 `${변수:기본값}` 자리를 담은
구조라서, 자리가 없으면 `config.properties` 에 값을 적어도 조용히 무시된다. 새 설정을 쓰려면 반드시
교체해야 한다. 운영자가 채운 값은 properties 쪽에 있으므로 교체해도 안전하고, 혹시 config.yml 을
직접 고쳤다면 백업본에서 확인해 옮긴다.

교체한 뒤 새 항목이 실제로 들어왔는지는 `bin/config-tui.sh` 로 확인하는 것이 빠르다. 항목 목록을
`config.yml` 에서 자동으로 읽으므로 새 설정이 보이면 반영된 것이다. 반영이 끝나면 서비스를
재기동한다. 메타 테이블을 바꾸는 버전이라면 `postgresql.sql` 과 `warehousepg.sql` 중 쓰는 쪽을 다시
적용해야 한다는 점도 잊지 않는다.

## 바꾼 뒤 확인하기

의존성을 올리거나 코드를 고쳤다면 테스트를 돌린다. 실제 DB 없이 MockBackend 와 가짜 커서·가짜 S3
클라이언트로 돌기 때문에 운영 장비에서도 안전하다.

```bash
.venv/bin/python -m pytest -q
```

## 끝으로 기억해 둘 것

운영 내내 바탕에 깔아 두면 좋은 원칙이 있다. coordinator 와 executor 는 모두 상태를 프로세스
메모리에 두므로 인스턴스마다 단일 워커로 실행해야 하고, 그래서 처리량을 늘릴 때 손대는 것은 워커
수가 아니라 언제나 executor 인스턴스 수다. 여기에서 자연스럽게 따라오는 것이 다음 원칙인데,
coordinator 를 여러 대 띄우려면 작업 저장소와 이력을 반드시 공유 PostgreSQL 로 외부화해야 한다.
메모리에만 상태를 두는 기본 구성으로 여러 대를 세우면 작업을 접수한 인스턴스만 그것을 알기 때문에,
사용자가 자기 작업을 조회하지 못하는 일이 생긴다.

설정 한 벌이 서비스와 도구를 함께 먹인다는 점도 기억해 둔다. `greenplum.dsn` 하나를 고치면 executor
의 적재 경로와 `bin/gp-shell` 이 함께 따라오므로, 접속 정보를 두 곳에 적어 두고 한쪽만 고쳐 어긋나는
사고가 없다. 반대로 말하면 그 한 줄을 잘못 고치면 양쪽이 함께 멈춘다.

마지막으로 시험해 볼 일이 있을 때는 executor 프로세스를 따로 띄우지 않아도 된다.
`coordinator.executor_mode=local` 로 두면 coordinator 안에서 백엔드를 직접 실행하므로, 설정이나
쿼리가 의도대로 도는지만 확인할 때 편하다.
