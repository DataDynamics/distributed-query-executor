# 운영자 가이드 (일상 운영과 장애 대응)

이 문서는 분산 쿼리 실행기를 돌보는 사람을 위한 것이다. 날마다 무엇을 보고, 느려지거나 실패했을 때
어디부터 뒤지고, 용량이 모자랄 때 무엇을 어떤 근거로 조절하는지를 다룬다. 다른 문서를 오가지 않아도
되도록 설정 항목과 사이징 기준까지 여기에 모두 담았다.

급한 상황이라면 바로 해당 절로 간다. 작업이 429 로 거절되고 있으면 "작업이 밀릴 때"를, 이관이 너무
느리면 "느릴 때"를, 실패한 작업을 좇고 있으면 "실패를 추적할 때"를 본다. 평상시라면 "매일 보는
것"부터 읽으면 된다.

---

## 무엇이 어떻게 돌아가는가

운영 판단을 하려면 구조를 한 문단쯤은 알고 있어야 한다.

coordinator 한 대가 요청을 받아 SQL 을 조각으로 나누고 executor 여러 대에 나눠 준다. 여기서 중요한
것은 데이터가 coordinator 를 지나가지 않는다는 점이다. executor 가 소스에서 읽어 Greenplum 으로
곧장 보내고 coordinator 로는 상태와 행 수만 올라온다. 그래서 coordinator 가 병목이 되는 일은 드물고,
처리량을 늘릴 때 손대는 것은 거의 언제나 executor 쪽이다.

두 서비스 모두 상태를 프로세스 메모리에 들고 있어 단일 워커로 돈다. 워커 수를 늘리는 방식의 확장은
하지 않으며, 늘릴 것은 executor 인스턴스 수다.

과부하 방어는 세 층으로 겹겹이 이루어진다. 첫째 층은 작업 단위 입구 통제다. 동시에 RUNNING 일 수
있는 작업 수를 실행 슬롯으로 제한하고, 슬롯이 차면 대기 큐에 세우며, 실행과 대기의 합마저 넘으면
429 로 거절한다. 둘째 층은 한 coordinator 가 모든 작업을 통틀어 동시에 띄우는 조각 수의 상한이고,
셋째 층은 executor 한 대가 동시에 실행하는 조각 수의 상한이다. 이 셋이 각각 어떤 설정에 대응하는지는
뒤의 "동시성과 용량"에서 다룬다.

---

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
나눠 받으므로 작업은 계속되지만 그만큼 용량이 줄어 있다.

다음으로 작업이 쌓이고 있지 않은지 본다. `jobs.running` 이 실행 슬롯에 붙어 있고 대기가 함께 늘고
있으면 곧 429 가 나기 시작한다.

마지막으로 디스크가 남아 있는지 본다. `local_stage` 나 `s3_stage` 를 쓴다면 CSV 가 잠시 쌓이는데,
적재에 실패한 작업은 그것을 정리하지 못하고 남길 수 있으므로 스테이징 경로를 가끔 들여다본다.

---

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

모든 로그 줄에는 작업과 조각 식별자가 붙는다. 그래서 사용자가 `job_id` 를 알려 주면 그것만으로
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

## 설정 바꾸기

설정은 두 파일로 나뉜다. 값은 `config.properties` 에 자바 스타일 `key=value` 로 적고, 그 값이
`config.yml` 의 `${변수:기본값}` 자리표시자를 채워 최종 설정이 된다. 설정 디렉터리는 기본적으로
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

### 처음에 채우는 항목

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
# TLS CA 인증서 배치(파일명은 임의 — config.properties 의 impala.ca_cert 와 일치시킬 것)
sudo cp impala-ca.pem /data1/distributed-query-executor/config/impala-ca.pem
sudo chown -R gpadmin:gpadmin /data1/distributed-query-executor/config
```

`impala.auth_mechanism=LDAP` 이 기본값이므로 `impala.user` 와 `impala.password` 에 LDAP 바인드
자격증명만 채우면 되고, 비밀번호 보호를 위해 TLS 를 함께 쓰는 것을 권한다. impyla 의 SASL 빌드에는
시스템 패키지가 필요하므로 `cyrus-sasl-devel`·`gcc`·`gcc-c++`·`make`·`python3-devel` 이 깔려 있어야
하고, executor 드라이버는 `requirements-executor.txt` 로 설치한다.

---

## 동시성과 용량

값을 얼마로 잡을지는 이 절 하나로 판단할 수 있다.

### 천장은 coordinator 가 아니라 다운스트림이다

전체 처리량의 천장은 coordinator 가 아니라 그 뒤의 Impala 와 Greenplum 이 정한다. 동시에 처리할 수
있는 조각 수의 실효 상한은 다음 세 값 중 가장 작은 것이다.

```
유효 동시 task ≈ min(
    Σ executor.max_concurrent_tasks,      (= executor 수 × executor 당 동시 task)
    Greenplum 이 견디는 동시 COPY 세션 수,
    Impala 동시 쿼리 슬롯(REQUEST_POOL 한도)
)
```

executor 를 아무리 많이 띄워도 Greenplum 의 동시 COPY 세션이나 Impala 의 쿼리 슬롯이 더 작으면
거기서 막힌다. 그래서 순서를 거꾸로 잡는다. 먼저 다운스트림이 안전하게 견디는 한도를 확정하고, 그
한도를 executor 풀에 나누어 분배한 뒤, coordinator 쪽 디스패치 상한은 그보다 넉넉히 크게 잡아
스스로 병목이 되지 않게 한다.

### 항목별로 정하는 기준

`executor.max_concurrent_tasks` 는 노드 한 대 기준으로 대략 코어 수와, 안전한 GP 동시 COPY 를
executor 수로 나눈 값과, 메모리를 조각 하나가 쓰는 메모리로 나눈 값 중 가장 작은 것으로 잡는다.
조각 하나는 소스 커넥션 하나와 Greenplum 커넥션 하나, 그리고 `copy.batch_size` 만큼의 버퍼를 쓴다.
메모리가 빡빡하면 이 값을 가장 먼저 줄인다.

`greenplum.pool_max` 는 executor 가 재사용하는 GP 커넥션 풀의 크기로, Greenplum 의
`max_connections` 를 직접 보호하는 손잡이다. 기본값 0이면 풀 크기가 `executor.max_concurrent_tasks`
와 같아져 동시 조각 하나당 연결 하나가 된다. 클러스터 전체의 동시 GP 연결은 이 값을 executor 수만큼
곱한 것이므로, 그 합이 Greenplum 이 허용하는 세션 수를 넘지 않게 잡는다. 동시 조각 수보다 작게 두면
조각이 연결을 기다리며 처리량만 깎이고, 크게 둬 봐야 동시 조각 수가 천장이라 의미가 없다.

`coordinator.max_dispatch_concurrency` 는 모든 executor 의 동시 조각 수 합 이상으로 둔다(기본 32).
너무 작으면 executor 가 노는데도 조각을 충분히 띄우지 못해 오히려 coordinator 가 병목이 된다. 이
값은 0으로 두면 안 된다. 세마포어가 0이 되어 디스패치가 영원히 멈춘다.

`coordinator.max_concurrent_jobs` 와 `max_pending_jobs` 는 입구 보호용이다. 동시 작업 수에 평균 분할
조각 수를 곱한 값이 앞서 구한 유효 동시 조각 수를 크게 넘지 않게 잡는다. 대기 큐는 잠깐 몰리는
요청을 흡수하는 완충이며, 길수록 429 거절은 줄지만 대기 지연이 늘어 오래된 요청이 쌓인다. 다만 0으로
두면 완충이 아예 사라져 슬롯을 넘는 요청이 곧바로 429 가 된다는 점에 주의한다. coordinator 를 여러
대 두면 이 값들이 인스턴스마다 따로 적용되므로 인스턴스 수만큼 나눠 총량을 맞춘다.

`copy.batch_size` 는 처리량과 메모리의 줄다리기다. 행이 넓거나 메모리가 빠듯하면 2000에서 5000
사이로 낮추고, 좁고 넉넉하면 20000 이상으로 올린다. 파이프라인을 쓴다면 `copy.queue_size` 를 곱한
만큼이 조각 하나가 메모리에 들고 있는 최대 행 수다.

`coordinator.task_timeout_s` 는 가장 큰 단일 조각의 예상 실행 시간에 여유를 더해 잡는다. 너무 짧으면
정상 조각이 타임아웃으로 실패하고 너무 길면 멈춘 조각을 뒤늦게 감지한다. 반면
`task_connect_timeout_s` 는 연결을 맺는 순간만의 타임아웃이라 짧게(기본 5초) 둘수록 죽은 노드를 빨리
걸러 failover 를 앞당긴다.

한 가지 예외가 있다. 커서 없는 커스텀 API 를 소스로 쓰는 경우 메모리 특성이 다르다. Impala 커서
경로는 진짜 스트리밍이라 메모리가 배치 크기에 묶이지만, 커스텀 API 가 결과를 한 번에 돌려주면 조각
하나의 결과 전체가 executor 메모리에 올라간다. 이때는 `parallelism` 을 늘려 조각당 파티션을 잘게
쪼개는 것이 1차 완화책이고, 근본 해결은 그 API 에 페이징을 넣어 청크를 넘겨주게 하는 것이다.
프레임워크는 이미 청크 형태를 받으므로 코드 수정 없이 스트리밍으로 전환된다.

### 튜닝하는 순서

다운스트림의 안전 한도부터 확정한다. 그다음 executor 수와 executor 당 동시 조각 수의 곱이 그 한도에
맞도록 분배하고, 디스패치 상한을 그 합 이상으로 두며, 입구 값으로 과부하를 막는다. 그 상태에서 부하를
걸고 메트릭을 보며 병목 지점을 찾아 값을 조정한다.

여기서 반드시 지킬 것은 한 번에 한 값씩 바꾸는 것이다. 여러 값을 동시에 바꾸면 어떤 변경이 효과를
냈는지 알 수 없어 튜닝이 미궁에 빠진다.

---

## 작업이 밀릴 때

사용자가 `429` 를 본다는 것은 실행 슬롯과 대기 큐가 모두 찼다는 뜻이다. 고장이 아니라 설계된
방어선이므로, 먼저 어느 층이 좁은지 가려낸다.

```bash
curl -s localhost:8088/cluster    # jobs.running / jobs.active 와 executor 부하를 함께 본다
```

executor 는 한가한데 429 가 난다면 입구가 좁은 것이다. `coordinator.max_concurrent_jobs` 로 동시 실행
슬롯을, `coordinator.max_pending_jobs` 로 대기 큐를 올린다.

executor 가 이미 포화라면 입구만 넓혀 봐야 대기만 길어진다. 이때는 executor 를 늘리거나
`executor.max_concurrent_tasks` 를 올리는데, 후자는 앞 절의 기준대로 소스와 대상의 여력을 함께 봐야
한다.

디스패치가 병목일 수도 있다. `coordinator.max_dispatch_concurrency` 가 플릿 전체 용량, 그러니까
executor 수에 동시 조각 수를 곱한 값보다 작으면 executor 가 놀아도 조각이 나가지 못한다. 설정 TUI 의
동시성 탭이 이 어긋남을 경고로 짚어 준다.

---

## 느릴 때

먼저 어디가 느린지 가른다. 소스에서 못 읽고 있는지, 대상에 못 넣고 있는지, 그 사이가 막혔는지를
나눠야 손댈 곳이 정해진다.

executor 상세에서 조각이 `READING` 에 오래 머물면 소스 쪽이고 `WRITING` 이면 Greenplum 쪽이다.
대시보드나 `GET /executors/{idx}/metrics` 로 볼 수 있고, 로그의 SQL 기록에서 `datasource` 를 봐도 같은
판단을 할 수 있다.

### 조각 하나를 네 갈래로 쪼개 보기

더 정확히 짚으려면 대시보드의 단계 타임라인과 `task_history` 테이블이 벽시계를 네 갈래로 나눠 준다.
`read_wait_ms` 는 소스에서 결과를 읽는 순수 시간이고, `read_starve_ms` 는 쓰는 쪽이 다음 배치를
기다린 시간이며, `write_wait_ms` 는 인코딩과 송신에 쓴 시간, `finalize_wait_ms` 는 COPY 가 서버에서
끝나기를 기다린 시간이다. 파이프라인 모드에서 벽시계는 대략 뒤의 세 항의 합이므로 그중 가장 큰 것이
곧 병목이다.

`read_starve` 가 지배적이면 소스가 느린 것이다. `parallelism` 을 늘려 여러 executor 가 서로 다른
파티션을 동시에 읽게 하는 것이 가장 효과가 크고, 이어서 `copy.batch_size` 를 올려 왕복을 줄이거나
`impala.query_options` 로 전용 풀과 메모리 상한을 조정한다. 이관이 Impala 의 다른 작업과 자원을
다투고 있다면 이 옵션으로 서로 밀어내지 않게 할 수 있다.

`write_wait` 가 지배적이면 executor 쪽의 인코딩과 전송이 병목이다. `copy.format=binary` 로 텍스트
인코딩 CPU 를 줄이고(타입 해석에 실패하면 자동으로 text 로 되돌아간다), executor 와 GP 사이 네트워크를
점검한 뒤 `copy.batch_size` 를 올린다.

`finalize_wait` 가 지배적이면 Greenplum 의 COPY 처리가 병목이다. 한 스트림이 마스터로 몰리는 구조라
`parallelism` 을 늘려 여러 executor 가 동시에 COPY 하게 하는 것이 가장 효과적이고, 동시 GP 연결은
`greenplum.pool_max` 로 조절한다. 대상 테이블의 인덱스와 트리거, 분산키도 함께 재검토한다.

원인을 격리하고 싶으면 `copy.pipeline=false` 로 잠깐 꺼 본다. 읽기와 쓰기가 직렬로 돌아
`read_wait` 와 `write_wait` 가 순수 벽시계로 나뉘므로 비교하기 쉽다. `read_starve` 와 `write_wait` 가
비슷하다면 이미 파이프라인이 잘 겹치는 상태이므로 다음 수는 수평 확장이다.

또한 GP 연결이 모자라지 않은지도 확인한다. `greenplum.pool_max` 가 동시 조각 수보다 작으면 조각마다
연결을 기다린다. 0으로 두면 동시 조각 수를 따라가므로 대개 0이 정답이다.

### 경로 자체를 바꿔야 할 때

파이프라인과 배치 크기, 수평 확장을 다 해도 `finalize_wait` 가 계속 지배적이라면 병목은 COPY 가
Greenplum 마스터 한 노드로 몰리는 구조 자체다. executor 를 아무리 늘려도 각자 마스터로 COPY 하므로
마스터가 최종 천장이 된다. 이때는 데이터 평면을 "우리가 밀어넣기"에서 "GP 가 당겨오기"로 바꾼다.

`local_stage` 나 `s3_stage` 로 옮기면 Greenplum 의 모든 세그먼트가 파일을 나눠 동시에 읽으므로 단일
소켓 병목이 사라진다. 어느 쪽이 가능한지는 배치 제약이 정하는데, `local_stage` 는 executor 와 GP
세그먼트가 같은 호스트에 있어야 하고 `s3_stage` 는 그 제약이 없는 대신 버킷과 PXF 설정이 필요하다.

세 번째 길도 있다. `exec_mode=statement` 로 두고 PXF 외부테이블을 읽는 `INSERT … SELECT` 를 넘기면
COPY 도 executor 를 통한 스트리밍도 전혀 없이 GP 가 스스로 읽는다. 이 방식은 코드를 한 줄도 고치지
않고 시험해 볼 수 있으므로, 먼저 파일럿으로 기존 경로와 처리량을 비교해 보는 편이 좋다. 다만 PXF 를
설치하고 구성해야 하고 모든 GP 세그먼트가 원본 저장소에 직접 도달해야 하므로, 망분리 환경에서는
방화벽과 라우팅이 실제 관문이 된다.

---

## 실패를 추적할 때

사용자가 `job_id` 를 들고 오면 순서는 이렇다.

먼저 작업 상태를 본다.

```bash
curl -s localhost:8088/jobs/$JOB_ID | python3 -m json.tool
```

`error` 에 이유가 있고 `tasks` 배열에서 어느 조각이 어느 executor 에서 실패했는지, 몇 번 재시도됐는지
보인다. `PARTIAL` 이면 일부만 들어간 것이므로 사용자에게 `POST /jobs/{id}/retry` 를 안내한다. 이미
성공한 조각은 건너뛰므로 중복 적재 걱정은 없다.

그다음 그 식별자로 로그를 모은다.

```bash
grep "$JOB_ID" $L/query-coordinator-server.log $L/query-executor-server-*.log | less
```

실행한 SQL 이 함께 남아 있으므로 소스 쿼리가 실패했는지 적재 문장이 실패했는지가 드러난다.

마지막으로 실제 엔진에 직접 물어본다. 로그의 SQL 을 그대로 손으로 실행해 보는 것이 가장 확실하다.

```bash
bin/impala-shell        # 소스 쪽 확인
bin/gp-shell            # 대상 쪽 확인 — 테이블이 있는지, 권한이 있는지
```

`s3_stage` 를 쓴다면 중간 산출물이 남아 있는지도 확인한다.

```bash
bin/s3-ops ls s3://<버킷>/<프리픽스>/$JOB_ID/
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
`greenplum.dsn` 이 비어 있으면 실제로는 아무것도 읽고 쓰지 않는 백엔드로 기동하는데, 기동할 때 경고
로그가 남으므로 `*-warn.log` 에서 바로 찾을 수 있다.

```bash
grep MockBackend $L/query-executor-server-*-warn.log
# greenplum.dsn 미설정 → MockBackend 사용
```

`impala.host` 만 비어 있는 경우는 다르다. 이때는 실제 백엔드로 뜨되 소스를 읽을 수 없어 `statement`
모드만 동작하며, 기동 로그에 `impala=(미설정 → statement 모드만)` 으로 남는다.

### local_stage 와 s3_stage 의 실패

이 두 모드는 2단계로 돌기 때문에 실패 지점이 더 나뉜다.

`local_stage` 에서 "파일 예산 초과"가 뜨면 요청의 `parallelism` 이 호스트별 세그먼트 수의 합보다 크다는
뜻이다. 값을 낮추거나 executor 호스트를 늘린다. "gp_segment_configuration 에 없습니다"는
`executor.gp_hostname` 이 실제 세그먼트 호스트명과 다르다는 뜻이므로, executor 의 `/metrics` 가
보고하는 값과 `SELECT DISTINCT hostname FROM gp_segment_configuration` 결과를 대조한다. Phase 2 에서
파일을 못 읽는다면 세그먼트 호스트에서 그 파일이 실제로 있는지, GP 세그먼트 프로세스가 읽을 권한이
있는지, `stage.local_dir` 이 모든 호스트에 같은 경로로 있는지 확인한다. CSV 파싱이 어긋난다면 데이터에
구분자로 쓰는 문자가 들어 있을 수 있으므로 `stage.csv_delimiter` 를 데이터에 없는 문자로 바꾼다.

`s3_stage` 에서 Phase 1 업로드가 실패하면 자격증명과 엔드포인트를 본다. `s3.bucket` 이 아예 설정되지
않았다면 그 취지의 예외가 난다. Phase 2 에서 실패하면 PXF SERVER 와 프로파일, GP 쪽 권한을 확인한다.
이때 S3 객체는 남아 있으므로 원인을 고친 뒤 재실행할 수 있다.

두 모드 모두 디버깅 중에는 정리를 꺼 두면 중간 산출물을 들여다볼 수 있다. `stage.cleanup=false` 또는
`s3.delete_on_cleanup=false` 로 두되, 끝난 뒤 되돌리지 않으면 디스크와 버킷이 계속 찬다.

---

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

executor 는 SIGTERM 을 받으면 진행 중인 조각이 끝나기를 기다렸다 내려간다. 기다리는 시간은
`executor.shutdown_drain_timeout_s` 이며 기본값은 25초다. 재기동 중에 조각이 잘리는 것이 곤란하다면
평소 조각 하나가 걸리는 시간보다 넉넉히 잡아 두고, systemd 로 돌린다면 `TimeoutStopSec` 이 이보다
길어야 뜻이 있다.

기동할 때 콘솔과 로그에 찍히는 배너에는 실제로 읽은 설정 파일의 절대 경로가 나온다. 설정을 바꿨는데
반영이 안 된 것 같으면 여기부터 본다. 엉뚱한 디렉터리를 읽고 있는 경우가 많다.

기동 뒤에는 실제로 두드려 확인한다. 헬스와 메트릭을 조회하고 작은 작업을 하나 넣어 끝까지 도는지 보면
전체 경로가 살아 있다는 좋은 신호가 된다.

```bash
curl -s localhost:8088/health
curl -s localhost:8087/health
curl -s localhost:8088/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('\''2026-01-01'\'','\''2026-01-02'\'') AND region='\''KR'\''",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "parallelism": 2
}'
```

서버 밖에서 coordinator 에 접근해야 한다면 방화벽을 연다. executor 포트는 보통 같은 호스트 안의 내부
통신이라 외부로 열 필요가 없다.

```bash
sudo firewall-cmd --permanent --add-port=8088/tcp
sudo firewall-cmd --reload
```

---

## executor 늘리고 줄이기

처리량 확장은 executor 인스턴스 수로 한다. 순서가 중요하다.

먼저 `coordinator.executors` 에 새 URL 을 추가한다. 이 목록에 없으면 프로세스를 띄워도 coordinator
가 일을 주지 않는다. 그다음 새 인스턴스를 띄운다.

```bash
sudo -u gpadmin $B/start-executor.sh 8003
# 또는 전체를 한 번에: EXECUTOR_PORTS="8087 8086 8003" $B/start-executor.sh
```

executor 목록은 기동할 때 읽으므로 coordinator 를 재기동한다. 마지막으로
`curl -s localhost:8088/cluster` 에서 새 executor 가 healthy 로 잡히는지 확인한다.

줄일 때는 역순이다. `coordinator.executors` 에서 빼고 coordinator 를 재기동해 새 조각이 가지 않게 한
뒤, 그 executor 에서 돌던 조각이 끝나기를 기다렸다 내린다.

늘린 뒤에는 함께 움직여야 하는 값이 있다. executor 가 늘면 플릿 전체 용량이 커지므로
`coordinator.max_dispatch_concurrency` 가 그보다 작지 않은지 보고, Greenplum 의 `max_connections` 가
executor 수에 `pool_max` 를 곱한 만큼을 감당하는지 확인한다. 설정 TUI 의 동시성 탭이 이 곱셈을 풀어
보여 준다.

`local_stage` 를 쓴다면 새 executor 도 GP 세그먼트 호스트 위에 있어야 하고 `executor.gp_hostname` 을
그 호스트명과 정확히 맞춰야 한다. 또한 `stage.local_dir` 이 모든 세그먼트 호스트에 같은 경로로
존재하고, executor 프로세스가 쓸 수 있으며 GP 세그먼트 프로세스가 읽을 수 있어야 한다.

---

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

이력은 두 계층으로 나뉜다. 작업의 시작과 종료는 coordinator 가 `job_history` 에 남기고, 각 조각의
상태 변화는 executor 가 `task_history` 에 남긴다. 조각 이력은 executor 가 직접 쓰므로 executor
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
된다. 참고로 입구 한도는 coordinator 인스턴스마다 따로 적용되므로, 여러 대를 띄우면 전체 한도가 그
수만큼 곱해진다는 점을 감안해 값을 나눠 잡는다.

### HA 타이밍 값의 순서 관계

여러 대를 띄운다면 타이밍 값들 사이에 반드시 지켜야 하는 순서가 있다. 핵심은 장애를 판정하는 임계가
생존 신호를 보내는 주기보다 넉넉히 길어야 잠깐 신호가 늦은 것을 죽음으로 오판하지 않는다는 것이다.

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
executor 성능이 고르고 조각 길이가 비슷할 때 무난하고, `least_loaded` 는 가장 한가한 곳을 고르지만
여러 coordinator 가 같은 판단을 동시에 내려 한쪽으로 몰릴 수 있다. 그래서 멀티 coordinator 에서는 두
곳만 비교해 덜 바쁜 쪽을 고르는 `p2c` 를 권한다. 배분이 실제로 쏠린다면
`coordinator.executor_reservation` 을 켜서 디스패치하는 동안 자리를 미리 잡아 둔다.

---

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
똑같이 적용한다.

```bash
psql "postgresql://user:pass@pg-host:5432/monitoring" -f /data1/distributed-query-executor/config/postgresql.sql

psql ... -c "SELECT recorded_at, executor_url, healthy, cpu_percent, memory_percent
             FROM executor_health_metrics ORDER BY recorded_at DESC LIMIT 20;"
```

`monitor.enabled` 를 끄면 대시보드의 executor 상태가 갱신되지 않고 `least_loaded` 나 `p2c` 선택도
판단 근거를 잃으므로, 특별한 이유가 없으면 켜 둔다.

---

## 정기적으로 살필 것

디스크부터 본다. `local_stage` 와 `s3_stage` 의 스테이징 경로, 그리고 로그 디렉터리가 대상이다.
적재에 실패한 작업은 중간 산출물을 정리하지 못하고 남기므로 가끔 들여다본다. 로그는 날짜별로 갈리며
`log.backup_count` 만큼만 남는다.

이력 DB 도 계속 자란다. `history.db_dsn` 을 설정했다면 `job_history` 와 `task_history` 가 쌓이는데
앱이 지우지 않으므로 보존 기간을 정해 오래된 행을 지우는 일은 운영 쪽에서 한다. 메트릭 테이블도
마찬가지다.

S3 를 쓴다면 `s3.delete_on_cleanup` 이 꺼져 있는지 확인한다. 꺼져 있으면 객체가 계속 남으므로,
보관이 필요해서 껐다면 수명주기 정책을 따로 거는 편이 낫다.

---

## 업그레이드할 때

새 버전을 설치해도 `config/` 와 `templates/`, `customs/` 는 덮이지 않는다. 운영자가 손으로 넣은 값과
직접 추가한 템플릿, 인증서를 지우지 않기 위해서인데, 그래서 새 버전이 추가하거나 바꾼 기본값과 예제도
저절로 들어오지 않는다. 직접 옮겨야 한다.

무엇이 달라졌는지 먼저 확인하고, `config.yml` 과 스키마는 새 버전으로 교체하며, `config.properties`
에는 새로 생긴 키만 손으로 더한다. 운영자가 채운 값은 그대로 둔다.

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
재기동한다.

설치 전후로 필요한 것이 갖춰졌는지 확인하고 싶으면 사전 점검 스크립트를 쓴다. OS 패키지와 파이썬
휠이 준비됐는지 확인만 하고 설치는 하지 않으며, 종료 코드로도 알려 주므로 자동화에 끼워 넣을 수 있다.

```bash
./bin/check-prereqs.sh
OS_ONLY=1     ./bin/check-prereqs.sh   # OS 패키지만
WHEELS_ONLY=1 ./bin/check-prereqs.sh   # 휠만
```

---

## 끝으로 기억해 둘 것

운영 내내 바탕에 깔아 두면 좋은 원칙이 있다. coordinator 와 executor 는 모두 상태를 프로세스
메모리에 두므로 인스턴스마다 단일 워커로 실행해야 하고, 그래서 처리량을 늘릴 때 손대는 것은 워커
수가 아니라 언제나 executor 인스턴스 수다. 여기에서 자연스럽게 따라오는 것이 다음 원칙인데,
coordinator 를 여러 대 띄우려면 작업 저장소와 이력을 반드시 공유 PostgreSQL 로 외부화해야 한다.
메모리에만 상태를 두는 기본 구성으로 여러 대를 세우면 작업을 접수한 인스턴스만 그것을 알기 때문에,
사용자가 자기 작업을 조회하지 못하는 일이 생긴다.

마지막으로 시험해 볼 일이 있을 때는 executor 프로세스를 따로 띄우지 않아도 된다.
`coordinator.executor_mode=local` 로 두면 coordinator 안에서 백엔드를 직접 실행하므로, 설정이나
쿼리가 의도대로 도는지만 확인할 때 편하다.
