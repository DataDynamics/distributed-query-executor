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

coordinator 한 대가 요청을 받아 SQL 을 task 로 나누고 executor 여러 대에 나눠 준다. 여기서 중요한
것은 데이터가 coordinator 를 지나가지 않는다는 점이다. executor 가 소스에서 읽어 Greenplum 으로
곧장 보내고 coordinator 로는 상태와 행 수만 올라온다. 그래서 coordinator 가 병목이 되는 일은 드물고,
처리량을 늘릴 때 손대는 것은 거의 언제나 executor 쪽이다.

두 서비스 모두 상태를 프로세스 메모리에 들고 있어 단일 워커로 돈다. 워커 수를 늘리는 방식의 확장은
하지 않으며, 늘릴 것은 executor 인스턴스 수다.

과부하 방어는 세 층으로 겹겹이 이루어진다. 첫째 층은 작업 단위 admission control 이다. 동시에 RUNNING
일 수 있는 작업 수를 실행 슬롯으로 제한하고, 슬롯이 차면 대기 큐에 세우며, 실행과 대기의 합마저
넘으면 429 로 거절한다. 둘째 층은 한 coordinator 가 모든 작업을 통틀어 동시에 띄우는 task 수의
상한이고, 셋째 층은 executor 한 대가 동시에 실행하는 task 수의 상한이다. 이 셋이 각각 어떤 설정에
대응하는지는 뒤의 "동시성과 용량"에서 다룬다.

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
모드라면 각 executor 도 `http://<host>:8087/` 처럼 자기 화면을 준다. 두 대시보드의 "API·링크" 탭에는
Swagger UI·ReDoc·OpenAPI 스키마, Prometheus scrape 주소(`/metrics/prometheus`)와 http_sd 주소
(`/prometheus/targets`), 헬스·메트릭·동기화 상태 같은 운영용 주소가 복사 버튼과 함께 모여 있고, 그 아래에 이 서버의
전체 엔드포인트가 Swagger 의 해당 항목 링크와 함께 나온다. coordinator 화면에는 executor 마다 대시보드·문서·
Prometheus 주소도 나오므로 Prometheus 설정을 채울 때 여기서 복사하면 된다. 헤더 오른쪽의 Swagger·ReDoc·
Prometheus 링크는 어느 탭에서나 보인다. 터미널만 있다면 읽기 전용 모니터를 띄운다.

```bash
bin/dashboard-tui.sh                                  # 설정에서 coordinator 주소를 유추한다
bin/dashboard-tui.sh --url http://host:8088 --interval 5
```

모니터는 스스로 갱신하는데, 목록을 읽는 동안 화면이 바뀌어 거슬리면 스페이스로 잠시 세우고 `+` 와
`-` 로 주기를 바꾼다. 상태 줄의 갱신 시각을 보면 지금 화면이 언제 것인지 알 수 있으므로, 화면이
그대로일 때 멈춘 것인지 조용한 것인지 구분된다. `Enter` 로 job 이나 executor 상세로 들어가고 `ESC`
로 나온다. Jobs 탭의 job 표 아래에는 모든 executor 의 처리중 query-run(결과 반환 실행)이, History
탭의 job 이력 아래에는 query-run 이력이 함께 나온다. query-run 행에서 `Enter` 를 누르면 그것을
실행 중인 executor 의 상세로 들어가고, executor 상세의 task 표에는 실행 방식(`mode`) 열이 있어
이관 task 와 `query_run` 을 구분할 수 있다.

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

모든 로그 줄에는 작업과 task 식별자가 붙는다. 그래서 사용자가 `job_id` 를 알려 주면 그것만으로
관련된 줄을 전부 모을 수 있고, coordinator 와 executor 의 로그를 같은 식별자로 이어 볼 수 있다.

```bash
grep 'a1b2c3d4' $L/query-coordinator-server.log $L/query-executor-server-*.log
```

`/query-execute`·`/query-count` 처럼 작업을 만들지 않는 요청은 작업 식별자 자리에 `req-<요청 식별자>`
가 찍힌다. 요청 식별자는 응답 헤더 `X-Request-ID` 로 클라이언트에 돌아가고, coordinator 가 executor
를 부를 때도 같은 값을 실어 보낸다. 사용자가 이 헤더 값을 알려 주면 그 요청이 두 서비스에 남긴 로그를
한 번에 모을 수 있다.

```bash
grep -A40 'req-ce5ee974149a' $L/query-coordinator-server.log $L/query-executor-server-*.log
```

SQL 과 HTTP 로그는 레코드 하나가 여러 줄이므로 `grep` 에 `-A` 를 붙여 뒤따르는 줄까지 본다.

실행 SQL은 `logging.sql.enabled=true`일 때 INFO로 기록된다. 로거와 핸들러의 레벨을
WARNING 이상으로 높이면 기록되지 않으므로, SQL 추적이 필요하면 INFO 이하를 유지한다. 기본 형식
(`logging.sql.format=multiline`)은 머리줄 아래에 SQL 원문을 줄바꿈과 들여쓰기 그대로, 줄마다
`    | ` 를 붙여 남긴다.

```
SQL 실행 datasource=impala phase=SOURCE_SELECT (4줄)
    | SELECT user_id, amount, region, dt
    | FROM sales
    | WHERE dt IN ('2026-01-01')
    |   AND region IN ( 'KR' )
```

coordinator 는 템플릿을 렌더한 직후에도 어떤 파라미터로 어떤 SQL 이 만들어졌는지를 조각별로 남긴다.
검증에 걸려 422 가 나간 요청도 이 기록이 남으므로, 사용자가 "요청이 거절됐다" 고 할 때 실제로 만들어진
SQL 을 확인할 수 있다.

```
템플릿 렌더 template=order_count params={'regions': ['KR'], ...} (datasource=trino)
  [select]
    | SELECT o.order_id,
    | ...
  [count]
    | SELECT COUNT(*) AS row_count FROM (
    | ...
```

`datasource` 로 어느 엔진에 던진 문장인지 갈리므로 소스에서 못 읽은 것인지 대상에 못 넣은 것인지가
한눈에 구분된다. Trino 로 읽을 줄 알았던 쿼리가 Impala 로 나간 것 같은 사고도 이 표시 하나로
판별된다. `logging.sql.max_length`(기본 20000자)를 넘는 SQL 은 잘리는데, 잘린 경우에는 전문이
아니라는 표시가 함께 남는다. 로그를 한 줄 단위로 처리하는 도구를 붙인다면 `logging.sql.format=oneline`
으로 바꿔 예전처럼 한 줄로 접는다.

HTTP 요청과 응답은 기본 INFO 에서도 API 호출(POST 등)과 실패 응답(400 이상)이 남는다. 요청 하나가
머리줄(메서드·경로·상태·소요시간·`req_id`)과 요청·응답 본문을 담은 레코드 하나가 되며, 본문은
마스킹을 거쳐 `logging.http.max_body`(기본 8192바이트)에서 잘린다. 성공한 조회(GET 등)는 대시보드
폴링이 몇 초마다 들어오므로 `logging.http.read_level`(기본 DEBUG)로만 남는다. 조회까지 따라가야 하면
이 값을 INFO 로 올리고 문제를 좇은 뒤 되돌린다.

---

## 설정 바꾸기

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

### ceiling 은 coordinator 가 아니라 downstream 이다

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

### 항목별로 정하는 기준

`executor.max_concurrent_tasks` 는 노드 한 대 기준으로 대략 코어 수와, 안전한 GP 동시 COPY 를
executor 수로 나눈 값과, 메모리를 task 하나가 쓰는 메모리로 나눈 값 중 가장 작은 것으로 잡는다.
task 하나는 소스 커넥션 하나와 Greenplum 커넥션 하나, 그리고 `copy.batch_size` 만큼의 버퍼를 쓴다.
메모리가 빡빡하면 이 값을 가장 먼저 줄인다. `POST /query-execute`·`/query-count` 가 executor 에
위임하는 결과 반환 실행(`/query-run`)도 이 슬롯을 함께 쓴다. executor 의 처리중 목록과 실행 이력에
`query_run` 실행 방식으로 나타나고(적재가 없어 적재 행수는 0, 읽은 행수가 반환 행수다), 슬롯이 다 차면
QUEUED 로 기다리므로 미리보기 응답이 늦어지면 이관 task 와 슬롯을 다투고 있는지 먼저 본다.
coordinator 대시보드에서도 보인다. "처리중인 Query" 탭의 job 표 아래 "결과 반환 실행(query-run)
처리중" 표가 모든 executor 의 진행 중 query-run 을 모아 보여 주고(어느 executor 가 실행 중인지, 실행 ID
를 누르면 SQL), "실행 이력" 탭의 job 이력 아래 "결과 반환 실행(query-run) 이력" 표가 이력 DB 의
task_history 에서 읽는다. 이력 표의 요청 ID(`req-<id>`)는 coordinator 로그의 요청 식별자와 같아 그
값으로 양쪽 로그를 함께 찾을 수 있다.

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

### 튜닝하는 순서

downstream 의 안전 한도부터 확정한다. 그다음 executor 수와 executor 당 동시 task 수의 곱이 그 한도에
맞도록 분배하고, 디스패치 상한을 그 합 이상으로 두며, admission 값으로 과부하를 막는다. 그 상태에서
부하를 걸고 메트릭을 보며 병목 지점을 찾아 값을 조정한다.

여기서 반드시 지킬 것은 한 번에 한 값씩 바꾸는 것이다. 여러 값을 동시에 바꾸면 어떤 변경이 효과를
냈는지 알 수 없어 튜닝이 미궁에 빠진다.

### 부하를 걸어 한도 확인하기(`bin/load-test`)

부하는 `bin/load-test` 로 건다. 가상 사용자(VU)를 ramp-up 으로 늘려 가며 coordinator API 를
부르는 도구다. VU 하나는 요청을 보내고 그 요청이 **완료될 때까지 기다린 뒤** 다음 요청을 보내므로,
동시에 진행 중인 요청 수는 VU 수를 넘지 않고 VU 를 늘려 가며 어디서 포화되는지 볼 수 있다.

무엇을 완료로 보는지는 요청의 **type** 이 정한다.

- **`async`** 는 `POST /jobs` 처럼 202 로 바로 돌아오고 실제 처리는 뒤에서 진행되는 요청이다.
  도구는 접수 응답의 `job_id` 로 `GET /jobs/{id}/status` 를 폴링해 DONE·PARTIAL·FAILED·CANCELLED
  중 하나가 되면 완료로 보고, 그중 DONE 만 성공으로 센다.
- **`sync`** 는 `POST /query-execute` 처럼 응답이 곧 결과인 요청이다. 응답을 받는 순간 완료이고,
  성공 코드(기본 2xx, `--success-status 200` 처럼 좁힐 수 있다)가 아니면 오류로 센다. `/jobs` 의
  dry_run 도 여기에 해당한다.

type 은 `--type` 으로 주고, 주지 않으면 경로가 `/jobs` 인 요청만 async 이고 나머지와 dry_run 은
sync 다. 응답을 보고 추측하지 않는 이유는 설정 실수를 드러내기 위해서다. async 로 의도한 요청이 id 없이
200 을 받으면 `missing_id` 오류로 잡힌다. 폴링 규칙(`--poll-path`·`--poll-id-field`·
`--poll-status-field`·`--poll-terminal`·`--poll-success`·`--cancel-path`)의 기본값은 `/jobs`
규칙이라 보통은 건드릴 일이 없고, 이 저장소 밖의 다른 비동기 API 를 잴 때만 바꾼다.

처음이라면 **`bin/load-test wizard`** 로 시작하는 것이 쉽다. 대상 → 요청 → 부하 곡선 → 대기 →
자원 수집 → 결과 파일 순으로 필요한 것만 묻고(sync 를 고르면 폴링 규칙은 묻지 않는다), 질문마다
Enter 로 기본값을, `?` 로 설명을 볼 수 있다. 끝에서 완성된 명령을 보여 주고 바로 실행하거나
시나리오 YAML 로 저장하거나 명령만 받아 갈 수 있다. 실행은 화면에 보인 그 명령 그대로 하므로, 한 번
받아 둔 명령은 다음부터 마법사 없이 쓰면 된다. `wizard --scenario 파일` 로 기존 시나리오의 값을 기본
답으로 채워 시작할 수도 있다.

```bash
bin/load-test wizard                       # 질문에 답하며 명령 구성

# 요청 없이 VU 곡선만 먼저 확인한다
bin/load-test plan --vus 20 --ramp-up 60s --duration 10m

# 20 VU 까지 60초에 걸쳐 늘리고 10분 동안 부하를 건다
bin/load-test run -c http://coord:8088 --body job.json \
    --vus 20 --ramp-up 60s --duration 10m --out result.json --csv timeline.csv

# 계단형 부하: 5 VU 로 2분, 20 VU 로 3분, 30초에 걸쳐 0 으로
bin/load-test run -c http://coord:8088 --body job.json --stages 30s:5,2m:5,1m:20,3m:20,30s:0

# sync: 결과 미리보기 API 에 10 VU 로 5분
bin/load-test run -c http://coord:8088 --type sync --path /query-execute --body preview.json \
    --vus 10 --ramp-up 30s --duration 5m
```

요청 본문(`--body`)은 `/jobs` 에 보내는 JSON 그대로이며, 문자열 안의 `${vu}`·`${iter}`·`${seq}`·
`${uuid}`·`${run_id}`·`${now:%Y%m%d}`·`${choice:a,b,c}`(하나)·`${choice:a,b,c|1-3}`(중복 없이 여러 개, 배열)·`${randint:1-9}`·`${date:today-7}`·
`${date:2024-01-01~2024-12-31}` 가 요청마다 치환된다. 예를 들어 `"target_table": "dwtemp.load_${vu}"`
로 VU 마다 대상 테이블을 나눌 수 있다. 템플릿 `params` 의 시작일·종료일처럼 값끼리 짝이 맞아야 하면
CSV·JSONL 데이터 파일을 `--data` 로 주고 본문에 `${data:<컬럼>}` 을 쓴다. 요청마다 행 하나를 꺼내므로
한 요청의 값들이 같은 행에서 나온다(`--data-mode sequential|random`). 자주 쓰는
옵션은 시나리오 YAML(`--scenario`)로 묶어 두고, 키 이름은 옵션에서 `--` 를 떼고 `-` 를 `_` 로 바꾼
것이다. 명령행 값이 YAML 보다 우선한다. 요청 명세는 아래처럼 `request:` 블록으로 묶어 쓸 수 있으며,
본문 경로는 시나리오 파일 기준 상대경로다.

```yaml
request:
  type: async              # async | sync
  method: POST
  path: /jobs
  body: job.json
  poll:                    # async 에서만. 생략한 항목은 /jobs 기본값
    path: /jobs/${id}/status
    id_field: job_id
    status_field: status
    terminal: [DONE, PARTIAL, FAILED, CANCELLED]
    success: [DONE]
    cancel_path: /jobs/${id}/cancel   # '' 이면 멈출 때 취소하지 않는다
vus: 20
ramp_up: 60s
duration: 10m
```

**이 도구는 실제로 적재한다.** dry_run 이 아닌 `/jobs` 요청이면 시작 전에 대상과 부하 곡선을 보여
주고 확인을 받으며(읽기 전용인 sync 요청은 묻지 않는다), 비대화형으로 돌릴 때는 `--yes` 가 있어야 시작한다. append 는 반복할 때마다 중복
적재되므로 부하용 대상 테이블을 따로 두는 것이 안전하다. executor 없이 coordinator 의 검증·분할
경로만 재고 싶으면 `--dry-run-body` 로 본문에 `dry_run=true` 를 넣는다.

**실행하는 동안**에는 화면(stderr)에 라이브 패널이 1초마다 갱신된다. 경과 시간과 현재 VU 수·진행
중 요청 수, 제출·완료 TPS(최근 10초 기준), 지연 p50/p95, 그리고 coordinator 와 각 executor 의
CPU·메모리 사용률(막대)과 실행 중 task 수를 함께 보여 주므로, 부하를 거는 중에 서버가 어디까지
차오르는지 바로 볼 수 있다. 터미널이 아니면(로그 파일·크론) 같은 내용을 한 줄 요약으로 30초마다
남긴다. 화면 출력을 끄려면 `--no-progress` 를 준다(요약과 결과 파일은 그대로 나온다).

끝나면 요약이 stdout 으로 나온다. async 결과를 읽을 때는 TPS 가 두 가지라는 점을 먼저 기억한다.
**제출 TPS** 는 초당 접수 건수이고, **완료 TPS** 는 초당 종료가 확인된 job 수로 서버가 실제로 처리한
양이다. 비동기 API 라 제출 TPS 는 서버가 받아 주기만 하면 얼마든지 높게 나오므로 용량 판단은 완료
TPS 로 한다. sync 는 응답이 곧 완료라 TPS 와 응답 지연이 하나씩만 나온다. 지연은 도구가 잰 e2e(제출부터 종료 확인까지)와, 서버 타임스탬프로 잰 대기 시간
(created→started, 실행 슬롯을 기다린 시간)과 실행 시간(started→finished)으로 나뉘어 나온다. 대기
시간이 늘기 시작하면 `max_concurrent_jobs` 에 닿은 것이다.

요약 끝에는 완료 TPS 를 시간축으로 그린 **[시간대별 추이]** 막대그래프가 붙는다. VU 수와 상관없이 모든 실행에
나오며, 막대 아래 줄이 그 시점의 VU 수이고 오류는 `!`, 429 는 `×` 로 짚는다. 워밍업 길이, 도중에 처리량이 꺼진
구간, 부하를 멈춘 뒤 남은 job 이 끝나기까지 걸린 시간을 여기서 읽는다. VU 별 처리량 곡선(`[포화 분석]`)은 VU 를
계단으로 올린 실행에서만 그려지고, `--vus 2` 처럼 VU 를 하나로 고정한 실행에서는 생략한 이유만 한 줄 나온다.

같은 화면에 서버 자원 표가 붙는다. 도구는 테스트하는 동안 `GET /cluster` 를 `--sample-interval`
(기본 5초)마다 불러 coordinator 와 모든 executor 의 CPU·메모리·실행 중 task 수를 모은다. 부하 전
`--baseline`(기본 10초) 동안 먼저 재 두므로 '기준CPU' 와 'CPU평균' 을 비교하면 부하로 얼마나
올랐는지가 보인다. 'task최대/상한' 이 상한에 붙어 있으면 executor 가 포화된 것이다. '포화 신호' 에는
처음 429 가 난 시점과 그때의 VU 수가 나오는데, 429 는 도구 입장에서 오류가 아니라 admission 한도에
닿았다는 신호라 오류율에서 빼고 따로 센다.

**도구의 '실패' 수는 coordinator 대시보드의 FAILED 수와 다를 수 있다.** 도구는 서버가 실패로 끝낸 job 뿐 아니라
제출 단계 오류와 도구 자신의 판정까지 실패로 센다. 실패가 있으면 요약에 `[실패 내역]` 이 붙어 셋으로 나눠 보여 준다.

| 분류 | 무엇 | 대시보드에서 |
|---|---|---|
| ① 서버가 실패로 끝낸 job | 서버 종료 상태가 성공(`DONE`)이 아닌 것. FAILED·PARTIAL·CANCELLED | 같은 상태로 보인다. PARTIAL·CANCELLED 는 FAILED 로 세지 않는다 |
| ② 제출 단계 오류 | `client_error`(4xx, 검증 실패 등)·`server_error`(5xx)·`conn_error`(연결 끊김·타임아웃)·`missing_id` | job 이 만들어지지 않아 없다. 단 `conn_error` 는 서버가 받았는데 응답만 늦었을 수 있어 대시보드에는 DONE 으로 남을 수 있다 |
| ③ 도구 판정 | `POLL_TIMEOUT`(`--job-timeout` 초과)·`LOST`(폴링이 연속 404) | 서버에서는 계속 돌았거나 성공했을 수 있다 |

①과 ③ 의 job id 는 요약에 몇 개, 결과 JSON 과 테스트 기록 파일의 `failures` 에 전부(분류·HTTP 상태·오류 포함, 최대
1000건) 담기므로 대시보드에서 job id 로 찾아 대조한다. `conn_error` 가 많다면 서버가 느려 제출 응답이
`--http-timeout`(async 기본 30초)을 넘긴 것이니 그 값을 늘리거나 서버 쪽 지연을 먼저 본다. 이때 도구는 실패로 세고
다음 요청을 보내므로, 서버에는 도구가 아는 것보다 많은 job 이 들어간다.

멈출 때는 Ctrl-C 를 한 번 누르면 새 제출을 멈추고 진행 중인 job 을 `--drain-timeout`(기본 5분)
동안 기다린 뒤 요약을 낸다. 두 번 누르면 기다리지 않는다. 그때까지 끝나지 않은 job 은
`--on-stop cancel`(기본)이면 취소를 보내고, `abandon` 이면 그대로 두며, 어느 쪽이든 ABANDONED 로
집계한다. 응답을 기다리던 sync 요청은 취소할 방법이 없어 연결만 끊는다. HTTP 타임아웃의 기본값도
type 마다 다르다. async 는 접수 응답만 기다리므로 30초이고(긴 대기는 `--job-timeout` 이 맡는다),
sync 는 요청 하나가 쿼리 실행 시간만큼 걸리므로 5분이며 이 값이 곧 작업 타임아웃이다. `--max-error-rate 0.05` 처럼 한도를 주면 오류율이 넘을 때 종료 코드 3 으로 끝나므로
정기 점검 스크립트에서 성능 회귀를 판정할 수 있다. 초 단위 시계열(`--csv`)과 서버 자원
표본(`--samples-csv`)은 엑셀에서 바로 열리는 CSV 이고, `--out` 의 JSON 에는 설정과 요약, 시계열,
표본이 모두 들어 있어 두 번의 실행을 나중에 비교할 수 있다.

이런 옵션을 주지 않아도 테스트가 끝날 때마다 로그 디렉터리 아래 `test_results/YYYYMMDD-HHMMSS.txt` 기록 파일이
자동으로 생긴다(이름은 테스트 시작 시각이다). 이 파일 하나에 실행한 명령, 실제로 적용된 입력 파라미터(명령행 >
시나리오 > 기본값을 합친 값), 요청 본문, 콘솔 요약 전체, `--out` 과 같은 결과 JSON 이 함께 담기므로, 지난 실행과
비교하거나 같은 조건으로 다시 돌릴 때 이것만 열면 된다. 민감 헤더와 비밀번호·토큰은 가려서 쓴다. 위치를
바꾸려면 `--result-dir`, 끄려면 `--no-result-file` 을 준다. 오래된 기록은 자동으로 지우지 않는다.

기록은 coordinator 대시보드의 **부하 테스트** 탭에서도 볼 수 있다. 최신순 표에 시작 시각·종료 코드(0 이 아니면 빨강)·
대상·시나리오·한 줄 요약·크기가 나오고, "보기"는 결과 JSON 을 뺀 본문을 팝업으로, "원본"은 파일 전체를 새 탭으로 연다.
"명령" 열의 복사 버튼으로 같은 조건의 명령을 바로 가져올 수 있다. 대시보드는 그 coordinator 호스트의 로그 디렉터리만
읽으므로, 다른 호스트에서 `bin/load-test` 를 돌렸다면 그 기록은 거기에 남고 여기에는 보이지 않는다. 같은 내용을
`GET /load-tests`(목록)와 `GET /load-tests/{파일명}`(본문)으로도 읽는다.

**포화점(무릎점)을 찾으려면** `saturation` 명령을 쓴다. VU 를 계단으로 올리며 각 수준의 정상상태
완료 TPS 를 재고, VU 를 더 얹어도 처리량이 늘지 않는 지점을 찾아 준다.

```bash
bin/load-test saturation -c http://coord:8088 --body job.json \
    --levels 1,2,4,8,16,32 --step-duration 60s --chart saturation.png
```

`--levels` 의 각 수준을 `--step-duration` 만큼 유지하며(수준이 오른 직후 과도구간은 버린다) 요약에
VU 별 TPS 표와 TPS–VU 곡선, 무릎점을 낸다. 무릎점은 이웃 수준 사이 처리량 증가율이 5% 아래로
떨어지는 첫 지점이고, 그 VU 수 × 평균 분할 task 수가 앞 절에서 구한 유효 동시 task 수와 맞는지
비교하면 admission·executor 설정이 실제 한도와 맞는지 확인할 수 있다. `--chart` 는 곡선을 PNG
(`.svg` 면 Pillow 없이)로 저장한다. 같은 분석은 `--stages`·ramp 로 여러 수준을 거친 일반 `run`
결과에도 자동으로 붙지만, 각 수준을 충분히 유지한 계단 부하라야 곡선이 안정적이다.

도구 자신도 부하의 일부라는 점에 주의한다. VU 마다 `--poll-interval`(기본 2초, ±20% 분산) 간격으로
상태를 폴링하므로 VU 가 수백이면 폴링만으로 coordinator 에 적지 않은 요청이 간다. job 이 수십 초
이상 걸리는 이관이라면 폴링 간격을 5초 이상으로 늘려도 결과가 거의 달라지지 않는다. `/cluster` 의
job 수는 이 도구가 만든 것만이 아니라 coordinator 전체의 수라는 점도 함께 기억한다.

---

## 작업이 밀릴 때

사용자가 `429` 를 본다는 것은 실행 슬롯과 대기 큐가 모두 찼다는 뜻이다. 고장이 아니라 설계된
방어선이므로, 먼저 어느 층이 좁은지 가려낸다.

```bash
curl -s localhost:8088/cluster    # jobs.running / jobs.active 와 executor 부하를 함께 본다
```

executor 는 한가한데 429 가 난다면 admission 이 좁은 것이다. `coordinator.max_concurrent_jobs` 로
동시 실행 슬롯을, `coordinator.max_pending_jobs` 로 대기 큐를 올린다.

executor 가 이미 포화라면 admission 만 넓혀 봐야 대기만 길어진다. 이때는 executor 를 늘리거나
`executor.max_concurrent_tasks` 를 올리는데, 후자는 앞 절의 기준대로 소스와 대상의 여력을 함께 봐야
한다.

디스패치가 병목일 수도 있다. `coordinator.max_dispatch_concurrency` 가 fleet 전체 용량, 그러니까
executor 수에 동시 task 수를 곱한 값보다 작으면 executor 가 놀아도 task 가 나가지 못한다. 설정 TUI 의
동시성 탭이 이 어긋남을 경고로 짚어 준다.

---

## 느릴 때

먼저 어디가 느린지 가른다. 소스에서 못 읽고 있는지, 대상에 못 넣고 있는지, 그 사이가 막혔는지를
나눠야 손댈 곳이 정해진다.

executor 상세에서 task 가 `READING` 에 오래 머물면 소스 쪽이고 `WRITING` 이면 Greenplum 쪽이다.
대시보드나 `GET /executors/{idx}/metrics` 로 볼 수 있고, 로그의 SQL 기록에서 `datasource` 를 봐도
같은 판단을 할 수 있다.

### task 하나를 네 갈래로 쪼개 보기

더 정확히 짚으려면 대시보드의 단계 타임라인과 `task_history` 테이블이 wall-clock time 을 네 갈래로
나눠 준다. `read_wait_ms` 는 소스에서 결과를 읽는 순수 시간이고, `read_starve_ms` 는 쓰는 쪽이 다음
배치를 기다린 시간이며, `write_wait_ms` 는 인코딩과 송신에 쓴 시간, `finalize_wait_ms` 는 COPY 가
서버에서 끝나기를 기다린 시간이다. 파이프라인 모드에서 wall-clock time 은 대략 뒤의 세 항의 합이므로
그중 가장 큰 것이 곧 병목이다.

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

원인을 격리하고 싶으면 `copy.pipeline=false` 로 잠깐 꺼 본다. 읽기와 쓰기가 직렬로 돌아 `read_wait`
와 `write_wait` 가 순수 wall-clock time 으로 나뉘므로 비교하기 쉽다. `read_starve` 와 `write_wait`
가 비슷하다면 이미 파이프라인이 잘 겹치는 상태이므로 다음 수는 수평 확장이다.

또한 GP 연결이 모자라지 않은지도 확인한다. `greenplum.pool_max` 가 동시 task 수보다 작으면 task 마다
연결을 기다린다. 0으로 두면 동시 task 수를 따라가므로 대개 0이 정답이다.

### 경로 자체를 바꿔야 할 때

파이프라인과 배치 크기, 수평 확장을 다 해도 `finalize_wait` 가 계속 지배적이라면 병목은 COPY 가
Greenplum 마스터 한 노드로 몰리는 구조 자체다. executor 를 아무리 늘려도 각자 마스터로 COPY 하므로
마스터가 최종 ceiling 이 된다. 이때는 data plane 을 "우리가 밀어넣기"에서 "GP 가 당겨오기"로 바꾼다.

`local_stage` 나 `s3_stage` 로 옮기면 Greenplum 의 모든 세그먼트가 파일을 나눠 동시에 읽으므로 단일
소켓 병목이 사라진다. 어느 쪽이 가능한지는 co-location 제약이 정하는데, `local_stage` 는 executor 와
GP 세그먼트가 같은 호스트에 있어야 하고 `s3_stage` 는 그 제약이 없는 대신 버킷과 PXF 설정이
필요하다.

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

`error` 에 이유가 있고 `tasks` 배열에서 어느 task 가 어느 executor 에서 실패했는지, 몇 번
재시도됐는지 보인다. `PARTIAL` 이면 일부만 들어간 것이므로 사용자에게 `POST /jobs/{id}/retry` 를
안내한다. 이미 성공한 task 는 건너뛰므로 중복 적재 걱정은 없다.

그다음 그 식별자로 로그를 모은다.

```bash
grep "$JOB_ID" $L/query-coordinator-server.log $L/query-executor-server-*.log | less
```

실행한 SQL 이 함께 남아 있으므로 소스 쿼리가 실패했는지 적재 문장이 실패했는지가 드러난다.

마지막으로 실제 엔진에 직접 물어본다. 로그의 SQL 을 그대로 손으로 실행해 보는 것이 가장 확실하다.

```bash
bin/impala-shell        # 소스 쪽 확인
bin/trino-shell         # 소스가 Trino(datasource=trino)라면 — query-execute 와 같은 실행 함수로 실행한다
bin/gp-shell            # 대상 쪽 확인 — 테이블이 있는지, 권한이 있는지
```

`bin/trino-shell` 은 Trino 접속을 따로 두지 않고 query-execute 가 쓰는 커스텀 실행 함수(`query.func.module`,
기본 `customs.query_funcs.trino_runner:run`)를 그대로 불러 쿼리를 실행한다. 그래서 대시보드 미리보기와 똑같은
경로로 확인할 수 있다. 접속 값은 `query.func.config.*` 를 읽고 `--host`·`--catalog`·`--schema`·
`--set 키=값` 으로 일부만 덮어쓴다. 셸 안에서 `USE 카탈로그.스키마` 로 대상을 바꾸고 `\dt`·`\d 테이블` 로
목록과 구조를 본다. 실행 함수가 매번 새로 접속하므로 `SET SESSION` 은 유지되지 않아 거절하며, 결과는
`--max-rows`(0 이면 제한 없음)에서 자르고 잘렸으면 알린다. `-q "SELECT ..." -o out.csv` 로 배치 실행도 된다.

`s3_stage` 를 쓴다면 중간 산출물이 남아 있는지도 확인한다.

```bash
bin/s3-ops ls s3://<버킷>/<프리픽스>/$JOB_ID/
```

### 자주 나오는 원인

가장 먼저 의심할 것은 executor 에 닿지 못한 경우다. coordinator 는 연결에 실패하면 몇 번 재시도했다가
미접수가 확실한 접속 실패·429·503에 한해 다른 executor로 넘긴다(`coordinator.task_failover`).
응답 유실이나 실행 중 연결 끊김은 재배정하지 않으므로 기존 실행을 먼저 확인한다. 반복 장애는 해당 executor의
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

executor 프로세스와 포트가 멀쩡하고 CPU·동시 task 에도 여유가 있는데 `executor ... 다운 감지` 나
`task ... 폴링 실패` 가 가끔 나온다면, executor 가 아니라 **coordinator 의 이벤트 루프가 멈춘 것**을
먼저 의심한다. coordinator 는 단일 워커라 헬스 체크·task 폴링·API 응답을 이벤트 루프 하나로 처리하므로,
그 루프에서 블로킹 호출이 돌면 그동안 모든 통신이 멈추고 걸어 둔 timeout 이 지나 연결이 끊긴 것처럼
보인다. 기다리는 동안 CPU 는 놀고 있어 자원 지표에는 나타나지 않는다. 두 서비스는 루프가
`logging.loop_stall_ms`(기본 500ms) 넘게 응답하지 않으면 그 순간 루프 스레드가 실행 중인 코드 위치를
남기므로, 같은 시각의 이 경고를 찾으면 무엇이 루프를 막았는지 바로 보인다.

```bash
grep -A30 '이벤트 루프가' $L/query-coordinator-server-warn.log
# 이벤트 루프가 500ms 넘게 응답하지 않음 — 루프 스레드 실행 위치:
#   File ".../coordinator/job_store.py", line ..., in _conn
# ...
# 이벤트 루프 정지 해소 — 약 3120ms 동안 응답 없음
```

job 저장·취소 플래그 조회·`/cluster` 집계처럼 예전에 루프를 막던 store 호출은 전용 스레드 풀로 옮겨
두었다. 이력 기록·heartbeat·self-report·예약·메트릭 기록도 적재가 쓰는 스레드 풀과 떨어진 별도 풀에서
돌므로, 적재가 몰려도 이 기록들이 적재가 끝나기를 기다리지 않는다. 메타 저장소 연결은
`db.connect_timeout_s`(기본 5초) 안에 붙지 못하면 포기하고 경고를 남긴다. 부하 중에 `이력 기록 실패`나
`coordinator heartbeat 실패` 가 `timeout` 과 함께 보이면 메타 DB 가 느리거나 접속이 막힌 것이다. 그래도 이 경고가 나온다면 스택이 가리키는 호출을 개발자에게 전달한다. 실행 중인 job 의 취소
요청은 공유 store 에서 job 단위로 `coordinator.cancel_check_interval_s`(기본 2초)에 한 번 확인하므로,
다른 coordinator 가 받은 취소는 그만큼 늦게 감지될 수 있다.

`*-warn.log` 에 `Server disconnected without sending a response` 가 반복된다면 HTTP keep-alive 시간을
본다. coordinator 의 HTTP 클라이언트는 5초 지난 유휴 연결을 버리는데, 서버의 keep-alive 도 5초면
클라이언트가 연결을 재사용하는 순간 서버가 닫는 경쟁이 생긴다. executor 가 적재로 바빠 루프가 늦게 깨면
들어온 요청을 읽기 전에 연결이 닫혀 이 메시지가 된다. `executor.keep_alive_s`·`coordinator.keep_alive_s`
(기본 30초)를 5초보다 넉넉히 길게 유지한다. 예전 설정을 복사해 쓰는 설치본은 이 키가 없어도 기본값
30초로 동작한다.

GP 쪽에서 `prepared statement "_pg3_N" does not exist` 나 `GP 커넥션 초기화(DISCARD ALL) 실패` 가 보였다면
예전 버전의 문제다. executor 는 GP 연결을 풀에서 재사용하고 반납할 때 `DISCARD ALL` 로 세션을
비우는데, 이 문장이 서버의 prepared statement 를 지워도 psycopg 는 캐시해 둔 이름을 그대로 불렀다.
지금은 풀 연결의 자동 prepare 를 꺼 두었으므로 업그레이드 뒤에도 나온다면 개발자에게 알린다.

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

## SSH 없이 배포 파일 맞추기(`bin/dirsync.py`)

SSH 로 들어갈 수 없는 호스트에는 ansible·rsync·scp 를 쓸 수 없다. `bin/dirsync.py` 는 이럴 때 HTTP 하나로
배포 트리를 맞추는 도구다. iperf 처럼 원본이 있는 호스트에서 `server` 를 띄우고, 맞출 호스트에서 `client`
를 실행하면 클라이언트가 서버의 파일·디렉터리 목록을 받아 자기 디렉터리를 서버와 똑같이 만든다. 서버에
없는 파일을 클라이언트에서 지우는 것까지 포함한 미러다. 파이썬 3.9 이상 표준 라이브러리만 쓰는 파일
하나라, 대상 호스트에 저장소나 가상환경이 없어도 이 파일만 옮겨 `python3` 로 실행하면 된다.

서버는 원본을 절대 바꾸지 않는다. 파일을 읽기만 하고, 목록·파일·헬스 세 가지 GET 요청만 받으며 다른 요청은
거절한다. 원본 디렉터리에 캐시나 잠금 파일도 만들지 않는다. 클라이언트를 서버와 같은 호스트의 같은
디렉터리(또는 그 상위·하위)에 실수로 돌리면 시작하기 전에 거절한다. 인증은 없으므로 서버는 동기화하는
동안에만 띄우고, 다 끝나면 Ctrl+C 로 내린다.

```bash
# 원본 호스트: 배포 트리를 9900 포트로 내보낸다
python3 /data1/distributed-query-executor/bin/dirsync.py server \
    --base /data1/distributed-query-executor --port 9900 \
    --exclude-dir logs,.venv --exclude '__pycache__,*.pyc'

# 대상 호스트: 서버 기준으로 맞춘다(계획을 보여 준 뒤 y/n 을 묻는다)
python3 dirsync.py client --server 10.0.0.1 --port 9900 \
    --base /data1/distributed-query-executor \
    --exclude-dir logs,.venv --exclude config.properties
```

제외는 두 가지로 준다. `--exclude-dir` 은 base 기준 상대 경로이고(`logs`, `data/tmp`) 그 디렉터리와 그
아래 전부를 뺀다. `--exclude` 는 이름 패턴(`*.pyc`, `__pycache__`)이고 `/` 를 넣으면 상대 경로 전체에
맞춘다(`config/*.bak`). 둘 다 여러 번 주거나 쉼표로 나열한다. 서버의 제외 규칙은 클라이언트에 전달되어
클라이언트 규칙과 합쳐지므로, 서버에서 `logs` 를 뺐다면 클라이언트에서 지정을 빠뜨려도 클라이언트의 로그는
지워지지 않는다. 제외된 경로는 클라이언트에서 복사도 삭제도 하지 않는다.

호스트마다 값이 다른 파일은 클라이언트에서 제외한다. 대표적으로 `config/config.properties`(DSN·포트·
executor 목록)와 로그·가상환경이 그렇다. 가상환경(`.venv`)은 파이썬 경로가 박혀 있어 호스트 사이에 그대로
옮기면 깨질 수 있으므로 설치 스크립트로 만들고 동기화에서는 뺀다.

클라이언트는 먼저 할 일을 전부 보여 준다.

```
서버  10.0.0.1:9900  src-host:/data1/distributed-query-executor  (파일 382개, 디렉터리 33개, 35.5MB)
로컬  /data1/distributed-query-executor
제외  디렉터리 .venv, logs / 패턴 *.pyc, __pycache__, config.properties

  [-] 파일 삭제    stale.txt
  [~] 파일 갱신    src/coordinator/app.py  142.7KB
  [+] 파일 추가    src/core/prometheus.py   18.4KB
  [m] 권한 변경    bin/perf-report         600 → 755

요약: 파일 추가 1 · 파일 갱신 1 · 권한 변경 1 · 파일 삭제 1   전송 161.1KB
동기화를 진행할까요? [y/N]
```

y 로 답하면 끝난 항목이 한 줄씩 쌓이고, 맨 아래 줄에 전체 진행 막대·전송량·속도·남은 시간과 받는 중인
파일이 표시된다. 마지막에 종류별 건수와 실패 목록이 나온다. 바뀐 것이 없으면 묻지 않고 끝낸다.

| 옵션 | 뜻 |
|---|---|
| `--dry-run` | 계획만 보여 주고 바꾸지 않는다. 처음 붙일 때는 이것부터 돌려 무엇이 지워지는지 본다 |
| `-y` | 묻지 않고 진행한다(스크립트용). 터미널이 아닌 곳에서 `-y` 없이 돌리면 진행하지 않는다 |
| `--force` | `-y` 와 함께, 로컬 항목의 절반 넘게 지우는 계획도 진행한다. 없으면 base 를 잘못 짚은 것으로 보고 멈춘다 |
| `--no-delete` | 서버에 없는 로컬 항목을 지우지 않는다 |
| `--checksum` | 크기·수정 시각이 같아도 sha256 으로 다시 비교한다(느리지만 확실하다) |
| `-v` | 계획을 전부 보여 준다(기본은 200건까지) |

파일이 같은지는 크기와 수정 시각으로 판단한다. 받은 파일은 수정 시각과 권한을 서버 값으로 맞추므로 다시
돌리면 바뀐 것만 받는다. 파일마다 임시 파일로 받아 sha256 을 검증한 뒤 한 번에 바꿔 끼우므로, 도중에
끊겨도 반쯤 쓰인 파일이 남지 않고 실행 중인 프로세스도 깨진 파일을 보지 않는다. 다만 트리 전체가 한 번에
바뀌는 것은 아니므로, 코드가 바뀌었다면 동기화가 끝난 뒤 서비스를 재기동해야 반영된다. 소유자는 맞추지
않으므로 서비스 계정으로 실행하거나, root 로 실행했다면 끝난 뒤 `chown -R` 으로 소유자를 맞춘다.

서버는 기본으로 9900 포트를 쓴다. 방화벽이 있으면 동기화하는 동안만 대상 호스트 주소에 그 포트를 연다.
종료 코드는 0(완료 또는 바꿀 것 없음), 1(일부 실패), 2(시작 전 중단·취소)다.

---

## 커스텀 코드를 executor 로 자동 배포하기(`sync.*`)

`customs/` 의 커스텀 함수나 템플릿처럼 자주 고치는 파일은 coordinator 에서 고치면 모든 executor 에 알아서
퍼지도록 할 수 있다. coordinator 가 지정한 경로를 감시하다가 파일이 바뀌면 묶음(번들)을 만들어 모든 executor
에 보내고, 모두 받을 준비가 되면 한꺼번에 새 버전으로 바꾼다. 기본으로 켜져 있지 않고 동기화할 대상도
없으므로, 쓰려면 경로와 파일명 패턴을 직접 적는다.

```properties
# coordinator 와 모든 executor 호스트의 config.properties 에 똑같이 둔다
sync.enabled=true
sync.paths=customs
sync.patterns=*.py,*.yml,*.yaml,*.json,*.sql
sync.exclude=__pycache__,*.pyc
```

`sync.paths` 는 앱 홈(`/data1/distributed-query-executor`) 기준 상대 경로이고 쉼표로 여러 개 적는다.
`sync.patterns` 는 그 아래에서 동기화할 파일 이름 패턴이며, 패턴에 맞지 않는 파일은 번들에 들어가지 않는다.
`sync.exclude` 는 뺄 이름 패턴이다. **executor 쪽에도 같은 값을 둬야 한다.** executor 는 `sync.enabled=true`
가 아니면 번들을 받지 않고, 받은 번들에 `sync.paths` 밖의 파일이 있으면 거절한다. 설정을 바꾼 뒤에는
coordinator 와 executor 를 재기동한다.

### 어떻게 한 번에 바뀌는가

쿼리가 도는 중에 파일이 하나씩 바뀌면 반은 옛 코드, 반은 새 코드로 도는 순간이 생긴다. 이를 막으려고 전환은
네 단계로 진행한다.

1. **감시와 확인** — coordinator 는 `sync.interval_s`(기본 5초)마다 대상 파일을 훑어 내용의 해시로 버전을
   정한다. 수정 시각만 바뀐 것은 변경으로 보지 않는다. 바뀐 내용이 `sync.settle_s`(기본 3초) 동안 그대로여야
   배포를 시작하므로 편집하는 도중의 파일은 나가지 않는다. 파이썬 파일은 문법 검사를 먼저 하고, 오류가 있으면
   배포하지 않고 WARNING 을 남긴다.
2. **준비(prepare)** — 모든 executor 에 번들을 보낸다. executor 는 새 버전을 별도 디렉터리에 풀고, 설정된 커스텀
   함수(`query.func.module`·`query.func.fetch_module` 중 동기화 경로 안의 것, 또는 `sync.verify_modules`)를 별도
   프로세스에서 import 해 본다(`sync.verify_import`). 이 단계에서는 현재 버전을 건드리지 않는다. **한 대라도
   실패하면 모두 취소**하고 `sync.retry_backoff_s`(기본 60초) 뒤에 다시 시도한다. 일부만 바뀌는 일은 없다.
3. **비우기(drain)** — 모두 준비되면 새 job 의 실행 시작을 잠시 막고(요청은 받아서 PENDING 으로 둔다) 실행 중인
   job 이 끝나기를 `sync.drain_timeout_s`(기본 600초)까지 기다린다. 한 job 의 task 가 두 버전에 걸치지 않게
   하려는 것이다. 시간을 넘기면 그대로 전환하고, 남은 job 의 나머지 task 는 새 버전으로 돈다는 WARNING 을 남긴다.
4. **전환(commit)** — 모든 executor 를 새 버전으로 바꾸고 job 실행을 다시 연다.

executor 쪽 전환은 심링크 하나를 바꾸는 것이라 원자적이다. 처음 전환할 때 동기화 경로가 심링크로 바뀐다.

```
/data1/distributed-query-executor/customs  ->  .sync/current/customs
/data1/distributed-query-executor/.sync/current  ->  releases/<버전>
/data1/distributed-query-executor/.sync/releases/<버전>/customs/...
```

원래 있던 실제 디렉터리는 `.sync/displaced/<시각>/` 으로 옮겨 두므로 잃어버리지 않는다. 옛 버전은
`sync.keep_releases`(기본 3)개까지 남는다. 전환 순간에 이미 돌고 있던 task 는 옛 코드로 끝까지 돌고, 그 뒤에
시작하는 task 부터 새 코드를 쓴다. **executor 를 재기동할 필요는 없다.**

### 따라잡기와 디스패치 제외

전환할 때 내려가 있던 executor 나 전환에 실패한 executor 는 버전이 어긋난다. coordinator 는 그런 executor 를
디스패치 후보에서 빼고(`/jobs` 와 `/query-execute` 모두), 다시 응답하면 그 한 대에만 준비·전환을 해서
따라잡게 한다. 이때는 drain 하지 않는다. 따라잡으면 다시 후보가 된다. task 요청에는 전환된 버전이 실리고
executor 는 자기 버전과 다르면 409(`BUNDLE_VERSION_MISMATCH`)로 거절하므로, coordinator 가 아직 알아채지
못한 사이 버전이 어긋난 executor 에도 task 가 들어가지 않고 다음 executor 로 넘어간다. 맞는 executor 가
하나도 없으면 task 는 그 이유를 담은 메시지와 함께 FAILED 가 되고, `/query-execute`·`/query-count` 는 503 을
돌려준다. 따라잡은 뒤 실패한 task 는 `retry` 로 다시 돌린다.

coordinator 를 처음 띄웠을 때 모든 executor 가 같은 버전이면 그 버전을 전환된 버전으로 받아들이고, 원본과
다르면 위 네 단계로 맞춘다.

### 상태 보기와 즉시 실행

```bash
curl -s http://<coordinator>:8088/sync/status | jq          # 원본·전환 버전, executor 별 버전·오류
curl -s -X POST http://<coordinator>:8088/sync/now | jq     # settle 대기 없이 지금 한 번 맞춘다
curl -s http://<executor>:8001/sync/status | jq             # executor 의 현재 버전·보관 버전·마지막 오류
```

coordinator 의 응답에서 `source_version` 은 지금 파일의 버전, `active_version` 은 전환을 마친 버전,
`pending_version` 은 settle 을 기다리거나 재시도를 기다리는 버전이다. `draining=true` 이면 지금 drain 중이라
새 job 이 PENDING 에 머문다. executor 마다 `in_sync`·`reachable`·`last_error` 가 나오므로 어긋난 executor 와
그 이유를 바로 알 수 있다. `source_errors` 에는 문법 오류처럼 배포를 막은 이유가 담긴다. 같은 값이 성능 지표
로그에도 `sync.*` 로 남는다.

### 주의할 점

- **executor 에서 동기화 경로의 파일을 직접 고치지 않는다.** 고친 파일은 다음 전환 때 coordinator 의 내용으로
  바뀐다. 고칠 때는 항상 원본 coordinator 에서 고친다.
- **coordinator 가 여러 대면 원본 한 대만 `sync.push=true`** 로 두고 나머지는 `sync.push=false` 로 둔다.
  두 대가 서로 다른 내용을 밀면 executor 가 번갈아 바뀐다. `push=false` 인 coordinator 는 감시도 배포도 하지
  않는다.
- coordinator 와 같은 호스트에서 같은 앱 홈을 쓰는 executor(한 서버에 함께 띄운 경우)는 이미 같은 파일을
  보고 있으므로 파일은 건드리지 않고 불러 둔 모듈만 비운다. 이런 executor 는 상태에 `shared=true` 로 나온다.
- 번들은 풀었을 때 `sync.max_bundle_mb`(기본 100MB)를 넘으면 거절된다. 큰 데이터 파일은 동기화 대상에
  넣지 않는다.
- 동기화되는 것은 파일뿐이다. `config.properties` 처럼 호스트마다 다른 파일은 대상에 넣지 않으며, 설정을 바꾸려면
  여전히 재기동해야 한다. 코드 전체를 맞추는 것은 `bin/dirsync.py` 의 몫이다.
- 동기화를 켠 executor 에 `bin/dirsync.py` 를 돌릴 때는 `--exclude-dir .sync` 와 동기화 경로(예: `customs`)를
  함께 뺀다. 빼지 않으면 dirsync 가 심링크를 실제 디렉터리로 바꿔 놓고, 다음 전환 때 그 디렉터리가
  `.sync/displaced/` 로 옮겨진다.
- 인증은 없다. executor 의 `/sync/*` 는 coordinator 와 운영자만 닿는 망에 둔다.

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

executor 는 SIGTERM 을 받으면 진행 중인 task 가 끝나기를 기다렸다 내려간다. 기다리는 시간은
`executor.shutdown_drain_timeout_s` 이며 기본값은 25초다. 재기동 중에 task 가 잘리는 것이 곤란하다면
평소 task 하나가 걸리는 시간보다 넉넉히 잡아 두고, systemd 로 돌린다면 `TimeoutStopSec` 이 이보다
길어야 뜻이 있다.

executor 는 처리한 task(이관 task 와 query-execute 가 위임한 query-run 모두)를 메모리에 들고 있다가
새 task 를 받을 때 오래된 것을 지운다. 끝난 지 `executor.task_retention_s`(기본 3600초)가 지난 task 와,
끝난 task 가 `executor.max_finished_tasks`(기본 1000개)를 넘을 때 오래 끝난 것부터 지운다. 끝난 지
60초가 안 된 task 는 coordinator 가 아직 결과를 확인하는 중일 수 있어 기준과 무관하게 남긴다. 지운
task 는 `/tasks` 와 대시보드 처리중 목록에서 빠지지만 task_history 이력에는 그대로 남으므로, 오래된
실행은 이력 탭에서 찾는다. 두 값을 0 으로 두면 그 기준이 꺼지는데, query-execute 가 잦은 환경에서 둘 다
끄면 executor 메모리가 계속 자라므로 끄지 않는다.

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

줄일 때는 역순이다. `coordinator.executors` 에서 빼고 coordinator 를 재기동해 새 task 가 가지 않게 한
뒤, 그 executor 에서 돌던 task 가 끝나기를 기다렸다 내린다.

늘린 뒤에는 함께 움직여야 하는 값이 있다. executor 가 늘면 fleet 전체 용량이 커지므로
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

### 메타 저장소를 WarehousePG 에서 PostgreSQL 로 옮기기

WarehousePG 에 둔 메타 저장소는 부하가 몰리면 느려지고 서로 막힌다. 작업 저장, self-report,
예약은 모두 한 행짜리 `INSERT ... ON CONFLICT DO UPDATE` 인데, MPP 는 이런 문장 하나에도 세그먼트
배분과 분산 커밋을 거친다. global deadlock detector 가 꺼져 있으면 UPDATE 가 테이블 전체를 잠가
같은 테이블의 저장이 한 줄로 서기도 한다. 관리 화면에서 INSERT 가 락을 기다리는 모습이 자주 보이면
이 경우다. 게다가 WarehousePG 에는 멱등 키의 UNIQUE 인덱스를 걸 수 없어 중복 제출 방지가 최선
노력에 그친다. 일반 PostgreSQL 로 옮기면 이 문제들이 함께 사라진다. 코드는 바꿀 것이 없고 DSN 만
바꾸면 된다.

먼저 무엇이 어디로 가는지 알아 둔다. 메타 테이블 일곱 개는 모두 `history.db_dsn` 하나를 쓰고
(공유 작업 저장소인 `jobs` 도 DSN 을 따로 두지 않는다), `executor_health_metrics` 만
`monitor.db_dsn` 을 쓴다. `history.db_dsn` 이 비어 있으면 `monitor.db_dsn` 을 대신 쓴다. 따라서 두
값을 새 PostgreSQL 로 바꾸면 메타 저장소 전체가 옮겨 가고, 데이터 적재 대상인 `greenplum.dsn` 만
WarehousePG 에 남는다.

**준비할 것은 세 가지다.** 첫째, PostgreSQL 에 DB 와 계정을 만든다. 슈퍼유저로 `postgres` DB 에
접속해 아래를 실행한다. 이름과 암호는 예시이며, 바꾸면 DSN 도 같은 값으로 맞춘다. 앱 사용자를 DB
소유자로 두고 다음 단계의 스키마도 그 사용자로 만들면, 테이블 소유자가 앱 사용자가 되어 따로 GRANT 할
것이 없다. 같은 문장이 `postgresql.sql` 맨 앞의 주석에도 있고, 점검용 읽기 전용 사용자를 만드는
방법도 거기에 적혀 있다.

```sql
CREATE ROLE queryexec_app LOGIN PASSWORD 'change_me';
CREATE DATABASE queryexec OWNER queryexec_app ENCODING 'UTF8' TEMPLATE template0;
REVOKE CONNECT ON DATABASE queryexec FROM PUBLIC;
GRANT CONNECT ON DATABASE queryexec TO queryexec_app;
```

둘째, `pg_hba.conf` 와
방화벽에서 coordinator 호스트뿐 아니라 **모든 executor 호스트**의 접속을 허용한다. executor 가
`task_history` 와 `executor_status` 를 직접 쓰기 때문에, 이것을 빠뜨리면 이력과 self-report 만
조용히 실패한다. 셋째, `max_connections` 를 확인한다. 앱은 호출마다 연결을 열고 닫으므로 동시 연결
수는 동시에 진행 중인 호출 수와 같다. coordinator 한 대는 job 저장용과 기록용 전용 스레드 풀에서 8개씩,
최대 16개를 열고, executor 한 대는 기록용 전용 풀에서 최대 8개를 연다. 여기에 대시보드 조회 몫과 운영자
접속 몫을 넉넉히 얹는다.

**1. 스키마를 만든다.** 서비스를 멈추기 전에 미리 해 둘 수 있다. 앞에서 만든 앱 사용자로 접속하고,
오류가 나면 바로 멈추도록 `ON_ERROR_STOP` 을 켠다. 여러 번 실행해도 안전하다.

```bash
PG="postgresql://queryexec_app:change_me@pg-host:5432/queryexec"
psql "$PG" -v ON_ERROR_STOP=1 -f /data1/distributed-query-executor/config/postgresql.sql
```

**2. 진행 중인 작업이 없을 때 서비스를 멈춘다.** 실행 중인 job 의 상태는 옛 저장소에 남으므로
옮기는 도중에 돌던 job 은 새 저장소에서 이어지지 않는다. 신규 제출을 막고 대시보드나
`GET /cluster` 의 `jobs.active` 가 0 이 된 것을 확인한 뒤 coordinator 와 executor 를 모두 멈춘다.

**3. 기존 데이터를 옮긴다(선택).** 이력을 새 대시보드에서 계속 보고 싶거나, 예전 job 을 같은 멱등
키로 다시 냈을 때 재생되기를 바란다면 옮긴다. 필요 없으면 이 단계를 건너뛰고 새로 시작해도 된다.
`executor_status`·`executor_reservation`·`coordinator_status` 는 서비스가 뜨면서 다시 채우므로 옮기지
않는다.

옮기기 전에 옛 `jobs` 에 같은 멱등 키를 가진 행이 둘 이상 있는지 확인한다. WarehousePG 에서는
막아 줄 UNIQUE 가 없었으므로 그런 행이 있을 수 있고, 있으면 PostgreSQL 의 UNIQUE 인덱스에 걸려 복사가
통째로 실패한다. 아래 조회가 행을 돌려주면 키마다 최신 행 하나만 남기고 지운 뒤 옮긴다.

```bash
WHPG="postgresql://user:pass@whpg-master:5432/queryexec"
psql "$WHPG" -c "SELECT data->>'idempotency_key' AS k, count(*) FROM public.jobs
                 WHERE data->>'idempotency_key' IS NOT NULL GROUP BY 1 HAVING count(*) > 1;"
```

복사는 새 DB 의 컬럼 목록을 기준으로 옛 DB 에서 같은 이름의 컬럼을 뽑아 넣는다. 두 DB 의 컬럼 순서가
업그레이드 이력에 따라 다를 수 있어서, 순서에 기대지 않고 이름으로 맞추려는 것이다. 이력 세 테이블은
`id` 까지 복사하므로, 끝난 뒤 시퀀스를 최댓값 뒤로 맞춰야 다음 기록이 PK 충돌로 실패하지 않는다.

```bash
set -euo pipefail
for t in jobs job_history task_history executor_health_metrics; do
  cols=$(psql "$PG" -Atc "SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
                          FROM information_schema.columns
                          WHERE table_schema='public' AND table_name='$t'")
  psql "$WHPG" -v ON_ERROR_STOP=1 -c "\copy (SELECT $cols FROM public.$t) TO STDOUT" \
    | psql "$PG" -v ON_ERROR_STOP=1 -c "\copy public.$t ($cols) FROM STDIN"
done
for t in job_history task_history executor_health_metrics; do
  psql "$PG" -v ON_ERROR_STOP=1 -Atc "SELECT setval(pg_get_serial_sequence('public.$t', 'id'),
                       COALESCE((SELECT max(id) FROM public.$t), 0) + 1, false)"
done
psql "$PG" -c "ANALYZE"
```

복사가 끝나면 테이블마다 양쪽의 `count(*)` 를 비교한다. `db.schema` 를 `public` 이 아닌 값으로
쓰고 있다면 위 명령의 `public` 을 그 스키마로 바꾼다.

**4. 설정을 바꾼다.** `history.db_dsn` 과 `monitor.db_dsn` 을 새 PostgreSQL 로 바꾸되, coordinator
호스트와 **모든 executor 호스트**의 `config.properties` 를 함께 고친다. executor 쪽을 빠뜨리면 이력과
self-report 가 옛 DB 로 계속 간다. `store.backend` 등 다른 값은 그대로 둔다.

**5. 띄우고 확인한다.** executor 를 먼저 띄우고 coordinator 를 띄운다. 대시보드의 설정 표에서 두
DSN 이 새 주소로 보이는지 확인하고, 작업 하나를 내서 끝까지 도는지 본다. 그다음 `*-warn.log` 에
`이력 기록 실패`, `executor self-report 실패`, `메트릭 PostgreSQL 기록 실패`,
`coordinator heartbeat 실패`, `relation does not exist` 가 없는지 확인한다. 하나라도 있으면 그 호스트의
DSN·접속 허용·스키마 적용을 다시 본다. 마지막으로 새 DB 에서 행이 늘어나는지 직접 본다.

```bash
psql "$PG" -c "SELECT status, count(*) FROM public.jobs GROUP BY 1;"
psql "$PG" -c "SELECT executor_id, updated_at FROM public.executor_status ORDER BY 1;"
```

**되돌릴 때**는 두 DSN 을 옛 값으로 돌리고 다시 띄우면 된다. 다만 PostgreSQL 에서 쌓인 이력과 작업은
옛 DB 에 없으므로, 그동안의 기록이 필요하면 위 복사 절차를 반대 방향으로 실행한다.

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

## 이력 보관 기간 정하기

`job_history`·`task_history`·`executor_health_metrics` 는 상태가 바뀌거나 메트릭을 잴 때마다 한 행씩
쌓이기만 한다. 그대로 두면 테이블이 끝없이 커지고, 대시보드의 이력 화면은 job·task 별 최신 행을 고르려고
전체 이력을 훑으므로 갈수록 느려진다. 그래서 coordinator 가 보관 기간이 지난 행을 주기적으로 지운다.

```properties
history.retention_days=90            # job_history·task_history 보관 일수(0=무기한)
monitor.retention_days=30            # executor_health_metrics 보관 일수(0=무기한)
store.retention_days=0               # 종료된 job 을 공유 jobs 테이블에 남길 일수(0=무기한, 기본)
history.retention_interval_s=3600    # 정리 주기(초, 0=정리 끔)
```

정리는 coordinator 가 기동 1분 뒤 한 번, 그 뒤로 `history.retention_interval_s` 마다 한다. 한 번에 다
지우지 않고 5000행씩 지우고 커밋하기를 반복하므로, 오랫동안 쌓인 행을 처음 정리할 때도 테이블을 오래
잠그지 않는다. 100만 행 테이블에서 60만 행을 지우는 데 십여 초가 걸리는 정도다. 지운 행 수는
coordinator 로그에 `보관 기간 정리 — <테이블> 에서 N일 지난 행 M건 삭제` 로 남고, 실패하면
`보관 기간 정리 실패` 가 경고 로그에 남는다. coordinator 를 여러 대 띄우면 모두 같은 정리를 돌리지만,
같은 행을 동시에 지우려 하면 뒤쪽은 0건으로 끝나므로 따로 조정할 필요가 없다. 기준 시각은 DB 의 KST
시각이다.

`store.retention_days` 는 신중하게 켠다. `store.backend=postgres` 일 때 끝난 job(DONE·PARTIAL·FAILED·
CANCELLED)이 `jobs` 테이블에 계속 남는데, 이 값을 주면 마지막 갱신에서 그 일수가 지난 job 을 지운다.
실행 중이거나 대기 중인 job 은 아무리 오래돼도 지우지 않는다. 대신 지운 job 은 `GET /jobs/{id}` 가
404 를 주고, 같은 `Idempotency-Key` 로 다시 내면 재생되지 않고 새 작업으로 실행된다. 그러므로 클라이언트가
재제출할 수 있는 기간보다 넉넉히 길게 잡는다. 지난 실행 기록은 `job_history` 에 남으므로 이력 화면에서는
`history.retention_days` 까지 계속 보인다.

정리 직후에는 지운 행의 공간이 바로 디스크로 돌아가지 않는다. PostgreSQL 의 autovacuum 이 그 공간을 다음
기록에 다시 쓰도록 표시할 뿐이므로, 처음 크게 정리한 뒤 디스크를 실제로 줄이고 싶다면 한가한 시간에
`VACUUM FULL public.task_history;` 처럼 테이블별로 실행한다(실행하는 동안 그 테이블을 잠그므로 서비스를
멈추고 한다).

---

## 성능 지표 로그로 튜닝하기

부하가 몰릴 때 어디서 기다리는지는 CPU 그래프만으로는 알 수 없다. 실행 슬롯이 찼는지, 디스패치 한도에
걸렸는지, 스레드 풀이나 GP 커넥션 풀 앞에서 줄을 섰는지, 이벤트 루프가 늦게 도는지가 각각 다른 설정을
가리키기 때문이다. 그래서 coordinator 와 executor 는 이 값들을 서비스 로그 옆의 별도 파일에 주기적으로
남긴다.

```properties
log.perf.enabled=true         # 지표 로그 사용(기본 켜짐)
log.perf.interval_s=30        # 한 줄을 남기는 주기(초). 부하 테스트 중에는 10 이 알맞다
log.perf.job_summary=true     # job·task 가 끝날 때마다 요약 한 줄 추가
```

파일은 `query-coordinator-server-perf.log`, `query-executor-server-<포트>-perf.log` 이고 다른 로그와 같이
자정마다 `<이름>-perf_YYYYMMDD.log` 로 넘어가며 같은 보관 개수를 따른다. 30초 주기면 서비스 하나가 하루
2880줄을 남긴다. 값은 메모리에서만 읽으므로 지표를 남기느라 DB 나 네트워크에 부하를 더하지 않는다.

### 한 줄의 모양

한 줄은 시각 뒤에 `키=값` 을 공백으로 이어 붙인 것이다. `kind=snapshot` 은 주기마다 남는 지표 줄이고,
`kind=job_done`(coordinator)과 `kind=task_done`(executor)은 끝날 때마다 남는 요약 줄이다.

```
2026-10-29 14:03:30 kind=snapshot role=coordinator window_s=30 admission.max_pending=100 admission.max_running=16
  admission.pending=37 admission.running=16 admission.rejected=3 admission.slot_wait_ms.n=12
  admission.slot_wait_ms.p50=800 admission.slot_wait_ms.p95=42000 admission.slot_wait_ms.max=61000
  dispatch.inflight=32 dispatch.max=32 dispatch.waiting=118 loop.lag_ms.max=48 pool.default.queued=9 ...
2026-10-29 14:03:41 kind=job_done job_id=job_3f9c2a1b7d4e status=DONE exec_mode=copy tasks=8 done=8
  rows=18400000 elapsed_s=212.4 slot_wait_ms=41800 execute_s=212.3 load_s=- rows_per_s=86629
  bottleneck=source read_starve_ms=1290400 ... slowest_task=t_a1b2c3d4e5f6 slowest_stage=STREAM_COPY
```

(실제로는 한 줄이다.) 값의 종류는 이름 끝으로 구분한다. `.n`·`.p50`·`.p95`·`.max` 가 붙은 것은 그 구간에
관측한 값들의 건수와 분포이고, 관측이 없던 구간에는 빠진다. `admission.rejected` 처럼 끝이 붙지 않은 횟수와
양은 그 구간에 일어난 만큼이며, `admission.running` 같은 상태 값은 기록하는 순간의 값이다. 시간은 `_ms`
(밀리초)와 `_s`(초)로 표시한다.

### 무엇을 보고 무엇을 바꾸나

| 지표 | 뜻 | 커지면 |
|---|---|---|
| `admission.running`/`max_running`, `admission.pending`/`max_pending`, `admission.rejected` | job 실행 슬롯과 대기 큐, 429 거절 수 | executor 에 여유가 있는데 슬롯이 늘 차 있으면 `coordinator.max_concurrent_jobs` 를, 순간 몰림이면 `max_pending_jobs` 를 올린다 |
| `admission.slot_wait_ms` | job 이 PENDING 에서 슬롯을 기다린 시간 | 위와 같다 |
| `dispatch.inflight`/`max`, `dispatch.waiting`, `dispatch.slot_wait_ms` | 동시에 띄운 task 수와 기다리는 task 수 | `inflight` 가 `max` 에 붙고 `waiting` 이 있으면 `coordinator.max_dispatch_concurrency` 를 executor 슬롯 합계까지 올린다 |
| `dispatch.poll_errors.*`, `dispatch.start_errors.*`, `dispatch.failover` | executor 호출 실패를 종류(timeout·disconnected·connect·http_코드)별로 센 것 | 같은 시각 executor 의 `loop.lag_ms` 와 `keep_alive_s` 를 본다 |
| `executor[호스트:포트].inflight`, `.poll_rtt_ms` | executor 별로 맡긴 task 수와 상태 조회 왕복 시간 | 한쪽으로 쏠리면 `coordinator.executor_select=p2c` 나 예약을 켠다 |
| `dispatch.phase2_running`, `phase2.load_s` | local_stage·s3_stage 의 Phase 2 적재 중인 job 수와 소요 | GP 쪽 적재 용량을 본다 |
| `tasks.running`/`max`, `tasks.queued` (executor) | task 실행 슬롯과 대기 | 슬롯이 늘 차 있고 CPU·네트워크에 여유가 있으면 `executor.max_concurrent_tasks` 를, 아니면 executor 를 늘린다 |
| `gp_pool.in_use`/`max`, `gp_pool.acquire_wait_ms` | GP 커넥션 풀 사용과 연결을 얻기까지 기다린 시간 | 기다림이 보이면 `greenplum.pool_max` 를 동시 task 수 이상으로 올린다 |
| `gp_pool.reset_failures`, `gp_pool.connect_errors` | 세션 초기화(DISCARD ALL)·연결 실패 | 경고 로그에서 원인을 본다 |
| `stage.<단계>_ms` (executor) | 끝난 task 의 단계별 소요 분포(QUEUE_WAIT·IMPALA_SUBMIT·STREAM_COPY·COMMIT 등) | 가장 큰 단계가 병목이다 |
| `copy.read_starve_ms`, `copy.read_wait_ms` | 소스(Impala 등)를 기다린 시간 | 크면 소스가 병목이다. 소스 쪽 자원과 `impala_query_options` 를 본다 |
| `copy.write_wait_ms`, `copy.finalize_wait_ms` | GP 에 쓰고 반영을 기다린 시간 | 크면 GP 가 병목이다. 대상 테이블 분산키와 GP 부하를 본다 |
| `loop.lag_ms`, `loop.stalls` | 이벤트 루프가 예정보다 늦게 돈 시간과 멈춘 횟수 | 수십 ms 를 넘으면 블로킹 호출이 있다. 경고 로그의 `이벤트 루프가` 스택을 본다 |
| `pool.default.*` | 적재 본체(executor)·Phase 2 INSERT(coordinator)가 쓰는 스레드 풀의 사용·큐·대기(`wait_ms`)·실행(`run_ms`) | `queued` 와 `wait_ms` 가 크면 동시 적재 수가 풀 크기(min(32, CPU+4))를 넘는다 |
| `pool.store.*`, `pool.meta.*`, `meta_db.*` | job 저장, 이력·heartbeat 기록 풀과 메타 DB 연결 시간·실패 | `run_ms` 가 크면 메타 DB 가 느리고, `wait_ms` 가 크면 호출이 몰린다 |
| `pool.anyio.*` | 대시보드·조회 API 가 쓰는 스레드 | `busy` 가 `max` 에 붙으면 조회가 밀린다 |
| `jobs_done.*`, `tasks_done.*`, `rows_written`, `rows_read` | 구간 처리량 | 튜닝 전후 비교의 기준이다 |
| `process.*`, `system.*` | 프로세스 CPU·RSS·스레드·fd, 호스트 CPU·메모리·디스크 사용률과 네트워크·디스크 I/O(MB/s) | 자원 포화 여부를 본다 |

요약 줄의 `bottleneck` 은 읽기와 쓰기 중 어느 쪽이 시간을 더 먹었는지를 판정한 것이다. `source` 면 소스를
기다린 시간이, `target` 이면 GP 에 쓰고 반영을 기다린 시간이 다른 쪽의 1.5배를 넘었다는 뜻이고, 비슷하면
`mixed`, 지표가 없는 모드(statement 등)면 `-` 다.

### 요약 도구

`bin/perf-report` 가 지표 파일을 서비스별로 나눠 읽고 시간대별 요약, 병목 의심 구간과 조정 방향, 느린
job·task, 병목 분포를 출력한다. 경로를 주지 않으면 설정의 로그 디렉터리 전체를 읽는다.

```bash
bin/perf-report                                              # 로그 디렉터리 전체, 10분 단위
bin/perf-report --bucket 1m --since '2026-10-29 09:00' --until '2026-10-29 12:00'
bin/perf-report /data1/distributed-query-executor/logs/query-executor-server-8087-perf.log
bin/perf-report --source query-coordinator-server --keys admission.pending,dispatch.waiting,rows_written:rate
```

`--keys` 는 요약 표의 열을 원하는 지표로 바꾼다. 키 뒤에 `:sum`(구간 합), `:max`(최대, 기본), `:rate`(초당),
`:last`(마지막 값)를 붙여 묶는 방법을 정한다. 병목 의심 항목은 지표 줄마다 규칙을 대 보고 걸린 구간을 묶은
것으로, 예를 들어 다음처럼 나온다.

```
[디스패치 한도 포화] 3구간 (14:01~14:05), 최대 118task @ 14:03:30
    → 동시에 띄운 task 가 coordinator.max_dispatch_concurrency 에 닿아 나머지가 기다린다. executor 슬롯 합계보다 작으면 올린다.
```

튜닝은 한 번에 한 값만 바꾸고 같은 부하로 다시 돌려, 처리량(`적재행/s`)과 바꾼 지점의 대기 지표가 함께
좋아졌는지 확인한다. 한 곳의 대기를 풀면 다음 병목이 드러나므로 이 과정을 반복한다.

---

## Prometheus 로 수집하기

지표 로그와 같은 측정값을 Prometheus 로도 긁어 갈 수 있다. coordinator 와 executor 는 각자
`GET /metrics/prometheus` 로 Prometheus 텍스트 형식을 내보낸다. `/metrics` 는 대시보드와 coordinator 헬스
모니터가 읽는 JSON 응답이라 경로를 따로 두었다. 별도 패키지나 사이드카 없이 서비스 프로세스가 직접
응답하고, 값은 메모리와 psutil 에서만 읽으므로 scrape 가 DB 나 네트워크 부하를 더하지 않는다. 지표
로그(`log.perf.enabled`)를 꺼도 exporter 는 따로 동작한다.

```properties
prometheus.enabled=true       # 기본 켜짐. 끄면 두 경로가 404 다
```

Prometheus 서버는 coordinator 포트(8088)와 모든 executor 포트(8087 등)에 닿아야 한다. executor 포트는
보통 바깥에 열지 않으므로, Prometheus 가 다른 호스트에 있으면 방화벽에서 Prometheus 서버 주소에만 길을
터 준다.

### scrape 설정

coordinator 는 자기 executor 목록을 `GET /prometheus/targets` 로 Prometheus `http_sd_configs` 형식으로
돌려준다. 경로(`__metrics_path__`)와 스킴까지 실어 주므로 executor 를 늘리고 줄일 때 Prometheus 설정을
고치지 않아도 된다. coordinator 자신은 고정 대상으로 적는다.

```yaml
scrape_configs:
  - job_name: dqe-coordinator
    metrics_path: /metrics/prometheus
    scrape_interval: 15s
    static_configs:
      - targets: ["coord1.example.com:8088"]
        labels: {role: coordinator}
  - job_name: dqe-executor
    scrape_interval: 15s
    http_sd_configs:
      - url: http://coord1.example.com:8088/prometheus/targets
        refresh_interval: 60s
```

coordinator 를 여러 대 둔다면 coordinator 는 모두 `static_configs` 에 나열하고, executor 목록은 그중 한
대에서만 받는다(모든 coordinator 가 같은 목록을 돌려주므로 여러 대에서 받으면 같은 executor 를 중복해 긁는다).
`http_sd_configs` 를 쓸 수 없는 환경이면 executor 도 `static_configs` 에 `metrics_path:
/metrics/prometheus` 와 함께 적으면 된다.

### 이름 읽는 법

지표 이름은 지표 로그의 키에서 기계적으로 만든다. 점을 밑줄로 바꾸고 앞에 `dqe_` 를 붙이며, 키 안의 가변
부분은 레이블로 뺀다. 그래서 지표 로그의 `executor[10.0.0.1:8087].inflight` 는
`dqe_executor_inflight{executor="10.0.0.1:8087"}` 가 되고, `pool.meta.wait_ms` 는
`dqe_pool_wait_seconds{pool="meta"}`, `jobs_done.FAILED` 는 `dqe_jobs_done_total{status="FAILED"}`,
`stage.STREAM_COPY_ms` 는 `dqe_task_stage_seconds{stage="STREAM_COPY"}` 가 된다. 시간 값은 모두 초로 바꿔
`_seconds` 로 끝난다.

값의 종류도 지표 로그와 다르다. 지표 로그는 구간마다 새로 세지만 Prometheus 에는 프로세스가 뜬 뒤 쌓인
누적값이 나가므로, 횟수는 `_total` 로 끝나는 counter 로 내보내 `rate()` 로 초당 값을 구한다. 지표 로그의
분포(p50·p95·max)는 누적 히스토그램(`_bucket`·`_sum`·`_count`)으로 나가고 백분위는
`histogram_quantile()` 로 구한다. 서비스를 재기동하면 누적값이 0 에서 다시 시작하는데, `rate()` 가 이를
알아서 처리한다.

지표 로그 키와 무관하게 더 나가는 것은 다음과 같다.

| 지표 | 뜻 |
|---|---|
| `dqe_info{role,version}` | 역할과 버전(값은 항상 1). 버전이 섞여 배포됐는지 볼 때 쓴다 |
| `dqe_executor_up{executor}` (coordinator) | coordinator 헬스 모니터가 본 executor 생존(1=정상). 헬스 모니터가 돌 때만 나간다. self-report 모드처럼 모니터가 꺼져 있으면 Prometheus 가 executor 를 직접 긁는 `up` 을 본다 |
| `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`, `process_threads`, `process_start_time_seconds` | 서비스 프로세스 자원(Prometheus 표준 이름) |
| `dqe_system_cpu_seconds_total{mode}`, `dqe_system_memory_{total,available}_bytes`, `dqe_system_disk_{total,used}_bytes{path}`, `dqe_system_network_{receive,transmit}_bytes_total`, `dqe_system_disk_{read,written}_bytes_total` | 호스트 자원. 사용률이 아니라 누적 시간·바이트라 `rate()` 로 본다. 디스크 용량은 `monitor.disk_path` 기준이다 |

### 자주 쓰는 쿼리

```promql
# 실행 슬롯과 대기 큐가 얼마나 찼나
dqe_admission_running / dqe_admission_max_running
dqe_admission_pending / dqe_admission_max_pending
# 429 거절(분당)과 job 종료 상태별 처리량(분당)
rate(dqe_admission_rejected_total[5m]) * 60
sum by (status) (rate(dqe_jobs_done_total[5m])) * 60
# job 이 실행 슬롯을 기다린 시간 p95
histogram_quantile(0.95, sum by (le) (rate(dqe_admission_slot_wait_seconds_bucket[5m])))
# 디스패치 한도에 막혀 기다리는 task 와 executor 별 맡긴 task
dqe_dispatch_waiting
dqe_executor_inflight
# executor 전체 적재 처리량(행/초)
sum(rate(dqe_rows_written_total{role="executor"}[5m]))
# 단계별 평균 소요(어느 단계가 길어졌나)
sum by (stage) (rate(dqe_task_stage_seconds_sum[15m])) / sum by (stage) (rate(dqe_task_stage_seconds_count[15m]))
# 소스 대기 대 GP 쓰기 대기(executor 별)
sum by (instance, metric) (rate(dqe_task_copy_seconds_sum[15m]))
# GP 커넥션을 얻기까지 기다린 시간 p95, 스레드 풀 큐 대기 p95
histogram_quantile(0.95, sum by (le, instance) (rate(dqe_gp_pool_acquire_wait_seconds_bucket[5m])))
histogram_quantile(0.95, sum by (le, pool, instance) (rate(dqe_pool_wait_seconds_bucket[5m])))
# 이벤트 루프 지연 p99
histogram_quantile(0.99, sum by (le, instance) (rate(dqe_loop_lag_seconds_bucket[5m])))
```

위 쿼리의 `role` 레이블은 scrape 설정에서 붙인 것이다(coordinator 는 `static_configs` 의 `labels`,
executor 는 `/prometheus/targets` 가 붙여 준다). 어떤 값이 커질 때 무엇을 바꾸는지는 지표 로그 절의 표와
같다. 경보를 건다면 다음 정도가 출발점이다.

```yaml
groups:
  - name: dqe
    rules:
      - alert: DqeJobRejected
        expr: rate(dqe_admission_rejected_total[5m]) > 0
        for: 5m
        annotations: {summary: "대기 큐가 차서 job 을 429 로 거절하고 있다"}
      - alert: DqeExecutorDown
        expr: up{job="dqe-executor"} == 0
        for: 2m
        annotations: {summary: "executor {{ $labels.instance }} 에 닿지 않는다"}
      - alert: DqeEventLoopLag
        expr: histogram_quantile(0.99, sum by (le, instance) (rate(dqe_loop_lag_seconds_bucket[5m]))) > 0.2
        for: 10m
        annotations: {summary: "{{ $labels.instance }} 이벤트 루프가 200ms 넘게 늦다(블로킹 호출 의심)"}
      - alert: DqeMetaDbConnectErrors
        expr: rate(dqe_meta_db_connect_errors_total[5m]) > 0
        for: 5m
        annotations: {summary: "{{ $labels.instance }} 가 메타 저장소에 연결하지 못한다"}
```

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
NEW='/path/to/new-source'  # 새 버전 소스 경로로 바꾼다
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

스테이징 정리 API는 안전한 job 식별자만 받으며, 잘못된 값은 422로 거절한다.
로컬 정리는 해당 job의 하위 디렉터리만 삭제하며 외부 경로를 가리키는 심볼릭 링크는 거절한다.
