# 운영자 가이드 (일상 운영과 장애 대응)

이 문서는 **분산 쿼리 실행기를 돌보는 사람**을 위한 것이다. 설치를 마친 뒤로 날마다 무엇을 보고,
느려지거나 실패했을 때 어디부터 뒤지고, 용량이 모자랄 때 무엇을 조절하는지를 다룬다.

앞뒤로 이어지는 문서가 있다. 설치와 최초 설정은 [배포 가이드](DEPLOY.md)에, 값을 정하는 근거와
확장·HA 설계는 [성능·확장 가이드](PERFORMANCE.md)에, API 를 쓰는 쪽 이야기는
[사용자 가이드](USER_GUIDE.md)에 있다. 여기서는 그 사이 — **이미 돌고 있는 시스템을 살피는 일**만
다룬다.

| 상황 | 어디를 보는가 |
| --- | --- |
| 지금 잘 돌고 있나 | [매일 보는 것](#매일-보는-것) |
| 작업이 429 로 거절된다 | [작업이 밀릴 때](#작업이-밀릴-때) |
| 이관이 너무 느리다 | [느릴 때](#느릴-때) |
| 작업이 실패했다 | [실패를 추적할 때](#실패를-추적할-때) |
| 설정을 바꾸고 싶다 | [설정 바꾸기](#설정-바꾸기) |
| 처리량을 늘리고 싶다 | [executor 늘리고 줄이기](#executor-늘리고-줄이기) |

---

## 무엇이 어떻게 돌아가는가

운영 판단을 하려면 구조를 한 문단쯤은 알고 있어야 한다.

coordinator 한 대가 요청을 받아 SQL 을 조각으로 나누고, executor 여러 대에 나눠 준다. **데이터는
coordinator 를 지나가지 않는다.** executor 가 소스에서 읽어 Greenplum 으로 곧장 보내고,
coordinator 로는 상태와 행 수만 올라온다. 그래서 coordinator 가 병목이 되는 일은 드물고,
처리량을 늘릴 때 손대는 것은 거의 언제나 executor 쪽이다.

두 서비스 모두 상태를 프로세스 메모리에 들고 있어 **단일 워커**로 돈다. 워커 수를 늘리는 방식의
확장은 하지 않으며, 늘릴 것은 executor 인스턴스 수다.

---

## 매일 보는 것

한 번에 전체를 보려면 클러스터 통합 상태가 가장 빠르다.

```bash
curl -s localhost:8088/cluster        # coordinator + 전체 executor + job 집계
curl -s localhost:8088/health         # 살아 있는지만
curl -s localhost:8088/metrics        # CPU·메모리·디스크
```

브라우저를 쓸 수 있으면 `http://<coordinator>:8088/` 의 대시보드가 같은 내용을 보여 준다.
터미널만 있다면 읽기 전용 모니터를 띄운다.

```bash
bin/dashboard-tui.sh                      # 설정에서 coordinator 주소를 유추한다
bin/dashboard-tui.sh --url http://host:8088 --interval 5
```

모니터는 스스로 갱신하는데, 목록을 읽는 동안 화면이 바뀌어 거슬리면 **스페이스로 잠시 세우고**
`+`/`-` 로 주기를 바꾼다. 상태 줄의 갱신 시각을 보면 지금 화면이 언제 것인지 알 수 있으므로,
화면이 그대로일 때 멈춘 것인지 조용한 것인지 구분된다. `Enter` 로 job 이나 executor 상세로
들어가고 `ESC` 로 나온다.

살펴야 할 것은 세 가지다.

**executor 가 다 살아 있는가.** `/cluster` 의 `executors_summary` 에서 `unhealthy` 가 0 이 아니면
그 executor 는 배분에서 빠진 상태다. 한 대가 죽어도 나머지가 일을 나눠 받으므로 작업은 계속되지만,
그만큼 용량이 줄어 있다.

**작업이 쌓이고 있지 않은가.** `jobs.running` 이 실행 슬롯(`coordinator.max_concurrent_jobs`)에
붙어 있고 대기가 함께 늘고 있으면 곧 429 가 나기 시작한다.

**디스크가 남아 있는가.** `local_stage` 나 `s3_stage` 를 쓴다면 CSV 가 잠시 쌓인다. 적재에 실패한
작업은 정리를 못 하고 남을 수 있으므로 스테이징 경로를 가끔 들여다본다.

---

## 로그 읽기

로그는 날짜 단위로 갈리며 두 벌이다.

```bash
L=/data1/distributed-query-executor/logs
tail -f $L/query-coordinator-server.log        # 전체
tail -f $L/query-coordinator-server-warn.log   # WARNING 이상만
tail -f $L/query-executor-server-8087.log
```

`*-warn.log` 는 경고와 오류만 모아 둔 것이라, 문제를 좇을 때는 이쪽부터 훑는 편이 빠르다.

**모든 로그 줄에 작업·조각 식별자가 붙는다.** 사용자가 `job_id` 를 알려 주면 그것만으로 관련된
줄을 전부 모을 수 있고, coordinator 와 executor 의 로그를 같은 식별자로 이어 볼 수 있다.

```bash
grep 'a1b2c3d4' $L/query-coordinator-server.log $L/query-executor-server-*.log
```

**실행한 SQL 은 전부 기록된다.** 로그 레벨과 무관하게 INFO 로 남으므로 평소 설정에서도 "무엇을
읽어 무엇을 적재했는지"가 비어 있지 않다.

```
SQL 실행 datasource=impala phase=SELECT | SELECT user_id, ... WHERE dt IN ('2026-01-01')
SQL 실행 datasource=greenplum phase=INSERT target=public.sales | INSERT INTO ...
```

`datasource` 로 어느 엔진에 던진 문장인지 갈리므로, 소스에서 못 읽은 것인지 대상에 못 넣은 것인지가
한눈에 구분된다. 아주 긴 SQL 은 잘리는데, 잘린 경우에는 전문이 아니라는 표시가 함께 남는다.

HTTP 요청·응답까지 보려면 로그 레벨을 DEBUG 로 내린다. 다만 양이 크게 늘므로 문제를 좇는 동안만
쓰고 되돌린다.

---

## 설정 바꾸기

설정은 `config.properties` 한 파일이며, 바꾼 뒤에는 **서비스를 재기동해야** 반영된다.

손으로 고쳐도 되지만 터미널 설정 편집기를 쓰면 항목마다 무엇인지·어떤 범위인지를 함께 볼 수 있다.

```bash
bin/config-tui.sh
```

첫 화면이 **동시성** 탭이다. 처리량을 좌우하는 값들이 섹션을 넘어 한자리에 모여 있고, `+`/`-` 로
올리고 내리면 화면 아래에서 실제 용량이 곧바로 다시 계산된다.

```
 입구: 동시 16건 실행 + 100건 대기 = 116건까지 수용(초과 429)
 플릿: executor 2대 × task 8개 = 동시 16개, GP 연결 최대 16개(pool_max 자동)
 copy 버퍼: 8 × 10,000행 ≈ task 당 최대 80,000행을 메모리에 보관
```

어떤 값인지 확실하지 않으면 `?` 를 누른다. 그 항목이 무엇을 정하는지, 얼마로 두어야 하는지, 함께
보아야 할 설정이 무엇인지가 한 화면에 나온다. 저장할 때 값들 사이가 어긋나면(예: GP 연결 풀이
동시 task 수보다 작으면) 경고로 알려 주고, 아예 동작을 멈추게 하는 값은 저장을 막는다.

저장 전에 `.bak` 로 원본을 백업하고 바꾼 값만 제자리에서 갱신하므로 주석과 순서는 그대로 남는다.

---

## 작업이 밀릴 때

사용자가 `429` 를 본다는 것은 **실행 슬롯과 대기 큐가 모두 찼다**는 뜻이다. 고장이 아니라 설계된
방어선이므로, 먼저 어느 층이 좁은지 가려낸다.

```bash
curl -s localhost:8088/cluster    # jobs.running / jobs.active 와 executor 부하를 함께 본다
```

**executor 는 한가한데 429 가 난다면** 입구가 좁은 것이다. `coordinator.max_concurrent_jobs`(동시
실행 슬롯)와 `coordinator.max_pending_jobs`(대기 큐)를 올린다. 특히 대기 큐가 0 이면 완충이 아예
없어 슬롯을 넘는 요청이 곧바로 429 가 되므로, 잠깐 몰리는 요청을 흡수하려면 넉넉히 둔다.

**executor 가 이미 포화라면** 입구만 넓혀 봐야 대기만 길어진다. 이때는 [executor 를 늘리거나](#executor-늘리고-줄이기)
`executor.max_concurrent_tasks` 를 올리는데, 후자는 소스와 대상의 여력을 함께 봐야 한다.

**디스패치가 병목일 수도 있다.** `coordinator.max_dispatch_concurrency` 가 플릿 전체 용량
(executor 수 × 동시 task 수)보다 작으면 executor 가 놀아도 조각이 나가지 못한다. 설정 TUI 의
동시성 탭이 이 어긋남을 경고로 짚어 준다.

---

## 느릴 때

먼저 **어디가 느린지** 가른다. 소스에서 못 읽고 있는지, 대상에 못 넣고 있는지, 그 사이가 막혔는지.

executor 상세(대시보드나 `GET /executors/{idx}/metrics`)에서 조각이 `READING` 에 오래 머물면 소스
쪽이고, `WRITING` 이면 Greenplum 쪽이다. 로그의 SQL 기록에서 `datasource` 를 봐도 같은 판단을 할
수 있다.

**소스가 느리다면** 이관이 Impala 의 다른 작업과 자원을 다투고 있을 수 있다. `impala.query_options`
로 전용 풀과 메모리 상한을 지정해 서로 밀어내지 않게 한다.

**대상이 느리다면** GP 연결이 모자라지 않은지 본다. `greenplum.pool_max` 가 동시 task 수보다 작으면
조각마다 연결을 기다린다. 0 으로 두면 동시 task 수를 따라가므로 대개 0 이 정답이다.

**둘 다 아니라면** 경로 자체를 바꿀 때다. `copy` 는 데이터가 executor 프로세스를 한 줄씩 통과하므로
아주 큰 이관에서는 그 자체가 한계가 된다. `local_stage` 나 `s3_stage` 로 옮기면 Greenplum 의 모든
세그먼트가 파일을 나눠 동시에 읽으므로 성격이 달라진다. 어느 쪽이 가능한지는 배치 제약이 정한다
— `local_stage` 는 executor 와 GP 세그먼트가 같은 호스트에 있어야 하고, `s3_stage` 는 그 제약이
없는 대신 버킷과 PXF 설정이 필요하다.

값을 얼마로 잡을지에 대한 근거와 계산은 [성능·확장 가이드](PERFORMANCE.md)에 있다.

---

## 실패를 추적할 때

사용자가 `job_id` 를 들고 오면 순서는 이렇다.

**1. 작업 상태를 본다.**

```bash
curl -s localhost:8088/jobs/$JOB_ID | python3 -m json.tool
```

`error` 에 이유가 있고, task 목록에서 어느 조각이 어느 executor 에서 실패했는지 보인다. `PARTIAL`
이면 일부만 들어간 것이므로 사용자에게 `POST /jobs/{id}/retry` 를 안내한다(성공한 조각은 건너뛴다).

**2. 그 식별자로 로그를 모은다.**

```bash
grep "$JOB_ID" $L/query-coordinator-server.log $L/query-executor-server-*.log | less
```

실행한 SQL 이 함께 남아 있으므로, 소스 쿼리가 실패했는지 적재 문장이 실패했는지가 드러난다.

**3. 실제 엔진에 직접 물어본다.** 로그의 SQL 을 그대로 손으로 실행해 보는 것이 가장 확실하다.

```bash
bin/impala-shell        # 소스 쪽 확인
bin/gp-shell            # 대상 쪽 확인 — 테이블이 있는지, 권한이 있는지
```

`s3_stage` 를 쓴다면 중간 산출물이 남아 있는지도 확인한다.

```bash
bin/s3-ops ls s3://<버킷>/<프리픽스>/$JOB_ID/
```

### 자주 나오는 원인

**executor 접속 실패.** coordinator 가 executor 에 닿지 못하면 재시도했다가 다른 executor 로
넘긴다(`coordinator.task_failover`). 로그에 연결 실패가 반복되면 그 executor 의 프로세스와 포트를
확인한다. 다만 `local_stage` 는 executor 와 세그먼트가 짝지어 있어 넘어가면 짝이 깨지므로, 그
모드에서 failover 가 도는 것 자체가 신호다.

**대상 테이블이나 컬럼이 어긋남.** `copy.preflight` 가 켜져 있으면 COPY 를 시작하기 전에 걸러
주지만, 꺼져 있으면 데이터를 반쯤 밀어 넣다 실패한다.

**staging 이름 충돌.** `stage_insert` 에서 TEMP 테이블이 `already exists` 로 부딪히면
`coordinator.stage_unique_staging` 이 꺼져 있는지 본다. GP 연결을 풀에서 재사용하는 구조라 이름이
같으면 앞 작업의 TEMP 가 남는다.

**MockBackend 로 떠 있음.** 작업은 성공이라는데 데이터가 없다면 이것을 의심한다. `greenplum.dsn`
이 비어 있으면 실제로는 아무것도 읽고 쓰지 않는 백엔드로 기동한다. 기동할 때 경고 로그가 남으므로
`*-warn.log` 에서 바로 찾을 수 있다.

```bash
grep MockBackend $L/query-executor-server-*-warn.log
# greenplum.dsn 미설정 → MockBackend 사용
```

`impala.host` 만 비어 있는 경우는 다르다. 이때는 실제 백엔드로 뜨되 소스를 읽을 수 없어
`statement` 모드만 동작하며, 기동 로그에 `impala=(미설정 → statement 모드만)` 으로 남는다.

---

## 기동·중지·재기동

중지는 **coordinator 부터**, 기동은 **executor 부터** 한다. 받아 줄 곳이 없는 상태에서 요청을 받지
않기 위해서다.

```bash
B=/data1/distributed-query-executor/bin
sudo -u gpadmin $B/status-coordinator.sh      # 프로세스 + health
sudo -u gpadmin $B/status-executor.sh

sudo -u gpadmin $B/stop-coordinator.sh
sudo -u gpadmin $B/restart-executor.sh        # 전체 재기동
sudo -u gpadmin $B/start-executor.sh 8086     # 특정 포트만
```

executor 는 SIGTERM 을 받으면 진행 중인 조각이 끝나기를 기다렸다 내려간다
(`executor.shutdown_drain_timeout_s`, 기본 25초). 그러므로 재기동 중에 조각이 잘리는 것이
곤란하다면 이 값을 평소 조각 하나가 걸리는 시간보다 넉넉히 잡아 둔다. systemd 로 돌린다면
`TimeoutStopSec` 이 이보다 길어야 뜻이 있다.

기동할 때 콘솔과 로그에 찍히는 배너에는 **실제로 읽은 설정 파일의 절대 경로**가 나온다. 설정을
바꿨는데 반영이 안 된 것 같으면 여기부터 본다 — 엉뚱한 디렉터리를 읽고 있는 경우가 많다.

---

## executor 늘리고 줄이기

처리량 확장은 executor 인스턴스 수로 한다. 순서가 중요하다.

**1. 먼저 설정에 등록한다.** `coordinator.executors` 에 새 URL 을 추가한다. 이 목록에 없으면
프로세스를 띄워도 coordinator 가 일을 주지 않는다.

**2. 새 인스턴스를 띄운다.**

```bash
sudo -u gpadmin $B/start-executor.sh 8003
# 또는 전체를 한 번에: EXECUTOR_PORTS="8087 8086 8003" $B/start-executor.sh
```

**3. coordinator 를 재기동한다.** executor 목록은 기동할 때 읽는다.

**4. 확인한다.** `curl -s localhost:8088/cluster` 에서 새 executor 가 healthy 로 잡히는지 본다.

줄일 때는 역순이다. `coordinator.executors` 에서 빼고 coordinator 를 재기동해 새 조각이 가지 않게
한 뒤, 그 executor 에서 돌던 조각이 끝나기를 기다렸다 내린다.

늘린 뒤에는 **함께 움직여야 하는 값**이 있다. executor 가 늘면 플릿 전체 용량이 커지므로
`coordinator.max_dispatch_concurrency` 가 그보다 작지 않은지, Greenplum 의 `max_connections` 가
`executor 수 × pool_max` 를 감당하는지 확인한다. 설정 TUI 의 동시성 탭이 이 곱셈을 풀어 보여 준다.

`local_stage` 를 쓴다면 새 executor 도 GP 세그먼트 호스트 위에 있어야 하고
`executor.gp_hostname` 을 그 호스트명과 정확히 맞춰야 한다.

---

## 정기적으로 살필 것

**디스크.** `local_stage`·`s3_stage` 의 스테이징 경로와 로그 디렉터리가 대상이다. 적재에 실패한
작업은 중간 산출물을 정리하지 못하고 남기므로 가끔 들여다본다. 로그는 날짜별로 갈리며
`log.backup_count` 만큼만 남는다.

**이력 DB.** `history.db_dsn` 을 설정했다면 `job_history`·`task_history` 가 계속 쌓인다. 앱이
지우지 않으므로 보존 기간을 정해 오래된 행을 지우는 일은 운영 쪽에서 한다. 메트릭
(`executor_health_metrics`)도 마찬가지다.

**S3 버킷.** `s3.delete_on_cleanup` 이 꺼져 있으면 객체가 계속 남는다. 보관이 필요해서 껐다면
수명주기 정책을 따로 거는 편이 낫다.

---

## 업그레이드할 때

새 버전을 설치해도 `config/`·`templates/`·`customs/` 는 덮이지 않는다. 운영자가 손으로 넣은 값을
지우지 않기 위해서인데, 그래서 **새 버전이 추가한 설정은 저절로 들어오지 않는다.**

여기서 놓치기 쉬운 것이 `config.yml` 이다. 이 파일은 값이 아니라 `${변수:기본값}` 자리를 담은
**구조**라서, 자리가 없으면 `config.properties` 에 값을 적어도 조용히 무시된다. 새 설정을 쓰려면
`config.yml` 을 새 버전 것으로 교체해야 한다.

절차와 diff 를 뜨는 방법은 [배포 가이드](DEPLOY.md)에 있다. 교체한 뒤 새 항목이 실제로 들어왔는지는
`bin/config-tui.sh` 로 확인하는 것이 빠르다 — 항목 목록을 `config.yml` 에서 자동으로 읽으므로,
새 설정이 보이면 반영된 것이다.

---

## 더 볼 것

- [배포 가이드](DEPLOY.md) — 설치, 최초 설정, 오프라인 설치, 업그레이드 절차
- [성능·확장 가이드](PERFORMANCE.md) — 값을 정하는 근거, 확장 전략, 고가용성
- [사용자 가이드](USER_GUIDE.md) — 사용자가 보는 오류와 API 동작
- [설계 문서](DESIGN.md) — 왜 이렇게 만들어졌는가
