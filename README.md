# query-executor

Coordinator + N Executor 구조의 API. 하나의 Impala `SELECT` 쿼리를 파티션 컬럼의
`IN` 목록 기준으로 분할하여, 각 부분집합을 병렬로 읽어 Greenplum에 적재한다.
자세한 설계는 [DESIGN.md](DESIGN.md) 참고.

## 디렉터리 구조

```
core/          # 공용: 설정 로더 + 설정 + 로깅 (coordinator·executor 공유)
  config_loader.py  config.properties + config.yml(${변수:기본값}) 치환 로더
  config.py         Settings (config 파일 기반 전역 설정)
  logging.py        일 단위 롤링 파일 로깅 (파일명_YYYYMMDD.log)
coordinator/   # FastAPI: 검증 → 분할 → 디스패치 → 상태 추적
  parser.py      1단계 검증 + IN 절 탐지 (sqlglot, hive 방언)
  splitter.py    IN 목록을 N개의 완전한 sub-query로 분할
  dispatcher.py  executor 비동기 디스패치 + 상태 polling (httpx)
  monitor.py     executor /health·/metrics 폴링 + PostgreSQL 메트릭 기록
  app.py         REST API (POST /jobs, .../result, /executors, /health, /metrics)
  __main__.py    실행 진입점 (python -m coordinator)
executor/      # FastAPI: Impala 읽기 → Greenplum COPY 적재, task 상태 노출
  backend.py     ImpalaToGreenplumBackend (impyla + psycopg) + MockBackend
  app.py         REST API (POST /tasks, GET /tasks/{id}, /health, /metrics)
  __main__.py    실행 진입점 (EXECUTOR_PORT=8001 python -m executor)
packaging/config/  # config.properties + config.yml 기본값
tests/         # coordinator 검증 + 라이프사이클 테스트
```

## 설정 (config.properties + config.yml)

argus-catalog backend와 동일한 방식이다. `config.properties`(Java 스타일 key=value)의
값으로 `config.yml`의 `${변수:기본값}` 자리표시자를 치환해 로드한다.

- 설정 디렉터리: `/etc/query-executor/` (환경변수 `QUERY_EXECUTOR_CONFIG_DIR` 로 변경)
- 로컬 개발 시: `QUERY_EXECUTOR_CONFIG_DIR=packaging/config` 로 저장소 기본값 사용
- 핵심 항목: `coordinator.executors`, `impala.*`, `greenplum.dsn`, `copy.batch_size`
- `impala.host` 와 `greenplum.dsn` 이 모두 설정되면 실제 `ImpalaToGreenplumBackend`,
  아니면 `MockBackend`(실제 I/O 없음)로 폴백
- Impala는 **TLS + Kerberos(GSSAPI)**: `impala.use_ssl`/`impala.ca_cert`,
  `impala.auth_mechanism=GSSAPI`/`impala.kerberos_service_name`. 티켓은 OS 자격증명
  캐시(KRB5CCNAME)를 사용 → systemd kinit 서비스/타이머로 keytab 갱신 ([deploy/README.md](deploy/README.md))
- 로깅: `/var/log/query-executor/` 에 일 단위 롤링 (`코드/argus 공통 포맷`)
- 모니터링: 두 서비스 모두 `/health`·`/metrics`(CPU·메모리·디스크) 제공. coordinator는
  executor `/health`·`/metrics` 를 주기 폴링(`GET /executors`)하고 `monitor.db_dsn`
  설정 시 CPU/메모리 사용량을 PostgreSQL(`monitor.table`)에 주기 기록

## 실행 환경 (RHEL 9.2)

RHEL 9.2 기본 Python은 3.9이므로, **Python 3.11+** 를 별도 설치한다.

```bash
# 1) Python 3.11 및 빌드 도구 설치
sudo dnf install -y python3.11 python3.11-pip python3.11-devel

# 2) (executor를 실제 Impala/Greenplum에 연결할 때만) impyla + Kerberos/TLS 의존성
#    Impala 는 TLS + Kerberos(GSSAPI) 환경이다.
sudo dnf install -y gcc gcc-c++ make python3.11-devel \
    krb5-workstation krb5-devel cyrus-sasl-devel cyrus-sasl-gssapi
```

## 설치 및 테스트

```bash
python3.11 -m venv .venv
.venv/bin/pip install --upgrade pip

# coordinator + 테스트 의존성
.venv/bin/pip install -r requirements-dev.txt

# 테스트 실행 (실제 DB 불필요: MockBackend / FakeRunner 사용)
.venv/bin/python -m pytest -q
```

executor를 실제 클러스터에 연결하려면 드라이버를 추가 설치한다:

```bash
.venv/bin/pip install -r requirements-executor.txt
```

## 의존성 파일

| 파일 | 용도 |
|---|---|
| `requirements.txt` | coordinator 런타임(fastapi, uvicorn, sqlglot, httpx, pydantic) |
| `requirements-executor.txt` | executor 런타임 + DB 드라이버(impyla, psycopg) |
| `requirements-dev.txt` | 개발/테스트(pytest, pytest-asyncio) |

## 로컬 실행

설정은 `packaging/config/` 의 기본값을 사용한다(`coordinator.executors`, 포트 등).

```bash
# executor 기동 (포트는 EXECUTOR_PORT 로 지정). 여러 개 띄울 수 있다.
QUERY_EXECUTOR_CONFIG_DIR=packaging/config EXECUTOR_PORT=8001 \
  .venv/bin/python -m executor &
QUERY_EXECUTOR_CONFIG_DIR=packaging/config EXECUTOR_PORT=8002 \
  .venv/bin/python -m executor &

# coordinator 기동 (host/port/executors 는 config 에서 읽음)
QUERY_EXECUTOR_CONFIG_DIR=packaging/config \
  .venv/bin/python -m coordinator
```

## API 문서 (Swagger)

두 서비스 모두 FastAPI 기반 대화형 문서를 제공한다.

| 경로 | 설명 |
|---|---|
| `/docs` | Swagger UI (대화형 API 문서) |
| `/redoc` | ReDoc 문서 |
| `/openapi.json` | OpenAPI 3 스키마 |

```bash
# 브라우저에서 http://localhost:8000/docs (coordinator), http://localhost:8001/docs (executor)
```

```bash
curl -s localhost:8000/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('\''2026-01-01'\'','\''2026-01-02'\'') AND region='\''KR'\''",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "write_mode": "overwrite_partitions",
  "parallelism": 2
}'
```

## 배포 (systemd, RHEL 9.2)

coordinator 1개 + executor 다수를 systemd 서비스로 운영하는 구성과 설치 스크립트는
[`deploy/README.md`](deploy/README.md) 참고.

```bash
sudo ./deploy/install.sh
sudo systemctl enable --now query-executor@8001 query-executor@8002
sudo systemctl enable --now query-coordinator
```

## 1단계(Stage 1) 지원 범위

단순 `SELECT`(+ `ORDER BY` / `LIMIT`)만 지원한다. 파서는 GROUP BY, 집계 함수,
DISTINCT, JOIN, NOT IN, 서브쿼리 IN, 파티션 `IN` 누락을 안정적인 에러 코드로 거부한다.
executor는 기본값이 `MockBackend` 라서 실제 DB 없이도 API를 구동할 수 있다.
