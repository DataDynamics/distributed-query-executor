# query-executor

Coordinator + N Executor 구조의 API. 하나의 Impala `SELECT` 쿼리를 파티션 컬럼의
`IN` 목록 기준으로 분할하여, 각 부분집합을 병렬로 읽어 Greenplum에 적재한다.
자세한 설계는 [DESIGN.md](DESIGN.md) 참고.

## 디렉터리 구조

```
coordinator/   # FastAPI: 검증 → 분할 → 디스패치 → 상태 추적
  parser.py      1단계 검증 + IN 절 탐지 (sqlglot, hive 방언)
  splitter.py    IN 목록을 N개의 완전한 sub-query로 분할
  dispatcher.py  executor 비동기 디스패치 + 상태 polling (httpx)
  app.py         REST API (POST /jobs, GET /jobs/{id}, .../result, .../tasks/{id})
executor/      # FastAPI: Impala 읽기 → Greenplum COPY 적재, task 상태 노출
  backend.py     ImpalaToGreenplumBackend (impyla + psycopg) + MockBackend
  app.py         REST API (POST /tasks, GET /tasks/{id}, .../result)
tests/         # coordinator 검증 + 라이프사이클 테스트
```

## 실행 환경 (RHEL 9.2)

RHEL 9.2 기본 Python은 3.9이므로, **Python 3.11+** 를 별도 설치한다.

```bash
# 1) Python 3.11 및 빌드 도구 설치
sudo dnf install -y python3.11 python3.11-pip python3.11-devel

# 2) (executor를 실제 Impala/Greenplum에 연결할 때만) impyla 빌드 의존성
#    impyla는 SASL/Thrift 컴파일이 필요할 수 있다.
sudo dnf install -y gcc gcc-c++ make cyrus-sasl-devel
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

```bash
# executor 기동 (여러 개 가능). coordinator는 EXECUTORS 환경변수로 이들을 가리킨다.
.venv/bin/uvicorn executor.app:app --port 8001
.venv/bin/uvicorn executor.app:app --port 8002

EXECUTORS="http://localhost:8001,http://localhost:8002" \
  .venv/bin/uvicorn coordinator.app:app --port 8000
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
