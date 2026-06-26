# query-executor

Coordinator + N Executor API. Splits one Impala `SELECT` by a partition column's
`IN`-list and loads each subset into Greenplum in parallel. See [DESIGN.md](DESIGN.md).

## Layout

```
coordinator/   # FastAPI: validate -> split -> dispatch -> track status
  parser.py      stage-1 validation + IN-clause detection (sqlglot, hive dialect)
  splitter.py    chunk the IN-list into N complete sub-queries
  dispatcher.py  async dispatch to executors + status polling (httpx)
  app.py         REST API (POST /jobs, GET /jobs/{id}, .../result, .../tasks/{id})
executor/      # FastAPI: read Impala -> COPY into Greenplum, expose task status
  backend.py     ImpalaToGreenplumBackend (impyla + psycopg) + MockBackend
  app.py         REST API (POST /tasks, GET /tasks/{id}, .../result)
tests/         # coordinator validation + lifecycle tests
```

## Setup & test

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"        # coordinator + test deps
.venv/bin/python -m pytest -q
```

To run the executor against real clusters also install the drivers: `pip install -e ".[executor]"`.

## Run locally

```bash
# executor(s) — set EXECUTORS for the coordinator to point at them
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

## Stage-1 scope

Only simple `SELECT` (+ `ORDER BY`/`LIMIT`). The parser rejects GROUP BY, aggregates,
DISTINCT, JOIN, NOT IN, subquery-IN, and missing partition `IN` with stable error codes.
The executor defaults to `MockBackend` so the API runs without live DBs.
