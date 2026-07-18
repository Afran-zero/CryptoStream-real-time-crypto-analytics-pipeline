# Module 7 — API & Dashboard (Serving + End-to-End)

## Context & Objective
Serve Gold through a documented FastAPI service and visualize it in a React
dashboard, then run an end-to-end check proving the whole pipeline works source →
dashboard. This is the final module; its Definition of Done is the project's
Definition of Done.

## Prerequisites
- Modules 5 & 6 complete: `gold.candles_1m` and `gold.candles_1m_ma` populated and
  kept fresh by `transform_dag`.
- Codebase state: `api/` and `dashboard/` empty except `.gitkeep`.

## Technical Specifications

### API (`api/`, FastAPI + Pydantic v2)
Repository pattern: a `GoldRepository` executes read-only SQL against Gold;
routers depend on it; Pydantic models validate responses; OpenAPI auto-generated.
Endpoints:
- `GET /health` → `{status, db, gold_freshness_seconds}` (checks DB + max bucket age).
- `GET /prices/latest?symbols=BTCUSD,ETHUSD` → latest `close` per symbol from
  `gold.candles_1m`.
- `GET /candles/{symbol}?interval=1m&limit=100` → recent OHLC candles ascending.
- `GET /indicators/{symbol}/ma?window=20` → `bucket, close, ma_20` series.
Read-only DB user recommended. CORS allows the dashboard origin.

### Dashboard (`dashboard/`, React + Vite)
- A symbol selector (from `WATCHLIST`).
- A candlestick or line chart of `/candles/{symbol}` (any charting lib).
- A latest-price panel from `/prices/latest`.
- A pipeline health badge from `/health` (green if `gold_freshness_seconds` under a
  threshold, else amber/red).
- Polls every few seconds; no secrets in the frontend; API base URL via env.

## Step-by-Step Implementation Instructions
1. API: `pyproject.toml` (`fastapi`, `uvicorn[standard]`, `psycopg[binary]`,
   `pydantic>=2`). Implement `db.py` (pool), `repository.py`, `models.py`,
   `routers/…`, `main.py` (app + CORS + include routers). `Dockerfile` runs uvicorn.
2. Add `api` service to compose (port 8000, `DATABASE_URL`, depends_on postgres).
3. API tests (`api/tests/`): unit-test the repository with a seeded temp schema or a
   mock; contract-test each endpoint with FastAPI `TestClient` (status + response
   schema). Include an empty-data case (no candles → 200 with empty list, not 500).
4. Dashboard: scaffold Vite React app, implement components + a small API client,
   `Dockerfile` (build + serve static). Add `dashboard` service to compose (port
   5173/80) with `VITE_API_BASE=http://localhost:8000`.
5. Update root `README.md` with the full quickstart and a demo script.

## Verification & Testing Criteria
```bash
# API tests
docker compose run --rm api pytest -q

# bring the serving tier up
docker compose up -d api dashboard

# endpoint smoke
curl -sf localhost:8000/health | tee /dev/stderr | grep -q '"status":"ok"'
curl -sf "localhost:8000/prices/latest?symbols=BTCUSD,ETHUSD" | grep -q BTCUSD
curl -sf "localhost:8000/candles/BTCUSD?interval=1m&limit=10" | grep -q '"close"'
curl -sf "localhost:8000/indicators/BTCUSD/ma?window=20" | grep -q '"ma_20"'
curl -sf localhost:8000/docs >/dev/null && echo "openapi OK"

# dashboard reachable and rendering
curl -sf localhost:5173 >/dev/null && echo "dashboard OK"
```

### End-to-end acceptance (the project DoD)
Run the full stack and confirm all five, matching the PRD success metrics:
1. `make up && make migrate && make dbt && make airflow-up` then start ingestion +
   `make stream`: live data flows source → Bronze → (dbt) Gold → API → dashboard.
2. Dashboard shows moving prices and a green health badge.
3. Stop and restart the Spark job mid-run → `bronze.prices_raw` has **zero**
   duplicate business keys (re-run the Module 4 no_dupes check).
4. A malformed source message appears in `prices.dlq`, ingestion stays up
   (Module 3 check).
5. A deliberately violated dbt test fails `transform_dag` visibly in Airflow
   (Module 5/6 check).

## Hand-off State
- FastAPI serving Gold with OpenAPI docs; React dashboard rendering live analytics
  and pipeline health.
- End-to-end pipeline verified against all PRD success metrics.
- Build cycle complete: a clean machine can go from `git clone` to a working demo by
  following Modules 0→7 in order.
