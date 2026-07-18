# Module 7 — API + Dashboard (FastAPI + React)

## Purpose

Expose the Gold tables to a UI and to ad-hoc callers:

- A **FastAPI** service that reads `gold.candles_1m`,
  `gold.candles_1m_ma`, and computes `latest` prices from Gold.
- A **React + Vite + recharts** dashboard with a candle chart, a
  latest-prices table, and a health badge.

## Files

### API

```
api/
  pyproject.toml                          # fastapi, uvicorn, psycopg[binary],
                                          # psycopg-pool, pydantic v2,
                                          # cryptostream-common
  Dockerfile                              # python:3.11-slim
  src/api/
    __init__.py
    config.py                             # ApiConfig dataclass from env
    db.py                                 # build_pool(cfg)
    dependencies.py                       # get_config, get_repo (FastAPI Depends)
    main.py                               # lifespan + CORS + uvicorn entrypoint
    models.py                             # Pydantic v2 response models
    repository.py                         # GoldRepository (psycopg)
    routers/
      __init__.py
      health.py                           # GET /health
      prices.py                           # GET /prices/latest
      candles.py                          # GET /candles/{symbol}
      indicators.py                       # GET /indicators/{symbol}/ma
  tests/
    conftest.py                           # pg_url + gold_schema fixtures
    test_endpoints.py                     # 9 endpoint contract tests
    test_repository.py                    # 6 repository tests
```

### Dashboard

```
dashboard/
  package.json                            # react 18, recharts 2.13, vite 5.4
  vite.config.js                          # dev proxy /api/* → VITE_API_BASE
  Dockerfile                              # multi-stage: node:20-alpine → nginx:alpine
  nginx.conf                              # SPA fallback + 1y cache for /assets/
  index.html
  src/
    main.jsx
    App.jsx                               # polling + symbol selector + AbortController
    api.js                                # fetch wrappers + buildQuery helper
    styles.css
    components/
      CandleChart.jsx                     # recharts LineChart (close + MA)
      HealthBadge.jsx                     # green/amber/red
      LatestPrices.jsx                    # table
```

## Endpoints

| Method | Path                            | Returns                                     |
|--------|---------------------------------|---------------------------------------------|
| GET    | `/health`                       | `{ status: "ok" \| "warming_up" \| "stale" \| "unavailable", db, gold_freshness_seconds }`; 503 if DB down |
| GET    | `/prices/latest?symbols=A,B,C`  | `{ prices: [...] }`                         |
| GET    | `/candles/{symbol}?interval=1m&limit=60` | `{ symbol, interval, candles: [...] }` |
| GET    | `/indicators/{symbol}/ma?limit=60` | `{ symbol, window: 20, points: [...] }` |
| GET    | `/docs`                         | OpenAPI explorer                            |
| GET    | `/openapi.json`                 | OpenAPI spec                                |

`/health` semantics:

- 200 with `status: ok` when DB is reachable **and** last Gold bucket
  is < 2 minutes old.
- 200 with `status: warming_up` when Gold is empty or last bucket is
  between 2 and 10 minutes old.
- 200 with `status: stale` when last bucket > 10 minutes old.
- 503 with `status: unavailable` when DB is unreachable.

## How to run

API + dashboard are started by `make up`. To rebuild just them:

```bash
make api
```

API tests:

```bash
make api-test
```

## How to verify

```bash
# API health
curl -sf localhost:8000/health

# Latest prices
curl -sf "localhost:8000/prices/latest?symbols=BTCUSD,ETHUSD"

# Candles
curl -sf "localhost:8000/candles/BTCUSD?interval=1m&limit=10"

# MA(20)
curl -sf "localhost:8000/indicators/BTCUSD/ma?limit=10"

# Open the dashboard
open http://localhost:5173
```

The dashboard polls every 5 s and updates the badge / chart / table
live.

## Env vars consumed

See [ENV_REFERENCE.md — Module 7](ENV_REFERENCE.md#module-7--api--dashboard).

**API (runtime):** `DATABASE_URL`, `GOLD_SCHEMA`, `CORS_ORIGINS`,
`DB_POOL_MIN`, `DB_POOL_MAX`, `DB_POOL_TIMEOUT_S`.

**Dashboard (build-time, baked into the JS bundle):** `VITE_API_BASE`,
`VITE_WATCHLIST`.

Changing either `VITE_*` requires `docker compose build dashboard`
before `docker compose up -d dashboard`.

## Failure modes

| Symptom                                              | Likely cause                                            |
|------------------------------------------------------|---------------------------------------------------------|
| API `/health` returns 503                            | DB down or unreachable; check `make logs SERVICE=api`   |
| Dashboard shows "API error: …"                       | Wrong `VITE_API_BASE`; rebuild the dashboard            |
| CORS error in browser console                        | Origin not in `CORS_ORIGINS`; update and restart `api`  |
| Candles endpoint returns `[]`                        | Gold empty; run `make dbt` to populate                 |
| MA endpoint returns `[]` for valid symbol            | Gold is fresh but window not yet accumulated; wait     |

## Tests

```bash
make api-test          # 9 endpoint + 6 repository tests, run inside the api container
```

Both fixture families (`pg_url` and `gold_schema`) create a temporary
schema, seed five BTCUSD candles, run the test, and tear the schema
down — so tests don't pollute the medallion DB.

Test counts:

- `test_endpoints.py`: 9 tests (health, prices latest × 4 incl.
  422, candles × 3 incl. 422, MA, OpenAPI).
- `test_repository.py`: 6 tests (health × 2 incl. empty-schema,
  latest × 2, candles, MA).