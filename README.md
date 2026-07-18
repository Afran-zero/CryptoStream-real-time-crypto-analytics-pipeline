# CryptoStream

A real-time crypto market-data pipeline built end-to-end on a local
Docker Compose stack. WebSocket ticks → Kafka → Spark → Bronze →
dbt (Silver + Gold) → FastAPI → React dashboard, with Airflow
orchestrating the batch lane and a retention sweep for Bronze.

```
  FreeCryptoAPI ──WS──▶ Ingestion ──▶ Kafka ──▶ Spark ──▶ Bronze (Postgres)
                                                            │
                                                            ▼
                                              Airflow + dbt (every 5 min)
                                                            │
                                                            ▼
                                                  Silver ─▶ Gold
                                                            │
                                                            ▼
                                              FastAPI ◀── React dashboard
```

---

## Table of contents

The full guide is split across `docs/`. This README is the landing page.

- [**Quickstart**](docs/QUICKSTART.md) — get the stack running in ~10 min.
- [**Architecture**](docs/ARCHITECTURE.md) — components, data flow, why each piece exists.
- [**Module docs**](docs/MODULES.md) — one page per module, each with files, run, verify, env.
  - [Module 1 — Infrastructure (Compose)](docs/MODULE_1_INFRASTRUCTURE.md)
  - [Module 2 — Database & medallion schema](docs/MODULE_2_DATABASE.md)
  - [Module 3 — Ingestion (WebSocket → Kafka)](docs/MODULE_3_INGESTION.md)
  - [Module 4 — Stream processing (Spark → Bronze)](docs/MODULE_4_STREAMING.md)
  - [Module 5 — Transforms (dbt Silver + Gold)](docs/MODULE_5_TRANSFORMS.md)
  - [Module 6 — Orchestration (Airflow)](docs/MODULE_6_ORCHESTRATION.md)
  - [Module 7 — API + Dashboard (FastAPI + React)](docs/MODULE_7_API_DASHBOARD.md)
- [**Environment reference**](docs/ENV_REFERENCE.md) — every `.env` var with defaults, owner, failure mode.
- [**Operations**](docs/OPERATIONS.md) — day-2 ops: build, logs, recovery, scaling notes.
- [**Troubleshooting**](docs/TROUBLESHOOTING.md) — symptom → cause → fix cookbook.

---

## What it does

CryptoStream ingests a configurable watchlist (default `BTCUSD,ETHUSD,SOLUSD`)
from a public WebSocket source, normalizes each tick into a canonical
schema, ships it through Kafka with idempotent production, lands it in
Postgres Bronze via a checkpointed Spark Structured Streaming job, and
rolls it up to Silver (clean typed rows) and Gold (1-minute OHLCV
candles + 20-period moving average) via dbt on a 5-minute Airflow
schedule. A FastAPI service exposes the Gold tables, and a React
dashboard visualises latest prices, candle history, and a health badge
with green/amber/red freshness.

---

## TL;DR — 4 commands

```bash
cp .env.example .env
docker compose up -d --build
make stream-bg           # start Spark → Bronze
make dbt                 # one-shot dbt build (Silver + Gold)
```

Then open:
- Dashboard: <http://localhost:5173>
- API docs:  <http://localhost:8000/docs>
- Airflow:   <http://localhost:8080>  (admin / admin)

For the full walkthrough, see [Quickstart](docs/QUICKSTART.md).

---

## Repository layout

```
.
├── docker-compose.yml        # 11-service local stack
├── Makefile                  # convenience targets (see docs/OPERATIONS.md)
├── .env / .env.example       # runtime config (see docs/ENV_REFERENCE.md)
│
├── infra/                    # static init scripts (kafka topics, airflow DB)
├── db/                       # Module 2 — raw SQL migrations + runner
├── cryptostream_common/      # shared Python helpers (env, logging)
├── ingestion/                # Module 3 — WebSocket → Kafka
├── streaming/                # Module 4 — Spark Structured Streaming
├── transforms/               # Module 5 — dbt project (Silver + Gold)
├── orchestration/            # Module 6 — Airflow DAGs + image
├── api/                      # Module 7 — FastAPI Gold-read service
├── dashboard/                # Module 7 — React + Vite frontend
└── docs/                     # this folder
```

---

## Tech stack

| Concern        | Choice                              | Why |
|----------------|-------------------------------------|-----|
| Transport      | Kafka 3.8.0 (KRaft single-node)     | No Zookeeper; one fewer moving part for the demo |
| Compute        | Spark 3.5.1 Structured Streaming    | Native Kafka source; foreachBatch upsert into Postgres |
| Storage        | Postgres 16 (medallion schemas)     | Bronze/Silver/Gold schemas; jsonb for raw payloads |
| Transforms     | dbt-postgres 1.8.0                  | Pure SQL; tests run on every Airflow tick |
| Orchestration  | Airflow 2.9.3 (LocalExecutor)       | Every-5-min schedule; same Postgres for metadata |
| API            | FastAPI + Pydantic v2 + psycopg-pool | Async-ready, typed, OpenAPI for free |
| Frontend       | React 18 + Vite + recharts          | Component-driven charts; nginx serves the build |
| Source         | FreeCryptoAPI WebSocket             | Free public key; spec'd in MODULE_3 |

---

## Service map

| Service              | Port  | Module | Purpose                                       |
|----------------------|-------|--------|-----------------------------------------------|
| `postgres`           | 5432  | 1, 2   | Medallion DB + Airflow metadata DB            |
| `kafka`              | 9092  | 1, 3, 4| Single-broker KRaft                           |
| `kafka-init`         | —     | 1      | Creates topics on first boot (one-shot)       |
| `spark`              | —     | 4      | Idle base; `spark-submit` runs inside it      |
| `airflow-init`       | —     | 6      | DB migrate + admin user + conn + Variable     |
| `airflow-webserver`  | 8080  | 6      | Airflow UI                                    |
| `airflow-scheduler`  | —     | 6      | Runs `transform_dag` every 5 min              |
| `ingestion`          | —     | 3      | WebSocket → Kafka producer                    |
| `dbt`                | —     | 5      | One-shot `dbt build` via `make dbt`           |
| `api`                | 8000  | 7      | FastAPI reads Gold tables                     |
| `dashboard`          | 5173  | 7      | React app served by nginx                     |

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for the per-module data-flow
narrative.

---

## Data contracts

**Canonical tick** (after ingestion normalises, written to Kafka and Bronze):

| field         | type            | notes                                |
|---------------|-----------------|--------------------------------------|
| `symbol`      | `text`          | e.g. `BTCUSD`                        |
| `exchange`    | `text`          | e.g. `FreeCryptoAPI`                 |
| `price`       | `numeric(20,8)` | > 0 (CHECK constraint)               |
| `volume`      | `numeric(20,8)` | nullable                             |
| `event_time`  | `timestamptz`   | UTC                                  |
| `ingested_at` | `timestamptz`   | server-side `now()`                  |
| `source`      | `text`          | constant per producer                |
| `raw`         | `jsonb`         | original payload for audit/debug     |

Business-key uniqueness: `(symbol, exchange, event_time)`. Both Kafka
producer and Bronze upsert rely on it.

---

## Conventions

- **No secrets in git.** `.env` is gitignored; only `.env.example` is committed.
- **Idempotency everywhere it matters.** Kafka producer (idempotence
  + acks=all), Bronze upsert (`INSERT … ON CONFLICT DO NOTHING` via a
  per-batch staging table), dbt materialisations as `table`.
- **Health-checked startup.** `docker compose` uses `depends_on:
  condition: service_healthy` so services boot in the right order
  (Postgres → Kafka → ingestion → dbt → Airflow → API → dashboard).
- **One canonical schema per medallion layer.** No parallel table
  families; every consumer reads from `bronze.prices_raw`,
  `silver.stg_prices`, `gold.candles_1m`, `gold.candles_1m_ma`.

---

## Next steps

1. [Quickstart](docs/QUICKSTART.md) — get the stack running.
2. [ENV reference](docs/ENV_REFERENCE.md) — customise the watchlist,
   swap the source, point at Neon when you're ready.
3. [Module docs](docs/MODULES.md) — drill into any module's files,
   tests, and verification steps.
4. [Troubleshooting](docs/TROUBLESHOOTING.md) — bookmark this; the
   first time Kafka takes 30 s to elect itself you'll want it.