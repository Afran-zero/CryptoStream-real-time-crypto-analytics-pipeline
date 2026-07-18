# 00 — Project Overview

**Project:** CryptoStream — real-time crypto analytics pipeline
**Audience:** Autonomous AI coding agent
**Reading order:** This file first, then `MODULE_1` … `MODULE_7` in strict numeric order.

> Execution rule for the agent: complete each module fully, run its
> **Verification** section, and confirm every check passes **before** opening the
> next module. Do not skip ahead. Each module's Hand-off State is the next
> module's assumed starting environment.

---

## 1. What is being built

A two-lane data platform. A **streaming lane** ingests live crypto ticks from a
WebSocket source, buffers them through Kafka, and lands them in a Bronze table via
Spark Structured Streaming. A **batch lane** (Airflow orchestrating dbt) transforms
Bronze into Silver (clean) and Gold (analytics). A FastAPI service serves Gold to a
React dashboard.

```
FreeCryptoAPI (WebSocket)
      │  streaming lane
      ▼
Ingestion service (Python)  ──▶  Kafka: prices (+ prices.dlq)  ──▶  Spark Structured Streaming
                                                                              │
                                                                              ▼
                                                              ┌──────────────────────────┐
                                                              │  Postgres (medallion)     │
                                                              │  bronze → silver → gold   │
                                                              └──────────────────────────┘
                                                                    ▲            │
                                          batch lane: Airflow ─────▶ dbt          │ serving
                                                                                  ▼
                                                                    FastAPI  ──▶  React dashboard
```

**Design invariant:** streaming tools (Kafka, Spark) stay in the streaming lane;
orchestration (Airflow) stays in the batch lane. Airflow never brokers or
processes the live feed — it only schedules dbt, retention, and backfills.

---

## 2. Tech stack (fixed)

| Layer | Technology | Version target |
|---|---|---|
| Language (services) | Python | 3.11 |
| Source | FreeCryptoAPI WebSocket | free tier |
| Broker | Apache Kafka (KRaft, single node) | 3.8.x |
| Stream processing | Apache Spark Structured Streaming | 3.5.x |
| Storage | PostgreSQL (local in Compose; Neon in prod) | 16 |
| Transforms | dbt (dbt-postgres) | 1.8.x |
| Orchestration | Apache Airflow (LocalExecutor) | 2.9.x |
| API | FastAPI + Uvicorn + Pydantic v2 | latest |
| Frontend | React + Vite + a charting lib | latest |
| Packaging | Docker + Docker Compose | v2 |

Kafka client library for Python: `confluent-kafka`.
Postgres driver: `psycopg[binary]` (services), JDBC `postgresql-42.x` (Spark).

> The agent must not substitute technologies. If a version pin fails to resolve,
> pin to the nearest working patch of the same minor line and record it in the
> module's notes — do not switch libraries.

---

## 3. Repository structure (target end state)

```
cryptostream/
├── README.md
├── Makefile
├── .env.example
├── docker-compose.yml
├── infra/
│   └── kafka/create_topics.sh
├── db/
│   ├── migrations/            # ordered SQL, applied by db/run_migrations.py
│   └── run_migrations.py
├── ingestion/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── src/ingestion/…        # ws client, normalizer, producer
│   └── tests/
├── streaming/
│   ├── Dockerfile
│   └── src/stream_to_bronze.py
├── transforms/                # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/{staging,marts}/…
├── orchestration/
│   ├── Dockerfile             # airflow + dbt-postgres
│   └── dags/
├── api/
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── src/api/…
└── dashboard/
    ├── Dockerfile
    └── src/…
```

The agent builds this incrementally; directories appear as their module runs.

---

## 4. Global environment setup (do this once, in Module 1)

Prerequisites on the host: Docker Engine + Docker Compose v2, `make`, `git`.
No cloud accounts are required for the local build.

Canonical environment variables (defined in `.env.example`, copied to `.env`):

```
# Postgres (medallion + airflow metadata share one instance, separate DBs)
POSTGRES_USER=cryptostream
POSTGRES_PASSWORD=cryptostream
POSTGRES_DB=cryptostream
DATABASE_URL=postgresql://cryptostream:cryptostream@postgres:5432/cryptostream

# Kafka
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_TOPIC_PRICES=prices
KAFKA_TOPIC_DLQ=prices.dlq

# Source
FREECRYPTO_WS_URL=wss://api.freecryptoapi.com/ws   # confirm exact URL at runtime
FREECRYPTO_API_KEY=changeme
WATCHLIST=BTCUSD,ETHUSD,SOLUSD

# Spark
SPARK_CHECKPOINT_DIR=/checkpoints/bronze
```

> `DATABASE_URL` is the single switch for the Neon migration story. Point it at a
> Neon connection string and nothing else changes.

---

## 5. The canonical data contract (referenced by every module)

Normalized tick message published to the `prices` topic (JSON, UTF-8):

```json
{
  "symbol": "BTCUSD",
  "exchange": "binance",
  "price": 68000.12,
  "volume": 1.234,
  "event_time": "2026-06-09T12:00:00.000Z",
  "ingested_at": "2026-06-09T12:00:00.050Z",
  "source": "freecryptoapi"
}
```

**Business key (idempotency key) everywhere:** `(symbol, exchange, event_time)`.
Types: `price`/`volume` are decimals; `event_time`/`ingested_at` are RFC-3339
UTC timestamps. Module 3 produces exactly this shape; Module 4 parses exactly this
shape; Modules 5–7 rely on the same key.

---

## 6. Sequential roadmap

1. **Module 1 — Infrastructure.** Compose stack up and healthy.
2. **Module 2 — Database & medallion schema.** Schemas + Bronze table + migrations.
3. **Module 3 — Ingestion → Kafka.** Live normalized ticks on `prices`, poison to DLQ.
4. **Module 4 — Stream processing.** Spark writes Bronze idempotently.
5. **Module 5 — Transforms (dbt).** Silver + Gold + data-quality tests.
6. **Module 6 — Orchestration (Airflow).** Scheduled transform/retention/backfill.
7. **Module 7 — API & dashboard.** FastAPI + React + end-to-end verification.

Definition of Done for the whole project: `make up` yields a working demo where
live data flows source → dashboard, a killed-and-restarted source produces no
duplicates, a malformed message lands in the DLQ, and a failing dbt test fails a
batch run visibly.
