# Architecture

CryptoStream is built as a sequence of independent modules, each with a
single responsibility and a clearly named output. This page walks the
end-to-end data flow and explains **why** each module exists the way it
does. Per-module detail (files, run, verify, env) lives in
[MODULES.md](MODULES.md).

---

## 30-second mental model

```
                          LIVE LANE
  ─────────────────────────────────────────────────────────────────
  source ──▶ ingest ──▶ Kafka ──▶ Spark ──▶ bronze.prices_raw
                                                      │
                                                      ▼
                          BATCH LANE
  ─────────────────────────────────────────────────────────────────
                       Airflow + dbt (every 5 min)
                                                      │
                                                      ▼
                              silver.stg_prices
                                                      │
                                                      ▼
                          gold.candles_1m / gold.candles_1m_ma
                                                      │
                                                      ▼
                          SERVING LANE
  ─────────────────────────────────────────────────────────────────
                       FastAPI ◀── React dashboard
```

- **Live lane** (modules 3–4): zero-shared-responsibility, runs every tick.
- **Batch lane** (modules 5–6): recomputes Silver + Gold from Bronze.
- **Serving lane** (module 7): read-only views of Gold.

---

## Module-by-module responsibilities

### Module 1 — Infrastructure

**Why:** a local stack must boot reproducibly on any machine. Compose
gives us one `docker compose up` and eleven services.

**What it provides:**
- Single-node Kafka in KRaft mode (no Zookeeper).
- Postgres for both the medallion DB and Airflow's metadata DB.
- Idle Spark base container (Module 4 uses `spark-submit` inside it).
- Airflow webserver + scheduler + one-shot `airflow-init` image-builder.
- Named volumes for `pg_data` and `spark_checkpoints` so restarts don't
  lose state.

**Key choices:**
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` — topic creation is explicit
  (`infra/kafka/create_topics.sh`) so we never accidentally have auto-named
  topics with the wrong partition count.
- `depends_on: condition: service_healthy` everywhere — the WebSocket
  producer doesn't try to publish until Kafka is actually serving.
- `x-airflow-common` YAML anchor keeps `POSTGRES_*` and `DBT_PROFILES_DIR`
  consistent across `airflow-init`, webserver, scheduler.

**Details:** [MODULE_1_INFRASTRUCTURE.md](MODULE_1_INFRASTRUCTURE.md)

---

### Module 2 — Database & medallion

**Why:** a single source-of-truth schema per layer. Bronze is the
landing zone, Silver is the typed view, Gold is the aggregation. The
business-key uniqueness invariant lives at the storage layer so every
upstream consumer can rely on it.

**What it provides:**
- Three schemas: `bronze`, `silver`, `gold`.
- `bronze.prices_raw` — typed landing table with:
  - `unique_business_key (symbol, exchange, event_time)`
  - `check (price > 0)`
  - an index `(symbol, event_time desc)` for downstream most-recent-first queries.
- A migration runner (`db/run_migrations.py`) that:
  - tracks applied files in `public.schema_migrations`,
  - wraps each file in its own transaction,
  - is safe to run repeatedly.

**Why a unique constraint on Bronze:** it lets the streaming upsert
(`INSERT ... ON CONFLICT DO NOTHING`) be the source of truth for
idempotency. Kafka offset resets, checkpoint corruption, or producer
redelivery all become harmless: the row is already there, the conflict
fires, the new write is dropped. This is the property Module 4's
verification step (`no_dupes = true`) checks.

**Details:** [MODULE_2_DATABASE.md](MODULE_2_DATABASE.md)

---

### Module 3 — Ingestion (WebSocket → Kafka)

**Why:** shield the rest of the system from the source's protocol
quirks. One normalizer, one producer, one DLQ.

**What it provides:**
- An async WebSocket client (`websockets` library) with exponential
  backoff + jitter reconnect.
- A Pydantic v2 `CanonicalTick` model that validates:
  - decimal prices (no float drift),
  - timezone-aware UTC `event_time`,
  - a fixed shape that downstream consumers can rely on.
- A `confluent-kafka` producer with `enable.idempotence=true` and
  `acks=all` so retries never duplicate.
- A per-message try/except that routes:
  - **parse/validation failure** → `prices.dlq` topic with the raw
    payload + reason as JSON,
  - **Kafka publish failure** → re-raise and let the outer reconnect
    loop handle it.

**Why idempotent producer:** combined with Bronze's unique constraint,
end-to-end exactly-once-into-Bronze behaviour follows without a two-phase
commit.

**Why two Kafka topics:** the canonical topic is for happy-path ticks;
the DLQ is for everything the normalizer can't parse. Bad data is a
first-class outcome and is auditable, not silenced.

**Details:** [MODULE_3_INGESTION.md](MODULE_3_INGESTION.md)

---

### Module 4 — Stream processing (Spark → Bronze)

**Why:** a checkpointed, restartable bridge from Kafka to Postgres.
Spark Structured Streaming is the only piece here that natively
consumes Kafka, tracks offsets per query, and writes via
`foreachBatch` — exactly what idempotent upsert needs.

**What it provides:**
- A `spark-submit` job (`streaming/stream_to_bronze.py`) that:
  - reads the `prices` Kafka topic from the latest offset (resumes
    from checkpoint thereafter),
  - parses `value` JSON against an explicit schema matching Module 0 §5,
  - drops parse-fail records silently (the DLQ's job),
  - writes each micro-batch into `bronze.prices_raw` via
    `foreachBatch` using a per-batch staging table +
    `INSERT ... ON CONFLICT (symbol, exchange, event_time) DO NOTHING`,
  - persists checkpoint state on the `spark_checkpoints` named volume.

**Why staging-table upsert over plain JDBC append:**
- Plain `df.write.jdbc(mode="append")` has no dedup path.
- `INSERT ... ON CONFLICT DO NOTHING` directly enforces the business
  key.
- A per-batch staging table (`bronze._prices_raw_stage_<hex>`) means
  concurrent workers never contend on a shared temp table; each batch
  has its own.

**Why `failOnDataLoss=false`:** tolerates Kafka retention / topic
deletion — important on a single-broker demo where retention expiry is
plausible between demos.

**Restart property:** kill `spark-submit`, restart it, the query
resumes from the last committed offset. The unique constraint absorbs
any re-delivered rows.

**Details:** [MODULE_4_STREAMING.md](MODULE_4_STREAMING.md)

---

### Module 5 — Transforms (dbt Silver + Gold)

**Why:** batch aggregations over Bronze should be **reproducible** and
**tested**. dbt gives us SQL models with a dependency graph, schema
tests, and a single CLI (`dbt build`).

**What it provides:**
- `silver.stg_prices` — typed view over `bronze.prices_raw` (column
  renaming, type casts, idempotent dedup via `select distinct`).
- `gold.candles_1m` — 1-minute OHLCV candles per `(symbol, exchange)`.
  Built with `date_trunc('minute', event_time)` and window aggregations.
- `gold.candles_1m_ma` — 20-period moving average over the close, per
  `(symbol, exchange)`.
- A literal-schema macro (`generate_schema_name.sql`) so `silver` and
  `gold` schemas are **not** prefixed with `dev_` (the dbt default).
- Schema tests (`not_null`, `unique`, `accepted_values`,
  `dbt_utils.expression_is_true` for candle bound invariants).

**Why literal schemas:** the API and dashboard query `silver.*` and
`gold.*` directly; a `dev_` prefix would break the serving tier.

**Why `table` materialisation:** every Airflow run rebuilds Silver +
Gold from Bronze. Storage is cheap; correctness is everything.

**Details:** [MODULE_5_TRANSFORMS.md](MODULE_5_TRANSFORMS.md)

---

### Module 6 — Orchestration (Airflow)

**Why:** dbt build should run on a schedule, retention should run
nightly, and a backfill should be triggerable with a date range.
Airflow gives us all three with the same operational interface.

**What it provides:**
- A custom Airflow image (`orchestration/Dockerfile`) extending
  `apache/airflow:2.9.3` with dbt-core + dbt-postgres pinned to 1.8
  (the same version Module 5 uses).
- Three DAGs:
  - **`transform_dag`** (`*/5 * * * *`): `dbt_deps` → `dbt build`. The
    primary loop.
  - **`retention_dag`** (`@daily`): `PythonOperator` that batched-DELETEs
    `bronze.prices_raw` rows older than `bronze_retention_days` (an
    Airflow Variable, default 7).
  - **`backfill_dag`** (manual, no schedule): takes `start_date` /
    `end_date` from `dag_run.conf`, writes them into a `--vars`
    file, and runs `dbt build --vars @$vars_file`.
- `airflow-init` (one-shot): migrates the metadata DB, creates the
  admin user, registers the `postgres_default` connection, and sets
  the `bronze_retention_days` Variable from `.env`.

**Why `*/5 * * * *`:** close to real-time for a demo, low enough
frequency to make 1-minute candle aggregation visible.

**Why PythonOperator for retention:** the SQL variant took parameters
via Jinja templating, which kept surprising us. A PythonOperator reads
the Variable at task time and loops over batched DELETEs with `ctid`.

**Details:** [MODULE_6_ORCHESTRATION.md](MODULE_6_ORCHESTRATION.md)

---

### Module 7 — API + Dashboard

**Why:** Gold tables are useless without a way to read them.
FastAPI gives us typed responses, OpenAPI for free, and a small enough
surface to maintain.

**What it provides:**
- A FastAPI app (`api/src/api/main.py`) with four routers:
  - `GET /health` — DB ping + last-bucket freshness; 503 if DB down.
  - `GET /prices/latest?symbols=…` — most-recent tick per
    `(symbol, exchange)`.
  - `GET /candles/{symbol}?interval=1m&limit=…` — ascending OHLCV
    candles from `gold.candles_1m`.
  - `GET /indicators/{symbol}/ma?limit=…` — MA(20) points from
    `gold.candles_1m_ma`.
- A psycopg connection pool (1–8 connections, configured via env).
- A React + Vite dashboard (`dashboard/`) that polls every 5 s:
  - latest prices table,
  - candle chart with MA overlay (composite-key merge so multi-exchange
    symbols don't collide),
  - health badge (green/amber/red).
- Nginx serves the static build; the JS bundle has `VITE_API_BASE` and
  `VITE_WATCHLIST` baked in at build time.

**Why a connection pool:** `psycopg-pool`'s `with pool.connection()`
context manager keeps connection checkout under 5 ms, well within the
5 s poll budget.

**Why `AbortController` in the dashboard:** when the user changes the
symbol selector mid-fetch, the in-flight requests are cancelled; React
state isn't updated with stale data.

**Details:** [MODULE_7_API_DASHBOARD.md](MODULE_7_API_DASHBOARD.md)

---

## Cross-cutting concerns

### Idempotency

| Layer              | Mechanism                                                      |
|--------------------|----------------------------------------------------------------|
| Kafka producer     | `enable.idempotence=true`, `acks=all`                          |
| Bronze upsert      | `INSERT ... ON CONFLICT (symbol, exchange, event_time) DO NOTHING` |
| dbt materialisations | `table` (full rebuild each tick)                              |
| Retention sweep    | DELETE by `event_time < cutoff`; idempotent                    |
| API reads          | DISTINCT ON, ORDER BY DESC LIMIT N (no write path)             |

### Config

All env-driven. `cryptostream_common.env._require` / `_optional_str`
give us one place for required-vs-optional semantics. Every module
loads its own config dataclass from those primitives; nothing else
reads `os.environ`.

### Observability

| Signal             | Where to look                                                |
|--------------------|--------------------------------------------------------------|
| Service health     | `make ps` (compose healthchecks)                             |
| Live stream status | `make airflow-runs DAG=transform_dag` + `/health` JSON        |
| Kafka offsets      | `make topics` + Spark logs (`make logs SERVICE=spark`)       |
| Bronze row count   | `make psql -- -c "select count(*) from bronze.prices_raw;"`   |
| API errors         | `make logs SERVICE=api` (structured JSON logs)               |
| Dashboard errors   | Browser console + the red error banner                       |

### Failure isolation

- **Ingestion dies** → Kafka loses the live source, but Bronze, Silver,
  Gold, API, and dashboard keep serving the last successful snapshot.
- **Spark dies** → Kafka backlog grows. Restart it and the checkpoint
  resumes; no duplicates because of the unique constraint.
- **dbt build fails** → Airflow marks the run red but doesn't roll
  back the previous (still-good) tables.
- **API dies** → Dashboard shows "API error: …" but the rest of the
  stack is unaffected.

---

## Why these technologies?

- **Kafka over a queue or Redis Streams:** native per-key ordering,
  per-partition offsets, replayability from any point — properties
  the rest of the architecture relies on.
- **Spark Structured Streaming over plain Kafka consumers:** the
  `foreachBatch` bridge to Postgres is the cleanest way to get
  micro-batch upserts without writing a custom consumer-group loop.
- **dbt over a hand-rolled SQL builder:** SQL models + a dependency
  graph + tests-on-every-run is what dbt does; we'd be reinventing it.
- **Airflow over cron + dbt CLI:** Airflow gives us retries, SLAs, a UI
  for backfills, and a clean place to add the retention sweep and any
  future DAGs without touching cron.
- **FastAPI over Flask / Django:** Pydantic v2 + type hints + native
  async + auto OpenAPI.
- **React + Vite over Next.js / CRA:** the dashboard has no SEO
  concerns, no server rendering needs, and Vite's build is fast enough
  that we don't notice the rebuild on env changes.

---

## Where to go next

- [QUICKSTART.md](QUICKSTART.md) — get the stack running.
- [ENV_REFERENCE.md](ENV_REFERENCE.md) — every var, every module.
- [MODULES.md](MODULES.md) — per-module detail.
- [OPERATIONS.md](OPERATIONS.md) — day-2 ops.
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — when something breaks.