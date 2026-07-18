# Quickstart

Get the full stack running in ~10 minutes (cold boot), ~2 minutes
subsequent boots.

## 0. Prerequisites

| Tool        | Version     | Why                          |
|-------------|-------------|------------------------------|
| Docker      | 24+         | Compose v2 (the `docker compose` subcommand) |
| Docker Compose | v2 (bundled) | Orchestrates the 11 services |
| Make        | any         | Convenience targets          |
| ~4 GB RAM   | free        | Spark + Airflow + Postgres + Kafka |

No Python, Node, dbt, or Airflow on the host required — everything
runs inside containers. The only host-side requirements are the ones
listed above.

---

## 1. Clone and configure

```bash
git clone <your-repo-url> cryptostream
cd cryptostream
cp .env.example .env
```

Open `.env` and (optionally) customise:

| Var                   | Default                          | When to change                           |
|-----------------------|----------------------------------|------------------------------------------|
| `POSTGRES_USER/PASSWORD/DB` | `cryptostream`            | Only if you want a different DB name     |
| `DATABASE_URL`        | `postgresql://...postgres:5432/cryptostream` | If you change the DB user/pass     |
| `FREECRYPTO_WS_URL`   | `wss://api.freecryptoapi.com/ws` | Source-specific                         |
| `FREECRYPTO_API_KEY`  | `changeme`                       | **Required** for live ticks; demo runs without but ingestion exits fast |
| `WATCHLIST`           | `BTCUSD,ETHUSD,SOLUSD`           | Any comma-separated symbols              |
| `CORS_ORIGINS`        | `http://localhost:5173,...`      | If you serve the dashboard elsewhere     |
| `VITE_API_BASE`       | `http://localhost:8000`          | Baked into the JS bundle at build time   |
| `VITE_WATCHLIST`      | `BTCUSD,ETHUSD,SOLUSD`           | Baked into the JS bundle at build time   |

Full per-variable breakdown: [ENV_REFERENCE.md](ENV_REFERENCE.md).

`.env` is git-ignored. Don't commit it.

---

## 2. Bring the stack up

```bash
make up                # docker compose up -d --build
```

This builds and starts:

- `postgres` — Postgres 16 with the medallion schemas and the Airflow
  metadata DB (`airflow`) created via `infra/postgres-init/`.
- `kafka` — single-node KRaft, no Zookeeper.
- `kafka-init` — one-shot topic creator; exits 0 on success.
- `spark` — idle `apache/spark:3.5.1` base image; Module 4 `spark-submit`s into it.
- `airflow-init` — runs `airflow db migrate`, creates the admin user, the
  `postgres_default` connection, and the `bronze_retention_days` Variable.
- `airflow-webserver` + `airflow-scheduler`.
- `ingestion` — the WebSocket → Kafka producer.
- `dbt` — a one-shot container, used by `make dbt`.
- `api` — FastAPI serving the Gold tables.
- `dashboard` — React + nginx.

Health checks gate startup; the whole graph is healthy in ~30–60 s on
a cold host. Watch progress with `make ps` and `make logs`.

---

## 3. Migrate the medallion DB

The Postgres container creates the `bronze`/`silver`/`gold` schemas on
first boot **only if** `db/migrations/` is in
`/docker-entrypoint-initdb.d/`. The current setup runs migrations via
the migration runner so you can re-apply safely:

```bash
make migrate           # runs db/run_migrations.py against local Postgres
```

If you point `DATABASE_URL` at an external Postgres (Neon), use:

```bash
make migrate-host      # runs the same migrations from the host
```

Both are idempotent (`create table if not exists` + a
`public.schema_migrations` ledger the runner dedups on).

---

## 4. Start the stream and the batch lane

```bash
make stream-bg         # Module 4: Spark → Bronze (background)
make dbt               # Module 5: one-shot dbt build (populates Silver + Gold immediately)
```

`transform_dag` will also rebuild Silver + Gold every 5 minutes going
forward (see [MODULE_6_ORCHESTRATION](MODULE_6_ORCHESTRATION.md)).

---

## 5. Verify

After ~30 s of data flowing, run all four checks at once:

```bash
make psql -- -c "select count(*) from bronze.prices_raw;"
make psql -- -c "select count(*) from silver.stg_prices;"
make psql -- -c "select count(*) from gold.candles_1m;"
curl -sf localhost:8000/health
```

Each should return a number (the first three) or `{"status":"ok",...}`
(the last). Then open:

| URL                            | What you'll see                               |
|--------------------------------|-----------------------------------------------|
| <http://localhost:5173>        | Live dashboard: prices + candle chart + badge |
| <http://localhost:8000/docs>   | OpenAPI explorer                              |
| <http://localhost:8080>        | Airflow UI (`admin` / `admin`)                |

The dashboard's health badge goes green within ~2 minutes of starting
the stream; otherwise amber (warming up).

---

## 6. Optional: bring up Airflow separately

`make up` already starts Airflow. But if you want to (re)build the
custom Airflow image and start just the airflow services:

```bash
make airflow-up
```

The image is built from `orchestration/Dockerfile` (extends
`apache/airflow:2.9.3`, pins dbt-postgres 1.8 + the matching
constraints file).

---

## 7. Tear down

| Goal                            | Command                          |
|---------------------------------|----------------------------------|
| Stop containers (keep volumes)  | `make down`                      |
| Stop + wipe ALL data            | `make nuke` *(destructive)*      |
| Wipe just Spark checkpoints     | `docker volume rm cs_spark_checkpoints` |
| Wipe just Postgres              | `docker volume rm cs_pg_data`    |

`make nuke` runs `docker compose down -v`; both `pg_data` and
`spark_checkpoints` named volumes are dropped, so every table and
every Kafka topic offset is gone.

---

## What's next?

- Customising? Start at [ENV_REFERENCE.md](ENV_REFERENCE.md).
- Hitting a wall? [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
- Want to know **why** before **how**? [ARCHITECTURE.md](ARCHITECTURE.md).
- Looking for a specific module? [MODULES.md](MODULES.md) is the index.