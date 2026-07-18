# Environment reference

Every runtime value CryptoStream reads from `.env`. Defaults come
from `.env.example`; you can override any of them in `.env`. The file
is loaded by Docker Compose (which passes the values into each
service's `environment:` block) and by `make` (via `include .env`).

> **Security:** `.env` is in `.gitignore`. Never commit it. `.env.example`
> is the safe-to-share template.

---

## How to use this page

The table is grouped by **owner module**. For each variable:

- **Default** — what `.env.example` ships with.
- **Where it's read** — file path that calls `os.environ.get(...)`.
- **What happens if it's wrong/missing** — observed behaviour.
- **Sample** — what a real value looks like.

If you're just trying to make the demo run, the only value you may
need to change is `FREECRYPTO_API_KEY`; everything else has safe
defaults.

---

## Module 1 — Infrastructure (compose)

These drive Docker Compose itself and the `x-airflow-common` YAML
anchor that all three Airflow services inherit.

| Var                                | Default                                          | Where read                                              | Failure mode                                | Sample                                                  |
|------------------------------------|--------------------------------------------------|---------------------------------------------------------|---------------------------------------------|---------------------------------------------------------|
| `POSTGRES_USER`                    | `cryptostream`                                   | `docker-compose.yml` (compose env); `postgres` service  | DB init fails                               | `cryptostream`                                          |
| `POSTGRES_PASSWORD`                | `cryptostream`                                   | same                                                    | same                                        | (use a real password for any non-local deploy)          |
| `POSTGRES_DB`                      | `cryptostream`                                   | same                                                    | same                                        | `cryptostream`                                          |
| `DATABASE_URL`                     | `postgresql://cryptostream:cryptostream@postgres:5432/cryptostream` | `db/run_migrations.py`, `streaming/src/streaming/config.py`, `api/src/api/config.py` | migrations fail; Spark→Bronze fails; API fails | `postgresql://user:pass@host:5432/db` (Neon: add `?sslmode=require`) |
| `KAFKA_BOOTSTRAP`                  | `kafka:9092`                                     | `ingestion/src/ingestion/config.py`, `streaming/...`, `docker-compose.yml` | ingestion + spark can't connect             | `localhost:9092` (from host), `kafka:9092` (from compose) |
| `KAFKA_TOPIC_PRICES`               | `prices`                                         | `docker-compose.yml` (kafka-init), `ingestion`, `streaming` | topic mismatch; messages go to wrong place  | `prices`                                                |
| `KAFKA_TOPIC_DLQ`                  | `prices.dlq`                                     | `docker-compose.yml`, `ingestion`                       | same                                        | `prices.dlq`                                            |
| `AIRFLOW__CORE__EXECUTOR`          | `LocalExecutor`                                  | `docker-compose.yml` (x-airflow-common anchor)          | Airflow won't start                         | `LocalExecutor`                                         |
| `AIRFLOW__CORE__LOAD_EXAMPLES`     | `False`                                          | same                                                    | Airflow UI clutters with stock examples     | `False`                                                 |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://cryptostream:cryptostream@postgres:5432/airflow` | same                                            | Airflow DB migrate fails                    | (use the same creds; the `airflow` DB is auto-created)   |

> **Note:** Compose also auto-creates a database called `airflow` via
> `infra/postgres-init/01-create-airflow-db.sql`, which runs only on
> first Postgres boot when the data dir is empty.

---

## Module 2 — Database & medallion

No env vars. Module 2 is pure SQL; the schemas and tables are
literal-named in `db/migrations/`. The runner reads `DATABASE_URL`
which is documented under Module 1.

---

## Module 3 — Ingestion

| Var                          | Default                                   | Where read                                                          | Failure mode                                                | Sample                                |
|------------------------------|-------------------------------------------|---------------------------------------------------------------------|-------------------------------------------------------------|---------------------------------------|
| `FREECRYPTO_WS_URL`          | `wss://api.freecryptoapi.com/ws`          | `ingestion/src/ingestion/config.py` (`Config.from_env`)             | `ConfigError` on import → ingestion exits                   | `wss://api.freecryptoapi.com/ws`      |
| `FREECRYPTO_API_KEY`         | `changeme`                                | same                                                                | WS handshake rejects → reconnect loop                       | (your real FreeCryptoAPI key)         |
| `WATCHLIST`                  | `BTCUSD,ETHUSD,SOLUSD`                    | same                                                                | `ConfigError` if empty after parsing                        | `BTCUSD,ETHUSD,SOLUSD,XRPUSD`         |
| `FREECRYPTO_SUBSCRIBE_FMT`   | `action_symbols`                          | same                                                                | `ConfigError` if not in `{action_symbols, type_channels}`    | `action_symbols`                      |
| `SUBSCRIBE_TIMEOUT_S`        | `10`                                      | same                                                                | subscribe message times out                                 | `10`                                  |
| `RECONNECT_INITIAL_S`        | `1`                                       | same                                                                | reconnect storm                                             | `1`                                   |
| `RECONNECT_CAP_S`            | `30`                                      | same                                                                | capped backoff too short                                    | `30`                                  |
| `KAFKA_BOOTSTRAP`            | (see Module 1)                            | same                                                                | (see Module 1)                                              | (see Module 1)                        |
| `KAFKA_TOPIC_PRICES`         | `prices`                                  | same                                                                | (see Module 1)                                              | (see Module 1)                        |
| `KAFKA_TOPIC_DLQ`            | `prices.dlq`                              | same                                                                | bad payloads would silently die instead of going to DLQ     | (see Module 1)                        |

`FREECRYPTO_API_KEY=changeme` will cause a reconnect loop on first
WS connect. For the demo to produce live data, **set this to a real
key**. Everything else in CryptoStream will run without it; just no
ticks flow.

---

## Module 4 — Stream processing

| Var                        | Default                                  | Where read                                          | Failure mode                                          | Sample                              |
|----------------------------|------------------------------------------|-----------------------------------------------------|-------------------------------------------------------|-------------------------------------|
| `DATABASE_URL`             | (see Module 1)                           | `streaming/src/streaming/config.py`                 | (see Module 1)                                       | (see Module 1)                      |
| `KAFKA_BOOTSTRAP`          | (see Module 1)                           | same                                                | (see Module 1)                                       | (see Module 1)                      |
| `KAFKA_TOPIC_PRICES`       | `prices`                                 | same                                                | reads wrong topic                                     | `prices`                            |
| `SPARK_CHECKPOINT_DIR`     | `/checkpoints/bronze`                    | same                                                | checkpoint state lost on container restart if changed | `/checkpoints/bronze`               |
| `SPARK_TRIGGER_INTERVAL_S` | `10 seconds`                             | `streaming/src/streaming/stream_to_bronze.py`       | micro-batches too fast → DB pressure; too slow → lag   | `10 seconds`                        |
| `BRONZE_TABLE`             | `bronze.prices_raw`                      | same                                                | upserts land in the wrong table                       | `bronze.prices_raw`                 |

---

## Module 5 — Transforms (dbt)

dbt reads `POSTGRES_*` from env (not `DATABASE_URL`). The
`transforms/profiles.yml` template uses dbt's `env_var()` Jinja helper
with sensible fallbacks.

| Var                | Default        | Where read                                  | Failure mode                              | Sample      |
|--------------------|----------------|---------------------------------------------|-------------------------------------------|-------------|
| `POSTGRES_HOST`    | `postgres`     | `transforms/profiles.yml`                   | dbt can't connect                         | `postgres`  |
| `POSTGRES_PORT`    | `5432`         | same                                        | same                                      | `5432`      |
| `POSTGRES_USER`    | `cryptostream` | same                                        | auth failure                              | `cryptostream` |
| `POSTGRES_PASSWORD`| `cryptostream` | same                                        | auth failure                              | (real pass) |
| `POSTGRES_DB`      | `cryptostream` | same                                        | db doesn't exist                          | `cryptostream` |
| `DBT_PROFILES_DIR` | `/dbt`         | `docker-compose.yml` (`dbt` service)        | dbt can't find profiles.yml               | `/dbt` (container), `./transforms` (host) |

For Neon: change the `POSTGRES_*` values in `.env`, then run
`make dbt-host` (the compose `dbt` service is hardcoded to the local
`postgres` container, so use the host target for external DBs).

---

## Module 6 — Orchestration (Airflow)

| Var                       | Default                                                       | Where read                                          | Failure mode                                          | Sample                                |
|---------------------------|---------------------------------------------------------------|-----------------------------------------------------|-------------------------------------------------------|---------------------------------------|
| `AIRFLOW_ADMIN_USER`      | `admin`                                                       | `docker-compose.yml` (`airflow-init` bash)          | admin user not created; can't log into UI             | `admin`                               |
| `AIRFLOW_ADMIN_PASSWORD`  | `admin`                                                       | same                                                | same                                                  | (use a real password for any deploy)  |
| `POSTGRES_HOST`           | `postgres`                                                    | `orchestration/dags/_common.py` (`dbt_env()`)       | dbt can't connect                                     | `postgres`                            |
| `POSTGRES_PORT`           | `5432`                                                        | same                                                | same                                                  | `5432`                                |
| `POSTGRES_USER`           | `cryptostream`                                                | same                                                | auth failure                                          | `cryptostream`                        |
| `POSTGRES_PASSWORD`       | `cryptostream`                                                | same                                                | auth failure                                          | (real pass)                           |
| `POSTGRES_DB`             | `cryptostream`                                                | same                                                | db doesn't exist                                      | `cryptostream`                        |
| `DBT_PROFILES_DIR`        | `/opt/airflow/transforms`                                     | `docker-compose.yml` (x-airflow-common anchor)       | dbt-invoking tasks can't find profiles.yml            | `/opt/airflow/transforms`             |
| `BRONZE_RETENTION_DAYS`   | `7`                                                           | `docker-compose.yml` (`airflow-init` writes Variable) | retention_dag deletes more or less than expected      | `7` (integer days)                    |

The `bronze_retention_days` value is written into the Airflow Variable
of the same name on first boot. Once the Variable exists, changing
`.env` does **not** propagate — update the Variable via the UI or
`airflow variables set`.

---

## Module 7 — API + Dashboard

### API

| Var                 | Default                                                          | Where read                                | Failure mode                                        | Sample                              |
|---------------------|------------------------------------------------------------------|-------------------------------------------|-----------------------------------------------------|-------------------------------------|
| `DATABASE_URL`      | (see Module 1)                                                   | `api/src/api/config.py`                   | (see Module 1)                                     | (see Module 1)                      |
| `GOLD_SCHEMA`       | `gold`                                                           | same                                      | 404 on `/prices/latest` if dbt wrote elsewhere     | `gold`                              |
| `CORS_ORIGINS`      | `http://localhost:5173,http://localhost:8000`                    | same                                      | browser blocks dashboard requests with CORS error   | `https://crypto.example.com`        |
| `DB_POOL_MIN`       | `1`                                                              | same                                      | cold-start latency spike                           | `1`                                 |
| `DB_POOL_MAX`       | `8`                                                              | same                                      | pool exhaustion under load                         | `8`                                 |
| `DB_POOL_TIMEOUT_S` | `10` (default inside config, not in `.env.example`)             | same                                      | requests fail with timeout                         | `10`                                |
| `API_HOST`          | `0.0.0.0`                                                        | `api/src/api/main.py` (`__main__`)        | API not reachable from host                        | `0.0.0.0`                           |
| `API_PORT`          | `8000`                                                           | same                                      | port mismatch with compose mapping                 | `8000`                              |
| `API_RELOAD`        | unset                                                            | same                                      | dev-only auto-reload off                           | `1` (truthy) for local dev          |

### Dashboard (build-time)

These are **baked into the JS bundle** at `docker compose build` time
(Vite reads `import.meta.env.VITE_*`). Changing them requires a rebuild.

| Var                | Default                       | Where read                | Failure mode                                      | Sample                       |
|--------------------|-------------------------------|---------------------------|---------------------------------------------------|------------------------------|
| `VITE_API_BASE`    | `http://localhost:8000`       | `dashboard/src/api.js`    | dashboard calls wrong host → "API error: …"       | `https://api.example.com`    |
| `VITE_WATCHLIST`   | `BTCUSD,ETHUSD,SOLUSD`       | `dashboard/src/App.jsx`   | symbol selector missing entries                   | `BTCUSD,ETHUSD,SOLUSD,XRPUSD`|

To rebuild the dashboard after changing these:

```bash
docker compose build dashboard
docker compose up -d dashboard
```

---

## Variables by service (quick reference)

```
postgres           POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
kafka-init         KAFKA_BOOTSTRAP, KAFKA_TOPIC_PRICES, KAFKA_TOPIC_DLQ
spark              SPARK_CHECKPOINT_DIR, DATABASE_URL, KAFKA_BOOTSTRAP, KAFKA_TOPIC_PRICES
airflow-*          AIRFLOW_*, POSTGRES_*, DBT_PROFILES_DIR, BRONZE_RETENTION_DAYS
ingestion          FREECRYPTO_*, WATCHLIST, KAFKA_*
dbt                DBT_PROFILES_DIR, POSTGRES_*
api                DATABASE_URL, GOLD_SCHEMA, CORS_ORIGINS, DB_POOL_*
dashboard (build)  VITE_API_BASE, VITE_WATCHLIST
```

---

## Validating your `.env`

Quick sanity check after editing `.env`:

```bash
# Make sure every var resolves to a non-empty value
set -a; . ./.env; set +a
env | grep -E '^(POSTGRES|KAFKA|FREECRYPTO|WATCHLIST|SPARK_|GOLD_|CORS_|DB_POOL|VITE_|AIRFLOW_|BRONZE_|DATABASE_URL)' | sort
```

For per-module verification (e.g. "is `FREECRYPTO_API_KEY` set to a
real key?"), see the relevant module page:

- [MODULE_1_INFRASTRUCTURE.md](MODULE_1_INFRASTRUCTURE.md)
- [MODULE_3_INGESTION.md](MODULE_3_INGESTION.md)
- [MODULE_4_STREAMING.md](MODULE_4_STREAMING.md)
- [MODULE_5_TRANSFORMS.md](MODULE_5_TRANSFORMS.md)
- [MODULE_6_ORCHESTRATION.md](MODULE_6_ORCHESTRATION.md)
- [MODULE_7_API_DASHBOARD.md](MODULE_7_API_DASHBOARD.md)