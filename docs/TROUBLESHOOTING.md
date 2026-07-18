# Troubleshooting

Symptom → cause → fix cookbook. If you hit something not listed here,
check [OPERATIONS.md](OPERATIONS.md) and the module-specific pages in
[MODULES.md](MODULES.md).

---

## "make up" hangs / containers flap

**Symptom:** containers start, then immediately restart, and the cycle
never converges.

**Likely causes:**
1. Postgres not yet ready → everyone depending on it loops.
2. Kafka KRaft election taking > 15 s on cold boot.
3. Airflow DB not yet migrated.

**Fix:**
```bash
make ps                       # which service is unhealthy?
make logs SERVICE=postgres    # then SERVICE=kafka, SERVICE=airflow-init
```

Wait 30–60 s; cold-boot is normal. If a single service stays unhealthy
past 90 s, see the specific section below.

---

## Postgres

### "relation does not exist"

`make migrate` not run. Run it:
```bash
make migrate
```

### "permission denied for schema bronze"

`POSTGRES_USER` in `.env` doesn't match the user that owns the schema
(created by the first `make migrate`). Either:
- Run `make migrate` (which uses the same user), or
- Drop the schemas and re-run: `make psql -- -c "drop schema bronze cascade; drop schema silver cascade; drop schema gold cascade;" && make migrate`.

### pg_isready fails

```bash
make logs SERVICE=postgres
```

Most common: data dir corruption from a forced `docker kill`. Fix:
```bash
docker compose down postgres
docker volume rm cs_pg_data
docker compose up -d postgres
make migrate
```

---

## Kafka

### "kafka_versions" hangs / broker doesn't respond

```bash
make logs SERVICE=kafka
```

Cold boot is normal. If 60 s pass and still unhealthy:

```bash
docker compose restart kafka
```

KRaft single-node can take a moment to elect itself. Check the log for
`KafkaServer id=1 started` and `Cluster ID = ...`.

### "Topic prices not found"

`kafka-init` didn't run, or ran before Kafka was healthy. Re-run:

```bash
docker compose up kafka-init
```

### Messages aren't being consumed

Check consumer-group offsets:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --all-groups --describe
```

Lag > 0 means Spark is alive but behind. Lag = current means Spark
isn't running — `make stream-bg`.

---

## Ingestion

### Restart loop, "API key invalid"

```bash
docker compose logs ingestion | head -40
```

`FREECRYPTO_API_KEY=changeme` is a placeholder. Set a real key in
`.env` and `docker compose up -d --build ingestion`.

### All messages go to `prices.dlq`

The protocol shape doesn't match `FREECRYPTO_SUBSCRIBE_FMT`. Try
flipping it:

```env
FREECRYPTO_SUBSCRIBE_FMT=type_channels
```

…and rebuild: `docker compose up -d --build ingestion`. Check the
`prices.dlq` payload's `reason` field for hints.

### `KafkaTimeoutError` on publish

Kafka isn't ready when ingestion starts. Ingestion auto-retries; if
it loops, check Kafka's healthcheck (`make ps`).

---

## Spark

### `ClassNotFoundException` on first run

`--packages` is downloading Kafka + JDBC drivers. Takes 20–40 s on a
cold image. Re-run after waiting.

### Spark won't start because the checkpoint dir is gone

```bash
make logs SERVICE=spark | head -50
```

If you ran `make nuke` and lost `cs_spark_checkpoints`, the query
will re-consume from Kafka's `latest` offset (the compose default).
That's by design; no data loss.

### Bronze not populating

```bash
make logs SERVICE=spark | grep -i 'micro-batch'
```

You should see `Processed micro-batch of N rows` every ~10 s. If you
see "No new rows" repeatedly, ingestion is the bottleneck — go check
ingestion logs.

### `no_dupes = false` after a restart

This is a bug — the unique constraint should always hold. Run:

```bash
make psql -- -c "select symbol, exchange, event_time, count(*) from bronze.prices_raw
                 group by 1,2,3 having count(*) > 1;"
```

…and report it; there shouldn't be any rows.

---

## dbt

### "Could not find profile named 'cryptostream'"

`DBT_PROFILES_DIR` not pointing at `transforms/`. In Compose:

```bash
docker compose run --rm dbt bash -lc 'echo $DBT_PROFILES_DIR && ls $DBT_PROFILES_DIR'
```

Should print `/dbt` and list `profiles.yml`. On the host, run with
`DBT_PROFILES_DIR=./transforms`.

### "relation 'bronze.prices_raw' does not exist"

`make migrate` not run. Run it.

### "Compilation Error in model stg_prices"

Sources drift. Check:

```bash
docker compose run --rm dbt bash -lc 'dbt compile --no-version-check'
```

…and inspect the generated SQL in `transforms/target/compiled/...`.

### dbt test fails on `assert_candle_bounds`

A tick made it into Bronze with `high < open`. Investigate:

```bash
make psql -- -c "select * from bronze.prices_raw where price <= 0 limit 5;"
```

If something is `price <= 0`, the Bronze check constraint should have
rejected it upstream. If you're seeing it in Gold, there's a bug
between Bronze and dbt's `silver.stg_prices` projection.

---

## Airflow

### "Webserver is not yet up"

```bash
make logs SERVICE=airflow-webserver
```

The webserver healthcheck polls `/health` for `"metadatabase"`. If
that string isn't in the response yet, give it 30 s more. If it never
appears, check that Postgres has the `airflow` DB (`make psql -- -c
"\l"`).

### DAGs not showing up

The DAGs folder is mounted at `/opt/airflow/dags`. The scheduler scans
every 30 s. If they still don't appear:

```bash
docker compose logs airflow-scheduler | grep -i 'dag'
```

If you see `ImportError`, fix the Python in `dags/`. The image is
rebuilt only when its Dockerfile changes; for DAG-only edits you
don't need to rebuild — just edit and the scheduler picks it up.

### "Variable bronze_retention_days does not exist"

Re-run the one-shot init:

```bash
docker compose up airflow-init
```

### `retention_dag` deletes 0 rows

Either Bronze is empty or your retention window is longer than the
data age. Set the Variable to a smaller value:

```bash
docker compose exec airflow-scheduler \
  airflow variables set bronze_retention_days 1
```

---

## API

### `/health` returns 503

```bash
make logs SERVICE=api | tail -30
```

Most common: DB unreachable from inside the `api` container. Check
`make ps SERVICE=postgres` and the API's `DATABASE_URL`.

### `/candles/BTCUSD` returns `[]`

Gold is empty. Run:

```bash
make dbt
```

…or wait for the next `transform_dag` tick.

### Dashboard shows "API error: …"

```bash
make logs SERVICE=api
```

The dashboard polls every 5 s. Once the API recovers, the banner
disappears on the next poll.

### CORS error in browser console

Origin not in `CORS_ORIGINS`. Update `.env` and:

```bash
docker compose up -d --build api
```

### Dashboard symbols selector empty

`VITE_WATCHLIST` is empty or wasn't baked in. Update `.env` and:

```bash
docker compose build dashboard
docker compose up -d dashboard
```

---

## Performance

### API responses > 200 ms

Likely causes:
1. `DB_POOL_MAX` too low → checkout queueing. Bump it.
2. `gold.candles_1m` not indexed. It currently isn't because `bucket`
   is part of the primary key; if you add indexes, profile first.
3. Dashboard polling too aggressively. Default is 5 s — comfortable
   for a single-instance demo.

### Spark micro-batch takes > 10 s

Either:
- `SPARK_TRIGGER_INTERVAL_S` too small (default 10 s is fine).
- Bronze table has heavy index churn after each upsert; the index
  helps reads but slows writes. Acceptable trade-off for the demo.

---

## "I just want to start over"

```bash
make nuke               # destructive: drops pg_data + spark_checkpoints
cp .env.example .env    # reset env
make up
make stream-bg
make dbt
```

---

## Where to go next

- [QUICKSTART.md](QUICKSTART.md) — get the stack running.
- [OPERATIONS.md](OPERATIONS.md) — day-2 ops.
- [ENV_REFERENCE.md](ENV_REFERENCE.md) — every var.
- [ARCHITECTURE.md](ARCHITECTURE.md) — why the system is shaped this way.