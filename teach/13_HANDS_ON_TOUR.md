# 13 — Hands-on tour (guided exercise)

The fastest way to internalise CryptoStream is to **poke the
running system**. This page is a guided tour: every step tells
you what to do, what you should see, and why.

Before starting, make sure the stack is running:

```bash
cd /path/to/CryptoStream
cp .env.example .env                 # if you haven't already
make up                              # ~30–60 s to healthy
make stream-bg                       # start Spark → Bronze
sleep 30                             # let some data flow
```

Throughout this tour, leave a terminal open with `make logs` (no
filter) so you can see the system as you poke it.

---

## Tour 1 — Watch a tick go through the system

**Goal:** See one tick travel from ingestion to Bronze.

```bash
# Open a tail of Bronze (count grows over time)
watch -n 2 'make psql -- -c "select count(*) from bronze.prices_raw;"'

# Open the dashboard
open http://localhost:5173
```

You should see:

- The Bronze count slowly increasing (~1 row per second per
  symbol).
- The dashboard's chart updating every 5 seconds with new candles.

**Why:** This is the live lane in action.

---

## Tour 2 — Inspect Bronze

```bash
# Schema
make psql -- -c "\d bronze.prices_raw"

# Latest 5 rows
make psql -- -c "select symbol, exchange, price, event_time
                 from bronze.prices_raw
                 order by event_time desc limit 5;"

# Per-symbol counts
make psql -- -c "select symbol, count(*) from bronze.prices_raw
                 group by symbol order by symbol;"

# Spot any price anomalies
make psql -- -c "select * from bronze.prices_raw
                 where price <= 0 or volume < 0 limit 5;"
```

You should see:

- A typed schema with the unique constraint and the index.
- Most-recent rows with current UTC timestamps.
- Roughly even counts per symbol (BTC, ETH, SOL).
- No rows in the anomaly query (the check constraint enforces
  this).

---

## Tour 3 — Inspect Kafka

```bash
# List topics
make topics

# Tail the prices topic
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic prices \
  --from-beginning \
  --max-messages 5

# Check the DLQ (should usually be empty)
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic prices.dlq \
  --from-beginning \
  --max-messages 5

# Consumer group offsets
docker compose exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --all-groups --describe
```

You should see:

- Two topics: `prices` and `prices.dlq`.
- A stream of valid JSON messages on `prices`.
- Either nothing or some "expected" failures on `prices.dlq`
  (depends on your FreeCryptoAPI key).
- Spark's consumer group with current offsets.

---

## Tour 4 — Verify the idempotency property

This is the canonical Module 4 verification. **Restart the
stream job and confirm no duplicates appear.**

```bash
# Take a "before" snapshot
make psql -- -c "select count(*) as c1 from bronze.prices_raw;"

# Kill the streaming job
docker compose exec -d spark pkill -f stream_to_bronze || true
sleep 5

# Check it's gone
docker compose exec spark ps aux | grep spark_to_bronze || echo "not running"

# Restart it
make stream-bg
sleep 30

# Verify uniqueness
make psql -- -c "select count(*) - <c1> as delta,
                       count(*) = count(distinct (symbol, exchange, event_time)) as no_dupes
                  from bronze.prices_raw;"
```

You should see:

- `delta` is some positive number (Bronze grew during restart).
- `no_dupes` is `t` (true).

**Why:** This is the proof that end-to-end idempotency works. The
unique constraint absorbs any re-delivered rows.

---

## Tour 5 — Run dbt manually

```bash
# One-shot dbt build (same as the Airflow DAG does)
make dbt
```

You should see:

- A `dbt deps` step (caches dbt-utils).
- Each model building in order:
  - `silver.stg_prices`
  - `gold.candles_1m`
  - `gold.candles_1m_ma`
- All tests passing.

Then:

```bash
# Inspect the rendered SQL for the candle model
docker compose run --rm dbt bash -lc \
  'dbt compile --no-version-check --select candles_1m && \
   cat target/compiled/cryptostream/models/marts/candles_1m.sql'

# Look at the latest candles
make psql -- -c "select bucket, symbol, open, close, volume
                 from gold.candles_1m
                 where symbol = 'BTCUSD'
                 order by bucket desc limit 5;"

# Look at the latest MA
make psql -- -c "select bucket, symbol, close, ma_20
                 from gold.candles_1m_ma
                 where symbol = 'BTCUSD'
                 order by bucket desc limit 5;"
```

You should see:

- The compiled SQL with `{{ ref('stg_prices') }}` resolved to
  `silver.stg_prices`.
- Candles with monotonically increasing `bucket` (until "now").
- MA values smoothly varying close to recent closes.

---

## Tour 6 — Hit the API directly

```bash
# Health
curl -sf localhost:8000/health | python -m json.tool

# Latest prices
curl -sf 'localhost:8000/prices/latest?symbols=BTCUSD,ETHUSD' \
  | python -m json.tool

# Candles
curl -sf 'localhost:8000/candles/BTCUSD?interval=1m&limit=5' \
  | python -m json.tool

# MA
curl -sf 'localhost:8000/indicators/BTCUSD/ma?limit=5' \
  | python -m json.tool

# 404 for a non-existent symbol
curl -sf 'localhost:8000/candles/NOPE?interval=1m&limit=5'

# Open the auto-generated docs
open http://localhost:8000/docs
```

You should see:

- `health` returns `{db: "ok", gold_freshness_seconds: <int>,
  status: "ok"}`.
- `latest` returns rows with `price` as strings (Decimal precision).
- `candles` returns ascending OHLCV rows.
- `ma` returns MA(20) points.
- `NOPE` returns `{candles: []}` (empty list, not 404).
- The Swagger UI at `/docs` is interactive.

---

## Tour 7 — Trigger Airflow DAGs

```bash
# List DAGs
make airflow-list

# Trigger transform_dag manually
make airflow-trigger DAG=transform_dag

# Watch its logs
make airflow-logs

# Trigger a backfill (any date range)
make airflow-trigger DAG=backfill_dag \
  CONF='{"start_date":"2026-06-01","end_date":"2026-06-02"}'

# Trigger retention manually
make airflow-trigger DAG=retention_dag
```

In the UI at <http://localhost:8080> (`admin`/`admin`), watch
the runs turn green.

---

## Tour 8 — Test a failure recovery

### What happens if Postgres restarts?

```bash
# Snapshot Bronze
make psql -- -c "select count(*) from bronze.prices_raw;"

# Restart Postgres
docker compose restart postgres

# Wait for it to come back
sleep 10
make ps

# Spark will retry failed batches automatically
make logs SERVICE=spark | tail -30

# Confirm Bronze is intact
make psql -- -c "select count(*) from bronze.prices_raw;"
```

You should see:

- Postgres restarts (~5–10 s).
- Spark logs show a few failed batches, then successful retries.
- Bronze count is the same (the volume `pg_data` kept the data).

### What happens if you change the watchlist?

```bash
# Edit .env
echo "WATCHLIST=BTCUSD,ETHUSD,SOLUSD,XRPUSD" >> .env

# Restart ingestion
docker compose up -d --build ingestion

# Wait for new ticks
sleep 30

# Confirm XRPUSD is now flowing
make psql -- -c "select symbol, count(*) from bronze.prices_raw
                 group by symbol order by symbol;"
```

You should see:

- Ingestion reconnects to the WS server.
- Bronze starts gaining XRPUSD rows.
- Spark and the rest of the pipeline are unchanged.

### What happens if you drop a Gold table?

```bash
make psql -- -c "drop table gold.candles_1m_ma;"

# Either wait 5 min for the next Airflow tick,
# or run dbt manually
make dbt

# Confirm it's back
make psql -- -c "select count(*) from gold.candles_1m_ma;"
```

You should see:

- The dashboard's MA line vanishes briefly.
- dbt rebuilds the table.
- The dashboard recovers on the next poll.

---

## Tour 9 — Add a new symbol end-to-end

The fastest way to see all 11 services cooperate:

1. Add `XRPUSD` to `WATCHLIST` in `.env`.
2. Add it to `VITE_WATCHLIST` in `.env`.
3. Restart ingestion: `docker compose up -d --build ingestion`.
4. Wait 1 minute; check `make psql -- -c "select symbol, count(*) from bronze.prices_raw group by symbol;"`.
5. Run `make dbt` to refresh Gold.
6. Rebuild the dashboard: `docker compose build dashboard && docker compose up -d dashboard`.
7. Open `http://localhost:5173` — XRPUSD is in the dropdown.

This exercise touches every layer: source, ingestion, Kafka,
Spark, Postgres, dbt, API, dashboard.

---

## Tour 10 — Read a code file with this tutorial open

Pick one file and read it top-to-bottom with these questions:

For `streaming/src/streaming/upsert.py`:

- What's `REQUIRED_FIELDS`? Why is it exported?
- Why does `_coerce_raw` raise `TypeError` instead of `ValueError`?
- Why does `_stage_name` use `secrets.token_hex(6)`?
- What does the comment on line X explain that the code doesn't?

For `dashboard/src/App.jsx`:

- What does the `useEffect` dependency array say about when it
  re-runs?
- Why does the cleanup function call `ctrl.abort()`?
- What happens if `Promise.all` rejects?

For `transforms/models/marts/candles_1m.sql`:

- What does `first(price order by event_time)` mean?
- Why is the materialisation `table` and not `incremental`?

For `orchestration/dags/retention_dag.py`:

- Why is the loop using `ctid` instead of `event_time` directly?
- What happens when `cur.rowcount == 0`?

---

## What's next?

- [14_GLOSSARY.md](14_GLOSSARY.md) — bookmark for reference.
- [../docs/QUICKSTART.md](../docs/QUICKSTART.md) — for first-time
  setup.
- [../docs/TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md) — when
  something breaks.