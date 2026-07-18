# 11 — How data flows (end-to-end walkthrough)

You've read about Postgres, Kafka, Spark, dbt, Airflow, FastAPI,
and React. Now let's trace one BTCUSD price tick from the moment
it leaves the crypto exchange to the moment it appears as a dot
on the dashboard.

This is the **integration story** — every concept from the
previous pages working together.

---

## The cast

| Actor | Where it runs | Its job |
|-------|---------------|---------|
| FreeCryptoAPI | The internet | Source: pushes price ticks over WebSocket |
| `ingestion` | Docker container | Receives ticks, validates, publishes to Kafka |
| `kafka` | Docker container | Holds the messages durably |
| `spark` | Docker container | Reads Kafka, writes to Bronze (idempotent) |
| `postgres` | Docker container | Stores Bronze, Silver, Gold tables |
| `airflow-scheduler` | Docker container | Triggers `transform_dag` every 5 min |
| `dbt` | Docker container | Rebuilds Silver + Gold from Bronze |
| `api` | Docker container | Reads Gold, answers HTTP questions |
| `dashboard` | Docker container | Browser loads React app from nginx |

---

## T+0.000s — Exchange publishes a tick

The crypto exchange's matching engine detects a trade:

```
BTCUSD traded at 67432.51 at 14:30:00.123 UTC.
```

Their WebSocket server broadcasts a JSON message to every
subscriber:

```json
{
  "symbol": "BTCUSD",
  "price": 67432.51,
  "volume": 0.5,
  "timestamp": 1721303400
}
```

This is over the **public internet**, encrypted (wss://).

---

## T+0.050s — Ingestion receives the frame

Inside the `ingestion` container, our async WebSocket client
(`ws_client.py`) has an open connection:

```python
async for message in ws:
    try:
        tick = normalizer.parse(message)
    except NormalizationError as e:
        producer.send_dlq(raw=message, reason=str(e))
        continue
    producer.send(tick)
```

The `message` arrives. The normalizer:

1. Decodes the JSON bytes → `{"symbol": "BTCUSD", "price": 67432.51, ...}`.
2. Maps field names to the canonical schema (`timestamp` →
   `event_time` as `datetime(2026, 07, 19, 14, 30, 00, tzinfo=utc)`).
3. Validates with Pydantic:
   - `price` is a `Decimal` ✓
   - `event_time` is tz-aware ✓
   - `symbol` is in `WATCHLIST` ✓
4. Returns a `CanonicalTick`.

If anything failed, the message goes to `prices.dlq` instead, with
a reason. We don't lose it; we just don't block the pipeline.

---

## T+0.055s — Ingestion publishes to Kafka

The producer (`producer.py`) sends to Kafka topic `prices`:

```python
producer.produce(
    topic="prices",
    key="BTCUSD".encode(),                  # same partition per symbol
    value=json.dumps(tick.to_dict()).encode(),
)
producer.poll(0)
```

A few important things:

- **`key="BTCUSD"`** — all BTCUSD messages land in the same Kafka
  partition (we have 1 partition, but if we had more this matters).
- **Idempotent producer.** The producer is configured with
  `enable.idempotence=true`, `acks=all`. If the broker hiccups and
  the producer retries, the message is stored exactly once.
- **`producer.poll(0)`** — flushes pending callbacks. Production
  code uses `flush()` periodically to ensure delivery.

---

## T+0.100s — Kafka stores the message

Kafka's broker appends the message to partition 0 of topic
`prices`:

```
prices
partition 0:
  offset 100  { "symbol":"BTCUSD", "price": 67428.10, ... }
  offset 101  { "symbol":"ETHUSD", "price":  3520.10, ... }
  offset 102  { "symbol":"BTCUSD", "price": 67432.51, ... }   ← our tick
```

The message is on disk, replicated (in a multi-broker setup), and
retention-tracked. It will stay here for 7 days (default Kafka
retention).

The broker returns an acknowledgement to the producer. The
producer's `acks=all` setting means the broker confirmed the write
**and** any in-sync replicas confirmed it before returning success.

---

## T+0.500s — Spark's micro-batch picks up the message

Spark's structured streaming query runs on a 10-second trigger.
Inside the `spark` container:

```
spark-sql-driver
   │
   ├─ Kafka source (subscribed to `prices`)
   │
   ├─ Read offsets from checkpoint: "I've read up to offset 101"
   │
   ├─ Pull offsets 102..102 (one new message)
   │
   ├─ Parse JSON: from_json(value, schema)
   │
   ├─ Project to typed columns
   │
   └─ foreachBatch(write_batch_to_bronze)
```

The batch is just **one row** — our BTCUSD tick.

---

## T+0.600s — Spark writes to Bronze

The Python `foreachBatch` callback runs. Inside
`streaming/src/streaming/upsert.py`:

```python
def write_batch_to_bronze(batch_df, batch_id):
    rows = batch_df.toPandas().to_dict("records")
    upsert_to_bronze(conn_str, rows)

def upsert_to_bronze(conn_str, rows):
    stage = f"bronze._prices_raw_stage_{secrets.token_hex(6)}"

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {stage} (LIKE bronze.prices_raw INCLUDING ALL)")
            execute_values(cur, f"INSERT INTO {stage} VALUES %s", [tuple(r.values()) for r in rows])
            cur.execute(f"""
                INSERT INTO bronze.prices_raw (symbol, exchange, price, volume,
                                              event_time, ingested_at, source, raw)
                SELECT symbol, exchange, price, volume, event_time,
                       ingested_at, source, raw
                FROM {stage}
                ON CONFLICT (symbol, exchange, event_time) DO NOTHING
            """)
            cur.execute(f"DROP TABLE {stage}")
```

Step by step:

1. Generate a unique staging-table name
   (`bronze._prices_raw_stage_a1b2c3`).
2. `CREATE TABLE` it, with the same structure as Bronze.
3. Bulk-insert the batch (just one row in this case).
4. `INSERT INTO bronze.prices_raw SELECT ... ON CONFLICT DO NOTHING`.
5. Drop the staging table.

The `ON CONFLICT DO NOTHING` clause:
- If the row's `(symbol, exchange, event_time)` is already in
  Bronze → skip.
- Otherwise → insert.

---

## T+0.700s — Bronze has the new row

```sql
select * from bronze.prices_raw
where symbol = 'BTCUSD'
order by event_time desc limit 1;
```

```
id   | symbol | exchange      | price    | volume | event_time           | ingested_at          | source         | raw
-----+--------+---------------+----------+--------+----------------------+----------------------+----------------+----
1042 | BTCUSD | FreeCryptoAPI | 67432.51 | 0.5    | 2026-07-19 14:30:00  | 2026-07-19 14:30:01  | FreeCryptoAPI  | {"symbol":"BTCUSD", ...}
```

A new row. The unique constraint enforced it was the only one for
this `(symbol, exchange, event_time)`.

Spark updates its checkpoint:

```
spark_checkpoints/bronze/offsets/0  → "102"
spark_checkpoints/bronze/commits/  → "1042"  (batch ID)
```

If Spark crashes now, on restart it resumes from offset 102 —
having already written this row. The next batch starts at offset
103. No duplicates possible.

---

## T+0:00 → T+5:00 — More ticks arrive

Over the next 5 minutes, ~300 more BTCUSD ticks arrive (one every
second or so). Each takes the same path: WebSocket → ingestion →
Kafka → Spark → Bronze. By the time the next dbt run happens,
Bronze has ~300 new BTCUSD rows plus all the historical ones.

---

## T+5:00 — Airflow triggers `transform_dag`

The Airflow scheduler notices it's been 5 minutes since the last
`transform_dag` run. It enqueues a DAG run.

```
scheduler → DAG run "transform_dag__2026-07-19T14_35_00" created
```

The DAG has two tasks in order:

1. `dbt_deps` — fetch packages (no-op if already cached).
2. `dbt_build` — runs all models + tests.

`dbt_deps` finishes in <1 second. `dbt_build` starts.

---

## T+5:05 — dbt rebuilds Silver

`dbt build` parses every model file, builds the dependency graph,
and runs them in order.

First: `silver.stg_prices`:

```sql
-- models/staging/stg_prices.sql
select symbol, exchange, price, volume, event_time,
       ingested_at, source, raw
from bronze.prices_raw
```

dbt does `CREATE TABLE silver.stg_prices AS (...)`. Postgres
executes this against all ~1000 Bronze rows. The result is a typed
copy in the `silver` schema.

---

## T+5:06 — dbt rebuilds Gold.candles_1m

```sql
-- models/marts/candles_1m.sql
select
    date_trunc('minute', event_time) as bucket,
    symbol, exchange,
    first(price order by event_time) as open,
    max(price) as high,
    min(price) as low,
    last(price order by event_time) as close,
    sum(volume) as volume
from silver.stg_prices
group by 1, 2, 3
```

For each `(minute, symbol, exchange)`:

- `bucket` = the minute the candle covers
- `open`, `high`, `low`, `close` = OHLC prices
- `volume` = total volume

`CREATE TABLE gold.candles_1m AS (...)`.

---

## T+5:07 — dbt rebuilds Gold.candles_1m_ma

```sql
-- models/marts/candles_1m_ma.sql
select
    bucket, symbol, exchange, close,
    avg(close) over (
        partition by symbol, exchange
        order by bucket
        rows between 19 preceding and current row
    ) as ma_20
from gold.candles_1m
```

For each candle, the average of the last 20 close prices
(including the current one). `CREATE TABLE gold.candles_1m_ma AS
(...)`.

---

## T+5:08 — dbt runs tests

After all models build, dbt runs the tests declared in
`models/schema.yml`:

- `not_null` on every non-nullable column.
- `unique` on business keys.
- `accepted_values` on `exchange`.
- `assert_candle_bounds` — a custom SQL test in
  `tests/assert_candle_bounds.sql` that fails if any candle has
  `high < open`.

If any test fails, the build is red. For our tick, everything is
green.

---

## T+5:10 — Airflow marks the run successful

`dbt_build` finishes with status "success". Airflow records this
in its metadata DB. The UI shows a green square for the run.

The next `transform_dag` run is scheduled for T+10:00.

---

## T+5:15 — A user opens the dashboard

The browser navigates to `http://localhost:5173`. Nginx serves
the static React bundle:

```
GET /index.html        → HTML shell
GET /assets/main.js    → React app
GET /assets/main.css   → styles
```

React boots up. The `App` component runs:

1. `useState` declares 5 state slots (health, latest, candles, ma,
   error).
2. `useEffect` fires: calls 4 API endpoints in parallel via
   `Promise.all`.

---

## T+5:15.100 — Browser calls FastAPI

The dashboard hits:

```
GET http://localhost:8000/health
GET http://localhost:8000/prices/latest?symbols=BTCUSD,ETHUSD,SOLUSD
GET http://localhost:8000/candles/BTCUSD?interval=1m&limit=60
GET http://localhost:8000/indicators/BTCUSD/ma?limit=60
```

These are 4 parallel HTTP requests to FastAPI.

---

## T+5:15.110 — FastAPI queries Postgres

Each endpoint calls a method on `GoldRepository`. For example,
`/candles/BTCUSD`:

```python
def candles(self, symbol, limit):
    with self._pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select bucket, open, high, low, close, volume
                from gold.candles_1m
                where symbol = %s
                order by bucket desc
                limit %s
            """, (symbol, limit))
            rows = cur.fetchall()
    return [Candle(...) for r in reversed(rows)]
```

Postgres returns the last 60 candles for BTCUSD, ordered ascending
by bucket. The repository reverses them (because the SQL used DESC
+ LIMIT for efficiency).

---

## T+5:15.130 — FastAPI returns JSON

FastAPI serialises the rows to JSON:

```json
{
  "symbol": "BTCUSD",
  "interval": "1m",
  "candles": [
    {"bucket": "2026-07-19T14:30:00Z", "open": "67428.10", "high": "67432.51", "low": "67428.00", "close": "67432.51", "volume": "1.25"},
    {"bucket": "2026-07-19T14:31:00Z", "open": "67432.51", "high": "67440.00", "low": "67430.00", "close": "67438.10", "volume": "2.10"},
    ...
  ]
}
```

Decimal prices are serialised as strings to preserve precision.

---

## T+5:15.200 — Browser updates the UI

Back in React:

```js
const data = candles.map(c => ({
  bucket: c.bucket,
  close: Number(c.close),
  ma_20: maLookup[`${c.bucket}|${c.exchange}`],
}));
```

The `data` array is built. Recharts receives it:

```jsx
<LineChart data={data}>
  <Line dataKey="close" stroke="#56d364" />
  <Line dataKey="ma_20" stroke="#e3b341" strokeDasharray="4 4" />
</LineChart>
```

Recharts draws the green close line and the dashed amber MA(20)
line on an SVG canvas.

---

## T+5:15.300 — User sees the chart

The chart appears. The user watches. Every 5 seconds, React
re-fetches, the state updates, the chart re-renders.

The user has just seen a BTCUSD tick that was published by an
exchange 15.3 seconds ago — after it travelled through 9 separate
systems without losing a single digit.

---

## What "real-time" actually means here

| Stage | Typical latency |
|-------|-----------------|
| Exchange → ingestion | 50–100 ms |
| ingestion → Kafka | 5–10 ms |
| Kafka → Spark micro-batch | 0–10 s (waits for next batch) |
| Spark → Bronze | 100–500 ms |
| Bronze → Gold | 0–5 min (waits for next dbt run) |
| Gold → dashboard | 50–200 ms |

**End-to-end latency for a tick:**

- To appear in Bronze: **~10–15 seconds** (live lane)
- To appear in Gold: **up to 5 minutes** (batch lane)
- To appear on the dashboard: **~5–15 seconds after Gold** (the
  dashboard polls every 5 s)

So when a user looks at the dashboard, they see data that's at
most **5–6 minutes old** (the worst case is when a tick arrives
just after a dbt run; the next dbt run is 5 min later, then the
next dashboard poll is 5 s later).

For a demo, this is invisible. For a production trading system,
5 minutes would be unacceptable — and you'd want to use Spark
Structured Streaming to compute candles directly, bypassing dbt.

---

## Failure scenarios

What if something breaks in the middle?

| Failure | What happens | Recovery |
|---------|--------------|----------|
| WebSocket disconnects | Reconnect with backoff | Automatic |
| Kafka broker restarts | Producer/consumer reconnect | Automatic |
| Spark crashes mid-batch | Checkpoint has last committed offset; restart resumes | Automatic |
| Postgres temporarily down | Spark's `foreachBatch` raises; Spark retries the batch | Automatic |
| dbt build fails | Airflow marks the run red; previous Gold tables still serve | Run again |
| API container dies | Docker restarts it; dashboard sees brief errors | Automatic |
| Dashboard loses connection | Red error banner; auto-recovers next poll | Automatic |

The pipeline is **resilient at every layer**. The end-to-end
behaviour is: "data flows, things occasionally hiccup, recovery
happens without human intervention".

---

## Try it yourself

With the stack running, trace a real tick:

```bash
# 1. Watch Bronze grow
watch -n 2 "make psql -- -c 'select count(*) from bronze.prices_raw;'"

# 2. Wait for the next dbt run (every 5 min) and watch Silver + Gold grow
watch -n 5 "make psql -- -c 'select count(*) from gold.candles_1m;'"

# 3. Curl the API to see what the dashboard sees
curl -sf 'localhost:8000/candles/BTCUSD?interval=1m&limit=5' | python -m json.tool

# 4. Watch the dashboard update in real time
open http://localhost:5173
```

---

## What's next?

- [12_DESIGN_DECISIONS.md](12_DESIGN_DECISIONS.md) — every
  trade-off we made, explained.
- [13_HANDS_ON_TOUR.md](13_HANDS_ON_TOUR.md) — exercise the system
  with guided commands.
- [14_GLOSSARY.md](14_GLOSSARY.md) — quick reference.