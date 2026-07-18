# 06 — Spark Structured Streaming fundamentals

CryptoStream uses **Apache Spark** to move data from Kafka into
Postgres Bronze. This page explains what Spark is, what
"Structured Streaming" means, and how CryptoStream uses the
`foreachBatch` pattern to do an idempotent upsert.

---

## What is Spark?

**Apache Spark** is a distributed data-processing engine. You give
it a chunk of data and a transformation; Spark splits the work
across many machines, runs it in parallel, and gives you back the
result.

It's the modern successor to **Hadoop MapReduce**. Where MapReduce
forced you to write two functions (`map` and `reduce`) and chain
them across disk reads, Spark keeps data in memory and offers a
rich set of operations (`select`, `filter`, `join`, `group by`,
`window`, ...) similar to SQL.

For CryptoStream we use Spark for **streaming**: processing an
unbounded stream of events as they arrive. The streaming API is
called **Structured Streaming**.

---

## Structured Streaming

The big idea: treat a stream as if it were a **table that grows
over time**.

```
Time T0:  +----+----+----+
          | a  | b  | c  |
          +----+----+----+

Time T1:  +----+----+----+----+
          | a  | b  | c  | d  |    (new row d arrived)
          +----+----+----+----+

Time T2:  +----+----+----+----+----+
          | a  | b  | c  | d  | e  |    (new row e arrived)
          +----+----+----+----+----+
```

You write a query against this table — same syntax as a static
SQL query. Spark runs it incrementally: every time new data
arrives, it computes the new rows of the result.

```
input table ──▶  query  ──▶  result table
(Kafka)         (SQL)       (Bronze)
```

---

## Micro-batches

Spark Structured Streaming is a **micro-batch** engine. It doesn't
process one message at a time; it waits for a small window of
messages and processes them all together.

```
T=0s    [msg1, msg2]   ──▶ micro-batch 1
T=10s   [msg3, msg4, msg5] ──▶ micro-batch 2
T=20s   [msg6]         ──▶ micro-batch 3
```

You control the window with `trigger(processingTime='10 seconds')`.
CryptoStream defaults to 10 seconds.

Why micro-batches instead of one-at-a-time?

- **Throughput.** One DB write per message is slow. One write per
  batch of N messages is much faster.
- **Fault tolerance.** Easier to checkpoint at batch boundaries.
- **Consistency.** All the rows in a batch are processed
  atomically.

The trade-off is latency: at most one trigger-interval of delay.
With a 10-second trigger, your data is at most 10 seconds behind
"now". For most use cases this is invisible.

---

## The streaming query

CryptoStream's `streaming/src/streaming/stream_to_bronze.py`
defines the query:

```python
raw = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "prices")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load())

parsed = (raw
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json("json_str", schema).alias("tick"))
    .select("tick.*"))

query = (parsed
    .writeStream
    .foreachBatch(write_batch_to_bronze)
    .option("checkpointLocation", "/checkpoints/bronze")
    .trigger(processingTime="10 seconds")
    .start())
```

Walking through it:

1. **`.readStream.format("kafka")`** — connect to Kafka as a stream
   source.
2. **`.option("subscribe", "prices")`** — read from the `prices`
   topic.
3. **`.option("startingOffsets", "latest")`** — on first run, start
   from the current end (don't replay history). After that, the
   checkpoint controls where we start.
4. **`.option("failOnDataLoss", "false")`** — if Kafka deletes
   messages before we read them (retention expiry), don't crash.
5. **`from_json(...)`** — parse the JSON value into a structured
   row using an explicit schema.
6. **`.foreachBatch(write_batch_to_bronze)`** — for each micro-batch,
   hand the rows to our Python function (see below).
7. **`.option("checkpointLocation", "/checkpoints/bronze")`** —
   save progress here. On restart, Spark resumes from the last
   completed batch.
8. **`.trigger(processingTime="10 seconds")`** — every 10 seconds,
   process whatever new messages arrived.

---

## `foreachBatch` — the bridge to Postgres

`foreachBatch` is the magic that lets Spark write to a system
that isn't natively supported (like Postgres). For each
micro-batch, Spark calls our Python function with the batch as a
DataFrame.

```python
def write_batch_to_bronze(batch_df, batch_id):
    rows = batch_df.toPandas().to_dict("records")
    upsert.upsert_to_bronze(conn_str, rows)
```

Inside our helper (`streaming/src/streaming/upsert.py`), each
batch does:

1. Create a **staging table** unique to this batch:
   `bronze._prices_raw_stage_a1b2c3`.
2. Bulk-insert the rows into staging.
3. `INSERT INTO bronze.prices_raw (...) SELECT ... FROM staging ON
   CONFLICT (symbol, exchange, event_time) DO NOTHING`.
4. Drop the staging table.

This pattern is called **staged upsert**. It's the idiomatic way
to do idempotent writes from Spark to Postgres.

---

## Why staged upsert, not plain JDBC append?

A naive approach: just append every batch to Bronze.

```python
batch_df.write.jdbc(url, "bronze.prices_raw", mode="append")
```

What goes wrong?

- Kafka redelivery → duplicate rows.
- Spark restart after partial batch → some rows written twice.
- Bug in our code → same row twice.

You can't deduplicate at the database level with `mode="append"`.
Postgres will happily insert the same business key twice and the
unique constraint will reject it — but you've now lost data
without knowing it.

The staged upsert pattern solves this:

```
┌─────────────────────┐
│ micro-batch (50 rows)│
└──────────┬──────────┘
           ▼
┌──────────────────────────────┐
│ CREATE TABLE staging         │
│ INSERT 50 rows INTO staging  │
└──────────┬───────────────────┘
           ▼
┌──────────────────────────────────────────┐
│ INSERT INTO bronze.prices_raw             │
│ SELECT * FROM staging                    │
│ ON CONFLICT (symbol, exchange, event_time)│
│ DO NOTHING;                              │
└──────────┬───────────────────────────────┘
           ▼
┌─────────────────────────┐
│ DROP TABLE staging      │
└─────────────────────────┘
```

The unique constraint on `(symbol, exchange, event_time)` makes
the `ON CONFLICT DO NOTHING` work. Any row that's already in
Bronze is silently skipped; new rows go through.

---

## Checkpoints

Spark needs to remember **how far it's read**. Without that, a
restart would either re-read everything (wasteful) or miss data
(catastrophic).

The **checkpoint** is a directory on disk where Spark writes:

- The current Kafka offsets (per partition).
- The state of any stateful operations (none in our case).
- A log of completed micro-batches.

On restart, Spark reads the checkpoint and resumes from the last
committed offset.

```
spark_checkpoints/bronze/
├── offsets/
│   └── 0     ← "I've read up to offset N on partition 0"
├── commits/
│   └── 42    ← "batch 42 is complete"
└── ...
```

We mount a named Docker volume at `/checkpoints` so the state
survives container restarts.

---

## Idempotency — the full story

End-to-end, here's how CryptoStream avoids duplicate writes:

| Layer | Mechanism |
|-------|-----------|
| Kafka producer | `enable.idempotence=true`, `acks=all` |
| Kafka broker | Sequence numbers + dedup at the broker |
| Spark | Checkpoint tracks offsets; restart resumes from last committed |
| Postgres | Unique constraint + `ON CONFLICT DO NOTHING` |

If everything works perfectly, no duplicates ever. If anything
goes wrong (Kafka retention, network glitch, Spark crash), the
idempotency layers absorb it. The verification step in Module 4
(`no_dupes = true`) proves this.

---

## Spark vs other streaming engines

| Engine | CryptoStream chose it? | Why / why not |
|--------|-----------------------|---------------|
| **Spark Structured Streaming** | ✅ yes | Native Kafka source; foreachBatch is exactly what we need |
| Flink | no | More powerful but heavier; overkill for our scale |
| Kafka Streams | no | Tied to Kafka; we wanted a separate compute tier |
| Plain Python consumer | no | We'd have to write checkpointing, watermarking, etc. ourselves |
| Materialized views in Postgres | no | Doesn't subscribe to Kafka directly |

For a small demo, plain Python would technically work. For
"production-ish" reliability, Spark wins because it gives us
checkpointing, schema enforcement, and horizontal scale for free.

---

## What's a "DataFrame"?

Throughout Spark code you see `.select()`, `.filter()`,
`.groupBy()`, `.join()` — these return **DataFrames**, which are
named, typed tables.

```python
df = spark.read.json("s3://bucket/file.json")
result = df.filter(df.price > 0).groupBy("symbol").avg("price")
```

You can think of a DataFrame as "a SQL table, but in code". The
operations are **lazy** — Spark doesn't actually compute anything
until you call an action like `.show()`, `.write()`, or `.count()`.

In `foreachBatch`, the `batch_df` parameter is a DataFrame
containing the current micro-batch. You can do anything you want
with it: collect to a list, convert to Pandas, write to Postgres,
call an HTTP API, etc.

---

## Visualising the flow

```
Kafka topic `prices`
┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
│ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │ ... offsets
└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
              │       │           │
              ▼       ▼           ▼
        micro-batch  micro-batch  micro-batch
         (3 msgs)     (3 msgs)     (2 msgs)
              │       │           │
              ▼       ▼           ▼
       foreachBatch  foreachBatch foreachBatch
              │       │           │
              ▼       ▼           ▼
       ┌──────────────────────────┐
       │   bronze.prices_raw      │
       │   + staging tables       │
       └──────────────────────────┘
```

Every 10 seconds, Spark pulls a slice of new messages and writes
them as one batch. Bronze accumulates the rows.

---

## Vocabulary

| Term | Meaning |
|------|---------|
| Spark | Distributed data-processing engine |
| Structured Streaming | Spark's stream-as-table API |
| Micro-batch | A small group of messages processed together |
| DataFrame | A named, typed table (lazy operations) |
| foreachBatch | Per-batch hook for arbitrary Python |
| Trigger | The schedule Spark uses to start a new micro-batch |
| Checkpoint | On-disk state for restart recovery |
| Staged upsert | Insert-into-temp-table + INSERT...SELECT ON CONFLICT pattern |
| Idempotent | Safe to re-run without changing the result |
| Offset | A numeric position in a Kafka partition |

---

## Try it yourself

```bash
# Tail the Spark logs
make logs SERVICE=spark

# Look for "Processed micro-batch of N rows" lines
docker compose logs spark 2>&1 | grep -i 'micro-batch' | tail -10

# Inspect the checkpoint directory
docker compose exec spark ls /checkpoints/bronze

# Run the idempotency proof (Module 4 verification)
make psql -- -c "select count(*) as c1 from bronze.prices_raw;"
docker compose exec -d spark pkill -f spark_to_bronze || true
sleep 5
make stream-bg
sleep 30
make psql -- -c "select count(*) - <c1> as delta,
                       count(*) = count(distinct (symbol, exchange, event_time)) as no_dupes
                  from bronze.prices_raw;"
# Expected: no_dupes = true
```

---

## What's next?

- [07_DBT_FUNDAMENTALS.md](07_DBT_FUNDAMENTALS.md) — what runs every
  5 minutes to refresh Silver + Gold from Bronze.
- [12_DESIGN_DECISIONS.md](12_DESIGN_DECISIONS.md) — why we chose
  Spark over alternatives for this specific job.