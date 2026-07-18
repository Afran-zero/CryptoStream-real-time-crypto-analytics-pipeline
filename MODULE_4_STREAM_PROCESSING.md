# Module 4 — Stream Processing (Spark → Bronze)

## Context & Objective
Consume the `prices` topic with Spark Structured Streaming, parse the canonical
message, and write each tick into `bronze.prices_raw` idempotently. Checkpointing
guarantees no data loss on restart; the business-key constraint guarantees no
duplicates. This closes the streaming lane: source → Kafka → Spark → Bronze.

## Prerequisites
- Modules 2 & 3 complete: `bronze.prices_raw` exists; canonical ticks flow on `prices`.
- Codebase state: `streaming/` empty except `.gitkeep`; Spark base container running.

## Technical Specifications
Job `streaming/src/stream_to_bronze.py`:
- Read stream: `format("kafka")`, `subscribe=prices`,
  `startingOffsets=latest` (first run) / checkpoint-resumed thereafter,
  `failOnDataLoss=false`.
- Parse `value` (bytes → string → JSON) using an explicit schema matching Module 0
  §5: `symbol string, exchange string, price double, volume double,
  event_time timestamp, ingested_at timestamp, source string`.
- Drop records failing schema parse (they are the DLQ's job, not Bronze's).
- Sink via `foreachBatch`: for each micro-batch, write to Postgres using an
  **idempotent upsert** — write batch to a temp staging table then
  `INSERT … SELECT … ON CONFLICT (symbol,exchange,event_time) DO NOTHING`, or use a
  JDBC batch with the same conflict clause. Keep the original JSON in `raw`.
- Checkpoint location: `SPARK_CHECKPOINT_DIR` (persisted volume from Module 1).
- Required packages at submit time:
  `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` and
  `org.postgresql:postgresql:42.7.3`.

## Step-by-Step Implementation Instructions
1. Implement `stream_to_bronze.py` per spec. Read all connection settings from env
   (`KAFKA_BOOTSTRAP`, `DATABASE_URL` → parse to JDBC url + user/password).
2. Implement the `foreachBatch` upsert. Do **not** use plain `df.write.jdbc(mode=
   "append")` alone — it does not dedupe. The conflict clause on the business key
   is mandatory.
3. Add a `streaming` service (or a `make stream` target) that runs `spark-submit`
   inside the `spark` container with the `--packages` above and the checkpoint
   volume mounted. Prefer a Make target so the agent can start/stop the job:
   ```
   stream:
       docker compose exec spark /opt/spark/bin/spark-submit \
         --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
         /app/streaming/src/stream_to_bronze.py
   ```
   (Mount `streaming/` into the spark container at `/app/streaming`.)
4. Ensure the checkpoint dir is on the persisted `spark_checkpoints` volume so a
   restart resumes from committed offsets.

## Verification & Testing Criteria
```bash
# with ingestion running (Module 3), start the stream
make stream &                    # runs in background for the test
sleep 45

# Bronze is populating with live rows
make psql -c "select count(*) from bronze.prices_raw;"           # > 0 and rising
make psql -c "select symbol,exchange,price,event_time from bronze.prices_raw
              order by event_time desc limit 5;"

# IDEMPOTENCY / no-data-loss proof:
make psql -c "select count(*) as c1 from bronze.prices_raw;"     # note c1
# stop and restart the spark job (Ctrl-C the make stream, re-run make stream)
sleep 30
make psql -c "select count(*) - <c1> as delta,
              count(*) = count(distinct (symbol,exchange,event_time)) as no_dupes
              from bronze.prices_raw;"
# no_dupes must be TRUE; delta reflects only genuinely new ticks, never re-inserts
```
Success = Bronze grows from live data, and a stop/restart of the job produces zero
duplicate business keys (`no_dupes = true`).

## Hand-off State
- `bronze.prices_raw` continuously populated by an idempotent, checkpointed Spark
  job. Restarts never duplicate or lose committed data.
- Checkpoint state persisted on a named volume.
Module 5 reads `bronze.prices_raw` as its dbt source to build Silver and Gold.
