# Module 4 — Stream processing (Spark → Bronze)

## Purpose

Bridge Kafka to Postgres Bronze with **exactly-once-into-Bronze**
semantics: the structured-streaming query tracks Kafka offsets and
writes each micro-batch through a per-batch staging table +
`INSERT ... ON CONFLICT (symbol, exchange, event_time) DO NOTHING`.
Restart picks up from checkpoint state; the unique constraint absorbs
any re-delivered rows.

## Files

```
streaming/
  pyproject.toml                              # package + deps + pytest config
  src/streaming/
    __init__.py
    config.py                                 # StreamConfig dataclass from env
    upsert.py                                 # staging-table upsert helper
    stream_to_bronze.py                       # the spark-submit entrypoint
  tests/
    conftest.py
    test_idempotency.py                       # integration test (marked `@pytest.mark.integration`)
```

`docker-compose.yml` mounts this directory into the `spark` service at
`/app/streaming`, and the `PYTHONPATH` includes it so `streaming.*`
imports resolve inside the Spark driver.

## How to run

```bash
make stream-bg       # background
# or
make stream          # foreground (Ctrl-C to stop)
```

The exact `spark-submit` invocation is:

```bash
docker compose exec -T spark \
  /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    --conf spark.sql.streaming.checkpointLocation=/checkpoints/bronze \
    /app/streaming/src/streaming/stream_to_bronze.py
```

The `--packages` flag pulls the Kafka connector and Postgres JDBC
driver into the Spark local Ivy cache on first run; subsequent runs
are cached on the `spark_checkpoints` volume? — no, in
`~/.ivy2` inside the container, which is fine because the image is
rebuilt only when its Dockerfile changes.

## How to verify

Bronze is populating:

```bash
make psql -- -c "select count(*) from bronze.prices_raw;"
make psql -- -c "select symbol, exchange, price, event_time from bronze.prices_raw order by event_time desc limit 5;"
```

Idempotency proof (the canonical Module 4 verification):

```bash
make psql -- -c "select count(*) as c1 from bronze.prices_raw;"
docker compose exec -d spark pkill -f spark_to_bronze || true
sleep 5
make stream-bg
sleep 30
make psql -- -c "select count(*) - <c1> as delta,
                       count(*) = count(distinct (symbol, exchange, event_time)) as no_dupes
                  from bronze.prices_raw;"
# no_dupes must be TRUE
```

Offline idempotency proof (no live data, no Kafka):

```bash
DATABASE_URL=postgresql://cryptostream:cryptostream@localhost:5432/cryptostream \
  python -m pytest streaming/tests -q -m integration
```

## Behaviour details

- **Read options**: `startingOffsets=latest` for first run, then
  checkpoint-driven. `failOnDataLoss=false` so a Kafka retention purge
  doesn't crash the query.
- **Schema parse**: an explicit `StructType` matches Module 0 §5.
  Records that fail to parse are silently dropped (the DLQ's job).
- **Per-batch staging table** named `bronze._prices_raw_stage_<hex>`
  (12 hex chars). Created with `CREATE TABLE`, bulk-inserted via
  `psycopg` + `execute_values`, then `INSERT INTO bronze.prices_raw
  (...) SELECT ... FROM staging ON CONFLICT DO NOTHING`, then dropped.
- **Checkpoint state** persists on the `spark_checkpoints` named
  volume at `/checkpoints/bronze`. Container restarts don't lose it.

## Env vars consumed

See [ENV_REFERENCE.md — Module 4](ENV_REFERENCE.md#module-4--stream-processing).

Required: `DATABASE_URL`. Optional with sensible defaults:
`KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_PRICES`, `SPARK_CHECKPOINT_DIR`,
`SPARK_TRIGGER_INTERVAL_S`, `BRONZE_TABLE`.

## Failure modes

| Symptom                                            | Likely cause                                          |
|----------------------------------------------------|-------------------------------------------------------|
| `spark-submit` exits with `ClassNotFoundException` | First run downloading `--packages`; wait ~30 s        |
| Bronze not populating, ingestion is fine           | Spark not running; `make stream-bg`                   |
| `no_dupes = false`                                 | Bug — the constraint should always hold; report it    |
| Bronze error logs `relation "bronze" does not exist` | Module 2 migrations not applied                      |

## Tests

```bash
DATABASE_URL=postgresql://cryptostream:cryptostream@localhost:5432/cryptostream \
  python -m pytest streaming/tests -q -m integration
```

The integration test:
1. Creates a temporary schema mimicking `bronze.prices_raw`.
2. Calls `upsert.upsert_to_bronze` with N rows.
3. Re-calls with the same N rows.
4. Asserts row count is unchanged after call 2 and that a third insert
   bypassing `ON CONFLICT` raises `UniqueViolation`.