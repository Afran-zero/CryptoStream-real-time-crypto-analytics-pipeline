# Operations

Day-2 operations for the local CryptoStream stack. Covers bring-up,
teardown, log inspection, data inspection, the most common recovery
flows, and scaling notes.

For bring-up see [QUICKSTART.md](QUICKSTART.md). For env vars see
[ENV_REFERENCE.md](ENV_REFERENCE.md). For symptom → fix see
[TROUBLESHOOTING.md](TROUBLESHOOTING.md).

---

## Make targets

`make help` prints them all. Highlights:

| Target                       | What it does                                                     |
|------------------------------|------------------------------------------------------------------|
| `make up`                    | `docker compose up -d --build` — the whole stack                 |
| `make down`                  | `docker compose down` — keep volumes                             |
| `make nuke`                  | `docker compose down -v` — **destructive**, wipes data           |
| `make ps`                    | `docker compose ps` — service health summary                    |
| `make logs SERVICE=api`      | Tail logs for one service                                        |
| `make psql -- -c "SELECT …"` | One-shot psql into the medallion DB                              |
| `make topics`                | List Kafka topics                                                |
| `make kafka-versions`        | Confirm broker is responding                                     |
| `make migrate`               | Apply migrations against the local Postgres container            |
| `make migrate-host`          | Apply migrations against `$DATABASE_URL` on the host (Neon)      |
| `make stream`                | Run Spark → Bronze **foreground** (Ctrl-C to stop)               |
| `make stream-bg`             | Run Spark → Bronze **background** (returns immediately)          |
| `make test`                  | Ingestion unit tests                                             |
| `make test-integration`      | Ingestion integration tests (real local WS loop)                 |
| `make test-integration-stream` | Streaming idempotency test                                     |
| `make dbt`                   | `dbt build` inside the dbt container                             |
| `make dbt-host`              | `dbt build` on the host (use for Neon)                           |
| `make dbt-deps`              | `dbt deps` only (fetch dbt-utils)                                |
| `make airflow-up`            | Build custom Airflow image + start webserver/scheduler           |
| `make airflow-logs`          | Tail scheduler logs                                              |
| `make airflow-trigger DAG=…` | Trigger a DAG by hand                                            |
| `make airflow-list`          | List all DAGs                                                    |
| `make airflow-runs DAG=…`    | List recent runs of a DAG                                        |
| `make api`                   | Build + start api + dashboard                                    |
| `make api-logs`              | Tail API logs                                                    |
| `make api-test`              | Run API tests inside the api container                           |
| `make dashboard-logs`        | Tail dashboard logs                                              |

---

## Bring-up

```bash
make up               # full stack
make ps               # confirm every service is healthy
```

Cold-boot takes ~30–60 s. Watch:

```bash
make logs             # all services, interleaved
make logs SERVICE=kafka   # just one
```

Healthy services show `(healthy)` next to their name in `make ps`.

---

## Teardown

```bash
make down             # stop containers, keep volumes (data preserved)
make nuke             # stop + delete volumes (data lost — Bronze, Gold, Kafka offsets, all gone)
```

`make nuke` is destructive. There's no confirmation prompt — if you
run it, both `pg_data` and `spark_checkpoints` named volumes are
dropped. Use it when you want a clean slate.

Per-volume wipes:

```bash
docker volume rm cs_pg_data             # wipes Postgres + Airflow metadata
docker volume rm cs_spark_checkpoints   # wipes Spark streaming state (forces re-consume from Kafka)
```

---

## Inspecting data

### Postgres

```bash
# Bronze row count + last event
make psql -- -c "select count(*) from bronze.prices_raw;"
make psql -- -c "select symbol, exchange, price, event_time from bronze.prices_raw order by event_time desc limit 5;"

# Idempotency proof: business keys are unique
make psql -- -c "select count(*) = count(distinct (symbol, exchange, event_time)) as no_dupes from bronze.prices_raw;"

# Silver
make psql -- -c "select count(*) from silver.stg_prices;"
make psql -- -c "select * from silver.stg_prices order by event_time desc limit 5;"

# Gold candles + MA
make psql -- -c "select bucket, symbol, open, high, low, close, volume from gold.candles_1m order by bucket desc limit 5;"
make psql -- -c "select bucket, symbol, ma_20 from gold.candles_1m_ma order by bucket desc limit 5;"
```

### Kafka

```bash
make topics                                        # list topics
make logs SERVICE=kafka                            # broker logs
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic prices --from-beginning --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic prices.dlq --from-beginning --max-messages 5
```

### Spark

```bash
make logs SERVICE=spark                            # spark-submit output
docker compose exec spark ls /checkpoints/bronze   # checkpoint state on the named volume
```

### API

```bash
curl -sf localhost:8000/health
curl -sf "localhost:8000/prices/latest?symbols=BTCUSD,ETHUSD"
curl -sf "localhost:8000/candles/BTCUSD?interval=1m&limit=10"
curl -sf "localhost:8000/indicators/BTCUSD/ma?limit=10"
```

The OpenAPI explorer is at <http://localhost:8000/docs>.

---

## Logs

Structured JSON logs from Python services, plain text from Spark and
Airflow.

```bash
make logs                                 # all services, tailed
make logs SERVICE=api                     # one service
make logs SERVICE=ingestion | jq -R 'fromjson? | .'   # pretty-print JSON logs
```

---

## Restarting individual services

```bash
docker compose restart api                # bounces the API container in place
docker compose restart ingestion          # bounces ingestion (will reconnect WS)
docker compose restart airflow-scheduler  # picks up DAG changes / clears stuck state
```

For DAG / image / Dockerfile changes:

```bash
docker compose up -d --build airflow-scheduler   # rebuild + restart scheduler
docker compose up -d --build api                 # rebuild + restart API
```

---

## Idempotency proof (Module 4 verification)

The canonical "stop the stream and restart it" test:

```bash
make psql -- -c "select count(*) as c1 from bronze.prices_raw;"
docker compose exec -d spark pkill -f spark_to_bronze || true
sleep 5
make stream-bg
sleep 30
make psql -- -c "select count(*) - <c1> as delta,
                       count(*) = count(distinct (symbol, exchange, event_time)) as no_dupes
                  from bronze.prices_raw;"
```

Expected: `no_dupes = true` always. The unique constraint absorbs any
re-delivered rows.

---

## Recovery flows

### Restart everything cleanly

```bash
make down && make up
```

### Restart only the stream and let it resume from checkpoint

```bash
docker compose exec -d spark pkill -f spark_to_bronze || true
sleep 5
make stream-bg
```

Spark resumes from the last committed offset on
`spark_checkpoints/bronze`. No data loss, no duplicates.

### Drop and re-create the medallion schemas

```bash
make psql -- -c "drop schema if exists bronze cascade;"
make psql -- -c "drop schema if exists silver cascade;"
make psql -- -c "drop schema if exists gold cascade;"
make migrate
make dbt
```

### Reset Airflow metadata only

```bash
docker compose down airflow-init airflow-webserver airflow-scheduler
docker volume rm cs_airflow_db           # if you named it; otherwise: docker volume ls
make up
make airflow-up                          # rebuilds image, runs airflow-init
```

---

## Scaling notes (where you'd push for prod)

- **Kafka:** swap single-broker KRaft for a 3-broker cluster, increase
  partition count on `prices` (more consumers = more parallelism).
- **Spark:** Module 4 currently runs inside the `spark` container with
  one executor. Move `spark-submit` to a Spark cluster (k8s, EMR,
  Databricks). Checkpoint dir moves to S3/ADLS/GCS.
- **dbt:** 5-minute schedule is fine for the demo. For prod, switch
  materialisations to `incremental` and trigger via Airflow sensors on
  Kafka offset progression.
- **API:** one FastAPI worker is fine for tens of req/s. Scale with
  `uvicorn --workers N` or run under gunicorn behind nginx.
- **Postgres:** Bronze grows linearly with tick volume. The retention
  sweep (`retention_dag`) is your knob. Move to a partitioned table
  (`PARTITION BY RANGE (event_time)`) once you're past 100M rows.
- **Source:** swap FreeCryptoAPI for a multi-exchange feed (Kraken,
  Binance) and add per-exchange row creation. The
  `(symbol, exchange, event_time)` business key already supports this.

---

## Backup / restore

The local stack is meant to be disposable. There is no built-in backup
target. For Neon hosting: use Neon's branching + PITR; the Compose
deployment doesn't take its own snapshots.

If you need to snapshot the local stack for any reason:

```bash
docker compose down
docker run --rm -v cs_pg_data:/from -v $(pwd)/backup:/to alpine \
  tar czf /to/pg-data.tar.gz -C /from .
```

To restore:

```bash
docker compose down
docker volume rm cs_pg_data
docker volume create cs_pg_data
docker run --rm -v cs_pg_data:/to -v $(pwd)/backup:/from alpine \
  tar xzf /from/pg-data.tar.gz -C /to
docker compose up -d
```

---

## Observability

Out of the box:

| Signal                | How to read                                       |
|-----------------------|---------------------------------------------------|
| Service health        | `make ps` (compose healthchecks)                  |
| Live stream lag       | Spark log lines `Processed micro-batch of N rows` |
| dbt test failures     | `make logs SERVICE=airflow-scheduler`             |
| API errors            | `make logs SERVICE=api` (JSON)                    |
| Dashboard errors      | Browser console + the red error banner            |
| Bronze row growth     | `make psql -- -c "select count(*) from bronze.prices_raw;"` |

Adding Prometheus / Grafana / Loki / OpenTelemetry is out of scope for
the demo — the structured JSON logs are designed to drop into a Loki
or Elastic pipeline if you want to forward them.

---

## Where to go next

- [QUICKSTART.md](QUICKSTART.md) — get the stack running.
- [ENV_REFERENCE.md](ENV_REFERENCE.md) — every var, every module.
- [ARCHITECTURE.md](ARCHITECTURE.md) — why the system looks the way it does.
- [MODULES.md](MODULES.md) — per-module detail.
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — when something breaks.