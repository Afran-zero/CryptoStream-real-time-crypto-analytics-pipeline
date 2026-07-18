# Module 1 — Infrastructure (Docker Compose)

## Purpose

Provide a single `docker compose up` that brings up every CryptoStream
service on any Docker-equipped host. The Compose file is the source of
truth for **services**, **ports**, **volumes**, **networks**, and
**inter-service dependencies** (via `condition: service_healthy`).

## Files

```
docker-compose.yml          # 11-service stack (postgres, kafka, kafka-init,
                            # spark, airflow-init, airflow-webserver,
                            # airflow-scheduler, ingestion, dbt, api, dashboard)
infra/
  postgres-init/
    01-create-airflow-db.sql    # creates the `airflow` DB on first boot
  kafka/
    create_topics.sh            # creates `prices` and `prices.dlq`
```

`cryptostream_common/` and `ingestion/`, `streaming/`, `transforms/`,
`orchestration/`, `api/`, `dashboard/` are all mounted or built into
Compose; their detail is in their own module pages.

## How to run

```bash
make up               # build + start everything
make ps               # confirm every service is healthy
```

First cold-boot takes ~30–60 s because of:
- Postgres data-dir initialisation (~3 s)
- Kafka KRaft controller election (~15 s)
- Airflow DB migrate + DAG parsing (~30 s after `airflow-init`)

Subsequent boots are ~10–15 s.

## How to verify

```bash
make ps                                       # all services (healthy)
make kafka-versions                           # broker answers
make psql -- -c "select current_database();"  # medallion DB responds
curl -sf localhost:8000/health                # API responds (after `make api`)
```

## Service map

| Service               | Image                              | Purpose                              |
|-----------------------|------------------------------------|--------------------------------------|
| `postgres`            | `postgres:16`                      | Medallion DB + Airflow metadata      |
| `kafka`               | `apache/kafka:3.8.0`               | Single-broker KRaft                  |
| `kafka-init`          | `apache/kafka:3.8.0`               | One-shot topic creator               |
| `spark`               | `apache/spark:3.5.1`               | Idle base for `spark-submit`         |
| `airflow-init`        | local (Dockerfile)                 | DB migrate + admin user + conn + Variable |
| `airflow-webserver`   | local (Dockerfile)                 | Airflow UI (port 8080)               |
| `airflow-scheduler`   | local (Dockerfile)                 | Runs DAGs                            |
| `ingestion`           | local (Dockerfile)                 | WS → Kafka producer                  |
| `dbt`                 | `ghcr.io/dbt-labs/dbt-postgres:1.8.0` | One-shot `dbt build`              |
| `api`                 | local (Dockerfile)                 | FastAPI (port 8000)                  |
| `dashboard`           | local (Dockerfile)                 | React + nginx (port 5173)            |

## Volumes

| Name                  | Used by         | Purpose                                  |
|-----------------------|-----------------|------------------------------------------|
| `pg_data`             | `postgres`      | Postgres data dir (medallion + airflow)  |
| `spark_checkpoints`   | `spark`         | Module 4 streaming checkpoint state      |

## Networks

A single user-defined bridge network `cryptostream` keeps every service
on the same L2 segment so `kafka:9092` and `postgres:5432` resolve
container-to-container without `-p` ports exposed except where the
host needs them.

## Health checks

| Service         | Healthcheck command                                                    |
|-----------------|------------------------------------------------------------------------|
| `postgres`      | `pg_isready -U $POSTGRES_USER -d $POSTGRES_DB`                         |
| `kafka`         | `kafka-broker-api-versions.sh --bootstrap-server kafka:9092`          |
| `airflow-webserver` | `curl -sf http://localhost:8080/health` matching `"metadatabase"`  |
| `airflow-scheduler` | `airflow jobs check --job-type SchedulerJob`                       |
| `api`           | `urllib` GET `http://localhost:8000/health` returning 200             |

Healthchecks gate the `depends_on: condition: service_healthy` chains,
which is why cold-boot takes 30+ s. The graph:

```
postgres ──▶ kafka ──▶ kafka-init
        └─▶ airflow-init ──▶ airflow-webserver
                         └─▶ airflow-scheduler
        └─▶ ingestion
        └─▶ dbt
        └─▶ api ──▶ dashboard
```

## Env vars consumed

See [ENV_REFERENCE.md — Module 1](ENV_REFERENCE.md#module-1--infrastructure-compose).

Specifically: `POSTGRES_*`, `KAFKA_*`, `DATABASE_URL`, `SPARK_CHECKPOINT_DIR`,
`FREECRYPTO_*`, `WATCHLIST`, `AIRFLOW_*`, `BRONZE_RETENTION_DAYS`.

## Failure modes

| Symptom                                      | Likely cause                                |
|----------------------------------------------|---------------------------------------------|
| `kafka` restart loop on first boot           | KRaft election; give it ~30 s and re-check |
| `airflow-init` exits non-zero                | Postgres not healthy yet; check `make logs SERVICE=postgres` |
| `dashboard` exits immediately                | `api` not healthy; check API logs           |
| `api` 503 on `/health`                       | DB down or `GOLD_SCHEMA` missing            |

## Tests

Module 1 has no test suite of its own — its correctness is verified by
every other module's tests when they exercise the running stack.