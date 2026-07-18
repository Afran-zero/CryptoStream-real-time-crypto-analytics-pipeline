# Module 1 — Infrastructure

## Context & Objective
Stand up the entire local runtime as a reproducible Docker Compose stack and lay
down the repository skeleton. Everything downstream assumes these services exist,
are networked together, and are healthy. Objective: `make up` brings up Postgres,
a single-node Kafka broker with the two required topics, a Spark base image, and
Airflow — all on one Docker network — and `make ps` shows every service healthy.

## Prerequisites
- Module 0 read. Host has Docker + Compose v2, `make`, `git`.
- Codebase state: empty repository (only the `docs/` markdown files may exist).

## Technical Specifications
Single Docker network `cryptostream`. One Postgres instance holds two databases:
`cryptostream` (medallion) and `airflow` (Airflow metadata). Kafka runs in KRaft
mode (no Zookeeper). Two topics are created at startup: `prices` and `prices.dlq`,
3 partitions each, replication factor 1.

Services:
- `postgres` — image `postgres:16`, healthcheck via `pg_isready`.
- `kafka` — image `apache/kafka:3.8.0`, KRaft single-node, listener `kafka:9092`.
- `kafka-init` — runs `infra/kafka/create_topics.sh`, then exits 0.
- `spark` — image `apache/spark:3.5.1`, idle base container (`tail -f /dev/null`);
  Module 4 runs `spark-submit` inside it.
- `airflow-init`, `airflow-webserver`, `airflow-scheduler` — image built in
  Module 6; for now define placeholders using `apache/airflow:2.9.3` that stay up.

## Step-by-Step Implementation Instructions
1. Create the repo skeleton directories from Module 0 §3 (empty with `.gitkeep`).
2. Create `.env.example` with the variables from Module 0 §4; `cp .env.example .env`.
3. Create `infra/kafka/create_topics.sh`:
   ```bash
   #!/usr/bin/env bash
   set -euo pipefail
   for t in "$KAFKA_TOPIC_PRICES" "$KAFKA_TOPIC_DLQ"; do
     /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
       --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
   done
   /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list
   ```
4. Create `docker-compose.yml` defining the services above. Requirements:
   - `postgres` mounts an init script that creates the `airflow` database in
     addition to `POSTGRES_DB` (use `/docker-entrypoint-initdb.d/`).
   - `kafka` configured for KRaft single-node (set `KAFKA_NODE_ID`,
     `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_CONTROLLER_QUORUM_VOTERS`,
     advertised listener `PLAINTEXT://kafka:9092`, and a `CONTROLLER` listener).
   - `kafka-init` `depends_on` kafka with `condition: service_healthy` and runs the
     topic script; mount `infra/kafka/` read-only; pass `KAFKA_*` env.
   - Named volumes: `pg_data`, `spark_checkpoints` (mounted at
     `${SPARK_CHECKPOINT_DIR%/*}` on the spark service).
   - Every long-running service has a `healthcheck`.
5. Create a `Makefile` with targets: `up` (`docker compose up -d --build`),
   `down` (`docker compose down`), `ps` (`docker compose ps`), `logs`
   (`docker compose logs -f`), `psql` (open psql to the medallion DB),
   `topics` (list Kafka topics), `nuke` (`docker compose down -v`).
6. Create a root `README.md` with a 5-line quickstart (`cp .env.example .env`,
   `make up`, `make ps`).

## Verification & Testing Criteria
Run and confirm each:
```bash
make up
make ps                     # every service: healthy or completed (kafka-init exits 0)
make topics                 # must list: prices, prices.dlq
make psql -c "\l"           # must list databases: cryptostream, airflow
docker compose exec postgres pg_isready -U "$POSTGRES_USER"   # accepting connections
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka:9092 >/dev/null && echo "kafka OK"
```
All five checks must pass. If `kafka-init` errored, fix the KRaft listener config
before proceeding — do not continue with a broker that failed topic creation.

## Hand-off State
- A running Compose stack on network `cryptostream`.
- Postgres reachable at `postgres:5432` with databases `cryptostream` and `airflow`.
- Kafka reachable at `kafka:9092` with topics `prices` and `prices.dlq` (3 parts each).
- Spark idle container ready for `spark-submit`.
- Airflow placeholder services running (rebuilt properly in Module 6).
- Repo skeleton and `.env` in place.
Module 2 assumes it can reach Postgres via `DATABASE_URL` from a one-off container.
