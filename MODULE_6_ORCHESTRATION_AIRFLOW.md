# Module 6 — Orchestration (Airflow)

## Context & Objective
Put the batch lane under Airflow. Airflow schedules the dbt transformation, enforces
retention, and supports parameterized backfills — all while staying strictly out of
the streaming lane. Objective: a scheduled `transform_dag` that runs `dbt build`
green on a cadence, a `retention_dag` that prunes Bronze on a defined window, and a
manually triggerable `backfill_dag` accepting a date range.

## Prerequisites
- Module 5 complete: `make dbt` (`dbt build`) succeeds on real data.
- Codebase state: `orchestration/` empty except `.gitkeep`; Airflow placeholder
  services from Module 1 exist and will be replaced by a proper image here.

## Technical Specifications
Build a custom Airflow image (`orchestration/Dockerfile`) extending
`apache/airflow:2.9.3` with `dbt-postgres` and `dbt_utils` deps installed, and the
`transforms/` dbt project mounted/copied in. Executor: `LocalExecutor`, metadata in
the `airflow` Postgres database from Module 1. Replace the Module 1 Airflow
placeholders in `docker-compose.yml` with `airflow-init`, `airflow-webserver`
(port 8080), `airflow-scheduler` using this image.

DAGs in `orchestration/dags/`:
- `transform_dag.py` — schedule `*/5 * * * *` (or `@hourly` for lower churn),
  `catchup=False`. Single task group: `dbt deps` → `dbt build` via `BashOperator`
  (or Cosmos if preferred) against the mounted project with `DATABASE_URL` env. A
  failing dbt test fails the task (and the DAG run) visibly.
- `retention_dag.py` — schedule `@daily`. Deletes `bronze.prices_raw` rows older
  than a configurable window (Airflow Variable `bronze_retention_days`, default 7)
  using a `SQLExecuteQueryOperator`/`PostgresOperator`. Gold is retained.
- `backfill_dag.py` — `schedule=None`, params `start_date`, `end_date`. Runs
  `dbt build --vars '{backfill_start: ..., backfill_end: ...}'` so models can filter
  to the window. Triggerable from UI or CLI with a config JSON.

## Step-by-Step Implementation Instructions
1. Write `orchestration/Dockerfile`; add `orchestration/requirements-airflow.txt`
   pinning `dbt-postgres`. Copy the dbt project to a known path (e.g.
   `/opt/airflow/transforms`).
2. Update `docker-compose.yml`: proper `airflow-init` (runs `airflow db migrate` and
   creates an admin user), `airflow-webserver`, `airflow-scheduler`; mount
   `orchestration/dags` → `/opt/airflow/dags`; pass `DATABASE_URL` and Airflow env
   (`AIRFLOW__CORE__EXECUTOR=LocalExecutor`,
   `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` → the `airflow` DB).
3. Implement the three DAGs. Keep the dbt invocation identical to Module 5's proven
   command so behavior matches (`dbt deps && dbt build`).
4. Add Make targets: `airflow-up`, `airflow-logs`, and `airflow-trigger DAG=...`.
5. Set Airflow Variable `bronze_retention_days=7` via `airflow-init` or CLI.

## Verification & Testing Criteria
```bash
make airflow-up
# UI reachable
curl -sf localhost:8080/health | grep -q '"scheduler"' && echo "airflow OK"

# transform_dag runs green
docker compose exec airflow-scheduler airflow dags trigger transform_dag
docker compose exec airflow-scheduler airflow dags list-runs -d transform_dag
# ^ latest run state must reach success; dbt tests pass inside it

# retention_dag prunes old bronze rows
make psql -c "insert into bronze.prices_raw(symbol,exchange,price,event_time,source)
              values('BTCUSD','binance',1,'2000-01-01T00:00:00Z','seed');"
docker compose exec airflow-scheduler airflow dags trigger retention_dag
sleep 20
make psql -c "select count(*) from bronze.prices_raw where event_time < now() - interval '7 days';"
# ^ must be 0 after retention run

# backfill_dag accepts a range
docker compose exec airflow-scheduler airflow dags trigger backfill_dag \
  --conf '{"start_date":"2026-06-01","end_date":"2026-06-02"}'
# ^ run reaches success
```
Success = all three DAGs reach `success`, retention deletes out-of-window Bronze
rows, and backfill accepts and runs a date range.

## Hand-off State
- Airflow (LocalExecutor) running on port 8080 with a proper image containing dbt.
- `transform_dag` (scheduled, green), `retention_dag` (daily prune), `backfill_dag`
  (parameterized, manual) all verified.
- Streaming lane untouched by Airflow — separation invariant preserved.
Module 7 relies on Gold being kept fresh by `transform_dag` while it serves reads.
