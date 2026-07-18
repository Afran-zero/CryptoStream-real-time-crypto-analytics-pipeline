# Module 6 — Orchestration (Airflow)

## Purpose

Schedule the batch lane and provide manual intervention paths:

- **`transform_dag`** runs `dbt build` every 5 minutes so Silver + Gold
  stay fresh.
- **`retention_dag`** purges Bronze rows older than the configured
  retention window daily.
- **`backfill_dag`** lets you re-run `dbt build` over a date range with
  `--vars`.

## Files

```
orchestration/
  Dockerfile                          # extends apache/airflow:2.9.3
  requirements-airflow.txt            # explicit pins: dbt-core 1.8 + dbt-postgres 1.8
  dags/
    _common.py                        # shared constants + dbt_env() helper
    transform_dag.py                  # */5 * * * * — dbt deps + build
    retention_dag.py                  # @daily — PythonOperator + batched DELETE
    backfill_dag.py                   # manual — conf → --vars file → dbt build
```

`airflow-init` (one-shot, in `docker-compose.yml`) does:

1. `airflow db migrate` against the Airflow metadata DB.
2. `airflow users create` if the admin user doesn't exist.
3. `airflow connections add postgres_default` pointing at the local
   Postgres.
4. `airflow variables set bronze_retention_days "${BRONZE_RETENTION_DAYS:-7}"`.

## DAGs

### `transform_dag`

- Schedule: `*/5 * * * *`
- Tasks:
  - `dbt_deps` (BashOperator): `dbt deps --no-version-check`
  - `dbt_build` (BashOperator): `dbt build --no-version-check`
- Env: `dbt_env()` from `_common.py` (POSTGRES_* with sensible defaults
  + passthrough of task env).
- Working dir: `/opt/airflow/transforms`.

### `retention_dag`

- Schedule: `@daily`
- Task: `purge_old_bronze` (PythonOperator):
  1. Reads `Variable.get("bronze_retention_days", default_var=7)`.
  2. Loops `DELETE FROM bronze.prices_raw WHERE event_time < cutoff
     LIMIT 10000` using `ctid` until no rows are deleted.
  3. Logs the deleted-row count.

### `backfill_dag`

- Schedule: none (manual trigger only).
- Inputs: `dag_run.conf = {"start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD"}`.
- Tasks:
  1. `write_vars_file` (PythonOperator): writes
     `.backfill_vars_<run_id>.json` with `backfill_start` and
     `backfill_end` from the conf.
  2. `dbt_build` (BashOperator): `dbt build --vars
     @/opt/airflow/transforms/.backfill_vars_<run_id>.json`.
  3. BashOperator's `on_success` / `on_failure` `trigger_rule` plus the
     PythonOperator's `trigger_rule="all_done"` ensure cleanup.

Usage:

```bash
make airflow-trigger DAG=backfill_dag CONF='{"start_date":"2026-06-01","end_date":"2026-06-02"}'
```

## How to run

```bash
make airflow-up       # build image, run init, start webserver + scheduler
```

Or as part of the full stack (already done by `make up`):

```bash
make up
```

Access the UI at <http://localhost:8080> (`admin` / `admin`).

## How to verify

```bash
# DAGs are registered
make airflow-list

# transform_dag ran successfully
make airflow-runs DAG=transform_dag

# Logs
make airflow-logs

# Trigger by hand
make airflow-trigger DAG=transform_dag
make airflow-trigger DAG=retention_dag
make airflow-trigger DAG=backfill_dag CONF='{"start_date":"2026-06-01","end_date":"2026-06-02"}'
```

## Env vars consumed

See [ENV_REFERENCE.md — Module 6](ENV_REFERENCE.md#module-6--orchestration-airflow).

Required (in `.env`): `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`,
`AIRFLOW__CORE__EXECUTOR`, `AIRFLOW__CORE__LOAD_EXAMPLES`,
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `POSTGRES_*`, `BRONZE_RETENTION_DAYS`.

`bronze_retention_days` is **also** an Airflow Variable — it's seeded
from `.env` on first boot. Changing `.env` after that doesn't
propagate; update the Variable via the UI or `airflow variables set`.

## Failure modes

| Symptom                                                     | Likely cause                                                |
|-------------------------------------------------------------|-------------------------------------------------------------|
| `airflow-init` exit non-zero                                | Postgres not healthy yet; re-run after Postgres settles     |
| `transform_dag` red                                         | A dbt test failed; check scheduler logs                     |
| `retention_dag` deletes 0 rows every day                    | Retention window too long, or Bronze has no rows that old   |
| `backfill_dag` conf ignored                                 | Conf JSON keys must be `start_date` / `end_date`            |

## Tests

There is no separate Module 6 test suite. The DAGs are exercised in
production-like usage via `make airflow-trigger`. Their correctness
is implicit in the round-trip:

- `transform_dag` red → dbt build red → check `make logs
  SERVICE=airflow-scheduler`.
- `retention_dag` red → Python traceback in scheduler logs.
- `backfill_dag` red → dbt vars file might be malformed.