"""Scheduled DAG: refresh Silver + Gold via `dbt build` every 5 minutes.

Streams are kept fresh by the Spark job (Module 4); this DAG only
re-materializes the batch lane so the API (Module 7) reads Gold that's
at most a few minutes stale.
"""
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_START_DATE, DBT_PROJECT_DIR, dbt_env

with DAG(
    dag_id="transform_dag",
    description="dbt build: refresh Silver + Gold.",
    schedule="*/5 * * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cryptostream",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["cryptostream", "dbt", "batch"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --no-version-check",
        env=dbt_env(),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --no-version-check",
        env=dbt_env(),
    )

    dbt_deps >> dbt_build