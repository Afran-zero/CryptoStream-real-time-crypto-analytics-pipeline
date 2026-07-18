"""Daily DAG: prune `bronze.prices_raw` rows older than the retention window.

The retention window is configurable via the Airflow Variable
`bronze_retention_days` (default `BRONZE_RETENTION_DAYS_DEFAULT` from
`_common`). Gold is unaffected — Silver/Gold are rebuilt from Bronze on
the next `transform_dag` run.

The DELETE is batched (10k rows per iteration) so a large Bronze doesn't
hold row-level locks long enough to stall the streaming writer (Module 4).
"""
from __future__ import annotations

from datetime import timedelta

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from _common import (
    BRONZE_RETENTION_DAYS_DEFAULT,
    DAG_START_DATE,
    POSTGRES_CONN_ID,
)

BATCH_SIZE = 10_000


def _prune_bronze(ds=None, **context) -> None:
    """Delete in batches of BATCH_SIZE until no rows match."""
    days = int(
        Variable.get(
            "bronze_retention_days",
            default_var=BRONZE_RETENTION_DAYS_DEFAULT,
        )
    )

    # Resolve the connection from Airflow's connection registry so the
    # DAG is decoupled from any specific Postgres URL.
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)

    dsn = (
        f"host={conn.host} port={conn.port} dbname={conn.schema} "
        f"user={conn.login} password={conn.password}"
    )

    total = 0
    with psycopg.connect(dsn) as db:
        with db.cursor() as cur:
            while True:
                cur.execute(
                    "with expired as ( "
                    "  select ctid from bronze.prices_raw "
                    "  where event_time < now() - make_interval(days => %s) "
                    "  limit %s "
                    ") "
                    "delete from bronze.prices_raw "
                    "where ctid in (select ctid from expired)",
                    (days, BATCH_SIZE),
                )
                if cur.rowcount == 0:
                    break
                total += cur.rowcount
        db.commit()

    print(f"retention: deleted {total} rows older than {days} days")


with DAG(
    dag_id="retention_dag",
    description="Prune Bronze rows older than the retention window.",
    schedule="@daily",
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cryptostream",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["cryptostream", "retention", "batch"],
) as dag:
    PythonOperator(
        task_id="prune_bronze_prices_raw",
        python_callable=_prune_bronze,
    )