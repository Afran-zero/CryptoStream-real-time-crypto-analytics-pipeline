"""Manual DAG: rebuild Silver + Gold over a date range.

Trigger from the UI or CLI with a JSON config:

    airflow dags trigger backfill_dag \\
        --conf '{"start_date": "2026-06-01", "end_date": "2026-06-02"}'

The dates are passed to `dbt build` via `--vars @vars.json` so models
that opt in (once they read `{{ var("backfill_start") }}` /
`{{ var("backfill_end") }}`) can scope their WHERE clauses to the window.
"""
from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_START_DATE, DBT_PROJECT_DIR, dbt_env

with DAG(
    dag_id="backfill_dag",
    description="Rebuild Silver + Gold over a date range (manual trigger).",
    schedule=None,
    start_date=DAG_START_DATE,
    catchup=False,
    params={
        "start_date": "2026-06-01",
        "end_date": "2026-06-02",
    },
    default_args={
        "owner": "cryptostream",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["cryptostream", "dbt", "backfill"],
) as dag:
    # `{{ run_id }}` is Airflow's Jinja for the run id (unique per trigger),
    # so concurrent backfills don't collide on the same vars file. The
    # `trap` cleans up the file regardless of dbt's exit status.
    BashOperator(
        task_id="dbt_build_with_vars",
        bash_command=(
            "set -euo pipefail; "
            f"cd {DBT_PROJECT_DIR} && "
            "vars_file=\".backfill_vars_{{ run_id }}.json\" && "
            "trap 'rm -f \"$vars_file\"' EXIT && "
            # printf interprets \n as a newline (bash printf, not /usr/bin/printf).
            "printf '{\"backfill_start\":\"%s\",\"backfill_end\":\"%s\"}\\n' "
            "\"{{ dag_run.conf.get('start_date', params.start_date) }}\" "
            "\"{{ dag_run.conf.get('end_date', params.end_date) }}\" "
            "> \"$vars_file\" && "
            "dbt build --no-version-check --vars @$vars_file"
        ),
        env=dbt_env(),
    )