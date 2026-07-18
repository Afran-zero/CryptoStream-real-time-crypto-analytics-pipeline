"""Shared constants and helpers for the CryptoStream Airflow DAGs.

Centralizes the dbt project path, the Postgres env vars dbt reads
(via `transforms/profiles.yml`), and the default retention window so
the values don't drift across DAGs.
"""
from __future__ import annotations

import os
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/transforms"

POSTGRES_CONN_ID = "postgres_default"

# Bronze retention window in days. Also the default written by
# airflow-init into the Airflow Variable of the same name.
BRONZE_RETENTION_DAYS_DEFAULT = 7

DAG_START_DATE = datetime(2026, 1, 1)


def dbt_env() -> dict[str, str]:
    """Compose env for dbt-invoking BashOperators.

    Postgres creds mirror `transforms/profiles.yml` env keys. Override
    values come from Airflow's task env, so any `Variable` updates flow
    through without touching this file.
    """
    return {
        "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
        "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
        "POSTGRES_USER": os.environ.get("POSTGRES_USER", "cryptostream"),
        "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "cryptostream"),
        "POSTGRES_DB": os.environ.get("POSTGRES_DB", "cryptostream"),
        **os.environ,
    }
