# CryptoStream — custom Airflow image (Module 6).
# Extends apache/airflow:2.9.3 with dbt-postgres + the dbt project mounted
# at /opt/airflow/transforms. DAGs live at /opt/airflow/dags (mounted from
# orchestration/dags in compose).
FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

USER airflow

# Pin against Airflow's own constraint file so provider deps don't drift
# out of sync with the core release. The base image's python version
# determines which constraints file we use.
ARG AIRFLOW_CONSTRAINTS_FILE="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
ENV AIRFLOW_CONSTRAINTS_FILE=${AIRFLOW_CONSTRAINTS_FILE}

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
      --constraint "${AIRFLOW_CONSTRAINTS_FILE}" \
      -r /tmp/requirements-airflow.txt

# The transforms/ tree is bind-mounted at runtime (see docker-compose),
# but copying it into the image lets `airflow dags list` and DAG parsing
# succeed in environments where the bind mount isn't available.
COPY --chown=airflow:root transforms /opt/airflow/transforms

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    DBT_PROFILES_DIR=/opt/airflow/transforms