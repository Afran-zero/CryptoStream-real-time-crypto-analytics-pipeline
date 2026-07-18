# CryptoStream — top-level convenience targets.
# Targets added by later modules:
#   stream  (Module 4),  dbt  (Module 5),
#   airflow-up / airflow-logs / airflow-trigger  (Module 6),
#   api / dashboard  (Module 7).
# Module 2 adds `migrate` (Compose) and `migrate-host` (external / Neon).
# Module 3 adds `test` (unit) and `test-integration` (real local WS loop).

# Load .env so env-only targets (psql, migrate) see POSTGRES_USER etc.
ifneq (,$(wildcard ./.env))
include .env
export
endif

COMPOSE  := docker compose
SERVICE  ?=
PROFILE ?=

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets.
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Build and start the full stack in the background.
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the stack (preserve volumes).
	$(COMPOSE) down

.PHONY: ps
ps: ## List services with health status.
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs (add SERVICE=name to filter).
	$(COMPOSE) logs -f $(SERVICE)

.PHONY: psql
psql: ## Open psql against the medallion DB. Pass args after `--`, e.g. `make psql -- -c "\\l"`.
	$(COMPOSE) exec postgres psql -U "$${POSTGRES_USER}" -d "$${POSTGRES_DB}" $(filter-out $@,$(MAKECMDGOALS))

.PHONY: topics
topics: ## List Kafka topics.
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server "$${KAFKA_BOOTSTRAP}" --list

.PHONY: nuke
nuke: ## Stop stack AND remove named volumes (destructive — loses data).
	$(COMPOSE) down -v

.PHONY: kafka-versions
kafka-versions: ## Confirm the broker is responding to API requests.
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
	  --bootstrap-server "$${KAFKA_BOOTSTRAP}" >/dev/null && echo "kafka OK"

.PHONY: migrate
migrate: ## Apply pending SQL migrations against $$DATABASE_URL (local Compose).
	$(COMPOSE) run --rm --no-deps \
	  -v "$(PWD)/db:/db" -w /db \
	  -e DATABASE_URL="$$DATABASE_URL" \
	  python:3.11-slim \
	  bash -lc "pip install -q -r /db/requirements.txt && python /db/run_migrations.py"

.PHONY: migrate-host
migrate-host: ## Apply migrations against $$DATABASE_URL on the host (use for Neon / external Postgres).
	python -m pip install -q -r db/requirements.txt && DATABASE_URL="$$DATABASE_URL" python db/run_migrations.py

.PHONY: test
test: ## Run ingestion service unit tests (no Kafka / no live WS).
	$(COMPOSE) run --rm --no-deps ingestion pytest -q

.PHONY: test-integration
test-integration: ## Run ingestion service integration tests (real local WS loop).
	$(COMPOSE) run --rm --no-deps ingestion pytest -q -m integration

.PHONY: stream
stream: ## Run the Spark → Bronze stream job (foreground; Ctrl-C to stop).
	$(COMPOSE) exec -T spark \
	  /opt/spark/bin/spark-submit \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
	    --conf spark.sql.streaming.checkpointLocation=/checkpoints/bronze \
	    /app/streaming/src/streaming/stream_to_bronze.py

.PHONY: stream-bg
stream-bg: ## Run the Spark → Bronze stream job in the background (returns immediately).
	$(COMPOSE) exec -d spark \
	  /opt/spark/bin/spark-submit \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
	    --conf spark.sql.streaming.checkpointLocation=/checkpoints/bronze \
	    /app/streaming/src/streaming/stream_to_bronze.py

.PHONY: test-integration-stream
test-integration-stream: ## Run streaming idempotency integration test (needs DATABASE_URL → Postgres).
	DATABASE_URL="$$DATABASE_URL" python -m pytest streaming/tests -q -m integration

.PHONY: dbt
dbt: ## Run dbt deps + dbt build inside the dbt container (Silver + Gold + tests).
	$(COMPOSE) run --rm dbt bash -lc "dbt deps --no-version-check && dbt build --no-version-check"

.PHONY: dbt-host
dbt-host: ## Run dbt on the host (use for Neon / external Postgres). Needs `pip install dbt-postgres`.
	cd transforms && DBT_PROFILES_DIR="$$PWD" dbt deps --no-version-check && DBT_PROFILES_DIR="$$PWD" dbt build --no-version-check

.PHONY: dbt-deps
dbt-deps: ## Fetch dbt packages only (dbt-utils) inside the container.
	$(COMPOSE) run --rm dbt dbt deps --no-version-check

.PHONY: airflow-up
airflow-up: ## Build the custom Airflow image and start webserver + scheduler.
	$(COMPOSE) up -d --build airflow-init && \
	  $(COMPOSE) up -d airflow-webserver airflow-scheduler

.PHONY: airflow-logs
airflow-logs: ## Tail scheduler logs.
	$(COMPOSE) logs -f airflow-scheduler

.PHONY: airflow-trigger
airflow-trigger: ## Trigger an Airflow DAG. Pass `DAG=name` (optional `CONF=...`).
	@if [ -z "$(DAG)" ]; then \
	  echo "Usage: make airflow-trigger DAG=transform_dag [CONF='{\"start_date\":\"...\"}']"; \
	  exit 2; \
	fi
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger $(DAG) $(if $(CONF),--conf '$(CONF)',)

.PHONY: airflow-list
airflow-list: ## List DAGs and recent runs.
	$(COMPOSE) exec -T airflow-scheduler airflow dags list

.PHONY: airflow-runs
airflow-runs: ## List recent runs of a DAG. Pass `DAG=name`.
	@if [ -z "$(DAG)" ]; then echo "Usage: make airflow-runs DAG=transform_dag"; exit 2; fi
	$(COMPOSE) exec -T airflow-scheduler airflow dags list-runs -d $(DAG)

.PHONY: api
api: ## Build the API image and start the serving tier (api + dashboard).
	$(COMPOSE) up -d --build api dashboard

.PHONY: api-logs
api-logs: ## Tail API logs.
	$(COMPOSE) logs -f api

.PHONY: api-test
api-test: ## Run API tests inside the api container (needs DATABASE_URL → Postgres).
	$(COMPOSE) run --rm --no-deps api pytest -q

.PHONY: dashboard-logs
dashboard-logs: ## Tail dashboard logs.
	$(COMPOSE) logs -f dashboard

# Catch-all so `make psql -- -c "\\l"` doesn't fail on the trailing args.
%:
	@: