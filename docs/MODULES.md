# Modules

CryptoStream is built in seven modules. Each one ships a single
responsibility, a single set of files, and a clear "done" check.

| # | Module                          | What it owns                                     | Status |
|---|---------------------------------|--------------------------------------------------|--------|
| 1 | [Infrastructure](MODULE_1_INFRASTRUCTURE.md) | Docker Compose stack, volumes, Kafka topics, Airflow image | ✅ |
| 2 | [Database & medallion](MODULE_2_DATABASE.md) | Postgres schemas, Bronze table, migration runner          | ✅ |
| 3 | [Ingestion](MODULE_3_INGESTION.md)         | WebSocket → Kafka producer, DLQ, normalisation           | ✅ |
| 4 | [Stream processing](MODULE_4_STREAMING.md) | Spark Structured Streaming → Bronze upsert              | ✅ |
| 5 | [Transforms](MODULE_5_TRANSFORMS.md)       | dbt project: Silver + Gold                              | ✅ |
| 6 | [Orchestration](MODULE_6_ORCHESTRATION.md) | Airflow: transform / retention / backfill DAGs          | ✅ |
| 7 | [API + Dashboard](MODULE_7_API_DASHBOARD.md) | FastAPI service + React dashboard                       | ✅ |

Each module page has the same sections:

1. **Purpose** — what it owns and why.
2. **Files** — every file the module creates or modifies.
3. **How to run** — the Make / shell commands to exercise it.
4. **How to verify** — concrete "this is working" checks.
5. **Env vars consumed** — with link back to [ENV_REFERENCE.md](ENV_REFERENCE.md).
6. **Failure modes** — what breaks if X is wrong.
7. **Tests** — how to run the module's test suite.