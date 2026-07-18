# 12 — Design decisions (the "why")

Every choice in CryptoStream could have been made differently.
This page lists the trade-offs we considered and what we picked,
so you understand the reasoning — not just the result.

---

## Infrastructure

### Docker Compose vs Kubernetes

| | Compose | Kubernetes |
|--|---------|------------|
| Setup | One YAML, `docker compose up` | Manifests, helm charts, ingress, ... |
| Cold boot | 30–60 s | Minutes |
| Resource overhead | Minimal | Control plane + nodes |
| Learning curve | Easy | Steep |
| Multi-host | Limited (single host) | Native |
| Right for | Local dev, demos, single-node prod | Multi-node prod at scale |

**Pick: Compose.** This is a local-first demo. Compose gives us
one-command bring-up. We can graduate to k8s later if needed (and
the manifests would be a port, not a rewrite — most of the
container images are already correct).

### Single-host Postgres + Kafka vs multi-node

For a demo, a single Postgres container and a single Kafka broker
are plenty. For production:

- Postgres: a primary + read replicas; or move to managed Postgres
  (Neon, RDS, Supabase).
- Kafka: 3+ brokers for replication; partition counts >1 for
  consumer parallelism.

We picked single-node for **simplicity**. The architectures don't
change — only the deployment manifests do.

---

## Kafka

### KRaft vs Zookeeper

Older Kafka required Zookeeper. Modern Kafka (3.3+) supports
**KRaft mode** where the Kafka brokers themselves manage cluster
state.

**Pick: KRaft.** No Zookeeper = one fewer container to run. KRaft
is the future direction of Kafka anyway; Zookeeper support is
deprecated.

### One partition vs many for `prices`

Partitions enable parallel consumption. With one partition, only
one consumer can read at a time.

**Pick: 1 partition.** We have one consumer (Spark). More
partitions would mean overhead without benefit. If we later add a
second consumer (e.g. a fraud detector), we'd bump to 2 or 3.

### Idempotent producer

`enable.idempotence=true` adds sequence numbers to messages; the
broker deduplicates.

**Pick: enabled.** Zero downside. With idempotent producer +
unique constraint at Bronze, we get end-to-end exactly-once
semantics "for free".

---

## Database

### numeric(20, 8) vs float vs decimal (no precision)

| Type | Pros | Cons |
|------|------|------|
| `float` / `double` | Fast, compact | Rounding errors (e.g. `0.1 + 0.2 ≠ 0.3`) |
| `numeric` (no precision) | Arbitrary precision | Slow |
| `numeric(20, 8)` | Exact, fast enough for our use | Caps at 8 decimal places |

**Pick: `numeric(20, 8)`.** Crypto prices fit comfortably in 8
decimal places (BTC has 8 satoshis per BTC). Storage is bigger
than `float` but small by modern standards. Precision is exact.

### timestamptz vs timestamp

`timestamp` (without timezone) loses context: what does
`2026-07-19 14:30:00` mean? In UTC? In the writer's local time?

**Pick: `timestamptz`.** Postgres converts to UTC internally and
displays in the session's timezone. Always unambiguous.

### Unique constraint on Bronze vs no constraint

Without the constraint, restart-after-crash could produce
duplicates. With it, `INSERT ... ON CONFLICT DO NOTHING` becomes
idempotent.

**Pick: yes, always.** The constraint is what makes Module 4's
verification step (`no_dupes = true`) actually meaningful.

### Medallion (Bronze/Silver/Gold) vs single table

Three layers means more storage and more rebuilds. One layer
means every consumer sees raw data and the project becomes a
write-only system you can never refactor.

**Pick: medallion.** The extra cost is tiny; the safety of
"always re-derive from raw" is huge.

---

## Ingestion

### WebSocket vs polling

| | WebSocket | HTTP polling |
|--|-----------|--------------|
| Latency | Push; ~50 ms | Pull; up to poll interval |
| Server load | Low (broadcast) | High (per-client) |
| Complexity | More (lifecycle, reconnect) | Less |

**Pick: WebSocket.** Crypto exchanges expect WebSocket clients;
the latency is right.

### async (asyncio) vs threading

`websockets` is async-native. Python's threading has the GIL and
is awkward for I/O-heavy code.

**Pick: asyncio.** One event loop, no thread coordination, clean
code.

### DLQ topic vs silent drop

Bad messages either go to a DLQ topic or are silently dropped.

**Pick: DLQ topic.** Visibility. If the source sends garbage, we
notice. Auditable. Reprocessable.

### confluent-kafka vs kafka-python vs aiokafka

| Library | Style | Notes |
|---------|-------|-------|
| `confluent-kafka` | Wraps librdkafka (C) | Fastest, most features, sync + async |
| `kafka-python` | Pure Python | Simpler, slower |
| `aiokafka` | Pure Python async | Async-native, less mature |

**Pick: confluent-kafka.** Best performance; native support for
idempotent producer; the industry standard.

---

## Spark / streaming

### Spark Structured Streaming vs Kafka Streams vs Flink

| | Spark SS | Kafka Streams | Flink |
|--|----------|---------------|-------|
| Native Kafka source | ✓ | ✓ | ✓ |
| foreachBatch (custom sink) | ✓ | ✗ | ✓ |
| Multi-language | Python, Scala, Java, R | Java only | Scala, Java |
| Learning curve | Moderate | Steep | Steep |
| Operational overhead | Spark cluster | Just Kafka | Flink cluster |

**Pick: Spark SS.** The `foreachBatch` hook is exactly what we
need for the staged upsert. Python is the easiest to maintain.

### Staged upsert vs plain JDBC append

Plain `df.write.jdbc(mode="append")` would let duplicates
through.

**Pick: staged upsert.** Unique constraint + `ON CONFLICT DO
NOTHING` gives us idempotency at the database level. A per-batch
staging table avoids contention on shared temp tables.

### 10-second trigger interval

Lower = lower latency, higher DB load. Higher = more lag, less
load.

**Pick: 10 seconds.** A good balance for a demo. In prod you'd
tune based on Bronze write rate and DB capacity.

### `failOnDataLoss=false`

If Kafka deletes messages before we read them (retention expiry),
do we crash or skip?

**Pick: false.** On a single-broker dev cluster, retention expiry
is plausible. We tolerate it; the unique constraint still
prevents duplicates on the rows we *do* read.

---

## dbt

### dbt vs hand-written SQL

For 3 models, hand-written SQL would work. For 30, you want
dependencies and tests.

**Pick: dbt.** Cheap to start, valuable as it grows. We use the
same `dbt build` command from the CLI and from Airflow.

### `table` vs `incremental` materialisation

`table` rebuilds fully every run. `incremental` only adds new
rows.

**Pick: `table`.** Our scale (thousands of rows) makes full
rebuilds instant. `incremental` would add bugs (handling schema
changes, late-arriving data) without much benefit.

### Literal schemas (`silver`, `gold`) vs `dev_silver`, `dev_gold`

dbt's default is to prefix the schema with the target name.
CryptoStream's API queries `gold.candles_1m` directly.

**Pick: literal schemas.** The `generate_schema_name.sql` macro
overrides the default. The API and dashboard don't care what
target dbt is using.

---

## Airflow

### Airflow vs cron

Cron runs jobs at times. Airflow runs jobs in a graph, with
retries, with a UI.

**Pick: Airflow.** Even for the simple "every 5 minutes run dbt"
case, Airflow's UI + retries + backfill support are worth it.

### LocalExecutor vs CeleryExecutor vs KubernetesExecutor

| Executor | Tasks run | Right for |
|----------|-----------|-----------|
| LocalExecutor | Subprocesses on scheduler host | Single-node dev |
| CeleryExecutor | Distributed workers | Mid-scale prod |
| KubernetesExecutor | Each task in a k8s pod | High-scale prod |

**Pick: LocalExecutor.** Single-node demo. Subprocesses are fine.

### */5 schedule for transform_dag

Lower = fresher Gold, more DB load. Higher = less fresh, less load.

**Pick: every 5 minutes.** Reasonable middle ground for a demo.
For prod, you'd switch to incremental materialisations and
trigger via sensors.

### Retention via PythonOperator vs SQL

A `PostgresOperator` with a templated `DELETE FROM ... WHERE
event_time < {{ ... }}` is shorter, but Jinja templating has
sharp edges.

**Pick: PythonOperator.** Reads the Variable at task time, loops
over 10k-row batched DELETEs. Easier to reason about, easier to
test, easier to log progress.

---

## API

### FastAPI vs Flask vs Django

| | FastAPI | Flask | Django |
|--|---------|-------|--------|
| Type-driven | ✓ (Pydantic) | ✗ | Partial |
| Async native | ✓ | ✗ | Partial |
| OpenAPI | Auto | Manual | Partial |
| ORM | Bring your own | Bring your own | Built-in |
| Right for | Typed APIs | Small prototypes | Full web apps |

**Pick: FastAPI.** Type hints + Pydantic = fewer bugs. Auto
OpenAPI = free documentation. We don't need Django's ORM or
admin.

### psycopg 3 + connection pool vs SQLAlchemy

| | psycopg 3 | SQLAlchemy |
|--|-----------|------------|
| Style | Lower-level | Higher-level ORM |
| Overhead | Minimal | Some |
| Best for | Raw SQL with type safety | Object-relational mapping |

**Pick: psycopg 3 + pool.** Our SQL is hand-written and small. An
ORM would add complexity for no gain.

### Repository pattern vs inline SQL in endpoints

Inline SQL in endpoints is faster to write initially; harder to
test and refactor.

**Pick: repository pattern.** All SQL lives in one file. Endpoints
focus on HTTP concerns. Easy to swap the DB driver later.

### Decimal in JSON as string vs float

JSON has no native decimal type. `json.dumps(0.1 + 0.2)`
produces `'0.30000000000000004'`. `json.dumps(Decimal('0.30'))`
produces `'0.30'` exactly.

**Pick: Decimal as string.** The client parses the string back
into a `Decimal` (or equivalent). Precision preserved.

---

## Dashboard

### React vs Vue vs Svelte

| | React | Vue | Svelte |
|--|-------|-----|--------|
| Ecosystem | Largest | Large | Growing |
| Job market | Most jobs | Many | Fewer |
| Learning curve | Moderate | Easy | Easy |

**Pick: React.** Largest ecosystem, most common in the industry,
matches the team's experience.

### Vite vs Create React App vs Next.js

| | Vite | CRA | Next.js |
|--|------|-----|---------|
| Status | Active | Deprecated | Active |
| Dev speed | Fast | Slow | Fast |
| SSR / routing | No | No | Yes |

**Pick: Vite.** CRA is dead. Next.js is overkill for a single-page
dashboard.

### Polling vs WebSocket vs Server-Sent Events

| | Polling | SSE | WebSocket |
|--|---------|-----|-----------|
| Direction | Client → Server | Server → Client | Both |
| Setup | Just `setInterval` | Simple | More involved |
| Right for | 5-second updates | One-way push | Two-way |

**Pick: polling every 5s.** The simplest thing that works. SSE
would be nicer but not necessary at 5-second cadence.

### AbortController for in-flight requests

Without it, an in-flight `fetch` would resolve after the user
changed symbols, leading to stale data overwriting fresh state.

**Pick: AbortController.** Cancels the fetch; component state
isn't corrupted.

### recharts vs Chart.js vs D3

| | recharts | Chart.js | D3 |
|--|----------|----------|-----|
| React-native | ✓ | ✗ (wrap needed) | ✗ |
| Simplicity | High | High | Low |
| Customisation | Medium | Medium | Infinite |

**Pick: recharts.** React-native, declarative, plenty for our
needs.

---

## Operational

### Local Postgres vs managed (Neon)

| | Local | Managed (Neon) |
|--|-------|----------------|
| Setup | One container | Sign up, create project |
| Cost | Free | Free tier; $$ for scale |
| Backups | Manual | Automatic |
| Branching | No | Yes (Neon's killer feature) |
| Right for | Local dev | Prod / staging |

**Pick: local for the demo.** The `.env` and compose files are
already set up so Neon could be plugged in by changing
`DATABASE_URL` and `POSTGRES_*` in `.env` and using
`make migrate-host` / `make dbt-host` for external execution.

### One postgres container for medallion + Airflow metadata

Airflow needs its own metadata DB. We could run two Postgres
containers; instead we use one with two databases (medallion +
`airflow`).

**Pick: one container.** Less resource use, simpler compose. The
`airflow` DB is created by
`infra/postgres-init/01-create-airflow-db.sql` on first boot.

---

## Summary table

| Concern | Decision | Why |
|---------|----------|-----|
| Container orchestration | Docker Compose | Single command bring-up |
| Kafka | KRaft, single broker, idempotent producer | Simpler, modern |
| Database | Postgres 16, numeric(20,8), timestamptz | Exact precision, UTC-safe |
| Medallion | Bronze/Silver/Gold | Re-derivable from raw |
| Ingestion | asyncio + WebSocket + DLQ topic | Push, low latency, auditable |
| Streaming | Spark Structured Streaming + foreachBatch + staged upsert | Idempotent by construction |
| Batch | dbt with table materialisation + tests | SQL + dependency graph |
| Orchestration | Airflow + LocalExecutor | Retries + UI + backfills |
| API | FastAPI + Pydantic + psycopg pool + repository | Type-driven, testable |
| Dashboard | React + Vite + recharts, polling | Simplest viable stack |
| Hosting | Local Docker; Neon-ready | Demo; portable to managed |

---

## What you'd change for production

| Concern | Local | Production |
|---------|-------|------------|
| Postgres | Single container | Managed (Neon, RDS) with replicas |
| Kafka | Single broker | 3+ brokers, more partitions |
| Spark | One executor | Spark cluster (k8s, EMR) |
| Airflow | LocalExecutor | CeleryExecutor or KubernetesExecutor |
| Dashboard | Polling every 5s | WebSocket or SSE for sub-second updates |
| Secrets | .env file | Vault, AWS Secrets Manager, k8s secrets |
| Backups | None | Automated snapshots |
| Auth | None (admin/admin) | OIDC, RBAC |
| Monitoring | Compose healthchecks | Prometheus, Grafana, Sentry |
| CI/CD | Manual | GitHub Actions, automated tests |

The local demo and the production architecture share the same
**shape** — the differences are in scale and operational
machinery, not in the core data flow.

---

## What's next?

- [13_HANDS_ON_TOUR.md](13_HANDS_ON_TOUR.md) — guided exercises.
- [14_GLOSSARY.md](14_GLOSSARY.md) — every term.