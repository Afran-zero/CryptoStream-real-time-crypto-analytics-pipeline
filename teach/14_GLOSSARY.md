# 14 — Glossary

Every term used in CryptoStream, with a plain-language definition
and a pointer to where to learn more.

If a term isn't here that you think should be, let us know.

---

## A

### ACID

A set of guarantees a database makes: **A**tomic (all-or-nothing),
**C**onsistent (rules aren't violated), **I**solated (concurrent
transactions don't interfere), **D**urable (committed = on disk).
Postgres is ACID. See
[02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md).

### Airflow

A workflow orchestrator. DAGs are graphs of tasks; Airflow runs
them on a schedule. See
[08_AIRFLOW_FUNDAMENTALS.md](08_AIRFLOW_FUNDAMENTALS.md).

### Airflow Variable

A key-value config item Airflow stores and DAGs read at runtime.
CryptoStream uses one: `bronze_retention_days`.

### API

**A**pplication **P**rogramming **I**nterface — a way for
programs to talk to each other. In CryptoStream, "the API" usually
means the FastAPI service.

### Argument

A value passed to a function. `f(x=1)` passes `x=1` as an argument
to `f`.

### async / await

A Python syntax for non-blocking I/O. Functions declared with
`async def` can `await` other async functions. Used for
WebSocket and other concurrent I/O.

### autospec

A pytest feature where mocks are validated against the real
function's signature.

---

## B

### Backoff

A retry strategy that waits progressively longer between attempts
(e.g. 1s, 2s, 4s, 8s, ...). Combined with **jitter**, prevents
thundering-herd reconnects.

### Batch

A group of records processed together. Spark Structured Streaming
uses **micro-batches**; dbt uses **batches** (full rebuilds).

### Bronze / Silver / Gold

The three **medallion** layers. Bronze = raw. Silver = typed.
Gold = aggregated. See
[02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md#the-medallion-pattern).

### Business key

The unique identifier of an observation in the business domain.
For CryptoStream: `(symbol, exchange, event_time)`.

---

## C

### Cached checkpoint

A directory where Spark stores its progress (offsets, batch IDs).
On restart, Spark resumes from there.

### Causal consistency

A weaker guarantee than ACID; used in distributed systems.

### Check constraint

A rule a column must satisfy (e.g. `price > 0`). Postgres rejects
rows that violate it.

### Checkpoint

Saved state for fault recovery. Spark uses them for Kafka offsets;
Airflow uses them to track DAG runs.

### Column

One field in a table. A row of `prices_raw` has columns like
`symbol`, `price`, `event_time`.

### Compose

A YAML file (`docker-compose.yml`) that declares multi-container
apps. See
[04_DOCKER_FUNDAMENTALS.md](04_DOCKER_FUNDAMENTALS.md).

### Connection pool

A set of reusable DB connections. Cheaper than opening a new one
per request.

### Container

A running instance of an image. Isolated process + filesystem.

### CORS

**C**ross-**O**rigin **R**esource **S**haring — the rules
browsers enforce to limit which sites can call your API. See
[09_FASTAPI_FUNDAMENTALS.md](09_FASTAPI_FUNDAMENTALS.md#cors).

### cron

A Unix scheduler. CryptoStream doesn't use it; Airflow's schedule
is cron-like.

### `ctid`

Postgres internal column with the physical location of a row. Used
in retention's batched DELETE.

---

## D

### DAG

**D**irected **A**cyclic **G**raph — the structure of a workflow
in Airflow. No cycles.

### Dashboard

The React UI in CryptoStream. Serves the chart, prices table, and
health badge. See
[10_REACT_FUNDAMENTALS.md](10_REACT_FUNDAMENTALS.md).

### DataFrame

A named, typed table. Spark's main abstraction. Operations are
lazy.

### dbt

**D**ata **B**uild **T**ool. SQL files that produce tables, with
a dependency graph and tests. See
[07_DBT_FUNDAMENTALS.md](07_DBT_FUNDAMENTALS.md).

### Decimal

An exact-arithmetic numeric type. `Decimal('0.1') + Decimal('0.2')
== Decimal('0.3')` exactly. Used for prices.

### Decorator

A Python syntax that wraps a function. `@app.get('/health')` wraps
the function below it as a FastAPI route.

### Dependency injection

Passing things to a function rather than having it create them.
FastAPI's `Depends(...)` does this.

### DLQ

**D**ead-**L**etter **Q**ueue — a Kafka topic for messages the
main pipeline can't handle. CryptoStream has `prices.dlq`.

### Docker

A platform for packaging and running programs in isolated
containers. See
[04_DOCKER_FUNDAMENTALS.md](04_DOCKER_FUNDAMENTALS.md).

### Dockerfile

Instructions for building a Docker image.

### Docker Compose

Tool for declaring multi-container apps in YAML.

### Dockerfile build context

The directory Docker uses to find files referenced in a Dockerfile.

---

## E

### Effect (React)

A side effect scheduled to run after render. `useEffect`.

### Endpoint

A specific URL a web API exposes. `/health`, `/prices/latest`.

### env var

**Environment variable** — a key-value string set in the OS
environment. CryptoStream reads many via `.env`.

### Exactly-once semantics

A delivery guarantee: each message is processed exactly once.
CryptoStream achieves it across Kafka + Bronze via the idempotent
producer and unique constraint.

### Executor (Airflow)

Determines where Airflow tasks run. CryptoStream uses
`LocalExecutor`.

---

## F

### FastAPI

A modern Python web framework. Type-driven, auto-OpenAPI, async
support. See
[09_FASTAPI_FUNDAMENTALS.md](09_FASTAPI_FUNDAMENTALS.md).

### File system

The directory structure on disk. Containers have their own; volumes
are persistent.

### Filter (SQL)

A `WHERE` clause. `WHERE symbol = 'BTCUSD'`.

### `foreachBatch`

A Spark Structured Streaming hook. For each micro-batch, call a
Python function with the data.

### Foreign key

A reference from one row to another's primary key. CryptoStream
doesn't use FKs (Bronze, Silver, Gold are independent).

### Full duplex

Both sides of a connection can send at any time. WebSockets are
full duplex; HTTP is half duplex (request, then response).

---

## G

### Group by (SQL)

Aggregate rows that share a key. `GROUP BY symbol` aggregates per
symbol.

### Gold

The aggregated analytics layer. `gold.candles_1m`,
`gold.candles_1m_ma`. See
[02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md).

---

## H

### Health check

A command run periodically to check if a service is ready.
Compose uses these to gate `depends_on`.

### Hook

A function that adds behaviour. React's `useState` and
`useEffect` are hooks; Airflow's `@task` is a hook.

### HTTP

The protocol browsers and APIs use. Request/response.

### HTTP API / REST

A way for programs to talk over HTTP. CryptoStream's API is REST.

### HTTPS

HTTP over TLS encryption. `https://`, `wss://`.

---

## I

### Idempotent

Safe to do twice — the second attempt is a no-op. `enable.idempotence=true`
on Kafka producers; `INSERT ... ON CONFLICT DO NOTHING` on Bronze.

### Image (Docker)

A packaged environment + program, immutable. Containers are
instances of images.

### Index

A sidecar data structure for fast lookups. `idx_bronze_prices_raw_symbol_event_time`.

### Ingestion

Module 3: the WebSocket → Kafka producer.

### INSERT ... ON CONFLICT

A Postgres clause for upserts. `DO NOTHING` skips duplicates;
`DO UPDATE` updates them.

### Integration test

A test that exercises real components (real Postgres, real Kafka,
real WebSocket server).

---

## J

### Jinja

A templating language. dbt uses it for `{{ ref(...) }}` and
`{{ source(...) }}`. Airflow uses it for templating task
arguments.

### Jitter

Random variation added to retry delays. Prevents thundering herd.

### JSON

**J**ava**S**cript **O**bject **N**otation. A text format for
structured data. Most APIs speak JSON.

### JSX

HTML-like syntax in JavaScript. What React components return.

---

## K

### Kafka

A distributed message broker. See
[03_KAFKA_FUNDAMENTALS.md](03_KAFKA_FUNDAMENTALS.md).

### Kafka topic

A named stream. CryptoStream has `prices` and `prices.dlq`.

### Kafka partition

A sub-stream. Within a partition, order is guaranteed; across
partitions, parallel.

### Kafka offset

A numeric position in a partition.

### KRaft

Kafka's mode without Zookeeper. The modern way.

---

## L

### Latency

Time from event to result. CryptoStream's live-lane latency is
~10–15 s.

### Lifespan (FastAPI)

Code that runs at app startup and shutdown. Used to manage the
DB connection pool.

### Lineage

The dependency graph of dbt models. "X depends on Y."

### LocalExecutor

Airflow executor that runs tasks as subprocesses on the scheduler
host.

---

## M

### MA(20)

**M**oving **A**verage over the last 20 periods. `gold.candles_1m_ma`
holds the 20-period moving average of `gold.candles_1m.close`.

### Mart (dbt)

A business-facing aggregated model. `gold.candles_1m` is a mart.

### Materialisation (dbt)

How a model's result is stored. `view`, `table`, `incremental`.

### Medallion

Bronze / Silver / Gold layers. See
[02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md).

### Merge

Combining data. In `incremental` dbt materialisations, you `merge`
new and existing rows.

### Message

A single record in a Kafka topic. JSON-encoded in CryptoStream.

### Method (HTTP)

The verb: GET, POST, PUT, DELETE. CryptoStream uses GET.

### micro-batch

A small group of stream records processed together. Spark's
streaming unit.

### Migration

A versioned change to a database schema. CryptoStream's migrations
are SQL files in `db/migrations/`.

### Mount (Docker)

Exposing a host directory (or named volume) inside a container.

---

## N

### Named volume

A persistent storage managed by Docker. CryptoStream uses
`pg_data` and `spark_checkpoints`.

### Network

A virtual network connecting containers. CryptoStream uses the
`cryptostream` bridge.

### Numeric(20, 8)

A Postgres type: a decimal with up to 20 total digits, 8 after
the decimal. Used for prices.

---

## O

### Offset

Position in a Kafka partition. Numeric, monotonic.

### OHLCV

**O**pen, **H**igh, **L**ow, **C**lose, **V**olume. Standard
candle columns.

### OLAP

**O**nline **A**nalytical **P**rocessing. Analytics queries.
Gold is OLAP-shaped.

### OLTP

**O**nline **T**ransaction **P**rocessing. Row-by-row writes.
Bronze is OLTP-shaped.

### ON CONFLICT

Postgres upsert clause. See
[02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md#constraints).

### OpenAPI

A standard for describing HTTP APIs. FastAPI generates one
automatically.

### Operator (Airflow)

A pre-built task template: BashOperator, PythonOperator, etc.

---

## P

### P95 / P99

95th / 99th percentile latency. The slowest 5% / 1% of requests.

### Partition

Subdivision of a Kafka topic (for parallelism) or a Postgres
table (for large tables). Different things.

### Path parameter

A variable part of a URL. `/candles/{symbol}` has `symbol` as a
path parameter.

### Pinned version

A specific version of a dependency, like `dbt-postgres==1.8.0`.
Reproducible builds.

### Postgres

An open-source relational database. CryptoStream's database of
record. See
[02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md).

### Polling

Repeatedly asking for updates. The dashboard polls the API every
5 seconds.

### Pool

A reusable set of resources. Connection pool = DB connections;
thread pool = worker threads.

### Port

A network endpoint. Port 5432 = Postgres; 8080 = Airflow UI; 8000
= API; 5173 = dashboard.

### Primary key

A column (or set) that uniquely identifies a row. `id` in
`bronze.prices_raw`.

### Producer

Code that writes to a Kafka topic. The ingestion service is a
producer.

### Promise.all

JavaScript pattern that runs multiple async operations in
parallel and waits for all.

### PythonOperator

Airflow operator that runs a Python function.

---

## Q

### Query parameter

A key-value pair after `?` in a URL. `?symbols=BTCUSD,ETHUSD`.

---

## R

### Raw

The original, unmodified form of data. In Bronze, the `raw` jsonb
column holds the original WebSocket payload.

### React

A JavaScript UI library. CryptoStream's dashboard uses it. See
[10_REACT_FUNDAMENTALS.md](10_REACT_FUNDAMENTALS.md).

### Reactive

Responding to changes automatically. React re-renders when state
changes.

### recharts

React chart library. CryptoStream uses it for the candle chart.

### Reconnect

Re-establishing a connection after it drops. With backoff + jitter.

### Ref (dbt)

`{{ ref('model_name') }}` — references another dbt model.

### Repository

A class that encapsulates data access. `GoldRepository` in
CryptoStream's API.

### Replica

A copy of data. In production Kafka, each partition has multiple
replicas. In production Postgres, you have read replicas.

### Retention

How long data is kept. Kafka has a retention period; Bronze has a
retention DAG.

### Row

One observation / record in a table.

### Run (Airflow)

One execution of a DAG. Multiple runs can be in flight; each has a
unique ID and timestamp.

---

## S

### Schema

A folder for tables in Postgres. Also: the structure of a
document (JSON schema, Pydantic schema).

### Schedule interval

How often a DAG runs. `*/5 * * * *` for transform_dag.

### Secret

Sensitive configuration like API keys. `.env` is gitignored
because it may contain secrets.

### Silver

The typed layer in the medallion. `silver.stg_prices`.

### Source (dbt)

`{{ source('bronze', 'prices_raw') }}` — references an external
table declared in `sources.yml`.

### Spark

Apache Spark. Distributed data-processing engine. CryptoStream
uses Spark Structured Streaming for the Kafka → Bronze bridge.
See [06_SPARK_FUNDAMENTALS.md](06_SPARK_FUNDAMENTALS.md).

### SQL

**S**tructured **Q**uery **L**anguage. The language for talking
to relational databases.

### Staged upsert

The pattern of inserting into a temp table then
`INSERT...SELECT...ON CONFLICT DO NOTHING`. See
[06_SPARK_FUNDAMENTALS.md](06_SPARK_FUNDAMENTALS.md#why-staged-upsert-not-plain-jdbc-append).

### Staging model

A typed projection of a raw source. The first dbt model in a
chain.

### Stream

A continuous flow of data. Spark Structured Streaming treats
streams as unbounded tables.

### Strict mode

A development mode in React that double-invokes effects to surface
bugs.

### Subprocess

A separate process spawned by a parent. Airflow's LocalExecutor
runs tasks as subprocesses.

---

## T

### Table

A named collection of rows in a database.

### Task

One node in an Airflow DAG. One piece of work.

### TaskFlow API

Airflow's `@task` decorator. Lets you write tasks as plain Python
functions.

### Test (dbt)

An assertion about your data that runs on every build.
`not_null`, `unique`, `accepted_values`, custom SQL.

### Test (pytest)

A Python function that asserts behaviour. Unit, integration.

### Timestamp

A point in time. `timestamptz` in Postgres = UTC instant.

### Token (auth)

A credential for API access. `FREECRYPTO_API_KEY` is a token.

### Topic (Kafka)

A named stream. `prices`, `prices.dlq`.

### Transaction

An all-or-nothing unit of work in a database.

### Trigger

In Spark: the schedule for micro-batches. In Airflow: what starts
a DAG run.

---

## U

### Unique constraint

A rule that no two rows can share a key. CryptoStream's Bronze has
`(symbol, exchange, event_time)` unique.

### Upsert

Insert-or-update in one statement. `INSERT ... ON CONFLICT ...`.

### Use case

A specific thing a system is used for. "Dashboard" is a use case
for the Gold tables.

### `useState`

React hook. Declare a state variable.

### `useEffect`

React hook. Run a side effect after render.

---

## V

### Variable (Airflow)

A key-value config item stored by Airflow. `bronze_retention_days`.

### Variable (programming)

A name that holds a value. `let x = 5;`

### Vite

A modern build tool / dev server for JavaScript apps. See
[10_REACT_FUNDAMENTALS.md](10_REACT_FUNDAMENTALS.md#vite--the-build-tool).

### Volume (Docker)

Persistent storage outside a container.

### Volume (trading)

How much of something was traded. A column in `prices_raw`.

### VWAP

**V**olume-**W**eighted **A**verage **P**rice. An aggregation we
don't compute yet.

---

## W

### Watchlist

The set of symbols we track. Default `BTCUSD,ETHUSD,SOLUSD`.

### WebSocket

A full-duplex protocol over TCP. Crypto exchanges push prices
over WebSocket. See
[05_WEBSOCKETS_FUNDAMENTALS.md](05_WEBSOCKETS_FUNDAMENTALS.md).

### Window (Spark)

A time-bounded group of records for aggregation.

### Wrapper

A function or class that delegates to another, possibly adding
behaviour.

### WSS

WebSocket over TLS encryption.

---

## X

### XCom (Airflow)

**Cr**oss-**com**munication. Tasks can pass small messages.

### XSS

**Cr**oss-**S**ite **S**cripting. A security issue not relevant
to CryptoStream (we don't render user content).

---

## Y

### YAML

A human-readable data format. Compose files, dbt configs, and
many other things are YAML.

### Yield

A Python keyword that turns a function into a generator. Context
managers use it for setup/teardown.

---

## Z

### Zookeeper

The old way to run Kafka. CryptoStream uses KRaft instead.

---

## One-liner per tool

| Tool | One-liner |
|------|-----------|
| Postgres | ACID relational database; stores Bronze/Silver/Gold |
| Kafka | Distributed message log; decouples producer from consumer |
| Spark | Distributed batch + stream processor; bridges Kafka → Bronze |
| dbt | SQL models with a dependency graph; rebuilds Silver + Gold |
| Airflow | Workflow orchestrator; runs DAGs on a schedule |
| FastAPI | Typed Python web framework; exposes Gold via HTTP |
| React + Vite | Component-based UI; the dashboard |
| Docker Compose | Multi-container orchestration; runs all 11 services |
| nginx | Production web server; serves the React bundle |
| recharts | React chart library |
| confluent-kafka | Producer/consumer library (librdkafka wrapper) |
| Pydantic | Data validation library; used in FastAPI and ingestion |

---

## Where to go next

- [00_LEARNING_PATH.md](00_LEARNING_PATH.md) — start at the
  beginning.
- [../docs/MODULES.md](../docs/MODULES.md) — per-module reference.
- [../docs/TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md) — when
  something breaks.