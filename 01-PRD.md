# CryptoStream — Product Requirements Document

**Deliverable:** 1 of 7 (PRD)
**Status:** Draft v0.1
**Owner:** *(you)*
**Last updated:** 2026-06-09

> Reader's note: this document is deliberately short. It exists to make every
> later design decision traceable to a stated goal. If a component in the
> architecture can't be tied back to a requirement here, it shouldn't be built.

---

## 1. Vision

A production-shaped data platform that ingests live cryptocurrency market data
from a streaming source, lands it through a medallion (Bronze/Silver/Gold) model,
and serves analytics to a web dashboard through an API.

The goal is not to trade crypto. The goal is to demonstrate — end to end, with
defensible engineering choices — that the author can design and operate a real
streaming-plus-batch data system. The primary audience for the finished artifact
is a hiring manager or senior data engineer evaluating it in an interview.

---

## 2. Problem statement

Crypto prices update continuously across many exchanges. Raw exchange feeds are
noisy, arrive out of order, occasionally malformed, and differ in shape between
providers. Consumers (a dashboard, an analyst) want clean, typed, deduplicated
data plus derived analytics (OHLC candles, moving averages) — not a raw firehose.

The system bridges that gap: it absorbs the live feed reliably, transforms it in
governed stages, and exposes only the curated result.

---

## 3. Goals

- **G1.** Ingest a continuous crypto price stream without data loss under normal
  operation, surviving source disconnects and rate limiting.
- **G2.** Land raw data immutably (Bronze), then produce cleaned (Silver) and
  analytics-ready (Gold) datasets through tested transformations.
- **G3.** Serve Gold data to a dashboard through a documented HTTP API with
  sub-second typical response times.
- **G4.** Run entirely reproducibly from a single command (Docker Compose) on a
  laptop, with a clear, un-hand-wavy path to AWS managed equivalents.
- **G5.** Make every technology choice individually justifiable in an interview.

---

## 4. Non-goals

Stating these is a deliberate signal of scope discipline.

- **N1.** Not a trading system. No order placement, no financial advice, no
  low-latency execution guarantees.
- **N2.** Not multi-tenant. Single logical user; auth exists to demonstrate the
  pattern, not to serve a customer base.
- **N3.** Not exhaustive coin coverage. A curated watchlist (e.g. 10–20 symbols)
  is enough to exercise every component.
- **N4.** Not real money, real SLAs, or 24/7 on-call. "Production-shaped," not
  "in production."

---

## 5. Users

| Persona | Who | What they need |
|---|---|---|
| Evaluator | Hiring manager / senior DE | To see correct architecture, defensible tradeoffs, clean repo, working demo |
| Analyst (hypothetical) | End user of the dashboard | Current prices, OHLC candles, a moving average, per-symbol history |
| Operator | The author | To run, monitor, backfill, and debug the pipeline locally |

The Evaluator is the real customer. Features that don't help the Evaluator
understand the system are candidates for cutting.

---

## 6. Functional requirements

- **F1. Ingestion.** Connect to the FreeCryptoAPI WebSocket feed for a configured
  symbol watchlist. Reconnect automatically on drop with exponential backoff and
  jitter. Never crash the process on a single bad message.
- **F2. Buffering.** Publish each normalized tick to a Kafka topic. Route
  un-parseable or schema-violating messages to a dead-letter topic rather than
  discarding them.
- **F3. Stream processing.** Consume the Kafka topic with Spark Structured
  Streaming and write ticks to the Bronze layer with idempotent, at-least-once
  semantics keyed on `(symbol, exchange, event_time)`.
- **F4. Batch transformation.** On a schedule, run dbt models to produce Silver
  (deduplicated, typed, validated) and Gold (OHLC candles at fixed intervals plus
  at least one indicator, e.g. an N-period moving average).
- **F5. Data quality.** Run automated tests on each batch run (not-null, unique
  key, accepted range for prices, freshness). A failing test fails the run
  visibly rather than silently corrupting Gold.
- **F6. Orchestration.** Airflow schedules and monitors the batch lane, supports
  manual backfill of a date range, and surfaces run status.
- **F7. Serving API.** FastAPI exposes: current price per symbol, OHLC candles for
  a symbol/interval, and a health/status endpoint. Responses validated by Pydantic
  models. OpenAPI docs auto-generated.
- **F8. Dashboard.** A React app shows a live-ish price panel, a candlestick or
  line chart per symbol, and a pipeline health indicator.
- **F9. Retention.** Bronze has a defined retention window; Gold is kept longer.
  Retention is enforced by a scheduled job, not manual cleanup.

---

## 7. Non-functional requirements

- **NFR1. Reliability.** Source disconnects, malformed messages, and transient
  DB errors are all handled without data loss or process death. Retries use
  backoff; poison messages go to the DLQ.
- **NFR2. Idempotency.** Re-running ingestion or a batch job over the same window
  does not create duplicates. Upserts key on the natural business key.
- **NFR3. Observability.** Structured logs across services; basic metrics
  (messages ingested, DLQ count, batch run duration, freshness lag). Enough to
  answer "is it healthy and how far behind is it?"
- **NFR4. Reproducibility.** `docker compose up` brings the whole system up.
  Configuration via environment variables; no secrets in the repo.
- **NFR5. Portability.** Each local component maps to a named AWS managed service
  (see §9) so the migration story is concrete, not aspirational.
- **NFR6. Performance (portfolio-scale).** Typical API read < 500 ms. Batch cycle
  completes well within its schedule interval. These are demo targets, not SLAs.
- **NFR7. Documentation.** Each deliverable is a short markdown doc in the repo;
  the README ties them together and gives a 5-minute quickstart.

---

## 8. Data flow (summary)

Two lanes sharing the medallion store.

**Streaming lane (real-time):**
FreeCryptoAPI WebSocket → Ingestion service (Python) → Kafka (`prices` topic,
`prices.dlq` dead-letter) → Spark Structured Streaming → Bronze.

**Batch lane (scheduled):**
Airflow triggers dbt → Silver → Gold, with data-quality tests gating each run.
Airflow also runs retention and backfills. It does **not** sit inside the stream.

**Serving:**
Gold → FastAPI → React dashboard.

The one design rule that keeps this coherent: **streaming tools stay in the
streaming lane, orchestration stays in the batch lane.** Airflow orchestrates
batch transforms; it never brokers or processes the live feed. (Full topology,
sequence diagrams, and failure handling live in Deliverable 2.)

---

## 9. Technology choices and justification

Every row answers the interview question "why is this here, and why not something
simpler?"

| Component | Choice | Why it earns its place | Honest tradeoff |
|---|---|---|---|
| Source | FreeCryptoAPI (WebSocket) | Continuous feed; free tier; multi-exchange normalized data | Exact rate limits unpublished — design is limit-agnostic |
| Ingestion | Python service | Full control over backoff, reconnect, normalization, DLQ routing | Custom code to maintain vs a managed connector |
| Broker | Apache Kafka | Buffers bursts, decouples ingest from processing, gives replay + DLQ; justified *by* the websocket stream | Operational weight; overkill if source were slow REST polling |
| Stream processor | Spark Structured Streaming | Windowed writes, exactly-once sink semantics, scales to EMR unchanged | Heavier than portfolio volume strictly needs — owned deliberately |
| Storage | Neon PostgreSQL | Serverless Postgres, generous free tier, real SQL for the medallion model | Not a purpose-built warehouse; fine at this scale |
| Transforms | dbt | Versioned SQL, built-in tests, lineage + docs | Adds a tool to learn; pays off immediately in testability |
| Orchestration | Apache Airflow | Scheduling, backfills, retries, run visibility for the batch lane | Setup cost; must be kept out of the stream path |
| API | FastAPI | Async, Pydantic validation, auto OpenAPI | — |
| Frontend | React | Standard, componentized dashboard with charting | — |
| Packaging | Docker Compose | One-command reproducible local stack | — |

**Cloud mapping (migration story):**
Kafka → Amazon MSK · Spark → EMR · Airflow → MWAA · Postgres → RDS ·
containers → ECS/Fargate. (Azure equivalents noted in Deliverable 7.)

---

## 10. Data model (overview)

Medallion, three governed stages. Full schema, keys, indexes, partitions, and
materialized views are Deliverable 3.

- **Bronze** — append-only raw ticks as received, plus ingestion metadata
  (source, ingested_at). Immutable; the system of record for replay.
- **Silver** — one clean, typed, deduplicated row per business event. Bad rows
  filtered out here, not upstream.
- **Gold** — analytics-ready: OHLC candles per symbol per interval, plus at least
  one derived indicator. This is the only layer the API reads.

Natural business key throughout: `(symbol, exchange, event_time)`.

---

## 11. Success metrics

The project succeeds if:

- **M1.** `docker compose up` yields a working end-to-end demo on a clean machine.
- **M2.** Live data visibly flows to the dashboard.
- **M3.** Killing the source mid-run and restarting it produces zero duplicates
  and zero lost data (demonstrates NFR1 + NFR2).
- **M4.** A deliberately malformed message lands in the DLQ instead of breaking
  ingestion.
- **M5.** A failing data-quality check fails a batch run visibly.
- **M6.** The author can whiteboard the architecture and defend every tool choice
  and tradeoff unprompted.

M6 is the one that gets the job.

---

## 12. Scope and phasing (build order)

Sequenced so no more than one unfamiliar tool is introduced at a time. Each phase
is independently demoable.

1. **Phase 0 — Ingestion → Postgres (plain Python).** Prove the API, land Bronze.
   No Kafka yet. Demoable: raw ticks in a table.
2. **Phase 1 — Insert Kafka.** Ingestion publishes to Kafka; a simple Python
   consumer writes Bronze. Add the DLQ. Demoable: replay + poison-message handling.
3. **Phase 2 — Swap consumer for Spark Structured Streaming.** Same Bronze output,
   now via Spark. Demoable: windowed streaming writes.
4. **Phase 3 — Batch lane: Airflow + dbt.** Bronze → Silver → Gold with tests and
   retention. Demoable: orchestrated, tested transforms + backfill.
5. **Phase 4 — Serving: FastAPI + React.** Gold → API → dashboard. Demoable:
   the full picture.
6. **Phase 5 — Polish: observability, CI/CD, README, migration doc.**

---

## 13. Open questions

- **OQ1.** FreeCryptoAPI free-tier exact rate limits and message shape — resolve
  once an API key is in hand. Ingestion is built limit-agnostic until then.
- **OQ2.** Deployment target for the demo: laptop-only via Compose, or a small
  cloud VM? Affects sizing, not architecture.
- **OQ3.** Candle intervals and which indicator(s) to compute in Gold — confirm
  before Deliverable 3.

---

## 14. Proposed repository structure

```
cryptostream/
├── README.md
├── docker-compose.yml
├── docs/
│   ├── 01-PRD.md
│   ├── 02-system-design.md
│   ├── 03-database-design.md
│   ├── 04-backend-design.md
│   ├── 05-frontend-design.md
│   ├── 06-pipeline-design.md
│   └── 07-devops-and-migration.md
├── ingestion/          # Python websocket → Kafka producer
├── streaming/          # Spark Structured Streaming consumer
├── transforms/         # dbt project (bronze → silver → gold)
├── orchestration/      # Airflow DAGs
├── api/                # FastAPI service
├── dashboard/          # React app
└── infra/              # env config, migration notes
```

---

*Next: Deliverable 2 — System Design Document (production topology, Kafka
topology, Airflow DAGs, sequence diagrams, failure handling).*
