# 01 — The big picture

If you finish this page and remember nothing else, remember the
**coffee shop analogy**.

---

## The problem in one sentence

Crypto exchanges publish prices in real time over the internet. We
want to capture every price tick, store it, compute useful
aggregations (like "average BTC price over the last minute"), and
display it on a website.

That's it. Everything in this project exists to serve that sentence.

---

## The coffee shop analogy

Imagine you run a busy coffee shop. Customers order drinks, you make
them, you record what was sold, and at the end of the day you
compute totals.

```
Customer walks in
        │
        ▼
  Order taker writes it on a slip
        │
        ▼
  Slip goes onto a spinning carousel (the queue)
        │
        ▼
  Barista picks slips off the carousel, makes drinks
        │
        ▼
  Drinks get served; the order slip is filed away
        │
        ▼
  End of day: tally up "how many lattes did we sell?"
```

CryptoStream is the same flow, with crypto prices instead of
lattes:

```
Crypto exchange publishes a price tick
        │
        ▼
  Ingestion service writes it to a Kafka topic (the carousel)
        │
        ▼
  Spark reads from Kafka, writes to Bronze (Postgres)
        │
        ▼
  dbt reads Bronze, computes Silver + Gold (the end-of-day tally)
        │
        ▼
  FastAPI reads Gold, serves it to the React dashboard
```

The two systems are identical in shape. The "queue" between the
order taker and the barista is what lets them work at different
speeds without blocking each other.

---

## Why so many pieces?

A beginner might ask: "why not just have one Python script that
reads from the WebSocket and writes to a database and updates a
webpage?"

You can. It would work — for a while. Here's why we don't:

### Problem 1: speed mismatch

The WebSocket sends a price **every few seconds**. The database can
absorb that. But what if you wanted to also save raw trades, order
book snapshots, every social media mention of "BTC"? You'd be
drowning in writes.

**Solution:** Kafka. The producer (ingestion) writes to Kafka as
fast as the source sends. The consumer (Spark) reads from Kafka at
whatever speed the database can handle. The two are decoupled.

### Problem 2: reliability

What if the database is briefly down? Without a queue, you'd lose
the prices that arrived during the outage. With Kafka, the messages
sit safely in the topic until the database is back.

### Problem 3: replayability

What if a bug in your processing code corrupts some data and you
need to recompute from yesterday? Without a log of messages, you'd
have to ask the source again — and they probably won't resend it.
With Kafka, you can re-read every message from any point in time.

### Problem 4: scale

What if you go from "watch BTC and ETH" to "watch all 500 coins on
5 exchanges"? With a single script, your laptop fan starts
whirring. With a pipeline, you can scale each piece independently:
add more Spark workers, add more Kafka partitions, add more API
replicas.

---

## The three lanes

Every real-time data system has three lanes. CryptoStream is no
different:

```
   LIVE LANE         BATCH LANE        SERVING LANE
   (low latency)     (correctness)     (reads only)
   ────────────      ─────────────     ─────────────
   ingestion         dbt (via Airflow) FastAPI
   Kafka             retention_dag     React dashboard
   Spark
   Bronze
```

- **Live lane**: speed matters. We don't want a 10-second delay
  between the price changing and Bronze reflecting it. Loose
  correctness is OK (we can fix it later in the batch lane).
- **Batch lane**: correctness matters. We re-process Bronze every 5
  minutes to make sure Silver + Gold are accurate. Slower is fine.
- **Serving lane**: read-only. The dashboard never writes back; it
  just asks "what's the latest?" and renders.

Most data systems have this shape. The names change (Lambda
architecture, Kappa, medallion), but the lanes are the same.

---

## The medallion pattern (Bronze / Silver / Gold)

A common way to organise data as it gets cleaned up:

- **Bronze** = raw, untouched, exactly what arrived from the source.
  You keep it forever (within your retention window) so you can
  always re-derive everything else.
- **Silver** = typed and deduplicated. Same data as Bronze, but
  with proper column types and the obvious bad rows removed.
- **Gold** = aggregated for a specific use case. 1-minute candles,
  moving averages, whatever your consumer actually wants.

Why three layers? Because Bronze is your **insurance policy**. If
your Silver logic has a bug, you fix it and re-run. If your Gold
table is wrong, you fix the SQL and re-run. You never have to
worry that you "lost" the original data — Bronze still has it.

---

## What each module owns

| Module | Owns | Plain-language description |
|--------|------|----------------------------|
| 1 | Infrastructure | The 11 Docker containers + their wiring |
| 2 | Database | The Postgres schemas + the Bronze table |
| 3 | Ingestion | The WebSocket → Kafka producer |
| 4 | Streaming | The Spark job that moves Kafka → Bronze |
| 5 | Transforms | The dbt project that computes Silver + Gold |
| 6 | Orchestration | The Airflow DAGs that schedule + retain |
| 7 | API + Dashboard | The FastAPI service + React UI |

Each one has its own page in `docs/MODULE_*.md` (reference) and
several have a fundamentals page in `teach/` (explanation).

---

## The data contract

Every price tick in the system — whether it's in Kafka, Bronze,
Silver, or Gold — has the same shape:

```
symbol:      "BTCUSD"           (which coin)
exchange:    "FreeCryptoAPI"    (which venue)
price:       67432.51           (the price itself)
volume:      0.5                (how much was traded, optional)
event_time:  2026-07-19 14:30:00 UTC  (when the price was seen)
ingested_at: 2026-07-19 14:30:00.123 UTC  (when our system got it)
source:      "FreeCryptoAPI"    (same as exchange; convenience)
raw:         { ... }            (the original JSON, kept for debug)
```

The triple `(symbol, exchange, event_time)` is the **business key**
— the unique identifier of a price observation. No two rows in the
whole system share those three values. This is what lets us have
idempotent writes: "if I see this triple again, ignore it."

---

## One tick's journey

A single BTCUSD tick at 14:30:00 travels through the system like
this:

```
1. Exchange publishes a JSON message on its WebSocket.
2. Our ingestion service receives it, parses it, validates it.
3. The validated message is published to Kafka topic `prices`.
4. Spark picks up the message in its next micro-batch (≤10s later).
5. Spark writes the row to Bronze, via a staging table.
6. Every 5 minutes, Airflow's transform_dag runs dbt.
7. dbt rebuilds Silver (cleaned typed rows) and Gold (1-min candles
   + moving average).
8. A user opens the dashboard; React asks FastAPI for the latest
   BTC candles.
9. FastAPI queries Gold, returns the data, recharts draws the chart.
```

Total time from "exchange published it" to "user sees it on the
chart" is about 10–30 seconds.

---

## The most important mental model

When you open the codebase and feel overwhelmed by 11 services,
remember:

> **Each service does exactly one thing.** The complexity isn't in
> any single piece — it's in how they cooperate.

| Service | One job |
|---------|---------|
| ingestion | turn WebSocket frames into Kafka messages |
| kafka | hold messages until someone reads them |
| spark | turn Kafka messages into Bronze rows |
| postgres | store rows and answer queries |
| dbt | rebuild Silver + Gold from Bronze |
| airflow | run dbt on a schedule, run retention daily |
| fastapi | answer HTTP questions about Gold |
| react | show the user the answer |

If you keep this table in your head, every file in the repo will
make sense.

---

## What's next?

- Continue to [02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md)
  to understand the storage layer that every other piece depends on.
- Or jump to [11_HOW_DATA_FLOWS.md](11_HOW_DATA_FLOWS.md) for a
  detailed trace of a single tick.