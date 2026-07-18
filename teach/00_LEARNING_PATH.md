# Learning path

This folder teaches you **what CryptoStream is, why it's built the way
it is, and how each piece works** — in plain language with diagrams and
analogies.

The `docs/` folder is the **reference manual** (every var, every
command, every endpoint). This `teach/` folder is the **textbook**.

---

## Who this is for

You're new to the project — maybe new to data engineering entirely.
You want to understand:

- What problem this system solves.
- Why it's split into 11 separate Docker containers instead of one
  big app.
- What each container actually does.
- Why we chose Kafka over a simpler queue, or Spark over plain Python.
- How to read the code and know what to expect.

You don't need to know Docker, Kafka, Spark, dbt, or Airflow before
starting. We'll explain each in plain language.

---

## How to read this folder

Read top to bottom the first time, then jump back to specific files
when you need a refresher. Each file is self-contained.

```
teach/
├── 00_LEARNING_PATH.md        ← you are here
├── 01_BIG_PICTURE.md          ← start here: the whole story in 10 min
├── 02_DATABASE_FUNDAMENTALS.md
├── 03_KAFKA_FUNDAMENTALS.md
├── 04_DOCKER_FUNDAMENTALS.md
├── 05_WEBSOCKETS_FUNDAMENTALS.md
├── 06_SPARK_FUNDAMENTALS.md
├── 07_DBT_FUNDAMENTALS.md
├── 08_AIRFLOW_FUNDAMENTALS.md
├── 09_FASTAPI_FUNDAMENTALS.md
├── 10_REACT_FUNDAMENTALS.md
├── 11_HOW_DATA_FLOWS.md       ← trace one tick end-to-end
├── 12_DESIGN_DECISIONS.md     ← the "why" behind the "what"
├── 13_HANDS_ON_TOUR.md        ← guided exercise: poke the live system
└── 14_GLOSSARY.md             ← every term, plain-language definitions
```

---

## Two reading modes

### Mode A — Just want the story (≈ 30 min)

1. [01_BIG_PICTURE.md](01_BIG_PICTURE.md) — 10 minutes. The whole
   pipeline explained as a coffee-shop analogy.
2. [11_HOW_DATA_FLOWS.md](11_HOW_DATA_FLOWS.md) — 10 minutes. Trace
   one BTCUSD tick from the WebSocket to the dashboard.
3. [14_GLOSSARY.md](14_GLOSSARY.md) — bookmark for later.

### Mode B — Want to actually understand each tool (≈ 2 hours)

Read in this order:

1. [01_BIG_PICTURE.md](01_BIG_PICTURE.md) — the 10,000 ft view.
2. [02_DATABASE_FUNDAMENTALS.md](02_DATABASE_FUNDAMENTALS.md) — start
   here because everything else stores or reads from Postgres.
3. [03_KAFKA_FUNDAMENTALS.md](03_KAFKA_FUNDAMENTALS.md) — the messaging
   backbone.
4. [04_DOCKER_FUNDAMENTALS.md](04_DOCKER_FUNDAMENTALS.md) — how the
   11 services run side by side.
5. [05_WEBSOCKETS_FUNDAMENTALS.md](05_WEBSOCKETS_FUNDAMENTALS.md) —
   where the data comes from.
6. [06_SPARK_FUNDAMENTALS.md](06_SPARK_FUNDAMENTALS.md) — the
   streaming engine.
7. [07_DBT_FUNDAMENTALS.md](07_DBT_FUNDAMENTALS.md) — the transform
   layer.
8. [08_AIRFLOW_FUNDAMENTALS.md](08_AIRFLOW_FUNDAMENTALS.md) — the
   scheduler.
9. [09_FASTAPI_FUNDAMENTALS.md](09_FASTAPI_FUNDAMENTALS.md) — the
   serving API.
10. [10_REACT_FUNDAMENTALS.md](10_REACT_FUNDAMENTALS.md) — the dashboard.
11. [11_HOW_DATA_FLOWS.md](11_HOW_DATA_FLOWS.md) — once you know the
    pieces, see them work together.
12. [12_DESIGN_DECISIONS.md](12_DESIGN_DECISIONS.md) — the trade-offs.
13. [13_HANDS_ON_TOUR.md](13_HANDS_ON_TOUR.md) — do this with the stack
    actually running. The fastest way to internalise it.
14. [14_GLOSSARY.md](14_GLOSSARY.md) — final reference.

---

## Learning outcomes

After working through this folder you will be able to:

- Explain **what each of the 11 services does** without looking at the
  code.
- Trace a single price tick **from the source WebSocket to a chart on
  the dashboard** and name every component it passes through.
- Read the `docker-compose.yml` and predict what `docker compose up`
  will start, in what order, and what will be unhealthy if anything
  is missing.
- Understand **why** we use Postgres + Kafka + Spark + dbt + Airflow
  + FastAPI instead of, say, a single Python script that polls the
  WebSocket and writes to a CSV.
- Make a small change (e.g. add a new symbol to the watchlist, add a
  new dbt model, change the dashboard's poll frequency) and know which
  files to touch.
- Diagnose common failures using [../docs/TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md).

---

## How to use this folder while reading the code

When you open a file under `ingestion/`, `streaming/`, `transforms/`,
etc., you'll often see imports and concepts that are explained in
this folder. The recommended flow:

1. Hit a concept you don't recognise → check the glossary
   ([14_GLOSSARY.md](14_GLOSSARY.md)).
2. Want the deeper explanation → read the relevant
   `NN_*_FUNDAMENTALS.md` file.
3. Need to know which file does what → see
   [../docs/MODULES.md](../docs/MODULES.md).

---

## What this folder is NOT

- It's not a Docker tutorial. We assume you'll read
  [04_DOCKER_FUNDAMENTALS.md](04_DOCKER_FUNDAMENTALS.md) for the
  minimum you need; deeper Docker knowledge is out of scope.
- It's not a production deployment guide. For that, see
  [../docs/OPERATIONS.md](../docs/OPERATIONS.md).
- It's not a replacement for the actual tool documentation. Kafka,
  Spark, dbt, and Airflow each have excellent official docs — this
  folder only teaches the pieces we actually use.

---

## Next step

→ [01_BIG_PICTURE.md](01_BIG_PICTURE.md)