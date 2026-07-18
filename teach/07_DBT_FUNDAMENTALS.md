# 07 — dbt fundamentals

CryptoStream uses **dbt** to turn Bronze into Silver and Gold.
This page explains what dbt is, what "models" are, and how
CryptoStream's three models fit together.

---

## What is dbt?

**dbt** (data build tool) is a command-line tool that turns SQL
files into tables in your database.

That's it. The whole pitch is:

> "Write SQL. dbt handles the dependencies, the materialisation,
> the testing, the documentation, and the lineage graph."

You put `.sql` files in a `models/` directory. Each file is a
**model**: a SQL query that produces a table. dbt runs them in the
right order, with the right materialisation, and validates the
results.

```
models/
├── staging/
│   └── stg_prices.sql       ← produces silver.stg_prices
└── marts/
    ├── candles_1m.sql       ← produces gold.candles_1m
    └── candles_1m_ma.sql    ← produces gold.candles_1m_ma
```

Run `dbt build` and:

1. dbt parses all `.sql` files.
2. Builds a dependency graph.
3. Runs each model in order, materialising the result.
4. Runs tests on each result.
5. Reports what passed and what failed.

---

## Why dbt instead of plain SQL scripts?

If you only have three models, plain `psql -f` works. But you'd
miss out on:

- **Dependency tracking.** `candles_1m.sql` references
  `silver.stg_prices`; dbt knows to run the staging model first.
- **Materialisation strategies.** dbt knows whether to `CREATE
  TABLE`, `CREATE VIEW`, `CREATE TABLE ... AS`, or do incremental
  updates — based on what you ask for.
- **Tests.** `not_null`, `unique`, `accepted_values`, custom
  expressions — all run on every build.
- **Documentation.** `dbt docs` generates a website with your
  models, columns, and lineage graph.
- **Refactorability.** Rename a column once; dbt shows you every
  downstream model that uses it.

For three models, dbt is overkill. For thirty, it's essential.

---

## Models in CryptoStream

### `silver.stg_prices` — typed view of Bronze

```sql
-- models/staging/stg_prices.sql
select
    symbol,
    exchange,
    price,
    volume,
    event_time,
    ingested_at,
    source,
    raw
from {{ source('bronze', 'prices_raw') }}
```

What's happening:

- `{{ source('bronze', 'prices_raw') }}` is dbt's Jinja syntax for
  "the table declared in `sources.yml` as `bronze.prices_raw`". This
  indirection lets you swap the source later (e.g. point at a
  snapshot) without editing every model.
- The query selects every column from Bronze with no
  transformation — this is just a "stage" that makes the type
  contract explicit.

A staging model like this is conventional in dbt projects. It's
the typed hand-off between raw and business logic.

### `gold.candles_1m` — 1-minute OHLCV candles

```sql
-- models/marts/candles_1m.sql
select
    date_trunc('minute', event_time) as bucket,
    symbol,
    exchange,
    first(price order by event_time) as open,
    max(price) as high,
    min(price) as low,
    last(price order by event_time) as close,
    sum(volume) as volume
from {{ ref('stg_prices') }}
group by 1, 2, 3
```

What's happening:

- `date_trunc('minute', event_time)` groups all ticks in the same
  minute together.
- For each `(minute, symbol, exchange)` group:
  - `open` = first price in that minute (by time)
  - `high` = max price
  - `low` = min price
  - `close` = last price in that minute
  - `volume` = sum of volumes
- `{{ ref('stg_prices') }}` is dbt's syntax for "the model named
  `stg_prices`". This creates a dependency edge in the graph.

This is a standard OHLCV (Open-High-Low-Close-Volume) candle —
the same shape you'd see on any trading chart.

### `gold.candles_1m_ma` — 20-period moving average

```sql
-- models/marts/candles_1m_ma.sql
select
    bucket,
    symbol,
    exchange,
    close,
    avg(close) over (
        partition by symbol, exchange
        order by bucket
        rows between 19 preceding and current row
    ) as ma_20
from {{ ref('candles_1m') }}
```

What's happening:

- For each candle, compute the average of the last 20 close prices
  (including the current one).
- Partitioned by `(symbol, exchange)` so the average is per-symbol.
- Ordered by `bucket` (the candle timestamp), using a sliding
  window of 20 rows.

A **moving average** smooths out short-term noise and shows the
trend. Traders love them.

---

## Materialisations

A model's **materialisation** is how dbt persists its result.

| Materialisation | What it does | When to use |
|-----------------|--------------|-------------|
| `view` | `CREATE VIEW` — query re-runs every time | When underlying data changes constantly |
| `table` | `CREATE TABLE AS` — fully rebuild each run | Default; simple, correct |
| `incremental` | `INSERT ... ON CONFLICT` only new rows | Large fact tables; expensive full rebuilds |
| `ephemeral` | `WITH ...` — inlined into dependent models | Tiny lookup tables you don't want as actual tables |

CryptoStream uses `table` for everything:

```yaml
# dbt_project.yml
models:
  cryptostream:
    staging:
      +materialized: table
    marts:
      +materialized: table
```

Why `table` (rebuild every time)?

- For the demo's data volume, rebuilds are fast (< 1 second).
- We always get correct results from scratch — no risk of
  incremental bugs.
- The query planner can fully optimise each table on every run.

For prod-scale Bronze (billions of rows), you'd switch to
`incremental`. For our scale, `table` is right.

---

## Sources, refs, and the lineage graph

dbt distinguishes between **sources** (raw tables you didn't
create) and **refs** (other models in your project).

- `{{ source('bronze', 'prices_raw') }}` — points at
  `bronze.prices_raw`, declared in `sources.yml`. dbt knows it's
  external; it won't try to rebuild it.
- `{{ ref('stg_prices') }}` — points at another model in the same
  project. dbt adds a dependency edge and runs them in order.

The lineage graph for CryptoStream:

```
bronze.prices_raw         (source, written by Spark)
        │
        ▼
silver.stg_prices         (staging model)
        │
        ▼
gold.candles_1m           (mart)
        │
        ▼
gold.candles_1m_ma        (mart)
```

`dbt docs generate` would render this as a clickable diagram.

---

## Tests

Tests are assertions about your data that run after every build.
CryptoStream defines tests in `models/schema.yml`:

```yaml
models:
  - name: stg_prices
    columns:
      - name: symbol
        tests:
          - not_null
      - name: symbol
        tests:
          - accepted_values:
              values: ['BTCUSD', 'ETHUSD', 'SOLUSD', ...]
      - name: price
        tests:
          - not_null
```

dbt ships with four built-in tests:

- `not_null` — column has no NULLs.
- `unique` — column values are all distinct.
- `accepted_values` — column values are from a list.
- `relationships` — column values are FKs to another table.

You can also write custom tests in `tests/`. CryptoStream has one:

```sql
-- tests/assert_candle_bounds.sql
-- Returns rows where high < open or high < close (which is impossible)
select * from {{ ref('candles_1m') }}
where high < open or high < close
```

If this query returns any rows, the test fails — meaning we have
corrupted candle data.

---

## `generate_schema_name` — the literal-schema macro

By default, dbt prefixes the schema name with the target
environment. For target `dev` and schema `gold`, dbt would
materialise to **`dev_gold`**.

CryptoStream doesn't want this — the API queries `gold.candles_1m`
directly, so the schema must be exactly `gold`.

We override the macro:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

With this, `+schema: gold` produces a table in `gold.*`, not
`dev_gold.*`.

---

## Running dbt

```bash
# Inside the dbt container
make dbt
# Equivalent to:
#   docker compose run --rm dbt dbt deps
#   docker compose run --rm dbt dbt build
```

`dbt deps` fetches any packages (e.g. dbt-utils) listed in
`packages.yml`. `dbt build` runs models + tests in dependency
order.

Useful flags:

| Flag | What it does |
|------|--------------|
| `--select stg_prices` | Only build/test the named model |
| `--select +candles_1m` | Build the model and everything upstream |
| `--select candles_1m+` | Build the model and everything downstream |
| `--full-refresh` | Drop and rebuild (for incremental) |
| `--no-version-check` | Skip dbt's "you should upgrade" nag |

---

## Try it yourself

```bash
# Run dbt
make dbt

# Inspect the generated SQL (handy when debugging)
docker compose run --rm dbt bash -lc 'dbt compile --no-version-check'
docker compose run --rm dbt bash -lc 'cat target/compiled/cryptostream/models/marts/candles_1m.sql'

# Look at the docs (requires `make dbt-docs` first, or after build:)
docker compose run --rm dbt bash -lc 'dbt docs generate --no-version-check'

# Run only tests
docker compose run --rm dbt bash -lc 'dbt test --no-version-check'
```

The `dbt compile` output is the **rendered SQL** — exactly what
gets sent to Postgres, with Jinja and refs expanded. Reading this
is the fastest way to understand what a model actually does.

---

## Vocabulary

| Term | Meaning |
|------|---------|
| dbt | SQL transformation tool with dependency graph + tests |
| Model | A SQL file that produces a table |
| Materialisation | How a model's result is stored (view, table, incremental) |
| Source | An external table dbt reads from (declared in sources.yml) |
| Ref | A reference to another dbt model |
| Test | An assertion about data; fails the build if violated |
| Schema | A folder for tables (in dbt and in Postgres) |
| Staging model | A typed projection of a raw source |
| Mart | A business-facing aggregated table |
| Lineage | The graph of "model A depends on model B" |

---

## What's next?

- [08_AIRFLOW_FUNDAMENTALS.md](08_AIRFLOW_FUNDAMENTALS.md) — how
  dbt gets invoked on a schedule.
- [11_HOW_DATA_FLOWS.md](11_HOW_DATA_FLOWS.md) — see Bronze → dbt
  → Silver → Gold traced end to end.