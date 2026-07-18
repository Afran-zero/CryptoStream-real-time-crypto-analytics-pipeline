# Module 5 — Transforms (dbt: Silver + Gold)

## Context & Objective
Build the batch transformations with dbt: a Silver layer (deduplicated, typed,
validated) from Bronze, and a Gold layer (OHLC candles per symbol per interval plus
a moving-average indicator). Data-quality tests gate the build so a bad batch fails
loudly instead of corrupting Gold. Gold is the only layer the API reads.

## Prerequisites
- Modules 2 & 4 complete: `bronze.prices_raw` is populated by the live stream.
- Codebase state: `transforms/` empty except `.gitkeep`.

## Technical Specifications
dbt project in `transforms/` targeting Postgres (`dbt-postgres`). Layout:
- `models/staging/stg_prices.sql` (Silver) — from source `bronze.prices_raw`:
  deduplicate on `(symbol, exchange, event_time)` keeping the max `ingested_at`;
  cast types; filter `price > 0`; materialize as a **table** in schema `silver`.
- `models/marts/candles_1m.sql` (Gold) — 1-minute OHLC per `(symbol, exchange)`:
  `date_trunc('minute', event_time)` as `bucket`; `open`=first by event_time,
  `high`=max, `low`=min, `close`=last by event_time, `volume`=sum. Materialize as a
  table in schema `gold`.
- `models/marts/candles_1m_ma.sql` (Gold) — from `candles_1m`, a 20-period moving
  average of `close` per symbol via window
  `avg(close) over (partition by symbol,exchange order by bucket rows between 19
  preceding and current row)`.
- `models/sources.yml` — declares the `bronze.prices_raw` source with a
  `freshness` block (warn/error thresholds on `ingested_at`).
- `models/schema.yml` — tests:
  - `stg_prices`: `unique` + `not_null` on the surrogate key; `not_null` on
    business-key columns; `accepted_range` (price > 0) via `dbt_utils` or a custom
    singular test.
  - `candles_1m`: `not_null` on `bucket/open/high/low/close`; a singular test
    asserting `high >= low` and `high >= open,close` and `low <= open,close`.
- `profiles.yml` — a `cryptostream` profile reading `DATABASE_URL` components
  (host `postgres`, db `cryptostream`), schemas per layer.

## Step-by-Step Implementation Instructions
1. `dbt_project.yml`: name `cryptostream`, model paths, configure
   `staging` → schema `silver`, `marts` → schema `gold` (via `+schema` or a custom
   `generate_schema_name` macro so schemas are literal, not prefixed).
2. Add `packages.yml` with `dbt_utils`; run `dbt deps`.
3. Implement the four models and the two YAML files exactly as specified. Prefer
   ephemeral/CTE clarity; keep SQL Postgres-dialect.
4. Write the singular test `tests/assert_candle_bounds.sql` (returns offending rows;
   empty result = pass).
5. Provide a `make dbt` target that runs dbt inside a dbt-postgres container on the
   `cryptostream` network with `transforms/` mounted and `DATABASE_URL` env, e.g.
   `dbt deps && dbt build`.

## Verification & Testing Criteria
```bash
make dbt                          # runs dbt deps, then dbt build (run + test)
# dbt build must end with all tests PASS and models built.

make psql -c "select count(*) from silver.stg_prices;"          # > 0
make psql -c "select symbol,bucket,open,high,low,close,volume
              from gold.candles_1m order by bucket desc limit 5;"
make psql -c "select symbol,bucket,close,ma_20 from gold.candles_1m_ma
              where ma_20 is not null order by bucket desc limit 5;"

# FAIL-LOUDLY proof: temporarily break a test and confirm dbt build fails non-zero.
# e.g. add a row violating high>=low into a scratch copy, or invert the bound test,
# run `make dbt`, observe FAIL + non-zero exit, then revert.
```
Success = `dbt build` green on real data, Gold candles and the MA indicator
populated, and a deliberately violated test makes the build fail visibly.

## Hand-off State
- `silver.stg_prices` (clean, deduped, typed) and `gold.candles_1m`,
  `gold.candles_1m_ma` exist and are correct.
- A tested, single-command `make dbt` (`dbt build`) transformation exists.
- Data-quality tests are wired and proven to fail the build on violation.
Module 6 will schedule `dbt build` (plus retention and backfill) under Airflow.
Module 7 will read `gold.candles_1m` and `gold.candles_1m_ma` from the API.
