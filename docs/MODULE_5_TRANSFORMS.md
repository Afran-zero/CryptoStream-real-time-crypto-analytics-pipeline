# Module 5 — Transforms (dbt Silver + Gold)

## Purpose

Build the typed Silver view and the aggregated Gold tables from
Bronze. dbt gives us a dependency graph, schema tests, and a single
`dbt build` CLI that runs everything in order.

## Files

```
transforms/
  dbt_project.yml                          # project: cryptostream
  profiles.yml                             # env-driven connection
  packages.yml                             # dbt-utils dep
  macros/
    generate_schema_name.sql               # override dbt's <target>_<schema> prefix
  models/
    sources.yml                            # bronze source definition
    schema.yml                             # table docs + tests
    staging/
      stg_prices.sql                       # → silver.stg_prices
    marts/
      candles_1m.sql                       # → gold.candles_1m
      candles_1m_ma.sql                    # → gold.candles_1m_ma
  tests/
    assert_candle_bounds.sql               # high ≥ open/close, low ≤ open/close
```

The `dbt` service in `docker-compose.yml` mounts this folder at `/dbt`
and sets `DBT_PROFILES_DIR=/dbt`.

## Models

| Schema / table         | Materialisation | Source                  | Notes                          |
|------------------------|-----------------|-------------------------|--------------------------------|
| `silver.stg_prices`    | `table`         | `bronze.prices_raw`     | Typed projection; idempotent   |
| `gold.candles_1m`      | `table`         | `silver.stg_prices`     | 1-min OHLCV per (symbol, exchange) |
| `gold.candles_1m_ma`   | `table`         | `gold.candles_1m`       | 20-period MA on close          |

Tests run on every `dbt build`:
- `not_null` on every non-nullable column.
- `unique` on the business-key composite.
- `accepted_values` on `exchange` (`FreeCryptoAPI`).
- `dbt_utils.expression_is_true` for candle bounds (high ≥ open/close,
  low ≤ open/close).

## Literal-schema macro

`transforms/macros/generate_schema_name.sql` overrides dbt's default
`<target>_<custom_schema>` rule so models with `+schema: silver` land
in **`silver`** (not `dev_silver`). The API and dashboard query
`gold.candles_1m` directly; a `dev_` prefix would break them.

## How to run

Inside Compose:

```bash
make dbt               # dbt deps + dbt build inside the dbt container
make dbt-deps          # only fetch dbt-utils
```

On the host (useful for Neon / external Postgres):

```bash
make dbt-host
```

Or via Airflow (every 5 minutes):

- `transform_dag` runs `dbt_deps` then `dbt_build`.
- Trigger manually: `make airflow-trigger DAG=transform_dag`.

## How to verify

```bash
# All three models built
make psql -- -c "select * from silver.stg_prices order by event_time desc limit 5;"
make psql -- -c "select bucket, symbol, open, high, low, close, volume from gold.candles_1m order by bucket desc limit 5;"
make psql -- -c "select bucket, symbol, ma_20 from gold.candles_1m_ma order by bucket desc limit 5;"

# dbt tests pass
docker compose run --rm dbt dbt test --no-version-check
```

A `dbt build` that ends with all green model rows and all green test
rows is the canonical "Module 5 done" check.

## Env vars consumed

See [ENV_REFERENCE.md — Module 5](ENV_REFERENCE.md#module-5--transforms-dbt).

Required: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB`. The dbt service container sets
these from `${POSTGRES_*}` in `.env`; on the host you set them in
your shell before running `make dbt-host`.

## Failure modes

| Symptom                                          | Likely cause                                          |
|--------------------------------------------------|-------------------------------------------------------|
| `Compilation Error in model stg_prices`          | Bronze schema changed without updating `models/sources.yml` |
| `Database Error in model candles_1m: relation "silver.stg_prices" does not exist` | Module 2 migrations not applied            |
| `dbt test FAILED on assert_candle_bounds`        | A bad tick made it into Bronze — investigate upstream |
| `Could not find profile named 'cryptostream'`    | `DBT_PROFILES_DIR` not pointing at `transforms/`      |

## Tests

There is no separate Module 5 test suite outside of `dbt test`
(defined in `models/schema.yml` and `tests/assert_candle_bounds.sql`).
Running `make dbt` runs the model build **and** all tests.