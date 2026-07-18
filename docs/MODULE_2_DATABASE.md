# Module 2 — Database & medallion schema

## Purpose

Define the storage layer for the whole pipeline:

- Three **schemas**: `bronze`, `silver`, `gold`.
- **Bronze landing table** with a business-key unique constraint, a
  positive-price check, and a most-recent-first index.
- A **migration runner** that's safe to re-run and tracks applied files.

The Bronze constraint is what makes Module 4's idempotent upsert
possible; without it, restart-after-crash would risk duplicates.

## Files

```
db/
  run_migrations.py             # migration runner (CLI)
  requirements.txt              # psycopg[binary]
  migrations/
    0001_schemas.sql            # create schemas bronze/silver/gold
    0002_bronze_prices_raw.sql  # landing table + unique constraint + index
infra/
  postgres-init/
    01-create-airflow-db.sql    # one-shot on first boot (creates `airflow` DB)
```

## Schema

```sql
create schema bronze;   -- raw landing (Module 4 writes here)
create schema silver;   -- typed view over Bronze (Module 5 / dbt)
create schema gold;     -- aggregated analytics (Module 5 / dbt; Module 7 reads)
```

`bronze.prices_raw`:

| Column        | Type             | Constraint                | Notes                            |
|---------------|------------------|---------------------------|----------------------------------|
| `id`          | `bigint identity`| primary key               | surrogate                        |
| `symbol`      | `text`           | not null                  | e.g. `BTCUSD`                    |
| `exchange`    | `text`           | not null                  | e.g. `FreeCryptoAPI`             |
| `price`       | `numeric(20,8)`  | not null, `> 0` (CHECK)   | Decimal, no float drift          |
| `volume`      | `numeric(20,8)`  | nullable                  |                                  |
| `event_time`  | `timestamptz`    | not null                  | UTC                              |
| `ingested_at` | `timestamptz`    | not null, default `now()` | server-side                      |
| `source`      | `text`           | not null                  | producer name                    |
| `raw`         | `jsonb`          | nullable                  | original payload                 |

Constraints:
- `unique_business_key unique (symbol, exchange, event_time)`
- `price_positive check (price > 0)`

Index:
- `idx_bronze_prices_raw_symbol_event_time on bronze.prices_raw (symbol, event_time desc)`

## Migration runner

`db/run_migrations.py` is intentionally tiny:

1. Connects to `$DATABASE_URL`.
2. Ensures `public.schema_migrations` exists (creates it on first run).
3. Lists `db/migrations/*.sql` in lexical order.
4. For each file: skips if its name is already in
   `schema_migrations`, otherwise executes it inside its own
   transaction and records the name on success.

Usage:

```bash
make migrate           # local Postgres container
make migrate-host      # host-side, against $DATABASE_URL (Neon-friendly)
```

Both targets are idempotent.

## How to run

```bash
make migrate
```

## How to verify

```bash
# All three schemas exist
make psql -- -c "select schema_name from information_schema.schemata where schema_name in ('bronze','silver','gold');"

# Bronze table is in place with the right constraints
make psql -- -c "\d bronze.prices_raw"
make psql -- -c "select conname from pg_constraint where conrelid = 'bronze.prices_raw'::regclass;"

# Index exists
make psql -- -c "select indexname from pg_indexes where tablename = 'prices_raw';"

# Migration ledger
make psql -- -c "select * from public.schema_migrations order by filename;"
```

Expected: all queries return rows; the constraint list includes
`unique_business_key` and `price_positive`; the index list includes
`idx_bronze_prices_raw_symbol_event_time`.

## Env vars consumed

This module reads only `DATABASE_URL` via the runner. See
[ENV_REFERENCE.md — Module 1](ENV_REFERENCE.md#module-1--infrastructure-compose).

## Failure modes

| Symptom                                          | Likely cause                                  |
|--------------------------------------------------|-----------------------------------------------|
| `relation "bronze.prices_raw" does not exist`    | `make migrate` not run yet                    |
| `permission denied for schema bronze`            | `POSTGRES_USER` doesn't own the schema        |
| `duplicate key value violates unique constraint` | Two ticks with same `(symbol, exchange, event_time)` — exactly what the constraint is for; the upsert's `ON CONFLICT DO NOTHING` handles it |
| `check constraint "price_positive" violated`     | Source sent `price <= 0`; would also be filtered upstream by Pydantic |

## Tests

There is no separate Module 2 test suite. The schema is exercised by:

- `streaming/tests/test_idempotency.py` — proves the unique constraint
  enforces dedup at the DB level.
- `api/tests/test_repository.py` — seeds 5 BTCUSD candles and runs the
  repository methods against the real schema.
- All Module 5 dbt schema tests pass only when Bronze's types match.