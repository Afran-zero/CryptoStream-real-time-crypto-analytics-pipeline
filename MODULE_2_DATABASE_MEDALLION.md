# Module 2 — Database & Medallion Schema

## Context & Objective
Create the medallion schemas and the Bronze landing table, applied through an
ordered, idempotent migration mechanism. Bronze is the system of record that Spark
writes to (Module 4) and dbt reads from (Module 5). Objective: a repeatable
`make migrate` that creates `bronze`, `silver`, `gold` schemas and
`bronze.prices_raw` with the correct business-key uniqueness constraint.

## Prerequisites
- Module 1 complete: Postgres healthy with database `cryptostream`, reachable via
  `DATABASE_URL`.
- Codebase state: repo skeleton exists; `db/` is empty except `.gitkeep`.

## Technical Specifications
Schemas: `bronze` (raw), `silver` (clean), `gold` (analytics). Silver and Gold
objects are created by dbt later; this module creates only the schemas plus the
Bronze table.

`bronze.prices_raw`:

| column | type | notes |
|---|---|---|
| id | bigint generated always as identity | surrogate PK |
| symbol | text not null | e.g. BTCUSD |
| exchange | text not null | e.g. binance |
| price | numeric(20,8) not null | > 0 enforced by CHECK |
| volume | numeric(20,8) | nullable |
| event_time | timestamptz not null | source event time |
| ingested_at | timestamptz not null default now() | pipeline time |
| source | text not null | e.g. freecryptoapi |
| raw | jsonb | original payload for replay/debug |

Constraints & indexes:
- `unique (symbol, exchange, event_time)` — the idempotency key from Module 0 §5.
- `check (price > 0)`.
- index on `(symbol, event_time desc)` for downstream reads.

## Step-by-Step Implementation Instructions
1. Create `db/migrations/0001_schemas.sql`:
   ```sql
   create schema if not exists bronze;
   create schema if not exists silver;
   create schema if not exists gold;
   ```
2. Create `db/migrations/0002_bronze_prices_raw.sql` implementing the table,
   the unique business-key constraint, the CHECK, and the index above. Use
   `create table if not exists`.
3. Create `db/run_migrations.py` — a small runner that:
   - reads `DATABASE_URL`,
   - creates a `public.schema_migrations(version text primary key, applied_at timestamptz default now())` table if absent,
   - lists `db/migrations/*.sql` sorted by filename,
   - applies any whose filename prefix is not yet in `schema_migrations`, each in
     its own transaction, recording the version on success,
   - is safe to run repeatedly (no-ops when nothing is pending).
   Use `psycopg[binary]`.
4. Add a `Makefile` target `migrate` that runs the runner in a one-off Python
   container on the `cryptostream` network with `.env` loaded, e.g.:
   ```
   migrate:
       docker compose run --rm --no-deps -v $(PWD)/db:/db -w /db \
         -e DATABASE_URL="$$DATABASE_URL" python:3.11-slim \
         bash -lc "pip install -q 'psycopg[binary]' && python run_migrations.py"
   ```

## Verification & Testing Criteria
```bash
make migrate            # applies 0001, 0002
make migrate            # second run is a clean no-op (idempotent)
make psql -c "\dn"                          # shows bronze, silver, gold
make psql -c "\d bronze.prices_raw"         # columns + constraints present
make psql -c "select conname from pg_constraint where conrelid='bronze.prices_raw'::regclass;"
# ^ must include the unique (symbol,exchange,event_time) and the price check
```
Insert a duplicate business key manually and confirm the unique constraint rejects
the second insert:
```bash
make psql -c "insert into bronze.prices_raw(symbol,exchange,price,event_time,source)
              values('BTCUSD','binance',1,'2020-01-01T00:00:00Z','test')
              on conflict do nothing;"   # run twice; second inserts 0 rows
```

## Hand-off State
- Schemas `bronze`, `silver`, `gold` exist.
- `bronze.prices_raw` exists with the business-key unique constraint, price CHECK,
  and read index.
- `schema_migrations` tracks applied versions; `make migrate` is idempotent.
Module 3 will produce messages matching these columns; Module 4 will `INSERT …
ON CONFLICT (symbol,exchange,event_time) DO NOTHING` into this exact table.
