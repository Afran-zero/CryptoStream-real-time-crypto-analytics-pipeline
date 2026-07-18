# 02 — Database fundamentals

CryptoStream stores everything in **Postgres**. This page explains
what that means, how the data is organised, and why the design
choices you see in `db/migrations/` are the way they are.

You don't need to know SQL to read this — we'll explain the
queries as we go.

---

## What is a database, really?

A database is a program that:

1. **Stores data on disk** (so it survives when the program restarts).
2. **Reads data back** when you ask.
3. **Guarantees** that even if two requests ask at the same time,
   or the power goes out mid-write, the data stays consistent.

Postgres is one specific database. It's:

- **Relational** — data is organised into *tables* with *rows* and
  *columns*, like a spreadsheet.
- **SQL-speaking** — you ask for data using SQL (Structured Query
  Language).
- **Open source** — free, widely used, runs almost anywhere.
- **ACID** — every transaction is *atomic* (all-or-nothing),
  *consistent* (rules aren't violated), *isolated* (concurrent
  transactions don't see each other's dirty data), and *durable*
  (once committed, it's on disk).

---

## Tables, rows, columns

A table is a named collection of rows. Each row is one observation;
each column is one field.

`bronze.prices_raw` is one table. It looks like this:

```
id | symbol | exchange      | price    | event_time           | source
---+--------+---------------+----------+----------------------+---------------
 1 | BTCUSD | FreeCryptoAPI | 67432.51 | 2026-07-19 14:30:00  | FreeCryptoAPI
 2 | ETHUSD | FreeCryptoAPI |  3520.10 | 2026-07-19 14:30:00  | FreeCryptoAPI
 3 | SOLUSD | FreeCryptoAPI |   152.83 | 2026-07-19 14:30:00  | FreeCryptoAPI
```

Each row is one price observation. Each column is one attribute.
Tables in Postgres can have millions of rows; ours will accumulate
over time.

---

## Schemas

A *schema* in Postgres is a folder for tables. It's purely
organisational — you can have a table called `prices_raw` in many
schemas, and they're different tables.

CryptoStream uses three schemas:

```
bronze   ← raw data, untouched, from the source
silver   ← typed and deduplicated version of bronze
gold     ← aggregated analytics (candles, moving averages)
```

This is the **medallion pattern**: bronze is the rough rock, silver
is the cleaned cut, gold is the polished jewellery.

Why bother?

- You never delete or modify bronze. If your silver logic has a bug,
  you fix it and rebuild — bronze is still there.
- Different teams can own different layers. The data engineer
  owns bronze; the analyst owns gold.
- Namespacing prevents confusion. `silver.stg_prices` is clearly
  different from `gold.candles_1m`.

---

## Data types

Each column has a **type** that says what kind of data it holds.
Postgres has many types; CryptoStream uses these:

| Type | What it holds | Example |
|------|---------------|---------|
| `text` | A variable-length string | `"BTCUSD"` |
| `numeric(20, 8)` | A decimal number with up to 20 digits, 8 after the decimal | `67432.51000000` |
| `timestamptz` | A timestamp with timezone info | `2026-07-19 14:30:00+00` |
| `jsonb` | A JSON document, stored in a binary form that's fast to query | `{"price": 67432.51, ...}` |

### Why `numeric` for prices, not `float`?

A `float` (floating-point) number can store very large or very small
numbers cheaply, but at the cost of **precision**. For example:

```
>>> 0.1 + 0.2
0.30000000000000004
```

That tiny error is fine for scientific computing. It's **catastrophic
for money**. If you compute a moving average of 1000 floats, the
errors accumulate. If you then display "BTC average price: 67432.49"
when the true answer is 67432.51, you're wrong.

`numeric(20, 8)` stores exactly the number you give it, with no
rounding error, ever. It's slower and uses more space than a float,
but for prices it's the only correct choice.

### Why `timestamptz`, not `timestamp`?

`timestamptz` (timestamp *with time zone*) stores the instant in
time unambiguously. Postgres converts to/from UTC internally. If
you mix `timestamp` (without timezone) across machines in different
timezones, you get bugs that only show up twice a year (during DST
transitions). Just use `timestamptz` always.

### Why `jsonb`, not `text`?

We could store the original WebSocket payload as a string, but
then we couldn't query inside it. With `jsonb`:

```sql
select raw->>'some_field' from bronze.prices_raw limit 1;
```

You can query into the JSON without parsing it on every read. The
`b` stands for "binary" — Postgres stores it in a parsed form.

---

## Constraints

A *constraint* is a rule that Postgres enforces automatically.

CryptoStream's `bronze.prices_raw` has two:

### `unique (symbol, exchange, event_time)`

This is the **business key**. It says: "no two rows can have the
same triple (symbol, exchange, event_time)."

Why? Because if the same observation arrives twice (Kafka
redelivery, replay from checkpoint, etc.), we don't want to insert
it twice. The constraint makes that impossible at the database
level.

This is what lets Module 4 say:

```sql
INSERT INTO bronze.prices_raw (...)
SELECT ... FROM staging
ON CONFLICT (symbol, exchange, event_time) DO NOTHING;
```

The `ON CONFLICT ... DO NOTHING` says: "if the row already exists,
don't error out — just skip it." Without the unique constraint,
that clause has nothing to conflict with and Postgres would error.

### `check (price > 0)`

Prices can't be zero or negative. If anyone tries to insert such a
row, Postgres refuses.

This is **defence in depth**. The Pydantic validator in ingestion
should already reject `price = 0`; but if a bug slips past that,
this constraint catches it.

---

## Indexes

An *index* is a sidecar data structure that makes certain queries
fast.

Without an index, finding a specific row in a million-row table
means scanning all million rows. With an index, Postgres jumps
straight to the right place.

CryptoStream's Bronze has one index:

```sql
create index idx_bronze_prices_raw_symbol_event_time
    on bronze.prices_raw (symbol, event_time desc);
```

This makes "give me the latest BTC price" instant. The index is on
`(symbol, event_time desc)` — Postgres can use it for queries like:

```sql
select * from bronze.prices_raw
where symbol = 'BTCUSD'
order by event_time desc
limit 1;
```

Indexes cost: every write has to update them. For our write-heavy
Bronze, we have **only one** index for that reason — we can always
add more later if a query pattern demands it.

---

## Transactions

A *transaction* is a unit of work that's all-or-nothing.

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

If the second `UPDATE` fails (e.g. the row doesn't exist), the first
is rolled back. Money is never created or destroyed by accident.

Postgres's idempotent upsert uses transactions internally. Each
micro-batch's staging-table + insert + drop is one transaction.

---

## The migration runner

`db/run_migrations.py` is a 60-line script that:

1. Reads every `.sql` file in `db/migrations/` in alphabetical order.
2. Skips files already in the `public.schema_migrations` ledger.
3. Executes each new file in its own transaction.
4. Records the filename on success.

Why a custom runner instead of `psql -f`?

- It remembers what's already applied (you don't have to).
- It wraps each file in a transaction (a failed file doesn't leave
  partial work).
- It's safe to run repeatedly (idempotent).

This is the same job that tools like Flyway, Liquibase, and Alembic
do — but for our 2 SQL files, a tiny script is plenty.

---

## The medallion in this codebase

```sql
-- 0001_schemas.sql
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

-- 0002_bronze_prices_raw.sql
create table if not exists bronze.prices_raw (
    id            bigint generated always as identity primary key,
    symbol        text        not null,
    exchange      text        not null,
    price         numeric(20,8) not null,
    volume        numeric(20,8),
    event_time    timestamptz not null,
    ingested_at   timestamptz not null default now(),
    source        text        not null,
    raw           jsonb,
    constraint unique_business_key unique (symbol, exchange, event_time),
    constraint price_positive       check (price > 0)
);
```

That's the entire Module 2 — two SQL files, one runner script.
Everything else (Silver, Gold) is built later by dbt.

---

## Try it yourself

Once the stack is running:

```bash
# Show every table
make psql -- -c "\dt bronze.*"
make psql -- -c "\dt silver.*"
make psql -- -c "\dt gold.*"

# Look at the Bronze schema
make psql -- -c "\d bronze.prices_raw"

# See the constraints
make psql -- -c "select conname from pg_constraint
                 where conrelid = 'bronze.prices_raw'::regclass;"

# See the indexes
make psql -- -c "select indexname from pg_indexes
                 where tablename = 'prices_raw';"
```

The output of that last query should include
`idx_bronze_prices_raw_symbol_event_time`.

---

## Vocabulary

| Term | Meaning |
|------|---------|
| Database | A program that stores data durably and answers queries |
| Table | A named collection of rows |
| Row | One observation / record |
| Column | One field / attribute |
| Schema | A folder for tables |
| Type | What kind of data a column holds (text, numeric, ...) |
| Constraint | A rule enforced automatically (unique, check, ...) |
| Index | A sidecar data structure for fast lookups |
| Transaction | An all-or-nothing unit of work |
| Primary key | A column (or set) that uniquely identifies a row |
| Foreign key | A reference from one row to another table's row |
| Upsert | Insert-or-update in one statement |
| SQL | The language you use to ask the database questions |

---

## What's next?

- [03_KAFKA_FUNDAMENTALS.md](03_KAFKA_FUNDAMENTALS.md) — the queue that
  sits between the source and the database.
- [04_DOCKER_FUNDAMENTALS.md](04_DOCKER_FUNDAMENTALS.md) — how Postgres
  (and everything else) actually runs on your machine.