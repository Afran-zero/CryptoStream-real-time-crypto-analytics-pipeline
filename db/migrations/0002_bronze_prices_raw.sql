-- Module 2 — Bronze landing table.
-- Idempotent via `create table if not exists`; the runner also dedups via
-- `public.schema_migrations`. Constraints are named explicitly so the
-- verification queries can find them predictably.

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

-- Read index for downstream queries by symbol/most-recent-first.
create index if not exists idx_bronze_prices_raw_symbol_event_time
    on bronze.prices_raw (symbol, event_time desc);