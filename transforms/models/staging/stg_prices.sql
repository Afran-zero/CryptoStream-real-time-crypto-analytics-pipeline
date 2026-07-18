{{ config(materialized='table') }}

-- Silver: deduplicated, typed, validated price ticks.
-- Dedupe keeps the row with the latest `ingested_at` for each
-- (symbol, exchange, event_time) — same business key the Bronze
-- unique constraint enforces, so this is a structural no-op when the
-- stream is healthy and a safety net when it isn't.

with source as (
    select * from {{ source('bronze', 'prices_raw') }}
),

deduped as (
    select distinct on (symbol, exchange, event_time)
        id,
        symbol,
        exchange,
        price::numeric(20, 8)            as price,
        volume::numeric(20, 8)           as volume,
        event_time,
        ingested_at,
        source
    from source
    where price is not null
      and price > 0
    order by symbol, exchange, event_time, ingested_at desc
)

select
    symbol,
    exchange,
    price,
    volume,
    event_time,
    ingested_at,
    source
from deduped