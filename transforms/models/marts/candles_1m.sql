{{ config(materialized='table') }}

-- Gold: 1-minute OHLC candles per (symbol, exchange).
-- `open` / `close` use first_value / last_value ordered by event_time,
-- `high` and `low` use simple aggregates. `volume` is summed; null volumes
-- are coerced to 0 so the column stays non-nullable for downstream readers.

with silver as (
    select * from {{ ref('stg_prices') }}
),

bucketed as (
    select
        symbol,
        exchange,
        date_trunc('minute', event_time) as bucket,
        event_time,
        price,
        coalesce(volume, 0)::numeric(20, 8) as volume
    from silver
),

aggregated as (
    select
        symbol,
        exchange,
        bucket,
        first_value(price) over w as open,
        max(price)         over w as high,
        min(price)         over w as low,
        last_value(price)  over w as close,
        sum(volume)        over w as volume
    from bucketed
    window w as (
        partition by symbol, exchange, bucket
        order by event_time
        rows between unbounded preceding and unbounded following
    )
)

select distinct
    symbol,
    exchange,
    bucket,
    open::numeric(20, 8)  as open,
    high::numeric(20, 8)  as high,
    low::numeric(20, 8)   as low,
    close::numeric(20, 8) as close,
    volume::numeric(20, 8) as volume
from aggregated
order by symbol, exchange, bucket