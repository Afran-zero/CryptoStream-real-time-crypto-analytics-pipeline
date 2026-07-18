{{ config(materialized='table') }}

-- Gold: 20-period moving average of `close` per (symbol, exchange),
-- ordered by bucket ascending. The window covers 19 preceding buckets
-- plus the current row, so the first 19 buckets of each symbol get a
-- partial-window MA (fewer rows in the window, but still numeric).

with candles as (
    select * from {{ ref('candles_1m') }}
),

windowed as (
    select
        symbol,
        exchange,
        bucket,
        close,
        avg(close) over (
            partition by symbol, exchange
            order by bucket
            rows between 19 preceding and current row
        ) as ma_20
    from candles
)

select
    symbol,
    exchange,
    bucket,
    close::numeric(20, 8)        as close,
    ma_20::numeric(20, 8)        as ma_20
from windowed
order by symbol, exchange, bucket