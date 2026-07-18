-- Singular test: every candle must satisfy OHLC invariants.
-- Returns any rows that violate the bounds. An empty result set
-- means the test passed.

select
    symbol,
    exchange,
    bucket,
    open,
    high,
    low,
    close
from {{ ref('candles_1m') }}
where not (
    high >= low
    and high >= open
    and high >= close
    and low  <= open
    and low  <= close
)