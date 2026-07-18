"""Candles endpoint."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query

from api.dependencies import get_repo
from api.models import Candle, CandlesResponse
from api.repository import GoldRepository

router = APIRouter(tags=["candles"])


@router.get("/candles/{symbol}", response_model=CandlesResponse)
def get_candles(
    symbol: str = Path(..., description="Symbol, e.g. `BTCUSD`"),
    interval: str = Query(
        "1m",
        pattern=r"^\d+[mhd]$",
        description="Candle interval. Only `1m` is materialized in the current Gold build.",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="How many of the most recent candles to return (ascending).",
    ),
    repo: GoldRepository = Depends(get_repo),
) -> CandlesResponse:
    rows = repo.candles(symbol.upper(), limit)
    return CandlesResponse(
        symbol=symbol.upper(),
        interval=interval,
        candles=[Candle(**r) for r in rows],
    )
