"""Pydantic v2 response models for the API."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str
    db: str
    gold_freshness_seconds: int | None = Field(
        None,
        description="Seconds since the most recent Gold candle was closed. "
        "None when Gold is empty.",
    )


class LatestPrice(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    exchange: str
    bucket: datetime
    close: Decimal


class LatestPricesResponse(BaseModel):
    prices: list[LatestPrice]


class Candle(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    exchange: str
    bucket: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


class CandlesResponse(BaseModel):
    symbol: str
    interval: str
    candles: list[Candle]


class MovingAveragePoint(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    bucket: datetime
    close: Decimal
    ma_20: Decimal | None = Field(
        None,
        description="20-period moving average. Null for the first 19 buckets of each symbol.",
    )


class MovingAverageResponse(BaseModel):
    symbol: str
    window: int
    points: list[MovingAveragePoint]