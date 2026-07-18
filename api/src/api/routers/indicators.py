"""Moving-average indicator endpoint."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query

from api.dependencies import get_repo
from api.models import MovingAveragePoint, MovingAverageResponse
from api.repository import GoldRepository

router = APIRouter(tags=["indicators"])

DEFAULT_MA_WINDOW = 20
MAX_LIMIT = 2000


@router.get("/indicators/{symbol}/ma", response_model=MovingAverageResponse)
def moving_average(
    symbol: str = Path(..., description="Symbol, e.g. `BTCUSD`"),
    limit: int = Query(
        200,
        ge=1,
        le=MAX_LIMIT,
        description="How many of the most recent MA points to return (ascending).",
    ),
    repo: GoldRepository = Depends(get_repo),
) -> MovingAverageResponse:
    rows = repo.moving_average(symbol.upper(), limit)
    return MovingAverageResponse(
        symbol=symbol.upper(),
        window=DEFAULT_MA_WINDOW,
        points=[MovingAveragePoint(**r) for r in rows],
    )