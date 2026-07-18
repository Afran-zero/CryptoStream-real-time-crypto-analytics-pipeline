"""Latest price endpoint."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_repo
from api.models import LatestPrice, LatestPricesResponse
from api.repository import GoldRepository

router = APIRouter(tags=["prices"])


@router.get("/prices/latest", response_model=LatestPricesResponse)
def latest_prices(
    symbols: str = Query(
        ...,
        description="Comma-separated symbols, e.g. `BTCUSD,ETHUSD`",
    ),
    repo: GoldRepository = Depends(get_repo),
) -> LatestPricesResponse:
    wanted = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    rows = repo.latest_prices(wanted)
    return LatestPricesResponse(
        prices=[LatestPrice(**r) for r in rows],
    )
