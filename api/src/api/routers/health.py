"""Health endpoint.

`status` is the overall service health ("ok" when DB + Gold freshness
are both fine), `db` is just the DB ping. The dashboard reads
`gold_freshness_seconds` for the green/amber/red badge.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from api.dependencies import get_repo
from api.models import HealthResponse
from api.repository import GoldRepository

router = APIRouter(tags=["health"])

# Beyond 10 minutes, the dashboard turns the badge red. Surface that
# in the API too so callers can decide without parsing the dashboard.
FRESHNESS_AMBER_S = 120
FRESHNESS_RED_S = 600


@router.get("/health", response_model=HealthResponse)
def health(
    response: Response,
    repo: GoldRepository = Depends(get_repo),
) -> HealthResponse:
    status = repo.health()
    freshness = status.freshness_seconds
    if not status.db_ok:
        # 503 signals "DB unreachable" so a load balancer can act.
        response.status_code = 503
        return HealthResponse(status="unavailable", db="down", gold_freshness_seconds=None)
    if freshness is None:
        return HealthResponse(status="warming_up", db="ok", gold_freshness_seconds=None)
    if freshness >= FRESHNESS_RED_S:
        return HealthResponse(status="stale", db="ok", gold_freshness_seconds=freshness)
    return HealthResponse(
        status="ok" if freshness < FRESHNESS_AMBER_S else "stale",
        db="ok",
        gold_freshness_seconds=freshness,
    )