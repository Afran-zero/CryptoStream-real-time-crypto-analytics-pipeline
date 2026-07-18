"""Routers package."""
from api.routers import candles, health, indicators, prices

__all__ = ["candles", "health", "indicators", "prices"]


# Used by `main.py` to register every router.
ALL_ROUTERS = (health.router, candles.router, indicators.router, prices.router)
