"""FastAPI application entrypoint.

`uvicorn api.main:app` to run; `python -m api.main` also works.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import ApiConfig
from api.db import build_pool
from api.routers import ALL_ROUTERS

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    cfg = ApiConfig.from_env()

    # CORS is installed during startup so preflight OPTIONS requests
    # are handled correctly even on the very first call.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cfg.cors_origins),
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    pool = build_pool(cfg)
    pool.open(wait=True, timeout=cfg.pool_timeout_s)

    app.state.config = cfg
    app.state.pool = pool
    logger.info(
        "api.started",
        extra={
            "gold_schema": cfg.gold_schema,
            "pool_min": cfg.pool_min,
            "pool_max": cfg.pool_max,
            "cors_origins": cfg.cors_origins,
        },
    )
    try:
        yield
    finally:
        pool.close()
        logger.info("api.stopped")


# `lifespan` is wired up here so the same instance can be imported
# (`from api.main import app`) without the decorator being missed.
app = FastAPI(
    title="CryptoStream Gold API",
    version="0.1.0",
    description="Read-only HTTP access to the Gold layer (OHLC candles + MA-20).",
    lifespan=lifespan,
)


for r in ALL_ROUTERS:
    app.include_router(r)


def run() -> None:
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    uvicorn.run(
        "api.main:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),
        port=int(os.environ.get("API_PORT", "8000")),
        reload=bool(os.environ.get("API_RELOAD", "")),
    )


if __name__ == "__main__":
    run()
