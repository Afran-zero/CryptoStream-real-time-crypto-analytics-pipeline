# CryptoStream — ingestion service image (Module 3).
# Build context is the repo root (set in docker-compose.yml) so we can
# COPY the sibling `cryptostream_common` package.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app/ingestion

COPY ingestion/pyproject.toml ./
COPY ingestion/src ./src
COPY cryptostream_common /app/cryptostream_common

RUN pip install /app/cryptostream_common && \
    pip install . && \
    pip install "pytest>=8" "pytest-asyncio>=0.23"

CMD ["python", "-m", "ingestion.main"]