# CryptoStream — API image (Module 7).
# Build context is the repo root (set in docker-compose.yml) so we can
# COPY the sibling `cryptostream_common` package.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app/api

COPY api/pyproject.toml ./
COPY api/src ./src
COPY cryptostream_common /app/cryptostream_common

RUN pip install /app/cryptostream_common && \
    pip install .

EXPOSE 8000

CMD ["python", "-m", "api.main"]