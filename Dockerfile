# ---------------------------------------------------------------------------
# Kafka Stream Feature Store — multi-stage Dockerfile
# ---------------------------------------------------------------------------
# Stage 1 (builder): installs Python deps into a venv
# Stage 2 (runtime): copies venv into a slim image
# ---------------------------------------------------------------------------

FROM python:3.9-slim AS builder

WORKDIR /build

# Install OS-level build deps for psycopg2-binary / confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Create isolated venv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt


# ---------------------------------------------------------------------------
# Runtime image
# ---------------------------------------------------------------------------
FROM python:3.9-slim AS runtime

WORKDIR /app

# curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source
COPY feature_store/ ./feature_store/
COPY scripts/ ./scripts/

# Non-root user
RUN useradd --create-home --shell /bin/bash appuser
USER appuser

# Default: run the API
CMD ["uvicorn", "feature_store.serving:app", "--host", "0.0.0.0", "--port", "8000"]

EXPOSE 8000
