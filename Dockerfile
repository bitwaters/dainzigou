FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY memebot ./memebot
RUN pip install --no-cache-dir uv && uv sync --frozen --no-dev

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 memebot
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY memebot ./memebot
COPY config.example.yaml /app/config.example.yaml
ENV PATH="/app/.venv/bin:$PATH" \
    TZ=UTC \
    PYTHONUNBUFFERED=1
USER memebot
HEALTHCHECK --interval=30s --timeout=5s --start-period=300s --retries=3 \
    CMD python -m memebot.healthcheck /data/heartbeat 300
CMD ["python", "-m", "memebot.main", "--config", "/config/config.yaml", "--env", "/config/.env"]
