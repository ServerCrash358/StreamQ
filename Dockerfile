# Multi-stage build for the worker. Light (no torch) — just the queue runtime.
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /app/.venv && \
    uv pip install --python /app/.venv -r pyproject.toml
COPY streamq ./streamq

FROM python:3.12-slim AS runtime
RUN useradd --create-home --uid 1000 appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/streamq /app/streamq
ENV PATH="/app/.venv/bin:$PATH"
USER appuser
EXPOSE 9100
# Exec form so the worker is PID 1 and gets SIGTERM for graceful shutdown.
CMD ["python", "-m", "streamq.worker"]
