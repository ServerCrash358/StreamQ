"""
config.py — typed settings for the queue, loaded from the environment.

Everything that tunes delivery/retry/scaling behaviour lives here so it's one
validated object, and so the worker, broker, and API all agree on names like the
stream and consumer group.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "development"
    log_level: str = "info"

    # ── backing stores ───────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6381/0"
    database_url: str = "postgresql://streamq:streamq@localhost:5436/streamq"
    pool_min_size: int = 2
    pool_max_size: int = 10

    # ── Redis Streams topology ───────────────────────────────────────────
    stream: str = "streamq:tasks"      # the main work stream (XADD here)
    group: str = "workers"             # consumer group all workers share
    dlq_stream: str = "streamq:dlq"    # dead-letter stream for give-ups
    scheduled_zset: str = "streamq:scheduled"   # delayed retries, scored by ready-at time

    # ── consumer behaviour ───────────────────────────────────────────────
    batch_size: int = 10               # XREADGROUP COUNT
    block_ms: int = 5000               # XREADGROUP BLOCK (long-poll)
    worker_concurrency: int = 4        # in-flight tasks per worker process

    # ── reliability / retry ──────────────────────────────────────────────
    max_retries: int = 3
    base_backoff_seconds: float = 1.0     # delay = base * 2**attempt (+ jitter)
    max_backoff_seconds: float = 60.0
    # A pending message older than this (ms, no ACK) is assumed orphaned by a
    # dead worker and becomes eligible for reclaim (XAUTOCLAIM).
    visibility_timeout_ms: int = 30000

    # ── per-tenant rate limiting (Step 4) ────────────────────────────────
    rate_limit_enabled: bool = True
    rate_limit_per_window: int = 100   # max tasks...
    rate_limit_window_seconds: float = 60.0   # ...per tenant per this window


@lru_cache
def get_settings() -> Settings:
    return Settings()
