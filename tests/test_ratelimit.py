"""Integration tests for the per-tenant limiter (needs Redis on :6381)."""

from __future__ import annotations

import uuid

import pytest
import redis.asyncio as aioredis

from streamq.config import get_settings
from streamq.ratelimit import RateLimiter


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(get_settings().redis_url)
    try:
        await client.ping()
    except Exception:
        pytest.skip("Redis not reachable on :6381 (docker compose up -d)")
    yield client
    await client.aclose()


async def test_allows_up_to_limit_then_blocks(redis_client):
    limiter = RateLimiter(redis_client, limit=3, window_seconds=10)
    tenant = f"tenant-{uuid.uuid4()}"          # fresh tenant → isolated test
    results = [await limiter.allow(tenant) for _ in range(4)]
    assert results == [True, True, True, False]


async def test_tenants_have_independent_budgets(redis_client):
    limiter = RateLimiter(redis_client, limit=1, window_seconds=10)
    a, b = f"a-{uuid.uuid4()}", f"b-{uuid.uuid4()}"
    assert await limiter.allow(a) is True
    assert await limiter.allow(b) is True      # different tenant, own budget
    assert await limiter.allow(a) is False      # a is exhausted
