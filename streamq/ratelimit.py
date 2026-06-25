"""
ratelimit.py — per-tenant sliding-window rate limiter (Redis ZSET + Lua).

Each tenant gets a sorted set of recent request timestamps. On each check we
drop entries older than the window, count what's left, and admit the request
only if we're under the limit. Running it as a single Lua script makes the
check-and-record ATOMIC, so many workers sharing one Redis can't collectively
overshoot a tenant's limit. (Same algorithm as the standalone rate-limiter
project, scoped per tenant here.)
"""

from __future__ import annotations

import time
import uuid

import redis.asyncio as aioredis

_SLIDING_WINDOW = """
local key, now, window, limit, member = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    return 1
end
return 0
"""


class RateLimiter:
    def __init__(self, redis: aioredis.Redis, limit: int, window_seconds: float) -> None:
        self._redis = redis
        self._limit = limit
        self._window_ms = int(window_seconds * 1000)
        self._script = redis.register_script(_SLIDING_WINDOW)

    async def allow(self, tenant: str) -> bool:
        """True if `tenant` is under its limit (and records the hit); else False."""
        now = int(time.time() * 1000)
        member = f"{now}-{uuid.uuid4().hex}"
        res = await self._script(
            keys=[f"streamq:ratelimit:{tenant}"],
            args=[now, self._window_ms, self._limit, member],
        )
        return bool(res)
