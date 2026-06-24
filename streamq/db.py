"""
db.py — asyncpg pool for the durable job store.

A single shared pool, created once at startup with a fail-fast connectivity
check. Workers and the API both use it to read/write the `jobs` table.
"""

from __future__ import annotations

import asyncpg

from streamq.config import get_settings


async def create_pool() -> asyncpg.Pool:
    s = get_settings()
    pool = await asyncpg.create_pool(
        dsn=s.database_url,
        min_size=s.pool_min_size,
        max_size=s.pool_max_size,
    )
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")   # prove the DB is reachable now, not later
    return pool
