"""
broker.py — the enqueue side + Redis Streams setup.

enqueue() does two writes:
  1. INSERT the job into Postgres (status=queued) — the durable record of truth.
  2. XADD a lightweight message (just job_id + routing) onto the Redis stream.
The worker reads the message, then loads the full payload from Postgres by id.
Keeping the payload in Postgres (not the stream) makes Postgres the source of
truth and keeps the stream small.

The consumer GROUP is what makes this distributed: every worker reads from the
same group, and Redis hands each message to exactly one worker.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import redis.exceptions

from streamq.config import get_settings
from streamq.db import create_pool


class Broker:
    def __init__(self, redis: aioredis.Redis, pool: asyncpg.Pool) -> None:
        self.redis = redis
        self.pool = pool
        self.s = get_settings()

    @classmethod
    async def create(cls) -> "Broker":
        s = get_settings()
        # decode_responses=True → stream fields come back as str, not bytes.
        redis = aioredis.from_url(s.redis_url, decode_responses=True)
        pool = await create_pool()
        broker = cls(redis, pool)
        await broker.ensure_group()
        return broker

    async def ensure_group(self) -> None:
        """Create the consumer group (and the stream) if it doesn't exist."""
        try:
            await self.redis.xgroup_create(self.s.stream, self.s.group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):   # already exists → fine
                raise

    async def enqueue(
        self,
        task_name: str,
        payload: dict[str, Any] | None = None,
        *,
        tenant_id: str = "default",
        max_retries: int | None = None,
    ) -> str:
        payload = payload or {}
        max_retries = self.s.max_retries if max_retries is None else max_retries
        job_id = uuid.uuid4()

        # 1. durable record
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO jobs (id, task_name, payload, tenant_id, max_retries, status) "
                "VALUES ($1, $2, $3::jsonb, $4, $5, 'queued')",
                job_id, task_name, json.dumps(payload), tenant_id, max_retries,
            )
        # 2. work message
        stream_id = await self.redis.xadd(
            self.s.stream, {"job_id": str(job_id), "task": task_name, "tenant": tenant_id}
        )
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE jobs SET stream_id = $1 WHERE id = $2", stream_id, job_id)
        return str(job_id)

    async def close(self) -> None:
        await self.redis.aclose()
        await self.pool.close()
