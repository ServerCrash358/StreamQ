"""
worker.py — an asyncio worker that consumes the stream and runs tasks.

Loop: XREADGROUP (long-poll) pulls a batch the group hasn't delivered yet ('>'),
then each message is processed concurrently (bounded by a semaphore). A message
is `XACK`-ed only AFTER the job finishes — that's the at-least-once contract: if
this worker dies mid-task, the message stays "pending" and another worker can
reclaim it (Step 3: XAUTOCLAIM).

Step 2 handles the happy path + marks failures; retry/backoff/DLQ arrive in
Step 3 (the `except` branch is where they'll slot in).
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import uuid
from typing import Any

from streamq.broker import Broker
from streamq.config import get_settings
from streamq.models import JobStatus
from streamq.registry import run_handler


def _jsonable(v: Any) -> Any:
    try:
        json.dumps(v)
        return v
    except (TypeError, ValueError):
        return str(v)


class Worker:
    def __init__(self, broker: Broker, name: str | None = None) -> None:
        self.broker = broker
        self.redis = broker.redis
        self.pool = broker.pool
        self.s = get_settings()
        # Unique consumer name within the group (host-pid-rand) so Redis tracks
        # each worker's pending messages separately.
        self.name = name or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._sem = asyncio.Semaphore(self.s.worker_concurrency)

    async def _process(self, msg_id: str, fields: dict[str, str]) -> None:
        job_id = uuid.UUID(fields["job_id"])
        # Mark running + bump attempts; pull the payload (Postgres = source of truth).
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE jobs SET status = 'running', attempts = attempts + 1, updated_at = now() "
                "WHERE id = $1 RETURNING task_name, payload",
                job_id,
            )
        if row is None:   # job row vanished (shouldn't happen) — drop the message
            await self.redis.xack(self.s.stream, self.s.group, msg_id)
            return

        task_name = row["task_name"]
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        try:
            result = await run_handler(task_name, payload)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status = 'succeeded', result = $2::jsonb, updated_at = now() "
                    "WHERE id = $1",
                    job_id, json.dumps(_jsonable(result)),
                )
        except Exception as e:
            # Step 3 replaces this with retry-with-backoff / dead-letter.
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status = $2, last_error = $3, updated_at = now() WHERE id = $1",
                    job_id, JobStatus.FAILED, str(e),
                )
        finally:
            # ACK regardless in Step 2 (no redelivery yet). Step 3 will only ACK
            # on terminal outcomes and let retries re-deliver.
            await self.redis.xack(self.s.stream, self.s.group, msg_id)

    async def _guarded(self, msg_id: str, fields: dict[str, str]) -> None:
        async with self._sem:
            await self._process(msg_id, fields)

    async def run(self, *, stop_when_idle: bool = False) -> None:
        s = self.s
        while True:
            resp = await self.redis.xreadgroup(
                s.group, self.name, {s.stream: ">"}, count=s.batch_size, block=s.block_ms
            )
            if not resp:
                if stop_when_idle:
                    return
                continue
            _stream, messages = resp[0]
            await asyncio.gather(*(self._guarded(mid, f) for mid, f in messages))


async def main() -> None:
    # Importing the example tasks registers their handlers in the registry.
    from streamq import tasks_example  # noqa: F401

    broker = await Broker.create()
    worker = Worker(broker)
    print(f"worker {worker.name} consuming group '{broker.s.group}' on '{broker.s.stream}'")
    try:
        await worker.run()
    finally:
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
