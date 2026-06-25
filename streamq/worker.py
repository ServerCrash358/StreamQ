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
import time
import uuid
from typing import Any

import redis.exceptions

from streamq.broker import Broker
from streamq.config import get_settings
from streamq.metrics import TASK_DURATION, TASKS_PROCESSED
from streamq.models import JobStatus
from streamq.ratelimit import RateLimiter
from streamq.registry import run_handler
from streamq.retry import compute_backoff


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
        # Consumer name within the group. In a StatefulSet the hostname is a
        # STABLE ordinal (streamq-worker-0), so a restarted pod reclaims its own
        # consumer identity; pid keeps it unique if you run several locally.
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self._sem = asyncio.Semaphore(self.s.worker_concurrency)
        self.limiter = RateLimiter(self.redis, self.s.rate_limit_per_window, self.s.rate_limit_window_seconds)

    async def _process(self, msg_id: str, fields: dict[str, str]) -> None:
        tenant = fields.get("tenant", "default")
        # Per-tenant gate FIRST — before marking running/attempts. If the tenant
        # is over its limit, defer (reschedule) the task; this is NOT a failure,
        # so it doesn't consume a retry. ACK the current delivery; the scheduler
        # re-enqueues it after the defer window.
        if self.s.rate_limit_enabled and not await self.limiter.allow(tenant):
            await self.broker.schedule_retry(
                fields["job_id"], fields["task"], tenant, self.s.rate_limit_defer_seconds
            )
            TASKS_PROCESSED.labels(fields.get("task", "?"), "deferred").inc()
            await self._ack(msg_id)
            return

        job_id = uuid.UUID(fields["job_id"])
        # Mark running + bump attempts; pull the payload (Postgres = source of truth).
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE jobs SET status = 'running', attempts = attempts + 1, updated_at = now() "
                "WHERE id = $1 RETURNING task_name, payload, attempts, max_retries, tenant_id",
                job_id,
            )
        if row is None:   # job row vanished (shouldn't happen) — drop the message
            await self._ack(msg_id)
            return

        task_name = row["task_name"]
        attempts, max_retries, tenant = row["attempts"], row["max_retries"], row["tenant_id"]
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        start = time.perf_counter()
        try:
            result = await run_handler(task_name, payload)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status = 'succeeded', result = $2::jsonb, updated_at = now() "
                    "WHERE id = $1",
                    job_id, json.dumps(_jsonable(result)),
                )
            TASKS_PROCESSED.labels(task_name, "succeeded").inc()
        except Exception as e:
            if attempts < max_retries:
                TASKS_PROCESSED.labels(task_name, "failed").inc()
                # transient failure → schedule a delayed retry (backoff + jitter)
                delay = compute_backoff(attempts, self.s.base_backoff_seconds, self.s.max_backoff_seconds)
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE jobs SET status = $2, last_error = $3, updated_at = now() WHERE id = $1",
                        job_id, JobStatus.FAILED, str(e),
                    )
                await self.broker.schedule_retry(str(job_id), task_name, tenant, delay)
            else:
                # out of retries → dead-letter it for inspection
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE jobs SET status = $2, last_error = $3, updated_at = now() WHERE id = $1",
                        job_id, JobStatus.DEAD, str(e),
                    )
                await self.broker.send_to_dlq(str(job_id), task_name, tenant, str(e))
                TASKS_PROCESSED.labels(task_name, "dead").inc()
        finally:
            TASK_DURATION.labels(task_name).observe(time.perf_counter() - start)
            # ACK the delivery. Terminal outcomes (succeeded/dead) are done; a
            # retry rides a NEW message re-enqueued by the scheduler, so acking
            # here is correct and avoids redelivery loops.
            await self._ack(msg_id)

    async def _ack(self, msg_id: str) -> None:
        # ACK removes it from the pending list; XDEL removes the entry entirely so
        # the stream doesn't grow unbounded with already-processed messages.
        await self.redis.xack(self.s.stream, self.s.group, msg_id)
        await self.redis.xdel(self.s.stream, msg_id)

    async def _guarded(self, msg_id: str, fields: dict[str, str]) -> None:
        async with self._sem:
            await self._process(msg_id, fields)

    async def run(self, *, stop_when_idle: bool = False) -> None:
        s = self.s
        while True:
            try:
                resp = await self.redis.xreadgroup(
                    s.group, self.name, {s.stream: ">"}, count=s.batch_size, block=s.block_ms
                )
            except (redis.exceptions.TimeoutError, asyncio.TimeoutError):
                # A blocking read that found nothing within block_ms — benign.
                resp = None
            except redis.exceptions.ConnectionError:
                # Transient Redis blip — back off briefly and retry.
                await asyncio.sleep(1)
                continue
            if not resp:
                if stop_when_idle:
                    return
                continue
            _stream, messages = resp[0]
            await asyncio.gather(*(self._guarded(mid, f) for mid, f in messages))


async def main() -> None:
    # Importing the example tasks registers their handlers in the registry.
    from streamq import tasks_example  # noqa: F401
    from streamq.metrics import collect_gauges, serve_metrics
    from streamq.reclaimer import Reclaimer
    from streamq.scheduler import Scheduler

    broker = await Broker.create()
    worker = Worker(broker)
    scheduler = Scheduler(broker)   # pumps delayed retries back onto the stream
    reclaimer = Reclaimer(worker)   # recovers work orphaned by crashed workers
    serve_metrics(broker.s.metrics_port)   # expose Prometheus /metrics
    print(f"worker {worker.name} on '{broker.s.stream}' (metrics :{broker.s.metrics_port})")
    try:
        # consume + scheduler + reclaimer + metrics sampler, all concurrent
        await asyncio.gather(
            worker.run(), scheduler.run(), reclaimer.run(), collect_gauges(broker)
        )
    finally:
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
