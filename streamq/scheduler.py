"""
scheduler.py — turns delayed retries into work.

Redis Streams has no native "deliver later", so a failed job is parked in a
sorted set scored by its ready-at timestamp (broker.schedule_retry). This loop
polls for entries whose time has come and re-enqueues them onto the work stream.
Runs as a background task inside each worker process; the atomic Lua pop means
multiple schedulers never double-enqueue the same retry.
"""

from __future__ import annotations

import asyncio
import json

from streamq.broker import Broker


class Scheduler:
    def __init__(self, broker: Broker, poll_interval: float = 1.0) -> None:
        self.broker = broker
        self.poll_interval = poll_interval

    async def run(self) -> None:
        while True:
            due = await self.broker.pop_due_retries()
            for member in due:
                d = json.loads(member)
                await self.broker.reenqueue(d["job_id"], d["task"], d.get("tenant", "default"))
            await asyncio.sleep(self.poll_interval)
