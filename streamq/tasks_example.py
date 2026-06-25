"""Example task handlers — registered by importing this module."""

from __future__ import annotations

import asyncio

from streamq.registry import task


@task("add")
def add(payload: dict) -> dict:
    return {"sum": payload["a"] + payload["b"]}


@task("slow_echo")
async def slow_echo(payload: dict) -> dict:
    await asyncio.sleep(payload.get("delay", 0.1))
    return {"echo": payload.get("msg", "")}


@task("boom")
def boom(payload: dict) -> dict:
    # Always fails — used to exercise the failure path (and retries in Step 3).
    raise RuntimeError("intentional failure for testing")
