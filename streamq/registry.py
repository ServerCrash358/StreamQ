"""
registry.py — maps a task NAME (string) to the function that runs it.

The queue only ever carries a name + payload; workers look the handler up here.
A handler may be sync or async — `run_handler` awaits it either way, so CPU-light
tasks can stay plain functions and I/O-bound ones can be `async def`.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

Handler = Callable[[dict[str, Any]], Any]

_REGISTRY: dict[str, Handler] = {}


def task(name: str) -> Callable[[Handler], Handler]:
    """Decorator: register `fn` under `name`."""
    def decorator(fn: Handler) -> Handler:
        if name in _REGISTRY:
            raise ValueError(f"task '{name}' already registered")
        _REGISTRY[name] = fn
        return fn
    return decorator


def get_handler(name: str) -> Handler | None:
    return _REGISTRY.get(name)


async def run_handler(name: str, payload: dict[str, Any]) -> Any:
    fn = _REGISTRY.get(name)
    if fn is None:
        raise KeyError(f"no handler registered for task '{name}'")
    result = fn(payload)
    if inspect.isawaitable(result):
        result = await result
    return result
