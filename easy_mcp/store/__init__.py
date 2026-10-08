"""Stores: where state that outlives one request is kept.

* :class:`MemoryStore` -- the default: everything stays in this process.
* :class:`RedisStore` -- shared between worker processes through Redis
  (the ``[redis]`` extra; ``redis`` is imported only when one is created).

The :class:`Store` interface is public and provisional until 1.0.
"""

from .base import (
    AsyncRateLimiter,
    ExpiredSession,
    Reservation,
    SessionKind,
    SessionRecord,
    Store,
    StoreHandle,
)
from .memory import MemoryStore
from .redis_store import RedisStore

__all__ = [
    "AsyncRateLimiter",
    "ExpiredSession",
    "MemoryStore",
    "RedisStore",
    "Reservation",
    "SessionKind",
    "SessionRecord",
    "Store",
    "StoreHandle",
]
