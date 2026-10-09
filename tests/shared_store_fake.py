"""A shared store without Redis, for tests: several "workers" in one process.

:class:`FakeHub` holds what Redis would: session records with their expiry,
call counts, rate-limit windows, and the subscribers of the bus.  A record
lapses at its expiry, as a Redis TTL would, while the session's index keeps
naming it until a create, a refresh or a delete removes it; the hub then
remembers it as removed for ``REMOVED_MEMORY_SECONDS``.  ``hub.remove(ref)``
loses a session without a trace, as a failover or a restart can.  Each
:class:`FakeSharedStore` is one worker's view of it (``shared = True``), so
one process can serve two ``MCPServer``s, each with its own store on one
hub and each on its own ``live_server`` thread and event loop, and every
cross-worker path runs without a Redis server.

Every operation suspends once, as a network round trip would, and is logged
in ``hub.calls``.  Hooks inject trouble: ``hub.down`` makes every operation
raise ``StoreUnavailableError``, ``hub.drop_bus`` loses every message, and
``hub.before_reserve`` runs inside a session call reservation.
"""

from __future__ import annotations

import asyncio
import dataclasses
import secrets
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from easy_mcp.exceptions import RateLimitError, StoreUnavailableError
from easy_mcp.security.ratelimit import SlidingWindowRateLimiter
from easy_mcp.store.base import (
    AsyncRateLimiter,
    ExpiredSession,
    Reservation,
    SessionKind,
    SessionRecord,
    Store,
)
from easy_mcp.store.redis_store import REMOVED_MEMORY_SECONDS


@dataclasses.dataclass
class _Session:
    record: SessionRecord
    expires: float
    counts: dict[str, int] = dataclasses.field(default_factory=dict)


class FakeHub:
    """The state FakeSharedStores share, as one Redis would hold it."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.lock = threading.Lock()
        self.clock = clock
        self.sessions: dict[str, _Session] = {}
        self.index: dict[str, dict[str, float]] = {"http": {}, "sse": {}}
        # Refs removed lately, with when that memory lapses.
        self.removed: dict[str, dict[str, float]] = {"http": {}, "sse": {}}
        self.clients: dict[str, tuple[dict[str, int], float]] = {}
        self.windows: dict[str, deque[float]] = {}
        self.stores: list[FakeSharedStore] = []
        self.calls: list[str] = []
        self.payloads: list[tuple[str | None, str]] = []
        self.pings = 0
        self.down = False
        self.drop_bus = False
        self.before_reserve: Callable[[], Awaitable[None]] | None = None

    def store(self, worker: str | None = None) -> FakeSharedStore:
        """A new worker's store on this hub."""
        store = FakeSharedStore(self, worker)
        self.stores.append(store)
        return store

    def remove(self, ref: str) -> None:
        """Lose a session without a trace, as a failover or a restart can (no message either)."""
        with self.lock:
            self.sessions.pop(ref, None)
            for index in self.index.values():
                index.pop(ref, None)

    def _remember(self, kind: str, ref: str, now: float) -> bool:
        """Remember *ref* as removed; whether it was not remembered already."""
        removed = self.removed[kind]
        for lapsed in [known for known, until in removed.items() if until <= now]:
            del removed[lapsed]
        if ref in removed:
            return False
        removed[ref] = now + REMOVED_MEMORY_SECONDS
        return True

    def published(self, op: str) -> list[str]:
        """The payloads published for *op*."""
        return [payload for _, payload in self.payloads if f'"op":"{op}"' in payload]

    def _live(self, ref: str, now: float) -> _Session | None:
        entry = self.sessions.get(ref)
        if entry is not None and entry.expires <= now:
            del self.sessions[ref]  # expired, as a Redis TTL would
            return None
        return entry


class _FakeLimiter:
    def __init__(self, store: FakeSharedStore, local: SlidingWindowRateLimiter) -> None:
        self._store = store
        self._max = local._max
        self._window = local._window

    async def acheck(self, client_id: str) -> None:
        await self._store._op("rate")
        hub = self._store.hub
        with hub.lock:
            now = hub.clock()
            window = hub.windows.setdefault(client_id, deque())
            while window and window[0] <= now - self._window:
                window.popleft()
            if len(window) >= self._max:
                raise RateLimitError(max(0.0, window[0] + self._window - now))
            window.append(now)


class FakeSharedStore(Store):
    """One worker's view of a :class:`FakeHub`."""

    shared = True

    def __init__(self, hub: FakeHub, worker: str | None = None) -> None:
        self.hub = hub
        self._worker = worker or secrets.token_hex(8)
        self._handlers: list[Callable[[str], None]] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self.started = 0
        self.closed = 0

    @property
    def worker_id(self) -> str:
        return self._worker

    def describe(self) -> str:
        return f"fake shared store worker={self._worker}"

    async def _op(self, name: str) -> None:
        if self.hub.down:
            raise StoreUnavailableError()
        self._loop = asyncio.get_running_loop()
        with self.hub.lock:
            self.hub.calls.append(name)
        await asyncio.sleep(0)  # a round trip suspends

    async def start(self) -> None:
        self.started += 1
        self._loop = asyncio.get_running_loop()

    async def aclose(self) -> None:
        self.closed += 1
        self._loop = None

    async def ping(self) -> bool:
        with self.hub.lock:
            self.hub.pings += 1
        return not self.hub.down

    def rate_limiter(self, local: SlidingWindowRateLimiter) -> AsyncRateLimiter:
        return _FakeLimiter(self, local)

    async def create_session(
        self, record: SessionRecord, *, cap: int, ttl: float | None
    ) -> tuple[bool, list[ExpiredSession]]:
        await self._op("create")
        assert record.session_id is None, "a shared store must never see a raw session id"
        assert ttl is not None, "sessions in a shared store must expire"
        hub = self.hub
        with hub.lock:
            now = hub.clock()
            index = hub.index[record.kind]
            expired = [ref for ref, expiry in index.items() if expiry <= now]
            for ref in expired:
                del index[ref]
                hub.sessions.pop(ref, None)
                hub._remember(record.kind, ref, now)
            if sum(1 for expiry in index.values() if expiry > now) >= cap:
                return False, [ExpiredSession(ref, None) for ref in expired]
            if hub._live(record.ref, now) is not None:
                raise ValueError("a session with this ref exists already")
            stored = dataclasses.replace(record, t0=int(time.time() * 1000))
            hub.sessions[record.ref] = _Session(stored, now + ttl)
            index[record.ref] = now + ttl
        return True, [ExpiredSession(ref, None) for ref in expired]

    async def acquire_session(
        self,
        kind: SessionKind,
        ref: str,
        *,
        ttl: float | None,
        binding: tuple[str | None, str | None] | None = None,
    ) -> tuple[SessionRecord | None, list[ExpiredSession]]:
        await self._op("acquire")
        hub = self.hub
        with hub.lock:
            now = hub.clock()
            entry = hub._live(ref, now)
            if entry is None or entry.record.kind != kind:
                return None, []
            record = entry.record
            if ttl is not None and binding in (None, (record.identity_fp, record.principal)):
                entry.expires = now + ttl
                hub.index[kind][ref] = entry.expires
            return record, []

    async def release_session(
        self,
        kind: SessionKind,
        ref: str,
        *,
        ttl: float | None,
        protocol_version: str | None = None,
        touch: bool = True,
    ) -> None:
        await self._op("release")
        hub = self.hub
        with hub.lock:
            now = hub.clock()
            entry = hub._live(ref, now)
            if entry is None or entry.record.kind != kind:
                return
            if ttl is not None and touch:
                entry.expires = now + ttl
                hub.index[kind][ref] = entry.expires
            if protocol_version is not None:
                entry.record = dataclasses.replace(entry.record, protocol_version=protocol_version)

    async def refresh_sessions(
        self, kind: SessionKind, refs: Sequence[str], *, ttl: float
    ) -> tuple[set[str], list[ExpiredSession]]:
        await self._op("refresh")
        hub = self.hub
        gone: set[str] = set()
        expired: list[ExpiredSession] = []
        with hub.lock:
            now = hub.clock()
            for ref in refs:
                entry = hub._live(ref, now)
                if entry is None:
                    gone.add(ref)
                    if hub.index[kind].pop(ref, None) is not None:
                        hub._remember(kind, ref, now)
                        expired.append(ExpiredSession(ref, None))
                    elif hub._remember(kind, ref, now):
                        expired.append(ExpiredSession(ref, None, lost=True))
                else:
                    entry.expires = now + ttl
                    hub.index[kind][ref] = entry.expires
        return gone, expired

    async def delete_session(self, kind: SessionKind, ref: str) -> bool:
        await self._op("delete")
        hub = self.hub
        with hub.lock:
            now = hub.clock()
            hub.sessions.pop(ref, None)
            indexed = hub.index[kind].pop(ref, None) is not None
            # Removed here, or lost without a trace: either way, audited here.
            return hub._remember(kind, ref, now) or indexed

    async def reserve_session_call(self, ref: str, tool: str, limit: int) -> Reservation:
        await self._op("reserve")
        if self.hub.before_reserve is not None:
            await self.hub.before_reserve()
        hub = self.hub
        with hub.lock:
            entry = hub._live(ref, hub.clock())
            if entry is None:
                return Reservation.GONE
            used = entry.counts.get(tool, 0)
            if used >= limit:
                return Reservation.LIMIT
            entry.counts[tool] = used + 1
            return Reservation.OK

    async def release_session_call(self, ref: str, tool: str) -> None:
        await self._op("unreserve")
        hub = self.hub
        with hub.lock:
            entry = hub._live(ref, hub.clock())
            if entry is not None and entry.counts.get(tool, 0) > 0:
                entry.counts[tool] -= 1

    async def touch_client(self, client_id: str, *, ttl: float | None) -> None:
        return None  # as in Redis: counted and refused calls refresh a client's counts

    async def reserve_client_call(
        self, client_id: str, tool: str, limit: int, *, ttl: float | None
    ) -> Reservation:
        await self._op("client_reserve")
        hub = self.hub
        with hub.lock:
            now = hub.clock()
            counts, expires = hub.clients.get(client_id, ({}, 0.0))
            if expires <= now:
                counts = {}
            used = counts.get(tool, 0)
            if used < limit:
                counts[tool] = used + 1
            hub.clients[client_id] = (counts, now + (ttl or 3600.0))
            return Reservation.OK if used < limit else Reservation.LIMIT

    async def release_client_call(self, client_id: str, tool: str) -> None:
        await self._op("client_unreserve")
        hub = self.hub
        with hub.lock:
            counts, _ = hub.clients.get(client_id, ({}, 0.0))
            if counts.get(tool, 0) > 0:
                counts[tool] -= 1

    async def publish(self, payload: str, *, to: str | None = None) -> int:
        await self._op("publish")
        hub = self.hub
        with hub.lock:
            hub.payloads.append((to, payload))
            if hub.drop_bus:
                return 0
            targets = [
                store
                for store in hub.stores
                if (to is None or store.worker_id == to) and store._handlers
            ]
        received = 0
        for store in targets:
            loop = store._loop
            if loop is None or loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(store._dispatch, payload)
            except RuntimeError:
                continue  # its loop closed meanwhile
            received += 1
        return received

    def subscribe(self, handler: Callable[[str], None]) -> Callable[[], None]:
        self._handlers.append(handler)
        return lambda: self._handlers.remove(handler)

    def _dispatch(self, payload: str) -> None:
        for handler in list(self._handlers):
            handler(payload)


def records(hub: FakeHub) -> list[SessionRecord]:
    with hub.lock:
        return [entry.record for entry in hub.sessions.values()]


def everything(hub: FakeHub) -> Any:
    """All the hub holds, for searching: records, counts, windows, payloads."""
    with hub.lock:
        return {
            "sessions": {ref: (entry.record, entry.counts) for ref, entry in hub.sessions.items()},
            "index": {kind: dict(index) for kind, index in hub.index.items()},
            "removed": {kind: dict(removed) for kind, removed in hub.removed.items()},
            "clients": dict(hub.clients),
            "windows": {client: list(window) for client, window in hub.windows.items()},
            "payloads": list(hub.payloads),
        }
