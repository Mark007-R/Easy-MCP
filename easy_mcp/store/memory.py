"""The default store: state lives in this process, as in 0.3.1."""

from __future__ import annotations

import dataclasses
import secrets
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from ..security.ratelimit import SlidingWindowRateLimiter
from .base import (
    AsyncRateLimiter,
    ExpiredSession,
    Reservation,
    SessionKind,
    SessionRecord,
    Store,
)

# Stateless requests have no session to hang per-client accounting on
# (max_calls_per_session), so it is kept per client id instead, for at most
# this many recently seen clients.  Like a session, it lapses once the client
# has been idle for its ttl.
STATELESS_CLIENTS_MAX = 4096


@dataclass(slots=True)
class _Session:
    record: SessionRecord
    last_seen: float
    # Its own idle timeout, the one its endpoint opened it with: another
    # endpoint pruning expired sessions must not apply its own.
    ttl: float | None
    active: int = 1  # requests holding it; a new session is held by its opener
    counts: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class _Client:
    counts: dict[str, int]
    last_seen: float


class MemoryStore(Store):
    """The default store: sessions, call counts and rate limits stay in this process.

    It behaves as 0.3.1 did: a session expires once it has been idle for its
    ttl with no request running, and stateless clients' counts are kept for
    the 4096 most recently seen clients.  ``max_sessions`` counts Streamable
    HTTP and legacy SSE sessions separately.  Unlike 0.3.1, when one server is
    served at several endpoints, it counts those of every endpoint together
    (each endpoint still knows only its own sessions), and a stateless
    client's counts are shared between the endpoints too; they lapse after the
    idle timeout of whichever endpoint the client reached last.  Its rate
    limiter is the server's own in-process one.

    No method awaits anything, so each call completes in one step of the
    event loop: no other request can run in between.

    Args:
        clock: Injectable monotonic clock, for deterministic tests.
    """

    shared = False

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._sessions: dict[str, _Session] = {}
        self._clients: OrderedDict[str, _Client] = OrderedDict()
        self._worker = secrets.token_hex(8)

    @property
    def worker_id(self) -> str:
        return self._worker

    def describe(self) -> str:
        return "memory"

    def rate_limiter(self, local: SlidingWindowRateLimiter) -> AsyncRateLimiter:
        return local

    # -------------------------------------------------------------- sessions

    @staticmethod
    def _expired(entry: _Session, now: float) -> bool:
        ttl = entry.ttl
        return ttl is not None and entry.active == 0 and now - entry.last_seen > ttl

    @staticmethod
    def _gone(entry: _Session) -> ExpiredSession:
        record = entry.record
        return ExpiredSession(record.ref, record.client_id, record.session_id, record.t0)

    def _entry(self, kind: SessionKind, ref: str) -> _Session | None:
        entry = self._sessions.get(ref)
        return entry if entry is not None and entry.record.kind == kind else None

    async def create_session(
        self, record: SessionRecord, *, cap: int, ttl: float | None
    ) -> tuple[bool, list[ExpiredSession]]:
        now = self._clock()
        expired: list[ExpiredSession] = []
        for ref, entry in list(self._sessions.items()):
            if entry.record.kind == record.kind and self._expired(entry, now):
                del self._sessions[ref]
                expired.append(self._gone(entry))
        live = sum(1 for entry in self._sessions.values() if entry.record.kind == record.kind)
        if live >= cap:
            return False, expired
        if record.ref in self._sessions:
            raise ValueError("a session with this ref exists already")
        if record.t0 is None:
            record = dataclasses.replace(record, t0=int(time.time() * 1000))
        self._sessions[record.ref] = _Session(record=record, last_seen=now, ttl=ttl)
        return True, expired

    async def acquire_session(
        self,
        kind: SessionKind,
        ref: str,
        *,
        ttl: float | None,
        binding: tuple[str | None, str | None] | None = None,
    ) -> tuple[SessionRecord | None, list[ExpiredSession]]:
        # A held session never expires here, so nothing is extended: a
        # refused request's release (touch=False) leaves its idle time alone.
        # It expires by the ttl it was opened with, as *ttl* always is.
        entry = self._entry(kind, ref)
        if entry is None:
            return None, []
        if self._expired(entry, self._clock()):
            del self._sessions[ref]
            return None, [self._gone(entry)]
        entry.active += 1
        return entry.record, []

    async def release_session(
        self,
        kind: SessionKind,
        ref: str,
        *,
        ttl: float | None,
        protocol_version: str | None = None,
        touch: bool = True,
    ) -> None:
        entry = self._entry(kind, ref)
        if entry is None:
            return
        entry.active = max(0, entry.active - 1)
        if touch:
            entry.last_seen = self._clock()
        if protocol_version is not None:
            entry.record = dataclasses.replace(entry.record, protocol_version=protocol_version)

    async def refresh_sessions(
        self, kind: SessionKind, refs: Sequence[str], *, ttl: float
    ) -> tuple[set[str], list[ExpiredSession]]:
        # A session in use never expires here: one that is gone was removed
        # by whoever found it expired or ended it.
        now = self._clock()
        gone: set[str] = set()
        for ref in refs:
            entry = self._entry(kind, ref)
            if entry is None:
                gone.add(ref)
            else:
                entry.last_seen = now
        return gone, []

    async def delete_session(self, kind: SessionKind, ref: str) -> bool:
        if self._entry(kind, ref) is None:
            return False
        del self._sessions[ref]
        return True

    # ------------------------------------------------- max_calls_per_session

    @staticmethod
    def _reserve(counts: dict[str, int], tool: str, limit: int) -> Reservation:
        used = counts.get(tool, 0)
        if used >= limit:
            return Reservation.LIMIT
        counts[tool] = used + 1
        return Reservation.OK

    @staticmethod
    def _release(counts: dict[str, int], tool: str) -> None:
        if counts.get(tool, 0) > 0:
            counts[tool] -= 1

    async def reserve_session_call(self, ref: str, tool: str, limit: int) -> Reservation:
        entry = self._sessions.get(ref)
        if entry is None:
            return Reservation.GONE
        return self._reserve(entry.counts, tool, limit)

    async def release_session_call(self, ref: str, tool: str) -> None:
        entry = self._sessions.get(ref)
        if entry is not None:
            self._release(entry.counts, tool)

    def _client(self, client_id: str, ttl: float | None) -> _Client:
        """A stateless client's counts, made fresh when new or idle past *ttl*."""
        now = self._clock()
        entry = self._clients.get(client_id)
        if entry is None or (ttl is not None and now - entry.last_seen > ttl):
            # New, or idle long enough that a session would have expired.
            entry = _Client(counts={}, last_seen=now)
            self._clients[client_id] = entry
        entry.last_seen = now
        self._clients.move_to_end(client_id)
        if len(self._clients) > STATELESS_CLIENTS_MAX:
            self._clients.popitem(last=False)
        return entry

    async def touch_client(self, client_id: str, *, ttl: float | None) -> None:
        self._client(client_id, ttl)

    async def reserve_client_call(
        self, client_id: str, tool: str, limit: int, *, ttl: float | None
    ) -> Reservation:
        return self._reserve(self._client(client_id, ttl).counts, tool, limit)

    async def release_client_call(self, client_id: str, tool: str) -> None:
        entry = self._clients.get(client_id)
        if entry is not None:
            self._release(entry.counts, tool)
