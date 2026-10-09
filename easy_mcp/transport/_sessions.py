"""Handshake-era sessions of the HTTP transports, kept in the server's store.

Both HTTP transports open, look up and end their sessions through a
:class:`SessionManager`.  The store holds what every worker must agree on:
that a session exists, who opened it, its call counts.  What cannot leave a
process stays in a :class:`LocalSession`: the session's calls running here,
and the stream this worker holds for it.

With a shared store, a session's requests can land on any worker, and at any
endpoint of its kind, so the session managers tell each other about them
(:mod:`._bus`): a cancel for a call running elsewhere, the end of a session,
and a legacy SSE answer for the worker that holds the stream.  The managers
of one worker's endpoints are handed cancels and ends directly.  A heartbeat
keeps the sessions this worker is busy with alive in the store, and renews
the lease of the legacy SSE streams it holds.

Whoever removes a session from the store audits its close, so each close is
audited once: the worker (or endpoint) that ends it, the one that finds it
expired, or, if the store lost it (a failover, a restart), the one serving
it that finds it gone first.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import json
import time
import uuid
from collections import OrderedDict
from collections.abc import Hashable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from ..exceptions import INTERNAL_ERROR, StoreUnavailableError
from ..logging import audit
from ..protocol import SUPPORTED_PROTOCOL_VERSIONS
from ..security.auth import ClientIdentity
from ..store.base import (
    ExpiredSession,
    Reservation,
    SessionKind,
    SessionRecord,
    Store,
    StoreHandle,
    principal_ref,
    session_ref,
)
from . import _bus
from ._outbox import coalesce_key
from .base import ClientContext

if TYPE_CHECKING:
    from ..server import MCPServer

# A legacy SSE session in a shared store lives on a lease that the worker
# holding its stream renews; if that worker dies, the session ends with the
# lease.  Renewed on the stream's keep-alive tick.
SSE_LEASE_SECONDS = 60.0
SSE_HEARTBEAT_SECONDS = 15.0

# A worker extends the sessions it runs requests for this often at most
# (and three times per idle timeout), so a long call never lets its session
# expire.
HTTP_HEARTBEAT_MAX_SECONDS = 30.0

# Sessions ended here or announced ended, remembered so that a request
# already on its way in is refused rather than served.
ENDED_MEMORY_SECONDS = 60.0
ENDED_MEMORY_MAX = 4096

# Sessions per refresh round trip.
REFRESH_BATCH = 256

# Put in a LocalSession's stream queue to close the stream.
CLOSE_STREAM = object()


class Rejection(enum.Enum):
    """Why a request names no session it may use."""

    MISSING_HEADER = "missing_header"  # no session id at all
    NOT_FOUND = "not_found"  # unknown, expired or ended
    FORBIDDEN = "forbidden"  # opened with another credential
    BAD_VERSION = "bad_version"  # an unsupported MCP-Protocol-Version header
    UNAVAILABLE = "unavailable"  # the store cannot be reached


class _InFlight(dict[Any, "asyncio.Task[Any]"]):
    """A session's calls on this worker.  Once ended, a call added later is cancelled at once."""

    __slots__ = ("ended",)

    def __init__(self) -> None:
        super().__init__()
        self.ended = False

    def __setitem__(self, key: Any, task: asyncio.Task[Any]) -> None:
        super().__setitem__(key, task)
        if self.ended:
            task.cancel()

    def end(self) -> None:
        self.ended = True
        for task in list(self.values()):
            task.cancel()


@dataclass(slots=True, eq=False)
class LocalSession:
    """A session as this worker serves it.

    ``context`` is the session's own :class:`ClientContext`; each request
    runs with it, or a copy holding the request's identity that shares its
    calls in flight (:meth:`SessionManager.context`).  ``active`` counts the
    requests of the session this worker is serving.  ``opened`` is set once
    its handshake has succeeded here.  ``owned`` marks the session whose
    stream this worker holds, and ``stream`` is the queue of what that
    stream sends; the server's own messages go there through :meth:`push`.
    ``notify_stream`` is a Streamable HTTP session's
    ``GET /mcp`` stream, when this worker holds it; it holds one of
    ``active`` while open, so the session neither expires nor is forgotten
    here meanwhile.
    """

    ref: str
    session_id: str
    record: SessionRecord
    context: ClientContext
    active: int = 0
    opened: bool = False
    owned: bool = False
    ended: bool = False
    # Its messages being dispatched, registered before their dispatch
    # starts, so an end that comes first still stops them.
    dispatches: set[asyncio.Task[Any]] = field(default_factory=set)
    # Background work for it: legacy SSE dispatches and relays.
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    stream: asyncio.Queue[Any] | None = None
    # The change notifications waiting in stream, by what they coalesce by.
    waiting: set[Hashable] = field(default_factory=set)
    # The transport's own object, with a close(reason) method.
    notify_stream: Any = None

    @property
    def in_flight(self) -> _InFlight:
        return cast(_InFlight, self.context.in_flight)

    def push(self, message: dict[str, Any]) -> None:
        """Queue a message of the server's own on the stream held here.

        A change notification already waiting there (the same list, for the
        same subscription) is not queued again: each only tells the client
        to fetch the list, so a client that stops reading cannot grow a
        backlog of them.
        """
        assert self.stream is not None
        key = coalesce_key(message)
        if key is not None:
            if key in self.waiting:
                return
            self.waiting.add(key)
        self.stream.put_nowait(message)

    def taken(self, item: Any) -> None:
        """The stream took *item* from its queue, to write it."""
        if isinstance(item, dict):
            key = coalesce_key(item)
            if key is not None:
                self.waiting.discard(key)

    def stop_work(self) -> None:
        """Cancel every call of the session on this worker, those yet to start included."""
        self.in_flight.end()
        for task in [*self.dispatches, *self.tasks]:
            task.cancel()


class _SessionHandle(StoreHandle):
    """A session's call counts and cancels, for its requests' dispatch."""

    __slots__ = ("_manager", "_local")

    def __init__(self, manager: SessionManager, local: LocalSession) -> None:
        self._manager = manager
        self._local = local

    async def reserve_call(self, tool: str, limit: int) -> Reservation:
        return await self._manager.store.reserve_session_call(self._local.ref, tool, limit)

    async def release_call(self, tool: str) -> None:
        await self._manager.store.release_session_call(self._local.ref, tool)

    async def cancel_elsewhere(self, request_id: str | int) -> None:
        await self._manager.cancel_elsewhere(self._local, request_id)


class ClientHandle(StoreHandle):
    """A stateless client's call counts: the stand-in for a session.

    A stateless request is cancelled by closing its connection, which is on
    this worker, so nothing is ever cancelled elsewhere.
    """

    __slots__ = ("_store", "_client_id", "_ttl")

    def __init__(self, store: Store, client_id: str, ttl: float | None) -> None:
        self._store = store
        self._client_id = client_id
        self._ttl = ttl

    async def reserve_call(self, tool: str, limit: int) -> Reservation:
        return await self._store.reserve_client_call(self._client_id, tool, limit, ttl=self._ttl)

    async def release_call(self, tool: str) -> None:
        await self._store.release_client_call(self._client_id, tool)


class SessionManager:
    """Open, find and end the sessions of one HTTP transport, in the server's store.

    Args:
        server: The server whose store keeps the sessions.
        kind: ``"http"`` (Streamable HTTP) or ``"sse"`` (legacy SSE).
        ttl: Idle seconds after which a session expires; ``None`` never.
            Legacy SSE sessions pass ``None``: they live as long as their
            stream (in a shared store, on a lease its owner renews).
        transport: The transport's name in audit events.
    """

    def __init__(
        self, server: MCPServer, kind: SessionKind, *, ttl: float | None, transport: str
    ) -> None:
        self._server = server
        self._kind: SessionKind = kind
        self._ttl = ttl
        self._transport = transport
        self._local: dict[str, LocalSession] = {}
        # Session ids of lookups under way here, by ref: an end announced
        # meanwhile can be checked against them.
        self._pending: dict[str, tuple[str, int]] = {}
        self._ended: OrderedDict[str, float] = OrderedDict()
        # Sessions ended here that the store could not be told about (an
        # outage), with why they ended: a heartbeat removes them later.
        self._unremoved: dict[str, tuple[LocalSession, str | None]] = {}
        self._heartbeat: asyncio.Task[None] | None = None
        self._background: set[asyncio.Task[Any]] = set()
        server._session_managers.add(self)
        if self.store.shared:
            self.store.subscribe(self._on_bus)

    @property
    def store(self) -> Store:
        return self._server.store

    @property
    def _shared_sse(self) -> bool:
        return self._kind == "sse" and self.store.shared

    @property
    def _create_ttl(self) -> float | None:
        return SSE_LEASE_SECONDS if self._shared_sse else self._ttl

    @property
    def _touch_ttl(self) -> float | None:
        # Only its owner extends an SSE lease: requests that reach other
        # workers must not keep a dead owner's session alive.
        return None if self._kind == "sse" else self._ttl

    def local_sessions(self) -> list[LocalSession]:
        """The sessions this worker holds state for."""
        return list(self._local.values())

    def context(self, local: LocalSession, identity: ClientIdentity | None) -> ClientContext:
        """The context one request of *local* runs with: the session's, with its identity."""
        return self._server._request_context(local.context, identity)

    # ------------------------------------------------------------- opening

    async def open(
        self,
        session_id: str,
        *,
        client_id: str,
        identity: ClientIdentity | None,
        owned: bool = False,
    ) -> LocalSession | None:
        """File a new session, held by its opener until :meth:`finish`.

        *owned* marks a session whose stream this worker holds.

        Returns:
            The session, or ``None`` when ``max_sessions`` are open.

        Raises:
            StoreUnavailableError: The store cannot be reached.
            ValueError: The session id is in use already.
        """
        store = self.store
        record = SessionRecord(
            ref=session_ref(session_id),
            kind=self._kind,
            client_id=client_id,
            identity_fp=identity.fingerprint if identity is not None else None,
            owner=store.worker_id if owned and store.shared else None,
            # A shared store must never see a raw session id.
            session_id=None if store.shared else session_id,
            principal=principal_ref(identity),
            t0=int(time.time() * 1000),
        )
        created, expired = await store.create_session(
            record, cap=self._server.max_sessions, ttl=self._create_ttl
        )
        self._expire(expired)
        if not created:
            return None
        local = self._attach(record, session_id, identity)
        local.active = 1
        local.owned = owned
        if owned:
            local.stream = asyncio.Queue()
        self._start_heartbeat()
        return local

    def opened(self, local: LocalSession) -> None:
        """Mark *local*'s handshake as done, and audit the session's opening."""
        local.opened = True
        self._server._session_event(
            True,
            kind=self._kind,
            transport=self._transport,
            session_id=local.session_id,
            ref=local.ref,
            client_id=local.record.client_id,
            protocol_version=local.context.protocol_version,
            t0=local.record.t0,
        )

    def _attach(
        self, record: SessionRecord, session_id: str, identity: ClientIdentity | None
    ) -> LocalSession:
        context = ClientContext(
            client_id=record.client_id,
            session_id=session_id,
            identity=identity,
            in_flight=_InFlight(),
            protocol_version=record.protocol_version,
        )
        local = LocalSession(record.ref, session_id, record, context)
        context.store_handle = _SessionHandle(self, local)
        self._local[record.ref] = local
        return local

    # ------------------------------------------------------------- lookups

    async def acquire(
        self,
        session_id: str | None,
        *,
        identity: ClientIdentity | None = None,
        extend: bool = True,
    ) -> LocalSession | Rejection:
        """The session *session_id* names, held for one request until :meth:`finish`.

        Neither the credential nor the version header is checked
        (:meth:`resolve` does both).  A shared store extends the session's
        life only for a request that may be served: with *extend*, and if
        the session is bound to *identity*.
        """
        if not session_id:
            return Rejection.MISSING_HEADER
        ref = session_ref(session_id)
        if self._recently_ended(ref):
            return Rejection.NOT_FOUND
        local = self._local.get(ref)
        if local is not None and local.owned:
            # Its stream is here: this worker knows all there is to know.
            local.active += 1
            return local
        if local is None and not self.store.shared:
            # Every session of this endpoint in this process is held here
            # until it ends; another endpoint's is none of its business.
            return Rejection.NOT_FOUND
        self._pending_add(ref, session_id)
        binding = (identity.fingerprint if identity is not None else None, principal_ref(identity))
        try:
            record, expired = await self.store.acquire_session(
                self._kind, ref, ttl=self._touch_ttl if extend else None, binding=binding
            )
        except StoreUnavailableError:
            return Rejection.UNAVAILABLE
        finally:
            self._pending_remove(ref)
        self._expire(expired)
        if record is None:
            local = self._local.get(ref)
            if local is not None and not self.store.shared:
                self._end_here(local)  # gone from a store that told no endpoint
            return Rejection.NOT_FOUND
        if self._recently_ended(ref):
            # Ended while the store was asked: the end has been announced.
            await self._release(ref, touch=False)
            return Rejection.NOT_FOUND
        local = self._local.get(ref)
        if local is None:
            local = self._attach(record, session_id, None)
        else:
            local.record = record
        if local.context.protocol_version is None and record.protocol_version is not None:
            local.context.protocol_version = record.protocol_version
        local.active += 1
        self._start_heartbeat()
        return local

    def binds(self, local: LocalSession, identity: ClientIdentity | None) -> bool:
        """Whether *identity* is the credential *local* was opened with.

        The fingerprint, and for a token the whole principal (issuer, subject
        and client), so a refreshed or broader token keeps the session.
        """
        presented = identity.fingerprint if identity is not None else None
        record = local.record
        return presented == record.identity_fp and principal_ref(identity) == record.principal

    def credential_mismatch(self, local: LocalSession) -> None:
        """Audit a request that named *local* with another credential."""
        audit(
            "session_credential_mismatch",
            session_id=local.session_id,
            session_ref=local.ref,
            client_id=local.record.client_id,
            transport=self._transport,
        )

    async def resolve(
        self,
        session_id: str | None,
        identity: ClientIdentity | None,
        *,
        version_header: str | None,
    ) -> LocalSession | Rejection:
        """The session a request names, held until :meth:`finish`, or why it is refused.

        In order: a missing id, an unknown session (or the store out of
        reach), another credential than the one it was opened with, an
        unsupported ``MCP-Protocol-Version`` header.  A refused request
        does not keep the session alive.
        """
        bad_version = version_header is not None and (
            version_header not in SUPPORTED_PROTOCOL_VERSIONS
        )
        local = await self.acquire(session_id, identity=identity, extend=not bad_version)
        if isinstance(local, Rejection):
            return local
        if not self.binds(local, identity):
            self.credential_mismatch(local)
            await self.finish(local, touch=False)
            return Rejection.FORBIDDEN
        if bad_version:
            await self.finish(local, touch=False)
            return Rejection.BAD_VERSION
        return local

    async def finish(
        self, local: LocalSession, *, protocol_version: str | None = None, touch: bool = True
    ) -> None:
        """End one request's hold on *local*; its idle time starts again.

        *protocol_version* is recorded for the session (its handshake
        negotiated it).  Never raises: the request has been answered.
        """
        local.active -= 1
        if local.ended:
            return
        if not local.owned:  # an owned session was never acquired from the store
            await self._release(local.ref, protocol_version=protocol_version, touch=touch)
        if (
            self.store.shared
            and local.active <= 0
            and not local.owned
            and self._local.get(local.ref) is local
        ):
            # The store has it all; nothing is left here to keep.
            del self._local[local.ref]

    async def save_baselines(self, local: LocalSession, baselines: Mapping[str, str]) -> None:
        """Record what *local*'s client was last told its lists hold, here and in the store.

        Never raises: a store that cannot take them leaves the next stream
        to announce only the changes made while it is open.
        """
        if local.ended:
            return
        local.record = dataclasses.replace(
            local.record, baselines=tuple(sorted(baselines.items()))
        )
        try:
            await self.store.save_baselines(self._kind, local.ref, baselines)
        except StoreUnavailableError:
            pass  # the store logged the outage
        except Exception:
            self._server._logger.error("could not record a session's lists", exc_info=True)

    def start_notifications(self, local: LocalSession, identity: ClientIdentity | None) -> None:
        """Announce list changes on *local*'s legacy SSE stream from now on.

        Called once its ``initialize`` result is queued on the stream held
        here, whichever worker dispatched it: nothing can then reach the
        stream before that result.
        """
        if local.ended or not local.owned or local.stream is None:
            return
        self._server._watch_session(
            local.session_id,
            push=local.push,
            identity=identity,
            client_id=local.record.client_id,
            multiplexed=True,
        )

    async def record_version(self, local: LocalSession, version: str) -> None:
        """Record in a shared store the version *local*'s handshake negotiated here.

        For a session whose stream this worker holds: the workers its
        messages reach read it there.
        """
        if self.store.shared and not local.ended:
            await self._release(local.ref, protocol_version=version, touch=False)

    async def _release(
        self, ref: str, *, protocol_version: str | None = None, touch: bool = True
    ) -> None:
        try:
            await self.store.release_session(
                self._kind,
                ref,
                ttl=self._touch_ttl,
                protocol_version=protocol_version,
                touch=touch,
            )
        except StoreUnavailableError:
            pass  # it lapses on its own; the store logged the outage
        except Exception:
            # Never raised to the caller: the request has been answered.
            self._server._logger.error("could not release a session in the store", exc_info=True)

    # -------------------------------------------------------------- ending

    async def end(
        self,
        local: LocalSession,
        *,
        reason: str | None,
        strict: bool = False,
        detach: bool = False,
    ) -> None:
        """End *local* everywhere: in the store, on every worker, and here.

        Its calls running here are cancelled, those yet to start included.
        *reason* is audited with ``session_close`` when this removes the
        session from the store, or finds that the store lost it; whatever
        removed it first audited it instead (``None``: nothing to audit, as
        for a handshake that failed).

        Args:
            strict: Remove it from the store first, and raise if that fails
                (a ``DELETE`` must not answer 204 for a session still there).
            detach: Do the store's part in the background: the caller may be
                cancelled at any await (a stream's own end).

        Raises:
            StoreUnavailableError: With *strict*, the store cannot be reached.
        """
        if local.ended:
            return
        if strict:
            removed = await self.store.delete_session(self._kind, local.ref)
            ended = local.ended  # meanwhile, announced by another worker
            if not ended:
                self._end_here(local, reason)
            if removed and reason is not None:
                self._closed(local.session_id, local.ref, local.record, reason)
            if not ended and self.store.shared:
                await self._announce_end(local)
            return
        self._end_here(local, reason)
        work = self._forget(local, reason)
        if detach and self.store.shared:
            self._spawn(work)
        else:
            await work

    async def _forget(self, local: LocalSession, reason: str | None) -> None:
        """The store's part of ending *local*: remove it, audit it, tell the other workers.

        A store out of reach is told on a later heartbeat (:meth:`_refresh`).
        """
        try:
            removed = await self.store.delete_session(self._kind, local.ref)
        except StoreUnavailableError:
            self._unremoved[local.ref] = (local, reason)
            removed = False
        except Exception:
            self._server._logger.error("could not remove a session from the store", exc_info=True)
            removed = True  # its close is audited all the same
        if removed and reason is not None:
            self._closed(local.session_id, local.ref, local.record, reason)
        if self.store.shared:
            await self._announce_end(local)

    async def _remove_unremoved(self) -> bool:
        """Remove the sessions ended here while the store was out of reach.

        Returns:
            Whether the store could be reached.
        """
        for ref, (local, reason) in list(self._unremoved.items()):
            try:
                removed = await self.store.delete_session(self._kind, ref)
            except StoreUnavailableError:
                return False
            except Exception:
                self._server._logger.error(
                    "could not remove a session from the store", exc_info=True
                )
                removed = True  # not tried again; its close is audited all the same
            if self._unremoved.pop(ref, None) is None:
                continue  # found expired meanwhile, and audited then (_expire)
            if removed and reason is not None:
                self._closed(local.session_id, ref, local.record, reason)
            if self.store.shared:
                await self._announce_end(local)
        return True

    async def _announce_end(self, local: LocalSession) -> None:
        payload = _bus.seal(
            "end",
            self._kind,
            local.ref,
            local.record.identity_fp,
            local.session_id,
            self.store.worker_id,
        )
        await self._broadcast(payload)

    def _end_here(self, local: LocalSession, reason: str | None = None) -> None:
        """Forget *local* on this worker and stop its work (no store call).

        Its subscriptions end first: a listen stream on its legacy SSE
        stream gets its final frames before the stream closes, and its
        ``GET /mcp`` stream ends.
        """
        local.ended = True
        if self._local.get(local.ref) is local:
            del self._local[local.ref]
        self._remember_ended(local.ref)
        ending = "shutdown" if reason == "shutdown" else "session_closed"
        self._server.close_subscriptions(local.context, reason=ending)
        if local.notify_stream is not None:
            local.notify_stream.close(ending)
        local.stop_work()
        if local.stream is not None:
            local.stream.put_nowait(CLOSE_STREAM)

    def _closed(
        self, session_id: str | None, ref: str, record: SessionRecord | None, reason: str
    ) -> None:
        self._server._session_event(
            False,
            kind=self._kind,
            transport=self._transport,
            session_id=session_id,
            ref=ref,
            client_id=record.client_id if record is not None else None,
            t0=record.t0 if record is not None else None,
            reason=reason,
        )

    def _expire(self, expired: Iterable[ExpiredSession]) -> None:
        """End the sessions the store found expired and removed, or lost, and audit their close.

        A session ends on whichever endpoint of this server holds it, which
        knows more of it than the store may: its id, its client and when it
        opened, or why it ended there before the store could be told.
        """
        expired = list(expired)
        if not expired:
            return
        managers = [self, *self._siblings()]
        for gone in expired:
            # An SSE session in a shared store expires when its owner
            # stopped renewing its lease, and its lease is lost with it when
            # the store loses it.
            reason: str | None
            if self._kind == "sse":
                reason = "lease_lost"
            else:
                reason = "store_lost" if gone.lost else "idle_timeout"
            transport = self._transport
            session_id, client_id, t0 = gone.session_id, gone.client_id, gone.t0
            for manager in managers:
                unremoved = manager._unremoved.pop(gone.ref, None)
                local = manager._local.get(gone.ref)
                if unremoved is not None:
                    local, reason = unremoved
                elif local is not None:
                    manager._end_here(local)
                else:
                    continue
                transport = manager._transport
                session_id, client_id, t0 = (
                    local.session_id,
                    local.record.client_id,
                    local.record.t0,
                )
                break
            if reason is not None:
                self._server._session_event(
                    False,
                    kind=self._kind,
                    transport=transport,
                    session_id=session_id,
                    ref=gone.ref,
                    client_id=client_id,
                    t0=t0,
                    reason=reason,
                )

    async def shutdown(self) -> None:
        """This worker stops serving: end its sessions, or leave them to the others.

        In a store that stays in this process every session ends (audited
        with ``reason="shutdown"``), except a handshake still running, which
        ends with its own dispatch.  In a shared store the sessions live on
        and other workers serve them: only the calls running here are
        cancelled, and the streams held here end.
        """
        heartbeat, self._heartbeat = self._heartbeat, None
        if heartbeat is not None:
            heartbeat.cancel()
        for local in self.local_sessions():
            if not self.store.shared:
                if local.opened:
                    await self.end(local, reason="shutdown")
            elif local.owned:
                await self.end(local, reason="shutdown")
            else:
                # No ended memory: served again, this worker takes it up anew.
                if self._local.get(local.ref) is local:
                    del self._local[local.ref]
                local.stop_work()
        if self._background:
            await asyncio.wait(set(self._background), timeout=2.0)

    # --------------------------------------------------- the ended memory

    def _remember_ended(self, ref: str) -> None:
        now = time.monotonic()
        self._ended[ref] = now + ENDED_MEMORY_SECONDS
        self._ended.move_to_end(ref)
        while self._ended:
            oldest, expiry = next(iter(self._ended.items()))
            if expiry > now and len(self._ended) <= ENDED_MEMORY_MAX:
                break
            del self._ended[oldest]

    def _recently_ended(self, ref: str) -> bool:
        expiry = self._ended.get(ref)
        if expiry is None:
            return False
        if expiry <= time.monotonic():
            del self._ended[ref]
            return False
        return True

    def _pending_add(self, ref: str, session_id: str) -> None:
        _, count = self._pending.get(ref, (session_id, 0))
        self._pending[ref] = (session_id, count + 1)

    def _pending_remove(self, ref: str) -> None:
        session_id, count = self._pending[ref]
        if count <= 1:
            del self._pending[ref]
        else:
            self._pending[ref] = (session_id, count - 1)

    # ------------------------------------------- messages between workers

    async def _publish(self, payload: str, *, to: str | None = None) -> int:
        try:
            return await self.store.publish(payload, to=to)
        except StoreUnavailableError:
            return 0
        except Exception:
            self._server._logger.error("could not publish to the store", exc_info=True)
            return 0

    def _siblings(self) -> list[SessionManager]:
        """The managers of this server's other endpoints of the same kind, in this process."""
        return [
            manager
            for manager in self._server._session_managers
            if manager is not self and manager._kind == self._kind
        ]

    async def _broadcast(self, payload: str) -> None:
        """Send *payload* to every other endpoint of this kind, on every worker.

        This worker's other endpoints share its id, so they ignore its
        messages on the bus as their own: they are handed it here instead.
        """
        for sibling in self._siblings():
            sibling._on_bus(payload, sibling=True)
        await self._publish(payload)

    async def cancel_elsewhere(self, local: LocalSession, request_id: object) -> None:
        """Ask whichever worker or endpoint runs *request_id* of *local* to cancel it.

        Only with a shared store, and only for an id that can travel
        (:func:`._bus.relayable_id`); any other id is an unknown one.
        """
        if not self.store.shared or not _bus.relayable_id(request_id):
            return
        payload = _bus.seal(
            "cancel",
            self._kind,
            local.ref,
            local.record.identity_fp,
            local.session_id,
            self.store.worker_id,
            rid=request_id,
        )
        await self._broadcast(payload)

    async def relay(
        self, local: LocalSession, message: dict[str, Any], *, protocol_version: str | None
    ) -> None:
        """Send *message*, an answer for *local*'s legacy SSE stream, to the worker holding it.

        It travels as the stream's worker would write an answer of its own,
        ``str()`` standing in for anything JSON has no type for.  An answer
        larger than the relay cap, or that no JSON can carry, is replaced by
        a ``-32603`` error, so the client still learns the request failed.
        """
        try:
            # Without sort_keys, as the stream writes it: keys of mixed types
            # are fine there.  canonical() alone would refuse a UUID.
            message = json.loads(json.dumps(message, ensure_ascii=False, default=str))
        except (TypeError, ValueError):
            failure: str | None = "unserializable"
        else:
            too_large = len(_bus.canonical(message)) > _bus.RELAY_MAX_BYTES
            failure = "too_large" if too_large else None
        if failure is not None:
            error_id = uuid.uuid4().hex[:12]
            self._server._logger.error(
                "could not relay an answer to the worker holding the stream (%s) error_id=%s",
                failure,
                error_id,
            )
            audit("sse_relay_failed", session_ref=local.ref, reason=failure, error_id=error_id)
            message = {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {
                    "code": INTERNAL_ERROR,
                    "message": f"Internal server error (error_id={error_id})",
                },
            }
        fields: dict[str, Any] = {"msg": message}
        if protocol_version is not None:
            fields["ver"] = protocol_version
        payload = _bus.seal(
            "deliver",
            self._kind,
            local.ref,
            local.record.identity_fp,
            local.session_id,
            self.store.worker_id,
            **fields,
        )
        if not await self._publish(payload, to=local.record.owner):
            audit("sse_relay_failed", session_ref=local.ref, reason="owner_unreachable")

    def _on_bus(self, payload: str, *, sibling: bool = False) -> None:
        """Act on a message from another worker, or (*sibling*) another endpoint of this one.

        Never raises.
        """
        try:
            self._handle_bus(payload, sibling)
        except Exception:
            self._server._logger.error("could not handle a message from the store", exc_info=True)

    def _handle_bus(self, payload: str, sibling: bool = False) -> None:
        envelope = _bus.peek(payload)
        if envelope is None or envelope.kind != self._kind:
            return
        if not sibling and envelope.op != "deliver" and envelope.src == self.store.worker_id:
            return  # our own broadcast: this worker's endpoints were handed it (_broadcast)
        local = self._local.get(envelope.ref)
        if local is not None:
            session_id = local.session_id
        elif envelope.op == "end" and envelope.ref in self._pending:
            # A lookup of it is under way here: the end must not be missed.
            session_id = self._pending[envelope.ref][0]
        else:
            return  # nothing of it here, so nothing to act on
        if not _bus.verify(envelope, session_id):
            self._rejected(envelope, "mac")
            return
        if local is None:
            self._remember_ended(envelope.ref)
            return
        if envelope.fp != (local.record.identity_fp or ""):
            self._rejected(envelope, "identity")
            return
        if envelope.op == "cancel":
            request_id = envelope.body["rid"]
            # A listen stream on the stream held here ends silently, at once.
            self._server._cancel_subscription(local.context, request_id)
            task = local.in_flight.get(request_id)
            if task is not None:
                task.cancel()
        elif envelope.op == "end":
            self._end_here(local)
        elif local.owned and local.stream is not None:  # deliver, to the stream's owner
            version = envelope.body.get("ver")
            message = envelope.body["msg"]
            local.stream.put_nowait(message)
            if isinstance(version, str) and version in SUPPORTED_PROTOCOL_VERSIONS:
                # The answer to a successful initialize, dispatched on another
                # worker: its list changes are announced from here on.
                local.context.protocol_version = version
                self.start_notifications(local, local.context.identity)

    @staticmethod
    def _rejected(envelope: _bus.Envelope, reason: str) -> None:
        audit(
            "bus_message_rejected",
            op=envelope.op,
            session_ref=envelope.ref,
            src=envelope.src,
            reason=reason,
        )

    # ----------------------------------------------------------- heartbeat

    def _start_heartbeat(self) -> None:
        """Keep the sessions this worker is busy with alive (shared stores only)."""
        if not self.store.shared:
            return
        loop = asyncio.get_running_loop()
        task = self._heartbeat
        if task is not None and not task.done() and task.get_loop() is loop:
            return
        self._heartbeat = loop.create_task(self._beat())

    async def _beat(self) -> None:
        if self._kind == "sse":
            interval = SSE_HEARTBEAT_SECONDS
        else:
            ttl = self._ttl if self._ttl is not None else HTTP_HEARTBEAT_MAX_SECONDS * 3
            interval = min(ttl / 3, HTTP_HEARTBEAT_MAX_SECONDS)
        while True:
            await asyncio.sleep(interval)
            try:
                await self._refresh()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._server._logger.error("session heartbeat failed", exc_info=True)

    async def _refresh(self) -> None:
        if not await self._remove_unremoved():
            return  # tried again on the next beat
        if self._kind == "sse":
            refs = [ref for ref, local in self._local.items() if local.owned]
            ttl = SSE_LEASE_SECONDS
        else:
            refs = [ref for ref, local in self._local.items() if local.active > 0]
            ttl = self._ttl if self._ttl is not None else HTTP_HEARTBEAT_MAX_SECONDS * 3
        for start in range(0, len(refs), REFRESH_BATCH):
            try:
                gone, expired = await self.store.refresh_sessions(
                    self._kind, refs[start : start + REFRESH_BATCH], ttl=ttl
                )
            except StoreUnavailableError:
                return  # tried again on the next beat
            ending: list[LocalSession] = []
            for ref in gone:
                local = self._local.get(ref)
                if local is not None and not local.ended:
                    ending.append(local)
            # The expired ones this refresh removed from the store, and the
            # ones it found lost from it, are audited here; whoever removed
            # any other audited it.
            self._expire(expired)
            for local in ending:
                if not local.ended:
                    self._end_here(local)
                if local.owned:
                    # Its lease ran out (a store outage longer than the lease),
                    # or its record was removed or lost: the client must open
                    # a new session.
                    await self._announce_end(local)

    def _spawn(self, work: Any) -> None:
        task = asyncio.ensure_future(work)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
