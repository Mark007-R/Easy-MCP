"""The store interface: where state that outlives one request is kept.

A store keeps handshake-era sessions, ``max_calls_per_session`` counts and
rate-limit windows, and carries messages between the worker processes that
share it.  :class:`~easy_mcp.store.MemoryStore`, the default, keeps them in
this process; :class:`~easy_mcp.store.RedisStore` shares them between
workers.  Running calls and open streams never leave the worker that owns
them.

The interface is public so that other stores can be written, and
provisional until 1.0: a later release may add methods to it.

A shared store never holds an API key, a token or a raw session id.  A
session is filed under its :func:`session_ref`, a digest of its id, and a
client under its :func:`client_ref`.
"""

from __future__ import annotations

import abc
import enum
import hashlib
import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Literal, Protocol

if TYPE_CHECKING:
    from ..security.auth import ClientIdentity
    from ..security.ratelimit import SlidingWindowRateLimiter

SessionKind = Literal["http", "sse"]

# The server name MCPServer uses when none is given; a namespace equal to it
# is probably shared by accident.
DEFAULT_NAMESPACE = "easy-mcp"

_NAMESPACE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
_NOT_NAMESPACE = re.compile(r"[^a-z0-9._-]")


def session_ref(session_id: str) -> str:
    """The name a session is filed under: 128 bits of a SHA-256 of its id.

    The id carries 192 random bits, so its ref can be neither reversed nor
    guessed; reading a store yields no usable session id.
    """
    return hashlib.sha256(b"easy-mcp/session\0" + session_id.encode()).hexdigest()[:32]


def client_ref(client_id: str) -> str:
    """The name a client's counts and rate-limit window are filed under.

    It keeps addresses and fingerprints out of key names.  It is not
    anonymization: an IPv4 address is cheap to find from its digest.
    """
    return hashlib.sha256(b"easy-mcp/client\0" + client_id.encode()).hexdigest()[:32]


def principal_ref(identity: ClientIdentity | None) -> str | None:
    """A digest of a token identity's whole principal: issuer, subject and client.

    ``None`` for an API key or an anonymous caller.  A session is bound to
    it as well as to the fingerprint, so no principal can use another's
    session even if their fingerprints matched.
    """
    if identity is None or identity.issuer is None:
        return None
    principal = json.dumps([identity.issuer, identity.subject, identity.client_id])
    return hashlib.sha256(b"easy-mcp/principal\0" + principal.encode()).hexdigest()[:32]


def slug_namespace(name: str) -> str:
    """A namespace made from a server name: ``"My Tools"`` becomes ``"my-tools"``.

    Lower case, every character outside ``[a-z0-9._-]`` replaced by ``-``,
    at most 64 characters, starting with a letter or digit.
    """
    slug = _NOT_NAMESPACE.sub("-", name.lower()).lstrip("-._")[:64]
    return slug or DEFAULT_NAMESPACE


def check_namespace(namespace: str) -> str:
    """Validate an explicit namespace.

    Raises:
        ValueError: It is not 1 to 64 of ``[a-z0-9._-]``, starting with a
            letter or digit.
    """
    if not isinstance(namespace, str) or _NAMESPACE.fullmatch(namespace) is None:
        raise ValueError(
            f"invalid namespace {namespace!r}: use 1 to 64 of a-z, 0-9, '.', '_' and '-', "
            "starting with a letter or digit"
        )
    return namespace


class Reservation(enum.Enum):
    """The answer to a request for one ``max_calls_per_session`` unit."""

    OK = "ok"
    LIMIT = "limit"  # max_calls_per_session reached
    GONE = "gone"  # the session ended between its lookup and the reservation


@dataclass(frozen=True, slots=True)
class SessionRecord:
    """What a store knows of one handshake-era session.

    ``ref`` is :func:`session_ref` of the id.  ``identity_fp`` is the
    fingerprint of the credential that opened the session (``None`` for an
    anonymous one) and ``principal`` the :func:`principal_ref` of a token's
    principal.  ``owner`` names the worker holding a legacy SSE stream (a
    shared store only).  ``session_id`` is the raw id, which only a store
    that stays in this process keeps; a shared store never writes it.
    ``t0`` is when the session was opened, in milliseconds since the epoch,
    by the store's clock.
    """

    ref: str
    kind: SessionKind
    client_id: str
    identity_fp: str | None
    protocol_version: str | None = None
    owner: str | None = None
    session_id: str | None = None
    principal: str | None = None
    t0: int | None = None


@dataclass(frozen=True, slots=True)
class ExpiredSession:
    """A session a store found expired and removed.

    ``client_id`` is ``None`` when the store no longer knows it, and
    ``session_id`` is kept only by a store that stays in this process.
    """

    ref: str
    client_id: str | None
    session_id: str | None = None


class AsyncRateLimiter(Protocol):
    """A rate limiter a store hands out: one unit of a client's budget per call."""

    async def acheck(self, client_id: str) -> None:
        """Record one request for *client_id*, or reject it.

        Raises:
            RateLimitError: The client is over its budget.
            StoreUnavailableError: The budget could not be checked.
        """
        ...


class Store(abc.ABC):
    """Where state that outlives one request lives.

    Methods are called from one event loop.  Every one may raise
    :class:`~easy_mcp.StoreUnavailableError` when the store cannot be
    reached; the server then refuses what needed it.  A store serves one
    :class:`~easy_mcp.MCPServer`.

    *ttl* arguments are seconds: how long a session (or a client's counts)
    lives without a request.  ``None`` means it never expires, which only a
    store that stays in this process accepts.
    """

    # Whether several worker processes share this store.  When they do,
    # sessions outlive this process and messages travel between workers.
    shared: ClassVar[bool] = False

    _server_name: str | None = None

    @property
    @abc.abstractmethod
    def worker_id(self) -> str:
        """This process's name on the store: who owns a stream, who sent a message."""

    # ---------------------------------------------------------- lifecycle

    def bind(self, server_name: str) -> None:
        """Attach the store to the server named *server_name* (``MCPServer`` calls it).

        Raises:
            ValueError: The store serves another server already.
        """
        if self._server_name is not None:
            raise ValueError("a store serves one server; create one per MCPServer")
        self._server_name = server_name

    async def start(self) -> None:
        """Connect, if there is anything to connect to.  Idempotent; also run lazily."""
        return None

    async def aclose(self) -> None:
        """Release connections and stop listening.  Idempotent."""
        return None

    def describe(self) -> str:
        """What the startup log says about the store, with every secret removed."""
        return type(self).__name__

    def warnings(self) -> list[str]:
        """Misconfigurations worth a warning at startup."""
        return []

    async def ping(self) -> bool:
        """Whether the store can be reached now.  Never raises."""
        return True

    # ----------------------------------------------------------- rate limits

    @abc.abstractmethod
    def rate_limiter(self, local: SlidingWindowRateLimiter) -> AsyncRateLimiter:
        """The limiter HTTP requests are charged to, with *local*'s budget and window.

        A store that stays in this process returns *local* itself.
        """

    # -------------------------------------------------------------- sessions

    @abc.abstractmethod
    async def create_session(
        self, record: SessionRecord, *, cap: int, ttl: float | None
    ) -> tuple[bool, list[ExpiredSession]]:
        """File a new session, unless *cap* sessions of its kind are live.

        Sessions of the kind found expired are removed first and returned.
        The new session starts held, as :meth:`acquire_session` holds one,
        until :meth:`release_session`.

        Returns:
            Whether it was created (``False``: the cap is reached), and the
            sessions found expired.

        Raises:
            ValueError: A session with this ref exists already.
        """

    @abc.abstractmethod
    async def acquire_session(
        self, kind: SessionKind, ref: str, *, ttl: float | None
    ) -> tuple[SessionRecord | None, list[ExpiredSession]]:
        """Look a session up and hold it for one request.

        A held session does not expire.  A shared store cannot know what is
        held, so it extends the session's life by *ttl* instead, and a
        worker running a long request keeps extending it
        (:meth:`refresh_sessions`).

        Returns:
            The record (``None``: unknown or expired), and the session if it
            was found expired.
        """

    @abc.abstractmethod
    async def release_session(
        self,
        kind: SessionKind,
        ref: str,
        *,
        ttl: float | None,
        protocol_version: str | None = None,
        touch: bool = True,
    ) -> None:
        """End one request's hold on a session.

        *touch* restarts its idle time (``False`` for a request that was
        refused).  *protocol_version* records the version its handshake
        negotiated.  A session that is gone is left alone.
        """

    @abc.abstractmethod
    async def refresh_sessions(
        self, kind: SessionKind, refs: Sequence[str], *, ttl: float
    ) -> set[str]:
        """Extend the life of sessions still in use here by *ttl*.

        Returns:
            The refs of those that no longer exist.
        """

    @abc.abstractmethod
    async def delete_session(self, kind: SessionKind, ref: str) -> SessionRecord | None:
        """Remove a session; returns its record, or ``None`` if it was gone."""

    # ------------------------------------------------- max_calls_per_session

    @abc.abstractmethod
    async def reserve_session_call(self, ref: str, tool: str, limit: int) -> Reservation:
        """Take one of a session's *limit* calls of *tool*."""

    @abc.abstractmethod
    async def release_session_call(self, ref: str, tool: str) -> None:
        """Give back a call :meth:`reserve_session_call` took (never below zero)."""

    @abc.abstractmethod
    async def touch_client(self, client_id: str, *, ttl: float | None) -> None:
        """Note a stateless request of *client_id* (its counts lapse after *ttl* idle)."""

    @abc.abstractmethod
    async def reserve_client_call(
        self, client_id: str, tool: str, limit: int, *, ttl: float | None
    ) -> Reservation:
        """Take one of a stateless client's *limit* calls of *tool*."""

    @abc.abstractmethod
    async def release_client_call(self, client_id: str, tool: str) -> None:
        """Give back a call :meth:`reserve_client_call` took (never below zero)."""

    # ------------------------------------------------- messages between workers

    async def publish(self, payload: str, *, to: str | None = None) -> int:
        """Send *payload* to every worker, or to the worker named *to*.

        Returns:
            How many workers received it (``0`` without a shared store).
        """
        return 0

    def subscribe(self, handler: Callable[[str], None]) -> Callable[[], None]:
        """Call *handler* on the event loop with every payload sent to this worker.

        Returns:
            A function that unsubscribes it.
        """
        return _no_op


def _no_op() -> None:
    return None


class StoreHandle(abc.ABC):
    """What one request's dispatch may consult beyond the request itself.

    The HTTP transports put one on every :class:`~easy_mcp.ClientContext`
    they build (``store_handle``): for a session's requests it reaches the
    session's state, for a stateless request the client's.  ``None``, as on
    stdio and for direct ``dispatch`` calls, keeps every count and cancel in
    the context itself.
    """

    @abc.abstractmethod
    async def reserve_call(self, tool: str, limit: int) -> Reservation:
        """Take one of the *limit* calls of *tool* this session or client may make."""

    @abc.abstractmethod
    async def release_call(self, tool: str) -> None:
        """Give back a call :meth:`reserve_call` took."""

    async def cancel_elsewhere(self, request_id: str | int) -> None:
        """Cancel *request_id* on whichever worker runs it, if it runs on another one."""
        return None
