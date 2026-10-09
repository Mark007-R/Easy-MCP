"""Transport abstraction shared by all easy_mcp transports."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..security.auth import ClientIdentity

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable

    from ..server import MCPServer
    from ..store.base import StoreHandle


# Compared and hashed by identity (eq=False), and weakly referable: the
# server keeps change-notification state per context without keeping a
# context a transport has forgotten alive.
@dataclass(slots=True, weakref_slot=True, eq=False)
class ClientContext:
    """Per-connection state threaded through the dispatcher.

    ``client_id`` is the rate-limiting key: the API-key fingerprint or the
    OAuth principal's fingerprint when the client authenticated, otherwise a
    transport address such as ``ip:...``.  In a session, ``identity`` is the
    one the current request's own credential resolved to (see
    ``MCPServer._request_context``); for an API key that is always the
    session's.

    ``protocol_version`` is the version negotiated by ``initialize`` on this
    connection or session, ``None`` before it; stateless requests carry
    their own.

    ``store_handle`` reaches the session's (or, for a stateless request,
    the client's) state in the server's store: its ``max_calls_per_session``
    counts, and the workers its calls may run on.  The HTTP transports set
    it; ``None`` (stdio, a direct ``dispatch``) keeps everything in this
    context, as in 0.3.1: ``tool_calls`` counts the calls, a cancel reaches
    ``in_flight`` only, and the rate limit is the server's in-process one.

    ``push`` delivers a server-initiated message (a list-change
    notification, a resource update, or a frame of a ``subscriptions/listen``
    stream) on this context's channel.  The server calls it on the event loop only; it must
    not block, and raises once the channel can take no more messages.  The
    contexts of one channel share the same ``push`` (it is how the server
    tells channels apart), and ``session_id`` names the session the channel
    carries, uniquely.  ``None``: this channel cannot carry server-initiated
    messages, so a well-formed ``subscriptions/listen`` is unknown on it
    (``-32601``) and it is never told that a list changed; nor is
    ``resources/subscribe`` served, unless a ``store_handle`` keeps the
    session (whose ``GET /mcp`` stream then delivers).  With ``push``
    set and no ``store_handle``, a successful ``initialize`` starts the
    session's list-change notifications as ``dispatch`` returns its result,
    so send that result before awaiting anything else.

    ``multiplexed`` is true when every subscription of the context shares one
    channel (stdio, the legacy SSE stream): a subscription the server ends
    is then also announced with ``notifications/cancelled``.  A transport
    calls :meth:`MCPServer.close_subscriptions
    <easy_mcp.MCPServer.close_subscriptions>` when the channel ends, and
    again once the requests it was still running have finished: an
    ``initialize`` answered meanwhile starts the session's notifications
    anew.

    New fields are only ever appended, with defaults, so positional
    construction keeps working.
    """

    client_id: str
    session_id: str
    identity: ClientIdentity | None = None
    tool_calls: dict[str, int] = field(default_factory=dict)
    in_flight: dict[Any, asyncio.Task[Any]] = field(default_factory=dict)
    protocol_version: str | None = None
    store_handle: StoreHandle | None = None
    push: Callable[[dict[str, Any]], None] | None = None
    multiplexed: bool = False


class Transport(abc.ABC):
    """A transport moves JSON-RPC messages between clients and the server.

    Transport responsibilities, in order:

    1. resolve the client's identity from transport credentials,
    2. enforce transport-level limits (payload size, session caps),
    3. hand each decoded message to ``server.dispatch`` with a
       :class:`ClientContext`,
    4. deliver responses back to the right client, and server-initiated
       messages through ``ClientContext.push``; call
       ``server.close_subscriptions(context)`` when a channel ends.

    Everything protocol-level (validation, auth *decisions*, rate limits,
    execution) lives in the server so new transports stay thin.
    """

    def __init__(self, server: MCPServer) -> None:
        self._server = server

    def describe(self) -> str:
        """Short human-readable description for the startup log."""
        return type(self).__name__

    @abc.abstractmethod
    def run(self) -> None:
        """Serve until stopped (blocking)."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Request a graceful shutdown."""
