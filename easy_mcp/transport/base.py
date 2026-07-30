"""Transport abstraction shared by all easy_mcp transports."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..security.auth import ClientIdentity

if TYPE_CHECKING:
    import asyncio

    from ..server import MCPServer


@dataclass(slots=True)
class ClientContext:
    """Per-connection state threaded through the dispatcher.

    ``client_id`` is the rate-limiting key: the API-key fingerprint when the
    client authenticated, otherwise a transport address such as ``ip:...``.
    """

    client_id: str
    session_id: str
    identity: ClientIdentity | None = None
    tool_calls: dict[str, int] = field(default_factory=dict)
    in_flight: dict[Any, asyncio.Task[Any]] = field(default_factory=dict)


class Transport(abc.ABC):
    """A transport moves JSON-RPC messages between clients and the server.

    Transport responsibilities, in order:

    1. resolve the client's identity from transport credentials,
    2. enforce transport-level limits (payload size, session caps),
    3. hand each decoded message to ``server.dispatch`` with a
       :class:`ClientContext`,
    4. deliver responses back to the right client.

    Everything protocol-level (validation, auth *decisions*, rate limits,
    execution) lives in the server so new transports stay thin.
    """

    def __init__(self, server: MCPServer) -> None:
        self._server = server

    @abc.abstractmethod
    def run(self) -> None:
        """Serve until stopped (blocking)."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Request a graceful shutdown."""
