"""Transports: how JSON-RPC messages reach the server.

* :class:`SSETransport` — HTTP + Server-Sent Events for remote clients.
* :class:`StdioTransport` — stdin/stdout for local clients such as Claude
  Desktop and Claude Code.

Streamable HTTP and WebSocket transports are planned.
"""

from .base import ClientContext, Transport
from .sse import SSETransport
from .stdio import StdioTransport

__all__ = ["ClientContext", "SSETransport", "StdioTransport", "Transport"]
