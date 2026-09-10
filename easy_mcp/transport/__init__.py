"""Transports: how JSON-RPC messages reach the server.

* :class:`StreamableHTTPTransport` — the MCP endpoint (``/mcp``) for remote
  clients; it serves the legacy SSE endpoints alongside by default.
* :class:`SSETransport` — the legacy HTTP + Server-Sent Events transport.
* :class:`StdioTransport` — stdin/stdout for local clients such as Claude
  Desktop and Claude Code.

A WebSocket transport is planned.
"""

from .base import ClientContext, Transport
from .sse import SSETransport
from .stdio import StdioTransport
from .streamable_http import StreamableHTTPTransport

__all__ = [
    "ClientContext",
    "SSETransport",
    "StdioTransport",
    "StreamableHTTPTransport",
    "Transport",
]
