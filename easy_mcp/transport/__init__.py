"""Transports: how JSON-RPC messages reach the server (SSE today; HTTP,
WebSocket and stdio are planned)."""

from .base import ClientContext, Transport
from .sse import SSETransport

__all__ = ["ClientContext", "SSETransport", "Transport"]
