"""Shared test helpers and fixtures."""

from __future__ import annotations

from typing import Any

import pytest

from easy_mcp import MCPServer
from easy_mcp.security.auth import ClientIdentity
from easy_mcp.transport.base import ClientContext


def make_context(
    identity: ClientIdentity | None = None,
    client_id: str = "ip:test",
    session_id: str = "test-session",
) -> ClientContext:
    """A fresh ClientContext, as a transport would build one."""
    return ClientContext(client_id=client_id, session_id=session_id, identity=identity)


def rpc(method: str, params: Any | None = None, msg_id: Any = 1) -> dict[str, Any]:
    """Build a JSON-RPC request message."""
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": msg_id, "method": method}
    if params is not None:
        message["params"] = params
    return message


def notification(method: str, params: Any | None = None) -> dict[str, Any]:
    """Build a JSON-RPC notification (no id)."""
    message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        message["params"] = params
    return message


@pytest.fixture
def server() -> MCPServer:
    """A bare server with rate limiting disabled for deterministic tests."""
    return MCPServer(port=0, rate_limit_per_minute=None)
