"""Shared test helpers and fixtures."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
import uvicorn

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


@pytest.fixture
def live_server() -> Iterator[Callable[[Any], str]]:
    """Serve an MCPServer (or a built ASGI app) on an ephemeral port in a
    background thread; returns the base URL.  Servers stop after the test."""
    running: list[tuple[uvicorn.Server, threading.Thread]] = []

    def start(target: Any) -> str:
        app = target.build_app() if isinstance(target, MCPServer) else target
        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
        uv = uvicorn.Server(config)
        thread = threading.Thread(target=uv.run, daemon=True)
        thread.start()
        deadline = time.time() + 10
        while not uv.started:
            if time.time() > deadline:
                raise RuntimeError("uvicorn failed to start within 10s")
            time.sleep(0.01)
        running.append((uv, thread))
        port = uv.servers[0].sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    yield start

    for uv, thread in running:
        uv.should_exit = True
        thread.join(timeout=5)
