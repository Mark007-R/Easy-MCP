"""Interop of list-change notifications with the official MCP Python SDK client.

Skipped unless ``EASY_MCP_LIVE_SDK_CLIENT=1`` is set and the ``mcp`` package
(2.3 or later) is installed: install it next to easy_mcp in a virtualenv of
its own, ``pip install "mcp>=2.3" -e .``, then run this file.

The SDK is driven in each mode it offers: ``auto`` (it probes
``server/discover`` and speaks 2026-07-28 here), pinned to ``2026-07-28``,
and ``legacy`` (the ``initialize`` handshake).  Stateless clients hear about
changes on a ``subscriptions/listen`` stream; legacy ones on their session's
``GET /mcp`` stream, or on stdout over stdio.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import live_sdk
import pytest

from easy_mcp import MCPServer

pytestmark = live_sdk.marker

REPO_ROOT = Path(__file__).resolve().parent.parent
LiveServer = Callable[[Any], str]

# A server run as the SDK's stdio subprocess.  grow() registers a tool, as
# an application changing its tools at runtime would; stop() ends serving a
# moment after it has answered, as a shutdown the client did not ask for.
STDIO_SERVER = '''
import threading

from easy_mcp import MCPServer

server = MCPServer(name="lc-interop", rate_limit_per_minute=None)


@server.tool
def grow(name: str) -> str:
    """Register a tool called name."""

    def extra() -> str:
        """Registered at runtime."""
        return name

    server.register_tool(extra, name=name)
    return "grown"


@server.tool
def stop() -> str:
    """Stop serving shortly."""
    threading.Timer(0.5, server.stop).start()
    return "stopping"


server.run("stdio")
'''


def make_server() -> MCPServer:
    server = MCPServer(port=0, name="lc-interop", rate_limit_per_minute=None)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def register(server: MCPServer, name: str = "extra") -> None:
    def tool() -> str:
        """A tool registered at runtime."""
        return name

    server.register_tool(tool, name=name)


def stdio_parameters() -> Any:
    from mcp.client.stdio import StdioServerParameters

    return StdioServerParameters(
        command=sys.executable,
        args=["-c", STDIO_SERVER],
        env={"PYTHONPATH": str(REPO_ROOT), "PYTHONUNBUFFERED": "1"},
        cwd=str(REPO_ROOT),
    )


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@contextlib.contextmanager
def running(server: MCPServer) -> Iterator[str]:
    """Serve *server* with ``server.run()`` in a thread, so stopping it is a real shutdown."""
    server.port = free_port()
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.port}"
    deadline = time.time() + 10
    while True:
        try:
            if httpx.get(f"{base}/healthz", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        assert time.time() < deadline, "server did not start"
        time.sleep(0.05)
    try:
        yield base
    finally:
        server.stop()
        thread.join(10)


class Heard:
    """A message_handler that records the list-change notifications it gets."""

    def __init__(self) -> None:
        self.methods: list[str] = []
        self.event = asyncio.Event()

    async def __call__(self, message: Any) -> None:
        method = getattr(message, "method", None)
        if method == "notifications/tools/list_changed":
            self.methods.append(method)
            self.event.set()

    async def wait(self, timeout: float = 15.0) -> None:
        await asyncio.wait_for(self.event.wait(), timeout)


async def next_event(subscription: Any, timeout: float = 15.0) -> Any:
    return await asyncio.wait_for(subscription.__anext__(), timeout)


@pytest.mark.parametrize("mode", ["auto", "2026-07-28"])
async def test_sdk_listen_over_http_sees_a_new_tool(live_server: LiveServer, mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    server = make_server()
    base = live_server(server)
    async with Client(f"{base}/mcp", mode=mode) as client:
        assert client.protocol_version == "2026-07-28"
        if mode == "auto":  # a pinned client takes the server on trust: no discover
            assert client.server_capabilities.tools is not None
            assert client.server_capabilities.tools.list_changed is True
        async with client.listen(tools_list_changed=True) as subscription:
            assert subscription.honored.tools_list_changed is True
            await asyncio.to_thread(register, server)
            event = await next_event(subscription)
            assert type(event).__name__ == "ToolsListChanged"
            tools = await client.list_tools()
            assert {tool.name for tool in tools.tools} == {"add", "extra"}


async def test_sdk_legacy_session_receives_list_changed_on_the_get_stream(
    live_server: LiveServer,
) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    server = make_server()
    base = live_server(server)
    heard = Heard()
    async with Client(f"{base}/mcp", mode="legacy", message_handler=heard) as client:
        assert client.protocol_version == "2025-11-25"
        assert client.server_capabilities.tools is not None
        assert client.server_capabilities.tools.list_changed is True
        await asyncio.to_thread(register, server)
        await heard.wait()
        tools = await client.list_tools()
        assert {tool.name for tool in tools.tools} == {"add", "extra"}
    assert heard.methods == ["notifications/tools/list_changed"]


@pytest.mark.parametrize("mode", ["auto", "2026-07-28", "legacy"])
async def test_sdk_stdio_both_eras(mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    heard = Heard()
    async with Client(stdio_parameters(), mode=mode, message_handler=heard) as client:
        if mode == "legacy":
            assert client.protocol_version == "2025-11-25"
            await client.call_tool("grow", {"name": "fresh"})
            await heard.wait()
        else:
            assert client.protocol_version == "2026-07-28"
            async with client.listen(tools_list_changed=True) as subscription:
                await client.call_tool("grow", {"name": "fresh"})
                event = await next_event(subscription)
                assert type(event).__name__ == "ToolsListChanged"
        tools = await client.list_tools()
        assert {tool.name for tool in tools.tools} == {"grow", "stop", "fresh"}


@pytest.mark.parametrize("mode", ["auto", "2026-07-28"])
async def test_sdk_sees_graceful_close_over_http(mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    server = make_server()
    with running(server) as base:
        async with Client(f"{base}/mcp", mode=mode) as client:
            async with client.listen(tools_list_changed=True) as subscription:
                await asyncio.to_thread(server.stop)
                # A graceful end finishes the loop; a drop would raise SubscriptionLost.
                events = await asyncio.wait_for(_drain(subscription), 15)
                assert events == []


@pytest.mark.parametrize("mode", ["auto", "2026-07-28"])
async def test_sdk_sees_graceful_close_over_stdio(mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    async with Client(stdio_parameters(), mode=mode) as client:
        async with client.listen(tools_list_changed=True) as subscription:
            await client.call_tool("stop", {})
            events = await asyncio.wait_for(_drain(subscription), 15)
            assert events == []


async def _drain(subscription: Any) -> list[Any]:
    return [event async for event in subscription]
