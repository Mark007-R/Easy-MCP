"""Request and tool middleware, and the request path they plug into."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from conftest import LogCapture, make_context, notification, rpc

from easy_mcp import MCPServer


def make_server(**kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def with_slow_tool(server: MCPServer) -> tuple[asyncio.Event, asyncio.Event]:
    """Register ``slow``, an async tool that waits until it is cancelled."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @server.tool
    async def slow() -> str:
        """Waits until cancelled."""
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return "finished"

    return started, cancelled


# ------------------------------------------------------- the request path


async def test_initialize_records_the_negotiated_version() -> None:
    server = make_server()
    context = make_context()
    assert context.protocol_version is None
    await server.dispatch(rpc("initialize", {"protocolVersion": "2025-03-26"}), context)
    assert context.protocol_version == "2025-03-26"
    # An unknown version is answered, and recorded, as the newest legacy one.
    await server.dispatch(rpc("initialize", {"protocolVersion": "1999-01-01"}, 2), context)
    assert context.protocol_version == "2025-11-25"


async def test_a_cancelled_caller_sees_its_cancellation() -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}), make_context()))
    await asyncio.wait_for(started.wait(), 5)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert call.cancelled()
    await asyncio.wait_for(cancelled.wait(), 5)  # the call did not outlive its caller


async def test_a_timeout_around_dispatch_raises_timeout_error() -> None:
    server = make_server()
    _, cancelled = with_slow_tool(server)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.1):
            await server.dispatch(rpc("tools/call", {"name": "slow"}), make_context())
    await asyncio.wait_for(cancelled.wait(), 5)


async def test_a_client_cancel_still_drops_the_response(logs: LogCapture) -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 7), context))
    await asyncio.wait_for(started.wait(), 5)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 7}), context)
    assert await asyncio.wait_for(call, 5) is None  # no response, and no exception
    assert not call.cancelled()
    assert cancelled.is_set()
    assert logs.events("tool_cancelled") == [
        {"type": "tool_cancelled", "client_id": "ip:test", "request_id": 7}
    ]
    assert context.in_flight == {}
