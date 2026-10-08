"""Request and tool middleware, and the request path they plug into."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import (
    LogCapture,
    headers_for,
    make_context,
    modern,
    notification,
    rpc,
)

from easy_mcp import AuthorizationError, MCPServer, ProtocolError
from easy_mcp.exceptions import (
    AUTHENTICATION_REQUIRED,
    FORBIDDEN,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
)

LiveServer = Callable[[Any], str]


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


async def test_methods_outside_the_table_are_answered_as_before() -> None:
    server = make_server()
    context = make_context()
    unknown = await server.dispatch(rpc("resources/list"), context)
    assert unknown is not None and unknown["error"]["code"] == METHOD_NOT_FOUND
    for method in ("ping", "initialize", "notifications/cancelled"):
        response = await server.dispatch(modern(method), context)
        assert response is not None and response["error"]["code"] == METHOD_NOT_FOUND
    # server/discover is always stateless, so without _meta it is malformed.
    discover = await server.dispatch(rpc("server/discover"), context)
    assert discover is not None and discover["error"]["code"] == INVALID_PARAMS
    assert await server.dispatch(notification("notifications/unknown"), context) is None
    assert await server.dispatch(rpc("notifications/unknown", msg_id=3), context) is None


async def test_a_legacy_request_naming_a_notification_acts_as_one() -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 7), context))
    await asyncio.wait_for(started.wait(), 5)
    cancel = rpc("notifications/cancelled", {"requestId": 7}, msg_id=8)
    assert await server.dispatch(cancel, context) is None
    assert await asyncio.wait_for(call, 5) is None
    assert cancelled.is_set()


async def test_request_ids_that_cannot_be_keys_are_still_served() -> None:
    server = make_server()
    context = make_context()
    ping = await server.dispatch(rpc("ping", msg_id=[1]), context)
    assert ping == {"jsonrpc": "2.0", "id": [1], "result": {}}
    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, msg_id={"n": 1})
    called = await server.dispatch(call, context)
    assert called is not None and called["result"]["content"][0]["text"] == "3"
    ignored = notification("notifications/cancelled", {"requestId": [1]})
    assert await server.dispatch(ignored, context) is None
    assert context.in_flight == {}


# ------------------------------------------------------------ error codes


async def test_reserved_codes_never_reach_the_wire(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    server = make_server()

    def refuse(context: Any) -> dict[str, Any]:
        raise ProtocolError("made-up code", code=-32050)

    monkeypatch.setattr(server, "_handle_tools_list", refuse)
    response = await server.dispatch(rpc("tools/list"), make_context())
    assert response is not None
    assert response["error"]["code"] == INTERNAL_ERROR
    assert "error_id=" in response["error"]["message"]
    assert "made-up" not in response["error"]["message"]
    assert "-32050" in logs.text


async def test_the_spec_defined_reserved_codes_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    server = make_server()
    for code in (-32020, -32021, -32022, -32001):

        def refuse(context: Any, code: int = code) -> dict[str, Any]:
            raise ProtocolError("defined", code=code)

        monkeypatch.setattr(server, "_handle_tools_list", refuse)
        response = await server.dispatch(rpc("tools/list"), make_context())
        assert response is not None and response["error"]["code"] == code


async def test_forbidden_is_never_sent_statelessly(monkeypatch: pytest.MonkeyPatch) -> None:
    server = make_server()

    def refuse(*args: Any) -> dict[str, Any]:
        raise AuthorizationError("no")

    monkeypatch.setattr(server, "_dispatch_modern", refuse)
    monkeypatch.setattr(server, "_handle_tools_list", refuse)
    stateless = await server.dispatch(modern("tools/list"), make_context())
    assert stateless is not None and stateless["error"]["code"] == AUTHENTICATION_REQUIRED
    legacy = await server.dispatch(rpc("tools/list"), make_context())
    assert legacy is not None and legacy["error"]["code"] == FORBIDDEN


def test_the_origin_refusal_speaks_the_requests_era(live_server: LiveServer) -> None:
    base = live_server(make_server())
    evil = {"Origin": "http://evil.example:8000"}
    with httpx.Client(base_url=base, timeout=10) as client:
        call = modern("tools/list")
        rejected = client.post("/mcp", json=call, headers={**headers_for(call), **evil})
        assert rejected.status_code == 403
        assert rejected.json()["error"]["code"] == INVALID_REQUEST
        legacy = client.post(
            "/mcp",
            json=rpc("initialize", {"protocolVersion": "2025-11-25"}),
            headers={"Accept": "application/json, text/event-stream", **evil},
        )
        assert legacy.status_code == 403
        assert legacy.json()["error"]["code"] == FORBIDDEN
