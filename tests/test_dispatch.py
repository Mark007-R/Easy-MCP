"""Protocol dispatch: MCP methods, execution, errors, timeouts, cancellation."""

from __future__ import annotations

import asyncio
import json

import pytest
from conftest import make_context, notification, rpc

from easy_mcp import PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS, MCPServer, ToolError
from easy_mcp.exceptions import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    SESSION_LIMIT_EXCEEDED,
    TOOL_TIMEOUT,
)


@pytest.fixture
def app() -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @server.tool
    async def greet(name: str) -> str:
        """Greet someone by name."""
        return f"hello {name}"

    @server.tool
    def report() -> dict:
        """Return a structured report."""
        return {"beta": 2, "alpha": 1}

    @server.tool
    def boom() -> str:
        """Always fails with an internal error."""
        raise RuntimeError("secret internal detail")

    @server.tool
    def polite_error() -> str:
        """Fails with a safe, intentional message."""
        raise ToolError("upstream service unavailable")

    @server.tool
    def mislabeled() -> dict[str, int]:
        """Promises an object of integers and returns a list."""
        return [1, 2, 3]  # type: ignore[return-value]

    return server


async def test_initialize(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("initialize", {"protocolVersion": "2024-11-05"}), make_context()
    )
    assert response is not None
    result = response["result"]
    assert result["protocolVersion"] == "2024-11-05"
    assert result["serverInfo"]["name"] == "easy-mcp"
    assert "tools" in result["capabilities"]


async def test_protocol_version_negotiation(app: MCPServer) -> None:
    # A supported version is echoed back ...
    for requested in SUPPORTED_PROTOCOL_VERSIONS:
        response = await app.dispatch(
            rpc("initialize", {"protocolVersion": requested}), make_context()
        )
        assert response["result"]["protocolVersion"] == requested
    # ... anything else gets the newest version this server speaks.
    assert PROTOCOL_VERSION == SUPPORTED_PROTOCOL_VERSIONS[0] == "2025-11-25"
    for params in ({"protocolVersion": "2099-01-01"}, {"protocolVersion": 42}, {}):
        response = await app.dispatch(rpc("initialize", params), make_context())
        assert response["result"]["protocolVersion"] == PROTOCOL_VERSION


async def test_ping(app: MCPServer) -> None:
    response = await app.dispatch(rpc("ping"), make_context())
    assert response == {"jsonrpc": "2.0", "id": 1, "result": {}}


async def test_unknown_method(app: MCPServer) -> None:
    response = await app.dispatch(rpc("resources/list"), make_context())
    assert response["error"]["code"] == METHOD_NOT_FOUND


async def test_unknown_notification_ignored(app: MCPServer) -> None:
    assert await app.dispatch(notification("weird/thing"), make_context()) is None
    assert await app.dispatch(notification("notifications/initialized"), make_context()) is None


async def test_malformed_envelope(app: MCPServer) -> None:
    ctx = make_context()
    response = await app.dispatch(["not", "an", "object"], ctx)
    assert response["error"]["code"] == INVALID_REQUEST

    response = await app.dispatch({"jsonrpc": "1.0", "id": 1, "method": "ping"}, ctx)
    assert response["error"]["code"] == INVALID_REQUEST

    response = await app.dispatch({"jsonrpc": "2.0", "id": 1}, ctx)
    assert response["error"]["code"] == INVALID_REQUEST

    response = await app.dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "ping", "params": []}, ctx
    )
    assert response["error"]["code"] == INVALID_PARAMS


async def test_tools_list(app: MCPServer) -> None:
    response = await app.dispatch(rpc("tools/list"), make_context())
    tools = response["result"]["tools"]
    assert [t["name"] for t in tools] == [
        "add",
        "boom",
        "greet",
        "mislabeled",
        "polite_error",
        "report",
    ]
    add_tool = tools[0]
    assert add_tool["description"] == "Add two integers."
    assert add_tool["inputSchema"]["required"] == ["a", "b"]
    assert add_tool["inputSchema"]["additionalProperties"] is False


async def test_call_sync_tool(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 2, "b": 3}}), make_context()
    )
    result = response["result"]
    assert result["isError"] is False
    assert result["content"] == [{"type": "text", "text": "5"}]


async def test_call_async_tool(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "greet", "arguments": {"name": "world"}}), make_context()
    )
    assert response["result"]["content"][0]["text"] == "hello world"


async def test_result_serialization_is_deterministic(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "report", "arguments": {}}), make_context()
    )
    text = response["result"]["content"][0]["text"]
    assert text == '{"alpha": 1, "beta": 2}'
    assert json.loads(text) == {"alpha": 1, "beta": 2}


async def test_structured_content_accompanies_the_text_block(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "report", "arguments": {}}), make_context()
    )
    result = response["result"]
    assert result["structuredContent"] == {"alpha": 1, "beta": 2}
    # The spec asks for the serialized JSON in a text block as well, so older
    # clients that ignore structuredContent still see the data.
    assert json.loads(result["content"][0]["text"]) == result["structuredContent"]


async def test_no_structured_content_without_an_output_schema(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 2, "b": 3}}), make_context()
    )
    assert "structuredContent" not in response["result"]


async def test_result_breaking_the_output_schema_is_an_error(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "mislabeled", "arguments": {}}), make_context()
    )
    result = response["result"]
    assert result["isError"] is True
    assert "structuredContent" not in result
    text = result["content"][0]["text"]
    assert "output schema" in text and "error_id=" in text
    # The offending data stays in the log, not in the client's hands.
    assert "1, 2, 3" not in text


async def test_call_unknown_tool(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "nope", "arguments": {}}), make_context()
    )
    assert response["error"]["code"] == INVALID_PARAMS
    assert "Unknown tool" in response["error"]["message"]


async def test_call_missing_required_argument(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 2}}), make_context()
    )
    assert response["error"]["code"] == INVALID_PARAMS
    assert "b" in response["error"]["message"]


async def test_call_unexpected_argument(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2, "c": 3}}),
        make_context(),
    )
    assert response["error"]["code"] == INVALID_PARAMS
    assert "unexpected" in response["error"]["message"]


async def test_call_wrong_type(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": "2", "b": 3}}), make_context()
    )
    assert response["error"]["code"] == INVALID_PARAMS


async def test_internal_error_is_sanitized(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "boom", "arguments": {}}), make_context()
    )
    result = response["result"]
    assert result["isError"] is True
    text = result["content"][0]["text"]
    assert "error_id=" in text
    assert "secret internal detail" not in text
    assert "Traceback" not in text


async def test_debug_mode_includes_detail() -> None:
    server = MCPServer(port=0, debug=True, rate_limit_per_minute=None)

    @server.tool
    def boom() -> str:
        """Always fails."""
        raise RuntimeError("secret internal detail")

    response = await server.dispatch(
        rpc("tools/call", {"name": "boom", "arguments": {}}), make_context()
    )
    assert "secret internal detail" in response["result"]["content"][0]["text"]


async def test_tool_error_is_shown_verbatim(app: MCPServer) -> None:
    response = await app.dispatch(
        rpc("tools/call", {"name": "polite_error", "arguments": {}}), make_context()
    )
    result = response["result"]
    assert result["isError"] is True
    assert result["content"][0]["text"] == "upstream service unavailable"


async def test_one_failing_tool_does_not_kill_the_server(app: MCPServer) -> None:
    ctx = make_context()
    await app.dispatch(rpc("tools/call", {"name": "boom", "arguments": {}}), ctx)
    response = await app.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, msg_id=2), ctx
    )
    assert response["result"]["content"][0]["text"] == "2"


async def test_timeout() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool(timeout=0.05)
    async def sleepy() -> str:
        """Sleeps too long."""
        await asyncio.sleep(1.0)
        return "done"

    response = await server.dispatch(
        rpc("tools/call", {"name": "sleepy", "arguments": {}}), make_context()
    )
    assert response["error"]["code"] == TOOL_TIMEOUT
    assert "timed out" in response["error"]["message"]


async def test_session_usage_limit() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Callable once per session."""
        return "ok"

    ctx = make_context()
    first = await server.dispatch(rpc("tools/call", {"name": "once", "arguments": {}}), ctx)
    assert first["result"]["isError"] is False
    second = await server.dispatch(
        rpc("tools/call", {"name": "once", "arguments": {}}, msg_id=2), ctx
    )
    assert second["error"]["code"] == SESSION_LIMIT_EXCEEDED

    # A different session starts with a fresh budget.
    other = await server.dispatch(
        rpc("tools/call", {"name": "once", "arguments": {}}),
        make_context(session_id="other-session"),
    )
    assert other["result"]["isError"] is False


async def test_cancellation() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    started = asyncio.Event()

    @server.tool
    async def long_running() -> str:
        """Runs for a long time."""
        started.set()
        await asyncio.sleep(30)
        return "never"

    ctx = make_context()
    call = asyncio.create_task(
        server.dispatch(rpc("tools/call", {"name": "long_running", "arguments": {}}, msg_id=7), ctx)
    )
    await asyncio.wait_for(started.wait(), timeout=5)
    cancel_ack = await server.dispatch(
        notification("notifications/cancelled", {"requestId": 7}), ctx
    )
    assert cancel_ack is None
    # Per MCP, a cancelled request produces no response at all.
    assert await asyncio.wait_for(call, timeout=5) is None


async def test_integral_float_arguments_reach_the_tool_as_int(app: MCPServer) -> None:
    @app.tool
    def repeat(text: str, times: int) -> str:
        """Repeat text; fails loudly if ``times`` is not an int."""
        return text * times

    response = await app.dispatch(
        rpc("tools/call", {"name": "repeat", "arguments": {"text": "ab", "times": 2.0}}),
        make_context(),
    )
    assert response is not None
    assert response["result"]["isError"] is False
    assert response["result"]["content"][0]["text"] == "abab"


async def test_server_reports_package_version_by_default() -> None:
    from easy_mcp import __version__

    response = await MCPServer(port=0, rate_limit_per_minute=None).dispatch(
        rpc("initialize", {"protocolVersion": PROTOCOL_VERSION}), make_context()
    )
    assert response is not None
    assert response["result"]["serverInfo"]["version"] == __version__
