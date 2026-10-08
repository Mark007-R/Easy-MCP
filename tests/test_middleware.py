"""Request and tool middleware, and the request path they plug into."""

from __future__ import annotations

from typing import Any

from conftest import make_context, rpc

from easy_mcp import MCPServer


def make_server(**kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


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
