"""Interop of elicit() with the official MCP Python SDK client.

Skipped unless ``EASY_MCP_LIVE_SDK_CLIENT=1`` is set and the ``mcp`` package
(2.3 or later) is installed: install it next to easy_mcp in a virtualenv of
its own, ``pip install "mcp>=2.3" -e .``, then run this file.

The SDK is driven in each mode it offers, over Streamable HTTP and stdio.
In ``legacy`` mode the server sends it an ``elicitation/create`` request and
runs the tool again; in the stateless modes the SDK gets an
``InputRequiredResult`` and retries the call by itself.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import live_sdk
import pytest

from easy_mcp import MCPServer, elicit

pytestmark = live_sdk.marker

REPO_ROOT = Path(__file__).resolve().parent.parent
LiveServer = Callable[[Any], str]
MODES = ["auto", "2026-07-28", "legacy"]

STDIO_SERVER = '''
from typing import Literal

from easy_mcp import MCPServer, elicit

server = MCPServer(name="ask-interop", rate_limit_per_minute=None)


@server.tool
def trip() -> str:
    """Plan a trip: two questions."""
    where = elicit("Where to?", {"city": str})
    if not where.accepted:
        return f"no trip: {where.action}"
    how = elicit(f"How do you get to {where['city']}?", {"by": Literal["train", "plane"]})
    if not how.accepted:
        return f"no trip: {how.action}"
    return f"{where['city']} by {how['by']}"


server.run("stdio")
'''


def make_server() -> MCPServer:
    server = MCPServer(name="ask-interop", port=0, rate_limit_per_minute=None)

    @server.tool
    def trip() -> str:
        """Plan a trip: two questions."""
        where = elicit("Where to?", {"city": str})
        if not where.accepted:
            return f"no trip: {where.action}"
        how = elicit(f"How do you get to {where['city']}?", {"by": Literal["train", "plane"]})
        if not how.accepted:
            return f"no trip: {how.action}"
        return f"{where['city']} by {how['by']}"

    return server


class User:
    """An elicitation_callback that answers as a user would, and records what it was asked."""

    def __init__(self, *, decline_second: bool = False) -> None:
        self.asked: list[tuple[str, dict[str, Any]]] = []
        self.decline_second = decline_second

    async def __call__(self, context: Any, params: Any) -> Any:
        from mcp.types import ElicitResult

        schema = params.requested_schema
        if not isinstance(schema, dict):
            schema = schema.model_dump(by_alias=True, exclude_none=True)
        self.asked.append((params.message, schema))
        if "city" in schema["properties"]:
            return ElicitResult(action="accept", content={"city": "Lisbon"})
        if self.decline_second:
            return ElicitResult(action="decline")
        return ElicitResult(action="accept", content={"by": "train"})


def check(user: User) -> None:
    assert [message for message, _ in user.asked] == ["Where to?", "How do you get to Lisbon?"]
    assert user.asked[1][1]["properties"]["by"]["enum"] == ["train", "plane"]


@pytest.mark.parametrize("mode", MODES)
async def test_sdk_answers_a_tools_questions_over_http(live_server: LiveServer, mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    base = live_server(make_server())
    user = User()
    async with Client(f"{base}/mcp", mode=mode, elicitation_callback=user) as client:
        result = await client.call_tool("trip", {})
        assert result.content[0].text == "Lisbon by train"
        check(user)
    declining = User(decline_second=True)
    async with Client(f"{base}/mcp", mode=mode, elicitation_callback=declining) as client:
        result = await client.call_tool("trip", {})
        assert result.content[0].text == "no trip: decline"


@pytest.mark.parametrize("mode", MODES)
async def test_sdk_answers_a_tools_questions_over_stdio(mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters

    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-c", STDIO_SERVER],
        env={"PYTHONPATH": str(REPO_ROOT), "PYTHONUNBUFFERED": "1"},
        cwd=str(REPO_ROOT),
    )
    user = User()
    async with Client(parameters, mode=mode, elicitation_callback=user) as client:
        result = await client.call_tool("trip", {})
        assert result.content[0].text == "Lisbon by train"
        check(user)


@pytest.mark.parametrize("mode", MODES)
async def test_a_client_without_a_callback_is_told_what_is_missing(
    live_server: LiveServer, mode: str
) -> None:
    live_sdk.require_sdk()
    from mcp import Client, MCPError

    base = live_server(make_server())
    async with Client(f"{base}/mcp", mode=mode) as client:
        if mode == "legacy":
            result = await client.call_tool("trip", {})
            assert result.is_error
            assert "did not declare the elicitation capability" in result.content[0].text
        else:
            with pytest.raises(MCPError) as refused:
                await client.call_tool("trip", {})
            assert refused.value.code == -32021
