"""stdio and OAuth: local servers take credentials from the environment, not tokens."""

from __future__ import annotations

import io
import json
from typing import Any

import pytest
from conftest import LogCapture

from easy_mcp import APIKeyAuth, AuthenticationError, MCPServer, OAuthResourceServer
from easy_mcp.transport.stdio import StdioTransport

ISSUER = "https://auth.example.com"
RESOURCE = "https://mcp.example.com/mcp"
KEY = "stdio-oauth-test-key-" + "k" * 11


class FailIfUsed:
    """Stands in for token verification, which stdio must never reach."""

    async def __call__(self, token: str) -> Any:
        raise AssertionError("stdio consulted OAuth")


def make_server(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> MCPServer:
    oauth = OAuthResourceServer(RESOURCE, [ISSUER])
    monkeypatch.setattr(oauth, "verify", FailIfUsed())
    monkeypatch.setattr(oauth, "warm_up", FailIfUsed())
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth, **kwargs)

    @server.tool
    def public() -> str:
        """Anyone."""
        return "public"

    @server.tool(scopes=("admin",))
    def protected() -> str:
        """Keyed callers only."""
        return "protected"

    return server


async def serve(
    server: MCPServer, api_key: str | None, *messages: dict[str, Any]
) -> dict[Any, Any]:
    """Serve *messages* over stdio; the replies by id (they may arrive in any order)."""
    stdin = io.BytesIO(b"".join(json.dumps(message).encode() + b"\n" for message in messages))
    stdout = io.BytesIO()
    transport = StdioTransport(server, api_key=api_key, stdin=stdin, stdout=stdout)
    await transport.serve()
    replies = [json.loads(line) for line in stdout.getvalue().splitlines()]
    return {reply["id"]: reply for reply in replies}


def call(name: str, msg_id: int) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "method": "tools/call", "params": {"name": name}}


async def test_stdio_uses_api_key_not_oauth(monkeypatch: pytest.MonkeyPatch) -> None:
    server = make_server(monkeypatch, auth=APIKeyAuth({KEY: ["admin"]}))
    replies = await serve(server, KEY, call("protected", 1), call("public", 2))
    assert replies[1]["result"]["content"][0]["text"] == "protected"
    assert replies[2]["result"]["content"][0]["text"] == "public"
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await serve(server, "an-access-token-is-no-key")


async def test_stdio_key_without_api_keys_fails_fast_with_oauth(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    server = make_server(monkeypatch)
    with pytest.raises(AuthenticationError, match="oauth= applies to HTTP transports only"):
        await serve(server, "eyJhbGciOiJSUzI1NiJ9.e30.c2ln")
    assert logs.events("stdio_auth_failed") == [{"type": "stdio_auth_failed"}]
    # Without oauth=, 0.3.1 behaviour: the key is ignored, the caller anonymous.
    plain = MCPServer(port=0, rate_limit_per_minute=None)

    @plain.tool
    def hello() -> str:
        """Hi."""
        return "hi"

    replies = await serve(plain, "whatever", call("hello", 1))
    assert replies[1]["result"]["content"][0]["text"] == "hi"


async def test_stdio_oauth_only_serves_public_tools_and_logs_once(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    monkeypatch.delenv("EASY_MCP_STDIO_API_KEY", raising=False)
    server = make_server(monkeypatch)
    replies = await serve(server, None, call("public", 1), call("protected", 2))
    assert replies[1]["result"]["content"][0]["text"] == "public"
    assert replies[2]["error"]["message"] == "Unknown tool: protected"
    assert logs.text.count("oauth= applies to HTTP transports;") == 1
