"""OAuth and middleware: credentials stay out of reach, and step-up stays built in."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
from conftest import OAUTH_RESOURCE, headers_for, modern, rpc
from oauth_fake_as import FakeAuthorizationServer

from easy_mcp import (
    APIKeyAuth,
    MCPServer,
    OAuthResourceServer,
    RequestInfo,
    RequestOutcome,
    ToolCall,
    ToolOutcome,
)
from easy_mcp.exceptions import AUTHENTICATION_REQUIRED
from easy_mcp.middleware import RequestNext, ToolNext

LiveServer = Callable[[Any], str]

KEY = "oauth-middleware-key-" + "k" * 11
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


def watched_server(
    fake_as: FakeAuthorizationServer,
) -> tuple[MCPServer, list[RequestInfo], list[tuple[str, int | None]], list[str]]:
    oauth = OAuthResourceServer(OAUTH_RESOURCE, [fake_as.issuer])
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth, auth=APIKeyAuth({KEY: "*"}))
    requests: list[RequestInfo] = []
    outcomes: list[tuple[str, int | None]] = []
    tool_calls: list[str] = []

    @server.middleware
    async def watch(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        requests.append(request)
        outcome = await call_next()
        outcomes.append((request.method, outcome.error_code))
        return outcome

    @server.tool_middleware
    async def guard(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        tool_calls.append(call.tool.name)
        return await call_next()

    @server.tool(scopes=("files:write",))
    def write_file(path: str) -> str:
        """Write a file."""
        return f"wrote {path}"

    return server, requests, outcomes, tool_calls


def test_transport_info_never_holds_credentials(
    live_server: LiveServer, fake_as: FakeAuthorizationServer
) -> None:
    server, requests, _, _ = watched_server(fake_as)
    base = live_server(server)
    token = fake_as.mint(claims={"scope": "mcp:access files:write"})
    with httpx.Client(base_url=base, timeout=10) as client:
        listed = modern("tools/list")
        for credential in ({"Authorization": f"Bearer {token}"}, {"X-API-Key": KEY}):
            headers = {**headers_for(listed), **credential, "X-Tenant": "acme"}
            assert client.post("/mcp", json=listed, headers=headers).status_code == 200
        init = client.post(
            "/mcp",
            json=rpc("initialize", INIT),
            headers={**ACCEPT, "Authorization": f"Bearer {token}"},
        )
        assert init.status_code == 200
    assert len(requests) == 3
    for request in requests:
        for name in ("authorization", "x-api-key", "mcp-session-id", "cookie"):
            assert name not in request.transport.headers
        dumped = json.dumps(dict(request.transport.headers))
        assert token not in dumped and KEY not in dumped
        assert request.identity is not None  # used, then withheld
    token_request = requests[0]
    assert token_request.identity is not None
    assert token_request.identity.issuer == fake_as.issuer
    assert token_request.client_id == token_request.identity.fingerprint
    assert token_request.transport.headers["x-tenant"] == "acme"
    # The identity holds verified claims, never the token.
    assert token not in repr(token_request.identity)
    assert token not in json.dumps(dict(token_request.identity.claims), default=str)


def test_step_up_403_skips_tool_middleware_and_survives_request_middleware(
    live_server: LiveServer, fake_as: FakeAuthorizationServer
) -> None:
    server, _, outcomes, tool_calls = watched_server(fake_as)
    base = live_server(server)
    narrow = fake_as.mint()
    broad = fake_as.mint(claims={"scope": "mcp:access files:write"})
    with httpx.Client(base_url=base, timeout=10) as client:
        message = modern("tools/call", {"name": "write_file", "arguments": {"path": "a"}})
        headers = {**headers_for(message), "Authorization": f"Bearer {narrow}"}
        refused = client.post("/mcp", json=message, headers=headers)
        assert refused.status_code == 403
        assert 'error="insufficient_scope"' in refused.headers["www-authenticate"]
        assert refused.json()["error"]["data"]["scope"] == "files:write"
        # A session too: the request middleware sees the refusal, then the
        # transport still answers 403.
        init = client.post(
            "/mcp",
            json=rpc("initialize", INIT),
            headers={**ACCEPT, "Authorization": f"Bearer {narrow}"},
        )
        session = init.headers["mcp-session-id"]
        in_session = client.post(
            "/mcp",
            json=rpc("tools/call", {"name": "write_file", "arguments": {"path": "a"}}, 2),
            headers={**ACCEPT, "Authorization": f"Bearer {narrow}", "MCP-Session-Id": session},
        )
        assert in_session.status_code == 403
        assert tool_calls == []  # the tool middleware never saw the refused calls
        headers["Authorization"] = f"Bearer {broad}"
        allowed = client.post("/mcp", json=message, headers=headers)
        assert allowed.status_code == 200
        assert tool_calls == ["write_file"]
    assert outcomes.count(("tools/call", AUTHENTICATION_REQUIRED)) == 2
    assert ("tools/call", None) in outcomes
