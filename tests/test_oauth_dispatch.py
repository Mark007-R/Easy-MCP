"""OAuth inside the server: visibility, step-up, scopes, the caller, credentials.

No HTTP here: token identities are built directly, as ``authenticate_request``
would return them, and handed to ``dispatch``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from types import MappingProxyType
from typing import Any

import pytest
from conftest import LogCapture, make_context, modern, rpc

from easy_mcp import (
    APIKeyAuth,
    AuthenticationError,
    ClientIdentity,
    InsufficientScopeError,
    MCPServer,
    OAuthResourceServer,
    TokenRequiredError,
    current_identity,
    current_tool_call,
)
from easy_mcp.exceptions import AUTHENTICATION_REQUIRED, INVALID_PARAMS
from easy_mcp.security.oauth import principal_fingerprint

ISSUER = "https://auth.example.com"
RESOURCE = "https://mcp.example.com/mcp"
KEY = "dispatch-test-key-" + "k" * 14


def token_identity(
    *scopes: str, subject: str = "user-1", client: str = "client-1"
) -> ClientIdentity:
    return ClientIdentity(
        fingerprint=principal_fingerprint(ISSUER, subject, client),
        scopes=frozenset(scopes),
        subject=subject,
        client_id=client,
        issuer=ISSUER,
        expires_at=1_900_000_000,
        claims=MappingProxyType({"sub": subject, "tenant": "acme"}),
    )


def oauth_server(*, step_up: bool = True, auth: APIKeyAuth | None = None, **kw: Any) -> MCPServer:
    oauth = OAuthResourceServer(RESOURCE, [ISSUER], step_up=step_up, **kw)
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth, auth=auth)

    @server.tool
    def status() -> str:
        """Any signed-in caller."""
        return "ok"

    @server.tool(requires_auth=True)
    def whoami() -> dict[str, Any]:
        """The caller."""
        who = current_identity()
        return {"subject": who.subject if who else None}

    @server.tool(scopes=("files:read", "files:write"))
    def read_file(path: str) -> str:
        """Read a file."""
        return f"read {path}"

    @server.tool(scopes=("files:write",), max_calls_per_session=1)
    def write_file(path: str) -> str:
        """Write a file."""
        return f"wrote {path}"

    return server


async def names(server: MCPServer, identity: ClientIdentity | None) -> list[str]:
    response = await server.dispatch(rpc("tools/list"), make_context(identity))
    assert response is not None
    return [tool["name"] for tool in response["result"]["tools"]]


async def call(
    server: MCPServer, tool: str, identity: ClientIdentity | None, **arguments: Any
) -> dict[str, Any]:
    context = make_context(identity, client_id=identity.fingerprint if identity else "ip:test")
    response = await server.dispatch(
        rpc("tools/call", {"name": tool, "arguments": arguments}, 7), context
    )
    assert response is not None
    return response


async def test_step_up_lists_every_tool() -> None:
    server = oauth_server()
    listed = await names(server, token_identity())
    assert listed == ["read_file", "status", "whoami", "write_file"]


async def test_step_up_call_raises_insufficient_scope_with_first_declared(
    logs: LogCapture,
) -> None:
    server = oauth_server()
    response = await call(server, "read_file", token_identity("email"), path="a")
    assert response["id"] == 7
    assert response["error"] == {
        "code": AUTHENTICATION_REQUIRED,
        "message": "Insufficient scope for tool 'read_file'",
        "data": {"error": "insufficient_scope", "scope": "files:read"},
    }
    (denied,) = logs.events("tool_denied")
    assert denied["reason"] == "InsufficientScopeError"
    assert denied["scope"] == "files:read"
    # Any one of the tool's scopes is enough.
    allowed = await call(server, "read_file", token_identity("files:write"), path="a")
    assert allowed["result"]["content"][0]["text"] == "read a"
    # A tool with requires_auth only is open to every token.
    assert "result" in await call(server, "whoami", token_identity())
    # A stateless request gets the same code (no -32002 in either era).
    stateless = await server.dispatch(
        modern("tools/call", {"name": "write_file", "arguments": {"path": "a"}}),
        make_context(token_identity()),
    )
    assert stateless is not None
    assert stateless["error"]["code"] == AUTHENTICATION_REQUIRED
    assert stateless["error"]["data"]["scope"] == "files:write"


async def test_step_up_off_hides_tools() -> None:
    server = oauth_server(step_up=False)
    assert await names(server, token_identity()) == ["status", "whoami"]
    assert await names(server, token_identity("files:read")) == [
        "read_file",
        "status",
        "whoami",
    ]
    response = await call(server, "write_file", token_identity("files:read"), path="a")
    assert response["error"]["code"] == INVALID_PARAMS
    assert response["error"]["message"] == "Unknown tool: write_file"


async def test_api_key_identity_unaffected_by_step_up() -> None:
    auth = APIKeyAuth({KEY: ["other"]})
    server = oauth_server(auth=auth)
    key_identity = auth.authenticate(KEY)
    assert await names(server, key_identity) == ["status", "whoami"]
    response = await call(server, "write_file", key_identity, path="a")
    assert response["error"]["message"] == "Unknown tool: write_file"
    # A token's "*" is no wildcard either (verification drops it), but an
    # API key's still is.
    admin = APIKeyAuth({KEY: "*"}).authenticate(KEY)
    assert "result" in await call(server, "write_file", admin, path="a")


async def test_denied_calls_do_not_count_toward_session_cap() -> None:
    server = oauth_server()
    context = make_context(token_identity())
    message = rpc("tools/call", {"name": "write_file", "arguments": {"path": "a"}})
    for _ in range(3):
        denied = await server.dispatch(message, context)
        assert denied is not None and denied["error"]["code"] == AUTHENTICATION_REQUIRED
    assert context.tool_calls.get("write_file", 0) == 0
    # The stepped-up token, same session: the one allowed call is still there.
    stepped = server._request_context(context, token_identity("files:write"))
    allowed = await server.dispatch(message, stepped)
    assert allowed is not None and "result" in allowed
    assert context.tool_calls["write_file"] == 1
    again = await server.dispatch(message, stepped)
    assert again is not None and again["error"]["code"] == -32006


async def test_current_identity_in_async_and_sync_tools() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    seen: dict[str, Any] = {}

    @server.tool
    async def async_tool() -> str:
        """Async."""
        seen["async"] = current_identity()
        return "ok"

    @server.tool
    def sync_tool() -> str:
        """Sync, on a thread of its own."""
        seen["sync"] = current_identity()
        call_ = current_tool_call()
        seen["call_identity"] = call_.identity if call_ else None
        return "ok"

    identity = token_identity("files:read")
    for name in ("async_tool", "sync_tool"):
        response = await server.dispatch(rpc("tools/call", {"name": name}), make_context(identity))
        assert response is not None and "result" in response
    assert seen["async"] is identity
    assert seen["sync"] is identity
    assert seen["sync"].claims["tenant"] == "acme"
    assert seen["call_identity"] is identity
    assert current_identity() is None  # outside a call
    await server.dispatch(rpc("tools/call", {"name": "sync_tool"}), make_context(None))
    assert seen["sync"] is None


async def test_tools_list_private_with_oauth() -> None:
    server = oauth_server()
    context = make_context(token_identity())
    listed = await server.dispatch(modern("tools/list"), context)
    assert listed is not None and listed["result"]["cacheScope"] == "private"
    discovered = await server.dispatch(modern("server/discover"), context)
    assert discovered is not None and discovered["result"]["cacheScope"] == "public"
    assert discovered["result"]["capabilities"] == {"tools": {"listChanged": False}}
    assert server.auth_configured
    assert not MCPServer(port=0).auth_configured


async def test_scopes_supported_by_mode() -> None:
    stepping = oauth_server(required_scopes=["mcp:access"])
    assert stepping._initial_scopes() == ("mcp:access",)  # the minimal set
    hiding = oauth_server(required_scopes=["mcp:access"], step_up=False)
    # Without step-up a tool is invisible until a token holds its scope.
    assert hiding._initial_scopes() == ("mcp:access", "files:read", "files:write")
    assert oauth_server()._initial_scopes() == ()
    assert hiding._known_scopes() == frozenset({"mcp:access", "files:read", "files:write"})


class FakeVerifier:
    """Stands in for OAuthResourceServer.verify; records what it was asked."""

    def __init__(self, identity: ClientIdentity) -> None:
        self.identity = identity
        self.tokens: list[str] = []

    async def __call__(self, token: str) -> ClientIdentity:
        self.tokens.append(token)
        return self.identity


async def test_authenticate_request_rules(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    auth = APIKeyAuth({KEY: ["files:read"]})
    server = oauth_server(auth=auth, required_scopes=["mcp:access"])
    assert server.oauth is not None
    verifier = FakeVerifier(token_identity("mcp:access"))
    monkeypatch.setattr(server.oauth, "verify", verifier)

    with pytest.raises(TokenRequiredError):
        await server.authenticate_request()
    # API keys are matched first and never sent for verification.
    key_identity = await server.authenticate_request(bearer=KEY)
    assert key_identity is not None and key_identity.issuer is None
    assert await server.authenticate_request(api_key=KEY) == key_identity
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await server.authenticate_request(api_key="not-a-key-but-maybe-a-token")
    assert verifier.tokens == []
    # A bearer value that is no key is a token.
    token = await server.authenticate_request(bearer="a-token", api_key=KEY)
    assert token is verifier.identity
    assert verifier.tokens == ["a-token"]
    # required_scopes: all of them.
    verifier.identity = token_identity("files:read")
    with pytest.raises(InsufficientScopeError) as caught:
        await server.authenticate_request(bearer="narrow")
    assert caught.value.scopes == ("mcp:access",)
    assert caught.value.granted == frozenset({"files:read"})
    # principal_seen, once per principal.
    verifier.identity = token_identity("mcp:access")
    await server.authenticate_request(bearer="again")
    verifier.identity = token_identity("mcp:access", subject="user-2")
    await server.authenticate_request(bearer="other")
    seen = logs.events("principal_seen")
    assert [event["subject"] for event in seen] == ["user-1", "user-2"]
    assert seen[0] == {
        "type": "principal_seen",
        "client_id": token_identity().fingerprint,
        "issuer": ISSUER,
        "subject": "user-1",
        "oauth_client_id": "client-1",
    }


async def test_authenticate_request_without_oauth_is_0_3_1() -> None:
    bare = MCPServer(port=0)
    assert await bare.authenticate_request() is None
    assert await bare.authenticate_request(bearer="anything") is None  # nothing to check
    keyed = MCPServer(port=0, auth=APIKeyAuth({KEY: "*"}))
    assert await keyed.authenticate_request() is None
    assert (await keyed.authenticate_request(bearer=KEY)) == keyed.authenticate_key(KEY)
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await keyed.authenticate_request(bearer="wrong")


def test_request_context_shares_the_session() -> None:
    session = make_context(token_identity("a"))
    assert MCPServer._request_context(session, session.identity) is session
    # The same token again (equal identity, equal claims): the session's own.
    assert MCPServer._request_context(session, token_identity("a")) is session
    broader = MCPServer._request_context(session, token_identity("a", "b"))
    assert broader is not session
    assert broader.identity == token_identity("a", "b")
    assert broader.tool_calls is session.tool_calls
    assert broader.in_flight is session.in_flight
    assert broader.session_id == session.session_id
    auth = APIKeyAuth({KEY: "*"})
    keyed = make_context(auth.authenticate(KEY))
    assert MCPServer._request_context(keyed, auth.authenticate(KEY)) is keyed
    anonymous = make_context(None)
    assert MCPServer._request_context(anonymous, None) is anonymous


async def test_lifespan_warms_up_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    server = oauth_server()
    assert server.oauth is not None
    calls: list[str] = []

    async def warm_up() -> None:
        calls.append("warm_up")

    monkeypatch.setattr(server.oauth, "warm_up", warm_up)
    monkeypatch.setattr(server.oauth, "close", lambda: calls.append("close"))
    server.build_app()

    @contextlib.asynccontextmanager
    async def host(app: Any) -> AsyncIterator[None]:
        async with server.lifespan():
            calls.append("serving")
            yield

    async with host(None):
        await asyncio.sleep(0)
    assert calls == ["warm_up", "serving", "close"]


def test_oauth_must_be_a_resource_server() -> None:
    with pytest.raises(TypeError, match="OAuthResourceServer"):
        MCPServer(oauth=APIKeyAuth({KEY: "*"}))  # type: ignore[arg-type]
