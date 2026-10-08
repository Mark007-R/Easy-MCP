"""OAuth over real HTTP: Streamable HTTP (both eras) and legacy SSE.

Every test runs the MCP server and the local authorization server of
tests/oauth_fake_as.py on 127.0.0.1 (``live_server``); tokens are signed with
keys generated in the test.  The MCP server's ``resource`` is
``conftest.OAUTH_RESOURCE``: what tokens and the metadata name, not the URL
the test connects to.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest
from conftest import OAUTH_RESOURCE, LogCapture, headers_for, modern, notification, rpc
from oauth_fake_as import CLIENT_ID, CLIENT_SECRET, FakeAuthorizationServer

from easy_mcp import (
    APIKeyAuth,
    Introspection,
    MCPServer,
    OAuthResourceServer,
    SSETransport,
    StreamableHTTPTransport,
    current_identity,
    current_tool_call,
)
from easy_mcp.exceptions import (
    AUTHENTICATION_REQUIRED,
    INVALID_REQUEST,
    RATE_LIMITED,
    SERVER_BUSY,
)
from easy_mcp.security.oauth import principal_fingerprint

LiveServer = Callable[[Any], str]

KEY = "oauth-http-test-key-" + "k" * 12
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "tests", "version": "1.0"},
}
METADATA_URL = "https://mcp.example.com/.well-known/oauth-protected-resource/mcp"
RM = f'resource_metadata="{METADATA_URL}"'


def make_server(
    fake_as: FakeAuthorizationServer,
    *,
    auth: APIKeyAuth | None = None,
    rate_limit_per_minute: int | None = None,
    **oauth_options: Any,
) -> MCPServer:
    oauth_options.setdefault("required_scopes", ["mcp:access"])
    oauth = OAuthResourceServer(OAUTH_RESOURCE, [fake_as.issuer], **oauth_options)
    server = MCPServer(port=0, rate_limit_per_minute=rate_limit_per_minute, oauth=oauth, auth=auth)

    @server.tool
    def status() -> str:
        """Any signed-in caller."""
        return "ok"

    @server.tool(requires_auth=True)
    def whoami() -> dict[str, Any]:
        """Who is calling."""
        who = current_identity()
        call = current_tool_call()
        return {
            "subject": who.subject if who else None,
            "scopes": sorted(who.scopes) if who else [],
            "client_id": call.client_id if call else None,
        }

    @server.tool(scopes=("files:read", "files:write"))
    def read_file(path: str) -> str:
        """Read a file."""
        return f"read {path}"

    @server.tool(scopes=("files:write",))
    def write_file(path: str) -> str:
        """Write a file."""
        return f"wrote {path}"

    return server


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def stateless(
    client: httpx.Client, message: dict[str, Any], headers: dict[str, str] | None = None
) -> httpx.Response:
    return client.post("/mcp", json=message, headers={**headers_for(message), **(headers or {})})


def post(
    client: httpx.Client,
    message: Any,
    *,
    session: str | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    all_headers = {**ACCEPT, **(headers or {})}
    if session is not None:
        all_headers["MCP-Session-Id"] = session
    return client.post("/mcp", json=message, headers=all_headers)


def open_session(client: httpx.Client, headers: dict[str, str]) -> str:
    init = post(client, rpc("initialize", INIT), headers=headers)
    assert init.status_code == 200, init.text
    session = init.headers["mcp-session-id"]
    initialized = post(
        client, notification("notifications/initialized"), session=session, headers=headers
    )
    assert initialized.status_code == 202
    return session


def call(tool: str, msg_id: int = 1, **arguments: Any) -> dict[str, Any]:
    return rpc("tools/call", {"name": tool, "arguments": arguments}, msg_id)


def modern_call(tool: str, msg_id: int = 1, **arguments: Any) -> dict[str, Any]:
    return modern("tools/call", {"name": tool, "arguments": arguments}, msg_id)


def challenge(response: httpx.Response) -> str:
    value = response.headers["www-authenticate"]
    assert value.startswith("Bearer ")
    return value


def rpc_body(response: httpx.Response) -> dict[str, Any]:
    body: dict[str, Any] = response.json()
    return body


@pytest.fixture
def served(live_server: LiveServer, fake_as: FakeAuthorizationServer) -> Iterator[Any]:
    """Build and serve an OAuth server; returns (base URL, server)."""

    def start(**kwargs: Any) -> tuple[str, MCPServer]:
        server = make_server(fake_as, **kwargs)
        return live_server(server), server

    yield start


# ------------------------------------------------------------ metadata


def test_metadata_served_unauthenticated(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, server = served()
    with httpx.Client(base_url=base, timeout=10) as client:
        path = "/.well-known/oauth-protected-resource/mcp"
        got = client.get(path)
        assert got.status_code == 200
        assert got.headers["content-type"] == "application/json"
        assert got.headers["cache-control"] == "public, max-age=300"
        assert got.json() == {
            "authorization_servers": [fake_as.issuer],
            "bearer_methods_supported": ["header"],
            "resource": OAUTH_RESOURCE,
            "resource_name": "easy-mcp",
            "scopes_supported": ["mcp:access"],
        }
        assert list(json.loads(got.content)) == sorted(got.json())
        # Byte-identical from call to call.
        assert client.get(path).content == got.content
        head = client.head(path)
        assert head.status_code == 200 and head.content == b""
        assert server.oauth is not None and server.oauth.resource == OAUTH_RESOURCE


def test_root_metadata_only_for_pathless_resource(
    live_server: LiveServer, fake_as: FakeAuthorizationServer
) -> None:
    base = live_server(make_server(fake_as))
    with httpx.Client(base_url=base, timeout=10) as client:
        assert client.get("/.well-known/oauth-protected-resource").status_code == 404
    oauth = OAuthResourceServer("https://mcp.example.com", [fake_as.issuer])
    origin = live_server(MCPServer(port=0, oauth=oauth))
    with httpx.Client(base_url=origin, timeout=10) as client:
        root = client.get("/.well-known/oauth-protected-resource")
        assert root.status_code == 200
        assert root.json()["resource"] == "https://mcp.example.com"
        assert "scopes_supported" not in root.json()


def test_metadata_behind_origin_guard(served: Any) -> None:
    base, _ = served()
    with httpx.Client(base_url=base, timeout=10) as client:
        path = "/.well-known/oauth-protected-resource/mcp"
        assert client.get(path, headers={"Origin": "http://evil.example"}).status_code == 403
        assert client.get(path, headers={"Origin": "http://localhost:6274"}).status_code == 200


# ---------------------------------------------------------------- 401s


def test_missing_token_401_challenge_each_era(served: Any) -> None:
    base, _ = served()
    with httpx.Client(base_url=base, timeout=10) as client:
        responses = [
            post(client, rpc("initialize", INIT)),
            stateless(client, modern("server/discover")),
            stateless(client, modern("tools/list")),
            post(client, notification("notifications/initialized")),
            client.delete("/mcp", headers={"MCP-Session-Id": "x"}),
        ]
        for response in responses:
            assert response.status_code == 401, response.text
            assert challenge(response) == f'Bearer scope="mcp:access", {RM}'
            assert "error=" not in response.headers["www-authenticate"]
            assert rpc_body(response) == {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": AUTHENTICATION_REQUIRED, "message": "Authentication required"},
            }
        # GET /mcp opens no stream in this release: 405, whoever asks.
        assert client.get("/mcp").status_code == 405


def test_invalid_and_expired_token_401(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    with httpx.Client(base_url=base, timeout=10) as client:
        cases = {
            "not-a-jwt": "The access token is invalid",
            fake_as.mint(claims={"exp": int(time.time()) - 120}): "The access token expired",
            fake_as.mint(audience="https://other.example.com/mcp"): (
                "The access token was not issued for this resource"
            ),
        }
        for token, description in cases.items():
            response = stateless(client, modern("tools/list"), bearer(token))
            assert response.status_code == 401
            assert challenge(response) == (
                f'Bearer error="invalid_token", {RM}, error_description="{description}"'
            )
            assert rpc_body(response)["error"] == {
                "code": AUTHENTICATION_REQUIRED,
                "message": "Invalid access token",
            }


def test_auth_runs_before_header_mirror_checks(served: Any) -> None:
    base, _ = served()
    with httpx.Client(base_url=base, timeout=10) as client:
        message = modern("tools/list")
        mismatched = {**headers_for(message), "Mcp-Method": "tools/call"}
        response = client.post("/mcp", json=message, headers=mismatched)
        assert response.status_code == 401  # not 400: nothing is learned without a token
        unknown_session = post(client, rpc("ping"), session="no-such-session")
        assert unknown_session.status_code == 401


# --------------------------------------------------------------- flows


def test_stateless_flow_with_token(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    token = fake_as.mint()
    with httpx.Client(base_url=base, timeout=10) as client:
        discovered = stateless(client, modern("server/discover"), bearer(token))
        assert discovered.status_code == 200
        assert discovered.json()["result"]["cacheScope"] == "public"
        listed = stateless(client, modern("tools/list"), bearer(token))
        assert [tool["name"] for tool in listed.json()["result"]["tools"]] == [
            "read_file",
            "status",
            "whoami",
            "write_file",
        ]
        assert listed.json()["result"]["cacheScope"] == "private"
        who = stateless(client, modern_call("whoami"), bearer(token))
        result = who.json()["result"]["structuredContent"]
        assert result["subject"] == "user-1"
        assert result["client_id"] == principal_fingerprint(fake_as.issuer, "user-1", "client-1")


def test_session_flow_with_token(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    headers = bearer(fake_as.mint(claims={"scope": "mcp:access files:read"}))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client, headers)
        assert len(session) >= 32
        listed = post(client, rpc("tools/list", msg_id=2), session=session, headers=headers)
        assert len(listed.json()["result"]["tools"]) == 4
        read = post(client, call("read_file", 3, path="a"), session=session, headers=headers)
        assert read.json()["result"]["content"][0]["text"] == "read a"
        deleted = client.delete("/mcp", headers={"MCP-Session-Id": session, **headers})
        assert deleted.status_code == 204


# ---------------------------------------------------------------- 403s


def test_required_scopes_door_403(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    token = fake_as.mint(claims={"scope": "files:read email"})
    with httpx.Client(base_url=base, timeout=10) as client:
        response = stateless(client, modern("tools/list"), bearer(token))
        assert response.status_code == 403
        assert challenge(response) == (
            f'Bearer error="insufficient_scope", scope="mcp:access", {RM}, '
            'error_description="Additional scope required"'
        )
        assert rpc_body(response)["error"]["data"] == {
            "error": "insufficient_scope",
            "scope": "mcp:access",
        }
        # Older clients do not add what they hold: it is echoed (but not email).
        legacy = post(client, rpc("initialize", INIT), headers=bearer(token))
        assert legacy.status_code == 403
        assert 'scope="mcp:access files:read"' in challenge(legacy)


def test_step_up_403_stateless_lists_needed_only(
    served: Any, fake_as: FakeAuthorizationServer
) -> None:
    base, _ = served()
    token = fake_as.mint(claims={"scope": "mcp:access files:read"})
    with httpx.Client(base_url=base, timeout=10) as client:
        response = stateless(client, modern_call("write_file", 9, path="a"), bearer(token))
        assert response.status_code == 403
        assert challenge(response) == (
            f'Bearer error="insufficient_scope", scope="files:write", {RM}, '
            'error_description="Additional scope required"'
        )
        assert rpc_body(response) == {
            "jsonrpc": "2.0",
            "id": 9,
            "error": {
                "code": AUTHENTICATION_REQUIRED,
                "message": "Insufficient scope for tool 'write_file'",
                "data": {"error": "insufficient_scope", "scope": "files:write"},
            },
        }
        # The narrowest declared scope is what is asked for.
        narrow = fake_as.mint(claims={"scope": "mcp:access"})
        read = stateless(client, modern_call("read_file", path="a"), bearer(narrow))
        assert 'scope="files:read"' in challenge(read)


def test_step_up_403_legacy_adds_granted_relevant(
    served: Any, fake_as: FakeAuthorizationServer
) -> None:
    base, _ = served()
    headers = bearer(fake_as.mint(claims={"scope": "mcp:access files:read email"}))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client, headers)
        response = post(client, call("write_file", path="a"), session=session, headers=headers)
        assert response.status_code == 403
        assert challenge(response) == (
            'Bearer error="insufficient_scope", scope="files:write files:read mcp:access", '
            f'{RM}, error_description="Additional scope required"'
        )
        assert "email" not in response.headers["www-authenticate"]
        # The JSON-RPC data names only what is needed.
        assert rpc_body(response)["error"]["data"]["scope"] == "files:write"


def test_step_up_then_retry_same_session(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    narrow = bearer(fake_as.mint(claims={"scope": "mcp:access"}))
    broad = bearer(fake_as.mint(claims={"scope": "mcp:access files:write"}))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client, narrow)
        denied = post(client, call("write_file", path="a"), session=session, headers=narrow)
        assert denied.status_code == 403
        allowed = post(client, call("write_file", 2, path="a"), session=session, headers=broad)
        assert allowed.status_code == 200
        assert allowed.json()["result"]["content"][0]["text"] == "wrote a"
        # The narrow token is still narrow: identity is per request.
        again = post(client, call("write_file", 3, path="a"), session=session, headers=narrow)
        assert again.status_code == 403


def test_session_bound_to_principal_not_token(
    served: Any, fake_as: FakeAuthorizationServer, logs: LogCapture
) -> None:
    base, _ = served()
    first = bearer(fake_as.mint())
    refreshed = bearer(fake_as.mint(now=time.time() + 5))
    other_user = bearer(fake_as.mint(claims={"sub": "user-2"}))
    other_client = bearer(fake_as.mint(claims={"client_id": "client-2"}))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client, first)
        assert post(client, rpc("ping", msg_id=2), session=session, headers=refreshed).json() == {
            "jsonrpc": "2.0",
            "id": 2,
            "result": {},
        }
        for stranger in (other_user, other_client):
            hijack = post(client, rpc("ping", msg_id=3), session=session, headers=stranger)
            assert hijack.status_code == 403
            assert rpc_body(hijack)["error"]["code"] == -32002  # the handshake era's code
    assert len(logs.events("session_credential_mismatch")) == 2


def test_expired_token_mid_session_then_resume(
    served: Any, fake_as: FakeAuthorizationServer
) -> None:
    base, _ = served()
    issued = time.time()
    short = fake_as.mint(now=issued, claims={"exp": int(issued) - 61})
    fresh = bearer(fake_as.mint(now=issued))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client, fresh)
        expired = post(client, rpc("ping", msg_id=2), session=session, headers=bearer(short))
        assert expired.status_code == 401
        assert 'error_description="The access token expired"' in challenge(expired)
        resumed = post(client, rpc("ping", msg_id=3), session=session, headers=fresh)
        assert resumed.status_code == 200


def test_delete_needs_token_and_matching_principal(
    served: Any, fake_as: FakeAuthorizationServer
) -> None:
    base, _ = served()
    mine = bearer(fake_as.mint())
    theirs = bearer(fake_as.mint(claims={"sub": "user-2"}))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client, mine)
        assert client.delete("/mcp", headers={"MCP-Session-Id": session}).status_code == 401
        stolen = client.delete("/mcp", headers={"MCP-Session-Id": session, **theirs})
        assert stolen.status_code == 403
        assert client.delete("/mcp", headers={"MCP-Session-Id": session, **mine}).status_code == 204
        assert post(client, rpc("ping"), session=session, headers=mine).status_code == 404


# ------------------------------------------------------- API keys too


def test_api_keys_and_tokens_coexist(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served(auth=APIKeyAuth({KEY: ["files:write"]}))
    with httpx.Client(base_url=base, timeout=10) as client:
        for headers in (bearer(KEY), {"X-API-Key": KEY}):
            written = stateless(client, modern_call("write_file", path="k"), headers)
            assert written.status_code == 200, written.text
            assert written.json()["result"]["content"][0]["text"] == "wrote k"
        token = bearer(fake_as.mint())
        assert stateless(client, modern_call("status"), token).status_code == 200
        # Keys keep hiding what they cannot call; required_scopes is for tokens.
        listed = stateless(client, modern("tools/list"), {"X-API-Key": KEY})
        names = [tool["name"] for tool in listed.json()["result"]["tools"]]
        assert names == ["read_file", "status", "whoami", "write_file"]
        assert stateless(client, modern("tools/list")).status_code == 401
        session = open_session(client, {"X-API-Key": KEY})
        listed = post(client, rpc("tools/list"), session=session, headers={"X-API-Key": KEY})
        assert listed.status_code == 200


def test_x_api_key_never_verified_as_token(
    live_server: LiveServer, fake_as: FakeAuthorizationServer
) -> None:
    introspection = Introspection(CLIENT_ID, CLIENT_SECRET, endpoint=f"{fake_as.issuer}/introspect")
    server = make_server(fake_as, auth=APIKeyAuth({KEY: "*"}), introspection=introspection)
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        response = stateless(client, modern("tools/list"), {"X-API-Key": "made-up-value-123456"})
        assert response.status_code == 401
        assert rpc_body(response)["error"]["message"] == "Invalid API key"
        # Where to sign in instead; no error code, as no token was presented.
        assert challenge(response) == f'Bearer scope="mcp:access", {RM}'
    assert fake_as.counters["introspect"] == 0


def test_bad_api_key_with_oauth_gets_invalid_token_challenge(
    served: Any, fake_as: FakeAuthorizationServer
) -> None:
    base, _ = served(auth=APIKeyAuth({KEY: "*"}))
    with httpx.Client(base_url=base, timeout=10) as client:
        response = stateless(client, modern("tools/list"), bearer("wrong-key-000000000000"))
        assert response.status_code == 401
        assert challenge(response).startswith('Bearer error="invalid_token"')


# ------------------------------------------------------ what is read


def test_two_authorization_headers_400(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    token = fake_as.mint()
    with httpx.Client(base_url=base, timeout=10) as client:
        message = modern("tools/list")
        headers = [*headers_for(message).items(), ("Authorization", f"Bearer {token}")]
        headers.append(("Authorization", f"Bearer {token}"))
        response = client.post("/mcp", json=message, headers=headers)
        assert response.status_code == 400
        assert challenge(response) == f'Bearer error="invalid_request", {RM}'
        assert rpc_body(response)["error"]["code"] == INVALID_REQUEST


def test_query_and_body_tokens_ignored(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    token = fake_as.mint()
    with httpx.Client(base_url=base, timeout=10) as client:
        message = modern("tools/list")
        in_query = client.post(
            "/mcp", params={"access_token": token}, json=message, headers=headers_for(message)
        )
        assert in_query.status_code == 401
        assert "error=" not in challenge(in_query)
        in_body = modern("tools/list", {"access_token": token})
        assert stateless(client, in_body).status_code == 401


def test_non_bearer_scheme_treated_as_missing(
    served: Any, fake_as: FakeAuthorizationServer
) -> None:
    base, _ = served()
    token = fake_as.mint()
    with httpx.Client(base_url=base, timeout=10) as client:
        for scheme in ("DPoP", "Basic", "Token"):
            response = stateless(
                client, modern("tools/list"), {"Authorization": f"{scheme} {token}"}
            )
            assert response.status_code == 401
            assert challenge(response) == f'Bearer scope="mcp:access", {RM}'
        lowercase = stateless(client, modern("tools/list"), {"Authorization": f"bearer {token}"})
        assert lowercase.status_code == 200


# ------------------------------------------------------- availability


def test_failed_auth_throttle_429(
    live_server: LiveServer, fake_as: FakeAuthorizationServer, logs: LogCapture
) -> None:
    introspection = Introspection(CLIENT_ID, CLIENT_SECRET, endpoint=f"{fake_as.issuer}/introspect")
    server = make_server(fake_as, introspection=introspection, rate_limit_per_minute=3)
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        for index in range(3):
            bad = stateless(client, modern("tools/list"), bearer(f"bad-token-{index}"))
            assert bad.status_code == 401
        assert fake_as.counters["introspect"] == 3
        throttled = stateless(client, modern("tools/list"), bearer("bad-token-4"))
        assert throttled.status_code == 429
        assert int(throttled.headers["retry-after"]) >= 1
        assert rpc_body(throttled)["error"]["code"] == RATE_LIMITED
        assert fake_as.counters["introspect"] == 3  # refused without verifying
        # A missing token is a normal first request: never throttled.
        assert stateless(client, modern("tools/list")).status_code == 401
    assert len(logs.events("auth_rate_limited")) == 1
    assert [event["reason"] for event in logs.events("auth_failed")] == ["inactive"] * 3


def test_auth_server_down_503(live_server: LiveServer, fake_as: FakeAuthorizationServer) -> None:
    fake_as.fail("jwks", 500)
    base = live_server(make_server(fake_as))
    with httpx.Client(base_url=base, timeout=10) as client:
        response = stateless(client, modern("tools/list"), bearer(fake_as.mint()))
        assert response.status_code == 503
        assert response.headers["retry-after"] == "5"
        assert "www-authenticate" not in response.headers
        assert rpc_body(response)["error"] == {
            "code": SERVER_BUSY,
            "message": "Authorization server unavailable; retry shortly",
            "data": {"reason": "auth_server_unavailable"},
        }
        assert client.get("/healthz").json()["oauth"] == "unavailable"
        assert client.get("/healthz").status_code == 200


# ---------------------------------------------------------- legacy SSE


def next_data(lines: Iterator[str]) -> str:
    for line in lines:
        if line.startswith("data: "):
            return line[len("data: ") :]
    raise AssertionError("SSE stream ended without a data event")


def test_legacy_sse_with_oauth(served: Any, fake_as: FakeAuthorizationServer) -> None:
    base, _ = served()
    token = bearer(fake_as.mint())
    stranger = bearer(fake_as.mint(claims={"sub": "user-2"}))
    with httpx.Client(base_url=base, timeout=10) as client:
        refused = client.get("/sse")
        assert refused.status_code == 401
        assert challenge(refused) == f'Bearer scope="mcp:access", {RM}'
        assert client.get("/sse", headers=bearer("garbage")).status_code == 401
        with client.stream("GET", "/sse", headers=token) as stream:
            lines = stream.iter_lines()
            endpoint = next_data(lines)
            assert client.post(endpoint, json=rpc("ping")).status_code == 401
            assert client.post(endpoint, json=rpc("ping"), headers=stranger).status_code == 403
            init = {"protocolVersion": "2024-11-05", "capabilities": {}}
            assert (
                client.post(endpoint, json=rpc("initialize", init), headers=token).status_code
                == 202
            )
            assert json.loads(next_data(lines))["result"]["protocolVersion"] == "2024-11-05"
            posted = client.post(endpoint, json=call("write_file", 5, path="a"), headers=token)
            assert posted.status_code == 202  # the stream carries the refusal
            reply = json.loads(next_data(lines))
            assert reply["id"] == 5
            assert reply["error"]["code"] == AUTHENTICATION_REQUIRED
            assert reply["error"]["data"] == {"error": "insufficient_scope", "scope": "files:write"}
            # A refreshed token keeps the stream (same principal).
            refreshed = bearer(
                fake_as.mint(now=time.time() + 5, claims={"scope": "mcp:access files:write"})
            )
            written = client.post(endpoint, json=call("write_file", 6, path="a"), headers=refreshed)
            assert written.status_code == 202
            assert json.loads(next_data(lines))["result"]["content"][0]["text"] == "wrote a"


# --------------------------------------------------------- the record


def test_no_token_or_secret_in_logs(
    live_server: LiveServer, fake_as: FakeAuthorizationServer, logs: LogCapture
) -> None:
    jwt_base = live_server(make_server(fake_as))
    introspection = Introspection(CLIENT_ID, CLIENT_SECRET, endpoint=f"{fake_as.issuer}/introspect")
    opaque_base = live_server(make_server(fake_as, introspection=introspection))
    good = fake_as.mint()
    expired = fake_as.mint(claims={"exp": int(time.time()) - 120})
    opaque = "opaque-token-abcdefghijklmnop"
    fake_as.set_introspection(
        opaque,
        {"active": True, "aud": OAUTH_RESOURCE, "sub": "user-9", "scope": "mcp:access"},
    )
    with httpx.Client(base_url=jwt_base, timeout=10) as client:
        for token in (good, expired, good + "x"):
            stateless(client, modern_call("write_file", path="a"), bearer(token))
    with httpx.Client(base_url=opaque_base, timeout=10) as client:
        stateless(client, modern_call("status"), bearer(opaque))
        stateless(client, modern_call("status"), bearer(opaque + "-bad"))
    everything = logs.text + json.dumps(
        [getattr(record, "event", None) for record in logs.records], default=str
    )
    for secret in (good, expired, opaque, CLIENT_SECRET):
        assert secret not in everything
    assert logs.events("auth_failed")  # the attempts were recorded, by fingerprint


def test_audit_events(served: Any, fake_as: FakeAuthorizationServer, logs: LogCapture) -> None:
    base, _ = served()
    with httpx.Client(base_url=base, timeout=10) as client:
        stateless(client, modern("tools/list"), bearer("garbage"))
        stateless(client, modern("tools/list"), bearer(fake_as.mint(issuer="https://evil.example")))
        expired = fake_as.mint(claims={"exp": int(time.time()) - 120})
        stateless(client, modern("tools/list"), bearer(expired))
        for _ in range(3):
            stateless(client, modern("tools/list"), bearer(fake_as.mint()))
        stateless(client, modern("tools/list"), bearer(fake_as.mint(claims={"sub": "user-2"})))
        stateless(client, modern_call("write_file", path="a"), bearer(fake_as.mint()))
    failed = logs.events("auth_failed")
    assert [event["reason"] for event in failed] == ["malformed", "wrong_issuer", "expired"]
    for event in failed:
        assert event["transport"] == "streamable-http"
        assert event["client_id"] == "ip:127.0.0.1"
        assert len(event["token_fp"]) == 12
    assert "issuer" not in failed[1]  # not a configured issuer: never echoed
    assert failed[2]["issuer"] == fake_as.issuer
    seen = logs.events("principal_seen")
    assert [event["subject"] for event in seen] == ["user-1", "user-2"]
    (denied,) = logs.events("tool_denied")
    assert denied["scope"] == "files:write"
    assert denied["client_id"] == principal_fingerprint(fake_as.issuer, "user-1", "client-1")


# ------------------------------------------- without oauth, as in 0.3.1


def test_api_key_only_server_unchanged(live_server: LiveServer) -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None, auth=APIKeyAuth({KEY: ["admin"]}))

    @server.tool(scopes=("admin",))
    def secret() -> str:
        """Protected."""
        return "s3cr3t"

    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        bad = post(client, rpc("initialize", INIT), headers=bearer("wrong-key-0000000000"))
        assert bad.status_code == 401
        assert "www-authenticate" not in bad.headers
        assert bad.content == (
            b'{"jsonrpc":"2.0","id":null,"error":{"code":-32001,"message":"Invalid API key"}}'
        )
        assert client.get("/sse", headers=bearer("wrong-key-0000000000")).json() == {
            "error": "invalid API key"
        }
        anonymous = stateless(client, modern("tools/list"))
        assert anonymous.status_code == 200
        assert [tool["name"] for tool in anonymous.json()["result"]["tools"]] == []
        # A token-looking value is just a wrong key here.
        token_like = stateless(client, modern("tools/list"), bearer("a.b.c"))
        assert token_like.status_code == 401 and "www-authenticate" not in token_like.headers
        assert "oauth" not in client.get("/healthz").json()
        assert client.get("/.well-known/oauth-protected-resource").status_code == 404
        doubled = [*headers_for(modern("tools/list")).items()]
        doubled += [("Authorization", f"Bearer {KEY}"), ("Authorization", "Bearer x")]
        twice = client.post("/mcp", json=modern("tools/list"), headers=doubled)
        assert twice.status_code == 200  # the first is used, as before
        assert [tool["name"] for tool in twice.json()["result"]["tools"]] == ["secret"]


# ------------------------------------------------------------ lifecycle


def test_build_app_lifespan_warms_up(
    live_server: LiveServer, fake_as: FakeAuthorizationServer
) -> None:
    base = live_server(make_server(fake_as))
    # uvicorn has run the lifespan startup: the keys are here already.
    assert fake_as.counters["jwks"] == 1
    with httpx.Client(base_url=base, timeout=10) as client:
        assert client.get("/healthz").json()["oauth"] == "ok"
        response = stateless(client, modern("tools/list"), bearer(fake_as.mint()))
        assert response.status_code == 200
    assert fake_as.counters["jwks"] == 1
    assert fake_as.counters["rfc8414"] == 1


def test_sse_only_app_serves_oauth(
    live_server: LiveServer, fake_as: FakeAuthorizationServer
) -> None:
    oauth = OAuthResourceServer("https://mcp.example.com", [fake_as.issuer])
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth)
    base = live_server(SSETransport(server).build_app())
    assert fake_as.counters["jwks"] == 1
    with httpx.Client(base_url=base, timeout=10) as client:
        assert client.get("/.well-known/oauth-protected-resource").status_code == 200
        assert client.get("/sse").status_code == 401


def test_mounted_app_runs_the_server_lifespan(
    live_server: LiveServer, fake_as: FakeAuthorizationServer
) -> None:
    import contextlib

    from starlette.applications import Starlette
    from starlette.routing import Mount

    server = make_server(fake_as)
    mcp_app = server.build_app()

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> Any:
        async with server.lifespan():
            yield

    host = Starlette(routes=[Mount("/", mcp_app)], lifespan=lifespan)
    base = live_server(host)
    assert fake_as.counters["jwks"] == 1
    with httpx.Client(base_url=base, timeout=10) as client:
        assert stateless(client, modern_call("status"), bearer(fake_as.mint())).status_code == 200


def test_well_known_endpoint_path_refused(fake_as: FakeAuthorizationServer) -> None:
    server = make_server(fake_as)
    for path in ("/.well-known", "/.well-known/mcp"):
        with pytest.raises(ValueError, match="well-known"):
            StreamableHTTPTransport(server, path=path)


def test_startup_warnings(fake_as: FakeAuthorizationServer, logs: LogCapture) -> None:
    server = make_server(fake_as, step_up=False)
    server._transport = StreamableHTTPTransport(server, path="/api")
    server._warn_if_misconfigured()
    assert "names the path '/mcp', but the MCP endpoint is /api" in logs.text
    assert "plain http" in logs.text
    assert "2 tool(s) are hidden from tokens that lack their scope" in logs.text
    assert "require authentication but no auth is configured" not in logs.text
