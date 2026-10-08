"""OAuth against the real world: a real authorization server, and the official SDK client.

Skipped unless switched on:

* ``EASY_MCP_LIVE_OAUTH_ISSUER``, ``EASY_MCP_LIVE_OAUTH_RESOURCE`` and
  ``EASY_MCP_LIVE_OAUTH_TOKEN`` point at whatever authorization server you
  have: its issuer, the resource (audience) it issued the token for, and a
  fresh access token.  Optional: ``EASY_MCP_LIVE_OAUTH_AUDIENCE`` (when the
  server maps the resource to another audience) and
  ``EASY_MCP_LIVE_OAUTH_INTROSPECTION_CLIENT_ID`` /
  ``..._INTROSPECTION_CLIENT_SECRET`` to test introspection too.  These catch
  real claim shapes (``scp``, ``azp``, ``typ: JWT``) the local server cannot.
* ``EASY_MCP_LIVE_SDK_CLIENT=1`` with the official MCP Python SDK installed
  runs its OAuth client against this server and the local authorization
  server of tests/oauth_fake_as.py: the 401, the metadata, PKCE, the token
  with ``resource``, then a step-up after a 403, in auto-detect, pinned
  2026-07-28 and legacy modes.
"""

from __future__ import annotations

import contextlib
import os
import socket
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx
import live_sdk
import pytest
import uvicorn
from conftest import LogCapture, headers_for, modern, rpc
from oauth_fake_as import FakeAuthorizationServer

from easy_mcp import Introspection, MCPServer, OAuthResourceServer, current_identity

LiveServer = Callable[[Any], str]

ISSUER = os.environ.get("EASY_MCP_LIVE_OAUTH_ISSUER")
RESOURCE = os.environ.get("EASY_MCP_LIVE_OAUTH_RESOURCE")
TOKEN = os.environ.get("EASY_MCP_LIVE_OAUTH_TOKEN")
AUDIENCE = os.environ.get("EASY_MCP_LIVE_OAUTH_AUDIENCE")
INTROSPECTION_ID = os.environ.get("EASY_MCP_LIVE_OAUTH_INTROSPECTION_CLIENT_ID")
INTROSPECTION_SECRET = os.environ.get("EASY_MCP_LIVE_OAUTH_INTROSPECTION_CLIENT_SECRET")

real_as = pytest.mark.skipif(
    not (ISSUER and RESOURCE and TOKEN),
    reason="EASY_MCP_LIVE_OAUTH_ISSUER, _RESOURCE and _TOKEN are not set",
)

ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "live"}}


def modes() -> list[str]:
    """Verification modes the configured token can be tested in."""
    found = []
    if TOKEN and TOKEN.count(".") == 2:
        found.append("jwt")
    if INTROSPECTION_ID and INTROSPECTION_SECRET:
        found.append("introspection")
    return found


def real_server(mode: str, resource: str | None = None, audience: str | None = None) -> MCPServer:
    assert ISSUER and RESOURCE
    introspection = (
        Introspection(INTROSPECTION_ID or "", INTROSPECTION_SECRET or "")
        if mode == "introspection"
        else None
    )
    oauth = OAuthResourceServer(
        resource or RESOURCE, [ISSUER], audience=audience, introspection=introspection
    )
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth)

    @server.tool
    def whoami() -> dict[str, Any]:
        """The verified caller."""
        who = current_identity()
        assert who is not None
        return {"subject": who.subject, "client_id": who.client_id, "issuer": who.issuer}

    return server


def stateless_whoami(base: str, token: str) -> httpx.Response:
    message = modern("tools/call", {"name": "whoami", "arguments": {}})
    headers = {**headers_for(message), "Authorization": f"Bearer {token}"}
    with httpx.Client(base_url=base, timeout=30) as client:
        return client.post("/mcp", json=message, headers=headers)


@real_as
@pytest.mark.parametrize("mode", modes() or ["jwt"])
def test_live_real_token_verifies_over_streamable_http(live_server: LiveServer, mode: str) -> None:
    if mode not in modes():
        pytest.skip("the token is not a JWT and no introspection credentials are set")
    assert TOKEN
    base = live_server(real_server(mode, audience=AUDIENCE))
    called = stateless_whoami(base, TOKEN)
    assert called.status_code == 200, called.text
    who = called.json()["result"]["structuredContent"]
    assert who["issuer"] == ISSUER
    assert who["subject"] or who["client_id"]
    with httpx.Client(base_url=base, timeout=30) as client:
        init = client.post(
            "/mcp",
            json=rpc("initialize", INIT),
            headers={**ACCEPT, "Authorization": f"Bearer {TOKEN}"},
        )
        assert init.status_code == 200, init.text
        assert "mcp-session-id" in init.headers


@real_as
@pytest.mark.parametrize("mode", modes() or ["jwt"])
def test_live_tampered_token_rejected(live_server: LiveServer, logs: LogCapture, mode: str) -> None:
    if mode not in modes():
        pytest.skip("the token is not a JWT and no introspection credentials are set")
    assert TOKEN
    if mode == "jwt":
        head, payload, signature = TOKEN.split(".")
        middle = len(signature) // 2
        flipped = "A" if signature[middle] != "A" else "B"
        tampered = f"{head}.{payload}.{signature[:middle]}{flipped}{signature[middle + 1 :]}"
    else:
        middle = len(TOKEN) // 2
        flipped = "A" if TOKEN[middle] != "A" else "B"
        tampered = f"{TOKEN[:middle]}{flipped}{TOKEN[middle + 1 :]}"
    base = live_server(real_server(mode, audience=AUDIENCE))
    refused = stateless_whoami(base, tampered)
    assert refused.status_code == 401
    assert 'error="invalid_token"' in refused.headers["www-authenticate"]
    reasons = [event["reason"] for event in logs.events("auth_failed")]
    assert reasons == ["bad_signature" if mode == "jwt" else "inactive"]


@real_as
@pytest.mark.parametrize("mode", modes() or ["jwt"])
def test_live_wrong_resource_rejected(live_server: LiveServer, logs: LogCapture, mode: str) -> None:
    if mode not in modes():
        pytest.skip("the token is not a JWT and no introspection credentials are set")
    assert TOKEN
    base = live_server(real_server(mode, resource="https://wrong-resource.example.com/mcp"))
    refused = stateless_whoami(base, TOKEN)
    assert refused.status_code == 401
    assert [event["reason"] for event in logs.events("auth_failed")] == ["wrong_audience"]


# ------------------------------------------------------------------ SDK


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@contextlib.contextmanager
def serve_on(port: int, app: Any) -> Iterator[str]:
    """Serve *app* on a port chosen beforehand: the resource must name it."""
    uv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=uv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not uv.started:
        if time.time() > deadline:
            raise RuntimeError("uvicorn failed to start within 10s")
        time.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        uv.should_exit = True
        thread.join(timeout=5)


class MemoryStorage:
    """The SDK client's token storage, in memory."""

    def __init__(self) -> None:
        self.tokens: Any = None
        self.client_info: Any = None

    async def get_tokens(self) -> Any:
        return self.tokens

    async def set_tokens(self, tokens: Any) -> None:
        self.tokens = tokens

    async def get_client_info(self) -> Any:
        return self.client_info

    async def set_client_info(self, client_info: Any) -> None:
        self.client_info = client_info


class Browser:
    """Plays the user: opens the authorization URL and hands the code back."""

    def __init__(self, result_type: Any) -> None:
        self.result_type = result_type
        self.urls: list[str] = []
        self.result: Any = None

    async def redirect(self, url: str) -> None:
        self.urls.append(url)
        async with httpx.AsyncClient(timeout=30) as client:
            approved = await client.get(url, follow_redirects=False)
        assert approved.status_code == 302, approved.text
        query = dict(parse_qsl(urlsplit(approved.headers["location"]).query))
        self.result = self.result_type(
            code=query["code"], state=query.get("state"), iss=query.get("iss")
        )

    async def callback(self) -> Any:
        result, self.result = self.result, None
        assert result is not None, "the client asked for a code before opening the browser"
        return result


def sdk_server(resource: str, issuer: str) -> MCPServer:
    oauth = OAuthResourceServer(resource, [issuer], required_scopes=["mcp:access"])
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth)

    @server.tool
    def whoami() -> str:
        """The verified caller."""
        who = current_identity()
        return (who.subject or "") if who is not None else ""

    @server.tool(scopes=("files:write",))
    def write_file(path: str) -> str:
        """Write a file."""
        return f"wrote {path}"

    return server


@live_sdk.marker
@pytest.mark.parametrize("mode", ["auto", "2026-07-28", "legacy"])
async def test_live_sdk_client_completes_oauth_against_fake_as(
    fake_as: FakeAuthorizationServer, mode: str
) -> None:
    live_sdk.require_sdk()
    httpx2 = pytest.importorskip("httpx2")
    from mcp import Client
    from mcp.client.auth import AuthorizationCodeResult, OAuthClientProvider
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared.auth import OAuthClientMetadata

    port = free_port()
    resource = f"http://127.0.0.1:{port}/mcp"
    server = sdk_server(resource, fake_as.issuer)
    with serve_on(port, server.build_app()) as base:
        browser = Browser(AuthorizationCodeResult)
        provider = OAuthClientProvider(
            server_url=f"{base}/mcp",
            client_metadata=OAuthClientMetadata(
                client_name="easy-mcp interop",
                redirect_uris=["http://127.0.0.1:9/callback"],
            ),
            storage=MemoryStorage(),
            redirect_handler=browser.redirect,
            callback_handler=browser.callback,
        )
        async with httpx2.AsyncClient(auth=provider, timeout=30) as http_client:
            transport = streamable_http_client(f"{base}/mcp", http_client=http_client)
            async with Client(transport, mode=mode) as client:
                tools = await client.list_tools()
                assert {tool.name for tool in tools.tools} == {"whoami", "write_file"}
                result = await client.call_tool("whoami", {})
                assert result.content[0].text == "user-1"
        assert len(browser.urls) == 1
        query = dict(parse_qsl(urlsplit(browser.urls[0]).query))
        assert query["resource"] == resource
        assert query["code_challenge_method"] == "S256"
        assert "mcp:access" in query.get("scope", "")


@live_sdk.marker
@pytest.mark.parametrize("mode", ["auto", "2026-07-28", "legacy"])
async def test_live_sdk_client_steps_up(fake_as: FakeAuthorizationServer, mode: str) -> None:
    live_sdk.require_sdk()
    httpx2 = pytest.importorskip("httpx2")
    from mcp import Client
    from mcp.client.auth import AuthorizationCodeResult, OAuthClientProvider
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared.auth import OAuthClientMetadata

    port = free_port()
    resource = f"http://127.0.0.1:{port}/mcp"
    server = sdk_server(resource, fake_as.issuer)
    with serve_on(port, server.build_app()) as base:
        browser = Browser(AuthorizationCodeResult)
        provider = OAuthClientProvider(
            server_url=f"{base}/mcp",
            client_metadata=OAuthClientMetadata(
                client_name="easy-mcp interop",
                redirect_uris=["http://127.0.0.1:9/callback"],
            ),
            storage=MemoryStorage(),
            redirect_handler=browser.redirect,
            callback_handler=browser.callback,
        )
        async with httpx2.AsyncClient(auth=provider, timeout=30) as http_client:
            transport = streamable_http_client(f"{base}/mcp", http_client=http_client)
            async with Client(transport, mode=mode) as client:
                # The first token holds mcp:access only: 403, then a second sign-in.
                result = await client.call_tool("write_file", {"path": "a"})
                assert result.content[0].text == "wrote a"
        assert len(browser.urls) == 2
        second = dict(parse_qsl(urlsplit(browser.urls[1]).query))
        assert set(second["scope"].split()) >= {"mcp:access", "files:write"}
