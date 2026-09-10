"""End-to-end Streamable HTTP transport tests against a real uvicorn server."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import notification, rpc

from easy_mcp import APIKeyAuth, MCPServer, SSETransport, StreamableHTTPTransport
from easy_mcp.exceptions import (
    AUTHENTICATION_REQUIRED,
    FORBIDDEN,
    INVALID_PARAMS,
    INVALID_REQUEST,
    PARSE_ERROR,
    PAYLOAD_TOO_LARGE,
    RATE_LIMITED,
    TOO_MANY_SESSIONS,
)

LiveServer = Callable[[Any], str]

KEY = "streamable-test-key-" + "k" * 12
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "tests", "version": "1.0"},
}


def make_server(**kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def post(
    client: httpx.Client,
    message: Any,
    session: str | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    all_headers = {**ACCEPT, **(headers or {})}
    if session is not None:
        all_headers["MCP-Session-Id"] = session
    return client.post("/mcp", json=message, headers=all_headers)


def open_session(client: httpx.Client, headers: dict[str, str] | None = None) -> str:
    """Run the initialize handshake; returns the session id."""
    init = post(client, rpc("initialize", INIT), headers=headers)
    assert init.status_code == 200, init.text
    session = init.headers["mcp-session-id"]
    initialized = post(client, notification("notifications/initialized"), session, headers)
    assert initialized.status_code == 202
    return session


def error_code(response: httpx.Response) -> int:
    """The code of an HTTP-level rejection's JSON-RPC error body."""
    body = response.json()
    assert body["id"] is None
    return int(body["error"]["code"])


def test_roundtrip(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        init = post(client, rpc("initialize", INIT))
        assert init.status_code == 200
        assert init.headers["content-type"] == "application/json"
        session = init.headers["mcp-session-id"]
        assert len(session) >= 32
        assert all(0x21 <= ord(char) <= 0x7E for char in session)  # visible ASCII only
        assert init.json()["result"]["protocolVersion"] == "2025-11-25"
        assert init.json()["result"]["serverInfo"]["name"] == "easy-mcp"

        initialized = post(client, notification("notifications/initialized"), session)
        assert initialized.status_code == 202
        assert initialized.content == b""

        version = {"MCP-Protocol-Version": "2025-11-25"}
        listed = post(client, rpc("tools/list", msg_id=2), session, version)
        assert [tool["name"] for tool in listed.json()["result"]["tools"]] == ["add"]

        called = post(
            client, rpc("tools/call", {"name": "add", "arguments": {"a": 2, "b": 3}}, 3), session
        )
        assert called.json() == {
            "jsonrpc": "2.0",
            "id": 3,
            "result": {"content": [{"type": "text", "text": "5"}], "isError": False},
        }

        # JSON-RPC errors travel in an ordinary 200 response.
        missing_b = rpc("tools/call", {"name": "add", "arguments": {"a": 1}}, 4)
        invalid = post(client, missing_b, session)
        assert invalid.status_code == 200
        assert invalid.json()["error"]["code"] == INVALID_PARAMS

        assert client.delete("/mcp", headers={"MCP-Session-Id": session}).status_code == 204
        assert post(client, rpc("ping", msg_id=5), session).status_code == 404


def test_session_header_rules(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        missing = post(client, rpc("ping"))
        assert missing.status_code == 400
        assert error_code(missing) == INVALID_REQUEST
        assert post(client, rpc("ping"), "not-a-real-session").status_code == 404
        assert client.delete("/mcp").status_code == 400

        session = open_session(client)
        wrong_version = post(client, rpc("ping"), session, {"MCP-Protocol-Version": "1999-01-01"})
        assert wrong_version.status_code == 400
        assert "2025-11-25" in wrong_version.json()["error"]["message"]

        # A reply to a server-to-client request is accepted and ignored.
        assert post(client, {"jsonrpc": "2.0", "id": 9, "result": {}}, session).status_code == 202


def test_http_level_rejections(live_server: LiveServer) -> None:
    base = live_server(make_server(max_request_bytes=200))
    with httpx.Client(base_url=base, timeout=10) as client:
        get = client.get("/mcp", headers={"Accept": "text/event-stream"})
        assert get.status_code == 405
        assert "POST" in get.headers["allow"]

        plain = client.post("/mcp", content=b"{}", headers={**ACCEPT, "Content-Type": "text/plain"})
        assert plain.status_code == 415

        sse_only = client.post(
            "/mcp", json=rpc("initialize", INIT), headers={"Accept": "text/event-stream"}
        )
        assert sse_only.status_code == 406

        garbage = client.post(
            "/mcp", content=b"{not json", headers={**ACCEPT, "Content-Type": "application/json"}
        )
        assert garbage.status_code == 400
        assert error_code(garbage) == PARSE_ERROR

        batch = post(client, [rpc("ping", msg_id=1), rpc("ping", msg_id=2)])
        assert batch.status_code == 400
        assert error_code(batch) == INVALID_REQUEST

        oversized = post(client, rpc("ping", {"pad": "x" * 500}))
        assert oversized.status_code == 413
        assert error_code(oversized) == PAYLOAD_TOO_LARGE


def test_origin_validation(live_server: LiveServer) -> None:
    base = live_server(make_server())
    init = rpc("initialize", INIT)
    with httpx.Client(base_url=base, timeout=10) as client:
        evil = {"Origin": "http://evil.example:8000"}
        rejected = post(client, init, headers=evil)
        assert rejected.status_code == 403
        assert error_code(rejected) == FORBIDDEN
        # The legacy SSE endpoints sit behind the same check.
        assert client.get("/sse", headers=evil).status_code == 403
        legacy_post = client.post(
            "/messages", params={"session_id": "x"}, json=rpc("ping"), headers=evil
        )
        assert legacy_post.status_code == 403

        for origin in ("http://localhost:6274", "http://127.0.0.1:8000", "https://[::1]"):
            assert post(client, init, headers={"Origin": origin}).status_code == 200
        for origin in ("http://localhost.evil.example", "null", "file://"):
            assert post(client, init, headers={"Origin": origin}).status_code == 403


def test_allowed_origins_setting(live_server: LiveServer) -> None:
    init = rpc("initialize", INIT)
    base = live_server(make_server(allowed_origins=["https://App.Example.com/"]))
    with httpx.Client(base_url=base, timeout=10) as client:
        assert post(client, init, headers={"Origin": "https://app.example.com"}).status_code == 200
        assert post(client, init, headers={"Origin": "http://localhost:6274"}).status_code == 403
        assert post(client, init).status_code == 200  # non-browser clients send no Origin

    base = live_server(make_server(allowed_origins=["*"]))
    with httpx.Client(base_url=base, timeout=10) as client:
        assert post(client, init, headers={"Origin": "https://any.example"}).status_code == 200

    with pytest.raises(ValueError, match="allowed origin"):
        MCPServer(allowed_origins=["example.com"])


def test_auth_and_session_binding(live_server: LiveServer) -> None:
    server = make_server(auth=APIKeyAuth({KEY: ["admin"]}))

    @server.tool(scopes=("admin",))
    def secret() -> str:
        """Protected tool."""
        return "s3cr3t"

    base = live_server(server)
    key = {"Authorization": f"Bearer {KEY}"}
    with httpx.Client(base_url=base, timeout=10) as client:
        wrong = {"Authorization": "Bearer wrong-key-0000000000"}
        bad = post(client, rpc("initialize", INIT), headers=wrong)
        assert bad.status_code == 401
        assert error_code(bad) == AUTHENTICATION_REQUIRED

        anonymous = open_session(client)
        listed = post(client, rpc("tools/list"), anonymous)
        assert [tool["name"] for tool in listed.json()["result"]["tools"]] == ["add"]
        # A session answers only the credential it was opened with.
        assert post(client, rpc("tools/list"), anonymous, key).status_code == 403

        authed = open_session(client, key)
        assert post(client, rpc("ping"), authed).status_code == 403
        assert client.delete("/mcp", headers={"MCP-Session-Id": authed}).status_code == 403
        called = post(client, rpc("tools/call", {"name": "secret"}), authed, key)
        assert called.json()["result"]["content"][0]["text"] == "s3cr3t"


async def test_cancel_and_delete_abort_running_calls(live_server: LiveServer) -> None:
    server = make_server()
    started = threading.Event()

    @server.tool
    async def slow() -> str:
        """Sleep for a long time."""
        started.set()
        await asyncio.sleep(30)
        return "done"

    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:

        async def apost(message: Any, session: str | None = None) -> httpx.Response:
            headers = dict(ACCEPT)
            if session is not None:
                headers["MCP-Session-Id"] = session
            return await client.post("/mcp", json=message, headers=headers)

        session = (await apost(rpc("initialize", INIT))).headers["mcp-session-id"]

        call = asyncio.create_task(apost(rpc("tools/call", {"name": "slow"}, 7), session))
        assert await asyncio.to_thread(started.wait, 5)
        cancel = await apost(notification("notifications/cancelled", {"requestId": 7}), session)
        assert cancel.status_code == 202
        # Per MCP a cancelled request gets no JSON-RPC reply.
        assert (await asyncio.wait_for(call, 5)).status_code == 202

        started.clear()
        call = asyncio.create_task(apost(rpc("tools/call", {"name": "slow"}, 8), session))
        assert await asyncio.to_thread(started.wait, 5)
        deleted = await client.delete("/mcp", headers={"MCP-Session-Id": session})
        assert deleted.status_code == 204
        assert (await asyncio.wait_for(call, 5)).status_code == 202


def test_session_cap_and_idle_expiry(live_server: LiveServer) -> None:
    transport = StreamableHTTPTransport(make_server(max_sessions=1), session_idle_timeout=0.3)
    base = live_server(transport.build_app())
    with httpx.Client(base_url=base, timeout=10) as client:
        first = open_session(client)
        full = post(client, rpc("initialize", INIT))
        assert full.status_code == 503
        assert error_code(full) == TOO_MANY_SESSIONS

        time.sleep(0.6)
        # The idle session expired: its id is gone and its slot is free.
        assert post(client, rpc("ping"), first).status_code == 404
        second = open_session(client)
        assert client.delete("/mcp", headers={"MCP-Session-Id": second}).status_code == 204
        open_session(client)  # the delete freed the slot again


def test_failed_initialize_opens_no_session(live_server: LiveServer) -> None:
    base = live_server(MCPServer(port=0, rate_limit_per_minute=1))
    with httpx.Client(base_url=base, timeout=10) as client:
        assert post(client, rpc("initialize", INIT)).status_code == 200
        limited = post(client, rpc("initialize", INIT))
        assert limited.json()["error"]["code"] == RATE_LIMITED
        assert "mcp-session-id" not in limited.headers


def test_legacy_sse_can_be_disabled(live_server: LiveServer) -> None:
    transport = StreamableHTTPTransport(make_server(), legacy_sse=False)
    base = live_server(transport.build_app())
    with httpx.Client(base_url=base, timeout=10) as client:
        assert client.get("/sse").status_code == 404
        assert client.get("/healthz").json()["status"] == "ok"
        open_session(client)


def test_transport_selection() -> None:
    server = make_server()
    for name in (None, "http", "streamable-http"):
        assert isinstance(server._resolve_transport(name), StreamableHTTPTransport)
    assert isinstance(server._resolve_transport("sse"), SSETransport)
    with pytest.raises(ValueError, match="collides"):
        StreamableHTTPTransport(server, path="/sse")
    with pytest.raises(ValueError, match="session_idle_timeout"):
        StreamableHTTPTransport(server, session_idle_timeout=0)
