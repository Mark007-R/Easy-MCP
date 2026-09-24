"""The stateless protocol revision (2026-07-28) alongside the initialize era."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
from conftest import make_context, notification, rpc

from easy_mcp import APIKeyAuth, MCPServer, StdioTransport
from easy_mcp.exceptions import (
    HEADER_MISMATCH,
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    SESSION_LIMIT_EXCEEDED,
    UNSUPPORTED_PROTOCOL_VERSION,
)
from easy_mcp.protocol import is_modern_request

LiveServer = Callable[[Any], str]

VERSION = "2026-07-28"
KEY = "stateless-test-key-" + "k" * 13
SERVER_INFO = "io.modelcontextprotocol/serverInfo"


def meta(**overrides: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "io.modelcontextprotocol/protocolVersion": VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "tests", "version": "1.0"},
    }
    fields.update(overrides)
    return {key: value for key, value in fields.items() if value is not None}


def modern(
    method: str, params: dict[str, Any] | None = None, msg_id: Any = 1, **meta_overrides: Any
) -> dict[str, Any]:
    return rpc(method, {**(params or {}), "_meta": meta(**meta_overrides)}, msg_id)


def make_server(**kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @server.tool(max_calls_per_session=2)
    def scarce() -> str:
        """May be called twice per client."""
        return "ok"

    @server.tool(requires_auth=True)
    def secret() -> str:
        """Only for authenticated callers."""
        return "hidden"

    return server


# ---------------------------------------------------------------- dispatch


def test_era_detection() -> None:
    assert is_modern_request("tools/list", {"_meta": meta()})
    # Any one reserved key opts in, so a half-filled request is rejected
    # rather than served under legacy rules.
    assert is_modern_request("tools/list", {"_meta": {"io.modelcontextprotocol/clientInfo": {}}})
    assert is_modern_request("server/discover", {})
    assert not is_modern_request("tools/list", {})
    assert not is_modern_request("tools/call", {"_meta": {"progressToken": 7}})


async def test_discover() -> None:
    server = make_server(name="calc", version="9.9", instructions="Adds numbers.")
    response = await server.dispatch(modern("server/discover"), make_context())
    assert response is not None
    result = response["result"]
    assert result["resultType"] == "complete"
    assert result["supportedVersions"][0] == VERSION
    assert "2025-11-25" in result["supportedVersions"]
    assert result["capabilities"] == {"tools": {"listChanged": False}}
    assert result["instructions"] == "Adds numbers."
    assert result["_meta"][SERVER_INFO] == {"name": "calc", "version": "9.9"}
    assert result["ttlMs"] > 0
    assert result["cacheScope"] == "public"


async def test_tools_list_and_call() -> None:
    server = make_server()
    listed = await server.dispatch(modern("tools/list"), make_context())
    assert listed is not None
    result = listed["result"]
    assert [tool["name"] for tool in result["tools"]] == ["add", "scarce"]
    assert result["resultType"] == "complete"
    assert result["ttlMs"] == 0
    assert result["cacheScope"] == "public"
    assert SERVER_INFO in result["_meta"]

    called = await server.dispatch(
        modern("tools/call", {"name": "add", "arguments": {"a": 2, "b": 3}}), make_context()
    )
    assert called is not None
    assert called["result"]["content"][0]["text"] == "5"
    assert called["result"]["resultType"] == "complete"
    assert SERVER_INFO in called["result"]["_meta"]


async def test_tools_list_is_private_when_it_depends_on_the_caller() -> None:
    server = make_server(auth=APIKeyAuth({KEY: "*"}))
    listed = await server.dispatch(modern("tools/list"), make_context())
    assert listed is not None
    assert listed["result"]["cacheScope"] == "private"


async def test_legacy_requests_are_untouched() -> None:
    server = make_server()
    listed = await server.dispatch(rpc("tools/list"), make_context())
    assert listed is not None
    assert "resultType" not in listed["result"]
    assert "ttlMs" not in listed["result"]
    pong = await server.dispatch(rpc("ping"), make_context())
    assert pong == {"jsonrpc": "2.0", "id": 1, "result": {}}


async def test_missing_required_meta_is_invalid_params() -> None:
    server = make_server()
    no_version = modern("tools/list", **{"io.modelcontextprotocol/protocolVersion": None})
    no_capabilities = modern("tools/list", **{"io.modelcontextprotocol/clientCapabilities": None})
    bad_capabilities = modern("tools/list", **{"io.modelcontextprotocol/clientCapabilities": []})
    for message in (no_version, no_capabilities, bad_capabilities, rpc("server/discover")):
        response = await server.dispatch(message, make_context())
        assert response is not None
        assert response["error"]["code"] == INVALID_PARAMS, message


async def test_unsupported_version_names_the_supported_ones() -> None:
    server = make_server()
    for requested in ("1900-01-01", "2025-11-25"):
        response = await server.dispatch(
            modern("tools/list", **{"io.modelcontextprotocol/protocolVersion": requested}),
            make_context(),
        )
        assert response is not None
        error = response["error"]
        assert error["code"] == UNSUPPORTED_PROTOCOL_VERSION
        assert error["data"]["requested"] == requested
        assert VERSION in error["data"]["supported"]


async def test_handshake_methods_do_not_exist_statelessly() -> None:
    server = make_server()
    for method in ("ping", "initialize", "no/such/method"):
        response = await server.dispatch(modern(method), make_context())
        assert response is not None
        assert response["error"]["code"] == METHOD_NOT_FOUND, method


async def test_tool_errors_keep_their_shape() -> None:
    server = make_server()
    response = await server.dispatch(
        modern("tools/call", {"name": "add", "arguments": {"a": "x", "b": 1}}), make_context()
    )
    assert response is not None
    assert response["error"]["code"] == INVALID_PARAMS
    hidden = await server.dispatch(modern("tools/call", {"name": "secret"}), make_context())
    assert hidden is not None
    assert hidden["error"]["code"] == INVALID_PARAMS  # reported as unknown


# ------------------------------------------------------------------- stdio


async def test_stdio_serves_both_eras_in_one_process() -> None:
    lines = [
        modern("server/discover", msg_id=1),
        modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, msg_id=2),
        rpc("initialize", {"protocolVersion": "2025-11-25"}, msg_id=3),
        rpc("tools/list", msg_id=4),
    ]
    stdin = io.BytesIO(b"".join(json.dumps(m).encode() + b"\n" for m in lines))
    stdout = io.BytesIO()
    await StdioTransport(make_server(), stdin=stdin, stdout=stdout).serve()
    responses = {r["id"]: r for r in map(json.loads, stdout.getvalue().splitlines())}

    assert responses[1]["result"]["supportedVersions"][0] == VERSION
    assert responses[2]["result"]["content"][0]["text"] == "2"
    assert responses[2]["result"]["resultType"] == "complete"
    assert responses[3]["result"]["protocolVersion"] == "2025-11-25"
    assert "resultType" not in responses[4]["result"]


# -------------------------------------------------------------------- HTTP


def headers_for(message: dict[str, Any], **extra: str) -> dict[str, str]:
    """The mirrored headers a conforming client sends with *message*."""
    headers = {
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": VERSION,
        "Mcp-Method": message["method"],
    }
    name = message.get("params", {}).get("name")
    if message["method"] == "tools/call" and name is not None:
        headers["Mcp-Name"] = name
    headers.update(extra)
    return headers


def post(client: httpx.Client, message: dict[str, Any], **extra: str) -> httpx.Response:
    return client.post("/mcp", json=message, headers=headers_for(message, **extra))


def test_http_stateless_roundtrip(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        discovered = post(client, modern("server/discover"))
        assert discovered.status_code == 200
        assert "mcp-session-id" not in discovered.headers  # no session is minted
        assert discovered.json()["result"]["supportedVersions"][0] == VERSION

        listed = post(client, modern("tools/list"))
        assert listed.status_code == 200
        assert [t["name"] for t in listed.json()["result"]["tools"]] == ["add", "scarce"]

        call = modern("tools/call", {"name": "add", "arguments": {"a": 20, "b": 22}})
        called = post(client, call, **{"MCP-Session-Id": "ignored"})
        assert called.status_code == 200
        assert called.json()["result"]["content"][0]["text"] == "42"


def test_http_protocol_errors_carry_http_statuses(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        unknown = post(client, modern("no/such/method"))
        assert unknown.status_code == 404
        assert unknown.json()["error"]["code"] == METHOD_NOT_FOUND

        old = modern("tools/list", **{"io.modelcontextprotocol/protocolVersion": "1900-01-01"})
        unsupported = post(client, old, **{"MCP-Protocol-Version": "1900-01-01"})
        assert unsupported.status_code == 400
        body = unsupported.json()
        assert body["id"] == 1
        assert body["error"]["code"] == UNSUPPORTED_PROTOCOL_VERSION
        assert VERSION in body["error"]["data"]["supported"]

        no_caps = modern("tools/list", **{"io.modelcontextprotocol/clientCapabilities": None})
        missing = post(client, no_caps)
        assert missing.status_code == 400
        assert missing.json()["error"]["code"] == INVALID_PARAMS

        # A tool-level failure is a normal JSON-RPC answer, not an HTTP error.
        bad_args = modern("tools/call", {"name": "add", "arguments": {"a": "x", "b": 1}})
        rejected = post(client, bad_args)
        assert rejected.status_code == 200
        assert rejected.json()["error"]["code"] == INVALID_PARAMS


def test_http_headers_must_match_the_body(live_server: LiveServer) -> None:
    base = live_server(make_server())
    call = modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
    with httpx.Client(base_url=base, timeout=10) as client:
        cases = [
            {"Mcp-Method": "tools/list"},
            {"Mcp-Name": "scarce"},
            {"MCP-Protocol-Version": "2025-11-25"},
            {"Mcp-Name": "=?base64?not base64!?="},
        ]
        for override in cases:
            response = post(client, call, **override)
            assert response.status_code == 400, override
            assert response.json()["error"]["code"] == HEADER_MISMATCH, override
            assert response.json()["id"] == 1

        for header in ("Mcp-Method", "Mcp-Name"):
            headers = headers_for(call)
            del headers[header]
            response = client.post("/mcp", json=call, headers=headers)
            assert response.status_code == 400, header
            assert response.json()["error"]["code"] == HEADER_MISMATCH

        # A modern body without the version header is missing a required header.
        headers = headers_for(call)
        del headers["MCP-Protocol-Version"]
        response = client.post("/mcp", json=call, headers=headers)
        assert response.status_code == 400
        assert response.json()["error"]["code"] == HEADER_MISMATCH

        encoded = "=?base64?" + base64.b64encode(b"add").decode() + "?="
        response = post(client, call, **{"Mcp-Name": encoded})
        assert response.status_code == 200
        assert response.json()["result"]["content"][0]["text"] == "3"


def test_http_modern_header_without_meta_is_rejected(live_server: LiveServer) -> None:
    # The version header alone selects the stateless era, so a body without
    # the per-request fields is malformed rather than a legacy request.
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        message = rpc("tools/list")
        response = client.post("/mcp", json=message, headers=headers_for(message))
        assert response.status_code == 400
        assert response.json()["error"]["code"] == INVALID_PARAMS


def test_http_per_client_limits_survive_statelessness(live_server: LiveServer) -> None:
    base = live_server(make_server())
    call = modern("tools/call", {"name": "scarce"})
    with httpx.Client(base_url=base, timeout=10) as client:
        assert "result" in post(client, call).json()
        assert "result" in post(client, call).json()
        third = post(client, call).json()
        assert third["error"]["code"] == SESSION_LIMIT_EXCEEDED


def test_http_legacy_session_still_works(live_server: LiveServer) -> None:
    base = live_server(make_server())
    accept = {"Accept": "application/json, text/event-stream"}
    with httpx.Client(base_url=base, timeout=10) as client:
        init = client.post(
            "/mcp",
            json=rpc("initialize", {"protocolVersion": "2025-11-25", "capabilities": {}}),
            headers=accept,
        )
        assert init.status_code == 200
        session = init.headers["mcp-session-id"]
        headers = {**accept, "MCP-Session-Id": session, "MCP-Protocol-Version": "2025-11-25"}
        client.post("/mcp", json=notification("notifications/initialized"), headers=headers)
        listed = client.post("/mcp", json=rpc("tools/list", msg_id=2), headers=headers)
        assert listed.status_code == 200
        assert "resultType" not in listed.json()["result"]


def test_http_disconnect_cancels_the_call(live_server: LiveServer) -> None:
    server = make_server()
    started = threading.Event()
    cancelled = threading.Event()

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

    base = live_server(server)
    call = modern("tools/call", {"name": "slow"})

    def fire() -> None:
        try:
            with httpx.Client(base_url=base, timeout=1.0) as client:
                post(client, call)
        except httpx.TimeoutException:
            pass  # the client gives up and closes the connection

    thread = threading.Thread(target=fire)
    thread.start()
    assert started.wait(5)
    thread.join(5)
    deadline = time.time() + 5
    while not cancelled.is_set() and time.time() < deadline:
        time.sleep(0.05)
    assert cancelled.is_set()
