"""End-to-end SSE transport tests against a real uvicorn server."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator

import httpx
import pytest
import uvicorn

from easy_mcp import APIKeyAuth, MCPServer

KEY = "transport-test-key-" + "k" * 13


def make_server(**kwargs: object) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)  # type: ignore[arg-type]

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


@pytest.fixture
def live_server() -> Iterator[Callable[[MCPServer], str]]:
    """Start servers on ephemeral ports in background threads; stop them after."""
    running: list[tuple[uvicorn.Server, threading.Thread]] = []

    def start(server: MCPServer) -> str:
        config = uvicorn.Config(
            server.build_app(), host="127.0.0.1", port=0, log_level="warning"
        )
        uv = uvicorn.Server(config)
        thread = threading.Thread(target=uv.run, daemon=True)
        thread.start()
        deadline = time.time() + 10
        while not uv.started:
            if time.time() > deadline:
                raise RuntimeError("uvicorn failed to start within 10s")
            time.sleep(0.01)
        running.append((uv, thread))
        port = uv.servers[0].sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    yield start

    for uv, thread in running:
        uv.should_exit = True
        thread.join(timeout=5)


def _next_data(lines: Iterator[str]) -> str:
    """Read SSE lines until the next data: payload."""
    for line in lines:
        if line.startswith("data: "):
            return line[len("data: ") :]
    raise AssertionError("SSE stream ended without a data event")


def test_health_unknown_session_and_payload_cap(live_server) -> None:  # type: ignore[no-untyped-def]
    base = live_server(make_server(max_request_bytes=200))
    with httpx.Client(base_url=base, timeout=10) as client:
        health = client.get("/healthz")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"
        assert health.json()["tools"] == 1

        unknown = client.post(
            "/messages",
            params={"session_id": "not-a-real-session"},
            json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
        )
        assert unknown.status_code == 404

        oversized = client.post(
            "/messages",
            params={"session_id": "irrelevant"},
            json={"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {"pad": "x" * 500}},
        )
        assert oversized.status_code == 413


def test_sse_roundtrip(live_server) -> None:  # type: ignore[no-untyped-def]
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=httpx.Timeout(10.0)) as client:
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()
            endpoint = _next_data(lines)
            assert "/messages?session_id=" in endpoint

            posted = client.post(
                endpoint,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"protocolVersion": "2024-11-05"},
                },
            )
            assert posted.status_code == 202
            reply = json.loads(_next_data(lines))
            assert reply["id"] == 1
            assert reply["result"]["serverInfo"]["name"] == "easy-mcp"

            posted = client.post(
                endpoint,
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "add", "arguments": {"a": 2, "b": 3}},
                },
            )
            assert posted.status_code == 202
            reply = json.loads(_next_data(lines))
            assert reply["result"]["content"][0]["text"] == "5"
            assert reply["result"]["isError"] is False

            malformed = client.post(
                endpoint, content=b"{not json", headers={"content-type": "application/json"}
            )
            assert malformed.status_code == 400
            assert malformed.json()["error"]["code"] == -32700


def test_transport_auth(live_server) -> None:  # type: ignore[no-untyped-def]
    server = make_server(auth=APIKeyAuth({KEY: "*"}))

    @server.tool(scopes=("admin",))
    def secret() -> str:
        """Protected tool."""
        return "s3cr3t"

    base = live_server(server)
    with httpx.Client(base_url=base, timeout=httpx.Timeout(10.0)) as client:
        # An invalid key is rejected at the SSE handshake.
        rejected = client.get("/sse", headers={"Authorization": "Bearer wrong-key-000000"})
        assert rejected.status_code == 401

        # A session opened anonymously cannot be driven with a different
        # credential (session hijack protection).
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()  # keep the iterator alive for the session
            endpoint = _next_data(lines)
            mismatched = client.post(
                endpoint,
                headers={"Authorization": f"Bearer {KEY}"},
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
            )
            assert mismatched.status_code == 403

        # Fully authenticated roundtrip reaches the protected tool.
        auth_headers = {"Authorization": f"Bearer {KEY}"}
        with client.stream("GET", "/sse", headers=auth_headers) as stream:
            lines = stream.iter_lines()
            endpoint = _next_data(lines)
            posted = client.post(
                endpoint,
                headers=auth_headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "secret", "arguments": {}},
                },
            )
            assert posted.status_code == 202
            reply = json.loads(_next_data(lines))
            assert reply["result"]["content"][0]["text"] == "s3cr3t"
