"""CORS: what a browser page on an allowed origin is told, on every HTTP endpoint."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import LogCapture, headers_for, modern, notification, rpc

from easy_mcp import APIKeyAuth, MCPServer
from easy_mcp.transport._http import (
    CORS_EXPOSED_HEADERS,
    CORS_MAX_AGE_SECONDS,
    CORS_METHODS,
    CORS_REQUEST_HEADERS,
)

LiveServer = Callable[[Any], str]

KEY = "cors-test-key-" + "k" * 18
APP = "https://app.example.com"
LOCAL = "http://localhost:6274"
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "tests", "version": "1.0"},
}


def make_server(**kwargs: Any) -> MCPServer:
    kwargs.setdefault("rate_limit_per_minute", None)
    server = MCPServer(port=0, **kwargs)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def preflight(
    client: httpx.Client, origin: str, path: str = "/mcp", method: str = "POST", **extra: str
) -> httpx.Response:
    headers = {
        "Origin": origin,
        "Access-Control-Request-Method": method,
        "Access-Control-Request-Headers": "content-type, mcp-protocol-version, authorization",
        **extra,
    }
    return client.options(path, headers=headers)


def test_a_preflight_from_an_allowed_origin_is_answered(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        for path in ("/mcp", "/sse", "/messages", "/healthz"):
            response = preflight(client, APP, path)
            assert response.status_code == 204, path
            assert response.content == b""
            assert response.headers["access-control-allow-origin"] == APP
            assert response.headers["access-control-allow-methods"] == CORS_METHODS
            assert response.headers["access-control-allow-headers"] == CORS_REQUEST_HEADERS
            assert response.headers["access-control-max-age"] == str(CORS_MAX_AGE_SECONDS)
            assert "Origin" in response.headers["vary"]
            assert "access-control-allow-credentials" not in response.headers
            assert "access-control-allow-private-network" not in response.headers


def test_every_header_a_client_sends_is_allowed() -> None:
    allowed = {name.strip().lower() for name in CORS_REQUEST_HEADERS.split(",")}
    for name in (
        "accept",
        "authorization",
        "content-type",
        "mcp-protocol-version",
        "mcp-session-id",
        "mcp-method",
        "mcp-name",
        "x-api-key",
        "traceparent",
    ):
        assert name in allowed
    assert {name.strip().lower() for name in CORS_EXPOSED_HEADERS.split(",")} == {
        "mcp-session-id",
        "www-authenticate",
        "retry-after",
    }


def test_a_preflight_needs_no_credential_and_spends_no_rate_limit(
    live_server: LiveServer,
) -> None:
    server = make_server(
        allowed_origins=[APP], auth=APIKeyAuth({KEY: ["*"]}), rate_limit_per_minute=2
    )
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        for _ in range(5):
            assert preflight(client, APP).status_code == 204


def test_a_preflight_from_another_origin_is_refused(
    live_server: LiveServer, logs: LogCapture
) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        for origin in ("https://evil.example", LOCAL, "null"):
            response = preflight(client, origin)
            assert response.status_code == 403
            assert "access-control-allow-origin" not in response.headers
    assert len(logs.events("origin_rejected")) == 3


def test_loopback_origins_are_served_by_default(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        response = preflight(client, LOCAL)
        assert response.status_code == 204
        assert response.headers["access-control-allow-origin"] == LOCAL
        assert preflight(client, APP).status_code == 403


def test_an_empty_list_allows_no_origin(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[]))
    with httpx.Client(base_url=base, timeout=10) as client:
        assert preflight(client, LOCAL).status_code == 403
        assert preflight(client, APP).status_code == 403
        assert client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT).status_code == 200


def test_any_origin_is_answered_with_a_star(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=["*"]))
    with httpx.Client(base_url=base, timeout=10) as client:
        response = preflight(client, "https://anything.example")
        assert response.headers["access-control-allow-origin"] == "*"
        answer = client.post(
            "/mcp", json=rpc("initialize", INIT), headers={**ACCEPT, "Origin": "https://x.example"}
        )
        assert answer.headers["access-control-allow-origin"] == "*"
        assert "origin" not in answer.headers.get("vary", "").lower()


def test_a_private_network_preflight_is_granted_to_an_allowed_origin(
    live_server: LiveServer,
) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        response = preflight(
            client, APP, **{"Access-Control-Request-Private-Network": "true"}
        )
        assert response.headers["access-control-allow-private-network"] == "true"
        refused = preflight(
            client, "https://evil.example", **{"Access-Control-Request-Private-Network": "true"}
        )
        assert refused.status_code == 403
        assert "access-control-allow-private-network" not in refused.headers


def test_options_that_is_no_preflight_reaches_the_app(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        # No Access-Control-Request-Method: not a preflight.
        with_origin = client.options("/mcp", headers={"Origin": APP})
        assert with_origin.status_code == 405
        assert with_origin.headers["access-control-allow-origin"] == APP
        # No Origin at all: not a browser, nothing added.
        plain = client.options("/mcp", headers={"Access-Control-Request-Method": "POST"})
        assert plain.status_code == 405
        assert "access-control-allow-origin" not in plain.headers


def test_answers_to_an_allowed_origin_are_readable(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    browser = {**ACCEPT, "Origin": APP}
    with httpx.Client(base_url=base, timeout=10) as client:
        init = client.post("/mcp", json=rpc("initialize", INIT), headers=browser)
        assert init.status_code == 200
        assert init.headers["access-control-allow-origin"] == APP
        exposed = init.headers["access-control-expose-headers"]
        assert exposed == CORS_EXPOSED_HEADERS
        assert "Origin" in init.headers["vary"]
        session = {**browser, "MCP-Session-Id": init.headers["mcp-session-id"]}
        # Every kind of answer: 202, a result, an error status, a DELETE.
        accepted = client.post(
            "/mcp", json=notification("notifications/initialized"), headers=session
        )
        called = client.post(
            "/mcp", json=rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, 2),
            headers=session,
        )
        missing = client.post("/mcp", json=rpc("ping"), headers={**browser, "MCP-Session-Id": "x"})
        deleted = client.delete("/mcp", headers=session)
        health = client.get("/healthz", headers={"Origin": APP})
        for response, status in (
            (accepted, 202),
            (called, 200),
            (missing, 404),
            (deleted, 204),
            (health, 200),
        ):
            assert response.status_code == status
            assert response.headers["access-control-allow-origin"] == APP
        assert called.json()["result"]["content"][0]["text"] == "3"


def test_stateless_answers_and_streams_are_readable(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        message = modern("tools/call", {"name": "add", "arguments": {"a": 2, "b": 2}})
        answer = client.post("/mcp", json=message, headers=headers_for(message, Origin=APP))
        assert answer.status_code == 200
        assert answer.headers["access-control-allow-origin"] == APP
        with client.stream("GET", "/sse", headers={"Origin": APP}) as stream:
            assert stream.status_code == 200
            assert stream.headers["access-control-allow-origin"] == APP


def test_a_challenge_is_readable_by_the_page(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[APP], auth=APIKeyAuth({KEY: ["*"]})))
    with httpx.Client(base_url=base, timeout=10) as client:
        bad = client.post(
            "/mcp",
            json=rpc("initialize", INIT),
            headers={**ACCEPT, "Origin": APP, "Authorization": "Bearer wrong-key-000000"},
        )
        assert bad.status_code == 401
        assert bad.headers["access-control-allow-origin"] == APP
        assert "WWW-Authenticate" in bad.headers["access-control-expose-headers"]


def test_requests_without_an_origin_are_untouched(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        init = client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        assert init.status_code == 200
        assert not [name for name in init.headers if name.startswith("access-control-")]
        assert "vary" not in init.headers


def test_a_refused_origin_gets_no_cors_headers(live_server: LiveServer) -> None:
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        refused = client.post(
            "/mcp", json=rpc("initialize", INIT), headers={**ACCEPT, "Origin": "https://evil.example"}
        )
        assert refused.status_code == 403
        assert not [name for name in refused.headers if name.startswith("access-control-")]


@pytest.mark.parametrize("origin", ["HTTPS://APP.EXAMPLE.COM", "https://app.example.com/"])
def test_the_origin_is_named_as_the_browser_sent_it(live_server: LiveServer, origin: str) -> None:
    # A browser compares the header with its own origin byte for byte.
    base = live_server(make_server(allowed_origins=[APP]))
    with httpx.Client(base_url=base, timeout=10) as client:
        response = preflight(client, origin)
        assert response.status_code == 204
        assert response.headers["access-control-allow-origin"] == origin
