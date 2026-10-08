"""MCPServer(store=...): the argument, the limits it serves, outages, /healthz, stdio."""

from __future__ import annotations

import asyncio
import io
import json
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import LogCapture, headers_for, make_context, modern, notification, rpc
from shared_store_fake import FakeHub

from easy_mcp import (
    MCPServer,
    MemoryStore,
    RedisStore,
    SSETransport,
    StdioTransport,
    StoreUnavailableError,
    StreamableHTTPTransport,
    Transport,
)
from easy_mcp.exceptions import (
    INVALID_PARAMS,
    RATE_LIMITED,
    SERVER_BUSY,
    SESSION_LIMIT_EXCEEDED,
)

LiveServer = Callable[[Any], str]
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


def make_server(store: Any = None, **options: Any) -> MCPServer:
    options.setdefault("rate_limit_per_minute", None)
    server = MCPServer(port=0, store=store, **options)
    runs: list[str] = []
    server.runs = runs  # type: ignore[attr-defined]

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        runs.append("add")
        return a + b

    @server.tool(max_calls_per_session=1)
    def once(n: int) -> int:
        """Once per session."""
        runs.append("once")
        return n

    return server


def session_post(client: httpx.Client, message: dict[str, Any], session: str) -> httpx.Response:
    return client.post("/mcp", json=message, headers={**ACCEPT, "MCP-Session-Id": session})


def open_session(client: httpx.Client) -> str:
    response = client.post("/mcp", json=rpc("initialize", INIT, "init"), headers=ACCEPT)
    assert response.status_code == 200, response.text
    return response.headers["mcp-session-id"]


def test_store_argument_is_type_checked() -> None:
    with pytest.raises(TypeError, match="store must be a Store"):
        MCPServer(port=0, store="redis://localhost")  # type: ignore[arg-type]
    store = MemoryStore()
    assert MCPServer(port=0, store=store).store is store


def test_shared_store_refuses_sessions_that_never_expire() -> None:
    shared = make_server(FakeHub().store())
    with pytest.raises(ValueError, match="session_idle_timeout=None"):
        StreamableHTTPTransport(shared, session_idle_timeout=None)
    StreamableHTTPTransport(shared)  # the default idle timeout is fine
    StreamableHTTPTransport(make_server(), session_idle_timeout=None)  # so is memory


def test_sync_check_rate_limit_is_unchanged(live_server: LiveServer) -> None:
    server = make_server(rate_limit_per_minute=2)
    base = live_server(server)
    server.check_rate_limit("ip:127.0.0.1")  # one unit, spent outside HTTP
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("GET", "/sse") as stream:  # the second
            assert stream.status_code == 200
            refused = client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
    assert refused.json()["error"]["code"] == RATE_LIMITED


def test_acheck_rate_limit_uses_the_store(live_server: LiveServer) -> None:
    hub = FakeHub()
    server = make_server(hub.store(), rate_limit_per_minute=50)
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        listed = modern("tools/list")
        assert client.post("/mcp", json=listed, headers=headers_for(listed)).status_code == 200
        with client.stream("GET", "/sse") as stream:
            assert stream.status_code == 200
    assert hub.calls.count("rate") == 2
    # check_rate_limit() stays the in-process budget: the hub sees nothing.
    server.check_rate_limit("ip:x")
    assert hub.calls.count("rate") == 2


async def test_dispatch_without_state_is_0_3_1() -> None:
    hub = FakeHub()
    server = make_server(hub.store())
    context = make_context()
    response = await server.dispatch(
        rpc("tools/call", {"name": "once", "arguments": {"n": 1}}), context
    )
    assert response is not None and response["result"]["content"][0]["text"] == "1"
    refused = await server.dispatch(
        rpc("tools/call", {"name": "once", "arguments": {"n": 1}}), context
    )
    assert refused is not None and refused["error"]["code"] == SESSION_LIMIT_EXCEEDED
    assert context.tool_calls == {"once": 1}
    await server.dispatch(notification("notifications/cancelled", {"requestId": 99}), context)
    assert hub.calls == [] and hub.payloads == []


def test_store_outage_fails_closed(live_server: LiveServer) -> None:
    hub = FakeHub()
    server = make_server(hub.store("a" * 16))
    base = live_server(server)
    other = live_server(make_server(hub.store("b" * 16)))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client)
        with client.stream("GET", "/sse") as stream:
            endpoint = next(
                line[len("data: ") :] for line in stream.iter_lines() if line.startswith("data: ")
            )
            hub.down = True
            answers = [
                client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT),
                session_post(client, rpc("tools/list", msg_id=2), session),
                client.get("/sse"),
                # The worker holding the stream needs no store to accept a
                # message; another worker does.
                httpx.post(f"{other}{endpoint}", json=rpc("ping"), timeout=10),
            ]
            capped = modern("tools/call", {"name": "once", "arguments": {"n": 1}})
            answers.append(client.post("/mcp", json=capped, headers=headers_for(capped)))
            for answer in answers:
                assert answer.status_code == 503, answer.text
                assert answer.headers["retry-after"] == "1"
                error = answer.json()["error"]
                assert error["code"] == SERVER_BUSY
                assert error["data"] == {"reason": "store_unavailable"}
            # Without a rate limit, what needs no store is still served.
            listed = modern("tools/list")
            assert client.post("/mcp", json=listed, headers=headers_for(listed)).status_code == 200
            uncapped = modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
            served = client.post("/mcp", json=uncapped, headers=headers_for(uncapped))
            assert served.json()["result"]["content"][0]["text"] == "3"
            hub.down = False
    assert server.runs == ["add"]  # type: ignore[attr-defined]


def test_a_rate_limit_without_its_store_refuses_everything(live_server: LiveServer) -> None:
    hub = FakeHub()
    base = live_server(make_server(hub.store(), rate_limit_per_minute=100))
    hub.down = True
    listed = modern("tools/list")
    with httpx.Client(base_url=base, timeout=10) as client:
        refused = client.post("/mcp", json=listed, headers=headers_for(listed))
    assert refused.status_code == 503
    assert refused.json()["error"]["data"] == {"reason": "store_unavailable"}


async def test_a_stateless_request_whose_client_cannot_be_noted_gets_503() -> None:
    # Any Store method may raise StoreUnavailableError: touch_client too.
    class Unreachable(MemoryStore):
        async def touch_client(self, client_id: str, *, ttl: float | None) -> None:
            raise StoreUnavailableError()

    app = StreamableHTTPTransport(make_server(Unreachable())).build_app()
    listed = modern("tools/list", msg_id=5)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as client:
        refused = await client.post("/mcp", json=listed, headers=headers_for(listed))
    assert refused.status_code == 503 and refused.headers["retry-after"] == "1"
    body = refused.json()
    assert body["id"] == 5 and body["error"]["code"] == SERVER_BUSY
    assert body["error"]["data"] == {"reason": "store_unavailable"}


def test_healthz_reports_the_store(live_server: LiveServer) -> None:
    hub = FakeHub()
    base = live_server(make_server(hub.store()))
    with httpx.Client(base_url=base, timeout=10) as client:
        healthy = client.get("/healthz")
        assert healthy.status_code == 200 and healthy.json()["store"] == "ok"
        for _ in range(5):
            client.get("/healthz")
        assert hub.pings == 1  # cached for a second
        hub.down = True
        time.sleep(1.1)
        unhealthy = client.get("/healthz")
        assert unhealthy.status_code == 503
        assert unhealthy.json()["store"] == "unreachable"
        assert unhealthy.json()["status"] == "unavailable"
        hub.down = False
        time.sleep(1.1)
        assert client.get("/healthz").status_code == 200


def test_reservation_is_released_when_arguments_are_invalid(live_server: LiveServer) -> None:
    hub = FakeHub()
    base = live_server(make_server(hub.store()))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client)
        bad = rpc("tools/call", {"name": "once", "arguments": {"n": "one"}}, 2)
        assert session_post(client, bad, session).json()["error"]["code"] == INVALID_PARAMS
        good = rpc("tools/call", {"name": "once", "arguments": {"n": 1}}, 3)
        assert "result" in session_post(client, good, session).json()
        again = rpc("tools/call", {"name": "once", "arguments": {"n": 1}}, 4)
        assert session_post(client, again, session).json()["error"]["code"] == (
            SESSION_LIMIT_EXCEEDED
        )
    assert hub.calls.count("unreserve") == 1


def test_reservation_is_released_when_workers_are_busy(live_server: LiveServer) -> None:
    hub = FakeHub()
    server = make_server(hub.store(), max_sync_workers=1)
    holding, release = threading.Event(), threading.Event()

    @server.tool
    def hold() -> str:
        """Holds the only worker."""
        holding.set()
        release.wait(10)
        return "held"

    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client)
        holder = threading.Thread(
            target=session_post,
            args=(client, rpc("tools/call", {"name": "hold"}, 2), session),
            daemon=True,
        )
        holder.start()
        try:
            # Only once the worker is taken: a call before would spend the unit.
            assert holding.wait(5)
            once = rpc("tools/call", {"name": "once", "arguments": {"n": 1}}, 3)
            busy = session_post(client, once, session).json()
            assert busy["error"]["code"] == SERVER_BUSY
        finally:
            release.set()
            holder.join(5)
        once = rpc("tools/call", {"name": "once", "arguments": {"n": 1}}, 4)
        assert "result" in session_post(client, once, session).json()  # the busy call was refunded


def test_session_limit_still_precedes_argument_errors(live_server: LiveServer) -> None:
    hub = FakeHub()
    base = live_server(make_server(hub.store()))
    with httpx.Client(base_url=base, timeout=10) as client:
        session = open_session(client)
        good = rpc("tools/call", {"name": "once", "arguments": {"n": 1}}, 2)
        assert "result" in session_post(client, good, session).json()
        bad = rpc("tools/call", {"name": "once", "arguments": {"n": "one"}}, 3)
        assert session_post(client, bad, session).json()["error"]["code"] == (
            SESSION_LIMIT_EXCEEDED
        )


class NoTransport(Transport):
    def run(self) -> None:
        return None

    def stop(self) -> None:
        return None


def test_startup_log_names_the_store_without_secrets(logs: LogCapture) -> None:
    pytest.importorskip("redis")
    # Built, not written out, so secret scanners do not take it for a credential.
    userinfo = "admin:" + "pa55" + "word"
    store = RedisStore(f"redis://{userinfo}@redis.internal:6379/0")
    server = MCPServer(port=0, store=store)  # the default name is the namespace
    server.run(NoTransport(server))
    (startup,) = [
        record.event  # type: ignore[attr-defined]
        for record in logs.records
        if getattr(record, "event", {}).get("type") == "startup"
    ]
    assert startup["store"] == "redis redis://redis.internal:6379/0 namespace=easy-mcp"
    warnings = [record.getMessage() for record in logs.records if record.levelname == "WARNING"]
    assert any("not encrypted" in warning for warning in warnings)
    assert any("default server name" in warning for warning in warnings)
    assert userinfo not in logs.text and "admin:" not in logs.text
    # With a password the warning about one goes; with TLS the other.
    tls = RedisStore(f"rediss://{userinfo}@redis.internal:6379/0", namespace="reports")
    assert tls.warnings() == []


def test_namespace_defaults_to_the_server_name() -> None:
    pytest.importorskip("redis")
    store = RedisStore("redis://127.0.0.1:6379/0")
    MCPServer(port=0, name="My Tools", store=store)
    assert store.namespace == "my-tools"
    assert MCPServer(port=0, name="--Odd {name}/x", store=RedisStore("redis://h")).store.namespace
    named = RedisStore("redis://127.0.0.1:6379/0", namespace="pg-ro")
    MCPServer(port=0, name="ignored", store=named)
    assert named.namespace == "pg-ro"
    for bad in ("", "Upper", "-lead", "a" * 65, "with space", "{tag}"):
        with pytest.raises(ValueError, match="invalid namespace"):
            RedisStore("redis://127.0.0.1:6379/0", namespace=bad)


async def test_stdio_never_touches_a_shared_store() -> None:
    hub = FakeHub()
    store = hub.store()
    server = make_server(store, rate_limit_per_minute=10)
    lines = [
        rpc("initialize", INIT),
        rpc("tools/call", {"name": "once", "arguments": {"n": 1}}, 2),
        rpc("tools/call", {"name": "once", "arguments": {"n": 1}}, 3),
        notification("notifications/cancelled", {"requestId": 42}),
    ]
    stdin = io.BytesIO(b"".join(json.dumps(line).encode() + b"\n" for line in lines))
    stdout = io.BytesIO()
    await StdioTransport(server, stdin=stdin, stdout=stdout).serve()
    answers = [json.loads(line) for line in stdout.getvalue().splitlines()]
    # The calls run concurrently: one is served, the other refused.
    calls = sorted((answer for answer in answers if answer["id"] in (2, 3)), key=str)
    assert sorted("result" in answer for answer in calls) == [False, True]
    assert [a["error"]["code"] for a in calls if "error" in a] == [SESSION_LIMIT_EXCEEDED]
    assert hub.calls == [] and hub.payloads == [] and store.started == 0


async def test_registering_a_tool_while_serving_with_a_shared_store_warns(
    logs: LogCapture,
) -> None:
    server = make_server(FakeHub().store())
    server.register_tool(lambda: "early", name="early", description="Before serving.")
    async with server.lifespan():
        server.register_tool(lambda: "late", name="late", description="While serving.")
        server.unregister_tool("late")
    warnings = [record.getMessage() for record in logs.records if record.levelname == "WARNING"]
    assert len([w for w in warnings if "shared store" in w]) == 2
    assert all("early" not in warning for warning in warnings)
    plain = make_server()
    async with plain.lifespan():
        plain.register_tool(lambda: "late", name="late", description="While serving.")
    assert len([r for r in logs.records if "shared store" in r.getMessage()]) == 2


async def test_the_lifespan_starts_and_closes_the_store() -> None:
    hub = FakeHub()
    store = hub.store()
    server = make_server(store)
    app = SSETransport(server).build_app()
    async with app.router.lifespan_context(app):
        assert store.started == 1 and store.closed == 0
        await asyncio.sleep(0)
    assert store.closed == 1
    async with server.lifespan():
        assert store.started == 2
    assert store.closed == 2
