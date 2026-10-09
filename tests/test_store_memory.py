"""MemoryStore, the default store: 0.3.1's in-process state behind the Store interface."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

import httpx
import pytest
from conftest import LogCapture, headers_for, modern, rpc
from starlette.applications import Starlette
from starlette.routing import Mount

from easy_mcp import MCPServer, MemoryStore, SSETransport, StreamableHTTPTransport
from easy_mcp.exceptions import SESSION_LIMIT_EXCEEDED, TOO_MANY_SESSIONS
from easy_mcp.security.ratelimit import SlidingWindowRateLimiter
from easy_mcp.store import Reservation, SessionRecord
from easy_mcp.store.base import session_ref
from easy_mcp.store.memory import STATELESS_CLIENTS_MAX

ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def run_now(coro: Coroutine[Any, Any, Any]) -> Any:
    """Run *coro* one step; it must finish without ever suspending."""
    try:
        coro.send(None)
    except StopIteration as stop:
        return stop.value
    coro.close()
    raise AssertionError("the coroutine suspended")


def record(session_id: str, kind: str = "http", client_id: str = "ip:test") -> SessionRecord:
    return SessionRecord(
        ref=session_ref(session_id),
        kind=kind,  # type: ignore[arg-type]
        client_id=client_id,
        identity_fp=None,
        session_id=session_id,
    )


async def test_memory_store_is_the_default() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    assert isinstance(server.store, MemoryStore)
    assert server.store.describe() == "memory"
    assert not server.store.shared
    app = server.build_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        health = await client.get("/healthz")
    assert health.status_code == 200
    assert "store" not in health.json()


def test_memory_store_methods_never_suspend() -> None:
    # 0.3.1 checked and changed sessions and counts with no await in between;
    # a MemoryStore call completes within one step, so that still holds.
    store = MemoryStore()
    rec = record("s1")
    limiter = SlidingWindowRateLimiter(5)
    calls: list[Coroutine[Any, Any, Any]] = [
        store.start(),
        store.ping(),
        store.create_session(rec, cap=10, ttl=60),
        store.acquire_session("http", rec.ref, ttl=60),
        store.release_session("http", rec.ref, ttl=60, protocol_version="2025-11-25"),
        store.refresh_sessions("http", [rec.ref, "0" * 32], ttl=60),
        store.reserve_session_call(rec.ref, "tool", 2),
        store.release_session_call(rec.ref, "tool"),
        store.touch_client("ip:a", ttl=60),
        store.reserve_client_call("ip:a", "tool", 2, ttl=60),
        store.release_client_call("ip:a", "tool"),
        store.publish("payload"),
        store.rate_limiter(limiter).acheck("ip:a"),
        store.delete_session("http", rec.ref),
        store.aclose(),
    ]
    for call in calls:
        run_now(call)


def test_memory_caps_are_per_kind() -> None:
    store = MemoryStore()
    assert run_now(store.create_session(record("h1"), cap=1, ttl=None)) == (True, [])
    assert run_now(store.create_session(record("h2"), cap=1, ttl=None)) == (False, [])
    # Legacy SSE sessions are counted apart, as two endpoints did in 0.3.1.
    assert run_now(store.create_session(record("s1", "sse"), cap=1, ttl=None)) == (True, [])
    assert run_now(store.create_session(record("s2", "sse"), cap=1, ttl=None)) == (False, [])
    run_now(store.delete_session("http", session_ref("h1")))
    assert run_now(store.create_session(record("h2"), cap=1, ttl=None)) == (True, [])


def test_memory_session_ids_are_never_reused() -> None:
    store = MemoryStore()
    run_now(store.create_session(record("h1"), cap=5, ttl=None))
    with pytest.raises(ValueError, match="exists"):
        run_now(store.create_session(record("h1"), cap=5, ttl=None))


def test_memory_active_sessions_never_expire() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    rec = record("s1")
    run_now(store.create_session(rec, cap=10, ttl=1.0))  # held by its opener
    clock.now += 5
    found, expired = run_now(store.acquire_session("http", rec.ref, ttl=1.0))
    assert found is not None and found.session_id == "s1" and expired == []
    run_now(store.release_session("http", rec.ref, ttl=1.0))  # the opener's hold
    clock.now += 5  # still held by the request above
    assert run_now(store.acquire_session("http", rec.ref, ttl=1.0))[0] is not None
    run_now(store.release_session("http", rec.ref, ttl=1.0))
    run_now(store.release_session("http", rec.ref, ttl=1.0))
    clock.now += 0.5
    assert run_now(store.acquire_session("http", rec.ref, ttl=1.0))[0] is not None
    run_now(store.release_session("http", rec.ref, ttl=1.0))
    clock.now += 1.5
    found, expired = run_now(store.acquire_session("http", rec.ref, ttl=1.0))
    assert found is None
    assert [(gone.ref, gone.client_id, gone.session_id) for gone in expired] == [
        (rec.ref, "ip:test", "s1")
    ]
    assert run_now(store.acquire_session("http", rec.ref, ttl=1.0)) == (None, [])


def test_memory_refused_requests_do_not_keep_a_session_alive() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    rec = record("s1")
    run_now(store.create_session(rec, cap=10, ttl=1.0))
    run_now(store.release_session("http", rec.ref, ttl=1.0))
    clock.now += 0.8
    run_now(store.acquire_session("http", rec.ref, ttl=1.0))
    run_now(store.release_session("http", rec.ref, ttl=1.0, touch=False))  # e.g. a 403
    clock.now += 0.5
    assert run_now(store.acquire_session("http", rec.ref, ttl=1.0))[0] is None


def test_memory_expired_sessions_are_pruned_on_create() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    for name in ("a", "b"):
        run_now(store.create_session(record(name), cap=2, ttl=1.0))
        run_now(store.release_session("http", session_ref(name), ttl=1.0))
    clock.now += 2
    created, expired = run_now(store.create_session(record("c"), cap=2, ttl=1.0))
    assert created
    assert sorted(gone.session_id or "" for gone in expired) == ["a", "b"]


def test_memory_stateless_counts_keep_0_3_1_semantics() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    assert run_now(store.reserve_client_call("ip:a", "t", 2, ttl=1.0)) is Reservation.OK
    assert run_now(store.reserve_client_call("ip:a", "t", 2, ttl=1.0)) is Reservation.OK
    assert run_now(store.reserve_client_call("ip:a", "t", 2, ttl=1.0)) is Reservation.LIMIT
    # Any request refreshes the client, as 0.3.1 did per request.
    for _ in range(3):
        clock.now += 0.8
        run_now(store.touch_client("ip:a", ttl=1.0))
    assert run_now(store.reserve_client_call("ip:a", "t", 2, ttl=1.0)) is Reservation.LIMIT
    # Idle past the ttl: the counts lapse, as a session would.
    clock.now += 1.5
    run_now(store.touch_client("ip:a", ttl=1.0))
    assert run_now(store.reserve_client_call("ip:a", "t", 2, ttl=1.0)) is Reservation.OK
    # Only the most recently seen clients are kept.
    for n in range(STATELESS_CLIENTS_MAX):
        run_now(store.touch_client(f"ip:{n}", ttl=None))
    assert "ip:a" not in store._clients
    assert len(store._clients) == STATELESS_CLIENTS_MAX


def test_memory_reservation_outcomes() -> None:
    store = MemoryStore()
    rec = record("s1")
    run_now(store.create_session(rec, cap=10, ttl=None))
    assert run_now(store.reserve_session_call(rec.ref, "t", 1)) is Reservation.OK
    assert run_now(store.reserve_session_call(rec.ref, "t", 1)) is Reservation.LIMIT
    run_now(store.release_session_call(rec.ref, "t"))
    run_now(store.release_session_call(rec.ref, "t"))  # never below zero
    assert run_now(store.reserve_session_call(rec.ref, "t", 1)) is Reservation.OK
    assert run_now(store.reserve_session_call(rec.ref, "t", 1)) is Reservation.LIMIT
    run_now(store.delete_session("http", rec.ref))
    assert run_now(store.reserve_session_call(rec.ref, "t", 1)) is Reservation.GONE
    run_now(store.release_client_call("ip:nobody", "t"))  # unknown: nothing to give back
    run_now(store.reserve_client_call("ip:b", "t", 1, ttl=None))
    run_now(store.release_client_call("ip:b", "t"))
    run_now(store.release_client_call("ip:b", "t"))
    assert run_now(store.reserve_client_call("ip:b", "t", 1, ttl=None)) is Reservation.OK
    assert run_now(store.reserve_client_call("ip:b", "t", 1, ttl=None)) is Reservation.LIMIT


def test_memory_store_records_the_negotiated_version() -> None:
    store = MemoryStore()
    rec = record("s1")
    run_now(store.create_session(rec, cap=10, ttl=None))
    run_now(store.release_session("http", rec.ref, ttl=None, protocol_version="2025-06-18"))
    found, _ = run_now(store.acquire_session("http", rec.ref, ttl=None))
    assert found.protocol_version == "2025-06-18" and found.t0 is not None
    # The kind is part of a session's identity.
    assert run_now(store.acquire_session("sse", rec.ref, ttl=None)) == (None, [])
    assert run_now(store.delete_session("sse", rec.ref)) is False
    assert run_now(store.delete_session("http", rec.ref)) is True


def test_memory_sessions_stay_with_the_endpoint_that_opened_them(
    live_server: Callable[[Any], str],
) -> None:
    # One server behind two transports of each kind: as in 0.3.1, a session
    # is unknown to every endpoint but the one that opened it.
    server = MCPServer(port=0, rate_limit_per_minute=None, max_sessions=1)
    ran: list[int] = []

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        ran.append(a + b)
        return a + b

    http_one = StreamableHTTPTransport(server, legacy_sse=False)
    http_two = StreamableHTTPTransport(server, legacy_sse=False)
    sse_one, sse_two = SSETransport(server), SSETransport(server)
    mounts = {"/h1": http_one, "/h2": http_two, "/s1": sse_one, "/s2": sse_two}
    base = live_server(
        Starlette(routes=[Mount(path, transport.build_app()) for path, transport in mounts.items()])
    )
    with httpx.Client(base_url=base, timeout=10) as client:
        opened = client.post("/h1/mcp", json=rpc("initialize", INIT, "init"), headers=ACCEPT)
        session = opened.headers["mcp-session-id"]
        headers = {**ACCEPT, "MCP-Session-Id": session}
        assert (
            client.post("/h2/mcp", json=rpc("ping", msg_id=2), headers=headers).status_code == 404
        )
        assert client.delete("/h2/mcp", headers=headers).status_code == 404
        assert (
            client.post("/h1/mcp", json=rpc("ping", msg_id=3), headers=headers).status_code == 200
        )
        # max_sessions counts the server's sessions of a kind, on every endpoint.
        refused = client.post("/h2/mcp", json=rpc("initialize", INIT, "init"), headers=ACCEPT)
        assert refused.status_code == 503
        assert refused.json()["error"]["code"] == TOO_MANY_SESSIONS
        with client.stream("GET", "/s1/sse") as stream:
            endpoint = next(
                line[len("data: ") :] for line in stream.iter_lines() if line.startswith("data: ")
            )
            call = rpc("tools/call", {"name": "add", "arguments": {"a": 40, "b": 2}}, 4)
            assert client.post(f"/s2{endpoint}", json=call).status_code == 404
    assert ran == []
    assert http_two._manager.local_sessions() == [] and sse_two._manager.local_sessions() == []


async def test_memory_sessions_expire_by_their_own_endpoints_timeout() -> None:
    clock = Clock()
    server = MCPServer(port=0, rate_limit_per_minute=None, store=MemoryStore(clock=clock))
    patient = StreamableHTTPTransport(server, session_idle_timeout=3600)
    hasty = StreamableHTTPTransport(server, session_idle_timeout=1)

    def client(transport: StreamableHTTPTransport) -> httpx.AsyncClient:
        app = httpx.ASGITransport(app=transport.build_app())
        return httpx.AsyncClient(transport=app, base_url="http://127.0.0.1")

    async with client(patient) as one, client(hasty) as two:
        opened = await one.post("/mcp", json=rpc("initialize", INIT, "init"), headers=ACCEPT)
        headers = {**ACCEPT, "MCP-Session-Id": opened.headers["mcp-session-id"]}
        clock.now += 60
        # Opening a session on the other endpoint prunes only what has expired.
        assert (await two.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)).is_success
        assert (await one.post("/mcp", json=rpc("ping", msg_id=2), headers=headers)).is_success


def asgi_client(transport: StreamableHTTPTransport) -> httpx.AsyncClient:
    app = httpx.ASGITransport(app=transport.build_app())
    return httpx.AsyncClient(transport=app, base_url="http://127.0.0.1")


async def test_memory_sessions_another_endpoint_prunes_end_where_they_are_held(
    logs: LogCapture,
) -> None:
    # Opening a session prunes the expired sessions of every endpoint, which
    # max_sessions counts together: the endpoint that held one forgets it
    # then, rather than keeping it until shutdown, and its close is audited once.
    clock = Clock()
    server = MCPServer(port=0, rate_limit_per_minute=None, store=MemoryStore(clock=clock))
    one = StreamableHTTPTransport(server, session_idle_timeout=1, legacy_sse=False)
    two = StreamableHTTPTransport(server, session_idle_timeout=1, legacy_sse=False)
    initialize = rpc("initialize", INIT, "init")
    async with asgi_client(one) as first, asgi_client(two) as second:
        for _ in range(3):
            for _ in range(5):
                assert (await first.post("/mcp", json=initialize, headers=ACCEPT)).is_success
            clock.now += 10
            assert (await second.post("/mcp", json=initialize, headers=ACCEPT)).is_success
            assert one._manager.local_sessions() == []
            assert len(two._manager.local_sessions()) == 1
    await one.close_streams()
    await two.close_streams()
    opened = [event["session_ref"] for event in logs.events("session_open")]
    closed = [event["session_ref"] for event in logs.events("session_close")]
    assert len(opened) == 18 and sorted(closed) == sorted(opened)
    reasons = [event["reason"] for event in logs.events("session_close")]
    assert reasons.count("idle_timeout") == 17 and reasons.count("shutdown") == 1


async def test_memory_session_closes_carry_when_the_session_opened(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Every close reaches the session hook with the t0 its open had,
    # an idle expiry (the usual end of a Streamable HTTP session) included.
    clock = Clock()
    server = MCPServer(port=0, rate_limit_per_minute=None, store=MemoryStore(clock=clock))
    events: list[tuple[bool, str | None, int | None]] = []
    hook = server._session_event

    def record_event(opened: bool, **fields: Any) -> None:
        events.append((opened, fields.get("reason"), fields.get("t0")))
        hook(opened, **fields)

    monkeypatch.setattr(server, "_session_event", record_event)
    transport = StreamableHTTPTransport(server, session_idle_timeout=1, legacy_sse=False)
    initialize = rpc("initialize", INIT, "init")
    async with asgi_client(transport) as http:
        assert (await http.post("/mcp", json=initialize, headers=ACCEPT)).is_success
        clock.now += 10
        opened = await http.post("/mcp", json=initialize, headers=ACCEPT)  # prunes the first
        clock.now += 10
        headers = {**ACCEPT, "MCP-Session-Id": opened.headers["mcp-session-id"]}
        expired = await http.post("/mcp", json=rpc("ping", msg_id=2), headers=headers)
        assert expired.status_code == 404  # found expired by its own lookup
    assert [(event[0], event[1]) for event in events] == [
        (True, None),
        (False, "idle_timeout"),
        (True, None),
        (False, "idle_timeout"),
    ]
    t0s = [event[2] for event in events]
    assert None not in t0s and t0s[0] == t0s[1] and t0s[2] == t0s[3]


async def test_memory_stateless_counts_are_shared_between_endpoints() -> None:
    # Unlike 0.3.1, where each transport kept its own: a stateless client's
    # max_calls_per_session counts are the server's, as with RedisStore.
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Once per client."""
        return "once"

    one = StreamableHTTPTransport(server, legacy_sse=False)
    two = StreamableHTTPTransport(server, legacy_sse=False)
    call = modern("tools/call", {"name": "once"})
    async with asgi_client(one) as first, asgi_client(two) as second:
        answered = await first.post("/mcp", json=call, headers=headers_for(call))
        assert answered.json()["result"]["content"][0]["text"] == "once"
        refused = await second.post("/mcp", json=call, headers=headers_for(call))
        assert refused.json()["error"]["code"] == SESSION_LIMIT_EXCEEDED


def test_a_store_binds_to_one_server() -> None:
    store = MemoryStore()
    MCPServer(port=0, store=store)
    with pytest.raises(ValueError, match="one server"):
        MCPServer(port=0, store=store)
