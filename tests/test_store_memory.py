"""MemoryStore, the default store: 0.3.1's in-process state behind the Store interface."""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any

import httpx
import pytest

from easy_mcp import MCPServer, MemoryStore
from easy_mcp.security.ratelimit import SlidingWindowRateLimiter
from easy_mcp.store import Reservation, SessionRecord
from easy_mcp.store.base import session_ref
from easy_mcp.store.memory import STATELESS_CLIENTS_MAX


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
    assert run_now(store.delete_session("sse", rec.ref)) is None


def test_a_store_binds_to_one_server() -> None:
    store = MemoryStore()
    MCPServer(port=0, store=store)
    with pytest.raises(ValueError, match="one server"):
        MCPServer(port=0, store=store)
