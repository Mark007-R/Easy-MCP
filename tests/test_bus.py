"""The envelopes workers sharing a store send each other: sealing, parsing, checking."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from conftest import LogCapture
from shared_store_fake import FakeHub, FakeSharedStore

from easy_mcp import MCPServer, StreamableHTTPTransport
from easy_mcp.store.base import session_ref
from easy_mcp.transport import _bus, _sessions

SESSION = "a-session-id-of-192-random-bits-"
OTHER = "another-session-id-of-192-bits--"
WORKER = "0123456789abcdef"


def sealed(op: str = "cancel", **fields: object) -> str:
    fields = {"rid": 7, **fields} if op == "cancel" else fields
    if op == "deliver" and "msg" not in fields:
        fields["msg"] = {"jsonrpc": "2.0", "id": 3, "result": {}}
    return _bus.seal(op, "http", session_ref(SESSION), "abcdef012345", SESSION, WORKER, **fields)


def test_envelope_roundtrip() -> None:
    for op in ("cancel", "end", "deliver"):
        payload = sealed(op)
        # Canonical: sorted keys, no spaces, so every worker MACs the same bytes.
        data = json.loads(payload)
        assert payload == json.dumps(data, sort_keys=True, separators=(",", ":"))
        envelope = _bus.peek(payload)
        assert envelope is not None
        assert (envelope.op, envelope.kind, envelope.ref) == (op, "http", session_ref(SESSION))
        assert envelope.identity_fp == "abcdef012345" and envelope.src == WORKER
        assert _bus.verify(envelope, SESSION)
    deliver = _bus.peek(sealed("deliver", msg={"id": 1, "result": {"text": "é \ud800"}}))
    assert deliver is not None and deliver.body["msg"]["result"]["text"] == "é \ud800"
    assert _bus.verify(deliver, SESSION)
    anonymous = _bus.seal("end", "sse", session_ref(SESSION), None, SESSION, WORKER)
    parsed = _bus.peek(anonymous)
    assert parsed is not None and parsed.fp == "" and parsed.identity_fp is None


def test_a_forged_envelope_fails_verification() -> None:
    payload = sealed()
    envelope = _bus.peek(payload)
    assert envelope is not None
    # A MAC made with another session's id.
    assert not _bus.verify(envelope, OTHER)
    # A flipped field.
    data = json.loads(payload)
    data["rid"] = 8
    flipped = _bus.peek(json.dumps(data))
    assert flipped is not None and not _bus.verify(flipped, SESSION)
    # A wrong MAC.
    data = json.loads(payload)
    data["mac"] = "0" * 64
    forged = _bus.peek(json.dumps(data))
    assert forged is not None and not _bus.verify(forged, SESSION)


def test_stale_envelopes_are_dropped() -> None:
    now = int(time.time() * 1000)
    old = _bus.seal("end", "http", session_ref(SESSION), None, SESSION, WORKER, now_ms=now - 61_000)
    ahead = _bus.seal(
        "end", "http", session_ref(SESSION), None, SESSION, WORKER, now_ms=now + 61_000
    )
    fresh = _bus.seal(
        "end", "http", session_ref(SESSION), None, SESSION, WORKER, now_ms=now - 59_000
    )
    assert _bus.peek(old, now_ms=now) is None
    assert _bus.peek(ahead, now_ms=now) is None
    assert _bus.peek(fresh, now_ms=now) is not None


def test_malformed_or_oversized_payloads_are_ignored() -> None:
    good = json.loads(sealed())
    variants: list[str | bytes] = [
        "not json",
        b"\xff\xfe",
        "[]",
        json.dumps({**good, "v": 2}),
        json.dumps({**good, "op": "notify"}),
        json.dumps({**good, "k": "stdio"}),
        json.dumps({**good, "ref": "x" * 32}),
        json.dumps({**good, "fp": "NOT-HEX"}),
        json.dumps({**good, "src": ""}),
        json.dumps({**good, "mac": "short"}),
        json.dumps({**good, "ts": "now"}),
        json.dumps({**good, "ts": True}),
        json.dumps({**good, "rid": [1]}),
        json.dumps({**json.loads(sealed("deliver")), "msg": "text"}),
        "x" * (_bus.MAX_PAYLOAD_BYTES + 1),
    ]
    for payload in variants:
        assert _bus.peek(payload) is None, payload[:40]


def test_only_scalar_request_ids_are_relayed() -> None:
    for request_id in (True, False, {"a": 1}, [1], 1.5, None, "x" * 129, 2**53):
        assert not _bus.relayable_id(request_id), request_id
    for request_id in (0, -1, 2**53 - 1, "", "x" * 128, "req-1"):
        assert _bus.relayable_id(request_id), request_id


# ------------------------------------------------- a worker receiving them


async def held_session() -> tuple[Any, Any, asyncio.Task[None]]:
    """A worker's manager with one session held open and a call in flight."""
    hub = FakeHub()
    store = hub.store(WORKER)
    server = MCPServer(port=0, rate_limit_per_minute=None, store=store)
    manager = StreamableHTTPTransport(server)._manager
    await store.start()
    local = await manager.open(SESSION, client_id="ip:x", identity=None)
    assert local is not None
    call = asyncio.create_task(asyncio.sleep(30))
    local.in_flight[7] = call
    return manager, local, call


def from_peer(op: str, session_id: str = SESSION, fp: str | None = None, **fields: Any) -> str:
    fields = {"rid": 7, **fields} if op == "cancel" else fields
    return _bus.seal(op, "http", session_ref(SESSION), fp, session_id, "fedcba9876543210", **fields)


async def test_a_forged_envelope_is_rejected_and_audited(logs: LogCapture) -> None:
    manager, local, call = await held_session()
    try:
        flipped = json.loads(from_peer("cancel"))
        flipped["rid"] = 8
        forged = json.loads(from_peer("cancel"))
        forged["mac"] = "0" * 64
        for payload in (from_peer("cancel", OTHER), json.dumps(flipped), json.dumps(forged)):
            manager._on_bus(payload)
        manager._on_bus(from_peer("end", OTHER))
        await asyncio.sleep(0)
        assert not call.cancelled() and not local.ended
        rejected = logs.events("bus_message_rejected")
        assert [event["reason"] for event in rejected] == ["mac"] * 4
        assert rejected[0] == {
            "type": "bus_message_rejected",
            "op": "cancel",
            "session_ref": session_ref(SESSION),
            "src": "fedcba9876543210",
            "reason": "mac",
        }
        # Our own broadcast, and sessions this worker knows nothing of, are ignored.
        manager._on_bus(_bus.seal("cancel", "http", local.ref, None, SESSION, WORKER, rid=7))
        stranger = _bus.seal("end", "http", session_ref(OTHER), None, OTHER, "fedcba9876543210")
        manager._on_bus(stranger)
        await asyncio.sleep(0)
        assert not call.cancelled() and len(logs.events("bus_message_rejected")) == 4
        # The genuine article works.
        manager._on_bus(from_peer("cancel"))
        await asyncio.sleep(0)
        assert call.cancelled()
    finally:
        call.cancel()
        await manager.shutdown()


async def test_an_envelope_for_another_identity_is_rejected(logs: LogCapture) -> None:
    manager, local, call = await held_session()
    try:
        manager._on_bus(from_peer("cancel", fp="abcdef012345"))
        manager._on_bus(from_peer("end", fp="abcdef012345"))
        await asyncio.sleep(0)
        assert not call.cancelled() and not local.ended
        reasons = [event["reason"] for event in logs.events("bus_message_rejected")]
        assert reasons == ["identity", "identity"]
        manager._on_bus(from_peer("end"))
        await asyncio.sleep(0)
        assert call.cancelled() and local.ended
    finally:
        call.cancel()
        await manager.shutdown()


async def test_no_payload_escapes_the_listener(logs: LogCapture) -> None:
    manager, local, call = await held_session()
    try:
        for payload in ("", "null", "{", "[1]", "x" * (_bus.MAX_PAYLOAD_BYTES + 1)):
            manager._on_bus(payload)
        deliver = from_peer("deliver", msg={"jsonrpc": "2.0", "id": 1, "result": {}})
        manager._on_bus(deliver)  # a stream this worker does not hold: nothing to do
        await asyncio.sleep(0)
        assert not call.cancelled() and logs.events("bus_message_rejected") == []
    finally:
        call.cancel()
        await manager.shutdown()


async def test_an_end_announced_while_a_lookup_is_under_way_is_not_missed() -> None:
    hub = FakeHub()
    server = MCPServer(port=0, rate_limit_per_minute=None, store=hub.store(WORKER))
    opener = StreamableHTTPTransport(
        MCPServer(port=0, rate_limit_per_minute=None, store=hub.store("1" * 16))
    )._manager
    manager = StreamableHTTPTransport(server)._manager
    local = await opener.open(SESSION, client_id="ip:x", identity=None)
    assert local is not None
    await opener.finish(local)
    # The end arrives while this worker is still asking the store.
    manager._pending_add(session_ref(SESSION), SESSION)
    manager._on_bus(from_peer("end"))
    manager._pending_remove(session_ref(SESSION))
    assert await manager.acquire(SESSION) is _sessions.Rejection.NOT_FOUND
    await manager.shutdown()
    await opener.shutdown()


class EndedDuringLookup(FakeSharedStore):
    """A store whose session another worker ends while it is being looked up."""

    manager: Any = None

    async def acquire_session(self, *args: Any, **kwargs: Any) -> Any:
        found = await super().acquire_session(*args, **kwargs)
        # With Redis, the bus listener runs while the lookup's round trip is
        # suspended: here, between the store's answer and acquire() seeing it.
        self.manager._on_bus(from_peer("end"))
        return found


async def test_an_end_announced_during_the_store_call_is_not_missed() -> None:
    hub = FakeHub()
    store = EndedDuringLookup(hub, WORKER)
    hub.stores.append(store)
    manager = StreamableHTTPTransport(
        MCPServer(port=0, rate_limit_per_minute=None, store=store)
    )._manager
    store.manager = manager
    opener = StreamableHTTPTransport(
        MCPServer(port=0, rate_limit_per_minute=None, store=hub.store("1" * 16))
    )._manager
    local = await opener.open(SESSION, client_id="ip:x", identity=None)
    assert local is not None
    await opener.finish(local)
    del hub.calls[:]
    # The store found the session, but it ended meanwhile: not served.
    assert await manager.acquire(SESSION) is _sessions.Rejection.NOT_FOUND
    assert hub.calls == ["acquire", "release"]  # the hold is given back, untouched
    assert session_ref(SESSION) not in manager._local
    await manager.shutdown()
    await opener.shutdown()
