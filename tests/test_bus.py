"""The envelopes workers sharing a store send each other: sealing, parsing, checking."""

from __future__ import annotations

import json
import time

from easy_mcp.store.base import session_ref
from easy_mcp.transport import _bus

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
