"""The messages workers sharing a store send each other about a session.

Four operations travel between workers: ``cancel`` (a request running on
another worker), ``end`` (a session ended on another worker), ``deliver``
(an answer for the legacy SSE stream another worker holds) and ``resub``
(the session's resource subscriptions changed in the store: the worker
holding its stream reads them again).
Each is a JSON envelope naming the session by its ref and authenticated
with a MAC keyed by the raw session id.  The store never sees that id, so
access to the store alone cannot forge, and so inject into a stream,
cancel or end anything.  A worker acts on an envelope only for a session
whose id it holds (one of its requests reached it), after checking the MAC,
the identity the session is bound to, and the envelope's age.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from dataclasses import dataclass
from typing import Any

VERSION = 1
OPS = frozenset({"cancel", "end", "deliver", "resub"})
KINDS = frozenset({"http", "sse"})

# A relayed legacy SSE answer larger than this is replaced by an error: it
# stays under Redis's default pub/sub output limit (8 MB for 60 s), so one
# huge result cannot get a worker's subscriber disconnected.
RELAY_MAX_BYTES = 4 * 1024 * 1024
MAX_PAYLOAD_BYTES = RELAY_MAX_BYTES + 64 * 1024

# Envelopes older (or newer) than this are dropped.  Generous, because only
# this uses the workers' own clocks, and they may drift.
FRESHNESS_MS = 60_000

# Request ids relayed in a cancel: JSON-safe integers, or short strings.
MAX_REQUEST_ID_CHARS = 128
_MAX_SAFE_INTEGER = 2**53 - 1

_REF = re.compile(r"[0-9a-f]{32}")
_FINGERPRINT = re.compile(r"[0-9a-f]{0,64}")
_WORKER = re.compile(r"[0-9a-f]{1,64}")
_MAC = re.compile(r"[0-9a-f]{64}")


def relayable_id(request_id: object) -> bool:
    """Whether a cancel for *request_id* may travel to other workers.

    An integer (not a bool) in the JSON-safe range, or a string of at most
    128 characters; anything else is treated as an unknown id.
    """
    if isinstance(request_id, bool):
        return False
    if isinstance(request_id, int):
        return -_MAX_SAFE_INTEGER <= request_id <= _MAX_SAFE_INTEGER
    return isinstance(request_id, str) and len(request_id) <= MAX_REQUEST_ID_CHARS


def canonical(body: dict[str, Any]) -> str:
    """*body* as the bus carries it: sorted keys, no spaces, ASCII only.

    ASCII escapes every character outside it, lone surrogates included, so
    any JSON a tool answers with can be sealed, and its length is its size.
    """
    return json.dumps(body, sort_keys=True, separators=(",", ":"))


def _key(session_id: str) -> bytes:
    return hmac.new(session_id.encode(), b"easy-mcp/bus/v1", hashlib.sha256).digest()


def _mac(session_id: str, body: dict[str, Any]) -> str:
    return hmac.new(_key(session_id), canonical(body).encode(), hashlib.sha256).hexdigest()


def _now_ms() -> int:
    return int(time.time() * 1000)


def seal(
    op: str,
    kind: str,
    ref: str,
    fp: str | None,
    session_id: str,
    worker: str,
    *,
    now_ms: int | None = None,
    **fields: Any,
) -> str:
    """An envelope for *op* on the session *session_id*, ready to publish.

    *fp* is the fingerprint the session is bound to (``None``: anonymous);
    *fields* are the operation's own (``rid`` for a cancel, ``msg`` for a
    deliver).
    """
    body: dict[str, Any] = {
        "v": VERSION,
        "op": op,
        "k": kind,
        "ref": ref,
        "fp": fp or "",
        "ts": _now_ms() if now_ms is None else now_ms,
        "src": worker,
        **fields,
    }
    body["mac"] = _mac(session_id, body)
    return canonical(body)


@dataclass(frozen=True, slots=True)
class Envelope:
    """A parsed envelope; nothing in it is trusted until :func:`verify` says so."""

    op: str
    kind: str
    ref: str
    fp: str
    ts: int
    src: str
    mac: str
    body: dict[str, Any]  # everything but the MAC

    @property
    def identity_fp(self) -> str | None:
        return self.fp or None


def peek(payload: str | bytes, *, now_ms: int | None = None) -> Envelope | None:
    """Parse *payload*; ``None`` for anything malformed, oversized or stale.

    Never raises.
    """
    if len(payload) > MAX_PAYLOAD_BYTES:
        return None
    try:
        data = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict) or data.get("v") != VERSION:
        return None
    op, kind, ref, fp = data.get("op"), data.get("k"), data.get("ref"), data.get("fp")
    ts, src, mac = data.get("ts"), data.get("src"), data.get("mac")
    if op not in OPS or kind not in KINDS:
        return None
    if not (isinstance(ref, str) and _REF.fullmatch(ref)):
        return None
    if not (isinstance(fp, str) and _FINGERPRINT.fullmatch(fp)):
        return None
    if not (isinstance(src, str) and _WORKER.fullmatch(src)):
        return None
    if not (isinstance(mac, str) and _MAC.fullmatch(mac)):
        return None
    if not isinstance(ts, int) or isinstance(ts, bool):
        return None
    now = _now_ms() if now_ms is None else now_ms
    if abs(now - ts) > FRESHNESS_MS:
        return None
    if op == "cancel" and not relayable_id(data.get("rid")):
        return None
    if op == "deliver" and not isinstance(data.get("msg"), dict):
        return None
    body = {key: value for key, value in data.items() if key != "mac"}
    return Envelope(op, kind, ref, fp, ts, src, mac, body)


def verify(envelope: Envelope, session_id: str) -> bool:
    """Whether *envelope* was sealed by a holder of *session_id* (constant time)."""
    return hmac.compare_digest(_mac(session_id, envelope.body), envelope.mac)
