"""Where a session's list baselines live: MemoryStore, RedisStore (unit), and a real Redis.

The live tests at the end are skipped unless ``EASY_MCP_LIVE_REDIS_URL`` is
set to an admin-capable URL of a scratch database (CI runs them against its
``redis:7-alpine`` service); each uses a namespace of its own and removes
its keys afterwards.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
from typing import Any

import pytest

from easy_mcp import MemoryStore
from easy_mcp.store.base import SessionRecord, Store, session_ref

REDIS_URL = os.environ.get("EASY_MCP_LIVE_REDIS_URL")
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}
DIGEST = "0123456789abcdef" * 2


def record(ref: str, kind: Any = "http") -> SessionRecord:
    return SessionRecord(ref, kind, "ip:x", None, session_id="raw-id")


async def test_memory_store_keeps_baselines_with_the_session() -> None:
    store = MemoryStore()
    ref = session_ref("s")
    assert (await store.create_session(record(ref), cap=5, ttl=60))[0]
    await store.release_session("http", ref, ttl=60)
    found, _ = await store.acquire_session("http", ref, ttl=60)
    assert found is not None and found.baselines is None
    await store.save_baselines("http", ref, {"tools": DIGEST, "prompts": "f" * 32})
    found, _ = await store.acquire_session("http", ref, ttl=60)
    assert found is not None
    assert found.baselines == (("prompts", "f" * 32), ("tools", DIGEST))
    # Another kind, or a session that is gone, is left alone.
    await store.save_baselines("sse", ref, {"tools": "0" * 32})
    await store.save_baselines("http", session_ref("gone"), {"tools": "0" * 32})
    found, _ = await store.acquire_session("http", ref, ttl=60)
    assert found is not None and dict(found.baselines or ()) == {
        "prompts": "f" * 32,
        "tools": DIGEST,
    }


async def test_a_store_without_baselines_records_nothing() -> None:
    class Minimal(MemoryStore):
        save_baselines = Store.save_baselines  # the interface's default

    store = Minimal()
    ref = session_ref("s")
    await store.create_session(record(ref), cap=5, ttl=60)
    await store.save_baselines("http", ref, {"tools": DIGEST})
    found, _ = await store.acquire_session("http", ref, ttl=60)
    assert found is not None and found.baselines is None


# ----------------------------------------------------------- RedisStore, unit


class _Script:
    def __init__(self, client: _Client, name: str) -> None:
        self._client = client
        self._name = name

    async def __call__(self, keys: Any = None, args: Any = None) -> Any:
        self._client.calls.append((self._name, list(keys or []), list(args or [])))
        return self._client.replies.get(self._name)


class _PubSub:
    async def subscribe(self, *channels: str) -> None:
        return None

    async def get_message(self, **_: Any) -> Any:
        await asyncio.sleep(0.05)
        return None

    async def aclose(self) -> None:
        return None


class _Client:
    """Just what RedisStore calls on a redis.asyncio.Redis."""

    def __init__(self, names: dict[str, str]) -> None:
        self._names = names
        self.calls: list[tuple[str, list[Any], list[Any]]] = []
        self.replies: dict[str, Any] = {}

    def register_script(self, text: str) -> _Script:
        return _Script(self, self._names[text])

    async def ping(self) -> bool:
        return True

    def pubsub(self) -> _PubSub:
        return _PubSub()


async def test_redis_store_writes_baselines_only_into_a_live_session_of_its_kind() -> None:
    pytest.importorskip("redis")
    from easy_mcp import RedisStore
    from easy_mcp.store.redis_store import _SCRIPTS, SESSION_BASELINES

    client = _Client({text: name for name, text in _SCRIPTS.items()})
    store = RedisStore.from_client(client, namespace="t")
    ref = session_ref("s")
    try:
        client.replies["baselines"] = 1
        await store.save_baselines("http", ref, {"tools": DIGEST, "prompts": "f" * 32})
        name, keys, args = client.calls[-1]
        assert name == "baselines"
        assert keys == [store._session_key(ref)]
        expected = json.dumps({"prompts": "f" * 32, "tools": DIGEST}, separators=(",", ":"))
        assert args == ["http", expected]
        # Guarded by the kind, so a lapsed record is never created again without a TTL.
        assert "HGET', KEYS[1], 'kind'" in SESSION_BASELINES

        flat = ["kind", "http", "cid", "ip:x", "fp", "", "t0", "1"]
        client.replies["touch"] = flat + ["bl", json.dumps({"tools": DIGEST})]
        found, _ = await store.acquire_session("http", ref, ttl=60)
        assert found is not None and found.baselines == (("tools", DIGEST),)
        for broken in ("not json", "[1, 2]", ""):
            client.replies["touch"] = flat + ["bl", broken]
            found, _ = await store.acquire_session("http", ref, ttl=60)
            assert found is not None and found.baselines is None, broken
        client.replies["touch"] = flat + ["bl", json.dumps({"tools": 7, "prompts": "p"})]
        found, _ = await store.acquire_session("http", ref, ttl=60)
        assert found is not None and found.baselines == (("prompts", "p"),)
    finally:
        await store.aclose()


# --------------------------------------------------------- a real Redis


live = pytest.mark.skipif(not REDIS_URL, reason="EASY_MCP_LIVE_REDIS_URL is not set")


async def _drop_namespace(namespace: str) -> None:
    redis = pytest.importorskip("redis.asyncio")
    admin = redis.Redis.from_url(REDIS_URL)
    try:
        keys = [key async for key in admin.scan_iter(match=f"easy-mcp:1:{{{namespace}}}:*")]
        if keys:
            await admin.delete(*keys)
    finally:
        await admin.aclose()


@live
async def test_live_redis_keeps_baselines_in_the_session_record() -> None:
    pytest.importorskip("redis")
    from easy_mcp import RedisStore

    namespace = "lc-" + secrets.token_hex(6)
    assert REDIS_URL is not None
    store = RedisStore(REDIS_URL, namespace=namespace)
    store.bind("list-changed-tests")
    ref = session_ref(secrets.token_urlsafe(24))
    shared = SessionRecord(ref, "http", "ip:x", None)
    try:
        await store.start()
        assert (await store.create_session(shared, cap=5, ttl=60))[0]
        await store.save_baselines("http", ref, {"tools": DIGEST})
        found, _ = await store.acquire_session("http", ref, ttl=60)
        assert found is not None and found.baselines == (("tools", DIGEST),)
        await store.save_baselines("sse", ref, {"tools": "0" * 32})  # another kind: no change
        found, _ = await store.acquire_session("http", ref, ttl=60)
        assert found is not None and found.baselines == (("tools", DIGEST),)
        # A session that is gone stays gone: no key is made for it.
        gone = session_ref("never-opened-" + secrets.token_hex(4))
        await store.save_baselines("http", gone, {"tools": DIGEST})
        assert (await store.acquire_session("http", gone, ttl=60))[0] is None
        assert await store.delete_session("http", ref)
    finally:
        await store.aclose()
        await _drop_namespace(namespace)
