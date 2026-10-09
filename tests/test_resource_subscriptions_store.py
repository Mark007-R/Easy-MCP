"""Where a session's resource subscriptions live: MemoryStore, RedisStore (unit), a real Redis.

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
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import notification, rpc

from easy_mcp import MCPServer, MemoryStore
from easy_mcp.store.base import SessionRecord, Store, StoreHandle, session_ref
from easy_mcp.transport import _bus

REDIS_URL = os.environ.get("EASY_MCP_LIVE_REDIS_URL")
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


def record(ref: str, kind: Any = "http") -> SessionRecord:
    return SessionRecord(ref, kind, "ip:x", None, session_id="raw-id")


async def test_memory_store_keeps_subscriptions_with_the_session() -> None:
    store = MemoryStore()
    ref = session_ref("s")
    assert (await store.create_session(record(ref), cap=5, ttl=60))[0]
    assert await store.update_subscriptions("http", ref, add=["b://x", "a://x"], cap=3) == (
        "a://x",
        "b://x",
    )
    found, _ = await store.acquire_session("http", ref, ttl=60)
    assert found is not None and found.subscriptions == ("a://x", "b://x")
    # The cap: what is over it is left out, what is there already stays.
    assert await store.update_subscriptions("http", ref, add=["c://x", "d://x"], cap=3) == (
        "a://x",
        "b://x",
        "c://x",
    )
    assert await store.update_subscriptions("http", ref, add=["a://x"], cap=3) == (
        "a://x",
        "b://x",
        "c://x",
    )
    both = await store.update_subscriptions("http", ref, add=["d://x"], remove=["a://x"], cap=3)
    assert both == ("b://x", "c://x", "d://x")
    # Another kind, or a session that is gone, is left alone.
    assert await store.update_subscriptions("sse", ref, add=["z://x"], cap=3) is None
    assert (
        await store.update_subscriptions("http", session_ref("gone"), add=["z://x"], cap=3) is None
    )


async def test_the_interface_default_keeps_none() -> None:
    class Minimal(MemoryStore):
        update_subscriptions = Store.update_subscriptions  # the interface's default

    with pytest.raises(NotImplementedError):
        await Minimal().update_subscriptions("http", session_ref("s"), add=["a://x"], cap=1)

    class Handle(StoreHandle):
        async def reserve_call(self, tool: str, limit: int) -> Any:
            raise AssertionError

        async def release_call(self, tool: str) -> None:
            raise AssertionError

    with pytest.raises(NotImplementedError):
        await Handle().update_subscriptions(add=["a://x"], cap=1)


def test_resub_travels_on_the_bus_and_needs_the_session_id() -> None:
    ref = session_ref("the-session")
    payload = _bus.seal("resub", "http", ref, None, "the-session", "a" * 16)
    envelope = _bus.peek(payload)
    assert envelope is not None and envelope.op == "resub"
    assert _bus.verify(envelope, "the-session")
    assert not _bus.verify(envelope, "another-session")


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


async def test_redis_store_updates_subscriptions_in_one_script() -> None:
    pytest.importorskip("redis")
    from easy_mcp import RedisStore
    from easy_mcp.store.redis_store import _SCRIPTS, SESSION_SUBSCRIPTIONS

    client = _Client({text: name for name, text in _SCRIPTS.items()})
    store = RedisStore.from_client(client, namespace="t")
    ref = session_ref("s")
    try:
        client.replies["subscriptions"] = [b"b://x", b"a://x"]
        subscribed = await store.update_subscriptions(
            "http", ref, add=["a://x", "b://x", "a://x"], remove=["c://x"], cap=1000
        )
        assert subscribed == ("a://x", "b://x")
        name, keys, args = client.calls[-1]
        assert name == "subscriptions"
        assert keys == [store._session_key(ref)]
        assert args == ["http", 1000, 2, "a://x", "b://x", "c://x"]
        client.replies["subscriptions"] = None  # the script's false: gone
        assert await store.update_subscriptions("http", ref, add=["a://x"], cap=5) is None
        client.replies["subscriptions"] = []
        assert await store.update_subscriptions("http", ref, remove=["a://x"], cap=5) == ()
        # Guarded by the kind, so a lapsed record is never created again without a TTL.
        assert "HGET', KEYS[1], 'kind'" in SESSION_SUBSCRIPTIONS

        flat = ["kind", "http", "cid", "ip:x", "fp", "", "t0", "1"]
        client.replies["touch"] = flat + ["rs", json.dumps(["b://x", "a://x", 3])]
        found, _ = await store.acquire_session("http", ref, ttl=60)
        assert found is not None and found.subscriptions == ("a://x", "b://x")
        for broken in ("not json", '{"a": 1}', ""):
            client.replies["touch"] = flat + ["rs", broken]
            found, _ = await store.acquire_session("http", ref, ttl=60)
            assert found is not None and found.subscriptions is None, broken
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
async def test_live_redis_keeps_subscriptions_in_the_session_record() -> None:
    pytest.importorskip("redis")
    from easy_mcp import RedisStore

    namespace = "rp-" + secrets.token_hex(6)
    assert REDIS_URL is not None
    store = RedisStore(REDIS_URL, namespace=namespace)
    store.bind("resources-tests")
    ref = session_ref(secrets.token_urlsafe(24))
    shared = SessionRecord(ref, "http", "ip:x", None)
    odd = 'files://dir/caf\u00e9 "quoted" /x'
    try:
        await store.start()
        assert (await store.create_session(shared, cap=5, ttl=60))[0]
        assert await store.update_subscriptions("http", ref, add=[odd, "a://x"], cap=2) == (
            "a://x",
            odd,
        )
        assert await store.update_subscriptions("http", ref, add=["b://x"], cap=2) == ("a://x", odd)
        found, _ = await store.acquire_session("http", ref, ttl=60)
        assert found is not None and found.subscriptions == ("a://x", odd)
        assert await store.update_subscriptions("http", ref, remove=["a://x", odd], cap=2) == ()
        found, _ = await store.acquire_session("http", ref, ttl=60)
        assert found is not None and not found.subscriptions
        # Another kind is not touched, and a session that is gone stays gone.
        assert await store.update_subscriptions("sse", ref, add=["a://x"], cap=2) is None
        gone = session_ref("never-opened-" + secrets.token_hex(4))
        assert await store.update_subscriptions("http", gone, add=["a://x"], cap=2) is None
        assert (await store.acquire_session("http", gone, ttl=60))[0] is None
        assert await store.delete_session("http", ref)
    finally:
        await store.aclose()
        await _drop_namespace(namespace)


@live
async def test_live_redis_subscription_on_one_worker_reaches_the_stream_on_another(
    live_server: Callable[[Any], str],
) -> None:
    pytest.importorskip("redis")
    from easy_mcp import RedisStore

    assert REDIS_URL is not None
    namespace = "rp-" + secrets.token_hex(6)
    servers = []
    bases = []
    for _ in range(2):
        server = MCPServer(
            port=0,
            name="resources-live",
            rate_limit_per_minute=None,
            store=RedisStore(REDIS_URL, namespace=namespace),
        )
        server.register_resource(lambda: "app", "config://app", name="config")
        servers.append(server)
        bases.append(live_server(server))
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            init = await client.post(
                f"{bases[0]}/mcp", json=rpc("initialize", INIT), headers=ACCEPT
            )
            assert init.status_code == 200, init.text
            session = init.headers["mcp-session-id"]
            headers = {**ACCEPT, "MCP-Session-Id": session}
            done = await client.post(
                f"{bases[0]}/mcp", json=notification("notifications/initialized"), headers=headers
            )
            assert done.status_code == 202
            stream_headers = {"Accept": "text/event-stream", "MCP-Session-Id": session}
            async with client.stream("GET", f"{bases[0]}/mcp", headers=stream_headers) as response:
                assert response.status_code == 200
                subscribed = await client.post(
                    f"{bases[1]}/mcp",
                    json=rpc("resources/subscribe", {"uri": "config://app"}, 2),
                    headers=headers,
                )
                assert subscribed.status_code == 200, subscribed.text
                deadline = asyncio.get_running_loop().time() + 10
                while servers[0]._notifier.subscriptions(session) != {"config://app"}:
                    assert asyncio.get_running_loop().time() < deadline, "no resub arrived"
                    await asyncio.sleep(0.05)
                await asyncio.to_thread(servers[0].notify_resource_updated, "config://app")
                lines = response.aiter_lines()

                async def first_event() -> Any:
                    async for line in lines:
                        if line.startswith("data: "):
                            return json.loads(line[len("data: ") :])
                    return None

                event = await asyncio.wait_for(first_event(), 10)
                assert event == {
                    "jsonrpc": "2.0",
                    "method": "notifications/resources/updated",
                    "params": {"uri": "config://app"},
                }
    finally:
        await _drop_namespace(namespace)
