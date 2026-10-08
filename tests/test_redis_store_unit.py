"""RedisStore without a Redis server: configuration, keys, error mapping, script replies.

These need the ``redis`` package (the ``[redis]`` extra, or ``[dev]``), not a
server; tests/test_live_redis.py runs the scripts against a real one.
"""

from __future__ import annotations

import asyncio
import collections
import sys
from typing import Any

import pytest
from conftest import LogCapture

pytest.importorskip("redis")

from redis import exceptions as redis_errors  # noqa: E402

from easy_mcp import MCPServer, RedisStore, StoreUnavailableError  # noqa: E402
from easy_mcp.exceptions import RateLimitError  # noqa: E402
from easy_mcp.security.ratelimit import SlidingWindowRateLimiter  # noqa: E402
from easy_mcp.store import Reservation, SessionRecord  # noqa: E402
from easy_mcp.store.base import client_ref, session_ref  # noqa: E402
from easy_mcp.store.redis_store import _SCRIPTS  # noqa: E402

_NAMES = {text: name for name, text in _SCRIPTS.items()}

# Built, not written out, so secret scanners do not take the fixtures for credentials.
PASSWORD = "s3cr3t-" + "pw"
QUERY_PASSWORD = "qs-" + "pw"


class FakeScript:
    def __init__(self, client: FakeClient, name: str) -> None:
        self._client = client
        self._name = name

    async def __call__(self, keys: Any = None, args: Any = None) -> Any:
        self._client.calls.append((self._name, list(keys or []), list(args or [])))
        reply = self._client.replies.get(self._name)
        if isinstance(reply, BaseException):
            raise reply
        return reply


class FakePubSub:
    def __init__(self, client: FakeClient) -> None:
        self._client = client

    async def subscribe(self, *channels: str) -> None:
        self._client.channels.extend(channels)

    async def get_message(self, **_: Any) -> Any:
        await asyncio.sleep(0.05)
        return None

    async def aclose(self) -> None:
        return None


class FakeClient:
    """Just what RedisStore calls on a redis.asyncio.Redis."""

    def __init__(self) -> None:
        self.replies: dict[str, Any] = {}
        self.calls: list[tuple[str, list[Any], list[Any]]] = []
        self.channels: list[str] = []
        self.ping_error: BaseException | None = None
        self.published: list[tuple[str, str]] = []

    def register_script(self, text: str) -> FakeScript:
        return FakeScript(self, _NAMES[text])

    async def ping(self) -> bool:
        if self.ping_error is not None:
            raise self.ping_error
        return True

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append((channel, payload))
        reply = self.replies.get("publish", 1)
        if isinstance(reply, BaseException):
            raise reply
        return int(reply)

    def pubsub(self) -> FakePubSub:
        return FakePubSub(self)


def test_missing_extra_names_the_install_command(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "redis", None)
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)
    with pytest.raises(ImportError, match=r'pip install "easy-mcp-kit\[redis\]"'):
        RedisStore("redis://127.0.0.1:6379/0")
    with pytest.raises(ImportError, match="redis extra"):
        RedisStore.from_client(object())


def test_from_env_requires_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EASY_MCP_REDIS_URL", raising=False)
    with pytest.raises(ValueError, match="EASY_MCP_REDIS_URL"):
        RedisStore.from_env()
    monkeypatch.setenv("EASY_MCP_REDIS_URL", "")
    with pytest.raises(ValueError, match="EASY_MCP_REDIS_URL"):
        RedisStore.from_env()
    monkeypatch.setenv("MY_REDIS", "redis://127.0.0.1:6379/3")
    store = RedisStore.from_env("MY_REDIS", namespace="mine")
    assert store.describe() == "redis redis://127.0.0.1:6379/3 namespace=mine"
    with pytest.raises(ValueError, match="URL"):
        RedisStore("")


def test_url_credentials_are_never_described() -> None:
    userinfo = "reports:" + PASSWORD
    url = f"redis://{userinfo}@redis.internal:6380/2?password={QUERY_PASSWORD}&socket_timeout=5"
    store = RedisStore(url, namespace="reports")
    described = store.describe()
    assert described == "redis redis://redis.internal:6380/2 namespace=reports"
    warnings = store.warnings()
    assert warnings == ["traffic to redis.internal is not encrypted; use rediss://"]
    for text in (described, *warnings):
        assert "reports:" not in text and PASSWORD not in text and QUERY_PASSWORD not in text
    tls = RedisStore("rediss://redis.internal:6379/0", namespace="reports")
    assert tls.warnings() == [
        "redis.internal is reached without a password; give the store an ACL user"
    ]
    unix = RedisStore(f"unix:///run/redis.sock?db=1&password={PASSWORD}", namespace="reports")
    assert unix.describe() == "redis unix:///run/redis.sock namespace=reports"
    assert unix.warnings() == []
    userinfo = "user:" + PASSWORD
    ipv6 = RedisStore(f"rediss://{userinfo}@[2001:db8::1]:6379/0", namespace="reports")
    assert ipv6.describe() == "redis rediss://[2001:db8::1]:6379/0 namespace=reports"
    assert RedisStore.from_client(FakeClient(), namespace="x").describe() == (
        "redis (own client) namespace=x"
    )


def test_keys_are_versioned_namespaced_and_hash_tagged() -> None:
    store = RedisStore("redis://127.0.0.1:6379/0")
    MCPServer(port=0, name="My Tools", store=store)
    ref = session_ref("some-session")
    assert store.namespace == "my-tools"
    assert store._session_key(ref) == f"easy-mcp:1:{{my-tools}}:s:{ref}"
    assert store._index_key("http") == "easy-mcp:1:{my-tools}:i:http"
    assert store._index_key("sse") == "easy-mcp:1:{my-tools}:i:sse"
    assert store._key("c", client_ref("ip:1.2.3.4")) == (
        f"easy-mcp:1:{{my-tools}}:c:{client_ref('ip:1.2.3.4')}"
    )
    assert store._key("r", client_ref("ip:1.2.3.4")).startswith("easy-mcp:1:{my-tools}:r:")
    assert store._bus_channel() == "easy-mcp:1:{my-tools}:bus"
    assert store._worker_channel(store.worker_id) == f"easy-mcp:1:{{my-tools}}:w:{store.worker_id}"
    assert len(ref) == 32 and len(store.worker_id) == 16
    # Refs and client refs are digests: no session id or address in a key.
    assert "some-session" not in store._session_key(ref)
    assert "1.2.3.4" not in store._key("c", client_ref("ip:1.2.3.4"))


async def test_redis_errors_map_to_store_unavailable(logs: LogCapture) -> None:
    client = FakeClient()
    store = RedisStore.from_client(client, namespace="t")
    record = SessionRecord(session_ref("s"), "http", "ip:x", None)
    try:
        for error in (
            redis_errors.ConnectionError("refused"),
            redis_errors.TimeoutError("slow"),
            redis_errors.BusyLoadingError("loading"),
            redis_errors.ResponseError("OOM command not allowed when used memory > 'maxmemory'"),
            redis_errors.ResponseError("READONLY You can't write against a read only replica."),
            redis_errors.ResponseError("MASTERDOWN Link with MASTER is down"),
            redis_errors.AuthenticationError("WRONGPASS"),
            redis_errors.NoPermissionError("NOPERM"),
            TimeoutError(),
            ConnectionResetError(),
        ):
            client.replies = {name: error for name in _SCRIPTS}
            client.replies["publish"] = error
            store._warned_at = float("-inf")
            with pytest.raises(StoreUnavailableError):
                await store.create_session(record, cap=5, ttl=60)
            with pytest.raises(StoreUnavailableError):
                await store.reserve_session_call(record.ref, "t", 1)
            with pytest.raises(StoreUnavailableError):
                await store.publish("x")
            with pytest.raises(StoreUnavailableError):
                await store.rate_limiter(SlidingWindowRateLimiter(3)).acheck("ip:x")
        # A reply that means a bug is not an outage.
        client.replies["reserve"] = redis_errors.ResponseError("WRONGTYPE Operation against a key")
        with pytest.raises(redis_errors.ResponseError):
            await store.reserve_session_call(record.ref, "t", 1)
        # The warning is logged at most once per 10 s, naming the error only.
        logs.records.clear()
        store._warned_at = float("-inf")
        client.replies["reserve"] = redis_errors.ConnectionError("refused")
        for _ in range(3):
            with pytest.raises(StoreUnavailableError):
                await store.reserve_session_call(record.ref, "t", 1)
        warned = [r.getMessage() for r in logs.records if r.name == "easy_mcp.store"]
        assert warned == ["store unreachable: ConnectionError"]
        # Credentials refused at startup are a configuration error: it raises.
        client.ping_error = redis_errors.AuthenticationError("WRONGPASS invalid password")
        with pytest.raises(redis_errors.AuthenticationError):
            await store.start()
        client.ping_error = redis_errors.NoPermissionError("NOPERM")
        with pytest.raises(redis_errors.NoPermissionError):
            await store.start()
        # One out of reach is logged, and the worker starts anyway.
        client.ping_error = redis_errors.ConnectionError("refused")
        await store.start()
        assert not await store.ping()
    finally:
        await store.aclose()


async def test_script_replies_are_parsed() -> None:
    client = FakeClient()
    store = RedisStore.from_client(client, namespace="t")
    ref = session_ref("s")
    other = session_ref("t")
    record = SessionRecord(ref, "sse", "ip:x", "abcdef012345", owner="0123456789abcdef")
    try:
        client.replies["create"] = [1, [other]]
        created, expired = await store.create_session(record, cap=5, ttl=60)
        assert created and [(gone.ref, gone.client_id, gone.session_id) for gone in expired] == [
            (other, None, None)
        ]
        name, keys, args = client.calls[-1]
        assert name == "create"
        assert keys == [store._session_key(ref), store._index_key("sse")]
        assert args == [ref, 60_000, 5, "sse", "ip:x", "abcdef012345", "0123456789abcdef", ""]
        client.replies["create"] = [0, []]
        assert await store.create_session(record, cap=5, ttl=60) == (False, [])
        client.replies["create"] = [-1, []]
        with pytest.raises(ValueError, match="exists"):
            await store.create_session(record, cap=5, ttl=60)

        flat = ["kind", "sse", "cid", "ip:x", "fp", "abcdef012345", "own", "0123456789abcdef"]
        flat += ["pr", "", "ver", "2025-11-25", "t0", "1791460000123", "c:add", "1"]
        client.replies["touch"] = flat
        found, expired = await store.acquire_session("sse", ref, ttl=None)
        assert expired == [] and found == SessionRecord(
            ref,
            "sse",
            "ip:x",
            "abcdef012345",
            protocol_version="2025-11-25",
            owner="0123456789abcdef",
            t0=1791460000123,
        )
        assert client.calls[-1][2] == [ref, 0, "", "1"]  # an SSE lease is its owner's to extend
        assert await store.acquire_session("http", ref, ttl=60) == (None, [])  # another kind
        client.replies["touch"] = None
        assert await store.acquire_session("sse", ref, ttl=None) == (None, [])
        client.replies["touch"] = {b"kind": b"http", b"cid": b"ip:y", b"fp": b"", b"t0": b"x"}
        found, _ = await store.acquire_session("http", ref, ttl=1.5)
        assert found == SessionRecord(ref, "http", "ip:y", None)
        assert client.calls[-1][2] == [ref, 1500, "", "1"]

        calls = len(client.calls)
        await store.release_session("sse", ref, ttl=None)  # nothing to change: no round trip
        await store.release_session("http", ref, ttl=60, touch=False)
        assert len(client.calls) == calls
        client.replies["touch"] = 1
        await store.release_session("http", ref, ttl=60, protocol_version="2025-06-18")
        assert client.calls[-1][2] == [ref, 60_000, "2025-06-18", "0"]

        client.replies["refresh"] = [1, 0, 1]
        refs = [ref, other, session_ref("u")]
        assert await store.refresh_sessions("http", refs, ttl=30) == {other}
        assert client.calls[-1][1][0] == store._index_key("http")
        assert client.calls[-1][2] == [30_000, *refs]
        assert await store.refresh_sessions("http", [], ttl=30) == set()

        client.replies["delete"] = flat
        deleted = await store.delete_session("sse", ref)
        assert deleted is not None and deleted.client_id == "ip:x"
        client.replies["delete"] = []
        assert await store.delete_session("sse", ref) is None

        for reply, outcome in (
            (-2, Reservation.GONE),
            (-1, Reservation.LIMIT),
            (3, Reservation.OK),
        ):
            client.replies["reserve"] = reply
            assert await store.reserve_session_call(ref, "add", 3) is outcome
            client.replies["client_reserve"] = reply
            assert await store.reserve_client_call("ip:x", "add", 3, ttl=60) is outcome
        assert client.calls[-1][1] == [store._key("c", client_ref("ip:x"))]
        assert client.calls[-1][2] == ["c:add", 3, 60_000]

        limiter = store.rate_limiter(SlidingWindowRateLimiter(5, 60))
        client.replies["rate"] = -1
        await limiter.acheck("ip:x")
        assert client.calls[-1][2][:2] == [5, 60_000]
        client.replies["rate"] = 2_500_000
        with pytest.raises(RateLimitError) as refused:
            await limiter.acheck("ip:x")
        assert refused.value.retry_after_seconds == 2.5

        assert await store.publish("payload") == 1
        assert await store.publish("payload", to="fedcba9876543210") == 1
        assert [channel for channel, _ in client.published] == [
            store._bus_channel(),
            store._worker_channel("fedcba9876543210"),
        ]
    finally:
        await store.aclose()


async def test_the_listener_hands_messages_to_every_handler() -> None:
    client = FakeClient()
    store = RedisStore.from_client(client, namespace="t")
    seen: list[str] = []

    def broken(payload: str) -> None:
        raise RuntimeError("a handler bug must not stop the others")

    store.subscribe(broken)
    unsubscribe = store.subscribe(seen.append)
    try:
        await store.start()
        await asyncio.sleep(0.01)
        assert client.channels == [store._bus_channel(), store._worker_channel(store.worker_id)]
        store._dispatch(b"one")
        unsubscribe()
        store._dispatch("two")
        assert seen == ["one"]
    finally:
        await store.aclose()


def rate_hit_port(window: collections.deque[int], now: int, limit: int, window_us: int) -> int:
    """RATE_HIT's arithmetic, step by step, in Python: retry-after in µs, or -1."""
    while window and window[0] <= now - window_us:
        window.popleft()
    if len(window) >= limit:
        return max(0, window[0] + window_us - now)
    window.append(now)
    return -1


def test_memory_and_redis_rate_limits_agree() -> None:
    clock = [0.0]
    local = SlidingWindowRateLimiter(3, 10.0, clock=lambda: clock[0])
    window: collections.deque[int] = collections.deque()
    for step in (0.0, 1.0, 2.0, 3.5, 9.99, 10.0, 10.5, 11.0, 12.0, 25.0, 25.0, 25.0, 25.0):
        clock[0] = step
        redis_retry = rate_hit_port(window, round(step * 1_000_000), 3, 10_000_000)
        try:
            local.check("c")
            local_retry = None
        except RateLimitError as exc:
            local_retry = exc.retry_after_seconds
        if redis_retry < 0:
            assert local_retry is None, step
        else:
            assert local_retry is not None and abs(local_retry - redis_retry / 1e6) < 1e-6, step


async def test_the_client_speaks_resp2_with_bounded_waits() -> None:
    # RESP2: the documented ACL grants no HELLO.  URL options still win.
    store = RedisStore("redis://127.0.0.1:1/3?socket_timeout=5", namespace="t")
    try:
        client = store._ensure()
        pool = client.connection_pool
        options = pool.connection_kwargs
        assert options["protocol"] == 2 and options["decode_responses"] is True
        assert options["socket_timeout"] == 5.0 and options["socket_connect_timeout"] == 2.0
        assert options["db"] == 3
        assert pool.max_connections == 64 and pool.timeout == 2.0
        assert store._ensure() is client  # one client per event loop
    finally:
        await store.aclose()
