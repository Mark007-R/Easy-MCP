"""RedisStore: sessions, call counts and rate limits shared between workers through Redis.

Needs the ``[redis]`` extra (redis-py's asyncio client); nothing here imports
``redis`` until a store is created.

Every key of a server lives under ``easy-mcp:1:{<namespace>}:``.  The ``1``
is the layout's version, so a later incompatible layout cannot be misread,
and the braces are a Redis Cluster hash tag that keeps a namespace in one
slot.  Each multi-step change is one Lua script, so it is atomic, and every
time and TTL is Redis's own (``TIME``), so the workers' clocks never matter.
Every key a script writes gets a TTL in the same script, or is written only
if it exists; the two session indexes are the only keys without one.

Workers talk over two pub/sub channels: ``...:bus``, which every worker
reads, and ``...:w:<worker id>``, which only that worker reads.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import secrets
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any
from urllib.parse import parse_qs, urlsplit

from ..exceptions import RateLimitError, StoreUnavailableError
from ..security.ratelimit import SlidingWindowRateLimiter
from .base import (
    DEFAULT_NAMESPACE,
    AsyncRateLimiter,
    ExpiredSession,
    Reservation,
    SessionKind,
    SessionRecord,
    Store,
    check_namespace,
    client_ref,
    slug_namespace,
)

logger = logging.getLogger("easy_mcp.store")

# The layout version in every key: a later incompatible layout uses 2.
SCHEMA_VERSION = 1

# Client defaults; query-string options in the URL take precedence.
POOL_SIZE = 64
TIMEOUT_SECONDS = 2.0
HEALTH_CHECK_SECONDS = 30

# At most one "store unreachable" warning per worker this often.
WARNING_INTERVAL_SECONDS = 10.0

# How long the pub/sub listener waits between reconnects, at least and at most.
_BACKOFF_MIN = 0.1
_BACKOFF_MAX = 5.0

# The life of a session or of a client's counts when no ttl is given.
_DEFAULT_TTL = 3600.0

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

# Replies that mean "the store cannot serve this now" rather than a bug.
_UNAVAILABLE_REPLIES = ("OOM", "READONLY", "MASTERDOWN", "NOSCRIPT", "LOADING")

# KEYS: rec, idx   ARGV: ref, ttl_ms, cap, kind, cid, fp, own, pr
SESSION_CREATE = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 64)
if #expired > 0 then redis.call('ZREM', KEYS[2], unpack(expired)) end
if redis.call('ZCOUNT', KEYS[2], '(' .. now, '+inf') >= tonumber(ARGV[3]) then
  return {0, expired}
end
if redis.call('EXISTS', KEYS[1]) == 1 then return {-1, expired} end
redis.call('HSET', KEYS[1], 'kind', ARGV[4], 'cid', ARGV[5], 'fp', ARGV[6], 'own', ARGV[7],
  'pr', ARGV[8], 'ver', '', 't0', now)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
return {1, expired}
"""

# KEYS: rec, idx   ARGV: ref, ttl_ms ('0' = keep), ver ('' = keep), want_record ('1'/'0')
SESSION_TOUCH = """
if redis.call('EXISTS', KEYS[1]) == 0 then return false end
local ttl = tonumber(ARGV[2])
if ttl > 0 then
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  redis.call('PEXPIRE', KEYS[1], ttl)
  redis.call('ZADD', KEYS[2], now + ttl, ARGV[1])
end
if ARGV[3] ~= '' then redis.call('HSET', KEYS[1], 'ver', ARGV[3]) end
if ARGV[4] == '1' then return redis.call('HGETALL', KEYS[1]) end
return 1
"""

# KEYS: idx, rec1..recN   ARGV: ttl_ms, ref1..refN
SESSION_REFRESH_MANY = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ttl = tonumber(ARGV[1])
local alive = {}
for i = 2, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    redis.call('PEXPIRE', KEYS[i], ttl)
    redis.call('ZADD', KEYS[1], now + ttl, ARGV[i])
    alive[#alive + 1] = 1
  else
    alive[#alive + 1] = 0
  end
end
return alive
"""

# KEYS: rec, idx   ARGV: ref
SESSION_DELETE = """
local rec = redis.call('HGETALL', KEYS[1])
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return rec
"""

# KEYS: rec   ARGV: field ('c:<tool>'), limit
SESSION_RESERVE = """
if redis.call('EXISTS', KEYS[1]) == 0 then return -2 end
local n = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
if n >= tonumber(ARGV[2]) then return -1 end
return redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
"""

# KEYS: rec   ARGV: field.  Guarded by EXISTS: a bare HINCRBY on an expired
# session would create the key again, without a TTL.
SESSION_UNRESERVE = """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0') > 0 then
  return redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
end
return 0
"""

# KEYS: counts   ARGV: field, limit, ttl_ms
CLIENT_RESERVE = """
local n = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
if n >= tonumber(ARGV[2]) then redis.call('PEXPIRE', KEYS[1], ARGV[3]); return -1 end
local v = redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return v
"""

# KEYS: counts   ARGV: field
CLIENT_UNRESERVE = """
if tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0') > 0 then
  return redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
end
return 0
"""

# KEYS: window   ARGV: max_requests, window_ms, nonce.  The sliding window of
# SlidingWindowRateLimiter, in microseconds of Redis time: the same algorithm
# and the same retry-after.
RATE_HIT = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local win = tonumber(ARGV[2]) * 1000
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - win)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[1]) then
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  return math.max(0, tonumber(oldest[2]) + win - now)
end
redis.call('ZADD', KEYS[1], now, now .. ':' .. ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return -1
"""

_SCRIPTS = {
    "create": SESSION_CREATE,
    "touch": SESSION_TOUCH,
    "refresh": SESSION_REFRESH_MANY,
    "delete": SESSION_DELETE,
    "reserve": SESSION_RESERVE,
    "unreserve": SESSION_UNRESERVE,
    "client_reserve": CLIENT_RESERVE,
    "client_unreserve": CLIENT_UNRESERVE,
    "rate": RATE_HIT,
}


def _redis() -> Any:
    """``redis.asyncio``, or an ImportError that says how to install it."""
    try:
        import redis.asyncio as aioredis
    except ImportError as exc:
        raise ImportError(
            'RedisStore needs the redis extra: pip install "easy-mcp-kit[redis]"'
        ) from exc
    return aioredis


def _ms(seconds: float) -> int:
    return max(1, math.ceil(seconds * 1000))


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


def _hash(reply: Any) -> dict[str, str]:
    """A ``HGETALL`` reply (a flat list, or a mapping) as a dict of text."""
    if isinstance(reply, dict):
        return {_text(key): _text(value) for key, value in reply.items()}
    if not isinstance(reply, list | tuple):
        return {}
    items = [_text(item) for item in reply]
    return dict(zip(items[::2], items[1::2], strict=False))


def _record(ref: str, reply: Any) -> SessionRecord | None:
    """The session a hash reply describes; ``None`` for an empty or foreign one."""
    fields = _hash(reply)
    kind = fields.get("kind")
    if kind not in ("http", "sse") or "cid" not in fields:
        return None
    t0 = fields.get("t0", "")
    return SessionRecord(
        ref=ref,
        kind=kind,  # type: ignore[arg-type]
        client_id=fields["cid"],
        identity_fp=fields.get("fp") or None,
        protocol_version=fields.get("ver") or None,
        owner=fields.get("own") or None,
        principal=fields.get("pr") or None,
        t0=int(t0) if t0.isdigit() else None,
    )


def _reservation(reply: Any) -> Reservation:
    value = int(reply)
    if value == -2:
        return Reservation.GONE
    if value == -1:
        return Reservation.LIMIT
    return Reservation.OK


def _describe_url(url: str) -> tuple[str, str | None, bool, bool]:
    """``(url without credentials or query, host, loopback, has a password)``."""
    parts = urlsplit(url)
    scheme = parts.scheme or "redis"
    if scheme == "unix":
        password = parts.password or parse_qs(parts.query).get("password")
        return f"unix://{parts.path}", None, True, bool(password)
    host = parts.hostname or "localhost"
    try:
        port = parts.port
    except ValueError:
        port = None
    shown = f"[{host}]" if ":" in host else host
    if port is not None:
        shown = f"{shown}:{port}"
    password = parts.password or parse_qs(parts.query).get("password")
    return f"{scheme}://{shown}{parts.path}", host, host in _LOOPBACK_HOSTS, bool(password)


class _RedisRateLimiter:
    """The server's rate limit, counted in Redis for every worker together."""

    def __init__(self, store: RedisStore, local: SlidingWindowRateLimiter) -> None:
        self._store = store
        self._max = local._max
        self._window_ms = _ms(local._window)

    async def acheck(self, client_id: str) -> None:
        retry_us = await self._store._run(
            "rate",
            [self._store._key("r", client_ref(client_id))],
            [self._max, self._window_ms, secrets.token_hex(4)],
        )
        if int(retry_us) >= 0:
            raise RateLimitError(int(retry_us) / 1e6)


class RedisStore(Store):
    """Share sessions, call counts and rate limits between workers through Redis.

    Needs the ``[redis]`` extra.  The client connects lazily, on the event
    loop of the app's startup (or of the first request), so a store created
    at import time is safe to use under ``uvicorn --workers``.  Pass it to
    one server: ``MCPServer(store=RedisStore.from_env())``.

    The store holds no API key, token or raw session id: sessions are filed
    under a digest of their id, and clients under a digest of their id.  When
    Redis cannot be reached, the requests that need it are refused
    (:class:`~easy_mcp.StoreUnavailableError`, HTTP 503) rather than served
    without their limits.

    Args:
        url: A ``redis://``, ``rediss://`` (TLS) or ``unix://`` URL.  Client
            options may follow in its query string (``?socket_timeout=5``);
            the defaults are 2 s timeouts and a pool of 64 connections.
        namespace: The keys' namespace; by default the server's ``name``,
            slugified.  Servers sharing one Redis need different ones.

    Raises:
        ImportError: The ``redis`` package is not installed.
        ValueError: *namespace* is not 1 to 64 of ``[a-z0-9._-]``.
    """

    shared = True

    def __init__(self, url: str, *, namespace: str | None = None) -> None:
        _redis()
        if not isinstance(url, str) or not url:
            raise ValueError("RedisStore needs a redis:// or rediss:// URL")
        self._url: str | None = url
        self._given_client: Any = None
        self._init(namespace)

    @classmethod
    def from_env(
        cls, var: str = "EASY_MCP_REDIS_URL", *, namespace: str | None = None
    ) -> RedisStore:
        """Read the URL from the environment variable *var*, keeping the password out of code.

        Raises:
            ValueError: The variable is unset or empty.
        """
        url = os.environ.get(var)
        if not url:
            raise ValueError(f"environment variable {var} is not set or empty")
        return cls(url, namespace=namespace)

    @classmethod
    def from_client(cls, client: Any, *, namespace: str | None = None) -> RedisStore:
        """Use a ``redis.asyncio.Redis`` client you configured yourself.

        For a custom TLS context, a Sentinel master, and so on.  You own its
        lifecycle, so :meth:`aclose` leaves it open; it is used on the event
        loop that serves the app.
        """
        _redis()
        store = cls.__new__(cls)
        store._url = None
        store._given_client = client
        store._init(namespace)
        return store

    def _init(self, namespace: str | None) -> None:
        self._namespace = check_namespace(namespace) if namespace is not None else None
        self._client: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._scripts: dict[str, Any] = {}
        self._listener: asyncio.Task[None] | None = None
        self._handlers: list[Callable[[str], None]] = []
        self._worker: str | None = None
        self._worker_pid = 0
        self._warned_at = -math.inf

    # ------------------------------------------------------------ identity

    @property
    def worker_id(self) -> str:
        # A process forked after the store was made gets a name of its own.
        pid = os.getpid()
        if self._worker is None or self._worker_pid != pid:
            self._worker = secrets.token_hex(8)
            self._worker_pid = pid
        return self._worker

    @property
    def namespace(self) -> str:
        """The namespace every key of this store lives in."""
        if self._namespace is not None:
            return self._namespace
        if self._server_name is not None:
            return slug_namespace(self._server_name)
        return DEFAULT_NAMESPACE

    def _prefix(self) -> str:
        return f"easy-mcp:{SCHEMA_VERSION}:{{{self.namespace}}}"

    def _key(self, kind: str, name: str) -> str:
        return f"{self._prefix()}:{kind}:{name}"

    def _session_key(self, ref: str) -> str:
        return self._key("s", ref)

    def _index_key(self, kind: SessionKind) -> str:
        return self._key("i", kind)

    def _bus_channel(self) -> str:
        return f"{self._prefix()}:bus"

    def _worker_channel(self, worker: str) -> str:
        return self._key("w", worker)

    def describe(self) -> str:
        if self._url is None:
            return f"redis (own client) namespace={self.namespace}"
        shown, _, _, _ = _describe_url(self._url)
        return f"redis {shown} namespace={self.namespace}"

    def warnings(self) -> list[str]:
        found: list[str] = []
        if self._url is not None:
            shown, host, loopback, password = _describe_url(self._url)
            if not loopback and urlsplit(self._url).scheme == "redis":
                found.append(f"traffic to {host} is not encrypted; use rediss://")
            if not loopback and not password:
                found.append(f"{host} is reached without a password; give the store an ACL user")
        if self.namespace == DEFAULT_NAMESPACE:
            found.append(
                f"namespace {DEFAULT_NAMESPACE!r} is the default server name: other servers "
                "sharing this Redis would share its sessions and limits; set name= or namespace="
            )
        return found

    # ----------------------------------------------------------- lifecycle

    def _ensure(self) -> Any:
        """The client for the running event loop, made (and listening) on first use."""
        loop = asyncio.get_running_loop()
        if self._client is not None and self._loop is loop:
            return self._client
        if self._given_client is not None:
            client = self._given_client
        else:
            aioredis = _redis()
            assert self._url is not None
            pool = aioredis.BlockingConnectionPool.from_url(
                self._url,
                decode_responses=True,
                max_connections=POOL_SIZE,
                timeout=TIMEOUT_SECONDS,
                socket_timeout=TIMEOUT_SECONDS,
                socket_connect_timeout=TIMEOUT_SECONDS,
                health_check_interval=HEALTH_CHECK_SECONDS,
            )
            client = aioredis.Redis(connection_pool=pool)
        # A client made on another loop (an app served again) is left behind.
        self._client = client
        self._loop = loop
        self._scripts = {name: client.register_script(text) for name, text in _SCRIPTS.items()}
        self._listener = loop.create_task(self._listen(client))
        return client

    async def start(self) -> None:
        """Connect and start listening to the other workers.

        Raises:
            redis.exceptions.AuthenticationError: The credentials are refused.
            redis.exceptions.NoPermissionError: The ACL user may not ``PING``.

        A Redis that cannot be reached is logged and retried: requests that
        need it are refused meanwhile, and ``/healthz`` answers 503.
        """
        from redis import exceptions

        client = self._ensure()
        try:
            await client.ping()
        except (exceptions.AuthenticationError, exceptions.NoPermissionError):
            raise
        except (exceptions.RedisError, OSError, TimeoutError) as exc:
            logger.error(
                "store unreachable at startup (%s): requests that need it are refused "
                "until it is back",
                type(exc).__name__,
            )

    async def aclose(self) -> None:
        listener, self._listener = self._listener, None
        if listener is not None:
            listener.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await listener
        client, self._client = self._client, None
        self._loop = None
        if client is not None and self._given_client is None:
            with contextlib.suppress(Exception):
                await client.aclose(close_connection_pool=True)

    async def ping(self) -> bool:
        try:
            return bool(await self._ensure().ping())
        except Exception:
            return False

    # ------------------------------------------------------------ plumbing

    def _unavailable(self, exc: BaseException, *, configuration: bool = False) -> None:
        """Log that the store failed us, at most once every 10 s."""
        now = time.monotonic()
        if now - self._warned_at < WARNING_INTERVAL_SECONDS:
            return
        self._warned_at = now
        if configuration:
            logger.error(
                "store refused this worker (%s): check its user and ACL", type(exc).__name__
            )
        else:
            logger.warning("store unreachable: %s", type(exc).__name__)

    async def _call(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        """Run one store operation, turning every outage into StoreUnavailableError."""
        from redis import exceptions

        try:
            return await operation()
        except (exceptions.AuthenticationError, exceptions.NoPermissionError) as exc:
            self._unavailable(exc, configuration=True)
            raise StoreUnavailableError() from None
        except (exceptions.ConnectionError, exceptions.TimeoutError, TimeoutError, OSError) as exc:
            self._unavailable(exc)
            raise StoreUnavailableError() from None
        except exceptions.ResponseError as exc:
            if str(exc).startswith(_UNAVAILABLE_REPLIES):
                self._unavailable(exc)
                raise StoreUnavailableError() from None
            raise

    async def _run(self, script: str, keys: Sequence[str], args: Sequence[Any]) -> Any:
        async def run() -> Any:
            self._ensure()
            return await self._scripts[script](keys=list(keys), args=list(args))

        return await self._call(run)

    # --------------------------------------------------------- rate limits

    def rate_limiter(self, local: SlidingWindowRateLimiter) -> AsyncRateLimiter:
        return _RedisRateLimiter(self, local)

    # ------------------------------------------------------------ sessions

    async def create_session(
        self, record: SessionRecord, *, cap: int, ttl: float | None
    ) -> tuple[bool, list[ExpiredSession]]:
        ttl_ms = _ms(ttl if ttl is not None else _DEFAULT_TTL)
        reply = await self._run(
            "create",
            [self._session_key(record.ref), self._index_key(record.kind)],
            [
                record.ref,
                ttl_ms,
                cap,
                record.kind,
                record.client_id,
                record.identity_fp or "",
                record.owner or "",
                record.principal or "",
            ],
        )
        status = int(reply[0])
        # The record of an expired session is gone with its TTL: only its ref
        # is left in the index.
        expired = [ExpiredSession(_text(ref), None) for ref in (reply[1] or [])]
        if status == -1:
            raise ValueError("a session with this ref exists already")
        return status == 1, expired

    async def acquire_session(
        self, kind: SessionKind, ref: str, *, ttl: float | None
    ) -> tuple[SessionRecord | None, list[ExpiredSession]]:
        reply = await self._run(
            "touch",
            [self._session_key(ref), self._index_key(kind)],
            [ref, _ms(ttl) if ttl is not None else 0, "", "1"],
        )
        if not reply:
            return None, []
        record = _record(ref, reply)
        if record is None or record.kind != kind:
            return None, []
        return record, []

    async def release_session(
        self,
        kind: SessionKind,
        ref: str,
        *,
        ttl: float | None,
        protocol_version: str | None = None,
        touch: bool = True,
    ) -> None:
        ttl_ms = _ms(ttl) if ttl is not None and touch else 0
        if not ttl_ms and protocol_version is None:
            return  # nothing to change
        await self._run(
            "touch",
            [self._session_key(ref), self._index_key(kind)],
            [ref, ttl_ms, protocol_version or "", "0"],
        )

    async def refresh_sessions(
        self, kind: SessionKind, refs: Sequence[str], *, ttl: float
    ) -> set[str]:
        if not refs:
            return set()
        reply = await self._run(
            "refresh",
            [self._index_key(kind), *(self._session_key(ref) for ref in refs)],
            [_ms(ttl), *refs],
        )
        return {ref for ref, alive in zip(refs, reply, strict=False) if not int(alive)}

    async def delete_session(self, kind: SessionKind, ref: str) -> SessionRecord | None:
        reply = await self._run("delete", [self._session_key(ref), self._index_key(kind)], [ref])
        return _record(ref, reply)

    # ----------------------------------------------- max_calls_per_session

    async def reserve_session_call(self, ref: str, tool: str, limit: int) -> Reservation:
        reply = await self._run("reserve", [self._session_key(ref)], [f"c:{tool}", limit])
        return _reservation(reply)

    async def release_session_call(self, ref: str, tool: str) -> None:
        await self._run("unreserve", [self._session_key(ref)], [f"c:{tool}"])

    async def touch_client(self, client_id: str, *, ttl: float | None) -> None:
        # Counted and refused calls refresh a client's counts; refreshing on
        # every request would cost every stateless request a round trip.
        return None

    async def reserve_client_call(
        self, client_id: str, tool: str, limit: int, *, ttl: float | None
    ) -> Reservation:
        reply = await self._run(
            "client_reserve",
            [self._key("c", client_ref(client_id))],
            [f"c:{tool}", limit, _ms(ttl if ttl is not None else _DEFAULT_TTL)],
        )
        return _reservation(reply)

    async def release_client_call(self, client_id: str, tool: str) -> None:
        await self._run("client_unreserve", [self._key("c", client_ref(client_id))], [f"c:{tool}"])

    # ------------------------------------------------ messages between workers

    async def publish(self, payload: str, *, to: str | None = None) -> int:
        channel = self._bus_channel() if to is None else self._worker_channel(to)

        async def send() -> Any:
            return await self._ensure().publish(channel, payload)

        return int(await self._call(send))

    def subscribe(self, handler: Callable[[str], None]) -> Callable[[], None]:
        self._handlers.append(handler)

        def unsubscribe() -> None:
            with contextlib.suppress(ValueError):
                self._handlers.remove(handler)

        return unsubscribe

    def _dispatch(self, payload: Any) -> None:
        text = _text(payload)
        for handler in list(self._handlers):
            try:
                handler(text)
            except Exception:
                logger.error("a store message handler failed", exc_info=True)

    async def _listen(self, client: Any) -> None:
        """Read both channels until cancelled, reconnecting with a backoff."""
        backoff = _BACKOFF_MIN
        down = False
        while True:
            pubsub = client.pubsub()
            try:
                await pubsub.subscribe(self._bus_channel(), self._worker_channel(self.worker_id))
                if down:
                    logger.warning("store pub/sub reconnected")
                    down = False
                backoff = _BACKOFF_MIN
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message is not None and message.get("type") == "message":
                        self._dispatch(message.get("data"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not down:
                    down = True
                    logger.warning(
                        "store pub/sub disconnected (%s): cancels, session ends and relayed "
                        "answers between workers are lost until it reconnects",
                        type(exc).__name__,
                    )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()
