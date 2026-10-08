"""Middleware that waits on a real network service: a quota, and a cancel reaching it.

Middleware is in-process asyncio, so the unit tests prove its contract; these
add the case that motivates it, a middleware awaiting a remote service.  They
run against a real Redis and are skipped unless ``EASY_MCP_LIVE_REDIS_URL`` is
set, e.g. ``redis://127.0.0.1:6379/0`` from
``docker run --rm -d -p 6379:6379 redis:7-alpine``.

Redis is spoken in RESP2 over ``asyncio.open_connection``, so there is no new
dependency.  Every key and client name starts with ``easy-mcp-mw-live``, so
nothing collides with other suites sharing the server.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx
import pytest
from conftest import headers_for, modern, notification, rpc

from easy_mcp import (
    MCPServer,
    RateLimitError,
    RequestInfo,
    RequestOutcome,
    StdioTransport,
    ToolCall,
    ToolOutcome,
)
from easy_mcp.exceptions import RATE_LIMITED
from easy_mcp.middleware import RequestNext, ToolNext

REDIS_URL = os.environ.get("EASY_MCP_LIVE_REDIS_URL")

pytestmark = pytest.mark.skipif(not REDIS_URL, reason="EASY_MCP_LIVE_REDIS_URL is not set")

LiveServer = Callable[[Any], str]

PREFIX = "easy-mcp-mw-live"
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}
BUDGET = 3


class Redis:
    """Just enough RESP2 to talk to Redis without a client library."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    @classmethod
    async def connect(cls) -> Redis:
        assert REDIS_URL is not None
        url = urlsplit(REDIS_URL)
        reader, writer = await asyncio.open_connection(
            url.hostname or "127.0.0.1", url.port or 6379
        )
        redis = cls(reader, writer)
        if url.password:
            user = [unquote(url.username)] if url.username else []
            await redis.call("AUTH", *user, unquote(url.password))
        if url.path.strip("/"):
            await redis.call("SELECT", url.path.strip("/"))
        return redis

    async def call(self, *args: Any) -> Any:
        parts = [b"*%d\r\n" % len(args)]
        for arg in args:
            data = str(arg).encode()
            parts.append(b"$%d\r\n%s\r\n" % (len(data), data))
        self._writer.write(b"".join(parts))
        await self._writer.drain()
        return await self._reply()

    async def _reply(self) -> Any:
        line = await self._reader.readline()
        kind, rest = line[:1], line[1:].rstrip(b"\r\n")
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            size = int(rest)
            return None if size < 0 else (await self._reader.readexactly(size + 2))[:-2].decode()
        if kind == b"*":
            size = int(rest)
            return None if size < 0 else [await self._reply() for _ in range(size)]
        raise RuntimeError(f"unexpected Redis reply {line!r}")

    async def close(self) -> None:
        self._writer.close()
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()


async def client_names() -> str:
    redis = await Redis.connect()
    try:
        listed: str = await redis.call("CLIENT", "LIST")
        return listed
    finally:
        await redis.close()


async def eventually(check: Callable[[], Awaitable[bool]], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not await check():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.05)
    return True


async def delete_keys(pattern: str) -> None:
    redis = await Redis.connect()
    try:
        keys = await redis.call("KEYS", pattern)
        if keys:
            await redis.call("DEL", *keys)
    finally:
        await redis.close()


# ------------------------------------------------------------------- quota


def quota_server(run: str) -> MCPServer:
    """A server whose tool middleware allows BUDGET calls per client per minute."""
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @server.tool_middleware
    async def quota(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        async with asyncio.timeout(2):  # the tool's timeout does not cover this
            redis = await Redis.connect()
            try:
                key = f"{PREFIX}:quota:{run}:{call.client_id}"
                used = await redis.call("INCR", key)
                if used == 1:
                    await redis.call("EXPIRE", key, 60)
                ttl = await redis.call("TTL", key)
            finally:
                await redis.close()
        if used > BUDGET:
            raise RateLimitError(retry_after_seconds=max(ttl, 1))
        return await call_next()

    return server


def assert_one_call_refused(responses: list[dict[str, Any]]) -> None:
    """BUDGET calls answered, and the one over budget refused with a retry hint."""
    answered = [r["result"]["content"][0]["text"] for r in responses if "result" in r]
    assert answered == ["2"] * BUDGET
    (error,) = [r["error"] for r in responses if "error" in r]
    assert error["code"] == RATE_LIMITED
    assert 0 < error["data"]["retry_after_seconds"] <= 60


async def test_live_quota_middleware_refuses_over_budget(live_server: LiveServer) -> None:
    run = uuid.uuid4().hex[:12]
    arguments = {"name": "add", "arguments": {"a": 1, "b": 1}}
    try:
        base = live_server(quota_server(f"{run}:stateless"))
        async with httpx.AsyncClient(base_url=base, timeout=10) as client:
            responses = []
            for n in range(BUDGET + 1):
                call = modern("tools/call", arguments, msg_id=n)
                posted = await client.post("/mcp", json=call, headers=headers_for(call))
                assert posted.status_code == 200
                responses.append(posted.json())
        assert "error" in responses[BUDGET]  # sent one at a time: the last is over
        assert_one_call_refused(responses)

        base = live_server(quota_server(f"{run}:session"))
        async with httpx.AsyncClient(base_url=base, timeout=10) as client:
            init = await client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
            session = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}
            responses = []
            for n in range(BUDGET + 1):
                call = rpc("tools/call", arguments, msg_id=n + 1)
                responses.append((await client.post("/mcp", json=call, headers=session)).json())
        assert "error" in responses[BUDGET]
        assert_one_call_refused(responses)

        lines = [rpc("tools/call", arguments, msg_id=n) for n in range(BUDGET + 1)]
        stdin = io.BytesIO(b"".join(json.dumps(m).encode() + b"\n" for m in lines))
        stdout = io.BytesIO()
        await StdioTransport(quota_server(f"{run}:stdio"), stdin=stdin, stdout=stdout).serve()
        # stdio serves the lines concurrently, so any one of them may be the
        # one over budget.
        assert_one_call_refused([json.loads(line) for line in stdout.getvalue().splitlines()])
    finally:
        await delete_keys(f"{PREFIX}:quota:{run}:*")


# ------------------------------------------------------------ cancellation


def blocking_server(name: str) -> tuple[MCPServer, list[bool]]:
    """A server whose request middleware blocks on Redis before every tools/call."""
    server = MCPServer(port=0, rate_limit_per_minute=None)
    ran: list[bool] = []

    @server.tool
    def touch() -> str:
        """Records that it ran."""
        ran.append(True)
        return "touched"

    @server.middleware
    async def wait_on_redis(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/call":
            redis = await Redis.connect()
            try:
                await redis.call("CLIENT", "SETNAME", name)
                await redis.call("BLPOP", f"{PREFIX}:never:{name}", 30)  # nothing arrives
            finally:
                await redis.close()  # on a cancel too: the connection goes away
        return await call_next()

    return server, ran


async def test_live_middleware_waiting_on_redis_is_abandoned_on_disconnect(
    live_server: LiveServer,
) -> None:
    name = f"{PREFIX}-{uuid.uuid4().hex[:12]}"
    server, ran = blocking_server(name)
    base = live_server(server)
    call = modern("tools/call", {"name": "touch"})

    async def connected() -> bool:
        return f"name={name} " in await client_names()

    async def gone() -> bool:
        return not await connected()

    async with httpx.AsyncClient(base_url=base, timeout=30) as client:
        post = asyncio.create_task(client.post("/mcp", json=call, headers=headers_for(call)))
        assert await eventually(connected, 5), "the middleware never reached Redis"
        post.cancel()  # the client gives up: its connection closes
        with contextlib.suppress(asyncio.CancelledError):
            await post
    began = time.monotonic()
    assert await eventually(gone, 2), "the middleware is still waiting on Redis"
    assert time.monotonic() - began < 2
    assert ran == []


async def test_live_stdio_cancel_reaches_middleware_waiting_on_redis() -> None:
    name = f"{PREFIX}-{uuid.uuid4().hex[:12]}"
    server, ran = blocking_server(name)
    read_end, write_end = os.pipe()
    stdin = os.fdopen(read_end, "rb")
    writer = os.fdopen(write_end, "wb")
    stdout = io.BytesIO()
    transport = StdioTransport(server, stdin=stdin, stdout=stdout, shutdown_timeout=0.2)

    def send(message: dict[str, Any]) -> None:
        writer.write(json.dumps(message).encode() + b"\n")
        writer.flush()

    async def connected() -> bool:
        return f"name={name} " in await client_names()

    async def gone() -> bool:
        return not await connected()

    serving = asyncio.create_task(transport.serve())
    try:
        send(rpc("tools/call", {"name": "touch"}, 7))
        assert await eventually(connected, 5), "the middleware never reached Redis"
        send(notification("notifications/cancelled", {"requestId": 7}))
        assert await eventually(gone, 2), "the middleware is still waiting on Redis"
        writer.close()
        await asyncio.wait_for(serving, 5)
        assert stdout.getvalue() == b""  # a cancelled request gets no response
        assert ran == []
    finally:
        if not writer.closed:
            writer.close()
        stdin.close()
