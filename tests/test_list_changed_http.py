"""List-change notifications over HTTP: the session's GET /mcp stream, stateless
subscriptions/listen streams, the legacy /sse stream, and two workers sharing a store.

Every test serves a real uvicorn, and reads its streams with httpx in the
background.  Tools are registered from the test's own thread, as an
application's other threads would.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest
from conftest import LogCapture, headers_for, listen, modern, notification, rpc
from oauth_fake_as import FakeAuthorizationServer
from shared_store_fake import FakeHub, records

from easy_mcp import APIKeyAuth, MCPServer, OAuthResourceServer, StreamableHTTPTransport
from easy_mcp.exceptions import (
    AUTHENTICATION_REQUIRED,
    FORBIDDEN,
    HEADER_MISMATCH,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    RATE_LIMITED,
    TOO_MANY_SESSIONS,
)
from easy_mcp.security.oauth import LEEWAY_SECONDS

LiveServer = Callable[[Any], str]

# Built, not written out, so secret scanners do not take the fixtures for credentials.
KEY = "lc-http-key-" + "k" * 20
OTHER_KEY = "lc-http-other-" + "k" * 18
TAG = "io.modelcontextprotocol/subscriptionId"
ACCEPT = {"Accept": "application/json, text/event-stream"}
STREAM = {"Accept": "text/event-stream"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "t", "version": "1"},
}
TOOLS_CHANGED = {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}
OAUTH_RESOURCE = "https://mcp.example.com/mcp"


def make_server(**kwargs: Any) -> MCPServer:
    options: dict[str, Any] = {"rate_limit_per_minute": None}
    options.update(kwargs)
    server = MCPServer(port=0, **options)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def register(server: MCPServer, name: str = "extra", **options: Any) -> None:
    def tool() -> str:
        """A tool registered at runtime."""
        return name

    server.register_tool(tool, name=name, **options)


def bearer(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


class Stream:
    """An SSE response, read in the background into a queue of its events."""

    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        self.events: asyncio.Queue[Any] = asyncio.Queue()
        self.comments = 0
        self.ended = asyncio.Event()
        self._task = asyncio.create_task(self._read())

    async def _read(self) -> None:
        data: str | None = None
        try:
            async for line in self.response.aiter_lines():
                if line.startswith(":"):
                    self.comments += 1
                elif line.startswith("data: "):
                    data = line[len("data: ") :]
                elif not line and data is not None:
                    try:
                        self.events.put_nowait(json.loads(data))
                    except ValueError:
                        self.events.put_nowait(data)  # the legacy endpoint event
                    data = None
        except httpx.HTTPError:
            pass
        finally:
            self.ended.set()

    async def next(self, timeout: float = 5.0) -> Any:
        return await asyncio.wait_for(self.events.get(), timeout)

    async def quiet(self, seconds: float = 0.3) -> None:
        """Nothing arrives for *seconds*."""
        await asyncio.sleep(seconds)
        assert self.events.empty(), self.events.get_nowait()

    async def end(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self.ended.wait(), timeout)

    async def aclose(self) -> None:
        await self.response.aclose()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


async def open_stream(
    client: httpx.AsyncClient, method: str, url: str, **kwargs: Any
) -> Stream | httpx.Response:
    """A stream when the answer is ``text/event-stream``; otherwise the read response."""
    response = await client.send(client.build_request(method, url, **kwargs), stream=True)
    if response.headers.get("content-type", "").startswith("text/event-stream"):
        return Stream(response)
    await response.aread()
    await response.aclose()
    return response


async def initialize(client: httpx.AsyncClient, headers: dict[str, str] | None = None) -> str:
    """Run the handshake; returns the session id."""
    extra = headers or {}
    init = await client.post("/mcp", json=rpc("initialize", INIT), headers={**ACCEPT, **extra})
    assert init.status_code == 200, init.text
    session = init.headers["mcp-session-id"]
    done = await client.post(
        "/mcp",
        json=notification("notifications/initialized"),
        headers={**ACCEPT, "MCP-Session-Id": session, **extra},
    )
    assert done.status_code == 202
    return session


async def get_stream(
    client: httpx.AsyncClient, session: str, headers: dict[str, str] | None = None
) -> Stream:
    opened = await open_stream(
        client, "GET", "/mcp", headers={**STREAM, "MCP-Session-Id": session, **(headers or {})}
    )
    assert isinstance(opened, Stream), (opened.status_code, opened.text)
    return opened


async def listen_stream(
    client: httpx.AsyncClient, msg_id: Any = "listen-1", headers: dict[str, str] | None = None
) -> Stream:
    message = listen(msg_id, toolsListChanged=True)
    opened = await open_stream(
        client, "POST", "/mcp", json=message, headers={**headers_for(message), **(headers or {})}
    )
    assert isinstance(opened, Stream), (opened.status_code, opened.text)
    return opened


async def until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition never held"
        await asyncio.sleep(0.01)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@contextlib.contextmanager
def running(server: MCPServer) -> Iterator[tuple[str, threading.Thread]]:
    """Serve *server* with ``server.run()`` in a thread, as an application does."""
    server.port = free_port()
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.port}"
    deadline = time.time() + 10
    while True:
        try:
            if httpx.get(f"{base}/healthz", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        assert time.time() < deadline, "server did not start"
        time.sleep(0.05)
    try:
        yield base, thread
    finally:
        server.stop()
        thread.join(10)


# ------------------------------------------------------------------ GET /mcp


def test_get_without_session_is_still_405(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        response = client.get("/mcp", headers=STREAM)
        assert response.status_code == 405
        assert response.headers["allow"] == "GET, POST, DELETE"
        assert response.json()["error"]["code"] == INVALID_REQUEST


def test_get_with_stateless_version_header_is_405(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        response = client.get(
            "/mcp",
            headers={**STREAM, "MCP-Session-Id": "any", "MCP-Protocol-Version": "2026-07-28"},
        )
        assert response.status_code == 405
        assert "GET" in response.headers["allow"]


def test_head_is_405(live_server: LiveServer) -> None:
    base = live_server(make_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        response = client.head("/mcp", headers=STREAM)
        assert response.status_code == 405
        assert response.headers["allow"] == "GET, POST, DELETE"


async def test_head_with_a_session_opens_no_stream(
    live_server: LiveServer, fast_debounce: float, logs: LogCapture
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        stream = await get_stream(client, session)
        # One connection, kept alive: what follows the HEAD is served on it.
        limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
        async with httpx.AsyncClient(base_url=base, timeout=3, limits=limits) as other:
            head = await other.head("/mcp", headers={**STREAM, "MCP-Session-Id": session})
            assert head.status_code == 405
            assert head.headers["allow"] == "GET, POST, DELETE"
            ping = await other.post(
                "/mcp", json=rpc("ping", msg_id=2), headers={**ACCEPT, "MCP-Session-Id": session}
            )
            assert ping.status_code == 200
        # The session's stream is still the one its client opened.
        register(server)
        assert await stream.next() == TOOLS_CHANGED
        await stream.aclose()
    await until(lambda: bool(logs.events("stream_close")))
    assert [event["reason"] for event in logs.events("stream_close")] == ["client_closed"]


async def test_get_needs_event_stream_accept(live_server: LiveServer) -> None:
    base = live_server(make_server())
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        refused = await client.get(
            "/mcp", headers={"Accept": "application/json", "MCP-Session-Id": session}
        )
        assert refused.status_code == 406


async def test_get_stream_delivers_list_changed(
    live_server: LiveServer, fast_debounce: float, logs: LogCapture
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        stream = await get_stream(client, session, {"MCP-Protocol-Version": "2025-11-25"})
        assert stream.response.status_code == 200
        assert stream.response.headers["cache-control"] == "no-cache"
        assert stream.response.headers["x-accel-buffering"] == "no"
        await stream.quiet(0.1)  # nothing changed since the handshake
        register(server)
        assert await stream.next() == TOOLS_CHANGED
        await stream.quiet()
        await stream.aclose()
    await until(lambda: bool(logs.events("stream_close")))
    opened, closed = logs.events("stream_open")[0], logs.events("stream_close")[0]
    assert opened["session_id"] == session and opened["transport"] == "streamable-http"
    assert closed["reason"] == "client_closed"


async def test_get_unknown_session_404_wrong_credential_403(live_server: LiveServer) -> None:
    base = live_server(make_server(auth=APIKeyAuth({KEY: "*", OTHER_KEY: "*"})))
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client, bearer(KEY))
        unknown = await client.get("/mcp", headers={**STREAM, **bearer(KEY), "MCP-Session-Id": "x"})
        assert unknown.status_code == 404
        other = await client.get(
            "/mcp", headers={**STREAM, **bearer(OTHER_KEY), "MCP-Session-Id": session}
        )
        assert other.status_code == 403 and other.json()["error"]["code"] == FORBIDDEN
        invalid = await client.get(
            "/mcp", headers={**STREAM, **bearer("not-a-key-" + "x" * 8), "MCP-Session-Id": session}
        )
        assert invalid.status_code == 401
        assert invalid.json()["error"]["code"] == AUTHENTICATION_REQUIRED
        bad_version = await client.get(
            "/mcp",
            headers={
                **STREAM,
                **bearer(KEY),
                "MCP-Session-Id": session,
                "MCP-Protocol-Version": "1999-01-01",
            },
        )
        assert bad_version.status_code == 400


async def test_pending_change_is_delivered_when_get_opens(
    live_server: LiveServer, fast_debounce: float
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        register(server, "one")
        register(server, "two")
        await asyncio.sleep(0.1)
        stream = await get_stream(client, session)
        assert await stream.next() == TOOLS_CHANGED
        await stream.quiet()
        await stream.aclose()


async def test_second_get_replaces_the_first(
    live_server: LiveServer, fast_debounce: float, logs: LogCapture
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        first = await get_stream(client, session)
        second = await get_stream(client, session)
        await first.end()
        register(server)
        assert await second.next() == TOOLS_CHANGED
        assert first.events.empty()
        await first.aclose()
        await second.aclose()
    await until(lambda: len(logs.events("stream_close")) == 2)
    assert [event["reason"] for event in logs.events("stream_close")] == [
        "replaced",
        "client_closed",
    ]


async def test_open_get_stream_keeps_the_session_alive(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(StreamableHTTPTransport(server, session_idle_timeout=0.2).build_app())
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        stream = await get_stream(client, session)
        await asyncio.sleep(0.5)
        ping = rpc("ping", msg_id=2)
        alive = await client.post("/mcp", json=ping, headers={**ACCEPT, "MCP-Session-Id": session})
        assert alive.status_code == 200
        await stream.aclose()
        await asyncio.sleep(0.6)
        gone = await client.post("/mcp", json=ping, headers={**ACCEPT, "MCP-Session-Id": session})
        assert gone.status_code == 404


async def test_delete_ends_the_get_stream(live_server: LiveServer, logs: LogCapture) -> None:
    base = live_server(make_server())
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        stream = await get_stream(client, session)
        deleted = await client.delete("/mcp", headers={"MCP-Session-Id": session})
        assert deleted.status_code == 204
        await stream.end()
        await stream.aclose()
    await until(lambda: bool(logs.events("stream_close")))
    assert logs.events("stream_close")[0]["reason"] == "session_closed"


async def test_get_is_rate_limited(live_server: LiveServer, logs: LogCapture) -> None:
    base = live_server(make_server(rate_limit_per_minute=3))
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)  # two of the three units
        stream = await get_stream(client, session)
        refused = await client.get("/mcp", headers={**STREAM, "MCP-Session-Id": session})
        assert refused.status_code == 429
        assert int(refused.headers["retry-after"]) >= 1
        assert refused.json()["error"]["code"] == RATE_LIMITED
        await stream.aclose()
    assert [event["method"] for event in logs.events("rate_limited")] == ["GET /mcp"]


async def test_a_change_a_closing_stream_could_not_send_is_told_on_the_next(
    live_server: LiveServer, logs: LogCapture
) -> None:
    server = make_server()
    transport = StreamableHTTPTransport(server)
    base = live_server(transport.build_app())
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        first = await get_stream(client, session)
        local = transport._sessions[session]
        notify = local.notify_stream
        sink = notify.sink
        done = threading.Event()

        def expire_with_a_change_due() -> None:
            # The token's timer closes the stream, and a change's flush runs
            # before the stream's end is handled.
            notify.close("token_expired")
            register(server)
            sink.deliver(["tools"])
            done.set()

        sink.loop.call_soon_threadsafe(expire_with_a_change_due)
        assert await asyncio.to_thread(done.wait, 5)
        await first.end()
        await first.aclose()
        # Its end recorded what it told, then let the session go.
        await until(lambda: bool(logs.events("stream_close")) and local.active == 0)
        second = await get_stream(client, session)
        assert await second.next() == TOOLS_CHANGED
        await second.aclose()


async def test_http_handshake_with_keys_of_mixed_types(
    live_server: LiveServer, fast_debounce: float
) -> None:
    server = make_server(max_sessions=1)

    # JSON keys are strings: a client reads this example's 0 as "0".
    @server.tool(examples=[{"arguments": {"weights": {0: 0.5, "default": 1.0}}}])
    def weigh(weights: dict[str, float]) -> float:
        """Weigh things."""
        return sum(weights.values())

    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        stream = await get_stream(client, session)
        register(server)
        assert await stream.next() == TOOLS_CHANGED
        await stream.aclose()


async def test_a_list_that_cannot_be_digested_holds_no_session_slot(
    live_server: LiveServer, monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    def undigestible(self: MCPServer, kind: str, identity: Any) -> str:
        raise TypeError("cannot digest this list")

    # Before the server exists, so its notifier digests with it too.
    monkeypatch.setattr(MCPServer, "_list_digest", undigestible)
    server = make_server(max_sessions=1)
    transport = StreamableHTTPTransport(server)
    base = live_server(transport.build_app())
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        for _ in range(2):  # the one slot is free again each time
            session = await initialize(client)
            assert transport._sessions[session].active == 0
            # Its stream opens, though no change can be told on it, and a
            # new one still replaces it.
            first = await get_stream(client, session)
            second = await get_stream(client, session)
            await first.end()
            await first.aclose()
            await second.aclose()
            deleted = await client.delete("/mcp", headers={"MCP-Session-Id": session})
            assert deleted.status_code == 204
    assert "could not compute the tools list" in logs.text


async def test_post_responses_stay_json(live_server: LiveServer, fast_debounce: float) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client)
        register(server)
        await asyncio.sleep(0.1)  # a change is waiting for a stream
        listed = await client.post(
            "/mcp", json=rpc("tools/list", msg_id=2), headers={**ACCEPT, "MCP-Session-Id": session}
        )
        assert listed.headers["content-type"] == "application/json"
        body = listed.json()
        assert set(body) == {"jsonrpc", "id", "result"}
        assert [tool["name"] for tool in body["result"]["tools"]] == ["add", "extra"]


async def test_get_stream_ends_when_its_token_expires(
    live_server: LiveServer, fake_as: FakeAuthorizationServer, logs: LogCapture
) -> None:
    oauth = OAuthResourceServer(OAUTH_RESOURCE, [fake_as.issuer])
    base = live_server(make_server(oauth=oauth))
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        session = await initialize(client, bearer(fake_as.mint()))
        # Still accepted (within the clock-skew leeway) for about 2 more seconds.
        short = fake_as.mint(claims={"exp": int(time.time()) - LEEWAY_SECONDS + 2})
        stream = await get_stream(client, session, bearer(short))
        await stream.end(10)
        await stream.aclose()
    await until(lambda: bool(logs.events("stream_close")))
    assert logs.events("stream_close")[0]["reason"] == "token_expired"


# ------------------------------------------------------- subscriptions/listen


async def test_listen_streams_ack_then_notifications(
    live_server: LiveServer, fast_debounce: float
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        stream = await listen_stream(client, "listen-1")
        assert stream.response.status_code == 200
        assert stream.response.headers["x-accel-buffering"] == "no"
        assert await stream.next() == {
            "jsonrpc": "2.0",
            "method": "notifications/subscriptions/acknowledged",
            "params": {"_meta": {TAG: "listen-1"}, "notifications": {"toolsListChanged": True}},
        }
        register(server)
        assert await stream.next() == {
            "jsonrpc": "2.0",
            "method": "notifications/tools/list_changed",
            "params": {"_meta": {TAG: "listen-1"}},
        }
        await stream.aclose()


async def test_listen_headers_must_match_the_body(live_server: LiveServer) -> None:
    base = live_server(make_server())
    message = listen(1, toolsListChanged=True)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        headers = headers_for(message)
        del headers["Mcp-Method"]
        missing = await client.post("/mcp", json=message, headers=headers)
        assert missing.status_code == 400
        assert missing.json()["error"]["code"] == HEADER_MISMATCH
        wrong = await client.post(
            "/mcp", json=message, headers=headers_for(message, **{"Mcp-Method": "tools/list"})
        )
        assert wrong.status_code == 400
        assert wrong.json()["error"]["code"] == HEADER_MISMATCH


async def test_listen_needs_event_stream_accept(live_server: LiveServer) -> None:
    base = live_server(make_server())
    message = listen(3, toolsListChanged=True)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        refused = await client.post(
            "/mcp", json=message, headers=headers_for(message, Accept="application/json")
        )
        assert refused.status_code == 406
        assert refused.json()["id"] == 3
        assert refused.json()["error"]["code"] == INVALID_REQUEST


async def test_listen_refusals_are_json(live_server: LiveServer) -> None:
    server = make_server(max_sessions=1)
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        bad = modern("subscriptions/listen", {"notifications": {"toolsListChanged": "yes"}}, 4)
        refused = await client.post("/mcp", json=bad, headers=headers_for(bad))
        assert refused.headers["content-type"] == "application/json"
        assert refused.status_code == 200
        assert refused.json()["error"]["code"] == INVALID_PARAMS
        stream = await listen_stream(client, "first")
        second = listen("second", toolsListChanged=True)
        capped = await client.post("/mcp", json=second, headers=headers_for(second))
        assert capped.status_code == 503
        assert capped.json()["error"]["code"] == TOO_MANY_SESSIONS
        await stream.aclose()


async def test_listen_disconnect_ends_the_subscription(
    live_server: LiveServer, logs: LogCapture
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        stream = await listen_stream(client, "listen-1")
        await stream.next()
        assert server._notifier.count() == 1
        await stream.aclose()
        await until(lambda: server._notifier.count() == 0, timeout=1.0)
    await until(lambda: bool(logs.events("subscription_close")))
    assert logs.events("subscription_close")[0]["reason"] == "disconnected"
    assert logs.events("request_cancelled") == []


async def test_listen_ignores_session_header_and_cancel_notifications(
    live_server: LiveServer,
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        stream = await listen_stream(client, "listen-1", {"MCP-Session-Id": "ignored"})
        await stream.next()
        cancel = notification("notifications/cancelled", {"requestId": "listen-1"})
        headers = {**ACCEPT, "MCP-Protocol-Version": "2026-07-28"}
        answered = await client.post("/mcp", json=cancel, headers=headers)
        assert answered.status_code == 202
        await asyncio.sleep(0.2)
        assert server._notifier.count() == 1
        assert not stream.ended.is_set()
        await stream.aclose()


async def test_anonymous_listener_is_not_told_about_a_protected_tool(
    live_server: LiveServer, fast_debounce: float
) -> None:
    server = make_server(auth=APIKeyAuth({KEY: "*"}))
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        anonymous = await listen_stream(client, "anonymous")
        keyed = await listen_stream(client, "keyed", bearer(KEY))
        await anonymous.next()
        await keyed.next()
        register(server, requires_auth=True)
        assert (await keyed.next())["method"] == "notifications/tools/list_changed"
        await anonymous.quiet()
        await anonymous.aclose()
        await keyed.aclose()


async def test_shutdown_closes_get_and_listen_streams(fast_debounce: float) -> None:
    server = make_server()
    with running(server) as (base, thread):
        async with httpx.AsyncClient(base_url=base, timeout=10) as client:
            session = await initialize(client)
            get = await get_stream(client, session)
            listening = await listen_stream(client, 9)
            await listening.next()
            started = time.monotonic()
            await asyncio.to_thread(server.stop)
            final = await listening.next()
            assert final["id"] == 9
            assert final["result"]["resultType"] == "complete"
            assert final["result"]["_meta"][TAG] == 9
            await listening.end()
            await get.end()
            await asyncio.to_thread(thread.join, 10)
            assert not thread.is_alive()
            assert time.monotonic() - started < 5
            assert listening.events.empty()  # no cancel after a final response over HTTP
            await get.aclose()
            await listening.aclose()


# ---------------------------------------------------------------- legacy /sse


async def test_sse_session_receives_list_changed_after_initialize(
    live_server: LiveServer, fast_debounce: float
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        opened = await open_stream(client, "GET", "/sse")
        assert isinstance(opened, Stream)
        endpoint = await opened.next()
        assert isinstance(endpoint, str) and "/messages?session_id=" in endpoint
        register(server, "early")  # before initialize: nobody is told
        posted = await client.post(endpoint, json=rpc("initialize", INIT))
        assert posted.status_code == 202
        answer = await opened.next()
        assert answer["result"]["capabilities"] == {"tools": {"listChanged": True}}
        await opened.quiet(0.1)
        register(server)
        assert await opened.next() == TOOLS_CHANGED
        await opened.aclose()


async def test_sse_change_notifications_coalesce_until_written(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        opened = await open_stream(client, "GET", "/sse")
        assert isinstance(opened, Stream)
        endpoint = await opened.next()
        await client.post(endpoint, json=rpc("initialize", INIT))
        await opened.next()
        # Three subscriptions on the session's stream: 1 and 1.0 are two.
        for msg_id in ("a", 1, 1.0):
            await client.post(endpoint, json=listen(msg_id, toolsListChanged=True))
            await opened.next()
        notifier = server._notifier
        sinks = [*notifier._sessions.values()]
        sinks += [sink for streams in notifier._channels.values() for sink in streams.values()]
        assert len(sinks) == 4
        done = threading.Event()

        def two_changes_before_the_stream_writes() -> None:
            for name in ("one", "two"):
                register(server, name)
                for sink in sinks:
                    sink.deliver(["tools"])
            done.set()

        sinks[0].loop.call_soon_threadsafe(two_changes_before_the_stream_writes)
        assert await asyncio.to_thread(done.wait, 5)
        frames = [await opened.next() for _ in range(4)]
        await opened.quiet()
        assert {frame["method"] for frame in frames} == {"notifications/tools/list_changed"}
        tags = [frame.get("params", {}).get("_meta", {}).get(TAG) for frame in frames]
        assert [(type(tag), tag) for tag in tags] == [
            (type(None), None),
            (str, "a"),
            (int, 1),
            (float, 1.0),
        ]
        await opened.aclose()


async def test_sse_shutdown_sends_listen_result_before_closing() -> None:
    server = make_server()
    with running(server) as (base, thread):
        async with httpx.AsyncClient(base_url=base, timeout=10) as client:
            opened = await open_stream(client, "GET", "/sse")
            assert isinstance(opened, Stream)
            endpoint = await opened.next()
            posted = await client.post(endpoint, json=listen("l", toolsListChanged=True))
            assert posted.status_code == 202
            ack = await opened.next()
            assert ack["method"] == "notifications/subscriptions/acknowledged"
            await asyncio.to_thread(server.stop)
            result = await opened.next()
            cancelled = await opened.next()
            assert result["id"] == "l" and result["result"]["resultType"] == "complete"
            assert cancelled["method"] == "notifications/cancelled"
            assert cancelled["params"]["requestId"] == "l"
            assert cancelled["params"]["_meta"] == {TAG: "l"}
            await opened.end()
            await asyncio.to_thread(thread.join, 10)
            await opened.aclose()


# --------------------------------------------------- two workers, one store


@pytest.fixture
def pair(live_server: LiveServer) -> Callable[[], tuple[FakeHub, Any, Any]]:
    def start() -> tuple[FakeHub, Any, Any]:
        hub = FakeHub()
        workers = []
        for name in ("a" * 16, "b" * 16):
            server = make_server(store=hub.store(name))
            workers.append((server, live_server(server)))
        return hub, workers[0], workers[1]

    return start


async def test_get_on_another_worker_announces_changes_since_initialize(
    pair: Callable[[], tuple[FakeHub, Any, Any]], fast_debounce: float
) -> None:
    hub, (server_a, base_a), (server_b, base_b) = pair()
    async with httpx.AsyncClient(timeout=10) as client:
        client.base_url = httpx.URL(base_a)
        session = await initialize(client)
        assert [record.baselines for record in records(hub)][0] is not None
        # The change runs in every worker, as the docs ask.
        register(server_a)
        register(server_b)
        client.base_url = httpx.URL(base_b)
        stream = await get_stream(client, session)
        assert await stream.next() == TOOLS_CHANGED
        await stream.quiet(0.1)
        await stream.aclose()
        # Its end recorded what it told: a stream opened elsewhere starts there.
        await until(lambda: records(hub)[0].baselines == (("tools", told(server_b)),))
        client.base_url = httpx.URL(base_a)
        again = await get_stream(client, session)
        await again.quiet()
        await again.aclose()


def told(server: MCPServer) -> str:
    return server._list_digest("tools", None)


async def test_sse_initialize_relayed_from_another_worker_starts_notifications(
    pair: Callable[[], tuple[FakeHub, Any, Any]], fast_debounce: float
) -> None:
    _, (server_a, base_a), (_, base_b) = pair()
    async with httpx.AsyncClient(timeout=10) as client:
        opened = await open_stream(client, "GET", f"{base_a}/sse")
        assert isinstance(opened, Stream)
        endpoint = await opened.next()
        posted = await client.post(f"{base_b}{endpoint}", json=rpc("initialize", INIT))
        assert posted.status_code == 202
        answer = await opened.next()
        assert answer["result"]["protocolVersion"] == "2025-11-25"
        register(server_a)
        assert await opened.next() == TOOLS_CHANGED
        await opened.aclose()


async def test_sse_listen_is_served_by_the_worker_holding_the_stream_only(
    pair: Callable[[], tuple[FakeHub, Any, Any]],
) -> None:
    _, (server_a, base_a), (_, base_b) = pair()
    async with httpx.AsyncClient(timeout=10) as client:
        opened = await open_stream(client, "GET", f"{base_a}/sse")
        assert isinstance(opened, Stream)
        endpoint = await opened.next()
        elsewhere = await client.post(
            f"{base_b}{endpoint}", json=listen("b", toolsListChanged=True)
        )
        assert elsewhere.status_code == 202
        refused = await opened.next()
        assert refused["id"] == "b" and refused["error"]["code"] == METHOD_NOT_FOUND
        here = await client.post(f"{base_a}{endpoint}", json=listen("a", toolsListChanged=True))
        assert here.status_code == 202
        ack = await opened.next()
        assert ack["params"]["_meta"] == {TAG: "a"}
        assert server_a._notifier.count() == 1
        await opened.aclose()
        await until(lambda: server_a._notifier.count() == 0)
