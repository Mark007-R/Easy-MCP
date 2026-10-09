"""Resource updates over HTTP: a session's GET /mcp stream, stateless listen streams,
the legacy /sse stream, and two workers sharing a store (``resub`` between them).

Every test serves a real uvicorn and reads its streams with httpx in the
background.  Updates are published from the test's own thread, as an
application's other threads would.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import time
from collections.abc import Callable, Collection
from typing import Any

import httpx
import pytest
from conftest import headers_for, listen, notification, rpc
from shared_store_fake import FakeHub, FakeSharedStore, records

import easy_mcp.server
from easy_mcp import MCPServer
from easy_mcp.exceptions import TOO_MANY_SESSIONS
from easy_mcp.store.base import SessionKind

LiveServer = Callable[[Any], str]

TAG = "io.modelcontextprotocol/subscriptionId"
ACCEPT = {"Accept": "application/json, text/event-stream"}
STREAM = {"Accept": "text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


def updated(uri: str, tag: Any = None) -> dict[str, Any]:
    params: dict[str, Any] = {"uri": uri} if tag is None else {"_meta": {TAG: tag}, "uri": uri}
    return {"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": params}


def make_server(**kwargs: Any) -> MCPServer:
    kwargs.setdefault("rate_limit_per_minute", None)
    server = MCPServer(port=0, **kwargs)
    server.register_resource(lambda: "app", "config://app", name="config")
    server.register_resource(lambda: "other", "config://other", name="other")
    return server


class Stream:
    """An SSE response, read in the background into a queue of its events."""

    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        self.events: asyncio.Queue[Any] = asyncio.Queue()
        self.ended = asyncio.Event()
        self._task = asyncio.create_task(self._read())

    async def _read(self) -> None:
        data: str | None = None
        try:
            async for line in self.response.aiter_lines():
                if line.startswith("data: "):
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

    async def next(self, timeout: float = 10.0) -> Any:
        return await asyncio.wait_for(self.events.get(), timeout)

    async def quiet(self, seconds: float = 0.5) -> None:
        await asyncio.sleep(seconds)
        assert self.events.empty(), self.events.get_nowait()

    async def end(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self.ended.wait(), timeout)

    async def aclose(self) -> None:
        await self.response.aclose()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


async def open_stream(
    client: httpx.AsyncClient, method: str, url: str, **kwargs: Any
) -> Stream | httpx.Response:
    response = await client.send(client.build_request(method, url, **kwargs), stream=True)
    if response.headers.get("content-type", "").startswith("text/event-stream"):
        return Stream(response)
    await response.aread()
    await response.aclose()
    return response


async def initialize(client: httpx.AsyncClient, base: str) -> str:
    init = await client.post(f"{base}/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
    assert init.status_code == 200, init.text
    session = init.headers["mcp-session-id"]
    done = await client.post(
        f"{base}/mcp",
        json=notification("notifications/initialized"),
        headers={**ACCEPT, "MCP-Session-Id": session},
    )
    assert done.status_code == 202
    return session


async def post(
    client: httpx.AsyncClient, base: str, session: str, method: str, uri: str, msg_id: Any = 5
) -> httpx.Response:
    return await client.post(
        f"{base}/mcp",
        json=rpc(method, {"uri": uri}, msg_id),
        headers={**ACCEPT, "MCP-Session-Id": session},
    )


async def get_stream(client: httpx.AsyncClient, base: str, session: str) -> Stream:
    opened = await open_stream(
        client, "GET", f"{base}/mcp", headers={**STREAM, "MCP-Session-Id": session}
    )
    assert isinstance(opened, Stream), (opened.status_code, opened.text)
    return opened


async def until(condition: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition never held"
        await asyncio.sleep(0.01)


# -------------------------------------------------------------- GET /mcp


async def test_get_stream_delivers_updates_queued_before_and_after_it_opens(
    live_server: LiveServer,
) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(timeout=10) as client:
        session = await initialize(client, base)
        subscribed = await post(client, base, session, "resources/subscribe", "config://app")
        assert subscribed.status_code == 200 and subscribed.json()["result"] == {}
        # No stream is open: the update waits for one (twice is told once).
        assert await asyncio.to_thread(server.notify_resource_updated, "config://app") == 1
        assert await asyncio.to_thread(server.notify_resource_updated, "config://app") == 1
        stream = await get_stream(client, base, session)
        assert await stream.next() == updated("config://app")
        await stream.quiet()
        await asyncio.to_thread(server.notify_resource_updated, "config://app")
        assert await stream.next() == updated("config://app")
        await asyncio.to_thread(server.notify_resource_updated, "config://other")
        await stream.quiet()
        await stream.aclose()


async def test_second_get_replaces_the_first(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(timeout=10) as client:
        session = await initialize(client, base)
        await post(client, base, session, "resources/subscribe", "config://app")
        first = await get_stream(client, base, session)
        second = await get_stream(client, base, session)
        await first.end()
        await asyncio.to_thread(server.notify_resource_updated, "config://app")
        assert await second.next() == updated("config://app")
        await second.aclose()
        await first.aclose()


async def test_delete_ends_the_get_stream_and_the_subscriptions(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(timeout=10) as client:
        session = await initialize(client, base)
        await post(client, base, session, "resources/subscribe", "config://app")
        stream = await get_stream(client, base, session)
        deleted = await client.delete(f"{base}/mcp", headers={"MCP-Session-Id": session})
        assert deleted.status_code == 204
        await stream.end()
        await stream.aclose()
    assert server._notifier.subscriptions(session) == frozenset()
    assert server.notify_resource_updated("config://app") == 0


async def test_unsubscribe_over_http(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(timeout=10) as client:
        session = await initialize(client, base)
        await post(client, base, session, "resources/subscribe", "config://app")
        stream = await get_stream(client, base, session)
        gone = await post(client, base, session, "resources/unsubscribe", "config://app")
        assert gone.status_code == 200 and gone.json()["result"] == {}
        assert server.notify_resource_updated("config://app") == 0
        await stream.quiet()
        await stream.aclose()


async def test_the_subscription_cap_is_503_over_http(
    live_server: LiveServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(easy_mcp.server, "MAX_RESOURCE_SUBSCRIPTIONS", 1)
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(timeout=10) as client:
        session = await initialize(client, base)
        first = await post(client, base, session, "resources/subscribe", "config://app")
        assert first.status_code == 200
        over = await post(client, base, session, "resources/subscribe", "config://other")
        assert over.status_code == 503
        assert over.json()["error"]["code"] == TOO_MANY_SESSIONS


# ------------------------------------------------------- stateless listen


async def test_listen_delivers_tagged_updates(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    message = listen("watch", resourceSubscriptions=["config://app", "missing://x"])
    async with httpx.AsyncClient(timeout=10) as client:
        opened = await open_stream(
            client, "POST", f"{base}/mcp", json=message, headers=headers_for(message)
        )
        assert isinstance(opened, Stream)
        assert opened.response.headers["x-accel-buffering"] == "no"
        ack = await opened.next()
        assert ack["params"]["notifications"] == {"resourceSubscriptions": ["config://app"]}
        await asyncio.to_thread(server.notify_resource_updated, "config://app")
        assert await opened.next() == updated("config://app", "watch")
        await opened.aclose()
    # Closing the stream was its cancel: nothing is left to tell.
    await until(lambda: server._notifier.count() == 0)
    assert server.notify_resource_updated("config://app") == 0


# ---------------------------------------------------------------- legacy /sse


async def test_sse_session_receives_updates(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        opened = await open_stream(client, "GET", "/sse")
        assert isinstance(opened, Stream)
        endpoint = await opened.next()
        await client.post(endpoint, json=rpc("initialize", INIT))
        assert (await opened.next())["result"]["protocolVersion"] == "2025-11-25"
        await client.post(endpoint, json=rpc("resources/subscribe", {"uri": "config://app"}, 2))
        assert await opened.next() == {"jsonrpc": "2.0", "id": 2, "result": {}}
        await asyncio.to_thread(server.notify_resource_updated, "config://app")
        assert await opened.next() == updated("config://app")
        await opened.aclose()
    await until(lambda: server.notify_resource_updated("config://app") == 0)


async def test_sse_stateless_listen_is_tagged_and_cancellable(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        opened = await open_stream(client, "GET", "/sse")
        assert isinstance(opened, Stream)
        endpoint = await opened.next()
        await client.post(endpoint, json=listen("s1", resourceSubscriptions=["config://app"]))
        ack = await opened.next()
        assert ack["params"]["_meta"] == {TAG: "s1"}
        await asyncio.to_thread(server.notify_resource_updated, "config://app")
        assert await opened.next() == updated("config://app", "s1")
        await client.post(
            endpoint, json=notification("notifications/cancelled", {"requestId": "s1"})
        )
        await until(lambda: server._notifier.count() == 0)
        assert server.notify_resource_updated("config://app") == 0
        await opened.aclose()


# ------------------------------------------------------- two workers, one store


class SubscribingStore(FakeSharedStore):
    """A FakeSharedStore that keeps resource subscriptions in its hub's records."""

    async def update_subscriptions(
        self,
        kind: SessionKind,
        ref: str,
        *,
        add: Collection[str] = (),
        remove: Collection[str] = (),
        cap: int,
    ) -> tuple[str, ...] | None:
        await self._op("subscriptions")
        hub = self.hub
        with hub.lock:
            entry = hub._live(ref, hub.clock())
            if entry is None or entry.record.kind != kind:
                return None
            uris = set(entry.record.subscriptions or ()) - set(remove)
            for uri in add:
                if uri not in uris and len(uris) < cap:
                    uris.add(uri)
            subscribed = tuple(sorted(uris))
            entry.record = dataclasses.replace(entry.record, subscriptions=subscribed)
            return subscribed


def shared_pair(
    live_server: LiveServer,
) -> tuple[FakeHub, tuple[MCPServer, str], tuple[MCPServer, str]]:
    hub = FakeHub()
    workers = []
    for name in ("a" * 16, "b" * 16):
        store = SubscribingStore(hub, name)
        hub.stores.append(store)
        server = make_server(store=store)
        workers.append((server, live_server(server)))
    return hub, workers[0], workers[1]


async def test_a_subscription_made_on_another_worker_reaches_the_stream(
    live_server: LiveServer,
) -> None:
    hub, (server_a, base_a), (server_b, base_b) = shared_pair(live_server)
    async with httpx.AsyncClient(timeout=10) as client:
        session = await initialize(client, base_a)
        stream = await get_stream(client, base_a, session)
        subscribed = await post(client, base_b, session, "resources/subscribe", "config://app")
        assert subscribed.status_code == 200, subscribed.text
        assert [r.subscriptions for r in records(hub)] == [("config://app",)]
        assert hub.published("resub")
        # Worker A reads the subscriptions again once it hears of the change.
        await until(lambda: server_a._notifier.subscriptions(session) == {"config://app"})
        # Worker B holds nothing for a session whose stream is elsewhere.
        assert server_b._notifier.subscriptions(session) == frozenset()
        await asyncio.to_thread(server_a.notify_resource_updated, "config://app")
        assert await stream.next() == updated("config://app")
        gone = await post(client, base_b, session, "resources/unsubscribe", "config://app")
        assert gone.status_code == 200
        await until(lambda: server_a._notifier.subscriptions(session) == frozenset())
        await stream.aclose()


async def test_a_stream_opened_on_another_worker_starts_from_the_record(
    live_server: LiveServer,
) -> None:
    hub, (server_a, base_a), (server_b, base_b) = shared_pair(live_server)
    async with httpx.AsyncClient(timeout=10) as client:
        session = await initialize(client, base_a)
        await post(client, base_a, session, "resources/subscribe", "config://app")
        # No stream anywhere: with a shared store nothing waits for one.
        assert server_a.notify_resource_updated("config://app") == 0
        stream = await get_stream(client, base_b, session)
        await until(lambda: server_b._notifier.subscriptions(session) == {"config://app"})
        await asyncio.to_thread(server_b.notify_resource_updated, "config://app")
        assert await stream.next() == updated("config://app")
        await stream.aclose()
        # The stream's end leaves nothing behind on its worker.
        await until(lambda: server_b._notifier.subscriptions(session) == frozenset())


async def test_sse_subscription_relayed_from_another_worker_reaches_the_owner(
    live_server: LiveServer,
) -> None:
    hub, (server_a, base_a), (_, base_b) = shared_pair(live_server)
    async with httpx.AsyncClient(timeout=10) as client:
        opened = await open_stream(client, "GET", f"{base_a}/sse")
        assert isinstance(opened, Stream)
        endpoint = await opened.next()
        await client.post(f"{base_a}{endpoint}", json=rpc("initialize", INIT))
        await opened.next()
        posted = await client.post(
            f"{base_b}{endpoint}", json=rpc("resources/subscribe", {"uri": "config://app"}, 3)
        )
        assert posted.status_code == 202
        assert await opened.next() == {"jsonrpc": "2.0", "id": 3, "result": {}}
        # The owner reads the subscriptions again once it hears of the change.
        session = _sse_session(server_a)
        await until(lambda: server_a._notifier.subscriptions(session) == {"config://app"})
        assert [r.subscriptions for r in records(hub) if r.kind == "sse"] == [("config://app",)]
        await asyncio.to_thread(server_a.notify_resource_updated, "config://app")
        assert await opened.next() == updated("config://app")
        await opened.aclose()


def _sse_session(server: MCPServer) -> str:
    transport = server._transport
    assert transport is not None
    legacy = transport._legacy  # type: ignore[attr-defined]
    (session_id,) = legacy._sessions
    return str(session_id)
