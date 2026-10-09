"""Resource subscriptions and update notifications: dispatch and stdio (no network).

resources/subscribe in the handshake era, resourceSubscriptions on a
subscriptions/listen stream in the stateless one, and
server.notify_resource_updated() delivering through the change notifier.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import threading
import time
from typing import Any

import pytest
from conftest import LogCapture, Pushed, listen, make_context, modern, notification, rpc

from easy_mcp import APIKeyAuth, ClientIdentity, MCPServer, StdioTransport
from easy_mcp.exceptions import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    TOO_MANY_SESSIONS,
)
from easy_mcp.subscriptions import MAX_RESOURCE_SUBSCRIPTIONS, _Sink
from easy_mcp.transport.base import ClientContext
from easy_mcp.uritemplate import MAX_URI_LENGTH

SEE_KEY = "subs-see-key-" + "k" * 18
TAG = "io.modelcontextprotocol/subscriptionId"
LEGACY_NOT_FOUND = -32002
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


def updated(uri: str, tag: Any = None) -> dict[str, Any]:
    if tag is None:
        return {
            "jsonrpc": "2.0",
            "method": "notifications/resources/updated",
            "params": {"uri": uri},
        }
    return {
        "jsonrpc": "2.0",
        "method": "notifications/resources/updated",
        "params": {"_meta": {TAG: tag}, "uri": uri},
    }


def make_server(**kwargs: Any) -> MCPServer:
    kwargs.setdefault("rate_limit_per_minute", None)
    server = MCPServer(port=0, **kwargs)
    server.register_resource(lambda: "app", "config://app", name="config")
    server.register_resource(lambda: "other", "config://other", name="other")
    server.register_resource(lambda n: f"item {n}", "items://{n}", name="item")
    return server


async def initialized(server: MCPServer, pushed: Pushed, **ctx: Any) -> ClientContext:
    context = make_context(push=pushed, multiplexed=True, **ctx)
    response = await server.dispatch(rpc("initialize", INIT), context)
    assert response is not None and "result" in response, response
    return context


async def subscribe(server: MCPServer, context: ClientContext, uri: Any, msg_id: Any = 7) -> Any:
    response = await server.dispatch(rpc("resources/subscribe", {"uri": uri}, msg_id), context)
    assert response is not None
    return response


async def quiet(pushed: Pushed, count: int, seconds: float = 0.3) -> None:
    """Nothing more than *count* frames arrives within *seconds*."""
    await asyncio.sleep(seconds)
    assert len(pushed.frames) == count, pushed.frames


# ------------------------------------------------------------------ dispatch


async def test_subscribe_needs_a_visible_uri() -> None:
    server = make_server(auth=APIKeyAuth({SEE_KEY: ["see"]}))
    server.register_resource(lambda: "s", "secret://x", name="secret", scopes=("see",))
    context = await initialized(server, Pushed())
    for uri in ("missing://x", "secret://x", "items://"):
        response = await subscribe(server, context, uri)
        assert response["error"]["code"] == LEGACY_NOT_FOUND, uri
        assert response["error"]["data"] == {"uri": uri}
    for bad in (5, None):
        response = await subscribe(server, context, bad)
        assert response["error"]["code"] == INVALID_PARAMS
    too_long = await subscribe(server, context, "items://" + "a" * 3000)
    assert too_long["error"]["code"] == INVALID_PARAMS
    who = ClientIdentity(fingerprint="s" * 12, scopes=frozenset({"see"}))
    seeing = await initialized(server, Pushed(), identity=who, session_id="other")
    assert (await subscribe(server, seeing, "secret://x"))["result"] == {}


async def test_subscribe_and_unsubscribe_roundtrip(logs: LogCapture) -> None:
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed)
    assert server.notify_resource_updated("config://app") == 0  # nobody subscribed yet
    assert (await subscribe(server, context, "config://app"))["result"] == {}
    assert (await subscribe(server, context, "config://app"))["result"] == {}  # idempotent
    assert (await subscribe(server, context, "items://42"))["result"] == {}
    assert server.notify_resource_updated("config://app") == 1
    assert await pushed.wait_for(1) == [updated("config://app")]
    # Exact URIs only: neither the template nor another URI matches.
    assert server.notify_resource_updated("items://{n}") == 0
    assert server.notify_resource_updated("config://other") == 0
    assert server.notify_resource_updated("items://42") == 1
    assert await pushed.wait_for(2) == [updated("config://app"), updated("items://42")]
    unsubscribe = rpc("resources/unsubscribe", {"uri": "config://app"}, 8)
    for _ in range(2):  # idempotent
        response = await server.dispatch(unsubscribe, context)
        assert response is not None and response["result"] == {}
    assert server.notify_resource_updated("config://app") == 0
    await quiet(pushed, 2)
    events = logs.events("resource_subscribe")
    assert [e["uri"] for e in events] == ["config://app", "config://app", "items://42"]
    assert set(events[0]) == {"type", "uri", "client_id", "session_id", "session_ref"}
    assert len(logs.events("resource_unsubscribe")) == 2


async def test_updates_are_coalesced_until_written() -> None:
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed)
    await subscribe(server, context, "config://app")

    def burst() -> None:
        for _ in range(50):
            server.notify_resource_updated("config://app")

    # From threads, before the loop gets to deliver: one notification.
    workers = [threading.Thread(target=burst) for _ in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    await pushed.wait_for(1)
    await quiet(pushed, 1)
    assert pushed.frames == [updated("config://app")]
    assert pushed.threads and set(pushed.threads) == {threading.get_ident()}


async def test_subscription_cap_per_session() -> None:
    server = make_server()
    context = await initialized(server, Pushed())
    for index in range(MAX_RESOURCE_SUBSCRIPTIONS):
        response = await subscribe(server, context, f"items://{index}", index)
        assert response["result"] == {}, index
    over = await subscribe(server, context, "items://over")
    assert over["error"]["code"] == TOO_MANY_SESSIONS
    again = await subscribe(server, context, "items://5")  # already watched: fine
    assert again["result"] == {}
    await server.dispatch(rpc("resources/unsubscribe", {"uri": "items://5"}), context)
    assert (await subscribe(server, context, "items://over"))["result"] == {}


async def test_subscribe_without_a_channel_is_method_not_found() -> None:
    server = make_server()
    context = make_context()  # no push, no store: nothing could ever deliver
    for method in ("resources/subscribe", "resources/unsubscribe"):
        response = await server.dispatch(rpc(method, {"uri": "config://app"}), context)
        assert response is not None and response["error"]["code"] == METHOD_NOT_FOUND


async def test_subscribe_with_a_store_that_keeps_none_is_method_not_found() -> None:
    from easy_mcp.store.base import StoreHandle

    class Bare(StoreHandle):
        async def reserve_call(self, tool: str, limit: int) -> Any:
            raise AssertionError

        async def release_call(self, tool: str) -> None:
            raise AssertionError

    server = make_server()
    context = make_context()
    context.store_handle = Bare()
    response = await server.dispatch(rpc("resources/subscribe", {"uri": "config://app"}), context)
    assert response is not None and response["error"]["code"] == METHOD_NOT_FOUND


async def test_methods_belong_to_their_era() -> None:
    server = make_server()
    pushed = Pushed()
    stateless = await server.dispatch(
        modern("resources/subscribe", {"uri": "config://app"}), make_context(push=pushed)
    )
    assert stateless is not None and stateless["error"]["code"] == METHOD_NOT_FOUND
    tool_only = MCPServer(port=0, rate_limit_per_minute=None)
    context = await initialized(tool_only, Pushed())
    response = await subscribe(tool_only, context, "config://app")
    assert response["error"]["code"] == METHOD_NOT_FOUND


async def test_listen_validation() -> None:
    server = make_server()
    context = make_context(push=Pushed())
    bad = [
        {"resourceSubscriptions": "config://app"},
        {"resourceSubscriptions": ["config://app", 3]},
    ]
    for wanted in bad:
        response = await server.dispatch(listen("l", **wanted), context)
        assert response is not None and response["error"]["code"] == INVALID_PARAMS, wanted
    # The cap is a limit, as the per-session one is (-32007, HTTP 503).
    many = [f"items://{i}" for i in range(MAX_RESOURCE_SUBSCRIPTIONS + 1)]
    response = await server.dispatch(listen("l", resourceSubscriptions=many), context)
    assert response is not None and response["error"]["code"] == TOO_MANY_SESSIONS
    assert server._notifier.count() == 0


async def open_listen(
    server: MCPServer, context: ClientContext, pushed: Pushed, msg_id: Any = "l1", **wanted: Any
) -> asyncio.Task[Any]:
    before = len(pushed.frames)
    task = asyncio.create_task(server.dispatch(listen(msg_id, **wanted), context))
    deadline = time.monotonic() + 5
    while len(pushed.frames) <= before:
        assert time.monotonic() < deadline and not task.done(), "no acknowledgment"
        await asyncio.sleep(0.005)
    return task


async def test_listen_ack_is_first_and_honors_only_visible_uris(logs: LogCapture) -> None:
    server = make_server(auth=APIKeyAuth({SEE_KEY: ["see"]}))
    server.register_resource(lambda: "s", "secret://x", name="secret", scopes=("see",))
    pushed = Pushed()
    context = make_context(push=pushed)
    asked = [
        "config://app",
        "secret://x",
        "missing://x",
        "items://9",
        "config://app",
        "items://" + "a" * 3000,
    ]
    task = await open_listen(server, context, pushed, resourceSubscriptions=asked)
    assert pushed.frames[0] == {
        "jsonrpc": "2.0",
        "method": "notifications/subscriptions/acknowledged",
        "params": {
            "_meta": {TAG: "l1"},
            "notifications": {"resourceSubscriptions": ["config://app", "items://9"]},
        },
    }
    (opened,) = logs.events("subscription_open")
    assert opened["resources"] == 2
    assert server.notify_resource_updated("secret://x") == 0
    assert server.notify_resource_updated("items://9") == 1
    assert await pushed.wait_for(2) == [pushed.frames[0], updated("items://9", "l1")]
    server.close_subscriptions(context, reason="shutdown")
    assert await asyncio.wait_for(task, 5) is None


async def test_a_uri_over_2048_characters_cannot_be_watched() -> None:
    server = make_server()
    longest = "items://" + "a" * (MAX_URI_LENGTH - len("items://"))
    too_long = longest + "a"
    context = await initialized(server, Pushed())
    assert (await subscribe(server, context, longest))["result"] == {}
    refused = await subscribe(server, context, too_long)
    assert refused["error"]["code"] == INVALID_PARAMS and "data" not in refused["error"]
    assert server._notifier.subscriptions(context.session_id) == {longest}
    pushed = Pushed()
    listening = make_context(push=pushed)
    task = await open_listen(server, listening, pushed, resourceSubscriptions=[too_long, longest])
    assert pushed.frames[0]["params"]["notifications"] == {"resourceSubscriptions": [longest]}
    assert server.notify_resource_updated(too_long) == 0
    assert server.notify_resource_updated(longest) == 2  # the session's and the listen's
    server.close_subscriptions(listening, reason="shutdown")
    await asyncio.wait_for(task, 5)


async def test_listen_without_resources_omits_resource_subscriptions() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    # However many it names: a server without resources ignores the field.
    many = [f"items://{i}" for i in range(MAX_RESOURCE_SUBSCRIPTIONS + 1)]
    for asked in (["config://app"], many):
        pushed = Pushed()
        context = make_context(push=pushed)
        task = await open_listen(
            server, context, pushed, toolsListChanged=True, resourceSubscriptions=asked
        )
        assert pushed.frames[0]["params"]["notifications"] == {"toolsListChanged": True}
        server.close_subscriptions(context, reason="shutdown")
        await asyncio.wait_for(task, 5)


async def test_listen_cancel_stops_everything_for_that_id() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    first = await open_listen(server, context, pushed, "a", resourceSubscriptions=["config://app"])
    second = await open_listen(server, context, pushed, "b", resourceSubscriptions=["config://app"])
    # Queued for both, then "a" is cancelled before the loop delivers.
    assert server.notify_resource_updated("config://app") == 2
    cancel = notification("notifications/cancelled", {"requestId": "a"})
    assert await server.dispatch(cancel, context) is None
    assert await asyncio.wait_for(first, 5) is None
    frames = await pushed.wait_for(3)
    await quiet(pushed, 3)
    assert frames[2] == updated("config://app", "b")
    server.close_subscriptions(context, reason="shutdown")
    await asyncio.wait_for(second, 5)


async def test_a_session_end_forgets_its_subscriptions() -> None:
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed)
    await subscribe(server, context, "config://app")
    assert server._notifier.subscriptions(context.session_id) == {"config://app"}
    server.close_subscriptions(context, reason="closed")
    assert server._notifier.subscriptions(context.session_id) == frozenset()
    assert server.notify_resource_updated("config://app") == 0


async def test_an_update_before_initialize_waits_for_the_session() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    response = await subscribe(server, context, "config://app")
    assert response["result"] == {}
    assert server.notify_resource_updated("config://app") == 1
    await asyncio.sleep(0.1)
    assert pushed.frames == []  # nothing is sent before the handshake's answer
    await server.dispatch(rpc("initialize", INIT), context)
    assert await pushed.wait_for(1) == [updated("config://app")]


async def test_a_resource_reprotected_keeps_its_subscribers() -> None:
    # Documented: only the URI the client already knew is sent; reads are refused.
    server = make_server(auth=APIKeyAuth({SEE_KEY: ["see"]}))
    pushed = Pushed()
    context = await initialized(server, pushed)
    await subscribe(server, context, "config://app")
    server.unregister_resource("config://app")
    server.register_resource(lambda: "now secret", "config://app", name="c", scopes=("see",))
    assert server.notify_resource_updated("config://app") == 1
    assert await pushed.wait_for(1) == [updated("config://app")]
    read = await server.dispatch(rpc("resources/read", {"uri": "config://app"}), context)
    assert read is not None and read["error"]["code"] == LEGACY_NOT_FOUND


class PausedPublisher:
    """Publish from a thread that stops right after picking the sinks, until resumed.

    That is where a thread switch can come: the loop may replace or end the
    session's sink before the update is handed to it.
    """

    def __init__(self, server: MCPServer, uri: str, monkeypatch: pytest.MonkeyPatch) -> None:
        self.picked = threading.Event()
        self.resume = threading.Event()
        self.result: int | None = None
        self.thread = threading.Thread(target=self._publish, args=(server, uri))
        original = _Sink.request_update
        paused = self

        def request_update(sink: _Sink, uri: str) -> bool:
            if threading.current_thread() is paused.thread and not paused.picked.is_set():
                paused.picked.set()
                assert paused.resume.wait(10)
            return original(sink, uri)

        monkeypatch.setattr(_Sink, "request_update", request_update)
        self.thread.start()

    def _publish(self, server: MCPServer, uri: str) -> None:
        self.result = server.notify_resource_updated(uri)

    async def finish(self) -> int | None:
        self.resume.set()
        await asyncio.to_thread(self.thread.join, 10)
        return self.result


async def test_an_update_racing_a_replaced_sink_reaches_the_new_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_server()
    first = Pushed()
    context = await initialized(server, first)
    await subscribe(server, context, "config://app")
    publisher = PausedPublisher(server, "config://app", monkeypatch)
    assert await asyncio.to_thread(publisher.picked.wait, 10)
    # A new stream takes over (as a second GET /mcp does) before the update lands.
    second = Pushed()
    server._watch_session(
        context.session_id, push=second, identity=None, client_id=context.client_id
    )
    assert await publisher.finish() == 1
    assert await second.wait_for(1) == [updated("config://app")]
    await quiet(second, 1)
    assert first.frames == []


async def test_an_update_racing_a_closing_sink_waits_for_the_next(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_server()
    first = Pushed()
    context = await initialized(server, first)
    await subscribe(server, context, "config://app")
    publisher = PausedPublisher(server, "config://app", monkeypatch)
    assert await asyncio.to_thread(publisher.picked.wait, 10)
    # The stream closes (the session stays) before the update lands.
    assert server._notifier.end_session(context.session_id)
    assert await publisher.finish() == 1
    second = Pushed()
    server._watch_session(
        context.session_id, push=second, identity=None, client_id=context.client_id
    )
    assert await second.wait_for(1) == [updated("config://app")]


async def test_an_update_queued_on_a_closing_sink_waits_for_the_next() -> None:
    server = make_server()
    first = Pushed()
    context = await initialized(server, first)
    await subscribe(server, context, "config://app")
    # Queued, then the stream closes before the loop writes it.
    assert server.notify_resource_updated("config://app") == 1
    assert server._notifier.end_session(context.session_id)
    second = Pushed()
    server._watch_session(
        context.session_id, push=second, identity=None, client_id=context.client_id
    )
    assert await second.wait_for(1) == [updated("config://app")]
    assert first.frames == []


class ClosedChannel(Pushed):
    """A channel that has closed while its sink is still the session's (a GET /mcp
    stream whose token expired, before its end is handled): it refuses every
    frame, and *late* is published while it refuses the first.
    """

    def __init__(self, server: MCPServer, late: str) -> None:
        super().__init__(explode=True)
        self.server = server
        self.late: str | None = late
        self.queued: int | None = None

    def __call__(self, message: dict[str, Any]) -> None:
        if self.late is not None:
            late, self.late = self.late, None
            self.queued = self.server.notify_resource_updated(late)
        super().__call__(message)


async def test_updates_a_closed_channel_refuses_wait_for_the_next_sink() -> None:
    server = make_server()
    first = Pushed()
    context = await initialized(server, first)
    for uri in ("config://app", "config://other", "items://42"):
        await subscribe(server, context, uri)
    closed = ClosedChannel(server, late="items://42")
    sink = server._watch_session(
        context.session_id, push=closed, identity=None, client_id=context.client_id
    )
    # Both in one batch: the first is refused, and the second is never tried.
    assert server.notify_resource_updated("config://app") == 1
    assert server.notify_resource_updated("config://other") == 1
    await asyncio.sleep(0.1)
    assert closed.calls == 1 and closed.frames == [] and closed.queued == 1
    # The stream's end is handled after its sink has gone.
    assert not server._notifier.end_session(context.session_id, sink)
    second = Pushed()
    server._watch_session(
        context.session_id, push=second, identity=None, client_id=context.client_id
    )
    assert await second.wait_for(3) == [
        updated("config://app"),
        updated("config://other"),
        updated("items://42"),
    ]
    await quiet(second, 3)
    assert first.frames == []


async def test_an_update_queued_on_a_replaced_sink_reaches_the_new_one() -> None:
    server = make_server()
    first = Pushed()
    context = await initialized(server, first)
    await subscribe(server, context, "config://app")
    # Queued on the first sink; a new stream takes over before the loop writes it.
    assert server.notify_resource_updated("config://app") == 1
    assert first.frames == []
    second = Pushed()
    server._watch_session(
        context.session_id, push=second, identity=None, client_id=context.client_id
    )
    assert await second.wait_for(1) == [updated("config://app")]
    await quiet(second, 1)
    assert first.frames == []


async def test_an_update_queued_before_unsubscribe_is_not_sent() -> None:
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed)
    await subscribe(server, context, "config://app")
    await subscribe(server, context, "config://other")
    assert server.notify_resource_updated("config://app") == 1
    assert pushed.frames == []
    # Before the loop writes it.  Not through dispatch, which would yield to
    # the loop first and let the update be written before the unsubscribe.
    server._notifier.unsubscribe(context.session_id, "config://app")
    await quiet(pushed, 0)
    assert server.notify_resource_updated("config://other") == 1
    assert await pushed.wait_for(1) == [updated("config://other")]


def test_notify_needs_a_string() -> None:
    with pytest.raises(TypeError):
        make_server().notify_resource_updated(b"config://app")  # type: ignore[arg-type]


async def test_session_gone_in_the_store_is_invalid_request() -> None:
    from easy_mcp.store.base import StoreHandle

    class Gone(StoreHandle):
        async def reserve_call(self, tool: str, limit: int) -> Any:
            raise AssertionError

        async def release_call(self, tool: str) -> None:
            raise AssertionError

        async def update_subscriptions(self, **_: Any) -> None:
            return None

    server = make_server()
    context = make_context()
    context.store_handle = Gone()
    response = await server.dispatch(rpc("resources/subscribe", {"uri": "config://app"}), context)
    assert response is not None and response["error"]["code"] == INVALID_REQUEST


# --------------------------------------------------------------------- stdio


class Stdin:
    """A real pipe for stdin, so the test decides when EOF arrives."""

    def __init__(self) -> None:
        read_fd, write_fd = os.pipe()
        self.reader = os.fdopen(read_fd, "rb")
        self._writer = os.fdopen(write_fd, "wb")

    def send(self, *messages: dict[str, Any]) -> None:
        for message in messages:
            self._writer.write(json.dumps(message).encode() + b"\n")
        self._writer.flush()

    def close(self) -> None:
        self._writer.close()


class Session:
    """A StdioTransport serving on this loop, stdin a pipe and stdout a buffer."""

    def __init__(self, server: MCPServer) -> None:
        self.stdin = Stdin()
        self.stdout = io.BytesIO()
        self.transport = StdioTransport(server, stdin=self.stdin.reader, stdout=self.stdout)
        self.task = asyncio.create_task(self.transport.serve())

    def lines(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.stdout.getvalue().splitlines() if line.strip()]

    async def wait_for(self, count: int, timeout: float = 5.0) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout
        while len(self.lines()) < count:
            assert time.monotonic() < deadline, f"expected {count} line(s), got {self.lines()}"
            await asyncio.sleep(0.005)
        return self.lines()

    async def finish(self) -> list[dict[str, Any]]:
        self.stdin.close()
        await asyncio.wait_for(self.task, 10)
        self.stdin.reader.close()
        return self.lines()


async def test_stdio_handshake_era_subscription_delivers_updates() -> None:
    server = make_server()
    session = Session(server)
    session.stdin.send(
        rpc("initialize", INIT),
        notification("notifications/initialized"),
        rpc("resources/subscribe", {"uri": "config://app"}, 2),
    )
    await session.wait_for(2)
    await asyncio.to_thread(server.notify_resource_updated, "config://app")
    lines = await session.wait_for(3)
    assert lines[1] == {"jsonrpc": "2.0", "id": 2, "result": {}}
    assert lines[2] == updated("config://app")
    assert len(await session.finish()) == 3
    # The session ended with stdin: nothing is left subscribed.
    assert server.notify_resource_updated("config://app") == 0


async def test_stdio_listen_ack_then_tagged_updates_and_two_listens() -> None:
    server = make_server()
    session = Session(server)
    session.stdin.send(
        listen("one", resourceSubscriptions=["config://app"]),
        listen(2, resourceSubscriptions=["config://app", "config://other"]),
    )
    await session.wait_for(2)
    server.notify_resource_updated("config://other")
    lines = await session.wait_for(3)
    assert lines[2] == updated("config://other", 2)
    server.notify_resource_updated("config://app")
    lines = await session.wait_for(5)
    assert sorted(json.dumps(line, sort_keys=True) for line in lines[3:5]) == sorted(
        json.dumps(updated("config://app", tag), sort_keys=True) for tag in ("one", 2)
    )
    session.stdin.send(notification("notifications/cancelled", {"requestId": "one"}))
    deadline = time.monotonic() + 10
    while server._notifier.count() != 1:  # the cancel has been acted on
        assert time.monotonic() < deadline, "the cancel never arrived"
        await asyncio.sleep(0.01)
    server.notify_resource_updated("config://app")
    lines = await session.wait_for(6)
    assert lines[5] == updated("config://app", 2)
    final = await session.finish()
    # Shutdown: listen 2 gets its result, then notifications/cancelled; "one" nothing more.
    assert [line.get("id") for line in final[6:]] == [2, None]
    assert final[7]["method"] == "notifications/cancelled"
    assert final[7]["params"]["requestId"] == 2
