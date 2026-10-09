"""List-change notifications and subscriptions/listen, at the dispatch level (no network)."""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import threading
import time
from typing import Any

import pytest
from conftest import LogCapture, Pushed, listen, make_context, modern, notification, rpc

import easy_mcp.subscriptions
from easy_mcp import APIKeyAuth, ClientIdentity, MCPServer, RequestInfo, SubscriptionLimitError
from easy_mcp.exceptions import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    RATE_LIMITED,
    TOO_MANY_SESSIONS,
    ProtocolError,
)
from easy_mcp.middleware import RequestNext, RequestOutcome
from easy_mcp.protocol import is_modern_request
from easy_mcp.security.oauth import LEEWAY_SECONDS
from easy_mcp.transport.base import ClientContext

# Built, not written out, so secret scanners do not take the fixtures for credentials.
REPORTS_KEY = "lc-reports-key-" + "k" * 12
ADMIN_KEY = "lc-admin-key-" + "k" * 14
ALL_KEY = "lc-all-key-" + "k" * 16

TAG = "io.modelcontextprotocol/subscriptionId"
SERVER_INFO = "io.modelcontextprotocol/serverInfo"
TOOLS_CHANGED = {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "t", "version": "1"},
}


def make_server(**kwargs: Any) -> MCPServer:
    options: dict[str, Any] = {"rate_limit_per_minute": None}
    options.update(kwargs)
    server = MCPServer(port=0, **options)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def keyed_server(**kwargs: Any) -> MCPServer:
    return make_server(
        auth=APIKeyAuth({REPORTS_KEY: ["reports"], ADMIN_KEY: ["admin"], ALL_KEY: "*"}),
        **kwargs,
    )


def register(server: MCPServer, name: str = "extra", **options: Any) -> None:
    def tool() -> str:
        """A tool registered at runtime."""
        return name

    server.register_tool(tool, name=name, **options)


def identity_of(server: MCPServer, key: str | None) -> ClientIdentity | None:
    return server.authenticate_key(key)


async def initialized(
    server: MCPServer,
    pushed: Pushed,
    identity: ClientIdentity | None = None,
    session_id: str = "session-1",
) -> ClientContext:
    """A session context whose initialize succeeded, as stdio builds one."""
    context = make_context(identity, session_id=session_id, push=pushed, multiplexed=True)
    response = await server.dispatch(rpc("initialize", INIT), context)
    assert response is not None and "result" in response, response
    return context


async def settle(window: float, factor: float = 6.0) -> None:
    """Wait long enough for every open window to have flushed."""
    await asyncio.sleep(window * factor + 0.05)


async def open_listen(
    server: MCPServer,
    context: ClientContext,
    pushed: Pushed,
    msg_id: Any = "listen-1",
    **notifications: Any,
) -> asyncio.Task[dict[str, Any] | None]:
    """Start a listen request and wait for its acknowledgment."""
    before = len(pushed.frames)
    task = asyncio.create_task(server.dispatch(listen(msg_id, **notifications), context))
    deadline = time.monotonic() + 5
    while len(pushed.frames) <= before and not task.done():
        assert time.monotonic() < deadline, "no acknowledgment"
        await asyncio.sleep(0.005)
    assert not task.done(), task.result()
    return task


# --------------------------------------------------- capabilities and eras


async def test_initialize_advertises_tools_list_changed() -> None:
    response = await make_server().dispatch(rpc("initialize", INIT), make_context())
    assert response is not None
    assert response["result"]["capabilities"] == {"tools": {"listChanged": True}}


async def test_discover_advertises_tools_list_changed() -> None:
    response = await make_server().dispatch(modern("server/discover"), make_context())
    assert response is not None
    result = response["result"]
    assert result["capabilities"] == {"tools": {"listChanged": True}}
    assert result["ttlMs"] == 3_600_000
    assert result["cacheScope"] == "public"


async def test_tools_list_ttl_stays_zero() -> None:
    response = await make_server().dispatch(modern("tools/list"), make_context())
    assert response is not None
    assert response["result"]["ttlMs"] == 0
    assert response["result"]["cacheScope"] == "public"


async def test_listen_exists_only_in_the_stateless_era() -> None:
    assert is_modern_request("subscriptions/listen", {})
    server = make_server()
    context = make_context(push=Pushed())
    bare = rpc("subscriptions/listen", {"notifications": {"toolsListChanged": True}})
    response = await server.dispatch(bare, context)
    assert response is not None and response["error"]["code"] == INVALID_PARAMS


async def test_capabilities_are_sticky_and_a_late_one_is_logged(logs: LogCapture) -> None:
    server = make_server()
    server._advertise("prompts", {"listChanged": True})
    assert "after serving began" not in logs.text
    server._started = True
    server._advertise("resources", {"subscribe": True, "listChanged": True})
    server._advertise("resources", {"listChanged": False})  # already there: kept as it was
    assert logs.text.count("after serving began") == 1
    capabilities = server._capabilities()
    assert capabilities["resources"] == {"subscribe": True, "listChanged": True}
    assert server._list_kinds() == ("tools", "prompts", "resources")


# --------------------------------------------------------- legacy sessions


async def test_registering_a_tool_notifies_an_initialized_session(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    register(server)
    assert await pushed.wait_for(1) == [TOOLS_CHANGED]
    await settle(fast_debounce)
    assert pushed.frames == [TOOLS_CHANGED]


async def test_unregistering_a_tool_notifies(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    server.unregister_tool("add")
    assert await pushed.wait_for(1) == [TOOLS_CHANGED]


async def test_nothing_is_sent_before_initialize(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    await server.dispatch(rpc("tools/list"), context)
    register(server)
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_a_failed_initialize_starts_nothing(fast_debounce: float) -> None:
    server = make_server()

    @server.middleware
    async def refuse(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "initialize":
            raise ProtocolError("not today")
        return await call_next()

    pushed = Pushed()
    context = make_context(push=pushed)
    response = await server.dispatch(rpc("initialize", INIT), context)
    assert response is not None and "error" in response
    assert server._notifier.session(context.session_id) is None
    register(server)
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_burst_of_registrations_is_one_notification(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    for index in range(20):
        register(server, f"burst{index}")
    await pushed.wait_for(1)
    await settle(fast_debounce)
    assert pushed.frames == [TOOLS_CHANGED]


async def test_add_then_remove_inside_the_window_is_silent(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    register(server)
    server.unregister_tool("extra")
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_identical_reregistration_is_silent(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    definition = server.unregister_tool("add")
    server.register_tool(definition.fn, name="add")
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_a_changed_definition_is_announced(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    definition = server.unregister_tool("add")
    server.register_tool(definition.fn, name="add", description="Now described differently.")
    assert await pushed.wait_for(1) == [TOOLS_CHANGED]


async def test_window_bounds_latency_under_constant_churn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    window = 0.05
    monkeypatch.setattr(easy_mcp.subscriptions, "LIST_CHANGED_DEBOUNCE_SECONDS", window)
    server = make_server()
    arrivals: list[float] = []
    pushed = Pushed()

    def record(message: dict[str, Any]) -> None:
        arrivals.append(time.monotonic())
        pushed(message)

    context = make_context(session_id="churn", push=record)
    await server.dispatch(rpc("initialize", INIT), context)
    first = time.monotonic()
    index = 0
    while time.monotonic() - first < 0.3:
        register(server, f"churn{index}")
        index += 1
        await asyncio.sleep(0.005)
    await settle(window, 3)
    assert arrivals, "no notification at all"
    assert arrivals[0] - first < 2 * window + 0.05
    assert 3 <= len(arrivals) <= 8, len(arrivals)


async def test_hidden_tool_changes_are_not_announced_to_anonymous(fast_debounce: float) -> None:
    server = keyed_server()
    anonymous, keyed = Pushed(), Pushed()
    _anonymous = await initialized(server, anonymous, None, "anonymous")
    _keyed = await initialized(server, keyed, identity_of(server, REPORTS_KEY), "keyed")
    register(server, requires_auth=True)
    assert await keyed.wait_for(1) == [TOOLS_CHANGED]
    await settle(fast_debounce)
    assert anonymous.frames == []


async def test_scoped_tool_reaches_only_keys_with_the_scope(fast_debounce: float) -> None:
    server = keyed_server()
    channels = {name: Pushed() for name in ("reports", "admin", "all", "none")}
    keys = {"reports": REPORTS_KEY, "admin": ADMIN_KEY, "all": ALL_KEY, "none": None}
    sessions = [
        await initialized(server, pushed, identity_of(server, keys[name]), name)
        for name, pushed in channels.items()
    ]
    register(server, scopes=("reports",))
    await channels["reports"].wait_for(1)
    await channels["all"].wait_for(1)
    await settle(fast_debounce)
    told = {name for name, pushed in channels.items() if pushed.frames}
    assert told == {"reports", "all"}
    assert len(sessions) == 4


async def test_scope_change_reaches_gainers_and_losers_only(fast_debounce: float) -> None:
    server = keyed_server()
    register(server, "report", scopes=("reports",))
    channels = {name: Pushed() for name in ("reports", "admin", "all", "none")}
    keys = {"reports": REPORTS_KEY, "admin": ADMIN_KEY, "all": ALL_KEY, "none": None}
    sessions = [
        await initialized(server, pushed, identity_of(server, keys[name]), name)
        for name, pushed in channels.items()
    ]
    definition = server.unregister_tool("report")
    server.register_tool(definition.fn, name="report", scopes=("admin",))
    await channels["reports"].wait_for(1)
    await channels["admin"].wait_for(1)
    await settle(fast_debounce)
    # The tool's entry is the same for every key that sees it before and after.
    told = {name for name, pushed in channels.items() if pushed.frames}
    assert told == {"reports", "admin"}
    assert len(sessions) == 4


async def test_registration_from_a_worker_thread_notifies(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    worker = threading.Thread(target=register, args=(server,))
    worker.start()
    await asyncio.to_thread(worker.join)
    assert await pushed.wait_for(1) == [TOOLS_CHANGED]
    assert set(pushed.threads) == {threading.get_ident()}  # delivered on the loop


def test_registration_before_serving_is_silent_and_cheap() -> None:
    server = make_server()
    for index in range(50):
        register(server, f"early{index}")
    assert server._notifier.count() == 0
    assert server._notifier._sessions == {}


async def test_close_subscriptions_stops_session_delivery(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed)
    assert server.close_subscriptions(context) == 1
    assert server.close_subscriptions(context) == 0  # idempotent
    register(server)
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_forgotten_context_is_released(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed, session_id="forgotten")
    assert server._notifier.session("forgotten") is not None
    del context
    gc.collect()
    assert server._notifier.session("forgotten") is None
    register(server)
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_failing_push_drops_the_recipient(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed(explode=True)
    _context = await initialized(server, pushed)
    register(server, "one")
    deadline = time.monotonic() + 5
    while pushed.calls < 1:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.005)
    register(server, "two")
    await settle(fast_debounce)
    assert pushed.calls == 1
    assert server._notifier.session("session-1") is None


async def test_two_contexts_on_two_loops(fast_debounce: float) -> None:
    server = make_server()
    here = Pushed()
    _context = await initialized(server, here, session_id="here")
    there = Pushed()
    ready = threading.Event()
    done = threading.Event()

    async def other_loop() -> None:
        _there = await initialized(server, there, session_id="there")
        ready.set()
        await there.wait_for(1)
        done.set()

    thread = threading.Thread(target=asyncio.run, args=(other_loop(),))
    thread.start()
    assert await asyncio.to_thread(ready.wait, 5)
    register(server)
    await here.wait_for(1)
    assert await asyncio.to_thread(done.wait, 5)
    await asyncio.to_thread(thread.join, 5)
    assert set(here.threads) == {threading.get_ident()}
    assert set(there.threads) == {thread.ident}


async def test_a_session_is_judged_by_its_latest_credential(fast_debounce: float) -> None:
    server = keyed_server()
    pushed = Pushed()
    reports = identity_of(server, REPORTS_KEY)
    admin = identity_of(server, ADMIN_KEY)
    context = await initialized(server, pushed, reports)
    # A later request of the session presents another credential (as a
    # refreshed OAuth token would): what the session is told follows it.
    await server.dispatch(rpc("ping", msg_id=2), dataclasses.replace(context, identity=admin))
    register(server, scopes=("admin",))
    assert await pushed.wait_for(1) == [TOOLS_CHANGED]


# --------------------------------------------------------- listen streams


async def test_listen_acks_first_with_the_honored_filter() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed, toolsListChanged=True)
    assert pushed.frames == [
        {
            "jsonrpc": "2.0",
            "method": "notifications/subscriptions/acknowledged",
            "params": {"_meta": {TAG: "listen-1"}, "notifications": {"toolsListChanged": True}},
        }
    ]
    server.close_subscriptions(context)
    assert await task is None


async def test_listen_omits_unsupported_kinds() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(
        server,
        context,
        pushed,
        toolsListChanged=False,
        promptsListChanged=True,
        resourcesListChanged=True,
        resourceSubscriptions=["file:///a"],
        somethingNew=True,
    )
    assert pushed.frames[0]["params"]["notifications"] == {}
    server.close_subscriptions(context)
    await task


async def test_listen_empty_filter_is_acked_and_stays_open() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed)
    assert pushed.frames[0]["params"]["notifications"] == {}
    await asyncio.sleep(0.1)
    assert not task.done()
    server.close_subscriptions(context)
    assert await task is None


@pytest.mark.parametrize("msg_id", ["listen-1", 7])
async def test_listen_delivers_tagged_list_changed(fast_debounce: float, msg_id: Any) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed, msg_id, toolsListChanged=True)
    register(server)
    frames = await pushed.wait_for(2)
    assert frames[1] == {
        "jsonrpc": "2.0",
        "method": "notifications/tools/list_changed",
        "params": {"_meta": {TAG: msg_id}},
    }
    assert type(frames[1]["params"]["_meta"][TAG]) is type(msg_id)
    server.close_subscriptions(context)
    await task


async def test_listen_without_tools_flag_hears_nothing(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed)
    register(server)
    await settle(fast_debounce)
    assert len(pushed.frames) == 1
    server.close_subscriptions(context)
    await task


async def test_two_subscriptions_on_one_channel_are_demultiplexed(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    tools = await open_listen(server, context, pushed, "a", toolsListChanged=True)
    other = await open_listen(server, context, pushed, 2, toolsListChanged=True)
    quiet = await open_listen(server, context, pushed, "quiet")
    register(server)
    await pushed.wait_for(5)
    await settle(fast_debounce)
    changes = [frame for frame in pushed.frames if frame["method"].endswith("list_changed")]
    assert sorted(str(frame["params"]["_meta"][TAG]) for frame in changes) == ["2", "a"]
    assert server.close_subscriptions(context, reason="shutdown") == 3
    assert [await tools, await other, await quiet] == [None, None, None]


async def test_listen_requires_a_notifications_object() -> None:
    server = make_server()
    context = make_context(push=Pushed())
    for params in ({}, {"notifications": None}, {"notifications": ["toolsListChanged"]}):
        response = await server.dispatch(modern("subscriptions/listen", params), context)
        assert response is not None and response["error"]["code"] == INVALID_PARAMS, params
        assert "notifications" in response["error"]["message"]


async def test_listen_rejects_non_boolean_flags() -> None:
    server = make_server()
    context = make_context(push=Pushed())
    for value in ("yes", 1, None, {}):
        response = await server.dispatch(listen(toolsListChanged=value), context)
        assert response is not None and response["error"]["code"] == INVALID_PARAMS
        assert "toolsListChanged" in response["error"]["message"]


async def test_listen_rejects_non_string_resource_subscriptions() -> None:
    server = make_server()
    context = make_context(push=Pushed())
    for value in ("file:///a", [1], [None], {"uri": "x"}):
        response = await server.dispatch(listen(resourceSubscriptions=value), context)
        assert response is not None and response["error"]["code"] == INVALID_PARAMS
        assert "resourceSubscriptions" in response["error"]["message"]


async def test_listen_requires_a_string_or_number_id() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    for bad in (None, True, {}, []):
        response = await server.dispatch(listen(bad), context)
        assert response is not None and response["error"]["code"] == INVALID_REQUEST, bad
    assert pushed.frames == []
    for good in (7, "listen-1", 1.5):
        task = await open_listen(server, context, pushed, good)
        tag = pushed.frames[-1]["params"]["_meta"][TAG]
        assert tag == good and type(tag) is type(good)
        server.close_subscriptions(context)
        assert await task is None


async def test_listen_without_a_push_channel_is_method_not_found() -> None:
    server = make_server()
    response = await server.dispatch(listen(toolsListChanged=True), make_context())
    assert response is not None and response["error"]["code"] == METHOD_NOT_FOUND


async def test_duplicate_subscription_id_on_one_channel_is_refused() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    task = await open_listen(server, context, pushed, 5, toolsListChanged=True)
    duplicate = await server.dispatch(listen(5), context)
    assert duplicate is not None and duplicate["error"]["code"] == INVALID_REQUEST
    assert "already open" in duplicate["error"]["message"]
    # The same id as a string, or on another channel, is another subscription.
    as_text = await open_listen(server, context, pushed, "5")
    elsewhere = Pushed()
    other = make_context(push=elsewhere)
    there = await open_listen(server, other, elsewhere, 5)
    assert server._notifier.count() == 3
    server.close_subscriptions(context)
    server.close_subscriptions(other)
    assert [await task, await as_text, await there] == [None, None, None]


async def test_client_cancel_ends_the_subscription_silently(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    task = await open_listen(server, context, pushed, "listen-1", toolsListChanged=True)
    register(server)  # a change is now pending in the window
    cancel = notification("notifications/cancelled", {"requestId": "listen-1"})
    assert await server.dispatch(cancel, context) is None
    assert await task is None
    await settle(fast_debounce)
    assert pushed.methods() == ["notifications/subscriptions/acknowledged"]
    assert server._notifier.count() == 0


async def test_cancel_names_a_subscription_of_its_own_channel_only() -> None:
    server = make_server()
    mine, theirs = Pushed(), Pushed()
    my_context = make_context(push=mine)
    their_context = make_context(push=theirs)
    mine_task = await open_listen(server, my_context, mine, "listen-1")
    theirs_task = await open_listen(server, their_context, theirs, "listen-1")
    cancel = notification("notifications/cancelled", {"requestId": "listen-1"})
    await server.dispatch(cancel, my_context)
    assert await mine_task is None
    assert not theirs_task.done()
    server.close_subscriptions(their_context)
    await theirs_task


async def test_server_close_sends_the_completion_result() -> None:
    server = make_server(name="calc", version="9.9")
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed, "listen-1", toolsListChanged=True)
    assert server.close_subscriptions(context, reason="shutdown") == 1
    assert pushed.frames[1:] == [
        {
            "jsonrpc": "2.0",
            "id": "listen-1",
            "result": {
                "resultType": "complete",
                "_meta": {TAG: "listen-1", SERVER_INFO: {"name": "calc", "version": "9.9"}},
            },
        }
    ]
    assert await task is None
    assert server._notifier.count() == 0


async def test_server_close_on_a_multiplexed_channel_then_cancels() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    task = await open_listen(server, context, pushed, 9, toolsListChanged=True)
    server.close_subscriptions(context, reason="shutdown")
    result, cancelled = pushed.frames[1:]
    assert result["id"] == 9 and result["result"]["resultType"] == "complete"
    assert cancelled == {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": 9, "reason": "server shutting down", "_meta": {TAG: 9}},
    }
    assert await task is None


async def test_per_client_subscription_cap() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, client_id="ip:crowded")
    tasks = [await open_listen(server, context, pushed, index) for index in range(8)]
    refused = await server.dispatch(listen(99), context)
    assert refused is not None and refused["error"]["code"] == TOO_MANY_SESSIONS
    assert SubscriptionLimitError.code == TOO_MANY_SESSIONS
    other_pushed = Pushed()
    other = make_context(push=other_pushed, client_id="ip:another")
    other_task = await open_listen(server, other, other_pushed)
    # Ending one frees its slot.
    cancel = notification("notifications/cancelled", {"requestId": 0})
    await server.dispatch(cancel, context)
    assert await tasks[0] is None
    again = await open_listen(server, context, pushed, 99)
    server.close_subscriptions(context)
    server.close_subscriptions(other)
    await asyncio.gather(*tasks[1:], other_task, again)
    assert server._notifier.count() == 0


async def test_server_wide_cap_uses_max_sessions() -> None:
    server = make_server(max_sessions=2)
    contexts = []
    tasks = []
    for index in range(2):
        pushed = Pushed()
        context = make_context(push=pushed, client_id=f"ip:client{index}")
        contexts.append(context)
        tasks.append(await open_listen(server, context, pushed))
    third = make_context(push=Pushed(), client_id="ip:client3")
    refused = await server.dispatch(listen(), third)
    assert refused is not None and refused["error"]["code"] == TOO_MANY_SESSIONS
    for context in contexts:
        server.close_subscriptions(context)
    await asyncio.gather(*tasks)


async def test_listen_spends_rate_limit_budget(fast_debounce: float) -> None:
    server = make_server(rate_limit_per_minute=2)
    pushed = Pushed()
    context = make_context(push=pushed, client_id="ip:budget")
    task = await open_listen(server, context, pushed, "listen-1", toolsListChanged=True)
    for index in range(3):
        register(server, f"churn{index}")
        await pushed.wait_for(2 + index)
    # Frames the server pushes spend nothing: one unit is left.
    second = await open_listen(server, context, pushed, "listen-2")
    refused = await server.dispatch(listen("listen-3"), context)
    assert refused is not None and refused["error"]["code"] == RATE_LIMITED
    server.close_subscriptions(context)
    await asyncio.gather(task, second)


async def test_listen_is_not_cut_by_the_tool_timeout() -> None:
    server = make_server(default_timeout=0.05)
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed)
    await asyncio.sleep(0.2)
    assert not task.done()
    server.close_subscriptions(context)
    assert await task is None


async def test_listen_does_not_touch_call_counts() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed)
    server.close_subscriptions(context)
    await task
    assert context.tool_calls == {}
    assert context.in_flight == {}


async def test_visibility_applies_to_listen_streams(fast_debounce: float) -> None:
    server = keyed_server()
    anonymous, keyed = Pushed(), Pushed()
    anonymous_context = make_context(push=anonymous)
    keyed_context = make_context(identity_of(server, REPORTS_KEY), push=keyed)
    first = await open_listen(server, anonymous_context, anonymous, toolsListChanged=True)
    second = await open_listen(server, keyed_context, keyed, toolsListChanged=True)
    register(server, requires_auth=True)
    await keyed.wait_for(2)
    await settle(fast_debounce)
    assert len(anonymous.frames) == 1
    server.close_subscriptions(anonymous_context)
    server.close_subscriptions(keyed_context)
    await asyncio.gather(first, second)


async def test_a_caller_cancel_ends_the_stream_and_propagates() -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await open_listen(server, context, pushed)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert server._notifier.count() == 0
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await server.dispatch(listen("listen-2"), context)
    assert server._notifier.count() == 0


async def test_listen_ends_when_its_token_expires() -> None:
    server = make_server()
    pushed = Pushed()
    token = ClientIdentity(
        fingerprint="f" * 12,
        scopes=frozenset(),
        subject="user",
        issuer="https://issuer.example.com",
        expires_at=int(time.time()) - LEEWAY_SECONDS + 1,
    )
    context = make_context(token, push=pushed)
    task = await open_listen(server, context, pushed)
    assert await asyncio.wait_for(task, 5) is None
    assert pushed.frames[-1]["result"]["resultType"] == "complete"


async def test_middleware_sees_listen_and_may_refuse_it() -> None:
    server = make_server()
    seen: list[str] = []

    @server.middleware
    async def gate(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        seen.append(request.method)
        if request.params.get("notifications", {}).get("promptsListChanged"):
            raise ProtocolError("no prompts here")
        return await call_next()

    pushed = Pushed()
    context = make_context(push=pushed)
    refused = await server.dispatch(listen(promptsListChanged=True), context)
    assert refused is not None and refused["error"]["message"] == "no prompts here"
    assert pushed.frames == []
    task = await open_listen(server, context, pushed, toolsListChanged=True)
    server.close_subscriptions(context)
    assert await task is None
    assert seen == ["subscriptions/listen", "subscriptions/listen"]


async def test_an_unsendable_acknowledgment_is_an_internal_error(logs: LogCapture) -> None:
    server = make_server()
    response = await server.dispatch(listen(), make_context(push=Pushed(explode=True)))
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert server._notifier.count() == 0
    assert [event["reason"] for event in logs.events("subscription_close")] == ["undeliverable"]


async def test_subscription_lifecycle_is_audited(fast_debounce: float, logs: LogCapture) -> None:
    server = make_server(max_sessions=3)
    pushed = Pushed()
    context = make_context(push=pushed, client_id="ip:audited", multiplexed=True)
    cancelled = await open_listen(server, context, pushed, "c", toolsListChanged=True)
    await server.dispatch(notification("notifications/cancelled", {"requestId": "c"}), context)
    await cancelled
    disconnected = await open_listen(server, context, pushed, "d")
    disconnected.cancel()
    with pytest.raises(asyncio.CancelledError):
        await disconnected
    shut = await open_listen(server, context, pushed, "s")
    server.close_subscriptions(context, reason="shutdown")
    await shut
    exploding = Pushed()
    broken_context = make_context(push=exploding, client_id="ip:audited")
    broken = await open_listen(server, broken_context, exploding, "u", toolsListChanged=True)
    exploding.explode = True
    register(server)
    assert await asyncio.wait_for(broken, 5) is None
    channels = [Pushed() for _ in range(4)]
    crowd = [
        make_context(push=channel, client_id=f"ip:crowd{index}")
        for index, channel in enumerate(channels)
    ]
    crowd_tasks = [
        await open_listen(server, context, channel, "x")
        for context, channel in zip(crowd[:3], channels, strict=False)
    ]
    await server.dispatch(listen("x"), crowd[3])
    for crowd_context in crowd[:3]:
        server.close_subscriptions(crowd_context)
    await asyncio.gather(*crowd_tasks)

    opened = logs.events("subscription_open")
    assert opened[0] == {
        "type": "subscription_open",
        "client_id": "ip:audited",
        "subscription_id": "c",
        "kinds": ["tools"],
    }
    reasons = [
        (event["subscription_id"], event["reason"]) for event in logs.events("subscription_close")
    ]
    assert reasons[:4] == [
        ("c", "client_cancelled"),
        ("d", "disconnected"),
        ("s", "shutdown"),
        ("u", "undeliverable"),
    ]
    assert [event["reason"] for event in logs.events("subscription_refused")] == ["server_limit"]
    assert logs.events("request_cancelled") == []
