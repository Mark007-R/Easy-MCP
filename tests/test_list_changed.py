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
from shared_store_fake import FakeHub

import easy_mcp.subscriptions
from easy_mcp import (
    APIKeyAuth,
    ClientIdentity,
    MCPServer,
    OAuthResourceServer,
    RequestInfo,
    SSETransport,
    SubscriptionLimitError,
)
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
from easy_mcp.transport import _bus
from easy_mcp.transport.base import ClientContext

# Built, not written out, so secret scanners do not take the fixtures for credentials.
REPORTS_KEY = "lc-reports-key-" + "k" * 12
ADMIN_KEY = "lc-admin-key-" + "k" * 14
ALL_KEY = "lc-all-key-" + "k" * 16
STEP_UP_KEY = "lc-stepup-key-" + "k" * 16

ISSUER = "https://auth.example.com"
RESOURCE = "https://mcp.example.com/mcp"
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


@pytest.mark.parametrize("token_first", [True, False], ids=["token-first", "key-first"])
async def test_a_token_that_steps_up_and_a_key_with_its_scopes_are_judged_apart(
    fast_debounce: float, token_first: bool
) -> None:
    # With step-up a token sees every tool, while an API key holding the same
    # scopes still sees only what they cover: their lists never share a digest.
    oauth = OAuthResourceServer(RESOURCE, [ISSUER], step_up=True)
    server = make_server(oauth=oauth, auth=APIKeyAuth({STEP_UP_KEY: ["mcp:access"]}))
    token = ClientIdentity(
        fingerprint="token-principal",
        scopes=frozenset({"mcp:access"}),
        subject="user-1",
        client_id="client-1",
        issuer=ISSUER,
    )
    key = identity_of(server, STEP_UP_KEY)
    assert key is not None and key.scopes == token.scopes
    channels = {"token": Pushed(), "key": Pushed()}
    identities = {"token": token, "key": key}
    order = ["token", "key"] if token_first else ["key", "token"]
    # Held for the whole test: the notifier holds a session's context weakly.
    contexts = [await initialized(server, channels[name], identities[name], name) for name in order]
    register(server, "hidden", scopes=["admin"])
    assert await channels["token"].wait_for(1) == [TOOLS_CHANGED]
    await settle(fast_debounce)
    assert channels["token"].frames == [TOOLS_CHANGED]
    assert channels["key"].frames == []
    assert len(contexts) == 2


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
    await asyncio.sleep(0)  # the session ends on its own loop
    assert server._notifier.session("forgotten") is None
    register(server)
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_a_context_collected_while_a_change_is_announced_hangs_nothing() -> None:
    def change(server: MCPServer, allocations: int) -> None:
        # The collection falls this many allocations into the change, perhaps
        # while it holds the notifier's lock.
        gc.set_threshold(gc.get_count()[0] + allocations, *saved[1:])
        gc.enable()
        server._notifier.changed("tools")

    saved = gc.get_threshold()
    for allocations in range(1, 25):
        server = make_server()
        gc.collect()
        gc.disable()  # the context below stays in the youngest generation
        try:
            context = await initialized(server, Pushed(), session_id="forgotten")
            # A transport that never ends the session, and whose context is
            # left in a reference cycle: only a collection releases it.
            context.in_flight["self"] = context  # type: ignore[assignment]
            del context
            worker = threading.Thread(target=change, args=(server, allocations), daemon=True)
            worker.start()
            worker.join(5)
        finally:
            gc.set_threshold(*saved)
            gc.enable()
        assert not worker.is_alive(), f"a collection {allocations} allocations in hung"
        gc.collect()
        deadline = time.monotonic() + 5
        while server._notifier.session("forgotten") is not None:
            assert time.monotonic() < deadline, "the session was never ended"
            await asyncio.sleep(0.005)


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


async def test_a_change_that_could_not_be_sent_is_not_recorded_as_told() -> None:
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed)
    sink = server._notifier.session(context.session_id)
    assert sink is not None
    told = dict(sink.baselines)
    pushed.explode = True  # its channel has closed
    register(server)
    sink.deliver(["tools"])
    assert pushed.calls == 1
    # Nothing reached the client: what it was last told is what it was.
    assert sink.baselines == told
    assert server._notifier.session(context.session_id) is None


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
    # It sees another list now, so it is told once to list again ...
    assert await pushed.wait_for(1) == [TOOLS_CHANGED]
    await settle(fast_debounce)
    # ... and from then on hears of what the new credential sees,
    register(server, "admin_only", scopes=("admin",))
    assert await pushed.wait_for(2) == [TOOLS_CHANGED, TOOLS_CHANGED]
    # and nothing of what only the first one sees.
    register(server, "reports_only", scopes=("reports",))
    await settle(fast_debounce)
    assert pushed.frames == [TOOLS_CHANGED, TOOLS_CHANGED]


async def test_a_session_that_listed_with_a_broader_credential_hears_its_list_shrink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A window long enough for the removal below to fall inside it.
    monkeypatch.setattr(easy_mcp.subscriptions, "LIST_CHANGED_DEBOUNCE_SECONDS", 0.2)
    server = keyed_server()
    register(server, "secret", scopes=("admin",))
    pushed = Pushed()
    context = await initialized(server, pushed, identity_of(server, REPORTS_KEY))
    # Told the list held "add" only; then it lists with a credential that sees more.
    broad = dataclasses.replace(context, identity=identity_of(server, ADMIN_KEY))
    listed = await server.dispatch(rpc("tools/list", msg_id=2), broad)
    assert listed is not None
    assert [tool["name"] for tool in listed["result"]["tools"]] == ["add", "secret"]
    # The list it holds now loses a tool, though it is the one it was told about.
    server.unregister_tool("secret")
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


async def test_a_cancel_handled_as_a_flush_falls_due_stops_the_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    window = 0.05
    monkeypatch.setattr(easy_mcp.subscriptions, "LIST_CHANGED_DEBOUNCE_SECONDS", window)
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    task = await open_listen(server, context, pushed, "listen-1", toolsListChanged=True)
    register(server)
    await asyncio.sleep(0)
    await asyncio.sleep(0)  # the change's window is open: its flush is timed
    cancel = notification("notifications/cancelled", {"requestId": "listen-1"})

    def handle_cancel() -> None:
        # To the end without suspending, as a transport task's first step
        # handles a cancel (a task of its own would let the flush run first).
        coro = server.dispatch(cancel, context)
        with pytest.raises(StopIteration) as stopped:
            coro.send(None)
        assert stopped.value.value is None

    loop = asyncio.get_running_loop()
    loop.call_at(loop.time() + window / 5, handle_cancel)
    time.sleep(0.2)  # the cancel and the flush fall due in the same loop iteration
    await asyncio.sleep(0.1)
    assert await task is None
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


async def test_cancelling_listen_1_0_leaves_listen_1_open(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    # Two subscriptions, though 1 and 1.0 are one key to a dict.
    floating = await open_listen(server, context, pushed, 1.0, toolsListChanged=True)
    whole = await open_listen(server, context, pushed, 1, toolsListChanged=True)
    cancel = notification("notifications/cancelled", {"requestId": 1.0})
    assert await server.dispatch(cancel, context) is None
    assert await floating is None
    assert server._notifier.count() == 1 and not whole.done()
    register(server)
    frames = await pushed.wait_for(3)
    assert frames[2]["params"]["_meta"] == {TAG: 1}
    assert type(frames[2]["params"]["_meta"][TAG]) is int
    server.close_subscriptions(context)
    assert await whole is None


async def test_a_cancel_from_another_worker_ends_only_the_listen_it_names() -> None:
    hub = FakeHub()
    store = hub.store("0123456789abcdef")
    server = make_server(store=store)
    manager = SSETransport(server)._manager
    await store.start()
    session_id = "a-session-id-of-192-random-bits-"
    local = await manager.open(session_id, client_id="ip:x", identity=None, owned=True)
    assert local is not None and local.stream is not None
    # As the legacy SSE stream held here carries them.
    local.context.push = local.push
    local.context.multiplexed = True
    try:
        tasks = []
        for msg_id in (1, 1.0):
            tasks.append(asyncio.create_task(server.dispatch(listen(msg_id), local.context)))
            deadline = time.monotonic() + 5
            while server._notifier.count() < len(tasks):
                assert time.monotonic() < deadline, "no acknowledgment"
                await asyncio.sleep(0.005)
        whole, floating = tasks
        # notifications/cancelled for 1, sent to another worker of the session.
        manager._on_bus(
            _bus.seal("cancel", "sse", local.ref, None, session_id, "fedcba9876543210", rid=1)
        )
        assert await asyncio.wait_for(whole, 5) is None
        await asyncio.sleep(0.05)
        assert server._notifier.count() == 1 and not floating.done()
        server.close_subscriptions(local.context)
        assert await asyncio.wait_for(floating, 5) is None
    finally:
        await manager.shutdown()


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
        # Still accepted (within the clock-skew leeway) for 2 to 3 more seconds.
        expires_at=int(time.time()) - LEEWAY_SECONDS + 3,
    )
    context = make_context(token, push=pushed)
    task = await open_listen(server, context, pushed)
    await asyncio.sleep(1.0)
    assert not task.done()  # the stream lasts as long as its token is accepted
    assert await asyncio.wait_for(task, 5) is None
    assert pushed.frames[-1]["result"]["resultType"] == "complete"


async def overrule_listen(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
    """Request middleware that replaces a listen's answer once its stream is over."""
    outcome = await call_next()
    if request.method == "subscriptions/listen":
        raise ProtocolError("refused after the fact")
    return outcome


async def test_a_listen_the_server_ended_gets_no_second_answer(logs: LogCapture) -> None:
    server = make_server()
    server.middleware(overrule_listen)
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    task = await open_listen(server, context, pushed, 5, toolsListChanged=True)
    server.close_subscriptions(context, reason="shutdown")
    # Its completion result was its answer: dispatch adds none.
    assert await task is None
    assert pushed.methods() == [
        "notifications/subscriptions/acknowledged",
        None,
        "notifications/cancelled",
    ]
    assert pushed.frames[1]["id"] == 5
    assert pushed.frames[1]["result"]["resultType"] == "complete"
    assert "withheld" in logs.text
    # Nor does a listen its client cancelled get one.
    cancelled = await open_listen(server, context, pushed, 6, toolsListChanged=True)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 6}), context)
    assert await cancelled is None
    assert len(pushed.frames) == 4


async def test_a_listen_middleware_cuts_short_is_answered_with_its_error() -> None:
    server = make_server()

    @server.middleware
    async def bounded(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        async with asyncio.timeout(0.1):
            return await call_next()

    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    task = await open_listen(server, context, pushed, 5, toolsListChanged=True)
    response = await asyncio.wait_for(task, 5)
    # Acknowledged, then cut: the error is its one answer.
    assert response is not None and response["id"] == 5
    assert response["error"]["code"] == INTERNAL_ERROR
    assert pushed.methods() == ["notifications/subscriptions/acknowledged"]
    assert server._notifier.count() == 0


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


# ----------------------------------------------------------------- digests

# Keys of two types: JSON has string keys only, so a client reads both as strings.
WEIGHTS = {0: 0.5, "default": 1.0}
BONUS = {1: 2.0, "base": 1.0}


def mixed_keys_server(weights: dict[Any, float], bonus: dict[Any, float]) -> MCPServer:
    """A server whose tools/list entries hold dicts with keys of mixed types."""
    server = make_server()

    # Examples are sent as given.
    @server.tool(examples=[{"arguments": {"weights": weights}}])
    def weigh(weights: dict[str, float]) -> float:
        """Weigh things."""
        return sum(weights.values())

    # A default JSON can encode is advertised in inputSchema.
    def score(bonus: dict[str, float] = bonus) -> float:
        """Score with a bonus."""
        return sum(bonus.values())

    server.register_tool(score)
    return server


async def test_keys_of_mixed_types_are_digested_as_clients_read_them(
    fast_debounce: float,
) -> None:
    server = mixed_keys_server(WEIGHTS, BONUS)
    pushed = Pushed()
    context = await initialized(server, pushed)  # the handshake takes a digest
    listed = await server.dispatch(rpc("tools/list", msg_id=2), context)
    assert listed is not None
    assert {tool["name"] for tool in listed["result"]["tools"]} == {"add", "score", "weigh"}
    task = await open_listen(server, context, pushed, "listen-1", toolsListChanged=True)
    assert pushed.frames[0]["params"]["notifications"] == {"toolsListChanged": True}
    register(server)
    await pushed.wait_for(3)
    assert (
        sorted(frame["method"] for frame in pushed.frames[1:])
        == ["notifications/tools/list_changed"] * 2
    )
    server.close_subscriptions(context)
    assert await task is None
    # The same lists with string keys are the same lists to a client.
    as_read = mixed_keys_server(
        {str(key): value for key, value in WEIGHTS.items()},
        {str(key): value for key, value in BONUS.items()},
    )
    register(as_read)
    assert as_read._list_digest("tools", None) == server._list_digest("tools", None)


async def test_a_list_that_cannot_be_digested_leaves_handshake_and_listen_working(
    monkeypatch: pytest.MonkeyPatch, fast_debounce: float, logs: LogCapture
) -> None:
    def undigestible(self: MCPServer, kind: str, identity: ClientIdentity | None) -> str:
        raise TypeError("cannot digest this list")

    # Before the server exists, so its notifier digests with it too.
    monkeypatch.setattr(MCPServer, "_list_digest", undigestible)
    server = make_server()
    pushed = Pushed()
    context = await initialized(server, pushed)
    listed = await server.dispatch(rpc("tools/list", msg_id=2), context)
    assert listed is not None and [tool["name"] for tool in listed["result"]["tools"]] == ["add"]
    task = await open_listen(server, context, pushed, "listen-1", toolsListChanged=True)
    # The listen honors no list it cannot follow, and says so.
    assert pushed.frames[0]["params"]["notifications"] == {}
    register(server)
    await settle(fast_debounce)
    assert len(pushed.frames) == 1
    server.close_subscriptions(context)
    assert await task is None
    assert "could not compute the tools list" in logs.text
    assert "error_id=" in logs.text


def register_undigestible(server: MCPServer) -> None:
    """Register a tool whose tools/list entry JSON cannot encode (a tuple key)."""

    def locate(point: dict[str, str]) -> str:
        """Name a point."""
        return "here"

    server.register_tool(locate, examples=[{"arguments": {"point": {(1, 2): "a"}}}])


async def test_a_listen_whose_list_can_no_longer_be_digested_ends_gracefully(
    fast_debounce: float, logs: LogCapture
) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    task = await open_listen(server, context, pushed, 3, toolsListChanged=True)
    register_undigestible(server)
    assert await asyncio.wait_for(task, 5) is None
    # The server ended it: its completion result, then on this channel the cancel.
    ack, result, cancelled = pushed.frames
    assert result["id"] == 3 and result["result"]["resultType"] == "complete"
    assert cancelled["method"] == "notifications/cancelled"
    assert cancelled["params"]["requestId"] == 3
    assert cancelled["params"]["_meta"] == {TAG: 3}
    assert server._notifier.count() == 0
    assert [event["reason"] for event in logs.events("subscription_close")] == ["undeliverable"]
    assert "could not compute the tools list" in logs.text
