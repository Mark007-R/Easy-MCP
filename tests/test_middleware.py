"""Request and tool middleware, and the request path they plug into."""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import gc
import io
import json
import logging
import os
import re
import threading
import time
import warnings
import weakref
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from typing import Any

import httpx
import pytest
from conftest import (
    LogCapture,
    headers_for,
    make_context,
    meta,
    modern,
    notification,
    rpc,
)

from easy_mcp import (
    APIKeyAuth,
    AuthenticationError,
    AuthorizationError,
    ClientIdentity,
    MCPServer,
    ProtocolError,
    RateLimitError,
    RequestInfo,
    RequestOutcome,
    SessionLimitError,
    StdioTransport,
    ToolCall,
    ToolError,
    ToolOutcome,
    Transport,
    TransportInfo,
    __version__,
    current_cancel_token,
    current_tool_call,
)
from easy_mcp.exceptions import (
    AUTHENTICATION_REQUIRED,
    FORBIDDEN,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    RATE_LIMITED,
    SERVER_BUSY,
    SESSION_LIMIT_EXCEEDED,
    TOOL_TIMEOUT,
)
from easy_mcp.middleware import RequestNext, ToolNext, describe
from easy_mcp.transport.base import ClientContext

LiveServer = Callable[[Any], str]

KEY = "middleware-test-key-" + "k" * 12
SERVER_INFO = "io.modelcontextprotocol/serverInfo"
VERSION_KEY = "io.modelcontextprotocol/protocolVersion"

REQUEST_VAR: ContextVar[str | None] = ContextVar("request_var", default=None)
TOOL_VAR: ContextVar[str | None] = ContextVar("tool_var", default=None)


def make_server(**kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def with_slow_tool(server: MCPServer) -> tuple[asyncio.Event, asyncio.Event]:
    """Register ``slow``, an async tool that waits until it is cancelled."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @server.tool
    async def slow() -> str:
        """Waits until cancelled."""
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return "finished"

    return started, cancelled


# ------------------------------------------------------- the request path


async def test_initialize_records_the_negotiated_version() -> None:
    server = make_server()
    context = make_context()
    assert context.protocol_version is None
    await server.dispatch(rpc("initialize", {"protocolVersion": "2025-03-26"}), context)
    assert context.protocol_version == "2025-03-26"
    # An unknown version is answered, and recorded, as the newest legacy one.
    await server.dispatch(rpc("initialize", {"protocolVersion": "1999-01-01"}, 2), context)
    assert context.protocol_version == "2025-11-25"


async def test_a_cancelled_caller_sees_its_cancellation() -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}), make_context()))
    await asyncio.wait_for(started.wait(), 5)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert call.cancelled()
    await asyncio.wait_for(cancelled.wait(), 5)  # the call did not outlive its caller


async def test_a_timeout_around_dispatch_raises_timeout_error() -> None:
    server = make_server()
    _, cancelled = with_slow_tool(server)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.1):
            await server.dispatch(rpc("tools/call", {"name": "slow"}), make_context())
    await asyncio.wait_for(cancelled.wait(), 5)


async def test_a_client_cancel_still_drops_the_response(logs: LogCapture) -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 7), context))
    await asyncio.wait_for(started.wait(), 5)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 7}), context)
    assert await asyncio.wait_for(call, 5) is None  # no response, and no exception
    assert not call.cancelled()
    assert cancelled.is_set()
    assert logs.events("tool_cancelled") == [
        {"type": "tool_cancelled", "client_id": "ip:test", "request_id": 7}
    ]
    assert context.in_flight == {}


async def test_methods_outside_the_table_are_answered_as_before() -> None:
    server = make_server()
    context = make_context()
    unknown = await server.dispatch(rpc("resources/list"), context)
    assert unknown is not None and unknown["error"]["code"] == METHOD_NOT_FOUND
    for method in ("ping", "initialize", "notifications/cancelled"):
        response = await server.dispatch(modern(method), context)
        assert response is not None and response["error"]["code"] == METHOD_NOT_FOUND
    # server/discover is always stateless, so without _meta it is malformed.
    discover = await server.dispatch(rpc("server/discover"), context)
    assert discover is not None and discover["error"]["code"] == INVALID_PARAMS
    assert await server.dispatch(notification("notifications/unknown"), context) is None
    # A message with an id is a request, whatever its method is called.
    named = await server.dispatch(rpc("notifications/unknown", msg_id=3), context)
    assert named is not None and named["error"]["code"] == METHOD_NOT_FOUND


async def test_a_request_naming_a_notification_is_an_unknown_method() -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 7), context))
    await asyncio.wait_for(started.wait(), 5)
    for method in ("notifications/cancelled", "notifications/initialized"):
        named = rpc(method, {"requestId": 7}, msg_id=8)
        response = await server.dispatch(named, context)
        assert response is not None and response["error"]["code"] == METHOD_NOT_FOUND, method
    await asyncio.sleep(0.05)
    assert not call.done() and not cancelled.is_set()  # it cancelled nothing
    await server.dispatch(notification("notifications/cancelled", {"requestId": 7}), context)
    assert await asyncio.wait_for(call, 5) is None
    assert cancelled.is_set()


async def test_a_refused_handshake_negotiates_no_version() -> None:
    for refusal in (AuthenticationError("handshake refused"), RuntimeError("broken")):
        server = make_server()
        seen: list[str | None] = []

        @server.middleware
        async def gate(
            request: RequestInfo,
            call_next: RequestNext,
            refusal: Exception = refusal,
            seen: list[str | None] = seen,
        ) -> RequestOutcome:
            outcome = await call_next()
            if request.method == "initialize":
                raise refusal  # after the handshake was served
            seen.append(request.protocol_version)
            return outcome

        context = make_context()
        handshake = rpc("initialize", {"protocolVersion": "2025-03-26"})
        refused = await server.dispatch(handshake, context)
        assert refused is not None and "error" in refused, refusal
        assert context.protocol_version is None
        listed = await server.dispatch(rpc("tools/list", msg_id=2), context)
        assert listed is not None and "result" in listed
        assert seen == [None]


async def test_request_ids_that_cannot_be_keys_are_still_served() -> None:
    server = make_server()
    context = make_context()
    ping = await server.dispatch(rpc("ping", msg_id=[1]), context)
    assert ping == {"jsonrpc": "2.0", "id": [1], "result": {}}
    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, msg_id={"n": 1})
    called = await server.dispatch(call, context)
    assert called is not None and called["result"]["content"][0]["text"] == "3"
    ignored = notification("notifications/cancelled", {"requestId": [1]})
    assert await server.dispatch(ignored, context) is None
    assert context.in_flight == {}


# ------------------------------------------------------------ error codes


async def test_reserved_codes_never_reach_the_wire(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    server = make_server()

    def refuse(context: Any) -> dict[str, Any]:
        raise ProtocolError("made-up code", code=-32050)

    monkeypatch.setattr(server, "_handle_tools_list", refuse)
    response = await server.dispatch(rpc("tools/list"), make_context())
    assert response is not None
    assert response["error"]["code"] == INTERNAL_ERROR
    assert "error_id=" in response["error"]["message"]
    assert "made-up" not in response["error"]["message"]
    assert "-32050" in logs.text


async def test_the_spec_defined_reserved_codes_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    server = make_server()
    for code in (-32020, -32021, -32022, -32001):

        def refuse(context: Any, code: int = code) -> dict[str, Any]:
            raise ProtocolError("defined", code=code)

        monkeypatch.setattr(server, "_handle_tools_list", refuse)
        response = await server.dispatch(rpc("tools/list"), make_context())
        assert response is not None and response["error"]["code"] == code


async def test_forbidden_is_never_sent_statelessly(monkeypatch: pytest.MonkeyPatch) -> None:
    server = make_server()

    def refuse(*args: Any) -> dict[str, Any]:
        raise AuthorizationError("no")

    monkeypatch.setattr(server, "_dispatch_modern", refuse)
    monkeypatch.setattr(server, "_handle_tools_list", refuse)
    stateless = await server.dispatch(modern("tools/list"), make_context())
    assert stateless is not None and stateless["error"]["code"] == AUTHENTICATION_REQUIRED
    legacy = await server.dispatch(rpc("tools/list"), make_context())
    assert legacy is not None and legacy["error"]["code"] == FORBIDDEN


async def test_outcomes_carry_the_code_a_stateless_client_gets() -> None:
    server = make_server()
    seen: list[tuple[Any, ...]] = []

    @server.middleware
    async def watch(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        outcome = await call_next()
        seen.append((request.method, outcome.error_code, outcome.error_type))
        return outcome

    @server.middleware
    async def deny_list(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/list":
            raise AuthorizationError("not for you")
        return await call_next()

    @server.tool_middleware
    async def watch_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        seen.append(("tool", outcome.error_code, outcome.status))
        return outcome

    @server.tool_middleware
    async def deny(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        raise AuthorizationError("not for you")

    arguments = {"name": "add", "arguments": {"a": 1, "b": 1}}
    for message in (modern("tools/call", arguments), modern("tools/list", msg_id=2)):
        response = await server.dispatch(message, make_context())
        assert response is not None and response["error"]["code"] == AUTHENTICATION_REQUIRED
    assert seen == [
        ("tool", AUTHENTICATION_REQUIRED, "refused"),
        ("tools/call", AUTHENTICATION_REQUIRED, str(AUTHENTICATION_REQUIRED)),
        ("tools/list", AUTHENTICATION_REQUIRED, str(AUTHENTICATION_REQUIRED)),
    ]
    seen.clear()
    # Older revisions still get -32002, and so does what middleware sees.
    legacy = await server.dispatch(rpc("tools/call", arguments, 3), make_context())
    assert legacy is not None and legacy["error"]["code"] == FORBIDDEN
    assert seen == [("tool", FORBIDDEN, "refused"), ("tools/call", FORBIDDEN, str(FORBIDDEN))]


def test_the_origin_refusal_speaks_the_requests_era(live_server: LiveServer) -> None:
    base = live_server(make_server())
    evil = {"Origin": "http://evil.example:8000"}
    with httpx.Client(base_url=base, timeout=10) as client:
        call = modern("tools/list")
        rejected = client.post("/mcp", json=call, headers={**headers_for(call), **evil})
        assert rejected.status_code == 403
        assert rejected.json()["error"]["code"] == INVALID_REQUEST
        legacy = client.post(
            "/mcp",
            json=rpc("initialize", {"protocolVersion": "2025-11-25"}),
            headers={"Accept": "application/json, text/event-stream", **evil},
        )
        assert legacy.status_code == 403
        assert legacy.json()["error"]["code"] == FORBIDDEN


# ------------------------------------------------------------- registration


async def passthrough(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
    """A request middleware that only watches."""
    return await call_next()


async def tool_passthrough(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
    """A tool middleware that only watches."""
    return await call_next()


def test_middleware_must_be_async() -> None:
    server = make_server()

    def sync_function(request: Any, call_next: Any) -> Any:
        return call_next()

    class SyncCallable:
        def __call__(self, request: Any, call_next: Any) -> Any:
            return call_next()

    class AsyncCallable:
        async def __call__(self, request: Any, call_next: Any) -> Any:
            return await call_next()

    class SyncOverride(AsyncCallable):
        def __call__(self, request: Any, call_next: Any) -> Any:
            return None  # the __call__ that runs is not async

    bad_ones = (
        sync_function,
        lambda request, call_next: call_next(),
        SyncCallable(),
        SyncOverride(),
    )
    for bad in bad_ones:
        with pytest.raises(TypeError, match="must be an async function"):
            server.middleware(bad)
        with pytest.raises(TypeError, match="must be an async function"):
            server.tool_middleware(bad)

    class InheritsAsync(AsyncCallable):
        """Its async __call__ comes from the base class."""

    async def labelled(label: str, request: Any, call_next: Any) -> Any:
        return await call_next()

    server.middleware(AsyncCallable())
    server.middleware(InheritsAsync())
    server.tool_middleware(functools.partial(labelled, "audit"))


def test_middleware_must_take_request_and_call_next() -> None:
    server = make_server()

    async def one(request: Any) -> Any:
        return None

    async def three(request: Any, call_next: Any, extra: Any) -> Any:
        return None

    for bad in (one, three):
        with pytest.raises(TypeError, match=r"\(request, call_next\)"):
            server.middleware(bad)
        with pytest.raises(TypeError, match=r"\(request, call_next\)"):
            server.tool_middleware(bad)

    async def defaulted(request: Any, call_next: Any, extra: Any = None) -> Any:
        return await call_next()

    async def variadic(*args: Any) -> Any:
        return await args[1]()

    server.middleware(defaulted)
    server.tool_middleware(variadic)


def test_the_same_middleware_cannot_be_registered_twice() -> None:
    server = make_server()
    server.middleware(passthrough)
    with pytest.raises(ValueError, match="registered already"):
        server.middleware(passthrough)
    server.tool_middleware(passthrough)  # the other level is a different list
    with pytest.raises(ValueError, match="registered already"):
        server.tool_middleware(passthrough)


def test_decorators_return_the_function_unchanged() -> None:
    server = make_server()
    assert server.middleware(passthrough) is passthrough
    assert server.tool_middleware(tool_passthrough) is tool_passthrough

    @server.middleware
    async def decorated(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        return await call_next()

    assert decorated.__name__ == "decorated"
    with pytest.raises(TypeError):
        server.middleware()  # type: ignore[call-arg]


async def test_middleware_registered_at_runtime_applies_to_later_requests() -> None:
    server = make_server()
    seen: list[str] = []
    entered = asyncio.Event()
    gate = asyncio.Event()

    @server.middleware
    async def first(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        seen.append(f"first:{request.request_id}")
        if request.request_id == 1:
            entered.set()
            await gate.wait()
        return await call_next()

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, 1)
    held = asyncio.create_task(server.dispatch(call, make_context()))
    await asyncio.wait_for(entered.wait(), 5)

    @server.middleware
    async def second(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        seen.append(f"second:{request.request_id}")
        return await call_next()

    @server.tool_middleware
    async def third(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        seen.append(f"third:{call.request.request_id}")
        return await call_next()

    gate.set()
    assert (await held)["result"]["content"][0]["text"] == "2"
    await server.dispatch({**call, "id": 2}, make_context())
    assert seen == ["first:1", "first:2", "second:2", "third:2"]


def test_startup_log_lists_middleware(logs: LogCapture) -> None:
    class Stub(Transport):
        def run(self) -> None:
            return None

        def stop(self) -> None:
            return None

    server = make_server()
    server.middleware(passthrough)
    server.tool_middleware(tool_passthrough)
    server.run(Stub(server))
    (startup,) = [
        record.event  # type: ignore[attr-defined]
        for record in logs.records
        if getattr(record, "event", {}).get("type") == "startup"
    ]
    assert startup["middleware"] == [describe(passthrough)]
    assert startup["tool_middleware"] == [describe(tool_passthrough)]
    assert describe(passthrough).endswith("test_middleware.passthrough")


def test_describe_names_what_was_registered() -> None:
    class Guard:
        async def __call__(self, request: Any, call_next: Any) -> Any:
            return await call_next()

    assert describe(passthrough).endswith(".passthrough")
    assert describe(functools.partial(passthrough)).endswith(".passthrough")
    assert describe(Guard()).endswith("test_describe_names_what_was_registered.<locals>.Guard")
    long = type("X" * 300, (), {"__call__": passthrough})()
    assert len(describe(long)) == 200


# --------------------------------------------------------- order and shape


async def test_first_registered_is_outermost() -> None:
    server = make_server()
    trace: list[str] = []

    def layer(name: str) -> Any:
        async def middleware(info: Any, call_next: Any) -> Any:
            trace.append(f"{name}>")
            outcome = await call_next()
            trace.append(f"<{name}")
            return outcome

        return middleware

    server.middleware(layer("r1"))
    server.middleware(layer("r2"))
    server.tool_middleware(layer("t1"))
    server.tool_middleware(layer("t2"))

    @server.tool
    def traced() -> str:
        """Leaves a mark."""
        trace.append("tool")
        return "ok"

    await server.dispatch(rpc("tools/call", {"name": "traced"}), make_context())
    assert trace == ["r1>", "r2>", "t1>", "t2>", "tool", "<t2", "<t1", "<r2", "<r1"]


async def test_request_middleware_sees_every_implemented_method() -> None:
    server = make_server()
    seen: list[tuple[str, bool, bool]] = []

    @server.middleware
    async def watch(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        seen.append((request.method, request.is_notification, request.stateless))
        return await call_next()

    context = make_context()
    add = {"name": "add", "arguments": {"a": 1, "b": 2}}
    await server.dispatch(rpc("initialize", {"protocolVersion": "2025-11-25"}), context)
    await server.dispatch(notification("notifications/initialized"), context)
    await server.dispatch(rpc("ping", msg_id=2), context)
    await server.dispatch(rpc("tools/list", msg_id=3), context)
    await server.dispatch(rpc("tools/call", add, 4), context)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 99}), context)
    await server.dispatch(modern("server/discover"), make_context())
    await server.dispatch(modern("tools/list"), make_context())
    await server.dispatch(modern("tools/call", add), make_context())
    assert seen == [
        ("initialize", False, False),
        ("notifications/initialized", True, False),
        ("ping", False, False),
        ("tools/list", False, False),
        ("tools/call", False, False),
        ("notifications/cancelled", True, False),
        ("server/discover", False, True),
        ("tools/list", False, True),
        ("tools/call", False, True),
    ]


async def test_unknown_methods_never_reach_middleware() -> None:
    server = make_server()
    seen: list[str] = []

    @server.middleware
    async def watch(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        seen.append(request.method)
        return await call_next()

    context = make_context()
    legacy = await server.dispatch(rpc("resources/list"), context)
    assert legacy is not None and legacy["error"]["code"] == METHOD_NOT_FOUND
    stateless = await server.dispatch(modern("ping"), context)
    assert stateless is not None and stateless["error"]["code"] == METHOD_NOT_FOUND
    assert await server.dispatch(notification("notifications/whatever"), context) is None
    assert await server.dispatch(notification("tools/call", {"name": "add"}), context) is None
    no_version = {"_meta": meta(**{VERSION_KEY: None})}
    malformed = await server.dispatch(rpc("tools/list", no_version), context)
    assert malformed is not None and malformed["error"]["code"] == INVALID_PARAMS
    assert seen == []


async def wire_script(logs: LogCapture, *, observed: bool) -> tuple[list[Any], list[Any]]:
    """Every kind of answer a client can get, as responses and audit events."""
    server = MCPServer(
        port=0, rate_limit_per_minute=None, max_sync_workers=1, auth=APIKeyAuth({KEY: ["read"]})
    )
    release = threading.Event()
    holding = threading.Event()

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @server.tool
    def polite() -> str:
        """Fails politely."""
        raise ToolError("upstream unavailable")

    @server.tool
    def boom() -> str:
        """Fails."""
        raise RuntimeError("secret detail")

    @server.tool
    def mislabeled() -> dict[str, int]:
        """Breaks its own schema."""
        return [1]  # type: ignore[return-value]

    @server.tool(timeout=0.05)
    async def sleepy() -> str:
        """Too slow."""
        await asyncio.sleep(1)
        return "late"

    @server.tool
    def hold() -> str:
        """Holds the only sync worker."""
        holding.set()
        release.wait(5)
        return "held"

    @server.tool(requires_auth=True, scopes=["admin"])
    def admin() -> str:
        """For admins."""
        return "admin"

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Once per session."""
        return "once"

    if observed:

        @server.middleware
        async def observe(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
            read = (request.params, request.meta, request.transport, request.tool)
            outcome = await call_next()
            read += (outcome.failed, outcome.error_type, outcome.tool, read)
            return outcome

        @server.tool_middleware
        async def observe_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
            read = (call.arguments, call.state, call.timeout)
            outcome = await call_next()
            read += (outcome.status, outcome.message, outcome.duration_ms)
            return outcome

    identity = ClientIdentity(fingerprint="f" * 12, scopes=frozenset({"read"}))
    context = make_context(identity=identity, client_id="f" * 12)
    first = len(logs.records)
    responses: list[Any] = []

    def call(name: str, msg_id: int, **arguments: Any) -> dict[str, Any]:
        return rpc("tools/call", {"name": name, "arguments": arguments}, msg_id)

    for msg_id, name in enumerate(("add", "polite", "boom", "mislabeled", "sleepy"), 1):
        arguments = {"a": 1, "b": 2} if name == "add" else {}
        responses.append(await server.dispatch(call(name, msg_id, **arguments), context))
    held = asyncio.create_task(server.dispatch(call("hold", 10), context))
    assert await asyncio.to_thread(holding.wait, 5)
    responses.append(await server.dispatch(call("add", 11, a=1, b=1), context))  # busy
    release.set()
    responses.append(await held)
    responses.append(await server.dispatch(call("nope", 12), context))
    responses.append(await server.dispatch(call("admin", 13), context))
    responses.append(await server.dispatch(call("add", 14, a="x", b=1), context))
    responses.append(await server.dispatch(call("once", 15), context))
    responses.append(await server.dispatch(call("once", 16), context))
    responses.append(await server.dispatch(rpc("tools/list", msg_id=17), context))
    responses.append(await server.dispatch(rpc("ping", msg_id=18), context))
    responses.append(await server.dispatch(modern("server/discover", msg_id=19), context))
    listed = await server.dispatch(modern("tools/list", msg_id=20), context)
    assert listed is not None
    listed["result"].pop("cacheScope")  # the one intended difference
    responses.append(listed)
    stateless_call = modern("tools/call", {"name": "add", "arguments": {"a": 2, "b": 2}}, 21)
    responses.append(await server.dispatch(stateless_call, context))
    events = [
        {key: value for key, value in record.event.items() if key != "duration_ms"}  # type: ignore[attr-defined]
        for record in logs.records[first:]
        if record.name == "easy_mcp.audit"
    ]
    normalized = json.loads(re.sub(r"[0-9a-f]{12}(?=\))", "<id>", json.dumps(responses)))
    for event in events:
        if "error_id" in event:
            event["error_id"] = "<id>"
    return normalized, events


async def test_an_observing_middleware_changes_nothing_on_the_wire(logs: LogCapture) -> None:
    plain_responses, plain_events = await wire_script(logs, observed=False)
    observed_responses, observed_events = await wire_script(logs, observed=True)
    assert observed_responses == plain_responses
    assert observed_events == plain_events
    codes = [r.get("error", {}).get("code") for r in plain_responses]
    assert codes == [
        None, None, None, None, -32005, -32008, None, -32602, -32602, -32602,
        None, -32006, None, None, None, None, None,
    ]  # fmt: skip


# ----------------------------------------------------------------- outcomes


async def test_request_outcome_describes_results_and_errors() -> None:
    server = make_server()
    outcomes: list[RequestOutcome] = []

    @server.tool
    def polite() -> str:
        """Fails politely."""
        raise ToolError("not now")

    @server.middleware
    async def keep(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        outcome = await call_next()
        outcomes.append(outcome)
        return outcome

    context = make_context()
    await server.dispatch(rpc("tools/list"), context)
    await server.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}), context
    )
    await server.dispatch(rpc("tools/call", {"name": "missing"}), context)
    await server.dispatch(rpc("tools/call", {"name": "polite"}), context)
    listed, added, missing, polite_outcome = outcomes

    assert (listed.error_code, listed.message, listed.failed, listed.error_type) == (
        None,
        None,
        False,
        None,
    )
    assert listed.tool is None
    assert added.tool is not None and added.tool.status == "ok" and not added.failed
    assert missing.error_code == INVALID_PARAMS and missing.message == "Unknown tool: missing"
    assert missing.failed and missing.error_type == "-32602" and missing.tool is None
    assert polite_outcome.error_code is None and polite_outcome.failed
    assert polite_outcome.error_type == "tool_error"
    assert polite_outcome.tool is not None and polite_outcome.tool.status == "tool_error"


async def test_tool_outcome_statuses() -> None:
    server = make_server(max_sync_workers=1)
    release = threading.Event()
    holding = threading.Event()
    outcomes: dict[Any, ToolOutcome] = {}

    @server.tool
    def polite() -> str:
        """Fails politely."""
        raise ToolError("not now")

    @server.tool
    def boom() -> str:
        """Fails."""
        raise RuntimeError("secret")

    @server.tool
    def mislabeled() -> dict[str, int]:
        """Breaks its own schema."""
        return [1]  # type: ignore[return-value]

    @server.tool(timeout=0.05)
    async def sleepy() -> str:
        """Too slow."""
        await asyncio.sleep(1)
        return "late"

    @server.tool
    def hold() -> str:
        """Holds the only sync worker."""
        holding.set()
        release.wait(5)
        return "held"

    @server.tool_middleware
    async def keep(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        outcomes[call.request.request_id] = outcome
        return outcome

    context = make_context()
    for msg_id, name in enumerate(("add", "polite", "boom", "mislabeled", "sleepy")):
        arguments = {"a": 1, "b": 2} if name == "add" else {}
        await server.dispatch(
            rpc("tools/call", {"name": name, "arguments": arguments}, msg_id), context
        )
    held = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "hold"}, 10), context))
    assert await asyncio.to_thread(holding.wait, 5)
    busy = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, 11)
    await server.dispatch(busy, context)
    release.set()
    await held

    ok, tool_error, error, schema, timeout = (outcomes[n] for n in range(5))
    assert (ok.status, ok.ok, ok.is_error, ok.error_code, ok.started) == (
        "ok",
        True,
        False,
        None,
        True,
    )
    assert ok.message == "3" and ok.error_id is None and ok.exception_type is None
    assert isinstance(ok.duration_ms, float)

    assert (tool_error.status, tool_error.is_error, tool_error.error_code) == (
        "tool_error",
        True,
        None,
    )
    assert tool_error.message == "not now" and tool_error.started
    assert tool_error.exception_type == "easy_mcp.exceptions.ToolError"
    assert tool_error.error_id is None

    assert (error.status, error.is_error, error.started) == ("error", True, True)
    assert error.exception_type == "RuntimeError"
    assert error.error_id is not None and f"error_id={error.error_id}" in error.message

    assert (schema.status, schema.is_error, schema.started) == ("output_schema_error", True, True)
    assert schema.error_id is not None and schema.error_id in schema.message
    assert schema.exception_type is None

    assert (timeout.status, timeout.error_code, timeout.is_error) == (
        "timeout",
        TOOL_TIMEOUT,
        False,
    )
    assert timeout.started and "timed out" in timeout.message and not timeout.ok

    busy_outcome = outcomes[11]
    assert (busy_outcome.status, busy_outcome.error_code) == ("busy", SERVER_BUSY)
    assert busy_outcome.started is False and busy_outcome.duration_ms is None
    assert outcomes[10].ok


async def test_tool_outcome_never_carries_exception_text() -> None:
    server = make_server()
    kept: list[ToolOutcome] = []

    @server.tool
    def boom() -> str:
        """Fails."""
        raise RuntimeError("secret")

    @server.tool_middleware
    async def keep(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        kept.append(await call_next())
        return kept[0]

    await server.dispatch(rpc("tools/call", {"name": "boom"}), make_context())
    (outcome,) = kept
    assert "secret" not in outcome.message
    assert outcome.exception_type == "RuntimeError"
    for name in ToolOutcome.__slots__:
        assert not isinstance(getattr(outcome, name), BaseException), name
    assert "secret" not in repr(outcome)


async def test_timeout_reaches_middleware_as_an_outcome() -> None:
    server = make_server()
    seen: list[tuple[Any, ...]] = []

    @server.tool(timeout=0.05)
    async def sleepy() -> str:
        """Too slow."""
        await asyncio.sleep(1)
        return "late"

    @server.tool_middleware
    async def watch(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()  # no try/except needed
        seen.append((outcome.status, outcome.error_code, call.cancel_token.reason))
        return outcome

    response = await server.dispatch(rpc("tools/call", {"name": "sleepy"}), make_context())
    assert response is not None and response["error"]["code"] == TOOL_TIMEOUT
    assert seen == [("timeout", TOOL_TIMEOUT, "timeout")]


# ----------------------------------------------------------------- refusals


async def test_request_middleware_refusal_returns_its_protocol_error(logs: LogCapture) -> None:
    server = make_server()
    ran: list[bool] = []

    @server.tool
    def touch() -> str:
        """Records that it ran."""
        ran.append(True)
        return "touched"

    @server.middleware
    async def limit(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/call":
            raise RateLimitError(retry_after_seconds=12.5)
        return await call_next()

    response = await server.dispatch(rpc("tools/call", {"name": "touch"}), make_context())
    assert response is not None
    assert response["error"] == {
        "code": RATE_LIMITED,
        "message": "Rate limit exceeded; retry in 12.5s",
        "data": {"retry_after_seconds": 12.5},
    }
    assert ran == []
    assert logs.events("request_denied") == [
        {
            "type": "request_denied",
            "method": "tools/call",
            "client_id": "ip:test",
            "reason": "RateLimitError",
            "middleware": describe(limit),
            "tool": "touch",
        }
    ]
    assert logs.events("tool_denied") == [] and logs.events("middleware_failed") == []
    listed = await server.dispatch(rpc("tools/list"), make_context())
    assert listed is not None and "result" in listed


async def test_tool_middleware_protocol_refusal(logs: LogCapture) -> None:
    server = make_server()
    seen: list[ToolOutcome] = []

    @server.tool_middleware
    async def outer(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        seen.append(outcome)
        return outcome

    @server.tool_middleware
    async def deny(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        raise AuthenticationError("No key for this tenant")

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    response = await server.dispatch(call, make_context())
    assert response is not None
    assert response["error"] == {
        "code": AUTHENTICATION_REQUIRED,
        "message": "No key for this tenant",
    }
    (outcome,) = seen
    assert (outcome.status, outcome.started, outcome.error_code) == ("refused", False, -32001)
    assert logs.events("tool_denied") == [
        {
            "type": "tool_denied",
            "tool": "add",
            "client_id": "ip:test",
            "reason": "AuthenticationError",
            "middleware": describe(deny),
        }
    ]
    assert logs.events("tool_call") == []  # the tool never ran


async def test_tool_middleware_refusal_keeps_its_error_data() -> None:
    server = make_server()
    seen: list[Any] = []

    @server.middleware
    async def request_view(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        outcome = await call_next()
        seen.append(outcome)
        return outcome

    @server.tool_middleware
    async def tool_view(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        seen.append(outcome)
        return outcome

    @server.tool_middleware
    async def daily_quota(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        raise RateLimitError(retry_after_seconds=12.5)  # the quota example of the docs

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    response = await server.dispatch(call, make_context())
    assert response is not None
    assert response["error"] == {
        "code": RATE_LIMITED,
        "message": "Rate limit exceeded; retry in 12.5s",
        "data": {"retry_after_seconds": 12.5},
    }
    tool_outcome, request_outcome = seen
    assert (tool_outcome.status, tool_outcome.started) == ("refused", False)
    assert tool_outcome.error_code == RATE_LIMITED
    assert request_outcome.tool is tool_outcome and request_outcome.error_code == RATE_LIMITED


async def test_tool_middleware_tool_error_is_an_is_error_result() -> None:
    server = make_server()

    @server.tool_middleware
    async def tenant_guard(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        raise ToolError("This key cannot read tenant 'acme'.")

    arguments = {"name": "add", "arguments": {"a": 1, "b": 1}}
    legacy = await server.dispatch(rpc("tools/call", arguments), make_context())
    assert legacy is not None
    assert legacy["result"] == {
        "content": [{"type": "text", "text": "This key cannot read tenant 'acme'."}],
        "isError": True,
    }
    stateless = await server.dispatch(modern("tools/call", arguments), make_context())
    assert stateless is not None
    result = stateless["result"]
    assert result["isError"] is True
    assert result["content"][0]["text"] == "This key cannot read tenant 'acme'."
    assert result["resultType"] == "complete"
    assert result["_meta"][SERVER_INFO] == {"name": "easy-mcp", "version": __version__}


async def test_request_middleware_tool_error_on_tools_call_and_elsewhere(
    logs: LogCapture,
) -> None:
    server = make_server()

    @server.middleware
    async def deny(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        raise ToolError("Not during maintenance.")

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    called = await server.dispatch(call, make_context())
    assert called is not None
    assert called["result"]["isError"] is True
    assert called["result"]["content"][0]["text"] == "Not during maintenance."
    listed = await server.dispatch(rpc("tools/list"), make_context())
    assert listed is not None
    # No result can carry it outside tools/call: -32603, with the message as written.
    assert listed["error"] == {"code": INTERNAL_ERROR, "message": "Not during maintenance."}
    assert [event["method"] for event in logs.events("request_denied")] == [
        "tools/call",
        "tools/list",
    ]
    assert {event["reason"] for event in logs.events("request_denied")} == {"ToolError"}
    assert logs.events("middleware_failed") == []


async def test_a_refused_call_does_not_count_against_the_session_cap() -> None:
    server = make_server()
    refuse = True

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Once per session."""
        return "done"

    @server.tool_middleware
    async def gate(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        if refuse:
            raise ToolError("Not yet.")
        return await call_next()

    context = make_context()
    for msg_id in (1, 2):
        refused = await server.dispatch(rpc("tools/call", {"name": "once"}, msg_id), context)
        assert refused is not None and refused["result"]["content"][0]["text"] == "Not yet."
    assert context.tool_calls == {"once": 0}
    refuse = False
    allowed = await server.dispatch(rpc("tools/call", {"name": "once"}, 3), context)
    assert allowed is not None and allowed["result"]["content"][0]["text"] == "done"
    capped = await server.dispatch(rpc("tools/call", {"name": "once"}, 4), context)
    assert capped is not None and capped["error"]["code"] == SESSION_LIMIT_EXCEEDED


async def test_refusal_after_the_tool_ran_is_withheld(logs: LogCapture) -> None:
    server = make_server()
    writes: list[str] = []

    @server.tool
    def write() -> str:
        """Writes, then answers with something sensitive."""
        writes.append("written")
        return "token=secret"

    @server.tool_middleware
    async def scrub(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        if outcome.ok and "secret" in outcome.message:
            raise ToolError("The result was withheld.")
        return outcome

    @server.middleware
    async def deny_after(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        outcome = await call_next()
        if request.request_id == 2:
            raise AuthenticationError("Too late.")
        return outcome

    context = make_context()
    response = await server.dispatch(rpc("tools/call", {"name": "write"}), context)
    assert response is not None
    assert response["result"]["content"][0]["text"] == "The result was withheld."
    assert writes == ["written"]  # the side effect stands
    assert context.tool_calls == {"write": 1}
    assert logs.events("tool_result_withheld") == [
        {
            "type": "tool_result_withheld",
            "tool": "write",
            "client_id": "ip:test",
            "middleware": describe(scrub),
            "status": "ok",
            "reason": "ToolError",
        }
    ]
    assert logs.events("tool_denied") == []

    late = await server.dispatch(rpc("tools/call", {"name": "write"}, 2), context)
    assert late is not None and late["error"]["code"] == AUTHENTICATION_REQUIRED
    withheld = logs.events("tool_result_withheld")[-1]
    assert withheld["middleware"] == describe(deny_after)
    assert withheld["status"] == "tool_error"  # what the request outcome held
    assert logs.events("request_denied") == []


def writing_server(kind: str) -> tuple[MCPServer, list[str], threading.Event]:
    """A server whose tool ``write`` writes at once, then takes 0.6 s to answer."""
    server = make_server()
    writes: list[str] = []
    started = threading.Event()

    async def write_async() -> str:
        writes.append("written")
        started.set()
        await asyncio.sleep(0.6)
        return "done"

    def write_sync() -> str:
        writes.append("written")
        started.set()
        time.sleep(0.6)
        return "done"

    tool = write_async if kind == "async" else write_sync
    server.register_tool(tool, name="write", description="Writes, then takes its time.")
    return server, writes, started


@pytest.mark.parametrize("ending", ["refuse", "fail"])
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("level", ["request", "tool"])
async def test_a_tool_its_middleware_stopped_is_audited_as_run(
    logs: LogCapture, level: str, kind: str, ending: str
) -> None:
    server, writes, _ = writing_server(kind)
    seen: list[Any] = []

    async def observe(info: Any, call_next: Any) -> Any:
        outcome = await call_next()
        seen.append(outcome)
        return outcome

    async def bounded(info: Any, call_next: Any) -> Any:
        # What the docs ask of a middleware that awaits: bound it.
        try:
            async with asyncio.timeout(0.2):
                return await call_next()
        except TimeoutError:
            if ending == "refuse":
                raise ToolError("Took too long.") from None
            raise

    if level == "request":
        server.middleware(observe)
        server.middleware(bounded)
    else:
        server.tool_middleware(observe)
        server.tool_middleware(bounded)
    context = make_context()
    response = await server.dispatch(rpc("tools/call", {"name": "write"}), context)
    assert response is not None
    if ending == "refuse":
        assert response["result"]["content"][0]["text"] == "Took too long."
    else:
        assert response["error"]["code"] == INTERNAL_ERROR
    assert writes == ["written"] and context.tool_calls == {"write": 1}
    reason = "ToolError" if ending == "refuse" else "middleware_failed"
    assert logs.events("tool_result_withheld") == [
        {
            "type": "tool_result_withheld",
            "tool": "write",
            "client_id": "ip:test",
            "middleware": describe(bounded),
            "status": "cancelled",  # the tool was stopped before it answered
            "reason": reason,
        }
    ]
    assert logs.events("tool_denied") == [] and logs.events("request_denied") == []
    failed = logs.events("middleware_failed")
    assert [event["stage"] for event in failed] == ([] if ending == "refuse" else ["after"])
    (outcome,) = seen
    if level == "tool":
        assert outcome.started is True
    elif ending == "refuse":
        assert outcome.tool is not None and outcome.tool.started is True
    assert await server.wait_for_tool_threads(5) == 0


@pytest.mark.parametrize("level", ["request", "tool"])
async def test_work_left_behind_that_started_the_tool_is_audited_as_run(
    logs: LogCapture, level: str
) -> None:
    server, writes, started = writing_server("sync")
    kept: list[asyncio.Future[Any]] = []

    async def detach(info: Any, call_next: Any) -> Any:
        kept.append(asyncio.ensure_future(call_next()))
        assert await asyncio.to_thread(started.wait, 5)
        raise ToolError("Gave up waiting.")

    if level == "request":
        server.middleware(detach)
    else:
        server.tool_middleware(detach)
    response = await server.dispatch(rpc("tools/call", {"name": "write"}), make_context())
    assert response is not None
    assert response["result"]["content"][0]["text"] == "Gave up waiting."
    assert writes == ["written"]
    (withheld,) = logs.events("tool_result_withheld")
    assert withheld["middleware"] == describe(detach) and withheld["status"] == "cancelled"
    assert logs.events("tool_denied") == [] and logs.events("request_denied") == []
    assert await server.wait_for_tool_threads(5) == 0
    (finished,) = logs.events("tool_finished_after_cancel")
    assert finished["status"] == "ok"  # the write the client was not told about


async def test_nested_request_refusals_after_the_tool_ran_are_withheld(logs: LogCapture) -> None:
    writes: list[str] = []
    seen: list[Any] = []
    for inner_error in (AuthenticationError("Inner says no."), RuntimeError("inner broke")):
        server = make_server()
        writes.clear()
        seen.clear()
        first = len(logs.records)

        @server.tool
        def write() -> str:
            """Writes."""
            writes.append("written")
            return "done"

        @server.middleware
        async def outer(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
            outcome = await call_next()
            if request.method != "tools/call":
                return outcome
            seen.append(outcome)
            raise AuthenticationError("Outer says no.")

        @server.middleware
        async def inner(
            request: RequestInfo, call_next: RequestNext, error: Exception = inner_error
        ) -> RequestOutcome:
            await call_next()
            raise error

        response = await server.dispatch(rpc("tools/call", {"name": "write"}), make_context())
        assert response is not None
        assert response["error"] == {"code": AUTHENTICATION_REQUIRED, "message": "Outer says no."}
        assert writes == ["written"]
        (outcome,) = seen
        assert outcome.tool is not None, inner_error
        assert (outcome.tool.status, outcome.tool.started) == ("ok", True)
        events = [
            r.event  # type: ignore[attr-defined]
            for r in logs.records[first:]
            if r.name == "easy_mcp.audit"
        ]
        withheld = [e for e in events if e["type"] == "tool_result_withheld"]
        assert [(e["middleware"], e["status"]) for e in withheld] == [
            (describe(inner), "ok"),
            (describe(outer), "ok"),
        ]
        assert [e for e in events if e["type"] == "request_denied"] == []


async def test_reserved_mcp_codes_from_middleware_become_internal_errors(
    logs: LogCapture,
) -> None:
    for level in ("request", "tool"):
        for code in (-32050, -32023, -32099, -32020, -32021, -32022, -32001):
            server = make_server()

            async def refuse(info: Any, call_next: Any, code: int = code) -> Any:
                raise ProtocolError("refused", code=code)

            if level == "request":
                server.middleware(refuse)
            else:
                server.tool_middleware(refuse)
            first = len(logs.records)
            call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
            response = await server.dispatch(call, make_context())
            assert response is not None
            if code <= -32023:
                assert response["error"]["code"] == INTERNAL_ERROR, (level, code)
                assert "error_id=" in response["error"]["message"]
                logged = "\n".join(r.getMessage() for r in logs.records[first:])
                assert str(code) in logged and describe(refuse) in logged
                assert logs.events("middleware_failed")
            else:
                assert response["error"] == {"code": code, "message": "refused"}, (level, code)


async def test_an_inner_refusal_is_an_outcome_for_the_outer_middleware() -> None:
    server = make_server()
    seen: list[Any] = []

    @server.middleware
    async def outer(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        outcome = await call_next()  # does not raise
        seen.append(("request", outcome.error_code, outcome.error_type))
        return outcome

    @server.middleware
    async def inner(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/list":
            raise AuthenticationError("Who are you?")
        return await call_next()

    @server.tool_middleware
    async def outer_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        seen.append(("tool", outcome.status, outcome.error_code))
        return outcome

    @server.tool_middleware
    async def inner_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        raise SessionLimitError("Daily budget spent.")

    listed = await server.dispatch(rpc("tools/list"), make_context())
    assert listed is not None and listed["error"]["code"] == AUTHENTICATION_REQUIRED
    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    called = await server.dispatch(call, make_context())
    assert called is not None and called["error"]["code"] == SESSION_LIMIT_EXCEEDED
    assert seen == [
        ("request", AUTHENTICATION_REQUIRED, "-32001"),
        ("tool", "refused", SESSION_LIMIT_EXCEEDED),
        ("request", SESSION_LIMIT_EXCEEDED, "-32006"),
    ]


# ---------------------------------------------------- failures and breaches


async def test_a_failing_middleware_fails_closed(logs: LogCapture) -> None:
    server = make_server()
    ran: list[bool] = []

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Once per session."""
        ran.append(True)
        return "done"

    @server.tool_middleware
    async def broken(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        raise RuntimeError("secret detail")

    context = make_context()
    response = await server.dispatch(rpc("tools/call", {"name": "once"}), context)
    assert response is not None
    error = response["error"]
    assert error["code"] == INTERNAL_ERROR
    assert "secret" not in error["message"]
    error_id = re.fullmatch(r"Internal server error \(error_id=([0-9a-f]{12})\)", error["message"])
    assert error_id is not None
    (record,) = [r for r in logs.records if error_id.group(1) in r.getMessage()]
    assert record.levelname == "ERROR" and record.exc_info is not None
    assert describe(broken) in record.getMessage()
    assert logs.events("middleware_failed") == [
        {
            "type": "middleware_failed",
            "middleware": describe(broken),
            "method": "tools/call",
            "tool": "once",
            "client_id": "ip:test",
            "error_id": error_id.group(1),
            "stage": "before",
        }
    ]
    assert ran == [] and context.tool_calls == {"once": 0}

    request_level = make_server()

    @request_level.middleware
    async def broken_request(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        raise KeyError("secret detail")

    listed = await request_level.dispatch(rpc("tools/list"), make_context())
    assert listed is not None and listed["error"]["code"] == INTERNAL_ERROR
    assert "secret" not in listed["error"]["message"]
    assert logs.events("middleware_failed")[-1]["stage"] == "before"
    assert "tool" not in logs.events("middleware_failed")[-1]


async def test_debug_mode_shows_middleware_failure_detail() -> None:
    server = make_server(debug=True)

    @server.tool_middleware
    async def broken(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        raise RuntimeError("secret detail")

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    response = await server.dispatch(call, make_context())
    assert response is not None
    assert response["error"]["message"].endswith(": RuntimeError: secret detail")


async def test_returning_none_is_a_breach(logs: LogCapture) -> None:
    server = make_server()

    @server.middleware
    async def forgetful(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        await call_next()  # and no return
        return None  # type: ignore[return-value]

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    response = await server.dispatch(call, make_context())
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert "returned None; return the value of `await call_next()`" in logs.text
    (failed,) = logs.events("middleware_failed")
    assert failed["middleware"] == describe(forgetful) and failed["stage"] == "after"
    (withheld,) = logs.events("tool_result_withheld")  # the tool did run
    assert withheld["status"] == "ok" and withheld["reason"] == "middleware_failed"


async def test_returning_without_calling_call_next_is_a_breach(logs: LogCapture) -> None:
    server = make_server()
    ran: list[bool] = []

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Once per session."""
        ran.append(True)
        return "done"

    @server.tool_middleware
    async def lazy(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        return None  # type: ignore[return-value]

    context = make_context()
    response = await server.dispatch(rpc("tools/call", {"name": "once"}), context)
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert "without calling call_next()" in logs.text
    assert ran == [] and context.tool_calls == {"once": 0}
    assert logs.events("middleware_failed")[0]["stage"] == "before"


async def test_calling_call_next_without_awaiting_it_is_a_breach(logs: LogCapture) -> None:
    server = make_server()
    ran: list[bool] = []

    @server.tool
    def touch() -> str:
        """Records that it ran."""
        ran.append(True)
        return "touched"

    @server.middleware
    async def careless(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        call_next()  # never awaited
        return None  # type: ignore[return-value]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        response = await server.dispatch(rpc("tools/call", {"name": "touch"}), make_context())
        gc.collect()
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert ran == []
    assert "never awaited" not in " ".join(str(w.message) for w in caught)
    assert "called call_next() without awaiting it" in logs.text


async def test_returning_a_different_outcome_is_a_breach(logs: LogCapture) -> None:
    server = make_server()
    kept: list[ToolOutcome] = []

    @server.tool_middleware
    async def swap(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        kept.append(outcome)
        return kept[0]  # right the first time, wrong after

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    first = await server.dispatch(call, make_context())
    assert first is not None and first["result"]["content"][0]["text"] == "2"
    second = await server.dispatch(call, make_context())
    assert second is not None and second["error"]["code"] == INTERNAL_ERROR
    assert "returned a ToolOutcome instead of the outcome call_next() returned" in logs.text
    (withheld,) = logs.events("tool_result_withheld")
    assert withheld["status"] == "ok" and withheld["middleware"] == describe(swap)


async def test_call_next_twice_raises() -> None:
    server = make_server()
    ran: list[bool] = []
    errors: list[str] = []

    @server.tool
    def touch() -> str:
        """Records that it ran."""
        ran.append(True)
        return "touched"

    @server.tool_middleware
    async def retry(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        try:
            call_next()
        except RuntimeError as exc:
            errors.append(str(exc))
        return outcome

    response = await server.dispatch(rpc("tools/call", {"name": "touch"}), make_context())
    assert response is not None and response["result"]["content"][0]["text"] == "touched"
    assert errors == ["call_next() may be called only once"] and ran == [True]

    @server.middleware
    async def stubborn(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        await call_next()
        return await call_next()

    response = await server.dispatch(rpc("tools/call", {"name": "touch"}, 2), make_context())
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert ran == [True, True]  # once per request, never twice


def capped_server() -> tuple[MCPServer, list[str]]:
    """A server whose tools ``once`` (async) and ``once_sync`` may run once per session."""
    server = make_server()
    runs: list[str] = []

    async def once() -> str:
        runs.append("once")
        return "done"

    def once_sync() -> str:
        runs.append("once_sync")
        return "done"

    for fn in (once, once_sync):
        server.register_tool(
            fn, name=fn.__name__, description="Once per session.", max_calls_per_session=1
        )
    return server, runs


async def quota(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
    """Asks a remote quota service first, as a real one would."""
    await asyncio.sleep(0.05)
    return await call_next()


async def test_work_call_next_left_in_a_gather_is_stopped(logs: LogCapture) -> None:
    server, runs = capped_server()

    @server.tool_middleware
    async def fan_out(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        async def log_remote() -> None:
            await asyncio.sleep(0.01)
            raise ConnectionError("log service down")

        outcome, _ = await asyncio.gather(call_next(), log_remote())
        return outcome

    server.tool_middleware(quota)
    context = make_context()
    for n, name in enumerate(("once", "once", "once_sync", "once_sync")):
        response = await server.dispatch(rpc("tools/call", {"name": name}, n), context)
        assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    await asyncio.sleep(0.2)  # time enough for left-behind work to reach the tool
    assert runs == [] and context.tool_calls == {"once": 0, "once_sync": 0}
    assert [event["stage"] for event in logs.events("middleware_failed")] == ["before"] * 4

    request_level, runs = capped_server()

    @request_level.middleware
    async def fan_out_request(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        async def log_remote() -> None:
            await asyncio.sleep(0.01)
            raise ConnectionError("log service down")

        outcome, _ = await asyncio.gather(call_next(), log_remote())
        return outcome

    @request_level.middleware
    async def policy(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        await asyncio.sleep(0.05)  # a remote policy service
        return await call_next()

    context = make_context()
    for n in range(2):
        response = await request_level.dispatch(rpc("tools/call", {"name": "once"}, n), context)
        assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    await asyncio.sleep(0.2)
    assert runs == [] and context.tool_calls.get("once", 0) == 0


async def test_work_call_next_left_in_a_task_is_stopped(logs: LogCapture) -> None:
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda loop, context: unhandled.append(context))
    try:
        for started in (False, True):
            server, runs = capped_server()
            kept: list[asyncio.Future[ToolOutcome]] = []

            @server.tool_middleware
            async def detach(
                call: ToolCall,
                call_next: ToolNext,
                started: bool = started,
                kept: list[asyncio.Future[ToolOutcome]] = kept,
            ) -> ToolOutcome:
                kept.append(asyncio.ensure_future(call_next()))
                if started:
                    await asyncio.sleep(0.01)  # the inner chain is under way
                return None  # type: ignore[return-value]

            server.tool_middleware(quota)
            context = make_context()
            for n in range(3):
                response = await server.dispatch(rpc("tools/call", {"name": "once"}, n), context)
                assert response is not None and response["error"]["code"] == INTERNAL_ERROR
            await asyncio.sleep(0.2)
            assert runs == [] and context.tool_calls == {"once": 0}, started
            assert all(task.done() for task in kept)
            kept.clear()
            gc.collect()
    finally:
        loop.set_exception_handler(None)
    assert unhandled == []
    assert "called call_next() without awaiting it" not in logs.text
    assert logs.text.count("returned before the call_next() it started had finished") == 6


async def test_work_call_next_left_in_a_shield_is_stopped() -> None:
    server, runs = capped_server()
    entered = asyncio.Event()

    @server.tool_middleware
    async def shielded(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        return await asyncio.shield(call_next())

    @server.tool_middleware
    async def slow_quota(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        entered.set()
        await asyncio.sleep(0.05)
        return await call_next()

    context = make_context()
    for n in range(3):
        entered.clear()
        call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "once"}, n), context))
        await asyncio.wait_for(entered.wait(), 5)
        await server.dispatch(notification("notifications/cancelled", {"requestId": n}), context)
        assert await asyncio.wait_for(call, 5) is None
    await asyncio.sleep(0.2)
    assert runs == [] and context.tool_calls == {"once": 0}


async def test_work_left_behind_never_starts_the_tool_once_the_call_is_over() -> None:
    server, runs = capped_server()
    detached = asyncio.Event()
    kept: list[asyncio.Future[ToolOutcome]] = []

    @server.tool_middleware
    async def detach(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        kept.append(asyncio.ensure_future(call_next()))
        await asyncio.sleep(0.01)
        detached.set()
        raise RuntimeError("gave up on it")

    @server.tool_middleware
    async def stubborn(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)  # takes its time over being stopped
        return await call_next()

    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "once"}, 1), context))
    await asyncio.wait_for(detached.wait(), 5)
    # The call is cancelled while its middleware waits for the work it left.
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(call, 5) is None
    await asyncio.sleep(0.3)
    assert runs == [] and context.tool_calls == {"once": 0}
    assert all(task.done() for task in kept)


async def test_call_next_first_called_after_the_middleware_returned_runs_nothing(
    logs: LogCapture,
) -> None:
    late: list[asyncio.Task[Any]] = []
    inner_ran: list[str] = []
    for level in ("request", "tool"):
        server, runs = capped_server()
        late.clear()

        async def forward(call_next: Any) -> Any:
            await asyncio.sleep(0.01)
            return await call_next()

        async def detach(info: Any, call_next: Any) -> Any:
            late.append(asyncio.create_task(forward(call_next)))
            return None  # by mistake, before the task has called call_next()

        async def inner(info: Any, call_next: Any) -> Any:
            inner_ran.append(type(info).__name__)
            return await call_next()

        if level == "request":
            server.middleware(detach)
            server.middleware(inner)
        else:
            server.tool_middleware(detach)
        server.tool_middleware(inner)
        context = make_context()
        response = await server.dispatch(rpc("tools/call", {"name": "once"}), context)
        assert response is not None and response["error"]["code"] == INTERNAL_ERROR, level
        (task,) = late
        await asyncio.wait({task}, timeout=5)
        await asyncio.sleep(0.1)  # time enough for anything it started to reach the tool
        assert runs == [] and context.tool_calls.get("once", 0) == 0, level
        assert inner_ran == [], level
        assert logs.events("tool_call") == []
        error = task.exception()
        assert isinstance(error, RuntimeError), level
        assert str(error) == "call_next() called after the middleware returned"
    assert [event["stage"] for event in logs.events("middleware_failed")] == ["before"] * 2
    assert logs.text.count("returned without calling call_next()") == 2


class Payload:
    """A middleware local that can be watched with a weak reference."""


async def test_middleware_exception_frames_are_freed(monkeypatch: pytest.MonkeyPatch) -> None:
    # With the cyclic collector off, only reference counting can free the
    # failing middleware's frame: the exception must not keep it.
    request_level = make_server()
    # pytest attaches handlers that keep every record, traceback included, to
    # loggers that exist when a test starts; only the server's own may stay.
    logger = logging.getLogger("easy_mcp")
    own = [handler for handler in logger.handlers if getattr(handler, "_easy_mcp", False)]
    monkeypatch.setattr(logger, "handlers", own)
    tool_level = make_server()
    observe_level = make_server()
    held: list[weakref.ref[Payload]] = []

    # Each keeps its exception in a local, so the exception's traceback holds
    # the frame that holds the exception: a cycle only clearing the
    # traceback breaks.
    @request_level.middleware
    async def failing(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        payload = Payload()
        held.append(weakref.ref(payload))
        try:
            raise KeyError("first")
        except KeyError as exc:
            error = RuntimeError("then this")
            raise error from exc

    @tool_level.tool_middleware
    async def refusing(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        payload = Payload()
        held.append(weakref.ref(payload))
        refusal = ToolError("refused")
        raise refusal

    @observe_level.middleware
    async def watching(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        payload = Payload()
        held.append(weakref.ref(payload))
        error = RuntimeError("cannot watch")
        raise error

    # The request level's refusals: a ProtocolError, and a ToolError before
    # and after the tool ran.
    refusals: dict[str, MCPServer] = {}
    for kind in ("protocol", "tool error", "after the call"):
        refusing_server = make_server()

        async def refusing_request(
            request: RequestInfo, call_next: RequestNext, kind: str = kind
        ) -> RequestOutcome:
            payload = Payload()
            held.append(weakref.ref(payload))
            if kind == "after the call":
                await call_next()
            refusal = AuthenticationError("who?") if kind == "protocol" else ToolError("no")
            raise refusal

        refusing_server.middleware(refusing_request)
        refusals[kind] = refusing_server

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    gc.disable()
    try:
        for n in range(3):
            response = await request_level.dispatch({**call, "id": n}, make_context())
            assert response is not None and response["error"]["code"] == INTERNAL_ERROR
            response = await tool_level.dispatch({**call, "id": n}, make_context())
            assert response is not None and response["result"]["isError"] is True
            initialized = notification("notifications/initialized")
            assert await observe_level.dispatch(initialized, make_context()) is None
            for kind, refusing_server in refusals.items():
                response = await refusing_server.dispatch({**call, "id": n}, make_context())
                assert response is not None
                if kind == "protocol":
                    assert response["error"]["code"] == AUTHENTICATION_REQUIRED
                else:
                    assert response["result"]["isError"] is True
        await asyncio.sleep(0)
        assert len(held) == 18
        assert [ref for ref in held if ref() is not None] == []
    finally:
        gc.enable()


# ------------------------------------------------ built-ins cannot be bypassed


async def test_rate_limit_is_charged_before_middleware() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=1)
    seen: list[str] = []

    @server.middleware
    async def watch(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        seen.append(request.method)
        return await call_next()

    context = make_context()
    first = await server.dispatch(rpc("tools/list"), context)
    assert first is not None and "result" in first
    second = await server.dispatch(rpc("tools/list", msg_id=2), context)
    assert second is not None and second["error"]["code"] == RATE_LIMITED
    assert seen == ["tools/list"]


async def test_hidden_tools_stay_unknown() -> None:
    server = make_server(auth=APIKeyAuth({KEY: "*"}))
    requested: list[Any] = []
    tool_calls: list[str] = []

    @server.tool(requires_auth=True)
    def secret() -> str:
        """For authenticated callers."""
        return "hidden"

    @server.middleware
    async def watch(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        requested.append(request.tool)
        return await call_next()

    @server.tool_middleware
    async def watch_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        tool_calls.append(call.tool.name)
        return await call_next()

    response = await server.dispatch(rpc("tools/call", {"name": "secret"}), make_context())
    assert response is not None
    assert response["error"] == {"code": INVALID_PARAMS, "message": "Unknown tool: secret"}
    assert [tool.name for tool in requested] == ["secret"]  # known server-side only
    assert tool_calls == []


async def test_scope_and_validation_run_before_tool_middleware(logs: LogCapture) -> None:
    server = make_server(auth=APIKeyAuth({KEY: ["read"]}))
    reached: list[str] = []

    @server.tool(requires_auth=True, scopes=["admin"])
    def admin() -> str:
        """For admins."""
        return "admin"

    @server.tool(max_calls_per_session=1)
    def spent() -> str:
        """Already called this session."""
        return "spent"

    @server.tool_middleware
    async def watch(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        reached.append(call.tool.name)
        return await call_next()

    identity = ClientIdentity(fingerprint="f" * 12, scopes=frozenset({"read"}))
    context = make_context(identity=identity)
    context.tool_calls["spent"] = 1
    scoped = await server.dispatch(rpc("tools/call", {"name": "admin"}), context)
    assert scoped is not None and scoped["error"]["code"] == INVALID_PARAMS
    bad = rpc("tools/call", {"name": "add", "arguments": {"a": "one", "b": 2}})
    invalid = await server.dispatch(bad, context)
    assert invalid is not None and invalid["error"]["code"] == INVALID_PARAMS
    capped = await server.dispatch(rpc("tools/call", {"name": "spent"}), context)
    assert capped is not None and capped["error"]["code"] == SESSION_LIMIT_EXCEEDED
    assert reached == []
    denied = logs.events("tool_denied")
    assert [event["reason"] for event in denied] == ["ValidationError", "SessionLimitError"]
    assert all("middleware" not in event for event in denied)


async def test_request_and_arguments_are_deep_read_only() -> None:
    server = make_server()
    kept: dict[str, Any] = {}

    @server.tool
    def nested(config: dict[str, Any], items: list[int]) -> str:
        """Takes nested arguments."""
        return "ok"

    @server.middleware
    async def keep_request(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        kept["request"] = request
        kept["request_outcome"] = await call_next()
        return kept["request_outcome"]

    @server.tool_middleware
    async def keep_call(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        kept["call"] = call
        kept["tool_outcome"] = await call_next()
        return kept["tool_outcome"]

    arguments = {"config": {"deep": {"k": 1}}, "items": [1, 2]}
    message = modern("tools/call", {"name": "nested", "arguments": arguments}, traceparent="t")
    await server.dispatch(message, make_context())
    request, call = kept["request"], kept["call"]

    for container, key in (
        (request.params, "name"),
        (request.params["arguments"]["config"]["deep"], "k"),
        (request.params["arguments"]["items"], 0),
        (request.meta, "traceparent"),
        (request.meta["io.modelcontextprotocol/clientInfo"], "name"),
        (call.arguments, "items"),
        (call.arguments["config"]["deep"], "k"),
        (call.request.transport.headers, "x"),
    ):
        with pytest.raises(TypeError):
            container[key] = "forged"
    assert isinstance(call.arguments["items"], tuple)
    assert message["params"]["arguments"] == arguments  # the original is untouched

    for obj in (request, call, kept["request_outcome"], kept["tool_outcome"]):
        for name in [n for n in dir(obj) if not n.startswith("_")]:
            if callable(getattr(type(obj), name, None)):
                continue
            with pytest.raises(AttributeError):
                setattr(obj, name, None)
        with pytest.raises(AttributeError):
            obj.anything_new = 1
    for cls in (RequestInfo, ToolCall, RequestOutcome, ToolOutcome):
        with pytest.raises(TypeError):
            cls()


async def test_the_tool_receives_its_own_arguments() -> None:
    pydantic = pytest.importorskip("pydantic")

    class Point(pydantic.BaseModel):  # type: ignore[name-defined, misc]
        x: int
        y: int

    server = make_server()
    received: list[Any] = []
    views: list[Any] = []

    def where(point: Any, tags: list[str]) -> str:
        """Moves things around."""
        received.append((type(point).__name__, point.x, list(tags)))
        tags.append("tool")  # the tool's own copy
        return "ok"

    # The model is local to this test, so the annotation cannot be a string.
    where.__annotations__ = {"point": Point, "tags": list[str], "return": str}
    server.register_tool(where, name="where")

    @server.tool_middleware
    async def look(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        views.append(call.arguments)  # first read only after the tool ran
        views.append(call.arguments)
        return outcome

    arguments = {"point": {"x": 1, "y": 2}, "tags": ["a"]}
    response = await server.dispatch(
        rpc("tools/call", {"name": "where", "arguments": arguments}), make_context()
    )
    assert response is not None and response["result"]["isError"] is False, response
    assert received == [("Point", 1, ["a"])]
    first, again = views
    assert first is again
    assert dict(first["point"]) == {"x": 1, "y": 2} and first["tags"] == ("a",)


def thawed(value: Any) -> Any:
    """A read-only view as plain JSON, for comparing."""
    if isinstance(value, Mapping):
        return {key: thawed(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [thawed(item) for item in value]
    return value


async def test_what_a_tool_does_to_its_arguments_never_shows_in_middleware() -> None:
    server = make_server()
    seen: list[tuple[str, str, Any]] = []

    def grow(items: list[int], config: dict[str, Any]) -> str:
        """Changes its arguments in place."""
        items.append(99)
        config["inner"]["k"] = "changed by the tool"
        config["added"] = True
        return "grown"

    async def grow_async(items: list[int], config: dict[str, Any]) -> str:
        """Changes its arguments in place, from a task."""
        return grow(items, config)

    server.register_tool(grow, name="grow")
    server.register_tool(grow_async, name="grow_async")

    @server.middleware
    async def after_request(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        outcome = await call_next()
        assert request.tool is not None
        seen.append((request.tool.name, "params", thawed(request.params["arguments"])))
        return outcome

    @server.tool_middleware
    async def after_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        seen.append((call.tool.name, "arguments", thawed(call.arguments)))
        return outcome

    sent = {"items": [1, 2], "config": {"inner": {"k": "v"}}}
    for name in ("grow", "grow_async"):
        message = rpc("tools/call", {"name": name, "arguments": json.loads(json.dumps(sent))})
        response = await server.dispatch(message, make_context())
        assert response is not None and response["result"]["content"][0]["text"] == "grown"
        assert message["params"]["arguments"] == sent  # the client's message is untouched
    assert seen == [
        ("grow", "arguments", sent),
        ("grow", "params", sent),
        ("grow_async", "arguments", sent),
        ("grow_async", "params", sent),
    ]


async def test_what_a_tool_does_to_its_arguments_never_shows_in_its_call() -> None:
    server = make_server()  # no middleware: current_tool_call() works all the same
    seen: list[Any] = []

    def grow(items: list, config: dict) -> str:  # type: ignore[type-arg]
        """Changes its arguments in place, then reads its call."""
        items.append("added by the tool")
        config["added"] = True
        call = current_tool_call()
        assert call is not None
        seen.append((thawed(call.arguments), thawed(call.request.params["arguments"])))
        return "grown"

    async def grow_async(items: list, config: dict) -> str:  # type: ignore[type-arg]
        """Changes its arguments in place, from a task, then reads its call."""
        return grow(items, config)

    server.register_tool(grow, name="grow")
    server.register_tool(grow_async, name="grow_async")
    sent = {"items": ["a"], "config": {"k": "v"}}
    for name in ("grow", "grow_async"):
        message = rpc("tools/call", {"name": name, "arguments": json.loads(json.dumps(sent))})
        response = await server.dispatch(message, make_context())
        assert response is not None and response["result"]["content"][0]["text"] == "grown"
        assert message["params"]["arguments"] == sent  # the client's message is untouched
    assert seen == [(sent, sent), (sent, sent)]


async def test_the_session_cap_holds_under_slow_middleware() -> None:
    server = make_server()
    gate = asyncio.Event()
    ran: list[Any] = []
    refuse_first = True

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Once per session."""
        ran.append(True)
        return "done"

    @server.tool_middleware
    async def slow(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        await gate.wait()
        if refuse_first and call.request.request_id == 0:
            raise ToolError("refused")
        return await call_next()

    context = make_context()
    calls = [
        asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "once"}, n), context))
        for n in range(5)
    ]
    await asyncio.sleep(0.05)
    gate.set()
    responses = await asyncio.gather(*calls)
    assert responses[0] is not None and responses[0]["result"]["isError"] is True
    assert [r["error"]["code"] for r in responses[1:]] == [SESSION_LIMIT_EXCEEDED] * 4
    assert ran == [] and context.tool_calls == {"once": 0}  # the refusal was refunded

    refuse_first = False
    gate.clear()
    calls = [
        asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "once"}, n), context))
        for n in range(10, 15)
    ]
    await asyncio.sleep(0.05)
    gate.set()
    responses = await asyncio.gather(*calls)
    assert sum("result" in r for r in responses) == 1
    assert ran == [True] and context.tool_calls == {"once": 1}


async def test_identity_cannot_be_changed() -> None:
    server = make_server()
    kept: list[Any] = []

    @server.middleware
    async def keep(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        kept.append(request)
        outcome = await call_next()
        kept.append(outcome)
        return outcome

    @server.tool_middleware
    async def keep_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        kept.append(call)
        outcome = await call_next()
        kept.append(outcome)
        return outcome

    identity = ClientIdentity(fingerprint="f" * 12, scopes=frozenset({"read"}))
    context = make_context(identity=identity)
    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    await server.dispatch(call, context)
    request = kept[0]
    assert request.identity is identity
    with pytest.raises(AttributeError):
        request.identity = ClientIdentity(fingerprint="x", scopes=frozenset({"*"}))
    with pytest.raises(dataclasses.FrozenInstanceError):
        request.identity.scopes = frozenset({"*"})  # type: ignore[misc]

    def public_values(obj: Any) -> list[Any]:
        values = []
        for name in dir(obj):
            if name.startswith("_"):
                continue
            value = getattr(obj, name)
            if not callable(value) or isinstance(value, type):
                values.append(value)
        return values

    frontier = list(kept)
    for _ in range(3):  # attributes of attributes, three levels down
        frontier = [value for obj in frontier for value in public_values(obj)]
        assert not any(isinstance(value, ClientContext) for value in frontier)


def test_credential_headers_are_never_exposed() -> None:
    info = TransportInfo(
        name="streamable-http",
        headers={
            "Authorization": "Bearer key",
            "X-API-Key": "key",
            "Cookie": "session=1",
            "MCP-Session-Id": "abc",
            "Proxy-Authorization": "Basic x",
            "X-Tenant": "a",
        },
    )
    assert dict(info.headers) == {"x-tenant": "a"}
    pairs = TransportInfo(headers=[("X-Tenant", "a"), ("x-tenant", "b"), ("authorization", "k")])
    assert dict(pairs.headers) == {"x-tenant": "a, b"}
    with pytest.raises(TypeError):
        info.headers["authorization"] = "Bearer key"  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        info.headers = {}  # type: ignore[misc]
    replaced = dataclasses.replace(info, name="sse")
    assert replaced.name == "sse" and dict(replaced.headers) == {"x-tenant": "a"}
    assert info == TransportInfo(name="streamable-http", headers={"x-tenant": "a"})
    assert hash(info) == hash(TransportInfo(name="streamable-http", headers={"X-Tenant": "a"}))
    assert TransportInfo().name == "custom" and dict(TransportInfo().headers) == {}


# ------------------------------------------------------------- cancellation


async def test_cancel_reaches_request_middleware_before_the_tool_starts(logs: LogCapture) -> None:
    server = make_server()
    entered = asyncio.Event()
    saw: list[str] = []
    tokens: list[Any] = []
    ran: list[bool] = []

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Once per session."""
        ran.append(True)
        return "done"

    @server.middleware
    async def outer(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        try:
            return await call_next()
        except asyncio.CancelledError:
            saw.append("request")
            raise

    @server.tool_middleware
    async def waits(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        tokens.append(call.cancel_token)
        assert current_cancel_token() is call.cancel_token
        entered.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            saw.append("tool")
            raise
        return await call_next()

    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "once"}, 7), context))
    await asyncio.wait_for(entered.wait(), 5)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 7}), context)
    assert await asyncio.wait_for(call, 5) is None
    assert saw == ["tool", "request"]
    assert tokens[0].cancelled and tokens[0].reason == "cancelled"
    assert len(logs.events("tool_cancelled")) == 1
    assert ran == [] and context.tool_calls == {"once": 0}


async def test_cancel_during_a_sync_tool_propagates_through_middleware() -> None:
    server = make_server()
    started = threading.Event()
    stopped = threading.Event()
    callback_threads: list[str] = []
    saw: list[str] = []

    def block() -> str:
        token = current_cancel_token()
        assert token is not None

        def stop() -> None:
            callback_threads.append(threading.current_thread().name)
            stopped.set()

        token.on_cancel(stop)
        started.set()
        stopped.wait(10)
        return "stopped"

    server.register_tool(block, name="block", description="Blocks until cancelled.")

    @server.middleware
    async def outer(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        try:
            return await call_next()
        except asyncio.CancelledError:
            saw.append("request")
            raise

    @server.tool_middleware
    async def inner(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        try:
            return await call_next()
        except asyncio.CancelledError:
            saw.append("tool")
            raise

    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "block"}, 3), context))
    assert await asyncio.to_thread(started.wait, 5)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 3}), context)
    assert await asyncio.wait_for(call, 5) is None
    assert await asyncio.to_thread(stopped.wait, 5)
    assert saw == ["tool", "request"]
    assert callback_threads == ["easy-mcp-cancel:block"]
    assert await server.wait_for_tool_threads(5) == 0


async def swallow(style: str, call_next: Any) -> Any:
    """Await *call_next* and keep a cancellation from reaching the caller."""
    if style == "shield":
        # The cancel lands on the shield; the inner chain never sees it.
        inner = asyncio.ensure_future(call_next())
        try:
            return await asyncio.shield(inner)
        except asyncio.CancelledError:
            return await inner
    if style == "wait":
        inner = asyncio.ensure_future(call_next())
        try:
            await asyncio.wait({inner})
        except asyncio.CancelledError:
            await asyncio.wait({inner})
        return inner.result()
    try:
        return await call_next()
    except asyncio.CancelledError:
        if style == "raise":
            raise RuntimeError("cancelled, apparently") from None
        return None


@pytest.mark.parametrize("style", ["return", "raise", "shield", "wait"])
@pytest.mark.parametrize("level", ["request", "tool"])
async def test_a_middleware_that_swallows_cancellation_is_overruled(
    logs: LogCapture, level: str, style: str
) -> None:
    server = make_server()
    started = asyncio.Event()

    @server.tool
    async def brief() -> str:
        """Finishes shortly, unless it is cancelled."""
        started.set()
        await asyncio.sleep(0.2)
        return "finished"

    async def stubborn(info: Any, call_next: Any) -> Any:
        return await swallow(style, call_next)

    if level == "request":
        server.middleware(stubborn)
    else:
        server.tool_middleware(stubborn)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "brief"}, 1), context))
    await asyncio.wait_for(started.wait(), 5)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(call, 5) is None  # still no response

    # A cancellation of the caller still reaches the caller.
    started.clear()
    caller = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "brief"}, 2), context))
    await asyncio.wait_for(started.wait(), 5)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    # So does a timeout around the call.
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.1):
            await server.dispatch(rpc("tools/call", {"name": "brief"}, 3), context)

    def swallowed() -> list[logging.LogRecord]:
        return [r for r in logs.records if "swallowed a cancellation" in r.getMessage()]

    deadline = time.monotonic() + 5
    while len(swallowed()) < 3 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)  # the caller's cancel is unwinding in its own task
    assert len(swallowed()) == 3 and {r.levelname for r in swallowed()} == {"WARNING"}
    assert logs.events("middleware_failed") == []


async def test_other_requests_are_cancellable_now(logs: LogCapture) -> None:
    server = make_server()
    entered = asyncio.Event()
    saw: list[str] = []

    @server.middleware
    async def slow_list(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/list":
            entered.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                saw.append("cancelled")
                raise
        return await call_next()

    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/list", msg_id=5), context))
    await asyncio.wait_for(entered.wait(), 5)
    assert 5 in context.in_flight
    await server.dispatch(notification("notifications/cancelled", {"requestId": 5}), context)
    assert await asyncio.wait_for(call, 5) is None
    assert saw == ["cancelled"]
    assert logs.events("request_cancelled") == [
        {
            "type": "request_cancelled",
            "method": "tools/list",
            "client_id": "ip:test",
            "request_id": 5,
        }
    ]
    assert context.in_flight == {}


async def test_a_tool_that_swallows_its_cancel_answers_alike_with_middleware(
    logs: LogCapture,
) -> None:
    responses: list[Any] = []
    started = asyncio.Event()

    async def in_a_task(info: Any, call_next: Any) -> Any:
        # Awaiting the task forwards the cancel to it, and on to the tool.
        return await asyncio.ensure_future(call_next())

    layers = ("none", "tool", "request", "tool task", "request task")
    for layer in layers:
        server = make_server()
        started.clear()

        @server.tool
        async def stubborn() -> str:
            """Ignores its cancel."""
            started.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                return "swallowed"
            return "finished"

        if layer == "tool":
            server.tool_middleware(tool_passthrough)
        elif layer == "request":
            server.middleware(passthrough)
        elif layer == "tool task":
            server.tool_middleware(in_a_task)
        elif layer == "request task":
            server.middleware(in_a_task)
        context = make_context()
        call = asyncio.create_task(
            server.dispatch(rpc("tools/call", {"name": "stubborn"}, 1), context)
        )
        await asyncio.wait_for(started.wait(), 5)
        await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
        responses.append(await asyncio.wait_for(call, 5))
    assert responses[0] is not None
    assert responses[0]["result"]["content"][0]["text"] == "swallowed"
    assert responses == [responses[0]] * len(layers)
    assert "swallowed a cancellation" not in logs.text


async def check_ok() -> None:
    await asyncio.sleep(0)


async def check_down() -> None:
    await asyncio.sleep(0.01)  # fails once the TaskGroup's body has finished
    raise ConnectionError("policy service down")


async def test_a_failed_task_group_in_middleware_is_no_cancellation(logs: LogCapture) -> None:
    # On Python 3.11 a TaskGroup whose child fails after the body finished
    # cancels the task it runs in and never takes that back.
    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
    for level in ("request", "tool"):
        for mode in ("fail closed", "fail open", "after the call"):
            server = make_server()

            async def checks(info: Any, call_next: Any, mode: str = mode) -> Any:
                if isinstance(info, RequestInfo) and info.method != "tools/call":
                    return await call_next()
                outcome = await call_next() if mode == "after the call" else None
                try:
                    async with asyncio.TaskGroup() as group:
                        group.create_task(check_ok())
                        group.create_task(check_down())
                except ExceptionGroup:
                    if mode == "fail closed":
                        raise AuthenticationError("policy unavailable") from None
                return outcome if outcome is not None else await call_next()

            if level == "request":
                server.middleware(checks)
            else:
                server.tool_middleware(checks)
            response = await asyncio.wait_for(server.dispatch(call, make_context()), 5)
            assert response is not None, (level, mode)
            if mode == "fail closed":
                assert response["error"] == {"code": -32001, "message": "policy unavailable"}
            else:
                assert response["result"]["content"][0]["text"] == "3", (level, mode)
    assert "swallowed a cancellation" not in logs.text
    assert logs.events("tool_cancelled") == []


def cancelled_future() -> asyncio.Future[None]:
    """A future something else cancelled: a shared lookup cancelled with its first waiter, say."""
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.cancel()
    return future


async def test_a_cancellation_nobody_asked_for_is_a_middleware_failure(logs: LogCapture) -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)

    @server.middleware
    async def flaky(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method != "tools/call" or request.request_id == "refusable":
            await cancelled_future()  # raises CancelledError; the request was not cancelled
        return await call_next()

    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 7), context))
    await asyncio.wait_for(started.wait(), 5)
    # A notification is processed whatever the middleware does: the cancel lands.
    cancel = notification("notifications/cancelled", {"requestId": 7})
    assert await server.dispatch(cancel, context) is None  # and dispatch did not raise
    assert await asyncio.wait_for(call, 5) is None
    assert cancelled.is_set()
    # server/discover is always answered.
    discovered = await server.dispatch(modern("server/discover"), make_context())
    assert discovered is not None and "supportedVersions" in discovered["result"]
    # A request that can be refused fails closed, rather than going unanswered.
    for message in (rpc("tools/list"), rpc("tools/call", {"name": "add"}, "refusable")):
        response = await server.dispatch(message, make_context())
        assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert [event["stage"] for event in logs.events("middleware_failed")] == [
        "observe",
        "observe",
        "before",
        "before",
    ]
    logged = [str(r.exc_info[1]) for r in logs.records if r.exc_info]
    assert logged == ["middleware raised CancelledError although the request was not cancelled"] * 4
    assert logs.events("request_cancelled") == []

    tool_level = make_server()

    @tool_level.tool_middleware
    async def flaky_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        await cancelled_future()
        return await call_next()

    response = await tool_level.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}), make_context()
    )
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert logs.events("middleware_failed")[-1]["middleware"] == describe(flaky_tool)
    # Only the call the client cancelled was cancelled.
    assert logs.events("tool_cancelled") == [
        {"type": "tool_cancelled", "client_id": "ip:test", "request_id": 7}
    ]


async def test_a_stray_cancellation_from_the_tool_is_not_blamed_on_middleware(
    logs: LogCapture,
) -> None:
    # The tool raises a CancelledError nobody asked for.  A middleware that
    # passes it on did what it must, so the client gets what it would get
    # without middleware, and the audit trail says the same.
    async def passthrough(info: Any, call_next: Any) -> Any:
        return await call_next()

    async def aside(info: Any, call_next: Any) -> Any:
        return await asyncio.ensure_future(call_next())

    def serving(kind: str) -> MCPServer:
        server = make_server()

        @server.tool
        async def flaky() -> str:
            """Awaits a shared lookup that was cancelled."""
            await cancelled_future()
            return "unreachable"

        if kind in ("request", "both"):
            server.middleware(passthrough)
        if kind in ("tool", "both"):
            server.tool_middleware(passthrough)
        if kind == "aside":
            server.middleware(aside)
            server.tool_middleware(aside)
        return server

    answered = []
    for kind in ("none", "request", "tool", "both", "aside"):
        first = len(logs.records)
        response = await serving(kind).dispatch(
            rpc("tools/call", {"name": "flaky"}, 7), make_context()
        )
        records = logs.records[first:]
        events = [r.event for r in records if r.name == "easy_mcp.audit"]  # type: ignore[attr-defined]
        errors = [r.getMessage() for r in records if r.levelno >= logging.ERROR]
        answered.append((kind, response, events, errors))
    cancelled = [{"type": "tool_cancelled", "client_id": "ip:test", "request_id": 7}]
    assert answered == [
        (kind, None, cancelled, []) for kind in ("none", "request", "tool", "both", "aside")
    ]

    # A middleware that cancels the call_next() it started and raises that
    # cancel still fails the request: the CancelledError is its own doing.
    server = make_server()
    started, _ = with_slow_tool(server)

    @server.middleware
    async def impatient(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        inner = asyncio.ensure_future(call_next())
        await asyncio.wait_for(started.wait(), 5)
        inner.cancel()
        return await inner

    response = await server.dispatch(rpc("tools/call", {"name": "slow"}, 8), make_context())
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    assert logs.events("middleware_failed")[-1]["middleware"] == describe(impatient)

    # An outer middleware's own timeout cancels the inner one: that is a cancel.
    bounded = make_server()

    @bounded.middleware
    async def deadline(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        try:
            async with asyncio.timeout(0.05):
                return await call_next()
        except TimeoutError:
            raise ToolError("Policy check timed out.") from None

    @bounded.middleware
    async def hung(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        await asyncio.sleep(30)
        return await call_next()

    failures = len(logs.events("middleware_failed"))
    response = await bounded.dispatch(rpc("tools/call", {"name": "add"}), make_context())
    assert response is not None
    assert response["result"]["content"][0]["text"] == "Policy check timed out."
    assert len(logs.events("middleware_failed")) == failures


async def test_initialize_is_never_cancelled() -> None:
    server = make_server()
    entered = asyncio.Event()
    gate = asyncio.Event()

    @server.middleware
    async def slow_handshake(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "initialize":
            entered.set()
            await gate.wait()
        return await call_next()

    context = make_context()
    handshake = rpc("initialize", {"protocolVersion": "2025-11-25"}, 1)
    call = asyncio.create_task(server.dispatch(handshake, context))
    await asyncio.wait_for(entered.wait(), 5)
    assert context.in_flight == {}
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    gate.set()
    response = await asyncio.wait_for(call, 5)
    assert response is not None and response["result"]["protocolVersion"] == "2025-11-25"


async def test_notifications_cannot_be_refused(logs: LogCapture) -> None:
    for refusal in (AuthenticationError("no"), ToolError("no"), RuntimeError("broken"), None):
        server = make_server()
        logging.getLogger("easy_mcp").setLevel(logging.DEBUG)
        started, cancelled = with_slow_tool(server)

        @server.middleware
        async def hostile(
            request: RequestInfo, call_next: RequestNext, refusal: Any = refusal
        ) -> RequestOutcome:
            if request.is_notification:
                if refusal is None:
                    return None  # type: ignore[return-value]  # never calls call_next
                raise refusal
            return await call_next()

        context = make_context()
        first = len(logs.records)
        call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 1), context))
        await asyncio.wait_for(started.wait(), 5)
        await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
        assert await asyncio.wait_for(call, 5) is None, refusal  # the cancel still cancels
        assert cancelled.is_set()
        records = logs.records[first:]
        failed = [r.event for r in records if r.getMessage() == "middleware_failed"]  # type: ignore[attr-defined]
        if isinstance(refusal, AuthenticationError | ToolError):
            assert failed == [], refusal
            (debug,) = [r for r in records if "cannot be refused" in r.getMessage()]
            assert debug.levelname == "DEBUG"
        else:
            (event,) = failed
            assert event["stage"] == "observe" and event["method"] == "notifications/cancelled"


async def test_discover_cannot_be_refused(logs: LogCapture) -> None:
    for refusal in (AuthenticationError("no"), ToolError("no"), RuntimeError("broken"), None):
        server = make_server()
        logging.getLogger("easy_mcp").setLevel(logging.DEBUG)
        first = len(logs.records)

        @server.middleware
        async def hostile(
            request: RequestInfo, call_next: RequestNext, refusal: Any = refusal
        ) -> RequestOutcome:
            if refusal is None:
                return None  # type: ignore[return-value]
            raise refusal

        discovered = await server.dispatch(modern("server/discover"), make_context())
        assert discovered is not None, refusal
        result = discovered["result"]
        assert result["supportedVersions"][0] == "2026-07-28"
        assert result["cacheScope"] == "public" and result["resultType"] == "complete"
        records = logs.records[first:]
        failed = [r.event for r in records if r.getMessage() == "middleware_failed"]  # type: ignore[attr-defined]
        if isinstance(refusal, AuthenticationError | ToolError):
            assert failed == [], refusal
            (debug,) = [r for r in records if "cannot be refused" in r.getMessage()]
            assert debug.levelname == "DEBUG"
        else:
            (event,) = failed
            assert event["stage"] == "observe" and event["method"] == "server/discover"
        listed = await server.dispatch(modern("tools/list"), make_context())
        assert listed is not None and "error" in listed  # everything else is refusable


# ------------------------------------------------------ threads and context


async def test_middleware_runs_on_the_event_loop_thread() -> None:
    server = make_server()
    loop_thread = threading.get_ident()
    threads: dict[str, int] = {}

    @server.tool
    def where() -> str:
        """Records its thread."""
        threads["tool"] = threading.get_ident()
        return "ok"

    @server.middleware
    async def request_level(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        threads["request"] = threading.get_ident()
        outcome = await call_next()
        threads["request_after"] = threading.get_ident()
        return outcome

    @server.tool_middleware
    async def tool_level(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        threads["tool_mw"] = threading.get_ident()
        outcome = await call_next()
        threads["tool_mw_after"] = threading.get_ident()
        return outcome

    await server.dispatch(rpc("tools/call", {"name": "where"}), make_context())
    loop_side = {threads[k] for k in ("request", "request_after", "tool_mw", "tool_mw_after")}
    assert loop_side == {loop_thread}
    assert threads["tool"] != loop_thread


async def test_context_vars_from_middleware_reach_sync_and_async_tools() -> None:
    server = make_server()

    @server.middleware
    async def set_request(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        REQUEST_VAR.set(f"request-{request.request_id}")
        return await call_next()

    @server.tool_middleware
    async def set_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        TOOL_VAR.set(f"tool-{call.tool.name}")
        return await call_next()

    @server.tool
    def sync_probe() -> str:
        """Reads the variables from a worker thread."""
        return f"{REQUEST_VAR.get()}|{TOOL_VAR.get()}"

    @server.tool
    async def async_probe() -> str:
        """Reads the variables from the tool's task."""
        return f"{REQUEST_VAR.get()}|{TOOL_VAR.get()}"

    for msg_id, name in enumerate(("sync_probe", "async_probe")):
        response = await server.dispatch(rpc("tools/call", {"name": name}, msg_id), make_context())
        assert response is not None
        assert response["result"]["content"][0]["text"] == f"request-{msg_id}|tool-{name}"
    assert REQUEST_VAR.get() is None and TOOL_VAR.get() is None  # nothing leaked back


async def test_current_tool_call() -> None:
    server = make_server()
    seen: list[Any] = []
    in_callback: list[Any] = []
    callback_ran = threading.Event()

    @server.middleware
    async def mark(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        assert current_tool_call() is None  # not a tool call yet
        request.state["trace"] = "r"
        return await call_next()

    @server.tool_middleware
    async def fill(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        assert current_tool_call() is call
        call.state["tenant"] = "acme"
        return await call_next()

    @server.tool
    def who() -> str:
        """Reports who called it."""
        call = current_tool_call()
        if call is None:
            return "no call"
        seen.append(call)
        identity = call.identity
        return json.dumps(
            {
                "fingerprint": identity.fingerprint if identity else None,
                "request_id": call.request.request_id,
                "traceparent": call.request.meta.get("traceparent"),
                "state": dict(call.state),
                "client_id": call.client_id,
            }
        )

    @server.tool(timeout=0.05)
    async def late() -> str:
        """Times out; its cancel callback looks for the call."""
        token = current_cancel_token()
        assert token is not None and current_tool_call() is not None

        def callback() -> None:
            in_callback.append(current_tool_call())
            callback_ran.set()

        token.on_cancel(callback)
        await asyncio.sleep(1)
        return "late"

    identity = ClientIdentity(fingerprint="f" * 12, scopes=frozenset())
    message = modern("tools/call", {"name": "who"}, msg_id=9, traceparent="00-abc-def-01")
    response = await server.dispatch(message, make_context(identity=identity, client_id="f" * 12))
    assert response is not None
    assert json.loads(response["result"]["content"][0]["text"]) == {
        "fingerprint": "f" * 12,
        "request_id": 9,
        "traceparent": "00-abc-def-01",
        "state": {"trace": "r", "tenant": "acme"},
        "client_id": "f" * 12,
    }
    assert seen[0].tool.name == "who"
    assert current_tool_call() is None
    await server.dispatch(rpc("tools/call", {"name": "late"}), make_context())
    assert await asyncio.to_thread(callback_ran.wait, 5)
    assert in_callback == [None]
    assert who() == "no call"  # called directly, outside the server


async def test_state_is_shared_within_a_request_only() -> None:
    server = make_server()
    seen: list[Any] = []

    @server.middleware
    async def start(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        assert request.state == {}
        request.state["id"] = request.request_id
        return await call_next()

    @server.tool_middleware
    async def middle(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        seen.append(("middleware", call.state["id"]))
        assert call.state is call.request.state
        return await call_next()

    @server.tool
    def end() -> str:
        """Reads the state."""
        call = current_tool_call()
        assert call is not None
        seen.append(("tool", call.state["id"]))
        return "ok"

    for msg_id in (1, 2):
        await server.dispatch(rpc("tools/call", {"name": "end"}, msg_id), make_context())
    assert seen == [("middleware", 1), ("tool", 1), ("middleware", 2), ("tool", 2)]


# --------------------------------------------------------- eras and caching


async def test_request_info_in_the_initialize_era() -> None:
    server = make_server()
    kept: list[RequestInfo] = []

    @server.middleware
    async def keep(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        kept.append(request)
        return await call_next()

    early = make_context(session_id="s-0")
    await server.dispatch(rpc("tools/list"), early)
    context = make_context(session_id="s-1")
    await server.dispatch(rpc("initialize", {"protocolVersion": "2025-03-26"}, 1), context)
    await server.dispatch(rpc("tools/list", msg_id=2), context)
    before, handshake, listed = kept
    assert before.protocol_version is None and before.session_id == "s-0"
    assert handshake.protocol_version == "2025-03-26" and handshake.method == "initialize"
    assert listed.protocol_version == "2025-03-26"
    assert listed.session_id == "s-1" and listed.stateless is False
    assert listed.client_id == "ip:test" and listed.identity is None
    assert listed.meta == {} and listed.request_id == 2


async def test_request_info_in_the_stateless_era() -> None:
    server = make_server()
    kept: list[RequestInfo] = []

    @server.middleware
    async def keep(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        kept.append(request)
        return await call_next()

    parent = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"
    message = modern(
        "tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, traceparent=parent
    )
    await server.dispatch(message, make_context(session_id="stateless"))
    (request,) = kept
    assert request.stateless and request.session_id is None
    assert request.protocol_version == "2026-07-28"
    assert request.meta["traceparent"] == parent
    assert request.meta["io.modelcontextprotocol/clientInfo"]["name"] == "tests"
    assert request.tool is not None and request.tool.name == "add"
    assert "tool='add'" in repr(request) and "traceparent" not in repr(request)


async def test_tools_list_is_private_with_request_middleware() -> None:
    plain = make_server()
    tool_only = make_server()
    tool_only.tool_middleware(tool_passthrough)
    request_level = make_server()
    request_level.middleware(passthrough)
    authenticated = make_server(auth=APIKeyAuth({KEY: "*"}))
    expected = {
        id(plain): ("public", "public"),
        id(tool_only): ("public", "public"),
        id(request_level): ("private", "public"),
        id(authenticated): ("private", "public"),
    }
    for server in (plain, tool_only, request_level, authenticated):
        listed = await server.dispatch(modern("tools/list"), make_context())
        discovered = await server.dispatch(modern("server/discover"), make_context())
        assert listed is not None and discovered is not None
        got = (listed["result"]["cacheScope"], discovered["result"]["cacheScope"])
        assert got == expected[id(server)]
        assert listed["result"]["ttlMs"] == 0


async def test_dispatch_without_transport_info_reports_custom() -> None:
    server = make_server()
    kept: list[TransportInfo] = []

    @server.middleware
    async def keep(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        kept.append(request.transport)
        return await call_next()

    await server.dispatch(rpc("ping"), make_context())
    mine = TransportInfo(name="websocket", headers={"X-Tenant": "a", "Cookie": "c"})
    await server.dispatch(rpc("ping"), make_context(), transport=mine)
    default, custom = kept
    assert default.name == "custom" and dict(default.headers) == {}
    assert default.client_address is None and default.http_version is None
    assert custom is mine and dict(custom.headers) == {"x-tenant": "a"}


# ------------------------------------------------------------------- memory


class Rows(list):  # type: ignore[type-arg]
    """A result that can be watched with a weak reference."""


async def test_a_finished_call_with_middleware_does_not_keep_its_result_alive() -> None:
    server = make_server()
    produced: list[weakref.ref[Rows]] = []

    @server.tool
    def rows() -> list[int]:
        """A result to watch."""
        result = Rows(range(1000))
        produced.append(weakref.ref(result))
        return result

    @server.middleware
    async def read_request(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        outcome = await call_next()
        assert outcome.tool is not None and outcome.tool.message
        return outcome

    @server.tool_middleware
    async def read_tool(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        outcome = await call_next()
        assert outcome.message.startswith("[0, 1")
        return outcome

    gc.disable()
    try:
        for n in range(20):
            response = await server.dispatch(rpc("tools/call", {"name": "rows"}, n), make_context())
            assert response is not None and response["result"]["isError"] is False
            del response
        await asyncio.sleep(0.05)  # let the last worker thread let go
        assert [ref for ref in produced if ref() is not None] == []
    finally:
        gc.enable()


class Watched(dict[str, Any]):
    """A JSON object that can be watched with a weak reference."""


def watched(
    method: str, params: dict[str, Any], msg_id: int
) -> tuple[Watched, list[weakref.ref[Watched]]]:
    """A request whose message, params and (when given) arguments can be watched."""
    outer = Watched(params)
    refs = [weakref.ref(outer)]
    if "arguments" in params:
        arguments = outer["arguments"] = Watched(params["arguments"])
        refs.append(weakref.ref(arguments))
    message = Watched(jsonrpc="2.0", id=msg_id, method=method, params=outer)
    return message, [weakref.ref(message), *refs]


def only_the_servers_log_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    # pytest attaches handlers that keep every record, traceback included, to
    # loggers that exist when a test starts; only the server's own may stay.
    logger = logging.getLogger("easy_mcp")
    own = [handler for handler in logger.handlers if getattr(handler, "_easy_mcp", False)]
    monkeypatch.setattr(logger, "handlers", own)


async def test_a_cancelled_request_nobody_awaits_is_freed_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A transport that cancels a dispatch and drops it (an SSE stream that
    # closes) leaves the task holding its CancelledError, traceback and all.
    # With the cyclic collector off, nothing in those frames may lead back to
    # the task, or the request stays alive until a full collection.
    only_the_servers_log_handlers(monkeypatch)
    for layer in ("none", "request", "tool"):
        server = make_server()
        started, _ = with_slow_tool(server)
        if layer == "request":
            server.middleware(passthrough)
        elif layer == "tool":
            server.tool_middleware(tool_passthrough)
        refs: list[weakref.ref[Watched]] = []
        gc.disable()
        try:
            for n in range(3):
                started.clear()
                message, watching = watched("tools/call", {"name": "slow", "arguments": {}}, n)
                refs.extend(watching)
                task = asyncio.create_task(server.dispatch(message, make_context()))
                del message
                await asyncio.wait_for(started.wait(), 5)
                task.cancel()
                await asyncio.wait({task})  # never awaited itself
                del task
            await asyncio.sleep(0.01)
            assert len(refs) == 9
            assert [ref for ref in refs if ref() is not None] == [], layer
        finally:
            gc.enable()


async def test_a_cancelled_handshake_nobody_awaits_is_freed_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    only_the_servers_log_handlers(monkeypatch)
    server = make_server()
    entered = asyncio.Event()

    @server.middleware
    async def hold(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "initialize":
            entered.set()
            await asyncio.sleep(30)
        return await call_next()

    refs: list[weakref.ref[Watched]] = []
    gc.disable()
    try:
        for n in range(3):
            entered.clear()
            message, watching = watched("initialize", {"protocolVersion": "2025-11-25"}, n)
            refs.extend(watching)
            task = asyncio.create_task(server.dispatch(message, make_context()))
            del message
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            await asyncio.wait({task})
            del task
        await asyncio.sleep(0.01)
        assert len(refs) == 6
        assert [ref for ref in refs if ref() is not None] == []
    finally:
        gc.enable()


async def test_a_request_served_through_middleware_is_freed_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    only_the_servers_log_handlers(monkeypatch)
    server = make_server()
    server.middleware(passthrough)
    server.tool_middleware(tool_passthrough)
    refs: list[weakref.ref[Watched]] = []
    gc.disable()
    try:
        for n in range(3):
            message, watching = watched(
                "tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, n
            )
            refs.extend(watching)
            response = await server.dispatch(message, make_context())
            assert response is not None and response["result"]["content"][0]["text"] == "3"
            del message, response
        await asyncio.sleep(0.05)  # let the last worker thread let go
        assert len(refs) == 9
        assert [ref for ref in refs if ref() is not None] == []
    finally:
        gc.enable()


# --------------------------------------------------------------- transports

ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "tests", "version": "1.0"},
}


def recording_server(**kwargs: Any) -> tuple[MCPServer, list[RequestInfo]]:
    """A server whose request middleware keeps every RequestInfo it sees."""
    server = make_server(**kwargs)
    seen: list[RequestInfo] = []

    @server.middleware
    async def record(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        seen.append(request)
        return await call_next()

    return server, seen


def holding_server() -> tuple[MCPServer, threading.Event, threading.Event, list[bool]]:
    """A server whose request middleware holds every tools/call until it is cancelled."""
    server = make_server()
    entered = threading.Event()
    cancelled = threading.Event()
    ran: list[bool] = []

    @server.tool
    def touch() -> str:
        """Records that it ran."""
        ran.append(True)
        return "touched"

    @server.middleware
    async def hold(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/call":
            entered.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return await call_next()

    return server, entered, cancelled, ran


def next_data(lines: Iterator[str]) -> str:
    """Read SSE lines until the next ``data:`` payload."""
    for line in lines:
        if line.startswith("data: "):
            return line[len("data: ") :]
    raise AssertionError("SSE stream ended without a data event")


def test_http_transport_info_and_redacted_headers(live_server: LiveServer) -> None:
    server, seen = recording_server(auth=APIKeyAuth({KEY: "*"}))
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        listed = modern("tools/list")
        headers = {**headers_for(listed), "Authorization": f"Bearer {KEY}", "X-Tenant": "acme"}
        assert client.post("/mcp", json=listed, headers=headers).status_code == 200
        init = client.post(
            "/mcp", json=rpc("initialize", INIT), headers={**ACCEPT, "X-API-Key": KEY}
        )
        session = init.headers["mcp-session-id"]
        headers = {**ACCEPT, "X-API-Key": KEY, "MCP-Session-Id": session, "X-Tenant": "acme"}
        listed_again = client.post("/mcp", json=rpc("tools/list", msg_id=2), headers=headers)
        assert listed_again.status_code == 200
    stateless, handshake, in_session = seen
    for request in seen:
        transport = request.transport
        assert transport.name == "streamable-http"
        assert transport.client_address == "127.0.0.1"
        assert isinstance(transport.client_port, int)
        assert transport.http_version == "1.1"
        for credential in ("authorization", "x-api-key", "mcp-session-id", "cookie"):
            assert credential not in transport.headers
        assert KEY not in json.dumps(dict(transport.headers))
        assert request.identity is not None  # the credential was used, then withheld
    assert stateless.stateless and stateless.session_id is None
    assert stateless.transport.headers["x-tenant"] == "acme"
    assert stateless.transport.headers["mcp-method"] == "tools/list"
    assert handshake.session_id == session and in_session.session_id == session
    assert in_session.transport.headers["x-tenant"] == "acme"
    assert in_session.protocol_version == "2025-11-25"


def test_sse_transport_info(live_server: LiveServer) -> None:
    server, seen = recording_server()
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()
            endpoint = next_data(lines)
            session_id = endpoint.split("session_id=", 1)[1]
            posted = client.post(endpoint, json=rpc("ping"), headers={"X-Tenant": "acme"})
            assert posted.status_code == 202
            assert json.loads(next_data(lines))["result"] == {}
    (request,) = seen
    assert request.transport.name == "sse"
    assert request.transport.headers["x-tenant"] == "acme"
    assert request.transport.client_address == "127.0.0.1"
    assert request.session_id == session_id
    assert session_id not in json.dumps(dict(request.transport.headers))


async def test_stdio_transport_info() -> None:
    server, seen = recording_server()
    stdin = io.BytesIO(json.dumps(rpc("ping")).encode() + b"\n")
    await StdioTransport(server, stdin=stdin, stdout=io.BytesIO()).serve()
    (request,) = seen
    assert request.transport.name == "stdio"
    assert dict(request.transport.headers) == {}
    assert request.transport.client_address is None and request.transport.http_version is None
    assert request.session_id is not None and request.session_id.startswith("stdio-")


def test_http_status_of_refusals(live_server: LiveServer) -> None:
    server = make_server()
    # The codes the stateless revision defines, which it answers with HTTP 400.
    defined = {
        3: ProtocolError(
            "Missing required client capability",
            code=-32021,
            data={"requiredCapabilities": {"sampling": {}}},
        ),
        4: ProtocolError(
            "Unsupported protocol version",
            code=-32022,
            data={"supported": ["2026-07-28"], "requested": "2026-07-28"},
        ),
        5: ProtocolError("Header mismatch: not from this proxy", code=-32020),
    }

    @server.middleware
    async def refuse(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/list":
            raise ProtocolError("Not here", code=METHOD_NOT_FOUND)
        if request.request_id in defined:
            raise defined[request.request_id]
        if request.method == "tools/call":
            raise AuthenticationError("Who are you?")
        return await call_next()

    base = live_server(server)
    arguments = {"name": "add", "arguments": {"a": 1, "b": 1}}
    with httpx.Client(base_url=base, timeout=10) as client:
        listed = modern("tools/list")
        response = client.post("/mcp", json=listed, headers=headers_for(listed))
        assert response.status_code == 404
        assert response.json()["error"]["code"] == METHOD_NOT_FOUND
        call = modern("tools/call", arguments)
        response = client.post("/mcp", json=call, headers=headers_for(call))
        assert response.status_code == 200
        assert response.json()["error"] == {"code": -32001, "message": "Who are you?"}
        for msg_id, refusal in defined.items():
            call = modern("tools/call", arguments, msg_id)
            response = client.post("/mcp", json=call, headers=headers_for(call))
            assert response.status_code == 400, refusal.code
            error = {"code": refusal.code, "message": str(refusal)}
            if refusal.data is not None:
                error["data"] = refusal.data
            assert response.json() == {"jsonrpc": "2.0", "id": msg_id, "error": error}
        init = client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        session = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}
        response = client.post("/mcp", json=rpc("tools/call", arguments, 2), headers=session)
        assert response.status_code == 200
        assert response.json()["error"]["code"] == -32001
        # The session era defines none of them: its answers stay 200.
        response = client.post("/mcp", json=rpc("tools/call", arguments, 3), headers=session)
        assert response.status_code == 200
        assert response.json()["error"]["code"] == -32021


def test_http_initialize_refused_creates_no_session(live_server: LiveServer) -> None:
    server = make_server()

    @server.middleware
    async def closed(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "initialize":
            raise AuthenticationError("Closed for maintenance")
        return await call_next()

    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        refused = client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        assert refused.status_code == 200
        assert refused.json()["error"]["code"] == -32001
        assert "mcp-session-id" not in refused.headers
        guessed = client.post(
            "/mcp", json=rpc("ping", msg_id=2), headers={**ACCEPT, "MCP-Session-Id": "guess"}
        )
        assert guessed.status_code == 404
    assert server._transport is not None
    assert server._transport._sessions == {}  # type: ignore[attr-defined]


def test_http_stateless_disconnect_cancels_middleware(live_server: LiveServer) -> None:
    server, entered, cancelled, ran = holding_server()
    base = live_server(server)
    call = modern("tools/call", {"name": "touch"})

    def fire() -> None:
        try:
            with httpx.Client(base_url=base, timeout=0.5) as client:
                client.post("/mcp", json=call, headers=headers_for(call))
        except httpx.TimeoutException:
            pass  # the client gives up and closes the connection

    thread = threading.Thread(target=fire)
    thread.start()
    assert entered.wait(5)
    thread.join(5)
    began = time.monotonic()
    assert cancelled.wait(2)  # noticed within the transport's poll interval
    assert time.monotonic() - began < 1.5
    assert ran == []


async def test_http_session_delete_cancels_middleware(live_server: LiveServer) -> None:
    server, entered, cancelled, ran = holding_server()
    base = live_server(server)
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        init = await client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        session = init.headers["mcp-session-id"]
        headers = {**ACCEPT, "MCP-Session-Id": session}
        call = asyncio.create_task(
            client.post("/mcp", json=rpc("tools/call", {"name": "touch"}, 2), headers=headers)
        )
        assert await asyncio.to_thread(entered.wait, 5)
        deleted = await client.delete("/mcp", headers={"MCP-Session-Id": session})
        assert deleted.status_code == 204
        assert (await asyncio.wait_for(call, 5)).status_code == 202  # no response, per MCP
    assert await asyncio.to_thread(cancelled.wait, 2)
    assert ran == []


def test_http_session_disconnect_cancels_nothing(live_server: LiveServer, logs: LogCapture) -> None:
    server = make_server()
    finished = {"tools/call": threading.Event(), "initialize": threading.Event()}
    cancelled: list[str] = []

    @server.tool
    async def lengthy() -> str:
        """Takes a second."""
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            cancelled.append("tools/call")
            raise
        finished["tools/call"].set()
        return "finished"

    @server.middleware
    async def slow_handshake(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.request_id != "held":
            return await call_next()
        try:
            await asyncio.sleep(1.0)  # a remote check that takes a second
        except asyncio.CancelledError:
            cancelled.append("initialize")
            raise
        outcome = await call_next()
        finished["initialize"].set()
        return outcome

    base = live_server(server)
    init = httpx.post(f"{base}/mcp", json=rpc("initialize", INIT), headers=ACCEPT, timeout=10)
    session = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}

    def give_up(message: dict[str, Any], headers: dict[str, str]) -> None:
        try:
            httpx.post(f"{base}/mcp", json=message, headers=headers, timeout=0.3)
        except httpx.TimeoutException:
            pass  # the client gives up and closes the connection

    # In this era a closed connection is not a cancel (only the stateless one is).
    give_up(rpc("tools/call", {"name": "lengthy"}, 2), session)
    assert finished["tools/call"].wait(5)
    give_up(rpc("initialize", INIT, "held"), ACCEPT)
    assert finished["initialize"].wait(5)
    assert cancelled == []
    assert logs.events("request_abandoned") == [] and logs.events("tool_cancelled") == []


async def test_http_session_delete_stops_a_call_not_yet_in_flight() -> None:
    from easy_mcp import StreamableHTTPTransport

    server = make_server()
    ran: list[bool] = []

    @server.tool
    async def touch() -> str:
        """Records that it ran."""
        ran.append(True)
        return "touched"

    transport = StreamableHTTPTransport(server)
    app = transport.build_app()  # its lifespan never runs here
    gaps = 0
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        # The DELETE lands at each point of the POST's way in, the moment its
        # dispatch is scheduled but has yet to put the call in flight included.
        for steps in range(8):
            init = await client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
            session_id = init.headers["mcp-session-id"]
            headers = {**ACCEPT, "MCP-Session-Id": session_id}
            ran.clear()
            post = client.post(
                "/mcp", json=rpc("tools/call", {"name": "touch"}, 2), headers=headers
            )
            call = asyncio.ensure_future(post)
            for _ in range(steps):
                await asyncio.sleep(0)
            session = transport._sessions.get(session_id)
            if session is not None and session.active and not session.context.in_flight:
                gaps += 1
            deleted = await client.delete("/mcp", headers={"MCP-Session-Id": session_id})
            assert deleted.status_code == 204
            ran_before = list(ran)
            posted = await call
            if not ran_before:
                # The session ended before its call started: it never does.
                await asyncio.sleep(0.05)
                assert ran == [], steps
                assert posted.status_code in (202, 404), (steps, posted.text)
    assert gaps  # the case that matters was reached
    assert transport._sessions == {}


def test_sse_stream_close_cancels_middleware(live_server: LiveServer) -> None:
    server, entered, cancelled, ran = holding_server()
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()  # kept: a dropped iterator closes the stream
            endpoint = next_data(lines)
            posted = client.post(endpoint, json=rpc("tools/call", {"name": "touch"}))
            assert posted.status_code == 202
            assert entered.wait(5)
        # Leaving the block closed the stream.
    assert cancelled.wait(5)
    assert ran == []


async def test_stdio_shutdown_cancels_middleware() -> None:
    server = make_server()
    entered = asyncio.Event()
    cleaned: list[str] = []

    @server.middleware
    async def hold(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "tools/call":
            entered.set()
            try:
                await asyncio.sleep(30)
            finally:
                await asyncio.sleep(0.01)  # cleanup that awaits, briefly
                cleaned.append("finally")
        return await call_next()

    read_end, write_end = os.pipe()
    stdin = os.fdopen(read_end, "rb")
    writer = os.fdopen(write_end, "wb")
    transport = StdioTransport(server, stdin=stdin, stdout=io.BytesIO(), shutdown_timeout=0.2)
    serving = asyncio.create_task(transport.serve())
    try:
        call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
        writer.write(json.dumps(call).encode() + b"\n")
        writer.flush()
        await asyncio.wait_for(entered.wait(), 5)
        writer.close()  # EOF: the held request is cancelled after shutdown_timeout
        await asyncio.wait_for(serving, 5)
        assert cleaned == ["finally"]  # before serve() returned
    finally:
        if not writer.closed:
            writer.close()
        stdin.close()


def run_http(server: MCPServer) -> tuple[str, threading.Thread]:
    """Start ``server.run("http")`` on a free port in a thread; returns its URL and the thread."""
    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server.port = port
    serving = threading.Thread(target=server.run, args=("http",), daemon=True)
    serving.start()
    base = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 10
    while True:
        try:
            httpx.get(f"{base}/healthz", timeout=1)
            return base, serving
        except httpx.TransportError:
            assert time.monotonic() < deadline, "the server never came up"
            time.sleep(0.05)


def stop_http(server: MCPServer, serving: threading.Thread) -> None:
    """Stop what :func:`run_http` started; shutdown must not wait for what still runs."""
    try:
        server.stop()
        serving.join(5)
        assert not serving.is_alive(), "shutdown waited for a running request"
    finally:
        if serving.is_alive():
            server._transport._uvicorn.force_exit = True  # type: ignore[union-attr]
            serving.join(5)


Answer = tuple[int, str | None, Any]


def answer(response: httpx.Response) -> Answer:
    """Status, content type and JSON body (``None`` when empty) of an HTTP response."""
    body = response.json() if response.content else None
    return response.status_code, response.headers.get("content-type"), body


def shutting_down(msg_id: Any) -> Answer:
    """The answer to a request that shutdown stopped: a JSON-RPC error, retry shortly."""
    error = {
        "code": SERVER_BUSY,
        "message": "Server is shutting down; retry shortly",
        "data": {"reason": "shutdown"},
    }
    return 503, "application/json", {"jsonrpc": "2.0", "id": msg_id, "error": error}


@pytest.fixture
def short_shutdown_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown lets running requests finish for 0.2 s rather than 5 s."""
    from easy_mcp.transport import streamable_http

    monkeypatch.setattr(streamable_http, "_SHUTDOWN_GRACE_SECONDS", 0.2)


@pytest.mark.usefixtures("short_shutdown_grace")
@pytest.mark.parametrize("era", ["session", "stateless", "handshake", "notification"])
def test_http_shutdown_cancels_middleware(era: str) -> None:
    server, entered, cancelled, ran = holding_server()
    held = {"handshake": "initialize", "notification": "notifications/initialized"}.get(era)

    @server.middleware
    async def hold_more(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == held:
            entered.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return await call_next()

    base, serving = run_http(server)
    answers: list[Answer] = []
    session_ids: list[str | None] = []

    def fire() -> None:
        # The client stays connected, waiting for its answer.
        try:
            with httpx.Client(base_url=base, timeout=20) as client:
                if era == "stateless":
                    call = modern("tools/call", {"name": "touch"})
                    answers.append(
                        answer(client.post("/mcp", json=call, headers=headers_for(call)))
                    )
                    return
                init = client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
                answers.append(answer(init))
                session_ids.append(init.headers.get("mcp-session-id"))
                if era in ("session", "notification"):
                    headers = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}
                    if era == "session":
                        message = rpc("tools/call", {"name": "touch"}, 2)
                    else:
                        message = notification("notifications/initialized")
                    answers.append(answer(client.post("/mcp", json=message, headers=headers)))
        except httpx.TransportError:
            answers.append((0, None, None))

    client = threading.Thread(target=fire, daemon=True)
    client.start()
    assert entered.wait(5)
    stop_http(server, serving)
    assert cancelled.is_set()
    client.join(5)
    # A request shutdown stopped is still answered, with an error the client
    # can retry; a handshake that never finished opens no session.  A
    # notification gets what it always gets.
    if era == "stateless":
        assert answers == [shutting_down(1)]
    elif era == "handshake":
        assert answers == [shutting_down(1)] and session_ids == [None]
    else:
        assert [status for status, _, _ in answers[:1]] == [200]
        assert answers[1:] == [shutting_down(2) if era == "session" else (202, None, None)]
    assert ran == []


@pytest.mark.parametrize("era", ["session", "stateless"])
def test_http_shutdown_answers_every_running_request(
    era: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from easy_mcp.transport import streamable_http

    monkeypatch.setattr(streamable_http, "_SHUTDOWN_GRACE_SECONDS", 1.0)
    server = make_server()  # no middleware: this is how every app shuts down
    started = {"brief": threading.Event(), "slow": threading.Event()}

    @server.tool
    async def brief() -> str:
        """Finishes shortly."""
        started["brief"].set()
        await asyncio.sleep(0.3)
        return "finished"

    @server.tool
    async def slow() -> str:
        """Takes longer than shutdown waits."""
        started["slow"].set()
        await asyncio.sleep(30)
        return "too late"

    base, serving = run_http(server)
    headers: dict[str, str] = {}
    if era == "session":
        init = httpx.post(f"{base}/mcp", json=rpc("initialize", INIT), headers=ACCEPT, timeout=10)
        headers = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}
    answers: dict[str, Answer] = {}

    def fire(name: str, msg_id: int) -> None:
        if era == "stateless":
            message = modern("tools/call", {"name": name}, msg_id)
            sent = headers_for(message)
        else:
            message, sent = rpc("tools/call", {"name": name}, msg_id), headers
        try:
            answers[name] = answer(
                httpx.post(f"{base}/mcp", json=message, headers=sent, timeout=20)
            )
        except httpx.TransportError:
            answers[name] = (0, None, None)

    clients = [
        threading.Thread(target=fire, args=(name, n), daemon=True)
        for n, name in enumerate(("brief", "slow"), start=7)
    ]
    for thread in clients:
        thread.start()
    assert started["brief"].wait(5) and started["slow"].wait(5)
    stop_http(server, serving)
    for thread in clients:
        thread.join(5)
    # A call that finishes while shutdown waits gets its result, as before;
    # one that does not is cancelled, and answered.
    status, content_type, body = answers["brief"]
    assert (status, content_type) == (200, "application/json")
    assert body["result"]["content"][0]["text"] == "finished"
    assert answers["slow"] == shutting_down(8)


async def test_http_requests_arriving_during_shutdown_are_answered() -> None:
    from easy_mcp import StreamableHTTPTransport

    server = make_server()
    ran: list[bool] = []

    @server.tool
    def touch() -> str:
        """Records that it ran."""
        ran.append(True)
        return "touched"

    transport = StreamableHTTPTransport(server)
    app = transport.build_app()  # its lifespan never runs here
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        init = await client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        session = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}
        transport._closing = True  # shutdown has begun; running requests may still finish
        call = modern("tools/call", {"name": "touch"}, 3)
        stateless = await client.post("/mcp", json=call, headers=headers_for(call))
        assert answer(stateless) == shutting_down(3)
        in_session = await client.post(
            "/mcp", json=rpc("tools/call", {"name": "touch"}, 4), headers=session
        )
        assert answer(in_session) == shutting_down(4)
        handshake = await client.post("/mcp", json=rpc("initialize", INIT, 5), headers=ACCEPT)
        assert answer(handshake) == shutting_down(5)
        assert "mcp-session-id" not in handshake.headers
        cancel = notification("notifications/cancelled", {"requestId": 4})
        assert answer(await client.post("/mcp", json=cancel, headers=session)) == (202, None, None)
    assert ran == []
    assert stateless.headers["retry-after"] == "1"
    assert len(transport._sessions) == 1  # the one opened before shutdown began


async def test_http_cancel_during_shutdown_stops_the_call(monkeypatch: pytest.MonkeyPatch) -> None:
    from easy_mcp import StreamableHTTPTransport
    from easy_mcp.transport import streamable_http

    monkeypatch.setattr(streamable_http, "_SHUTDOWN_GRACE_SECONDS", 2.0)
    server = make_server()
    started, cancelled = with_slow_tool(server)
    transport = StreamableHTTPTransport(server)
    app = transport.build_app()  # its lifespan never runs here
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        init = await client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        session = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}
        call = asyncio.create_task(
            client.post("/mcp", json=rpc("tools/call", {"name": "slow"}, 2), headers=session)
        )
        await asyncio.wait_for(started.wait(), 5)
        closing = asyncio.create_task(transport.close_streams())
        while not transport._closing:
            await asyncio.sleep(0)
        # The client cancels its call while shutdown waits for it: the cancel
        # takes effect at once, and the call gets no answer, as for any cancel.
        cancel = notification("notifications/cancelled", {"requestId": 2})
        assert answer(await client.post("/mcp", json=cancel, headers=session)) == (202, None, None)
        await asyncio.wait_for(cancelled.wait(), 1)
        assert answer(await asyncio.wait_for(call, 1)) == (202, None, None)
        await asyncio.wait_for(closing, 1)  # long before the 2 s grace is up
    assert transport._sessions == {}


def test_http_shutdown_refuses_legacy_sse_streams_opened_during_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from easy_mcp import StreamableHTTPTransport
    from easy_mcp.transport import streamable_http

    monkeypatch.setattr(streamable_http, "_SHUTDOWN_GRACE_SECONDS", 1.0)
    server = make_server()

    @server.tool
    async def slow() -> str:
        """Takes longer than shutdown waits."""
        started.set()
        await asyncio.sleep(30)
        return "too late"

    started = threading.Event()
    base, serving = run_http(server)
    transport = server._transport
    assert isinstance(transport, StreamableHTTPTransport)
    answers: list[Answer] = []
    streams: list[tuple[int, str | None]] = []

    def call() -> None:
        message = modern("tools/call", {"name": "slow"}, 7)
        try:
            sent = httpx.post(f"{base}/mcp", json=message, headers=headers_for(message), timeout=20)
            answers.append(answer(sent))
        except httpx.TransportError:
            answers.append((0, None, None))

    def reconnect() -> None:
        # A legacy client connecting (or reconnecting, as EventSource does)
        # while shutdown waits for the call; it reads what it gets to the end.
        try:
            with httpx.Client(base_url=base, timeout=20) as client:
                with client.stream("GET", "/sse") as stream:
                    for _ in stream.iter_lines():
                        pass
                    streams.append((stream.status_code, stream.headers.get("retry-after")))
        except httpx.TransportError:
            streams.append((0, None))

    caller = threading.Thread(target=call, daemon=True)
    legacy = threading.Thread(target=reconnect, daemon=True)
    caller.start()
    assert started.wait(5)
    try:
        server.stop()
        deadline = time.monotonic() + 5
        while not transport._closing:
            assert time.monotonic() < deadline, "shutdown never began"
            time.sleep(0.01)
        legacy.start()
        serving.join(3)  # the 1 s grace, and some
        assert not serving.is_alive(), "shutdown waited for a stream opened during it"
    finally:
        if serving.is_alive():
            transport._uvicorn.force_exit = True
            serving.join(5)
    legacy.join(5)
    caller.join(5)
    assert streams == [(503, "1")]
    assert answers == [shutting_down(7)]


def test_sse_refuses_new_streams_and_messages_once_shutdown_begins(
    live_server: LiveServer,
) -> None:
    from easy_mcp import SSETransport

    transport = SSETransport(make_server())
    app = transport.build_app()
    base = live_server(app)
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()  # kept: its end would close the stream
            endpoint = next_data(lines)
            transport._closing = True  # shutdown is closing the streams
            posted = client.post(endpoint, json=rpc("ping"))
            with client.stream("GET", "/sse") as opened:
                refused = (opened.status_code, opened.headers.get("retry-after"))
    assert (posted.status_code, posted.headers.get("retry-after")) == (503, "1")
    assert refused == (503, "1")
    # Served again, the transport opens streams anew.
    base = live_server(app)
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("GET", "/sse") as stream:
            assert stream.status_code == 200
            assert next_data(stream.iter_lines()).startswith("/messages?session_id=")


async def test_stdio_serves_both_eras_to_middleware() -> None:
    server = make_server()
    kept: list[tuple[Any, ...]] = []

    @server.middleware
    async def keep(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        session = request.session_id
        kept.append(
            (
                request.method,
                request.stateless,
                session.startswith("stdio-") if session else None,
                request.protocol_version,
                request.transport.name,
            )
        )
        return await call_next()

    lines = [
        modern("server/discover", msg_id=1),
        modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, msg_id=2),
        rpc("initialize", {"protocolVersion": "2025-11-25"}, msg_id=3),
        rpc("tools/list", msg_id=4),
    ]
    read_end, write_end = os.pipe()
    stdin = os.fdopen(read_end, "rb")
    writer = os.fdopen(write_end, "wb")
    stdout = io.BytesIO()
    serving = asyncio.create_task(StdioTransport(server, stdin=stdin, stdout=stdout).serve())
    try:
        writer.write(b"".join(json.dumps(m).encode() + b"\n" for m in lines[:3]))
        writer.flush()
        deadline = time.monotonic() + 5
        while stdout.getvalue().count(b"\n") < 3:
            assert time.monotonic() < deadline, "no answer to initialize"
            await asyncio.sleep(0.01)
        # A session-era request, once initialize has been answered.
        writer.write(json.dumps(lines[3]).encode() + b"\n")
        writer.close()
        await asyncio.wait_for(serving, 5)
    finally:
        if not writer.closed:
            writer.close()
        stdin.close()
    responses = {r["id"]: r for r in map(json.loads, stdout.getvalue().splitlines())}
    assert responses[2]["result"]["content"][0]["text"] == "2"
    assert sorted(kept, key=str) == sorted(
        [
            ("server/discover", True, None, "2026-07-28", "stdio"),
            ("tools/call", True, None, "2026-07-28", "stdio"),
            ("initialize", False, True, "2025-11-25", "stdio"),
            ("tools/list", False, True, "2025-11-25", "stdio"),
        ],
        key=str,
    )
