"""Request and tool middleware, and the request path they plug into."""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import gc
import json
import logging
import re
import threading
import time
import warnings
import weakref
from collections.abc import Callable
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
    assert await server.dispatch(rpc("notifications/unknown", msg_id=3), context) is None


async def test_a_legacy_request_naming_a_notification_acts_as_one() -> None:
    server = make_server()
    started, cancelled = with_slow_tool(server)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 7), context))
    await asyncio.wait_for(started.wait(), 5)
    cancel = rpc("notifications/cancelled", {"requestId": 7}, msg_id=8)
    assert await server.dispatch(cancel, context) is None
    assert await asyncio.wait_for(call, 5) is None
    assert cancelled.is_set()


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

    for bad in (sync_function, lambda request, call_next: call_next(), SyncCallable()):
        with pytest.raises(TypeError, match="must be an async function"):
            server.middleware(bad)
        with pytest.raises(TypeError, match="must be an async function"):
            server.tool_middleware(bad)

    class AsyncCallable:
        async def __call__(self, request: Any, call_next: Any) -> Any:
            return await call_next()

    async def labelled(label: str, request: Any, call_next: Any) -> Any:
        return await call_next()

    server.middleware(AsyncCallable())
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
    held: list[weakref.ref[Payload]] = []

    @request_level.middleware
    async def failing(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        payload = Payload()
        held.append(weakref.ref(payload))
        try:
            raise KeyError("first")
        except KeyError as exc:
            raise RuntimeError("then this") from exc

    @tool_level.tool_middleware
    async def refusing(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        payload = Payload()
        held.append(weakref.ref(payload))
        raise ToolError("refused")

    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}})
    gc.disable()
    try:
        for n in range(3):
            response = await request_level.dispatch({**call, "id": n}, make_context())
            assert response is not None and response["error"]["code"] == INTERNAL_ERROR
            response = await tool_level.dispatch({**call, "id": n}, make_context())
            assert response is not None and response["result"]["isError"] is True
        await asyncio.sleep(0)
        assert len(held) == 6
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
        views.append(call.arguments)  # snapshot before the tool runs
        outcome = await call_next()
        views.append(call.arguments)
        return outcome

    arguments = {"point": {"x": 1, "y": 2}, "tags": ["a"]}
    response = await server.dispatch(
        rpc("tools/call", {"name": "where", "arguments": arguments}), make_context()
    )
    assert response is not None and response["result"]["isError"] is False, response
    assert received == [("Point", 1, ["a"])]
    before, after = views
    assert before is after
    assert dict(before["point"]) == {"x": 1, "y": 2} and before["tags"] == ("a",)


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


async def test_cancel_reaches_middleware_before_the_tool_starts(logs: LogCapture) -> None:
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


async def test_a_middleware_that_swallows_cancellation_is_overruled(logs: LogCapture) -> None:
    for style in ("return", "raise"):
        server = make_server()
        started, cancelled = with_slow_tool(server)

        @server.middleware
        async def stubborn(
            request: RequestInfo, call_next: RequestNext, style: str = style
        ) -> RequestOutcome:
            try:
                return await call_next()
            except asyncio.CancelledError:
                if style == "raise":
                    raise RuntimeError("cancelled, apparently") from None
                return None  # type: ignore[return-value]

        context = make_context()
        call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 1), context))
        await asyncio.wait_for(started.wait(), 5)
        await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
        assert await asyncio.wait_for(call, 5) is None, style  # still no response
        assert cancelled.is_set()

        # A cancellation of the caller still reaches the caller.
        started.clear()
        caller = asyncio.create_task(
            server.dispatch(rpc("tools/call", {"name": "slow"}, 2), context)
        )
        await asyncio.wait_for(started.wait(), 5)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller

    def swallowed() -> list[logging.LogRecord]:
        return [r for r in logs.records if "swallowed a cancellation" in r.getMessage()]

    deadline = time.monotonic() + 5
    while len(swallowed()) < 4 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)  # the caller's cancel is unwinding in its own task
    assert len(swallowed()) == 4 and {r.levelname for r in swallowed()} == {"WARNING"}
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
    for refusal in (AuthenticationError("no"), RuntimeError("broken"), None):
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
        if isinstance(refusal, AuthenticationError):
            assert failed == []
            (debug,) = [r for r in records if "cannot be refused" in r.getMessage()]
            assert debug.levelname == "DEBUG"
        else:
            (event,) = failed
            assert event["stage"] == "observe" and event["method"] == "notifications/cancelled"


async def test_discover_cannot_be_refused(logs: LogCapture) -> None:
    for refusal in (AuthenticationError("no"), RuntimeError("broken"), None):
        server = make_server()

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
