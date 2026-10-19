"""OpenTelemetry: one SERVER span per dispatched message, its parent, attributes and outcome.

Every server here traces through providers of the test's own (the ``otel``
fixture), exporting to memory; the global providers are never touched.
Skipped without ``opentelemetry-sdk``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest
from conftest import (
    LogCapture,
    Otel,
    Pushed,
    headers_for,
    listen,
    make_context,
    modern,
    notification,
    parse_exposition,
    rpc,
)

from easy_mcp import APIKeyAuth, MCPServer, ToolError
from easy_mcp.cancellation import current_cancel_token
from easy_mcp.exceptions import (
    FORBIDDEN,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    RESOURCE_NOT_FOUND_LEGACY,
    SERVER_BUSY,
    SESSION_LIMIT_EXCEEDED,
    TOOL_TIMEOUT,
    AuthorizationError,
    ProtocolError,
)
from easy_mcp.logging import JSONLogFormatter
from easy_mcp.middleware import RequestInfo, RequestNext, RequestOutcome, TransportInfo
from easy_mcp.protocol import is_modern_request
from easy_mcp.telemetry import Telemetry
from easy_mcp.telemetry._otel import META_GETTER, OtelBridge, load_bridge
from easy_mcp.telemetry.prometheus import DURATION_BUCKETS
from easy_mcp.transport.streamable_http import StreamableHTTPTransport

pytest.importorskip("opentelemetry.sdk")

from opentelemetry import baggage, propagate  # noqa: E402
from opentelemetry.context import Context  # noqa: E402
from opentelemetry.propagators.textmap import (  # noqa: E402
    TextMapPropagator,
    default_getter,
    default_setter,
)
from opentelemetry.trace import SpanKind, StatusCode  # noqa: E402
from opentelemetry.trace.propagation.tracecontext import (  # noqa: E402
    TraceContextTextMapPropagator,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
KEY = "tracing-test-key-" + "k" * 15
CANARY = "do-not-leak-" + "s" * 10

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
CLIENT_SPAN = "00f067aa0ba902b7"
TRACEPARENT = f"00-{TRACE_ID}-{CLIENT_SPAN}-01"
HEADER_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
HEADER_SPAN = "00f067aa0ba90200"
HEADER_TRACEPARENT = f"00-{HEADER_TRACE}-{HEADER_SPAN}-01"

ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}
DURATION = "mcp_server_operation_duration_seconds"


def hex_id(value: int, width: int) -> str:
    return format(value, f"0{width}x")


def add_tool(server: MCPServer) -> None:
    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b


def call(name: str = "add", arguments: dict[str, Any] | None = None, msg_id: Any = 1) -> Any:
    args = {"a": 1, "b": 2} if arguments is None and name == "add" else (arguments or {})
    return rpc("tools/call", {"name": name, "arguments": args}, msg_id)


def only(otel: Otel, name: str) -> Any:
    """The one ended span named *name*."""
    found = [span for span in otel.spans() if span.name == name]
    assert len(found) == 1, [span.name for span in otel.spans()]
    return found[0]


def asgi(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1")


# --------------------------------------------------------------- spans


async def test_one_server_span_per_message(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    context = make_context()
    await server.dispatch(rpc("initialize", INIT, "init"), context)
    await server.dispatch(notification("notifications/initialized"), context)
    await server.dispatch(rpc("tools/list", msg_id=2), context)
    await server.dispatch(rpc("ping", msg_id=3), context)
    await server.dispatch(call(msg_id=4), context)
    await server.dispatch(modern("server/discover", msg_id=5), make_context())
    names = [span.name for span in otel.spans()]
    assert names == [
        "initialize",
        "notifications/initialized",
        "tools/list",
        "ping",
        "tools/call add",
        "server/discover",
    ]
    assert {span.kind for span in otel.spans()} == {SpanKind.SERVER}
    scope = otel.spans()[0].instrumentation_scope
    assert scope.name == "easy_mcp" and scope.version == server.version


async def test_tools_call_span_attributes(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    context = make_context(client_id="ip:10.1.2.3", session_id="sess-1")
    stdio = TransportInfo("stdio")
    await server.dispatch(rpc("initialize", INIT, "init"), context, transport=stdio)
    await server.dispatch(call(msg_id=7), context, transport=stdio)
    span = only(otel, "tools/call add")
    assert dict(span.attributes) == {
        "mcp.method.name": "tools/call",
        "gen_ai.operation.name": "execute_tool",
        "gen_ai.tool.name": "add",
        "jsonrpc.request.id": "7",
        "mcp.protocol.version": "2025-11-25",
        "mcp.session.id": hashlib.sha256(b"sess-1").hexdigest()[:32],
        "network.transport": "pipe",
        "easy_mcp.transport": "stdio",
        "easy_mcp.client_id": "ip:10.1.2.3",
        "easy_mcp.status": "ok",
    }
    assert span.status.status_code is StatusCode.UNSET
    assert span.parent is None  # nothing propagated: a root span


async def test_meta_traceparent_is_parent_stateless(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    message = modern(
        "tools/call",
        {"name": "add", "arguments": {"a": 1, "b": 2}},
        traceparent=TRACEPARENT,
        tracestate="vendor=opaque",
    )
    response = await server.dispatch(message, make_context(session_id="stateless"))
    assert response is not None and response["result"]["resultType"] == "complete"
    span = only(otel, "tools/call add")
    assert hex_id(span.context.trace_id, 32) == TRACE_ID
    assert span.parent is not None and span.parent.is_remote
    assert hex_id(span.parent.span_id, 16) == CLIENT_SPAN
    assert span.context.trace_state.get("vendor") == "opaque"
    assert span.attributes["mcp.protocol.version"] == "2026-07-28"
    assert "mcp.session.id" not in span.attributes  # no session statelessly


async def test_meta_traceparent_is_parent_handshake_era(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    context = make_context()
    await server.dispatch(rpc("initialize", INIT, "init"), context)
    message = call()
    message["params"]["_meta"] = {"traceparent": TRACEPARENT}
    response = await server.dispatch(message, context)
    assert response is not None and "resultType" not in response["result"]
    span = only(otel, "tools/call add")
    assert hex_id(span.context.trace_id, 32) == TRACE_ID
    assert hex_id(span.parent.span_id, 16) == CLIENT_SPAN
    assert span.attributes["mcp.protocol.version"] == "2025-11-25"


async def test_trace_keys_do_not_change_the_era(otel: Otel) -> None:
    trace_meta = {"traceparent": TRACEPARENT, "tracestate": "a=b", "baggage": "k=v"}
    assert not is_modern_request("tools/list", {"_meta": trace_meta})
    server = otel.server()
    response = await server.dispatch(rpc("tools/list", {"_meta": trace_meta}), make_context())
    assert response is not None and "resultType" not in response["result"]


@pytest.mark.parametrize(
    "value",
    [
        f"00-{'0' * 32}-{CLIENT_SPAN}-01",  # all-zero trace id
        f"00-{TRACE_ID}-{'0' * 16}-01",  # all-zero span id
        f"ff-{TRACE_ID}-{CLIENT_SPAN}-01",  # a version W3C forbids
        f"00-{TRACE_ID}-{CLIENT_SPAN}-01-extra",  # version 00 has nothing after its flags
        "not a traceparent",
        12345,
        [TRACEPARENT],
        {"traceparent": TRACEPARENT},
        TRACEPARENT + " " * 9000,  # longer than any value read
    ],
)
async def test_invalid_trace_context_is_ignored(otel: Otel, value: Any) -> None:
    server = otel.server()
    add_tool(server)
    message = call()
    message["params"]["_meta"] = {"traceparent": value, "baggage": "k=v"}
    response = await server.dispatch(message, make_context())
    assert response is not None and response["result"]["content"][0]["text"] == "3"
    span = only(otel, "tools/call add")
    assert span.parent is None
    assert span.status.status_code is StatusCode.UNSET


def test_only_w3c_keys_are_read_from_meta() -> None:
    carrier = {
        "traceparent": TRACEPARENT,
        "tracestate": "a=b",
        "baggage": "k=v",
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "uber-trace-id": "x",
        "b3": "y",
    }
    assert META_GETTER.get(carrier, "traceparent") == [TRACEPARENT]
    assert META_GETTER.get(carrier, "uber-trace-id") is None
    assert META_GETTER.get(carrier, "b3") is None
    assert META_GETTER.get(carrier, "io.modelcontextprotocol/protocolVersion") is None
    assert META_GETTER.keys(carrier) == ["traceparent", "tracestate", "baggage"]
    assert META_GETTER.get({"traceparent": "x" * 8193}, "traceparent") is None
    assert META_GETTER.get({"traceparent": "x" * 8192}, "traceparent") == ["x" * 8192]
    assert META_GETTER.keys({"traceparent": 1, "baggage": "k=v"}) == ["baggage"]


# ------------------------------------------------- parents over HTTP


async def post_stateless(server: MCPServer, message: dict[str, Any], **headers: Any) -> Any:
    async with asgi(server.build_app()) as http:
        return await http.post("/mcp", json=message, headers={**headers_for(message), **headers})


async def test_http_traceparent_header_is_parent_when_meta_absent(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    message = modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
    answer = await post_stateless(server, message, traceparent=HEADER_TRACEPARENT)
    assert answer.status_code == 200
    span = only(otel, "tools/call add")
    assert hex_id(span.context.trace_id, 32) == HEADER_TRACE
    assert hex_id(span.parent.span_id, 16) == HEADER_SPAN
    assert span.links == ()
    attributes = span.attributes
    assert attributes["network.transport"] == "tcp"
    assert attributes["network.protocol.name"] == "http"
    assert attributes["network.protocol.version"] == "1.1"
    assert attributes["client.address"] == "127.0.0.1"
    assert isinstance(attributes["client.port"], int)
    assert attributes["easy_mcp.transport"] == "streamable-http"


async def test_meta_wins_and_header_becomes_link(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    message = modern(
        "tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, traceparent=TRACEPARENT
    )
    answer = await post_stateless(server, message, traceparent=HEADER_TRACEPARENT)
    assert answer.status_code == 200
    span = only(otel, "tools/call add")
    assert hex_id(span.parent.span_id, 16) == CLIENT_SPAN
    (link,) = span.links
    assert hex_id(link.context.trace_id, 32) == HEADER_TRACE
    assert hex_id(link.context.span_id, 16) == HEADER_SPAN


async def test_repeated_traceparent_header_is_ignored(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    message = modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
    headers = [*headers_for(message).items(), ("traceparent", HEADER_TRACEPARENT)]
    headers.append(("traceparent", f"00-{TRACE_ID}-{CLIENT_SPAN}-01"))
    async with asgi(server.build_app()) as http:
        answer = await http.post("/mcp", json=message, headers=headers)
    assert answer.status_code == 200
    span = only(otel, "tools/call add")
    assert span.parent is None and span.links == ()


class MixedCaseField(TextMapPropagator):
    """W3C trace context carried in one header whose name has capitals, as X-Ray's is."""

    FIELD = "X-Test-Trace"

    def extract(self, carrier: Any, context: Any = None, getter: Any = default_getter) -> Context:
        values = getter.get(carrier, self.FIELD)
        if not values:
            return context if context is not None else Context()
        return TraceContextTextMapPropagator().extract({"traceparent": values[0]}, context)

    def inject(self, carrier: Any, context: Any = None, setter: Any = default_setter) -> None:
        pass

    @property
    def fields(self) -> set[str]:
        return {self.FIELD}


async def test_header_of_a_propagator_with_capitals_is_parent(otel: Otel) -> None:
    # Header names arrive lowercased; a propagator names its fields as it
    # likes (X-Ray's is X-Amzn-Trace-Id), and HTTP header names have no case.
    server = otel.server()
    add_tool(server)
    message = modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
    previous = propagate.get_global_textmap()
    propagate.set_global_textmap(MixedCaseField())
    try:
        answer = await post_stateless(server, message, **{"X-Test-Trace": HEADER_TRACEPARENT})
    finally:
        propagate.set_global_textmap(previous)
    assert answer.status_code == 200
    span = only(otel, "tools/call add")
    assert hex_id(span.context.trace_id, 32) == HEADER_TRACE
    assert span.parent is not None and hex_id(span.parent.span_id, 16) == HEADER_SPAN


async def test_ambient_span_is_parent_without_meta(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    tracer = otel.tracer()
    with tracer.start_as_current_span("outer") as outer:
        await server.dispatch(call(msg_id=1), make_context())
        message = call(msg_id=2)
        message["params"]["_meta"] = {"traceparent": TRACEPARENT}
        await server.dispatch(message, make_context())
    first, second = [span for span in otel.spans() if span.name == "tools/call add"]
    assert first.parent.span_id == outer.get_span_context().span_id
    assert first.context.trace_id == outer.get_span_context().trace_id
    # With _meta, the client's span is the parent and the ambient one a link.
    assert hex_id(second.parent.span_id, 16) == CLIENT_SPAN
    (link,) = second.links
    assert link.context.span_id == outer.get_span_context().span_id


async def test_tool_spans_nest_under_the_request_span(otel: Otel) -> None:
    server = otel.server()
    tracer = otel.tracer()

    @server.tool
    async def fetch() -> str:
        """Starts a span of its own, async."""
        with tracer.start_as_current_span("fetch work"):
            return "ok"

    @server.tool
    def crunch() -> str:
        """Starts a span of its own, on its worker thread."""
        with tracer.start_as_current_span("crunch work"):
            return threading.current_thread().name

    context = make_context()
    await server.dispatch(call("fetch", msg_id=1), context)
    answer = await server.dispatch(call("crunch", msg_id=2), context)
    assert answer is not None and answer["result"]["content"][0]["text"] != "MainThread"
    for tool in ("fetch", "crunch"):
        request = only(otel, f"tools/call {tool}")
        work = only(otel, f"{tool} work")
        assert work.parent.span_id == request.context.span_id
        assert work.context.trace_id == request.context.trace_id


async def test_cancel_callbacks_run_in_the_calls_trace(otel: Otel) -> None:
    server = otel.server()
    tracer = otel.tracer()
    entered = threading.Event()
    called = threading.Event()

    @server.tool(timeout=0.3)
    def query() -> str:
        """A query whose cancel opens a span, as an instrumented driver's would."""
        token = current_cancel_token()
        assert token is not None

        def kill() -> None:
            with tracer.start_as_current_span("KILL QUERY"):
                called.set()

        token.on_cancel(kill)
        entered.set()
        token.wait(30)
        return "stopped"

    response = await server.dispatch(call("query"), make_context())
    assert response is not None and response["error"]["code"] == TOOL_TIMEOUT
    assert await asyncio.to_thread(called.wait, 10)
    assert await server.wait_for_tool_threads(10) == 0
    request = only(otel, "tools/call query")
    kill = only(otel, "KILL QUERY")
    assert kill.context.trace_id == request.context.trace_id
    assert kill.parent.span_id == request.context.span_id


# -------------------------------------------------------- classification


def classification(span: Any) -> tuple[Any, Any, Any, Any]:
    attributes = span.attributes
    return (
        attributes.get("error.type"),
        attributes.get("rpc.response.status_code"),
        span.status.status_code,
        attributes.get("easy_mcp.status"),
    )


async def test_outcome_classification(otel: Otel) -> None:
    server = otel.server(max_sync_workers=1, auth=APIKeyAuth({KEY: "*"}))
    holding = threading.Event()
    release = threading.Event()
    add_tool(server)

    @server.tool
    def polite() -> str:
        """Fails politely."""
        raise ToolError("not now")

    @server.tool
    def boom() -> str:
        """Fails."""
        raise RuntimeError("detail")

    @server.tool
    def mislabeled() -> dict[str, int]:
        """Breaks its own schema."""
        return [1]  # type: ignore[return-value]

    @server.tool(timeout=0.2)
    async def sleepy() -> str:
        """Far too slow."""
        await asyncio.sleep(30)
        return "late"

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """One call per session."""
        return "once"

    @server.tool(requires_auth=True)
    def secret() -> str:
        """Hidden from anonymous callers."""
        return "secret"

    @server.tool
    def hold() -> str:
        """Holds the only sync worker."""
        holding.set()
        release.wait(30)
        return "held"

    context = make_context()
    for msg_id, name, arguments in [
        (1, "add", None),
        (2, "polite", {}),
        (3, "boom", {}),
        (4, "mislabeled", {}),
        (5, "sleepy", {}),
        (6, "add", {"a": "x"}),
        (7, "once", {}),
        (8, "once", {}),
        (9, "secret", {}),
        (10, "missing", {}),
    ]:
        await server.dispatch(call(name, arguments, msg_id), context)
    held = asyncio.create_task(server.dispatch(call("hold", msg_id=11), context))
    assert await asyncio.to_thread(holding.wait, 10)
    await server.dispatch(call("add", msg_id=12), context)
    release.set()
    await asyncio.wait_for(held, 10)
    await server.dispatch(rpc("made/up", msg_id=13), context)

    by_id = {span.attributes["jsonrpc.request.id"]: span for span in otel.spans()}
    error, unset = StatusCode.ERROR, StatusCode.UNSET
    assert classification(by_id["1"]) == (None, None, unset, "ok")
    assert classification(by_id["2"]) == ("tool_error", None, error, "tool_error")
    assert classification(by_id["3"]) == ("tool_error", None, error, "error")
    assert classification(by_id["4"]) == ("tool_error", None, error, "output_schema_error")
    timeout = str(TOOL_TIMEOUT)
    assert classification(by_id["5"]) == (timeout, timeout, error, "timeout")
    invalid = str(INVALID_PARAMS)
    assert classification(by_id["6"]) == (None, invalid, unset, "invalid_arguments")
    assert classification(by_id["7"]) == (None, None, unset, "ok")
    limit = str(SESSION_LIMIT_EXCEEDED)
    assert classification(by_id["8"]) == (limit, limit, error, "session_limit")
    assert classification(by_id["9"]) == (None, invalid, unset, "unauthorized")
    assert classification(by_id["10"]) == (None, invalid, unset, None)
    assert by_id["10"].name == "tools/call"  # an unknown tool names no span
    assert classification(by_id["11"]) == (None, None, unset, "ok")
    busy = str(SERVER_BUSY)
    assert classification(by_id["12"]) == (busy, busy, error, "busy")
    method = str(METHOD_NOT_FOUND)
    assert classification(by_id["13"]) == (None, method, unset, None)
    assert by_id["13"].name == "_OTHER"
    # Status descriptions: the JSON-RPC message, none for an isError result.
    assert by_id["5"].status.description == "Tool 'sleepy' timed out after 0.2s"
    assert by_id["2"].status.description is None
    assert by_id["3"].attributes["easy_mcp.error_id"]
    assert by_id["4"].attributes["easy_mcp.error_id"]


async def test_internal_error_carries_its_error_id(otel: Otel, logs: LogCapture) -> None:
    server = otel.server()
    add_tool(server)

    @server.middleware
    async def broken(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        raise RuntimeError("middleware bug")

    response = await server.dispatch(call(), make_context())
    assert response is not None and response["error"]["code"] == INTERNAL_ERROR
    span = only(otel, "tools/call add")
    error_id = span.attributes["easy_mcp.error_id"]
    assert f"error_id={error_id}" in response["error"]["message"]
    assert span.attributes["error.type"] == str(INTERNAL_ERROR)
    assert span.status.status_code is StatusCode.ERROR
    assert any(error_id in record.getMessage() for record in logs.records)


async def test_inner_server_lends_the_outer_span_no_error_id(otel: Otel) -> None:
    # A tool forwarding to an in-process server that records nothing: the
    # inner tool's failure and error_id stay off the outer call's span.
    outer = otel.server()
    inner = MCPServer(port=0, rate_limit_per_minute=None)
    inner._telemetry_state = Telemetry(inner, metrics=False, otel=None)

    @inner.tool
    async def boom() -> str:
        """Fails."""
        raise RuntimeError("inner failure")

    @outer.tool
    async def relay() -> str:
        """Forwards a call to the inner server."""
        answer = await inner.dispatch(call("boom", msg_id=99), make_context(session_id="inner"))
        assert answer is not None and answer["result"]["isError"] is True
        return "relayed"

    answer = await outer.dispatch(call("relay"), make_context())
    assert answer is not None and answer["result"]["isError"] is False
    span = only(otel, "tools/call relay")
    assert span.attributes["easy_mcp.status"] == "ok"
    assert "easy_mcp.error_id" not in span.attributes
    assert span.status.status_code is StatusCode.UNSET


async def test_calls_ended_by_request_middleware_carry_a_status(otel: Otel) -> None:
    server = otel.server()
    entered = asyncio.Event()

    @server.tool
    def denied() -> str:
        """Refused by the gate."""
        return "ran"

    @server.tool
    def forbidden() -> str:
        """Not this caller's, says the gate."""
        return "ran"

    @server.tool
    def broken() -> str:
        """The gate fails on it."""
        return "ran"

    @server.tool
    def stalled() -> str:
        """The gate holds it until it is cancelled."""
        return "ran"

    @server.middleware
    async def gate(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        tool = request.tool.name if request.tool is not None else None
        if tool == "denied":
            raise ProtocolError("not today", code=INVALID_REQUEST)
        if tool == "forbidden":
            raise AuthorizationError("not yours")
        if tool == "broken":
            raise RuntimeError("gate bug")
        if tool == "stalled":
            entered.set()
            await asyncio.sleep(30)
        return await call_next()

    context = make_context()
    for msg_id, name in enumerate(["denied", "forbidden", "broken"], start=1):
        await server.dispatch(call(name, {}, msg_id), context)
    task = asyncio.create_task(server.dispatch(call("stalled", {}, 4), context))
    await asyncio.wait_for(entered.wait(), 10)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 4}), context)
    assert await asyncio.wait_for(task, 10) is None
    by_id = {
        span.attributes["jsonrpc.request.id"]: span
        for span in otel.spans()
        if span.name.startswith("tools/call")
    }
    error, unset = StatusCode.ERROR, StatusCode.UNSET
    refused = str(INVALID_REQUEST)
    assert classification(by_id["1"]) == (None, refused, unset, "refused")
    # -32002 is never an error (it is also "resource not found"); the call
    # still counts as unauthorized.
    assert classification(by_id["2"]) == (None, str(FORBIDDEN), unset, "unauthorized")
    internal = str(INTERNAL_ERROR)
    assert classification(by_id["3"]) == (internal, internal, error, "internal_error")
    assert classification(by_id["4"]) == (None, None, unset, "cancelled")


async def test_minus_32002_is_never_an_error(otel: Otel) -> None:
    server = otel.server()

    @server.resource("config://app")
    def app_config() -> str:
        """The configuration."""
        return "region=eu"

    context = make_context()
    await server.dispatch(rpc("initialize", INIT, "init"), context)
    response = await server.dispatch(rpc("resources/read", {"uri": "config://missing"}), context)
    assert response is not None and response["error"]["code"] == RESOURCE_NOT_FOUND_LEGACY
    span = only(otel, "resources/read")
    assert classification(span) == (None, "-32002", StatusCode.UNSET, None)
    assert span.attributes["mcp.resource.uri"] == "config://missing"


async def test_resource_and_prompt_spans(otel: Otel) -> None:
    server = otel.server()

    @server.resource("config://app")
    def app_config() -> str:
        """The configuration."""
        return "region=eu"

    @server.prompt(complete={"topic": ["alpha", "beta"]})
    def greet(topic: str) -> str:
        """Greets about a topic."""
        return f"Hello about {topic}"

    context = make_context()
    long_uri = "config://" + "x" * 2000
    await server.dispatch(rpc("resources/read", {"uri": "config://app"}, 1), context)
    await server.dispatch(rpc("resources/read", {"uri": long_uri}, 2), context)
    await server.dispatch(
        rpc("prompts/get", {"name": "greet", "arguments": {"topic": "x"}}, 3), context
    )
    await server.dispatch(rpc("prompts/get", {"name": "nobody"}, 4), context)
    complete = {
        "ref": {"type": "ref/prompt", "name": "greet"},
        "argument": {"name": "topic", "value": "a"},
    }
    await server.dispatch(rpc("completion/complete", complete, 5), context)
    by_id = {span.attributes["jsonrpc.request.id"]: span for span in otel.spans()}
    assert by_id["1"].name == "resources/read"  # a URI never names a span
    assert by_id["1"].attributes["mcp.resource.uri"] == "config://app"
    assert by_id["2"].attributes["mcp.resource.uri"] == long_uri[:512]
    assert by_id["3"].name == "prompts/get greet"
    assert by_id["3"].attributes["gen_ai.prompt.name"] == "greet"
    assert by_id["4"].name == "prompts/get"  # unregistered: no target
    assert "gen_ai.prompt.name" not in by_id["4"].attributes
    assert by_id["5"].name == "completion/complete greet"
    assert by_id["5"].attributes["gen_ai.prompt.name"] == "greet"


async def test_prompt_name_labels_the_duration_histogram(otel: Otel) -> None:
    # The conventions make gen_ai.prompt.name conditionally required on
    # mcp.server.operation.duration, as gen_ai.tool.name is.
    server = otel.server(metrics=True)

    @server.prompt(complete={"topic": ["alpha", "beta"]})
    def greet(topic: str) -> str:
        """Greets about a topic."""
        return f"Hello about {topic}"

    @server.prompt
    def bye() -> str:
        """Says goodbye."""
        return "Goodbye"

    context = make_context()
    stdio = TransportInfo("stdio")
    for msg_id, params in enumerate(
        [
            {"name": "greet", "arguments": {"topic": "x"}},
            {"name": "bye"},
            {"name": "greet", "arguments": {"topic": "y"}},
            {"name": "nobody"},
        ]
    ):
        await server.dispatch(rpc("prompts/get", params, msg_id), context, transport=stdio)
    complete = {
        "ref": {"type": "ref/prompt", "name": "greet"},
        "argument": {"name": "topic", "value": "a"},
    }
    await server.dispatch(rpc("completion/complete", complete, 9), context, transport=stdio)

    counts: dict[tuple[str, str | None], float] = {}
    for series, value in parse_exposition(server.render_metrics()).items():
        if series.startswith(f"{DURATION}_count{{"):
            labels = dict(re.findall(r'(\w+)="([^"]*)"', series))
            key = (labels["mcp_method_name"], labels.get("gen_ai_prompt_name"))
            counts[key] = counts.get(key, 0) + value
    assert counts == {
        ("prompts/get", "greet"): 2,
        ("prompts/get", "bye"): 1,
        ("prompts/get", None): 1,  # an unregistered prompt is no label value
        ("completion/complete", "greet"): 1,
    }

    data = otel.reader.get_metrics_data()
    points = [
        point
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == "mcp.server.operation.duration"
        for point in metric.data.data_points
    ]
    mirrored: dict[tuple[str, str | None], int] = {}
    for point in points:
        attributes = point.attributes
        key = (attributes["mcp.method.name"], attributes.get("gen_ai.prompt.name"))
        mirrored[key] = mirrored.get(key, 0) + point.count
    assert mirrored == counts


async def test_listen_stream_span_and_gauge(otel: Otel) -> None:
    server = otel.server(metrics=True)
    pushed = Pushed()
    context = make_context(push=pushed)
    gauge = 'easy_mcp_subscriptions_open{easy_mcp_transport="stdio"}'
    stdio = TransportInfo("stdio")
    task = asyncio.create_task(
        server.dispatch(listen("stream-1", toolsListChanged=True), context, transport=stdio)
    )
    await pushed.wait_for(1)
    assert parse_exposition(server.render_metrics())[gauge] == 1
    assert [span.name for span in otel.spans()] == []  # still open
    server.close_subscriptions(context)
    assert await asyncio.wait_for(task, 10) is None
    span = only(otel, "subscriptions/listen")
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes["easy_mcp.subscription.end"] == "closed"
    assert "error.type" not in span.attributes
    samples = parse_exposition(server.render_metrics())
    assert samples[gauge] == 0
    listen_count = f'{DURATION}_count{{mcp_method_name="subscriptions/listen"'
    assert not any(series.startswith(listen_count) for series in samples)
    # A refused listen is answered at once, and measured like any request.
    refused = await server.dispatch(listen({"not": "an id"}), context, transport=stdio)
    assert refused is not None and refused["error"]["code"] == INVALID_REQUEST
    samples = parse_exposition(server.render_metrics())
    assert any(series.startswith(listen_count) for series in samples)
    # A client's cancel ends it the same way, with its own reason.
    task = asyncio.create_task(
        server.dispatch(listen("stream-2", toolsListChanged=True), context, transport=stdio)
    )
    await pushed.wait_for(3)
    cancel = notification("notifications/cancelled", {"requestId": "stream-2"})
    await server.dispatch(cancel, context, transport=stdio)
    assert await asyncio.wait_for(task, 10) is None
    ended = [s for s in otel.spans() if s.name == "subscriptions/listen"]
    assert ended[-1].attributes["easy_mcp.subscription.end"] == "client_cancelled"
    assert parse_exposition(server.render_metrics())[gauge] == 0


async def test_listen_ended_by_a_disconnect_is_no_error(otel: Otel) -> None:
    server = otel.server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = asyncio.create_task(server.dispatch(listen("gone", toolsListChanged=True), context))
    await pushed.wait_for(1)
    task.cancel()  # the transport's caller went away
    with pytest.raises(asyncio.CancelledError):
        await task
    span = only(otel, "subscriptions/listen")
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes["easy_mcp.subscription.end"] == "disconnected"


async def test_cancelled_span_is_unset_with_status_cancelled(
    otel: Otel, caplog: pytest.LogCaptureFixture
) -> None:
    server = otel.server()
    started = asyncio.Event()

    @server.tool
    async def slow() -> str:
        """Waits to be cancelled."""
        started.set()
        await asyncio.sleep(30)
        return "late"

    context = make_context()
    caplog.set_level(logging.WARNING, logger="opentelemetry")
    # A client's cancel.
    task = asyncio.create_task(server.dispatch(call("slow", msg_id=1), context))
    await asyncio.wait_for(started.wait(), 10)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(task, 10) is None
    # Our caller's cancel (a closed stateless connection, shutdown).
    started.clear()
    task = asyncio.create_task(server.dispatch(call("slow", msg_id=2), context))
    await asyncio.wait_for(started.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    spans = [span for span in otel.spans() if span.name == "tools/call slow"]
    assert len(spans) == 2
    for span in spans:
        assert span.status.status_code is StatusCode.UNSET
        assert span.attributes["easy_mcp.status"] == "cancelled"
        assert "error.type" not in span.attributes
    assert not [r for r in caplog.records if r.name.startswith("opentelemetry")]


# ------------------------------------------------------------------ safety


async def test_no_arguments_results_or_exception_text_in_spans(
    otel: Otel, logs: LogCapture
) -> None:
    server = otel.server()

    @server.tool
    def leak(password: str) -> str:
        """Fails with its argument in the message."""
        raise RuntimeError(f"failed for {password}")

    @server.tool
    def echo(text: str) -> str:
        """Returns its argument."""
        return text

    context = make_context()
    failed = await server.dispatch(call("leak", {"password": CANARY}, 1), context)
    echoed = await server.dispatch(call("echo", {"text": CANARY}, 2), context)
    assert echoed is not None and echoed["result"]["content"][0]["text"] == CANARY
    for span in otel.spans():
        assert CANARY not in span.to_json()
        assert span.events == ()
    span = only(otel, "tools/call leak")
    error_id = span.attributes["easy_mcp.error_id"]
    assert failed is not None
    assert f"error_id={error_id}" in failed["result"]["content"][0]["text"]
    assert any(f"error_id={error_id}" in record.getMessage() for record in logs.records)


async def test_debug_mode_puts_no_exception_text_in_spans(otel: Otel) -> None:
    # In debug mode the client's -32603 message carries the exception and,
    # for user code, its traceback; the span's status description must not.
    server = otel.server(debug=True)

    @server.resource("config://app")
    def app_config() -> str:
        """Fails with detail in its message."""
        raise RuntimeError(f"cannot connect with {CANARY}")

    @server.prompt
    def greet() -> str:
        """Fails with detail in its message."""
        raise RuntimeError(f"cannot render with {CANARY}")

    @server.middleware
    async def gate(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "ping":
            raise ValueError(f"middleware saw {CANARY}")
        return await call_next()

    context = make_context()
    answers = {
        "1": await server.dispatch(rpc("resources/read", {"uri": "config://app"}, 1), context),
        "2": await server.dispatch(rpc("prompts/get", {"name": "greet"}, 2), context),
        "3": await server.dispatch(rpc("ping", msg_id=3), context),
    }
    by_id = {span.attributes["jsonrpc.request.id"]: span for span in otel.spans()}
    for msg_id, answer in answers.items():
        assert answer is not None and answer["error"]["code"] == INTERNAL_ERROR
        assert CANARY in answer["error"]["message"]  # debug mode: the client is told
        span = by_id[msg_id]
        assert CANARY not in span.to_json() and "Traceback" not in span.to_json()
        assert span.status.status_code is StatusCode.ERROR
        error_id = span.attributes["easy_mcp.error_id"]
        assert span.status.description == f"Internal server error (error_id={error_id})"
        assert f"error_id={error_id}" in answer["error"]["message"]


async def test_tool_error_outside_tools_call_describes_its_span(otel: Otel) -> None:
    # A ToolError's message is meant for the client: raised by a resource, a
    # prompt or request middleware, it answers -32603 as written, and the
    # span's description is that message, as for any other code.  In debug
    # mode a -32603 without an error_id still says only "Internal server error".
    for debug in (False, True):
        otel.exporter.clear()
        server = otel.server(debug=debug)

        @server.resource("config://app")
        def app_config() -> str:
            """Its backend is down."""
            raise ToolError("the configuration service is down")

        @server.prompt
        def greet() -> str:
            """Its backend is down."""
            raise ToolError("the prompt backend is down")

        @server.middleware
        async def gate(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
            if request.method == "ping":
                raise ToolError("pings are paused for maintenance")
            return await call_next()

        context = make_context()
        answers = {
            "1": await server.dispatch(rpc("resources/read", {"uri": "config://app"}, 1), context),
            "2": await server.dispatch(rpc("prompts/get", {"name": "greet"}, 2), context),
            "3": await server.dispatch(rpc("ping", msg_id=3), context),
        }
        by_id = {span.attributes["jsonrpc.request.id"]: span for span in otel.spans()}
        for msg_id, answer in answers.items():
            assert answer is not None and answer["error"]["code"] == INTERNAL_ERROR
            span = by_id[msg_id]
            assert span.status.status_code is StatusCode.ERROR
            assert span.attributes["error.type"] == "-32603"
            assert "easy_mcp.error_id" not in span.attributes
            expected = "Internal server error" if debug else answer["error"]["message"]
            assert span.status.description == expected, (debug, msg_id)
            assert "error_id" not in answer["error"]["message"]


async def test_session_id_is_hashed(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    transport = StreamableHTTPTransport(server, legacy_sse=False)
    async with asgi(transport.build_app()) as http:
        opened = await http.post("/mcp", json=rpc("initialize", INIT, "init"), headers=ACCEPT)
        session_id = opened.headers["mcp-session-id"]
        headers = {**ACCEPT, "MCP-Session-Id": session_id}
        await http.post("/mcp", json=call(msg_id=2), headers=headers)
        stateless = modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, 3)
        await http.post("/mcp", json=stateless, headers=headers_for(stateless))
    by_id = {span.attributes["jsonrpc.request.id"]: span for span in otel.spans()}
    digest = hashlib.sha256(session_id.encode()).hexdigest()[:32]
    assert by_id["init"].attributes["mcp.session.id"] == digest
    assert by_id["2"].attributes["mcp.session.id"] == digest
    assert "mcp.session.id" not in by_id["3"].attributes
    for span in otel.spans():
        assert session_id not in span.to_json()


async def test_method_and_request_id_are_bounded(otel: Otel) -> None:
    server = otel.server()
    await server.dispatch(rpc("x" * 5000, msg_id="i" * 5000), make_context())
    (span,) = otel.spans()
    assert span.name == "_OTHER"
    assert span.attributes["mcp.method.name"] == "_OTHER"
    assert span.attributes["jsonrpc.request.id"] == "i" * 128


async def test_responses_are_identical_with_and_without_trace_context(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    plain = modern("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
    traced = modern(
        "tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, traceparent=TRACEPARENT
    )
    async with asgi(server.build_app()) as http:
        first = await http.post("/mcp", json=plain, headers=headers_for(plain))
        second = await http.post(
            "/mcp", json=traced, headers={**headers_for(traced), "traceparent": HEADER_TRACEPARENT}
        )
    assert first.content == second.content
    assert TRACE_ID not in second.text and HEADER_TRACE not in second.text
    assert not [name for name in second.headers if "trace" in name.lower()]


async def test_rate_limited_message_has_no_span(otel: Otel) -> None:
    server = otel.server(rate_limit_per_minute=1)
    context = make_context()
    await server.dispatch(rpc("ping", msg_id=1), context)
    limited = await server.dispatch(rpc("ping", msg_id=2), context)
    assert limited is not None and "error" in limited
    assert [span.attributes["jsonrpc.request.id"] for span in otel.spans()] == ["1"]


class Broken:
    """A span processor that fails on start and on end."""

    def on_start(self, span: Any, parent_context: Any = None) -> None:
        raise RuntimeError("processor bug")

    def on_end(self, span: Any) -> None:
        raise RuntimeError("processor bug")

    def _on_ending(self, span: Any) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


async def test_broken_span_processor_never_breaks_a_request(logs: LogCapture) -> None:
    from opentelemetry.sdk.trace import TracerProvider

    for broken_at in ("start", "end"):
        provider = TracerProvider()
        processor = Broken()
        if broken_at == "end":
            processor.on_start = lambda span, parent_context=None: None  # type: ignore[method-assign]
        provider.add_span_processor(processor)  # type: ignore[arg-type]
        server = MCPServer(port=0, rate_limit_per_minute=None)
        server._telemetry_state = Telemetry(
            server, metrics=False, otel=OtelBridge(tracer_provider=provider)
        )
        add_tool(server)
        logger = logging.getLogger("easy_mcp")
        level = logger.level
        logger.setLevel(logging.DEBUG)
        try:
            for n in range(3):
                answer = await server.dispatch(call(msg_id=n), make_context())
                assert answer is not None and answer["result"]["content"][0]["text"] == "3"
        finally:
            logger.setLevel(level)
        failures = [r for r in logs.records if "telemetry failed" in r.getMessage()]
        assert [r.levelno for r in failures][:1] == [logging.WARNING], broken_at
        assert all(r.levelno == logging.DEBUG for r in failures[1:])
        assert len(failures) >= 3
        logs.records.clear()


async def test_disabled_by_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "foo, Easy_MCP ,bar")
    assert load_bridge() is None
    server = MCPServer(port=0, rate_limit_per_minute=None, metrics=True)
    add_tool(server)
    answer = await server.dispatch(call(), make_context())
    assert answer is not None and answer["result"]["content"][0]["text"] == "3"
    assert server._telemetry.otel is None
    assert 'easy_mcp_tool_calls_total{easy_mcp_status="ok",gen_ai_tool_name="add"} 1' in (
        server.render_metrics()
    )
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "easy_mcp_extra,other")
    assert load_bridge() is not None


async def test_without_opentelemetry_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [name for name in sys.modules if name.split(".")[0] == "opentelemetry"]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    assert load_bridge() is None
    server = MCPServer(port=0, rate_limit_per_minute=None, metrics=True)
    add_tool(server)
    answer = await server.dispatch(call(), make_context())
    assert answer is not None and answer["result"]["content"][0]["text"] == "3"
    assert server._telemetry.otel is None
    assert "easy_mcp_tool_calls_total" in server.render_metrics()
    plain = MCPServer(port=0, rate_limit_per_minute=None)
    add_tool(plain)
    assert plain._telemetry.active is False  # nothing to record: dispatch skips the hook
    answer = await plain.dispatch(call(), make_context())
    assert answer is not None and answer["result"]["content"][0]["text"] == "3"
    invalid = await plain.dispatch(call("add", {"a": "x"}, 2), make_context())
    assert invalid is not None and invalid["error"]["code"] == INVALID_PARAMS


async def test_otel_metrics_mirror_builtin_names(otel: Otel) -> None:
    server = otel.server()
    add_tool(server)
    stdio = TransportInfo("stdio")
    await server.dispatch(call(), make_context(), transport=stdio)
    await server.dispatch(call("add", {"a": "x"}, 2), make_context(), transport=stdio)
    data = otel.reader.get_metrics_data()
    metrics = {
        metric.name: metric
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        if scope.scope.name == "easy_mcp"
        for metric in scope.metrics
    }
    duration = metrics["mcp.server.operation.duration"]
    assert duration.unit == "s"
    points = {tuple(sorted(point.attributes.items())): point for point in duration.data.data_points}
    ok = points[
        (
            ("gen_ai.operation.name", "execute_tool"),
            ("gen_ai.tool.name", "add"),
            ("mcp.method.name", "tools/call"),
            ("network.transport", "pipe"),
        )
    ]
    assert list(ok.explicit_bounds) == list(DURATION_BUCKETS)
    assert ok.count == 1
    calls = metrics["easy_mcp.tool.calls"]
    assert calls.unit == "{call}"
    statuses = {
        (point.attributes["gen_ai.tool.name"], point.attributes["easy_mcp.status"]): point.value
        for point in calls.data.data_points
    }
    assert statuses == {("add", "ok"): 1, ("add", "invalid_arguments"): 1}
    assert metrics["easy_mcp.tool.calls.active"].unit == "{call}"


async def test_log_lines_carry_trace_ids_inside_a_request(otel: Otel) -> None:
    server = otel.server()
    formatted: list[dict[str, Any]] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            formatted.append(json.loads(JSONLogFormatter().format(record)))

    @server.tool
    def chatty() -> str:
        """Logs from inside a call."""
        logging.getLogger("easy_mcp.app").warning("inside the tool")
        return "ok"

    handler = Capture()
    logger = logging.getLogger("easy_mcp")
    logger.addHandler(handler)
    try:
        logger.warning("outside any request")
        await server.dispatch(call("chatty"), make_context())
    finally:
        logger.removeHandler(handler)
    request = only(otel, "tools/call chatty")
    trace_id = hex_id(request.context.trace_id, 32)
    span_id = hex_id(request.context.span_id, 16)
    outside = [line for line in formatted if line["message"] == "outside any request"]
    assert outside and "trace_id" not in outside[0] and "span_id" not in outside[0]
    inside = [line for line in formatted if line["message"] == "inside the tool"]
    audit = [line for line in formatted if line["message"] == "tool_call"]
    for line in (*inside, *audit):
        assert (line["trace_id"], line["span_id"]) == (trace_id, span_id)
    assert inside and audit


async def chatty_lines(server: MCPServer) -> list[dict[str, Any]]:
    """The JSON log lines of one call carrying the client's trace context, from inside and audit."""
    formatted: list[dict[str, Any]] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            formatted.append(json.loads(JSONLogFormatter().format(record)))

    @server.tool
    def chatty() -> str:
        """Logs from inside a call."""
        logging.getLogger("easy_mcp.app").warning("inside the tool")
        return "ok"

    message = call("chatty")
    message["params"]["_meta"] = {"traceparent": TRACEPARENT}
    handler = Capture()
    logger = logging.getLogger("easy_mcp")
    logger.addHandler(handler)
    try:
        answer = await server.dispatch(message, make_context())
    finally:
        logger.removeHandler(handler)
    assert answer is not None and answer["result"]["content"][0]["text"] == "ok"
    lines = [line for line in formatted if line["message"] in ("inside the tool", "tool_call")]
    assert sorted(line["message"] for line in lines) == ["inside the tool", "tool_call"]
    return lines


async def test_log_lines_name_no_client_span_without_an_sdk() -> None:
    # opentelemetry-api with no SDK configured, as next to the official MCP
    # SDK: the server's spans are no-ops, so the only valid span context in
    # the request is the client's own, from its _meta.  Log lines name only
    # spans this process made, and so carry no ids, as in 0.3.1.
    from opentelemetry.trace import NoOpTracerProvider

    server = MCPServer(port=0, rate_limit_per_minute=None)
    server._telemetry_state = Telemetry(
        server, metrics=False, otel=OtelBridge(tracer_provider=NoOpTracerProvider())
    )
    for line in await chatty_lines(server):
        assert "trace_id" not in line and "span_id" not in line, line


async def test_log_lines_name_the_server_span_under_a_client_parent(otel: Otel) -> None:
    # With an SDK the client's trace holds the server span, and the lines name it.
    lines = await chatty_lines(otel.server())
    request = only(otel, "tools/call chatty")
    assert hex_id(request.context.trace_id, 32) == TRACE_ID
    span_id = hex_id(request.context.span_id, 16)
    assert span_id != CLIENT_SPAN
    for line in lines:
        assert (line["trace_id"], line["span_id"]) == (TRACE_ID, span_id), line


async def test_baggage_from_meta_reaches_the_tool(otel: Otel) -> None:
    server = otel.server()

    @server.tool
    def tenant() -> str:
        """Reads the client's baggage."""
        return str(baggage.get_baggage("tenant"))

    message = call("tenant")
    message["params"]["_meta"] = {"traceparent": TRACEPARENT, "baggage": "tenant=acme"}
    answer = await server.dispatch(message, make_context())
    assert answer is not None and answer["result"]["content"][0]["text"] == "acme"
    assert baggage.get_baggage("tenant") is None  # only inside the request


STDIO_SERVER = """
import sys

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter(out=sys.stderr)))
trace.set_tracer_provider(provider)

from easy_mcp import MCPServer

server = MCPServer(name="traced", rate_limit_per_minute=None)


@server.tool
def add(a: int, b: int) -> int:
    \"\"\"Add.\"\"\"
    print("a stray print goes to stderr")
    return a + b


server.run("stdio")
"""


def test_stdio_stdout_stays_protocol_only(tmp_path: Path) -> None:
    script = tmp_path / "traced_server.py"
    script.write_text(STDIO_SERVER, encoding="utf-8")
    message = call()
    message["params"]["_meta"] = {"traceparent": TRACEPARENT}
    lines = [rpc("initialize", INIT, "init"), message, rpc("tools/list", msg_id=3)]
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT), "PYTHONUNBUFFERED": "1"}
    env.pop("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", None)
    finished = subprocess.run(
        [sys.executable, str(script)],
        input="".join(json.dumps(line) + "\n" for line in lines).encode(),
        capture_output=True,
        env=env,
        cwd=str(tmp_path),
        timeout=60,
        check=False,
    )
    stdout = finished.stdout.decode().splitlines()
    assert len(stdout) == 3, finished.stderr.decode()
    answers = {json.loads(line)["id"]: json.loads(line) for line in stdout}
    assert all(answer["jsonrpc"] == "2.0" for answer in answers.values())
    assert answers[1]["result"]["content"][0]["text"] == "3"
    stderr = finished.stderr.decode()
    assert '"name": "tools/call add"' in stderr
    assert f'"trace_id": "0x{TRACE_ID}"' in stderr
    assert f'"parent_id": "0x{CLIENT_SPAN}"' in stderr
    assert "a stray print goes to stderr" in stderr
