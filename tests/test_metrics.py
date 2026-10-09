"""Metrics: the built-in registry and exposition, what the server counts, and ``GET /metrics``.

None of this needs OpenTelemetry: ``metrics=True`` keeps a registry of the
server's own, rendered as Prometheus text.
"""

from __future__ import annotations

import asyncio
import io
import json
import re
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest
from conftest import (
    LogCapture,
    headers_for,
    make_context,
    modern,
    notification,
    parse_exposition,
    rpc,
)

from easy_mcp import (
    APIKeyAuth,
    MCPServer,
    MemoryStore,
    ToolError,
)
from easy_mcp.exceptions import (
    INVALID_PARAMS,
    RATE_LIMITED,
    SERVER_BUSY,
    TOOL_TIMEOUT,
)
from easy_mcp.middleware import TransportInfo
from easy_mcp.telemetry.prometheus import (
    DURATION_BUCKETS,
    MetricsRegistry,
)
from easy_mcp.transport.stdio import StdioTransport
from easy_mcp.transport.streamable_http import StreamableHTTPTransport

LiveServer = Callable[[Any], str]

APP_KEY = "metrics-app-key-" + "a" * 16

ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}
DURATION = "mcp_server_operation_duration_seconds"
SESSION_DURATION = "mcp_server_session_duration_seconds"

_LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def split(series: str) -> tuple[str, dict[str, str]]:
    """A series as written, as ``(name, labels)``."""
    name, _, rest = series.partition("{")
    return name, dict(_LABEL.findall(rest))


def find(samples: dict[str, float], name: str, **labels: str) -> list[tuple[dict[str, str], float]]:
    """The series of *name* whose labels include *labels*."""
    found = []
    for series, value in samples.items():
        series_name, series_labels = split(series)
        if series_name == name and all(series_labels.get(k) == v for k, v in labels.items()):
            found.append((series_labels, value))
    return found


def total(samples: dict[str, float], name: str, **labels: str) -> float:
    """The sum of the series of *name* whose labels include *labels* (0 when there is none)."""
    return sum(value for _, value in find(samples, name, **labels))


def metrics_of(server: MCPServer) -> dict[str, float]:
    return parse_exposition(server.render_metrics())


def tool_calls(server: MCPServer) -> Counter[tuple[str, str]]:
    """``easy_mcp_tool_calls_total`` as ``{(tool, status): count}``."""
    counts: Counter[tuple[str, str]] = Counter()
    for labels, value in find(metrics_of(server), "easy_mcp_tool_calls_total"):
        counts[(labels["gen_ai_tool_name"], labels["easy_mcp_status"])] += int(value)
    return counts


def make_server(**options: Any) -> MCPServer:
    options.setdefault("rate_limit_per_minute", None)
    server = MCPServer(port=0, metrics=True, **options)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def asgi(app: Any) -> httpx.AsyncClient:
    """An in-process client; the server sees a 127.0.0.1 peer and ``Host: 127.0.0.1``."""
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1")


async def wait_until(check: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


# ------------------------------------------------------------- the registry


def test_empty_registry_renders_valid_exposition() -> None:
    text = MCPServer(port=0, metrics=True).render_metrics()
    assert text.endswith("\n")
    lines = text.splitlines()
    assert all(line.startswith("# ") for line in lines)  # no samples yet
    families = {
        "mcp_server_operation_duration_seconds": "histogram",
        "mcp_server_session_duration_seconds": "histogram",
        "easy_mcp_tool_calls_total": "counter",
        "easy_mcp_tool_calls_active": "gauge",
        "easy_mcp_tool_finished_after_cancel_total": "counter",
        "easy_mcp_sync_workers_in_use": "gauge",
        "easy_mcp_sessions_open": "gauge",
        "easy_mcp_rejected_total": "counter",
        "easy_mcp_subscriptions_open": "gauge",
    }
    for name, kind in families.items():
        assert f"# TYPE {name} {kind}" in lines
        assert any(line.startswith(f"# HELP {name} ") for line in lines)
    assert len(lines) == 2 * len(families)


def test_label_values_and_help_are_escaped() -> None:
    registry = MetricsRegistry()
    counter = registry.counter("odd_total", 'Help with a \\ backslash\nand a newline and "quotes".')
    counter.add({"value": 'back\\slash "quoted"\nnext line'})
    text = registry.render()
    assert '# HELP odd_total Help with a \\\\ backslash\\nand a newline and "quotes".' in text
    assert 'odd_total{value="back\\\\slash \\"quoted\\"\\nnext line"} 1' in text
    assert text.count("\n") == 3  # HELP, TYPE, one sample: nothing broke a line


def test_series_cap_folds_overflow_into_other(logs: LogCapture) -> None:
    registry = MetricsRegistry(max_series=3)
    counter = registry.counter("capped_total", "Capped.")
    for value in ("a", "b", "c", "d", "e"):
        counter.add({"tool": value, "status": "ok"})
    samples = parse_exposition(registry.render())
    assert samples == {
        'capped_total{status="ok",tool="a"}': 1,
        'capped_total{status="ok",tool="b"}': 1,
        'capped_total{status="ok",tool="c"}': 1,
        'capped_total{status="_OTHER",tool="_OTHER"}': 2,
    }
    warnings = [r for r in logs.records if "reached 3 series" in r.getMessage()]
    assert len(warnings) == 1


def test_registry_refuses_what_it_cannot_render() -> None:
    registry = MetricsRegistry()
    counter = registry.counter("up_total", "Only up.")
    with pytest.raises(ValueError):
        counter.add({}, -1)
    with pytest.raises(ValueError):
        registry.counter("up_total", "Twice.")
    with pytest.raises(ValueError):
        registry.histogram("bad_seconds", "Unsorted.", (1.0, 0.5))
    with pytest.raises(ValueError):
        MetricsRegistry(max_series=0)


def test_exposition_is_deterministic() -> None:
    registry = MetricsRegistry()
    gauge = registry.gauge("b_gauge", "B.")
    counter = registry.counter("a_total", "A.")
    histogram = registry.histogram("c_seconds", "C.", (0.1, 1.0))
    for value in ("z", "m", "a"):
        counter.add({"k": value}, 2)
        gauge.add({"k": value}, -1.5)
        histogram.observe({"k": value}, 0.5)
    first, second = registry.render(), registry.render()
    assert first == second
    names = [line.split()[2] for line in first.splitlines() if line.startswith("# TYPE")]
    assert names == sorted(names)
    a_series = [line for line in first.splitlines() if line.startswith("a_total{")]
    assert a_series == ['a_total{k="a"} 2', 'a_total{k="m"} 2', 'a_total{k="z"} 2']
    assert 'b_gauge{k="a"} -1.5' in first
    assert 'c_seconds_bucket{k="a",le="0.1"} 0' in first
    assert 'c_seconds_bucket{k="a",le="1.0"} 1' in first
    assert 'c_seconds_bucket{k="a",le="+Inf"} 1' in first


async def test_telemetry_is_built_once_and_fixed() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    await server.dispatch(rpc("ping"), make_context())
    first = server._telemetry
    server.metrics = True  # too late: the server has started dispatching
    assert server._telemetry is first
    with pytest.raises(RuntimeError):
        server.render_metrics()


# ------------------------------------------------------------ tool outcomes


def outcome_server(**options: Any) -> tuple[MCPServer, threading.Event, threading.Event]:
    """A server with a tool for each outcome; returns it, and hold()'s two events."""
    server = make_server(max_sync_workers=1, **options)
    holding = threading.Event()
    release = threading.Event()

    @server.tool
    def polite() -> str:
        """Fails politely."""
        raise ToolError("not now")

    @server.tool
    def boom() -> str:
        """Fails."""
        raise RuntimeError("secret detail")

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
        """Holds the only sync worker until released."""
        holding.set()
        release.wait(30)
        return "held"

    return server, holding, release


async def exercise(
    server: MCPServer, holding: threading.Event, release: threading.Event, *, hidden: bool
) -> None:
    """Call every tool of outcome_server() once per outcome it has."""
    context = make_context()
    calls = [
        ("add", {"a": 1, "b": 2}),
        ("polite", {}),
        ("boom", {}),
        ("mislabeled", {}),
        ("sleepy", {}),
        ("add", {"a": "one"}),
        ("once", {}),
        ("once", {}),
    ]
    if hidden:
        calls.append(("secret", {}))
    for msg_id, (name, arguments) in enumerate(calls):
        message = rpc("tools/call", {"name": name, "arguments": arguments}, msg_id)
        response = await server.dispatch(message, context)
        assert response is not None
    held = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "hold"}, 50), context))
    assert await asyncio.to_thread(holding.wait, 10)
    busy = await server.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, 51), context
    )
    assert busy is not None and busy["error"]["code"] == SERVER_BUSY
    release.set()
    assert await asyncio.wait_for(held, 10) is not None


async def test_tool_call_statuses_are_counted() -> None:
    server, holding, release = outcome_server()
    await exercise(server, holding, release, hidden=True)
    assert tool_calls(server) == Counter(
        {
            ("add", "ok"): 1,
            ("polite", "tool_error"): 1,
            ("boom", "error"): 1,
            ("mislabeled", "output_schema_error"): 1,
            ("sleepy", "timeout"): 1,
            ("add", "invalid_arguments"): 1,
            ("once", "ok"): 1,
            ("once", "session_limit"): 1,
            ("secret", "unauthorized"): 1,
            ("hold", "ok"): 1,
            ("add", "busy"): 1,
        }
    )


async def test_status_counts_match_audit_events(logs: LogCapture) -> None:
    server, holding, release = outcome_server()
    await exercise(server, holding, release, hidden=False)
    audited: Counter[tuple[str, str]] = Counter()
    for event in logs.events("tool_call"):
        audited[(event["tool"], event["status"])] += 1
    denial = {"ValidationError": "invalid_arguments", "SessionLimitError": "session_limit"}
    for event in logs.events("tool_denied"):
        audited[(event["tool"], denial[event["reason"]])] += 1
    assert tool_calls(server) == audited


async def test_hidden_tool_is_counted_as_unauthorized(logs: LogCapture) -> None:
    server, _, _ = outcome_server(auth=APIKeyAuth({APP_KEY: ["reports"]}))
    response = await server.dispatch(rpc("tools/call", {"name": "secret"}), make_context())
    assert response is not None
    # The client cannot tell it from a missing tool; only the operator can.
    assert response["error"] == {"code": INVALID_PARAMS, "message": "Unknown tool: secret"}
    assert tool_calls(server) == Counter({("secret", "unauthorized"): 1})
    samples = metrics_of(server)
    ((labels, _),) = find(samples, f"{DURATION}_count", gen_ai_tool_name="secret")
    assert labels["rpc_response_status_code"] == "-32602" and "error_type" not in labels
    # The audit log's tool_denied events stay what they were.
    assert logs.events("tool_denied") == []


async def test_cancelled_call_is_counted_but_has_no_duration() -> None:
    server = make_server()
    started = asyncio.Event()

    @server.tool
    async def slow() -> str:
        """Waits to be cancelled."""
        started.set()
        await asyncio.sleep(30)
        return "late"

    context = make_context()
    task = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "slow"}, 5), context))
    await asyncio.wait_for(started.wait(), 10)
    cancel = notification("notifications/cancelled", {"requestId": 5})
    assert await server.dispatch(cancel, context) is None
    assert await asyncio.wait_for(task, 10) is None
    assert tool_calls(server) == Counter({("slow", "cancelled"): 1})
    samples = metrics_of(server)
    assert find(samples, f"{DURATION}_count", mcp_method_name="tools/call") == []
    # The notification itself is a message like any other.
    assert total(samples, f"{DURATION}_count", mcp_method_name="notifications/cancelled") == 1


async def test_duration_histogram_follows_the_conventions() -> None:
    server = make_server()
    context = make_context()
    stdio = TransportInfo("stdio")
    await server.dispatch(rpc("initialize", INIT, "init"), context, transport=stdio)
    call = rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}})
    response = await server.dispatch(call, context, transport=stdio)
    assert response is not None and response["result"]["isError"] is False
    samples = metrics_of(server)
    labels = {
        "gen_ai_operation_name": "execute_tool",
        "gen_ai_tool_name": "add",
        "mcp_method_name": "tools/call",
        "mcp_protocol_version": "2025-11-25",
        "network_transport": "pipe",
    }
    buckets = find(samples, f"{DURATION}_bucket", **labels)
    assert {key for found, _ in buckets for key in found} == {*labels, "le"}
    assert [found["le"] for found, _ in buckets] == [
        *(repr(float(bound)) for bound in DURATION_BUCKETS),
        "+Inf",
    ]
    counts = [value for _, value in buckets]
    assert counts == sorted(counts)  # cumulative
    assert counts[-1] == total(samples, f"{DURATION}_count", **labels) == 1
    assert total(samples, f"{DURATION}_sum", **labels) > 0


async def test_non_error_codes_carry_no_error_type() -> None:
    server = make_server()
    context = make_context()
    await server.dispatch(rpc("does/not-exist"), context)
    await server.dispatch(rpc("tools/call", {"name": "add", "arguments": {"a": "x"}}), context)
    samples = metrics_of(server)
    codes = {
        found["rpc_response_status_code"]
        for found, _ in find(samples, f"{DURATION}_count")
        if "rpc_response_status_code" in found
    }
    assert codes == {"-32601", "-32602"}
    assert not [found for found, _ in find(samples, f"{DURATION}_count") if "error_type" in found]


async def test_error_codes_carry_an_error_type() -> None:
    server = make_server()
    context = make_context()

    @server.tool(timeout=0.2)
    async def sleepy() -> str:
        """Far too slow."""
        await asyncio.sleep(30)
        return "late"

    @server.tool
    def polite() -> str:
        """Fails politely."""
        raise ToolError("no")

    timed_out = await server.dispatch(rpc("tools/call", {"name": "sleepy"}), context)
    assert timed_out is not None and timed_out["error"]["code"] == TOOL_TIMEOUT
    await server.dispatch(rpc("tools/call", {"name": "polite"}), context)
    samples = metrics_of(server)
    ((slow, _),) = find(samples, f"{DURATION}_count", gen_ai_tool_name="sleepy")
    assert slow["error_type"] == slow["rpc_response_status_code"] == str(TOOL_TIMEOUT)
    ((failed, _),) = find(samples, f"{DURATION}_count", gen_ai_tool_name="polite")
    assert failed["error_type"] == "tool_error" and "rpc_response_status_code" not in failed


async def test_unknown_methods_share_one_series() -> None:
    server = make_server()
    context = make_context()
    for n in range(500):
        await server.dispatch(rpc(f"made/up-{uuid.uuid4().hex}", msg_id=n), context)
    samples = metrics_of(server)
    methods = {found["mcp_method_name"] for found, _ in find(samples, f"{DURATION}_count")}
    assert methods == {"_OTHER"}
    assert total(samples, f"{DURATION}_count", mcp_method_name="_OTHER") == 500


async def test_unknown_tool_names_create_no_series() -> None:
    server = make_server()
    context = make_context()
    for n in range(500):
        call = rpc("tools/call", {"name": f"tool-{uuid.uuid4().hex}"}, n)
        response = await server.dispatch(call, context)
        assert response is not None and response["error"]["code"] == INVALID_PARAMS
    samples = metrics_of(server)
    assert not any("gen_ai_tool_name" in series for series in samples)
    assert find(samples, "easy_mcp_tool_calls_total") == []
    assert total(samples, f"{DURATION}_count", mcp_method_name="tools/call") == 500
    assert len(find(samples, f"{DURATION}_count")) == 1


async def test_rate_limited_message_is_rejected_not_dispatched(logs: LogCapture) -> None:
    server = make_server(rate_limit_per_minute=1)
    context = make_context()
    assert await server.dispatch(rpc("ping", msg_id=1), context) is not None
    limited = await server.dispatch(rpc("ping", msg_id=2), context)
    assert limited is not None and limited["error"]["code"] == RATE_LIMITED
    samples = metrics_of(server)
    assert samples['easy_mcp_rejected_total{easy_mcp_reason="rate_limited"}'] == 1
    assert total(samples, f"{DURATION}_count") == 1
    assert len(logs.events("rate_limited")) == 1


async def test_active_and_worker_gauges_track_a_blocked_sync_tool() -> None:
    server = make_server()
    entered = threading.Event()
    release = threading.Event()

    @server.tool(timeout=0.3)
    def stubborn() -> str:
        """Ignores its cancel token."""
        entered.set()
        release.wait(30)
        return "done"

    def gauges() -> tuple[float, float]:
        samples = metrics_of(server)
        active = total(samples, "easy_mcp_tool_calls_active", gen_ai_tool_name="stubborn")
        return active, samples.get("easy_mcp_sync_workers_in_use", 0.0)

    task = asyncio.create_task(
        server.dispatch(rpc("tools/call", {"name": "stubborn"}), make_context())
    )
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        assert gauges() == (1, 1)
        response = await asyncio.wait_for(task, 10)
        assert response is not None and response["error"]["code"] == TOOL_TIMEOUT
        # The answer went out; the thread the tool ignored its token in did not.
        assert gauges() == (0, 1)
    finally:
        release.set()
    await wait_until(lambda: gauges() == (0, 0))
    assert await server.wait_for_tool_threads(10) == 0


async def test_late_finish_is_counted() -> None:
    server = make_server()
    entered = threading.Event()
    release = threading.Event()

    @server.tool(timeout=0.3)
    def stubborn() -> str:
        """Finishes after its timeout."""
        entered.set()
        release.wait(30)
        return "done"

    response = await server.dispatch(rpc("tools/call", {"name": "stubborn"}), make_context())
    assert response is not None and response["error"]["code"] == TOOL_TIMEOUT
    assert entered.is_set()
    release.set()
    late = "easy_mcp_tool_finished_after_cancel_total"
    series = f'{late}{{easy_mcp_status="ok",gen_ai_tool_name="stubborn"}}'
    await wait_until(lambda: metrics_of(server).get(series) == 1)
    assert await server.wait_for_tool_threads(10) == 0


async def test_exposition_parses_with_reference_parser() -> None:
    parser = pytest.importorskip("prometheus_client.parser")
    server, holding, release = outcome_server()
    await exercise(server, holding, release, hidden=True)
    for n in range(3):
        await server.dispatch(rpc(f"made/up-{n}", msg_id=n), make_context())
    text = server.render_metrics()
    families = {family.name: family for family in parser.text_string_to_metric_families(text)}
    # The parser names a counter family without its _total suffix.
    assert families["easy_mcp_tool_calls"].type == "counter"
    assert families["easy_mcp_sync_workers_in_use"].type == "gauge"
    duration = families["mcp_server_operation_duration_seconds"]
    assert duration.type == "histogram"
    by_series: dict[tuple[tuple[str, str], ...], dict[str, Any]] = {}
    for sample in duration.samples:
        labels = {k: v for k, v in sample.labels.items() if k != "le"}
        entry = by_series.setdefault(tuple(sorted(labels.items())), {"buckets": []})
        if sample.name.endswith("_bucket"):
            entry["buckets"].append((float(sample.labels["le"]), sample.value))
        else:
            entry[sample.name.rsplit("_", 1)[1]] = sample.value
    assert by_series
    for entry in by_series.values():
        bounds = [bound for bound, _ in entry["buckets"]]
        assert bounds == sorted(bounds) and bounds[-1] == float("inf")
        assert entry["buckets"][-1][1] == entry["count"]
        assert entry["sum"] >= 0
    calls = {
        (s.labels["gen_ai_tool_name"], s.labels["easy_mcp_status"]): s.value
        for s in families["easy_mcp_tool_calls"].samples
        if s.name == "easy_mcp_tool_calls_total"
    }
    assert calls[("add", "busy")] == 1 and calls[("sleepy", "timeout")] == 1


# ----------------------------------------------------------- sessions


async def test_streamable_session_metrics() -> None:
    server = make_server()
    transport = StreamableHTTPTransport(server, legacy_sse=False)
    async with asgi(transport.build_app()) as http:
        opened = await http.post("/mcp", json=rpc("initialize", INIT, "init"), headers=ACCEPT)
        assert opened.status_code == 200
        samples = metrics_of(server)
        assert samples['easy_mcp_sessions_open{easy_mcp_transport="streamable-http"}'] == 1
        assert find(samples, f"{SESSION_DURATION}_count") == []
        headers = {**ACCEPT, "MCP-Session-Id": opened.headers["mcp-session-id"]}
        assert (await http.delete("/mcp", headers=headers)).status_code == 204
    samples = metrics_of(server)
    assert samples['easy_mcp_sessions_open{easy_mcp_transport="streamable-http"}'] == 0
    ((labels, count),) = find(samples, f"{SESSION_DURATION}_count")
    assert count == 1
    assert labels == {
        "mcp_protocol_version": "2025-11-25",
        "network_protocol_name": "http",
        "network_transport": "tcp",
    }


class Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


async def test_idle_expired_session_duration_uses_last_seen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Both clocks are the test's: the store's (idle time) and the wall clock
    # (when the session opened and when its expiry is noticed).
    idle_clock = Clock(1000.0)
    wall = Clock(1_800_000_000.0)
    monkeypatch.setattr(time, "time", wall)
    server = make_server(store=MemoryStore(clock=idle_clock))
    transport = StreamableHTTPTransport(server, session_idle_timeout=5, legacy_sse=False)
    async with asgi(transport.build_app()) as http:
        opened = await http.post("/mcp", json=rpc("initialize", INIT, "init"), headers=ACCEPT)
        headers = {**ACCEPT, "MCP-Session-Id": opened.headers["mcp-session-id"]}
        idle_clock.now += 2
        wall.now += 2
        assert (await http.post("/mcp", json=rpc("ping", msg_id=2), headers=headers)).is_success
        # Idle far past its timeout before anything looks at it.
        idle_clock.now += 60
        wall.now += 60
        expired = await http.post("/mcp", json=rpc("ping", msg_id=3), headers=headers)
        assert expired.status_code == 404
    samples = metrics_of(server)
    assert samples['easy_mcp_sessions_open{easy_mcp_transport="streamable-http"}'] == 0
    assert total(samples, f"{SESSION_DURATION}_count") == 1
    # Up to its last activity (2 s in), not to when the expiry was found (62 s).
    assert total(samples, f"{SESSION_DURATION}_sum") == pytest.approx(2.0, abs=0.01)


def _next_data(lines: Iterator[str]) -> str:
    for line in lines:
        if line.startswith("data: "):
            return line[len("data: ") :]
    raise AssertionError("SSE stream ended without a data event")


def test_sse_session_metrics(live_server: LiveServer) -> None:
    server = make_server()
    base = live_server(server)
    gauge = 'easy_mcp_sessions_open{easy_mcp_transport="sse"}'
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("GET", "/sse") as stream:
            # Held while the stream is checked: an abandoned line iterator,
            # once collected, closes the connection.
            lines = stream.iter_lines()
            endpoint = _next_data(lines)
            assert "/messages?session_id=" in endpoint
            assert metrics_of(server)[gauge] == 1
            lines.close()
    deadline = time.monotonic() + 10
    while metrics_of(server).get(gauge) != 0:
        assert time.monotonic() < deadline, "the closed stream's session never ended"
        time.sleep(0.02)
    ((labels, count),) = find(metrics_of(server), f"{SESSION_DURATION}_count")
    assert count == 1
    assert labels == {"network_protocol_name": "http", "network_transport": "tcp"}


async def test_stdio_session_and_oversized_line() -> None:
    server = make_server(max_request_bytes=4096)
    gauge = 'easy_mcp_sessions_open{easy_mcp_transport="stdio"}'

    @server.tool
    def sessions() -> float:
        """How many stdio sessions the metrics say are open."""
        return parse_exposition(server.render_metrics()).get(gauge, 0.0)

    lines = [
        b"x" * 10_000,
        json.dumps(rpc("initialize", INIT, "init")).encode(),
        json.dumps(rpc("tools/call", {"name": "sessions"}, "peek")).encode(),
    ]
    stdout = io.BytesIO()
    stdin = io.BytesIO(b"\n".join(lines) + b"\n")
    await StdioTransport(server, stdin=stdin, stdout=stdout).serve()
    answers = {
        answer.get("id"): answer
        for answer in (json.loads(line) for line in stdout.getvalue().splitlines())
    }
    assert answers["peek"]["result"]["content"][0]["text"] == "1.0"
    samples = metrics_of(server)
    assert samples[gauge] == 0
    assert samples['easy_mcp_rejected_total{easy_mcp_reason="payload_too_large"}'] == 1
    ((labels, count),) = find(samples, f"{SESSION_DURATION}_count")
    assert count == 1
    assert labels == {"mcp_protocol_version": "2025-11-25", "network_transport": "pipe"}


async def test_metrics_over_stdio_warns_once(logs: LogCapture) -> None:
    server = make_server()
    await StdioTransport(server, stdin=io.BytesIO(b""), stdout=io.BytesIO()).serve()
    warnings = [r for r in logs.records if "no endpoint over stdio" in r.getMessage()]
    assert len(warnings) == 1
    assert "easy_mcp_sessions_open" in server.render_metrics()
    # Without metrics there is nothing to warn about.
    quiet = MCPServer(port=0, rate_limit_per_minute=None)
    await StdioTransport(quiet, stdin=io.BytesIO(b""), stdout=io.BytesIO()).serve()
    assert len([r for r in logs.records if "no endpoint over stdio" in r.getMessage()]) == 1


def test_stateless_disconnect_counts_cancelled(live_server: LiveServer) -> None:
    server = make_server()
    started = threading.Event()

    @server.tool
    async def slow() -> str:
        """Runs until its client gives up."""
        started.set()
        await asyncio.sleep(30)
        return "late"

    base = live_server(server)
    message = modern("tools/call", {"name": "slow"})
    with pytest.raises(httpx.ReadTimeout):
        httpx.post(
            f"{base}/mcp",
            json=message,
            headers=headers_for(message),
            timeout=httpx.Timeout(10.0, read=2.0),
        )
    assert started.is_set()
    deadline = time.monotonic() + 15
    while tool_calls(server) != Counter({("slow", "cancelled"): 1}):
        assert time.monotonic() < deadline, tool_calls(server)
        time.sleep(0.05)
    assert find(metrics_of(server), f"{DURATION}_count", mcp_method_name="tools/call") == []
