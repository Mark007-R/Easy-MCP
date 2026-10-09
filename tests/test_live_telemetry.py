"""Tracing and metrics against the official MCP Python SDK client.

Skipped unless ``EASY_MCP_LIVE_SDK_CLIENT=1`` is set and the ``mcp`` package
(2.3 or later) is installed, with ``opentelemetry-sdk`` (and
``prometheus-client`` for the scrape): install them next to easy_mcp in a
virtualenv of its own, ``pip install "mcp>=2.3" -e ".[dev]"``, then run this
file.

The SDK client injects ``_meta.traceparent`` into every request on its own,
from the span it opens for the request.  Its tracer is pointed at the test's
in-memory provider, so the client's span and the server's land side by side.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import live_sdk
import pytest
from conftest import Otel

from easy_mcp import MCPServer

pytestmark = live_sdk.marker

REPO_ROOT = Path(__file__).resolve().parent.parent
LiveServer = Callable[[Any], str]

# The server the SDK launches over stdio: spans go to the file TRACE_FILE
# names, one JSON object per line, never to stdout.
STDIO_SERVER = '''
import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

out = open(os.environ["TRACE_FILE"], "a", encoding="utf-8")
exporter = ConsoleSpanExporter(out=out, formatter=lambda span: span.to_json(indent=None) + "\\n")
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(exporter))
trace.set_tracer_provider(provider)

from easy_mcp import MCPServer

server = MCPServer(name="telemetry-interop", rate_limit_per_minute=None)


@server.tool
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


server.run("stdio")
'''


def build(otel: Otel) -> MCPServer:
    server = otel.server(name="telemetry-interop", metrics=True)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @server.resource("config://app", mime_type="application/json")
    def app_config() -> dict[str, str]:
        """The application's configuration."""
        return {"region": "eu-west-1"}

    @server.prompt
    def greet(name: str) -> str:
        """Greets someone."""
        return f"Hello, {name}"

    return server


@pytest.fixture
def traced_client(otel: Otel, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Point the SDK client's own tracer at the test's provider."""
    live_sdk.require_sdk()
    import mcp.shared._otel as client_otel

    monkeypatch.setattr(client_otel, "_tracer", otel.tracer_provider.get_tracer("mcp-python-sdk"))
    yield


def spans_named(otel: Otel, name: str) -> list[Any]:
    return [span for span in otel.spans() if span.name == name]


@pytest.mark.parametrize("mode", ["auto", "2026-07-28"])
async def test_sdk_client_trace_joins_server_span_over_http_stateless(
    live_server: LiveServer, otel: Otel, traced_client: None, mode: str
) -> None:
    from mcp import Client

    base = live_server(build(otel))
    async with Client(f"{base}/mcp", mode=mode) as client:
        assert client.protocol_version == "2026-07-28"
        result = await client.call_tool("add", {"a": 2, "b": 3})
        assert result.content[0].text == "5"
    (client_span,) = spans_named(otel, "MCP send tools/call add")
    (server_span,) = spans_named(otel, "tools/call add")
    assert server_span.context.trace_id == client_span.context.trace_id
    assert server_span.parent.span_id == client_span.context.span_id
    assert server_span.parent.is_remote
    assert server_span.attributes["mcp.protocol.version"] == "2026-07-28"
    assert server_span.attributes["network.transport"] == "tcp"
    assert "mcp.session.id" not in server_span.attributes


async def test_sdk_client_trace_joins_server_span_over_http_legacy(
    live_server: LiveServer, otel: Otel, traced_client: None
) -> None:
    from mcp import Client

    base = live_server(build(otel))
    async with Client(f"{base}/mcp", mode="legacy") as client:
        assert client.protocol_version == "2025-11-25"
        result = await client.call_tool("add", {"a": 2, "b": 3})
        assert result.content[0].text == "5"
    (client_span,) = spans_named(otel, "MCP send tools/call add")
    (server_span,) = spans_named(otel, "tools/call add")
    assert server_span.context.trace_id == client_span.context.trace_id
    assert server_span.parent.span_id == client_span.context.span_id
    assert server_span.attributes["mcp.protocol.version"] == "2025-11-25"
    session = server_span.attributes["mcp.session.id"]
    assert len(session) == 32 and int(session, 16) >= 0
    (initialize,) = spans_named(otel, "initialize")
    assert initialize.attributes["mcp.session.id"] == session


async def test_sdk_client_trace_joins_server_span_over_stdio(
    otel: Otel, traced_client: None, tmp_path: Path
) -> None:
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters

    trace_file = tmp_path / "spans.jsonl"
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-c", STDIO_SERVER],
        env={
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONUNBUFFERED": "1",
            "TRACE_FILE": str(trace_file),
        },
        cwd=str(tmp_path),
    )
    async with Client(parameters, mode="auto") as client:
        result = await client.call_tool("add", {"a": 2, "b": 3})
        assert result.content[0].text == "5"  # the protocol stream is intact
    (client_span,) = spans_named(otel, "MCP send tools/call add")
    deadline = time.monotonic() + 10
    while True:
        lines = trace_file.read_text(encoding="utf-8").splitlines() if trace_file.exists() else []
        exported = [json.loads(line) for line in lines if line.strip()]
        found = [span for span in exported if span["name"] == "tools/call add"]
        if found or time.monotonic() > deadline:
            break
        time.sleep(0.05)
    (server_span,) = found
    assert server_span["kind"] == "SpanKind.SERVER"
    assert server_span["context"]["trace_id"] == f"0x{client_span.context.trace_id:032x}"
    assert server_span["parent_id"] == f"0x{client_span.context.span_id:016x}"
    assert server_span["attributes"]["network.transport"] == "pipe"


@pytest.mark.parametrize("mode", ["auto", "legacy"])
async def test_sdk_client_traffic_moves_the_metrics(
    live_server: LiveServer, otel: Otel, mode: str
) -> None:
    live_sdk.require_sdk()
    parser = pytest.importorskip("prometheus_client.parser")
    from mcp import Client

    server = build(otel)
    base = live_server(server)
    async with Client(f"{base}/mcp", mode=mode) as client:
        for n in range(3):
            assert (await client.call_tool("add", {"a": n, "b": 1})).content[0].text == str(n + 1)
        read = await client.read_resource("config://app")
        assert json.loads(read.contents[0].text) == {"region": "eu-west-1"}
        got = await client.get_prompt("greet", {"name": "Ada"})
        assert got.messages[0].content.text == "Hello, Ada"
    if mode == "legacy":
        # The client ends its session on exit (DELETE): its duration is recorded.
        deadline = time.monotonic() + 10
        while "mcp_server_session_duration_seconds_count" not in server.render_metrics():
            assert time.monotonic() < deadline, "the session was never closed"
            time.sleep(0.05)
    response = httpx.get(f"{base}/metrics", timeout=10)
    assert response.status_code == 200
    families = {
        family.name: family for family in parser.text_string_to_metric_families(response.text)
    }
    calls = {
        (sample.labels["gen_ai_tool_name"], sample.labels["easy_mcp_status"]): sample.value
        for sample in families["easy_mcp_tool_calls"].samples
        if sample.name == "easy_mcp_tool_calls_total"
    }
    assert calls == {("add", "ok"): 3}
    version = "2025-11-25" if mode == "legacy" else "2026-07-28"
    counts: dict[str, float] = {}
    for sample in families["mcp_server_operation_duration_seconds"].samples:
        if sample.name.endswith("_count"):
            assert sample.labels["mcp_protocol_version"] == version
            assert sample.labels["network_transport"] == "tcp"
            method = sample.labels["mcp_method_name"]
            counts[method] = counts.get(method, 0) + sample.value
    assert counts["tools/call"] == 3
    assert counts["resources/read"] == 1
    assert counts["prompts/get"] == 1
    sessions = families["mcp_server_session_duration_seconds"].samples
    session_count = sum(s.value for s in sessions if s.name.endswith("_count"))
    assert session_count == (1 if mode == "legacy" else 0)
    open_now = families["easy_mcp_sessions_open"].samples
    assert all(sample.value == 0 for sample in open_now)
