"""stdio transport tests: in-memory streams plus a real subprocess round trip."""

from __future__ import annotations

import asyncio
import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from easy_mcp import APIKeyAuth, AuthenticationError, MCPServer, StdioTransport
from easy_mcp.transport.stdio import API_KEY_ENV_VAR

KEY = "stdio-test-key-" + "k" * 17
WRONG_KEY = "wrong-key-" + "0" * 10
REPO_ROOT = Path(__file__).resolve().parent.parent


def make_server(**kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


def encode(*messages: dict[str, Any] | bytes) -> io.BytesIO:
    """Serialize messages as newline-delimited JSON into a stdin stand-in."""
    lines = [m if isinstance(m, bytes) else json.dumps(m).encode() for m in messages]
    return io.BytesIO(b"\n".join(lines) + b"\n")


def decode(stdout: io.BytesIO) -> list[dict[str, Any]]:
    """Parse every line written to the stdout stand-in."""
    return [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]


def by_id(responses: list[dict[str, Any]]) -> dict[Any, dict[str, Any]]:
    return {r["id"]: r for r in responses}


async def run_stdio(server: MCPServer, stdin: io.BytesIO, **kwargs: Any) -> list[dict[str, Any]]:
    stdout = io.BytesIO()
    transport = StdioTransport(server, stdin=stdin, stdout=stdout, **kwargs)
    await transport.serve()
    return decode(stdout)


async def test_stdio_roundtrip() -> None:
    stdin = encode(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "x"}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "add", "arguments": {"a": 2, "b": 3}},
        },
        {"jsonrpc": "2.0", "id": 4, "method": "no/such/method"},
        b"",  # blank lines are ignored
        b"   ",
    )
    responses = by_id(await run_stdio(make_server(), stdin))

    assert set(responses) == {1, 2, 3, 4}  # the notification produced no output
    assert responses[1]["result"]["serverInfo"]["name"] == "easy-mcp"
    assert [t["name"] for t in responses[2]["result"]["tools"]] == ["add"]
    assert responses[3]["result"]["content"][0]["text"] == "5"
    assert responses[3]["result"]["isError"] is False
    assert responses[4]["error"]["code"] == -32601


async def test_stdio_parse_error_and_payload_cap() -> None:
    server = make_server(max_request_bytes=200)
    stdin = encode(
        b"{not json",
        {"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {"pad": "x" * 500}},
        {"jsonrpc": "2.0", "id": 2, "method": "ping"},
    )
    responses = await run_stdio(server, stdin)

    assert len(responses) == 3
    parse_error, too_large = [r for r in responses if r["id"] is None]
    assert parse_error["error"]["code"] == -32700
    assert too_large["error"]["code"] == -32004
    assert "200 bytes" in too_large["error"]["message"]
    # The oversized line was discarded whole; the next line still works.
    assert by_id(responses)[2]["result"] == {}


async def test_stdio_output_is_one_json_object_per_line() -> None:
    server = make_server()

    @server.tool
    def multiline() -> str:
        """Return text containing newlines and non-ASCII."""
        return "line one\nline two — ünïcode"

    stdin = encode(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "multiline"}}
    )
    stdout = io.BytesIO()
    await StdioTransport(server, stdin=stdin, stdout=stdout).serve()

    raw_lines = stdout.getvalue().splitlines()
    assert len(raw_lines) == 1
    text = json.loads(raw_lines[0])["result"]["content"][0]["text"]
    assert text == "line one\nline two — ünïcode"


def _auth_server() -> MCPServer:
    server = make_server(auth=APIKeyAuth({KEY: ["admin"]}))

    @server.tool(scopes=("admin",))
    def secret() -> str:
        """Protected tool."""
        return "s3cr3t"

    return server


def _secret_call() -> io.BytesIO:
    return encode(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "secret"}},
    )


async def test_stdio_anonymous_cannot_see_or_call_protected_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    responses = by_id(await run_stdio(_auth_server(), _secret_call()))
    assert [t["name"] for t in responses[1]["result"]["tools"]] == ["add"]
    assert responses[2]["error"]["code"] == -32602
    assert "Unknown tool" in responses[2]["error"]["message"]


async def test_stdio_api_key_argument_reaches_protected_tools() -> None:
    responses = by_id(await run_stdio(_auth_server(), _secret_call(), api_key=KEY))
    assert [t["name"] for t in responses[1]["result"]["tools"]] == ["add", "secret"]
    assert responses[2]["result"]["content"][0]["text"] == "s3cr3t"


async def test_stdio_api_key_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(API_KEY_ENV_VAR, KEY)
    responses = by_id(await run_stdio(_auth_server(), _secret_call()))
    assert responses[2]["result"]["content"][0]["text"] == "s3cr3t"


async def test_stdio_invalid_api_key_fails_fast() -> None:
    stdout = io.BytesIO()
    transport = StdioTransport(
        _auth_server(), api_key=WRONG_KEY, stdin=_secret_call(), stdout=stdout
    )
    with pytest.raises(AuthenticationError):
        await transport.serve()
    # Nothing was served: no downgrade to anonymous access.
    assert stdout.getvalue() == b""


async def test_stdio_cancellation_drops_response_and_shuts_down_promptly() -> None:
    server = make_server()

    @server.tool
    async def slow() -> str:
        """Sleep for a long time."""
        await asyncio.sleep(30)
        return "done"

    stdin = encode(
        {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "slow"}},
        {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 7}},
        {"jsonrpc": "2.0", "id": 8, "method": "ping"},
    )
    started = time.perf_counter()
    responses = by_id(await run_stdio(server, stdin, shutdown_timeout=5.0))
    assert time.perf_counter() - started < 4.0
    assert 7 not in responses  # cancelled requests get no response, per MCP
    assert responses[8]["result"] == {}


async def test_stdio_in_flight_calls_are_cancelled_after_shutdown_timeout() -> None:
    server = make_server()

    @server.tool
    async def slow() -> str:
        """Sleep for a long time."""
        await asyncio.sleep(30)
        return "done"

    stdin = encode({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "slow"}})
    started = time.perf_counter()
    responses = await run_stdio(server, stdin, shutdown_timeout=0.2)
    assert time.perf_counter() - started < 3.0
    assert responses == []


async def test_stdio_stop_ends_serving() -> None:
    server = make_server()
    stdout = io.BytesIO()
    # An input stream that never reaches EOF on its own.
    read_end, write_end = os.pipe()
    stdin = os.fdopen(read_end, "rb")
    transport = StdioTransport(server, stdin=stdin, stdout=stdout)
    try:
        serving = asyncio.create_task(transport.serve())
        await asyncio.sleep(0.05)
        assert not serving.done()
        transport.stop()
        await asyncio.wait_for(serving, timeout=5)
    finally:
        os.close(write_end)
        stdin.close()


def test_server_run_accepts_transport_names(monkeypatch: pytest.MonkeyPatch) -> None:
    server = make_server()
    with pytest.raises(ValueError, match="unknown transport"):
        server.run("websocket")  # type: ignore[arg-type]

    fake_in = encode({"jsonrpc": "2.0", "id": 1, "method": "ping"})
    fake_out = io.BytesIO()
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(fake_in))
    monkeypatch.setattr(sys, "stdout", io.TextIOWrapper(fake_out))
    server.run("stdio")
    assert by_id(decode(fake_out))[1]["result"] == {}


SUBPROCESS_SERVER = """
from easy_mcp import MCPServer

server = MCPServer(name="subprocess-demo")

@server.tool
def shout(text: str) -> str:
    \"\"\"Upper-case text, printing to stdout on the way (must not corrupt output).\"\"\"
    print("this print must not reach the protocol stream")
    return text.upper()

server.run("stdio")
"""


def test_stdio_subprocess_end_to_end() -> None:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT), "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(
        [sys.executable, "-c", SUBPROCESS_SERVER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd=REPO_ROOT,
    )
    assert proc.stdin and proc.stdout and proc.stderr
    try:
        requests = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2024-11-05"},
            },
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "shout", "arguments": {"text": "hi"}},
            },
        ]
        for request in requests:
            proc.stdin.write(json.dumps(request).encode() + b"\n")
        proc.stdin.flush()

        first = json.loads(proc.stdout.readline())
        second = json.loads(proc.stdout.readline())
        # communicate() closes stdin itself; that EOF makes the server exit cleanly.
        stdout_rest, stderr = proc.communicate(timeout=15)
    except BaseException:
        proc.kill()
        raise

    assert proc.returncode == 0
    assert first["id"] == 1
    assert first["result"]["serverInfo"]["name"] == "subprocess-demo"
    assert second["id"] == 2
    assert second["result"]["content"][0]["text"] == "HI"
    assert stdout_rest == b""  # nothing but JSON-RPC ever hits stdout
    # Logs and the stray print both land on stderr.
    assert b"this print must not reach the protocol stream" in stderr
    log_lines = [json.loads(line) for line in stderr.splitlines() if line.startswith(b"{")]
    events = {line.get("event", {}).get("type") for line in log_lines}
    assert {"startup", "session_open", "tool_call", "session_close"} <= events
