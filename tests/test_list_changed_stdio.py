"""List-change notifications and subscriptions/listen over stdio (in-memory streams)."""

from __future__ import annotations

import asyncio
import io
import json
import os
import random
import threading
import time
from typing import Any

from conftest import listen, notification, rpc

from easy_mcp import MCPServer, RequestInfo, StdioTransport
from easy_mcp.exceptions import INTERNAL_ERROR, ProtocolError
from easy_mcp.middleware import RequestNext, RequestOutcome

TAG = "io.modelcontextprotocol/subscriptionId"
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "t", "version": "1"},
}


def make_server(**kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    @server.tool
    async def slow_echo(text: str) -> str:
        """Echo after a short pause."""
        await asyncio.sleep(random.random() / 100)
        return text

    return server


def register(server: MCPServer, name: str = "extra") -> None:
    def tool() -> str:
        """A tool registered at runtime."""
        return name

    server.register_tool(tool, name=name)


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
    """A StdioTransport serving on this loop, with a pipe for stdin and a buffer for stdout."""

    def __init__(self, server: MCPServer, **kwargs: Any) -> None:
        self.stdin = Stdin()
        self.stdout = io.BytesIO()
        self.transport = StdioTransport(
            server, stdin=self.stdin.reader, stdout=self.stdout, **kwargs
        )
        self.task = asyncio.create_task(self.transport.serve())

    def lines(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.stdout.getvalue().splitlines() if line.strip()]

    async def wait_for(self, count: int, timeout: float = 5.0) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout
        while len(self.lines()) < count:
            if time.monotonic() > deadline:
                raise AssertionError(f"expected {count} line(s), got {self.lines()}")
            await asyncio.sleep(0.005)
        return self.lines()

    async def finish(self) -> list[dict[str, Any]]:
        self.stdin.close()
        await asyncio.wait_for(self.task, 10)
        self.stdin.reader.close()
        return self.lines()


async def test_stdio_initialized_client_receives_list_changed(fast_debounce: float) -> None:
    server = make_server()
    session = Session(server)
    session.stdin.send(rpc("initialize", INIT), notification("notifications/initialized"))
    await session.wait_for(1)
    register(server)
    await session.wait_for(2)
    session.stdin.send(rpc("tools/list", msg_id=2))
    lines = await session.wait_for(3)
    assert lines[0]["id"] == 1 and lines[0]["result"]["capabilities"] == {
        "tools": {"listChanged": True}
    }
    assert lines[1] == {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}
    assert [tool["name"] for tool in lines[2]["result"]["tools"]] == ["extra", "slow_echo"]
    assert len(await session.finish()) == 3


async def test_stdio_says_nothing_before_initialize(fast_debounce: float) -> None:
    server = make_server()
    session = Session(server)
    session.stdin.send(rpc("tools/list"))
    await session.wait_for(1)
    register(server)
    await asyncio.sleep(0.1)
    assert len(await session.finish()) == 1


async def test_stdio_listen_roundtrip_and_cancel(fast_debounce: float) -> None:
    server = make_server()
    session = Session(server)
    session.stdin.send(listen("listen-1", toolsListChanged=True))
    ack = (await session.wait_for(1))[0]
    assert ack["method"] == "notifications/subscriptions/acknowledged"
    assert ack["params"] == {
        "_meta": {TAG: "listen-1"},
        "notifications": {"toolsListChanged": True},
    }
    register(server, "one")
    changed = (await session.wait_for(2))[1]
    assert changed == {
        "jsonrpc": "2.0",
        "method": "notifications/tools/list_changed",
        "params": {"_meta": {TAG: "listen-1"}},
    }
    session.stdin.send(notification("notifications/cancelled", {"requestId": "listen-1"}))
    deadline = time.monotonic() + 5
    while server._notifier.count():
        assert time.monotonic() < deadline
        await asyncio.sleep(0.005)
    register(server, "two")
    await asyncio.sleep(0.1)
    # No frame for the cancelled subscription, and no response to its request.
    assert len(await session.finish()) == 2


async def test_stdio_eof_closes_subscriptions_promptly() -> None:
    server = make_server()
    session = Session(server, shutdown_timeout=5.0)
    session.stdin.send(listen(7, toolsListChanged=True))
    await session.wait_for(1)
    started = time.monotonic()
    lines = await session.finish()
    assert time.monotonic() - started < 2.0
    ack, result, cancelled = lines
    assert ack["params"]["_meta"] == {TAG: 7}
    assert result["id"] == 7
    assert result["result"]["resultType"] == "complete"
    assert result["result"]["_meta"][TAG] == 7
    assert cancelled == {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": 7, "reason": "server shutting down", "_meta": {TAG: 7}},
    }


async def test_stdio_listen_racing_the_end_of_input_still_gets_its_final_frames() -> None:
    server = make_server()
    line = json.dumps(listen(7, toolsListChanged=True)).encode() + b"\n"
    stdout = io.BytesIO()
    transport = StdioTransport(server, stdin=io.BytesIO(line), stdout=stdout, shutdown_timeout=5.0)
    started = time.monotonic()
    serving = asyncio.create_task(transport.serve())
    await asyncio.sleep(0)  # serving: its reader reads the line, then the end of input
    # The loop, held up meanwhile, takes both in one go: the listen is
    # dispatched only once serving has begun to end.
    time.sleep(0.2)
    await asyncio.wait_for(serving, 10)
    assert time.monotonic() - started < 2.0
    lines = [json.loads(raw) for raw in stdout.getvalue().splitlines()]
    assert [line.get("method") for line in lines] == [
        "notifications/subscriptions/acknowledged",
        None,
        "notifications/cancelled",
    ]
    assert lines[1]["id"] == 7 and lines[1]["result"]["resultType"] == "complete"
    assert lines[2]["params"] == {
        "requestId": 7,
        "reason": "server shutting down",
        "_meta": {TAG: 7},
    }


async def test_stdio_handshake_with_keys_of_mixed_types(fast_debounce: float) -> None:
    server = make_server()

    # JSON keys are strings: a client reads this example's 0 as "0".
    @server.tool(examples=[{"arguments": {"weights": {0: 0.5, "default": 1.0}}}])
    def weigh(weights: dict[str, float]) -> float:
        """Weigh things."""
        return sum(weights.values())

    session = Session(server)
    session.stdin.send(rpc("initialize", INIT))
    answer = (await session.wait_for(1))[0]
    assert answer["result"]["capabilities"] == {"tools": {"listChanged": True}}, answer
    session.stdin.send(rpc("tools/list", msg_id=2))
    listed = (await session.wait_for(2))[1]
    assert [tool["name"] for tool in listed["result"]["tools"]] == ["slow_echo", "weigh"]
    register(server)
    assert (await session.wait_for(3))[2] == {
        "jsonrpc": "2.0",
        "method": "notifications/tools/list_changed",
    }
    assert len(await session.finish()) == 3


async def test_stdio_lines_stay_whole_under_concurrent_writes(fast_debounce: float) -> None:
    server = make_server()
    session = Session(server)
    session.stdin.send(rpc("initialize", INIT), listen("l", toolsListChanged=True))
    await session.wait_for(2)
    stop = threading.Event()

    def churn() -> None:
        index = 0
        while not stop.is_set():
            register(server, f"churn{index}")
            index += 1
            time.sleep(0.002)

    worker = threading.Thread(target=churn)
    worker.start()
    try:
        calls = [
            rpc("tools/call", {"name": "slow_echo", "arguments": {"text": "é" * n}}, 100 + n)
            for n in range(40)
        ]
        session.stdin.send(*calls)
        deadline = time.monotonic() + 10
        while sum(1 for line in session.lines() if line.get("id", 0) >= 100) < 40:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.to_thread(worker.join)
    raw = session.stdout.getvalue().splitlines()
    for line in raw:
        json.loads(line)  # every line is one whole message
    lines = await session.finish()
    kinds = {line.get("method") for line in lines}
    assert "notifications/tools/list_changed" in kinds
    tagged = [line for line in lines if line.get("method") == "notifications/tools/list_changed"]
    assert any("params" not in line for line in tagged)  # the session's own
    assert any(line.get("params", {}).get("_meta") == {TAG: "l"} for line in tagged)


async def test_stdio_initialize_answered_as_serving_ends_leaves_no_session(
    fast_debounce: float,
) -> None:
    server = make_server()

    @server.middleware
    async def slow(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        if request.method == "initialize":
            await asyncio.sleep(0.2)  # the end of input arrives meanwhile
        return await call_next()

    session = Session(server)
    session.stdin.send(rpc("initialize", INIT))
    lines = await session.finish()
    assert [line["id"] for line in lines] == [1]  # the handshake is still answered
    # Its session ended with serving: no change reaches stdout any more.
    assert server._notifier._sessions == {}
    register(server)
    await asyncio.sleep(0.1)
    assert session.lines() == lines


async def test_stdio_initialize_racing_the_end_of_input_leaves_no_session(
    fast_debounce: float,
) -> None:
    server = make_server()
    for attempt in range(3):
        line = json.dumps(rpc("initialize", INIT)).encode() + b"\n"
        stdout = io.BytesIO()
        transport = StdioTransport(server, stdin=io.BytesIO(line), stdout=stdout)
        serving = asyncio.create_task(transport.serve())
        await asyncio.sleep(0)  # serving: its reader reads the line, then the end of input
        # The loop, held up meanwhile, takes both in one go: the handshake is
        # dispatched only once serving has begun to end.
        time.sleep(0.1)
        await asyncio.wait_for(serving, 10)
        answered = stdout.getvalue()
        assert [json.loads(raw)["id"] for raw in answered.splitlines()] == [1]
        assert server._notifier._sessions == {}
        register(server, f"extra{attempt}")
        await asyncio.sleep(0.1)
        assert stdout.getvalue() == answered


async def overrule_listen(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
    """Request middleware that replaces a listen's answer once its stream is over."""
    outcome = await call_next()
    if request.method == "subscriptions/listen":
        raise ProtocolError("refused after the fact")
    return outcome


async def test_stdio_listen_the_server_ended_gets_one_answer() -> None:
    server = make_server()
    server.middleware(overrule_listen)
    session = Session(server)
    session.stdin.send(listen(5, toolsListChanged=True))
    await session.wait_for(1)
    lines = await session.finish()
    assert [line.get("method") for line in lines] == [
        "notifications/subscriptions/acknowledged",
        None,
        "notifications/cancelled",
    ]
    assert lines[1]["id"] == 5 and lines[1]["result"]["resultType"] == "complete"


async def test_stdio_listen_cut_short_by_middleware_gets_its_error_once() -> None:
    server = make_server()

    @server.middleware
    async def bounded(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        async with asyncio.timeout(0.1):
            return await call_next()

    session = Session(server)
    session.stdin.send(listen(5, toolsListChanged=True))
    ack, error = await session.wait_for(2)
    assert ack["method"] == "notifications/subscriptions/acknowledged"
    assert error["id"] == 5 and error["error"]["code"] == INTERNAL_ERROR
    assert len(await session.finish()) == 2
