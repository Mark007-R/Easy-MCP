"""Cancellation reaching sync tools: the token, the server hook, every route."""

from __future__ import annotations

import asyncio
import gc
import io
import json
import logging
import os
import subprocess
import sys
import threading
import time
import weakref
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from conftest import LogCapture, make_context, notification, rpc

from easy_mcp import (
    CancelToken,
    MCPServer,
    StdioTransport,
    cancel_scope,
    current_cancel_token,
)
from easy_mcp.exceptions import SERVER_BUSY, TOOL_TIMEOUT, ToolError

LiveServer = Callable[[Any], str]

ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "tests", "version": "1.0"},
}
STATELESS_VERSION = "2026-07-28"


class Blocker:
    """A sync tool that runs until its call is cancelled, like a long query.

    ``stop`` is what a connector's cancel callback does (KILL QUERY, say); it
    records the reason and ends the work.
    """

    def __init__(self, *, obey: bool = True) -> None:
        self.started = threading.Event()
        self.stopped = threading.Event()
        self.finished = threading.Event()
        self.release = threading.Event()
        self.reasons: list[str | None] = []
        self.threads: list[str] = []
        self.obey = obey
        self.token: CancelToken | None = None

    def __call__(self) -> str:
        token = current_cancel_token()
        assert token is not None
        self.token = token
        if self.obey:
            token.on_cancel(self.stop)
        self.started.set()
        try:
            self.release.wait(30)
            return "done"
        finally:
            self.finished.set()

    def stop(self) -> None:
        self.threads.append(threading.current_thread().name)
        self.reasons.append(self.token.reason if self.token is not None else None)
        self.stopped.set()
        self.release.set()


def make_server(blocker: Blocker, **kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)

    def block() -> str:
        """Run until cancelled."""
        return blocker()

    server.register_tool(block, name="block")

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return server


async def wait_for(event: threading.Event, timeout: float = 5.0) -> bool:
    return await asyncio.to_thread(event.wait, timeout)


# ------------------------------------------------------------------ token


def test_token_runs_each_callback_once_and_keeps_the_first_reason() -> None:
    token = CancelToken()
    ran: list[str] = []
    token.on_cancel(lambda: ran.append("a"))
    token.on_cancel(lambda: ran.append("b"))
    assert not token.cancelled and token.reason is None
    token.cancel("timeout")
    token.cancel("cancelled")
    assert ran == ["a", "b"]
    assert token.cancelled and token.reason == "timeout"
    assert token.wait(0)


def test_a_callback_registered_after_the_cancel_runs_at_once() -> None:
    token = CancelToken()
    token.cancel()
    ran: list[bool] = []
    remove = token.on_cancel(lambda: ran.append(True))
    assert ran == [True]
    remove()  # harmless


def test_a_removed_callback_does_not_run() -> None:
    token = CancelToken()
    ran: list[bool] = []
    remove = token.on_cancel(lambda: ran.append(True))
    remove()
    remove()
    token.cancel()
    assert ran == []


def test_a_failing_callback_does_not_stop_the_others(logs: LogCapture) -> None:
    token = CancelToken()
    ran: list[bool] = []

    def boom() -> None:
        raise RuntimeError("callback failed")

    token.on_cancel(boom)
    token.on_cancel(lambda: ran.append(True))
    token.cancel()
    assert ran == [True]
    assert "cancel callback failed" in logs.text


def test_the_token_is_scoped() -> None:
    assert current_cancel_token() is None
    token = CancelToken()
    with cancel_scope(token):
        assert current_cancel_token() is token
    assert current_cancel_token() is None


# ------------------------------------------------------------ server hook


async def test_every_tool_call_gets_a_live_token() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def sync_probe() -> bool:
        """Whether a live token is visible from the worker thread."""
        token = current_cancel_token()
        return token is not None and not token.cancelled

    @server.tool
    async def async_probe() -> bool:
        """Whether a live token is visible from the tool task."""
        token = current_cancel_token()
        return token is not None and not token.cancelled

    for name in ("sync_probe", "async_probe"):
        response = await server.dispatch(rpc("tools/call", {"name": name}), make_context())
        assert response is not None
        assert response["result"]["content"][0]["text"] == "true"
    assert current_cancel_token() is None  # nothing leaks into the caller


async def test_notifications_cancelled_reaches_the_sync_tool() -> None:
    blocker = Blocker()
    server = make_server(blocker)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "block"}, 7), context))
    assert await wait_for(blocker.started)

    started = time.perf_counter()
    await server.dispatch(notification("notifications/cancelled", {"requestId": 7}), context)
    assert await asyncio.wait_for(call, 5) is None  # dropped, per MCP
    assert await wait_for(blocker.stopped, 1)
    assert await wait_for(blocker.finished, 1)
    assert time.perf_counter() - started < 1.0
    assert blocker.reasons == ["cancelled"]
    # Callbacks run on a thread of their own, never on the event loop.
    assert blocker.threads == ["easy-mcp-cancel:block"]


async def test_the_server_timeout_cancels_the_sync_tool() -> None:
    blocker = Blocker()
    server = make_server(blocker)
    server.unregister_tool("block")
    server.register_tool(lambda: blocker(), name="block", description="Block.", timeout=0.2)

    response = await server.dispatch(rpc("tools/call", {"name": "block"}), make_context())
    assert response is not None
    assert response["error"]["code"] == TOOL_TIMEOUT
    assert await wait_for(blocker.stopped, 1)
    assert await wait_for(blocker.finished, 1)
    assert blocker.reasons == ["timeout"]


async def test_a_blocking_callback_does_not_hold_up_the_event_loop() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    started = threading.Event()
    release = threading.Event()

    @server.tool
    def slow_to_stop() -> str:
        """A tool whose cancel callback itself takes a while."""
        token = current_cancel_token()
        assert token is not None
        token.on_cancel(lambda: (time.sleep(1.0), release.set()))
        started.set()
        release.wait(10)
        return "done"

    context = make_context()
    call = asyncio.create_task(
        server.dispatch(rpc("tools/call", {"name": "slow_to_stop"}, 1), context)
    )
    assert await wait_for(started)
    began = time.perf_counter()
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(call, 5) is None
    assert time.perf_counter() - began < 0.5  # the loop did not wait for the callback
    assert await wait_for(release, 3)


async def test_a_failing_callback_is_logged_and_audited(
    logs: LogCapture,
) -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    started = threading.Event()
    release = threading.Event()

    def boom() -> None:
        release.set()
        raise RuntimeError("kill failed")

    @server.tool
    def fragile() -> str:
        """A tool whose cancel callback fails."""
        token = current_cancel_token()
        assert token is not None
        token.on_cancel(boom)
        started.set()
        release.wait(10)
        return "done"

    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "fragile"}, 1), context))
    assert await wait_for(started)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(call, 5) is None
    deadline = time.monotonic() + 3
    while not logs.events("cancel_callback_failed") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    (event,) = logs.events("cancel_callback_failed")
    assert event["tool"] == "fragile" and event["error_id"]
    assert "kill failed" not in json.dumps(event)  # the detail stays in the log


async def test_a_tool_that_ignores_its_token_is_audited_when_it_finishes(
    logs: LogCapture,
) -> None:
    blocker = Blocker(obey=False)
    server = make_server(blocker)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "block"}, 3), context))
    assert await wait_for(blocker.started)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 3}), context)
    assert await asyncio.wait_for(call, 5) is None
    assert not blocker.stopped.is_set()
    blocker.release.set()  # it finishes on its own, after the fact
    assert await wait_for(blocker.finished)
    deadline = time.monotonic() + 3
    while not logs.events("tool_finished_after_cancel") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    (event,) = logs.events("tool_finished_after_cancel")
    assert event == {
        "type": "tool_finished_after_cancel",
        "tool": "block",
        "client_id": "ip:test",
        "reason": "cancelled",
        "status": "ok",
    }


# --------------------------------------------------------- worker threads


async def test_sync_calls_beyond_the_worker_cap_are_refused_not_queued() -> None:
    blocker = Blocker(obey=False)
    server = make_server(blocker, max_sync_workers=1)
    context = make_context()
    first = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "block"}, 1), context))
    assert await wait_for(blocker.started)

    busy = await server.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, 2), context
    )
    assert busy is not None
    assert busy["error"]["code"] == SERVER_BUSY
    assert "retry" in busy["error"]["message"]
    assert context.tool_calls["add"] == 0  # a refused call does not count

    # The worker is free again by the time the answer arrives.
    blocker.release.set()
    assert (await asyncio.wait_for(first, 5))["result"]["isError"] is False
    again = await server.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, 3), context
    )
    assert again is not None
    assert again["result"]["content"][0]["text"] == "3"


async def test_sequential_calls_at_the_cap_are_never_refused() -> None:
    # One call at a time never exceeds one worker, so none may be refused:
    # the worker is freed before the answer can reach the client.
    server = make_server(Blocker(), max_sync_workers=1)
    context = make_context()
    for n in range(300):
        response = await server.dispatch(
            rpc("tools/call", {"name": "add", "arguments": {"a": n, "b": 1}}, n), context
        )
        assert response is not None and "result" in response, (n, response)


async def test_an_abandoned_thread_keeps_its_worker_until_it_returns() -> None:
    # The cap counts threads, not calls: a tool that ignores its token still
    # occupies a worker after its call was cancelled.
    blocker = Blocker(obey=False)
    server = make_server(blocker, max_sync_workers=1)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "block"}, 1), context))
    assert await wait_for(blocker.started)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(call, 5) is None

    busy = await server.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 1}}, 2), context
    )
    assert busy is not None and busy["error"]["code"] == SERVER_BUSY
    blocker.release.set()


def test_worker_cap_validation() -> None:
    with pytest.raises(ValueError, match="max_sync_workers"):
        MCPServer(max_sync_workers=0)
    assert MCPServer(max_sync_workers=None).max_sync_workers is None


# ------------------------------------------------------------------ routes


async def test_http_session_cancel_and_delete_reach_the_sync_tool(
    live_server: LiveServer,
) -> None:
    blocker = Blocker()
    base = live_server(make_server(blocker))
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:

        async def post(message: Any, session: str | None = None) -> httpx.Response:
            headers = dict(ACCEPT)
            if session is not None:
                headers["MCP-Session-Id"] = session
            return await client.post("/mcp", json=message, headers=headers)

        session = (await post(rpc("initialize", INIT))).headers["mcp-session-id"]

        call = asyncio.create_task(post(rpc("tools/call", {"name": "block"}, 7), session))
        assert await wait_for(blocker.started)
        await post(notification("notifications/cancelled", {"requestId": 7}), session)
        assert (await asyncio.wait_for(call, 5)).status_code == 202
        assert await wait_for(blocker.stopped, 1)
        assert await wait_for(blocker.finished, 1)

        for event in (blocker.started, blocker.stopped, blocker.finished, blocker.release):
            event.clear()
        call = asyncio.create_task(post(rpc("tools/call", {"name": "block"}, 8), session))
        assert await wait_for(blocker.started)
        deleted = await client.delete("/mcp", headers={"MCP-Session-Id": session})
        assert deleted.status_code == 204
        assert (await asyncio.wait_for(call, 5)).status_code == 202
        assert await wait_for(blocker.stopped, 1)
        assert await wait_for(blocker.finished, 1)
    assert blocker.reasons == ["cancelled", "cancelled"]


def test_http_stateless_disconnect_reaches_the_sync_tool(live_server: LiveServer) -> None:
    blocker = Blocker()
    base = live_server(make_server(blocker))
    message = rpc(
        "tools/call",
        {
            "name": "block",
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": STATELESS_VERSION,
                "io.modelcontextprotocol/clientCapabilities": {},
            },
        },
    )
    headers = {
        **ACCEPT,
        "MCP-Protocol-Version": STATELESS_VERSION,
        "Mcp-Method": "tools/call",
        "Mcp-Name": "block",
    }

    def fire() -> None:
        try:
            with httpx.Client(base_url=base, timeout=0.5) as client:
                client.post("/mcp", json=message, headers=headers)
        except httpx.TimeoutException:
            pass  # the client gives up and closes the connection

    thread = threading.Thread(target=fire)
    thread.start()
    assert blocker.started.wait(5)
    thread.join(5)
    # The transport notices the closed connection within its poll interval.
    assert blocker.stopped.wait(2)
    assert blocker.finished.wait(1)
    assert blocker.reasons == ["cancelled"]


async def test_stdio_cancel_and_shutdown_reach_the_sync_tool() -> None:
    for route in ("notification", "shutdown"):
        blocker = Blocker()
        server = make_server(blocker)
        read_end, write_end = os.pipe()
        stdin = os.fdopen(read_end, "rb")
        writer = os.fdopen(write_end, "wb")
        transport = StdioTransport(server, stdin=stdin, stdout=io.BytesIO(), shutdown_timeout=0.2)

        def send(message: dict[str, Any], out: Any = writer) -> None:
            out.write(json.dumps(message).encode() + b"\n")
            out.flush()

        serving = asyncio.create_task(transport.serve())
        try:
            send(rpc("tools/call", {"name": "block"}, 7))
            assert await wait_for(blocker.started)
            if route == "notification":
                send(notification("notifications/cancelled", {"requestId": 7}))
                assert await wait_for(blocker.stopped, 1)
            writer.close()  # EOF: in-flight calls are cancelled after shutdown_timeout
            await asyncio.wait_for(serving, 5)
            assert await wait_for(blocker.stopped, 1)
            assert await wait_for(blocker.finished, 1)
            assert blocker.reasons == ["cancelled"], route
        finally:
            if not writer.closed:
                writer.close()
            stdin.close()


async def test_a_sync_tool_raising_stop_iteration_still_answers() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None, default_timeout=5)

    @server.tool
    def exhausted() -> int:
        """Leaks a StopIteration."""
        return next(iter([]))

    response = await asyncio.wait_for(
        server.dispatch(rpc("tools/call", {"name": "exhausted"}), make_context()), 3
    )
    assert response is not None and response["result"]["isError"] is True


# ---------------------------------------------------------------- shutdown


class SlowToStop:
    """A tool that honours its token, with a cancel callback that takes a
    while, like opening a second connection to send KILL QUERY."""

    def __init__(self, delay: float = 0.4) -> None:
        self.delay = delay
        self.started = threading.Event()
        self.callback_done = threading.Event()
        self.release = threading.Event()

    def __call__(self) -> str:
        token = current_cancel_token()
        assert token is not None
        token.on_cancel(self.stop)
        self.started.set()
        self.release.wait(30)
        return "done"

    def stop(self) -> None:
        time.sleep(self.delay)
        self.callback_done.set()
        self.release.set()


async def test_stdio_shutdown_waits_for_cancel_callbacks() -> None:
    tool = SlowToStop(delay=0.2)
    server = MCPServer(port=0, rate_limit_per_minute=None)
    server.register_tool(lambda: tool(), name="slow", description="Stops slowly.")
    read_end, write_end = os.pipe()
    stdin = os.fdopen(read_end, "rb")
    writer = os.fdopen(write_end, "wb")
    # 0.3s to finish in-flight calls, then as long again for the callbacks
    # they trigger; the callback needs 0.2s.
    transport = StdioTransport(server, stdin=stdin, stdout=io.BytesIO(), shutdown_timeout=0.3)
    serving = asyncio.create_task(transport.serve())
    try:
        writer.write(json.dumps(rpc("tools/call", {"name": "slow"}, 1)).encode() + b"\n")
        writer.flush()
        assert await wait_for(tool.started)
        writer.close()
        await asyncio.wait_for(serving, 10)
        # serve() returned only after the callback had done its work.
        assert tool.callback_done.is_set()
    finally:
        if not writer.closed:
            writer.close()
        stdin.close()


STDIO_CHILD = """
import sys, threading, time
from pathlib import Path
sys.path.insert(0, sys.argv[2])
from easy_mcp import MCPServer, StdioTransport, current_cancel_token

marker = Path(sys.argv[1])
server = MCPServer(port=0, rate_limit_per_minute=None)

@server.tool
def slow() -> str:
    \"\"\"Stops slowly.\"\"\"
    release = threading.Event()

    def stop() -> None:
        time.sleep(0.3)  # a connection round trip, say
        marker.write_text("killed")
        release.set()

    current_cancel_token().on_cancel(stop)
    marker.with_suffix(".started").write_text("")
    release.wait(30)
    return "done"

StdioTransport(server, shutdown_timeout=1.0).run()
"""


def test_a_stdio_process_does_not_exit_before_its_cancel_callbacks(tmp_path: Path) -> None:
    # The real exit: daemon threads die with the interpreter, so the
    # transport must wait for them before it returns.
    script = tmp_path / "child.py"
    script.write_text(STDIO_CHILD)
    marker = tmp_path / "marker.txt"
    repo = str(Path(__file__).resolve().parent.parent)
    child = subprocess.Popen(
        [sys.executable, str(script), str(marker), repo],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert child.stdin is not None
    child.stdin.write(json.dumps(rpc("tools/call", {"name": "slow"}, 1)).encode() + b"\n")
    child.stdin.flush()
    deadline = time.monotonic() + 10
    while not marker.with_suffix(".started").exists():
        assert time.monotonic() < deadline, "the tool never started"
        time.sleep(0.05)
    child.stdin.close()
    assert child.wait(10) == 0
    assert marker.read_text() == "killed"


async def test_the_http_lifespan_waits_for_cancel_callbacks() -> None:
    tool = SlowToStop()
    server = MCPServer(port=0, rate_limit_per_minute=None)
    server.register_tool(lambda: tool(), name="slow", description="Stops slowly.")
    app = server.build_app()
    async with app.router.lifespan_context(app):
        call = asyncio.create_task(
            server.dispatch(rpc("tools/call", {"name": "slow"}, 1), make_context())
        )
        assert await wait_for(tool.started)
        call.cancel()  # what ending a session at shutdown does
    assert tool.callback_done.is_set()
    done, _ = await asyncio.wait({call}, timeout=5)
    assert done  # the call itself ended (dropped, or cancelled outright)


async def test_waiting_for_tool_threads_is_bounded(logs: LogCapture) -> None:
    blocker = Blocker(obey=False)
    server = make_server(blocker)
    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "block"}, 1), context))
    assert await wait_for(blocker.started)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(call, 5) is None
    began = time.perf_counter()
    assert await server.wait_for_tool_threads(0.2) == 1
    assert time.perf_counter() - began < 1.0
    assert "easy-mcp-tool:block" in logs.text
    blocker.release.set()
    assert await server.wait_for_tool_threads(5) == 0


async def test_a_cancel_thread_that_cannot_start_does_not_mask_the_cancel(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    real_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        if thread.name.startswith("easy-mcp-cancel"):
            raise RuntimeError("can't start new thread")
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    blocker = Blocker()
    server = make_server(blocker)
    server.unregister_tool("block")
    server.register_tool(lambda: blocker(), name="block", description="Block.", timeout=0.2)
    context = make_context()
    timed_out = await server.dispatch(rpc("tools/call", {"name": "block"}, 1), context)
    assert timed_out is not None and timed_out["error"]["code"] == TOOL_TIMEOUT
    assert logs.events("cancel_callback_failed")
    blocker.release.set()
    assert await wait_for(blocker.finished)

    for event in (blocker.started, blocker.finished, blocker.release):
        event.clear()
    server.unregister_tool("block")
    server.register_tool(lambda: blocker(), name="block", description="Block.")
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "block"}, 2), context))
    assert await wait_for(blocker.started)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 2}), context)
    assert await asyncio.wait_for(call, 5) is None  # still dropped, per MCP
    blocker.release.set()


async def test_a_tool_finishing_between_the_deadline_and_the_trigger_is_audited(
    logs: LogCapture,
) -> None:
    # The deadline cancels the call's future a step before the token fires;
    # a tool that returns in between still had its answer thrown away.
    blocker = Blocker(obey=False)
    server = make_server(blocker)
    server.unregister_tool("block")
    server.register_tool(lambda: blocker(), name="block", description="Block.", timeout=0.2)
    stop_tool = server._stop_tool

    def late(token: CancelToken, reason: str, name: str, context: Any) -> None:
        blocker.release.set()
        assert blocker.finished.wait(2)
        time.sleep(0.1)  # the worker is done with the tool before the token fires
        stop_tool(token, reason, name, context)

    server._stop_tool = late  # type: ignore[method-assign]
    response = await server.dispatch(rpc("tools/call", {"name": "block"}), make_context())
    assert response is not None and response["error"]["code"] == TOOL_TIMEOUT
    deadline = time.monotonic() + 3
    while not logs.events("tool_finished_after_cancel") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    (event,) = logs.events("tool_finished_after_cancel")
    assert event["reason"] == "timeout" and event["status"] == "ok"


class Rows(list):  # type: ignore[type-arg]
    """A result that can be watched with a weak reference."""


async def test_a_finished_call_does_not_keep_its_result_alive() -> None:
    # With the cyclic collector off, only reference counting can free the
    # result: nothing (the late-audit hook included) may still hold it.
    server = MCPServer(port=0, rate_limit_per_minute=None)
    produced: list[weakref.ref[Rows]] = []

    @server.tool
    def rows() -> list[int]:
        """A result to watch."""
        result = Rows(range(1000))
        produced.append(weakref.ref(result))
        return result

    gc.disable()
    try:
        for n in range(20):
            response = await server.dispatch(rpc("tools/call", {"name": "rows"}, n), make_context())
            assert response is not None and response["result"]["isError"] is False
            del response
        await asyncio.sleep(0.05)  # let the last worker thread let go
        alive = [ref for ref in produced if ref() is not None]
        assert alive == []
    finally:
        gc.enable()


async def test_the_lifespan_waits_for_an_async_tools_cancel_callback() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    started = asyncio.Event()
    callback_done = threading.Event()

    def stop() -> None:
        time.sleep(0.2)
        callback_done.set()

    @server.tool
    async def slow() -> str:
        """An async tool with a slow cancel callback."""
        token = current_cancel_token()
        assert token is not None
        token.on_cancel(stop)
        started.set()
        await asyncio.sleep(30)
        return "done"

    app = server.build_app()
    async with app.router.lifespan_context(app):
        call = asyncio.create_task(
            server.dispatch(rpc("tools/call", {"name": "slow"}, 1), make_context())
        )
        await asyncio.wait_for(started.wait(), 5)
        call.cancel()
    assert callback_done.is_set()


async def lifespan_round_with_a_polling_tool() -> bool:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    started = threading.Event()
    cleaned = threading.Event()

    def clean() -> None:
        time.sleep(0.05)
        cleaned.set()

    def poll() -> str:
        token = current_cancel_token()
        assert token is not None
        token.on_cancel(clean)
        started.set()
        token.wait(30)
        return "stopped"

    server.register_tool(poll, name="poll", description="Polls its token.")
    app = server.build_app()
    async with app.router.lifespan_context(app):
        call = asyncio.create_task(
            server.dispatch(rpc("tools/call", {"name": "poll"}, 1), make_context())
        )
        assert await wait_for(started)
        call.cancel()
    return cleaned.is_set()


async def test_the_lifespan_waits_for_a_polling_tools_cleanup() -> None:
    # The tool polls its token, so its thread ends the moment the token
    # fires; the cleanup callback is on a thread started just after.
    for _ in range(20):
        assert await lifespan_round_with_a_polling_tool()


async def test_sse_shutdown_cancels_session_calls_right_away() -> None:
    from easy_mcp.transport.sse import SSETransport

    tool = SlowToStop(delay=0.1)
    server = MCPServer(port=0, rate_limit_per_minute=None)
    server.register_tool(lambda: tool(), name="slow", description="Stops slowly.")
    transport = SSETransport(server)
    app = transport.build_app()
    async with app.router.lifespan_context(app):
        from easy_mcp.transport.sse import _Session

        session = _Session(id="s1", context=make_context(), identity_fp=None)
        transport._sessions[session.id] = session
        task = asyncio.create_task(
            transport._deliver(session, rpc("tools/call", {"name": "slow"}, 1))
        )
        session.tasks.add(task)
        assert await wait_for(tool.started)
    assert tool.callback_done.is_set()


async def test_a_cancel_landing_right_after_the_answer_is_ready_is_audited(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    # The answer is settled, then the call is cancelled in the same loop
    # turn before it takes the answer: the client gets nothing, so a write
    # the tool made must still be on record.
    import easy_mcp.server as server_module

    # No timeout: on 3.11, wait_for with one hands back a result that is
    # already there even when cancelled (on 3.12+ it does not).
    server = MCPServer(port=0, rate_limit_per_minute=None, default_timeout=None)

    @server.tool
    def write() -> str:
        """Writes something."""
        return "written"

    context = make_context()
    settle = server_module._settle

    def settle_then_cancel(*args: Any) -> None:
        settle(*args)
        context.in_flight[7].cancel()

    monkeypatch.setattr(server_module, "_settle", settle_then_cancel)
    response = await server.dispatch(rpc("tools/call", {"name": "write"}, 7), context)
    assert response is None  # cancelled: dropped, per MCP
    deadline = time.monotonic() + 3
    while not logs.events("tool_finished_after_cancel") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    (event,) = logs.events("tool_finished_after_cancel")
    assert event["reason"] == "cancelled" and event["status"] == "ok"


class Payload:
    """A tool local that can be watched with a weak reference."""


async def test_a_late_exception_does_not_keep_the_tools_frames_alive() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    held: list[weakref.ref[Payload]] = []

    def fail() -> str:
        payload = Payload()
        held.append(weakref.ref(payload))
        time.sleep(0.15)
        raise ValueError("after the deadline")

    server.register_tool(fail, name="fail", description="Fails late.", timeout=0.05)
    gc.disable()
    try:
        for n in range(3):
            response = await server.dispatch(rpc("tools/call", {"name": "fail"}, n), make_context())
            assert response is not None and response["error"]["code"] == TOOL_TIMEOUT
        assert await server.wait_for_tool_threads(5) == 0
        await asyncio.sleep(0.05)
        assert [ref for ref in held if ref() is not None] == []
    finally:
        gc.enable()


@pytest.mark.parametrize("transport", ["sse", "http"])
def test_run_shuts_down_with_an_sse_client_connected(transport: str) -> None:
    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = MCPServer(port=port, rate_limit_per_minute=None)
    serving = threading.Thread(target=server.run, args=(transport,), daemon=True)
    serving.start()
    base = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 10
    while True:
        try:
            httpx.get(f"{base}/healthz", timeout=1)
            break
        except httpx.TransportError:
            assert time.monotonic() < deadline, "the server never came up"
            time.sleep(0.05)
    client = httpx.Client(base_url=base, timeout=10)
    try:
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()
            assert next(lines).startswith("event: endpoint")
            server.stop()
            serving.join(8)
            assert not serving.is_alive(), "shutdown waited for the SSE client"
    finally:
        if serving.is_alive():
            server._transport._uvicorn.force_exit = True  # type: ignore[union-attr]
            serving.join(5)
        client.close()


async def test_the_late_audit_keeps_its_reason_behind_a_slow_callback(
    monkeypatch: pytest.MonkeyPatch, logs: LogCapture
) -> None:
    # A tool leaves a slow cancel callback registered; by the time the audit
    # hook behind it runs, the call itself is long finished.
    import easy_mcp.server as server_module

    server = MCPServer(port=0, rate_limit_per_minute=None, default_timeout=None)
    seen: list[CancelToken | None] = []

    @server.tool
    def write() -> str:
        """Writes something."""
        token = current_cancel_token()
        assert token is not None
        token.on_cancel(lambda: (time.sleep(0.2), seen.append(current_cancel_token())))
        return "written"

    context = make_context()
    settle = server_module._settle

    def settle_then_cancel(*args: Any) -> None:
        settle(*args)
        context.in_flight[7].cancel()

    monkeypatch.setattr(server_module, "_settle", settle_then_cancel)
    assert await server.dispatch(rpc("tools/call", {"name": "write"}, 7), context) is None
    deadline = time.monotonic() + 3
    while not logs.events("tool_finished_after_cancel") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    (event,) = logs.events("tool_finished_after_cancel")
    assert event["reason"] == "cancelled"
    # Callbacks run with their call's token current.
    assert seen and seen[0] is not None and seen[0].reason == "cancelled"


@pytest.mark.parametrize("timeout", [None, 30.0])
async def test_an_error_on_time_does_not_keep_the_tools_frames_alive(
    timeout: float | None,
) -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None, default_timeout=timeout)
    held: list[weakref.ref[Payload]] = []

    def fail() -> str:
        payload = Payload()
        held.append(weakref.ref(payload))
        raise ValueError("on time")

    def refuse() -> str:
        payload = Payload()
        held.append(weakref.ref(payload))
        raise ToolError("refused")

    server.register_tool(fail, name="fail", description="Fails.")
    server.register_tool(refuse, name="refuse", description="Refuses.")
    # A log record keeps its traceback, and pytest keeps the records it
    # captures (a production handler formats a record and lets it go): log
    # nothing, so only the server's own references are left to test.
    logging.disable(logging.CRITICAL)
    gc.disable()
    try:
        for n, tool in enumerate(["fail", "refuse", "fail", "refuse"]):
            response = await server.dispatch(rpc("tools/call", {"name": tool}, n), make_context())
            assert response is not None and response["result"]["isError"] is True
        await asyncio.sleep(0.05)
        assert [ref for ref in held if ref() is not None] == []
    finally:
        gc.enable()
        logging.disable(logging.NOTSET)


async def test_a_timeout_error_raised_by_the_tool_is_a_tool_failure(logs: LogCapture) -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    tokens: list[CancelToken] = []

    @server.tool
    def read() -> str:
        """A socket read that times out on its own."""
        token = current_cancel_token()
        assert token is not None
        tokens.append(token)
        raise TimeoutError("timed out reading from the socket")

    response = await server.dispatch(rpc("tools/call", {"name": "read"}), make_context())
    assert response is not None
    assert response["result"]["isError"] is True
    assert "Tool execution failed" in response["result"]["content"][0]["text"]
    assert not tokens[0].cancelled
    await asyncio.sleep(0.05)
    assert logs.events("tool_finished_after_cancel") == []


async def test_a_failing_callback_registered_after_the_cancel_is_audited(
    logs: LogCapture,
) -> None:
    # A connector still connecting when the cancel lands registers its stop
    # after the fact; it runs at once, in the tool's thread, and a failure
    # must still be on record.
    server = MCPServer(port=0, rate_limit_per_minute=None)
    started = threading.Event()

    def boom() -> None:
        raise RuntimeError("KILL QUERY failed")

    @server.tool
    def late() -> str:
        """Registers its cancel callback only after the cancel."""
        token = current_cancel_token()
        assert token is not None
        started.set()
        token.wait(10)
        token.on_cancel(boom)
        return "done"

    context = make_context()
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "late"}, 1), context))
    assert await wait_for(started)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
    assert await asyncio.wait_for(call, 5) is None
    deadline = time.monotonic() + 3
    while not logs.events("cancel_callback_failed") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    (event,) = logs.events("cancel_callback_failed")
    assert event["tool"] == "late"
