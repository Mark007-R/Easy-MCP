"""Cancellation reaching sync tools: the token, the server hook, every route."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import make_context, notification, rpc

from easy_mcp import (
    CancelToken,
    MCPServer,
    StdioTransport,
    cancel_scope,
    current_cancel_token,
)
from easy_mcp.exceptions import SERVER_BUSY, TOOL_TIMEOUT

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


def audit_events(caplog: pytest.LogCaptureFixture, kind: str) -> list[dict[str, Any]]:
    return [
        record.event  # type: ignore[attr-defined]
        for record in caplog.records
        if record.name == "easy_mcp.audit" and record.getMessage() == kind
    ]


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


def test_a_failing_callback_does_not_stop_the_others(caplog: pytest.LogCaptureFixture) -> None:
    token = CancelToken()
    ran: list[bool] = []

    def boom() -> None:
        raise RuntimeError("callback failed")

    token.on_cancel(boom)
    token.on_cancel(lambda: ran.append(True))
    with caplog.at_level(logging.WARNING, logger="easy_mcp"):
        token.cancel()
    assert ran == [True]
    assert "cancel callback failed" in caplog.text


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
    caplog: pytest.LogCaptureFixture,
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
    with caplog.at_level(logging.INFO):
        call = asyncio.create_task(
            server.dispatch(rpc("tools/call", {"name": "fragile"}, 1), context)
        )
        assert await wait_for(started)
        await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
        assert await asyncio.wait_for(call, 5) is None
        deadline = time.monotonic() + 3
        while not audit_events(caplog, "cancel_callback_failed") and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
    (event,) = audit_events(caplog, "cancel_callback_failed")
    assert event["tool"] == "fragile" and event["error_id"]
    assert "kill failed" not in json.dumps(event)  # the detail stays in the log


async def test_a_tool_that_ignores_its_token_is_audited_when_it_finishes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    blocker = Blocker(obey=False)
    server = make_server(blocker)
    context = make_context()
    with caplog.at_level(logging.INFO):
        call = asyncio.create_task(
            server.dispatch(rpc("tools/call", {"name": "block"}, 3), context)
        )
        assert await wait_for(blocker.started)
        await server.dispatch(notification("notifications/cancelled", {"requestId": 3}), context)
        assert await asyncio.wait_for(call, 5) is None
        assert not blocker.stopped.is_set()
        blocker.release.set()  # it finishes on its own, after the fact
        assert await wait_for(blocker.finished)
        deadline = time.monotonic() + 3
        while (
            not audit_events(caplog, "tool_finished_after_cancel") and time.monotonic() < deadline
        ):
            await asyncio.sleep(0.02)
    (event,) = audit_events(caplog, "tool_finished_after_cancel")
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

    # Once the worker is free again, so are calls.
    blocker.release.set()
    assert (await asyncio.wait_for(first, 5))["result"]["isError"] is False
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        again = await server.dispatch(
            rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, 3), context
        )
        assert again is not None
        if "result" in again:
            break
        await asyncio.sleep(0.02)
    assert again["result"]["content"][0]["text"] == "3"


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
