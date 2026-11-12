"""Progress notifications: report_progress(), what reaches each channel, and when."""

from __future__ import annotations

import asyncio
import io
import json
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import Pushed, headers_for, make_context, modern, notification, rpc

import easy_mcp.progress
from easy_mcp import (
    MCPServer,
    StdioTransport,
    ToolError,
    current_cancel_token,
    current_progress,
    report_progress,
)
from easy_mcp.progress import PROGRESS_METHOD, ProgressReporter, progress_token
from easy_mcp.transport._outbox import Outbox, coalesce_key

LiveServer = Callable[[Any], str]

ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "tests", "version": "1.0"},
}


@pytest.fixture
def unthrottled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Send every report at once, so a test sees each of them."""
    monkeypatch.setattr(easy_mcp.progress, "MIN_INTERVAL_SECONDS", 0.0)


def call(name: str, arguments: dict[str, Any] | None = None, token: Any = "tok", msg_id: Any = 1):
    params: dict[str, Any] = {"name": name, "arguments": arguments or {}}
    if token is not None:
        params["_meta"] = {"progressToken": token}
    return rpc("tools/call", params, msg_id)


def progress_of(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [frame["params"] for frame in frames if frame.get("method") == PROGRESS_METHOD]


def channel() -> tuple[Pushed, Any]:
    """A multiplexed channel, as stdio is: (what was pushed, its context)."""
    pushed = Pushed()
    return pushed, make_context(push=pushed, multiplexed=True)


# ------------------------------------------------------------ the reporter


async def test_sync_tool_reports_reach_the_channel_before_the_answer(
    server: MCPServer, unthrottled: None
) -> None:
    seen: list[bool] = []

    @server.tool
    def work() -> str:
        for step in (1, 2, 3):
            seen.append(report_progress(step, total=3, message=f"step {step}"))
            time.sleep(0.02)
        return "done"

    pushed, context = channel()
    response = await server.dispatch(call("work"), context)
    assert response is not None and response["result"]["isError"] is False
    assert seen == [True, True, True]
    assert progress_of(pushed.frames) == [
        {"progressToken": "tok", "progress": 1, "total": 3, "message": "step 1"},
        {"progressToken": "tok", "progress": 2, "total": 3, "message": "step 2"},
        {"progressToken": "tok", "progress": 3, "total": 3, "message": "step 3"},
    ]
    # Pushed on the event loop, never from the tool's thread.
    assert set(pushed.threads) == {threading.get_ident()}


async def test_async_tool_reports(server: MCPServer, unthrottled: None) -> None:
    @server.tool
    async def work() -> str:
        report_progress(0.25)
        await asyncio.sleep(0)
        report_progress(0.5, 1.0)
        return "done"

    pushed, context = channel()
    await server.dispatch(call("work", token=7), context)
    assert progress_of(pushed.frames) == [
        {"progressToken": 7, "progress": 0.25},
        {"progressToken": 7, "progress": 0.5, "total": 1.0},
    ]


async def test_the_last_report_is_sent_before_the_answer_even_when_throttled(
    server: MCPServer,
) -> None:
    @server.tool
    def work() -> str:
        for step in range(1, 501):
            report_progress(step, total=500)
        return "done"

    pushed, context = channel()
    response = await server.dispatch(call("work"), context)
    assert response is not None
    reports = progress_of(pushed.frames)
    # 500 reports in far less than the interval: the first and the last.
    assert 1 <= len(reports) <= 3
    assert reports[-1] == {"progressToken": "tok", "progress": 500, "total": 500}
    values = [report["progress"] for report in reports]
    assert values == sorted(set(values))


async def test_reports_are_spaced_by_the_interval(
    server: MCPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(easy_mcp.progress, "MIN_INTERVAL_SECONDS", 0.05)

    @server.tool
    async def work() -> str:
        for step in range(1, 31):
            report_progress(step)
            await asyncio.sleep(0.01)
        return "done"

    pushed, context = channel()
    await server.dispatch(call("work"), context)
    reports = progress_of(pushed.frames)
    # About 0.3 s of work at one per 0.05 s, plus the last.
    assert 3 <= len(reports) <= 9
    assert reports[-1]["progress"] == 30


async def test_without_a_token_nothing_is_reported(server: MCPServer) -> None:
    seen: list[Any] = []

    @server.tool
    def work() -> str:
        seen.append(report_progress(1))
        seen.append(current_progress())
        return "done"

    pushed, context = channel()
    await server.dispatch(call("work", token=None), context)
    assert seen == [False, None]
    assert pushed.frames == []


@pytest.mark.parametrize("token", [True, 1.5, None, ["a"], {"a": 1}])
async def test_a_token_that_is_not_a_string_or_integer_is_ignored(
    server: MCPServer, token: Any
) -> None:
    seen: list[bool] = []

    @server.tool
    def work() -> str:
        seen.append(report_progress(1))
        return "done"

    pushed, context = channel()
    message = rpc("tools/call", {"name": "work", "_meta": {"progressToken": token}})
    response = await server.dispatch(message, context)
    assert response is not None and "result" in response
    assert seen == [False]
    assert pushed.frames == []


def test_progress_token() -> None:
    assert progress_token({"_meta": {"progressToken": "a"}}) == "a"
    assert progress_token({"_meta": {"progressToken": 0}}) == 0
    assert progress_token({"_meta": {"progressToken": False}}) is None
    assert progress_token({"_meta": "x"}) is None
    assert progress_token({}) is None


async def test_a_channel_for_change_notifications_only_carries_no_progress(
    server: MCPServer,
) -> None:
    seen: list[bool] = []

    @server.tool
    def work() -> str:
        seen.append(report_progress(1))
        return "done"

    pushed = Pushed()
    await server.dispatch(call("work"), make_context(push=pushed))  # not multiplexed
    await server.dispatch(call("work"), make_context())  # no channel at all
    assert seen == [False, False]
    assert pushed.frames == []


async def test_notify_is_preferred_to_push(server: MCPServer) -> None:
    @server.tool
    def work() -> str:
        report_progress(1)
        return "done"

    pushed, own = Pushed(), Pushed()
    context = make_context(push=pushed, multiplexed=True)
    context.notify = own
    await server.dispatch(call("work"), context)
    assert pushed.frames == []
    assert progress_of(own.frames) == [{"progressToken": "tok", "progress": 1}]


async def test_progress_that_does_not_grow_is_dropped(server: MCPServer, unthrottled: None) -> None:
    seen: list[bool] = []

    @server.tool
    async def work() -> str:
        for value in (5, 5, 3, 6):
            seen.append(report_progress(value))
            await asyncio.sleep(0)
        return "done"

    pushed, context = channel()
    await server.dispatch(call("work"), context)
    assert seen == [True, False, False, True]
    assert [report["progress"] for report in progress_of(pushed.frames)] == [5, 6]


@pytest.mark.parametrize(
    ("arguments", "error"),
    [
        (("half",), TypeError),
        ((True,), TypeError),
        ((float("nan"),), ValueError),
        ((1, float("inf")), ValueError),
        ((1, "ten"), TypeError),
        ((1, None, 5), TypeError),
    ],
)
def test_a_malformed_report_raises_with_or_without_a_client(
    arguments: tuple[Any, ...], error: type[Exception]
) -> None:
    with pytest.raises(error):
        report_progress(*arguments)
    reporter = ProgressReporter("tok", lambda message: None, asyncio.new_event_loop())
    try:
        with pytest.raises(error):
            reporter.report(*arguments)
    finally:
        reporter._loop.close()


def test_outside_a_request_a_report_is_not_taken() -> None:
    assert report_progress(1, 2, "three") is False
    assert current_progress() is None


async def test_a_malformed_report_fails_the_tool(server: MCPServer) -> None:
    @server.tool
    def work() -> str:
        report_progress("half")  # type: ignore[arg-type]
        return "done"

    pushed, context = channel()
    response = await server.dispatch(call("work"), context)
    assert response is not None and response["result"]["isError"] is True
    assert pushed.frames == []


async def test_a_failing_tool_still_sends_its_last_report(server: MCPServer) -> None:
    @server.tool
    def work() -> str:
        report_progress(1)
        report_progress(2)
        raise ToolError("no more")

    pushed, context = channel()
    response = await server.dispatch(call("work"), context)
    assert response is not None and response["result"]["isError"] is True
    assert progress_of(pushed.frames)[-1]["progress"] == 2


async def test_a_closed_channel_does_not_fail_the_tool(
    server: MCPServer, unthrottled: None
) -> None:
    seen: list[bool] = []

    @server.tool
    async def work() -> str:
        seen.append(report_progress(1))
        await asyncio.sleep(0.01)
        seen.append(report_progress(2))
        return "done"

    pushed = Pushed(explode=True)
    context = make_context(push=pushed, multiplexed=True)
    response = await server.dispatch(call("work"), context)
    assert response is not None and response["result"]["isError"] is False
    # The first was taken and found the channel closed; the second was not taken.
    assert seen == [True, False]
    assert pushed.calls == 1


async def test_a_cancelled_call_sends_nothing_more(server: MCPServer) -> None:
    started = asyncio.Event()

    @server.tool
    async def work() -> str:
        report_progress(1)
        await asyncio.sleep(0)  # the first goes out
        report_progress(2)  # held back by the interval
        started.set()
        await asyncio.sleep(30)
        return "never"

    pushed, context = channel()
    task = asyncio.create_task(server.dispatch(call("work", msg_id=9), context))
    await asyncio.wait_for(started.wait(), 5)
    await server.dispatch(
        notification("notifications/cancelled", {"requestId": 9, "reason": "test"}), context
    )
    assert await asyncio.wait_for(task, 5) is None
    await asyncio.sleep(easy_mcp.progress.MIN_INTERVAL_SECONDS * 2)
    assert [report["progress"] for report in progress_of(pushed.frames)] == [1]


async def test_a_thread_that_outlives_its_call_reports_to_nobody(
    server: MCPServer, unthrottled: None
) -> None:
    late: list[bool] = []
    finished = threading.Event()

    @server.tool(timeout=0.05)
    def work() -> str:
        report_progress(1)
        token = current_cancel_token()
        assert token is not None
        token.wait(5)
        time.sleep(0.2)  # the call has been answered by now
        late.append(report_progress(2))
        finished.set()
        return "late"

    pushed, context = channel()
    response = await server.dispatch(call("work"), context)
    assert response is not None and "error" in response  # timed out
    assert await asyncio.to_thread(finished.wait, 5)
    await asyncio.sleep(0.05)
    assert late == [False]
    assert [report["progress"] for report in progress_of(pushed.frames)] == [1]


async def test_stateless_requests_report_too(server: MCPServer) -> None:
    @server.tool
    def work() -> str:
        report_progress(1, 1)
        return "done"

    own = Pushed()
    context = make_context()
    context.notify = own
    message = modern("tools/call", {"name": "work"}, progressToken="p-1")
    response = await server.dispatch(message, context)
    assert response is not None and response["result"]["resultType"] == "complete"
    assert progress_of(own.frames) == [{"progressToken": "p-1", "progress": 1, "total": 1}]


async def test_resources_and_prompts_report(server: MCPServer) -> None:
    @server.resource("notes://all")
    def notes() -> str:
        report_progress(1, message="reading")
        return "notes"

    @server.prompt
    def brief() -> str:
        report_progress(2, message="writing")
        return "Be brief."

    pushed, context = channel()
    meta = {"_meta": {"progressToken": "r"}}
    read = await server.dispatch(rpc("resources/read", {"uri": "notes://all", **meta}), context)
    got = await server.dispatch(rpc("prompts/get", {"name": "brief", **meta}, 2), context)
    assert read is not None and "result" in read
    assert got is not None and "result" in got
    assert [report["message"] for report in progress_of(pushed.frames)] == ["reading", "writing"]


async def test_middleware_can_report(server: MCPServer) -> None:
    @server.tool
    def work() -> str:
        return "done"

    @server.tool_middleware
    async def announce(tool_call: Any, call_next: Any) -> Any:
        report_progress(0, message="starting")
        return await call_next()

    pushed, context = channel()
    await server.dispatch(call("work"), context)
    assert progress_of(pushed.frames) == [
        {"progressToken": "tok", "progress": 0, "message": "starting"}
    ]


async def test_a_request_served_inside_another_does_not_report_as_it(server: MCPServer) -> None:
    inner = MCPServer(port=0, rate_limit_per_minute=None)
    seen: list[bool] = []

    @inner.tool
    def leaf() -> str:
        seen.append(report_progress(1))
        return "leaf"

    @server.tool
    async def outer() -> str:
        await inner.dispatch(call("leaf", token=None), make_context())
        return "outer"

    pushed, context = channel()
    await server.dispatch(call("outer"), context)
    assert seen == [False]
    assert pushed.frames == []


# ------------------------------------------------------------- the queues


def progress(token: Any, value: float) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "method": PROGRESS_METHOD,
        "params": {"progressToken": token, "progress": value},
    }


async def test_an_outbox_keeps_the_latest_progress_of_each_token() -> None:
    outbox = Outbox()
    outbox.put(progress("a", 1))
    outbox.put(progress("b", 1))
    outbox.put(progress("a", 2))
    outbox.put(progress(1, 5))
    outbox.put(progress("1", 6))
    outbox.put(progress("a", 3))
    assert len(outbox) == 4
    order = [await outbox.get(1) for _ in range(4)]
    assert [(m["params"]["progressToken"], m["params"]["progress"]) for m in order] == [
        ("a", 3),
        ("b", 1),
        (1, 5),
        ("1", 6),
    ]
    # Taken: the next one for the token is queued anew.
    outbox.put(progress("a", 4))
    assert len(outbox) == 1


def test_progress_coalesces_by_token_and_its_type() -> None:
    assert coalesce_key(progress("a", 1)) == coalesce_key(progress("a", 2))
    assert coalesce_key(progress(1, 1)) != coalesce_key(progress("1", 1))
    assert coalesce_key(progress("a", 1)) != coalesce_key(progress("b", 1))


# ------------------------------------------------------------------ stdio


async def test_stdio_writes_progress_between_the_lines(unthrottled: None) -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def work() -> str:
        report_progress(1, 2)
        time.sleep(0.02)
        report_progress(2, 2)
        return "done"

    lines = [
        rpc("initialize", INIT, 1),
        notification("notifications/initialized"),
        call("work", token="s-1", msg_id=2),
    ]
    stdin = io.BytesIO(b"\n".join(json.dumps(line).encode() for line in lines) + b"\n")
    stdout = io.BytesIO()
    await StdioTransport(server, stdin=stdin, stdout=stdout).serve()
    written = [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]
    tail = [message for message in written if message.get("id") != 1]
    assert [message.get("method", "response") for message in tail] == [
        PROGRESS_METHOD,
        PROGRESS_METHOD,
        "response",
    ]
    assert tail[1]["params"] == {"progressToken": "s-1", "progress": 2, "total": 2}
    assert tail[2]["id"] == 2


# ------------------------------------------------------------------- HTTP


def slow_server() -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def work(steps: int = 3) -> str:
        for step in range(1, steps + 1):
            report_progress(step, total=steps)
            time.sleep(0.05)
        return "done"

    @server.tool
    def quiet() -> str:
        return "quiet"

    return server


def read_events(response: httpx.Response) -> list[dict[str, Any]]:
    """Every ``message`` event of a text/event-stream response."""
    events = []
    for line in response.iter_lines():
        if line.startswith("data: "):
            events.append(json.loads(line[len("data: ") :]))
    return events


def stateless_call(name: str, token: Any = "h-1", **arguments: Any) -> dict[str, Any]:
    overrides = {} if token is None else {"progressToken": token}
    return modern("tools/call", {"name": name, "arguments": arguments}, **overrides)


def test_stateless_http_answers_with_a_stream_once_progress_is_reported(
    live_server: LiveServer, unthrottled: None
) -> None:
    base = live_server(slow_server())
    message = stateless_call("work")
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("POST", "/mcp", json=message, headers=headers_for(message)) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            events = read_events(response)
    assert [event["params"]["progress"] for event in events[:-1]] == [1, 2, 3]
    assert all(event["method"] == PROGRESS_METHOD for event in events[:-1])
    assert all(event["params"]["progressToken"] == "h-1" for event in events[:-1])
    final = events[-1]
    assert final["id"] == 1
    assert final["result"]["content"][0]["text"] == "done"
    assert final["result"]["resultType"] == "complete"


def test_http_answers_as_json_when_nothing_is_reported(live_server: LiveServer) -> None:
    base = live_server(slow_server())
    message = stateless_call("quiet")
    with httpx.Client(base_url=base, timeout=10) as client:
        response = client.post("/mcp", json=message, headers=headers_for(message))
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["result"]["content"][0]["text"] == "quiet"


def test_http_answers_as_json_without_a_token(live_server: LiveServer) -> None:
    base = live_server(slow_server())
    message = stateless_call("work", token=None)
    with httpx.Client(base_url=base, timeout=10) as client:
        response = client.post("/mcp", json=message, headers=headers_for(message))
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["result"]["content"][0]["text"] == "done"


@pytest.mark.parametrize("accept", ["application/json", None])
def test_http_answers_as_json_to_a_client_that_reads_no_streams(
    live_server: LiveServer, accept: str | None
) -> None:
    base = live_server(slow_server())
    message = stateless_call("work")
    headers = headers_for(message)
    del headers["Accept"]
    if accept is not None:
        headers["Accept"] = accept
    # Sent as built: a client's own request would add "Accept: */*".
    request = httpx.Request("POST", base + "/mcp", json=message, headers=headers)
    assert ("accept" in request.headers) is (accept is not None)
    with httpx.Client(timeout=10) as client:
        response = client.send(request)
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["result"]["content"][0]["text"] == "done"


def test_an_unknown_tool_with_a_token_keeps_its_json_error(live_server: LiveServer) -> None:
    base = live_server(slow_server())
    message = stateless_call("missing")
    with httpx.Client(base_url=base, timeout=10) as client:
        response = client.post("/mcp", json=message, headers=headers_for(message))
    assert response.headers["content-type"].startswith("application/json")
    assert "error" in response.json()


def test_session_http_streams_progress_and_keeps_the_session(
    live_server: LiveServer, unthrottled: None
) -> None:
    base = live_server(slow_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        init = client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        session = {"MCP-Session-Id": init.headers["mcp-session-id"], **ACCEPT}
        client.post("/mcp", json=notification("notifications/initialized"), headers=session)
        message = call("work", {"steps": 2}, token=42, msg_id=5)
        with client.stream("POST", "/mcp", json=message, headers=session) as response:
            assert response.headers["content-type"].startswith("text/event-stream")
            events = read_events(response)
        assert [event.get("method", "response") for event in events] == [
            PROGRESS_METHOD,
            PROGRESS_METHOD,
            "response",
        ]
        assert events[0]["params"]["progressToken"] == 42
        assert events[-1]["id"] == 5 and "resultType" not in events[-1]["result"]
        # The session is still there, and serves plain answers as before.
        again = client.post("/mcp", json=call("quiet", token=None, msg_id=6), headers=session)
        assert again.headers["content-type"].startswith("application/json")
        assert again.json()["result"]["content"][0]["text"] == "quiet"


def test_closing_the_stream_cancels_a_stateless_call(
    live_server: LiveServer, unthrottled: None
) -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    cancelled = threading.Event()

    @server.tool
    def forever() -> str:
        token = current_cancel_token()
        assert token is not None
        report_progress(1)
        if token.wait(10):
            cancelled.set()
        return "stopped"

    base = live_server(server)
    message = stateless_call("forever")
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("POST", "/mcp", json=message, headers=headers_for(message)) as response:
            assert response.headers["content-type"].startswith("text/event-stream")
            for line in response.iter_lines():
                if line.startswith("data: "):
                    break  # the first report arrived: hang up
    assert cancelled.wait(5)


def test_legacy_sse_carries_progress_on_the_session_stream(
    live_server: LiveServer, unthrottled: None
) -> None:
    base = live_server(slow_server())
    with httpx.Client(base_url=base, timeout=10) as client:
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()
            endpoint = ""
            for line in lines:
                if line.startswith("data: "):
                    endpoint = line[len("data: ") :]
                    break
            client.post(endpoint, json=rpc("initialize", INIT, 1))
            client.post(endpoint, json=notification("notifications/initialized"))
            client.post(endpoint, json=call("work", {"steps": 2}, token="l-1", msg_id=2))
            events: list[dict[str, Any]] = []
            for line in lines:
                if line.startswith("data: "):
                    events.append(json.loads(line[len("data: ") :]))
                    if events[-1].get("id") == 2:
                        break
    tail = [event for event in events if event.get("id") != 1]
    assert [event.get("method", "response") for event in tail] == [
        PROGRESS_METHOD,
        PROGRESS_METHOD,
        "response",
    ]
    assert tail[1]["params"] == {"progressToken": "l-1", "progress": 2, "total": 2}


# ------------------------------------------ a stream cut short by the server


def events_of(response: httpx.Response) -> list[dict[str, Any]]:
    """Every ``message`` event of a text/event-stream response read to its end."""
    assert response.headers["content-type"].startswith("text/event-stream"), response.text
    return [
        json.loads(line[len("data: ") :])
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]


def waiting_server() -> tuple[MCPServer, asyncio.Event]:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    started = asyncio.Event()

    @server.tool
    async def wait() -> str:
        """Report once, then wait to be stopped."""
        report_progress(1, message="waiting")
        started.set()
        await asyncio.sleep(30)
        return "never"

    return server, started


async def test_shutdown_ends_a_progress_stream_with_its_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from easy_mcp import StreamableHTTPTransport
    from easy_mcp.exceptions import SERVER_BUSY
    from easy_mcp.transport import streamable_http

    monkeypatch.setattr(streamable_http, "_SHUTDOWN_GRACE_SECONDS", 0.1)
    server, started = waiting_server()
    transport = StreamableHTTPTransport(server)
    app = transport.build_app()  # its lifespan never runs here
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        message = stateless_call("wait", token="s-1")
        call = asyncio.create_task(client.post("/mcp", json=message, headers=headers_for(message)))
        await asyncio.wait_for(started.wait(), 5)
        await asyncio.sleep(0.05)  # the report is on the stream by now
        await asyncio.wait_for(transport.close_streams(), 5)
        events = events_of(await asyncio.wait_for(call, 5))
    assert events[0]["params"] == {"progressToken": "s-1", "progress": 1, "message": "waiting"}
    final = events[-1]
    assert final["id"] == 1 and final["error"]["code"] == SERVER_BUSY
    assert final["error"]["data"] == {"reason": "shutdown"}
    assert len(events) == 2
    assert transport._dispatches == set()


async def test_ending_the_session_ends_its_progress_stream_without_an_answer() -> None:
    from easy_mcp import StreamableHTTPTransport

    server, started = waiting_server()
    transport = StreamableHTTPTransport(server)
    app = transport.build_app()  # its lifespan never runs here
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        init = await client.post("/mcp", json=rpc("initialize", INIT), headers=ACCEPT)
        session = {**ACCEPT, "MCP-Session-Id": init.headers["mcp-session-id"]}
        message = call("wait", token="d-1", msg_id=3)
        running = asyncio.create_task(client.post("/mcp", json=message, headers=session))
        await asyncio.wait_for(started.wait(), 5)
        await asyncio.sleep(0.05)
        deleted = await client.delete("/mcp", headers=session)
        assert deleted.status_code == 204
        events = events_of(await asyncio.wait_for(running, 5))
        # As for any cancelled request: its progress so far, and no response.
        assert [event.get("method") for event in events] == [PROGRESS_METHOD]
        gone = await client.post("/mcp", json=rpc("ping", msg_id=4), headers=session)
        assert gone.status_code == 404
    for _ in range(100):
        if not transport._sessions and not transport._stream_ends:
            break
        await asyncio.sleep(0.01)
    assert transport._sessions == {}
    assert transport._dispatches == set() and transport._stream_ends == set()
