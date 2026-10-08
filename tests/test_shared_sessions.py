"""Two workers sharing one store: every cross-worker path, without Redis.

Each test serves two ``MCPServer``s, each with its own FakeSharedStore on one
FakeHub (tests/shared_store_fake.py) and each on its own uvicorn thread and
event loop, as two worker processes behind a load balancer would be.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
import uvicorn
from conftest import LogCapture, headers_for, modern, notification, rpc
from shared_store_fake import FakeHub, everything, records

from easy_mcp import APIKeyAuth, MCPServer, StreamableHTTPTransport, current_cancel_token
from easy_mcp.exceptions import (
    FORBIDDEN,
    INTERNAL_ERROR,
    RATE_LIMITED,
    SESSION_LIMIT_EXCEEDED,
    TOO_MANY_SESSIONS,
)
from easy_mcp.security.auth import fingerprint
from easy_mcp.store.base import session_ref
from easy_mcp.transport import _bus, _sessions

KEY_A = "shared-sessions-key-a-" + "a" * 16
KEY_B = "shared-sessions-key-b-" + "b" * 16
WORKER_A = "a" * 16
WORKER_B = "b" * 16
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


@dataclass
class Signals:
    """What the tools of one worker report, across threads."""

    started: threading.Event = field(default_factory=threading.Event)
    cancelled: threading.Event = field(default_factory=threading.Event)
    finished: threading.Event = field(default_factory=threading.Event)
    reasons: list[str | None] = field(default_factory=list)
    ran: list[str] = field(default_factory=list)


@dataclass
class Worker:
    server: MCPServer
    base: str
    signals: Signals
    uv: uvicorn.Server
    thread: threading.Thread

    def stop(self) -> None:
        self.uv.should_exit = True
        self.thread.join(10)


def make_server(store: Any, signals: Signals, **options: Any) -> MCPServer:
    options.setdefault("rate_limit_per_minute", None)
    server = MCPServer(port=0, auth=APIKeyAuth({KEY_A: "*", KEY_B: ["b"]}), store=store, **options)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @server.tool
    def whoami() -> str:
        """The worker that served the call."""
        return str(store.worker_id)

    @server.tool(max_calls_per_session=2)
    def scarce() -> str:
        """Twice per session."""
        signals.ran.append("scarce")
        return "spent"

    @server.tool(scopes=("admin",), requires_auth=True)
    def admin() -> str:
        """Only for admin keys."""
        return "admin"

    @server.tool
    async def slow(seconds: float = 30.0) -> str:
        """Sleeps."""
        signals.started.set()
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            signals.cancelled.set()
            raise
        signals.finished.set()
        return "slept"

    @server.tool
    def slow_sync() -> str:
        """Polls its cancel token."""
        token = current_cancel_token()
        signals.started.set()
        deadline = time.monotonic() + 30
        while token is not None and not token.cancelled and time.monotonic() < deadline:
            time.sleep(0.01)
        signals.reasons.append(token.reason if token is not None else None)
        return "stopped"

    @server.tool
    def big(size: int) -> str:
        """A long answer."""
        return "x" * size

    return server


@pytest.fixture
def serve() -> Iterator[Callable[..., Worker]]:
    running: list[Worker] = []

    def start(store: Any, *, idle: float | None = 3600.0, **options: Any) -> Worker:
        signals = Signals()
        server = make_server(store, signals, **options)
        app = StreamableHTTPTransport(server, session_idle_timeout=idle).build_app()
        uv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"))
        thread = threading.Thread(target=uv.run, daemon=True)
        thread.start()
        deadline = time.time() + 10
        while not uv.started:
            assert time.time() < deadline, "uvicorn did not start"
            time.sleep(0.01)
        port = uv.servers[0].sockets[0].getsockname()[1]
        worker = Worker(server, f"http://127.0.0.1:{port}", signals, uv, thread)
        running.append(worker)
        return worker

    yield start
    for worker in running:
        worker.stop()


def pair(serve: Callable[..., Worker], **options: Any) -> tuple[FakeHub, Worker, Worker]:
    hub = FakeHub()
    return hub, serve(hub.store(WORKER_A), **options), serve(hub.store(WORKER_B), **options)


def post(
    worker: Worker,
    message: dict[str, Any],
    *,
    session: str | None = None,
    key: str | None = KEY_A,
    timeout: float = 10.0,
) -> httpx.Response:
    headers = dict(ACCEPT)
    if key is not None:
        headers["Authorization"] = f"Bearer {key}"
    if session is not None:
        headers["MCP-Session-Id"] = session
    return httpx.post(f"{worker.base}/mcp", json=message, headers=headers, timeout=timeout)


def open_session(worker: Worker, key: str | None = KEY_A) -> str:
    response = post(worker, rpc("initialize", INIT, "init"), key=key)
    assert response.status_code == 200, response.text
    return response.headers["mcp-session-id"]


def call(name: str, msg_id: Any = 1, **arguments: Any) -> dict[str, Any]:
    return rpc("tools/call", {"name": name, "arguments": arguments}, msg_id)


def text(response: httpx.Response) -> str:
    return str(response.json()["result"]["content"][0]["text"])


class Background:
    """A request sent from a thread of its own; its answer is kept."""

    def __init__(self, send: Callable[[], httpx.Response]) -> None:
        self.response: httpx.Response | None = None
        self.error: BaseException | None = None

        def run() -> None:
            try:
                self.response = send()
            except BaseException as exc:
                self.error = exc

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def result(self, timeout: float = 10.0) -> httpx.Response:
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "no answer in time"
        assert self.response is not None, self.error
        return self.response


def wait_until(check: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def next_data(lines: Iterator[str]) -> str:
    for line in lines:
        if line.startswith("data: "):
            return line[len("data: ") :]
    raise AssertionError("the stream ended")


# ------------------------------------------------------- Streamable HTTP


def test_a_session_opened_on_one_worker_works_on_the_other(serve: Callable[..., Worker]) -> None:
    hub, a, b = pair(serve)
    session = open_session(a)
    listed = post(b, rpc("tools/list", msg_id=2), session=session)
    assert listed.status_code == 200
    assert "add" in {tool["name"] for tool in listed.json()["result"]["tools"]}
    assert text(post(b, call("add", 3, a=2, b=3), session=session)) == "5"
    assert text(post(a, call("whoami", 4), session=session)) == WORKER_A
    assert text(post(b, call("whoami", 5), session=session)) == WORKER_B
    # The negotiated version is in the record, for the workers that did not negotiate it.
    (record,) = records(hub)
    assert record.protocol_version == "2025-11-25" and record.owner is None


def test_credential_binding_holds_on_every_worker(
    serve: Callable[..., Worker], logs: LogCapture
) -> None:
    hub, a, b = pair(serve)
    session = open_session(a, key=KEY_A)
    for key in (KEY_B, None):
        refused = post(b, rpc("ping", msg_id=2), session=session, key=key)
        assert refused.status_code == 403
        assert refused.json()["error"]["code"] == FORBIDDEN
    (mismatch, _) = logs.events("session_credential_mismatch")
    assert mismatch["session_ref"] == session_ref(session)
    # Someone who can write to the store binds the session to their own key:
    # that key can use it, with that key's scopes and nothing more.
    ref = session_ref(session)
    with hub.lock:
        entry = hub.sessions[ref]
        entry.record = dataclasses.replace(entry.record, identity_fp=fingerprint(KEY_B))
    hijack = post(b, call("admin", 3), session=session, key=KEY_B)
    assert hijack.status_code == 200
    assert hijack.json()["error"]["message"] == "Unknown tool: admin"
    assert post(b, call("admin", 4), session=session, key=KEY_A).status_code == 403


def test_delete_on_one_worker_ends_the_session_everywhere(serve: Callable[..., Worker]) -> None:
    _, a, b = pair(serve)
    session = open_session(a)
    running = Background(lambda: post(a, call("slow_sync", 2), session=session, timeout=20))
    assert a.signals.started.wait(5)
    deleted = httpx.delete(
        f"{b.base}/mcp",
        headers={"MCP-Session-Id": session, "Authorization": f"Bearer {KEY_A}"},
        timeout=10,
    )
    assert deleted.status_code == 204
    answered = running.result()
    assert answered.status_code == 202  # cancelled: no JSON-RPC answer
    assert wait_until(lambda: a.signals.reasons == ["cancelled"])
    for worker in (a, b):
        assert post(worker, rpc("ping", msg_id=3), session=session).status_code == 404


def test_cancel_reaches_a_call_on_another_worker(
    serve: Callable[..., Worker], logs: LogCapture
) -> None:
    _, a, b = pair(serve)
    session = open_session(a)
    running = Background(lambda: post(a, call("slow", 7), session=session, timeout=20))
    assert a.signals.started.wait(5)
    started = time.monotonic()
    cancel = notification("notifications/cancelled", {"requestId": 7, "reason": "stop"})
    assert post(b, cancel, session=session).status_code == 202
    answered = running.result()
    assert answered.status_code == 202 and time.monotonic() - started < 1.0
    assert a.signals.cancelled.is_set() and not a.signals.finished.is_set()
    assert wait_until(lambda: bool(logs.events("tool_cancelled")))


def test_a_local_cancel_is_not_broadcast(serve: Callable[..., Worker]) -> None:
    hub, a, _ = pair(serve)
    session = open_session(a)
    running = Background(lambda: post(a, call("slow", 7), session=session, timeout=20))
    assert a.signals.started.wait(5)
    cancel = notification("notifications/cancelled", {"requestId": 7})
    assert post(a, cancel, session=session).status_code == 202
    assert running.result().status_code == 202
    assert hub.published("cancel") == []
    # An id nobody runs here may run elsewhere: it is asked for once, to no effect.
    unknown = notification("notifications/cancelled", {"requestId": "nobody"})
    assert post(a, unknown, session=session).status_code == 202
    assert len(hub.published("cancel")) == 1
    # Ids that cannot be relayed are not.
    for request_id in (True, 1.5, {"a": 1}, "x" * 129):
        odd = notification("notifications/cancelled", {"requestId": request_id})
        assert post(a, odd, session=session).status_code == 202
    assert len(hub.published("cancel")) == 1
    assert text(post(a, call("add", 8, a=1, b=1), session=session)) == "2"


def test_a_delete_racing_a_new_call_cancels_it(serve: Callable[..., Worker]) -> None:
    hub, a, b = pair(serve)
    session = open_session(a)

    async def delete_meanwhile() -> None:
        # The session ends on A while B is taking a call unit for it.
        hub.before_reserve = None
        deleted = await asyncio.to_thread(
            httpx.delete,
            f"{a.base}/mcp",
            headers={"MCP-Session-Id": session, "Authorization": f"Bearer {KEY_A}"},
            timeout=10,
        )
        assert deleted.status_code == 204
        await asyncio.sleep(0.5)  # the call is cancelled before this ends

    hub.before_reserve = delete_meanwhile
    answered = post(b, call("scarce", 2), session=session)
    # Cancelled by the end A announced, not refused by a reservation that
    # found the session gone afterwards.
    assert answered.status_code == 202, answered.text
    time.sleep(0.1)
    assert b.signals.ran == []


def test_call_caps_are_shared_between_workers(serve: Callable[..., Worker]) -> None:
    _, a, b = pair(serve)
    session = open_session(a)
    assert text(post(a, call("scarce", 2), session=session)) == "spent"
    assert text(post(b, call("scarce", 3), session=session)) == "spent"
    refused = post(a, call("scarce", 4), session=session)
    assert refused.json()["error"]["code"] == SESSION_LIMIT_EXCEEDED
    # Another session has its own counts.
    other = open_session(b)
    assert text(post(b, call("scarce", 5), session=other)) == "spent"


def test_stateless_caps_and_rate_limits_are_shared(serve: Callable[..., Worker]) -> None:
    _, a, b = pair(serve)

    def stateless(worker: Worker, message: dict[str, Any]) -> httpx.Response:
        headers = {**headers_for(message), "Authorization": f"Bearer {KEY_A}"}
        return httpx.post(f"{worker.base}/mcp", json=message, headers=headers, timeout=10)

    capped = modern("tools/call", {"name": "scarce"})
    assert "result" in stateless(a, capped).json()
    assert "result" in stateless(b, capped).json()
    assert stateless(a, capped).json()["error"]["code"] == SESSION_LIMIT_EXCEEDED

    hub = FakeHub()
    c = serve(hub.store("c" * 16), rate_limit_per_minute=3)
    d = serve(hub.store("d" * 16), rate_limit_per_minute=3)
    listed = modern("tools/list")
    answers = [stateless(worker, listed) for worker in (c, d, c, d)]
    assert [answer.status_code for answer in answers[:3]] == [200, 200, 200]
    assert answers[3].json()["error"]["code"] == RATE_LIMITED


def test_the_session_cap_is_global(serve: Callable[..., Worker]) -> None:
    _, a, b = pair(serve, max_sessions=1)
    open_session(a)
    refused = post(b, rpc("initialize", INIT, "init"))
    assert refused.status_code == 503
    assert refused.json()["error"]["code"] == TOO_MANY_SESSIONS


def test_idle_expiry_is_audited_with_a_session_ref(
    serve: Callable[..., Worker], logs: LogCapture
) -> None:
    _, a, b = pair(serve, idle=0.3)
    session = open_session(a)
    time.sleep(0.6)
    open_session(b)  # prunes the expired one
    closed = [event for event in logs.events("session_close") if event["reason"] == "idle_timeout"]
    assert closed and closed[0]["session_ref"] == session_ref(session)
    assert "session_id" not in closed[0]
    assert post(a, rpc("ping", msg_id=2), session=session).status_code == 404


def test_a_long_call_keeps_its_session_alive(serve: Callable[..., Worker]) -> None:
    _, a, b = pair(serve, idle=0.3)
    session = open_session(a)
    answered = post(a, call("slow", 2, seconds=1.0), session=session)
    assert text(answered) == "slept"
    assert post(b, rpc("ping", msg_id=3), session=session).status_code == 200


def test_a_session_ended_elsewhere_cancels_local_calls_on_heartbeat(
    serve: Callable[..., Worker],
) -> None:
    hub, a, _ = pair(serve, idle=0.6)
    session = open_session(a)
    running = Background(lambda: post(a, call("slow", 2), session=session, timeout=20))
    assert a.signals.started.wait(5)
    hub.remove(session_ref(session))  # no message on the bus
    assert running.result(timeout=3).status_code == 202
    assert a.signals.cancelled.is_set()


def test_worker_shutdown_keeps_shared_sessions(
    serve: Callable[..., Worker], logs: LogCapture
) -> None:
    hub, a, b = pair(serve)
    session = open_session(a)
    assert text(post(a, call("whoami", 2), session=session)) == WORKER_A
    a.stop()
    assert a.server.store.closed == 1  # type: ignore[attr-defined]
    assert [record.ref for record in records(hub)] == [session_ref(session)]
    assert text(post(b, call("whoami", 3), session=session)) == WORKER_B
    assert logs.events("session_close") == []


def test_session_events_carry_ref_worker_and_version(
    serve: Callable[..., Worker], logs: LogCapture
) -> None:
    _, a, b = pair(serve)
    session = open_session(a)
    deleted = httpx.delete(
        f"{b.base}/mcp",
        headers={"MCP-Session-Id": session, "Authorization": f"Bearer {KEY_A}"},
        timeout=10,
    )
    assert deleted.status_code == 204
    (opened,) = logs.events("session_open")
    assert opened["session_ref"] == session_ref(session) and opened["worker"] == WORKER_A
    assert opened["protocol_version"] == "2025-11-25" and opened["session_id"] == session
    (closed,) = logs.events("session_close")
    assert closed["worker"] == WORKER_B and closed["reason"] == "client_terminated"
    assert closed["session_ref"] == session_ref(session)


# --------------------------------------------------------- legacy SSE


@pytest.fixture
def sse() -> Iterator[Callable[[Worker], tuple[Any, Iterator[str], str]]]:
    clients: list[httpx.Client] = []
    streams: list[Any] = []

    def open_stream(worker: Worker) -> tuple[Any, Iterator[str], str]:
        client = httpx.Client(base_url=worker.base, timeout=10)
        clients.append(client)
        stream = client.stream("GET", "/sse", headers={"Authorization": f"Bearer {KEY_A}"})
        response = stream.__enter__()
        streams.append(stream)
        lines = response.iter_lines()
        endpoint = next_data(lines)
        return stream, lines, endpoint

    yield open_stream
    for stream in streams:
        stream.__exit__(None, None, None)
    for client in clients:
        client.close()


def post_message(worker: Worker, endpoint: str, message: dict[str, Any]) -> httpx.Response:
    return httpx.post(
        f"{worker.base}{endpoint}",
        json=message,
        headers={"Authorization": f"Bearer {KEY_A}"},
        timeout=10,
    )


def test_sse_post_to_another_worker_is_answered_on_the_stream(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any]
) -> None:
    hub, a, b = pair(serve)
    _, lines, endpoint = sse(a)
    (record,) = records(hub)
    assert record.kind == "sse" and record.owner == WORKER_A
    assert post_message(b, endpoint, rpc("initialize", INIT, 1)).status_code == 202
    assert json.loads(next_data(lines))["result"]["protocolVersion"] == "2025-11-25"
    assert post_message(b, endpoint, call("whoami", 2)).status_code == 202
    answer = json.loads(next_data(lines))
    assert answer["id"] == 2 and answer["result"]["content"][0]["text"] == WORKER_B
    assert post_message(a, endpoint, call("add", 3, a=1, b=2)).status_code == 202
    assert json.loads(next_data(lines))["result"]["content"][0]["text"] == "3"
    # The version negotiated through B is recorded for every worker.
    (record,) = records(hub)
    assert record.protocol_version == "2025-11-25"


def test_sse_cancel_posted_to_another_worker(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any]
) -> None:
    _, a, b = pair(serve)
    _, lines, endpoint = sse(a)
    assert post_message(a, endpoint, call("slow", 1)).status_code == 202
    assert post_message(b, endpoint, call("slow", 2)).status_code == 202
    assert a.signals.started.wait(5) and b.signals.started.wait(5)
    cancel_a = notification("notifications/cancelled", {"requestId": 1})
    cancel_b = notification("notifications/cancelled", {"requestId": 2})
    assert post_message(b, endpoint, cancel_a).status_code == 202
    assert post_message(a, endpoint, cancel_b).status_code == 202
    assert wait_until(lambda: a.signals.cancelled.is_set() and b.signals.cancelled.is_set())
    # Neither was answered: the next answer on the stream is this one.
    assert post_message(b, endpoint, call("add", 3, a=2, b=2)).status_code == 202
    assert json.loads(next_data(lines))["id"] == 3


def test_sse_stream_close_ends_the_session_everywhere(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any], logs: LogCapture
) -> None:
    hub, a, b = pair(serve)
    stream, _, endpoint = sse(a)
    assert post_message(b, endpoint, call("slow", 1)).status_code == 202
    assert b.signals.started.wait(5)
    stream.__exit__(None, None, None)  # the client leaves
    assert wait_until(lambda: b.signals.cancelled.is_set(), timeout=5)
    assert wait_until(lambda: post_message(b, endpoint, rpc("ping", msg_id=2)).status_code == 404)
    assert records(hub) == []
    (closed,) = logs.events("session_close")
    assert closed["reason"] == "stream_closed" and closed["transport"] == "sse"


def test_sse_lease_loss_closes_the_stream(
    serve: Callable[..., Worker],
    sse: Callable[[Worker], Any],
    logs: LogCapture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_sessions, "SSE_HEARTBEAT_SECONDS", 0.2)
    hub, a, _ = pair(serve)
    _, lines, endpoint = sse(a)
    hub.remove(session_ref(endpoint.split("=", 1)[1]))
    ended = threading.Event()

    def drain() -> None:
        for _ in lines:
            pass
        ended.set()

    threading.Thread(target=drain, daemon=True).start()
    assert ended.wait(3), "the stream was not closed"
    (closed,) = logs.events("session_close")
    assert closed["reason"] == "lease_lost"


def test_sse_relay_of_an_oversized_answer_is_an_error(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any], logs: LogCapture
) -> None:
    _, a, b = pair(serve)
    _, lines, endpoint = sse(a)
    assert post_message(b, endpoint, call("big", 1, size=_bus.RELAY_MAX_BYTES)).status_code == 202
    answer = json.loads(next_data(lines))
    assert answer["id"] == 1 and answer["error"]["code"] == INTERNAL_ERROR
    assert "error_id=" in answer["error"]["message"]
    (failed,) = logs.events("sse_relay_failed")
    assert failed["reason"] == "too_large"
    # Under the cap, it is relayed.
    assert post_message(b, endpoint, call("big", 2, size=1000)).status_code == 202
    assert len(json.loads(next_data(lines))["result"]["content"][0]["text"]) == 1000


def test_sse_relay_to_a_dead_owner_is_audited(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any], logs: LogCapture
) -> None:
    hub, a, b = pair(serve)
    _, _, endpoint = sse(a)
    hub.drop_bus = True  # nobody receives what is published
    assert post_message(b, endpoint, call("add", 1, a=1, b=1)).status_code == 202
    assert wait_until(lambda: bool(logs.events("sse_relay_failed")))
    (failed,) = logs.events("sse_relay_failed")
    assert failed["reason"] == "owner_unreachable"


def test_the_store_never_sees_raw_ids_or_keys(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any]
) -> None:
    hub, a, b = pair(serve)
    session = open_session(a)
    post(b, call("scarce", 2), session=session)
    running = Background(lambda: post(a, call("slow", 3), session=session, timeout=20))
    assert a.signals.started.wait(5)
    post(b, notification("notifications/cancelled", {"requestId": 3}), session=session)
    running.result()
    _, lines, endpoint = sse(a)
    stream_id = endpoint.split("=", 1)[1]
    post_message(b, endpoint, call("add", 4, a=1, b=1))
    next_data(lines)
    stored = repr(everything(hub))
    for secret in (session, stream_id, KEY_A, KEY_B):
        assert secret not in stored
    for record in records(hub):
        assert record.session_id is None
        assert record.identity_fp is not None and len(record.identity_fp) == 12
        int(record.identity_fp, 16)
    assert hub.published("cancel") and hub.published("deliver")
