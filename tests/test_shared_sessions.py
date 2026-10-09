"""Two workers sharing one store: every cross-worker path, without Redis.

Each test serves two ``MCPServer``s, each with its own FakeSharedStore on one
FakeHub (tests/shared_store_fake.py) and each on its own uvicorn thread and
event loop, as two worker processes behind a load balancer would be.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
import uvicorn
from conftest import LogCapture, headers_for, modern, notification, rpc
from shared_store_fake import FakeHub, everything, records
from starlette.applications import Starlette

from easy_mcp import (
    APIKeyAuth,
    MCPServer,
    SSETransport,
    StreamableHTTPTransport,
    current_cancel_token,
)
from easy_mcp.exceptions import (
    FORBIDDEN,
    INTERNAL_ERROR,
    RATE_LIMITED,
    SERVER_BUSY,
    SESSION_LIMIT_EXCEEDED,
    TOO_MANY_SESSIONS,
    ProtocolError,
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


class GracefulServer(uvicorn.Server):
    """Stops as BaseHTTPTransport.run() does: the streams and running requests first."""

    def __init__(self, config: uvicorn.Config, transport: Any) -> None:
        super().__init__(config)
        self.transport = transport

    async def shutdown(self, sockets: Any = None) -> None:
        await self.transport.close_streams()
        await super().shutdown(sockets)


@pytest.fixture
def serve() -> Iterator[Callable[..., Worker]]:
    running: list[Worker] = []

    def start(
        store: Any,
        *,
        idle: float | None = 3600.0,
        sse_only: bool = False,
        graceful: bool = False,
        **options: Any,
    ) -> Worker:
        signals = Signals()
        server = make_server(store, signals, **options)
        transport: Any
        if sse_only:
            transport = SSETransport(server)
        else:
            transport = StreamableHTTPTransport(server, session_idle_timeout=idle)
        config = uvicorn.Config(
            transport.build_app(), host="127.0.0.1", port=0, log_level="warning"
        )
        uv = GracefulServer(config, transport) if graceful else uvicorn.Server(config)
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


# One client per thread, kept open: a client of its own per request costs a
# new connection and certificate store each time, which on a slow machine
# eats the short idle timeouts some tests use.
_local = threading.local()
_clients: list[httpx.Client] = []


def client() -> httpx.Client:
    found: httpx.Client | None = getattr(_local, "client", None)
    if found is None:
        found = _local.client = httpx.Client(timeout=10)
        _clients.append(found)
    return found


@pytest.fixture(scope="module", autouse=True)
def close_clients() -> Iterator[None]:
    yield
    while _clients:
        _clients.pop().close()


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
    return client().post(f"{worker.base}/mcp", json=message, headers=headers, timeout=timeout)


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


class Clock:
    """A hand-moved clock for a FakeHub, read from every worker's thread."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_refused_requests_do_not_keep_a_session_alive(serve: Callable[..., Worker]) -> None:
    # As with MemoryStore: a request refused for its credential (403) or its
    # version header (400) leaves the session's idle time running.
    clock = Clock()
    hub = FakeHub(clock=clock)
    a = serve(hub.store(WORKER_A), idle=1.0)
    b = serve(hub.store(WORKER_B), idle=1.0)
    other_key, bad_version = open_session(a), open_session(a)
    clock.now += 0.8
    assert post(b, rpc("ping", msg_id=2), session=other_key, key=KEY_B).status_code == 403
    headers = {**ACCEPT, "Authorization": f"Bearer {KEY_A}", "MCP-Session-Id": bad_version}
    headers["MCP-Protocol-Version"] = "1999-01-01"
    refused = client().post(f"{b.base}/mcp", json=rpc("ping", msg_id=3), headers=headers)
    assert refused.status_code == 400
    clock.now += 0.8  # 1.6 s after the last accepted request
    for session in (other_key, bad_version):
        assert post(a, rpc("ping", msg_id=4), session=session).status_code == 404


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
    _, a, b = pair(serve, idle=0.5)
    session = open_session(a)
    answered = post(a, call("slow", 2, seconds=1.5), session=session)
    assert text(answered) == "slept"
    assert post(b, rpc("ping", msg_id=3), session=session).status_code == 200


def test_a_session_ended_elsewhere_cancels_local_calls_on_heartbeat(
    serve: Callable[..., Worker],
) -> None:
    hub, a, _ = pair(serve, idle=1.5)
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


def test_worker_shutdown_keeps_the_sessions_it_is_serving(
    serve: Callable[..., Worker], logs: LogCapture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A stops while one of the session's calls runs there, so its shutdown
    # still holds the session: it stops the call and leaves the session to B.
    monkeypatch.setattr("easy_mcp.transport.streamable_http._SHUTDOWN_GRACE_SECONDS", 0.3)
    hub = FakeHub()
    a = serve(hub.store(WORKER_A), graceful=True)
    b = serve(hub.store(WORKER_B))
    session = open_session(a)
    running = Background(lambda: post(a, call("slow", 2), session=session, timeout=20))
    assert a.signals.started.wait(5)
    a.stop()
    answered = running.result()
    assert answered.status_code == 503
    assert answered.json()["error"]["code"] == SERVER_BUSY
    assert a.signals.cancelled.is_set()
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
    return client().post(
        f"{worker.base}{endpoint}", json=message, headers={"Authorization": f"Bearer {KEY_A}"}
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
    clock = Clock()
    hub = FakeHub(clock=clock)
    a, b = serve(hub.store(WORKER_A)), serve(hub.store(WORKER_B))
    _, lines, endpoint = sse(a)
    # Its owner could not renew the lease for longer than it lasts (a store
    # outage): the record lapses, as a Redis TTL would, and only the index
    # still names the session.
    clock.now += _sessions.SSE_LEASE_SECONDS + 1
    ended = threading.Event()

    def drain() -> None:
        for _ in lines:
            pass
        ended.set()

    threading.Thread(target=drain, daemon=True).start()
    assert ended.wait(3), "the stream was not closed"
    sse(b)  # an open on another worker prunes the index: its close is not audited again
    # (B's own stream may close meanwhile: its lines are dropped.)
    ref = session_ref(endpoint.split("=", 1)[1])
    (closed,) = [event for event in logs.events("session_close") if event["session_ref"] == ref]
    assert closed["reason"] == "lease_lost" and closed["worker"] == WORKER_A
    assert closed["client_id"] == fingerprint(KEY_A)


def test_sse_a_session_the_store_lost_closes_the_stream(
    serve: Callable[..., Worker],
    sse: Callable[[Worker], Any],
    logs: LogCapture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Its record and index entry vanish together (a failover to a replica
    # that missed them, a restart without persistence): nobody removed it,
    # so its owner audits its close.
    monkeypatch.setattr(_sessions, "SSE_HEARTBEAT_SECONDS", 0.2)
    hub, a, b = pair(serve)
    _, lines, endpoint = sse(a)
    ref = session_ref(endpoint.split("=", 1)[1])
    hub.remove(ref)
    ended = threading.Event()

    def drain() -> None:
        for _ in lines:
            pass
        ended.set()

    threading.Thread(target=drain, daemon=True).start()
    assert ended.wait(3), "the stream was not closed"
    sse(b)  # nothing is left for another worker to prune (B's own stream may close)
    (closed,) = [event for event in logs.events("session_close") if event["session_ref"] == ref]
    assert closed["reason"] == "lease_lost" and closed["worker"] == WORKER_A


async def test_a_session_close_is_audited_once_by_whoever_removes_it(logs: LogCapture) -> None:
    # Whichever worker removes what the store still holds of a session audits
    # its close, in whatever order the owner and the others find it gone.
    clock = Clock()
    hub = FakeHub(clock=clock)
    server_a = make_server(hub.store(WORKER_A), Signals())
    server_b = make_server(hub.store(WORKER_B), Signals())
    a, b = SSETransport(server_a)._manager, SSETransport(server_b)._manager
    http_a = StreamableHTTPTransport(server_a, legacy_sse=False)._manager
    http_b = StreamableHTTPTransport(server_b, legacy_sse=False)._manager
    lapse = _sessions.SSE_LEASE_SECONDS + 1

    async def stream(manager: _sessions.SessionManager, name: str) -> _sessions.LocalSession:
        local = await manager.open(name, client_id=f"ip:{name}", identity=None, owned=True)
        assert local is not None
        await manager.finish(local)
        manager.opened(local)
        return local

    def closes(local: _sessions.LocalSession) -> list[str]:
        events = logs.events("session_close")
        return [event["reason"] for event in events if event["session_ref"] == local.ref]

    async def end_offline(manager: _sessions.SessionManager, name: str) -> _sessions.LocalSession:
        local = await stream(manager, name)
        hub.down = True
        await manager.end(local, reason="stream_closed")  # the store cannot be told
        hub.down = False
        assert local.ended and closes(local) == []
        return local

    try:
        # The owner finds its lease lost, then another worker's open prunes.
        first = await stream(a, "first")
        clock.now += lapse
        await a._refresh()
        assert first.ended
        await stream(b, "b1")
        assert closes(first) == ["lease_lost"]
        # Another worker's open prunes it, then its owner finds it gone.
        second = await stream(a, "second")
        clock.now += lapse
        await stream(b, "b2")
        await a._refresh()
        assert second.ended and closes(second) == ["lease_lost"]
        # The stream closes during an outage: the owner's next heartbeat
        # removes the session and audits its close.
        third = await end_offline(a, "third")
        await a._refresh()
        clock.now += lapse
        await stream(b, "b3")
        assert closes(third) == ["stream_closed"]
        # The lease lapses before that: another worker's open prunes it.
        fourth = await end_offline(a, "fourth")
        clock.now += lapse
        await stream(b, "b4")
        await a._refresh()
        assert closes(fourth) == ["lease_lost"]
        # Or the owner's own open does, which still knows why it ended.
        fifth = await end_offline(a, "fifth")
        clock.now += lapse
        await stream(a, "a5")
        await a._refresh()
        assert closes(fifth) == ["stream_closed"]
        # A failed handshake opened no session, so its removal closes none.
        handshake = await http_a.open("handshake", client_id="ip:h", identity=None)
        assert handshake is not None
        hub.down = True
        await http_a.end(handshake, reason=None)
        hub.down = False
        await http_a._refresh()
        clock.now += 7200
        assert await http_b.open("later", client_id="ip:l", identity=None) is not None
        assert closes(handshake) == []
    finally:
        for manager in (a, b, http_a, http_b):
            await manager.shutdown()


async def test_a_session_the_store_lost_is_audited_once(logs: LogCapture) -> None:
    # The store lost what it held of three sessions (a failover to a replica
    # that missed them, a restart without persistence): nobody removed them,
    # so the worker serving each audits its close, and no other worker does.
    hub = FakeHub()
    server_a = make_server(hub.store(WORKER_A), Signals())
    server_b = make_server(hub.store(WORKER_B), Signals())
    sse_a = SSETransport(server_a)._manager
    http_a = StreamableHTTPTransport(server_a, legacy_sse=False)._manager
    http_b = StreamableHTTPTransport(server_b, legacy_sse=False)._manager

    async def stream(name: str) -> _sessions.LocalSession:
        local = await sse_a.open(name, client_id=f"ip:{name}", identity=None, owned=True)
        assert local is not None
        await sse_a.finish(local)
        sse_a.opened(local)
        return local

    def closes() -> dict[str, list[tuple[str, str]]]:
        found: dict[str, list[tuple[str, str]]] = {}
        for event in logs.events("session_close"):
            found.setdefault(event["client_id"], []).append((event["reason"], event["worker"]))
        return found

    try:
        lost, closing = await stream("lost"), await stream("closing")
        held = await http_a.open("held", client_id="ip:held", identity=None)
        assert held is not None  # held by its opener, as during its handshake
        http_a.opened(held)
        on_b = await http_b.acquire("held")  # one of its requests runs on B too
        assert isinstance(on_b, _sessions.LocalSession)
        for local in (lost, closing, held):
            hub.remove(local.ref)
        # A stream that closes before the heartbeat finds it lost.
        await sse_a.end(closing, reason="stream_closed")
        for manager in (sse_a, http_a, http_b):
            await manager._refresh()
        assert lost.ended and held.ended and on_b.ended
        expected = {
            "ip:lost": [("lease_lost", WORKER_A)],
            "ip:closing": [("stream_closed", WORKER_A)],
            "ip:held": [("store_lost", WORKER_A)],
        }
        assert closes() == expected
        await http_b._refresh()
        assert await http_b.open("later", client_id="ip:later", identity=None) is not None
        assert closes() == expected
    finally:
        for manager in (sse_a, http_a, http_b):
            await manager.shutdown()


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


def test_sse_relay_of_an_answer_that_is_not_plain_json(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any], logs: LogCapture
) -> None:
    # Answered on the stream whichever worker the message reached, as the
    # stream's own worker writes it: str() for what JSON has no type for.
    _, a, b = pair(serve)
    ticket = uuid.uuid4()
    data: dict[Any, Any] = {"ticket": ticket, "until": datetime.date(2030, 1, 1), 7: "mixed keys"}

    async def refuse(request: Any, call_next: Any) -> Any:
        if request.method == "tools/call":
            name = request.params.get("name")
            raise ProtocolError(
                "refused", code=-32001, data={(1, 2): "x"} if name == "big" else data
            )
        return await call_next()

    for worker in (a, b):
        worker.server.middleware(refuse)
    _, lines, endpoint = sse(a)
    expected = {"ticket": str(ticket), "until": "2030-01-01", "7": "mixed keys"}
    for worker, msg_id in ((a, 1), (b, 2)):
        assert post_message(worker, endpoint, call("add", msg_id, a=1, b=1)).status_code == 202
        answer = json.loads(next_data(lines))
        assert answer["id"] == msg_id and answer["error"]["data"] == expected
    # What no JSON can carry still gets an answer, rather than none.
    assert post_message(b, endpoint, call("big", 3, size=1)).status_code == 202
    answer = json.loads(next_data(lines))
    assert answer["id"] == 3 and answer["error"]["code"] == INTERNAL_ERROR
    (failed,) = logs.events("sse_relay_failed")
    assert failed["reason"] == "unserializable"
    assert failed["error_id"] in answer["error"]["message"]


def test_sse_relay_that_fails_still_answers(
    serve: Callable[..., Worker],
    sse: Callable[[Worker], Any],
    logs: LogCapture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(*args: Any) -> None:
        raise RuntimeError("a bug in the relay")

    _, a, b = pair(serve)
    _, lines, endpoint = sse(a)
    monkeypatch.setattr("easy_mcp.transport.sse._negotiated", broken)
    assert post_message(b, endpoint, call("add", 1, a=1, b=1)).status_code == 202
    answer = json.loads(next_data(lines))
    assert answer["id"] == 1 and answer["error"]["code"] == INTERNAL_ERROR
    error_id = answer["error"]["message"].split("error_id=")[1].rstrip(")")
    assert f"could not relay an answer error_id={error_id}" in logs.text


@pytest.mark.parametrize("sse_only", [False, True], ids=["streamable-http", "sse"])
def test_sse_calls_a_stopping_worker_relays_are_answered(
    serve: Callable[..., Worker],
    sse: Callable[[Worker], Any],
    monkeypatch: pytest.MonkeyPatch,
    sse_only: bool,
) -> None:
    # B stops while it serves calls for the stream A holds, whose client
    # still listens: they get the grace a Streamable HTTP request gets, then
    # an answer to retry shortly.
    for module in ("streamable_http", "sse"):
        monkeypatch.setattr(f"easy_mcp.transport.{module}._SHUTDOWN_GRACE_SECONDS", 1.0)
    hub = FakeHub()
    a = serve(hub.store(WORKER_A))
    b = serve(hub.store(WORKER_B), sse_only=sse_only)
    _, lines, endpoint = sse(a)
    assert post_message(b, endpoint, call("slow", 1, seconds=0.3)).status_code == 202
    assert post_message(b, endpoint, call("slow", 2, seconds=30)).status_code == 202
    assert b.signals.started.wait(5)
    b.stop()
    answers = {}
    for _ in range(2):
        answer = json.loads(next_data(lines))
        answers[answer["id"]] = answer
    assert answers[1]["result"]["content"][0]["text"] == "slept"  # within the grace
    error = answers[2]["error"]
    assert error["code"] == SERVER_BUSY and error["data"] == {"reason": "shutdown"}
    assert b.signals.cancelled.is_set()
    # The stream lives on, served by A.
    assert post_message(a, endpoint, call("add", 3, a=1, b=2)).status_code == 202
    assert json.loads(next_data(lines))["id"] == 3


@pytest.mark.parametrize("limit", [120, None], ids=["rate-limited", "unlimited"])
def test_sse_message_to_the_stream_worker_during_an_outage(
    serve: Callable[..., Worker], sse: Callable[[Worker], Any], limit: int | None
) -> None:
    # The worker holding the stream accepts the message without the store,
    # but answers it -32008 on the stream if serving it needs the store:
    # for the rate limit, or a capped tool.
    hub = FakeHub()
    a = serve(hub.store(WORKER_A), rate_limit_per_minute=limit)
    _, lines, endpoint = sse(a)
    hub.down = True
    try:
        assert post_message(a, endpoint, rpc("ping", msg_id=1)).status_code == 202
        pinged = json.loads(next_data(lines))
        assert post_message(a, endpoint, call("scarce", 2)).status_code == 202
        capped = json.loads(next_data(lines))
    finally:
        hub.down = False
    if limit is None:
        assert pinged["id"] == 1 and pinged["result"] == {}
    else:
        assert pinged["id"] == 1 and pinged["error"]["code"] == SERVER_BUSY
        assert pinged["error"]["data"] == {"reason": "store_unavailable"}
    assert capped["id"] == 2 and capped["error"]["code"] == SERVER_BUSY
    assert capped["error"]["data"] == {"reason": "store_unavailable"}


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


# ------------------------------------------ several endpoints, one worker
#
# One server served at two endpoints of a kind in one process: their session
# managers share the worker's store, and so its worker id.


@pytest.mark.parametrize("stop", ["cancel", "delete"])
async def test_a_cancel_or_delete_reaches_a_call_at_another_endpoint(stop: str) -> None:
    signals = Signals()
    server = make_server(FakeHub().store(WORKER_A), signals)
    one, two = (StreamableHTTPTransport(server, legacy_sse=False) for _ in range(2))
    auth = {**ACCEPT, "Authorization": f"Bearer {KEY_A}"}

    def connect(transport: StreamableHTTPTransport) -> httpx.AsyncClient:
        app = httpx.ASGITransport(app=transport.build_app())
        return httpx.AsyncClient(transport=app, base_url="http://127.0.0.1")

    async with connect(one) as first, connect(two) as second:
        opened = await first.post("/mcp", json=rpc("initialize", INIT, "init"), headers=auth)
        headers = {**auth, "MCP-Session-Id": opened.headers["mcp-session-id"]}
        running = asyncio.ensure_future(second.post("/mcp", json=call("slow", 7), headers=headers))
        try:
            for _ in range(500):
                if signals.started.is_set() or running.done():
                    break
                await asyncio.sleep(0.01)
            assert signals.started.is_set()
            if stop == "cancel":
                cancel = notification("notifications/cancelled", {"requestId": 7})
                assert (await first.post("/mcp", json=cancel, headers=headers)).status_code == 202
            else:
                assert (await first.delete("/mcp", headers=headers)).status_code == 204
            answered = await asyncio.wait_for(running, 3)
        finally:
            running.cancel()
    assert answered.status_code == 202  # cancelled: no JSON-RPC answer
    assert signals.cancelled.is_set() and not signals.finished.is_set()


@pytest.mark.parametrize("stop", ["cancel", "close"])
def test_sse_a_cancel_or_close_reaches_a_call_at_another_endpoint(
    live_server: Callable[[Any], str], stop: str
) -> None:
    signals = Signals()
    server = make_server(FakeHub().store(WORKER_A), signals)
    one = SSETransport(server, sse_path="/one/sse", messages_path="/one/messages")
    two = SSETransport(server, sse_path="/two/sse", messages_path="/two/messages")
    base = live_server(Starlette(routes=[*one.routes(), *two.routes()]))
    auth = {"Authorization": f"Bearer {KEY_A}"}
    with httpx.Client(base_url=base, timeout=10) as http:
        with http.stream("GET", "/one/sse", headers=auth) as stream:
            lines = stream.iter_lines()  # kept: dropping it closes the stream
            endpoint = next_data(lines)
            elsewhere = endpoint.replace("/one/", "/two/")
            assert http.post(elsewhere, json=call("slow", 1), headers=auth).status_code == 202
            assert signals.started.wait(5)
            if stop == "cancel":
                cancel = notification("notifications/cancelled", {"requestId": 1})
                assert http.post(endpoint, json=cancel, headers=auth).status_code == 202
                assert signals.cancelled.wait(3)
        # Leaving the stream ends the session: at every endpoint, its calls too.
        assert signals.cancelled.wait(3)
    assert not signals.finished.is_set()


async def test_an_end_reaches_a_lookup_under_way_at_another_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeHub().store(WORKER_A)
    server = make_server(store, Signals())
    one = StreamableHTTPTransport(server, legacy_sse=False)._manager
    two = StreamableHTTPTransport(server, legacy_sse=False)._manager
    opened = await one.open("session", client_id="ip:s", identity=None)
    assert opened is not None
    one.opened(opened)
    found, release = asyncio.Event(), asyncio.Event()
    lookup = store.acquire_session

    async def slow_lookup(*args: Any, **kwargs: Any) -> Any:
        record = await lookup(*args, **kwargs)
        found.set()  # found, and on its way back when the session ends
        await release.wait()
        return record

    monkeypatch.setattr(store, "acquire_session", slow_lookup)
    try:
        acquiring = asyncio.ensure_future(two.acquire("session"))
        await asyncio.wait_for(found.wait(), 5)
        await one.end(opened, reason="client_terminated", strict=True)
        release.set()
        assert await acquiring is _sessions.Rejection.NOT_FOUND
    finally:
        release.set()
        for manager in (one, two):
            await manager.shutdown()
