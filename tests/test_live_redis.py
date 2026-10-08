"""RedisStore across real worker processes and a real Redis.

Skipped unless ``EASY_MCP_LIVE_REDIS_URL`` is set to an admin-capable URL of
a scratch database, for example::

    docker run -d --name easy-mcp-redis -p 6379:6379 redis:7-alpine
    EASY_MCP_LIVE_REDIS_URL=redis://127.0.0.1:6379/15 pytest tests/test_live_redis.py -v

The tests use that URL themselves to create an ACL user with exactly the
least-privilege rules SECURITY.md gives, to read keys and to clean up.  The
servers connect as that user, so every flow here also proves those rules are
enough.  Each test starts worker processes with
``python -m uvicorn live_redis_app:app`` (tests/live_redis_app.py) in a
namespace of its own, and removes its keys afterwards.
"""

from __future__ import annotations

import functools
import json
import os
import secrets
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
import pytest
from conftest import headers_for, modern, notification, rpc

REDIS_URL = os.environ.get("EASY_MCP_LIVE_REDIS_URL")

pytestmark = pytest.mark.skipif(not REDIS_URL, reason="EASY_MCP_LIVE_REDIS_URL is not set")

redis = pytest.importorskip("redis")

from easy_mcp.exceptions import (  # noqa: E402
    FORBIDDEN,
    RATE_LIMITED,
    SERVER_BUSY,
    SESSION_LIMIT_EXCEEDED,
    TOO_MANY_SESSIONS,
)
from easy_mcp.store.base import session_ref  # noqa: E402

TESTS = Path(__file__).resolve().parent
# The workers' API keys: KEY_A may call every tool, KEY_B only those scoped "b".
KEY_A = "live-redis-key-a-" + "a" * 16
KEY_B = "live-redis-key-b-" + "b" * 16
ACCEPT = {"Accept": "application/json, text/event-stream"}
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}
ACL_USER = "easy-mcp-test"

# SECURITY.md's least-privilege rules for the store's user, word for word.
ACL_RULES = (
    "resetkeys ~easy-mcp:1:* resetchannels &easy-mcp:1:* nocommands "
    "+ping +select +evalsha +script|load +publish +subscribe +unsubscribe +client|setinfo "
    "+exists +hget +hgetall +hset +hincrby +pexpire +del +zadd +zrem +zrange +zrangebyscore "
    "+zremrangebyscore +zcount +zcard +time"
)


def admin() -> Any:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


@pytest.fixture(scope="module")
def user_url() -> Iterator[str]:
    """A URL for a fresh ACL user with exactly the documented rules."""
    password = secrets.token_hex(16)
    connection = admin()
    connection.execute_command("ACL", "SETUSER", ACL_USER, "reset", "on", f">{password}")
    connection.execute_command("ACL", "SETUSER", ACL_USER, *ACL_RULES.split())
    assert REDIS_URL is not None
    parts = urlsplit(REDIS_URL)
    host = parts.hostname or "127.0.0.1"
    netloc = f"{quote(ACL_USER)}:{password}@{host}"
    if parts.port is not None:
        netloc += f":{parts.port}"
    try:
        yield urlunsplit((parts.scheme, netloc, parts.path, "", ""))
    finally:
        connection.execute_command("ACL", "DELUSER", ACL_USER)
        connection.close()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class Process:
    """One worker process serving tests/live_redis_app.py."""

    name: str
    port: int
    env: dict[str, str]
    log: Path
    proc: subprocess.Popen[bytes] | None = None

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self, *, healthy: bool = True) -> None:
        with self.log.open("ab") as log:
            self.proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "live_redis_app:app",
                    "--app-dir",
                    str(TESTS),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(self.port),
                    "--log-level",
                    "warning",
                ],
                env=self.env,
                stdout=subprocess.DEVNULL,
                stderr=log,
            )
        deadline = time.monotonic() + 20
        while True:
            assert self.proc.poll() is None, f"{self.name} exited:\n{self.output()}"
            try:
                status = httpx.get(f"{self.base}/healthz", timeout=2).status_code
                if not healthy or status == 200:
                    return
            except httpx.TransportError:
                pass
            assert time.monotonic() < deadline, f"{self.name} did not start:\n{self.output()}"
            time.sleep(0.1)

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(10)
        self.proc = None

    def output(self) -> str:
        return self.log.read_text(encoding="utf-8", errors="replace")

    def events(self, kind: str) -> list[dict[str, Any]]:
        found = []
        for line in self.output().splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            event = record.get("event") if isinstance(record, dict) else None
            if isinstance(event, dict) and event.get("type") == kind:
                found.append(event)
        return found


@dataclass
class Fleet:
    namespace: str
    markers: Path
    workers: list[Process]

    def marker(self, name: str, timeout: float = 5.0) -> str | None:
        path = self.markers / name
        deadline = time.monotonic() + timeout
        while not path.exists():
            if time.monotonic() > deadline:
                return None
            time.sleep(0.02)
        return path.read_text(encoding="utf-8")


@pytest.fixture
def fleet(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Callable[..., Fleet]]:
    """Start worker processes in a namespace of their own; *url* overrides the ACL user's."""
    started: list[Fleet] = []
    scratch: list[str] = []  # namespaces written to the scratch database

    def start(count: int = 2, *, url: str | None = None, **settings: Any) -> Fleet:
        if url is None:
            url = request.getfixturevalue("user_url")
            healthy = True
        else:
            healthy = False  # a store out of reach: /healthz answers 503
        namespace = f"t-{secrets.token_hex(4)}"
        markers = tmp_path / namespace
        markers.mkdir()
        workers = []
        for index in range(count):
            name = "AB"[index] if index < 2 else f"W{index}"
            env = {
                **os.environ,
                "EASY_MCP_REDIS_URL": url,
                "LIVE_NAMESPACE": namespace,
                "LIVE_WORKER": name,
                "LIVE_MARKER_DIR": str(markers),
                "LIVE_KEY_A": KEY_A,
                "LIVE_KEY_B": KEY_B,
                **{f"LIVE_{key.upper()}": str(value) for key, value in settings.items()},
            }
            worker = Process(name, free_port(), env, tmp_path / f"{namespace}-{name}.log")
            worker.start(healthy=healthy)
            workers.append(worker)
        fleet = Fleet(namespace, markers, workers)
        started.append(fleet)
        if healthy:
            scratch.append(namespace)
        return fleet

    yield start
    for fleet in started:
        for worker in fleet.workers:
            worker.stop()
    if not scratch:
        return
    connection = admin()
    try:
        for namespace in scratch:
            keys = list(connection.scan_iter(match=f"easy-mcp:1:{{{namespace}}}:*"))
            if keys:
                connection.delete(*keys)
    finally:
        connection.close()


def post(
    worker: Process,
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


def stateless(worker: Process, message: dict[str, Any], key: str = KEY_A) -> httpx.Response:
    headers = {**headers_for(message), "Authorization": f"Bearer {key}"}
    return httpx.post(f"{worker.base}/mcp", json=message, headers=headers, timeout=10)


def open_session(worker: Process, key: str | None = KEY_A) -> str:
    response = post(worker, rpc("initialize", INIT, "init"), key=key)
    assert response.status_code == 200, response.text
    return response.headers["mcp-session-id"]


def call(name: str, msg_id: Any = 1, **arguments: Any) -> dict[str, Any]:
    return rpc("tools/call", {"name": name, "arguments": arguments}, msg_id)


def text(response: httpx.Response) -> str:
    assert "result" in response.json(), response.text
    return str(response.json()["result"]["content"][0]["text"])


def delete(worker: Process, session: str, key: str = KEY_A) -> httpx.Response:
    headers = {"MCP-Session-Id": session, "Authorization": f"Bearer {key}"}
    return httpx.delete(f"{worker.base}/mcp", headers=headers, timeout=10)


class Background:
    def __init__(self, send: Callable[[], httpx.Response]) -> None:
        self.response: httpx.Response | None = None
        self.error: BaseException | None = None
        self.done_at = 0.0

        def run() -> None:
            try:
                self.response = send()
            except BaseException as exc:
                self.error = exc
            self.done_at = time.monotonic()

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
        time.sleep(0.05)
    return True


class Stream:
    """A legacy SSE stream read on a thread of its own."""

    def __init__(self, worker: Process, key: str = KEY_A) -> None:
        self.client = httpx.Client(base_url=worker.base, timeout=30)
        self.context = self.client.stream("GET", "/sse", headers={"Authorization": f"Bearer {key}"})
        response = self.context.__enter__()
        assert response.status_code == 200
        self.events: list[str] = []
        self.ended = threading.Event()
        lines = response.iter_lines()
        self.endpoint = self._data(lines)
        self.thread = threading.Thread(target=self._read, args=(lines,), daemon=True)
        self.thread.start()

    @staticmethod
    def _data(lines: Iterator[str]) -> str:
        for line in lines:
            if line.startswith("data: "):
                return line[len("data: ") :]
        raise AssertionError("the stream ended")

    def _read(self, lines: Iterator[str]) -> None:
        try:
            for line in lines:
                if line.startswith("data: "):
                    self.events.append(line[len("data: ") :])
        except Exception:
            pass  # closed under the reader by close(), or by the server
        finally:
            self.ended.set()

    @property
    def session_id(self) -> str:
        return self.endpoint.split("session_id=", 1)[1]

    def answer(self, msg_id: Any, timeout: float = 10.0) -> dict[str, Any]:
        found: dict[str, Any] = {}

        def arrived() -> bool:
            for event in self.events:
                message = json.loads(event)
                if message.get("id") == msg_id:
                    found.update(message)
                    return True
            return False

        assert wait_until(arrived, timeout), f"no answer {msg_id!r} on the stream"
        return found

    def post(self, worker: Process, message: dict[str, Any]) -> httpx.Response:
        return httpx.post(
            f"{worker.base}{self.endpoint}",
            json=message,
            headers={"Authorization": f"Bearer {KEY_A}"},
            timeout=10,
        )

    def close(self) -> None:
        self.context.__exit__(None, None, None)
        self.client.close()


@pytest.fixture
def streams() -> Iterator[Callable[[Process], Stream]]:
    opened: list[Stream] = []

    def open_stream(worker: Process) -> Stream:
        stream = Stream(worker)
        opened.append(stream)
        return stream

    yield open_stream
    for stream in opened:
        try:
            stream.close()
        except Exception:
            pass


# --------------------------------------------------------------- sessions


def test_live_a_session_is_served_by_both_workers(fleet: Callable[..., Fleet]) -> None:
    a, b = fleet().workers
    session = open_session(a)
    seen = [
        text(post(worker, call("whoami", n), session=session))
        for n, worker in enumerate((a, b, a, b))
    ]
    assert seen == ["A", "B", "A", "B"]
    assert text(post(b, call("add", 9, a=2, b=3), session=session)) == "5"
    assert delete(b, session).status_code == 204
    assert post(a, rpc("ping", msg_id=10), session=session).status_code == 404
    assert post(b, rpc("ping", msg_id=11), session=session).status_code == 404


def test_live_credential_binding_across_processes(fleet: Callable[..., Fleet]) -> None:
    a, b = fleet().workers
    session = open_session(a, key=KEY_A)
    for key in (KEY_B, None):
        refused = post(b, rpc("ping", msg_id=2), session=session, key=key)
        assert refused.status_code == 403
        assert refused.json()["error"]["code"] == FORBIDDEN
    assert post(b, rpc("ping", msg_id=3), session=session, key=KEY_A).status_code == 200
    assert b.events("session_credential_mismatch")


def test_live_call_caps_are_global(fleet: Callable[..., Fleet]) -> None:
    a, b = fleet().workers
    session = open_session(a)
    assert text(post(a, call("scarce", 2), session=session)) == "spent"
    assert text(post(b, call("scarce", 3), session=session)) == "spent"
    refused = post(a, call("scarce", 4), session=session)
    assert refused.json()["error"]["code"] == SESSION_LIMIT_EXCEEDED
    capped = modern("tools/call", {"name": "scarce"})
    assert "result" in stateless(a, capped).json()
    assert "result" in stateless(b, capped).json()
    assert stateless(a, capped).json()["error"]["code"] == SESSION_LIMIT_EXCEEDED


def test_live_rate_limit_is_global(fleet: Callable[..., Fleet]) -> None:
    a, b = fleet(rate_limit=5).workers
    listed = modern("tools/list")
    for worker in (a, a, a, b, b):
        assert stateless(worker, listed).status_code == 200
    for worker in (a, b):
        refused = stateless(worker, listed)
        error = refused.json()["error"]
        assert error["code"] == RATE_LIMITED
        assert 0 < error["data"]["retry_after_seconds"] <= 60


def test_live_cancel_on_one_process_stops_the_call_on_the_other(
    fleet: Callable[..., Fleet],
) -> None:
    group = fleet()
    a, b = group.workers
    session = open_session(a)
    for tool, tag, msg_id, reason in (
        ("slow", "async", 7, "cancelled"),
        ("slow_sync", "sync", 8, "cancelled"),
    ):
        message = call(tool, msg_id, tag=tag)
        running = Background(functools.partial(post, a, message, session=session, timeout=30))
        assert group.marker(f"{tag}.started") == "A"
        cancel = notification("notifications/cancelled", {"requestId": msg_id})
        sent = time.monotonic()
        assert post(b, cancel, session=session).status_code == 202
        answered = running.result()
        assert answered.status_code == 202
        assert running.done_at - sent < 1.0
        assert group.marker(f"{tag}.{reason}") == "A"
    assert len(a.events("tool_cancelled")) == 2


def test_live_delete_cancels_calls_on_the_other_process(fleet: Callable[..., Fleet]) -> None:
    group = fleet()
    a, b = group.workers
    session = open_session(a)
    running = Background(lambda: post(a, call("slow", 2, tag="del"), session=session, timeout=30))
    assert group.marker("del.started") == "A"
    assert delete(b, session).status_code == 204
    assert running.result().status_code == 202
    assert group.marker("del.cancelled") == "A"


# -------------------------------------------------------------- legacy SSE


def test_live_sse_answers_cross_processes(
    fleet: Callable[..., Fleet], streams: Callable[[Process], Stream]
) -> None:
    group = fleet()
    a, b = group.workers
    stream = streams(a)
    assert stream.post(b, call("add", 1, a=20, b=22)).status_code == 202
    assert stream.answer(1)["result"]["content"][0]["text"] == "42"
    assert stream.post(b, call("whoami", 2)).status_code == 202
    assert stream.answer(2)["result"]["content"][0]["text"] == "B"
    # A cancel through either worker stops a call running on the other.
    assert stream.post(a, call("slow", 3, tag="on-a")).status_code == 202
    assert stream.post(b, call("slow", 4, tag="on-b")).status_code == 202
    assert group.marker("on-a.started") == "A" and group.marker("on-b.started") == "B"
    assert (
        stream.post(b, notification("notifications/cancelled", {"requestId": 3})).status_code == 202
    )
    assert (
        stream.post(a, notification("notifications/cancelled", {"requestId": 4})).status_code == 202
    )
    assert group.marker("on-a.cancelled") == "A" and group.marker("on-b.cancelled") == "B"


def test_live_sse_close_ends_the_session_everywhere(
    fleet: Callable[..., Fleet], streams: Callable[[Process], Stream]
) -> None:
    a, b = fleet().workers
    stream = streams(a)
    assert stream.post(b, rpc("ping", msg_id=1)).status_code == 202
    stream.answer(1)
    stream.close()
    closed_at = time.monotonic()
    assert wait_until(lambda: stream.post(b, rpc("ping", msg_id=2)).status_code == 404, 2.0)
    assert time.monotonic() - closed_at < 1.5


# --------------------------------------------------------------- lifecycle


def test_live_sessions_survive_a_worker_restart(fleet: Callable[..., Fleet]) -> None:
    a, b = fleet().workers
    session = open_session(a)
    a.stop()
    assert text(post(b, call("whoami", 2), session=session)) == "B"
    a.start()
    assert text(post(a, call("whoami", 3), session=session)) == "A"


def test_live_idle_expiry_and_global_cap(fleet: Callable[..., Fleet]) -> None:
    a, b = fleet(idle=1, max_sessions=2).workers
    first, second = open_session(a), open_session(a)
    refused = post(b, rpc("initialize", INIT, "init"))
    assert refused.status_code == 503
    assert refused.json()["error"]["code"] == TOO_MANY_SESSIONS
    time.sleep(1.5)
    open_session(b)
    for session in (first, second):
        assert post(a, rpc("ping", msg_id=2), session=session).status_code == 404
    pruned = {event["session_ref"] for event in b.events("session_close")}
    assert pruned == {session_ref(first), session_ref(second)}


def test_live_store_holds_no_secrets_and_every_key_expires(
    fleet: Callable[..., Fleet], streams: Callable[[Process], Stream]
) -> None:
    group = fleet(rate_limit=100)
    a, b = group.workers
    session = open_session(a)
    post(b, call("scarce", 2), session=session)
    stateless(a, modern("tools/call", {"name": "scarce"}))
    stream = streams(a)
    stream.post(b, call("add", 3, a=1, b=1))
    stream.answer(3)
    connection = admin()
    try:
        keys = sorted(connection.scan_iter(match=f"easy-mcp:1:{{{group.namespace}}}:*"))
        kinds = {key.split(":")[3] for key in keys}
        assert {"s", "i", "c", "r"} <= kinds, keys
        dump = []
        for key in keys:
            kind = connection.type(key)
            if kind == "hash":
                dump.append(json.dumps(connection.hgetall(key)))
            elif kind == "zset":
                dump.append(json.dumps(connection.zrange(key, 0, -1, withscores=True)))
            ttl = connection.pttl(key)
            if key.split(":")[3] == "i":
                assert ttl == -1, key  # the two indexes, bounded by the session cap
            else:
                assert ttl > 0, key
        stored = "\n".join(keys + dump)
        for secret in (session, stream.session_id, KEY_A, KEY_B):
            assert secret not in stored
    finally:
        connection.close()


def test_live_forged_bus_messages_are_rejected(
    fleet: Callable[..., Fleet], streams: Callable[[Process], Stream]
) -> None:
    from easy_mcp.transport import _bus

    group = fleet()
    a, b = group.workers
    session = open_session(a)
    running = Background(
        lambda: post(a, call("slow", 5, tag="forged"), session=session, timeout=30)
    )
    assert group.marker("forged.started") == "A"
    stream = streams(a)
    prefix = f"easy-mcp:1:{{{group.namespace}}}"
    connection = admin()
    try:
        # Someone with access to Redis sees refs and owners, never the ids
        # that key the MACs: they seal with a guess.
        record = connection.hgetall(f"{prefix}:s:{session_ref(stream.session_id)}")
        guess = "not-the-session-id"
        fp = record["fp"]
        forged = [
            ("bus", _bus.seal("cancel", "http", session_ref(session), fp, guess, "f" * 16, rid=5)),
            ("bus", _bus.seal("end", "http", session_ref(session), fp, guess, "f" * 16)),
            (
                f"w:{record['own']}",
                _bus.seal(
                    "deliver",
                    "sse",
                    session_ref(stream.session_id),
                    fp,
                    guess,
                    "f" * 16,
                    msg={"jsonrpc": "2.0", "id": 99, "result": {"injected": True}},
                ),
            ),
        ]
        for channel, payload in forged:
            assert connection.publish(f"{prefix}:{channel}", payload) >= 1
    finally:
        connection.close()
    assert wait_until(lambda: len(a.events("bus_message_rejected")) >= 3)
    assert {event["reason"] for event in a.events("bus_message_rejected")} == {"mac"}
    assert group.marker("forged.cancelled", timeout=0.5) is None
    assert text(post(a, call("add", 6, a=1, b=1), session=session)) == "2"
    assert all(json.loads(event).get("id") != 99 for event in stream.events)
    assert (
        post(
            b, notification("notifications/cancelled", {"requestId": 5}), session=session
        ).status_code
        == 202
    )
    assert running.result().status_code == 202


def test_live_least_privilege_acl_is_enough(user_url: str) -> None:
    # Every test above ran as this user; it can do nothing else.
    connection = redis.Redis.from_url(user_url, decode_responses=True)
    try:
        assert connection.ping()
        for command in (("KEYS", "*"), ("CONFIG", "GET", "*"), ("FLUSHDB",)):
            with pytest.raises(redis.exceptions.NoPermissionError):
                connection.execute_command(*command)
        with pytest.raises(redis.exceptions.NoPermissionError):
            connection.execute_command("HSET", "elsewhere", "f", "v")
    finally:
        connection.close()


def test_live_unreachable_store_fails_closed(fleet: Callable[..., Fleet]) -> None:
    (lonely,) = fleet(1, url="redis://127.0.0.1:1/0").workers
    health = httpx.get(f"{lonely.base}/healthz", timeout=10)
    assert health.status_code == 503 and health.json()["store"] == "unreachable"
    refused = post(lonely, rpc("initialize", INIT, "init"))
    assert refused.status_code == 503 and refused.headers["retry-after"] == "1"
    assert refused.json()["error"]["code"] == SERVER_BUSY
    assert refused.json()["error"]["data"] == {"reason": "store_unavailable"}
