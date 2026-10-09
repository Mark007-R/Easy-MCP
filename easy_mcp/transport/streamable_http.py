"""Streamable HTTP transport, the MCP spec's current HTTP transport.

One endpoint (``/mcp`` by default) carries the whole protocol:

* ``POST`` delivers exactly one JSON-RPC message.  A request is answered in
  the HTTP response body as ``application/json``; notifications and client
  responses get ``202 Accepted``.  The one exception is
  ``subscriptions/listen``, whose answer is a ``text/event-stream``.
* ``GET`` with a session's ``MCP-Session-Id`` opens that session's stream
  (``text/event-stream``), which carries ``notifications/tools/list_changed``
  and nothing else.  One per session: a new one replaces the old.  A change
  made while none is open is announced when one opens.  Without a session,
  or for the stateless revision, ``GET`` answers ``405``.

Two protocol eras share the endpoint, chosen per request:

* **Stateless** (``2026-07-28``): the request names its protocol version in
  the ``MCP-Protocol-Version`` header and in ``params._meta``.  There is no
  session and no handshake.  The ``MCP-Protocol-Version``, ``Mcp-Method`` and
  (for ``tools/call``) ``Mcp-Name`` headers must match the body, since a
  proxy may route on the headers while this server executes the body; a
  mismatch is ``400`` with ``-32020``.  Closing the connection cancels the
  request.  A ``subscriptions/listen`` stream starts with its
  acknowledgment and ends when its client closes it, or with its result
  when the server ends it (shutdown, the token's expiry); a refused one is
  answered as JSON (``503`` for ``-32007``).
* **Session** (``2025-11-25`` and earlier): ``initialize`` opens a session
  whose ``MCP-Session-Id`` response header the client echoes on every later
  request, and ``DELETE`` with that header ends it.

By default the legacy HTTP+SSE endpoints (``/sse`` + ``/messages``) are
served from the same app, the spec's recommended setup for older clients.

Security handled here (before anything reaches the dispatcher):

* Browser ``Origin`` headers must be on the allowlist (403 otherwise), which
  defeats DNS-rebinding attacks against servers on loopback.
* API keys are resolved from ``Authorization: Bearer`` / ``X-API-Key`` on
  every request, and a session only answers the credential it was opened
  with (403 otherwise), so a leaked session id alone is useless.
* With ``oauth=``, every request needs a credential and every access token
  is verified on every request, sessions included: after the ``Accept``,
  ``Content-Type``, size and JSON checks, and before the MCP header-mirror
  checks, the session lookup or dispatch, so an unauthenticated caller
  learns nothing about headers, sessions or methods.
  A session is bound to the token's principal rather than the token, so a
  refreshed or broader token keeps it, and each request runs with the
  identity its own token grants.  A tool call the token lacks a scope for is
  answered ``403`` with an ``insufficient_scope`` challenge.
* Session ids are 192-bit random tokens; idle sessions expire and live ones
  are capped at ``max_sessions``.  Sessions are kept in the server's store
  (:mod:`._sessions`): with a shared one, any worker serves any session.
* ``Content-Type`` must be ``application/json`` (415), bodies are size-capped
  while streaming (413), and an unsupported ``MCP-Protocol-Version`` header
  is rejected (400).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import json
import math
import secrets
import time
import uuid
from typing import TYPE_CHECKING, Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from ..exceptions import (
    FORBIDDEN,
    HEADER_MISMATCH,
    INTERNAL_ERROR,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    MISSING_REQUIRED_CLIENT_CAPABILITY,
    PARSE_ERROR,
    PAYLOAD_TOO_LARGE,
    RATE_LIMITED,
    SERVER_BUSY,
    TOO_MANY_SESSIONS,
    UNSUPPORTED_PROTOCOL_VERSION,
    ProtocolError,
    RateLimitError,
    StoreUnavailableError,
)
from ..logging import audit
from ..middleware import TransportInfo
from ..protocol import (
    LISTEN_METHOD,
    META_PROTOCOL_VERSION,
    MODERN_PROTOCOL_VERSIONS,
    SUPPORTED_PROTOCOL_VERSIONS,
    check_request_meta,
    is_modern_request,
)
from ..security.auth import ClientIdentity
from ._http import (
    STORE_RETRY_SECONDS,
    BaseHTTPTransport,
    is_store_unavailable,
    rpc_error,
    store_unavailable,
)
from ._outbox import EventStreamResponse, Outbox, accepts_event_stream, stream_events
from ._sessions import ClientHandle, LocalSession, Rejection, SessionManager
from .base import ClientContext
from .sse import SSETransport

if TYPE_CHECKING:
    from ..server import MCPServer
    from ..subscriptions import _Sink

SESSION_HEADER = "MCP-Session-Id"
PROTOCOL_VERSION_HEADER = "MCP-Protocol-Version"
METHOD_HEADER = "Mcp-Method"
NAME_HEADER = "Mcp-Name"

# Methods whose Mcp-Name header mirrors a body field, and that field.
_NAME_FIELDS = {"tools/call": "name", "resources/read": "uri", "prompts/get": "name"}

# The HTTP status the stateless revision gives a stateless error, by code
# (middleware may raise any of these); every other error is answered 200.
_STATELESS_ERROR_STATUS: dict[object, int] = {
    METHOD_NOT_FOUND: 404,
    HEADER_MISMATCH: 400,
    MISSING_REQUIRED_CLIENT_CAPABILITY: 400,
    UNSUPPORTED_PROTOCOL_VERSION: 400,
}

# A refused subscriptions/listen is answered like any stateless error, and
# the stream cap like the session cap.
_LISTEN_ERROR_STATUS: dict[object, int] = {**_STATELESS_ERROR_STATUS, TOO_MANY_SESSIONS: 503}

# How often a running stateless request checks whether its client hung up.
_DISCONNECT_POLL_SECONDS = 0.25

# How long shutdown lets the requests still running finish before it cancels
# them, as stdio does with its default shutdown_timeout.
_SHUTDOWN_GRACE_SECONDS = 5.0

_TRANSPORT = "streamable-http"


class _ShuttingDown(Exception):
    """Shutdown stopped a message being dispatched, or refused to start one."""


class _NotifyStream:
    """A session's ``GET /mcp`` stream, held by this worker.

    ``outbox`` feeds the stream, ``sink`` is what tells it about list
    changes, and ``reason`` says why it ended, once it has.
    """

    __slots__ = ("outbox", "reason", "sink", "timer")

    def __init__(self) -> None:
        self.outbox = Outbox()
        self.sink: _Sink | None = None
        self.reason: str | None = None
        self.timer: asyncio.TimerHandle | None = None

    def close(self, reason: str) -> None:
        """End the stream once what is queued has been written; the first reason stands."""
        if self.reason is None:
            self.reason = reason
        self.outbox.close()


def _media_type(value: str | None) -> str:
    return (value or "").split(";", 1)[0].strip().lower()


def _accepts_json(accept: str | None) -> bool:
    if not accept:
        return True  # no Accept header: any media type is acceptable
    ranges = {_media_type(part) for part in accept.split(",")}
    return bool(ranges & {"application/json", "application/*", "*/*"})


async def _read_body(request: Request, max_bytes: int) -> bytes | None:
    """The request body, or ``None`` once it exceeds *max_bytes*."""
    # Cheap header-based rejection first, then enforce the cap while
    # reading, since Content-Length can lie.
    content_length = request.headers.get("content-length")
    if content_length and content_length.isdigit() and int(content_length) > max_bytes:
        return None
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > max_bytes:
            return None
    return bytes(body)


def _json_response(
    payload: dict[str, Any], headers: dict[str, str] | None = None, status: int = 200
) -> Response:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    return Response(body, status_code=status, media_type="application/json", headers=headers)


def _answer(payload: dict[str, Any], status: int = 200) -> Response:
    """A dispatched response; ``503`` with ``Retry-After`` when the store was out of reach."""
    if is_store_unavailable(payload):
        headers = {"Retry-After": str(STORE_RETRY_SECONDS)}
        return _json_response(payload, headers=headers, status=503)
    return _json_response(payload, status=status)


def _rpc_error_body(msg_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": msg_id, "error": error}


def _shutting_down(msg_id: Any) -> Response:
    """The answer to a request shutdown stopped or refused: retry shortly.

    Not the bare ``202`` of a cancel: the client cancelled nothing, and a
    request must be answered with a JSON-RPC message.
    """
    body = _rpc_error_body(
        msg_id, SERVER_BUSY, "Server is shutting down; retry shortly", {"reason": "shutdown"}
    )
    return _json_response(body, headers={"Retry-After": "1"}, status=503)


def _decode_header_value(value: str) -> str | None:
    """A mirrored header's value, Base64 sentinel decoded; ``None`` if invalid."""
    if value.startswith("=?base64?") and value.endswith("?=") and len(value) >= 11:
        try:
            return base64.b64decode(value[9:-2], validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            return None
    return value


def _check_mirrored_headers(request: Request, message: dict[str, Any]) -> str | None:
    """Why the request's mirrored headers disagree with its body, if they do."""
    for name in (PROTOCOL_VERSION_HEADER, METHOD_HEADER, NAME_HEADER):
        # Components that read only the first (or the last) copy of a repeated
        # header would each see a different value.
        if len(request.headers.getlist(name)) > 1:
            return f"Header mismatch: {name} header appears more than once"
    params = message.get("params")
    params = params if isinstance(params, dict) else {}
    meta = params.get("_meta")
    body_version = meta.get(META_PROTOCOL_VERSION) if isinstance(meta, dict) else None
    header_version = request.headers.get(PROTOCOL_VERSION_HEADER)
    if header_version is None:
        return f"Header mismatch: missing {PROTOCOL_VERSION_HEADER} header"
    if isinstance(body_version, str) and header_version != body_version:
        return (
            f"Header mismatch: {PROTOCOL_VERSION_HEADER} header value {header_version!r} "
            f"does not match body value {body_version!r}"
        )
    method = message.get("method")
    header_method = request.headers.get(METHOD_HEADER)
    if header_method is None:
        return f"Header mismatch: missing {METHOD_HEADER} header"
    if header_method != method:
        return (
            f"Header mismatch: {METHOD_HEADER} header value {header_method!r} "
            f"does not match body value {method!r}"
        )
    field = _NAME_FIELDS.get(method) if isinstance(method, str) else None
    if field is not None:
        raw_name = request.headers.get(NAME_HEADER)
        if raw_name is None:
            return f"Header mismatch: missing {NAME_HEADER} header"
        header_name = _decode_header_value(raw_name)
        if header_name is None:
            return f"Header mismatch: {NAME_HEADER} header is not valid Base64 UTF-8"
        if header_name != params.get(field):
            return (
                f"Header mismatch: {NAME_HEADER} header value {header_name!r} "
                f"does not match body value {params.get(field)!r}"
            )
    return None


class StreamableHTTPTransport(BaseHTTPTransport):
    """Serve MCP over Streamable HTTP, plus the legacy SSE endpoints by default.

    Args:
        server: The :class:`~easy_mcp.server.MCPServer` to expose.
        path: The MCP endpoint path.
        legacy_sse: Also serve the deprecated HTTP+SSE endpoints (``/sse`` and
            ``/messages``) so clients that predate Streamable HTTP still connect.
        session_idle_timeout: Seconds without a request after which a session
            expires (its id then gets 404 and the client re-initializes);
            ``None`` keeps sessions until the client deletes them, which a
            shared store refuses.  Stateless clients' per-client call counts
            lapse after the same idle time.

    Raises:
        ValueError: An invalid *path* or *session_idle_timeout*, or
            ``session_idle_timeout=None`` with a shared store: sessions in a
            shared store must expire.
    """

    def __init__(
        self,
        server: MCPServer,
        *,
        path: str = "/mcp",
        legacy_sse: bool = True,
        session_idle_timeout: float | None = 3600.0,
    ) -> None:
        super().__init__(server)
        if not path.startswith("/"):
            raise ValueError("path must start with '/'")
        reserved = set(self._reserved_paths())
        if legacy_sse:
            reserved |= {"/sse", "/messages"}
        if path in reserved:
            raise ValueError(f"path {path!r} collides with another endpoint")
        if path == "/.well-known" or path.startswith("/.well-known/"):
            raise ValueError(f"path {path!r} is under /.well-known/, which serves metadata")
        if session_idle_timeout is not None and session_idle_timeout <= 0:
            raise ValueError("session_idle_timeout must be positive or None")
        if session_idle_timeout is None and server.store.shared:
            raise ValueError(
                "session_idle_timeout=None needs the in-memory store: sessions in a shared "
                "store must expire"
            )
        self._path = path
        self._idle_timeout = session_idle_timeout
        self._legacy = SSETransport(server) if legacy_sse else None
        self._manager = SessionManager(
            server, "http", ttl=session_idle_timeout, transport=_TRANSPORT
        )
        # Every message being dispatched, so shutdown can cancel it.
        self._dispatches: set[asyncio.Task[Any]] = set()
        # The subscriptions/listen streams open here: their context, and the
        # task dispatching their request.
        self._listens: dict[ClientContext, asyncio.Task[Any]] = {}
        # What a closed GET stream leaves to do: record its lists, release its session.
        self._stream_ends: set[asyncio.Task[None]] = set()
        self._closing = False

    _audit_transport = _TRANSPORT

    @property
    def _sessions(self) -> dict[str, LocalSession]:
        """The sessions this worker holds state for, by id (all of them with MemoryStore)."""
        return {local.session_id: local for local in self._manager.local_sessions()}

    def describe(self) -> str:
        legacy = " (+ legacy sse)" if self._legacy is not None else ""
        return f"streamable-http on {self._server.host}:{self._server.port}{self._path}{legacy}"

    def _endpoint_paths(self) -> tuple[str, ...]:
        return (self._path,)

    def _reopen(self) -> None:
        self._closing = False
        if self._legacy is not None:
            self._legacy._reopen()

    # ------------------------------------------------------------------ app

    async def close_streams(self) -> None:
        """End the SSE streams, the sessions and the messages still being served.

        uvicorn waits for every open connection before the lifespan shutdown
        runs, and a request held in middleware (which no tool timeout
        bounds) or in a slow tool keeps its connection open.  So nothing new
        is served from now on (the legacy endpoints included, or a stream
        opened meanwhile would hold shutdown up for good; a
        ``notifications/cancelled`` is still acted on), and the requests
        still running get ``_SHUTDOWN_GRACE_SECONDS`` to finish, as on stdio,
        or less if uvicorn is told to quit at once (a second Ctrl-C); then
        they are cancelled.  A request stopped or refused this way is
        answered ``503`` with ``-32008`` (retry shortly), and a handshake cut
        short opens no session.  With a shared store the sessions live on:
        other workers serve them, and the legacy SSE messages relayed here
        for a stream another worker holds get the same grace, then the same
        answer on that stream.

        First of all, each ``subscriptions/listen`` stream gets its
        completion result and ends, and each session's ``GET`` stream ends.
        """
        self._closing = True
        for context in list(self._listens):
            self._server.close_subscriptions(context, reason="shutdown")
        for local in self._manager.local_sessions():
            if local.notify_stream is not None:
                local.notify_stream.close("shutdown")
        relays: set[asyncio.Task[Any]] = set()
        if self._legacy is not None:
            await self._legacy.close_all_sessions()
            relays = self._legacy._relays()
        running = await self._grace(self._dispatches | relays, _SHUTDOWN_GRACE_SECONDS)
        for task in running - relays:
            task.cancel()
        if self._legacy is not None:
            await self._legacy._stop_relays(running & relays)
        # Only now: a session's end cancels its requests as a client's cancel
        # would, and they would go unanswered.
        await self._manager.shutdown()
        if self._legacy is not None:
            await self._legacy._manager.shutdown()

    def build_app(self) -> Starlette:
        """Build the ASGI application (also usable for tests or mounting)."""
        routes = [
            Route(self._path, self._handle, methods=["GET", "POST", "DELETE"]),
            Route("/healthz", self._handle_health, methods=["GET"]),
            *self._metadata_routes(),
        ]
        legacy = self._legacy
        if legacy is not None:
            routes.extend(legacy.routes())

        @contextlib.asynccontextmanager
        async def lifespan(app: Starlette) -> Any:
            # Serves anew (an app built again from this transport), warms up
            # OAuth, and at the end closes streams and waits for threads.
            async with self._server._lifespan(self):
                yield

        return Starlette(routes=routes, middleware=self._middleware(), lifespan=lifespan)

    # ------------------------------------------------------------- endpoint

    async def _handle(self, request: Request) -> Response:
        if request.method == "POST":
            return await self._handle_post(request)
        if request.method == "DELETE":
            return await self._handle_delete(request)
        if request.method == "GET":
            return await self._handle_get(request)
        # HEAD, which Starlette routes wherever GET goes: a stream opened for
        # it would replace the session's own, and its body is never sent.
        return rpc_error(
            405,
            INVALID_REQUEST,
            f"Method Not Allowed: {request.method}; GET opens a session's stream, "
            "POST sends JSON-RPC messages",
            headers={"Allow": "GET, POST, DELETE"},
        )

    async def _handle_get(self, request: Request) -> Response:
        """Open a session's notification stream.

        In order: ``405`` without a session id or for the stateless revision
        (which has no GET stream), ``406`` unless the client accepts
        ``text/event-stream``, the credential (``401``), the session
        (``404``, ``403`` for another credential, ``400`` for an unsupported
        version), and one unit of the session's rate-limit budget (``429``).
        """
        if request.headers.get(SESSION_HEADER) is None or (
            request.headers.get(PROTOCOL_VERSION_HEADER) in MODERN_PROTOCOL_VERSIONS
        ):
            return rpc_error(
                405,
                INVALID_REQUEST,
                f"Method Not Allowed: GET opens a session's stream and needs its "
                f"{SESSION_HEADER}; POST JSON-RPC messages",
                headers={"Allow": "GET, POST, DELETE"},
            )
        if not accepts_event_stream(request.headers.get("accept")):
            return rpc_error(
                406, INVALID_REQUEST, "Not Acceptable: the client must accept text/event-stream"
            )
        resolved = await self._resolve_identity(request, modern=False)
        if isinstance(resolved, Response):
            return resolved
        identity = resolved
        session = await self._session_for(request, identity)
        if isinstance(session, Response):
            return session
        opened = False
        try:
            client_id = session.context.client_id
            try:
                # Opening a stream costs one request, as GET /sse does.
                await self._server.acheck_rate_limit(client_id)
            except RateLimitError as exc:
                audit("rate_limited", client_id=client_id, method="GET " + self._path)
                return rpc_error(
                    429,
                    RATE_LIMITED,
                    str(exc),
                    headers={"Retry-After": str(max(1, math.ceil(exc.retry_after_seconds)))},
                    data=exc.data,
                )
            except StoreUnavailableError:
                return store_unavailable()
            if self._closing:
                # A stream opened now would hold shutdown up.
                return rpc_error(
                    503,
                    SERVER_BUSY,
                    "Server is shutting down; retry shortly",
                    headers={"Retry-After": "1"},
                    data={"reason": "shutdown"},
                )
            if session.ended:
                return rpc_error(
                    404, INVALID_REQUEST, "Session not found; send a new initialize request"
                )
            response = self._open_stream(session, identity)
            opened = True
            return response
        finally:
            if not opened:
                await self._manager.finish(session)

    def _open_stream(self, session: LocalSession, identity: ClientIdentity | None) -> Response:
        """The session's stream, replacing any other it has here; holds the session while open.

        It starts with a ``list_changed`` for every list that changed since
        the client was last told (at ``initialize``, or by its last stream).
        """
        stream = _NotifyStream()
        baselines: dict[str, str] | None = None
        replaced = session.notify_stream
        if replaced is not None:
            # Newest wins, so a message never goes to two streams; what the
            # old one had yet to write goes to the new one.
            if replaced.sink is not None and not replaced.sink.closed:
                baselines = dict(replaced.sink.baselines)
            replaced.outbox.drain_into(stream.outbox)
            replaced.close("replaced")
        if baselines is None and session.record.baselines is not None:
            baselines = dict(session.record.baselines)
        session.notify_stream = stream
        client_id = session.context.client_id
        sink = self._server._watch_session(
            session.session_id,
            push=stream.outbox.put,
            identity=identity,
            client_id=client_id,
            baselines=baselines,
        )
        stream.sink = sink
        fields = {
            "session_id": session.session_id,
            "session_ref": session.ref,
            "client_id": client_id,
            "transport": _TRANSPORT,
        }
        audit("stream_open", **fields)
        deadline = self._server._stream_deadline(identity)
        if deadline is not None:
            # A token's stream ends with the token.
            stream.timer = asyncio.get_running_loop().call_later(
                max(0.0, deadline - time.time()), stream.close, "token_expired"
            )

        def on_close() -> None:
            if stream.timer is not None:
                stream.timer.cancel()
            stream.close("client_closed")
            if session.notify_stream is stream:
                session.notify_stream = None
            self._server._notifier.end_session(session.session_id, sink)
            audit("stream_close", **fields, reason=stream.reason)
            # The lists it last told its client about are where the next
            # stream starts from, on whichever worker it opens.
            keep = None if stream.reason == "replaced" else dict(sink.baselines)
            task = asyncio.ensure_future(self._stream_closed(session, keep))
            self._stream_ends.add(task)
            task.add_done_callback(self._stream_ends.discard)

        return EventStreamResponse(stream_events(stream.outbox), on_close=on_close)

    async def _stream_closed(self, session: LocalSession, baselines: dict[str, str] | None) -> None:
        try:
            if baselines is not None:
                await self._manager.save_baselines(session, baselines)
        finally:
            await self._manager.finish(session)

    async def _handle_post(self, request: Request) -> Response:
        if not _accepts_json(request.headers.get("accept")):
            return rpc_error(
                406, INVALID_REQUEST, "Not Acceptable: the client must accept application/json"
            )
        if _media_type(request.headers.get("content-type")) != "application/json":
            return rpc_error(
                415,
                INVALID_REQUEST,
                "Unsupported Media Type: Content-Type must be application/json",
            )
        max_bytes = self._server.max_request_bytes
        body = await _read_body(request, max_bytes)
        if body is None:
            return rpc_error(413, PAYLOAD_TOO_LARGE, f"request exceeds {max_bytes} bytes")
        try:
            message = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return rpc_error(400, PARSE_ERROR, "Parse error: invalid JSON")
        if not isinstance(message, dict):
            return rpc_error(
                400,
                INVALID_REQUEST,
                "Invalid request: the body must be one JSON-RPC message object "
                "(batches are not supported)",
            )
        params = message.get("params")
        modern = request.headers.get(PROTOCOL_VERSION_HEADER) in MODERN_PROTOCOL_VERSIONS or (
            isinstance(params, dict) and is_modern_request(message.get("method"), params)
        )
        # The tool a tools/call names: a token refused for required_scopes is
        # asked for its scope in the same challenge, rather than in a second.
        name = params.get("name") if isinstance(params, dict) else None
        tool = name if message.get("method") == "tools/call" and isinstance(name, str) else None
        # Before the MCP header checks, the handshake and the session lookup,
        # so an unauthenticated caller learns nothing about any of them.
        resolved = await self._resolve_identity(request, modern=modern, tool=tool)
        if isinstance(resolved, Response):
            return resolved
        identity = resolved
        info = self._transport_info(request, _TRANSPORT)

        if modern:
            return await self._handle_stateless(message, identity, request, info)

        if message.get("method") == "initialize" and "id" in message:
            return await self._initialize(message, identity, request, info)

        session = await self._session_for(request, identity)
        if isinstance(session, Response):
            return session
        try:
            if "method" not in message and ("result" in message or "error" in message):
                # A reply to a server-to-client request.  This server never
                # sends those, so nothing is waiting for it.
                return Response(status_code=202)
            # This request's own identity: a refreshed or broader token takes
            # effect at once, in the same session.
            context = self._manager.context(session, identity)
            try:
                # A disconnect cancels nothing in this era; ending the session
                # (DELETE) and shutdown do.
                response = await self._dispatch_until_disconnect(
                    message, context, request, info, disconnect=False, owner=session
                )
            except _ShuttingDown:
                if "id" in message:
                    return _shutting_down(message["id"])
                return Response(status_code=202)  # a notification gets no answer either way
        finally:
            await self._manager.finish(session)
        if response is None:
            # A notification, or a request cancelled via notifications/cancelled:
            # MCP sends a reply to neither.
            return Response(status_code=202)
        challenge = self._step_up_headers(response, identity, modern=False)
        if challenge is not None:
            return _json_response(response, headers=challenge, status=403)
        return _answer(response)

    async def _handle_stateless(
        self,
        message: dict[str, Any],
        identity: ClientIdentity | None,
        request: Request,
        info: TransportInfo,
    ) -> Response:
        """Serve one request of the stateless era; no session is read or made."""
        msg_id = message.get("id")
        client_id = self._client_id(identity, request)
        method = message.get("method")
        if "id" not in message:
            if isinstance(method, str) and method.startswith("notifications/"):
                # This revision defines no client-to-server notifications over
                # HTTP: closing the connection is how a request is cancelled.
                # Acting on notifications/cancelled here would let anyone who
                # shares an address or key cancel someone else's call.
                return Response(status_code=202)
            # Anything else without an id would be a request that skips the
            # header checks below; refuse it rather than run it.
            return _json_response(
                _rpc_error_body(None, INVALID_REQUEST, "Invalid request: a request needs an id"),
                status=400,
            )
        if "method" in message:
            mismatch = _check_mirrored_headers(request, message)
            if mismatch is not None:
                audit("header_mismatch", client_id=client_id, transport=_TRANSPORT)
                return _json_response(
                    _rpc_error_body(msg_id, HEADER_MISMATCH, mismatch), status=400
                )
            params = message.get("params")
            try:
                check_request_meta(params if isinstance(params, dict) else {})
            except ProtocolError as exc:
                # The spec makes these 400s, and their JSON-RPC body is what
                # tells a probing client this is a modern server rather than
                # a legacy one rejecting the request.
                return _json_response(
                    _rpc_error_body(msg_id, exc.code, str(exc), exc.data), status=400
                )
        if method == LISTEN_METHOD:
            # Its answer is a stream; it makes no tool call, so it leaves the
            # client's call counts (and their idle clock) alone.
            return await self._open_listen(message, identity, request, info, client_id)

        # A context of its own, so nothing in flight is shared with other
        # requests; only the per-client call counts are, kept in the store.
        store = self._server.store
        try:
            await store.touch_client(client_id, ttl=self._idle_timeout)
        except StoreUnavailableError:
            return store_unavailable(msg_id)
        context = ClientContext(
            client_id=client_id,
            session_id="stateless",
            identity=identity,
            store_handle=ClientHandle(store, client_id, self._idle_timeout),
        )
        try:
            response = await self._dispatch_until_disconnect(message, context, request, info)
        except _ShuttingDown:
            return _shutting_down(msg_id)
        if response is None:
            return Response(status_code=202)  # the client hung up: nobody reads it
        challenge = self._step_up_headers(response, identity, modern=True)
        if challenge is not None:
            return _json_response(response, headers=challenge, status=403)
        error = response.get("error")
        code = error.get("code") if isinstance(error, dict) else None
        return _answer(response, status=_STATELESS_ERROR_STATUS.get(code, 200))

    async def _open_listen(
        self,
        message: dict[str, Any],
        identity: ClientIdentity | None,
        request: Request,
        info: TransportInfo,
        client_id: str,
    ) -> Response:
        """Serve ``subscriptions/listen``: its first frame decides the answer.

        The request is dispatched with a context whose channel is this
        response.  If the acknowledgment comes first, the answer is a
        ``text/event-stream`` starting with it; if ``dispatch`` finishes
        first, the listen was refused, and the error is answered as JSON.
        Closing the stream is the client's cancel.
        """
        msg_id = message.get("id")
        if not accepts_event_stream(request.headers.get("accept")):
            return _json_response(
                _rpc_error_body(
                    msg_id,
                    INVALID_REQUEST,
                    "Not Acceptable: subscriptions/listen answers with text/event-stream, "
                    "which the client must accept",
                ),
                status=406,
            )
        if self._closing:
            return _shutting_down(msg_id)
        outbox = Outbox()
        context = ClientContext(
            client_id=client_id,
            session_id="stateless",
            identity=identity,
            store_handle=ClientHandle(self._server.store, client_id, self._idle_timeout),
            push=outbox.put,
        )
        task = asyncio.ensure_future(self._server.dispatch(message, context, transport=info))
        # The stream ends once the request does and its last frame is written.
        task.add_done_callback(lambda _: outbox.close())
        self._dispatches.add(task)
        streaming = False
        try:
            waiter = asyncio.ensure_future(outbox.wait())
            try:
                while True:
                    done, _ = await asyncio.wait(
                        {task, waiter},
                        timeout=_DISCONNECT_POLL_SECONDS,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if done:
                        break
                    if await request.is_disconnected():
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                        audit(
                            "request_abandoned",
                            client_id=client_id,
                            request_id=msg_id,
                            transport=_TRANSPORT,
                        )
                        return Response(status_code=202)
            finally:
                waiter.cancel()
            if len(outbox):
                streaming = True
            elif task.cancelled():
                if self._closing:
                    return _shutting_down(msg_id)
                return Response(status_code=202)
        finally:
            self._dispatches.discard(task)
            if not streaming and not task.done():
                task.cancel()  # our own caller is going away
        if not streaming:
            response = task.result()
            if response is None:
                return Response(status_code=202)
            challenge = self._step_up_headers(response, identity, modern=True)
            if challenge is not None:
                return _json_response(response, headers=challenge, status=403)
            error = response.get("error")
            code = error.get("code") if isinstance(error, dict) else None
            return _answer(response, status=_LISTEN_ERROR_STATUS.get(code, 200))

        self._listens[context] = task
        if self._closing:
            # Shutdown began while it was acknowledged: it ends at once, gracefully.
            self._server.close_subscriptions(context, reason="shutdown")

        def on_close() -> None:
            self._listens.pop(context, None)
            outbox.close()
            if not task.done():
                task.cancel()  # the client closed the stream: that is its cancel

        return EventStreamResponse(stream_events(outbox), on_close=on_close)

    async def _dispatch_until_disconnect(
        self,
        message: dict[str, Any],
        context: ClientContext,
        request: Request,
        info: TransportInfo | None = None,
        *,
        disconnect: bool = True,
        owner: LocalSession | None = None,
    ) -> dict[str, Any] | None:
        """Dispatch, cancelling the work if the client closes the connection.

        In the stateless era a closed connection is the cancellation signal:
        nobody is left to read the answer.  The session era passes
        *disconnect* false: there a closed connection cancels nothing, and
        the end of the *owner* session does.  Returns ``None`` for work
        cancelled either way (no answer is sent, as for any cancel).

        Raises:
            _ShuttingDown: Shutdown cancelled the work, or had begun before it
                started (a ``notifications/cancelled`` is still acted on);
                the request must still be answered.
        """
        if self._closing:
            if message.get("method") == "notifications/cancelled" and "id" not in message:
                # Still acted on, without middleware (which could start new
                # work): the call it names stops now, and is not answered,
                # rather than running on until shutdown cancels it.
                params = message.get("params")
                cancel = params if isinstance(params, dict) else {}
                await self._server._handle_notification("notifications/cancelled", cancel, context)
                return None
            raise _ShuttingDown  # nothing new is served
        task = asyncio.ensure_future(self._server.dispatch(message, context, transport=info))
        self._dispatches.add(task)
        if owner is not None:
            # Registered before the dispatch's first step, so a DELETE handled
            # before the request is in flight still stops it.
            owner.dispatches.add(task)
            if owner.ended:
                task.cancel()  # it ended while this request was on its way in
        try:
            while True:
                timeout = _DISCONNECT_POLL_SECONDS if disconnect else None
                done, _ = await asyncio.wait({task}, timeout=timeout)
                if done:
                    if not task.cancelled():
                        return task.result()
                    if self._closing:
                        raise _ShuttingDown
                    return None  # the session ended
                if await request.is_disconnected():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                    audit(
                        "request_abandoned",
                        client_id=context.client_id,
                        request_id=message.get("id"),
                        transport=_TRANSPORT,
                    )
                    return None
        finally:
            self._dispatches.discard(task)
            if owner is not None:
                owner.dispatches.discard(task)
            # Our own caller may be cancelled too (shutdown, a timeout in a
            # wrapping app): the call must not outlive its request.
            if not task.done():
                task.cancel()

    @staticmethod
    def _client_id(identity: ClientIdentity | None, request: Request) -> str:
        if identity is not None:
            return identity.fingerprint
        return f"ip:{request.client.host if request.client else 'unknown'}"

    async def _handle_delete(self, request: Request) -> Response:
        # A session era request: it carries its own credential too.
        modern = request.headers.get(PROTOCOL_VERSION_HEADER) in MODERN_PROTOCOL_VERSIONS
        resolved = await self._resolve_identity(request, modern=modern)
        if isinstance(resolved, Response):
            return resolved
        session = await self._session_for(request, resolved)
        if isinstance(session, Response):
            return session
        try:
            # Ended in the store first: every worker answers 404 from now on.
            await self._manager.end(session, reason="client_terminated", strict=True)
        except StoreUnavailableError:
            return store_unavailable()
        finally:
            await self._manager.finish(session)
        return Response(status_code=204)

    # -------------------------------------------------------------- sessions

    async def _initialize(
        self,
        message: dict[str, Any],
        identity: ClientIdentity | None,
        request: Request,
        info: TransportInfo,
    ) -> Response:
        client_host = request.client.host if request.client else "unknown"
        client_id = identity.fingerprint if identity else f"ip:{client_host}"
        # The slot is held while the handshake runs (the store counts it), so
        # concurrent handshakes cannot overshoot max_sessions.
        session: LocalSession | None = None
        for attempt in range(2):
            # 192-bit random token: the session id is a bearer capability, it
            # must be unguessable.
            session_id = secrets.token_urlsafe(24)
            try:
                session = await self._manager.open(
                    session_id, client_id=client_id, identity=identity
                )
                break
            except StoreUnavailableError:
                return store_unavailable(message["id"])
            except ValueError:
                if attempt:  # the store found the id taken twice: something is wrong
                    error_id = uuid.uuid4().hex[:12]
                    self._server._logger.error(
                        "could not file a new session error_id=%s", error_id, exc_info=True
                    )
                    return rpc_error(
                        500, INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
                    )
        if session is None:
            return rpc_error(503, TOO_MANY_SESSIONS, "too many concurrent sessions")

        response: dict[str, Any] | None = None
        try:
            # A disconnect cancels nothing in this era, and nor can the
            # client cancel a handshake; shutdown does.
            response = await self._dispatch_until_disconnect(
                message, session.context, request, info, disconnect=False
            )
        except _ShuttingDown:
            return _shutting_down(message["id"])  # and no session, below
        finally:
            if response is None or "error" in response:
                # The handshake failed (e.g. rate limited): no session.
                await self._manager.end(session, reason=None)
                session.active -= 1
            else:
                try:
                    # What its client is about to see, which the session's GET
                    # stream, on whichever worker it opens, compares lists with.
                    baselines = self._server._list_baselines(session.context.identity)
                    await self._manager.save_baselines(session, baselines)
                finally:
                    # Whatever became of that, the handshake's hold ends, or
                    # the session could never expire.
                    await self._manager.finish(
                        session, protocol_version=session.context.protocol_version
                    )
        if response is None:
            return Response(status_code=202)
        if "error" in response:
            return _answer(response)
        self._manager.opened(session)
        return _json_response(response, headers={SESSION_HEADER: session.session_id})

    async def _session_for(
        self, request: Request, identity: ClientIdentity | None
    ) -> LocalSession | Response:
        """Resolve the request's session, held until ``finish``, or the response rejecting it."""
        resolved = await self._manager.resolve(
            request.headers.get(SESSION_HEADER),
            identity,
            version_header=request.headers.get(PROTOCOL_VERSION_HEADER),
        )
        if not isinstance(resolved, Rejection):
            return resolved
        if resolved is Rejection.MISSING_HEADER:
            return rpc_error(
                400,
                INVALID_REQUEST,
                f"Bad Request: missing {SESSION_HEADER} header; send initialize first",
            )
        if resolved is Rejection.NOT_FOUND:
            return rpc_error(
                404, INVALID_REQUEST, "Session not found; send a new initialize request"
            )
        if resolved is Rejection.FORBIDDEN:
            # The session must not be usable with a different (or missing)
            # credential than it was opened with.
            return rpc_error(403, FORBIDDEN, "Forbidden: credential does not match session")
        if resolved is Rejection.BAD_VERSION:
            return rpc_error(
                400,
                INVALID_REQUEST,
                f"Bad Request: unsupported {PROTOCOL_VERSION_HEADER} "
                f"(supported: {', '.join(SUPPORTED_PROTOCOL_VERSIONS)})",
            )
        return store_unavailable()
