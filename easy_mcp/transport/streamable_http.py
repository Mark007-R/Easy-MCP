"""Streamable HTTP transport, the MCP spec's current HTTP transport.

One endpoint (``/mcp`` by default) carries the whole protocol:

* ``POST`` delivers exactly one JSON-RPC message.  A request is answered in
  the HTTP response body as ``application/json``; notifications and client
  responses get ``202 Accepted``.
* ``initialize`` opens a session: its response carries an ``MCP-Session-Id``
  header that the client echoes on every later request.
* ``DELETE`` with that header ends the session.
* ``GET`` answers ``405``: every server message is the reply to a client
  request, so there is nothing to push on a standalone stream.

By default the legacy HTTP+SSE endpoints (``/sse`` + ``/messages``) are
served from the same app, the spec's recommended setup for older clients.

Security handled here (before anything reaches the dispatcher):

* Browser ``Origin`` headers must be on the allowlist (403 otherwise), which
  defeats DNS-rebinding attacks against servers on loopback.
* API keys are resolved from ``Authorization: Bearer`` / ``X-API-Key`` on
  every request, and a session only answers the credential it was opened
  with (403 otherwise), so a leaked session id alone is useless.
* Session ids are 192-bit random tokens; idle sessions expire and live ones
  are capped at ``max_sessions``.
* ``Content-Type`` must be ``application/json`` (415), bodies are size-capped
  while streaming (413), and an unsupported ``MCP-Protocol-Version`` header
  is rejected (400).
"""

from __future__ import annotations

import contextlib
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from ..exceptions import (
    AUTHENTICATION_REQUIRED,
    FORBIDDEN,
    INVALID_REQUEST,
    PARSE_ERROR,
    PAYLOAD_TOO_LARGE,
    TOO_MANY_SESSIONS,
    AuthenticationError,
)
from ..logging import audit
from ..protocol import SUPPORTED_PROTOCOL_VERSIONS
from ..security.auth import ClientIdentity
from ._http import BaseHTTPTransport, rpc_error
from .base import ClientContext
from .sse import SSETransport

SESSION_HEADER = "MCP-Session-Id"
PROTOCOL_VERSION_HEADER = "MCP-Protocol-Version"

_TRANSPORT = "streamable-http"


@dataclass(slots=True)
class _Session:
    """One logical client session; no connection stays open between requests."""

    id: str
    context: ClientContext
    identity_fp: str | None
    last_seen: float
    active: int = 0  # requests currently being dispatched


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


def _json_response(payload: dict[str, Any], headers: dict[str, str] | None = None) -> Response:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    return Response(body, media_type="application/json", headers=headers)


class StreamableHTTPTransport(BaseHTTPTransport):
    """Serve MCP over Streamable HTTP, plus the legacy SSE endpoints by default.

    Args:
        server: The :class:`~easy_mcp.server.MCPServer` to expose.
        path: The MCP endpoint path.
        legacy_sse: Also serve the deprecated HTTP+SSE endpoints (``/sse`` and
            ``/messages``) so clients that predate Streamable HTTP still connect.
        session_idle_timeout: Seconds without a request after which a session
            expires (its id then gets 404 and the client re-initializes);
            ``None`` keeps sessions until the client deletes them.
    """

    def __init__(
        self,
        server: Any,
        *,
        path: str = "/mcp",
        legacy_sse: bool = True,
        session_idle_timeout: float | None = 3600.0,
    ) -> None:
        super().__init__(server)
        if not path.startswith("/"):
            raise ValueError("path must start with '/'")
        reserved = {"/healthz", "/sse", "/messages"} if legacy_sse else {"/healthz"}
        if path in reserved:
            raise ValueError(f"path {path!r} collides with another endpoint")
        if session_idle_timeout is not None and session_idle_timeout <= 0:
            raise ValueError("session_idle_timeout must be positive or None")
        self._path = path
        self._idle_timeout = session_idle_timeout
        self._legacy = SSETransport(server) if legacy_sse else None
        self._sessions: dict[str, _Session] = {}

    def describe(self) -> str:
        legacy = " (+ legacy sse)" if self._legacy is not None else ""
        return f"streamable-http on {self._server.host}:{self._server.port}{self._path}{legacy}"

    # ------------------------------------------------------------------ app

    def build_app(self) -> Starlette:
        """Build the ASGI application (also usable for tests or mounting)."""
        routes = [
            Route(self._path, self._handle, methods=["GET", "POST", "DELETE"]),
            Route("/healthz", self._handle_health, methods=["GET"]),
        ]
        legacy = self._legacy
        if legacy is not None:
            routes.extend(legacy.routes())

        @contextlib.asynccontextmanager
        async def lifespan(app: Starlette) -> Any:
            try:
                yield
            finally:
                if legacy is not None:
                    await legacy.close_all_sessions()
                for session in list(self._sessions.values()):
                    self._end_session(session, reason="shutdown")

        return Starlette(routes=routes, middleware=self._middleware(), lifespan=lifespan)

    # ------------------------------------------------------------- endpoint

    async def _handle(self, request: Request) -> Response:
        if request.method == "POST":
            return await self._handle_post(request)
        if request.method == "DELETE":
            return await self._handle_delete(request)
        # Every server message answers a client request, so there is nothing
        # to push on a standalone stream; the spec allows 405 here.
        return rpc_error(
            405,
            INVALID_REQUEST,
            "Method Not Allowed: this server offers no GET stream; POST JSON-RPC messages",
            headers={"Allow": "POST, DELETE"},
        )

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
        try:
            identity = self._resolve_identity(request)
        except AuthenticationError:
            return rpc_error(401, AUTHENTICATION_REQUIRED, "Invalid API key")

        if message.get("method") == "initialize" and "id" in message:
            return await self._initialize(message, identity, request)

        session = self._session_for(request, identity)
        if isinstance(session, Response):
            return session
        if "method" not in message and ("result" in message or "error" in message):
            # A reply to a server-to-client request.  This server never sends
            # those, so nothing is waiting for it.
            return Response(status_code=202)

        session.active += 1
        try:
            response = await self._server.dispatch(message, session.context)
        finally:
            session.active -= 1
            session.last_seen = time.monotonic()
        if response is None:
            # A notification, or a request cancelled via notifications/cancelled:
            # MCP sends a reply to neither.
            return Response(status_code=202)
        return _json_response(response)

    async def _handle_delete(self, request: Request) -> Response:
        try:
            identity = self._resolve_identity(request)
        except AuthenticationError:
            return rpc_error(401, AUTHENTICATION_REQUIRED, "Invalid API key")
        session = self._session_for(request, identity)
        if isinstance(session, Response):
            return session
        self._end_session(session, reason="client_terminated")
        return Response(status_code=204)

    # -------------------------------------------------------------- sessions

    async def _initialize(
        self, message: dict[str, Any], identity: ClientIdentity | None, request: Request
    ) -> Response:
        self._expire_idle_sessions()
        if len(self._sessions) >= self._server.max_sessions:
            return rpc_error(503, TOO_MANY_SESSIONS, "too many concurrent sessions")

        # 192-bit random token: the session id is a bearer capability, it
        # must be unguessable.
        session_id = secrets.token_urlsafe(24)
        client_host = request.client.host if request.client else "unknown"
        client_id = identity.fingerprint if identity else f"ip:{client_host}"
        session = _Session(
            id=session_id,
            context=ClientContext(client_id=client_id, session_id=session_id, identity=identity),
            identity_fp=identity.fingerprint if identity else None,
            last_seen=time.monotonic(),
            active=1,
        )
        # Hold the slot while the handshake runs, so concurrent handshakes
        # cannot overshoot max_sessions.
        self._sessions[session_id] = session
        response: dict[str, Any] | None = None
        try:
            response = await self._server.dispatch(message, session.context)
        finally:
            session.active -= 1
            session.last_seen = time.monotonic()
            if response is None or "error" in response:
                # The handshake failed (e.g. rate limited): no session.
                self._sessions.pop(session_id, None)
        if response is None:
            return Response(status_code=202)
        if "error" in response:
            return _json_response(response)
        audit("session_open", session_id=session_id, client_id=client_id, transport=_TRANSPORT)
        return _json_response(response, headers={SESSION_HEADER: session_id})

    def _session_for(
        self, request: Request, identity: ClientIdentity | None
    ) -> _Session | Response:
        """Resolve the request's session, or the error response rejecting it."""
        session_id = request.headers.get(SESSION_HEADER)
        if not session_id:
            return rpc_error(
                400,
                INVALID_REQUEST,
                f"Bad Request: missing {SESSION_HEADER} header; send initialize first",
            )
        session = self._sessions.get(session_id)
        if session is not None and self._expired(session, time.monotonic()):
            self._end_session(session, reason="idle_timeout")
            session = None
        if session is None:
            return rpc_error(
                404, INVALID_REQUEST, "Session not found; send a new initialize request"
            )
        # The session must not be usable with a different (or missing)
        # credential than it was opened with.
        presented_fp = identity.fingerprint if identity else None
        if presented_fp != session.identity_fp:
            audit(
                "session_credential_mismatch",
                session_id=session_id,
                client_id=session.context.client_id,
                transport=_TRANSPORT,
            )
            return rpc_error(403, FORBIDDEN, "Forbidden: credential does not match session")
        version = request.headers.get(PROTOCOL_VERSION_HEADER)
        if version is not None and version not in SUPPORTED_PROTOCOL_VERSIONS:
            return rpc_error(
                400,
                INVALID_REQUEST,
                f"Bad Request: unsupported {PROTOCOL_VERSION_HEADER} "
                f"(supported: {', '.join(SUPPORTED_PROTOCOL_VERSIONS)})",
            )
        session.last_seen = time.monotonic()
        return session

    def _expired(self, session: _Session, now: float) -> bool:
        return (
            self._idle_timeout is not None
            and session.active == 0
            and now - session.last_seen > self._idle_timeout
        )

    def _expire_idle_sessions(self) -> None:
        now = time.monotonic()
        for session in list(self._sessions.values()):
            if self._expired(session, now):
                self._end_session(session, reason="idle_timeout")

    def _end_session(self, session: _Session, *, reason: str) -> None:
        if self._sessions.pop(session.id, None) is None:
            return
        # Calls still running for this session have nobody left to answer.
        for task in list(session.context.in_flight.values()):
            task.cancel()
        audit(
            "session_close",
            session_id=session.id,
            client_id=session.context.client_id,
            transport=_TRANSPORT,
            reason=reason,
        )
