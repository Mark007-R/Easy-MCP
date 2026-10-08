"""SSE transport (HTTP + Server-Sent Events), the legacy MCP remote transport.

Superseded by Streamable HTTP (:mod:`.streamable_http`), which serves these
same endpoints next to ``/mcp`` by default so older clients keep working.

Flow:

1. The client opens ``GET /sse``.  The server creates a session and streams
   an ``endpoint`` event containing the URL to POST messages to.
2. The client POSTs JSON-RPC messages to ``/messages?session_id=...``.
3. Responses are streamed back over the open SSE connection.

Security handled here (before anything reaches the dispatcher):

* Browser ``Origin`` headers must be on the allowlist (403 otherwise), the
  DNS-rebinding defense shared with Streamable HTTP.
* API keys are resolved from ``Authorization: Bearer`` / ``X-API-Key`` and
  invalid keys are rejected with 401.  With ``oauth=``, opening a stream and
  every POST need a valid credential (401 with a challenge otherwise), and a
  stream is bound to the token's principal.  The stream itself is not cut
  when its token expires: it only delivers, and every POST is checked.  A
  tool call the token lacks a scope for cannot change the ``202`` its POST
  already got, so it arrives on the stream as a ``-32001`` error with
  ``data.error = "insufficient_scope"``.
* Session ids are 192-bit random capability tokens, and every POST must
  present the *same* credential the session was opened with (403 otherwise),
  so a leaked session id alone cannot escalate privileges.
* Bodies are size-capped while streaming — a lying ``Content-Length`` header
  does not bypass the limit.
* Opening a session (``GET /sse``) spends the client's rate-limit budget like
  any message (429 when exhausted), so an anonymous client cannot fill
  ``max_sessions`` and lock everyone else out.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import secrets
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from ..exceptions import PARSE_ERROR, RateLimitError
from ..logging import audit
from ..middleware import TransportInfo
from ._http import BaseHTTPTransport
from .base import ClientContext

if TYPE_CHECKING:
    from ..server import MCPServer

KEEPALIVE_SECONDS = 15.0

_CLOSE = object()  # sentinel pushed into session queues on shutdown


def _shutting_down() -> Response:
    """The answer to a stream or message arriving once shutdown has begun."""
    return JSONResponse(
        {"error": "server is shutting down; retry shortly"},
        status_code=503,
        headers={"Retry-After": "1"},
    )


@dataclass(slots=True)
class _Session:
    """One live SSE connection and its outbound message queue."""

    id: str
    context: ClientContext
    identity_fp: str | None
    queue: asyncio.Queue[Any] = field(default_factory=asyncio.Queue)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)


class SSETransport(BaseHTTPTransport):
    """Serve MCP over HTTP + Server-Sent Events using Starlette/uvicorn."""

    def __init__(
        self,
        server: MCPServer,
        *,
        sse_path: str = "/sse",
        messages_path: str = "/messages",
    ) -> None:
        super().__init__(server)
        self._sse_path = sse_path
        self._messages_path = messages_path
        self._sessions: dict[str, _Session] = {}
        # Set once shutdown closes the streams: nothing new is served after.
        self._closing = False

    _audit_transport = "sse"

    def describe(self) -> str:
        return f"sse on {self._server.host}:{self._server.port}"

    def _endpoint_paths(self) -> tuple[str, ...]:
        return (self._sse_path, self._messages_path)

    def _reserved_paths(self) -> frozenset[str]:
        return super()._reserved_paths() | {self._sse_path, self._messages_path}

    def _reopen(self) -> None:
        self._closing = False

    def _invalid_key_response(self) -> Response:
        return JSONResponse({"error": "invalid API key"}, status_code=401)

    # ------------------------------------------------------------------ app

    def routes(self) -> list[Route]:
        """The SSE endpoints, for serving from another transport's app."""
        return [
            Route(self._sse_path, self._handle_sse, methods=["GET"]),
            Route(self._messages_path, self._handle_messages, methods=["POST"]),
        ]

    def build_app(self) -> Starlette:
        """Build the ASGI application (also usable for tests or mounting)."""

        @contextlib.asynccontextmanager
        async def lifespan(app: Starlette) -> Any:
            # Serves anew (an app built again from this transport), warms up
            # OAuth, and at the end unblocks every open SSE stream and waits
            # for tool threads.
            async with self._server._lifespan(self):
                yield

        return Starlette(
            routes=[
                *self.routes(),
                Route("/healthz", self._handle_health, methods=["GET"]),
                *self._metadata_routes(),
            ],
            middleware=self._middleware(),
            lifespan=lifespan,
        )

    async def close_streams(self) -> None:
        await self.close_all_sessions()

    async def close_all_sessions(self) -> None:
        """Unblock every open SSE stream so its connection can close.

        Calls still running have nobody left to answer; they are cancelled
        now rather than when each stream winds down, so shutdown can wait
        for their cancel callbacks.  From now on new streams and messages
        are refused (``503``): uvicorn may still be accepting connections,
        and a stream opened now would never be closed, so shutdown would
        wait for it forever.
        """
        self._closing = True
        for session in list(self._sessions.values()):
            for task in list(session.tasks):
                task.cancel()
            await session.queue.put(_CLOSE)

    # ------------------------------------------------------------- endpoints

    async def _handle_sse(self, request: Request) -> Response:
        if self._closing:
            return _shutting_down()
        resolved = await self._resolve_identity(request, modern=False)
        if isinstance(resolved, Response):
            return resolved
        identity = resolved
        if self._closing:
            # Shutdown began while the token was checked: a stream opened now
            # would never be closed.
            return _shutting_down()

        client_host = request.client.host if request.client else "unknown"
        client_id = identity.fingerprint if identity else f"ip:{client_host}"
        # Opening a session costs one request, so session slots cannot be
        # exhausted faster than the rate limit allows.
        try:
            self._server.check_rate_limit(client_id)
        except RateLimitError as exc:
            audit("rate_limited", client_id=client_id, method="GET " + self._sse_path)
            return JSONResponse(
                {"error": str(exc)},
                status_code=429,
                headers={"Retry-After": str(max(1, math.ceil(exc.retry_after_seconds)))},
            )

        if len(self._sessions) >= self._server.max_sessions:
            return JSONResponse({"error": "too many concurrent sessions"}, status_code=503)

        # 192-bit random token: the session id is a bearer capability, it
        # must be unguessable.
        session_id = secrets.token_urlsafe(24)
        context = ClientContext(client_id=client_id, session_id=session_id, identity=identity)
        session = _Session(
            id=session_id,
            context=context,
            identity_fp=identity.fingerprint if identity else None,
        )
        self._sessions[session_id] = session
        audit("session_open", session_id=session_id, client_id=client_id)
        endpoint = f"{self._messages_path}?session_id={session_id}"

        async def stream() -> Any:
            try:
                yield f"event: endpoint\ndata: {endpoint}\n\n"
                while True:
                    try:
                        item = await asyncio.wait_for(
                            session.queue.get(), timeout=KEEPALIVE_SECONDS
                        )
                    except TimeoutError:
                        yield ": keep-alive\n\n"
                        continue
                    if item is _CLOSE:
                        break
                    payload = json.dumps(item, ensure_ascii=False, default=str)
                    yield f"event: message\ndata: {payload}\n\n"
            finally:
                self._sessions.pop(session_id, None)
                # Calls still running for this session have nobody left to answer.
                for task in list(session.tasks):
                    task.cancel()
                audit("session_close", session_id=session_id, client_id=client_id)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async def _handle_messages(self, request: Request) -> Response:
        max_bytes = self._server.max_request_bytes

        # Cheap header-based rejection first ...
        content_length = request.headers.get("content-length")
        if content_length and content_length.isdigit() and int(content_length) > max_bytes:
            return JSONResponse({"error": f"request exceeds {max_bytes} bytes"}, status_code=413)

        session_id = request.query_params.get("session_id", "")
        session = self._sessions.get(session_id)
        if session is None:
            return JSONResponse({"error": "unknown or expired session_id"}, status_code=404)

        # ... then enforce the cap while reading, since Content-Length can lie.
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > max_bytes:
                return JSONResponse(
                    {"error": f"request exceeds {max_bytes} bytes"}, status_code=413
                )

        # Re-authenticate every POST: the session must not be usable with a
        # different (or missing) credential than it was opened with.
        resolved = await self._resolve_identity(request, modern=False)
        if isinstance(resolved, Response):
            return resolved
        identity = resolved
        presented_fp = identity.fingerprint if identity else None
        if presented_fp != session.identity_fp:
            audit(
                "session_credential_mismatch",
                session_id=session_id,
                client_id=session.context.client_id,
            )
            return JSONResponse({"error": "credential does not match session"}, status_code=403)

        try:
            message = json.loads(bytes(body).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": PARSE_ERROR, "message": "Parse error: invalid JSON"},
                },
                status_code=400,
            )

        if self._closing:
            # Checked after the last await: its stream may have closed
            # already, and nothing would cancel the call.
            return _shutting_down()
        if self._sessions.get(session_id) is not session:
            # Its stream closed while the body was read: nothing would
            # cancel the call (nor could a notifications/cancelled reach it).
            return JSONResponse({"error": "unknown or expired session_id"}, status_code=404)

        # Dispatch in the background and answer 202 now: the JSON-RPC response
        # travels over the SSE stream, and holding this POST open would stall
        # clients that send one message at a time (a notifications/cancelled
        # could never overtake the slow call it targets).
        info = self._transport_info(request, "sse")
        # This POST's own identity: a refreshed or broader token takes effect
        # at once, on the same stream.
        context = self._server._request_context(session.context, identity)
        task = asyncio.create_task(self._deliver(session, message, info, context=context))
        session.tasks.add(task)
        task.add_done_callback(session.tasks.discard)
        return Response(status_code=202)

    async def _deliver(
        self,
        session: _Session,
        message: Any,
        info: TransportInfo | None = None,
        *,
        context: ClientContext | None = None,
    ) -> None:
        """Dispatch one message and queue its answer on the session's stream.

        *context* is the message's own (the session's, with the identity its
        POST presented); by default the session's.
        """
        if context is None:
            context = session.context
        response = await self._server.dispatch(message, context, transport=info)
        if context is not session.context and context.protocol_version is not None:
            # An initialize sent with a refreshed token negotiated on the
            # copy: the session speaks that version from now on.
            if context.protocol_version != session.context.protocol_version:
                session.context.protocol_version = context.protocol_version
        if response is not None:
            await session.queue.put(response)
