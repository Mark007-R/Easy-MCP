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
  stream is bound to the token's principal.  As in 0.3.1, a POST's session
  is looked up before its credential, so an unknown ``session_id`` gets 404
  either way (session ids are unguessable).  The stream itself is not cut
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

With a shared store (``MCPServer(store=RedisStore(...))``) a message may be
posted to a worker other than the one holding the stream.  That worker
checks the session in the store, answers ``202``, dispatches the message and
relays the answer to the stream's worker, which sends it (each answer goes
to exactly one stream).  Closing the stream ends the session on every
worker.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import secrets
import uuid
from typing import TYPE_CHECKING, Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from ..exceptions import (
    INTERNAL_ERROR,
    PARSE_ERROR,
    SERVER_BUSY,
    RateLimitError,
    StoreUnavailableError,
)
from ..logging import audit
from ..middleware import TransportInfo
from ._http import BaseHTTPTransport, store_unavailable
from ._sessions import CLOSE_STREAM, LocalSession, Rejection, SessionManager
from .base import ClientContext

if TYPE_CHECKING:
    from ..server import MCPServer

KEEPALIVE_SECONDS = 15.0

# How long shutdown lets the messages relayed here for a stream another
# worker holds finish, as Streamable HTTP does the requests it serves; and
# how long it then waits for those it stopped to answer on their stream.
_SHUTDOWN_GRACE_SECONDS = 5.0
_STOPPED_ANSWER_SECONDS = 2.0


def _shutting_down() -> Response:
    """The answer to a stream or message arriving once shutdown has begun."""
    return JSONResponse(
        {"error": "server is shutting down; retry shortly"},
        status_code=503,
        headers={"Retry-After": "1"},
    )


def _stopped(message: Any) -> dict[str, Any] | None:
    """The answer to a request shutdown stopped: retry shortly (``None``: no request)."""
    if not isinstance(message, dict) or "id" not in message:
        return None
    error = {
        "code": SERVER_BUSY,
        "message": "Server is shutting down; retry shortly",
        "data": {"reason": "shutdown"},
    }
    return {"jsonrpc": "2.0", "id": message["id"], "error": error}


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
        # Sessions live as long as their stream: in a shared store, on a
        # lease the worker holding the stream renews.
        self._manager = SessionManager(server, "sse", ttl=None, transport="sse")
        # Set once shutdown closes the streams: nothing new is served after.
        self._closing = False

    _audit_transport = "sse"

    @property
    def _sessions(self) -> dict[str, LocalSession]:
        """The sessions whose stream this worker holds, by id."""
        return {local.session_id: local for local in self._manager.local_sessions() if local.owned}

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
        await self._stop_relays(await self._grace(self._relays(), _SHUTDOWN_GRACE_SECONDS))
        await self._manager.shutdown()

    async def close_all_sessions(self) -> None:
        """Unblock every SSE stream this worker holds so its connection can close.

        The calls still running for those streams have nobody left to
        answer; they are cancelled now rather than when each stream winds
        down, so shutdown can wait for their cancel callbacks.  Messages
        relayed here for a stream another worker holds (a shared store) are
        left running: their client still listens, so shutdown gives them a
        grace and then answers them on that stream (:meth:`_stop_relays`).
        From now on new streams and messages are refused (``503``): uvicorn
        may still be accepting connections, and a stream opened now would
        never be closed, so shutdown would wait for it forever.
        """
        self._closing = True
        for session in self._manager.local_sessions():
            if not session.owned:
                continue  # its stream is on another worker
            for task in list(session.tasks):
                task.cancel()
            if session.stream is not None:
                await session.stream.put(CLOSE_STREAM)

    def _relays(self) -> set[asyncio.Task[Any]]:
        """The messages being served here for streams other workers hold."""
        return {
            task
            for session in self._manager.local_sessions()
            if not session.owned
            for task in session.tasks
        }

    async def _stop_relays(self, running: set[asyncio.Task[Any]]) -> None:
        """Stop the relays still *running* once their grace is over.

        Only their dispatch is cancelled: each then tells its stream to
        retry shortly, which is waited for, briefly, before the store closes.
        """
        if not running:
            return
        for session in self._manager.local_sessions():
            if not session.owned:
                for task in list(session.dispatches):
                    task.cancel()
        await asyncio.wait(running, timeout=_STOPPED_ANSWER_SECONDS)

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
            await self._server.acheck_rate_limit(client_id)
        except RateLimitError as exc:
            audit("rate_limited", client_id=client_id, method="GET " + self._sse_path)
            return JSONResponse(
                {"error": str(exc)},
                status_code=429,
                headers={"Retry-After": str(max(1, math.ceil(exc.retry_after_seconds)))},
            )
        except StoreUnavailableError:
            return store_unavailable()

        # 192-bit random token: the session id is a bearer capability, it
        # must be unguessable.
        session_id = secrets.token_urlsafe(24)
        try:
            session = await self._manager.open(
                session_id, client_id=client_id, identity=identity, owned=True
            )
        except StoreUnavailableError:
            return store_unavailable()
        if session is None:
            return JSONResponse({"error": "too many concurrent sessions"}, status_code=503)
        if self._closing:
            # Shutdown began meanwhile: a stream opened now would never be closed.
            await self._manager.end(session, reason=None)
            return _shutting_down()
        await self._manager.finish(session)
        self._manager.opened(session)
        endpoint = f"{self._messages_path}?session_id={session_id}"
        queue = session.stream
        assert queue is not None

        async def stream() -> Any:
            try:
                yield f"event: endpoint\ndata: {endpoint}\n\n"
                while True:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
                    except TimeoutError:
                        yield ": keep-alive\n\n"
                        continue
                    if item is CLOSE_STREAM:
                        break
                    payload = json.dumps(item, ensure_ascii=False, default=str)
                    yield f"event: message\ndata: {payload}\n\n"
            finally:
                # Calls still running for this session have nobody left to
                # answer, on any worker.  Detached: this stream's task may be
                # cancelled at every await from now on.
                reason = "shutdown" if self._closing else "stream_closed"
                await self._manager.end(session, reason=reason, detach=True)

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

        # The stream's own worker knows the session; any other asks the store.
        session = await self._manager.acquire(request.query_params.get("session_id", ""))
        if session is Rejection.UNAVAILABLE:
            return store_unavailable()
        if isinstance(session, Rejection):
            return JSONResponse({"error": "unknown or expired session_id"}, status_code=404)
        relaying = False
        try:
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
            if not self._manager.binds(session, identity):
                self._manager.credential_mismatch(session)
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
            if session.ended:
                # Its stream closed while the body was read: nothing would
                # cancel the call (nor could a notifications/cancelled reach it).
                return JSONResponse({"error": "unknown or expired session_id"}, status_code=404)

            # Dispatch in the background and answer 202 now: the JSON-RPC
            # response travels over the SSE stream, and holding this POST open
            # would stall clients that send one message at a time (a
            # notifications/cancelled could never overtake the slow call it
            # targets).
            info = self._transport_info(request, "sse")
            # This POST's own identity: a refreshed or broader token takes
            # effect at once, on the same stream.
            context = self._manager.context(session, identity)
            if session.owned:
                work = self._deliver(session, message, info, context=context)
            else:
                # The stream is on another worker: the answer is relayed there.
                work = self._relay(session, message, info, context=context)
                relaying = True
            task = asyncio.create_task(work)
            session.tasks.add(task)
            task.add_done_callback(session.tasks.discard)
            return Response(status_code=202)
        finally:
            if not relaying:
                await self._manager.finish(session)

    async def _deliver(
        self,
        session: LocalSession,
        message: Any,
        info: TransportInfo | None = None,
        *,
        context: ClientContext | None = None,
    ) -> None:
        """Dispatch one message and queue its answer on the session's stream, held here.

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
        version = _negotiated(message, response, context)
        if version is not None:
            # Other workers serving its messages read it from the store.
            await self._manager.record_version(session, version)
        if response is not None and session.stream is not None:
            await session.stream.put(response)

    async def _relay(
        self,
        session: LocalSession,
        message: Any,
        info: TransportInfo | None,
        *,
        context: ClientContext,
    ) -> None:
        """Dispatch one message here and send its answer to the worker holding the stream.

        The session stays held until the answer is sent.  A request whose
        dispatch shutdown stops is answered ``-32008`` (retry shortly), as
        Streamable HTTP answers one: its client still listens on the stream.
        A session's end, or a cancel from its client, leaves it unanswered.
        """
        version: str | None = None
        # A task of its own, so shutdown can stop the dispatch alone.
        work = asyncio.ensure_future(self._server.dispatch(message, context, transport=info))
        session.dispatches.add(work)
        try:
            try:
                response = await work
            except asyncio.CancelledError:
                task = asyncio.current_task()
                if (task is not None and task.cancelling()) or not self._closing or session.ended:
                    raise  # this relay was cancelled, not just its dispatch
                response = _stopped(message)
            version = _negotiated(message, response, context)
            if response is not None:
                await self._manager.relay(session, response, protocol_version=version)
        except Exception:
            # Nothing may be lost in this background task: the client waits for an answer.
            error_id = uuid.uuid4().hex[:12]
            self._server._logger.error(
                "could not relay an answer error_id=%s", error_id, exc_info=True
            )
            if isinstance(message, dict) and "id" in message:
                failed = {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "error": {
                        "code": INTERNAL_ERROR,
                        "message": f"Internal server error (error_id={error_id})",
                    },
                }
                await self._manager.relay(session, failed, protocol_version=None)
        finally:
            session.dispatches.discard(work)
            if not work.done():
                work.cancel()  # the call must not outlive its relay
            await self._manager.finish(session, protocol_version=version)


def _negotiated(
    message: Any, response: dict[str, Any] | None, context: ClientContext
) -> str | None:
    """The version a successful ``initialize`` negotiated; ``None`` for any other message."""
    if (
        isinstance(message, dict)
        and message.get("method") == "initialize"
        and response is not None
        and "result" in response
    ):
        return context.protocol_version
    return None
