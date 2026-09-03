"""SSE transport (HTTP + Server-Sent Events), the classic MCP remote transport.

Flow:

1. The client opens ``GET /sse``.  The server creates a session and streams
   an ``endpoint`` event containing the URL to POST messages to.
2. The client POSTs JSON-RPC messages to ``/messages?session_id=...``.
3. Responses are streamed back over the open SSE connection.

Security handled here (before anything reaches the dispatcher):

* API keys are resolved from ``Authorization: Bearer`` / ``X-API-Key`` and
  invalid keys are rejected with 401.
* Session ids are 192-bit random capability tokens, and every POST must
  present the *same* credential the session was opened with (403 otherwise),
  so a leaked session id alone cannot escalate privileges.
* Bodies are size-capped while streaming — a lying ``Content-Length`` header
  does not bypass the limit.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
from dataclasses import dataclass, field
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from ..exceptions import PARSE_ERROR, AuthenticationError
from ..logging import audit
from ..security.auth import ClientIdentity
from .base import ClientContext, Transport

KEEPALIVE_SECONDS = 15.0

_CLOSE = object()  # sentinel pushed into session queues on shutdown


@dataclass(slots=True)
class _Session:
    """One live SSE connection and its outbound message queue."""

    id: str
    context: ClientContext
    identity_fp: str | None
    queue: asyncio.Queue[Any] = field(default_factory=asyncio.Queue)


class SSETransport(Transport):
    """Serve MCP over HTTP + Server-Sent Events using Starlette/uvicorn."""

    def __init__(
        self,
        server: Any,
        *,
        sse_path: str = "/sse",
        messages_path: str = "/messages",
    ) -> None:
        super().__init__(server)
        self._sse_path = sse_path
        self._messages_path = messages_path
        self._sessions: dict[str, _Session] = {}
        self._uvicorn: Any = None

    def describe(self) -> str:
        return f"sse on {self._server.host}:{self._server.port}"

    # ------------------------------------------------------------------ app

    def build_app(self) -> Starlette:
        """Build the ASGI application (also usable for tests or mounting)."""

        @contextlib.asynccontextmanager
        async def lifespan(app: Starlette) -> Any:
            try:
                yield
            finally:
                # Graceful shutdown: unblock every open SSE stream.
                await self._close_all_sessions()

        return Starlette(
            routes=[
                Route(self._sse_path, self._handle_sse, methods=["GET"]),
                Route(self._messages_path, self._handle_messages, methods=["POST"]),
                Route("/healthz", self._handle_health, methods=["GET"]),
            ],
            lifespan=lifespan,
        )

    def run(self) -> None:
        """Serve blocking; SIGINT/SIGTERM trigger a graceful uvicorn shutdown."""
        import uvicorn

        config = uvicorn.Config(
            self.build_app(),
            host=self._server.host,
            port=self._server.port,
            log_level="info" if self._server.debug else "warning",
        )
        self._uvicorn = uvicorn.Server(config)
        self._uvicorn.run()

    def stop(self) -> None:
        """Ask the running uvicorn server to exit gracefully."""
        if self._uvicorn is not None:
            self._uvicorn.should_exit = True

    # ------------------------------------------------------------- endpoints

    def _resolve_identity(self, request: Request) -> ClientIdentity | None:
        """Extract and verify the API key from request headers.

        Raises:
            AuthenticationError: If a key was presented but is invalid.
        """
        key: str | None = None
        authorization = request.headers.get("authorization")
        if authorization and authorization.lower().startswith("bearer "):
            key = authorization[7:].strip() or None
        if key is None:
            key = request.headers.get("x-api-key")
        return self._server.authenticate_key(key)

    async def _handle_sse(self, request: Request) -> Response:
        try:
            identity = self._resolve_identity(request)
        except AuthenticationError:
            return JSONResponse({"error": "invalid API key"}, status_code=401)

        if len(self._sessions) >= self._server.max_sessions:
            return JSONResponse({"error": "too many concurrent sessions"}, status_code=503)

        # 192-bit random token: the session id is a bearer capability, it
        # must be unguessable.
        session_id = secrets.token_urlsafe(24)
        client_host = request.client.host if request.client else "unknown"
        client_id = identity.fingerprint if identity else f"ip:{client_host}"
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
            return JSONResponse(
                {"error": f"request exceeds {max_bytes} bytes"}, status_code=413
            )

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
        try:
            identity = self._resolve_identity(request)
        except AuthenticationError:
            return JSONResponse({"error": "invalid API key"}, status_code=401)
        presented_fp = identity.fingerprint if identity else None
        if presented_fp != session.identity_fp:
            audit(
                "session_credential_mismatch",
                session_id=session_id,
                client_id=session.context.client_id,
            )
            return JSONResponse(
                {"error": "credential does not match session"}, status_code=403
            )

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

        response = await self._server.dispatch(message, session.context)
        if response is not None:
            await session.queue.put(response)
        # 202: the JSON-RPC response travels over the SSE stream, not here.
        return Response(status_code=202)

    async def _handle_health(self, request: Request) -> Response:
        return JSONResponse(
            {
                "status": "ok",
                "server": self._server.name,
                "version": self._server.version,
                "tools": len(self._server.tools),
            }
        )

    async def _close_all_sessions(self) -> None:
        for session in list(self._sessions.values()):
            await session.queue.put(_CLOSE)
