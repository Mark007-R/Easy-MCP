"""Plumbing shared by the HTTP transports (Streamable HTTP and legacy SSE).

* :class:`OriginGuard` rejects browser requests from origins outside the
  allowlist with 403.  A DNS-rebinding page can make a victim's browser talk
  to a server on loopback, but the browser still stamps the page's real
  origin on every POST and DELETE, so the request stops here before any
  route runs.
* :class:`BaseHTTPTransport` resolves credentials from headers, serves the
  health endpoint, and owns the uvicorn lifecycle.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from ..exceptions import FORBIDDEN
from ..logging import audit
from ..security.auth import ClientIdentity
from .base import Transport

if TYPE_CHECKING:
    from ..server import MCPServer

LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def normalize_origins(origins: Iterable[str]) -> frozenset[str]:
    """Validate and normalize an ``allowed_origins`` setting.

    Raises:
        ValueError: If an entry is neither ``"*"`` nor an http(s) origin.
    """
    normalized: set[str] = set()
    for origin in origins:
        value = origin.strip().lower().rstrip("/")
        if value != "*" and not value.startswith(("http://", "https://")):
            raise ValueError(
                f"allowed origin must be '*' or start with http:// or https://, got {origin!r}"
            )
        normalized.add(value)
    return frozenset(normalized)


def is_loopback_origin(origin: str) -> bool:
    """Whether *origin* is http(s) on localhost, 127.0.0.1 or [::1] (any port)."""
    try:
        parts = urlsplit(origin)
    except ValueError:
        return False
    return parts.scheme in ("http", "https") and parts.hostname in LOOPBACK_HOSTS


def origin_allowed(origin: str, allowed: frozenset[str] | None) -> bool:
    """Check an ``Origin`` header value against an allowlist.

    ``None`` (the default) admits loopback origins only; ``"*"`` admits all.
    """
    if allowed is None:
        return is_loopback_origin(origin)
    return "*" in allowed or origin.strip().lower().rstrip("/") in allowed


def rpc_error(
    status: int, code: int, message: str, *, headers: dict[str, str] | None = None
) -> JSONResponse:
    """An HTTP error whose body is a JSON-RPC error response without an id."""
    return JSONResponse(
        {"jsonrpc": "2.0", "id": None, "error": {"code": code, "message": message}},
        status_code=status,
        headers=headers,
    )


def _header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", ()):
        if key == name:
            return str(value.decode("latin-1"))
    return None


class OriginGuard:
    """ASGI middleware: answer 403 when the ``Origin`` header is not allowed.

    Requests without an ``Origin`` header come from non-browser clients and
    pass through; browsers always send one on POST and DELETE.
    """

    def __init__(self, app: ASGIApp, allowed_origins: frozenset[str] | None = None) -> None:
        self.app = app
        self.allowed_origins = allowed_origins

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            origin = _header(scope, b"origin")
            if origin is not None and not origin_allowed(origin, self.allowed_origins):
                audit("origin_rejected", origin=origin[:200], path=scope.get("path", ""))
                response = rpc_error(403, FORBIDDEN, "Forbidden: origin not allowed")
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


class BaseHTTPTransport(Transport):
    """Shared plumbing for transports that uvicorn serves over HTTP."""

    def __init__(self, server: MCPServer) -> None:
        super().__init__(server)
        self._uvicorn: Any = None

    @abc.abstractmethod
    def build_app(self) -> Starlette:
        """Build the ASGI application (also usable for tests or mounting)."""

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

    def _middleware(self) -> list[Middleware]:
        return [Middleware(OriginGuard, allowed_origins=self._server.allowed_origins)]

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
        identity: ClientIdentity | None = self._server.authenticate_key(key)
        return identity

    async def _handle_health(self, request: Request) -> Response:
        return JSONResponse(
            {
                "status": "ok",
                "server": self._server.name,
                "version": self._server.version,
                "tools": len(self._server.tools),
            }
        )
