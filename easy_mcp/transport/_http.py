"""Plumbing shared by the HTTP transports (Streamable HTTP and legacy SSE).

* :class:`OriginGuard` rejects browser requests from origins outside the
  allowlist with 403.  A DNS-rebinding page can make a victim's browser talk
  to a server on loopback, but the browser still stamps the page's real
  origin on every POST and DELETE, so the request stops here before any
  route runs.
* :class:`BaseHTTPTransport` resolves credentials from headers (API keys,
  and OAuth access tokens with their ``WWW-Authenticate`` challenges), serves
  the health endpoint and the OAuth Protected Resource Metadata, and owns the
  uvicorn lifecycle.
"""

from __future__ import annotations

import abc
import json
import math
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from ..exceptions import (
    AUTHENTICATION_REQUIRED,
    FORBIDDEN,
    INVALID_REQUEST,
    RATE_LIMITED,
    AuthenticationError,
    AuthServerUnavailableError,
    InsufficientScopeError,
    InvalidTokenError,
    RateLimitError,
    TokenRequiredError,
)
from ..logging import audit
from ..middleware import TransportInfo
from ..protocol import MODERN_PROTOCOL_VERSIONS
from ..security.auth import ClientIdentity, fingerprint, is_scope_token
from ..security.oauth import UNAVAILABLE_RETRY_SECONDS
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
    status: int,
    code: int,
    message: str,
    *,
    headers: dict[str, str] | None = None,
    data: Any = None,
) -> JSONResponse:
    """An HTTP error whose body is a JSON-RPC error response without an id."""
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return JSONResponse(
        {"jsonrpc": "2.0", "id": None, "error": error},
        status_code=status,
        headers=headers,
    )


def bearer_challenge(
    metadata_url: str,
    *,
    error: str | None = None,
    scope: Iterable[str] = (),
    description: str | None = None,
) -> str:
    """A ``WWW-Authenticate: Bearer`` value (RFC 6750 section 3, RFC 9728 section 5.1).

    Every value comes from validated configuration or a fixed string, so none
    can hold a ``"`` or ``\\``; scopes that are not scope-tokens are dropped.
    """
    params: list[str] = []
    if error is not None:
        params.append(f'error="{error}"')
    scopes = [item for item in dict.fromkeys(scope) if is_scope_token(item)]
    if scopes:
        params.append(f'scope="{" ".join(scopes)}"')
    params.append(f'resource_metadata="{metadata_url}"')
    if description is not None:
        params.append(f'error_description="{description}"')
    value = "Bearer " + ", ".join(params)
    assert '\\' not in value and value.count('"') == 2 * len(params), "unsafe challenge"
    return value


def token_principal(
    identity: ClientIdentity | None,
) -> tuple[str, str | None, str | None] | None:
    """The whole principal of a token identity: issuer, subject and client.

    ``None`` for an API key or anonymous caller.  A session compares it as
    well as the fingerprint, so no principal can use another's session even
    if their fingerprints matched.
    """
    if identity is None or identity.issuer is None:
        return None
    return (identity.issuer, identity.subject, identity.client_id)


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
                # The stateless revision forbids -32002 in any response, so a
                # request that names it gets the generic -32600; older clients
                # keep the code they have always seen.
                version = _header(scope, b"mcp-protocol-version")
                code = INVALID_REQUEST if version in MODERN_PROTOCOL_VERSIONS else FORBIDDEN
                response = rpc_error(403, code, "Forbidden: origin not allowed")
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


# How long the HTTP transports wait at shutdown for sync tool threads and
# cancel callbacks to finish (stdio uses its own shutdown_timeout).
THREAD_SHUTDOWN_GRACE = 5.0


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
        transport = self

        class Server(uvicorn.Server):
            async def shutdown(self, sockets: Any = None) -> None:
                # uvicorn waits for every connection to close before the
                # lifespan shutdown runs, and an open SSE stream only closes
                # when told to: tell it first, or shutdown waits for the
                # client to leave (and a forced exit skips the lifespan).
                await transport.close_streams()
                await super().shutdown(sockets)

        self._uvicorn = Server(config)
        self._uvicorn.run()

    async def close_streams(self) -> None:
        """End the long-lived streams that would hold up a graceful shutdown."""

    def _forced_exit(self) -> bool:
        """Whether uvicorn was told to quit without waiting (a second Ctrl-C)."""
        return self._uvicorn is not None and bool(self._uvicorn.force_exit)

    def stop(self) -> None:
        """Ask the running uvicorn server to exit gracefully."""
        if self._uvicorn is not None:
            self._uvicorn.should_exit = True

    def _middleware(self) -> list[Middleware]:
        return [Middleware(OriginGuard, allowed_origins=self._server.allowed_origins)]

    # The transport's name in audit events.
    _audit_transport = "http"

    def _reopen(self) -> None:
        """Serve anew: an app started again after a shutdown takes requests again."""

    def _reserved_paths(self) -> frozenset[str]:
        """Paths an MCP endpoint must not take: the transport serves something else there."""
        return frozenset({"/healthz"})

    def _endpoint_paths(self) -> tuple[str, ...]:
        """The paths clients address, for the check of the OAuth ``resource``."""
        return ()

    def _invalid_key_response(self) -> Response:
        """The answer to a credential that is no API key (and cannot be a token)."""
        return rpc_error(401, AUTHENTICATION_REQUIRED, "Invalid API key")

    async def _resolve_identity(
        self, request: Request, *, modern: bool, tool: str | None = None
    ) -> ClientIdentity | None | Response:
        """The request's identity, or the response that rejects its credential.

        Without ``oauth``, the API key comes from ``Authorization: Bearer`` or
        ``X-API-Key`` as it always has.  With it, an ``Authorization: Bearer``
        value is an API key or an access token, and a request without a
        credential is challenged.  *modern* says which revision's challenge
        to send; *tool* is the tool a ``tools/call`` names, whose scope a
        token refused for ``required_scopes`` is asked for in the same
        challenge.
        """
        server = self._server
        if server.oauth is None:
            key: str | None = None
            authorization = request.headers.get("authorization")
            if authorization and authorization.lower().startswith("bearer "):
                key = authorization[7:].strip() or None
            if key is None:
                key = request.headers.get("x-api-key")
            try:
                return server.authenticate_key(key)
            except AuthenticationError:
                return self._invalid_key_response()
        return await self._resolve_oauth(request, modern=modern, tool=tool)

    async def _resolve_oauth(
        self, request: Request, *, modern: bool, tool: str | None = None
    ) -> ClientIdentity | None | Response:
        server = self._server
        oauth = server.oauth
        assert oauth is not None
        metadata_url = oauth.metadata_url
        values = request.headers.getlist("authorization")
        if len(values) > 1:
            # Components that read only the first (or the last) copy would
            # each see a different credential.
            return rpc_error(
                400,
                INVALID_REQUEST,
                "Bad Request: more than one Authorization header",
                headers={
                    "WWW-Authenticate": bearer_challenge(metadata_url, error="invalid_request")
                },
            )
        bearer: str | None = None
        if values:
            scheme, _, credentials = values[0].strip().partition(" ")
            # Any other scheme (DPoP, Basic) is no credential here.
            if scheme.lower() == "bearer":
                bearer = credentials.strip() or None
        api_key = request.headers.get("x-api-key") if bearer is None else None
        address = request.client.host if request.client else "unknown"
        client_id = f"ip:{address}"
        throttle_key = f"authfail:{client_id}"

        reservation: float | None = None
        if bearer is not None or api_key is not None:
            try:
                # Held while the credential is checked, and kept if it fails.
                reservation = server._reserve_auth_attempt(throttle_key)
            except RateLimitError as exc:
                # Too many failed (or running) checks from this address:
                # refuse without verifying, which bounds signature checks,
                # key fetches and introspection calls.
                audit("auth_rate_limited", client_id=client_id, transport=self._audit_transport)
                return rpc_error(
                    429,
                    RATE_LIMITED,
                    str(exc),
                    headers={"Retry-After": str(max(1, math.ceil(exc.retry_after_seconds)))},
                    data=exc.data,
                )
        failed = False
        try:
            return await server.authenticate_request(bearer=bearer, api_key=api_key, tool=tool)
        except TokenRequiredError:
            # No credential: RFC 6750 sends no error code, only where to sign in.
            challenge = bearer_challenge(metadata_url, scope=server._initial_scopes())
            return rpc_error(
                401,
                AUTHENTICATION_REQUIRED,
                "Authentication required",
                headers={"WWW-Authenticate": challenge},
            )
        except InvalidTokenError as exc:
            failed = True  # the reservation is this failure's charge
            fields = {"issuer": exc.issuer} if exc.issuer in oauth.authorization_servers else {}
            audit(
                "auth_failed",
                reason=exc.reason,
                transport=self._audit_transport,
                client_id=client_id,
                token_fp=fingerprint(bearer or ""),
                **fields,
            )
            challenge = bearer_challenge(
                metadata_url, error="invalid_token", description=exc.description
            )
            return rpc_error(
                401,
                AUTHENTICATION_REQUIRED,
                "Invalid access token",
                headers={"WWW-Authenticate": challenge},
            )
        except InsufficientScopeError as exc:
            audit(
                "auth_failed",
                reason="insufficient_scope",
                transport=self._audit_transport,
                client_id=client_id,
                token_fp=fingerprint(bearer or ""),
                scope=" ".join(exc.scopes),
            )
            return rpc_error(
                403,
                AUTHENTICATION_REQUIRED,
                str(exc),
                headers={
                    "WWW-Authenticate": self._scope_challenge(exc.scopes, exc.granted, modern)
                },
                data=exc.data,
            )
        except AuthServerUnavailableError as exc:
            # Charged when this token was sent for introspection and that
            # request failed: a token can be made to fail it (a filter in
            # front of the endpoint), so it must not be repeated for free.  A
            # refusal while an outage window is open sent nothing.
            failed = exc.sent_request
            return rpc_error(
                503,
                exc.code,
                str(exc),
                headers={"Retry-After": str(UNAVAILABLE_RETRY_SECONDS)},
                data=exc.data,
            )
        except AuthenticationError:
            # An X-API-Key that matches no key: the 0.3.1 answer, plus where
            # to sign in instead.
            response = self._invalid_key_response()
            response.headers["WWW-Authenticate"] = bearer_challenge(
                metadata_url, scope=server._initial_scopes()
            )
            return response
        finally:
            if not failed:
                server._release_auth_attempt(throttle_key, reservation)

    def _scope_challenge(
        self, needed: Iterable[str], granted: Iterable[str], modern: bool
    ) -> str:
        """The ``403 insufficient_scope`` challenge for a request of either era.

        A stateless (2026-07-28) client adds the challenged scopes to those it
        holds, so only what the operation needs is listed.  Older clients ask
        for exactly what the challenge lists, so it also lists the scopes the
        token already holds that this server knows, or the client would lose
        them.  Unrelated scopes and ``offline_access`` are never echoed.
        """
        oauth = self._server.oauth
        assert oauth is not None
        scopes = [scope for scope in needed if scope != "offline_access"]
        if not modern:
            known = self._server._known_scopes()
            scopes.extend(sorted((set(granted) & known) - set(scopes)))
        return bearer_challenge(
            oauth.metadata_url,
            error="insufficient_scope",
            scope=scopes,
            description="Additional scope required",
        )

    def _step_up_headers(
        self, response: dict[str, Any], identity: ClientIdentity | None, modern: bool
    ) -> dict[str, str] | None:
        """The ``403`` challenge for a dispatched request a token lacked a scope for.

        ``None`` unless *response* is an ``insufficient_scope`` error answered
        to an OAuth token identity.
        """
        if self._server.oauth is None or identity is None or identity.issuer is None:
            return None
        error = response.get("error")
        data = error.get("data") if isinstance(error, dict) else None
        if not isinstance(data, dict) or data.get("error") != "insufficient_scope":
            return None
        scope = data.get("scope")
        needed = scope.split() if isinstance(scope, str) else []
        return {"WWW-Authenticate": self._scope_challenge(needed, identity.scopes, modern)}

    def _metadata_routes(self) -> list[Route]:
        """The OAuth Protected Resource Metadata route, when ``oauth`` is set."""
        oauth = self._server.oauth
        if oauth is None:
            return []
        return [Route(oauth.metadata_path, self._handle_resource_metadata, methods=["GET"])]

    async def _handle_resource_metadata(self, request: Request) -> Response:
        """RFC 9728 Protected Resource Metadata: no authentication, the same bytes each time."""
        oauth = self._server.oauth
        assert oauth is not None
        document = oauth._metadata(
            resource_name=self._server.name, scopes=self._server._initial_scopes()
        )
        body = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return Response(
            body.encode("utf-8"),
            media_type="application/json",
            # Short: with step_up=False, tools registered at runtime change it.
            headers={"Cache-Control": "public, max-age=300"},
        )

    @staticmethod
    def _transport_info(request: Request, name: str) -> TransportInfo:
        """How *request* arrived, for middleware; credential headers are left out."""
        client = request.client
        return TransportInfo(
            name,
            client_address=client.host if client else None,
            client_port=client.port if client else None,
            http_version=request.scope.get("http_version"),
            headers=[
                (key.decode("latin-1"), value.decode("latin-1"))
                for key, value in request.headers.raw
            ],
        )

    async def _handle_health(self, request: Request) -> Response:
        health: dict[str, Any] = {
            "status": "ok",
            "server": self._server.name,
            "version": self._server.version,
            "tools": len(self._server.tools),
        }
        oauth = self._server.oauth
        if oauth is not None:
            # Still 200 when unavailable: every worker shares the cause, so
            # draining this one would not help; this says why tokens get 503.
            health["oauth"] = "ok" if oauth._ready() else "unavailable"
        return JSONResponse(health)
