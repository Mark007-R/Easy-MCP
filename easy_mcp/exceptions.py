"""Exception hierarchy and JSON-RPC error codes for easy_mcp.

Two kinds of errors exist:

* :class:`ProtocolError` subclasses map directly onto JSON-RPC error
  responses.  Their messages are written to be safe to send to clients.
* Everything else is an *internal* error.  Outside debug mode the server
  never forwards its message or traceback to a client; it logs the full
  detail server-side under a unique ``error_id`` instead.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

# --- Standard JSON-RPC 2.0 error codes --------------------------------------
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# --- easy_mcp error codes (JSON-RPC reserves -32000..-32099 for servers) -----
AUTHENTICATION_REQUIRED = -32001
FORBIDDEN = -32002
RATE_LIMITED = -32003
PAYLOAD_TOO_LARGE = -32004
TOOL_TIMEOUT = -32005
SESSION_LIMIT_EXCEEDED = -32006
TOO_MANY_SESSIONS = -32007
SERVER_BUSY = -32008

# --- Codes the MCP spec defines (its reserved -32020..-32099 sub-range) -------
HEADER_MISMATCH = -32020
MISSING_REQUIRED_CLIENT_CAPABILITY = -32021
UNSUPPORTED_PROTOCOL_VERSION = -32022


class EasyMCPError(Exception):
    """Base class for every easy_mcp exception."""


class ToolRegistrationError(EasyMCPError):
    """A function could not be registered as a tool."""


class SchemaError(ToolRegistrationError):
    """A type annotation could not be converted to JSON Schema."""


class ToolError(EasyMCPError):
    """Raised *inside a tool* to return an intentional, safe error message.

    Unlike arbitrary exceptions (which are sanitized down to an opaque
    ``error_id``), the message of a ``ToolError`` is sent to the client
    verbatim.  Only raise it with text you would show an end user.
    """


class ProtocolError(EasyMCPError):
    """An error with a JSON-RPC error code, safe to serialize to clients."""

    code: int = INVALID_REQUEST

    def __init__(self, message: str, *, code: int | None = None, data: Any = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.data = data


class ValidationError(ProtocolError):
    """Tool arguments -- or a tool's own result -- failed schema validation."""

    code = INVALID_PARAMS

    def __init__(self, errors: list[str], *, message: str = "Invalid tool arguments") -> None:
        self.errors = list(errors)
        super().__init__(
            f"{message}: " + "; ".join(self.errors),
            data={"errors": self.errors},
        )


class AuthenticationError(ProtocolError):
    """The request needs a valid API key."""

    code = AUTHENTICATION_REQUIRED


class AuthorizationError(ProtocolError):
    """The authenticated client lacks a scope the tool requires."""

    code = FORBIDDEN


class RateLimitError(ProtocolError):
    """The client exceeded its request budget."""

    code = RATE_LIMITED

    def __init__(self, retry_after_seconds: float) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"Rate limit exceeded; retry in {retry_after_seconds:.1f}s",
            data={"retry_after_seconds": round(retry_after_seconds, 3)},
        )


class PayloadTooLargeError(ProtocolError):
    """The request body exceeded ``max_request_bytes``."""

    code = PAYLOAD_TOO_LARGE


class SessionLimitError(ProtocolError):
    """A per-session tool usage limit was reached."""

    code = SESSION_LIMIT_EXCEEDED


class ServerBusyError(ProtocolError):
    """Every worker thread for sync tools is occupied; retry shortly."""

    code = SERVER_BUSY


# --- OAuth (MCPServer(oauth=...)) ---------------------------------------------

# What a client is told about a refused token: fixed strings only, so nothing
# from the token (or the reason it failed) can reach a response.
_TOKEN_DESCRIPTIONS = {
    "expired": "The access token expired",
    "wrong_audience": "The access token was not issued for this resource",
}


class TokenRequiredError(AuthenticationError):
    """No credential was presented, and the server requires one (``oauth=`` is set)."""

    def __init__(self, message: str = "Authentication required") -> None:
        super().__init__(message)


class InvalidTokenError(AuthenticationError):
    """An access token failed verification.

    Attributes:
        reason: Why, for the audit log: ``malformed``, ``too_large``,
            ``encrypted``, ``unsupported_alg``, ``bad_type``, ``crit``,
            ``unknown_key``, ``bad_key``, ``bad_signature``, ``expired``,
            ``not_yet_valid``, ``wrong_issuer``, ``wrong_audience``,
            ``missing_claims``, ``bound_token``, ``inactive`` or
            ``wrong_token_type``.
        description: A fixed text safe to send to the client; it never holds
            anything taken from the token.
        issuer: The token's issuer, only when it is a configured one.
    """

    def __init__(self, reason: str, *, issuer: str | None = None) -> None:
        self.reason = reason
        self.issuer = issuer
        self.description = _TOKEN_DESCRIPTIONS.get(reason, "The access token is invalid")
        super().__init__("Invalid access token")


class InsufficientScopeError(AuthorizationError):
    """A valid access token lacks a scope the request needs.

    ``scopes`` are the scopes to ask for, narrowest first.  The error carries
    ``-32001`` rather than :data:`FORBIDDEN`, which the stateless revision
    forbids, and ``data = {"error": "insufficient_scope", "scope": ...}``;
    over Streamable HTTP it becomes ``403`` with a ``WWW-Authenticate``
    challenge naming them.
    """

    code = AUTHENTICATION_REQUIRED

    def __init__(
        self,
        scopes: Iterable[str],
        message: str = "Insufficient scope",
        *,
        granted: Iterable[str] = (),
    ) -> None:
        self.scopes = tuple(scopes)
        # What the token already holds, for the challenge of older clients,
        # which do not add it to their next request themselves.  Never sent
        # as it is.
        self.granted = frozenset(granted)
        super().__init__(
            message, data={"error": "insufficient_scope", "scope": " ".join(self.scopes)}
        )


class AuthServerUnavailableError(ProtocolError):
    """The authorization server's metadata, keys or introspection could not be reached.

    ``-32008`` with ``data.reason = "auth_server_unavailable"``: the token may
    well be fine, so the client should retry rather than sign in again.  Not
    a :class:`ServerBusyError`, so code that refunds busy calls never catches it.

    ``sent_request`` is set when this token was sent for introspection and
    that request failed (a refusal during an outage window sends nothing).
    Such a failure is charged to the caller's failed-authentication budget,
    since a token can be made to fail it.
    """

    code = SERVER_BUSY

    def __init__(
        self,
        message: str = "Authorization server unavailable; retry shortly",
        *,
        issuer: str | None = None,
        stage: str | None = None,
        sent_request: bool = False,
    ) -> None:
        self.issuer = issuer
        self.stage = stage
        self.sent_request = sent_request
        super().__init__(message, data={"reason": "auth_server_unavailable"})
