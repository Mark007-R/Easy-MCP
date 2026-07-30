"""Exception hierarchy and JSON-RPC error codes for easy_mcp.

Two kinds of errors exist:

* :class:`ProtocolError` subclasses map directly onto JSON-RPC error
  responses.  Their messages are written to be safe to send to clients.
* Everything else is an *internal* error.  Outside debug mode the server
  never forwards its message or traceback to a client; it logs the full
  detail server-side under a unique ``error_id`` instead.
"""

from __future__ import annotations

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
    """Tool arguments failed schema validation."""

    code = INVALID_PARAMS

    def __init__(self, errors: list[str]) -> None:
        self.errors = list(errors)
        super().__init__(
            "Invalid tool arguments: " + "; ".join(self.errors),
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
