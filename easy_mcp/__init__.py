"""easy_mcp — build secure MCP (Model Context Protocol) servers from plain
Python functions.

Quickstart::

    from easy_mcp import MCPServer

    server = MCPServer(port=8000)

    @server.tool
    def add(a: int, b: int) -> int:
        \"\"\"Add two numbers.\"\"\"
        return a + b

    server.run()
"""

from ._version import __version__
from .cancellation import CancelToken, cancel_scope, current_cancel_token
from .decorators import ToolDefinition
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    EasyMCPError,
    PayloadTooLargeError,
    ProtocolError,
    RateLimitError,
    SchemaError,
    ServerBusyError,
    SessionLimitError,
    ToolError,
    ToolRegistrationError,
    ValidationError,
)
from .middleware import (
    RequestInfo,
    RequestOutcome,
    ToolCall,
    ToolOutcome,
    TransportInfo,
    current_tool_call,
)
from .protocol import SUPPORTED_PROTOCOL_VERSIONS
from .security.auth import APIKeyAuth, ClientIdentity
from .security.ratelimit import SlidingWindowRateLimiter
from .server import PROTOCOL_VERSION, MCPServer
from .transport.base import ClientContext, Transport
from .transport.sse import SSETransport
from .transport.stdio import StdioTransport
from .transport.streamable_http import StreamableHTTPTransport

__all__ = [
    "APIKeyAuth",
    "AuthenticationError",
    "AuthorizationError",
    "CancelToken",
    "ClientContext",
    "ClientIdentity",
    "EasyMCPError",
    "MCPServer",
    "PROTOCOL_VERSION",
    "PayloadTooLargeError",
    "ProtocolError",
    "RateLimitError",
    "RequestInfo",
    "RequestOutcome",
    "SSETransport",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "SchemaError",
    "ServerBusyError",
    "SessionLimitError",
    "SlidingWindowRateLimiter",
    "StdioTransport",
    "StreamableHTTPTransport",
    "ToolDefinition",
    "ToolError",
    "ToolRegistrationError",
    "ToolCall",
    "ToolOutcome",
    "Transport",
    "TransportInfo",
    "ValidationError",
    "__version__",
    "cancel_scope",
    "current_cancel_token",
    "current_tool_call",
]
