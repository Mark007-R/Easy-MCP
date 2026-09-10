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

from .decorators import ToolDefinition
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    EasyMCPError,
    PayloadTooLargeError,
    ProtocolError,
    RateLimitError,
    SchemaError,
    SessionLimitError,
    ToolError,
    ToolRegistrationError,
    ValidationError,
)
from .protocol import SUPPORTED_PROTOCOL_VERSIONS
from .security.auth import APIKeyAuth, ClientIdentity
from .security.ratelimit import SlidingWindowRateLimiter
from .server import PROTOCOL_VERSION, MCPServer
from .transport.base import ClientContext, Transport
from .transport.sse import SSETransport
from .transport.stdio import StdioTransport
from .transport.streamable_http import StreamableHTTPTransport

__version__ = "0.2.1"

__all__ = [
    "APIKeyAuth",
    "AuthenticationError",
    "AuthorizationError",
    "ClientContext",
    "ClientIdentity",
    "EasyMCPError",
    "MCPServer",
    "PROTOCOL_VERSION",
    "PayloadTooLargeError",
    "ProtocolError",
    "RateLimitError",
    "SSETransport",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "SchemaError",
    "SessionLimitError",
    "SlidingWindowRateLimiter",
    "StdioTransport",
    "StreamableHTTPTransport",
    "ToolDefinition",
    "ToolError",
    "ToolRegistrationError",
    "Transport",
    "ValidationError",
    "__version__",
]
