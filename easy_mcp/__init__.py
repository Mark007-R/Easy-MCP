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
from .security.auth import APIKeyAuth, ClientIdentity
from .security.ratelimit import SlidingWindowRateLimiter
from .server import PROTOCOL_VERSION, MCPServer
from .transport.base import ClientContext, Transport
from .transport.sse import SSETransport

__version__ = "0.1.0"

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
    "SchemaError",
    "SessionLimitError",
    "SlidingWindowRateLimiter",
    "ToolDefinition",
    "ToolError",
    "ToolRegistrationError",
    "Transport",
    "ValidationError",
    "__version__",
]
