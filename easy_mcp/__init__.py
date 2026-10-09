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
from .content import Audio, Image, Message, ResourceContent, ResourceLink
from .decorators import ToolDefinition
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    AuthServerUnavailableError,
    EasyMCPError,
    InsufficientScopeError,
    InvalidTokenError,
    PayloadTooLargeError,
    ProtocolError,
    RateLimitError,
    RegistrationError,
    ResourceNotFoundError,
    SchemaError,
    ServerBusyError,
    SessionLimitError,
    StoreUnavailableError,
    SubscriptionLimitError,
    TokenRequiredError,
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
from .prompts import PromptDefinition
from .protocol import SUPPORTED_PROTOCOL_VERSIONS
from .resources import ResourceDefinition, ResourceTemplateDefinition, safe_path
from .security.auth import APIKeyAuth, ClientIdentity, current_identity
from .security.oauth import Introspection, OAuthResourceServer
from .security.ratelimit import SlidingWindowRateLimiter
from .server import PROTOCOL_VERSION, MCPServer
from .store import MemoryStore, RedisStore, Store
from .transport.base import ClientContext, Transport
from .transport.sse import SSETransport
from .transport.stdio import StdioTransport
from .transport.streamable_http import StreamableHTTPTransport

__all__ = [
    "APIKeyAuth",
    "Audio",
    "AuthServerUnavailableError",
    "AuthenticationError",
    "AuthorizationError",
    "CancelToken",
    "ClientContext",
    "ClientIdentity",
    "EasyMCPError",
    "Image",
    "InsufficientScopeError",
    "Introspection",
    "InvalidTokenError",
    "MCPServer",
    "MemoryStore",
    "Message",
    "OAuthResourceServer",
    "PROTOCOL_VERSION",
    "PayloadTooLargeError",
    "PromptDefinition",
    "ProtocolError",
    "RateLimitError",
    "RedisStore",
    "RegistrationError",
    "RequestInfo",
    "RequestOutcome",
    "ResourceContent",
    "ResourceDefinition",
    "ResourceLink",
    "ResourceNotFoundError",
    "ResourceTemplateDefinition",
    "SSETransport",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "SchemaError",
    "ServerBusyError",
    "SessionLimitError",
    "SlidingWindowRateLimiter",
    "StdioTransport",
    "Store",
    "StoreUnavailableError",
    "StreamableHTTPTransport",
    "SubscriptionLimitError",
    "TokenRequiredError",
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
    "current_identity",
    "current_tool_call",
    "safe_path",
]
