"""The :class:`MCPServer` class: registration, dispatch, security, lifecycle.

``dispatch`` is transport-independent: it takes one decoded JSON-RPC message
plus a :class:`ClientContext` and returns the response dict (or ``None`` for
notifications).  Transports stay thin; every security decision that is not
transport-specific happens here, so adding a transport cannot silently drop
a protection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ._version import __version__
from .decorators import ToolDefinition, ToolRegistry, build_tool
from .exceptions import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    TOOL_TIMEOUT,
    ProtocolError,
    SessionLimitError,
    ToolError,
    ValidationError,
)
from .logging import audit, configure_logging
from .protocol import LATEST_PROTOCOL_VERSION, negotiate_protocol_version
from .schema import build_param_models, dump_model, validate_arguments, validate_result
from .security.auth import APIKeyAuth, ClientIdentity, authorize, visible
from .security.ratelimit import SlidingWindowRateLimiter
from .transport._http import BaseHTTPTransport, normalize_origins
from .transport.base import ClientContext, Transport
from .transport.sse import SSETransport
from .transport.stdio import StdioTransport
from .transport.streamable_http import StreamableHTTPTransport

# The newest revision spoken; see protocol.SUPPORTED_PROTOCOL_VERSIONS for all.
PROTOCOL_VERSION = LATEST_PROTOCOL_VERSION


def _result_response(msg_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _error_response(msg_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": msg_id, "error": error}


def _protocol_error_response(msg_id: Any, exc: ProtocolError) -> dict[str, Any]:
    return _error_response(msg_id, exc.code, str(exc), exc.data)


def _tool_failure(text: str) -> dict[str, Any]:
    """An MCP CallToolResult marking a tool-level (not protocol-level) error."""
    return {"content": [{"type": "text", "text": text}], "isError": True}


def _serialize_result(result: Any) -> str:
    if isinstance(result, str):
        return result
    dump = getattr(result, "model_dump", None)
    if callable(dump):
        # A Pydantic model that no output schema covers still serializes as
        # its data rather than as its repr.
        try:
            result = dump(mode="json")
        except Exception:  # not a Pydantic model after all
            pass
    # sort_keys keeps output byte-identical for identical inputs, which
    # matters for reproducible agent runs and response caching.
    return json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)


def _render_result(definition: ToolDefinition, result: Any) -> tuple[str, Any | None]:
    """The text block and the ``structuredContent`` for a tool's return value.

    ``None`` for the second item means the tool advertises no output schema and
    so sends no structured content.

    Raises:
        ValidationError: The value does not match the schema the tool published.
    """
    if definition.output_model is not None:
        # The model both checks and serializes, so a tool may return an
        # instance or any dict the model accepts.
        structured = dump_model(definition.output_model, result)
        return _serialize_result(structured), (
            structured if definition.output_schema is not None else None
        )

    text = _serialize_result(result)
    if definition.output_schema is None:
        return text, None
    try:
        # Check and send exactly what the client will parse: a value JSON can
        # only carry loosely -- a datetime, say -- is judged in its serialized
        # form, not its richer Python one.
        structured = json.loads(text)
    except ValueError:
        structured = result  # not JSON at all; the check below says so
    return text, validate_result(structured, definition.output_schema)


class MCPServer:
    """A secure-by-default MCP server exposing Python functions as tools.

    Example::

        from easy_mcp import MCPServer

        server = MCPServer(port=8000)

        @server.tool
        def add(a: int, b: int) -> int:
            \"\"\"Add two numbers.\"\"\"
            return a + b

        server.run()

    Args:
        port: TCP port to bind.
        host: Interface to bind. Defaults to loopback — exposing the server
            beyond localhost is an explicit decision.
        name: Server name reported during the MCP handshake.
        version: Server version reported during the MCP handshake; defaults
            to the easy_mcp package version.
        debug: When True, clients receive full tracebacks and uvicorn logs
            verbosely. Never enable in production.
        auth: Optional :class:`APIKeyAuth`. Without it, only public tools
            (no ``requires_auth``/``scopes``) are reachable.
        rate_limit_per_minute: Per-client request budget; ``None`` disables.
        max_request_bytes: Hard cap on request body size.
        default_timeout: Tool execution timeout in seconds unless a tool
            overrides it; ``None`` disables.
        max_sessions: Cap on concurrent sessions, enforced by each HTTP
            endpoint (Streamable HTTP, legacy SSE); stdio has exactly one.
        allowed_origins: Browser origins allowed to call the HTTP endpoints,
            e.g. ``["https://app.example.com"]``; ``"*"`` allows any.  The
            default (``None``) allows loopback origins only.  Requests that
            carry no ``Origin`` header (non-browser clients) are unaffected.
        instructions: Optional usage hints sent to clients at initialize.
        json_logs: Emit structured JSON logs (recommended) or plain text.
    """

    def __init__(
        self,
        port: int = 8000,
        host: str = "127.0.0.1",
        *,
        name: str = "easy-mcp",
        version: str = __version__,
        debug: bool = False,
        auth: APIKeyAuth | None = None,
        rate_limit_per_minute: int | None = 120,
        max_request_bytes: int = 1_048_576,
        default_timeout: float | None = 30.0,
        max_sessions: int = 256,
        allowed_origins: Iterable[str] | None = None,
        instructions: str | None = None,
        json_logs: bool = True,
    ) -> None:
        if default_timeout is not None and default_timeout <= 0:
            raise ValueError("default_timeout must be positive or None")
        if max_request_bytes < 1:
            raise ValueError("max_request_bytes must be >= 1")
        self.host = host
        self.port = port
        self.name = name
        self.version = version
        self.debug = debug
        self.auth = auth
        self.max_request_bytes = max_request_bytes
        self.default_timeout = default_timeout
        self.max_sessions = max_sessions
        self.allowed_origins = (
            normalize_origins(allowed_origins) if allowed_origins is not None else None
        )
        self.instructions = instructions
        self._registry = ToolRegistry()
        self._limiter = (
            SlidingWindowRateLimiter(rate_limit_per_minute)
            if rate_limit_per_minute
            else None
        )
        self._transport: Transport | None = None
        self._logger: logging.Logger = configure_logging(debug=debug, json_logs=json_logs)

    # ------------------------------------------------------------ registration

    def tool(
        self,
        fn: Callable[..., Any] | None = None,
        /,
        *,
        name: str | None = None,
        description: str | None = None,
        output_schema: dict[str, Any] | None = None,
        requires_auth: bool = False,
        scopes: Iterable[str] = (),
        tags: Iterable[str] = (),
        category: str | None = None,
        examples: Iterable[Mapping[str, Any]] = (),
        timeout: float | None = None,
        max_calls_per_session: int | None = None,
    ) -> Callable[..., Any]:
        """Register a function as an MCP tool.

        Works bare (``@server.tool``) or with options
        (``@server.tool(name="sum", scopes=("math",))``).  The wrapped
        function is returned unchanged, so it stays directly callable.
        """

        def decorate(target: Callable[..., Any]) -> Callable[..., Any]:
            self.register_tool(
                target,
                name=name,
                description=description,
                output_schema=output_schema,
                requires_auth=requires_auth,
                scopes=scopes,
                tags=tags,
                category=category,
                examples=examples,
                timeout=timeout,
                max_calls_per_session=max_calls_per_session,
            )
            return target

        if fn is not None:
            return decorate(fn)
        return decorate

    def register_tool(self, fn: Callable[..., Any], **options: Any) -> ToolDefinition:
        """Register a tool dynamically at runtime (same options as ``tool``)."""
        definition = build_tool(fn, **options)
        self._registry.register(definition)
        self._logger.debug("registered tool %r", definition.name)
        return definition

    def unregister_tool(self, name: str) -> ToolDefinition:
        """Remove a tool at runtime; returns its definition."""
        removed = self._registry.unregister(name)
        self._logger.debug("unregistered tool %r", name)
        return removed

    @property
    def tools(self) -> list[ToolDefinition]:
        """All registered tools, sorted by name."""
        return self._registry.list()

    # ------------------------------------------------------------------- auth

    def authenticate_key(self, api_key: str | None) -> ClientIdentity | None:
        """Resolve an API key to an identity via the configured auth backend.

        Returns ``None`` when no auth is configured or no key was presented.

        Raises:
            AuthenticationError: If a key was presented but is invalid.
        """
        if self.auth is None:
            return None
        return self.auth.authenticate(api_key)

    def check_rate_limit(self, client_id: str) -> None:
        """Consume one unit of *client_id*'s request budget.

        ``dispatch`` calls this for every message; transports call it for
        work that happens before any message exists (opening an SSE session)
        so that path cannot sidestep the budget.

        Raises:
            RateLimitError: If the client is over budget.  A no-op when rate
                limiting is disabled.
        """
        if self._limiter is not None:
            self._limiter.check(client_id)

    # --------------------------------------------------------------- dispatch

    async def dispatch(self, message: Any, context: ClientContext) -> dict[str, Any] | None:
        """Handle one JSON-RPC message; returns the response or ``None``.

        This is the single entry point every transport funnels through, and
        the place rate limiting and method routing are enforced.  It never
        raises: malformed input and internal failures both come back as
        JSON-RPC error responses (sanitized outside debug mode).
        """
        if not isinstance(message, dict):
            return _error_response(None, INVALID_REQUEST, "Invalid request: expected a JSON object")
        msg_id = message.get("id")
        is_notification = "id" not in message
        if message.get("jsonrpc") != "2.0":
            if is_notification:
                return None
            return _error_response(
                msg_id, INVALID_REQUEST, "Invalid request: jsonrpc must be '2.0'"
            )
        method = message.get("method")
        if not isinstance(method, str):
            if is_notification:
                return None
            return _error_response(msg_id, INVALID_REQUEST, "Invalid request: missing method")
        params = message.get("params")
        if params is None:
            params = {}
        if not isinstance(params, dict):
            if is_notification:
                return None
            return _error_response(msg_id, INVALID_PARAMS, "params must be an object")

        # Rate limiting applies to every method, so discovery endpoints cannot
        # be used to bypass the budget.
        try:
            self.check_rate_limit(context.client_id)
        except ProtocolError as exc:
            audit("rate_limited", client_id=context.client_id, method=method)
            return None if is_notification else _protocol_error_response(msg_id, exc)

        try:
            if method == "initialize":
                result: Any = self._handle_initialize(params)
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = self._handle_tools_list(context)
            elif method == "tools/call":
                return await self._handle_tools_call(params, context, msg_id, is_notification)
            elif method.startswith("notifications/"):
                self._handle_notification(method, params, context)
                return None
            else:
                if is_notification:
                    return None
                return _error_response(msg_id, METHOD_NOT_FOUND, f"Method not found: {method}")
        except ProtocolError as exc:
            return None if is_notification else _protocol_error_response(msg_id, exc)
        except Exception:
            # Sanitize: clients get an opaque error_id; the log gets the trace.
            error_id = uuid.uuid4().hex[:12]
            self._logger.error("internal error error_id=%s", error_id, exc_info=True)
            if is_notification:
                return None
            return _error_response(
                msg_id, INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
            )
        return None if is_notification else _result_response(msg_id, result)

    def _handle_initialize(self, params: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {
            "protocolVersion": negotiate_protocol_version(params.get("protocolVersion")),
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": self.name, "version": self.version},
        }
        if self.instructions:
            result["instructions"] = self.instructions
        return result

    def _handle_tools_list(self, context: ClientContext) -> dict[str, Any]:
        # Protected tools are omitted for callers who could not invoke them.
        return {
            "tools": [
                definition.to_mcp()
                for definition in self._registry.list()
                if visible(context.identity, definition)
            ]
        }

    def _handle_notification(
        self, method: str, params: dict[str, Any], context: ClientContext
    ) -> None:
        if method == "notifications/initialized":
            self._logger.debug("client initialized (session %s)", context.session_id)
        elif method == "notifications/cancelled":
            request_id = params.get("requestId")
            task = context.in_flight.get(request_id)
            if task is not None:
                task.cancel()
        # Unknown notifications are ignored per JSON-RPC semantics.

    async def _handle_tools_call(
        self,
        params: dict[str, Any],
        context: ClientContext,
        msg_id: Any,
        is_notification: bool,
    ) -> dict[str, Any] | None:
        # The call runs as its own task so notifications/cancelled can abort it.
        task: asyncio.Task[dict[str, Any]] = asyncio.create_task(
            self._execute_tool(params, context)
        )
        if msg_id is not None:
            context.in_flight[msg_id] = task
        try:
            result = await task
        except asyncio.CancelledError:
            if task.cancelled():
                # Cancelled via notifications/cancelled: per MCP, the request's
                # response is dropped.
                audit("tool_cancelled", client_id=context.client_id, request_id=msg_id)
                return None
            task.cancel()  # our own caller is being cancelled; don't orphan it
            raise
        except ProtocolError as exc:
            return None if is_notification else _protocol_error_response(msg_id, exc)
        except Exception:
            error_id = uuid.uuid4().hex[:12]
            self._logger.error("internal error error_id=%s", error_id, exc_info=True)
            if is_notification:
                return None
            return _error_response(
                msg_id, INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
            )
        finally:
            if msg_id is not None:
                context.in_flight.pop(msg_id, None)
        return None if is_notification else _result_response(msg_id, result)

    async def _execute_tool(
        self, params: dict[str, Any], context: ClientContext
    ) -> dict[str, Any]:
        name = params.get("name")
        if not isinstance(name, str):
            raise ProtocolError("tools/call requires a string 'name'", code=INVALID_PARAMS)
        definition = self._registry.get(name)
        # Report protected tools as unknown to unauthorized callers, so their
        # existence is not enumerable.
        if definition is None or not visible(context.identity, definition):
            raise ProtocolError(f"Unknown tool: {name}", code=INVALID_PARAMS)

        try:
            authorize(context.identity, definition)
            call_count = context.tool_calls.get(name, 0)
            if (
                definition.max_calls_per_session is not None
                and call_count >= definition.max_calls_per_session
            ):
                raise SessionLimitError(
                    f"Session limit reached for tool '{name}' "
                    f"({definition.max_calls_per_session} calls)"
                )
            arguments = params.get("arguments")
            if arguments is None:
                arguments = {}
            if not isinstance(arguments, dict):
                raise ProtocolError("'arguments' must be an object", code=INVALID_PARAMS)
            arguments = validate_arguments(arguments, definition.arguments_schema)
            arguments = build_param_models(definition.param_models, arguments)
        except ProtocolError as exc:
            audit(
                "tool_denied",
                tool=name,
                client_id=context.client_id,
                reason=type(exc).__name__,
            )
            raise

        context.tool_calls[name] = call_count + 1
        timeout = definition.timeout if definition.timeout is not None else self.default_timeout
        started = time.perf_counter()

        def _duration_ms() -> float:
            return round((time.perf_counter() - started) * 1000, 2)

        try:
            if definition.is_async:
                awaitable: Any = definition.fn(**arguments)
            else:
                # Sync tools run in a worker thread so they cannot block the
                # event loop.  NOTE: a timeout/cancel abandons the thread —
                # Python cannot force-kill it (documented in SECURITY.md).
                awaitable = asyncio.to_thread(definition.fn, **arguments)
            result = await asyncio.wait_for(awaitable, timeout)
        except TimeoutError:
            audit(
                "tool_call",
                tool=name,
                client_id=context.client_id,
                duration_ms=_duration_ms(),
                status="timeout",
            )
            raise ProtocolError(
                f"Tool '{name}' timed out after {timeout:g}s", code=TOOL_TIMEOUT
            ) from None
        except asyncio.CancelledError:
            raise
        except ToolError as exc:
            # Intentional, safe-to-show tool error raised by the tool author.
            audit(
                "tool_call",
                tool=name,
                client_id=context.client_id,
                duration_ms=_duration_ms(),
                status="tool_error",
            )
            return _tool_failure(str(exc))
        except Exception as exc:
            error_id = uuid.uuid4().hex[:12]
            self._logger.error("tool %r failed error_id=%s", name, error_id, exc_info=True)
            audit(
                "tool_call",
                tool=name,
                client_id=context.client_id,
                duration_ms=_duration_ms(),
                status="error",
                error_id=error_id,
            )
            if self.debug:
                detail = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
                return _tool_failure(f"Tool execution failed (error_id={error_id}): {detail}")
            # Production: opaque message only — no exception text, no trace.
            return _tool_failure(f"Tool execution failed (error_id={error_id})")

        try:
            text, structured = _render_result(definition, result)
        except ValidationError as exc:
            error_id = uuid.uuid4().hex[:12]
            self._logger.error(
                "tool %r broke its own output schema error_id=%s: %s",
                name,
                error_id,
                "; ".join(exc.errors),
            )
            audit(
                "tool_call",
                tool=name,
                client_id=context.client_id,
                duration_ms=_duration_ms(),
                status="output_schema_error",
                error_id=error_id,
            )
            # The mismatch describes the server's own data, so it stays in the
            # log unless the operator asked for detail.
            detail = f": {'; '.join(exc.errors)}" if self.debug else ""
            return _tool_failure(
                f"Tool result did not match its output schema (error_id={error_id}){detail}"
            )

        payload: dict[str, Any] = {
            "content": [{"type": "text", "text": text}],
            "isError": False,
        }
        if structured is not None:
            payload["structuredContent"] = structured

        audit(
            "tool_call",
            tool=name,
            client_id=context.client_id,
            duration_ms=_duration_ms(),
            status="ok",
        )
        return payload

    # -------------------------------------------------------------- lifecycle

    def build_app(self) -> Any:
        """Return the ASGI app (for tests, mounting, or ``uvicorn --factory``).

        It serves Streamable HTTP at ``/mcp`` plus the legacy SSE endpoints.
        """
        if not isinstance(self._transport, BaseHTTPTransport):
            self._transport = StreamableHTTPTransport(self)
        return self._transport.build_app()

    def run(self, transport: Transport | str | None = None) -> None:
        """Start the server (blocking).  Ctrl-C shuts down gracefully.

        Args:
            transport: ``"http"`` (default; alias ``"streamable-http"``)
                serves Streamable HTTP at ``/mcp`` plus the legacy SSE
                endpoints on ``host:port``; ``"sse"`` serves only the legacy
                HTTP + SSE transport; ``"stdio"`` serves the parent process
                over stdin/stdout.  A :class:`Transport` instance can be
                passed for custom configuration.
        """
        self._transport = self._resolve_transport(transport)
        self._warn_if_misconfigured()
        self._logger.info(
            "starting %s v%s via %s",
            self.name,
            self.version,
            self._transport.describe(),
            extra={
                "event": {
                    "type": "startup",
                    "transport": self._transport.describe(),
                    "tools": [definition.name for definition in self.tools],
                    "auth": self.auth is not None,
                    "rate_limit": self._limiter is not None,
                    "debug": self.debug,
                }
            },
        )
        try:
            self._transport.run()
        finally:
            self._logger.info("server stopped")

    def stop(self) -> None:
        """Request a graceful shutdown of a running server."""
        if self._transport is not None:
            self._transport.stop()

    def _resolve_transport(self, transport: Transport | str | None) -> Transport:
        if isinstance(transport, Transport):
            return transport
        if transport is None or transport in ("http", "streamable-http"):
            return StreamableHTTPTransport(self)
        if transport == "sse":
            return SSETransport(self)
        if transport == "stdio":
            return StdioTransport(self)
        raise ValueError(
            f"unknown transport {transport!r}: expected 'http', 'sse', 'stdio', "
            "or a Transport instance"
        )

    def _warn_if_misconfigured(self) -> None:
        if self.auth is None:
            protected = [d.name for d in self.tools if d.requires_auth]
            if protected:
                self._logger.warning(
                    "tools %s require authentication but no auth is configured; "
                    "they will be unreachable",
                    protected,
                )
            if self.host not in ("127.0.0.1", "localhost", "::1") and not isinstance(
                self._transport, StdioTransport
            ):
                self._logger.warning(
                    "binding %s without authentication exposes all public tools "
                    "to the network; configure APIKeyAuth",
                    self.host,
                )
        if self.debug:
            self._logger.warning("debug mode is ON: clients will receive tracebacks")
