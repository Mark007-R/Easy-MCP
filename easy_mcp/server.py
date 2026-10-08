"""The :class:`MCPServer` class: registration, dispatch, security, lifecycle.

``dispatch`` is transport-independent: it takes one decoded JSON-RPC message
plus a :class:`ClientContext` and returns the response dict (or ``None`` for
notifications).  Transports stay thin; every security decision that is not
transport-specific happens here, so adding a transport cannot silently drop
a protection.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import json
import logging
import threading
import time
import traceback
import uuid
import weakref
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ._version import __version__
from .cancellation import CANCELLED, TIMEOUT, CancelToken, _run_callbacks, cancel_scope
from .decorators import ToolDefinition, ToolRegistry, build_tool
from .exceptions import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    TOOL_TIMEOUT,
    ProtocolError,
    ServerBusyError,
    SessionLimitError,
    ToolError,
    ValidationError,
)
from .logging import audit, configure_logging
from .protocol import (
    DISCOVER_METHOD,
    LATEST_PROTOCOL_VERSION,
    META_SERVER_INFO,
    SUPPORTED_PROTOCOL_VERSIONS,
    check_request_meta,
    is_modern_request,
    negotiate_protocol_version,
)
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

# Cache hints (ttlMs) on stateless results.  What server/discover reports is
# fixed for the life of the process, so clients may keep it for an hour.  The
# tool list is not: tools can be registered at runtime and no listChanged
# notification announces it yet, so a cached copy is stale immediately.
DISCOVER_TTL_MS = 3_600_000
TOOLS_LIST_TTL_MS = 0

# Sync tools that may run at once: the most asyncio's default executor, where
# sync tools used to run, ever allowed.
DEFAULT_MAX_SYNC_WORKERS = 32


def _result_response(msg_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _error_response(msg_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": msg_id, "error": error}


def _protocol_error_response(msg_id: Any, exc: ProtocolError) -> dict[str, Any]:
    return _error_response(msg_id, exc.code, str(exc), exc.data)


def _settle(future: asyncio.Future[Any], ok: bool, value: Any) -> None:
    # Runs on the event loop.  The future is already cancelled when the call
    # was cancelled or timed out; the late outcome then has nobody to go to.
    if future.done():
        return
    if ok:
        future.set_result(value)
    elif isinstance(value, StopIteration):
        # A future refuses StopIteration; the call would never settle.
        future.set_exception(RuntimeError("tool raised StopIteration"))
    else:
        future.set_exception(value)


def _drop_unreported_error(awaitable: Any) -> None:
    """Free the frames of an error the tool raised but nobody will report.

    A tool can fail in the same loop turn as the deadline or a cancel: its
    error is settled on the future, then replaced by the cancellation.  The
    error's traceback holds the worker's frame, which holds the future, which
    holds the error, so the failed tool's frames would wait for a full
    collection.
    """
    if isinstance(awaitable, asyncio.Future) and awaitable.done() and not awaitable.cancelled():
        error = awaitable.exception()  # also marks it retrieved
        # KeyboardInterrupt and SystemExit escape the event loop from the
        # task's own step and are reported by whoever runs the loop.
        if error is not None and not isinstance(error, KeyboardInterrupt | SystemExit):
            error.__traceback__ = None


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
        max_sync_workers: Cap on sync tools running at once.  Each runs in a
            thread of its own, and a cancelled or timed-out call keeps its
            thread until the tool returns, so the cap also bounds threads
            left behind by tools that ignore their cancel token.  A call
            beyond it is refused with ``-32008`` rather than queued.
            ``None`` removes the cap.
        max_sessions: Cap on concurrent sessions, enforced by each HTTP
            endpoint (Streamable HTTP, legacy SSE); stdio has exactly one.
        allowed_origins: Browser origins allowed to call the HTTP endpoints,
            e.g. ``["https://app.example.com"]``; ``"*"`` allows any.  The
            default (``None``) allows loopback origins only.  Requests that
            carry no ``Origin`` header (non-browser clients) are unaffected.
        instructions: Optional usage hints sent to clients at initialize and
            in server/discover.
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
        max_sync_workers: int | None = DEFAULT_MAX_SYNC_WORKERS,
        max_sessions: int = 256,
        allowed_origins: Iterable[str] | None = None,
        instructions: str | None = None,
        json_logs: bool = True,
    ) -> None:
        if default_timeout is not None and default_timeout <= 0:
            raise ValueError("default_timeout must be positive or None")
        if max_request_bytes < 1:
            raise ValueError("max_request_bytes must be >= 1")
        if max_sync_workers is not None and max_sync_workers < 1:
            raise ValueError("max_sync_workers must be >= 1 or None")
        self.host = host
        self.port = port
        self.name = name
        self.version = version
        self.debug = debug
        self.auth = auth
        self.max_request_bytes = max_request_bytes
        self.default_timeout = default_timeout
        self.max_sync_workers = max_sync_workers
        self._sync_slots = (
            threading.BoundedSemaphore(max_sync_workers) if max_sync_workers is not None else None
        )
        # Sync tool and cancel-callback threads still running, and the tool
        # call tasks in flight (touched only on the event loop).
        self._threads: set[threading.Thread] = set()
        self._threads_lock = threading.Lock()
        self._calls: set[asyncio.Task[Any]] = set()
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

        if is_notification and not method.startswith("notifications/"):
            # Only requests invoke methods.  A tools/call without an id would
            # run a tool whose answer nobody can receive, and over HTTP it
            # would skip the header checks that apply to requests.
            return None

        # A request carrying the modern per-request _meta is served statelessly
        # (2026-07-28); anything else keeps the initialize-era behaviour.
        modern = not is_notification and is_modern_request(method, params)
        try:
            if modern:
                check_request_meta(params)
            if method.startswith("notifications/"):
                if modern:
                    # A notification has no id; a request naming one of these
                    # methods is asking for a method that does not exist.
                    raise ProtocolError(f"Method not found: {method}", code=METHOD_NOT_FOUND)
                self._handle_notification(method, params, context)
                return None
            if method == "tools/call":
                response = await self._handle_tools_call(params, context, msg_id, is_notification)
                if modern and response is not None and "result" in response:
                    response["result"] = self._modern_result(response["result"])
                return response
            if modern:
                result: Any = self._dispatch_modern(method, context)
            elif method == "initialize":
                result = self._handle_initialize(params, context)
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = self._handle_tools_list(context)
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
        if modern:
            result = self._modern_result(result)
        return None if is_notification else _result_response(msg_id, result)

    def _capabilities(self) -> dict[str, Any]:
        return {"tools": {"listChanged": False}}

    def _server_info(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version}

    def _handle_initialize(
        self, params: dict[str, Any], context: ClientContext
    ) -> dict[str, Any]:
        version = negotiate_protocol_version(params.get("protocolVersion"))
        # Later requests on this connection or session are spoken in it.
        context.protocol_version = version
        result: dict[str, Any] = {
            "protocolVersion": version,
            "capabilities": self._capabilities(),
            "serverInfo": self._server_info(),
        }
        if self.instructions:
            result["instructions"] = self.instructions
        return result

    def _dispatch_modern(self, method: str, context: ClientContext) -> dict[str, Any]:
        """Serve a stateless request other than ``tools/call``.

        ``initialize``, ``ping`` and ``notifications/initialized`` do not
        exist in this era, so they are unknown methods here.
        """
        if method == DISCOVER_METHOD:
            result: dict[str, Any] = {
                "supportedVersions": list(SUPPORTED_PROTOCOL_VERSIONS),
                "capabilities": self._capabilities(),
                "ttlMs": DISCOVER_TTL_MS,
                "cacheScope": "public",
            }
            if self.instructions:
                result["instructions"] = self.instructions
            return result
        if method == "tools/list":
            result = self._handle_tools_list(context)
            result["ttlMs"] = TOOLS_LIST_TTL_MS
            # With auth configured the list depends on who asks (protected
            # tools are hidden from callers who cannot use them), so a shared
            # cache must not hand one caller's list to another.
            result["cacheScope"] = "private" if self.auth is not None else "public"
            return result
        raise ProtocolError(f"Method not found: {method}", code=METHOD_NOT_FOUND)

    def _modern_result(self, result: dict[str, Any]) -> dict[str, Any]:
        """Stamp a stateless result with its ``resultType`` and our identity."""
        meta = dict(result.get("_meta") or {})
        meta[META_SERVER_INFO] = self._server_info()
        return {**result, "resultType": "complete", "_meta": meta}

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
        # Whether our own caller is being cancelled is told apart from that by
        # the caller's cancel count: when the caller is cancelled while it
        # waits here, asyncio cancels the call's task too, so the task being
        # cancelled says nothing about who asked.
        caller = asyncio.current_task()
        baseline = caller.cancelling() if caller is not None else 0
        task: asyncio.Task[dict[str, Any]] = asyncio.create_task(
            self._execute_tool(params, context)
        )
        self._calls.add(task)
        task.add_done_callback(self._calls.discard)
        if msg_id is not None:
            context.in_flight[msg_id] = task
        try:
            result = await task
        except asyncio.CancelledError:
            if not task.done() or task.cancelled():
                audit("tool_cancelled", client_id=context.client_id, request_id=msg_id)
            if caller is not None and caller.cancelling() > baseline:
                # Our own caller is being cancelled (a timeout around dispatch,
                # a closed connection, shutdown): the cancellation is theirs,
                # and the call must not outlive it.
                task.cancel()
                raise
            # Cancelled via notifications/cancelled: per MCP, the request's
            # response is dropped.
            return None
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

        # The tool (and, for a sync tool, its thread) finds this through
        # current_cancel_token(); a cancel or timeout below triggers it.
        token = CancelToken()
        token._on_error = self._callback_failed(name, context)
        deadline: asyncio.Timeout | None = None
        awaitable: Any = None
        try:
            with cancel_scope(token):
                if definition.is_async:
                    # A task of its own, as asyncio.wait_for gave it on 3.11:
                    # a cancel request the tool leaves on its task (an old
                    # async-timeout, say) must not turn this call's timeout
                    # into a cancellation that answers nobody.
                    awaitable = asyncio.ensure_future(definition.fn(**arguments))
                else:
                    # Sync tools run in a worker thread so they cannot block
                    # the event loop.  Python cannot kill that thread, so a
                    # cancel or timeout reaches the tool through the token.
                    awaitable = self._start_sync_tool(definition, arguments, token, context)
                async with asyncio.timeout(timeout) as deadline:
                    result = await awaitable
        except ServerBusyError:
            context.tool_calls[name] -= 1  # it never ran
            audit("tool_call", tool=name, client_id=context.client_id, status="busy")
            raise
        except TimeoutError as exc:
            if deadline is None or not deadline.expired():
                # The tool raised it (a socket read timing out, say): a tool
                # failure like any other, not the server's deadline.
                return self._tool_failed(name, context, exc, _duration_ms())
            _drop_unreported_error(awaitable)
            self._stop_tool(token, TIMEOUT, name, context)
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
            _drop_unreported_error(awaitable)
            self._stop_tool(token, CANCELLED, name, context)
            raise
        except ToolError as exc:
            # Intentional, safe-to-show tool error raised by the tool author.
            try:
                audit(
                    "tool_call",
                    tool=name,
                    client_id=context.client_id,
                    duration_ms=_duration_ms(),
                    status="tool_error",
                )
                return _tool_failure(str(exc))
            finally:
                exc.__traceback__ = None  # see _tool_failed
        except Exception as exc:
            return self._tool_failed(name, context, exc, _duration_ms())

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

    def _tool_failed(
        self, name: str, context: ClientContext, exc: BaseException, duration_ms: float
    ) -> dict[str, Any]:
        """The CallToolResult for a tool that raised; logs and audits it."""
        error_id = uuid.uuid4().hex[:12]
        try:
            self._logger.error("tool %r failed error_id=%s", name, error_id, exc_info=exc)
            audit(
                "tool_call",
                tool=name,
                client_id=context.client_id,
                duration_ms=duration_ms,
                status="error",
                error_id=error_id,
            )
            if self.debug:
                trace = "".join(traceback.format_exception(exc))
                detail = f"{type(exc).__name__}: {exc}\n{trace}"
                return _tool_failure(f"Tool execution failed (error_id={error_id}): {detail}")
            # Production: opaque message only — no exception text, no trace.
            return _tool_failure(f"Tool execution failed (error_id={error_id})")
        finally:
            # The traceback holds this call's frames, which hold the future
            # that holds the exception: without this, every frame of the
            # failed tool, locals and all, waits for a full collection.
            exc.__traceback__ = None

    def _callback_failed(
        self, name: str, context: ClientContext
    ) -> Callable[[BaseException], None]:
        """How a failing cancel callback of tool *name* is reported."""

        def failed(exc: BaseException) -> None:
            error_id = uuid.uuid4().hex[:12]
            self._logger.warning(
                "cancel callback of tool %r failed error_id=%s", name, error_id, exc_info=exc
            )
            audit(
                "cancel_callback_failed",
                tool=name,
                client_id=context.client_id,
                error_id=error_id,
            )

        return failed

    def _start_sync_tool(
        self,
        definition: ToolDefinition,
        arguments: dict[str, Any],
        token: CancelToken,
        context: ClientContext,
    ) -> asyncio.Future[Any]:
        """Run a sync tool on a thread of its own; returns its future.

        A daemon thread rather than a shared pool: a tool that ignores its
        token keeps its thread after the call is abandoned, and in a pool
        that thread would hold up unrelated calls queued behind it (and, at
        exit, the interpreter).  ``max_sync_workers`` bounds them instead,
        and :meth:`wait_for_tool_threads` gives them time at shutdown.

        Raises:
            ServerBusyError: ``max_sync_workers`` tools are already running.
        """
        slots = self._sync_slots
        if slots is not None and not slots.acquire(blocking=False):
            raise ServerBusyError(
                f"Server busy: all {self.max_sync_workers} tool workers are in use; retry shortly"
            )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        # Carries the cancel token (and any other context) into the thread,
        # as asyncio.to_thread does.
        run_in_context = contextvars.copy_context().run
        name = definition.name

        def work() -> None:
            try:
                outcome: tuple[bool, Any] = (True, run_in_context(definition.fn, **arguments))
            except BaseException as exc:
                outcome = (False, exc)
            finally:
                # Free the worker before the answer can reach the client, or
                # the client's next call could find it still taken.
                if slots is not None:
                    slots.release()
            status = "ok" if outcome[0] else "error"
            token_ref = weakref.ref(token)

            def finished_late() -> None:
                # The client was told the call was cancelled or timed out; a
                # tool that acts (a write, say) may have done so anyway, and
                # this is the record of it.
                fired = token_ref()
                audit(
                    "tool_finished_after_cancel",
                    tool=name,
                    client_id=context.client_id,
                    reason=fired.reason if fired is not None else None,
                    status=status,
                )

            # Runs at once if the token has fired, or when it fires: the
            # deadline, or a cancel, can land after the answer is ready but
            # before the call takes it.  It stays registered for the token's
            # life; it holds neither the result nor the token itself, so a
            # finished call keeps nothing alive through it.
            try:
                token.on_cancel(finished_late)
            finally:
                with contextlib.suppress(RuntimeError):  # the loop has closed
                    loop.call_soon_threadsafe(_settle, future, *outcome)
                # An exception's traceback holds this frame, and this frame
                # the exception: break the cycle, or a late error keeps every
                # frame of the failed tool alive until a full collection.
                del outcome

        try:
            self._spawn(f"easy-mcp-tool:{name}", work)
        except BaseException:
            if slots is not None:
                slots.release()
            raise
        return future

    def _stop_tool(
        self, token: CancelToken, reason: str, name: str, context: ClientContext
    ) -> None:
        """Trigger *token* and run its callbacks off the event loop.

        The flag is set at once, so a tool polling ``token.cancelled`` sees
        it immediately; callbacks may block (a MySQL ``KILL QUERY`` opens a
        connection), so they get a thread of their own.  Never raises: it
        runs while a cancellation or timeout is propagating.
        """
        callbacks = token._trigger(reason)
        if not callbacks:
            return
        failed = token._on_error or self._callback_failed(name, context)

        def run() -> None:
            # The token stays alive while its callbacks run (the call may be
            # long gone): they can read its reason, or find it through
            # current_cancel_token().
            with cancel_scope(token):
                _run_callbacks(callbacks, failed)

        try:
            self._spawn(f"easy-mcp-cancel:{name}", run)
        except Exception as exc:
            # No thread to run them on.  Running them here would block the
            # event loop, so they are dropped, loudly.
            failed(exc)

    def _spawn(self, name: str, target: Callable[[], None]) -> None:
        """Start *target* on a daemon thread that shutdown waits for."""

        def run() -> None:
            try:
                target()
            finally:
                with self._threads_lock:
                    self._threads.discard(thread)

        thread = threading.Thread(target=run, name=name, daemon=True)
        with self._threads_lock:
            self._threads.add(thread)
        try:
            thread.start()
        except BaseException:
            with self._threads_lock:
                self._threads.discard(thread)
            raise

    async def wait_for_tool_threads(self, timeout: float) -> int:
        """Give sync tool threads and cancel callbacks up to *timeout* seconds to finish.

        They are daemon threads, so a process that exits while they run
        stops them where they are: a cancel callback that has not sent its
        ``KILL QUERY`` yet never sends it.  The transports call this as they
        shut down; call it yourself before exiting when you drive
        :meth:`dispatch` directly.  Calls cancelled just before (a session
        ended at shutdown) are waited for until they have started their
        callbacks, and those are waited for too.  It polls on the event
        loop, so it needs no free worker thread.

        Returns:
            How many are still running when it gives up (logged as well).
        """
        deadline = time.monotonic() + max(timeout, 0.0)
        while True:
            left = self._still_running()
            if not left or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.02)
        if left:
            self._logger.warning(
                "%d tool call(s) or thread(s) still running after %gs; stopping without them: %s",
                len(left),
                timeout,
                ", ".join(sorted(left)),
            )
        return len(left)

    def _still_running(self) -> list[str]:
        # A call being cancelled has yet to trigger its token and start its
        # callbacks' thread; _stop_tool does both in one step on this loop.
        unwinding = [
            "cancelled call" for task in self._calls if not task.done() and task.cancelling()
        ]
        with self._threads_lock:
            threads = [t.name for t in self._threads if t.ident is None or t.is_alive()]
        return unwinding + threads

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
