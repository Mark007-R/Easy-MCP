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
import dataclasses
import functools
import hashlib
import json
import logging
import threading
import time
import traceback
import uuid
import weakref
from collections import OrderedDict
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Hashable,
    Iterable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from typing import Any, TypeVar
from urllib.parse import urlsplit

from ._version import __version__
from .cancellation import CANCELLED, TIMEOUT, CancelToken, _run_callbacks, cancel_scope
from .completion import CompletionSource, collect, empty, filter_static, shape
from .content import NotFound, to_prompt_messages, to_resource_contents
from .decorators import ToolDefinition, ToolRegistry, build_tool
from .exceptions import (
    AUTHENTICATION_REQUIRED,
    FORBIDDEN,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    RESOURCE_NOT_FOUND_LEGACY,
    TOOL_TIMEOUT,
    AuthenticationError,
    AuthorizationError,
    InsufficientScopeError,
    ProtocolError,
    RegistrationError,
    ResourceNotFoundError,
    ServerBusyError,
    SessionLimitError,
    StoreUnavailableError,
    SubscriptionLimitError,
    TokenRequiredError,
    ToolError,
    ToolRegistrationError,
    ValidationError,
)
from .logging import audit, configure_logging
from .middleware import (
    ObservePolicy,
    RequestInfo,
    RequestMiddleware,
    RequestMiddlewareT,
    RequestOutcome,
    RequestPolicy,
    ToolCall,
    ToolMiddleware,
    ToolMiddlewareT,
    ToolOutcome,
    ToolPolicy,
    TransportInfo,
    _copy,
    _task_cancels,
    _tool_call_scope,
    _type_name,
    check_middleware,
    describe,
    run_chain,
)
from .pagination import paginate
from .prompts import PromptDefinition, PromptRegistry, build_prompt
from .protocol import (
    DISCOVER_METHOD,
    LATEST_PROTOCOL_VERSION,
    LIST_KINDS,
    LISTEN_METHOD,
    META_PROTOCOL_VERSION,
    META_SERVER_INFO,
    META_SUBSCRIPTION_ID,
    SUPPORTED_PROTOCOL_VERSIONS,
    check_request_meta,
    era_error_code,
    is_modern_request,
    is_reserved_error_code,
    negotiate_protocol_version,
)
from .resources import (
    ResourceDefinition,
    ResourceRegistry,
    ResourceTemplateDefinition,
    build_resource,
)
from .schema import build_param_models, dump_model, validate_arguments, validate_result
from .security.auth import (
    APIKeyAuth,
    ClientIdentity,
    Guarded,
    _identity_scope,
    authorize,
    is_scope_token,
    visible,
)
from .security.oauth import LEEWAY_SECONDS, OAuthResourceServer
from .security.ratelimit import SlidingWindowRateLimiter
from .store.base import AsyncRateLimiter, Reservation, Store, StoreHandle, session_ref
from .store.memory import MemoryStore
from .subscriptions import (
    MAX_RESOURCE_SUBSCRIPTIONS,
    ChangeNotifier,
    Push,
    _Sink,
    ack_message,
    parse_filter,
    valid_subscription_id,
)
from .transport._http import THREAD_SHUTDOWN_GRACE, BaseHTTPTransport, normalize_origins
from .transport._sessions import SessionManager
from .transport.base import ClientContext, Transport
from .transport.sse import SSETransport
from .transport.stdio import StdioTransport
from .transport.streamable_http import StreamableHTTPTransport

# The newest revision spoken; see protocol.SUPPORTED_PROTOCOL_VERSIONS for all.
PROTOCOL_VERSION = LATEST_PROTOCOL_VERSION

_F = TypeVar("_F", bound=Callable[..., Any])

# Cache hints (ttlMs) on stateless results.  What server/discover reports
# only ever grows (a capability, once advertised, stays), so clients may keep
# it for an hour.  The tool list is not fixed: tools can be registered at
# runtime.  A list_changed notification tells the clients holding a
# subscriptions/listen stream, but only them, so for every other client a
# cached copy is stale immediately.
DISCOVER_TTL_MS = 3_600_000
TOOLS_LIST_TTL_MS = 0
# The same holds for the resources, templates and prompts lists.
RESOURCES_LIST_TTL_MS = RESOURCE_TEMPLATES_LIST_TTL_MS = PROMPTS_LIST_TTL_MS = 0

# The paginated list methods: (the kind their cursor names, the result field).
_LISTS: dict[str, tuple[str, str]] = {
    "resources/list": ("resources", "resources"),
    "resources/templates/list": ("templates", "resourceTemplates"),
    "prompts/list": ("prompts", "prompts"),
}

# A resource URI is echoed in data.uri only up to this length, and audited
# (logs, never responses) up to the second, so a hostile one-megabyte URI
# doubles neither a response nor a log line.
_ECHO_URI_MAX = 2048
_AUDIT_URI_MAX = 512
# Messages name a URI by at most this many characters.
_LABEL_MAX = 256

# Digests of lists as one visibility class sees them, kept for this many
# (kind, class) pairs: enough for every scope set a deployment uses.
_DIGESTS_MAX = 1024

# Sync tools that may run at once: the most asyncio's default executor, where
# sync tools used to run, ever allowed.
DEFAULT_MAX_SYNC_WORKERS = 32

# OAuth principals remembered for the once-per-principal principal_seen audit.
_PRINCIPALS_SEEN_MAX = 4096

# How long /healthz trusts a ping of a shared store, so probes cannot hammer it.
_STORE_PING_CACHE_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class _Method:
    """One JSON-RPC method the server implements, and how dispatch serves it."""

    legacy: bool  # served in the initialize era
    modern: bool  # served statelessly (2026-07-28)
    notification: bool = False
    # Middleware may watch it but not refuse it.
    observe_only: bool = False
    # Runs as a task of its own that notifications/cancelled can stop.
    cancellable: bool = True
    # Served only while the server advertises this capability.
    capability: str | None = None
    # Served only on a channel that can carry server-initiated messages
    # (ClientContext.push).
    needs_push: bool = False
    # Served only for a session something can later be delivered to: one
    # with a channel (ClientContext.push), or one kept in the store, whose
    # GET /mcp stream delivers.
    needs_session: bool = False


# Every method dispatch serves.  Anything else is answered -32601 (or, for a
# notification, ignored) before any middleware runs.
_METHODS: dict[str, _Method] = {
    # The initialize request must not be cancelled (2025-11-25 lifecycle).
    "initialize": _Method(legacy=True, modern=False, cancellable=False),
    "ping": _Method(legacy=True, modern=False),
    "tools/list": _Method(legacy=True, modern=True, capability="tools"),
    "tools/call": _Method(legacy=True, modern=True, capability="tools"),
    "resources/list": _Method(legacy=True, modern=True, capability="resources"),
    "resources/templates/list": _Method(legacy=True, modern=True, capability="resources"),
    "resources/read": _Method(legacy=True, modern=True, capability="resources"),
    # Removed in 2026-07-28, where subscriptions/listen replaces them (they
    # would keep per-connection state).
    "resources/subscribe": _Method(
        legacy=True, modern=False, capability="resources", needs_session=True
    ),
    "resources/unsubscribe": _Method(
        legacy=True, modern=False, capability="resources", needs_session=True
    ),
    "prompts/list": _Method(legacy=True, modern=True, capability="prompts"),
    "prompts/get": _Method(legacy=True, modern=True, capability="prompts"),
    "completion/complete": _Method(legacy=True, modern=True, capability="completions"),
    # Refusing server/discover would make a dual-era client take this server
    # for a legacy one.
    DISCOVER_METHOD: _Method(legacy=False, modern=True, observe_only=True),
    # Its response is the stream; the client ends it by cancelling it.
    LISTEN_METHOD: _Method(legacy=False, modern=True, needs_push=True),
    # Notifications carry no era of their own and have no response to refuse.
    "notifications/initialized": _Method(
        legacy=True, modern=False, notification=True, observe_only=True, cancellable=False
    ),
    "notifications/cancelled": _Method(
        legacy=True, modern=False, notification=True, observe_only=True, cancellable=False
    ),
}


@dataclass(frozen=True, slots=True)
class _Target:
    """What a call of user code runs, for thread names and audit events."""

    kind: str  # "tool", "resource", "prompt" or "completion"
    label: str  # the tool or prompt name, the URI, or "<prompt or template>.<argument>"
    field: str  # the audit field the label goes in: "tool", "uri" or "prompt"


class _DeadlineExceeded(Exception):
    """The server's deadline for one call of user code passed; its token fired."""


class _UserCancelled(Exception):
    """User code raised ``CancelledError`` although nothing cancelled its call."""

    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error: BaseException | None = error


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


def _gather(produced: Any) -> tuple[list[str], int | None]:
    """What a completer returned, read as :func:`~easy_mcp.completion.collect` reads it."""
    if isinstance(produced, str | bytes) or not isinstance(produced, Iterable):
        raise TypeError(f"a completer result of type {type(produced).__name__}")
    return collect(produced, sized=isinstance(produced, Sequence))


def _completer_call(
    source: CompletionSource, value: str, known: Mapping[str, str]
) -> tuple[Callable[[], Any], bool]:
    """A function of no arguments running *source*'s completer, and whether it is async.

    The completer's result is read inside it: on its worker thread for a
    sync completer, so even a slow generator never holds the event loop.
    """
    fn = source.fn
    assert fn is not None
    arguments = dict(known)
    if source.is_async:

        async def run_async() -> tuple[list[str], int | None]:
            return _gather(await fn(value, arguments))

        return run_async, True

    def run() -> tuple[list[str], int | None]:
        return _gather(fn(value, arguments))

    return run, False


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
    """A secure-by-default MCP server exposing Python functions as tools,
    resources and prompts.

    Example::

        from easy_mcp import MCPServer

        server = MCPServer(port=8000)

        @server.tool
        def add(a: int, b: int) -> int:
            \"\"\"Add two numbers.\"\"\"
            return a + b

        @server.resource("config://app")
        def app_config() -> dict[str, str]:
            \"\"\"The application's configuration.\"\"\"
            return {"region": "eu-west-1"}

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
        auth: Optional :class:`APIKeyAuth`. Without it (or ``oauth``), only
            public tools (no ``requires_auth``/``scopes``) are reachable.
        oauth: Optional :class:`~easy_mcp.OAuthResourceServer`: accept OAuth
            2.1 access tokens on Streamable HTTP and SSE, as the MCP
            authorization spec describes.  Every HTTP request then needs a
            credential (a token, or an API key when ``auth`` is set too);
            stdio ignores it.  A tool's ``scopes`` map onto the token's.
        rate_limit_per_minute: Per-client request budget; ``None`` disables.
            With ``oauth``, failed token checks from one address are limited
            to the same budget.
        max_request_bytes: Hard cap on request body size.
        default_timeout: Tool execution timeout in seconds unless a tool
            overrides it; ``None`` disables.  Resources, prompts and
            completers get it too.
        max_sync_workers: Cap on sync tools running at once.  Each runs in a
            thread of its own, and a cancelled or timed-out call keeps its
            thread until the tool returns, so the cap also bounds threads
            left behind by tools that ignore their cancel token.  A call
            beyond it is refused with ``-32008`` rather than queued.
            ``None`` removes the cap.  Sync resources, prompts and
            completers share the same workers.
        max_sessions: Cap on concurrent handshake-era sessions, counted
            separately for Streamable HTTP and legacy SSE; stdio has exactly
            one.  With a shared ``store`` it counts the sessions of every
            worker together.  It also caps the ``subscriptions/listen``
            streams open in this process, over every transport together (a
            client may hold 8 of them); a stream over the cap is refused
            with ``-32007`` (HTTP ``503``).
        allowed_origins: Browser origins allowed to call the HTTP endpoints,
            e.g. ``["https://app.example.com"]``; ``"*"`` allows any.  The
            default (``None``) allows loopback origins only.  Requests that
            carry no ``Origin`` header (non-browser clients) are unaffected.
        instructions: Optional usage hints sent to clients at initialize and
            in server/discover.
        store: Where state that outlives one request is kept: handshake-era
            sessions, ``max_calls_per_session`` counts and rate-limit
            windows.  The default :class:`~easy_mcp.MemoryStore` keeps it in
            this process.  Pass a :class:`~easy_mcp.RedisStore` to share it
            between worker processes, so that any worker can serve any
            request.  The stdio transport always keeps its state in process.
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
        oauth: OAuthResourceServer | None = None,
        rate_limit_per_minute: int | None = 120,
        max_request_bytes: int = 1_048_576,
        default_timeout: float | None = 30.0,
        max_sync_workers: int | None = DEFAULT_MAX_SYNC_WORKERS,
        max_sessions: int = 256,
        allowed_origins: Iterable[str] | None = None,
        instructions: str | None = None,
        store: Store | None = None,
        json_logs: bool = True,
    ) -> None:
        if default_timeout is not None and default_timeout <= 0:
            raise ValueError("default_timeout must be positive or None")
        if max_request_bytes < 1:
            raise ValueError("max_request_bytes must be >= 1")
        if max_sync_workers is not None and max_sync_workers < 1:
            raise ValueError("max_sync_workers must be >= 1 or None")
        if oauth is not None and not isinstance(oauth, OAuthResourceServer):
            raise TypeError("oauth must be an OAuthResourceServer")
        if store is not None and not isinstance(store, Store):
            raise TypeError("store must be a Store, such as MemoryStore or RedisStore")
        self.host = host
        self.port = port
        self.name = name
        self.version = version
        self.debug = debug
        self.auth = auth
        self.oauth = oauth
        # Token principals already audited as principal_seen, oldest first.
        self._principals_seen: OrderedDict[str, None] = OrderedDict()
        self._principals_lock = threading.Lock()
        self.max_request_bytes = max_request_bytes
        self.default_timeout = default_timeout
        self.max_sync_workers = max_sync_workers
        self._sync_slots = (
            threading.BoundedSemaphore(max_sync_workers) if max_sync_workers is not None else None
        )
        # Sync tool and cancel-callback threads still running, and the request
        # tasks in flight (touched only on the event loop).
        self._threads: set[threading.Thread] = set()
        self._threads_lock = threading.Lock()
        self._calls: set[asyncio.Task[Any]] = set()
        # Replaced, never mutated, under the lock: a request uses the tuples
        # as they stood when it arrived.
        self._request_middleware: tuple[RequestMiddleware, ...] = ()
        self._tool_middleware: tuple[ToolMiddleware, ...] = ()
        self._middleware_lock = threading.Lock()
        self.max_sessions = max_sessions
        self.allowed_origins = (
            normalize_origins(allowed_origins) if allowed_origins is not None else None
        )
        self.instructions = instructions
        self._registry = ToolRegistry()
        self._resources = ResourceRegistry()
        self._prompts = PromptRegistry()
        self._limiter = (
            SlidingWindowRateLimiter(rate_limit_per_minute)
            if rate_limit_per_minute
            else None
        )
        self._store: Store = store if store is not None else MemoryStore()
        self._store.bind(name)
        # What HTTP requests are charged to: the in-process limiter itself
        # with MemoryStore, the store's own with a shared store.
        self._store_limiter: AsyncRateLimiter | None = (
            self._store.rate_limiter(self._limiter) if self._limiter is not None else None
        )
        # The last ping of a shared store, for /healthz: (when, reachable).
        self._store_ping: tuple[float, bool] | None = None
        # The session managers of every HTTP endpoint serving this server: a
        # session the store removes ends on whichever endpoint holds it.
        self._session_managers: weakref.WeakSet[SessionManager] = weakref.WeakSet()
        # Set while an HTTP app of this server is serving (its lifespan runs).
        self._serving = False
        # Set once any transport has started serving: clients may have
        # cached what server/discover reported since.
        self._started = False
        self._transport: Transport | None = None
        self._logger: logging.Logger = configure_logging(debug=debug, json_logs=json_logs)
        # The capabilities advertised so far, by kind.  Sticky: a kind once
        # advertised stays, so a capability a client cached never goes
        # wrong; a kind emptied later is listed empty.
        self._advertised: dict[str, dict[str, Any]] = {"tools": {"listChanged": True}}
        self._advertised_lock = threading.Lock()
        # Digests of each list as a visibility class sees it: (kind, class)
        # -> (the registry version it was computed at, digest).
        self._digests: OrderedDict[tuple[str, Hashable], tuple[int, str]] = OrderedDict()
        self._digests_lock = threading.Lock()
        # Who is told when a list changes: sessions and listen streams.
        self._notifier = ChangeNotifier(
            self._list_digest, view=self._visibility_class, final=self._listen_final
        )

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
        """Register a tool dynamically at runtime (same options as ``tool``).

        Works from any thread.  Clients connected meanwhile that may see the
        tool are told the tool list changed (changes within 0.1 s are
        combined into one notice).

        Raises:
            ToolRegistrationError: The function cannot be exposed, or (with
                ``oauth``) a scope could not appear in a ``WWW-Authenticate``
                challenge: it is not an RFC 6749 scope-token, or it is
                ``offline_access``.
        """
        definition = build_tool(fn, **options)
        if self.oauth is not None:
            for scope in definition.declared_scopes:
                if not is_scope_token(scope) or scope == "offline_access":
                    raise ToolRegistrationError(
                        f"cannot register tool {definition.name!r}: scope {scope!r} cannot be "
                        "used with OAuth (a scope is printable ASCII without spaces, quotes "
                        "or backslashes, and offline_access is no resource scope)"
                    )
        self._registry.register(definition)
        self._logger.debug("registered tool %r", definition.name)
        self._warn_if_shared("registered", definition.name)
        self._notifier.changed("tools")
        return definition

    def unregister_tool(self, name: str) -> ToolDefinition:
        """Remove a tool at runtime; returns its definition.

        Clients connected meanwhile that could see it are told the tool list
        changed, as for :meth:`register_tool`.
        """
        removed = self._registry.unregister(name)
        self._logger.debug("unregistered tool %r", name)
        self._warn_if_shared("unregistered", name)
        self._notifier.changed("tools")
        return removed

    def _warn_if_shared(self, change: str, name: str, noun: str = "tool") -> None:
        """Warn that a tool (or other *noun*) changed in this worker only, with a shared store."""
        if self._serving and self._store.shared:
            self._logger.warning(
                "%s %r %s while serving with a shared store: only this worker sees the "
                "change; every worker must register the same %ss",
                noun,
                name,
                change,
                noun,
            )

    def _check_oauth_scopes(self, scopes: Iterable[str], what: str) -> None:
        """Refuse a scope no ``WWW-Authenticate`` challenge could name, with ``oauth``.

        Raises:
            RegistrationError: Naming *what* and the scope.
        """
        if self.oauth is None:
            return
        for scope in scopes:
            if not is_scope_token(scope) or scope == "offline_access":
                raise RegistrationError(
                    f"cannot register {what}: scope {scope!r} cannot be used with OAuth (a "
                    "scope is printable ASCII without spaces, quotes or backslashes, and "
                    "offline_access is no resource scope)"
                )

    # ------------------------------------------------------- resources, prompts

    def resource(
        self,
        uri: str,
        /,
        *,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        mime_type: str | None = None,
        size: int | None = None,
        annotations: Mapping[str, Any] | None = None,
        requires_auth: bool = False,
        scopes: Iterable[str] = (),
        timeout: float | None = None,
        cache_ttl: float = 0.0,
        complete: Mapping[str, Any] | None = None,
    ) -> Callable[[_F], _F]:
        """Register a function as an MCP resource at *uri*.

        The function's return value is the content: ``str`` is text,
        ``bytes`` a base64 ``blob``, dicts, lists and Pydantic models JSON,
        :class:`~easy_mcp.ResourceContent` items as given.  Returning
        ``None`` means the resource does not exist.  A *uri* containing
        ``{name}`` (one path segment) or ``{+name}`` (several) is a template
        whose variables become the function's parameters (``str``, ``int``,
        ``float``, ``bool`` or a ``Literal``)::

            @server.resource("users://{user_id}/profile", scopes=("users",))
            def profile(user_id: int) -> dict[str, Any] | None:
                \"\"\"A user's profile.\"\"\"
                return load_profile(user_id)

        The function is returned unchanged.  *mime_type* defaults from the
        return annotation, *description* from the docstring.  *cache_ttl* is
        how many seconds a stateless client may cache a read (default 0).
        *complete* maps a template variable to a completer: a list of
        strings, or ``fn(value, arguments)``.  The other options are the
        tools' own.

        Raises:
            RegistrationError: The resource cannot be served safely.
        """
        if not isinstance(uri, str):
            raise RegistrationError(
                "@server.resource needs the URI first: @server.resource('scheme://...')"
            )

        def decorate(target: _F) -> _F:
            self.register_resource(
                target,
                uri,
                name=name,
                title=title,
                description=description,
                mime_type=mime_type,
                size=size,
                annotations=annotations,
                requires_auth=requires_auth,
                scopes=scopes,
                timeout=timeout,
                cache_ttl=cache_ttl,
                complete=complete,
            )
            return target

        return decorate

    def register_resource(
        self, fn: Callable[..., Any], uri: str, /, **options: Any
    ) -> ResourceDefinition | ResourceTemplateDefinition:
        """Register a resource dynamically at runtime (same options as :meth:`resource`).

        Works from any thread.  Clients that asked to hear of resource list
        changes, and may see it, are told the list changed.

        Raises:
            RegistrationError: The resource cannot be served safely, or (with
                ``oauth``) a scope could not appear in a challenge.
        """
        definition = build_resource(fn, uri, **options)
        key = (
            definition.uri_template
            if isinstance(definition, ResourceTemplateDefinition)
            else definition.uri
        )
        self._check_oauth_scopes(definition.declared_scopes, f"resource {key!r}")
        self._resources.register(definition)
        self._logger.debug("registered resource %r", key)
        self._advertise("resources", {"subscribe": True, "listChanged": True})
        if isinstance(definition, ResourceTemplateDefinition) and definition.completers:
            self._advertise("completions", {})
        self._warn_if_shared("registered", key, "resource")
        self._notifier.changed("resources")
        return definition

    def unregister_resource(self, uri: str) -> ResourceDefinition | ResourceTemplateDefinition:
        """Remove a resource or template at runtime (by its URI or template); returns it.

        The ``resources`` capability stays advertised: clients may have
        cached it.

        Raises:
            RegistrationError: Nothing is registered under *uri*.
        """
        removed = self._resources.unregister(uri)
        self._logger.debug("unregistered resource %r", uri)
        self._warn_if_shared("unregistered", uri, "resource")
        self._notifier.changed("resources")
        return removed

    @property
    def resources(self) -> list[ResourceDefinition]:
        """All registered concrete resources, sorted by URI."""
        return self._resources.list_resources()

    @property
    def resource_templates(self) -> list[ResourceTemplateDefinition]:
        """All registered resource templates, sorted by template."""
        return self._resources.list_templates()

    def prompt(
        self,
        fn: Callable[..., Any] | None = None,
        /,
        *,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        requires_auth: bool = False,
        scopes: Iterable[str] = (),
        timeout: float | None = None,
        complete: Mapping[str, Any] | None = None,
    ) -> Any:
        """Register a function as an MCP prompt.

        Works bare (``@server.prompt``) or with options, as :meth:`tool`
        does; the function is returned unchanged.  Its parameters are the
        prompt's arguments, which arrive as strings and are converted to
        ``str``, ``int``, ``float``, ``bool`` or a ``Literal``.  It returns a
        string (one user message), a :class:`~easy_mcp.Message`, or a list
        of strings and messages.  ``Literal`` and ``bool`` arguments complete
        automatically; *complete* adds completers for the others.

        Raises:
            RegistrationError: The prompt cannot be served safely.
        """

        def decorate(target: Callable[..., Any]) -> Callable[..., Any]:
            self.register_prompt(
                target,
                name=name,
                title=title,
                description=description,
                requires_auth=requires_auth,
                scopes=scopes,
                timeout=timeout,
                complete=complete,
            )
            return target

        if fn is not None:
            return decorate(fn)
        return decorate

    def register_prompt(self, fn: Callable[..., Any], **options: Any) -> PromptDefinition:
        """Register a prompt dynamically at runtime (same options as :meth:`prompt`).

        Works from any thread; clients that asked to hear of prompt list
        changes, and may see it, are told the list changed.

        Raises:
            RegistrationError: The prompt cannot be served safely.
        """
        definition = build_prompt(fn, **options)
        self._check_oauth_scopes(definition.declared_scopes, f"prompt {definition.name!r}")
        self._prompts.register(definition)
        self._logger.debug("registered prompt %r", definition.name)
        self._advertise("prompts", {"listChanged": True})
        if definition.completers:
            self._advertise("completions", {})
        self._warn_if_shared("registered", definition.name, "prompt")
        self._notifier.changed("prompts")
        return definition

    def unregister_prompt(self, name: str) -> PromptDefinition:
        """Remove a prompt at runtime; returns its definition.

        Raises:
            RegistrationError: No prompt has that name.
        """
        removed = self._prompts.unregister(name)
        self._logger.debug("unregistered prompt %r", name)
        self._warn_if_shared("unregistered", name, "prompt")
        self._notifier.changed("prompts")
        return removed

    @property
    def prompts(self) -> list[PromptDefinition]:
        """All registered prompts, sorted by name."""
        return self._prompts.list()

    def middleware(self, fn: RequestMiddlewareT, /) -> RequestMiddlewareT:
        """Register request middleware: your async code around every request.

        Used as a bare decorator (``@server.middleware``) or called directly;
        *fn* is returned unchanged.  It is called as
        ``await fn(request, call_next)`` with a
        :class:`~easy_mcp.RequestInfo`, and must return what
        ``await call_next()`` returned (a :class:`~easy_mcp.RequestOutcome`)::

            @server.middleware
            async def via_gateway(request, call_next):
                if request.transport.headers.get("x-verified-by") != "gateway":
                    raise AuthenticationError("Requests must come through the gateway")
                return await call_next()

        It runs after the transport's checks, the rate limit and protocol
        validation, which it cannot skip.  Raising a ``ProtocolError`` refuses
        the request with that error; on ``tools/call`` a ``ToolError`` gives an
        ``isError`` result.  Notifications and ``server/discover`` pass
        through it but cannot be refused.  The first middleware registered is
        the outermost, and request middleware encloses tool middleware.  See
        :mod:`easy_mcp.middleware` for the whole contract.

        Raises:
            TypeError: *fn* is not async or cannot take ``(request, call_next)``.
            ValueError: *fn* is registered already.
        """
        check_middleware(fn, "request middleware")
        with self._middleware_lock:
            if any(existing is fn for existing in self._request_middleware):
                raise ValueError(f"request middleware {describe(fn)} is registered already")
            self._request_middleware = (*self._request_middleware, fn)
        self._logger.debug("registered request middleware %s", describe(fn))
        return fn

    def tool_middleware(self, fn: ToolMiddlewareT, /) -> ToolMiddlewareT:
        """Register tool middleware: your async code around every tool's execution.

        Used as a bare decorator (``@server.tool_middleware``) or called
        directly; *fn* is returned unchanged.  It is called as
        ``await fn(call, call_next)`` with a :class:`~easy_mcp.ToolCall`, and
        must return what ``await call_next()`` returned (a
        :class:`~easy_mcp.ToolOutcome`)::

            @server.tool_middleware
            async def quota(call, call_next):
                if await over_budget(call.client_id):
                    raise RateLimitError(retry_after_seconds=60)
                return await call_next()

        It runs after visibility, scopes, ``max_calls_per_session`` and
        argument validation, so ``call.arguments`` are validated and
        ``call.tool`` is a tool this caller may use.  Raising before
        ``call_next()`` refuses the call: the tool does not run and the call
        is not counted.  The tool's ``timeout`` covers the tool only, so
        bound your own awaits.

        Raises:
            TypeError: *fn* is not async or cannot take ``(call, call_next)``.
            ValueError: *fn* is registered already.
        """
        check_middleware(fn, "tool middleware")
        with self._middleware_lock:
            if any(existing is fn for existing in self._tool_middleware):
                raise ValueError(f"tool middleware {describe(fn)} is registered already")
            self._tool_middleware = (*self._tool_middleware, fn)
        self._logger.debug("registered tool middleware %s", describe(fn))
        return fn

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

    @property
    def auth_configured(self) -> bool:
        """Whether callers can authenticate at all: ``auth`` or ``oauth`` is set."""
        return self.auth is not None or self.oauth is not None

    async def authenticate_request(
        self,
        *,
        bearer: str | None = None,
        api_key: str | None = None,
        tool: str | None = None,
        client: str | None = None,
    ) -> ClientIdentity | None:
        """Resolve one HTTP request's credential to an identity.

        *bearer* is the token of an ``Authorization: Bearer`` header and
        *api_key* the value of an ``X-API-Key`` header (the bearer wins when
        both are given).  A value that matches an API key is that key; with
        ``oauth`` set, a bearer value that does not is verified as an access
        token, which must also hold every ``required_scopes``.  An
        ``X-API-Key`` value is never sent for token verification.  Query
        strings and bodies are never consulted.

        *tool* is the tool a ``tools/call`` names.  A token that lacks a
        required scope is then also asked for the scope that tool needs (with
        step-up), so one challenge covers the whole call.

        *client* names who presented the credential, as
        :meth:`~easy_mcp.OAuthResourceServer.verify` takes it (the HTTP
        transports pass ``"ip:<address>"``).

        Returns:
            The identity, or ``None`` for anonymous access, which only a
            server without ``oauth`` allows (as does one without any auth,
            whatever is presented).

        Raises:
            TokenRequiredError: ``oauth`` is set and nothing was presented.
            InvalidTokenError: The access token failed verification.
            InsufficientScopeError: A valid token lacks a required scope.
            AuthServerUnavailableError: The token could not be checked.
            AuthenticationError: The value is no API key and cannot be a token.
        """
        presented = bearer if bearer is not None else api_key
        if presented is None:
            if self.oauth is not None:
                raise TokenRequiredError()
            return None
        if self.auth is not None:
            identity = self.auth.match(presented)
            if identity is not None:
                return identity
        elif self.oauth is None:
            return None  # nothing to check credentials against: anonymous
        if self.oauth is not None and bearer is not None:
            identity = await self.oauth.verify(bearer, client=client)
            required = self.oauth.required_scopes
            missing = [scope for scope in required if scope not in identity.scopes]
            if missing:
                definition = self._registry.get(tool) if tool is not None else None
                step = self._step_up_scope(identity, definition)
                if step is not None and step not in missing:
                    missing.append(step)
                raise InsufficientScopeError(missing, granted=identity.scopes)
            self._note_principal(identity)
            return identity
        raise AuthenticationError("Invalid API key")

    def _note_principal(self, identity: ClientIdentity) -> None:
        """Audit ``principal_seen`` the first time a token principal shows up.

        Every other event names the principal by fingerprint only, so its
        subject is logged once rather than on every call.
        """
        with self._principals_lock:
            if identity.fingerprint in self._principals_seen:
                self._principals_seen.move_to_end(identity.fingerprint)
                return
            self._principals_seen[identity.fingerprint] = None
            if len(self._principals_seen) > _PRINCIPALS_SEEN_MAX:
                self._principals_seen.popitem(last=False)
        audit(
            "principal_seen",
            client_id=identity.fingerprint,
            issuer=identity.issuer,
            subject=identity.subject,
            oauth_client_id=identity.client_id,
        )

    def _reserve_auth_attempt(self, key: str) -> float | None:
        """Hold one unit of *key*'s failed-authentication budget while a credential is checked.

        The failed-authentication budget of one client address is the
        server's rate limit, kept under a key of its own.  A failed check
        keeps its unit; any other outcome gives it back
        (:meth:`_release_auth_attempt`).  So checks still running count
        against the budget too, and a burst of bad credentials sent at once
        cannot all be verified.  Without rate limiting there is no throttle.

        Returns:
            The reservation, or ``None`` without rate limiting.

        Raises:
            RateLimitError: The budget is used up, by failures or by checks
                in flight.
        """
        if self._limiter is None:
            return None
        return self._limiter._record(key)

    def _release_auth_attempt(self, key: str, reservation: float | None) -> None:
        """Give back the unit :meth:`_reserve_auth_attempt` held for a check that did not fail."""
        if self._limiter is not None and reservation is not None:
            self._limiter._refund(key, reservation)

    def _steps_up(self, identity: ClientIdentity | None) -> bool:
        """Whether *identity* sees every tool and is challenged for a missing scope.

        Only token identities, and only with ``oauth.step_up``; API keys keep
        hiding what they cannot call.
        """
        return (
            self.oauth is not None
            and self.oauth.step_up
            and identity is not None
            and identity.issuer is not None
        )

    def _visible(self, identity: ClientIdentity | None, item: Guarded) -> bool:
        """Whether *item* exists for this caller (lists and lookups alike)."""
        return self._steps_up(identity) or visible(identity, item)

    def _step_up_scope(self, identity: ClientIdentity | None, item: Guarded | None) -> str | None:
        """The scope a token must ask for to use *item*; ``None`` when there is none.

        With step-up, for a token holding none of *item*'s scopes: the first
        one declared, the narrowest by convention.
        """
        if item is None or identity is None or not item.scopes or not self._steps_up(identity):
            return None
        if identity.scopes & item.scopes:
            return None
        return item.declared_scopes[0] if item.declared_scopes else min(item.scopes)

    def _check_step_up(
        self,
        identity: ClientIdentity | None,
        item: Guarded,
        kind: str = "tool",
        label: str | None = None,
    ) -> None:
        """Refuse a token that holds none of *item*'s scopes, naming the one to ask for.

        *kind* and *label* name the item in the message (by default a tool
        and its name).

        Raises:
            InsufficientScopeError: With the narrowest declared scope.
        """
        first = self._step_up_scope(identity, item)
        if identity is not None and first is not None:
            named = label if label is not None else item.name
            raise InsufficientScopeError(
                (first,), f"Insufficient scope for {kind} '{named}'", granted=identity.scopes
            )

    def _initial_scopes(self) -> tuple[str, ...]:
        """What a client should ask for up front: the metadata's ``scopes_supported``.

        With step-up, ``required_scopes`` only, the minimal set; without it
        every tool scope too, since a tool stays invisible until a token
        holds its scope.  Never ``offline_access``.
        """
        if self.oauth is None:
            return ()
        scopes = list(self.oauth.required_scopes)
        if not self.oauth.step_up:
            item_scopes = {scope for item in self._guarded() for scope in item.scopes}
            scopes.extend(sorted(item_scopes - set(scopes)))
        return tuple(scope for scope in scopes if scope != "offline_access")

    def _known_scopes(self) -> frozenset[str]:
        """Every scope this server checks: ``required_scopes`` and every item's scopes."""
        scopes = set(self.oauth.required_scopes) if self.oauth is not None else set()
        for item in self._guarded():
            scopes.update(item.scopes)
        scopes.discard("offline_access")
        return frozenset(scopes)

    def _guarded(self) -> list[Guarded]:
        """Every registered item that scopes can guard: tools, resources, templates, prompts."""
        return [*self.tools, *self.resources, *self.resource_templates, *self.prompts]

    @staticmethod
    def _request_context(
        context: ClientContext, identity: ClientIdentity | None
    ) -> ClientContext:
        """The context one request of a session runs with: the session's, with its identity.

        Each request of a session carries its own credential.  For an API key
        the identity is always the session's, so the session's own context
        is used; a refreshed or broader token gets a copy that shares the
        session's call counts and calls in flight.
        """
        current = context.identity
        same = identity is current or (
            identity is not None
            and current is not None
            and identity == current
            and identity.claims == current.claims
        )
        return context if same else dataclasses.replace(context, identity=identity)

    def check_rate_limit(self, client_id: str) -> None:
        """Consume one unit of *client_id*'s request budget, in this process.

        ``dispatch`` calls this for every message whose context has no
        ``store_handle``: stdio, and a direct ``dispatch`` call.  The HTTP
        transports charge their messages, and the opening of a legacy SSE
        session, through :meth:`acheck_rate_limit` instead, which spends the
        store's budget (with :class:`~easy_mcp.MemoryStore`, this same one):
        override that one to change how HTTP requests are limited.

        Raises:
            RateLimitError: If the client is over budget.  A no-op when rate
                limiting is disabled.
        """
        if self._limiter is not None:
            self._limiter.check(client_id)

    @property
    def store(self) -> Store:
        """Where sessions, call counts and rate-limit windows are kept (see ``store=``)."""
        return self._store

    async def acheck_rate_limit(self, client_id: str) -> None:
        """Consume one unit of *client_id*'s request budget in the store.

        The async counterpart of :meth:`check_rate_limit`, which the HTTP
        transports use for every message and every legacy SSE session they
        open.  With :class:`~easy_mcp.MemoryStore` both spend the same
        in-process budget; with a shared store this one is shared between the
        workers and :meth:`check_rate_limit` stays per process.

        Raises:
            RateLimitError: If the client is over budget.  A no-op when rate
                limiting is disabled.
            StoreUnavailableError: The shared store cannot be reached.
        """
        if self._store_limiter is not None:
            await self._store_limiter.acheck(client_id)

    async def _charge(self, context: ClientContext) -> None:
        """Charge one message to its client's budget.

        A context from an HTTP transport (with a ``store_handle``) is charged
        to the store's budget, shared between workers with a shared store;
        any other (stdio, a direct ``dispatch``) to the in-process one.
        """
        if context.store_handle is not None:
            await self.acheck_rate_limit(context.client_id)
        else:
            self.check_rate_limit(context.client_id)

    async def _store_reachable(self) -> bool:
        """Whether the store answers a ping; the answer is kept for a second."""
        now = time.monotonic()
        cached = self._store_ping
        if cached is not None and now - cached[0] < _STORE_PING_CACHE_SECONDS:
            return cached[1]
        try:
            reachable = await self._store.ping()
        except Exception:
            reachable = False
        self._store_ping = (time.monotonic(), reachable)
        return reachable

    def _session_event(
        self,
        opened: bool,
        *,
        kind: str,
        transport: str,
        session_id: str | None,
        ref: str,
        client_id: str | None,
        protocol_version: str | None = None,
        t0: int | None = None,
        reason: str | None = None,
    ) -> None:
        """Every session opening or closing goes through here: HTTP, SSE and stdio.

        Audited as ``session_open`` or ``session_close``.  *ref* is the
        session's ``session_ref``; the raw *session_id* is audited too, when
        this process knows it (it is dropped from audit events in 0.4).
        *t0* is when the session opened (epoch milliseconds) and *kind* is
        ``"http"``, ``"sse"`` or ``"stdio"``.
        """
        fields: dict[str, Any] = {}
        if session_id is not None:
            fields["session_id"] = session_id
        fields["session_ref"] = ref
        fields["client_id"] = client_id
        fields["transport"] = transport
        if opened:
            if protocol_version is not None:
                fields["protocol_version"] = protocol_version
        elif reason is not None:
            fields["reason"] = reason
        if self._store.shared and kind != "stdio":
            fields["worker"] = self._store.worker_id
        audit("session_open" if opened else "session_close", **fields)

    # --------------------------------------------------------------- dispatch

    async def dispatch(
        self,
        message: Any,
        context: ClientContext,
        *,
        transport: TransportInfo | None = None,
    ) -> dict[str, Any] | None:
        """Handle one JSON-RPC message; returns the response or ``None``.

        This is the single entry point every transport funnels through, and
        the place rate limiting and method routing are enforced.  It never
        raises: malformed input and internal failures both come back as
        JSON-RPC error responses (sanitized outside debug mode).  Only a
        cancellation of the caller itself is re-raised.

        Args:
            message: One decoded JSON-RPC message.
            context: The connection's or session's state.
            transport: How the message arrived, for middleware.  A custom
                transport should pass one; without it, middleware sees a
                transport named ``"custom"``.
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
            await self._charge(context)
        except StoreUnavailableError as exc:
            # The budget cannot be checked, so the message is not served
            # (the store logs the outage).
            return None if is_notification else _protocol_error_response(msg_id, exc)
        except ProtocolError as exc:
            audit("rate_limited", client_id=context.client_id, method=method)
            return None if is_notification else _protocol_error_response(msg_id, exc)
        except Exception:
            # A store that failed in a way it does not report as an outage:
            # still not served, and answered rather than raised.
            error_id = uuid.uuid4().hex[:12]
            self._logger.error("rate limit check failed error_id=%s", error_id, exc_info=True)
            if is_notification:
                return None
            return _error_response(
                msg_id, INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
            )

        if is_notification and not method.startswith("notifications/"):
            # Only requests invoke methods.  A tools/call without an id would
            # run a tool whose answer nobody can receive, and over HTTP it
            # would skip the header checks that apply to requests.
            return None

        # A request carrying the modern per-request _meta is served statelessly
        # (2026-07-28); anything else keeps the initialize-era behaviour.
        modern = not is_notification and is_modern_request(method, params)
        if not modern:
            # What the session is told about follows the credential of its
            # latest request (a refreshed or broader token).
            self._notifier.refresh_identity(context.session_id, context.identity)
        response: dict[str, Any] | None
        try:
            if modern:
                check_request_meta(params)
            notification_method = method.startswith("notifications/")
            if notification_method and not is_notification:
                # A notification has no id; a request naming one of these
                # methods is asking for a method that does not exist.
                raise ProtocolError(f"Method not found: {method}", code=METHOD_NOT_FOUND)
            # Methods the server does not serve never reach middleware, so
            # RequestInfo.method only ever holds one of the table's names.
            spec = self._method(
                method, modern=modern, notification=notification_method, context=context
            )
            if spec is None:
                if notification_method:
                    return None  # unknown notifications are ignored, per JSON-RPC
                return _error_response(msg_id, METHOD_NOT_FOUND, f"Method not found: {method}")
            request = self._request_info(
                method, msg_id, is_notification, modern, params, context, transport
            )
            if spec.notification:
                notify = functools.partial(self._handle_notification_outcome, request, context)
                await self._isolated(request, functools.partial(self._observe, request, notify))
                return None
            if spec.cancellable:
                response = await self._serve_cancellable(request, context)
            else:
                response = await self._serve(request, context)
        except ProtocolError as exc:
            response = None if is_notification else _protocol_error_response(msg_id, exc)
        except Exception:
            # Sanitize: clients get an opaque error_id; the log gets the trace.
            error_id = uuid.uuid4().hex[:12]
            self._logger.error("internal error error_id=%s", error_id, exc_info=True)
            if is_notification:
                return None
            response = _error_response(
                msg_id, INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
            )
        if response is not None and "error" in response:
            response = self._finalize_error(response, modern)
        return response

    def _method(
        self, method: str, *, modern: bool, notification: bool, context: ClientContext
    ) -> _Method | None:
        """The table row serving *method* in this era, or ``None`` if there is none.

        A method of the other era, one whose capability is not advertised, or
        one that needs a channel *context* lacks, does not exist for this
        request.
        """
        spec = _METHODS.get(method)
        if spec is None or spec.notification != notification:
            return None
        if not (spec.modern if modern else spec.legacy):
            return None
        if spec.capability is not None and spec.capability not in self._capabilities():
            return None
        if spec.needs_push and context.push is None:
            return None
        if spec.needs_session and context.push is None and context.store_handle is None:
            return None
        return spec

    def _request_info(
        self,
        method: str,
        msg_id: Any,
        is_notification: bool,
        modern: bool,
        params: dict[str, Any],
        context: ClientContext,
        transport: TransportInfo | None,
    ) -> RequestInfo:
        if modern:
            # check_request_meta has made sure it is there.
            version: str | None = params["_meta"][META_PROTOCOL_VERSION]
        elif method == "initialize":
            version = negotiate_protocol_version(params.get("protocolVersion"))
        else:
            version = context.protocol_version
        tool: ToolDefinition | None = None
        if method == "tools/call":
            name = params.get("name")
            if isinstance(name, str):
                tool = self._registry.get(name)
        return RequestInfo._create(
            method=method,
            request_id=msg_id,
            is_notification=is_notification,
            stateless=modern,
            protocol_version=version,
            client_id=context.client_id,
            session_id=None if modern else context.session_id,
            identity=context.identity,
            transport=transport,
            tool=tool,
            params=params,
            request_layers=self._request_middleware,
            tool_layers=self._tool_middleware,
        )

    async def _serve_cancellable(
        self, request: RequestInfo, context: ClientContext
    ) -> dict[str, Any] | None:
        """Serve a request as a task of its own, so notifications/cancelled can stop it.

        Returns ``None`` when the client cancelled the request (MCP sends
        no response then).  When it is our own caller that is cancelled (a
        timeout around ``dispatch``, a closed stateless connection,
        shutdown), the cancellation is re-raised instead: the caller's
        cancel count tells the two apart, since asyncio cancels the task
        we await in both cases.  Middleware runs inside the task, so a
        cancel reaches it wherever the request is.  A ``CancelledError``
        the request raised when nothing cancelled it is an internal error.

        A ``subscriptions/listen`` stream ends here whichever way it ends:
        audited ``subscription_close`` (not ``request_cancelled``) with
        ``disconnected`` when our caller was cancelled and
        ``client_cancelled`` when the request itself was.
        """
        caller = asyncio.current_task()
        baseline = caller.cancelling() if caller is not None else 0
        listen = request.method == LISTEN_METHOD
        ended = "closed"
        task: asyncio.Task[dict[str, Any] | None] = asyncio.create_task(
            self._serve(request, context)
        )
        self._calls.add(task)
        task.add_done_callback(self._calls.discard)
        msg_id = request.request_id
        registered = msg_id is not None and isinstance(msg_id, Hashable)
        if registered:
            context.in_flight[msg_id] = task
        try:
            return await task
        except asyncio.CancelledError:
            ours = caller is not None and caller.cancelling() > baseline
            # A cancelled caller that nobody awaits keeps its CancelledError,
            # whose traceback holds this frame: were the frame to hold the
            # caller too, the request would stay alive until the cyclic
            # collector ran.
            caller = None
            if not ours and task.cancelled() and task.cancelling() == 0:
                # Nobody cancelled the request (a client's cancel and a
                # session's end cancel the task itself): something it awaited
                # raised CancelledError on its own, a shared future another
                # waiter cancelled, say.  A failure, which must be answered.
                error_id = uuid.uuid4().hex[:12]
                self._logger.error(
                    "%s raised CancelledError although nothing cancelled it error_id=%s",
                    request.method,
                    error_id,
                    exc_info=True,
                )
                return _error_response(
                    msg_id, INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
                )
            if not task.done() or task.cancelled():
                if listen:
                    ended = "disconnected" if ours else "client_cancelled"
                elif request.method == "tools/call":
                    audit("tool_cancelled", client_id=context.client_id, request_id=msg_id)
                else:
                    audit(
                        "request_cancelled",
                        method=request.method,
                        client_id=context.client_id,
                        request_id=msg_id,
                    )
            if ours:
                task.cancel()  # the request must not outlive its caller
                raise
            return None  # cancelled by the client: per MCP, no response
        except Exception:
            error_id = uuid.uuid4().hex[:12]
            self._logger.error("internal error error_id=%s", error_id, exc_info=True)
            return _error_response(
                msg_id, INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
            )
        finally:
            if listen and request._subscription is not None:
                # Gone already when the server or the client ended it.
                self._notifier.drop(request._subscription, ended)
            if registered and context.in_flight.get(msg_id) is task:
                del context.in_flight[msg_id]

    async def _serve(self, request: RequestInfo, context: ClientContext) -> dict[str, Any] | None:
        """Run the request middleware around the request; returns the response.

        ``None`` for a ``subscriptions/listen`` that was served, or whose
        stream had its answer before middleware replaced it: its frames, the
        final one included, went through ``context.push``.
        """
        outcome = await self._isolated(request, functools.partial(self._outcome, request, context))
        if request.method == "initialize" and outcome.error_code is None:
            # Only a handshake the client is answered with negotiates a
            # version; later requests on this connection or session are
            # spoken in it.
            context.protocol_version = request.protocol_version
            if context.push is not None and context.store_handle is None:
                # The context holds the session's whole state, its channel
                # included: list changes are announced on it from now on,
                # since the transport sends this result before anything
                # else.  (A transport keeping sessions in the store starts
                # them itself, on the worker holding the session's stream.)
                self._watch_session(
                    context.session_id,
                    push=context.push,
                    identity=context.identity,
                    client_id=context.client_id,
                    multiplexed=context.multiplexed,
                    anchor=context,
                )
        if request.method == LISTEN_METHOD:
            if outcome.error_code is None:
                return None
            sink = request._subscription
            if sink is not None and sink.settled:
                # Its stream had its answer already (its completion result,
                # or none for a client's cancel): a request gets one.
                self._logger.info(
                    "subscriptions/listen %r: withheld the error (code %s) middleware "
                    "answered once its stream had ended",
                    request.request_id,
                    outcome.error_code,
                )
                return None
        if outcome.error_code is not None:
            return _error_response(
                request.request_id, outcome.error_code, outcome.message or "", outcome._data
            )
        result = outcome._result
        if request.stateless:
            result = self._modern_result(result)
        return _result_response(request.request_id, result)

    async def _outcome(self, request: RequestInfo, context: ClientContext) -> RequestOutcome:
        """The request's outcome, with its request middleware around it."""
        route = functools.partial(self._route, request, context)
        layers = request._request_layers
        if _METHODS[request.method].observe_only:
            return await self._observe(request, route)
        if layers:
            policy = RequestPolicy(self._logger, self.debug)
            return await run_chain(layers, request, route, policy, request)
        return await route()

    @staticmethod
    async def _isolated(
        request: RequestInfo, serve: Callable[[], Coroutine[Any, Any, RequestOutcome]]
    ) -> RequestOutcome:
        """Run *serve* in a task of its own whenever middleware will run in it.

        A middleware swallowed a cancellation if the request was cancelled
        and the middleware did not re-raise it.  asyncio counts every cancel
        of a task, those a library makes and takes back itself included (and
        on Python 3.11, a TaskGroup whose child fails never takes back its
        own), so the task middleware runs in cannot tell.  The task awaiting
        it can: only cancels of the request reach it (the task registered
        for ``notifications/cancelled``, or the caller of ``dispatch``).
        """
        if not request._request_layers and not (
            request._tool_layers and request.method == "tools/call"
        ):
            return await serve()
        request._watch_current_task()
        return await asyncio.create_task(serve())

    async def _observe(
        self, request: RequestInfo, handle: Callable[[], Awaitable[RequestOutcome]]
    ) -> RequestOutcome:
        """Run the request middleware around a message it may watch but not refuse.

        Whatever a middleware raises or returns, *handle* runs (once), and
        its outcome is what the client gets: a ``notifications/cancelled``
        always cancels, and ``server/discover`` always answers.
        """
        layers = request._request_layers
        if not layers:
            return await handle()
        policy = ObservePolicy(self._logger, self.debug)
        return await run_chain(layers, request, handle, policy, request)

    async def _route(self, request: RequestInfo, context: ClientContext) -> RequestOutcome:
        """Serve the request itself, inside the request middleware.

        Never raises but ``CancelledError``: errors become outcomes.
        """
        method = request.method
        result: Any
        try:
            if method == "tools/call":
                return RequestOutcome._of_tool(await self._execute_tool(request, context))
            if method == LISTEN_METHOD:
                # Returns when the stream ends; what the client gets went
                # through context.push.
                await self._handle_listen(request, context)
                return RequestOutcome._create(result={})
            if method == "resources/read":
                result = await self._read_resource(request, context)
            elif method == "prompts/get":
                result = await self._get_prompt(request, context)
            elif method == "completion/complete":
                result = await self._complete(request, context)
            elif method == "resources/subscribe":
                result = await self._subscribe(request, context)
            elif method == "resources/unsubscribe":
                result = await self._unsubscribe(request, context)
            elif request.stateless:
                result = self._dispatch_modern(method, context, request._params)
            elif method == "initialize":
                result = self._handle_initialize(request._params, context)
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = self._handle_tools_list(context)
            elif method in _LISTS:
                result = self._handle_list(method, request._params, context)
            else:  # the method table and this routing disagree
                raise ProtocolError(f"Method not found: {method}", code=METHOD_NOT_FOUND)
        except ProtocolError as exc:
            # The code the client will get, so outer middleware sees it too.
            code = era_error_code(exc.code, stateless=request.stateless)
            return RequestOutcome._create(error_code=code, message=str(exc), data=exc.data)
        except Exception:
            # Sanitize: clients get an opaque error_id; the log gets the trace.
            error_id = uuid.uuid4().hex[:12]
            self._logger.error("internal error error_id=%s", error_id, exc_info=True)
            return RequestOutcome._create(
                error_code=INTERNAL_ERROR, message=f"Internal server error (error_id={error_id})"
            )
        return RequestOutcome._create(result=result)

    def _finalize_error(self, response: dict[str, Any], modern: bool) -> dict[str, Any]:
        """The last check on every error response ``dispatch`` returns.

        The MCP specification reserves ``-32020``..``-32099`` and defines
        only ``-32020``..``-32022`` in it; a code from the rest is a server
        bug, whoever raised it, so it is answered as ``-32603`` with an
        ``error_id`` and logged.  And the stateless revision forbids
        ``-32002`` (this package's ``FORBIDDEN``; older revisions use it for
        "resource not found"), so a stateless answer carries ``-32001``
        instead.
        """
        error = response.get("error")
        if not isinstance(error, dict):
            return response
        code = error.get("code")
        if isinstance(code, int) and is_reserved_error_code(code):
            error_id = uuid.uuid4().hex[:12]
            self._logger.error(
                "refused to send error code %d, which MCP reserves, error_id=%s: %s",
                code,
                error_id,
                error.get("message"),
            )
            return _error_response(
                response.get("id"), INTERNAL_ERROR, f"Internal server error (error_id={error_id})"
            )
        if modern and code == FORBIDDEN:
            return {**response, "error": {**error, "code": AUTHENTICATION_REQUIRED}}
        return response

    def _capabilities(self) -> dict[str, Any]:
        """What ``initialize`` and ``server/discover`` advertise (a copy)."""
        with self._advertised_lock:
            return {kind: dict(capability) for kind, capability in self._advertised.items()}

    def _advertise(self, kind: str, capability: dict[str, Any]) -> None:
        """Advertise the *kind* capability from now on, for the life of the process.

        Each feature calls it once its first item is registered; tools are
        advertised from the start.  A capability is never withdrawn, so a
        client that cached ``server/discover`` (for up to an hour) is never
        told something that stopped being true.  One added once serving has
        begun is logged: such clients learn of it only when their copy
        expires.
        """
        with self._advertised_lock:
            if kind in self._advertised:
                return
            self._advertised[kind] = dict(capability)
        if self._started:
            self._logger.warning(
                "capability %r added after serving began: clients that cached server/discover "
                "may not see it for up to an hour",
                kind,
            )

    def _list_kinds(self) -> tuple[str, ...]:
        """The list kinds whose changes are announced: those advertised with ``listChanged``."""
        with self._advertised_lock:
            return tuple(
                kind
                for kind in LIST_KINDS
                if self._advertised.get(kind, {}).get("listChanged") is True
            )

    def _server_info(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version}

    def _handle_initialize(self, params: dict[str, Any], context: ClientContext) -> dict[str, Any]:
        # The version is recorded on *context* by _serve, once the answer
        # stands: middleware may still refuse the handshake.
        version = negotiate_protocol_version(params.get("protocolVersion"))
        result: dict[str, Any] = {
            "protocolVersion": version,
            "capabilities": self._capabilities(),
            "serverInfo": self._server_info(),
        }
        if self.instructions:
            result["instructions"] = self.instructions
        return result

    def _dispatch_modern(
        self, method: str, context: ClientContext, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Serve a stateless request that runs no user code: discovery and the lists.

        ``initialize``, ``ping`` and ``notifications/initialized`` do not
        exist in this era, so they are unknown methods here.
        """
        if method == DISCOVER_METHOD:
            result: dict[str, Any] = {
                "supportedVersions": list(SUPPORTED_PROTOCOL_VERSIONS),
                "capabilities": self._capabilities(),
                "ttlMs": DISCOVER_TTL_MS,
                "cacheScope": self._cache_scope(method),
            }
            if self.instructions:
                result["instructions"] = self.instructions
            return result
        if method == "tools/list":
            result = self._handle_tools_list(context)
            result["ttlMs"] = TOOLS_LIST_TTL_MS
            result["cacheScope"] = self._cache_scope(method)
            return result
        if method in _LISTS:
            result = self._handle_list(method, params or {}, context)
            # The same scope on every page, as the caching spec requires.
            result["ttlMs"] = (
                PROMPTS_LIST_TTL_MS if method == "prompts/list" else RESOURCES_LIST_TTL_MS
            )
            result["cacheScope"] = self._cache_scope(method)
            return result
        raise ProtocolError(f"Method not found: {method}", code=METHOD_NOT_FOUND)

    def _cache_scope(self, method: str, item: Guarded | None = None, *, retry: bool = False) -> str:
        """The ``cacheScope`` of a stateless result.

        ``"public"`` only when an anonymous request would get the same bytes.
        ``server/discover`` is the same for everyone (with ``oauth`` it needs
        a token, but its answer does not depend on which).  A list is not once
        auth is configured (protected items are hidden from callers who cannot
        use them, and API keys and tokens may see different lists) or request
        middleware is registered (it may answer each caller differently), and
        a shared cache must not hand one caller's list to another.  A
        ``resources/read`` of *item* is not when the resource is protected,
        ``oauth`` is set, request middleware is registered, or it is a retry
        carrying ``inputResponses``/``requestState`` (*retry*), whose result
        must not be cached at all.
        """
        if method == DISCOVER_METHOD:
            return "public"
        if method == "resources/read":
            protected = item is not None and item.requires_auth
            if retry or protected or self.oauth is not None or self._request_middleware:
                return "private"
            return "public"
        if self.auth_configured or self._request_middleware:
            return "private"
        return "public"

    def _modern_result(self, result: dict[str, Any]) -> dict[str, Any]:
        """Stamp a stateless result with its ``resultType`` and our identity."""
        meta = dict(result.get("_meta") or {})
        meta[META_SERVER_INFO] = self._server_info()
        return {**result, "resultType": "complete", "_meta": meta}

    def _handle_tools_list(self, context: ClientContext) -> dict[str, Any]:
        _, tools = self._list_entries("tools", context.identity)
        return {"tools": tools}

    def _handle_list(
        self, method: str, params: dict[str, Any], context: ClientContext
    ) -> dict[str, Any]:
        """One page of ``resources/list``, ``resources/templates/list`` or ``prompts/list``.

        Items the caller cannot see are left out before paging, so they are
        neither shown nor counted.

        Raises:
            ProtocolError: ``-32602`` for an invalid cursor.
        """
        kind, field = _LISTS[method]
        identity = context.identity
        cursor = params.get("cursor")
        entries: list[dict[str, Any]]
        next_cursor: str | None
        if kind == "resources":
            resources = [r for r in self.resources if self._visible(identity, r)]
            page, next_cursor = paginate(resources, key=lambda r: r.uri, kind=kind, cursor=cursor)
            entries = [r.to_mcp() for r in page]
        elif kind == "templates":
            templates = [t for t in self.resource_templates if self._visible(identity, t)]
            pages, next_cursor = paginate(
                templates, key=lambda t: t.uri_template, kind=kind, cursor=cursor
            )
            entries = [t.to_mcp() for t in pages]
        else:
            prompts = [p for p in self.prompts if self._visible(identity, p)]
            found, next_cursor = paginate(prompts, key=lambda p: p.name, kind=kind, cursor=cursor)
            entries = [p.to_mcp() for p in found]
        result: dict[str, Any] = {field: entries}
        if next_cursor is not None:
            result["nextCursor"] = next_cursor
        return result

    # ------------------------------------------------- resources and prompts

    def _resolve_resource(
        self, uri: str, identity: ClientIdentity | None
    ) -> tuple[ResourceDefinition | ResourceTemplateDefinition | None, dict[str, Any], bool]:
        """What a read of *uri* by *identity* runs: ``(definition, arguments, hidden)``.

        A concrete resource with exactly that URI first, then the templates,
        most specific first, whose pattern matches and whose variables
        convert.  Items *identity* cannot see are skipped as if they did not
        exist; *hidden* says one of them matched (for the audit log only).
        """
        hidden = False
        concrete = self._resources.get(uri)
        if concrete is not None:
            if self._visible(identity, concrete):
                return concrete, {}, False
            hidden = True
        for template in self._resources.templates_by_specificity():
            arguments = template.bind(uri)
            if arguments is None:
                continue
            if self._visible(identity, template):
                return template, arguments, False
            hidden = True
        return None, {}, hidden

    @staticmethod
    def _resource_not_found(
        uri: str | None, stateless: bool, message: str = "Resource not found"
    ) -> ProtocolError:
        """The error for a missing resource: ``-32602`` statelessly, ``-32002`` before."""
        data = {"uri": uri} if uri is not None and len(uri) <= _ECHO_URI_MAX else None
        code = INVALID_PARAMS if stateless else RESOURCE_NOT_FOUND_LEGACY
        return ProtocolError(message, code=code, data=data)

    @staticmethod
    def _label(uri: str) -> str:
        """*uri* as messages and thread names name it: cut short when long."""
        return uri if len(uri) <= _LABEL_MAX else uri[: _LABEL_MAX - 3] + "..."

    def _user_code_failed(
        self, kind: str, label: str, exc: BaseException, *, unsupported: bool = False
    ) -> tuple[str, ProtocolError]:
        """Log what a resource, prompt or completer raised; the sanitized error for the client.

        *unsupported* marks a return value the server cannot send, which is
        logged by its type only, never by its value.
        """
        error_id = uuid.uuid4().hex[:12]
        try:
            if unsupported:
                self._logger.error(
                    "%s %r returned something it cannot return (%s) error_id=%s",
                    kind,
                    label,
                    exc,
                    error_id,
                )
            else:
                self._logger.error("%s %r failed error_id=%s", kind, label, error_id, exc_info=exc)
            message = f"Internal server error (error_id={error_id})"
            if self.debug:
                trace = "".join(traceback.format_exception(exc))
                message = f"{message}: {type(exc).__name__}: {exc}\n{trace}"
            return error_id, ProtocolError(message, code=INTERNAL_ERROR)
        finally:
            exc.__traceback__ = None  # see _tool_failed

    async def _call_user_code(
        self,
        fn: Callable[..., Any],
        arguments: Mapping[str, Any],
        *,
        is_async: bool,
        timeout: float | None,
        target: _Target,
        context: ClientContext,
    ) -> Any:
        """Run a resource, prompt or completer the way tools run: token, deadline, caller.

        Raises what :meth:`_run_user_code` raises.  ``current_cancel_token()``
        and ``current_identity()`` work inside the function.
        """
        token = CancelToken()
        token._on_error = self._callback_failed(target.label, context, target.kind)
        with cancel_scope(token), _identity_scope(context.identity):
            return await self._run_user_code(
                fn,
                arguments,
                is_async=is_async,
                timeout=timeout,
                token=token,
                target=target,
                context=context,
            )

    async def _read_resource(self, request: RequestInfo, context: ClientContext) -> dict[str, Any]:
        """Serve ``resources/read``: resolve the URI, run the resource, render its contents.

        Raises:
            ProtocolError: Not found (``-32602`` statelessly, ``-32002``
                before), ``-32005`` past its timeout, ``-32008`` with no free
                worker, ``-32603`` for a failure (a ``ToolError`` keeps its
                message), or a step-up ``InsufficientScopeError``.
        """
        params = request._params
        uri = params.get("uri")
        if not isinstance(uri, str):
            raise ProtocolError("resources/read requires a string 'uri'", code=INVALID_PARAMS)
        stateless = request.stateless
        identity = context.identity
        started = time.perf_counter()
        fields: dict[str, Any] = {"uri": uri[:_AUDIT_URI_MAX]}

        def done(status: str, **extra: Any) -> None:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            audit(
                "resource_read",
                **fields,
                client_id=context.client_id,
                duration_ms=duration_ms,
                status=status,
                **extra,
            )

        definition, arguments, hidden = self._resolve_resource(uri, identity)
        if definition is None:
            done("not_found", **({"hidden": True} if hidden else {}))
            raise self._resource_not_found(uri, stateless)
        if isinstance(definition, ResourceTemplateDefinition):
            fields["template"] = definition.uri_template
        label = self._label(uri)
        try:
            self._check_step_up(identity, definition, "resource", label)
        except InsufficientScopeError:
            done("denied")
            raise
        try:
            authorize(identity, definition, "Resource")
        except (AuthenticationError, AuthorizationError):
            # Cannot happen while visibility and authorization agree.
            done("not_found", hidden=True)
            raise self._resource_not_found(uri, stateless) from None
        timeout = definition.timeout if definition.timeout is not None else self.default_timeout
        try:
            result = await self._call_user_code(
                definition.fn,
                arguments,
                is_async=definition.is_async,
                timeout=timeout,
                target=_Target("resource", label, "uri"),
                context=context,
            )
        except ServerBusyError:
            done("busy")
            raise
        except _DeadlineExceeded:
            done("timeout")
            raise ProtocolError(
                f"Resource '{label}' timed out after {timeout:g}s", code=TOOL_TIMEOUT
            ) from None
        except ResourceNotFoundError as exc:
            done("not_found")
            missing = exc.uri if exc.uri is not None else uri
            error = self._resource_not_found(missing, stateless, str(exc))
            exc.__traceback__ = None
            raise error from None
        except _UserCancelled as wrapped:
            cause, wrapped.error = wrapped.error, None
            assert cause is not None
            error_id, error = self._user_code_failed("resource", label, cause)
            done("error", error_id=error_id)
            raise error from None
        except ToolError as exc:
            done("tool_error")
            message = str(exc)
            exc.__traceback__ = None
            raise ProtocolError(message, code=INTERNAL_ERROR) from None
        except Exception as exc:
            error_id, error = self._user_code_failed("resource", label, exc)
            done("error", error_id=error_id)
            raise error from None
        try:
            contents = to_resource_contents(result, uri=uri, mime_type=definition.mime_type)
        except NotFound:
            done("not_found")
            raise self._resource_not_found(uri, stateless) from None
        except TypeError as exc:
            error_id, error = self._user_code_failed("resource", label, exc, unsupported=True)
            done("error", error_id=error_id)
            raise error from None
        done("ok")
        read: dict[str, Any] = {"contents": contents}
        if stateless:
            # A retry this server never asked for (it does not implement
            # input requests) is served, and must not be cached.
            retry = "inputResponses" in params or "requestState" in params
            read["ttlMs"] = 0 if retry else definition.cache_ttl_ms
            read["cacheScope"] = self._cache_scope("resources/read", definition, retry=retry)
        return read

    def _find_prompt(self, name: object, identity: ClientIdentity | None) -> PromptDefinition:
        """The prompt *name* names, if *identity* may see it and use it.

        Raises:
            ProtocolError: ``-32602 "Unknown prompt"`` for a missing or hidden
                one, alike; a step-up ``InsufficientScopeError``.
        """
        definition = self._prompts.get(name) if isinstance(name, str) else None
        if definition is None or not self._visible(identity, definition):
            raise ProtocolError(f"Unknown prompt: {name}", code=INVALID_PARAMS)
        self._check_step_up(identity, definition, "prompt")
        try:
            authorize(identity, definition, "Prompt")
        except (AuthenticationError, AuthorizationError):
            raise ProtocolError(f"Unknown prompt: {name}", code=INVALID_PARAMS) from None
        return definition

    async def _get_prompt(self, request: RequestInfo, context: ClientContext) -> dict[str, Any]:
        """Serve ``prompts/get``: check and convert the arguments, run the prompt.

        Raises:
            ProtocolError: ``-32602`` for an unknown prompt or invalid
                arguments (every violation listed), ``-32005``, ``-32008``,
                ``-32603``, or a step-up ``InsufficientScopeError``.
        """
        params = request._params
        name = params.get("name")
        if not isinstance(name, str):
            raise ProtocolError("prompts/get requires a string 'name'", code=INVALID_PARAMS)
        definition = self._find_prompt(name, context.identity)
        started = time.perf_counter()

        def done(status: str, **extra: Any) -> None:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            audit(
                "prompt_get",
                prompt=name,
                client_id=context.client_id,
                duration_ms=duration_ms,
                status=status,
                **extra,
            )

        raw = params.get("arguments")
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            done("denied")
            raise ProtocolError("'arguments' must be an object", code=INVALID_PARAMS)
        try:
            arguments = definition.bind(raw)
        except ValidationError:
            done("denied")
            raise
        timeout = definition.timeout if definition.timeout is not None else self.default_timeout
        try:
            result = await self._call_user_code(
                definition.fn,
                arguments,
                is_async=definition.is_async,
                timeout=timeout,
                target=_Target("prompt", name, "prompt"),
                context=context,
            )
        except ServerBusyError:
            done("busy")
            raise
        except _DeadlineExceeded:
            done("timeout")
            raise ProtocolError(
                f"Prompt '{name}' timed out after {timeout:g}s", code=TOOL_TIMEOUT
            ) from None
        except _UserCancelled as wrapped:
            cause, wrapped.error = wrapped.error, None
            assert cause is not None
            error_id, error = self._user_code_failed("prompt", name, cause)
            done("error", error_id=error_id)
            raise error from None
        except ToolError as exc:
            done("tool_error")
            message = str(exc)
            exc.__traceback__ = None
            raise ProtocolError(message, code=INTERNAL_ERROR) from None
        except Exception as exc:
            error_id, error = self._user_code_failed("prompt", name, exc)
            done("error", error_id=error_id)
            raise error from None
        try:
            messages = to_prompt_messages(result)
        except TypeError as exc:
            error_id, error = self._user_code_failed("prompt", name, exc, unsupported=True)
            done("error", error_id=error_id)
            raise error from None
        done("ok")
        answer: dict[str, Any] = {}
        if definition.description:
            answer["description"] = definition.description
        answer["messages"] = messages
        return answer

    async def _complete(self, request: RequestInfo, context: ClientContext) -> dict[str, Any]:
        """Serve ``completion/complete`` for a prompt argument or a template variable.

        Completions are not audited per request (they arrive per keystroke);
        failures are logged with an ``error_id``.

        Raises:
            ProtocolError: ``-32602`` for a malformed request or an unknown
                (or hidden) prompt, template or argument, ``-32005``,
                ``-32008``, ``-32603``, or a step-up ``InsufficientScopeError``.
        """
        params = request._params
        identity = context.identity
        ref = params.get("ref")
        argument = params.get("argument")
        if not isinstance(ref, dict):
            raise ProtocolError("completion/complete requires a 'ref' object", code=INVALID_PARAMS)
        if (
            not isinstance(argument, dict)
            or not isinstance(argument.get("name"), str)
            or not isinstance(argument.get("value"), str)
        ):
            raise ProtocolError(
                "completion/complete requires 'argument' with a string name and value",
                code=INVALID_PARAMS,
            )
        given = params.get("context")
        given_arguments: Any = None
        if given is not None:
            if not isinstance(given, dict):
                raise ProtocolError("'context' must be an object", code=INVALID_PARAMS)
            given_arguments = given.get("arguments")
        if given_arguments is None:
            given_arguments = {}
        if not isinstance(given_arguments, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in given_arguments.items()
        ):
            raise ProtocolError(
                "'context.arguments' must map argument names to strings", code=INVALID_PARAMS
            )
        name: str = argument["name"]
        value: str = argument["value"]
        ref_type = ref.get("type")
        names: tuple[str, ...]
        completers: Mapping[str, CompletionSource]
        if ref_type == "ref/prompt":
            prompt_name = ref.get("name")
            if not isinstance(prompt_name, str):
                raise ProtocolError("ref/prompt requires a string 'name'", code=INVALID_PARAMS)
            prompt = self._find_prompt(prompt_name, identity)
            names = tuple(param.name for param in prompt.arguments)
            completers = prompt.completers
            timeout = prompt.timeout
            label, field = f"{prompt.name}.{name}", "prompt"
        elif ref_type == "ref/resource":
            uri = ref.get("uri")
            if not isinstance(uri, str):
                raise ProtocolError("ref/resource requires a string 'uri'", code=INVALID_PARAMS)
            template = self._resources.get_template(uri)
            if template is None or not self._visible(identity, template):
                concrete = self._resources.get(uri)
                if concrete is not None and self._visible(identity, concrete):
                    return {"completion": empty()}  # a concrete URI has no arguments
                raise ProtocolError(
                    f"Unknown resource template: {self._label(uri)}", code=INVALID_PARAMS
                )
            self._check_step_up(identity, template, "resource template", self._label(uri))
            try:
                authorize(identity, template, "Resource template")
            except (AuthenticationError, AuthorizationError):
                raise ProtocolError(
                    f"Unknown resource template: {self._label(uri)}", code=INVALID_PARAMS
                ) from None
            names = template.template.variables
            completers = template.completers
            timeout = template.timeout
            label, field = f"{self._label(uri)}.{name}", "uri"
        else:
            raise ProtocolError(
                "ref.type must be 'ref/prompt' or 'ref/resource'", code=INVALID_PARAMS
            )
        if name not in names:
            raise ProtocolError(f"Unknown argument: {name}", code=INVALID_PARAMS)
        source = completers.get(name)
        if source is None:
            return {"completion": empty()}
        if source.values is not None:
            found, total = collect(filter_static(source.values, value), sized=True)
            return {"completion": shape(found, total)}
        # Only the other arguments of the same prompt or template.
        known = {key: text for key, text in given_arguments.items() if key in names}
        work, is_async = _completer_call(source, value, known)
        if timeout is None:
            timeout = self.default_timeout
        try:
            found, total = await self._call_user_code(
                work,
                {},
                is_async=is_async,
                timeout=timeout,
                target=_Target("completion", label, field),
                context=context,
            )
        except ServerBusyError:
            raise
        except _DeadlineExceeded:
            raise ProtocolError(
                f"Completion for '{label}' timed out after {timeout:g}s", code=TOOL_TIMEOUT
            ) from None
        except _UserCancelled as wrapped:
            cause, wrapped.error = wrapped.error, None
            assert cause is not None
            raise self._user_code_failed("completer", label, cause)[1] from None
        except ToolError as exc:
            message = str(exc)
            exc.__traceback__ = None
            raise ProtocolError(message, code=INTERNAL_ERROR) from None
        except Exception as exc:
            raise self._user_code_failed("completer", label, exc)[1] from None
        return {"completion": shape(found, total)}

    # -------------------------------------------------- change notifications

    def _list_entries(
        self, kind: str, identity: ClientIdentity | None
    ) -> tuple[int, list[dict[str, Any]]]:
        """The *kind* list as *identity* gets it, and the registry version it was read at.

        The one source of both the list a client fetches and the digest it
        is told about changes of, so the two never disagree.
        """
        if kind == "resources":
            # One list, as one notification covers both: resources/list and
            # resources/templates/list.
            version, resources, templates = self._resources.snapshot()
            entries = [r.to_mcp() for r in resources if self._visible(identity, r)]
            entries.extend(t.to_mcp() for t in templates if self._visible(identity, t))
            return version, entries
        definitions: list[ToolDefinition] | list[PromptDefinition]
        if kind == "tools":
            version, definitions = self._registry.snapshot()
        elif kind == "prompts":
            version, definitions = self._prompts.snapshot()
        else:
            raise KeyError(kind)
        # Protected items are omitted for callers who could not use them (a
        # token with step-up sees them all, and is challenged on a call).
        return version, [
            definition.to_mcp()
            for definition in definitions
            if self._visible(identity, definition)
        ]

    def _list_version(self, kind: str) -> int:
        """How many changes the registry behind the *kind* list has seen."""
        if kind == "tools":
            return self._registry.version
        if kind == "prompts":
            return self._prompts.version
        if kind == "resources":
            return self._resources.version
        raise KeyError(kind)

    def _visibility_class(self, identity: ClientIdentity | None) -> Hashable:
        """What decides which items *identity* sees, and nothing else.

        Anonymous; a token that sees everything (step-up); or a scope set.
        Every kind's visibility must stay a function of this alone, or the
        digests below would be shared by callers that see different lists.
        """
        if identity is None:
            return None
        if self._steps_up(identity):
            return "step-up"
        return identity.scopes

    def _list_digest(self, kind: str, identity: ClientIdentity | None) -> str:
        """A digest of the *kind* list *identity* sees.

        Kept per visibility class and registry version, so the hundreds of
        clients told about one change share a handful of computations.
        """
        key = (kind, self._visibility_class(identity))
        current = self._list_version(kind)
        with self._digests_lock:
            cached = self._digests.get(key)
            if cached is not None and cached[0] == current:
                self._digests.move_to_end(key)
                return cached[1]
        version, entries = self._list_entries(kind, identity)
        # The list as its clients read it: encoded as tools/list is (str()
        # for what JSON has no type for, every key a string, so keys of
        # mixed types can be sorted), then in canonical form.
        parsed = json.loads(json.dumps(entries, default=str))
        canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("utf-8", "surrogatepass")).hexdigest()[:32]
        with self._digests_lock:
            self._digests[key] = (version, digest)
            self._digests.move_to_end(key)
            while len(self._digests) > _DIGESTS_MAX:
                self._digests.popitem(last=False)
        return digest

    def _list_baselines(self, identity: ClientIdentity | None) -> dict[str, str]:
        """The digest of every announced list as *identity* sees it now.

        A list whose digest cannot be computed is left out, and logged: its
        recipient is not told about that list, rather than the handshake or
        stream that asked failing.
        """
        return self._notifier.baselines(self._list_kinds(), identity)

    def _watch_session(
        self,
        key: str,
        *,
        push: Push,
        identity: ClientIdentity | None,
        client_id: str,
        multiplexed: bool = False,
        baselines: dict[str, str] | None = None,
        anchor: object | None = None,
        subscriptions: Iterable[str] | None = None,
    ) -> _Sink:
        """Announce list changes to the session *key* through *push* from now on.

        On the loop that delivers to it.  *baselines* is what its client was
        last told the lists hold; by default the lists as they are now, which
        is right just before its ``initialize`` result is sent (its client
        lists after that).  A list that differs from its baseline is
        announced at once.  A list whose digest cannot be computed is not
        watched (:meth:`_list_baselines`).  Updates of the resources the
        session is subscribed to go there too: *subscriptions*, read from
        the session's record in the store, or those this process holds.
        """
        current = self._list_baselines(identity)
        sink = self._notifier.watch_session(
            key,
            push=push,
            identity=identity,
            client_id=client_id,
            kinds=current,
            baselines=current if baselines is None else baselines,
            multiplexed=multiplexed,
            anchor=anchor,
            subscriptions=subscriptions,
            # Updates made while no stream is open wait for one only where
            # the session is kept: in this process.
            hold=not self._store.shared,
        )
        if baselines is not None:
            sink.deliver(sorted(current))
        return sink

    def _listen_final(self, subscription_id: Any) -> dict[str, Any]:
        """The completion result of a listen stream the server ends."""
        result = self._modern_result({"_meta": {META_SUBSCRIPTION_ID: subscription_id}})
        return _result_response(subscription_id, result)

    @staticmethod
    def _stream_deadline(identity: ClientIdentity | None) -> float | None:
        """When a stream opened with *identity* must end: its token's expiry (epoch seconds).

        ``None`` for an API key or an anonymous caller.  The token is
        accepted until its expiry plus the clock-skew leeway, and so is the
        stream.
        """
        if identity is None or identity.issuer is None or identity.expires_at is None:
            return None
        return float(identity.expires_at + LEEWAY_SECONDS)

    async def _handle_listen(self, request: RequestInfo, context: ClientContext) -> None:
        """Serve ``subscriptions/listen``: acknowledge it, then wait until the stream ends.

        The acknowledgment and every notification go through
        ``context.push``; so does the completion result when the server ends
        the stream (:meth:`close_subscriptions`, or the token's expiry).

        Raises:
            ProtocolError: ``-32600`` for an id that is no string or number,
                or one open on this channel already; ``-32602`` for a
                malformed filter.
            SubscriptionLimitError: Too many streams are open.
        """
        msg_id = request.request_id
        push = context.push
        if push is None:  # the method table refuses it first
            raise ProtocolError(f"Method not found: {LISTEN_METHOD}", code=METHOD_NOT_FOUND)
        if not valid_subscription_id(msg_id):
            raise ProtocolError(
                "Invalid request: subscriptions/listen needs a string or number id",
                code=INVALID_REQUEST,
            )
        requested, asked = parse_filter(request._params.get("notifications"))
        # Kinds the server does not announce are left out of the
        # acknowledgment and never sent; so is a list whose digest cannot be
        # computed (the sink's kinds).
        kinds = requested & frozenset(self._list_kinds())
        # Resource URIs only with the resources capability, and only those
        # the caller may read: a missing and a protected one look the same.
        honored: tuple[str, ...] | None = None
        if asked is not None and "resources" in self._capabilities():
            honored = self._honored_resources(asked, context.identity)
        sink = self._notifier.open(
            channel=push,
            client_id=context.client_id,
            identity=context.identity,
            subscription_id=msg_id,
            kinds=kinds,
            push=push,
            multiplexed=context.multiplexed,
            max_total=self.max_sessions,
            uris=frozenset(honored or ()),
        )
        request._subscription = sink
        # In the same step as the registration, and flushes run only from
        # loop callbacks: the acknowledgment is the stream's first frame.
        try:
            push(ack_message(msg_id, sink.kinds, honored))
        except Exception:
            self._notifier.drop(sink, "undeliverable")
            raise
        audit(
            "subscription_open",
            client_id=context.client_id,
            subscription_id=msg_id,
            kinds=sorted(sink.kinds),
            **({"resources": len(honored)} if honored is not None else {}),
        )
        timer: asyncio.TimerHandle | None = None
        deadline = self._stream_deadline(context.identity)
        if deadline is not None:
            timer = asyncio.get_running_loop().call_later(
                max(0.0, deadline - time.time()), self._notifier.end, sink, "token_expired"
            )
        try:
            await sink.wait_ended()
        finally:
            if timer is not None:
                timer.cancel()

    def close_subscriptions(self, context: ClientContext, *, reason: str = "closed") -> int:
        """End every subscription opened through *context*, gracefully.

        Each open ``subscriptions/listen`` stream on *context*'s channel gets
        its completion result (and, on a multiplexed channel,
        ``notifications/cancelled``) through ``context.push`` before this
        returns; then its ``dispatch`` returns ``None``.  The session the
        context carries stops receiving list changes, and its resource
        subscriptions end (this is the end of the session, not of one of
        its streams).  Transports call this,
        on the event loop, when a channel ends or the server shuts down, and
        again once the channel's requests still running have finished (an
        ``initialize`` answered meanwhile starts the session's notifications
        anew); *reason* is audited.  Idempotent.

        Returns:
            How many subscriptions ended.
        """
        return self._notifier.close(context.push, context.session_id, reason=reason)

    def _honored_resources(
        self, uris: Iterable[str], identity: ClientIdentity | None
    ) -> tuple[str, ...]:
        """The URIs of a listen's ``resourceSubscriptions`` its acknowledgment honors.

        Those that resolve to a resource *identity* may read, each once, in
        order.  A missing one, a protected one, one a token would need a
        scope for (one acknowledgment cannot carry a partial 403) and one
        over 2048 characters are left out alike.
        """
        honored: list[str] = []
        for uri in dict.fromkeys(uris):
            if len(uri) > _ECHO_URI_MAX:
                continue
            definition, _, _ = self._resolve_resource(uri, identity)
            if definition is None or self._step_up_scope(identity, definition) is not None:
                continue
            try:
                authorize(identity, definition, "Resource")
            except (AuthenticationError, AuthorizationError):
                continue
            honored.append(uri)
        return tuple(honored)

    def notify_resource_updated(self, uri: str) -> int:
        """Tell every client subscribed to the resource *uri* that it changed.

        Thread-safe: call it from anywhere (a sync tool's thread, a file
        watcher, an async task).  Clients get the URI only and read the
        resource again: ``notifications/resources/updated`` on the channel of
        each handshake-era session that subscribed to exactly *uri*
        (``resources/subscribe``), and tagged on each ``subscriptions/listen``
        stream whose acknowledgment honored it.  An update still waiting to
        be written is not queued again.  A session without an open stream
        (a Streamable HTTP session between ``GET /mcp`` streams) gets it when
        one opens, with the in-process store; with a shared store it is
        lost.  Per process: with several workers, call it in each.

        Returns:
            How many subscriptions matched and had the update queued (0 when
            nobody listens); delivery happens on the event loop.

        Raises:
            TypeError: *uri* is not a string.
        """
        if not isinstance(uri, str):
            raise TypeError(f"uri must be a str, got {type(uri).__name__}")
        return self._notifier.publish_resource_updated(uri)

    async def _subscribe(self, request: RequestInfo, context: ClientContext) -> dict[str, Any]:
        """Serve ``resources/subscribe`` (handshake era): watch one resource for the session.

        The URI must resolve to a resource the caller may read now, as for a
        read, without running it.  The session's subscriptions live with it:
        in this process (stdio, a direct ``dispatch``), or in the store's
        session record (the HTTP transports), and end with it.

        Raises:
            ProtocolError: ``-32602`` for a URI that is no string or is too
                long, ``-32002`` for one that resolves to nothing the caller
                may read, ``-32007`` at 1000 URIs, ``-32601`` with a store
                that keeps no subscriptions, ``-32600`` for a session gone,
                or a step-up ``InsufficientScopeError``.
        """
        uri = self._subscription_uri(request)
        identity = context.identity
        definition, _, _ = self._resolve_resource(uri, identity)
        if definition is None:
            raise self._resource_not_found(uri, request.stateless)
        self._check_step_up(identity, definition, "resource", self._label(uri))
        try:
            authorize(identity, definition, "Resource")
        except (AuthenticationError, AuthorizationError):
            raise self._resource_not_found(uri, request.stateless) from None
        key = context.session_id
        handle = context.store_handle
        if handle is None:
            added = self._notifier.subscribe(key, uri, cap=MAX_RESOURCE_SUBSCRIPTIONS)
        else:
            subscribed = await self._update_subscriptions(handle, add=(uri,))
            self._notifier.set_subscriptions(key, subscribed, hold=not self._store.shared)
            added = uri in subscribed
        if not added:
            raise SubscriptionLimitError(
                f"Too many resource subscriptions for this session (at most "
                f"{MAX_RESOURCE_SUBSCRIPTIONS}); unsubscribe from one first"
            )
        self._audit_subscription("resource_subscribe", uri, context)
        return {}

    async def _unsubscribe(self, request: RequestInfo, context: ClientContext) -> dict[str, Any]:
        """Serve ``resources/unsubscribe``: stop watching a resource.  Idempotent.

        Raises:
            ProtocolError: ``-32602`` for a URI that is no string or is too
                long, ``-32601`` with a store that keeps no subscriptions,
                ``-32600`` for a session gone.
        """
        uri = self._subscription_uri(request)
        key = context.session_id
        handle = context.store_handle
        if handle is None:
            self._notifier.unsubscribe(key, uri)
        else:
            subscribed = await self._update_subscriptions(handle, remove=(uri,))
            self._notifier.set_subscriptions(key, subscribed, hold=not self._store.shared)
        self._audit_subscription("resource_unsubscribe", uri, context)
        return {}

    @staticmethod
    def _subscription_uri(request: RequestInfo) -> str:
        uri = request._params.get("uri")
        if not isinstance(uri, str):
            raise ProtocolError(f"{request.method} requires a string 'uri'", code=INVALID_PARAMS)
        if len(uri) > _ECHO_URI_MAX:
            raise ProtocolError(
                f"{request.method}: the URI is longer than {_ECHO_URI_MAX} characters",
                code=INVALID_PARAMS,
            )
        return uri

    async def _update_subscriptions(
        self, handle: StoreHandle, *, add: Iterable[str] = (), remove: Iterable[str] = ()
    ) -> tuple[str, ...]:
        """Change a stored session's subscriptions; every URI it is subscribed to now.

        Raises:
            ProtocolError: ``-32601`` when the store keeps none; ``-32600``
                when the session is gone.
        """
        try:
            subscribed = await handle.update_subscriptions(
                add=tuple(add), remove=tuple(remove), cap=MAX_RESOURCE_SUBSCRIPTIONS
            )
        except NotImplementedError:
            raise ProtocolError(
                "Method not found: this server's store keeps no resource subscriptions",
                code=METHOD_NOT_FOUND,
            ) from None
        if subscribed is None:
            raise ProtocolError(
                "Session not found; send a new initialize request", code=INVALID_REQUEST
            )
        return subscribed

    def _audit_subscription(self, event: str, uri: str, context: ClientContext) -> None:
        audit(
            event,
            uri=uri[:_AUDIT_URI_MAX],
            client_id=context.client_id,
            session_id=context.session_id,
            session_ref=session_ref(context.session_id),
        )

    def _cancel_subscription(self, context: ClientContext, request_id: object) -> bool:
        """End the listen stream *request_id* names on *context*'s channel, as its client asked."""
        return self._notifier.cancel(context.push, request_id)

    async def _handle_notification(
        self, method: str, params: dict[str, Any], context: ClientContext
    ) -> None:
        if method == "notifications/initialized":
            self._logger.debug("client initialized (session %s)", context.session_id)
        elif method == "notifications/cancelled":
            request_id = params.get("requestId")
            # A listen stream ends at once: nothing more is written for it,
            # not even a change already pending.  Its request then returns
            # by itself, unanswered; no other request is looked up, since
            # 1 and 1.0 name two subscriptions but one in_flight entry.
            if self._cancel_subscription(context, request_id):
                return
            # A list or an object is no request id this server handed out.
            task = context.in_flight.get(request_id) if isinstance(request_id, Hashable) else None
            if task is not None:
                task.cancel()
            elif context.store_handle is not None and isinstance(request_id, str | int):
                # Not running here: with a shared store it may run on another
                # worker (which ignores an unknown id too).
                await context.store_handle.cancel_elsewhere(request_id)

    async def _handle_notification_outcome(
        self, request: RequestInfo, context: ClientContext
    ) -> RequestOutcome:
        await self._handle_notification(request.method, request._params, context)
        return RequestOutcome._create()

    async def _execute_tool(self, request: RequestInfo, context: ClientContext) -> ToolOutcome:
        """Check a tools/call, then run the tool middleware chain around the tool.

        Raises:
            ProtocolError: The call is refused before any middleware sees it:
                an unknown or hidden tool, a missing scope (for a token with
                step-up, :class:`InsufficientScopeError`), the session cap,
                invalid arguments.
        """
        params = request._params
        name = params.get("name")
        if not isinstance(name, str):
            raise ProtocolError("tools/call requires a string 'name'", code=INVALID_PARAMS)
        # The definition the request named when it arrived is the one that
        # runs, whatever the registry holds by now.
        definition = request.tool
        # Report protected tools as unknown to unauthorized callers, so their
        # existence is not enumerable.
        if definition is None or not self._visible(context.identity, definition):
            raise ProtocolError(f"Unknown tool: {name}", code=INVALID_PARAMS)

        handle = context.store_handle
        limit = definition.max_calls_per_session
        call_count = 0
        # Whether a unit of the store's count is held for this call.
        reserved = False
        try:
            self._check_step_up(context.identity, definition)
            authorize(context.identity, definition)
            if handle is None:
                call_count = context.tool_calls.get(name, 0)
                if limit is not None and call_count >= limit:
                    raise SessionLimitError(
                        f"Session limit reached for tool '{name}' ({limit} calls)"
                    )
            elif limit is not None:
                # Taken where the count above is checked, so the errors keep
                # their order.  The store's count is shared between workers,
                # and taking a unit is atomic there.
                reservation = await handle.reserve_call(name, limit)
                if reservation is Reservation.LIMIT:
                    raise SessionLimitError(
                        f"Session limit reached for tool '{name}' ({limit} calls)"
                    )
                if reservation is Reservation.GONE:
                    raise ProtocolError(
                        "Session not found; send a new initialize request", code=INVALID_REQUEST
                    )
                reserved = True
            arguments = params.get("arguments")
            if arguments is None:
                arguments = {}
            if not isinstance(arguments, dict):
                raise ProtocolError("'arguments' must be an object", code=INVALID_PARAMS)
            # Middleware reads the plain JSON form; the tool gets its own
            # models, built from a copy of its own: what the tool does to its
            # arguments (from a thread that may outlive the call) never shows
            # in ToolCall.arguments or RequestInfo.params, which the tool
            # itself reaches through current_tool_call().
            plain = validate_arguments(arguments, definition.arguments_schema)
            built = build_param_models(definition.param_models, _copy(plain))
        except ProtocolError as exc:
            if reserved and handle is not None:
                await self._release_call(handle, name)
            # A step-up denial names the scope the client was asked for.
            scope = {}
            if isinstance(exc, InsufficientScopeError):
                scope["scope"] = " ".join(exc.scopes)
            audit(
                "tool_denied",
                tool=name,
                client_id=context.client_id,
                reason=type(exc).__name__,
                **scope,
            )
            raise

        # No await since the cap check (a store reserves atomically), so
        # concurrent calls cannot overshoot it while a middleware awaits; the
        # finally below refunds a call whose tool never started.
        if handle is None:
            context.tool_calls[name] = call_count + 1
        # The tool (and, for a sync tool, its thread) finds this through
        # current_cancel_token(); a cancel or timeout triggers it.
        token = CancelToken()
        token._on_error = self._callback_failed(name, context)
        timeout = definition.timeout if definition.timeout is not None else self.default_timeout
        call = ToolCall._create(request, definition, plain, token, timeout)
        try:
            # Set here rather than around the tool alone, so tool middleware
            # sees the token, the call and the caller too, and context
            # variables it sets reach the tool (a sync tool's thread gets a
            # copy of this context).
            with cancel_scope(token), _tool_call_scope(call), _identity_scope(context.identity):
                run = functools.partial(self._run_tool, call, built, context)
                layers = request._tool_layers
                if not layers:
                    return await run()
                policy = ToolPolicy(self._logger, self.debug)
                return await run_chain(layers, call, run, policy, request)
        except asyncio.CancelledError:
            # Wherever the call was, in a middleware or in the tool.  Only the
            # first trigger counts, so a tool stopped already keeps its reason.
            self._stop_tool(token, CANCELLED, name, context)
            raise
        finally:
            # Work a middleware left behind must not start the tool from now
            # on, when its count may be refunded.
            call._closed = True
            if not call._started:
                # Refused, busy, a middleware failure or an early cancel: it
                # never ran, so it does not count against the session cap.
                if handle is None:
                    context.tool_calls[name] -= 1
                elif reserved:
                    await self._release_call(handle, name)

    async def _release_call(self, handle: StoreHandle, name: str) -> None:
        """Give back a unit of the store's count.  Never raises but ``CancelledError``.

        A unit the store cannot take back now stays spent: that fails safe
        (the cap is reached early), and a session's counts end with it.
        """
        try:
            await handle.release_call(name)
        except Exception:
            self._logger.warning(
                "could not give back a call of tool %r to the store", name, exc_info=True
            )

    async def _run_tool(
        self, call: ToolCall, arguments: dict[str, Any], context: ClientContext
    ) -> ToolOutcome:
        """Run the tool itself: the innermost step of the tool middleware chain.

        Every answer is an outcome, timeouts and a busy server included; only
        a cancellation is raised.
        """
        if call._closed:
            # The call is over and its count may be refunded: this is work a
            # middleware left running past its end, and the tool must not
            # start for it.
            raise asyncio.CancelledError
        definition = call.tool
        name = definition.name
        timeout = call.timeout
        started = time.perf_counter()

        def _duration_ms() -> float:
            return round((time.perf_counter() - started) * 1000, 2)

        def _started() -> None:
            call._started = call.request._tool_started = True

        try:
            result = await self._run_user_code(
                definition.fn,
                arguments,
                is_async=definition.is_async,
                timeout=timeout,
                token=call.cancel_token,
                target=_Target("tool", name, "tool"),
                context=context,
                started=_started,
            )
        except ServerBusyError as exc:
            audit("tool_call", tool=name, client_id=context.client_id, status="busy")
            return ToolOutcome._create(
                "busy", message=str(exc), started=call._started, error_code=exc.code
            )
        except _DeadlineExceeded:
            duration_ms = _duration_ms()
            audit(
                "tool_call",
                tool=name,
                client_id=context.client_id,
                duration_ms=duration_ms,
                status="timeout",
            )
            return ToolOutcome._create(
                "timeout",
                message=f"Tool '{name}' timed out after {timeout:g}s",
                started=True,
                error_code=TOOL_TIMEOUT,
                duration_ms=duration_ms,
            )
        except _UserCancelled as wrapped:
            # The tool raised CancelledError on its own: it failed, and the
            # client is told so.
            error, wrapped.error = wrapped.error, None
            assert error is not None
            return self._tool_failed(name, context, error, _duration_ms())
        except ToolError as exc:
            # Intentional, safe-to-show tool error raised by the tool author.
            try:
                duration_ms = _duration_ms()
                audit(
                    "tool_call",
                    tool=name,
                    client_id=context.client_id,
                    duration_ms=duration_ms,
                    status="tool_error",
                )
                return ToolOutcome._create(
                    "tool_error",
                    message=str(exc),
                    started=True,
                    is_error=True,
                    exception_type=_type_name(exc),
                    duration_ms=duration_ms,
                )
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
            duration_ms = _duration_ms()
            audit(
                "tool_call",
                tool=name,
                client_id=context.client_id,
                duration_ms=duration_ms,
                status="output_schema_error",
                error_id=error_id,
            )
            # The mismatch describes the server's own data, so it stays in the
            # log unless the operator asked for detail.
            detail = f": {'; '.join(exc.errors)}" if self.debug else ""
            message = f"Tool result did not match its output schema (error_id={error_id}){detail}"
            return ToolOutcome._create(
                "output_schema_error",
                message=message,
                started=True,
                is_error=True,
                error_id=error_id,
                duration_ms=duration_ms,
            )

        payload: dict[str, Any] = {
            "content": [{"type": "text", "text": text}],
            "isError": False,
        }
        if structured is not None:
            payload["structuredContent"] = structured

        duration_ms = _duration_ms()
        audit(
            "tool_call",
            tool=name,
            client_id=context.client_id,
            duration_ms=duration_ms,
            status="ok",
        )
        return ToolOutcome._create(
            "ok", message=text, started=True, payload=payload, duration_ms=duration_ms
        )

    async def _run_user_code(
        self,
        fn: Callable[..., Any],
        arguments: Mapping[str, Any],
        *,
        is_async: bool,
        timeout: float | None,
        token: CancelToken,
        target: _Target,
        context: ClientContext,
        started: Callable[[], None] | None = None,
    ) -> Any:
        """Run one registered function (a tool, resource, prompt or completer) and return its value.

        The machinery every call of user code gets: an async function runs
        as a task of its own, a sync one on a thread of its own (counted
        against ``max_sync_workers``), under *timeout*; a cancel or the
        deadline triggers *token* and abandons the call.  *started* is
        called once the function has been started.  The caller sets the
        context variables the function reads (the cancel token, the caller's
        identity) before calling this, so a sync function's thread gets them.

        Raises:
            ServerBusyError: No sync worker was free; the function never started.
            _DeadlineExceeded: *timeout* passed; *token* fired with ``TIMEOUT``.
            asyncio.CancelledError: The call was cancelled; *token* fired with
                ``CANCELLED``.
            _UserCancelled: The function raised ``CancelledError`` although
                nothing cancelled the call (awaiting a shared future another
                waiter cancelled, say): a failure like any other.
            Exception: Whatever the function raised, ``TimeoutError``
                included when it was not the server's deadline.
        """
        deadline: asyncio.Timeout | None = None
        awaitable: Any = None
        # Every cancel of the call (the request's, its caller's, a
        # middleware's timeout) is a cancel of the task this runs in.
        cancels = _task_cancels()
        try:
            if is_async:
                # A task of its own, as asyncio.wait_for gave it on 3.11:
                # a cancel request the function leaves on its task (an old
                # async-timeout, say) must not turn this call's timeout
                # into a cancellation that answers nobody.
                awaitable = asyncio.ensure_future(fn(**arguments))
            else:
                # Sync functions run in a worker thread so they cannot block
                # the event loop.  Python cannot kill that thread, so a
                # cancel or timeout reaches the function through the token.
                awaitable = self._start_sync_call(fn, arguments, token, context, target)
            if started is not None:
                started()
            async with asyncio.timeout(timeout) as deadline:
                return await awaitable
        except TimeoutError:
            if deadline is None or not deadline.expired():
                # The function raised it (a socket read timing out, say): a
                # failure like any other, not the server's deadline.
                raise
            _drop_unreported_error(awaitable)
            self._stop_call(token, TIMEOUT, target, context)
            raise _DeadlineExceeded from None
        except asyncio.CancelledError as exc:
            if _task_cancels() == cancels:
                raise _UserCancelled(exc) from None
            _drop_unreported_error(awaitable)
            self._stop_call(token, CANCELLED, target, context)
            raise

    def _stop_call(
        self, token: CancelToken, reason: str, target: _Target, context: ClientContext
    ) -> None:
        if target.kind == "tool":
            # As tools always called it (a test of the late-finish audit
            # replaces it with one taking exactly these).
            self._stop_tool(token, reason, target.label, context)
        else:
            self._stop_tool(token, reason, target.label, context, kind=target.kind)

    def _tool_failed(
        self, name: str, context: ClientContext, exc: BaseException, duration_ms: float
    ) -> ToolOutcome:
        """The outcome for a tool that raised; logs and audits it."""
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
                message = f"Tool execution failed (error_id={error_id}): {detail}"
            else:
                # Production: opaque message only — no exception text, no trace.
                message = f"Tool execution failed (error_id={error_id})"
            return ToolOutcome._create(
                "error",
                message=message,
                started=True,
                is_error=True,
                error_id=error_id,
                exception_type=_type_name(exc),
                duration_ms=duration_ms,
            )
        finally:
            # The traceback holds this call's frames, which hold the future
            # that holds the exception: without this, every frame of the
            # failed tool, locals and all, waits for a full collection.
            exc.__traceback__ = None

    def _callback_failed(
        self, name: str, context: ClientContext, kind: str = "tool"
    ) -> Callable[[BaseException], None]:
        """How a failing cancel callback of the tool (or other *kind*) *name* is reported."""
        fields: dict[str, Any] = {"tool": name} if kind == "tool" else {"kind": kind, "name": name}

        def failed(exc: BaseException) -> None:
            error_id = uuid.uuid4().hex[:12]
            self._logger.warning(
                "cancel callback of %s %r failed error_id=%s", kind, name, error_id, exc_info=exc
            )
            audit(
                "cancel_callback_failed",
                **fields,
                client_id=context.client_id,
                error_id=error_id,
            )

        return failed

    def _start_sync_call(
        self,
        fn: Callable[..., Any],
        arguments: Mapping[str, Any],
        token: CancelToken,
        context: ClientContext,
        target: _Target,
    ) -> asyncio.Future[Any]:
        """Run a sync function (a tool, resource, prompt or completer) on a thread of its own.

        Returns its future.  A daemon thread rather than a shared pool: a
        function that ignores its token keeps its thread after the call is
        abandoned, and in a pool that thread would hold up unrelated calls
        queued behind it (and, at exit, the interpreter).
        ``max_sync_workers`` bounds them instead, and
        :meth:`wait_for_tool_threads` gives them time at shutdown.

        Raises:
            ServerBusyError: ``max_sync_workers`` sync calls are already running.
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
        kind, label, field = target.kind, target.label, target.field

        def work() -> None:
            try:
                outcome: tuple[bool, Any] = (True, run_in_context(fn, **arguments))
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
                # function that acts (a write, say) may have done so anyway,
                # and this is the record of it.
                fired = token_ref()
                audit(
                    f"{kind}_finished_after_cancel",
                    **{field: label},
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
            self._spawn(f"easy-mcp-{kind}:{label}", work)
        except BaseException:
            if slots is not None:
                slots.release()
            raise
        return future

    def _stop_tool(
        self,
        token: CancelToken,
        reason: str,
        name: str,
        context: ClientContext,
        kind: str = "tool",
    ) -> None:
        """Trigger *token* and run its callbacks off the event loop.

        The flag is set at once, so a function polling ``token.cancelled``
        sees it immediately; callbacks may block (a MySQL ``KILL QUERY``
        opens a connection), so they get a thread of their own.  Never
        raises: it runs while a cancellation or timeout is propagating.
        *kind* is what was called, ``"tool"`` or another kind of call.
        """
        callbacks = token._trigger(reason)
        if not callbacks:
            return
        failed = token._on_error or self._callback_failed(name, context, kind)

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
        An app mounted inside another one runs no lifespan of its own: run
        :meth:`lifespan` from the host app's.
        """
        if not isinstance(self._transport, BaseHTTPTransport):
            self._transport = StreamableHTTPTransport(self)
        return self._transport.build_app()

    def lifespan(self) -> contextlib.AbstractAsyncContextManager[None]:
        """Startup and shutdown of the app :meth:`build_app` returned, for mounting it.

        A Starlette or FastAPI app that mounts ``server.build_app()`` does
        not run the mounted app's lifespan, so run this from its own::

            @contextlib.asynccontextmanager
            async def lifespan(app):
                async with server.lifespan():
                    yield

            mcp_app = server.build_app()
            app = Starlette(routes=[Mount("/", mcp_app)], lifespan=lifespan)

        At startup it warns about a misconfigured store (plaintext to a
        remote Redis, say) and connects it (a shared store that refuses the
        credentials stops the startup; one out of reach is retried, and
        ``/healthz`` answers 503 meanwhile), then fetches the OAuth
        authorization servers' metadata and keys (best effort).  At shutdown
        it closes the streams, cancels the requests still running after
        their grace, ends the sessions (with a shared store, only those
        whose stream this worker holds: other workers serve the rest), gives
        sync tool threads and cancel callbacks 5 s, releases the OAuth fetch
        threads and closes the store.  ``build_app()``'s own lifespan does
        exactly this.
        """
        transport = self._transport if isinstance(self._transport, BaseHTTPTransport) else None
        return self._lifespan(transport)

    @contextlib.asynccontextmanager
    async def _lifespan(self, transport: BaseHTTPTransport | None) -> AsyncIterator[None]:
        if transport is not None:
            transport._reopen()  # an app started again serves anew
        # Here rather than in run(): an app uvicorn serves itself, or one
        # mounted in another app, never calls run().
        self._warn_about_store()
        await self._store.start()
        try:
            if self.oauth is not None:
                await self.oauth.warm_up()
            self._serving = self._started = True
            yield
        finally:
            self._serving = False
            try:
                if transport is not None:
                    # The streams, the requests still running, then the sessions.
                    await transport.close_streams()
                await self.wait_for_tool_threads(THREAD_SHUTDOWN_GRACE)
                if self.oauth is not None:
                    self.oauth.close()
            finally:
                await self._store.aclose()

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
                    "resources": [definition.uri for definition in self.resources],
                    "resource_templates": [
                        definition.uri_template for definition in self.resource_templates
                    ],
                    "prompts": [definition.name for definition in self.prompts],
                    "middleware": [describe(fn) for fn in self._request_middleware],
                    "tool_middleware": [describe(fn) for fn in self._tool_middleware],
                    "auth": self.auth is not None,
                    "oauth": self.oauth is not None,
                    "rate_limit": self._limiter is not None,
                    "store": self._store.describe(),
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
        if not self.auth_configured:
            protected = [d.name for d in self.tools if d.requires_auth]
            if protected:
                self._logger.warning(
                    "tools %s require authentication but no auth is configured; "
                    "they will be unreachable",
                    protected,
                )
            others = [
                *(f"resource {d.uri}" for d in self.resources if d.requires_auth),
                *(f"template {d.uri_template}" for d in self.resource_templates if d.requires_auth),
                *(f"prompt {d.name}" for d in self.prompts if d.requires_auth),
            ]
            if others:
                self._logger.warning(
                    "%s require authentication but no auth is configured; "
                    "they will be unreachable",
                    others,
                )
            if self.host not in ("127.0.0.1", "localhost", "::1") and not isinstance(
                self._transport, StdioTransport
            ):
                self._logger.warning(
                    "binding %s without authentication exposes all public tools "
                    "to the network; configure APIKeyAuth",
                    self.host,
                )
        if self.oauth is not None:
            self._warn_about_oauth(self.oauth)
        # An HTTP transport's app warns about the store as its lifespan starts.
        if not isinstance(self._transport, StdioTransport | BaseHTTPTransport):
            self._warn_about_store()
        if self.debug:
            self._logger.warning("debug mode is ON: clients will receive tracebacks")

    def _warn_about_store(self) -> None:
        for warning in self._store.warnings():
            self._logger.warning("store: %s", warning)

    def _warn_about_oauth(self, oauth: OAuthResourceServer) -> None:
        transport = self._transport
        if isinstance(transport, BaseHTTPTransport):
            path = urlsplit(oauth.resource).path
            # The origin is a valid resource for every endpoint of the host.
            if path and path not in transport._endpoint_paths():
                self._logger.warning(
                    "oauth resource %s names the path %r, but the MCP endpoint is %s: tokens "
                    "must carry the resource clients use. Fine behind a path-rewriting proxy; "
                    "otherwise set resource to the endpoint's public URL",
                    oauth.resource,
                    path,
                    " and ".join(transport._endpoint_paths()),
                )
        for issuer in oauth.authorization_servers:
            if urlsplit(issuer).scheme == "http":
                self._logger.warning(
                    "authorization server %s is reached over plain http (loopback only): "
                    "use https outside development",
                    issuer,
                )
        if not oauth.step_up:
            hidden = [d.name for d in self.tools if d.scopes]
            if hidden:
                self._logger.info(
                    "%d tool(s) are hidden from tokens that lack their scope; clients are "
                    "asked for these scopes up front",
                    len(hidden),
                )
            items: list[Guarded] = [*self.resources, *self.resource_templates, *self.prompts]
            others = [item for item in items if item.scopes]
            if others:
                self._logger.info(
                    "%d resource(s), template(s) or prompt(s) are hidden from tokens that lack "
                    "their scope; clients are asked for these scopes up front",
                    len(others),
                )
