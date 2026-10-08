"""Middleware: your own async code around every request and every tool call.

Two hook points exist.  ``@server.middleware`` wraps every request the server
implements, and ``@server.tool_middleware`` wraps the execution of one tool
call.  Both take what is being served and a ``call_next`` continuation, and
return what ``call_next()`` returned::

    @server.middleware
    async def timing(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
        started = time.perf_counter()
        outcome = await call_next()
        record(request.method, outcome.error_type or "ok", time.perf_counter() - started)
        return outcome

    @server.tool_middleware
    async def tenant_guard(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
        if call.arguments.get("tenant") not in allowed_tenants(call.identity):
            raise ToolError("This key cannot read that tenant.")  # the tool never runs
        return await call_next()

The contract:

* Middleware runs after the built-in checks and cannot skip them.  A request
  has passed the transport's checks, the rate limit and protocol validation;
  a tool call has also passed visibility, scopes, ``max_calls_per_session``
  and argument validation.
* It refuses by raising.  A :class:`~easy_mcp.ProtocolError` becomes that
  JSON-RPC error.  A :class:`~easy_mcp.ToolError` becomes an ``isError``
  result on ``tools/call``, and ``-32603`` with its message elsewhere.  A
  refused call does not run and is not counted against
  ``max_calls_per_session``.  Raising after ``call_next()`` replaces the
  answer, but the tool has run; the audit log records ``tool_result_withheld``.
* It observes and does not rewrite.  ``params``, ``meta`` and ``arguments``
  are deep read-only, and the outcome ``call_next()`` returns describes what
  the client will get without letting it change.  Return exactly that
  object: anything else, or an unexpected exception, fails the request with
  ``-32603`` and an ``error_id``.
* ``call_next()`` may be called once.  It never raises for an outcome (a
  timeout, a busy server, an inner middleware's refusal all arrive as
  outcomes); only a cancellation reaches middleware as ``CancelledError``,
  and it must be re-raised.  It may be awaited in another task (``gather``,
  ``create_task``), but that work is cancelled if the middleware returns or
  raises before it is done.
* The first middleware registered is the outermost, and request middleware
  always encloses tool middleware.
* Notifications and ``server/discover`` pass through request middleware for
  observation only: whatever a middleware does, they are processed.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import inspect
import logging
import uuid
import weakref
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, Literal, Protocol, TypeVar

from .exceptions import INTERNAL_ERROR, ProtocolError, ToolError
from .logging import audit
from .protocol import era_error_code, is_reserved_error_code

if TYPE_CHECKING:
    from .cancellation import CancelToken
    from .decorators import ToolDefinition
    from .security.auth import ClientIdentity

__all__ = [
    "RequestInfo",
    "RequestMiddleware",
    "RequestMiddlewareT",
    "RequestNext",
    "RequestOutcome",
    "ToolCall",
    "ToolMiddleware",
    "ToolMiddlewareT",
    "ToolNext",
    "ToolOutcome",
    "ToolStatus",
    "TransportInfo",
    "current_tool_call",
]

ToolStatus = Literal[
    "ok",
    "tool_error",
    "error",
    "output_schema_error",
    "timeout",
    "busy",
    "refused",
    "internal_error",
]

# Headers that carry credentials or the session capability.  Middleware never
# sees them, so it cannot pass a client's token through to another service.
_REDACTED_HEADERS = frozenset(
    {"authorization", "proxy-authorization", "x-api-key", "cookie", "mcp-session-id"}
)

_EMPTY: Mapping[str, Any] = MappingProxyType({})


def _freeze(value: Any) -> Any:
    """A deep read-only copy of decoded JSON: dicts become mappingproxies, lists tuples."""
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _copy(value: Any) -> Any:
    """A deep copy of decoded JSON: new dicts and lists, the other values shared."""
    if isinstance(value, dict):
        return {key: _copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy(item) for item in value]
    return value


# --------------------------------------------------------------- transport


@dataclass(frozen=True, slots=True, init=False)
class TransportInfo:
    """How a message reached the server.

    Transports build one per HTTP request (or once, for stdio) and pass it to
    :meth:`MCPServer.dispatch <easy_mcp.MCPServer.dispatch>` as
    ``transport=``.  Header names are lowercased and repeated headers joined
    with ``", "``.  ``Authorization``, ``Proxy-Authorization``, ``X-API-Key``,
    ``Cookie`` and ``MCP-Session-Id`` are always left out, whoever builds the
    object.  A header is only as trustworthy as the proxy in front of the
    server: one the client can set, it can forge.

    Attributes:
        name: ``"stdio"``, ``"streamable-http"``, ``"sse"``, or ``"custom"``
            (what middleware sees when ``dispatch`` is called without one).
        client_address: The peer address over HTTP; ``None`` on stdio.
        client_port: The peer port over HTTP.
        http_version: ``"1.1"``, ``"2"``, ... over HTTP.
        headers: The headers of the HTTP request that carried the message.
    """

    name: str
    client_address: str | None
    client_port: int | None
    http_version: str | None
    headers: Mapping[str, str]

    def __init__(
        self,
        name: str = "custom",
        *,
        client_address: str | None = None,
        client_port: int | None = None,
        http_version: str | None = None,
        headers: Mapping[str, str] | Iterable[tuple[str, str]] = (),
    ) -> None:
        items = headers.items() if isinstance(headers, Mapping) else headers
        joined: dict[str, str] = {}
        for raw_name, value in items:
            key = str(raw_name).lower()
            if key in _REDACTED_HEADERS:
                continue
            joined[key] = f"{joined[key]}, {value}" if key in joined else str(value)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "client_address", client_address)
        object.__setattr__(self, "client_port", client_port)
        object.__setattr__(self, "http_version", http_version)
        object.__setattr__(self, "headers", MappingProxyType(joined))

    def __hash__(self) -> int:
        return hash(
            (
                self.name,
                self.client_address,
                self.client_port,
                self.http_version,
                tuple(sorted(self.headers.items())),
            )
        )


_CUSTOM_TRANSPORT = TransportInfo()


# ----------------------------------------------------------------- requests


class RequestInfo:
    """One JSON-RPC message the server implements, as middleware sees it.

    Read-only, apart from :attr:`state`.  The server creates it; middleware
    receives it.
    """

    __slots__ = (
        "_client_id",
        "_identity",
        "_is_notification",
        "_meta",
        "_method",
        "_params",
        "_params_view",
        "_protocol_version",
        "_request_id",
        "_request_layers",
        "_session_id",
        "_state",
        "_stateless",
        "_tool",
        "_tool_layers",
        "_transport",
        "_watch",
        "_watch_baseline",
    )

    _client_id: str
    _identity: ClientIdentity | None
    _is_notification: bool
    _meta: Mapping[str, Any] | None
    _method: str
    _params: dict[str, Any]
    _params_view: Mapping[str, Any] | None
    _protocol_version: str | None
    _request_id: Any
    _request_layers: tuple[RequestMiddleware, ...]
    _session_id: str | None
    _state: dict[str, Any]
    _stateless: bool
    _tool: ToolDefinition | None
    _tool_layers: tuple[ToolMiddleware, ...]
    _transport: TransportInfo
    _watch: weakref.ref[asyncio.Task[Any]] | None
    _watch_baseline: int

    def __init__(self) -> None:
        raise TypeError("RequestInfo is created by the server, not by user code")

    @classmethod
    def _create(
        cls,
        *,
        method: str,
        request_id: Any,
        is_notification: bool,
        stateless: bool,
        protocol_version: str | None,
        client_id: str,
        session_id: str | None,
        identity: ClientIdentity | None,
        transport: TransportInfo | None,
        tool: ToolDefinition | None,
        params: dict[str, Any],
        request_layers: tuple[RequestMiddleware, ...] = (),
        tool_layers: tuple[ToolMiddleware, ...] = (),
    ) -> RequestInfo:
        self = object.__new__(cls)
        self._method = method
        self._request_id = request_id
        self._is_notification = is_notification
        self._stateless = stateless
        self._protocol_version = protocol_version
        self._client_id = client_id
        self._session_id = session_id
        self._identity = identity
        self._transport = transport if transport is not None else _CUSTOM_TRANSPORT
        self._tool = tool
        self._params = params
        self._params_view = None
        self._meta = None
        # Made now rather than on first use: a sync tool's thread and a
        # middleware on the loop may reach for it at the same time.
        self._state = {}
        # The middleware registered when the request arrived; registering
        # more while it is served affects only later requests.
        self._request_layers = request_layers
        self._tool_layers = tool_layers
        self._watch = None
        self._watch_baseline = 0
        return self

    def _watch_current_task(self) -> None:
        """Count every cancel of the current task as a cancel of this request.

        The server calls it from the task that awaits the one middleware runs
        in.  A weak reference: the frames that hold this object must not hold
        the task too, or a cancelled task would keep them in a cycle.
        """
        task = asyncio.current_task()
        if task is not None:
            self._watch = weakref.ref(task)
            self._watch_baseline = task.cancelling()

    def _cancels(self) -> int:
        """How many times the request has been cancelled while middleware served it."""
        task = self._watch() if self._watch is not None else None
        return task.cancelling() - self._watch_baseline if task is not None else 0

    @property
    def method(self) -> str:
        """The JSON-RPC method: always one the server implements, never a client's invention."""
        return self._method

    @property
    def request_id(self) -> Any:
        """The JSON-RPC ``id``, or ``None`` for a notification.

        Client-chosen and possibly long: fine on a span, never a metric label.
        """
        return self._request_id

    @property
    def is_notification(self) -> bool:
        """Whether the message carries no ``id`` (no response will be sent)."""
        return self._is_notification

    @property
    def stateless(self) -> bool:
        """Whether this is a stateless (``2026-07-28``) request."""
        return self._stateless

    @property
    def protocol_version(self) -> str | None:
        """The revision the request is spoken in.

        The ``_meta`` version of a stateless request, the version an
        ``initialize`` will answer with, or the one negotiated earlier on this
        connection or session (``None`` before ``initialize``).
        """
        return self._protocol_version

    @property
    def client_id(self) -> str:
        """The rate-limit key: the API key's fingerprint, or ``ip:<address>``."""
        return self._client_id

    @property
    def session_id(self) -> str | None:
        """The session's id, or ``None`` for a stateless request.

        For an anonymous session this id is a bearer capability: avoid
        exporting it in plain form.
        """
        return self._session_id

    @property
    def identity(self) -> ClientIdentity | None:
        """The authenticated caller (key fingerprint and scopes), or ``None``."""
        return self._identity

    @property
    def transport(self) -> TransportInfo:
        """How the message arrived, headers included (credentials removed)."""
        return self._transport

    @property
    def tool(self) -> ToolDefinition | None:
        """For ``tools/call``: the registered tool the request names, if any.

        Resolved before the visibility and scope checks, so it says nothing
        about whether this caller may use the tool.
        """
        return self._tool

    @property
    def params(self) -> Mapping[str, Any]:
        """A deep read-only copy of the request's ``params``, built on first access."""
        if self._params_view is None:
            self._params_view = _freeze(self._params)
        return self._params_view

    @property
    def meta(self) -> Mapping[str, Any]:
        """``params._meta``, deep read-only; empty when absent.

        Carries the W3C Trace Context keys (``traceparent``, ``tracestate``,
        ``baggage``) when the client propagates a trace.  Client-controlled:
        validate what you read, and never authorize on ``clientInfo``.
        """
        if self._meta is None:
            meta = self._params.get("_meta")
            self._meta = _freeze(meta) if isinstance(meta, dict) else _EMPTY
        return self._meta

    @property
    def state(self) -> dict[str, Any]:
        """Scratch space shared by this request's middleware and its tool.

        The one mutable thing middleware gets.  A sync tool writing to it from
        its thread while a middleware reads it on the loop needs its own lock.
        """
        return self._state

    def __repr__(self) -> str:
        tool = f", tool={self._tool.name!r}" if self._tool is not None else ""
        return (
            f"RequestInfo(method={self._method!r}, request_id={self._request_id!r}"
            f"{tool}, stateless={self._stateless})"
        )


class ToolCall:
    """One admitted ``tools/call``, after every built-in check.  Read-only."""

    __slots__ = (
        "_arguments",
        "_cancel_token",
        "_closed",
        "_plain",
        "_request",
        "_started",
        "_timeout",
        "_tool",
    )

    _arguments: Mapping[str, Any] | None
    _cancel_token: CancelToken
    _closed: bool
    _plain: dict[str, Any]
    _request: RequestInfo
    _started: bool
    _timeout: float | None
    _tool: ToolDefinition

    def __init__(self) -> None:
        raise TypeError("ToolCall is created by the server, not by user code")

    @classmethod
    def _create(
        cls,
        request: RequestInfo,
        tool: ToolDefinition,
        arguments: dict[str, Any],
        cancel_token: CancelToken,
        timeout: float | None,
    ) -> ToolCall:
        self = object.__new__(cls)
        self._request = request
        self._tool = tool
        self._plain = arguments
        self._arguments = None
        self._cancel_token = cancel_token
        self._timeout = timeout
        self._started = False  # whether the tool function was started
        self._closed = False  # whether the call is over: the tool may no longer start
        return self

    @property
    def request(self) -> RequestInfo:
        """The request the call came from."""
        return self._request

    @property
    def tool(self) -> ToolDefinition:
        """The tool being called; the caller may use it."""
        return self._tool

    @property
    def arguments(self) -> Mapping[str, Any]:
        """The validated and normalized arguments, in JSON form, deep read-only.

        Built on first access.  The tool gets a copy of its own (and its own
        Pydantic models), so nothing read here can reach it, and nothing the
        tool does to its arguments shows here, before or after it ran.
        """
        if self._arguments is None:
            self._arguments = _freeze(self._plain)
        return self._arguments

    @property
    def cancel_token(self) -> CancelToken:
        """The call's cancel token, also ``current_cancel_token()`` in middleware."""
        return self._cancel_token

    @property
    def timeout(self) -> float | None:
        """The tool's effective timeout; it covers the tool function only."""
        return self._timeout

    @property
    def identity(self) -> ClientIdentity | None:
        """The authenticated caller, as :attr:`RequestInfo.identity`."""
        return self._request.identity

    @property
    def client_id(self) -> str:
        """The rate-limit key, as :attr:`RequestInfo.client_id`."""
        return self._request.client_id

    @property
    def state(self) -> dict[str, Any]:
        """The request's scratch space, as :attr:`RequestInfo.state`."""
        return self._request.state

    def __repr__(self) -> str:
        return f"ToolCall(tool={self._tool.name!r}, request_id={self._request.request_id!r})"


# ----------------------------------------------------------------- outcomes


class ToolOutcome:
    """What the client will receive for one tool call.  Read-only.

    ``status`` is one of:

    * ``ok``: the result.
    * ``tool_error``: an ``isError`` result with a ``ToolError``'s message,
      raised by the tool or by a middleware.
    * ``error``: an ``isError`` result, ``Tool execution failed (error_id=...)``.
    * ``output_schema_error``: an ``isError`` result; the tool's value broke
      its own output schema.
    * ``timeout``: ``-32005``; the tool ran past its timeout.
    * ``busy``: ``-32008``; every sync worker was taken, and the tool never started.
    * ``refused``: a middleware's ``ProtocolError``.
    * ``internal_error``: ``-32603``; a middleware failed or broke its contract.
    """

    __slots__ = (
        "_data",
        "_duration_ms",
        "_error_code",
        "_error_id",
        "_exception_type",
        "_is_error",
        "_message",
        "_payload",
        "_started",
        "_status",
    )

    _data: Any
    _duration_ms: float | None
    _error_code: int | None
    _error_id: str | None
    _exception_type: str | None
    _is_error: bool
    _message: str
    _payload: dict[str, Any] | None
    _started: bool
    _status: ToolStatus

    def __init__(self) -> None:
        raise TypeError("ToolOutcome is created by the server, not by user code")

    @classmethod
    def _create(
        cls,
        status: ToolStatus,
        *,
        message: str,
        started: bool,
        is_error: bool = False,
        error_code: int | None = None,
        error_id: str | None = None,
        exception_type: str | None = None,
        duration_ms: float | None = None,
        payload: dict[str, Any] | None = None,
        data: Any = None,
    ) -> ToolOutcome:
        self = object.__new__(cls)
        self._status = status
        self._message = message
        self._started = started
        self._is_error = is_error
        self._error_code = error_code
        self._error_id = error_id
        self._exception_type = exception_type
        self._duration_ms = duration_ms
        self._payload = payload
        self._data = data
        return self

    @property
    def status(self) -> ToolStatus:
        """What happened; see the class docstring."""
        return self._status

    @property
    def ok(self) -> bool:
        """Whether the tool returned a result the client gets as is."""
        return self._status == "ok"

    @property
    def started(self) -> bool:
        """Whether the tool function was started."""
        return self._started

    @property
    def is_error(self) -> bool:
        """Whether the client gets a result carrying ``isError: true``."""
        return self._is_error

    @property
    def error_code(self) -> int | None:
        """The JSON-RPC error code when the client gets an error instead of a result."""
        return self._error_code

    @property
    def message(self) -> str:
        """The text block the client reads, or the JSON-RPC error message; sanitized."""
        return self._message

    @property
    def error_id(self) -> str | None:
        """For ``error``, ``output_schema_error`` and ``internal_error``: the log's key."""
        return self._error_id

    @property
    def exception_type(self) -> str | None:
        """The type of what the tool raised (``error``, ``tool_error``); never sent."""
        return self._exception_type

    @property
    def duration_ms(self) -> float | None:
        """The tool function's own time, as audited; ``None`` if it never started."""
        return self._duration_ms

    def _result(self) -> dict[str, Any]:
        """The CallToolResult the client receives (when it gets a result)."""
        if self._payload is not None:
            return self._payload
        return {"content": [{"type": "text", "text": self._message}], "isError": True}

    def __repr__(self) -> str:
        return f"ToolOutcome(status={self._status!r}, started={self._started})"


class RequestOutcome:
    """What the client will receive for one request.  Read-only."""

    __slots__ = ("_data", "_error_code", "_message", "_result", "_tool")

    _data: Any
    _error_code: int | None
    _message: str | None
    _result: Any
    _tool: ToolOutcome | None

    def __init__(self) -> None:
        raise TypeError("RequestOutcome is created by the server, not by user code")

    @classmethod
    def _create(
        cls,
        *,
        result: Any = None,
        error_code: int | None = None,
        message: str | None = None,
        data: Any = None,
        tool: ToolOutcome | None = None,
    ) -> RequestOutcome:
        self = object.__new__(cls)
        self._result = result
        self._error_code = error_code
        self._message = message
        self._data = data
        self._tool = tool
        return self

    @classmethod
    def _of_tool(cls, tool: ToolOutcome) -> RequestOutcome:
        if tool.error_code is not None:
            return cls._create(
                error_code=tool.error_code, message=tool.message, data=tool._data, tool=tool
            )
        return cls._create(result=tool._result(), tool=tool)

    @property
    def error_code(self) -> int | None:
        """The JSON-RPC error code, or ``None`` for a result."""
        return self._error_code

    @property
    def message(self) -> str | None:
        """The JSON-RPC error message, or ``None`` for a result."""
        return self._message

    @property
    def tool(self) -> ToolOutcome | None:
        """For ``tools/call``, once the call was admitted; ``None`` otherwise.

        ``None`` when the call was refused before tool-level processing (an
        unknown tool, a missing scope, invalid arguments, the session cap).
        """
        return self._tool

    @property
    def failed(self) -> bool:
        """Whether the client gets an error or an ``isError`` result."""
        return self._error_code is not None or (self._tool is not None and self._tool.is_error)

    @property
    def error_type(self) -> str | None:
        """The error code as a string, ``"tool_error"`` for an ``isError`` result, or ``None``.

        Low-cardinality by construction, and what the OpenTelemetry MCP
        conventions use for ``error.type``.
        """
        if self._error_code is not None:
            return str(self._error_code)
        if self._tool is not None and self._tool.is_error:
            return "tool_error"
        return None

    def __repr__(self) -> str:
        if self._error_code is not None:
            return f"RequestOutcome(error_code={self._error_code})"
        return f"RequestOutcome(error_type={self.error_type!r})"


# ------------------------------------------------------------------- types

RequestNext = Callable[[], Awaitable[RequestOutcome]]
ToolNext = Callable[[], Awaitable[ToolOutcome]]
RequestMiddleware = Callable[[RequestInfo, RequestNext], Awaitable[RequestOutcome]]
ToolMiddleware = Callable[[ToolCall, ToolNext], Awaitable[ToolOutcome]]
RequestMiddlewareT = TypeVar("RequestMiddlewareT", bound=RequestMiddleware)
ToolMiddlewareT = TypeVar("ToolMiddlewareT", bound=ToolMiddleware)


# ---------------------------------------------------------- current call

_current_call: contextvars.ContextVar[ToolCall | None] = contextvars.ContextVar(
    "easy_mcp_tool_call", default=None
)


def current_tool_call() -> ToolCall | None:
    """The tool call running in this task or thread.

    Inside tool middleware and the tool itself, sync or async, it is the
    call: who made it (:attr:`ToolCall.identity`), the request's ``meta``
    and the :attr:`ToolCall.state` middleware filled.  ``None`` elsewhere,
    including cancel callbacks and a tool function called directly.
    """
    return _current_call.get()


@contextlib.contextmanager
def _tool_call_scope(call: ToolCall) -> Iterator[ToolCall]:
    reset = _current_call.set(call)
    try:
        yield call
    finally:
        _current_call.reset(reset)


# ------------------------------------------------------------ registration


def describe(fn: object) -> str:
    """``module.qualname`` of a middleware, for logs and audit events (<= 200 chars)."""
    target = fn
    while isinstance(target, functools.partial):
        target = target.func
    qualname = getattr(target, "__qualname__", None)
    module = getattr(target, "__module__", None)
    if not isinstance(qualname, str):
        # A callable object: name its class.
        qualname = type(target).__qualname__
        module = type(target).__module__
    name = f"{module}.{qualname}" if isinstance(module, str) and module else qualname
    return name[:200]


def _is_async(fn: object) -> bool:
    """Whether calling *fn* gives a coroutine: an async function, a partial of
    one, or an object whose ``__call__`` (the one that runs, wherever in its
    class hierarchy it is defined) is ``async def``."""
    if inspect.iscoroutinefunction(fn):
        return True
    return inspect.iscoroutinefunction(type(fn).__call__)


def check_middleware(fn: object, kind: str) -> None:
    """Refuse what cannot work as middleware, with a message saying how to fix it.

    Raises:
        TypeError: *fn* is not async, or cannot take ``(request, call_next)``.
    """
    if not callable(fn):
        raise TypeError(f"{kind} must be an async function, got {type(fn).__name__}")
    if not _is_async(fn):
        raise TypeError(
            f"{kind} {describe(fn)} must be an async function: "
            "async def mw(request, call_next) -> outcome. Middleware runs on the event "
            "loop; run blocking work with `await asyncio.to_thread(...)`."
        )
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return  # no signature to check (a builtin, say)
    try:
        signature.bind(None, None)
    except TypeError:
        raise TypeError(
            f"{kind} {describe(fn)} must take two arguments, (request, call_next); "
            f"its signature is {signature}"
        ) from None


# ------------------------------------------------------------------ chains

_OutcomeT = TypeVar("_OutcomeT", RequestOutcome, ToolOutcome)
Stage = Literal["before", "after", "observe"]


class _Policy(Protocol[_OutcomeT]):
    """How one level turns a middleware's refusal, failure or breach into an outcome.

    ``None`` means "serve the message anyway" (observe-only messages).
    """

    def refused(
        self, info: Any, middleware: object, exc: Exception, inner: _OutcomeT | None
    ) -> _OutcomeT | None: ...

    def breached(
        self, info: Any, middleware: object, problem: str, inner: _OutcomeT | None
    ) -> _OutcomeT | None: ...

    def swallowed_cancel(self, info: Any, middleware: object) -> None: ...


async def run_chain(
    layers: Sequence[Callable[[Any, Callable[[], Awaitable[_OutcomeT]]], Awaitable[Any]]],
    info: Any,
    terminal: Callable[[], Awaitable[_OutcomeT]],
    policy: _Policy[_OutcomeT],
    request: RequestInfo,
) -> _OutcomeT:
    """Run *terminal* inside *layers*, the first one outermost.

    The terminal never raises but ``CancelledError``.  Neither does this,
    apart from a middleware's ``KeyboardInterrupt`` or ``SystemExit``.
    *request* is the request being served: a middleware is taken to have
    swallowed a cancellation only if the request itself was cancelled (see
    :meth:`RequestInfo._cancels`).
    """
    if not layers:
        return await terminal()
    return await _Chain(layers, info, terminal, policy, request).layer(0)


class _Chain(Generic[_OutcomeT]):
    """One run of a middleware chain.

    An object rather than nested functions: a closure that calls itself is a
    reference cycle, which would keep every request served through
    middleware, params and arguments included, alive until the cyclic
    collector runs.
    """

    __slots__ = ("_info", "_layers", "_policy", "_reached", "_request", "_terminal")

    _info: Any
    _layers: Sequence[Callable[[Any, Callable[[], Awaitable[_OutcomeT]]], Awaitable[Any]]]
    _policy: _Policy[_OutcomeT]
    _reached: list[_OutcomeT]
    _request: RequestInfo
    _terminal: Callable[[], Awaitable[_OutcomeT]]

    def __init__(
        self,
        layers: Sequence[Callable[[Any, Callable[[], Awaitable[_OutcomeT]]], Awaitable[Any]]],
        info: Any,
        terminal: Callable[[], Awaitable[_OutcomeT]],
        policy: _Policy[_OutcomeT],
        request: RequestInfo,
    ) -> None:
        self._layers = layers
        self._info = info
        self._terminal = terminal
        self._policy = policy
        self._request = request
        self._reached = []

    async def _once(self) -> _OutcomeT:
        # Observe-only messages run their terminal even when a middleware
        # never reached it; it runs at most once either way.
        if not self._reached:
            self._reached.append(await self._terminal())
        return self._reached[0]

    async def layer(self, index: int) -> _OutcomeT:
        if index == len(self._layers):
            return await self._once()
        middleware = self._layers[index]
        info, policy, request = self._info, self._policy, self._request
        produced: list[_OutcomeT] = []
        pending: list[Coroutine[Any, Any, _OutcomeT]] = []
        runner: list[weakref.ref[asyncio.Task[Any]]] = []
        absorbed = [0]

        async def collect() -> _OutcomeT:
            runner.extend(_current_task_ref())
            before = request._cancels()
            outcome = await self.layer(index + 1)
            # A cancel the inner chain took in and answered anyway (a tool
            # that swallows its own, say) is not this layer's doing.
            absorbed[0] = request._cancels() - before
            produced.append(outcome)
            return outcome

        def call_next() -> Awaitable[_OutcomeT]:
            if pending:
                raise RuntimeError("call_next() may be called only once")
            coroutine = collect()
            pending.append(coroutine)
            return coroutine

        baseline = request._cancels()
        try:
            returned = await middleware(info, call_next)
        except asyncio.CancelledError:
            await _abandon(pending, runner)
            raise
        except Exception as exc:
            await _abandon(pending, runner)
            if request._cancels() - absorbed[0] > baseline:
                # A cancellation arrived and was turned into another error.
                _drop_traceback(exc)
                policy.swallowed_cancel(info, middleware)
                raise asyncio.CancelledError from None
            inner = produced[0] if produced else None
            outcome = policy.refused(info, middleware, exc, inner)
            return outcome if outcome is not None else await self._once()
        except BaseException:  # KeyboardInterrupt, SystemExit
            _detach(pending, runner)
            raise
        unawaited = await _abandon(pending, runner)
        if request._cancels() - absorbed[0] > baseline:
            policy.swallowed_cancel(info, middleware)  # caught and not re-raised
            raise asyncio.CancelledError
        if produced and returned is produced[0]:
            return returned
        inner = produced[0] if produced else None
        problem = _breach(returned, called=bool(pending), unawaited=unawaited, done=bool(produced))
        outcome = policy.breached(info, middleware, problem, inner)
        return outcome if outcome is not None else await self._once()


def _current_task_ref() -> list[weakref.ref[asyncio.Task[Any]]]:
    """A weak reference to the running task, if any.

    Weak, because the frames that keep it may end up in that task's own
    CancelledError, and a strong one would close a cycle.
    """
    task = asyncio.current_task()
    return [weakref.ref(task)] if task is not None else []


def _detach(
    pending: list[Coroutine[Any, Any, Any]], runner: list[weakref.ref[asyncio.Task[Any]]]
) -> tuple[asyncio.Task[Any] | None, bool]:
    """Cancel the inner chain a middleware started and left unfinished.

    Returns the task still running it, if there is one, and whether
    ``call_next()`` was called and never awaited at all.
    """
    if not pending:
        return None, False
    coroutine = pending[0]
    state = inspect.getcoroutinestate(coroutine)
    if state == inspect.CORO_CLOSED:
        return None, False  # it ran to its end
    if state == inspect.CORO_CREATED:
        # Never awaited, or handed to a task that has yet to start it.
        task = next((t for t in asyncio.all_tasks() if t.get_coro() is coroutine), None)
        if task is None:
            # Close it, or it would warn when collected.  The inner chain
            # never ran.
            coroutine.close()
            return None, True
    else:
        task = runner[0]() if runner else None
    if task is None or task.done() or task is asyncio.current_task():
        return None, False
    task.cancel()
    return task, False


async def _abandon(
    pending: list[Coroutine[Any, Any, Any]], runner: list[weakref.ref[asyncio.Task[Any]]]
) -> bool:
    """Stop the inner chain a middleware left running in a task of its own.

    A middleware may await ``call_next()`` in another task (``create_task``,
    ``gather``, ``shield``).  If it returns or raises before that work is
    done, the work is cancelled and waited for: it must not go on to run the
    tool once the call has been answered and its count refunded.  Returns
    whether ``call_next()`` was called and never awaited.
    """
    task, unawaited = _detach(pending, runner)
    if task is not None:
        # Its CancelledError stays in it; one of our own, meanwhile, does not.
        await asyncio.wait({task})
    return unawaited


def _breach(returned: object, *, called: bool, unawaited: bool, done: bool) -> str:
    """What a middleware that did not return its outcome did instead, for the log."""
    fix = "return the value of `await call_next()`"
    if not called:
        return f"returned without calling call_next(); call it, and {fix}"
    if unawaited:
        return f"called call_next() without awaiting it; {fix}"
    if not done:
        return f"returned before the call_next() it started had finished; {fix}"
    if returned is None:
        return f"returned None; {fix}"
    kind = type(returned).__name__
    return f"returned a {kind} instead of the outcome call_next() returned; {fix}"


def _drop_traceback(exc: BaseException) -> None:
    """Free the frames an exception (and the ones it chains to) holds."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        current.__traceback__ = None
        current = current.__cause__ or current.__context__


def _type_name(exc: BaseException) -> str:
    """``RuntimeError``, or ``module.Name`` for a type outside the builtins."""
    cls = type(exc)
    if cls.__module__ == "builtins":
        return cls.__qualname__
    return f"{cls.__module__}.{cls.__qualname__}"


class _Level(Generic[_OutcomeT]):
    """Logging and auditing shared by the request and tool levels."""

    __slots__ = ("_debug", "_logger")

    def __init__(self, logger: logging.Logger, debug: bool) -> None:
        self._logger = logger
        self._debug = debug

    def swallowed_cancel(self, info: Any, middleware: object) -> None:
        self._logger.warning(
            "middleware %s swallowed a cancellation; it was re-raised and no response "
            "is sent. Re-raise asyncio.CancelledError.",
            describe(middleware),
        )

    def _failure(
        self,
        middleware: object,
        *,
        method: str,
        tool: str | None,
        client_id: str,
        stage: Stage,
        exc: Exception | None = None,
        problem: str | None = None,
    ) -> tuple[str, str]:
        """Log and audit a failed middleware; returns ``(error_id, client message)``."""
        name = describe(middleware)
        error_id = uuid.uuid4().hex[:12]
        try:
            if exc is None:
                self._logger.error(
                    "middleware %s broke its contract on %s error_id=%s: it %s",
                    name,
                    method,
                    error_id,
                    problem,
                )
            elif isinstance(exc, ProtocolError):
                self._logger.error(
                    "middleware %s raised error code %d on %s, which MCP reserves "
                    "error_id=%s; answered as -32603",
                    name,
                    exc.code,
                    method,
                    error_id,
                    exc_info=exc,
                )
            else:
                self._logger.error(
                    "middleware %s failed on %s error_id=%s", name, method, error_id, exc_info=exc
                )
            fields: dict[str, Any] = {"middleware": name, "method": method}
            if tool is not None:
                fields["tool"] = tool
            audit(
                "middleware_failed",
                **fields,
                client_id=client_id,
                error_id=error_id,
                stage=stage,
            )
            message = f"Internal server error (error_id={error_id})"
            if self._debug and exc is not None:
                message += f": {type(exc).__name__}: {exc}"
            elif self._debug and problem is not None:
                message += f": middleware {name} {problem}"
            return error_id, message
        finally:
            if exc is not None:
                _drop_traceback(exc)


def _refusal(exc: Exception) -> bool:
    """Whether *exc* is a refusal the client may see as it is."""
    if isinstance(exc, ProtocolError):
        return not is_reserved_error_code(exc.code)
    return isinstance(exc, ToolError)


class RequestPolicy(_Level[RequestOutcome]):
    """Request middleware on a message that can be refused."""

    __slots__ = ()

    def refused(
        self,
        info: RequestInfo,
        middleware: object,
        exc: Exception,
        inner: RequestOutcome | None,
    ) -> RequestOutcome:
        stage: Stage = "after" if inner is not None else "before"
        if not _refusal(exc):
            return self._failed(info, middleware, inner, stage, exc=exc)
        try:
            ran = inner.tool if inner is not None and inner.tool is not None else None
            if isinstance(exc, ProtocolError):
                outcome = RequestOutcome._create(
                    error_code=era_error_code(exc.code, stateless=info.stateless),
                    message=str(exc),
                    data=exc.data,
                )
            elif info.method == "tools/call":
                # A ToolError is an isError result the model can read.
                tool = ToolOutcome._create(
                    "tool_error",
                    message=str(exc),
                    started=ran is not None and ran.started,
                    is_error=True,
                    exception_type=_type_name(exc),
                    duration_ms=ran.duration_ms if ran is not None else None,
                )
                outcome = RequestOutcome._of_tool(tool)
            else:
                # Elsewhere there is no result to carry it: -32603 with the
                # message as written, as for any ToolError outside tools/call.
                outcome = RequestOutcome._create(error_code=INTERNAL_ERROR, message=str(exc))
            self._audit_refusal(info, middleware, type(exc).__name__, inner)
            return outcome
        finally:
            _drop_traceback(exc)

    def breached(
        self,
        info: RequestInfo,
        middleware: object,
        problem: str,
        inner: RequestOutcome | None,
    ) -> RequestOutcome:
        stage: Stage = "after" if inner is not None else "before"
        return self._failed(info, middleware, inner, stage, problem=problem)

    def _failed(
        self,
        info: RequestInfo,
        middleware: object,
        inner: RequestOutcome | None,
        stage: Stage,
        *,
        exc: Exception | None = None,
        problem: str | None = None,
    ) -> RequestOutcome:
        _, message = self._failure(
            middleware,
            method=info.method,
            tool=info.tool.name if info.tool is not None else None,
            client_id=info.client_id,
            stage=stage,
            exc=exc,
            problem=problem,
        )
        self._audit_withheld(info, middleware, "middleware_failed", inner)
        return RequestOutcome._create(error_code=INTERNAL_ERROR, message=message)

    def _audit_refusal(
        self, info: RequestInfo, middleware: object, reason: str, inner: RequestOutcome | None
    ) -> None:
        if self._audit_withheld(info, middleware, reason, inner):
            return
        tool = {"tool": info.tool.name} if info.tool is not None else {}
        audit(
            "request_denied",
            method=info.method,
            client_id=info.client_id,
            reason=reason,
            middleware=describe(middleware),
            **tool,
        )

    @staticmethod
    def _audit_withheld(
        info: RequestInfo, middleware: object, reason: str, inner: RequestOutcome | None
    ) -> bool:
        """Audit a tool result that ran but is withheld; whether there was one."""
        if inner is None or inner.tool is None or not inner.tool.started:
            return False
        audit(
            "tool_result_withheld",
            tool=info.tool.name if info.tool is not None else None,
            client_id=info.client_id,
            middleware=describe(middleware),
            status=inner.tool.status,
            reason=reason,
        )
        return True


class ObservePolicy(_Level[RequestOutcome]):
    """Request middleware on a notification or ``server/discover``: never refusable."""

    __slots__ = ()

    def refused(
        self,
        info: RequestInfo,
        middleware: object,
        exc: Exception,
        inner: RequestOutcome | None,
    ) -> RequestOutcome | None:
        try:
            if isinstance(exc, ProtocolError | ToolError):
                # A policy that refuses everything also meets these messages;
                # they are served regardless, and this is not a failure.
                self._logger.debug(
                    "middleware %s refused %s, which cannot be refused; serving it anyway",
                    describe(middleware),
                    info.method,
                )
            else:
                self._failure(
                    middleware,
                    method=info.method,
                    tool=None,
                    client_id=info.client_id,
                    stage="observe",
                    exc=exc,
                )
        finally:
            _drop_traceback(exc)
        return inner

    def breached(
        self,
        info: RequestInfo,
        middleware: object,
        problem: str,
        inner: RequestOutcome | None,
    ) -> RequestOutcome | None:
        self._failure(
            middleware,
            method=info.method,
            tool=None,
            client_id=info.client_id,
            stage="observe",
            problem=problem,
        )
        return inner


class ToolPolicy(_Level[ToolOutcome]):
    """Tool middleware."""

    __slots__ = ()

    def refused(
        self,
        info: ToolCall,
        middleware: object,
        exc: Exception,
        inner: ToolOutcome | None,
    ) -> ToolOutcome:
        stage: Stage = "after" if inner is not None else "before"
        if not _refusal(exc):
            return self._failed(info, middleware, inner, stage, exc=exc)
        try:
            started = info._started
            duration = inner.duration_ms if started and inner is not None else None
            if isinstance(exc, ProtocolError):
                outcome = ToolOutcome._create(
                    "refused",
                    message=str(exc),
                    started=started,
                    error_code=era_error_code(exc.code, stateless=info.request.stateless),
                    data=exc.data,
                    duration_ms=duration,
                )
            else:
                outcome = ToolOutcome._create(
                    "tool_error",
                    message=str(exc),
                    started=started,
                    is_error=True,
                    exception_type=_type_name(exc),
                    duration_ms=duration,
                )
            if not self._audit_withheld(info, middleware, type(exc).__name__, inner):
                audit(
                    "tool_denied",
                    tool=info.tool.name,
                    client_id=info.client_id,
                    reason=type(exc).__name__,
                    middleware=describe(middleware),
                )
            return outcome
        finally:
            _drop_traceback(exc)

    def breached(
        self,
        info: ToolCall,
        middleware: object,
        problem: str,
        inner: ToolOutcome | None,
    ) -> ToolOutcome:
        stage: Stage = "after" if inner is not None else "before"
        return self._failed(info, middleware, inner, stage, problem=problem)

    def _failed(
        self,
        info: ToolCall,
        middleware: object,
        inner: ToolOutcome | None,
        stage: Stage,
        *,
        exc: Exception | None = None,
        problem: str | None = None,
    ) -> ToolOutcome:
        error_id, message = self._failure(
            middleware,
            method="tools/call",
            tool=info.tool.name,
            client_id=info.client_id,
            stage=stage,
            exc=exc,
            problem=problem,
        )
        self._audit_withheld(info, middleware, "middleware_failed", inner)
        started = info._started
        return ToolOutcome._create(
            "internal_error",
            message=message,
            started=started,
            error_code=INTERNAL_ERROR,
            error_id=error_id,
            duration_ms=inner.duration_ms if started and inner is not None else None,
        )

    @staticmethod
    def _audit_withheld(
        info: ToolCall, middleware: object, reason: str, inner: ToolOutcome | None
    ) -> bool:
        """Audit a tool result that ran but is withheld; whether there was one."""
        if not info._started or inner is None:
            return False
        audit(
            "tool_result_withheld",
            tool=info.tool.name,
            client_id=info.client_id,
            middleware=describe(middleware),
            status=inner.status,
            reason=reason,
        )
        return True
