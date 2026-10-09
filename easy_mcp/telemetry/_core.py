"""Telemetry: one span and one duration sample per dispatched message, plus the server's metrics.

:class:`Telemetry` is the built-in hook ``dispatch`` runs every message
through once it has passed the envelope checks and the rate limit, outside
the user's middleware.  It names, attributes and classifies the message as
the OpenTelemetry semantic conventions for MCP describe, and records the same
measurements in two places: the server's own :class:`.prometheus.MetricsRegistry`
(with ``metrics=True``) and the OpenTelemetry metrics API (whenever
``opentelemetry-api`` can be imported and is not switched off).

Telemetry never breaks a request: every step of its own is guarded, and the
first failure is logged as a warning (later ones at debug level).  What the
request raises is never caught here.

Nothing recorded holds tool arguments, results, exception text, raw session
ids, credentials or headers.  Method names outside :data:`KNOWN_METHODS` are
recorded as ``_OTHER``, tools and prompts only when registered, and request
ids and resource URIs are cut short.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
import time
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from ..exceptions import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    RESOURCE_NOT_FOUND_LEGACY,
)
from ..logging import _set_trace_ids
from ..protocol import (
    LISTEN_METHOD,
    META_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    is_modern_request,
    negotiate_protocol_version,
)
from .prometheus import (
    DURATION_BUCKETS,
    OTHER,
    Counter,
    Gauge,
    Histogram,
    MetricsRegistry,
)

if TYPE_CHECKING:
    from ..middleware import RequestOutcome, TransportInfo
    from ..server import MCPServer
    from ..transport.base import ClientContext
    from ._otel import OtelBridge

logger = logging.getLogger("easy_mcp.telemetry")

# Every method of the 2025-11-25 and 2026-07-28 schemas.  Anything else is
# named and labelled _OTHER: a client could otherwise put a megabyte-long
# method name into every span name and label.
KNOWN_METHODS: Final[frozenset[str]] = frozenset(
    {
        "completion/complete",
        "elicitation/create",
        "initialize",
        "logging/setLevel",
        "notifications/cancelled",
        "notifications/elicitation/complete",
        "notifications/initialized",
        "notifications/message",
        "notifications/progress",
        "notifications/prompts/list_changed",
        "notifications/resources/list_changed",
        "notifications/resources/updated",
        "notifications/roots/list_changed",
        "notifications/subscriptions/acknowledged",
        "notifications/tasks/status",
        "notifications/tools/list_changed",
        "ping",
        "prompts/get",
        "prompts/list",
        "resources/list",
        "resources/read",
        "resources/subscribe",
        "resources/templates/list",
        "resources/unsubscribe",
        "roots/list",
        "sampling/createMessage",
        "server/discover",
        "subscriptions/listen",
        "tasks/cancel",
        "tasks/get",
        "tasks/list",
        "tasks/result",
        "tools/call",
        "tools/list",
    }
)

# Codes that say the caller sent something the server could not serve, which
# the conventions do not count as errors.  -32002 is "resource not found" in
# the handshake era; this server sends it for nothing else on a dispatched
# message (an Origin refusal never reaches dispatch).
NON_ERROR_CODES: Final = frozenset(
    {PARSE_ERROR, INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS, RESOURCE_NOT_FOUND_LEGACY}
)

# The final status of a tools/call, as easy_mcp_tool_calls_total and the
# span's easy_mcp.status record it: the tool's own outcome (ToolOutcome.status,
# which middleware can make "refused" or "internal_error"), a refusal before
# the tool ran, or a cancel.
TOOL_STATUSES: Final = frozenset(
    {
        "ok",
        "tool_error",
        "error",
        "output_schema_error",
        "timeout",
        "cancelled",
        "busy",
        "unauthorized",
        "session_limit",
        "invalid_arguments",
        "refused",
        "internal_error",
    }
)

# Why a tools/call of a registered tool was refused before any middleware
# (RequestOutcome._denial), and the status it is counted with.
DENIAL_STATUSES: Final[Mapping[str, str]] = {
    "not_visible": "unauthorized",
    "scope": "unauthorized",
    "session_limit": "session_limit",
    "invalid_arguments": "invalid_arguments",
    "refused": "refused",
}

# Why a request was refused before it was dispatched (easy_mcp_rejected_total).
REJECT_REASONS: Final = frozenset(
    {
        "rate_limited",
        "unauthenticated",
        "origin",
        "credential_mismatch",
        "header_mismatch",
        "payload_too_large",
        "too_many_sessions",
        "auth_rate_limited",
        "store_unavailable",
    }
)

# How much of a client-chosen value a span carries.
REQUEST_ID_MAX: Final = 128
RESOURCE_URI_MAX: Final = 512
STATUS_DESCRIPTION_MAX: Final = 256

# The methods whose params name a resource (mcp.resource.uri).
_URI_METHODS: Final = frozenset({"resources/read", "resources/subscribe", "resources/unsubscribe"})

_HTTP_TRANSPORTS: Final = frozenset({"streamable-http", "sse"})
_KNOWN_TRANSPORTS: Final = frozenset({"stdio", *_HTTP_TRANSPORTS})

# The session id the stateless HTTP requests' contexts carry: no session.
_STATELESS_SESSION: Final = "stateless"

# The attributes of the duration histogram, of everything a span carries.
_DURATION_LABELS: Final[tuple[str, ...]] = (
    "mcp.method.name",
    "gen_ai.operation.name",
    "gen_ai.tool.name",
    "error.type",
    "rpc.response.status_code",
    "mcp.protocol.version",
    "network.transport",
    "network.protocol.name",
)


def is_error_code(code: int) -> bool:
    """Whether a JSON-RPC error *code* counts as an error (``error.type``, span ``ERROR``)."""
    return code not in NON_ERROR_CODES


def hash_session_id(session_id: str) -> str:
    """What spans carry for a session id: 32 hex digits of its SHA-256.

    The id is a bearer capability; its digest still tells sessions apart but
    cannot be replayed.
    """
    return hashlib.sha256(session_id.encode("utf-8", "surrogatepass")).hexdigest()[:32]


def _label_name(key: str) -> str:
    return key.replace(".", "_")


def _labels(attributes: Mapping[str, Any]) -> dict[str, str]:
    """OpenTelemetry attributes as Prometheus labels: dots become underscores."""
    return {_label_name(key): str(value) for key, value in attributes.items()}


@dataclass(slots=True, eq=False)
class _Observation:
    """What the server reports about one message while it is being served.

    Shared with every task and thread the message's work runs in (they copy
    the context, which holds this object), and holding only short strings.
    """

    tool: str | None = None  # the registered tool a tools/call names
    tool_status: str | None = None
    error_id: str | None = None
    listening: bool = False  # a subscriptions/listen stream was established
    subscription_end: str | None = None


_observation: ContextVar[_Observation | None] = ContextVar(
    "easy_mcp_telemetry_observation", default=None
)


def note_error_id(error_id: str) -> None:
    """Record the ``error_id`` the client is answered with on the message being served.

    The span carries it as ``easy_mcp.error_id``, so a trace leads to the log
    line holding the traceback.  Outside a message, it does nothing.
    """
    observation = _observation.get()
    if observation is not None:
        observation.error_id = error_id


@dataclass(slots=True, eq=False)
class _Plan:
    """What is known about a message before it is served."""

    name: str
    method: str  # bucketed: one of KNOWN_METHODS, or _OTHER
    attributes: dict[str, Any]
    notification: bool
    listen: bool
    meta: dict[str, Any] | None
    headers: Mapping[str, str] | None
    started: float = field(default_factory=time.perf_counter)


class Telemetry:
    """A server's spans and metrics.

    Args:
        server: The server it reports on (its registries name tools and prompts).
        metrics: Keep a :class:`~.prometheus.MetricsRegistry` for ``/metrics``.
        otel: The OpenTelemetry bridge, or ``None`` when it is not installed
            or switched off.

    Attributes:
        active: Whether anything is recorded at all; ``dispatch`` skips the
            hook when not.
        registry: The Prometheus registry, ``None`` without ``metrics``.
    """

    def __init__(self, server: MCPServer, *, metrics: bool, otel: OtelBridge | None) -> None:
        self._server = server
        self.otel = otel
        self.registry: MetricsRegistry | None = None
        self.active = metrics or otel is not None
        self._failures_lock = threading.Lock()
        self._failed = False
        # The registry's families; None without metrics.
        self._operation_duration: Histogram | None = None
        self._session_duration: Histogram | None = None
        self._tool_calls: Counter | None = None
        self._tool_calls_active: Gauge | None = None
        self._finished_after_cancel: Counter | None = None
        self._sync_workers: Gauge | None = None
        self._sessions_open: Gauge | None = None
        self._rejected: Counter | None = None
        self._subscriptions_open: Gauge | None = None
        if metrics:
            registry = self.registry = MetricsRegistry()
            self._operation_duration = registry.histogram(
                "mcp_server_operation_duration_seconds",
                "MCP request or notification duration as observed on the server, from the "
                "time it was received until the result or ack was sent.",
                DURATION_BUCKETS,
            )
            self._session_duration = registry.histogram(
                "mcp_server_session_duration_seconds",
                "The duration of handshake-era MCP sessions.",
                DURATION_BUCKETS,
            )
            self._tool_calls = registry.counter(
                "easy_mcp_tool_calls_total", "Tool calls, by final status."
            )
            self._tool_calls_active = registry.gauge(
                "easy_mcp_tool_calls_active", "Tool calls between their validation and answer."
            )
            self._finished_after_cancel = registry.counter(
                "easy_mcp_tool_finished_after_cancel_total",
                "Sync tools that finished after their call was cancelled or timed out.",
            )
            self._sync_workers = registry.gauge(
                "easy_mcp_sync_workers_in_use",
                "Sync tool threads holding a max_sync_workers slot, abandoned ones included.",
            )
            self._sessions_open = registry.gauge(
                "easy_mcp_sessions_open", "Handshake-era sessions this process holds."
            )
            self._rejected = registry.counter(
                "easy_mcp_rejected_total", "Requests refused before they were dispatched."
            )
            self._subscriptions_open = registry.gauge(
                "easy_mcp_subscriptions_open", "Open subscriptions/listen streams."
            )
        if otel is not None:
            # Log lines written while a request is served name its trace.
            _set_trace_ids(otel.current_trace_ids)

    # ---------------------------------------------------------- failures

    def _step_failed(self) -> None:
        """Log a telemetry step that raised: a warning the first time, then at debug level."""
        with self._failures_lock:
            first, self._failed = not self._failed, True
        level = logging.WARNING if first else logging.DEBUG
        logger.log(level, "telemetry failed; the request is unaffected", exc_info=True)

    def _add(
        self,
        family: Counter | Gauge | None,
        instrument: str,
        attributes: Mapping[str, Any],
        amount: float,
    ) -> None:
        """Add *amount* to a counter or gauge in both backends; never raises."""
        try:
            if family is not None:
                family.add(_labels(attributes), amount)
            if self.otel is not None:
                getattr(self.otel, instrument).add(amount, dict(attributes))
        except Exception:
            self._step_failed()

    def _record(
        self,
        family: Histogram | None,
        instrument: str,
        attributes: Mapping[str, Any],
        value: float,
    ) -> None:
        """Record *value* in a histogram of both backends; never raises."""
        try:
            if family is not None:
                family.observe(_labels(attributes), value)
            if self.otel is not None:
                getattr(self.otel, instrument).record(value, dict(attributes))
        except Exception:
            self._step_failed()

    # ----------------------------------------------------------- messages

    async def observe(
        self,
        method: str,
        params: dict[str, Any],
        msg_id: Any,
        is_notification: bool,
        context: ClientContext,
        transport: TransportInfo | None,
        call: Callable[[], Awaitable[dict[str, Any] | None]],
    ) -> dict[str, Any] | None:
        """Serve one message through *call* inside its span; returns what *call* returned.

        The span is current while *call* runs, so spans the message's code
        starts are its children, sync tools' threads included.  A
        ``CancelledError`` (and anything else *call* raises) is re-raised
        unchanged once the span has ended.
        """
        observation = _Observation()
        try:
            plan = self._plan(method, params, msg_id, is_notification, context, transport)
        except Exception:
            self._step_failed()
            return await call()
        observation.tool = plan.attributes.get("gen_ai.tool.name")
        reset = _observation.set(observation)
        span: Any = None
        token: Any = None
        try:
            span, token = self._begin(plan)
            response = await call()
        except asyncio.CancelledError:
            self._end(plan, observation, span, cancelled=True)
            raise
        except BaseException as exc:
            # KeyboardInterrupt or SystemExit from a tool: it still ends here.
            self._end(plan, observation, span, escaped=type(exc).__qualname__)
            raise
        else:
            self._end(plan, observation, span, response=response)
            return response
        finally:
            otel = self.otel
            if token is not None and otel is not None:
                try:
                    otel.deactivate(token)
                except Exception:
                    self._step_failed()
            _observation.reset(reset)

    def _plan(
        self,
        method: str,
        params: dict[str, Any],
        msg_id: Any,
        is_notification: bool,
        context: ClientContext,
        transport: TransportInfo | None,
    ) -> _Plan:
        bucketed = method if method in KNOWN_METHODS else OTHER
        name = bucketed
        attributes: dict[str, Any] = {"mcp.method.name": bucketed}
        if (
            not is_notification
            and isinstance(msg_id, str | int | float)
            and not isinstance(msg_id, bool)
        ):
            attributes["jsonrpc.request.id"] = str(msg_id)[:REQUEST_ID_MAX]
        modern = not is_notification and is_modern_request(method, params)
        version: object
        if modern:
            meta = params.get("_meta")
            version = meta.get(META_PROTOCOL_VERSION) if isinstance(meta, dict) else None
        elif method == "initialize":
            version = negotiate_protocol_version(params.get("protocolVersion"))
        else:
            version = context.protocol_version
        if isinstance(version, str) and version in SUPPORTED_PROTOCOL_VERSIONS:
            attributes["mcp.protocol.version"] = version
        name_of = transport.name if transport is not None else None
        stateless_http = modern and name_of == "streamable-http"
        if not stateless_http and context.session_id != _STATELESS_SESSION:
            attributes["mcp.session.id"] = hash_session_id(context.session_id)
        server = self._server
        if method == "tools/call":
            attributes["gen_ai.operation.name"] = "execute_tool"
            tool = params.get("name")
            if isinstance(tool, str) and tool in server._registry:
                attributes["gen_ai.tool.name"] = tool
                name = f"tools/call {tool}"
        elif method == "prompts/get":
            prompt = params.get("name")
            if isinstance(prompt, str) and prompt in server._prompts:
                attributes["gen_ai.prompt.name"] = prompt
                name = f"prompts/get {prompt}"
        elif method == "completion/complete":
            ref = params.get("ref")
            prompt = ref.get("name") if isinstance(ref, dict) else None
            if (
                isinstance(ref, dict)
                and ref.get("type") == "ref/prompt"
                and isinstance(prompt, str)
                and prompt in server._prompts
            ):
                attributes["gen_ai.prompt.name"] = prompt
                name = f"completion/complete {prompt}"
        if method in _URI_METHODS:
            uri = params.get("uri")
            if isinstance(uri, str):
                attributes["mcp.resource.uri"] = uri[:RESOURCE_URI_MAX]
        headers: Mapping[str, str] | None = None
        if name_of == "stdio":
            attributes["network.transport"] = "pipe"
        elif name_of in _HTTP_TRANSPORTS and transport is not None:
            attributes["network.transport"] = "tcp"
            attributes["network.protocol.name"] = "http"
            if transport.http_version:
                attributes["network.protocol.version"] = transport.http_version
            if transport.client_address:
                attributes["client.address"] = transport.client_address
            if transport.client_port is not None:
                attributes["client.port"] = transport.client_port
            headers = transport.headers
        if name_of in _KNOWN_TRANSPORTS:
            attributes["easy_mcp.transport"] = name_of
        attributes["easy_mcp.client_id"] = context.client_id
        meta = params.get("_meta")
        return _Plan(
            name=name,
            method=bucketed,
            attributes=attributes,
            notification=is_notification,
            listen=method == LISTEN_METHOD and not is_notification,
            meta=meta if isinstance(meta, dict) else None,
            headers=headers,
        )

    def _begin(self, plan: _Plan) -> tuple[Any, Any]:
        """Start the message's span and make it current: ``(span, token)``, or ``(None, None)``.

        The parent is the trace context in ``params._meta``; without one, the
        span current when the message arrived (an enclosing span); without
        that, the HTTP request's ``traceparent`` header.  When ``_meta`` gives
        the parent, the HTTP-level one, if any, becomes a link.
        """
        otel = self.otel
        if otel is None:
            return None, None
        try:
            from_meta = otel.extract_meta(plan.meta) if plan.meta is not None else None
            transport_level = otel.ambient_span_context()
            from_headers: Any = None
            if transport_level is None and plan.headers is not None:
                from_headers = otel.extract_headers(plan.headers)
                if from_headers is not None:
                    transport_level = otel.span_context(from_headers)
            links: list[Any] = []
            parent: Any = None
            if from_meta is not None:
                parent = from_meta
                if (
                    transport_level is not None
                    and transport_level.span_id != otel.span_context(from_meta).span_id
                ):
                    links.append(otel.link(transport_level))
            elif from_headers is not None:
                parent = from_headers
            span = otel.start_server_span(
                plan.name, parent=parent, links=links, attributes=plan.attributes
            )
        except Exception:
            self._step_failed()
            return None, None
        try:
            return span, otel.activate(span, parent)
        except Exception:
            self._step_failed()
            return span, None

    def _end(
        self,
        plan: _Plan,
        observation: _Observation,
        span: Any,
        *,
        response: dict[str, Any] | None = None,
        cancelled: bool = False,
        escaped: str | None = None,
    ) -> None:
        """Classify the message's outcome, record its duration and end its span."""
        try:
            duration = time.perf_counter() - plan.started
            attributes: dict[str, Any] = {}
            error_type: str | None = None
            description: str | None = None
            sample = True
            if escaped is not None:
                error_type = escaped
            elif plan.listen and (cancelled or response is None or observation.listening):
                # A stream that was established has no operation latency, and
                # however it ended (the server closed it, its client cancelled
                # or went away), that was no error.  One cut short by
                # middleware after it began still says what it answered.
                sample = False
                if response is not None:
                    error_type, description = self._classify(plan, response, attributes)
            elif cancelled or (response is None and not plan.notification):
                # The client (or our caller) gave up on it: no result was
                # sent, so there is no duration to sample, and no error.
                sample = False
            elif response is not None:
                error_type, description = self._classify(plan, response, attributes)
            if error_type is not None:
                attributes["error.type"] = error_type
            if observation.tool is not None and observation.tool_status is not None:
                attributes["easy_mcp.status"] = observation.tool_status
                self._add(
                    self._tool_calls,
                    "tool_calls",
                    {
                        "gen_ai.tool.name": observation.tool,
                        "easy_mcp.status": observation.tool_status,
                    },
                    1,
                )
            if observation.error_id is not None:
                attributes["easy_mcp.error_id"] = observation.error_id
            if plan.listen and observation.subscription_end is not None:
                attributes["easy_mcp.subscription.end"] = observation.subscription_end
            if sample:
                every = {**plan.attributes, **attributes}
                labels = {key: every[key] for key in _DURATION_LABELS if key in every}
                self._record(
                    self._operation_duration, "operation_duration", labels, duration
                )
        except Exception:
            self._step_failed()
            attributes, error_type, description = {}, None, None
        if span is None:
            return
        try:
            if attributes:
                span.set_attributes(attributes)
            if error_type is not None:
                assert self.otel is not None
                self.otel.set_error(span, description)
        except Exception:
            self._step_failed()
        try:
            span.end()
        except Exception:
            self._step_failed()

    @staticmethod
    def _classify(
        plan: _Plan, response: dict[str, Any], attributes: dict[str, Any]
    ) -> tuple[str | None, str | None]:
        """``(error.type, status description)`` for an answer; sets ``rpc.response.status_code``."""
        error = response.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            if not isinstance(code, int) or isinstance(code, bool):
                return None, None
            attributes["rpc.response.status_code"] = str(code)
            if not is_error_code(code):
                return None, None
            message = error.get("message")
            description = message[:STATUS_DESCRIPTION_MAX] if isinstance(message, str) else None
            return str(code), description
        result = response.get("result")
        if plan.method == "tools/call" and isinstance(result, dict) and result.get("isError"):
            # An isError result: the tool failed, and said so in its result.
            return "tool_error", None
        return None, None

    # ----------------------------------------------------- tool-level reports

    def note_outcome(self, outcome: RequestOutcome) -> None:
        """Record what tool-level processing answered a ``tools/call`` with.

        For a call refused before any tool middleware, the reason
        ``_execute_tool`` gave (``RequestOutcome._denial``); otherwise the
        status of the tool's outcome, as its tool middleware returned it.
        Request middleware may replace the answer afterwards; the call's
        status stays.  Only the first report of a message counts.
        """
        observation = _observation.get()
        if observation is None or observation.tool is None or observation.tool_status is not None:
            return
        if outcome._denial is not None:
            observation.tool_status = DENIAL_STATUSES.get(outcome._denial, "refused")
        elif outcome.tool is not None:
            observation.tool_status = outcome.tool.status

    def tool_cancelled(self) -> None:
        """The ``tools/call`` being served was cancelled wherever it was."""
        observation = _observation.get()
        if observation is not None and observation.tool is not None:
            if observation.tool_status is None:
                observation.tool_status = "cancelled"

    def tool_started(self, tool: str) -> None:
        if self.active:
            self._add(
                self._tool_calls_active,
                "tool_calls_active",
                {"gen_ai.tool.name": tool},
                1,
            )

    def tool_ended(self, tool: str) -> None:
        if self.active:
            self._add(
                self._tool_calls_active,
                "tool_calls_active",
                {"gen_ai.tool.name": tool},
                -1,
            )

    def late_finish(self, tool: str, status: str) -> None:
        """A sync tool finished after its call was cancelled or timed out.  Any thread."""
        if self.active:
            self._add(
                self._finished_after_cancel,
                "finished_after_cancel",
                {"gen_ai.tool.name": tool, "easy_mcp.status": status},
                1,
            )

    def worker_acquired(self) -> None:
        """A sync call took a ``max_sync_workers`` slot (any thread)."""
        if self.active:
            self._add(self._sync_workers, "sync_workers_in_use", {}, 1)

    def worker_released(self) -> None:
        """A sync call gave its slot back (any thread)."""
        if self.active:
            self._add(self._sync_workers, "sync_workers_in_use", {}, -1)

    # ------------------------------------------------------- transport level

    def rejected(self, reason: str) -> None:
        """A request was refused before it was dispatched (one of :data:`REJECT_REASONS`)."""
        if self.active:
            self._add(self._rejected, "rejected", {"easy_mcp.reason": reason}, 1)

    def session_opened(self, transport: str) -> None:
        if self.active:
            self._add(
                self._sessions_open,
                "sessions_open",
                {"easy_mcp.transport": transport},
                1,
            )

    def session_closed(
        self,
        transport: str,
        *,
        duration_s: float | None,
        protocol_version: str | None,
        http: bool,
    ) -> None:
        """A session ended; its duration is recorded when known."""
        if not self.active:
            return
        self._add(
            self._sessions_open,
            "sessions_open",
            {"easy_mcp.transport": transport},
            -1,
        )
        if duration_s is None:
            return
        attributes: dict[str, str] = {}
        if isinstance(protocol_version, str) and protocol_version in SUPPORTED_PROTOCOL_VERSIONS:
            attributes["mcp.protocol.version"] = protocol_version
        attributes["network.transport"] = "tcp" if http else "pipe"
        if http:
            attributes["network.protocol.name"] = "http"
        self._record(
            self._session_duration,
            "session_duration",
            attributes,
            max(0.0, duration_s),
        )

    def subscription_opened(self, transport: str) -> None:
        """A ``subscriptions/listen`` stream was established for the message being served."""
        observation = _observation.get()
        if observation is not None:
            observation.listening = True
        if self.active:
            self._add(
                self._subscriptions_open,
                "subscriptions_open",
                {"easy_mcp.transport": transport},
                1,
            )

    def subscription_closed(self, transport: str, reason: str | None) -> None:
        """The listen stream of the message being served ended, for *reason*."""
        observation = _observation.get()
        if observation is not None and reason is not None:
            observation.subscription_end = reason
        if self.active:
            self._add(
                self._subscriptions_open,
                "subscriptions_open",
                {"easy_mcp.transport": transport},
                -1,
            )

    # ------------------------------------------------------------- elsewhere

    def capture_context(self) -> Any:
        """The current OpenTelemetry context, for work another thread does for this call."""
        if self.otel is None:
            return None
        try:
            return self.otel.capture()
        except Exception:
            self._step_failed()
            return None

    def run_in(self, captured: Any, work: Callable[[], None]) -> None:
        """Run *work* with *captured* (from :meth:`capture_context`) as the current context."""
        token: Any = None
        if captured is not None and self.otel is not None:
            try:
                token = self.otel.attach(captured)
            except Exception:
                self._step_failed()
        try:
            work()
        finally:
            otel = self.otel
            if token is not None and otel is not None:
                try:
                    otel.detach(token)
                except Exception:
                    self._step_failed()

    def render(self) -> str:
        """The metrics in the Prometheus text format.

        Raises:
            RuntimeError: Metrics were not turned on.
        """
        if self.registry is None:
            raise RuntimeError(
                "metrics are off: build the server with MCPServer(metrics=True) to collect them"
            )
        return self.registry.render()
