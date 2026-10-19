"""The OpenTelemetry bridge: the only module that imports ``opentelemetry``.

Only the OpenTelemetry *API* is used, as a library should: the application
decides whether anything is recorded by configuring an SDK, or not.  Without
one every call here is a no-op.  ``opentelemetry`` is imported when a server
first needs its telemetry, never when ``easy_mcp`` is imported.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from typing import Any, Final

from .._version import __version__
from .prometheus import DURATION_BUCKETS

# OpenTelemetry's own switch for turning instrumentations off by name.
DISABLE_ENV: Final = "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"

# The instrumentation scope spans and instruments are created under.
SCOPE_NAME: Final = "easy_mcp"

# The keys MCP reserves in params._meta for W3C Trace Context and Baggage;
# no other key of _meta is ever read.
META_KEYS: Final = ("traceparent", "tracestate", "baggage")

# The longest trace-context value read, from _meta or a header: past what
# W3C asks to propagate, and short of what a client could use to bloat spans.
MAX_VALUE_LENGTH: Final = 8192

logger = logging.getLogger("easy_mcp.telemetry")


def disabled_by_environment(environ: Mapping[str, str] | None = None) -> bool:
    """Whether ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` names ``easy_mcp``.

    A comma-separated list; names are compared without surrounding
    whitespace and ignoring case.
    """
    raw = (os.environ if environ is None else environ).get(DISABLE_ENV, "")
    return any(item.strip().lower() == SCOPE_NAME for item in raw.split(","))


def load_bridge() -> OtelBridge | None:
    """The bridge to the global OpenTelemetry providers, or ``None``.

    ``None`` when the environment switches easy_mcp's instrumentation off,
    or when ``opentelemetry-api`` cannot be imported (or is too broken to
    use): the server then runs untraced, with one debug log line.
    """
    if disabled_by_environment():
        logger.debug("tracing is off: %s names easy_mcp", DISABLE_ENV)
        return None
    try:
        return OtelBridge()
    except Exception:
        logger.debug("OpenTelemetry is not available; requests are not traced", exc_info=True)
        return None


class MetaGetter:
    """Reads trace context from a request's ``params._meta``, and nothing else.

    A ``Getter`` for OpenTelemetry propagators.  Only ``traceparent``,
    ``tracestate`` and ``baggage`` are ever looked up, and only a string of
    at most :data:`MAX_VALUE_LENGTH` characters is handed to the W3C parsers.
    MCP says to make no assumptions about these values: anything else is
    ignored, never echoed and never an error.
    """

    def get(self, carrier: Mapping[str, Any], key: str) -> list[str] | None:
        if key not in META_KEYS:
            return None
        value = carrier.get(key)
        if isinstance(value, str) and len(value) <= MAX_VALUE_LENGTH:
            return [value]
        return None

    def keys(self, carrier: Mapping[str, Any]) -> list[str]:
        return [key for key in META_KEYS if self.get(carrier, key) is not None]


META_GETTER: Final = MetaGetter()


class OtelBridge:
    """Spans, context propagation and metric instruments through the OpenTelemetry API.

    Args:
        tracer_provider: The provider spans come from; the global one by
            default.  That is a proxy, so an SDK the application configures
            after the server was built still receives every span.
        meter_provider: The provider instruments come from; the global one
            by default, likewise a proxy.

    Raises:
        ImportError: ``opentelemetry-api`` is not installed.
    """

    def __init__(self, *, tracer_provider: Any = None, meter_provider: Any = None) -> None:
        from opentelemetry import context as context_api
        from opentelemetry import metrics, propagate, trace
        from opentelemetry.context import Context

        self._context_api: Any = context_api
        self._trace: Any = trace
        self._propagate: Any = propagate
        self._empty_context: Any = Context
        self._server_kind: Any = trace.SpanKind.SERVER
        self._error_code: Any = trace.StatusCode.ERROR
        self._status: Any = trace.Status
        self._link: Any = trace.Link
        self._format_trace_id: Any = trace.format_trace_id
        self._format_span_id: Any = trace.format_span_id
        self.tracer: Any = trace.get_tracer(
            SCOPE_NAME, __version__, tracer_provider=tracer_provider
        )
        meter: Any = metrics.get_meter(SCOPE_NAME, __version__, meter_provider=meter_provider)
        self.operation_duration: Any = self._histogram(
            meter,
            "mcp.server.operation.duration",
            "MCP request or notification duration as observed on the receiver from the time "
            "it was received until the result or ack is sent.",
        )
        self.session_duration: Any = self._histogram(
            meter,
            "mcp.server.session.duration",
            "The duration of the MCP session as observed on the MCP server.",
        )
        self.tool_calls: Any = meter.create_counter(
            "easy_mcp.tool.calls", unit="{call}", description="Tool calls, by final status."
        )
        self.tool_calls_active: Any = meter.create_up_down_counter(
            "easy_mcp.tool.calls.active",
            unit="{call}",
            description="Tool calls between their validation and their answer.",
        )
        self.finished_after_cancel: Any = meter.create_counter(
            "easy_mcp.tool.finished_after_cancel",
            unit="{call}",
            description="Sync tools that finished after their call was cancelled or timed out.",
        )
        self.sync_workers_in_use: Any = meter.create_up_down_counter(
            "easy_mcp.sync_workers.in_use",
            unit="{thread}",
            description="Threads of sync tools, resources, prompts and completers holding a "
            "max_sync_workers slot.",
        )
        self.sessions_open: Any = meter.create_up_down_counter(
            "easy_mcp.sessions.open",
            unit="{session}",
            description="Handshake-era sessions this process holds.",
        )
        self.rejected: Any = meter.create_counter(
            "easy_mcp.rejected",
            unit="{request}",
            description="Requests refused before they were dispatched, by reason.",
        )
        self.subscriptions_open: Any = meter.create_up_down_counter(
            "easy_mcp.subscriptions.open",
            unit="{subscription}",
            description="Open subscriptions/listen streams.",
        )

    @staticmethod
    def _histogram(meter: Any, name: str, description: str) -> Any:
        """A histogram in seconds with the conventions' advisory buckets, where the API has them."""
        try:
            return meter.create_histogram(
                name,
                unit="s",
                description=description,
                explicit_bucket_boundaries_advisory=list(DURATION_BUCKETS),
            )
        except TypeError:
            # An API older than 1.30, which the [otel] extra's pin rules out
            # but a transitive dependency may bring: the SDK's default buckets.
            logger.debug("this OpenTelemetry API takes no bucket advice for %s", name)
            return meter.create_histogram(name, unit="s", description=description)

    # ------------------------------------------------------------- context

    def _valid(self, context: Any) -> bool:
        return bool(self._trace.get_current_span(context).get_span_context().is_valid)

    def _extract(self, carrier: Mapping[str, Any], getter: Any = None) -> Any | None:
        """The current context with what *carrier* propagates, if it names a valid span.

        A carrier holding nothing valid gives ``None``, never an empty context
        that would orphan the span.
        """
        propagator = self._propagate.get_global_textmap()
        options = {} if getter is None else {"getter": getter}
        probe = propagator.extract(carrier, context=self._empty_context(), **options)
        if not self._valid(probe):
            return None
        # On the current context, so values the application put there stay.
        return propagator.extract(carrier, context=self._context_api.get_current(), **options)

    def extract_meta(self, meta: Mapping[str, Any]) -> Any | None:
        """The context a request's ``params._meta`` propagates (only its three W3C keys)."""
        if not any(key in meta for key in META_KEYS):
            return None
        return self._extract(meta, META_GETTER)

    def extract_headers(self, headers: Mapping[str, str]) -> Any | None:
        """The context an HTTP request's headers propagate.

        Only the global propagator's own fields are read, whatever their case
        (*headers* has lower-case names, and a field such as X-Ray's
        ``X-Amzn-Trace-Id`` need not).  A header sent more than once arrives
        joined with ``", "``, which the W3C parser refuses, so a repeated
        ``traceparent`` is ignored.
        """
        fields = self._propagate.get_global_textmap().fields
        carrier = {
            name: value
            for name in fields
            if isinstance(value := headers.get(name.lower()), str)
            and len(value) <= MAX_VALUE_LENGTH
        }
        if not carrier:
            return None
        return self._extract(carrier)

    def span_context(self, context: Any) -> Any:
        """The span context *context* names as its current span."""
        return self._trace.get_current_span(context).get_span_context()

    def ambient_span_context(self) -> Any | None:
        """The current span's context when it is valid (an enclosing span), else ``None``."""
        span_context = self._trace.get_current_span().get_span_context()
        return span_context if span_context.is_valid else None

    def link(self, span_context: Any) -> Any:
        return self._link(span_context)

    # --------------------------------------------------------------- spans

    def start_server_span(
        self,
        name: str,
        *,
        parent: Any | None,
        links: Sequence[Any],
        attributes: Mapping[str, Any],
    ) -> Any:
        """Start a ``SERVER`` span; *parent* is a context, or ``None`` for the current one."""
        return self.tracer.start_span(
            name,
            context=parent,
            kind=self._server_kind,
            attributes=dict(attributes),
            links=list(links),
            record_exception=False,
            set_status_on_exception=False,
        )

    def activate(self, span: Any, parent: Any | None) -> Any:
        """Make *span* current, in *parent* (with its baggage) or the current context."""
        base = parent if parent is not None else self._context_api.get_current()
        return self._context_api.attach(self._trace.set_span_in_context(span, base))

    def deactivate(self, token: Any) -> None:
        self._context_api.detach(token)

    def set_error(self, span: Any, description: str | None) -> None:
        """Mark *span* failed; *description* is a server-authored message, or none."""
        span.set_status(self._status(self._error_code, description or None))

    # ------------------------------------------------- context elsewhere

    def capture(self) -> Any:
        """The current OpenTelemetry context, to carry to another thread."""
        return self._context_api.get_current()

    def attach(self, context: Any) -> Any:
        return self._context_api.attach(context)

    def detach(self, token: Any) -> None:
        self._context_api.detach(token)

    def current_trace_ids(self) -> tuple[str, str] | None:
        """``(trace_id, span_id)`` of the current span in lower-case hex, if this process made it.

        ``None`` for an invalid span, and for a remote one: without an SDK
        the server's span is a no-op that stands for the client's own span
        from ``_meta``, whose ids a log line must not carry as the server's.
        """
        span_context = self._trace.get_current_span().get_span_context()
        if not span_context.is_valid or span_context.is_remote:
            return None
        return (
            self._format_trace_id(span_context.trace_id),
            self._format_span_id(span_context.span_id),
        )
