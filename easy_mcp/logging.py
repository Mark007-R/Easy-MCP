"""Structured JSON logging and the security audit trail.

All server logs go through the ``easy_mcp`` logger; audit events go through
``easy_mcp.audit``.  Audit callers must never pass secrets — pass API-key
*fingerprints* (see :func:`easy_mcp.security.auth.fingerprint`), never keys.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

LOGGER_NAME = "easy_mcp"
AUDIT_LOGGER_NAME = "easy_mcp.audit"

# Set once a server traces with OpenTelemetry: the current span's
# (trace_id, span_id), or None outside one.
_trace_ids: Callable[[], tuple[str, str] | None] | None = None


def _set_trace_ids(hook: Callable[[], tuple[str, str] | None] | None) -> None:
    """Name the current trace in every JSON log line, through *hook* (``None``: stop)."""
    global _trace_ids
    _trace_ids = hook


def _current_trace_ids() -> tuple[str, str] | None:
    hook = _trace_ids
    if hook is None:
        return None
    try:
        return hook()
    except Exception:  # a log line must never fail for its trace ids
        return None


class JSONLogFormatter(logging.Formatter):
    """Format log records as single-line JSON objects.

    A line written while a traced request is handled also carries the
    current span's ``trace_id`` and ``span_id`` (lower-case hex).
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        ids = _current_trace_ids()
        if ids is not None:
            payload["trace_id"], payload["span_id"] = ids
        event = getattr(record, "event", None)
        if isinstance(event, dict):
            payload["event"] = event
        if record.exc_info:
            # Tracebacks are for server-side logs only; the dispatcher never
            # sends them to clients outside debug mode.
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(*, debug: bool = False, json_logs: bool = True) -> logging.Logger:
    """Attach a stderr handler to the ``easy_mcp`` logger (idempotent)."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    if not any(getattr(handler, "_easy_mcp", False) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler._easy_mcp = True  # type: ignore[attr-defined]
        if json_logs:
            handler.setFormatter(JSONLogFormatter())
        else:
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            )
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def audit(event_type: str, **fields: Any) -> None:
    """Emit a structured audit event.

    Args:
        event_type: Short machine-readable event name, e.g. ``tool_call``.
        **fields: Event payload. Must not contain secrets.
    """
    logging.getLogger(AUDIT_LOGGER_NAME).info(
        event_type, extra={"event": {"type": event_type, **fields}}
    )
