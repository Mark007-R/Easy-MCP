"""Structured JSON logging and the security audit trail.

All server logs go through the ``easy_mcp`` logger; audit events go through
``easy_mcp.audit``.  Audit callers must never pass secrets — pass API-key
*fingerprints* (see :func:`easy_mcp.security.auth.fingerprint`), never keys.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

LOGGER_NAME = "easy_mcp"
AUDIT_LOGGER_NAME = "easy_mcp.audit"


class JSONLogFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
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
