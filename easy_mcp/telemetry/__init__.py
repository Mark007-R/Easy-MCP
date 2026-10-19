"""OpenTelemetry spans and Prometheus metrics for an :class:`~easy_mcp.MCPServer`.

Tracing is on whenever ``opentelemetry-api`` can be imported (``pip install
"easy-mcp-kit[otel]"``) and costs nothing until the application configures an
OpenTelemetry SDK; ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=easy_mcp`` turns
it off.  ``MCPServer(metrics=True)`` keeps the server's metrics and serves
them at ``GET /metrics`` on the HTTP transports.

Only :data:`METRICS_SCOPE` is public; the rest is the server's own.
"""

from __future__ import annotations

from typing import Final

from ._core import Telemetry

#: The scope an API key (or OAuth token) needs to read ``GET /metrics``
#: when the server has auth configured; ``*`` grants it too.
METRICS_SCOPE: Final = "metrics:read"

__all__ = ["METRICS_SCOPE", "Telemetry"]
