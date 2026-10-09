"""A per-server metrics registry and its Prometheus text exposition (format 0.0.4).

Standard library only.  Each :class:`~easy_mcp.MCPServer` built with
``metrics=True`` keeps one :class:`MetricsRegistry`, so two servers in one
process never share (or collide on) a series, which a process-global
registry would.

The metric set is small and fixed, and every label name comes from the
server's own code, never from a client.  Label values are bounded too, and a
family that reaches ``max_series`` distinct label sets folds every further
one into a single series whose values are all ``_OTHER``.
"""

from __future__ import annotations

import logging
import math
import threading
from collections.abc import Mapping, Sequence
from typing import Final

CONTENT_TYPE: Final = "text/plain; version=0.0.4; charset=utf-8"

# The bucket boundaries the OpenTelemetry MCP conventions advise for both
# duration histograms, in seconds.
DURATION_BUCKETS: Final = (
    0.01,
    0.02,
    0.05,
    0.1,
    0.2,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

# Distinct label sets kept per family, as the OpenTelemetry SDK's default
# cardinality limit.
MAX_SERIES_PER_METRIC: Final = 2000

# The value every label of the series that collects a family's overflow has.
OTHER: Final = "_OTHER"

logger = logging.getLogger("easy_mcp.telemetry")

_Key = tuple[tuple[str, str], ...]


def _key(labels: Mapping[str, str]) -> _Key:
    """A label set as a sorted tuple; empty values are left out (an absent label)."""
    return tuple(sorted((name, value) for name, value in labels.items() if value))


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _escape_help(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\n", "\\n")


def _number(value: float) -> str:
    """A sample value: an int when integral, else the float's repr."""
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if math.isnan(value):
        return "NaN"
    if value == int(value):
        return str(int(value))
    return repr(float(value))


def _labels(key: _Key, extra: tuple[str, str] | None = None) -> str:
    pairs = [*key, extra] if extra is not None else list(key)
    if not pairs:
        return ""
    inner = ",".join(f'{name}="{_escape_label(value)}"' for name, value in pairs)
    return "{" + inner + "}"


class _Family:
    """One metric family: its name, help, type and series, guarded by the registry's lock."""

    kind = ""

    def __init__(self, registry: MetricsRegistry, name: str, help: str) -> None:
        self._registry = registry
        self.name = name
        self.help = help
        self._warned = False

    def _slot(self, labels: Mapping[str, str], series: Mapping[_Key, object]) -> _Key:
        """The key *labels* are recorded under; with the lock held."""
        key = _key(labels)
        if key in series or len(series) < self._registry.max_series:
            return key
        if not self._warned:
            self._warned = True
            logger.warning(
                "metric %s reached %d series; further label sets are recorded as %s",
                self.name,
                self._registry.max_series,
                OTHER,
            )
        return tuple((name, OTHER) for name, _ in key)

    def _render(self, lines: list[str]) -> None:
        lines.append(f"# HELP {self.name} {_escape_help(self.help)}")
        lines.append(f"# TYPE {self.name} {self.kind}")


class Counter(_Family):
    """A monotonic counter."""

    kind = "counter"

    def __init__(self, registry: MetricsRegistry, name: str, help: str) -> None:
        super().__init__(registry, name, help)
        self._series: dict[_Key, float] = {}

    def add(self, labels: Mapping[str, str], amount: float = 1.0) -> None:
        if amount < 0:
            raise ValueError("a counter only goes up")
        with self._registry._lock:
            key = self._slot(labels, self._series)
            self._series[key] = self._series.get(key, 0.0) + amount

    def _render(self, lines: list[str]) -> None:
        super()._render(lines)
        for key in sorted(self._series):
            lines.append(f"{self.name}{_labels(key)} {_number(self._series[key])}")


class Gauge(_Family):
    """A value that goes up and down."""

    kind = "gauge"

    def __init__(self, registry: MetricsRegistry, name: str, help: str) -> None:
        super().__init__(registry, name, help)
        self._series: dict[_Key, float] = {}

    def add(self, labels: Mapping[str, str], amount: float) -> None:
        with self._registry._lock:
            key = self._slot(labels, self._series)
            self._series[key] = self._series.get(key, 0.0) + amount

    def _render(self, lines: list[str]) -> None:
        super()._render(lines)
        for key in sorted(self._series):
            lines.append(f"{self.name}{_labels(key)} {_number(self._series[key])}")


class _Buckets:
    __slots__ = ("count", "counts", "sum")

    def __init__(self, size: int) -> None:
        self.counts = [0] * size
        self.count = 0
        self.sum = 0.0


class Histogram(_Family):
    """Observations counted into fixed buckets, with their sum and count."""

    kind = "histogram"

    def __init__(
        self, registry: MetricsRegistry, name: str, help: str, buckets: Sequence[float]
    ) -> None:
        super().__init__(registry, name, help)
        bounds = tuple(float(bound) for bound in buckets)
        if not bounds or list(bounds) != sorted(set(bounds)) or math.isinf(bounds[-1]):
            raise ValueError("buckets must be finite, distinct and increasing")
        self.buckets = bounds
        self._series: dict[_Key, _Buckets] = {}

    def observe(self, labels: Mapping[str, str], value: float) -> None:
        with self._registry._lock:
            key = self._slot(labels, self._series)
            entry = self._series.get(key)
            if entry is None:
                entry = self._series[key] = _Buckets(len(self.buckets))
            for index, bound in enumerate(self.buckets):
                if value <= bound:
                    entry.counts[index] += 1
                    break
            entry.count += 1
            entry.sum += value

    def _render(self, lines: list[str]) -> None:
        super()._render(lines)
        for key in sorted(self._series):
            entry = self._series[key]
            cumulative = 0
            for bound, count in zip(self.buckets, entry.counts, strict=True):
                cumulative += count
                le = ("le", repr(bound))
                lines.append(f"{self.name}_bucket{_labels(key, le)} {cumulative}")
            lines.append(f"{self.name}_bucket{_labels(key, ('le', '+Inf'))} {entry.count}")
            lines.append(f"{self.name}_sum{_labels(key)} {_number(entry.sum)}")
            lines.append(f"{self.name}_count{_labels(key)} {entry.count}")


class MetricsRegistry:
    """The metric families of one server, rendered as Prometheus text.

    Thread-safe: one lock guards every family, so updates may come from the
    event loop and from worker threads alike.

    Args:
        max_series: Distinct label sets kept per family; further ones are
            recorded under one series whose label values are all ``_OTHER``,
            and a warning is logged once per family.
    """

    def __init__(self, *, max_series: int = MAX_SERIES_PER_METRIC) -> None:
        if max_series < 1:
            raise ValueError("max_series must be >= 1")
        self.max_series = max_series
        self._lock = threading.Lock()
        self._families: dict[str, _Family] = {}

    def _register(self, family: _Family) -> None:
        with self._lock:
            if family.name in self._families:
                raise ValueError(f"metric {family.name!r} is registered already")
            self._families[family.name] = family

    def counter(self, name: str, help: str) -> Counter:
        family = Counter(self, name, help)
        self._register(family)
        return family

    def gauge(self, name: str, help: str) -> Gauge:
        family = Gauge(self, name, help)
        self._register(family)
        return family

    def histogram(self, name: str, help: str, buckets: Sequence[float]) -> Histogram:
        family = Histogram(self, name, help, buckets)
        self._register(family)
        return family

    def render(self) -> str:
        """Every family in the text exposition format, families and series sorted.

        Two renders with nothing recorded between them are byte-identical.
        """
        lines: list[str] = []
        with self._lock:
            for name in sorted(self._families):
                self._families[name]._render(lines)
        return "\n".join(lines) + "\n"
