"""Progress notifications for a request that is still running.

A client that wants to follow a slow request puts a ``progressToken`` in its
``_meta``.  The function serving it (a tool, a resource, a prompt) may then
say how far it has got::

    from easy_mcp import report_progress

    @server.tool
    def reindex(collection: str) -> str:
        documents = load(collection)
        for done, document in enumerate(documents, start=1):
            index(document)
            report_progress(done, total=len(documents), message=document.title)
        return f"indexed {len(documents)} documents"

:func:`report_progress` never blocks and may be called from a sync function's
thread or from a coroutine.  It does nothing (and returns ``False``) when the
client sent no token, when the channel cannot carry notifications, or once
the request has been answered.

What reaches the client is bounded, however often the function reports: at
most one ``notifications/progress`` per :data:`MIN_INTERVAL_SECONDS`, always
the latest value, and the last report before the answer is sent ahead of it.
The specification requires ``progress`` to increase, so a report that does
not is dropped.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import math
import threading
from collections.abc import Callable, Iterator
from typing import Any

PROGRESS_METHOD = "notifications/progress"

# The shortest time between two notifications of one request.
MIN_INTERVAL_SECONDS = 0.1

_current: contextvars.ContextVar[ProgressReporter | None] = contextvars.ContextVar(
    "easy_mcp_progress", default=None
)


def progress_token(params: dict[str, Any]) -> str | int | None:
    """The progress token a request's ``_meta`` carries; ``None`` without a valid one."""
    meta = params.get("_meta")
    token = meta.get("progressToken") if isinstance(meta, dict) else None
    # A bool is an int in Python, and not a token in JSON.
    if isinstance(token, bool) or not isinstance(token, str | int):
        return None
    return token


def _number(value: object, name: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be a number, not {type(value).__name__}")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


class ProgressReporter:
    """Sends the progress of one request; thread-safe.

    The server makes one for every request that carries a progress token on
    a channel able to deliver notifications, and closes it when the request
    is answered.  Functions reach it through :func:`report_progress`.
    """

    __slots__ = (
        "_closed",
        "_interval",
        "_last",
        "_lock",
        "_loop",
        "_next",
        "_pending",
        "_scheduled",
        "_send",
        "_timer",
        "_token",
    )

    def __init__(
        self,
        token: str | int,
        send: Callable[[dict[str, Any]], None],
        loop: asyncio.AbstractEventLoop,
        interval: float | None = None,
    ) -> None:
        self._token = token
        self._send = send
        self._loop = loop
        self._interval = MIN_INTERVAL_SECONDS if interval is None else interval
        self._lock = threading.Lock()
        self._closed = False
        # The highest progress reported so far: the next must exceed it.
        self._last: int | float | None = None
        # The latest report not yet sent, and whether a flush is on its way.
        self._pending: dict[str, Any] | None = None
        self._scheduled = False
        self._timer: asyncio.TimerHandle | None = None
        # When the next notification may be sent (the loop's clock).
        self._next = 0.0

    @property
    def token(self) -> str | int:
        """The client's progress token."""
        return self._token

    def report(
        self, progress: int | float, total: int | float | None = None, message: str | None = None
    ) -> bool:
        """Record how far the request has got; returns whether the report was taken.

        ``False`` once the request is answered, and for a *progress* that
        does not exceed the last one reported.

        Raises:
            TypeError: *progress* or *total* is not a number, or *message* not a string.
            ValueError: *progress* or *total* is not finite.
        """
        params: dict[str, Any] = {
            "progressToken": self._token,
            "progress": _number(progress, "progress"),
        }
        if total is not None:
            params["total"] = _number(total, "total")
        if message is not None:
            if not isinstance(message, str):
                raise TypeError(f"message must be a string, not {type(message).__name__}")
            params["message"] = message
        with self._lock:
            if self._closed:
                return False
            if self._last is not None and progress <= self._last:
                return False
            self._last = progress
            self._pending = params
            if self._scheduled:
                return True  # the flush on its way sends this one instead
            self._scheduled = True
        try:
            self._loop.call_soon_threadsafe(self._flush_when_due)
        except RuntimeError:  # the loop has closed: nobody is left to tell
            with self._lock:
                self._closed = True
            return False
        return True

    def _flush_when_due(self) -> None:
        """On the loop: send what is pending, now or once the interval has passed."""
        delay = self._next - self._loop.time()
        if delay <= 0:
            self._flush()
        else:
            self._timer = self._loop.call_later(delay, self._flush)

    def _flush(self) -> None:
        self._timer = None
        with self._lock:
            params, self._pending = self._pending, None
            self._scheduled = False
            if self._closed or params is None:
                return
        self._next = self._loop.time() + self._interval
        self._deliver(params)

    def _deliver(self, params: dict[str, Any]) -> None:
        try:
            self._send({"jsonrpc": "2.0", "method": PROGRESS_METHOD, "params": params})
        except Exception:
            # The channel has ended; the request may run on, unheard.
            with self._lock:
                self._closed = True

    def _close(self, *, flush: bool = True) -> None:
        """On the loop: the request is over.  With *flush*, its last report is sent first."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        with self._lock:
            if self._closed:
                return
            self._closed = True
            params, self._pending = self._pending, None
        if flush and params is not None:
            # The channel may have ended already; the request is over either way.
            with contextlib.suppress(Exception):
                self._send({"jsonrpc": "2.0", "method": PROGRESS_METHOD, "params": params})


def current_progress() -> ProgressReporter | None:
    """The reporter of the request being served in this thread or task, if it has one."""
    return _current.get()


def report_progress(
    progress: int | float, total: int | float | None = None, message: str | None = None
) -> bool:
    """Tell the client how far the request being served has got.

    Call it from a tool, resource or prompt function (or from middleware).
    *progress* must grow from one call to the next; *total* is the value it
    will end at, when known, and *message* a short human-readable note.

    Returns:
        Whether the report was taken.  ``False`` when the client asked for
        no progress, the channel cannot carry it, the request has been
        answered, or *progress* did not grow.

    Raises:
        TypeError: *progress* or *total* is not a number, or *message* not a string.
        ValueError: *progress* or *total* is not finite.
    """
    reporter = _current.get()
    if reporter is None:
        # Still checked, so a mistake shows without a client that asks for progress.
        _number(progress, "progress")
        if total is not None:
            _number(total, "total")
        if message is not None and not isinstance(message, str):
            raise TypeError(f"message must be a string, not {type(message).__name__}")
        return False
    return reporter.report(progress, total, message)


@contextlib.contextmanager
def _progress_scope(reporter: ProgressReporter | None) -> Iterator[None]:
    reset = _current.set(reporter)
    try:
        yield
    finally:
        _current.reset(reset)

