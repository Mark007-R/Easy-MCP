"""Cancellation signals that reach the thread a sync tool runs in.

A sync tool runs in a worker thread, and Python cannot stop a thread from
outside.  When its call is cancelled (``notifications/cancelled``, a closed
stateless connection, a deleted session, stdio shutdown) or runs past its
timeout, the server stops waiting for it, but the thread -- and whatever it
started, such as a database query -- keeps going unless the tool is told.

Every tool call gets a :class:`CancelToken`, reachable from inside the tool
with :func:`current_cancel_token`.  A tool that can stop its work early
registers a callback with :meth:`CancelToken.on_cancel` (kill a query, close
a socket), or checks :attr:`CancelToken.cancelled` between steps::

    from easy_mcp import current_cancel_token

    @server.tool
    def report(query: str) -> list[dict]:
        token = current_cancel_token()
        connection = open_connection()
        remove = token.on_cancel(connection.cancel) if token else None
        try:
            return run(connection, query)
        finally:
            if remove:
                remove()

The server runs the callbacks on a thread of their own, off the event loop,
so a callback may block (open a second connection, say).  Their exceptions
are logged and swallowed.  Tools that never look at the token behave as they
always have.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
import threading
from collections.abc import Callable, Iterator

CANCELLED = "cancelled"
TIMEOUT = "timeout"

_current: contextvars.ContextVar[CancelToken | None] = contextvars.ContextVar(
    "easy_mcp_cancel_token", default=None
)


class CancelToken:
    """The cancellation signal for one tool call; thread-safe."""

    __slots__ = ("__weakref__", "_callbacks", "_event", "_lock", "_reason")

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._callbacks: list[Callable[[], object]] = []
        self._reason: str | None = None

    @property
    def cancelled(self) -> bool:
        """Whether the call has been cancelled or has timed out."""
        return self._event.is_set()

    @property
    def reason(self) -> str | None:
        """``"cancelled"``, ``"timeout"``, or ``None`` while the call is live."""
        return self._reason

    def wait(self, timeout: float | None = None) -> bool:
        """Block until the call is cancelled or *timeout* passes; returns :attr:`cancelled`."""
        return self._event.wait(timeout)

    def on_cancel(self, callback: Callable[[], object]) -> Callable[[], None]:
        """Run *callback* when the call is cancelled; returns a function that unregisters it.

        A callback registered after the call was already cancelled runs at
        once, in the registering thread.  Unregister once the work it would
        stop is finished; a callback that is already running is not waited
        for, so it must tolerate finding its work done.
        """
        with self._lock:
            if not self._event.is_set():
                self._callbacks.append(callback)

                def remove() -> None:
                    with self._lock, contextlib.suppress(ValueError):
                        self._callbacks.remove(callback)

                return remove
        _run_callbacks([callback])
        return _noop

    def cancel(self, reason: str = CANCELLED) -> None:
        """Mark the call cancelled and run the callbacks in this thread."""
        _run_callbacks(self._trigger(reason))

    def _trigger(self, reason: str) -> list[Callable[[], object]]:
        """Set the flag at once; hand back the callbacks for the caller to run.

        Only the first trigger counts: a call that timed out and is then
        cancelled stays a timeout, and its callbacks run once.
        """
        with self._lock:
            if self._event.is_set():
                return []
            self._reason = reason
            self._event.set()
            callbacks, self._callbacks = self._callbacks, []
        return callbacks


def current_cancel_token() -> CancelToken | None:
    """The token of the tool call running in this thread or task.

    ``None`` outside a tool call, e.g. when a tool function is called
    directly rather than through the server.
    """
    return _current.get()


@contextlib.contextmanager
def cancel_scope(token: CancelToken) -> Iterator[CancelToken]:
    """Make *token* the current one for the duration of the block.

    The server does this for every tool call; it is public so that code
    calling a tool function directly (tests, a batch job) can cancel it too.
    """
    reset = _current.set(token)
    try:
        yield token
    finally:
        _current.reset(reset)


def _noop() -> None:
    return None


def _run_callbacks(
    callbacks: list[Callable[[], object]],
    on_error: Callable[[BaseException], None] | None = None,
) -> None:
    for callback in callbacks:
        try:
            callback()
        except Exception as exc:  # a failed callback must not stop the rest
            if on_error is not None:
                on_error(exc)
            else:
                logging.getLogger("easy_mcp").warning("cancel callback failed", exc_info=True)
