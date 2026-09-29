"""Stopping a connector's database work when its tool call is cancelled.

The server stops waiting for a cancelled or timed-out call at once, but the
statement it started keeps running on the database until something tells
the database to stop.  Each connector registers that something -- ``KILL
QUERY``, a Postgres cancel request, ``sqlite3.Connection.interrupt``,
MongoDB ``killSessions`` -- through :func:`on_cancel` while a statement runs.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from ..cancellation import TIMEOUT, current_cancel_token
from ..exceptions import ToolError
from ..logging import LOGGER_NAME
from ..server import MCPServer


def _noop() -> None:
    return None


def on_cancel(stop: Callable[[], object]) -> Callable[[], None]:
    """Run *stop* if the current call is cancelled; returns the unregister function.

    A no-op outside a tool call (a tool function called directly).
    """
    token = current_cancel_token()
    if token is None:
        return _noop
    return token.on_cancel(stop)


def cancelled_error() -> ToolError:
    """The error a statement stopped by a cancel ends with.

    Nobody receives it (the client already has its cancellation or timeout
    error); it keeps the log accurate.
    """
    token = current_cancel_token()
    if token is not None and token.reason == TIMEOUT:
        return ToolError("Database error: stopped because the tool call timed out")
    return ToolError("Database error: stopped because the tool call was cancelled")


def raise_if_cancelled() -> None:
    """Refuse to start database work for a call that is already cancelled.

    Raises:
        ToolError: The current call has been cancelled or has timed out.
    """
    token = current_cancel_token()
    if token is not None and token.cancelled:
        raise cancelled_error()


def warn_if_server_gives_up_first(server: MCPServer, statement_timeout: float) -> None:
    """Warn when the server's tool timeout would cut statements off first.

    The call is then stopped by the server timeout (which also stops the
    statement) instead of by the database's own limit, so the statement
    limit never applies and clients see a timeout error that does not name
    it.
    """
    timeout = server.default_timeout
    if timeout is not None and timeout <= statement_timeout:
        logging.getLogger(LOGGER_NAME).warning(
            "default_timeout (%gs) is not longer than the statement timeout (%gs): "
            "the server's tool timeout ends calls before the statement limit does; "
            "raise default_timeout or lower the statement timeout",
            timeout,
            statement_timeout,
        )
