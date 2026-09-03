"""stdio transport: newline-delimited JSON-RPC over stdin/stdout.

This is the transport Claude Desktop, Claude Code (``claude mcp add``), and
most local MCP clients use: the client *launches* the server as a child
process, writes one JSON-RPC message per line to its stdin, and reads one
JSON-RPC message per line from its stdout.  Logs go to stderr, so they never
corrupt the protocol stream.

Security handled here (before anything reaches the dispatcher):

* The client is the parent process, so there is no network attack surface.
  Its credential is an optional API key taken from ``api_key=`` or the
  ``EASY_MCP_STDIO_API_KEY`` environment variable; an invalid key fails fast
  at startup rather than silently downgrading to anonymous access.
* Every input line is capped at ``max_request_bytes``; an oversized line is
  discarded (not buffered) and answered with a ``-32004`` error.
* ``sys.stdout`` is redirected to stderr while serving, so a stray ``print``
  inside a tool cannot break the protocol stream.

The dispatcher is shared with every other transport, so validation, auth
decisions, rate limits, timeouts, and error sanitization apply unchanged.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import sys
import threading
from typing import Any, BinaryIO

from ..exceptions import PARSE_ERROR, PAYLOAD_TOO_LARGE, AuthenticationError
from ..logging import audit
from .base import ClientContext, Transport

API_KEY_ENV_VAR = "EASY_MCP_STDIO_API_KEY"

_EOF = None  # queue sentinel: stdin closed or stop() requested
_TOO_LARGE = object()  # queue sentinel: a line exceeded max_request_bytes


def _error_response(msg_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


class StdioTransport(Transport):
    """Serve MCP over the process's standard streams.

    Args:
        server: The :class:`~easy_mcp.server.MCPServer` to expose.
        api_key: Credential the (local) client presents.  Defaults to the
            ``EASY_MCP_STDIO_API_KEY`` environment variable; ``None`` means
            anonymous (public tools only).  Only meaningful when the server
            has ``auth`` configured.
        stdin: Binary stream to read requests from (defaults to the real
            stdin; injectable for tests).
        stdout: Binary stream to write responses to (defaults to the real
            stdout; injectable for tests).
        shutdown_timeout: Seconds to let in-flight tool calls finish after
            stdin closes before they are cancelled.
    """

    def __init__(
        self,
        server: Any,
        *,
        api_key: str | None = None,
        stdin: BinaryIO | None = None,
        stdout: BinaryIO | None = None,
        shutdown_timeout: float = 5.0,
    ) -> None:
        super().__init__(server)
        if shutdown_timeout < 0:
            raise ValueError("shutdown_timeout must be >= 0")
        self._api_key = api_key if api_key is not None else os.environ.get(API_KEY_ENV_VAR)
        self._stdin_override = stdin
        self._stdout_override = stdout
        self._shutdown_timeout = shutdown_timeout
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[Any] | None = None
        self._stdout: BinaryIO | None = None
        self._write_lock = threading.Lock()

    def describe(self) -> str:
        return "stdio"

    # -------------------------------------------------------------- lifecycle

    def run(self) -> None:
        """Serve blocking until stdin closes or :meth:`stop` is called."""
        asyncio.run(self.serve())

    async def serve(self) -> None:
        """Serve on the current event loop until stdin closes or ``stop()``.

        Raises:
            AuthenticationError: The configured ``api_key`` is invalid.
        """
        # Fail fast: a bad key must not silently become anonymous access.
        try:
            identity = self._server.authenticate_key(self._api_key)
        except AuthenticationError:
            audit("stdio_auth_failed")
            raise

        session_id = "stdio-" + secrets.token_urlsafe(12)
        client_id = identity.fingerprint if identity else "stdio"
        context = ClientContext(client_id=client_id, session_id=session_id, identity=identity)

        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._loop = loop
        self._queue = queue

        stdin = self._stdin_override if self._stdin_override is not None else sys.stdin.buffer
        real_stdout = sys.stdout
        if self._stdout_override is not None:
            self._stdout = self._stdout_override
        else:
            self._stdout = sys.stdout.buffer
            # Protect the protocol stream: anything a tool prints goes to
            # stderr instead of being parsed by the client as JSON-RPC.
            sys.stdout = sys.stderr

        reader = threading.Thread(
            target=self._read_loop,
            args=(stdin, loop, queue),
            name="easy-mcp-stdio-reader",
            daemon=True,
        )
        in_flight: set[asyncio.Task[None]] = set()
        audit("session_open", session_id=session_id, client_id=client_id, transport="stdio")
        reader.start()
        try:
            while True:
                item = await queue.get()
                if item is _EOF:
                    break
                task = asyncio.create_task(self._handle(item, context))
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)
        finally:
            await self._drain(in_flight)
            if self._stdout_override is None:
                sys.stdout = real_stdout
            self._loop = None
            self._queue = None
            audit("session_close", session_id=session_id, client_id=client_id, transport="stdio")

    def stop(self) -> None:
        """Stop serving after in-flight calls finish (thread-safe)."""
        loop, queue = self._loop, self._queue
        if loop is None or queue is None:
            return
        try:
            loop.call_soon_threadsafe(queue.put_nowait, _EOF)
        except RuntimeError:
            pass  # loop already closed

    async def _drain(self, in_flight: set[asyncio.Task[None]]) -> None:
        if not in_flight:
            return
        _, pending = await asyncio.wait(in_flight, timeout=self._shutdown_timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    # ------------------------------------------------------------------ I/O

    def _read_loop(
        self, stdin: BinaryIO, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue[Any]
    ) -> None:
        """Blocking reader thread: one queue item per input line."""
        limit = self._server.max_request_bytes

        def push(item: Any) -> None:
            try:
                loop.call_soon_threadsafe(queue.put_nowait, item)
            except RuntimeError:
                pass  # loop closed during shutdown; nothing left to deliver

        while True:
            try:
                # Read at most limit+1 bytes: a line that long without a
                # trailing newline is over the cap.
                line = stdin.readline(limit + 1)
            except (OSError, ValueError):
                line = b""
            if not line:
                push(_EOF)
                return
            if len(line) > limit and not line.endswith(b"\n"):
                # Discard the rest of the oversized line without buffering it.
                while line and not line.endswith(b"\n"):
                    try:
                        line = stdin.readline(65536)
                    except (OSError, ValueError):
                        line = b""
                push(_TOO_LARGE)
                if not line:
                    push(_EOF)
                    return
                continue
            stripped = line.strip()
            if stripped:
                push(stripped)

    async def _handle(self, item: Any, context: ClientContext) -> None:
        if item is _TOO_LARGE:
            audit("payload_too_large", client_id=context.client_id, transport="stdio")
            self._write(
                _error_response(
                    None,
                    PAYLOAD_TOO_LARGE,
                    f"request exceeds {self._server.max_request_bytes} bytes",
                )
            )
            return
        try:
            message = json.loads(item.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(_error_response(None, PARSE_ERROR, "Parse error: invalid JSON"))
            return
        response = await self._server.dispatch(message, context)
        if response is not None:
            self._write(response)

    def _write(self, response: dict[str, Any]) -> None:
        # json.dumps never emits a raw newline, so one message is one line.
        data = json.dumps(response, ensure_ascii=False, default=str).encode("utf-8") + b"\n"
        stdout = self._stdout
        if stdout is None:
            return
        with self._write_lock:
            try:
                stdout.write(data)
                stdout.flush()
            except (OSError, ValueError):
                # The client went away (broken pipe / closed stream): stop.
                self.stop()
