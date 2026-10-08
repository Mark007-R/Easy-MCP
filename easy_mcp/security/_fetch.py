"""Outbound HTTP for OAuth: authorization server metadata, key sets, introspection.

Which URLs are fetched is the caller's decision (configured ones, or ones from
a configured issuer's validated metadata; never one a client supplied).  This
module adds the transport rules every fetch follows:

* ``https`` only, or ``http`` to a loopback host; any other scheme is refused
  before anything is sent;
* no redirects: a ``3xx`` is a failure, so a document cannot send the server
  somewhere else;
* a timeout for the whole exchange, and a cap on the body while it is read;
* the body must be a JSON object.

urllib blocks, so each fetch runs on the executor the caller passes.  Only the
standard library is used: introspection needs no extra dependency.

A socket timeout bounds one call on the socket, not the exchange: a server
(or a filter in front of it) that sends a byte every few seconds would make
every read succeed, and hold the thread for as long as it kept on, status
line and headers included.  So the sockets of a fetch give each call only
the time left before the exchange's deadline (:class:`_Deadline`).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import functools
import http.client
import json
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from concurrent.futures import Executor
from typing import Any

from .._version import __version__

LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

_USER_AGENT = f"easy-mcp-kit/{__version__}"
_CHUNK = 65536


class FetchError(Exception):
    """A fetch failed; ``status`` is the HTTP status when the server answered.

    ``too_large`` is set when the answer was longer than the cap, and
    ``malformed`` when it was JSON nested too deeply to parse.  ``queued`` is
    set when no fetch thread became free in time: nothing was sent.
    """

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        too_large: bool = False,
        malformed: bool = False,
        queued: bool = False,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.too_large = too_large
        self.malformed = malformed
        self.queued = queued


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect: urllib then raises the 3xx as an ``HTTPError``."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def allowed_url(url: str) -> bool:
    """Whether *url* may be fetched: ``https``, or ``http`` to a loopback host."""
    try:
        parts = urllib.parse.urlsplit(url)
        host = parts.hostname
    except ValueError:
        return False
    if not host or parts.username is not None or parts.password is not None:
        return False
    if parts.scheme == "https":
        return True
    return parts.scheme == "http" and host in LOOPBACK_HOSTS


class _Deadline:
    """When one exchange must be over; its sockets give each call only the time left."""

    def __init__(self, seconds: float) -> None:
        self._at = time.monotonic() + seconds

    def left(self) -> float:
        """Seconds left.

        Raises:
            TimeoutError: None are.
        """
        left = self._at - time.monotonic()
        if left <= 0:
            raise TimeoutError("timed out")
        return left

    def connect(
        self,
        address: tuple[str, int],
        timeout: float | None = None,
        source_address: tuple[str, int] | None = None,
    ) -> socket.socket:
        """``socket.create_connection`` with a :class:`_TimedSocket` of this deadline.

        *timeout* is ignored: each attempt gets the time left.  The TLS
        handshake that may follow runs in one call, bounded by the timeout
        the socket has when it starts: the time left once connected.
        """
        host, port = address
        error: OSError | None = None
        for family, kind, proto, _, peer in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
            sock = _TimedSocket(family, kind, proto)
            sock.deadline = self
            try:
                sock.settimeout(self.left())
                if source_address is not None:
                    sock.bind(source_address)
                sock.connect(peer)
                sock.settimeout(self.left())
            except OSError as exc:
                sock.close()
                if isinstance(exc, TimeoutError):
                    raise
                error = exc
                continue
            return sock
        raise error if error is not None else OSError(f"no address found for {host}")


class _TimedSocket(socket.socket):
    """A TCP socket whose every send and receive ends by its deadline."""

    deadline: _Deadline | None = None

    def _arm(self) -> None:
        if self.deadline is not None:
            self.settimeout(self.deadline.left())

    def recv(self, bufsize: int, flags: int = 0, /) -> bytes:
        self._arm()
        return super().recv(bufsize, flags)

    def recv_into(self, buffer: Any, nbytes: int = 0, flags: int = 0) -> int:
        self._arm()
        return super().recv_into(buffer, nbytes, flags)

    def send(self, data: Any, flags: int = 0, /) -> int:
        self._arm()
        return super().send(data, flags)

    def sendall(self, data: Any, flags: int = 0, /) -> None:
        self._arm()
        super().sendall(data, flags)


class _TimedSSLSocket(ssl.SSLSocket):
    """A TLS socket whose every read and write ends by its deadline.

    ``recv`` and ``recv_into`` go through :meth:`read`, and ``sendall``
    through :meth:`send`.  The deadline is set once the handshake is over.
    """

    deadline: _Deadline | None = None

    def _arm(self) -> None:
        if self.deadline is not None:
            self.settimeout(self.deadline.left())

    def read(self, len: int = 1024, buffer: Any = None) -> bytes:
        self._arm()
        return super().read(len, buffer)

    def send(self, data: Any, flags: int = 0) -> int:
        self._arm()
        return super().send(data, flags)


@functools.lru_cache(maxsize=1)
def _tls_context() -> ssl.SSLContext:
    # Built once: loading the system's trusted certificates is slow on some
    # platforms, and the context is safe to share between threads.
    context = ssl.create_default_context()
    context.sslsocket_class = _TimedSSLSocket
    return context


class _HTTPConnection(http.client.HTTPConnection):
    """Plain HTTP (to a loopback host only) under one exchange's deadline."""

    def __init__(self, host: str, /, *, deadline: _Deadline, **kwargs: Any) -> None:
        super().__init__(host, **kwargs)
        self._create_connection = deadline.connect


class _HTTPSConnection(http.client.HTTPSConnection):
    """HTTPS under one exchange's deadline."""

    def __init__(self, host: str, /, *, deadline: _Deadline, **kwargs: Any) -> None:
        super().__init__(host, **kwargs)
        self._create_connection = deadline.connect
        self._deadline = deadline

    def connect(self) -> None:
        super().connect()  # the TCP connection, a proxy's tunnel, the TLS handshake
        if isinstance(self.sock, _TimedSSLSocket):
            self.sock.deadline = self._deadline


class _HTTPHandler(urllib.request.HTTPHandler):
    """Open ``http`` URLs under one deadline (:class:`_HTTPConnection`)."""

    def __init__(self, deadline: _Deadline) -> None:
        super().__init__()
        self._deadline = deadline

    def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        deadline = self._deadline

        def connection(host: str, /, **kwargs: Any) -> http.client.HTTPConnection:
            return _HTTPConnection(host, deadline=deadline, **kwargs)

        return self.do_open(connection, req)


class _HTTPSHandler(urllib.request.HTTPSHandler):
    """Open ``https`` URLs under one deadline (:class:`_HTTPSConnection`)."""

    def __init__(self, deadline: _Deadline) -> None:
        # The shared context: without one, Python 3.12+ builds a new one here.
        super().__init__(context=_tls_context())
        self._deadline = deadline

    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        deadline = self._deadline

        def connection(host: str, /, **kwargs: Any) -> http.client.HTTPConnection:
            return _HTTPSConnection(host, deadline=deadline, context=_tls_context(), **kwargs)

        return self.do_open(connection, req)


def _opener(url: str, deadline: _Deadline) -> urllib.request.OpenerDirector:
    parts = urllib.parse.urlsplit(url)
    handlers: list[Any] = [_NoRedirect(), _HTTPHandler(deadline), _HTTPSHandler(deadline)]
    if parts.hostname in LOOPBACK_HOSTS:
        # A loopback address is never reached through a proxy; anything else
        # honours the environment's proxy settings, as the rest of urllib does.
        handlers.append(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener(*handlers)


def _exchange(
    url: str,
    *,
    data: bytes | None,
    headers: Mapping[str, str],
    max_bytes: int,
    timeout: float,
) -> dict[str, Any]:
    """One blocking request; returns the JSON object the server answered with."""
    deadline = _Deadline(timeout)
    request = urllib.request.Request(
        url,
        data=data,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json", **headers},
        method="POST" if data is not None else "GET",
    )
    try:
        with _opener(url, deadline).open(request, timeout=timeout) as response:
            status: int = response.status
            body = bytearray()
            while True:
                chunk = response.read1(_CHUNK)
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > max_bytes:
                    raise FetchError(
                        f"response from {url} exceeds {max_bytes} bytes", too_large=True
                    )
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
        kind = "redirect refused" if 300 <= status < 400 else "error"
        raise FetchError(f"{url} answered HTTP {status} ({kind})", status=status) from None
    except FetchError:
        raise
    except TimeoutError:
        raise FetchError(f"{url} timed out") from None
    except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
        reason = getattr(exc, "reason", None) or type(exc).__name__
        if isinstance(reason, TimeoutError):  # while connecting or sending
            raise FetchError(f"{url} timed out") from None
        raise FetchError(f"{url} unreachable: {reason}") from None
    # The server answered: these keep its status, so a caller can tell an
    # answer it cannot use from a server it cannot reach.
    try:
        document = json.loads(bytes(body).decode("utf-8"))
    except RecursionError:  # JSON nested deeper than the parser recurses
        raise FetchError(
            f"{url} answered JSON nested too deeply", status=status, malformed=True
        ) from None
    except Exception:  # invalid UTF-8 or JSON
        raise FetchError(f"{url} did not answer with JSON", status=status) from None
    if not isinstance(document, dict):
        raise FetchError(f"{url} did not answer with a JSON object", status=status)
    return document


def _set_started(started: asyncio.Future[None]) -> None:
    if not started.done():
        started.set_result(None)


async def _run(
    executor: Executor, timeout: float, work: Callable[[], dict[str, Any]]
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    started: asyncio.Future[None] = loop.create_future()

    def run() -> dict[str, Any]:
        with contextlib.suppress(RuntimeError):  # the loop closed meanwhile
            loop.call_soon_threadsafe(_set_started, started)
        return work()

    try:
        job = executor.submit(run)
    except RuntimeError:  # the executor was shut down meanwhile
        raise FetchError("the fetch threads are shut down") from None
    future = asyncio.wrap_future(job)
    try:
        # Waiting for a free thread is not part of the exchange: its deadline
        # starts once a thread runs it, so a fetch queued behind slow ones is
        # not timed out for their slowness.
        await asyncio.wait(
            (started, future), timeout=timeout + 1.0, return_when=asyncio.FIRST_COMPLETED
        )
        if job.cancel():  # still waiting for a thread: nothing was sent
            raise FetchError(f"no fetch thread was free within {timeout + 1.0:g} s", queued=True)
        # The exchange keeps its own deadline; this one also covers what
        # urllib cannot bound, such as a name lookup that hangs.
        return await asyncio.wait_for(future, timeout + 1.0)
    except TimeoutError:
        raise FetchError("request timed out") from None
    finally:
        future.cancel()  # nothing once it is done; otherwise no one wants it


async def fetch_json(
    url: str, *, max_bytes: int, timeout: float, executor: Executor
) -> dict[str, Any]:
    """``GET`` *url* and return its JSON object.

    Raises:
        FetchError: The URL is not allowed, the server could not be reached,
            answered anything but ``2xx`` (redirects included), took longer
            than *timeout*, sent more than *max_bytes*, or sent no JSON object;
            or no thread of *executor* became free in time to send it
            (``queued``).
    """
    if not allowed_url(url):
        raise FetchError(f"refusing to fetch {url!r}: https, or http to a loopback host, only")

    def work() -> dict[str, Any]:
        return _exchange(url, data=None, headers={}, max_bytes=max_bytes, timeout=timeout)

    return await _run(executor, timeout, work)


async def post_form_json(
    url: str,
    form: Mapping[str, str],
    *,
    auth: tuple[str, str],
    max_bytes: int,
    timeout: float,
    executor: Executor,
) -> dict[str, Any]:
    """``POST`` *form* to *url* with HTTP Basic *auth*; return the JSON object answered.

    The client id and secret are form-urlencoded before they are joined, as
    RFC 6749 section 2.3.1 asks.  Neither ever appears in an error message.

    Raises:
        FetchError: As :func:`fetch_json`.
    """
    if not allowed_url(url):
        raise FetchError(f"refusing to fetch {url!r}: https, or http to a loopback host, only")
    client_id, secret = auth
    credentials = f"{urllib.parse.quote_plus(client_id)}:{urllib.parse.quote_plus(secret)}"
    headers = {
        "Authorization": "Basic " + base64.b64encode(credentials.encode("utf-8")).decode("ascii"),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = urllib.parse.urlencode(dict(form)).encode("ascii")

    def work() -> dict[str, Any]:
        return _exchange(url, data=data, headers=headers, max_bytes=max_bytes, timeout=timeout)

    return await _run(executor, timeout, work)
