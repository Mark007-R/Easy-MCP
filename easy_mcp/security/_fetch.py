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
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import functools
import http.client
import json
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


@functools.lru_cache(maxsize=1)
def _tls_context() -> ssl.SSLContext:
    # Built once: loading the system's trusted certificates is slow on some
    # platforms, and the context is safe to share between threads.
    return ssl.create_default_context()


def _opener(url: str) -> urllib.request.OpenerDirector:
    parts = urllib.parse.urlsplit(url)
    handlers: list[Any] = [_NoRedirect()]
    if parts.scheme == "https":
        handlers.append(urllib.request.HTTPSHandler(context=_tls_context()))
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
    deadline = time.monotonic() + timeout
    request = urllib.request.Request(
        url,
        data=data,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json", **headers},
        method="POST" if data is not None else "GET",
    )
    try:
        with _opener(url).open(request, timeout=timeout) as response:
            status: int = response.status
            body = bytearray()
            # read1 returns what one read of the socket brings, so a server
            # that trickles its answer meets the deadline between reads.
            while True:
                chunk = response.read1(_CHUNK)
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > max_bytes:
                    raise FetchError(
                        f"response from {url} exceeds {max_bytes} bytes", too_large=True
                    )
                if time.monotonic() > deadline:
                    raise FetchError(f"{url} timed out")
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
