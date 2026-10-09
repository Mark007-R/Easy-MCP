"""Server-initiated messages on their way to an SSE stream, and how streams are framed.

An :class:`Outbox` holds what the server pushes for one stream (a
Streamable HTTP session's ``GET /mcp`` stream, or the response stream of one
``subscriptions/listen`` request) until the stream writes it.  List-change
notifications coalesce: a second one for the same list (and subscription)
before the first is written adds nothing, since each only tells the client
to fetch the list again.  So a stream's backlog stays bounded by the number
of lists (plus the acknowledgment and the final result of a listen), however
slowly its client reads.  The legacy SSE stream, whose queue also carries
responses, coalesces by the same :func:`coalesce_key`.

:class:`EventStreamResponse` serves such a stream and runs its ``on_close``
however the stream ends, even when the client left before the first byte.
"""

from __future__ import annotations

import asyncio
import collections
import json
from collections.abc import AsyncIterator, Callable, Hashable
from typing import Any

from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from ..protocol import LIST_KINDS, META_SUBSCRIPTION_ID

# How long a stream may stay silent before it sends an SSE comment, which
# keeps proxies from closing it and finds dead peers.
KEEPALIVE_SECONDS = 15.0

# The headers of every event stream: never cached, never buffered by a proxy.
STREAM_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}

_LIST_CHANGED = frozenset(method for method, _ in LIST_KINDS.values())
_RESOURCE_UPDATED = "notifications/resources/updated"


class ChannelClosed(Exception):
    """The stream behind an outbox has ended: it takes no more messages."""


def coalesce_key(message: dict[str, Any]) -> Hashable | None:
    """What *message* coalesces by, if it is a change notification: else ``None``.

    Its method, the subscription it is tagged with (one channel may carry
    several), and for a resource update its uri.
    """
    method = message.get("method")
    if "id" in message or not isinstance(method, str):
        return None
    params = message.get("params")
    params = params if isinstance(params, dict) else {}
    meta = params.get("_meta")
    tag = meta.get(META_SUBSCRIPTION_ID) if isinstance(meta, dict) else None
    # By type as well as value, so 1, 1.0 and "1" are three subscriptions.
    subscription = (type(tag).__name__, tag)
    if method in _LIST_CHANGED:
        return method, subscription
    if method == _RESOURCE_UPDATED:
        uri = params.get("uri")
        return (method, subscription, uri) if isinstance(uri, str) else None
    return None


class Outbox:
    """Messages for one stream, in order, change notifications coalesced.  Loop-only."""

    def __init__(self) -> None:
        self._items: collections.deque[tuple[Hashable | None, dict[str, Any]]] = collections.deque()
        self._keys: set[Hashable] = set()
        self._closed = False
        self._ready = asyncio.Event()

    @property
    def closed(self) -> bool:
        return self._closed

    def __len__(self) -> int:
        return len(self._items)

    def put(self, message: dict[str, Any]) -> None:
        """Queue *message* for the stream.

        Raises:
            ChannelClosed: The stream has ended.
        """
        if self._closed:
            raise ChannelClosed
        key = coalesce_key(message)
        if key is not None:
            if key in self._keys:
                return  # one is waiting already, and says the same
            self._keys.add(key)
        self._items.append((key, message))
        self._ready.set()

    async def get(self, timeout: float) -> dict[str, Any] | None:
        """The next message; ``None`` after *timeout* seconds without one (a keep-alive is due).

        Raises:
            ChannelClosed: The outbox is closed and empty.
        """
        if not self._items and not self._closed:
            self._ready.clear()
            try:
                async with asyncio.timeout(timeout):
                    await self._ready.wait()
            except TimeoutError:
                return None
        if self._items:
            key, message = self._items.popleft()
            if key is not None:
                self._keys.discard(key)
            return message
        raise ChannelClosed

    async def wait(self) -> None:
        """Wait until a message is queued or the outbox closes."""
        while not self._items and not self._closed:
            self._ready.clear()
            await self._ready.wait()

    def drain_into(self, other: Outbox) -> None:
        """Move every message waiting here to *other*, in order."""
        while self._items:
            key, message = self._items.popleft()
            if key is not None:
                self._keys.discard(key)
            try:
                other.put(message)
            except ChannelClosed:
                break

    def close(self) -> None:
        """Take no more messages; the stream ends once those waiting are written."""
        self._closed = True
        self._ready.set()


def accepts_event_stream(accept: str | None) -> bool:
    """Whether an ``Accept`` header admits ``text/event-stream`` (no header admits anything)."""
    if not accept:
        return True
    ranges = {part.split(";", 1)[0].strip().lower() for part in accept.split(",")}
    return bool(ranges & {"text/event-stream", "text/*", "*/*"})


def sse_event(message: dict[str, Any]) -> str:
    """*message* as one SSE ``message`` event, as every easy_mcp stream frames it."""
    payload = json.dumps(message, ensure_ascii=False, default=str)
    return f"event: message\ndata: {payload}\n\n"


async def stream_events(outbox: Outbox) -> AsyncIterator[str]:
    """The SSE body of a stream fed by *outbox*: its messages, keep-alives while it is silent.

    Ends once the outbox is closed and every message in it has been yielded.
    """
    while True:
        try:
            message = await outbox.get(KEEPALIVE_SECONDS)
        except ChannelClosed:
            return
        if message is None:
            yield ": keep-alive\n\n"
        else:
            yield sse_event(message)


class EventStreamResponse(StreamingResponse):
    """A ``text/event-stream`` response that runs *on_close* however it ends.

    The body is streamed while the client's disconnect is listened for, so a
    client that leaves ends the stream at once, whichever ASGI version the
    server speaks; *on_close* then runs even if the body never started,
    which a generator's ``finally`` cannot promise.
    """

    def __init__(self, body: AsyncIterator[str], *, on_close: Callable[[], None]) -> None:
        super().__init__(body, media_type="text/event-stream", headers=STREAM_HEADERS)
        self._on_close = on_close

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        stream = asyncio.ensure_future(self.stream_response(send))
        listener = asyncio.ensure_future(self.listen_for_disconnect(receive))
        try:
            await asyncio.wait({stream, listener}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stream.cancel()
            listener.cancel()
            try:
                outcomes = await asyncio.gather(stream, listener, return_exceptions=True)
            finally:
                self._on_close()
        for outcome in outcomes:
            # OSError: the client went away mid-write (ASGI 2.4 servers say so).
            if isinstance(outcome, Exception) and not isinstance(outcome, OSError):
                raise outcome
