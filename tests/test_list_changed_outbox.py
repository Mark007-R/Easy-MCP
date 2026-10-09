"""The outbox behind notification streams, and the response that serves them (no network)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from easy_mcp.transport import _outbox, sse
from easy_mcp.transport._outbox import (
    ChannelClosed,
    EventStreamResponse,
    Outbox,
    accepts_event_stream,
    sse_event,
    stream_events,
)

TAG = "io.modelcontextprotocol/subscriptionId"
TOOLS = {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}
PROMPTS = {"jsonrpc": "2.0", "method": "notifications/prompts/list_changed"}


def updated(uri: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": uri}}


async def test_change_notifications_coalesce_until_written() -> None:
    outbox = Outbox()
    ack = {"jsonrpc": "2.0", "method": "notifications/subscriptions/acknowledged"}
    result = {"jsonrpc": "2.0", "id": 1, "result": {}}
    for message in (ack, TOOLS, TOOLS, PROMPTS, updated("a"), updated("a"), updated("b"), TOOLS):
        outbox.put(message)
    outbox.put(result)
    outbox.put(result)  # a response never coalesces
    assert len(outbox) == 7
    taken = [await outbox.get(1) for _ in range(len(outbox))]
    assert taken == [ack, TOOLS, PROMPTS, updated("a"), updated("b"), result, result]
    outbox.put(TOOLS)  # written already: a new change queues again
    assert await outbox.get(1) == TOOLS


def tagged(subscription_id: Any) -> dict[str, Any]:
    return {
        **TOOLS,
        "params": {"_meta": {TAG: subscription_id}},
    }


async def test_notifications_of_different_subscriptions_never_coalesce() -> None:
    # One channel may carry several subscriptions (stdio, legacy SSE): each
    # must hear of its own change.  1, 1.0 and "1" name three of them.
    outbox = Outbox()
    for message in (tagged("a"), tagged("a"), tagged(1), tagged(1.0), tagged("1"), TOOLS, TOOLS):
        outbox.put(message)
    taken = [await outbox.get(1) for _ in range(len(outbox))]
    assert taken == [tagged("a"), tagged(1), tagged(1.0), tagged("1"), TOOLS]
    tags = [message.get("params", {}).get("_meta", {}).get(TAG) for message in taken]
    assert [type(tag) for tag in tags] == [str, int, float, str, type(None)]


async def test_get_times_out_for_a_keep_alive_and_ends_once_closed_and_empty() -> None:
    outbox = Outbox()
    assert await outbox.get(0.01) is None
    outbox.put(TOOLS)
    outbox.close()
    with pytest.raises(ChannelClosed):
        outbox.put(PROMPTS)
    assert await outbox.get(1) == TOOLS  # what was queued is still written
    with pytest.raises(ChannelClosed):
        await outbox.get(1)
    await asyncio.wait_for(outbox.wait(), 1)  # closed: nothing to wait for


async def test_drain_into_moves_what_is_waiting_in_order() -> None:
    old, new = Outbox(), Outbox()
    new.put(TOOLS)
    old.put(TOOLS)
    old.put(PROMPTS)
    old.drain_into(new)
    assert len(old) == 0
    assert [await new.get(1), await new.get(1)] == [TOOLS, PROMPTS]


async def test_pending_kinds_name_the_lists_whose_change_is_not_yet_taken() -> None:
    outbox = Outbox()
    ack = {"jsonrpc": "2.0", "method": "notifications/subscriptions/acknowledged"}
    for message in (ack, TOOLS, updated("a"), PROMPTS, {"jsonrpc": "2.0", "id": 1, "result": {}}):
        outbox.put(message)
    assert outbox.pending_kinds() == {"tools", "prompts"}
    assert [await outbox.get(1), await outbox.get(1)] == [ack, TOOLS]
    assert outbox.pending_kinds() == {"prompts"}
    outbox.drain_into(Outbox())
    assert outbox.pending_kinds() == set()


def test_accepts_event_stream() -> None:
    for accept in (None, "", "text/event-stream", "application/json, text/event-stream", "*/*"):
        assert accepts_event_stream(accept), accept
    assert accepts_event_stream("TEXT/*;q=0.5")
    assert not accepts_event_stream("application/json")


def test_frames_match_the_legacy_stream() -> None:
    message = {"jsonrpc": "2.0", "id": 1, "result": {"text": "ünïcode"}}
    assert (
        sse_event(message) == f"event: message\ndata: {json.dumps(message, ensure_ascii=False)}\n\n"
    )
    assert sse.KEEPALIVE_SECONDS == _outbox.KEEPALIVE_SECONDS == 15.0


async def test_stream_events_sends_keep_alives_while_silent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_outbox, "KEEPALIVE_SECONDS", 0.01)
    outbox = Outbox()
    events = stream_events(outbox)
    assert await events.__anext__() == ": keep-alive\n\n"
    outbox.put(TOOLS)
    assert await events.__anext__() == sse_event(TOOLS)
    outbox.close()
    with pytest.raises(StopAsyncIteration):
        await events.__anext__()


class _Client:
    """An ASGI peer: records what is sent, and leaves when told to."""

    def __init__(self, *, gone: bool = False) -> None:
        self.sent: list[dict[str, Any]] = []
        self.leave = asyncio.Event()
        if gone:
            self.leave.set()
        self._requested = False

    async def receive(self) -> dict[str, Any]:
        if not self._requested:
            self._requested = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await self.leave.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)


@pytest.mark.parametrize("spec", ["2.3", "2.4"])
async def test_a_client_that_leaves_ends_the_stream_at_once(spec: str) -> None:
    outbox = Outbox()
    closed: list[bool] = []
    response = EventStreamResponse(stream_events(outbox), on_close=lambda: closed.append(True))
    assert response.headers["x-accel-buffering"] == "no"
    assert response.headers["cache-control"] == "no-cache"
    client = _Client()
    scope = {"type": "http", "asgi": {"spec_version": spec}}
    serving = asyncio.create_task(response(scope, client.receive, client.send))
    outbox.put(TOOLS)
    for _ in range(100):
        if len(client.sent) >= 2:
            break
        await asyncio.sleep(0.01)
    assert client.sent[0]["type"] == "http.response.start"
    assert client.sent[1]["body"] == sse_event(TOOLS).encode()
    client.leave.set()
    await asyncio.wait_for(serving, 2)  # well before any keep-alive would be written
    assert closed == [True]


async def test_on_close_runs_when_the_client_left_before_the_first_byte() -> None:
    closed: list[bool] = []
    response = EventStreamResponse(stream_events(Outbox()), on_close=lambda: closed.append(True))
    client = _Client(gone=True)
    await asyncio.wait_for(response({"type": "http"}, client.receive, client.send), 2)
    assert closed == [True]


async def test_a_stream_that_ends_completes_the_response() -> None:
    outbox = Outbox()
    outbox.put(TOOLS)
    outbox.close()
    closed: list[bool] = []
    response = EventStreamResponse(stream_events(outbox), on_close=lambda: closed.append(True))
    client = _Client()
    await asyncio.wait_for(response({"type": "http"}, client.receive, client.send), 2)
    assert client.sent[-1] == {"type": "http.response.body", "body": b"", "more_body": False}
    assert closed == [True]
