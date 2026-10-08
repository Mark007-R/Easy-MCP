"""The app tests/test_live_redis.py serves from separate worker processes.

Configured from the environment by the test that starts it::

    python -m uvicorn live_redis_app:app --app-dir tests --port ...

``EASY_MCP_REDIS_URL`` (the store), ``LIVE_KEY_A`` and ``LIVE_KEY_B`` (API keys
with every scope and with scope ``b``), ``LIVE_NAMESPACE`` (one per test),
``LIVE_WORKER`` (what ``whoami`` answers), ``LIVE_MARKER_DIR`` (where tools
leave ``<tag>.started`` and ``<tag>.<reason>`` files), and optionally
``LIVE_RATE_LIMIT``, ``LIVE_MAX_SESSIONS`` and ``LIVE_IDLE`` (seconds).
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

from easy_mcp import (
    APIKeyAuth,
    MCPServer,
    RedisStore,
    StreamableHTTPTransport,
    current_cancel_token,
)

WORKER = os.environ.get("LIVE_WORKER", "?")
MARKERS = Path(os.environ.get("LIVE_MARKER_DIR", "."))
RATE_LIMIT = os.environ.get("LIVE_RATE_LIMIT")

server = MCPServer(
    name="live-redis",
    auth=APIKeyAuth({os.environ["LIVE_KEY_A"]: "*", os.environ["LIVE_KEY_B"]: ["b"]}),
    rate_limit_per_minute=int(RATE_LIMIT) if RATE_LIMIT else None,
    max_sessions=int(os.environ.get("LIVE_MAX_SESSIONS", "256")),
    store=RedisStore.from_env(namespace=os.environ.get("LIVE_NAMESPACE", "live-redis")),
)


def mark(name: str) -> None:
    (MARKERS / name).write_text(WORKER, encoding="utf-8")


@server.tool
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


@server.tool(max_calls_per_session=2)
def scarce() -> str:
    """Twice per session (or per stateless client)."""
    return "spent"


@server.tool
async def slow(seconds: float = 30.0, tag: str = "slow") -> str:
    """Sleep, leaving markers when it starts and when it is cancelled."""
    mark(f"{tag}.started")
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        mark(f"{tag}.cancelled")
        raise
    mark(f"{tag}.finished")
    return "slept"


@server.tool
def slow_sync(tag: str = "sync") -> str:
    """Poll the cancel token, then leave a marker named after its reason."""
    token = current_cancel_token()
    mark(f"{tag}.started")
    deadline = time.monotonic() + 30
    while token is not None and not token.cancelled and time.monotonic() < deadline:
        time.sleep(0.01)
    mark(f"{tag}.{token.reason if token is not None else 'none'}")
    return "stopped"


@server.tool
def whoami() -> str:
    """The worker process that served the call."""
    return WORKER


@server.tool
def big(size: int) -> str:
    """A long answer."""
    return "x" * size


app = StreamableHTTPTransport(
    server, session_idle_timeout=float(os.environ.get("LIVE_IDLE", "3600"))
).build_app()
