"""A demo MCP server showcasing easy_mcp features.

Run it:

    python examples/demo_server.py

Then point an MCP client at http://127.0.0.1:8000/sse — for example:

    npx @modelcontextprotocol/inspector
    claude mcp add --transport sse demo http://127.0.0.1:8000/sse

Protected tools need an API key (32+ random characters recommended):

    set EASY_MCP_DEMO_KEY=your-long-random-key        (Windows cmd)
    $env:EASY_MCP_DEMO_KEY = "your-long-random-key"   (PowerShell)
    export EASY_MCP_DEMO_KEY=your-long-random-key     (POSIX)

Clients then send ``Authorization: Bearer <key>`` (or ``X-API-Key``).
"""

from __future__ import annotations

import asyncio
import os
import statistics

from easy_mcp import APIKeyAuth, MCPServer, ToolError

demo_key = os.environ.get("EASY_MCP_DEMO_KEY")
auth = APIKeyAuth({demo_key: ["admin"]}) if demo_key else None

server = MCPServer(
    port=8000,
    name="easy-mcp-demo",
    auth=auth,
    rate_limit_per_minute=60,
    instructions="A demo server: arithmetic, text stats, and one admin-only tool.",
)


@server.tool
def add(a: float, b: float) -> float:
    """Add two numbers.

    Args:
        a: First operand.
        b: Second operand.
    """
    return a + b


@server.tool(
    tags=("text",),
    category="strings",
    examples=({"arguments": {"text": "hello world"}},),
)
def word_count(text: str) -> dict[str, int]:
    """Count words and characters in a text.

    Args:
        text: The text to analyze.
    """
    return {"words": len(text.split()), "characters": len(text)}


@server.tool
def summarize_numbers(values: list[float]) -> dict[str, float]:
    """Compute the mean, minimum, and maximum of a list of numbers.

    Args:
        values: Numbers to summarize. Must not be empty.
    """
    if not values:
        # ToolError messages are sent to the client verbatim — use it for
        # intentional, user-facing failures.
        raise ToolError("values must not be empty")
    return {"mean": statistics.fmean(values), "min": min(values), "max": max(values)}


@server.tool(timeout=2.0)
async def slow_echo(text: str, delay_seconds: float = 0.5) -> str:
    """Echo text after a delay (demonstrates async tools and timeouts).

    Args:
        text: Text to echo back.
        delay_seconds: How long to wait before responding (capped at 2s
            by the tool timeout).
    """
    await asyncio.sleep(delay_seconds)
    return text


@server.tool(scopes=("admin",), max_calls_per_session=3)
def read_secret_config() -> dict[str, str]:
    """Return the demo 'secret' configuration (requires the admin scope)."""
    return {"environment": "demo", "feature_flags": "all-on"}


if __name__ == "__main__":
    server.run()
