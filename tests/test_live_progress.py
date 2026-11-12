"""Interop of progress notifications with the official MCP Python SDK client.

Skipped unless ``EASY_MCP_LIVE_SDK_CLIENT=1`` is set and the ``mcp`` package
(2.3 or later) is installed: install it next to easy_mcp in a virtualenv of
its own, ``pip install "mcp>=2.3" -e .``, then run this file.

The SDK is driven in each mode it offers, over Streamable HTTP (where the
answer of a call that reports progress is an event stream) and over stdio.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import live_sdk
import pytest

from easy_mcp import MCPServer, report_progress

pytestmark = live_sdk.marker

REPO_ROOT = Path(__file__).resolve().parent.parent
LiveServer = Callable[[Any], str]
MODES = ["auto", "2026-07-28", "legacy"]

STDIO_SERVER = '''
import time

from easy_mcp import MCPServer, report_progress

server = MCPServer(name="progress-interop", rate_limit_per_minute=None)


@server.tool
def count(steps: int) -> str:
    """Count to steps, slowly."""
    for step in range(1, steps + 1):
        report_progress(step, total=steps, message=f"step {step}")
        time.sleep(0.15)
    return f"counted to {steps}"


server.run("stdio")
'''


def make_server() -> MCPServer:
    server = MCPServer(name="progress-interop", port=0, rate_limit_per_minute=None)

    @server.tool
    def count(steps: int) -> str:
        """Count to steps, slowly."""
        for step in range(1, steps + 1):
            report_progress(step, total=steps, message=f"step {step}")
            time.sleep(0.15)
        return f"counted to {steps}"

    @server.tool
    def quiet() -> str:
        """Report nothing."""
        return "quiet"

    return server


class Seen:
    """A progress_callback that records what it is told."""

    def __init__(self) -> None:
        self.reports: list[tuple[float, float | None, str | None]] = []

    async def __call__(self, progress: float, total: float | None, message: str | None) -> None:
        self.reports.append((progress, total, message))


def check(seen: Seen, steps: int) -> None:
    values = [progress for progress, _, _ in seen.reports]
    assert values, "no progress reached the client"
    assert values == sorted(set(values))
    assert seen.reports[-1] == (steps, steps, f"step {steps}")


@pytest.mark.parametrize("mode", MODES)
async def test_sdk_hears_progress_over_http(live_server: LiveServer, mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    base = live_server(make_server())
    seen = Seen()
    async with Client(f"{base}/mcp", mode=mode) as client:
        result = await client.call_tool("count", {"steps": 4}, progress_callback=seen)
        assert result.content[0].text == "counted to 4"
        check(seen, 4)
        # A call that reports nothing, and one that asks for no progress,
        # are answered as plain JSON, which the client reads as before.
        silent = Seen()
        quiet = await client.call_tool("quiet", {}, progress_callback=silent)
        assert quiet.content[0].text == "quiet"
        assert silent.reports == []
        plain = await client.call_tool("count", {"steps": 1})
        assert plain.content[0].text == "counted to 1"


@pytest.mark.parametrize("mode", MODES)
async def test_sdk_hears_progress_over_stdio(mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters

    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-c", STDIO_SERVER],
        env={"PYTHONPATH": str(REPO_ROOT), "PYTHONUNBUFFERED": "1"},
        cwd=str(REPO_ROOT),
    )
    seen = Seen()
    async with Client(parameters, mode=mode) as client:
        result = await client.call_tool("count", {"steps": 3}, progress_callback=seen)
        assert result.content[0].text == "counted to 3"
        check(seen, 3)
