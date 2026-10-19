"""A server that records nothing: the default install, without the ``[otel]`` extra.

Without ``metrics=True`` and without ``opentelemetry-api`` (or with it
switched off), ``dispatch`` skips the telemetry hook.  The ``[dev]`` extra
installs OpenTelemetry for the tracing tests, so these tests take it away
themselves: one server's telemetry is built inactive, and a subprocess
cannot import ``opentelemetry`` (or ``prometheus_client``) at all.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import make_context, modern, notification, rpc

from easy_mcp import MCPServer
from easy_mcp.exceptions import INVALID_PARAMS, METHOD_NOT_FOUND
from easy_mcp.telemetry import Telemetry

REPO_ROOT = Path(__file__).resolve().parent.parent
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}


def quiet_server() -> MCPServer:
    """A server whose telemetry is inactive, as on a default install."""
    server = MCPServer(port=0, rate_limit_per_minute=None)
    server._telemetry_state = Telemetry(server, metrics=False, otel=None)

    @server.tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @server.tool
    async def boom() -> str:
        """Fails."""
        raise RuntimeError("tool failure")

    return server


async def test_inactive_server_answers_every_message() -> None:
    server = quiet_server()
    assert server._telemetry.active is False
    context = make_context()
    init = await server.dispatch(rpc("initialize", INIT, "init"), context)
    assert init is not None and init["result"]["protocolVersion"] == "2025-11-25"
    assert await server.dispatch(notification("notifications/initialized"), context) is None
    listed = await server.dispatch(rpc("tools/list", msg_id=2), context)
    assert listed is not None
    assert [tool["name"] for tool in listed["result"]["tools"]] == ["add", "boom"]
    added = await server.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}, 3), context
    )
    assert added is not None and added["result"]["content"][0]["text"] == "3"
    failed = await server.dispatch(rpc("tools/call", {"name": "boom"}, 4), context)
    assert failed is not None and failed["result"]["isError"] is True
    invalid = await server.dispatch(
        rpc("tools/call", {"name": "add", "arguments": {"a": "one"}}, 5), context
    )
    assert invalid is not None and invalid["error"]["code"] == INVALID_PARAMS
    unknown = await server.dispatch(rpc("no/such/method", msg_id=6), context)
    assert unknown is not None and unknown["error"]["code"] == METHOD_NOT_FOUND
    stateless = await server.dispatch(
        modern("tools/call", {"name": "add", "arguments": {"a": 2, "b": 2}}, 7),
        make_context(session_id="stateless"),
    )
    assert stateless is not None and stateless["result"]["content"][0]["text"] == "4"
    with pytest.raises(RuntimeError, match="metrics=True"):
        server.render_metrics()


BLOCKED = """
import asyncio
import importlib.abc
import json
import sys


class Blocked(importlib.abc.MetaPathFinder):
    # As if neither extra were installed.
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in ("opentelemetry", "prometheus_client"):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None


sys.meta_path.insert(0, Blocked())

from easy_mcp import MCPServer
from easy_mcp.telemetry._otel import load_bridge
from easy_mcp.transport.base import ClientContext


async def main():
    report = {"bridge": load_bridge() is not None, "servers": []}
    for metrics in (False, True):
        server = MCPServer(port=0, rate_limit_per_minute=None, metrics=metrics)

        @server.tool
        def add(a: int, b: int) -> int:
            \"\"\"Add two integers.\"\"\"
            return a + b

        context = ClientContext(client_id="ip:test", session_id="test-session")
        answers = []
        for msg_id, method, params in (
            (1, "tools/list", {}),
            (2, "tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}),
            (3, "tools/call", {"name": "add", "arguments": {"a": "one"}}),
        ):
            message = {"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params}
            answers.append(await server.dispatch(message, context))
        rendered = server.render_metrics() if metrics else None
        report["servers"].append(
            {
                "metrics": metrics,
                "active": server._telemetry.active,
                "otel": server._telemetry.otel is not None,
                "answers": answers,
                "rendered": rendered,
            }
        )
    report["loaded"] = sorted(
        name
        for name in sys.modules
        if name.split(".")[0] in ("opentelemetry", "prometheus_client")
    )
    print(json.dumps(report))


asyncio.run(main())
"""


def test_without_the_extras_at_import(tmp_path: Path) -> None:
    # opentelemetry is unimportable before easy_mcp is imported, so a
    # module-level import of it anywhere would fail here.
    script = tmp_path / "blocked.py"
    script.write_text(BLOCKED, encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    env.pop("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", None)
    finished = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        env=env,
        cwd=str(tmp_path),
        timeout=60,
        check=False,
    )
    assert finished.returncode == 0, finished.stderr.decode()
    report = json.loads(finished.stdout.decode().splitlines()[-1])
    assert report["bridge"] is False
    assert report["loaded"] == []
    plain, measured = report["servers"]
    assert (plain["active"], plain["otel"]) == (False, False)
    assert (measured["active"], measured["otel"]) == (True, False)
    for server in (plain, measured):
        listed, added, invalid = server["answers"]
        assert [tool["name"] for tool in listed["result"]["tools"]] == ["add"]
        assert added["result"]["content"][0]["text"] == "3"
        assert invalid["error"]["code"] == INVALID_PARAMS
    assert plain["rendered"] is None
    rendered = measured["rendered"]
    assert 'easy_mcp_tool_calls_total{easy_mcp_status="ok",gen_ai_tool_name="add"} 1' in rendered
    assert (
        'easy_mcp_tool_calls_total{easy_mcp_status="invalid_arguments",gen_ai_tool_name="add"} 1'
        in rendered
    )
