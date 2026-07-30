"""A minimal raw MCP client for smoke-testing an easy_mcp server.

This deliberately speaks the wire protocol directly (SSE + JSON-RPC) so you
can see exactly what travels over the network.  Real applications should use
an MCP client library instead.

Usage:
    python examples/demo_server.py          # terminal 1
    python examples/raw_client.py           # terminal 2
"""

from __future__ import annotations

import json
import sys

import httpx

BASE_URL = "http://127.0.0.1:8000"


def main() -> None:
    with httpx.Client(base_url=BASE_URL, timeout=httpx.Timeout(30.0)) as client:
        with client.stream("GET", "/sse") as stream:
            lines = stream.iter_lines()

            def next_data() -> str:
                for line in lines:
                    if line.startswith("data: "):
                        return line[len("data: ") :]
                raise RuntimeError("SSE stream closed")

            endpoint = next_data()
            print(f"session endpoint: {endpoint}")

            def send(method: str, params: dict | None = None, msg_id: int | None = 1) -> None:
                message: dict = {"jsonrpc": "2.0", "method": method}
                if msg_id is not None:
                    message["id"] = msg_id
                if params is not None:
                    message["params"] = params
                client.post(endpoint, json=message)

            send("initialize", {"protocolVersion": "2024-11-05"}, msg_id=1)
            print("initialize ->", json.dumps(json.loads(next_data()), indent=2))
            send("notifications/initialized", msg_id=None)

            send("tools/list", msg_id=2)
            tools = json.loads(next_data())["result"]["tools"]
            print("tools:", [tool["name"] for tool in tools])

            send("tools/call", {"name": "add", "arguments": {"a": 2, "b": 3}}, msg_id=3)
            print("add(2, 3) ->", json.dumps(json.loads(next_data())["result"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except httpx.ConnectError:
        sys.exit("Could not connect — start examples/demo_server.py first.")
