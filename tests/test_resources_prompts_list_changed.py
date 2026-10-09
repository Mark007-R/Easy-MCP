"""List-change notifications for the resources (templates included) and prompts lists."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from conftest import Pushed, listen, make_context, rpc

from easy_mcp import APIKeyAuth, ClientIdentity, MCPServer
from easy_mcp.transport.base import ClientContext

SEE_KEY = "lc-rp-see-key-" + "k" * 16
TAG = "io.modelcontextprotocol/subscriptionId"
INIT = {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "t"}}
PROMPTS_CHANGED = {"jsonrpc": "2.0", "method": "notifications/prompts/list_changed"}
RESOURCES_CHANGED = {"jsonrpc": "2.0", "method": "notifications/resources/list_changed"}


def make_server(**kwargs: Any) -> MCPServer:
    kwargs.setdefault("rate_limit_per_minute", None)
    server = MCPServer(port=0, **kwargs)
    server.register_prompt(lambda: "x", name="first")
    server.register_resource(lambda: "x", "x://first", name="first")
    return server


async def initialized(
    server: MCPServer, pushed: Pushed, identity: ClientIdentity | None = None
) -> ClientContext:
    context = make_context(identity, push=pushed, multiplexed=True)
    response = await server.dispatch(rpc("initialize", INIT), context)
    assert response is not None and "result" in response, response
    return context


async def settle(window: float) -> None:
    await asyncio.sleep(window * 6 + 0.05)


async def acknowledged(
    server: MCPServer, context: ClientContext, pushed: Pushed, **wanted: Any
) -> Any:
    task = asyncio.create_task(server.dispatch(listen("l1", **wanted), context))
    deadline = time.monotonic() + 5
    while not pushed.frames:
        assert time.monotonic() < deadline and not task.done(), "no acknowledgment"
        await asyncio.sleep(0.005)
    return task


async def test_initialize_advertises_list_changed_for_every_kind() -> None:
    server = make_server()
    response = await server.dispatch(rpc("initialize", INIT), make_context())
    assert response is not None
    assert response["result"]["capabilities"] == {
        "tools": {"listChanged": True},
        "resources": {"subscribe": True, "listChanged": True},
        "prompts": {"listChanged": True},
    }


async def test_a_session_hears_of_prompt_resource_and_template_changes(
    fast_debounce: float,
) -> None:
    server = make_server()
    pushed = Pushed()
    _context = await initialized(server, pushed)
    server.register_prompt(lambda: "y", name="second")
    assert await pushed.wait_for(1) == [PROMPTS_CHANGED]
    server.register_resource(lambda name: name, "x://items/{name}", name="items")
    assert await pushed.wait_for(2) == [PROMPTS_CHANGED, RESOURCES_CHANGED]
    server.unregister_resource("x://first")
    server.unregister_prompt("first")
    await settle(fast_debounce)
    assert sorted(m["method"] for m in pushed.frames[2:]) == [
        "notifications/prompts/list_changed",
        "notifications/resources/list_changed",
    ]
    # Added and removed within one window: the list is what it was, nothing is sent.
    before = len(pushed.frames)
    server.register_prompt(lambda: "z", name="fleeting")
    server.unregister_prompt("fleeting")
    await settle(fast_debounce)
    assert len(pushed.frames) == before


async def test_hidden_changes_are_not_announced(fast_debounce: float) -> None:
    server = make_server(auth=APIKeyAuth({SEE_KEY: ["see"]}))
    pushed = Pushed()
    _context = await initialized(server, pushed)
    server.register_prompt(lambda: "x", name="secret", scopes=("see",))
    server.register_resource(lambda: "x", "x://secret", name="secret", scopes=("see",))
    await settle(fast_debounce)
    assert pushed.frames == []
    seeing = Pushed()
    who = ClientIdentity(fingerprint="s" * 12, scopes=frozenset({"see"}))
    _other = await initialized(server, seeing, who)
    server.register_prompt(lambda: "x", name="secret2", scopes=("see",))
    assert await seeing.wait_for(1) == [PROMPTS_CHANGED]
    await settle(fast_debounce)
    assert pushed.frames == []


async def test_listen_honors_prompts_and_resources_list_changed(fast_debounce: float) -> None:
    server = make_server()
    pushed = Pushed()
    context = make_context(push=pushed)
    task = await acknowledged(
        server, context, pushed, promptsListChanged=True, resourcesListChanged=True
    )
    ack = pushed.frames[0]
    assert ack["params"]["notifications"] == {
        "promptsListChanged": True,
        "resourcesListChanged": True,
    }
    server.register_resource(lambda v: v, "x://t/{v}", name="t")
    frames = await pushed.wait_for(2)
    assert frames[1] == {
        "jsonrpc": "2.0",
        "method": "notifications/resources/list_changed",
        "params": {"_meta": {TAG: "l1"}},
    }
    server.close_subscriptions(context, reason="shutdown")
    assert await asyncio.wait_for(task, 5) is None


async def test_a_capability_added_after_a_session_began_is_not_announced_to_it(
    fast_debounce: float,
) -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    pushed = Pushed()
    _context = await initialized(server, pushed)
    server.register_prompt(lambda: "x", name="late")  # its client was never told of prompts
    await settle(fast_debounce)
    assert pushed.frames == []
