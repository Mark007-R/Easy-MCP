"""Interop of resources, prompts and completion with the official MCP Python SDK client.

Skipped unless ``EASY_MCP_LIVE_SDK_CLIENT=1`` is set and the ``mcp`` package
(2.3 or later) is installed: install it next to easy_mcp in a virtualenv of
its own, ``pip install "mcp>=2.3" -e .``, then run this file.

The SDK is driven in each mode it offers: ``auto`` (it probes
``server/discover`` and speaks 2026-07-28 here), pinned to ``2026-07-28``,
and ``legacy`` (the ``initialize`` handshake), over Streamable HTTP and over
stdio.  Resource updates reach legacy clients through ``resources/subscribe``
(on the session's ``GET /mcp`` stream, or stdout), and stateless clients
through ``subscriptions/listen`` with ``resourceSubscriptions``.
"""

from __future__ import annotations

import asyncio
import base64
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import live_sdk
import pytest

from easy_mcp import MCPServer

pytestmark = live_sdk.marker

REPO_ROOT = Path(__file__).resolve().parent.parent
LiveServer = Callable[[Any], str]
MODES = ["auto", "2026-07-28", "legacy"]
LOGO = b"\x89PNG\r\n\x1a\n" + bytes(range(16))
EXTRA_PROMPTS = 150

# The same server, run as the SDK's stdio subprocess.  touch() publishes a
# resource update and grow() registers a prompt and a resource, as an
# application changing them at runtime would.
STDIO_SERVER = '''
from typing import Literal

from easy_mcp import MCPServer

server = MCPServer(name="rp-interop", rate_limit_per_minute=None)


@server.resource("config://app", mime_type="application/json")
def app_config() -> dict:
    """The application's configuration."""
    return {"region": "eu-west-1"}


@server.resource("users://{user_id}/avatar", mime_type="image/png")
def avatar(user_id: int) -> bytes:
    """A user's avatar."""
    return bytes([user_id % 256]) * 4


@server.prompt
def code_review(code: str, language: Literal["python", "go", "rust"] = "python") -> str:
    """Review a snippet."""
    return f"Review this {language} code: {code}"


@server.tool
def touch(uri: str) -> int:
    """Publish an update of a resource."""
    return server.notify_resource_updated(uri)


@server.tool
def grow() -> str:
    """Register a prompt and a resource."""
    server.register_prompt(lambda: "late", name="late")
    server.register_resource(lambda: "late", "late://x", name="late")
    return "grown"


server.run("stdio")
'''


def make_server() -> MCPServer:
    server = MCPServer(port=0, name="rp-interop", rate_limit_per_minute=None)

    @server.resource("config://app", mime_type="application/json")
    def app_config() -> dict[str, Any]:
        """The application's configuration."""
        return {"region": "eu-west-1"}

    @server.resource("logo://png", mime_type="image/png")
    def logo() -> bytes:
        """The logo."""
        return LOGO

    @server.resource("users://{user_id}/avatar", mime_type="image/png")
    def avatar(user_id: int) -> bytes:
        """A user's avatar."""
        return bytes([user_id % 256]) * 4

    @server.resource("docs://{+path}", complete={"path": ["guides/setup.md", "guides/faq.md"]})
    def doc(path: str) -> str | None:
        """A document."""
        return f"# {path}" if path.endswith(".md") else None

    @server.prompt
    def code_review(code: str, language: Literal["python", "go", "rust"] = "python") -> str:
        """Review a snippet."""
        return f"Review this {language} code: {code}"

    for index in range(EXTRA_PROMPTS):
        server.register_prompt(lambda: "filler", name=f"p{index:03d}")
    return server


def stdio_parameters() -> Any:
    from mcp.client.stdio import StdioServerParameters

    return StdioServerParameters(
        command=sys.executable,
        args=["-c", STDIO_SERVER],
        env={"PYTHONPATH": str(REPO_ROOT), "PYTHONUNBUFFERED": "1"},
        cwd=str(REPO_ROOT),
    )


class Heard:
    """A message_handler that records the notifications it gets."""

    def __init__(self) -> None:
        self.messages: list[Any] = []
        self.changed = asyncio.Event()

    async def __call__(self, message: Any) -> None:
        method = getattr(message, "method", None)
        if isinstance(method, str) and method.startswith("notifications/"):
            self.messages.append(message)
            self.changed.set()

    async def wait_for(self, method: str, timeout: float = 15.0) -> Any:
        async def found() -> Any:
            while True:
                for message in self.messages:
                    if message.method == method:
                        return message
                self.changed.clear()
                await self.changed.wait()

        return await asyncio.wait_for(found(), timeout)


async def next_event(subscription: Any, timeout: float = 15.0) -> Any:
    return await asyncio.wait_for(subscription.__anext__(), timeout)


def expected_version(mode: str) -> str:
    return "2025-11-25" if mode == "legacy" else "2026-07-28"


@pytest.mark.parametrize("mode", MODES)
async def test_sdk_lists_and_reads_resources_over_http(live_server: LiveServer, mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client, MCPError

    base = live_server(make_server())
    async with Client(f"{base}/mcp", mode=mode) as client:
        assert client.protocol_version == expected_version(mode)
        if mode != "2026-07-28":  # a pinned client takes the server on trust: no discover
            resources_capability = client.server_capabilities.resources
            assert resources_capability is not None
            assert resources_capability.subscribe is True
            assert resources_capability.list_changed is True
        listed = await client.list_resources()
        assert [str(r.uri) for r in listed.resources] == ["config://app", "logo://png"]
        templates = await client.list_resource_templates()
        assert [t.uri_template for t in templates.resource_templates] == [
            "docs://{+path}",
            "users://{user_id}/avatar",
        ]
        text = await client.read_resource("config://app")
        assert text.contents[0].text == '{"region": "eu-west-1"}'
        assert text.contents[0].mime_type == "application/json"
        blob = await client.read_resource("logo://png")
        assert base64.b64decode(blob.contents[0].blob) == LOGO
        avatar = await client.read_resource("users://7/avatar")
        assert base64.b64decode(avatar.contents[0].blob) == b"\x07" * 4
        doc = await client.read_resource("docs://guides/setup.md")
        assert doc.contents[0].text == "# guides/setup.md"
        with pytest.raises(MCPError) as missing:
            await client.read_resource("docs://guides/setup.txt")
        assert missing.value.code == (-32002 if mode == "legacy" else -32602)


@pytest.mark.parametrize("mode", MODES)
async def test_sdk_prompts_and_completion_over_http(live_server: LiveServer, mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client
    from mcp.types import PromptReference, ResourceTemplateReference

    base = live_server(make_server())
    async with Client(f"{base}/mcp", mode=mode) as client:
        names: list[str] = []
        cursor = None
        pages = 0
        while True:
            page = await client.list_prompts(cursor=cursor)
            names.extend(prompt.name for prompt in page.prompts)
            pages += 1
            cursor = page.next_cursor
            if cursor is None:
                break
        assert pages == 2 and len(names) == EXTRA_PROMPTS + 1 and names[0] == "code_review"
        review = next(p for p in (await client.list_prompts()).prompts if p.name == "code_review")
        assert [(a.name, a.required) for a in review.arguments or []] == [
            ("code", True),
            ("language", False),
        ]
        got = await client.get_prompt("code_review", {"code": "x = 1", "language": "go"})
        assert got.messages[0].role == "user"
        assert got.messages[0].content.text == "Review this go code: x = 1"
        completed = await client.complete(
            PromptReference(type="ref/prompt", name="code_review"),
            {"name": "language", "value": "r"},
        )
        assert completed.completion.values == ["rust"]
        paths = await client.complete(
            ResourceTemplateReference(type="ref/resource", uri="docs://{+path}"),
            {"name": "path", "value": "guides/f"},
        )
        assert paths.completion.values == ["guides/faq.md"]


async def test_sdk_legacy_subscription_over_http(live_server: LiveServer) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    server = make_server()
    base = live_server(server)
    heard = Heard()
    async with Client(f"{base}/mcp", mode="legacy", message_handler=heard) as client:
        await client.subscribe_resource("config://app")
        # The session's GET stream opens in the background: retry until it delivers.
        for _ in range(30):
            await asyncio.to_thread(server.notify_resource_updated, "config://app")
            try:
                message = await heard.wait_for("notifications/resources/updated", timeout=1.0)
                break
            except TimeoutError:
                continue
        else:
            raise AssertionError("no resource update arrived")
        assert str(message.params.uri) == "config://app"
        await client.unsubscribe_resource("config://app")


@pytest.mark.parametrize("mode", ["auto", "2026-07-28"])
async def test_sdk_listen_resource_subscriptions_over_http(
    live_server: LiveServer, mode: str
) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    server = make_server()
    base = live_server(server)
    async with Client(f"{base}/mcp", mode=mode) as client:
        async with client.listen(
            resource_subscriptions=["config://app", "missing://x"]
        ) as subscription:
            assert list(subscription.honored.resource_subscriptions or []) == ["config://app"]
            await asyncio.to_thread(server.notify_resource_updated, "config://app")
            event = await next_event(subscription)
            assert type(event).__name__ == "ResourceUpdated" and event.uri == "config://app"


@pytest.mark.parametrize("mode", ["auto", "2026-07-28"])
async def test_sdk_listen_prompts_and_resources_list_changed_over_http(
    live_server: LiveServer, mode: str
) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    server = make_server()
    base = live_server(server)
    async with Client(f"{base}/mcp", mode=mode) as client:
        async with client.listen(
            prompts_list_changed=True, resources_list_changed=True
        ) as subscription:
            assert subscription.honored.prompts_list_changed is True
            assert subscription.honored.resources_list_changed is True
            await asyncio.to_thread(server.register_prompt, lambda: "late", name="late")
            assert type(await next_event(subscription)).__name__ == "PromptsListChanged"
            await asyncio.to_thread(server.register_resource, lambda: "x", "late://x", name="late")
            assert type(await next_event(subscription)).__name__ == "ResourcesListChanged"
        listed = await client.list_resources(cache_mode="bypass")
        assert "late://x" in [str(r.uri) for r in listed.resources]


async def test_sdk_legacy_list_changed_for_prompts_and_resources_over_http(
    live_server: LiveServer,
) -> None:
    live_sdk.require_sdk()
    from mcp import Client

    server = make_server()
    base = live_server(server)
    heard = Heard()
    async with Client(f"{base}/mcp", mode="legacy", message_handler=heard) as client:
        for _ in range(30):  # until the session's GET stream is open
            name = f"late{len(heard.messages)}{_}"
            await asyncio.to_thread(server.register_prompt, lambda: "late", name=name)
            try:
                await heard.wait_for("notifications/prompts/list_changed", timeout=1.0)
                break
            except TimeoutError:
                continue
        else:
            raise AssertionError("no prompts list_changed arrived")
        await asyncio.to_thread(server.register_resource, lambda: "x", "late://x", name="late")
        await heard.wait_for("notifications/resources/list_changed")
        listed = await client.list_resources()
        assert "late://x" in [str(r.uri) for r in listed.resources]


@pytest.mark.parametrize("mode", MODES)
async def test_sdk_stdio_resources_prompts_and_updates(mode: str) -> None:
    live_sdk.require_sdk()
    from mcp import Client
    from mcp.types import PromptReference

    heard = Heard()
    async with Client(stdio_parameters(), mode=mode, message_handler=heard) as client:
        assert client.protocol_version == expected_version(mode)
        listed = await client.list_resources()
        assert [str(r.uri) for r in listed.resources] == ["config://app"]
        avatar = await client.read_resource("users://3/avatar")
        assert base64.b64decode(avatar.contents[0].blob) == b"\x03" * 4
        got = await client.get_prompt("code_review", {"code": "f()"})
        assert got.messages[0].content.text == "Review this python code: f()"
        completed = await client.complete(
            PromptReference(type="ref/prompt", name="code_review"),
            {"name": "language", "value": "g"},
        )
        assert completed.completion.values == ["go"]
        if mode == "legacy":
            await client.subscribe_resource("config://app")
            await client.call_tool("touch", {"uri": "config://app"})
            message = await heard.wait_for("notifications/resources/updated")
            assert str(message.params.uri) == "config://app"
            await client.call_tool("grow", {})
            await heard.wait_for("notifications/prompts/list_changed")
            await heard.wait_for("notifications/resources/list_changed")
        else:
            async with client.listen(
                resource_subscriptions=["config://app"], prompts_list_changed=True
            ) as subscription:
                await client.call_tool("touch", {"uri": "config://app"})
                event = await next_event(subscription)
                assert type(event).__name__ == "ResourceUpdated" and event.uri == "config://app"
                await client.call_tool("grow", {})
                event = await next_event(subscription)
                assert type(event).__name__ == "PromptsListChanged"
        prompts = await client.list_prompts(cache_mode="bypass")
        assert "late" in [prompt.name for prompt in prompts.prompts]
