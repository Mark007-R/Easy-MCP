"""Prompts: registration, listing, arguments, messages, errors, execution, audit."""

from __future__ import annotations

import asyncio
import base64
import json
import threading
from collections.abc import Callable
from typing import Annotated, Any, Literal

import httpx
import pytest
from conftest import LogCapture, headers_for, make_context, modern, notification, rpc

from easy_mcp import (
    APIKeyAuth,
    Audio,
    ClientIdentity,
    Image,
    MCPServer,
    Message,
    PromptDefinition,
    RegistrationError,
    ResourceContent,
    ResourceLink,
    ToolError,
    current_cancel_token,
    current_identity,
)
from easy_mcp.exceptions import (
    HEADER_MISMATCH,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    TOOL_TIMEOUT,
)

DEV_KEY = "prompts-dev-key-" + "d" * 16


def make_server(**kwargs: Any) -> MCPServer:
    kwargs.setdefault("rate_limit_per_minute", None)
    return MCPServer(port=0, **kwargs)


async def call(server: MCPServer, message: dict[str, Any], **ctx: Any) -> dict[str, Any]:
    response = await server.dispatch(message, make_context(**ctx))
    assert response is not None
    return response


def get(name: Any, arguments: Any = None, msg_id: Any = 1) -> dict[str, Any]:
    params: dict[str, Any] = {"name": name}
    if arguments is not None:
        params["arguments"] = arguments
    return rpc("prompts/get", params, msg_id)


def review_server() -> MCPServer:
    server = make_server()

    @server.prompt
    def summarize(text: str) -> str:
        """Summarize a passage in three bullet points."""
        return f"Summarize this in three bullet points:\n\n{text}"

    @server.prompt(title="Review code")
    def code_review(
        code: Annotated[str, "The code to review"],
        language: Literal["python", "go", "rust"] = "python",
        max_issues: int = 5,
    ) -> list[Message]:
        """Ask for a focused review of a snippet."""
        return [
            Message.user(f"Review this {language} code; list at most {max_issues} issues."),
            Message.user(
                ResourceContent(
                    uri="docs://style/" + language, text="style guide", mime_type="text/markdown"
                )
            ),
            Message.user(code),
        ]

    return server


# ----------------------------------------------------------------- registration


def test_bare_decorator_and_options() -> None:
    server = make_server()

    def plain(text: str) -> str:
        """Plain."""
        return text

    assert server.prompt(plain) is plain
    decorated = server.prompt(name="named", title="Named", description="Given.")(plain)
    assert decorated is plain
    names = [p.name for p in server.prompts]
    assert names == ["named", "plain"]
    named = server.prompts[0]
    assert isinstance(named, PromptDefinition)
    assert named.to_mcp() == {
        "name": "named",
        "title": "Named",
        "description": "Given.",
        "arguments": [{"name": "text", "required": True}],
    }


def test_name_rules_and_duplicates() -> None:
    server = make_server()
    for bad in ("", "1abc", "has space", "x" * 65, "a.b"):
        with pytest.raises(RegistrationError, match="invalid prompt name"):
            server.register_prompt(lambda: "x", name=bad)
    server.register_prompt(lambda: "x", name="once")
    with pytest.raises(RegistrationError, match="already registered"):
        server.register_prompt(lambda: "y", name="once")
    assert server.unregister_prompt("once").name == "once"
    with pytest.raises(RegistrationError, match="no prompt"):
        server.unregister_prompt("once")


def test_arguments_come_from_the_signature() -> None:
    server = make_server()

    @server.prompt
    def documented(
        first: Annotated[str, "From the annotation"],
        second: int = 3,
        third: bool | None = None,
    ) -> str:
        """A prompt.

        Args:
            first: Loses to the annotation.
            second: From the docstring.
        """
        return ""

    (definition,) = server.prompts
    assert definition.to_mcp()["arguments"] == [
        {"name": "first", "description": "From the annotation", "required": True},
        {"name": "second", "description": "From the docstring.", "required": False},
        {"name": "third", "required": False},
    ]


def test_unsupported_argument_types_are_refused() -> None:
    server = make_server()

    def as_list(values: list[str]) -> str:
        return ""

    def as_dict(values: dict[str, str]) -> str:
        return ""

    def as_any(value: Any) -> str:
        return ""

    def star(*values: str) -> str:
        return ""

    def positional(value: str, /) -> str:
        return ""

    def literal_object(value: Literal[None]) -> str:
        return ""

    for fn in (as_list, as_dict, as_any, star, positional, literal_object):
        with pytest.raises(RegistrationError, match="cannot register prompt"):
            server.register_prompt(fn)


def test_scopes_imply_requires_auth_and_complete_keys_are_checked() -> None:
    server = make_server()
    definition = server.register_prompt(lambda: "x", name="guarded", scopes=("dev", "dev"))
    assert definition.requires_auth and definition.declared_scopes == ("dev",)
    with pytest.raises(RegistrationError, match="no argument"):
        server.register_prompt(lambda text: text, name="bad", complete={"other": ["a"]})
    with pytest.raises(RegistrationError, match="timeout"):
        server.register_prompt(lambda: "x", name="slow", timeout=0)


# ----------------------------------------------------------------- capabilities


async def test_no_prompt_capability_without_prompts() -> None:
    server = make_server()
    init = await call(server, rpc("initialize", {"protocolVersion": "2025-11-25"}))
    assert "prompts" not in init["result"]["capabilities"]
    for method in ("prompts/list", "prompts/get"):
        response = await call(server, rpc(method, {"name": "x"}))
        assert response["error"]["code"] == METHOD_NOT_FOUND


async def test_prompt_capability_with_prompts() -> None:
    server = make_server()
    server.register_prompt(lambda: "x", name="one")
    init = await call(server, rpc("initialize", {"protocolVersion": "2025-11-25"}))
    assert init["result"]["capabilities"]["prompts"] == {"listChanged": True}
    discover = await call(server, modern("server/discover"))
    assert discover["result"]["capabilities"]["prompts"] == {"listChanged": True}
    # A prompt without Literal, bool or complete= offers no completion.
    assert "completions" not in discover["result"]["capabilities"]


# -------------------------------------------------------------------- listing


async def test_prompts_list_is_sorted_and_shaped() -> None:
    server = review_server()
    response = await call(server, rpc("prompts/list"))
    assert response["result"] == {
        "prompts": [
            {
                "name": "code_review",
                "title": "Review code",
                "description": "Ask for a focused review of a snippet.",
                "arguments": [
                    {"name": "code", "description": "The code to review", "required": True},
                    {"name": "language", "required": False},
                    {"name": "max_issues", "required": False},
                ],
            },
            {
                "name": "summarize",
                "description": "Summarize a passage in three bullet points.",
                "arguments": [{"name": "text", "required": True}],
            },
        ]
    }


# -------------------------------------------------------------------- messages


async def test_string_result_is_one_user_message() -> None:
    server = review_server()
    response = await call(server, get("summarize", {"text": "Once upon a time"}))
    assert response["result"] == {
        "description": "Summarize a passage in three bullet points.",
        "messages": [
            {
                "role": "user",
                "content": {
                    "type": "text",
                    "text": "Summarize this in three bullet points:\n\nOnce upon a time",
                },
            }
        ],
    }


async def test_message_list_with_every_content_type() -> None:
    server = make_server()

    @server.prompt
    def everything() -> list[Any]:
        return [
            "plain text",
            Message.assistant("from the assistant"),
            Message.user(Image(b"\x89PNG", "image/png")),
            Message.user(Audio(b"RIFF", "audio/wav")),
            Message.user(ResourceContent(blob=b"\x00", uri="x://b", mime_type="application/x")),
            Message.user(
                ResourceLink(
                    uri="db://t",
                    name="t",
                    title="T",
                    description="A table",
                    mime_type="text/plain",
                    size=5,
                )
            ),
            {"role": "assistant", "content": {"type": "text", "text": "raw"}},
            Message.user({"type": "resource", "resource": {"uri": "x://r", "text": "r"}}),
        ]

    response = await call(server, get("everything"))
    assert "description" not in response["result"]
    assert response["result"]["messages"] == [
        {"role": "user", "content": {"type": "text", "text": "plain text"}},
        {"role": "assistant", "content": {"type": "text", "text": "from the assistant"}},
        {
            "role": "user",
            "content": {
                "type": "image",
                "data": base64.b64encode(b"\x89PNG").decode(),
                "mimeType": "image/png",
            },
        },
        {
            "role": "user",
            "content": {
                "type": "audio",
                "data": base64.b64encode(b"RIFF").decode(),
                "mimeType": "audio/wav",
            },
        },
        {
            "role": "user",
            "content": {
                "type": "resource",
                "resource": {"uri": "x://b", "mimeType": "application/x", "blob": "AA=="},
            },
        },
        {
            "role": "user",
            "content": {
                "type": "resource_link",
                "uri": "db://t",
                "name": "t",
                "title": "T",
                "description": "A table",
                "mimeType": "text/plain",
                "size": 5,
            },
        },
        {"role": "assistant", "content": {"type": "text", "text": "raw"}},
        {
            "role": "user",
            "content": {"type": "resource", "resource": {"uri": "x://r", "text": "r"}},
        },
    ]


def test_content_helpers_check_their_fields() -> None:
    with pytest.raises(ValueError):
        ResourceContent()
    with pytest.raises(ValueError):
        ResourceContent(text="a", blob=b"b")
    with pytest.raises(ValueError):
        Image(b"x", "png")
    with pytest.raises(ValueError):
        Audio("not bytes", "audio/wav")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ResourceLink(uri="", name="x")
    with pytest.raises(ValueError):
        ResourceLink(uri="x://a", name="x", size=-1)
    with pytest.raises(ValueError):
        Message("system", "x")  # type: ignore[arg-type]
    assert Message.user("hi") == Message("user", "hi")


async def test_embedded_resource_needs_uri_and_mime_type(logs: LogCapture) -> None:
    server = make_server()
    server.register_prompt(lambda: [Message.user(ResourceContent(text="t"))], name="no_uri")
    server.register_prompt(
        lambda: [Message.user(ResourceContent(text="t", uri="x://a"))], name="no_mime"
    )
    server.register_prompt(
        lambda: [Message.user({"type": "resource", "resource": {"uri": "x://a"}})],
        name="raw_without_text",
    )
    server.register_prompt(lambda: [Message.user({"type": "video"})], name="unknown_type")
    server.register_prompt(lambda: [Message.user({"type": "image", "data": "x"})], name="partial")
    for name in ("no_uri", "no_mime", "raw_without_text", "unknown_type", "partial"):
        response = await call(server, get(name))
        assert response["error"]["code"] == INTERNAL_ERROR, name
        assert "error_id=" in response["error"]["message"]


async def test_arguments_are_converted() -> None:
    server = make_server()
    seen: list[Any] = []

    @server.prompt
    def typed(
        count: int,
        ratio: float,
        flag: bool,
        mode: Literal["a", 1, True],
        label: str | None = None,
        fallback: int = 7,
    ) -> str:
        seen.append((count, ratio, flag, mode, label, fallback))
        return "ok"

    await call(server, get("typed", {"count": "+42", "ratio": "1e3", "flag": "False", "mode": "1"}))
    await call(
        server,
        get("typed", {"count": "-1", "ratio": ".5", "flag": "TRUE", "mode": "True", "label": "x"}),
    )
    assert seen == [(42, 1000.0, False, 1, None, 7), (-1, 0.5, True, True, "x", 7)]


async def test_every_argument_violation_is_reported() -> None:
    server = make_server()

    @server.prompt
    def strict(
        required: str,
        number: int,
        mode: Literal["a", "b"],
        ratio: float = 1.0,
    ) -> str:
        return "unreachable"

    response = await call(
        server,
        get("strict", {"number": "1.5", "mode": "c", "ratio": "1e999", "extra": "x", "other": 3}),
    )
    error = response["error"]
    assert error["code"] == INVALID_PARAMS
    assert error["message"].startswith("Invalid prompt arguments: ")
    assert sorted(error["data"]["errors"]) == sorted(
        [
            "arguments.extra: unexpected argument",
            "arguments.other: unexpected argument",
            "arguments.required: missing required argument",
            "arguments.number: expected integer",
            "arguments.mode: must be one of ['a', 'b']",
            "arguments.ratio: expected a finite number",
        ]
    )
    wrong_type = await call(server, get("strict", {"required": 5, "number": "1", "mode": "a"}))
    assert wrong_type["error"]["data"]["errors"] == ["arguments.required: expected string, got int"]
    not_object = await call(server, get("strict", ["a"]))
    assert not_object["error"] == {
        "code": INVALID_PARAMS,
        "message": "'arguments' must be an object",
    }


async def test_unknown_and_hidden_prompts_answer_alike() -> None:
    server = make_server(auth=APIKeyAuth({DEV_KEY: ["dev"]}))
    server.register_prompt(lambda: "secret", name="internal", scopes=("dev",))
    hidden = await call(server, get("internal"))
    missing = await call(server, get("nothing"))
    assert hidden["error"] == {"code": INVALID_PARAMS, "message": "Unknown prompt: internal"}
    assert missing["error"] == {"code": INVALID_PARAMS, "message": "Unknown prompt: nothing"}
    listed = await call(server, rpc("prompts/list"))
    assert listed["result"] == {"prompts": []}
    dev = ClientIdentity(fingerprint="d" * 12, scopes=frozenset({"dev"}))
    allowed = await call(server, get("internal"), identity=dev)
    assert allowed["result"]["messages"][0]["content"]["text"] == "secret"
    no_name = await call(server, rpc("prompts/get", {}))
    assert no_name["error"] == {
        "code": INVALID_PARAMS,
        "message": "prompts/get requires a string 'name'",
    }


async def test_description_is_returned() -> None:
    server = make_server()
    server.register_prompt(lambda: "x", name="described", description="What it does.")
    server.register_prompt(lambda: "x", name="silent")
    described = await call(server, get("described"))
    assert described["result"]["description"] == "What it does."
    silent = await call(server, get("silent"))
    assert "description" not in silent["result"]


async def test_bad_return_value_is_an_internal_error(logs: LogCapture) -> None:
    server = make_server()
    server.register_prompt(lambda: 42, name="number")
    server.register_prompt(lambda: [object()], name="objects")
    server.register_prompt(lambda: {"role": "system", "content": "x"}, name="system")
    for name in ("number", "objects", "system"):
        response = await call(server, get(name))
        assert response["error"]["code"] == INTERNAL_ERROR, name
    assert "cannot return" in logs.text


async def test_tool_error_text_is_shown() -> None:
    server = make_server()

    def refuse() -> str:
        raise ToolError("That prompt is disabled.")

    server.register_prompt(refuse)
    response = await call(server, get("refuse"))
    assert response["error"] == {"code": INTERNAL_ERROR, "message": "That prompt is disabled."}


# ---------------------------------------------------------------- stateless era


async def test_stateless_get_has_result_type_but_no_cache_hints() -> None:
    server = review_server()
    response = await call(
        server, modern("prompts/get", {"name": "summarize", "arguments": {"text": "x"}})
    )
    result = response["result"]
    assert result["resultType"] == "complete"
    assert "io.modelcontextprotocol/serverInfo" in result["_meta"]
    assert "ttlMs" not in result and "cacheScope" not in result
    listed = await call(server, modern("prompts/list"))
    assert listed["result"]["ttlMs"] == 0 and listed["result"]["cacheScope"] == "public"


async def test_http_get_requires_matching_mcp_name(live_server: Callable[[Any], str]) -> None:
    server = review_server()
    base = live_server(server)
    message = modern("prompts/get", {"name": "summarize", "arguments": {"text": "hi"}})
    async with httpx.AsyncClient(timeout=10) as client:
        ok = await client.post(
            f"{base}/mcp", json=message, headers=headers_for(message, **{"Mcp-Name": "summarize"})
        )
        assert ok.status_code == 200, ok.text
        assert ok.json()["result"]["messages"][0]["role"] == "user"
        for headers in (headers_for(message), headers_for(message, **{"Mcp-Name": "other"})):
            refused = await client.post(f"{base}/mcp", json=message, headers=headers)
            assert refused.status_code == 400
            assert refused.json()["error"]["code"] == HEADER_MISMATCH


# -------------------------------------------------------------------- execution


async def test_prompt_timeout_and_cancellation(logs: LogCapture) -> None:
    server = make_server()

    @server.prompt(timeout=0.2)
    async def slow() -> str:
        await asyncio.sleep(30)
        return "late"

    response = await call(server, get("slow"))
    assert response["error"] == {
        "code": TOOL_TIMEOUT,
        "message": "Prompt 'slow' timed out after 0.2s",
    }

    entered = threading.Event()
    fired = threading.Event()

    @server.prompt
    def waiting() -> str:
        token = current_cancel_token()
        assert token is not None
        entered.set()
        if token.wait(10):
            fired.set()
        return "late"

    context = make_context()
    pending = asyncio.ensure_future(server.dispatch(get("waiting", None, "p1"), context))
    assert await asyncio.to_thread(entered.wait, 10)
    await server.dispatch(notification("notifications/cancelled", {"requestId": "p1"}), context)
    assert await asyncio.wait_for(pending, 10) is None
    assert await asyncio.to_thread(fired.wait, 10)
    await server.wait_for_tool_threads(5)
    events = logs.events("request_cancelled")
    assert [(e["method"], e["request_id"]) for e in events] == [("prompts/get", "p1")]


async def test_prompts_see_the_caller() -> None:
    server = make_server(auth=APIKeyAuth({DEV_KEY: ["dev"]}))
    seen: list[Any] = []

    @server.prompt
    def who() -> str:
        caller = current_identity()
        seen.append(caller.scopes if caller else None)
        return "ok"

    dev = ClientIdentity(fingerprint="d" * 12, scopes=frozenset({"dev"}))
    await call(server, get("who"), identity=dev)
    assert seen == [frozenset({"dev"})]


async def test_prompt_get_is_audited_without_argument_values(logs: LogCapture) -> None:
    server = review_server()
    await call(server, get("summarize", {"text": "TOP-SECRET-TEXT"}))
    await call(server, get("summarize", {}))
    events = logs.events("prompt_get")
    assert [(e["prompt"], e["status"]) for e in events] == [
        ("summarize", "ok"),
        ("summarize", "denied"),
    ]
    assert "TOP-SECRET-TEXT" not in json.dumps(events)
    assert "TOP-SECRET-TEXT" not in logs.text
