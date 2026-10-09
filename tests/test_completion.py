"""Argument completion (completion/complete) for prompts and resource templates."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Iterator, Mapping
from typing import Any, Literal

import pytest
from conftest import LogCapture, make_context, modern, rpc

from easy_mcp import APIKeyAuth, ClientIdentity, MCPServer, RegistrationError, ToolError
from easy_mcp.exceptions import INTERNAL_ERROR, INVALID_PARAMS, METHOD_NOT_FOUND, TOOL_TIMEOUT

DEV_KEY = "completion-dev-key-" + "c" * 13


def make_server(**kwargs: Any) -> MCPServer:
    kwargs.setdefault("rate_limit_per_minute", None)
    return MCPServer(port=0, **kwargs)


def complete(
    ref: Any, name: Any, value: Any = "", context: Any = None, msg_id: Any = 1
) -> dict[str, Any]:
    params: dict[str, Any] = {"ref": ref, "argument": {"name": name, "value": value}}
    if context is not None:
        params["context"] = context
    return rpc("completion/complete", params, msg_id)


def prompt_ref(name: str) -> dict[str, Any]:
    return {"type": "ref/prompt", "name": name}


def resource_ref(uri: str) -> dict[str, Any]:
    return {"type": "ref/resource", "uri": uri}


async def completion_of(server: MCPServer, message: dict[str, Any], **ctx: Any) -> Any:
    response = await server.dispatch(message, make_context(**ctx))
    assert response is not None and "result" in response, response
    return response["result"]["completion"]


async def error_of(server: MCPServer, message: dict[str, Any]) -> dict[str, Any]:
    response = await server.dispatch(message, make_context())
    assert response is not None and "error" in response, response
    return dict(response["error"])


async def test_literal_and_bool_arguments_complete_automatically() -> None:
    server = make_server()

    @server.prompt
    def review(language: Literal["python", "go", "rust"], strict: bool = False) -> str:
        return ""

    assert await completion_of(server, complete(prompt_ref("review"), "language", "py")) == {
        "values": ["python"],
        "total": 1,
        "hasMore": False,
    }
    assert await completion_of(server, complete(prompt_ref("review"), "language")) == {
        "values": ["python", "go", "rust"],
        "total": 3,
        "hasMore": False,
    }
    assert await completion_of(server, complete(prompt_ref("review"), "strict", "T")) == {
        "values": ["true"],
        "total": 1,
        "hasMore": False,
    }


async def test_static_list_prefix_then_substring_case_insensitive() -> None:
    server = make_server()
    tables = ["users", "Orders", "user_roles", "audit_users", "items"]

    @server.prompt(complete={"table": tables})
    def explain(table: str) -> str:
        return ""

    found = await completion_of(server, complete(prompt_ref("explain"), "table", "USER"))
    assert found == {"values": ["users", "user_roles", "audit_users"], "total": 3, "hasMore": False}
    found = await completion_of(server, complete(prompt_ref("explain"), "table", "order"))
    assert found["values"] == ["Orders"]


async def test_callable_completer_gets_value_and_context() -> None:
    server = make_server()
    calls: list[tuple[str, dict[str, str]]] = []

    def sync_completer(value: str, arguments: Mapping[str, str]) -> list[str]:
        calls.append((value, dict(arguments)))
        return [f"{arguments.get('schema', '?')}.{value}x"]

    async def async_completer(value: str, arguments: Mapping[str, str]) -> Iterator[str]:
        await asyncio.sleep(0)
        calls.append((value, dict(arguments)))
        return iter([value + "1", value + "2"])

    @server.prompt(complete={"table": sync_completer, "column": async_completer})
    def query(schema: str, table: str, column: str) -> str:
        return ""

    context = {"arguments": {"schema": "public", "unrelated": "dropped"}}
    found = await completion_of(server, complete(prompt_ref("query"), "table", "or", context))
    assert found == {"values": ["public.orx"], "total": 1, "hasMore": False}
    found = await completion_of(server, complete(prompt_ref("query"), "column", "id", context))
    assert found == {"values": ["id1", "id2"], "hasMore": False}  # an iterator: no total
    assert calls == [("or", {"schema": "public"}), ("id", {"schema": "public"})]


async def test_values_capped_at_100_with_total_and_has_more() -> None:
    server = make_server()
    many = [f"v{index:03d}" for index in range(250)]

    @server.prompt(complete={"name": many, "other": lambda value, _: list(many)})
    def pick(name: str, other: str) -> str:
        return ""

    for argument in ("name", "other"):
        found = await completion_of(server, complete(prompt_ref("pick"), argument, "v"))
        assert found["values"] == many[:100]
        assert found["total"] == 250 and found["hasMore"] is True


async def test_endless_generator_is_cut_at_101() -> None:
    server = make_server()
    produced: list[int] = []

    def endless(value: str, _: Mapping[str, str]) -> Iterator[str]:
        for index in itertools.count():
            produced.append(index)
            yield f"{value}{index}"

    @server.prompt(complete={"name": endless})
    def pick(name: str) -> str:
        return ""

    found = await completion_of(server, complete(prompt_ref("pick"), "name", "n"))
    assert len(found["values"]) == 100 and found["hasMore"] is True
    assert "total" not in found
    assert len(produced) == 101


async def test_values_are_deduplicated_in_order() -> None:
    server = make_server()

    @server.prompt(complete={"name": lambda value, _: ["b", "a", "b", "c", "a"]})
    def pick(name: str) -> str:
        return ""

    found = await completion_of(server, complete(prompt_ref("pick"), "name"))
    assert found == {"values": ["b", "a", "c"], "total": 3, "hasMore": False}


async def test_template_variable_completion() -> None:
    server = make_server()

    @server.resource("db://{schema}/tables/{table}", complete={"table": ["users", "orders"]})
    def table(schema: Literal["public", "audit"], table: str) -> str:
        return ""

    server.register_resource(lambda: "x", "db://index", name="index")
    found = await completion_of(
        server, complete(resource_ref("db://{schema}/tables/{table}"), "table", "o")
    )
    assert found == {"values": ["orders"], "total": 1, "hasMore": False}
    found = await completion_of(
        server, complete(resource_ref("db://{schema}/tables/{table}"), "schema", "a")
    )
    assert found["values"] == ["audit"]
    concrete = await completion_of(server, complete(resource_ref("db://index"), "anything"))
    assert concrete == {"values": [], "hasMore": False}


async def test_known_argument_without_a_source_is_empty() -> None:
    server = make_server()

    @server.prompt(complete={"b": ["x"]})
    def two(a: str, b: str) -> str:
        return ""

    assert await completion_of(server, complete(prompt_ref("two"), "a", "q")) == {
        "values": [],
        "hasMore": False,
    }


async def test_invalid_requests_are_minus_32602() -> None:
    server = make_server()

    @server.prompt
    def review(language: Literal["python", "go"]) -> str:
        return ""

    server.register_resource(lambda id: "", "x://{id}", name="t", complete={"id": ["1"]})
    bad = [
        rpc("completion/complete", {"argument": {"name": "language", "value": ""}}),
        rpc("completion/complete", {"ref": prompt_ref("review")}),
        complete({"type": "ref/tool", "name": "review"}, "language"),
        complete({"type": "ref/prompt"}, "language"),
        complete({"type": "ref/resource", "uri": 5}, "id"),
        complete(prompt_ref("missing"), "language"),
        complete(resource_ref("x://{other}"), "id"),
        complete(prompt_ref("review"), "nope"),
        complete(prompt_ref("review"), 5),
        complete(prompt_ref("review"), "language", 7),
        complete(prompt_ref("review"), "language", "", context=["x"]),
        complete(prompt_ref("review"), "language", "", context={"arguments": {"a": 1}}),
        complete(prompt_ref("review"), "language", "", context={"arguments": "x"}),
    ]
    for message in bad:
        error = await error_of(server, message)
        assert error["code"] == INVALID_PARAMS, (message, error)


async def test_hidden_prompt_completer_never_runs() -> None:
    server = make_server(auth=APIKeyAuth({DEV_KEY: ["dev"]}))
    ran: list[str] = []

    def completer(value: str, _: Mapping[str, str]) -> list[str]:
        ran.append(value)
        return ["secret-table"]

    server.register_prompt(
        lambda table: "", name="internal", scopes=("dev",), complete={"table": completer}
    )
    server.register_resource(
        lambda name: "", "hidden://{name}", name="h", scopes=("dev",), complete={"name": completer}
    )
    for ref, argument in (
        (prompt_ref("internal"), "table"),
        (resource_ref("hidden://{name}"), "name"),
    ):
        error = await error_of(server, complete(ref, argument))
        assert error["code"] == INVALID_PARAMS
        assert error["message"].startswith(("Unknown prompt", "Unknown resource template"))
    assert ran == []
    dev = ClientIdentity(fingerprint="d" * 12, scopes=frozenset({"dev"}))
    found = await completion_of(server, complete(prompt_ref("internal"), "table"), identity=dev)
    assert found["values"] == ["secret-table"] and ran == [""]


async def test_completions_capability_only_with_a_source() -> None:
    server = make_server()
    server.register_prompt(lambda text: text, name="plain")
    server.register_resource(lambda id: id, "x://{id}", name="t")
    caps = (await server.dispatch(modern("server/discover"), make_context()) or {})["result"]
    assert "completions" not in caps["capabilities"]
    response = await server.dispatch(complete(prompt_ref("plain"), "text"), make_context())
    assert response is not None and response["error"]["code"] == METHOD_NOT_FOUND

    def choose(mode: Literal["a", "b"]) -> str:
        return mode

    server.register_prompt(choose)
    caps = (await server.dispatch(modern("server/discover"), make_context()) or {})["result"]
    assert caps["capabilities"]["completions"] == {}
    with_template = make_server()
    with_template.register_resource(lambda id: id, "y://{id}", name="t", complete={"id": ["1"]})
    caps = (await with_template.dispatch(modern("server/discover"), make_context()) or {})["result"]
    assert caps["capabilities"]["completions"] == {}


def test_bare_string_completer_is_refused() -> None:
    server = make_server()
    with pytest.raises(RegistrationError, match="bare string"):
        server.register_prompt(lambda name: name, name="p", complete={"name": "abc"})
    with pytest.raises(RegistrationError, match="strings only"):
        server.register_prompt(lambda name: name, name="p", complete={"name": [1, 2]})
    with pytest.raises(RegistrationError, match="list of strings or a callable"):
        server.register_prompt(lambda name: name, name="p", complete={"name": 5})


async def test_completer_errors_are_sanitized(logs: LogCapture) -> None:
    server = make_server()

    def broken(value: str, _: Mapping[str, str]) -> list[str]:
        raise RuntimeError("connection string with a password")

    def refusing(value: str, _: Mapping[str, str]) -> list[str]:
        raise ToolError("Completion is off for this field.")

    def numbers(value: str, _: Mapping[str, str]) -> list[Any]:
        return [1, 2]

    def text(value: str, _: Mapping[str, str]) -> str:
        return "abc"

    @server.prompt(complete={"a": broken, "b": refusing, "c": numbers, "d": text})
    def fields(a: str, b: str, c: str, d: str) -> str:
        return ""

    error = await error_of(server, complete(prompt_ref("fields"), "a"))
    assert error["code"] == INTERNAL_ERROR and "error_id=" in error["message"]
    assert "password" not in error["message"]
    assert "completer 'fields.a' failed" in logs.text
    assert await error_of(server, complete(prompt_ref("fields"), "b")) == {
        "code": INTERNAL_ERROR,
        "message": "Completion is off for this field.",
    }
    for argument in ("c", "d"):
        error = await error_of(server, complete(prompt_ref("fields"), argument))
        assert error["code"] == INTERNAL_ERROR and "error_id=" in error["message"]


async def test_completer_timeout_uses_the_prompts() -> None:
    server = make_server()

    async def slow(value: str, _: Mapping[str, str]) -> list[str]:
        await asyncio.sleep(30)
        return []

    @server.prompt(timeout=0.2, complete={"name": slow})
    def pick(name: str) -> str:
        return ""

    error = await error_of(server, complete(prompt_ref("pick"), "name"))
    assert error == {
        "code": TOOL_TIMEOUT,
        "message": "Completion for 'pick.name' timed out after 0.2s",
    }


async def test_stateless_completion_shape() -> None:
    server = make_server()

    @server.prompt
    def review(language: Literal["python", "go"]) -> str:
        return ""

    params = {"ref": prompt_ref("review"), "argument": {"name": "language", "value": "g"}}
    response = await server.dispatch(modern("completion/complete", params), make_context())
    assert response is not None
    result = response["result"]
    assert result["completion"] == {"values": ["go"], "total": 1, "hasMore": False}
    assert result["resultType"] == "complete"
    assert "ttlMs" not in result and "cacheScope" not in result


async def test_completion_is_not_audited(logs: LogCapture) -> None:
    server = make_server()

    @server.prompt
    def review(language: Literal["python", "go"]) -> str:
        return ""

    await completion_of(server, complete(prompt_ref("review"), "language", "p"))
    assert [r for r in logs.records if r.name == "easy_mcp.audit"] == []
