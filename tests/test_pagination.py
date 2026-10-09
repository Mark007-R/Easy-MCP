"""Pagination of resources/list, resources/templates/list and prompts/list."""

from __future__ import annotations

import base64
import json
import logging
from typing import Any

import pytest
from conftest import LogCapture, make_context, modern, rpc

from easy_mcp import APIKeyAuth, ClientIdentity, MCPServer
from easy_mcp.exceptions import INVALID_PARAMS, ProtocolError
from easy_mcp.pagination import PAGE_SIZE, decode_cursor, encode_cursor, paginate
from easy_mcp.uritemplate import MAX_URI_LENGTH

SEE_KEY = "pagination-see-key-" + "s" * 13
# Under the length cap, but JSON nested too deeply for the parser to recurse into.
NESTED = base64.urlsafe_b64encode(b"[" * 6000).decode().rstrip("=")


def keys(count: int) -> list[str]:
    return [f"item-{index:04d}" for index in range(count)]


def test_cursor_format_is_compact_base64url_json() -> None:
    cursor = encode_cursor("prompts", "code_review")
    assert cursor == "eyJrIjoicHJvbXB0cyIsImEiOiJjb2RlX3JldmlldyJ9"
    assert decode_cursor("prompts", cursor) == "code_review"
    assert "=" not in encode_cursor("resources", "x://a")
    assert decode_cursor("resources", encode_cursor("resources", "x://é/ü")) == "x://é/ü"


def test_short_lists_have_no_cursor_unit() -> None:
    page, cursor = paginate(keys(PAGE_SIZE), key=str, kind="prompts", cursor=None)
    assert len(page) == PAGE_SIZE and cursor is None
    assert paginate([], key=str, kind="prompts", cursor=None) == ([], None)


def test_cursor_walks_every_item_once_unit() -> None:
    items = keys(250)
    seen: list[str] = []
    cursor: Any = None
    sizes = []
    while True:
        page, cursor = paginate(items, key=str, kind="prompts", cursor=cursor)
        seen.extend(page)
        sizes.append(len(page))
        if cursor is None:
            break
    assert sizes == [100, 100, 50]
    assert seen == items


def test_cursor_is_stable_across_inserts_and_removals_unit() -> None:
    items = keys(150)
    first, cursor = paginate(items, key=str, kind="prompts", cursor=None)
    # One item before the cursor removed, one after it removed, one added on each side.
    changed = sorted(
        [item for item in items if item not in ("item-0010", "item-0120")]
        + ["item-0005a", "item-0130a"]
    )
    second, last = paginate(changed, key=str, kind="prompts", cursor=cursor)
    assert last is None
    assert second[0] == "item-0100"
    assert "item-0120" not in second and "item-0130a" in second
    assert not set(first) & set(second)


@pytest.mark.parametrize(
    "cursor",
    [
        "",
        "!!!",
        123,
        ["x"],
        "not-base64-json",
        base64.urlsafe_b64encode(b"[1, 2]").decode().rstrip("="),
        base64.urlsafe_b64encode(json.dumps({"k": "templates", "a": "x"}).encode()).decode(),
        base64.urlsafe_b64encode(json.dumps({"k": "prompts"}).encode()).decode(),
        base64.urlsafe_b64encode(json.dumps({"k": "prompts", "a": 5}).encode()).decode(),
        base64.urlsafe_b64encode(b"\xff\xfe").decode(),
        "a" * 9000,
        # Well formed, but longer than any cursor this server hands out.
        encode_cursor("prompts", "p" * 20000),
        pytest.param(NESTED, id="nested"),
    ],
)
def test_invalid_cursor_is_minus_32602_unit(cursor: Any) -> None:
    with pytest.raises(ProtocolError) as raised:
        paginate(keys(3), key=str, kind="prompts", cursor=cursor)
    assert raised.value.code == INVALID_PARAMS
    assert str(raised.value) == "Invalid cursor"


def test_cursor_of_the_longest_key_round_trips_unit() -> None:
    # Four UTF-8 bytes per character, the most any key can take.
    longest = "x://" + "\U0001f600" * (MAX_URI_LENGTH - 4)
    for kind in ("resources", "templates"):
        assert decode_cursor(kind, encode_cursor(kind, longest)) == longest


# ------------------------------------------------------------- through dispatch


def prompt_server(count: int, **kwargs: Any) -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)
    for index in range(count):
        server.register_prompt(lambda: "x", name=f"p{index:04d}")
    return server


async def walk(server: MCPServer, method: str, field: str, **ctx: Any) -> list[list[str]]:
    pages: list[list[str]] = []
    cursor: Any = None
    while True:
        params = {} if cursor is None else {"cursor": cursor}
        response = await server.dispatch(rpc(method, params), make_context(**ctx))
        assert response is not None and "result" in response, response
        result = response["result"]
        key = (
            "uriTemplate"
            if field == "resourceTemplates"
            else ("uri" if field == "resources" else "name")
        )
        pages.append([entry[key] for entry in result[field]])
        cursor = result.get("nextCursor")
        if cursor is None:
            return pages


async def test_short_lists_have_no_cursor() -> None:
    server = prompt_server(100)
    response = await server.dispatch(rpc("prompts/list"), make_context())
    assert response is not None
    assert "nextCursor" not in response["result"]
    assert len(response["result"]["prompts"]) == 100


async def test_cursor_walks_every_item_once() -> None:
    server = prompt_server(250)
    pages = await walk(server, "prompts/list", "prompts")
    assert [len(page) for page in pages] == [100, 100, 50]
    assert sum(pages, []) == [f"p{index:04d}" for index in range(250)]
    resources = MCPServer(port=0, rate_limit_per_minute=None)
    for index in range(150):
        resources.register_resource(lambda: "x", f"r://{index:04d}", name=f"r{index}")
        resources.register_resource(lambda v: v, f"t://{index:04d}/{{v}}", name=f"t{index}")
    assert [len(p) for p in await walk(resources, "resources/list", "resources")] == [100, 50]
    templates = await walk(resources, "resources/templates/list", "resourceTemplates")
    assert [len(page) for page in templates] == [100, 50]


async def test_the_longest_keys_can_end_a_page() -> None:
    # The longest URI registration allows, of four-byte characters, last on page 1.
    wide = "\U0001f600"
    server = MCPServer(port=0, rate_limit_per_minute=None)
    for index in range(PAGE_SIZE - 1):
        server.register_resource(lambda: "x", f"a://{index:04d}", name=f"r{index}")
        server.register_resource(lambda v: v, f"a://{index:04d}/{{v}}", name=f"t{index}")
    longest = "b://" + wide * (MAX_URI_LENGTH - 4)
    server.register_resource(lambda: "x", longest, name="long")
    longest_template = "b://" + wide * (MAX_URI_LENGTH - 8) + "/{v}"
    assert len(longest) == len(longest_template) == MAX_URI_LENGTH
    server.register_resource(lambda v: v, longest_template, name="long_template")
    server.register_resource(lambda: "x", "c://z", name="last")
    server.register_resource(lambda v: v, "c://{v}", name="last_template")
    resources = await walk(server, "resources/list", "resources")
    assert [len(page) for page in resources] == [PAGE_SIZE, 1]
    assert resources[0][-1] == longest and resources[1] == ["c://z"]
    templates = await walk(server, "resources/templates/list", "resourceTemplates")
    assert [len(page) for page in templates] == [PAGE_SIZE, 1]
    assert templates[0][-1] == longest_template and templates[1] == ["c://{v}"]


async def test_cursor_is_stable_across_inserts_and_removals() -> None:
    server = prompt_server(150)
    first = await server.dispatch(rpc("prompts/list"), make_context())
    assert first is not None
    cursor = first["result"]["nextCursor"]
    server.unregister_prompt("p0010")  # before the cursor
    server.unregister_prompt("p0120")  # after it
    server.register_prompt(lambda: "x", name="p0005a")
    server.register_prompt(lambda: "x", name="p0130a")
    second = await server.dispatch(rpc("prompts/list", {"cursor": cursor}), make_context())
    assert second is not None
    names = [entry["name"] for entry in second["result"]["prompts"]]
    assert names[0] == "p0100" and "p0120" not in names and "p0130a" in names
    assert "p0005a" not in names and "nextCursor" not in second["result"]


async def test_invalid_cursor_is_minus_32602() -> None:
    server = prompt_server(3)
    server.register_resource(lambda: "x", "r://a", name="r")
    wrong_kind = encode_cursor("resources", "r://a")
    for method, cursor in (
        ("prompts/list", "garbage!"),
        ("prompts/list", wrong_kind),
        ("prompts/list", 7),
        ("resources/templates/list", wrong_kind),
    ):
        for message in (rpc(method, {"cursor": cursor}), modern(method, {"cursor": cursor})):
            response = await server.dispatch(message, make_context())
            assert response is not None
            assert response["error"] == {"code": INVALID_PARAMS, "message": "Invalid cursor"}


async def test_deeply_nested_cursor_is_minus_32602(logs: LogCapture) -> None:
    assert len(NESTED) < 4 * MAX_URI_LENGTH  # well under the cap: the parser sees it
    server = prompt_server(3)
    server.register_resource(lambda: "x", "r://a", name="r")
    for method in ("prompts/list", "resources/list", "resources/templates/list"):
        for message in (rpc(method, {"cursor": NESTED}), modern(method, {"cursor": NESTED})):
            response = await server.dispatch(message, make_context())
            assert response is not None
            assert response["error"] == {"code": INVALID_PARAMS, "message": "Invalid cursor"}
    assert not [record for record in logs.records if record.levelno >= logging.ERROR]


async def test_pages_count_only_visible_items() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None, auth=APIKeyAuth({SEE_KEY: ["see"]}))
    for index in range(150):
        hidden = index % 2 == 0
        server.register_prompt(lambda: "x", name=f"p{index:04d}", scopes=("see",) if hidden else ())
    anonymous = await walk(server, "prompts/list", "prompts")
    assert [len(page) for page in anonymous] == [75]
    assert all(int(name[1:]) % 2 == 1 for name in anonymous[0])
    who = ClientIdentity(fingerprint="s" * 12, scopes=frozenset({"see"}))
    assert [len(page) for page in await walk(server, "prompts/list", "prompts", identity=who)] == [
        100,
        50,
    ]


async def test_every_page_has_the_same_cache_scope() -> None:
    for kwargs, scope in (({}, "public"), ({"auth": APIKeyAuth({SEE_KEY: "*"})}, "private")):
        server = prompt_server(250, **kwargs)
        cursor: Any = None
        scopes = []
        while True:
            params = {} if cursor is None else {"cursor": cursor}
            response = await server.dispatch(modern("prompts/list", params), make_context())
            assert response is not None
            scopes.append((response["result"]["cacheScope"], response["result"]["ttlMs"]))
            cursor = response["result"].get("nextCursor")
            if cursor is None:
                break
        assert scopes == [(scope, 0)] * 3


async def test_tools_list_is_not_paginated() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    for index in range(150):
        server.register_tool(lambda: "x", name=f"t{index:04d}", description="A tool.")
    response = await server.dispatch(rpc("tools/list", {"cursor": "ignored"}), make_context())
    assert response is not None
    assert len(response["result"]["tools"]) == 150
    assert "nextCursor" not in response["result"]
