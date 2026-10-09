"""Resources: registration, listing, reading, errors per era, security, execution."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Literal

import httpx
import pytest
from conftest import LogCapture, headers_for, make_context, modern, notification, rpc

from easy_mcp import (
    APIKeyAuth,
    ClientIdentity,
    MCPServer,
    RegistrationError,
    ResourceContent,
    ResourceNotFoundError,
    ToolError,
    ToolRegistrationError,
    current_cancel_token,
    current_identity,
    safe_path,
)
from easy_mcp.exceptions import (
    HEADER_MISMATCH,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    SERVER_BUSY,
    TOOL_TIMEOUT,
)

# Built, not written out, so secret scanners do not take the fixtures for credentials.
READER_KEY = "resources-reader-key-" + "r" * 12
OTHER_KEY = "resources-other-key-" + "o" * 13
LEGACY_NOT_FOUND = -32002


def make_server(**kwargs: Any) -> MCPServer:
    kwargs.setdefault("rate_limit_per_minute", None)
    return MCPServer(port=0, **kwargs)


async def call(server: MCPServer, message: dict[str, Any], **ctx: Any) -> dict[str, Any]:
    response = await server.dispatch(message, make_context(**ctx))
    assert response is not None
    return response


def read(uri: Any, msg_id: Any = 1) -> dict[str, Any]:
    return rpc("resources/read", {"uri": uri}, msg_id)


def modern_read(uri: Any, msg_id: Any = 1, **extra: Any) -> dict[str, Any]:
    return modern("resources/read", {"uri": uri, **extra}, msg_id)


def text_of(response: dict[str, Any]) -> str:
    return str(response["result"]["contents"][0]["text"])


# ----------------------------------------------------------------- registration


def test_decorator_returns_the_function_unchanged() -> None:
    server = make_server()

    def config() -> str:
        """Config."""
        return "x"

    assert server.resource("config://app")(config) is config
    assert config() == "x"
    (definition,) = server.resources
    assert definition.uri == "config://app" and definition.name == "config"
    assert server.resource_templates == []


def test_resource_needs_a_uri() -> None:
    server = make_server()
    with pytest.raises(RegistrationError, match="needs the URI first"):

        @server.resource  # type: ignore[arg-type]
        def bare() -> str:
            return "x"


def test_concrete_resource_takes_no_parameters() -> None:
    server = make_server()
    with pytest.raises(RegistrationError, match="takes no parameters"):

        @server.resource("config://app")
        def config(section: str) -> str:
            return section


def test_template_parameters_must_equal_its_variables() -> None:
    server = make_server()

    def missing() -> str:
        return ""

    def extra(user_id: int, other: str) -> str:
        return ""

    def star(*args: str) -> str:
        return ""

    def keyword(**kwargs: str) -> str:
        return ""

    for fn in (missing, extra):
        with pytest.raises(RegistrationError, match="template's variables"):
            server.register_resource(fn, "users://{user_id}")
    for fn in (star, keyword):
        with pytest.raises(RegistrationError, match=r"\*args"):
            server.register_resource(fn, "users://{user_id}")


def test_template_parameter_types_are_the_string_subset() -> None:
    server = make_server()

    def as_list(value: list[str]) -> str:
        return ""

    def as_dict(value: dict[str, str]) -> str:
        return ""

    def as_any(value: Any) -> str:
        return ""

    def optional_without_default(value: int | None) -> str:
        return ""

    def union(value: int | str) -> str:
        return ""

    for fn in (as_list, as_dict, as_any, optional_without_default, union):
        with pytest.raises(RegistrationError):
            server.register_resource(fn, "x://{value}")

    def unannotated(value):  # type: ignore[no-untyped-def]
        return ""

    plain = server.register_resource(unannotated, "x://{value}")
    assert plain.bind("x://abc") == {"value": "abc"}  # type: ignore[union-attr]

    def typed(
        number: int,
        mode: Literal["a", "b"],
        label: Annotated[str, "A label"],
        flag: bool,
        ratio: float,
    ) -> str:
        return ""

    template = server.register_resource(typed, "x://{number}/{mode}/{label}/{flag}/{ratio}")
    assert template.name == "typed"


def test_duplicate_uri_and_template_are_refused() -> None:
    server = make_server()
    server.register_resource(lambda: "a", "config://app", name="one")
    server.register_resource(lambda id: "a", "users://{id}", name="two")
    with pytest.raises(RegistrationError, match="already registered"):
        server.register_resource(lambda: "b", "config://app", name="three")
    with pytest.raises(RegistrationError, match="already registered"):
        server.register_resource(lambda id: "b", "users://{id}", name="four")
    removed = server.unregister_resource("users://{id}")
    assert removed.name == "two"
    with pytest.raises(RegistrationError, match="no resource"):
        server.unregister_resource("users://{id}")


def test_size_on_template_and_complete_on_concrete_are_refused() -> None:
    server = make_server()
    with pytest.raises(RegistrationError, match="no size"):
        server.register_resource(lambda id: "", "x://{id}", name="t", size=3)
    with pytest.raises(RegistrationError, match="needs a template"):
        server.register_resource(lambda: "", "x://a", name="c", complete={"id": ["1"]})
    with pytest.raises(RegistrationError, match="no variable"):
        server.register_resource(lambda id: "", "x://{id}", name="t", complete={"other": ["1"]})
    with pytest.raises(RegistrationError, match="bare string"):
        server.register_resource(lambda id: "", "x://{id}", name="t", complete={"id": "abc"})
    with pytest.raises(RegistrationError, match="size"):
        server.register_resource(lambda: "", "x://a", name="c", size=-1)
    definition = server.register_resource(lambda: "abc", "x://sized", name="s", size=3)
    assert definition.to_mcp()["size"] == 3


def test_annotations_and_mime_type_are_validated() -> None:
    server = make_server()
    bad: list[dict[str, Any]] = [
        {"mime_type": "plain"},
        {"mime_type": "text/"},
        {"annotations": {"audience": ["robot"]}},
        {"annotations": {"audience": []}},
        {"annotations": {"priority": 2}},
        {"annotations": {"priority": True}},
        {"annotations": {"lastModified": 5}},
        {"annotations": {"color": "red"}},
        {"timeout": 0},
        {"cache_ttl": -1},
        {"cache_ttl": float("inf")},
        {"name": ""},
        {"name": "n" * 129},
        {"title": ""},
    ]
    for index, options in enumerate(bad):
        with pytest.raises(RegistrationError):
            server.register_resource(lambda: "", f"x://bad{index}", **options)
    good = server.register_resource(
        lambda: "",
        "x://good",
        name="good",
        title="Good",
        mime_type="text/markdown; charset=utf-8",
        annotations={"audience": ["user"], "priority": 0.5, "lastModified": "2026-01-01"},
    )
    assert good.to_mcp() == {
        "uri": "x://good",
        "name": "good",
        "title": "Good",
        "mimeType": "text/markdown; charset=utf-8",
        "annotations": {"audience": ["user"], "priority": 0.5, "lastModified": "2026-01-01"},
    }


def test_invalid_uris_are_refused() -> None:
    server = make_server()
    for uri in ("no-scheme", "x://a b", "x://{#a}", "x://{a", "x://" + "a" * 2050, 7):
        with pytest.raises(RegistrationError):
            server.register_resource(lambda: "", uri, name="n")  # type: ignore[arg-type]


def test_tool_registration_error_is_a_registration_error() -> None:
    assert issubclass(ToolRegistrationError, RegistrationError)
    server = make_server()
    with pytest.raises(RegistrationError):
        server.register_tool(lambda: None, name="bad name")


# ----------------------------------------------------------------- capabilities


async def test_no_resource_capability_without_resources() -> None:
    server = make_server()
    init = await call(server, rpc("initialize", {"protocolVersion": "2025-11-25"}))
    assert init["result"]["capabilities"] == {"tools": {"listChanged": True}}
    discover = await call(server, modern("server/discover"))
    assert discover["result"]["capabilities"] == {"tools": {"listChanged": True}}
    for method in ("resources/list", "resources/templates/list", "resources/read"):
        response = await call(server, rpc(method, {"uri": "x://a"}))
        assert response["error"]["code"] == METHOD_NOT_FOUND, method
        stateless = await call(server, modern(method, {"uri": "x://a"}))
        assert stateless["error"]["code"] == METHOD_NOT_FOUND, method


async def test_resource_capability_with_a_resource_or_template() -> None:
    for uri, fn in (("config://app", lambda: "x"), ("users://{id}", lambda id: id)):
        server = make_server()
        server.register_resource(fn, uri, name="r")
        init = await call(server, rpc("initialize", {"protocolVersion": "2025-11-25"}))
        expected = {"subscribe": True, "listChanged": True}
        assert init["result"]["capabilities"]["resources"] == expected
        discover = await call(server, modern("server/discover"))
        assert discover["result"]["capabilities"]["resources"] == expected
        # Sticky: removing every resource keeps the capability, and lists are empty.
        server.unregister_resource(uri)
        again = await call(server, modern("server/discover"))
        assert again["result"]["capabilities"]["resources"] == expected
        listed = await call(server, rpc("resources/list"))
        assert listed["result"] == {"resources": []}


# ------------------------------------------------------------ listing and reading


def listing_server() -> MCPServer:
    server = make_server()

    @server.resource("config://app", mime_type="application/json")
    def app_config() -> dict[str, Any]:
        """The application's runtime configuration."""
        return {"region": "eu-west-1", "features": {"beta": True}}

    @server.resource("alpha://first", title="First")
    def first() -> str:
        return "first"

    @server.resource("users://{user_id}/avatar", mime_type="image/png")
    def avatar(user_id: int) -> bytes:
        """A user's avatar image."""
        return b"\x89PNG" + bytes([user_id % 256])

    @server.resource("docs://{+path}")
    def doc(path: str) -> str:
        """A document."""
        return f"doc {path}"

    return server


async def test_resources_list_is_sorted_and_shaped() -> None:
    server = listing_server()
    response = await call(server, rpc("resources/list"))
    assert response["result"] == {
        "resources": [
            {"uri": "alpha://first", "name": "first", "title": "First", "mimeType": "text/plain"},
            {
                "uri": "config://app",
                "name": "app_config",
                "description": "The application's runtime configuration.",
                "mimeType": "application/json",
            },
        ]
    }


async def test_templates_list_is_sorted_and_shaped() -> None:
    server = listing_server()
    response = await call(server, rpc("resources/templates/list"))
    assert response["result"] == {
        "resourceTemplates": [
            {
                "uriTemplate": "docs://{+path}",
                "name": "doc",
                "description": "A document.",
                "mimeType": "text/plain",
            },
            {
                "uriTemplate": "users://{user_id}/avatar",
                "name": "avatar",
                "description": "A user's avatar image.",
                "mimeType": "image/png",
            },
        ]
    }


class Model:
    """Looks like a Pydantic model to easy_mcp, which never imports Pydantic."""

    @classmethod
    def model_json_schema(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def model_validate(cls, value: Any) -> Any:
        return value

    def model_dump(self, **_: Any) -> dict[str, Any]:
        return {}


def test_mime_type_defaults_from_the_return_annotation() -> None:
    server = make_server()

    def as_str() -> str:
        return ""

    def as_bytes() -> bytes:
        return b""

    def as_dict() -> dict[str, int]:
        return {}

    def as_list() -> list[int]:
        return []

    def as_model() -> Model:
        return Model()

    def as_optional() -> str | None:
        return None

    def unannotated():  # type: ignore[no-untyped-def]
        return ""

    expected = {
        as_str: "text/plain",
        as_bytes: "application/octet-stream",
        as_dict: "application/json",
        as_list: "application/json",
        as_model: "application/json",
        as_optional: None,
        unannotated: None,
    }
    for fn, mime in expected.items():
        definition = server.register_resource(fn, f"x://{fn.__name__}")
        assert definition.mime_type == mime, fn.__name__
        assert ("mimeType" in definition.to_mcp()) is (mime is not None)


async def test_read_text() -> None:
    server = listing_server()
    response = await call(server, read("alpha://first"))
    assert response["result"] == {
        "contents": [{"uri": "alpha://first", "mimeType": "text/plain", "text": "first"}]
    }


async def test_read_json_is_deterministic() -> None:
    server = listing_server()
    response = await call(server, read("config://app"))
    (item,) = response["result"]["contents"]
    assert item["mimeType"] == "application/json"
    assert item["text"] == '{"features": {"beta": true}, "region": "eu-west-1"}'


async def test_read_bytes_is_base64_blob() -> None:
    server = listing_server()
    response = await call(server, read("users://42/avatar"))
    (item,) = response["result"]["contents"]
    assert item == {
        "uri": "users://42/avatar",
        "mimeType": "image/png",
        "blob": base64.b64encode(b"\x89PNG*").decode(),
    }


async def test_read_pydantic_model() -> None:
    pydantic = pytest.importorskip("pydantic")

    class Profile(pydantic.BaseModel):
        name: str
        age: int

    server = make_server()

    @server.resource("profile://me")
    def me() -> Profile:
        return Profile(name="Ada", age=36)

    response = await call(server, read("profile://me"))
    (item,) = response["result"]["contents"]
    assert item["mimeType"] == "application/json"
    assert json.loads(item["text"]) == {"name": "Ada", "age": 36}


async def test_read_multiple_contents() -> None:
    server = make_server()

    @server.resource("logs://today", mime_type="text/plain")
    def todays_logs() -> list[ResourceContent]:
        return [
            ResourceContent(text="one", uri="logs://today/a"),
            ResourceContent(blob=b"\x00\x01", mime_type="application/octet-stream"),
        ]

    response = await call(server, read("logs://today"))
    assert response["result"]["contents"] == [
        {"uri": "logs://today/a", "mimeType": "text/plain", "text": "one"},
        {"uri": "logs://today", "mimeType": "application/octet-stream", "blob": "AAE="},
    ]


async def test_empty_list_is_json_text() -> None:
    server = make_server()
    server.register_resource(lambda: [], "x://empty", name="empty")
    response = await call(server, read("x://empty"))
    assert response["result"]["contents"] == [
        {"uri": "x://empty", "mimeType": "application/json", "text": "[]"}
    ]


async def test_template_variables_are_converted() -> None:
    server = make_server()
    seen: list[Any] = []

    @server.resource("x://{count}/{ratio}/{flag}/{mode}")
    def typed(count: int, ratio: float, flag: bool, mode: Literal["a", "b"]) -> str:
        seen.append((count, ratio, flag, mode))
        return "ok"

    response = await call(server, read("x://-12/2.5/TRUE/b"))
    assert text_of(response) == "ok"
    assert seen == [(-12, 2.5, True, "b")]


async def test_conversion_failure_reads_as_not_found() -> None:
    server = make_server()
    calls: list[Any] = []

    @server.resource("users://{user_id}")
    def user(user_id: int) -> str:
        calls.append(user_id)
        return "x"

    for uri in ("users://abc", "users://1.5", "users://"):
        response = await call(server, read(uri))
        assert response["error"]["code"] == LEGACY_NOT_FOUND, uri
    assert calls == []


async def test_concrete_beats_template_and_specific_beats_general() -> None:
    server = make_server()
    server.register_resource(lambda: "concrete", "x://a/b", name="concrete")
    server.register_resource(lambda rest: "general", "x://{+rest}", name="general")
    server.register_resource(lambda last: "specific", "x://a/{last}", name="specific")
    assert text_of(await call(server, read("x://a/b"))) == "concrete"
    assert text_of(await call(server, read("x://a/c"))) == "specific"
    assert text_of(await call(server, read("x://z/c"))) == "general"


async def test_not_found_is_minus_32002_in_the_handshake_era() -> None:
    server = listing_server()
    response = await call(server, read("missing://x", 9))
    assert response == {
        "jsonrpc": "2.0",
        "id": 9,
        "error": {
            "code": LEGACY_NOT_FOUND,
            "message": "Resource not found",
            "data": {"uri": "missing://x"},
        },
    }


async def test_not_found_is_minus_32602_statelessly() -> None:
    server = make_server(auth=APIKeyAuth({READER_KEY: ["reader"]}))
    server.register_resource(lambda: "secret", "secret://x", name="secret", scopes=("reader",))

    def num_resource(n: int) -> str:
        return "n"

    server.register_resource(num_resource, "num://{n}")

    @server.resource("raise://x")
    def raising() -> str:
        raise ResourceNotFoundError()

    @server.resource("none://x")
    def nothing() -> None:
        return None

    for uri in ("missing://x", "secret://x", "num://abc", "raise://x", "none://x"):
        response = await call(server, modern_read(uri))
        assert response["error"]["code"] == INVALID_PARAMS, uri
        assert response["error"]["message"] == "Resource not found"
        assert response["error"]["data"] == {"uri": uri}
        assert "-32002" not in json.dumps(response)


async def test_none_and_resource_not_found_error_mean_not_found() -> None:
    server = make_server()
    server.register_resource(lambda: None, "none://x", name="none")

    @server.resource("elsewhere://x")
    def elsewhere() -> str:
        raise ResourceNotFoundError("elsewhere://child", "Child resource is gone")

    none = await call(server, read("none://x"))
    assert none["error"] == {
        "code": LEGACY_NOT_FOUND,
        "message": "Resource not found",
        "data": {"uri": "none://x"},
    }
    child = await call(server, read("elsewhere://x"))
    assert child["error"] == {
        "code": LEGACY_NOT_FOUND,
        "message": "Child resource is gone",
        "data": {"uri": "elsewhere://child"},
    }


async def test_long_uris_are_not_echoed() -> None:
    server = make_server()
    server.register_resource(lambda path: None, "docs://{+path}", name="docs")
    long_uri = "docs://" + "a/" * 2000
    response = await call(server, read(long_uri))
    assert response["error"]["code"] == LEGACY_NOT_FOUND
    assert "data" not in response["error"]


async def test_uri_must_be_a_string() -> None:
    server = listing_server()
    for params in ({}, {"uri": 5}, {"uri": None}):
        response = await call(server, rpc("resources/read", params))
        assert response["error"] == {
            "code": INVALID_PARAMS,
            "message": "resources/read requires a string 'uri'",
        }


async def test_tool_error_text_is_shown() -> None:
    server = make_server()

    @server.resource("x://fail")
    def fail() -> str:
        raise ToolError("The archive is offline.")

    response = await call(server, read("x://fail"))
    assert response["error"] == {"code": INTERNAL_ERROR, "message": "The archive is offline."}
    stateless = await call(server, modern_read("x://fail"))
    assert stateless["error"] == {"code": INTERNAL_ERROR, "message": "The archive is offline."}


async def test_exceptions_are_sanitized(logs: LogCapture) -> None:
    server = make_server()

    @server.resource("x://boom")
    def boom() -> str:
        raise RuntimeError("database password is hunter2")

    response = await call(server, read("x://boom"))
    message = response["error"]["message"]
    assert response["error"]["code"] == INTERNAL_ERROR
    assert re.fullmatch(r"Internal server error \(error_id=\w+\)", message)
    assert "hunter2" not in json.dumps(response)
    error_id = message.split("=")[1].rstrip(")")
    assert f"resource 'x://boom' failed error_id={error_id}" in logs.text


async def test_debug_mode_adds_detail() -> None:
    server = make_server(debug=True)

    @server.resource("x://boom")
    def boom() -> str:
        raise RuntimeError("detail for the developer")

    response = await call(server, read("x://boom"))
    assert "RuntimeError: detail for the developer" in response["error"]["message"]


async def test_unsupported_return_is_an_internal_error(logs: LogCapture) -> None:
    server = make_server()

    class Opaque:
        def __repr__(self) -> str:
            return "Opaque(secret-value)"

    server.register_resource(lambda: Opaque(), "x://opaque", name="opaque")
    server.register_resource(lambda: [ResourceContent(text="a"), "b"], "x://mixed", name="mixed")
    for uri in ("x://opaque", "x://mixed"):
        response = await call(server, read(uri))
        assert response["error"]["code"] == INTERNAL_ERROR
        assert "error_id=" in response["error"]["message"]
    assert "Opaque" in logs.text and "secret-value" not in logs.text


# -------------------------------------------------------------------- security


def protected_server() -> MCPServer:
    server = make_server(auth=APIKeyAuth({READER_KEY: ["reader"], OTHER_KEY: ["other"]}))

    @server.resource("secret://plans", scopes=("reader",))
    def plans() -> str:
        return "the plans"

    @server.resource("public://notes")
    def notes() -> str:
        return "notes"

    return server


def identity(*scopes: str) -> ClientIdentity:
    return ClientIdentity(fingerprint="f" * 12, scopes=frozenset(scopes))


async def test_protected_resource_is_hidden_and_reads_as_missing() -> None:
    server = protected_server()
    for who in (None, identity("other")):
        listed = await call(server, rpc("resources/list"), identity=who)
        assert [r["uri"] for r in listed["result"]["resources"]] == ["public://notes"]
        for make in (read, modern_read):
            protected = await call(server, make("secret://plans"), identity=who)
            missing = await call(server, make("secret://nothing"), identity=who)
            assert protected["error"]["code"] == missing["error"]["code"]
            assert protected["error"]["message"] == missing["error"]["message"]
            assert protected["error"]["data"] == {"uri": "secret://plans"}
            assert missing["error"]["data"] == {"uri": "secret://nothing"}


async def test_scoped_key_sees_and_reads_it() -> None:
    server = protected_server()
    reader = identity("reader")
    listed = await call(server, rpc("resources/list"), identity=reader)
    assert [r["uri"] for r in listed["result"]["resources"]] == ["public://notes", "secret://plans"]
    assert text_of(await call(server, read("secret://plans"), identity=reader)) == "the plans"
    star = identity("*")
    assert text_of(await call(server, read("secret://plans"), identity=star)) == "the plans"


async def test_hidden_concrete_falls_through_to_a_visible_template(logs: LogCapture) -> None:
    server = make_server(auth=APIKeyAuth({READER_KEY: ["reader"]}))
    server.register_resource(lambda: "concrete", "x://a/b", name="concrete", scopes=("reader",))
    server.register_resource(lambda last: f"template {last}", "x://a/{last}", name="template")
    assert text_of(await call(server, read("x://a/b"))) == "template b"
    assert text_of(await call(server, read("x://a/b"), identity=identity("reader"))) == "concrete"
    missing = await call(server, read("x://z/b"))
    assert missing["error"]["code"] == LEGACY_NOT_FOUND
    hidden = [e for e in logs.events("resource_read") if e.get("hidden")]
    assert hidden == []  # a visible item answered every read that matched one


async def test_hidden_matches_are_audited_but_never_sent(logs: LogCapture) -> None:
    server = protected_server()
    response = await call(server, read("secret://plans"))
    assert "hidden" not in json.dumps(response)
    (event,) = logs.events("resource_read")
    assert event["status"] == "not_found" and event["hidden"] is True


def test_safe_path_allows_inside_and_refuses_escapes(tmp_path: Path) -> None:
    root = tmp_path / "guides"
    (root / "setup").mkdir(parents=True)
    (root / "setup" / "install.md").write_text("install", "utf-8")
    (tmp_path / "secret.txt").write_text("secret", "utf-8")
    assert safe_path(root, "setup/install.md") == (root / "setup" / "install.md").resolve()
    assert safe_path(str(root), "setup/../setup/install.md").read_text("utf-8") == "install"
    escapes = [
        "../secret.txt",
        "setup/../../secret.txt",
        "/etc/passwd",
        "C:/Windows/win.ini",
        "C:secret",
        "\\\\host\\share\\x",
        "setup\\..\\..\\secret.txt",
        "a\x00b",
        "",
    ]
    for untrusted in escapes:
        with pytest.raises(ResourceNotFoundError):
            safe_path(root, untrusted)
    link = root / "link"
    try:
        os.symlink(tmp_path / "secret.txt", link)
    except (OSError, NotImplementedError):
        return  # no symlinks here (Windows without the privilege)
    with pytest.raises(ResourceNotFoundError):
        safe_path(root, "link")


async def test_safe_path_in_a_resource_reads_as_not_found(tmp_path: Path) -> None:
    (tmp_path / "doc.md").write_text("hello", "utf-8")
    server = make_server()

    @server.resource("docs://{+path}")
    def doc(path: str) -> str | None:
        file = safe_path(tmp_path, path)
        return file.read_text("utf-8") if file.is_file() else None

    assert text_of(await call(server, read("docs://doc.md"))) == "hello"
    response = await call(server, read("docs://nested%5C..%5Cdoc.md"))
    assert response["error"]["code"] == LEGACY_NOT_FOUND


# ----------------------------------------------------------------- stateless era


async def test_lists_carry_ttl_zero_and_scope_by_auth() -> None:
    open_server = listing_server()
    for method in ("resources/list", "resources/templates/list"):
        response = await call(open_server, modern(method))
        result = response["result"]
        assert result["ttlMs"] == 0 and result["cacheScope"] == "public"
        assert result["resultType"] == "complete"
        assert "io.modelcontextprotocol/serverInfo" in result["_meta"]
    keyed = protected_server()
    for method in ("resources/list", "resources/templates/list"):
        response = await call(keyed, modern(method))
        assert response["result"]["cacheScope"] == "private"


async def test_read_carries_cache_ttl_and_scope_by_protection() -> None:
    server = make_server(auth=APIKeyAuth({READER_KEY: ["reader"]}))
    server.register_resource(lambda: "a", "x://cached", name="cached", cache_ttl=2.5)
    server.register_resource(lambda: "b", "x://plain", name="plain")
    server.register_resource(lambda: "c", "x://secret", name="secret", scopes=("reader",))
    cached = await call(server, modern_read("x://cached"))
    assert cached["result"]["ttlMs"] == 2500 and cached["result"]["cacheScope"] == "public"
    plain = await call(server, modern_read("x://plain"))
    assert plain["result"]["ttlMs"] == 0 and plain["result"]["cacheScope"] == "public"
    secret = await call(server, modern_read("x://secret"), identity=identity("reader"))
    assert secret["result"]["cacheScope"] == "private"
    assert secret["result"]["resultType"] == "complete"
    # Legacy results carry no cache hints.
    legacy = await call(server, read("x://cached"))
    assert set(legacy["result"]) == {"contents"}


async def test_read_is_private_with_request_middleware() -> None:
    server = listing_server()

    @server.middleware
    async def passthrough(request: Any, call_next: Any) -> Any:
        return await call_next()

    response = await call(server, modern_read("alpha://first"))
    assert response["result"]["cacheScope"] == "private"
    listed = await call(server, modern("resources/list"))
    assert listed["result"]["cacheScope"] == "private"


async def test_mrtr_retry_fields_make_a_read_uncacheable() -> None:
    server = make_server()
    server.register_resource(lambda: "a", "x://cached", name="cached", cache_ttl=60)
    for field in ("inputResponses", "requestState"):
        response = await call(server, modern_read("x://cached", **{field: {}}))
        assert response["result"]["ttlMs"] == 0
        assert response["result"]["cacheScope"] == "private"
        assert text_of(response) == "a"


async def test_http_read_requires_matching_mcp_name(live_server: Callable[[Any], str]) -> None:
    server = listing_server()
    base = live_server(server)
    uri = "users://7/avatar"
    message = modern_read(uri)
    async with httpx.AsyncClient(timeout=10) as client:
        for name in (uri, "=?base64?" + base64.b64encode(uri.encode()).decode() + "?="):
            ok = await client.post(
                f"{base}/mcp", json=message, headers=headers_for(message, **{"Mcp-Name": name})
            )
            assert ok.status_code == 200, ok.text
            assert ok.json()["result"]["contents"][0]["uri"] == uri
        for headers in (headers_for(message), headers_for(message, **{"Mcp-Name": "x://other"})):
            refused = await client.post(f"{base}/mcp", json=message, headers=headers)
            assert refused.status_code == 400
            assert refused.json()["error"]["code"] == HEADER_MISMATCH


async def test_http_unknown_capability_is_404(live_server: Callable[[Any], str]) -> None:
    server = make_server()

    @server.tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    base = live_server(server)
    message = modern("resources/list")
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(f"{base}/mcp", json=message, headers=headers_for(message))
    assert response.status_code == 404
    assert response.json()["error"]["code"] == METHOD_NOT_FOUND


# -------------------------------------------------------------------- execution


async def test_read_timeout_is_minus_32005() -> None:
    server = make_server()

    @server.resource("x://slow", timeout=0.2)
    async def slow() -> str:
        await asyncio.sleep(30)
        return "late"

    response = await call(server, read("x://slow"))
    assert response["error"] == {
        "code": TOOL_TIMEOUT,
        "message": "Resource 'x://slow' timed out after 0.2s",
    }


async def test_sync_reads_share_the_worker_cap() -> None:
    server = make_server(max_sync_workers=1)
    release = threading.Event()
    entered = threading.Event()

    @server.resource("x://busy")
    def busy() -> str:
        entered.set()
        release.wait(10)
        return "done"

    @server.tool
    def sync_tool() -> str:
        """A sync tool."""
        return "tool"

    first = asyncio.ensure_future(server.dispatch(read("x://busy", 1), make_context()))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        second = await call(server, read("x://busy", 2))
        assert second["error"]["code"] == SERVER_BUSY
        tool = await call(server, rpc("tools/call", {"name": "sync_tool"}, 3))
        assert tool["error"]["code"] == SERVER_BUSY
    finally:
        release.set()
    done = await asyncio.wait_for(first, 10)
    assert done is not None and text_of(done) == "done"


async def test_notifications_cancelled_cancels_a_read(logs: LogCapture) -> None:
    server = make_server()
    entered = threading.Event()
    fired = threading.Event()

    @server.resource("x://wait")
    def wait() -> str:
        token = current_cancel_token()
        assert token is not None
        entered.set()
        if token.wait(10):
            fired.set()
        return "late"

    context = make_context()
    pending = asyncio.ensure_future(server.dispatch(read("x://wait", "r1"), context))
    assert await asyncio.to_thread(entered.wait, 10)
    cancel = notification("notifications/cancelled", {"requestId": "r1"})
    assert await server.dispatch(cancel, context) is None
    assert await asyncio.wait_for(pending, 10) is None
    assert await asyncio.to_thread(fired.wait, 10)
    await server.wait_for_tool_threads(5)
    (event,) = logs.events("request_cancelled")
    assert event["method"] == "resources/read" and event["request_id"] == "r1"
    deadline = time.monotonic() + 5
    while not logs.events("resource_finished_after_cancel") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    (late,) = logs.events("resource_finished_after_cancel")
    assert late["uri"] == "x://wait" and late["reason"] == "cancelled"


async def test_http_disconnect_cancels_a_read(live_server: Callable[[Any], str]) -> None:
    server = make_server()
    entered = threading.Event()
    fired = threading.Event()

    @server.resource("x://wait")
    def wait() -> str:
        token = current_cancel_token()
        assert token is not None
        entered.set()
        if token.wait(20):
            fired.set()
        return "late"

    base = live_server(server)
    message = modern_read("x://wait")
    headers = headers_for(message, **{"Mcp-Name": "x://wait"})

    async def post() -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(f"{base}/mcp", json=message, headers=headers)

    request = asyncio.ensure_future(post())
    assert await asyncio.to_thread(entered.wait, 10)
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert await asyncio.to_thread(fired.wait, 15)


async def test_resources_see_the_caller_and_token() -> None:
    server = make_server(auth=APIKeyAuth({READER_KEY: ["reader"]}))
    seen: list[Any] = []

    @server.resource("x://who")
    def who() -> str:
        caller = current_identity()
        seen.append((caller.scopes if caller else None, current_cancel_token() is not None))
        return "ok"

    @server.resource("x://who-async")
    async def who_async() -> str:
        caller = current_identity()
        seen.append((caller.scopes if caller else None, current_cancel_token() is not None))
        return "ok"

    await call(server, read("x://who"), identity=identity("reader"))
    await call(server, read("x://who-async"))
    assert seen == [(frozenset({"reader"}), True), (None, True)]


async def test_reads_are_audited_without_content(logs: LogCapture) -> None:
    server = listing_server()
    await call(server, read("alpha://first"))
    await call(server, read("users://3/avatar"))
    await call(server, read("missing://x"))
    events = logs.events("resource_read")
    assert [(e["uri"], e["status"]) for e in events] == [
        ("alpha://first", "ok"),
        ("users://3/avatar", "ok"),
        ("missing://x", "not_found"),
    ]
    assert events[1]["template"] == "users://{user_id}/avatar"
    assert "template" not in events[0]
    assert all(isinstance(e["duration_ms"], float) for e in events)
    assert "first" not in json.dumps([{k: v for k, v in e.items() if k != "uri"} for e in events])


async def test_long_uris_are_cut_in_the_audit(logs: LogCapture) -> None:
    server = make_server()
    server.register_resource(lambda path: "x", "docs://{+path}", name="docs")
    await call(server, read("docs://" + "a" * 5000))
    (event,) = logs.events("resource_read")
    assert len(event["uri"]) == 512


async def test_request_middleware_sees_reads() -> None:
    server = listing_server()
    seen: list[tuple[str, Any]] = []

    @server.middleware
    async def watch(request: Any, call_next: Any) -> Any:
        outcome = await call_next()
        seen.append((request.method, outcome.error_code))
        return outcome

    await call(server, read("alpha://first"))
    await call(server, read("missing://x"))
    await call(server, rpc("resources/list"))
    assert seen == [
        ("resources/read", None),
        ("resources/read", LEGACY_NOT_FOUND),
        ("resources/list", None),
    ]


async def test_tool_error_from_request_middleware_on_a_read() -> None:
    server = listing_server()

    @server.middleware
    async def refuse(request: Any, call_next: Any) -> Any:
        if request.method == "resources/read":
            raise ToolError("Reads are paused.")
        return await call_next()

    response = await call(server, read("alpha://first"))
    assert response["error"] == {"code": INTERNAL_ERROR, "message": "Reads are paused."}


# ------------------------------------------------------------ operator warnings


def test_registering_while_serving_with_a_shared_store_warns(logs: LogCapture) -> None:
    from shared_store_fake import FakeHub

    server = MCPServer(port=0, rate_limit_per_minute=None, store=FakeHub().store())
    server._serving = True
    server.register_resource(lambda: "x", "x://a", name="a")
    server.register_prompt(lambda: "x", name="p")
    server.unregister_resource("x://a")
    assert "resource 'x://a' registered while serving with a shared store" in logs.text
    assert "prompt 'p' registered while serving with a shared store" in logs.text
    assert "resource 'x://a' unregistered while serving with a shared store" in logs.text
    assert "every worker must register the same resources" in logs.text


def test_a_capability_added_after_serving_began_is_logged(logs: LogCapture) -> None:
    server = make_server()
    server.register_resource(lambda: "x", "x://early", name="early")
    assert "after serving began" not in logs.text
    server._started = True
    server.register_prompt(lambda: "x", name="late")
    server.register_resource(lambda: "x", "x://late", name="late")  # advertised already
    assert logs.text.count("after serving began") == 1
    assert "capability 'prompts' added after serving began" in logs.text


def test_protected_items_without_auth_are_warned_about(logs: LogCapture) -> None:
    server = make_server()
    server.register_resource(lambda: "x", "x://secret", name="secret", requires_auth=True)
    server.register_resource(lambda id: id, "x://t/{id}", name="t", scopes=("s",))
    server.register_prompt(lambda: "x", name="internal", scopes=("s",))
    server._warn_if_misconfigured()
    for named in ("resource x://secret", "template x://t/{id}", "prompt internal"):
        assert named in logs.text
    assert "they will be unreachable" in logs.text
