"""Icons on the server, tools, resources and prompts."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

import pytest
from conftest import make_context, modern, rpc

from easy_mcp import Icon, MCPServer, RegistrationError, ToolRegistrationError
from easy_mcp.icons import MAX_ICON_BYTES, MAX_ICONS, MAX_SRC_LENGTH, normalize_icons

PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(24))
PNG_URI = "data:image/png;base64," + base64.b64encode(PNG).decode()
URL = "https://example.com/icons/search.svg"
INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "tests", "version": "1.0"},
}


# ---------------------------------------------------------------- the Icon


def test_an_icon_serializes_only_what_it_was_given() -> None:
    assert Icon(URL).to_mcp() == {"src": URL}
    full = Icon(URL, mime_type="image/svg+xml", sizes=("any",), theme="dark")
    assert full.to_mcp() == {
        "src": URL,
        "mimeType": "image/svg+xml",
        "sizes": ["any"],
        "theme": "dark",
    }
    assert Icon(PNG_URI, sizes=["48x48", "96x96"]).sizes == ("48x48", "96x96")
    assert Icon(URL, sizes="48x48").sizes == ("48x48",)  # one size, not its letters


@pytest.mark.parametrize(
    "src",
    [
        "https://example.com/a.png",
        "HTTPS://EXAMPLE.COM/a.png",
        "https://example.com:8443/a.png?v=2#x",
        "https://example.com",
        "https://[2001:db8::1]:8443/a.png",
        "https://xn--exmple-cua.com/%E2%9C%93.png",
        PNG_URI,
        "data:image/svg+xml;base64,PHN2Zy8+",
    ],
)
def test_https_and_data_sources_are_accepted(src: str) -> None:
    assert Icon(src).src == src


@pytest.mark.parametrize(
    "src",
    [
        "",
        "http://example.com/a.png",
        "javascript:alert(1)",
        "file:///etc/passwd",
        "ftp://example.com/a.png",
        "ws://example.com/a",
        "/static/logo.png",
        "logo.png",
        "https://",
        "https:///a.png",
        "https://user@example.com/a.png",
        "https://example.com/a b.png",
        "https://example.com/a.png\nX-Injected: 1",
        "https://example.com/\ud800",
        "https://exa\x00mple.com/a.png",
        "https://example.com/\x7f\x1b[31m",
        "https://example.com/\u202egnp.exe",
        "https://example.com/\u200b",
        "https://ex\u00e4mple.com/a.png",
        "https://:443",
        "https://.",
        "https://[",
        "https://example.com:999999/a.png",
        "https://example.com/" + "a" * 2048,
        "data:image/png;base64,A",
        "data:image/png;base64,AAAA=",
        "data:text/html;base64,AAAA",
        "data:image/png,rawbytes",
        "data:image/png;base64,",
        "data:image/png;base64,not base64!",
        "data:;base64,AAAA",
        "data:text/html;charset=utf-8;base64,AAAA",
        7,
        None,
    ],
)
def test_other_sources_are_refused(src: Any) -> None:
    with pytest.raises(ValueError):
        Icon(src)


@pytest.mark.parametrize(
    "options",
    [
        {"mime_type": "png"},
        {"mime_type": "image/"},
        {"mime_type": 7},
        {"sizes": ("48",)},
        {"sizes": ("48x",)},
        {"sizes": ("0x48",)},
        {"sizes": ("48X48",)},
        {"sizes": (48,)},
        {"theme": "blue"},
    ],
)
def test_malformed_fields_are_refused(options: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        Icon(URL, **options)


def test_a_source_too_long_is_refused() -> None:
    assert Icon("https://example.com/" + "a" * 500).src
    with pytest.raises(ValueError, match="serve a larger image by URL"):
        Icon("data:image/png;base64," + "A" * MAX_SRC_LENGTH)
    with pytest.raises(ValueError, match="at most 2048 characters"):
        Icon("https://example.com/" + "a" * 2048)


def test_the_icons_of_one_item_share_the_size_cap(tmp_path: Path) -> None:
    (tmp_path / "half.png").write_bytes(b"x" * (MAX_ICON_BYTES // 2))
    half = Icon.from_file(tmp_path / "half.png")
    assert len(normalize_icons([half, URL], "x")) == 2
    with pytest.raises(RegistrationError, match="add up to more than 64 KiB"):
        normalize_icons([half, half, half], "x")


def test_sizes_may_be_left_out_as_none() -> None:
    assert Icon(URL, sizes=None).sizes == ()  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="list of strings"):
        Icon(URL, sizes=7)  # type: ignore[arg-type]


def test_from_file_reads_the_image_once(tmp_path: Path) -> None:
    logo = tmp_path / "Logo.PNG"
    logo.write_bytes(PNG)
    icon = Icon.from_file(logo, sizes=("32x32",), theme="light")
    assert icon.src == PNG_URI
    assert icon.to_mcp() == {
        "src": PNG_URI,
        "mimeType": "image/png",
        "sizes": ["32x32"],
        "theme": "light",
    }
    logo.unlink()  # the icon holds the bytes
    assert icon.src == PNG_URI
    for name, kind in (("a.jpg", "image/jpeg"), ("a.jpeg", "image/jpeg"), ("a.webp", "image/webp")):
        (tmp_path / name).write_bytes(b"x")
        assert Icon.from_file(str(tmp_path / name)).mime_type == kind
    (tmp_path / "a.svg").write_text("<svg/>", encoding="utf-8")
    assert Icon.from_file(tmp_path / "a.svg").src == "data:image/svg+xml;base64,PHN2Zy8+"


def test_from_file_refuses_what_it_cannot_serve(tmp_path: Path) -> None:
    (tmp_path / "logo.bmp").write_bytes(b"BM")
    with pytest.raises(ValueError, match="pass mime_type="):
        Icon.from_file(tmp_path / "logo.bmp")
    assert Icon.from_file(tmp_path / "logo.bmp", mime_type="image/BMP").mime_type == "image/bmp"
    with pytest.raises(ValueError, match="is not one"):
        Icon.from_file(tmp_path / "logo.bmp", mime_type="text/html")
    (tmp_path / "empty.png").write_bytes(b"")
    with pytest.raises(ValueError, match="empty"):
        Icon.from_file(tmp_path / "empty.png")
    (tmp_path / "exact.png").write_bytes(b"x" * MAX_ICON_BYTES)
    assert Icon.from_file(tmp_path / "exact.png").src.startswith("data:image/png;base64,")
    (tmp_path / "big.png").write_bytes(b"x" * (MAX_ICON_BYTES + 1))
    with pytest.raises(ValueError, match="serve it by URL"):
        Icon.from_file(tmp_path / "big.png")
    with pytest.raises(OSError):
        Icon.from_file(tmp_path / "missing.png")


def test_normalize_icons() -> None:
    assert normalize_icons(None, "x") == ()
    assert normalize_icons([], "x") == ()
    assert normalize_icons(URL, "x") == (Icon(URL),)
    assert normalize_icons(Icon(URL), "x") == (Icon(URL),)
    assert normalize_icons((item for item in [URL, Icon(PNG_URI)]), "x") == (
        Icon(URL),
        Icon(PNG_URI),
    )
    with pytest.raises(RegistrationError, match="x: an icon must be an Icon or a string"):
        normalize_icons([7], "x")  # type: ignore[list-item]
    with pytest.raises(RegistrationError, match="x: an icon's src must be an https"):
        normalize_icons(["http://example.com/a.png"], "x")
    with pytest.raises(RegistrationError, match=f"at most {MAX_ICONS} icons"):
        normalize_icons([URL] * (MAX_ICONS + 1), "x")
    with pytest.raises(RegistrationError, match="x: "):
        normalize_icons(7, "x")  # type: ignore[arg-type]


# ------------------------------------------------------------ registration


def icon_server() -> MCPServer:
    server = MCPServer(
        name="icons", port=0, rate_limit_per_minute=None, icons=[Icon(PNG_URI, sizes=("32x32",))]
    )

    @server.tool(icons=[URL, Icon(PNG_URI, theme="dark")])
    def search(query: str) -> str:
        """Search."""
        return query

    @server.tool
    def plain() -> str:
        """No icon."""
        return "plain"

    @server.resource("config://app", icons=URL)
    def config() -> str:
        """The configuration."""
        return "{}"

    @server.resource("users://{user_id}", icons=Icon(URL, sizes=("any",)))
    def user(user_id: int) -> str:
        """A user."""
        return str(user_id)

    @server.prompt(icons=(URL,))
    def brief() -> str:
        """Be brief."""
        return "Be brief."

    return server


async def listed(server: MCPServer, method: str, field: str, stateless: bool) -> list[Any]:
    message = modern(method) if stateless else rpc(method)
    response = await server.dispatch(message, make_context())
    assert response is not None and "result" in response, response
    return list(response["result"][field])


@pytest.mark.parametrize("stateless", [False, True])
async def test_lists_carry_the_icons(stateless: bool) -> None:
    server = icon_server()
    tools = {t["name"]: t for t in await listed(server, "tools/list", "tools", stateless)}
    assert tools["search"]["icons"] == [{"src": URL}, {"src": PNG_URI, "theme": "dark"}]
    assert "icons" not in tools["plain"]
    resources = await listed(server, "resources/list", "resources", stateless)
    assert resources[0]["icons"] == [{"src": URL}]
    templates = await listed(server, "resources/templates/list", "resourceTemplates", stateless)
    assert templates[0]["icons"] == [{"src": URL, "sizes": ["any"]}]
    prompts = await listed(server, "prompts/list", "prompts", stateless)
    assert prompts[0]["icons"] == [{"src": URL}]


async def test_the_server_names_its_icons_in_both_eras() -> None:
    server = icon_server()
    expected = [{"src": PNG_URI, "sizes": ["32x32"]}]
    init = await server.dispatch(rpc("initialize", INIT), make_context())
    assert init is not None
    assert init["result"]["serverInfo"]["icons"] == expected
    assert init["result"]["serverInfo"]["name"] == "icons"
    discover = await server.dispatch(modern("server/discover"), make_context())
    assert discover is not None
    stamped = discover["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]
    assert stamped["icons"] == expected


async def test_a_server_without_icons_says_nothing_of_them() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    init = await server.dispatch(rpc("initialize", INIT), make_context())
    assert init is not None
    assert "icons" not in init["result"]["serverInfo"]


def test_definitions_hold_their_icons() -> None:
    server = icon_server()
    assert [tool.icons for tool in server.tools] == [(), (Icon(URL), Icon(PNG_URI, theme="dark"))]
    assert server.resources[0].icons == (Icon(URL),)
    assert server.resource_templates[0].icons == (Icon(URL, sizes=("any",)),)
    assert server.prompts[0].icons == (Icon(URL),)
    assert server.icons == (Icon(PNG_URI, sizes=("32x32",)),)


def test_a_bad_icon_is_a_registration_error() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    with pytest.raises(ToolRegistrationError, match="cannot register tool 'f'.*https"):
        server.register_tool(lambda: "x", name="f", description="F.", icons=["http://x.example/a"])
    with pytest.raises(RegistrationError, match="cannot register resource 'r://a'"):
        server.register_resource(lambda: "x", "r://a", name="r", icons=["logo.png"])
    with pytest.raises(RegistrationError, match="cannot register prompt 'p'"):
        server.register_prompt(lambda: "x", name="p", icons=[object()])
    with pytest.raises(ValueError, match="icons"):
        MCPServer(icons=["javascript:alert(1)"])
    assert server.tools == [] and server.resources == [] and server.prompts == []


async def test_registering_a_tool_with_another_icon_changes_the_list() -> None:
    # The digest clients are told about changes by covers the icons.
    server = MCPServer(port=0, rate_limit_per_minute=None)
    server.register_tool(lambda: "x", name="t", description="T.", icons=[URL])
    before = server._list_digest("tools", None)
    server.unregister_tool("t")
    server.register_tool(lambda: "x", name="t", description="T.", icons=[PNG_URI])
    assert server._list_digest("tools", None) != before
