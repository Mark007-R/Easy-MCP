"""Tool registration via the decorator and dynamic registry APIs."""

from __future__ import annotations

import pytest

from easy_mcp import MCPServer, ToolRegistrationError


def test_bare_decorator(server: MCPServer) -> None:
    @server.tool
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    assert add(2, 3) == 5  # function stays directly callable
    (definition,) = server.tools
    assert definition.name == "add"
    assert definition.description == "Add two numbers."
    assert definition.is_async is False
    assert definition.input_schema["required"] == ["a", "b"]


def test_decorator_with_options(server: MCPServer) -> None:
    @server.tool(
        name="sum_two",
        description="Custom description.",
        tags=("math",),
        category="arithmetic",
        examples=({"arguments": {"a": 1, "b": 2}},),
        timeout=5.0,
        max_calls_per_session=10,
    )
    def add(a: int, b: int) -> int:
        """Docstring ignored when description= is given."""
        return a + b

    (definition,) = server.tools
    assert definition.name == "sum_two"
    assert definition.description == "Custom description."
    assert definition.tags == ("math",)
    assert definition.category == "arithmetic"
    assert definition.timeout == 5.0
    assert definition.max_calls_per_session == 10
    mcp_entry = definition.to_mcp()
    assert mcp_entry["_meta"]["easy_mcp"]["tags"] == ["math"]
    assert mcp_entry["_meta"]["easy_mcp"]["examples"] == [{"arguments": {"a": 1, "b": 2}}]


def test_async_tools_flagged(server: MCPServer) -> None:
    @server.tool
    async def fetch(url: str) -> str:
        """Fetch something."""
        return url

    assert server.tools[0].is_async is True


def test_duplicate_names_rejected(server: MCPServer) -> None:
    @server.tool
    def ping_tool() -> str:
        """Ping."""
        return "pong"

    with pytest.raises(ToolRegistrationError, match="already registered"):

        @server.tool(name="ping_tool")
        def other() -> str:
            """Other."""
            return "x"


def test_invalid_tool_name_rejected(server: MCPServer) -> None:
    with pytest.raises(ToolRegistrationError, match="invalid tool name"):

        @server.tool(name="bad name!")
        def fn() -> str:
            """X."""
            return "x"


def test_unannotated_params_rejected(server: MCPServer) -> None:
    with pytest.raises(ToolRegistrationError, match="type annotation"):

        @server.tool
        def fn(a):  # type: ignore[no-untyped-def]
            """X."""
            return a


def test_scopes_imply_requires_auth(server: MCPServer) -> None:
    @server.tool(scopes=("admin",))
    def secret() -> str:
        """Secret."""
        return "s"

    definition = server.tools[0]
    assert definition.requires_auth is True
    assert definition.scopes == frozenset({"admin"})


def test_dynamic_register_and_unregister(server: MCPServer) -> None:
    def late(a: int) -> int:
        """Registered at runtime."""
        return a

    definition = server.register_tool(late, name="late_tool")
    assert "late_tool" in [t.name for t in server.tools]
    assert definition.name == "late_tool"

    removed = server.unregister_tool("late_tool")
    assert removed.name == "late_tool"
    assert server.tools == []
    with pytest.raises(ToolRegistrationError, match="no tool named"):
        server.unregister_tool("late_tool")


def test_tools_listed_sorted(server: MCPServer) -> None:
    for tool_name in ("charlie", "alpha", "bravo"):

        def fn() -> str:
            """Named tool."""
            return "x"

        server.register_tool(fn, name=tool_name)

    assert [t.name for t in server.tools] == ["alpha", "bravo", "charlie"]


def test_invalid_timeout_rejected(server: MCPServer) -> None:
    with pytest.raises(ToolRegistrationError, match="timeout"):

        @server.tool(timeout=0)
        def fn() -> str:
            """X."""
            return "x"
