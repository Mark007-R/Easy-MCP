"""Tool registration: the ``@server.tool`` decorator machinery and registry."""

from __future__ import annotations

import inspect
import logging
import re
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .exceptions import SchemaError, ToolRegistrationError
from .schema import build_input_schema, build_output_schema, parse_docstring

logger = logging.getLogger("easy_mcp.registry")

_TOOL_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    """Everything the server knows about one registered tool."""

    name: str
    description: str
    fn: Callable[..., Any]
    input_schema: dict[str, Any]
    is_async: bool
    output_schema: dict[str, Any] | None = None
    requires_auth: bool = False
    scopes: frozenset[str] = frozenset()
    tags: tuple[str, ...] = ()
    category: str | None = None
    examples: tuple[Mapping[str, Any], ...] = ()
    timeout: float | None = None
    max_calls_per_session: int | None = None

    def to_mcp(self) -> dict[str, Any]:
        """Serialize this tool for a ``tools/list`` response."""
        entry: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }
        if self.output_schema is not None:
            entry["outputSchema"] = self.output_schema
        meta: dict[str, Any] = {}
        if self.tags:
            meta["tags"] = list(self.tags)
        if self.category:
            meta["category"] = self.category
        if self.examples:
            meta["examples"] = [dict(example) for example in self.examples]
        if meta:
            # `_meta` is MCP's designated slot for implementation metadata.
            entry["_meta"] = {"easy_mcp": meta}
        return entry


def build_tool(
    fn: Callable[..., Any],
    *,
    name: str | None = None,
    description: str | None = None,
    output_schema: dict[str, Any] | None = None,
    requires_auth: bool = False,
    scopes: Iterable[str] = (),
    tags: Iterable[str] = (),
    category: str | None = None,
    examples: Iterable[Mapping[str, Any]] = (),
    timeout: float | None = None,
    max_calls_per_session: int | None = None,
) -> ToolDefinition:
    """Introspect *fn* and produce a :class:`ToolDefinition`.

    Args:
        fn: The plain (or async) Python function to expose.
        name: Override for the tool name (defaults to ``fn.__name__``).
        description: Override for the description (defaults to the docstring
            summary line).
        output_schema: Override for the output schema.  By default one is
            derived from the return annotation when it describes a JSON
            object; pass ``{}`` to advertise none at all.
        requires_auth: Mark the tool as callable only by authenticated clients.
        scopes: Scopes an API key must hold to call the tool.  A non-empty
            value implies ``requires_auth``.
        tags: Free-form labels surfaced to clients in tool metadata.
        category: Optional grouping label surfaced in tool metadata.
        examples: Example invocations, e.g. ``({"arguments": {...}},)``.
        timeout: Per-tool execution timeout in seconds (overrides the server
            default).
        max_calls_per_session: Cap on how often one session may call the tool.

    Raises:
        ToolRegistrationError: If the function cannot be exposed safely.
    """
    if not callable(fn):
        raise ToolRegistrationError(f"@tool target must be callable, got {type(fn).__name__}")
    tool_name: str = name or str(getattr(fn, "__name__", "") or "")
    if not _TOOL_NAME_RE.match(tool_name):
        raise ToolRegistrationError(
            f"invalid tool name {tool_name!r}: use 1-64 chars [A-Za-z0-9_-], "
            "starting with a letter"
        )
    if timeout is not None and timeout <= 0:
        raise ToolRegistrationError("timeout must be positive")
    if max_calls_per_session is not None and max_calls_per_session < 1:
        raise ToolRegistrationError("max_calls_per_session must be >= 1")

    summary, param_docs = parse_docstring(inspect.getdoc(fn))
    tool_description = (description or summary).strip()
    if not tool_description:
        # LLM clients pick tools by their descriptions; a tool without one is
        # effectively invisible to them.
        logger.warning(
            "tool %r has no description; add a docstring or description=", tool_name
        )
        tool_description = tool_name

    try:
        input_schema = build_input_schema(fn, param_docs)
    except SchemaError as exc:
        raise ToolRegistrationError(f"cannot register tool {tool_name!r}: {exc}") from exc

    if output_schema is None:
        result_schema = build_output_schema(fn)
    elif not output_schema:
        result_schema = None  # explicit opt-out
    elif output_schema.get("type") != "object":
        # structuredContent is a JSON object; advertising anything else would
        # promise clients something the protocol cannot carry.
        raise ToolRegistrationError(
            f"cannot register tool {tool_name!r}: output_schema must describe an object"
        )
    else:
        result_schema = output_schema

    scope_set = frozenset(scopes)
    return ToolDefinition(
        name=tool_name,
        description=tool_description,
        fn=fn,
        input_schema=input_schema,
        is_async=inspect.iscoroutinefunction(fn),
        output_schema=result_schema,
        # A scope requirement implies the tool is protected.
        requires_auth=bool(requires_auth or scope_set),
        scopes=scope_set,
        tags=tuple(tags),
        category=category,
        examples=tuple(dict(example) for example in examples),
        timeout=timeout,
        max_calls_per_session=max_calls_per_session,
    )


class ToolRegistry:
    """Thread-safe, deterministic registry of tool definitions."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}
        self._lock = threading.Lock()

    def register(self, tool: ToolDefinition, *, replace: bool = False) -> None:
        """Add a tool; refuses silent overwrites unless ``replace=True``."""
        with self._lock:
            if tool.name in self._tools and not replace:
                raise ToolRegistrationError(f"a tool named {tool.name!r} is already registered")
            self._tools[tool.name] = tool

    def unregister(self, name: str) -> ToolDefinition:
        """Remove and return a tool by name."""
        with self._lock:
            try:
                return self._tools.pop(name)
            except KeyError:
                raise ToolRegistrationError(f"no tool named {name!r} is registered") from None

    def get(self, name: str) -> ToolDefinition | None:
        """Look up a tool by name, or ``None``."""
        with self._lock:
            return self._tools.get(name)

    def list(self) -> list[ToolDefinition]:
        """Tools sorted by name, so ``tools/list`` output is reproducible."""
        with self._lock:
            return sorted(self._tools.values(), key=lambda tool: tool.name)

    def __contains__(self, name: object) -> bool:
        with self._lock:
            return name in self._tools

    def __len__(self) -> int:
        with self._lock:
            return len(self._tools)
