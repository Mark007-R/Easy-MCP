"""Tool registration: the ``@server.tool`` decorator machinery, and the registries."""

from __future__ import annotations

import inspect
import logging
import re
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar

from .exceptions import RegistrationError, SchemaError, ToolRegistrationError
from .schema import (
    build_input_schema,
    build_output_schema,
    build_validation_schema,
    collect_param_models,
    output_model,
    parse_docstring,
)

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
    param_models: Mapping[str, Any] = field(default_factory=dict)
    output_model: Any | None = None
    #: What arguments are validated against.  Same as ``input_schema`` unless
    #: Pydantic models are involved, which validate themselves.
    validation_schema: dict[str, Any] | None = None

    @property
    def arguments_schema(self) -> dict[str, Any]:
        """The schema arguments are checked against before the tool runs."""
        return self.validation_schema or self.input_schema
    requires_auth: bool = False
    scopes: frozenset[str] = frozenset()
    tags: tuple[str, ...] = ()
    category: str | None = None
    examples: tuple[Mapping[str, Any], ...] = ()
    timeout: float | None = None
    max_calls_per_session: int | None = None
    #: ``scopes`` in the order given, duplicates removed.  The first is what
    #: an OAuth step-up challenge asks for, so list the narrowest first.
    declared_scopes: tuple[str, ...] = ()

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
        scopes: Scopes the caller must hold one of to call the tool.  A
            non-empty value implies ``requires_auth``.  List the narrowest
            first: with OAuth, a step-up challenge asks for the first one.
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

    param_models = collect_param_models(fn)
    result_model = output_model(fn)
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

    # Read once: *scopes* may be a one-shot iterator.
    declared = tuple(dict.fromkeys(scopes))
    scope_set = frozenset(declared)
    return ToolDefinition(
        name=tool_name,
        description=tool_description,
        fn=fn,
        input_schema=input_schema,
        is_async=inspect.iscoroutinefunction(fn),
        output_schema=result_schema,
        param_models=param_models,
        output_model=result_model,
        validation_schema=build_validation_schema(input_schema, param_models),
        # A scope requirement implies the tool is protected.
        requires_auth=bool(requires_auth or scope_set),
        scopes=scope_set,
        tags=tuple(tags),
        category=category,
        examples=tuple(dict(example) for example in examples),
        timeout=timeout,
        max_calls_per_session=max_calls_per_session,
        declared_scopes=declared,
    )


class _Named(Protocol):
    @property
    def name(self) -> str: ...


_T = TypeVar("_T", bound=_Named)


class Registry(Generic[_T]):
    """Thread-safe, deterministic registry of definitions, keyed by name.

    Args:
        noun: What the definitions are, for messages (``"tool"``).
        error: What a refused registration or removal raises.
    """

    def __init__(
        self, *, noun: str = "item", error: type[RegistrationError] = RegistrationError
    ) -> None:
        self._items: dict[str, _T] = {}
        self._lock = threading.Lock()
        self._noun = noun
        self._error = error
        # Bumped by every change, so a digest of the list knows it is stale.
        self._version = 0

    def register(self, item: _T, *, replace: bool = False) -> None:
        """Add an item; refuses silent overwrites unless ``replace=True``."""
        with self._lock:
            if item.name in self._items and not replace:
                raise self._error(f"a {self._noun} named {item.name!r} is already registered")
            self._items[item.name] = item
            self._version += 1

    def unregister(self, name: str) -> _T:
        """Remove and return an item by name."""
        with self._lock:
            try:
                removed = self._items.pop(name)
            except KeyError:
                raise self._error(f"no {self._noun} named {name!r} is registered") from None
            self._version += 1
            return removed

    @property
    def version(self) -> int:
        """How many changes the registry has seen."""
        with self._lock:
            return self._version

    def snapshot(self) -> tuple[int, list[_T]]:
        """The version and the items sorted by name, as they stood together."""
        with self._lock:
            return self._version, sorted(self._items.values(), key=lambda item: item.name)

    def get(self, name: str) -> _T | None:
        """Look up an item by name, or ``None``."""
        with self._lock:
            return self._items.get(name)

    def list(self) -> list[_T]:
        """Items sorted by name, so list output is reproducible."""
        with self._lock:
            return sorted(self._items.values(), key=lambda item: item.name)

    def __contains__(self, name: object) -> bool:
        with self._lock:
            return name in self._items

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


class ToolRegistry(Registry[ToolDefinition]):
    """Thread-safe, deterministic registry of tool definitions."""

    def __init__(self) -> None:
        super().__init__(noun="tool", error=ToolRegistrationError)
