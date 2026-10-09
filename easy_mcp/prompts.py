"""Prompt registration: the ``@server.prompt`` decorator machinery.

A prompt is a function whose parameters are the prompt's arguments and whose
return value is its messages.  Arguments arrive as strings and are converted
to the parameter's type (``str``, ``int``, ``float``, ``bool`` or a
``Literal``) before the function runs.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .completion import CompletionSource, choices_source, source_from
from .decorators import Registry
from .exceptions import RegistrationError, SchemaError
from .schema import StringParameter, bind_string_arguments, parse_docstring, string_parameters

# The same rule as tool names: safe in an Mcp-Name header.
_PROMPT_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")


@dataclass(frozen=True, slots=True)
class PromptDefinition:
    """Everything the server knows about one registered prompt."""

    name: str
    fn: Callable[..., Any]
    is_async: bool
    arguments: tuple[StringParameter, ...] = ()
    title: str | None = None
    description: str | None = None
    requires_auth: bool = False
    scopes: frozenset[str] = frozenset()
    declared_scopes: tuple[str, ...] = ()
    timeout: float | None = None
    completers: Mapping[str, CompletionSource] = field(default_factory=dict)

    def to_mcp(self) -> dict[str, Any]:
        """Serialize it for a ``prompts/list`` response."""
        entry: dict[str, Any] = {"name": self.name}
        if self.title is not None:
            entry["title"] = self.title
        if self.description:
            entry["description"] = self.description
        if self.arguments:
            arguments = []
            for param in self.arguments:
                argument: dict[str, Any] = {"name": param.name}
                if param.description:
                    argument["description"] = param.description
                argument["required"] = param.required
                arguments.append(argument)
            entry["arguments"] = arguments
        return entry

    def bind(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        """The function's keyword arguments for the strings a client sent.

        Raises:
            ValidationError: ``-32602`` listing every violation.
        """
        return bind_string_arguments(self.arguments, arguments)


def build_prompt(
    fn: Callable[..., Any],
    *,
    name: str | None = None,
    title: str | None = None,
    description: str | None = None,
    requires_auth: bool = False,
    scopes: Iterable[str] = (),
    timeout: float | None = None,
    complete: Mapping[str, Any] | None = None,
) -> PromptDefinition:
    """Introspect *fn* and produce a :class:`PromptDefinition`.

    Raises:
        RegistrationError: The prompt cannot be served safely.
    """
    if not callable(fn):
        raise RegistrationError(f"@prompt target must be callable, got {type(fn).__name__}")
    prompt_name = str(name or getattr(fn, "__name__", "") or "")
    if not _PROMPT_NAME_RE.match(prompt_name):
        raise RegistrationError(
            f"invalid prompt name {prompt_name!r}: use 1-64 chars [A-Za-z0-9_-], "
            "starting with a letter"
        )
    what = f"cannot register prompt {prompt_name!r}"
    if title is not None and (not isinstance(title, str) or not title):
        raise RegistrationError(f"{what}: title must be a non-empty string")
    if timeout is not None and timeout <= 0:
        raise RegistrationError(f"{what}: timeout must be positive")
    summary, param_docs = parse_docstring(inspect.getdoc(fn))
    text = (description if description is not None else summary).strip() or None
    try:
        params = string_parameters(fn, param_docs)
    except SchemaError as exc:
        raise RegistrationError(f"{what}: {exc}") from exc
    names = {param.name for param in params}
    completers: dict[str, CompletionSource] = {}
    for param in params:
        if param.choices is not None:
            completers[param.name] = choices_source(param.choices)
    for key, spec in (complete or {}).items():
        if key not in names:
            raise RegistrationError(f"{what}: complete= names {key!r}, which is no argument")
        completers[key] = source_from(spec, what=f"{what}: complete[{key!r}]")
    declared = tuple(dict.fromkeys(scopes))
    scope_set = frozenset(declared)
    return PromptDefinition(
        name=prompt_name,
        fn=fn,
        is_async=inspect.iscoroutinefunction(fn),
        arguments=params,
        title=title,
        description=text,
        requires_auth=bool(requires_auth or scope_set),
        scopes=scope_set,
        declared_scopes=declared,
        timeout=timeout,
        completers=completers,
    )


class PromptRegistry(Registry[PromptDefinition]):
    """Thread-safe, deterministic registry of prompt definitions."""

    def __init__(self) -> None:
        super().__init__(noun="prompt", error=RegistrationError)
