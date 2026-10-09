"""Argument completion (``completion/complete``) for prompts and resource templates.

A completion source is a static list of strings, or a callable
``fn(value, arguments)`` that returns an iterable of strings (sync or
async), where *value* is what the client has typed so far and *arguments*
the other arguments it has already filled in.  ``Literal`` and ``bool``
parameters complete from their members without one.  At most
:data:`COMPLETION_MAX` values are returned.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .exceptions import RegistrationError

# Values one completion/complete result carries at most (the spec's cap).
COMPLETION_MAX = 100

Completer = (
    Iterable[str] | Callable[[str, Mapping[str, str]], Iterable[str] | Awaitable[Iterable[str]]]
)


@dataclass(frozen=True, slots=True)
class CompletionSource:
    """Where an argument's completions come from: a static tuple, or a callable."""

    values: tuple[str, ...] | None = None
    fn: Callable[..., Any] | None = None
    is_async: bool = False


def source_from(spec: Any, *, what: str) -> CompletionSource:
    """The completion source *spec* describes.

    Raises:
        RegistrationError: A bare string (it would complete character by
            character), or an iterable holding something other than strings.
    """
    if isinstance(spec, str | bytes):
        raise RegistrationError(
            f"{what}: a completer must be a list of strings or a callable, not a bare string"
        )
    if callable(spec):
        return CompletionSource(fn=spec, is_async=inspect.iscoroutinefunction(spec))
    try:
        values = tuple(spec)
    except TypeError:
        raise RegistrationError(
            f"{what}: a completer must be a list of strings or a callable"
        ) from None
    if not all(isinstance(value, str) for value in values):
        raise RegistrationError(f"{what}: a completer list must hold strings only")
    return CompletionSource(values=values)


def choices_source(choices: Sequence[str]) -> CompletionSource:
    """The automatic source of a ``Literal`` or ``bool`` parameter."""
    return CompletionSource(values=tuple(choices))


def filter_static(values: Iterable[str], prefix: str) -> list[str]:
    """*values* matching *prefix*: prefix matches first, then substring matches.

    Both case-insensitive, each group in the declared order.
    """
    needle = prefix.casefold()
    starts: list[str] = []
    contains: list[str] = []
    for value in values:
        folded = value.casefold()
        if folded.startswith(needle):
            starts.append(value)
        elif needle in folded:
            contains.append(value)
    return starts + contains


def collect(values: Iterable[Any], *, sized: bool) -> tuple[list[str], int | None]:
    """The first ``COMPLETION_MAX + 1`` distinct strings of *values*, and their total if known.

    A *sized* source (a static list, or a callable that returned a sequence)
    is read whole, so its total is known; any other is read no further than
    one value past the cap, so an endless generator is safe.

    Raises:
        TypeError: A value that is not a string.
    """
    seen: dict[str, None] = {}
    for value in values:
        if not isinstance(value, str):
            raise TypeError(f"a completion value of type {type(value).__name__}")
        seen.setdefault(value, None)
        if not sized and len(seen) > COMPLETION_MAX:
            break
    found = list(seen)
    return found, (len(found) if sized else None)


def shape(values: list[str], total: int | None) -> dict[str, Any]:
    """The ``completion`` object of a result: at most 100 values, ``hasMore``, ``total``."""
    completion: dict[str, Any] = {"values": values[:COMPLETION_MAX]}
    if total is not None:
        completion["total"] = total
    completion["hasMore"] = len(values) > COMPLETION_MAX
    return completion


def empty() -> dict[str, Any]:
    """The ``completion`` object of an argument with nothing to offer."""
    return {"values": [], "hasMore": False}
