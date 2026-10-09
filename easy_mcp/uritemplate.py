"""URI templates for resources: the part of RFC 6570 easy_mcp supports, and URI checks.

Two expressions are supported, decided at registration:

* ``{name}`` matches one path segment: no ``/``, ``?`` or ``#``.
* ``{+name}`` (reserved expansion) may span segments: no ``?`` or ``#``.

A URI is matched against the whole template; each captured value is
percent-decoded as strict UTF-8, and then refused if it could walk out of a
folder: a ``.`` or ``..`` segment, a backslash, NUL or another control
character, a leading ``/`` for ``{+name}`` and any ``/`` for ``{name}``.  A
value that is refused means the URI does not match, so it reads as "not
found" and the resource function never sees it.  Empty values never match.
Every other RFC 6570 operator, list, prefix and explode modifier is refused
at registration.
"""

from __future__ import annotations

import re
from urllib.parse import unquote

from .exceptions import RegistrationError

# The longest URI a resource may be registered under, or a template written as.
MAX_URI_LENGTH = 2048

_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:")
# Whitespace and every C0/C1 control character, DEL included.
_FORBIDDEN = re.compile(r"[\s\x00-\x1f\x7f-\x9f]")
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_EXPRESSION = re.compile(r"\{([^{}]*)\}")
_VARIABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

_SIMPLE_VALUE = r"[^/?#]+"
_RESERVED_VALUE = r"[^?#]+"


def is_template(uri: str) -> bool:
    """Whether *uri* is a template rather than a concrete URI (a ``{`` decides)."""
    return "{" in uri


def validate_uri(uri: str, *, what: str = "resource URI") -> None:
    """Check a concrete URI, or the literal text of a template.

    It needs a scheme (``scheme:...``), no whitespace or control characters,
    and at most 2048 characters.

    Raises:
        RegistrationError: Naming the problem.
    """
    if not isinstance(uri, str) or not uri:
        raise RegistrationError(f"invalid {what} {uri!r}: expected a non-empty string")
    if len(uri) > MAX_URI_LENGTH:
        raise RegistrationError(f"invalid {what}: longer than {MAX_URI_LENGTH} characters")
    if _SCHEME.match(uri) is None:
        raise RegistrationError(
            f"invalid {what} {uri!r}: it needs a scheme, as in 'config://app' or 'file:///x'"
        )
    if _FORBIDDEN.search(uri) is not None:
        raise RegistrationError(
            f"invalid {what} {uri!r}: whitespace and control characters are not allowed"
        )


def _unsafe(value: str, reserved: bool) -> bool:
    """Whether a decoded template value could escape a folder (the traversal guard)."""
    if not value or "\\" in value or _CONTROL.search(value) is not None:
        return True
    if not reserved:
        return "/" in value or value in (".", "..")
    if value.startswith("/"):
        return True
    return any(segment in (".", "..") for segment in value.split("/"))


class UriTemplate:
    """A parsed resource template: ``{name}`` and ``{+name}`` expressions only.

    Attributes:
        template: The template as registered (what ``uriTemplate`` advertises).
        variables: Its variable names, in order.
        reserved: The names written ``{+name}``.
        literal_length: How many characters are not expressions; when several
            templates match a URI, the one with the most wins.

    Raises:
        RegistrationError: An unsupported or malformed template.
    """

    __slots__ = ("_pattern", "literal_length", "reserved", "template", "variables")

    def __init__(self, template: str) -> None:
        validate_uri(template, what="resource template")
        if template.count("{") != template.count("}"):
            raise RegistrationError(f"invalid resource template {template!r}: unbalanced braces")
        variables: list[str] = []
        reserved: set[str] = set()
        pattern: list[str] = []
        literal_length = 0
        position = 0
        for match in _EXPRESSION.finditer(template):
            literal = template[position : match.start()]
            if "{" in literal or "}" in literal:
                raise RegistrationError(
                    f"invalid resource template {template!r}: unbalanced braces"
                )
            pattern.append(re.escape(literal))
            literal_length += len(literal)
            position = match.end()
            expression = match.group(1)
            is_reserved = expression.startswith("+")
            name = expression[1:] if is_reserved else expression
            if not expression:
                raise RegistrationError(
                    f"invalid resource template {template!r}: an empty expression {{}}"
                )
            if _VARIABLE.fullmatch(name) is None:
                raise RegistrationError(
                    f"invalid resource template {template!r}: {{{expression}}} is not supported; "
                    "use {name} for one path segment or {+name} for several (RFC 6570 "
                    "operators, lists, prefixes and explode are not supported)"
                )
            if name in variables:
                raise RegistrationError(
                    f"invalid resource template {template!r}: variable {name!r} appears twice"
                )
            variables.append(name)
            if is_reserved:
                reserved.add(name)
            pattern.append(f"(?P<{name}>{_RESERVED_VALUE if is_reserved else _SIMPLE_VALUE})")
        tail = template[position:]
        if "{" in tail or "}" in tail:
            raise RegistrationError(f"invalid resource template {template!r}: unbalanced braces")
        pattern.append(re.escape(tail))
        literal_length += len(tail)
        if not variables:
            raise RegistrationError(f"invalid resource template {template!r}: no variables")
        self.template = template
        self.variables = tuple(variables)
        self.reserved = frozenset(reserved)
        self.literal_length = literal_length
        self._pattern = re.compile("".join(pattern), re.DOTALL)

    def match(self, uri: str) -> dict[str, str] | None:
        """The decoded value of each variable when *uri* matches the whole template.

        ``None`` when it does not match, or when a value is not valid
        percent-encoded UTF-8 or fails the traversal guard.
        """
        found = self._pattern.fullmatch(uri)
        if found is None:
            return None
        values: dict[str, str] = {}
        for name in self.variables:
            raw = found.group(name)
            try:
                value = unquote(raw, encoding="utf-8", errors="strict")
            except UnicodeDecodeError:
                return None
            if _bad_escape(raw) or _unsafe(value, name in self.reserved):
                return None
            values[name] = value
        return values

    def __repr__(self) -> str:
        return f"UriTemplate({self.template!r})"


_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


def _bad_escape(raw: str) -> bool:
    """Whether *raw* holds a ``%`` that starts no valid escape (``unquote`` keeps those)."""
    return _ESCAPE.search(raw) is not None
