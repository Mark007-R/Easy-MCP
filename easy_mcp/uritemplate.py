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

When a URI could be split in several ways, each value is as long as it can
be, the first one first: ``{name}.{ext}`` binds ``a.b.c`` as ``a.b`` and
``c``.  Matching takes time linear in the URI's length, whatever it holds.
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

# The characters a value cannot hold: {name} stays in one path segment,
# {+name} may span several; neither reaches the query or the fragment.
_SIMPLE_STOPS = "/?#"
_RESERVED_STOPS = "?#"
_STOP_PATTERNS = {
    stops: re.compile(f"[{re.escape(stops)}]") for stops in (_SIMPLE_STOPS, _RESERVED_STOPS)
}


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

    __slots__ = (
        "_literals",
        "_shortest",
        "_stops",
        "literal_length",
        "reserved",
        "template",
        "variables",
    )

    def __init__(self, template: str) -> None:
        validate_uri(template, what="resource template")
        if template.count("{") != template.count("}"):
            raise RegistrationError(f"invalid resource template {template!r}: unbalanced braces")
        variables: list[str] = []
        reserved: set[str] = set()
        # The text around the expressions, one more than there are of them,
        # and the characters each expression's value cannot hold.
        literals: list[str] = []
        stops: list[str] = []
        literal_length = 0
        position = 0
        for match in _EXPRESSION.finditer(template):
            literal = template[position : match.start()]
            if "{" in literal or "}" in literal:
                raise RegistrationError(
                    f"invalid resource template {template!r}: unbalanced braces"
                )
            literals.append(literal)
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
            stops.append(_RESERVED_STOPS if is_reserved else _SIMPLE_STOPS)
        tail = template[position:]
        if "{" in tail or "}" in tail:
            raise RegistrationError(f"invalid resource template {template!r}: unbalanced braces")
        literals.append(tail)
        literal_length += len(tail)
        if not variables:
            raise RegistrationError(f"invalid resource template {template!r}: no variables")
        self.template = template
        self.variables = tuple(variables)
        self.reserved = frozenset(reserved)
        self.literal_length = literal_length
        self._literals = tuple(literals)
        self._stops = tuple(stops)
        # The shortest URI that can match, less the head and the tail: one
        # character per value and every literal between two of them.
        self._shortest = len(variables) + sum(len(literal) for literal in literals[1:-1])

    def match(self, uri: str) -> dict[str, str] | None:
        """The decoded value of each variable when *uri* matches the whole template.

        ``None`` when it does not match, or when a value is not valid
        percent-encoded UTF-8 or fails the traversal guard.
        """
        raws = self._split(uri)
        if raws is None:
            return None
        values: dict[str, str] = {}
        for name, raw in zip(self.variables, raws, strict=True):
            try:
                value = unquote(raw, encoding="utf-8", errors="strict")
            except UnicodeDecodeError:
                return None
            if _bad_escape(raw) or _unsafe(value, name in self.reserved):
                return None
            values[name] = value
        return values

    def may_overlap(self, other: UriTemplate) -> bool:
        """Whether a URI might match both templates.

        ``False`` only when none can: the literal text before their first
        expressions, or after their last ones, disagree.
        """
        head, tail = self._literals[0], self._literals[-1]
        other_head, other_tail = other._literals[0], other._literals[-1]
        heads_agree = head.startswith(other_head) or other_head.startswith(head)
        tails_agree = tail.endswith(other_tail) or other_tail.endswith(tail)
        return heads_agree and tails_agree

    def _split(self, uri: str) -> list[str] | None:
        """Each variable's raw value when *uri* matches the whole template, else ``None``.

        The split is the one a regular expression with a greedy ``[^/?#]+``
        or ``[^?#]+`` group per expression finds, each value ending as late
        as it can, the first one first, but found without backtracking, so
        in time linear in the URI's length.  A *point* is where the literal
        between two values starts.  Each point starts as late as it could
        be and only ever moves left, to the previous occurrence of its
        literal, when a rule forces it: a value holds at least one
        character (so the point after it bounds the one before it), and
        none of its stop characters (so the point before it bounds the one
        after it).  Both rules only push points left, so once they all hold,
        every point is as late as any split allows, which is the greedy
        split; a point pushed below the earliest it could be means there is
        none.  As points never move right, each stretch of the URI is
        searched about once per value.
        """
        head, *inner, tail = self._literals
        start = len(head)
        end = len(uri) - len(tail)
        if end - start < self._shortest or not uri.startswith(head) or not uri.endswith(tail):
            return None
        stops = self._stops
        count = len(inner)
        if not count:
            if _STOP_PATTERNS[stops[0]].search(uri, start, end) is not None:
                return None
            return [uri[start:end]]
        sizes = [len(literal) for literal in inner]
        # The earliest each point can be: every value before it holds a character.
        low: list[int] = []
        floor = start
        for size in sizes:
            low.append(floor + 1)
            floor += size + 1
        # The last value runs to the tail, so it starts after the last of its stops.
        last = max(uri.rfind(char, start, end) for char in stops[-1])
        low[-1] = max(low[-1], last + 1 - sizes[-1])
        points = [end] * count
        # [clean[i], points[i]) holds none of value i's stops (empty to begin with).
        clean = [end] * count
        # The values (but the last) whose stops are to be looked for, as their
        # start moved left; the first one is looked at first.
        queue = list(range(count - 1, -1, -1))
        queued = [True] * count

        def push_left(index: int, cap: int) -> bool:
            # Move point index to the last occurrence of its literal at or
            # before cap, and the points before it as far as that pushes them.
            while cap < points[index]:
                if cap < low[index]:
                    return False
                found = uri.rfind(inner[index], start, cap + sizes[index])
                if found < low[index]:
                    return False
                points[index] = found
                after = index + 1
                if after < count and not queued[after]:
                    queued[after] = True
                    queue.append(after)
                if index == 0:
                    break
                index -= 1
                cap = found - sizes[index] - 1
            return True

        if not push_left(count - 1, end - sizes[-1] - 1):
            return None
        while queue:
            index = queue.pop()
            queued[index] = False
            begin = start if index == 0 else points[index - 1] + sizes[index - 1]
            unchecked = min(clean[index], points[index])
            if begin < unchecked:
                stop = _STOP_PATTERNS[stops[index]].search(uri, begin, unchecked)
                if stop is not None and not push_left(index, stop.start()):
                    return None
            clean[index] = begin
        values = [uri[start : points[0]]]
        for index in range(1, count):
            values.append(uri[points[index - 1] + sizes[index - 1] : points[index]])
        values.append(uri[points[-1] + sizes[-1] : end])
        return values

    def __repr__(self) -> str:
        return f"UriTemplate({self.template!r})"


_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


def _bad_escape(raw: str) -> bool:
    """Whether *raw* holds a ``%`` that starts no valid escape (``unquote`` keeps those)."""
    return _ESCAPE.search(raw) is not None
