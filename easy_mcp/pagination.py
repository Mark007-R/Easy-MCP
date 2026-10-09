"""Pagination of ``resources/list``, ``resources/templates/list`` and ``prompts/list``.

A page holds :data:`PAGE_SIZE` items.  The cursor is the key of the last
item of the page, so the next page starts after it whatever was added or
removed meanwhile: no item is repeated, and only removed items are skipped.
It is ``base64url(JSON {"k": kind, "a": last key})`` without padding and is
not signed: it only holds a key its caller was already shown, and a forged
one only moves where the caller's own visible list starts.  ``tools/list``
is not paginated.
"""

from __future__ import annotations

import base64
import binascii
import bisect
import json
import re
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from .exceptions import INVALID_PARAMS, ProtocolError
from .uritemplate import MAX_URI_LENGTH

PAGE_SIZE = 100

_T = TypeVar("_T")
_CURSOR = re.compile(r"[A-Za-z0-9_-]+")
# The longest cursor handed out: base64 of the JSON holding the longest key,
# a URI or template of MAX_URI_LENGTH characters at up to four UTF-8 bytes
# each (an escaped quote or backslash takes two; URIs hold no control
# characters), plus room for the rest of the JSON.
_MAX_CURSOR = 4 * ((4 * MAX_URI_LENGTH + 64 + 2) // 3)


def encode_cursor(kind: str, key: str) -> str:
    """The cursor of a page of *kind* whose last item has *key*."""
    raw = json.dumps({"k": kind, "a": key}, separators=(",", ":"), ensure_ascii=False)
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _invalid() -> ProtocolError:
    return ProtocolError("Invalid cursor", code=INVALID_PARAMS)


def decode_cursor(kind: str, cursor: object) -> str:
    """The key a cursor of *kind* names.

    Raises:
        ProtocolError: ``-32602 "Invalid cursor"`` for anything this server
            did not hand out for *kind*.
    """
    if not isinstance(cursor, str) or len(cursor) > _MAX_CURSOR:
        raise _invalid()
    if _CURSOR.fullmatch(cursor) is None:
        raise _invalid()
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        data = json.loads(raw.decode("utf-8"))
    except (binascii.Error, ValueError):
        raise _invalid() from None
    if not isinstance(data, dict) or data.get("k") != kind or not isinstance(data.get("a"), str):
        raise _invalid()
    return str(data["a"])


def paginate(
    items: Sequence[_T], *, key: Callable[[_T], str], kind: str, cursor: Any
) -> tuple[list[_T], str | None]:
    """One page of *items* (sorted by *key*) after *cursor*, and the next page's cursor.

    *cursor* ``None`` starts at the beginning.  The cursor is ``None`` on
    the last page.

    Raises:
        ProtocolError: ``-32602`` for an invalid cursor.
    """
    start = 0
    if cursor is not None:
        after = decode_cursor(kind, cursor)
        start = bisect.bisect_right([key(item) for item in items], after)
    page = list(items[start : start + PAGE_SIZE])
    if start + PAGE_SIZE < len(items) and page:
        return page, encode_cursor(kind, key(page[-1]))
    return page, None
