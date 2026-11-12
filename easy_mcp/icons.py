"""Icons a client may show next to a server and its tools, resources and prompts.

An icon is a URL the client fetches, or the image itself as a ``data:`` URI::

    from easy_mcp import Icon, MCPServer

    server = MCPServer(icons=[Icon.from_file("logo.png")])

    @server.tool(icons=["https://example.com/icons/search.svg"])
    def search(query: str) -> list[str]: ...

A plain string stands for ``Icon(src=...)``.  Several icons let the client
pick by size or theme.

The specification tells clients to refuse every icon that is not ``https:``
or ``data:``, so those are the only ones registered here: an ``http:`` URL,
a file path or any other scheme is a registration error, not an icon that
silently never shows.
"""

from __future__ import annotations

import base64
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal

from .exceptions import RegistrationError

# The longest icon source accepted: a data: URI of a 64 KiB image.  Every
# list a client fetches carries the icons of every item on the page.
MAX_ICON_BYTES = 64 * 1024
MAX_SRC_LENGTH = 4 * ((MAX_ICON_BYTES + 2) // 3) + 128

# How many icons one item may carry.
MAX_ICONS = 8

_MIME_TYPE = re.compile(r"[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*")
_SIZE = re.compile(r"[1-9][0-9]{0,4}x[1-9][0-9]{0,4}")
_DATA_URI = re.compile(r"data:[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+;base64,[A-Za-z0-9+/]+={0,2}")
_HTTPS_URL = re.compile(r"https://[^\s/?#\\@]+(?:[/?#][^\s\\]*)?", re.IGNORECASE)

# What a file's extension says it is, for Icon.from_file.
_FILE_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".svg": "image/svg+xml",
    ".webp": "image/webp",
}


@dataclass(frozen=True, slots=True)
class Icon:
    """One icon, as the MCP ``Icon`` object.

    Args:
        src: An ``https://`` URL, or a ``data:`` URI holding the image in
            Base64.
        mime_type: The image's type (``"image/png"``), when the URL does not
            say.
        sizes: The sizes it suits, each ``"48x48"`` style or ``"any"`` (a
            scalable image).  Empty: any size.
        theme: ``"light"`` for an icon drawn for light backgrounds,
            ``"dark"`` for dark ones.  ``None``: either.

    Raises:
        ValueError: A value the specification, or a client following it,
            would refuse.
    """

    src: str
    mime_type: str | None = None
    sizes: tuple[str, ...] = ()
    theme: Literal["light", "dark"] | None = None

    def __post_init__(self) -> None:
        src = self.src
        if not isinstance(src, str) or not src:
            raise ValueError("an icon's src must be a non-empty string")
        if len(src) > MAX_SRC_LENGTH:
            raise ValueError(
                f"an icon's src is over {MAX_SRC_LENGTH} characters "
                f"(a data: URI of {MAX_ICON_BYTES // 1024} KiB); serve a larger image by URL"
            )
        if src.startswith("data:"):
            if _DATA_URI.fullmatch(src) is None:
                raise ValueError("a data: icon must be 'data:<type>/<subtype>;base64,<data>'")
        elif _HTTPS_URL.fullmatch(src) is None:
            raise ValueError(
                f"an icon's src must be an https:// URL or a data: URI, got {src[:60]!r}"
            )
        mime_type = self.mime_type
        if mime_type is not None and (
            not isinstance(mime_type, str) or _MIME_TYPE.fullmatch(mime_type) is None
        ):
            raise ValueError(f"an icon's mime_type must look like 'image/png', got {mime_type!r}")
        sizes = self.sizes
        if isinstance(sizes, str):
            sizes = (sizes,)
        sizes = tuple(sizes)
        for size in sizes:
            if not isinstance(size, str) or (size != "any" and _SIZE.fullmatch(size) is None):
                raise ValueError(f"an icon size must be like '48x48' or 'any', got {size!r}")
        object.__setattr__(self, "sizes", sizes)
        if self.theme not in (None, "light", "dark"):
            raise ValueError(f"an icon's theme must be 'light' or 'dark', got {self.theme!r}")

    @classmethod
    def from_file(
        cls,
        path: str | os.PathLike[str],
        *,
        mime_type: str | None = None,
        sizes: Iterable[str] = (),
        theme: Literal["light", "dark"] | None = None,
    ) -> Icon:
        """The image file at *path* as a ``data:`` icon, read once, now.

        The type comes from the extension (``.png``, ``.jpg``, ``.jpeg``,
        ``.svg``, ``.webp``) unless *mime_type* names it.

        Raises:
            ValueError: An unknown extension without *mime_type*, an empty
                file, or one over 64 KiB.
            OSError: The file cannot be read.
        """
        name = os.fspath(path)
        kind = mime_type or _FILE_TYPES.get(os.path.splitext(name)[1].lower())
        if kind is None:
            raise ValueError(
                f"cannot tell the image type of {name!r} from its extension; pass mime_type="
            )
        with open(name, "rb") as file:
            data = file.read(MAX_ICON_BYTES + 1)
        if not data:
            raise ValueError(f"the icon file {name!r} is empty")
        if len(data) > MAX_ICON_BYTES:
            raise ValueError(
                f"the icon file {name!r} is over {MAX_ICON_BYTES // 1024} KiB; "
                "serve it by URL instead"
            )
        encoded = base64.b64encode(data).decode("ascii")
        return cls(f"data:{kind};base64,{encoded}", mime_type=kind, sizes=tuple(sizes), theme=theme)

    def to_mcp(self) -> dict[str, Any]:
        """Serialize it as an MCP ``Icon``."""
        entry: dict[str, Any] = {"src": self.src}
        if self.mime_type is not None:
            entry["mimeType"] = self.mime_type
        if self.sizes:
            entry["sizes"] = list(self.sizes)
        if self.theme is not None:
            entry["theme"] = self.theme
        return entry


def normalize_icons(
    icons: Iterable[Icon | str] | Icon | str | None,
    what: str,
    error: type[Exception] = RegistrationError,
) -> tuple[Icon, ...]:
    """The ``icons=`` option of *what* as a tuple of :class:`Icon`.

    One icon, or a string, stands for a list of one.

    Raises:
        RegistrationError: (or *error*) Something in it is no icon, or there
            are more than :data:`MAX_ICONS`.
    """
    if icons is None:
        return ()
    if isinstance(icons, Icon | str):
        icons = (icons,)
    result: list[Icon] = []
    try:
        for icon in icons:
            if isinstance(icon, Icon):
                result.append(icon)
            elif isinstance(icon, str):
                result.append(Icon(icon))
            else:
                raise ValueError(f"an icon must be an Icon or a string, got {type(icon).__name__}")
    except (TypeError, ValueError) as exc:
        raise error(f"{what}: {exc}") from exc
    if len(result) > MAX_ICONS:
        raise error(f"{what}: at most {MAX_ICONS} icons, got {len(result)}")
    return tuple(result)


def icons_to_mcp(icons: tuple[Icon, ...]) -> list[dict[str, Any]]:
    return [icon.to_mcp() for icon in icons]
