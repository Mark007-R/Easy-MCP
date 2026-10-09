"""Content a resource or a prompt returns, and how the server renders it.

Resources return their content as a plain value (``str`` is text, ``bytes``
a base64 ``blob``, dicts, lists and Pydantic models JSON), or as
:class:`ResourceContent` items.  Prompts return a string or
:class:`Message` items whose content is text, an :class:`Image`, an
:class:`Audio` clip, an embedded :class:`ResourceContent` or a
:class:`ResourceLink`.
"""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

# type/subtype, optionally followed by parameters (text/plain; charset=utf-8).
_MIME_TYPE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*"
    r"(?:\s*;\s*[A-Za-z0-9!#$&^_.+-]+=(?:[A-Za-z0-9!#$&^_.+-]+|\"[^\"\x00-\x1f]*\"))*"
)

_ROLES = ("user", "assistant")


def is_mime_type(value: object) -> bool:
    """Whether *value* is a MIME type (``type/subtype``, parameters allowed)."""
    return isinstance(value, str) and _MIME_TYPE.fullmatch(value) is not None


def _check_mime_type(value: object, what: str) -> None:
    if not is_mime_type(value):
        raise ValueError(
            f"{what}: mime_type must be a MIME type such as 'image/png', got {value!r}"
        )


def _b64(data: bytes | bytearray | memoryview) -> str:
    """Standard, padded base64, as MCP's ``blob`` and ``data`` fields carry binary data."""
    return base64.b64encode(bytes(data)).decode("ascii")


_BINARY = (bytes, bytearray, memoryview)


@dataclass(frozen=True, slots=True)
class ResourceContent:
    """One item of a resource's contents, or a resource embedded in a prompt message.

    Exactly one of *text* and *blob* is set.  Returned from a resource,
    *uri* defaults to the URI that was read and *mime_type* to the
    resource's; embedded in a prompt message, both are required.
    """

    text: str | None = None
    blob: bytes | bytearray | memoryview | None = None
    uri: str | None = None
    mime_type: str | None = None

    def __post_init__(self) -> None:
        if (self.text is None) == (self.blob is None):
            raise ValueError("ResourceContent needs exactly one of text and blob")
        if self.text is not None and not isinstance(self.text, str):
            raise ValueError("ResourceContent.text must be a str")
        if self.blob is not None and not isinstance(self.blob, _BINARY):
            raise ValueError("ResourceContent.blob must be bytes")
        if self.uri is not None and not isinstance(self.uri, str):
            raise ValueError("ResourceContent.uri must be a str")
        if self.mime_type is not None:
            _check_mime_type(self.mime_type, "ResourceContent")


@dataclass(frozen=True, slots=True)
class Image:
    """An image in a prompt message: its bytes and MIME type."""

    data: bytes | bytearray | memoryview
    mime_type: str

    def __post_init__(self) -> None:
        if not isinstance(self.data, _BINARY):
            raise ValueError("Image.data must be bytes")
        _check_mime_type(self.mime_type, "Image")


@dataclass(frozen=True, slots=True)
class Audio:
    """An audio clip in a prompt message: its bytes and MIME type."""

    data: bytes | bytearray | memoryview
    mime_type: str

    def __post_init__(self) -> None:
        if not isinstance(self.data, _BINARY):
            raise ValueError("Audio.data must be bytes")
        _check_mime_type(self.mime_type, "Audio")


@dataclass(frozen=True, slots=True)
class ResourceLink:
    """A link to a resource in a prompt message; the client reads it if it wants it."""

    uri: str
    name: str
    title: str | None = None
    description: str | None = None
    mime_type: str | None = None
    size: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.uri, str) or not self.uri:
            raise ValueError("ResourceLink.uri must be a non-empty str")
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("ResourceLink.name must be a non-empty str")
        if self.mime_type is not None:
            _check_mime_type(self.mime_type, "ResourceLink")
        if self.size is not None and (
            isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0
        ):
            raise ValueError("ResourceLink.size must be a non-negative int")


Content = str | Image | Audio | ResourceContent | ResourceLink | Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Message:
    """One message of a prompt: who says it, and what."""

    role: Literal["user", "assistant"]
    content: Content

    def __post_init__(self) -> None:
        if self.role not in _ROLES:
            raise ValueError(f"Message.role must be 'user' or 'assistant', got {self.role!r}")

    @classmethod
    def user(cls, content: Content) -> Message:
        """A message from the user."""
        return cls("user", content)

    @classmethod
    def assistant(cls, content: Content) -> Message:
        """A message from the assistant."""
        return cls("assistant", content)


class NotFound(Exception):
    """A resource function returned ``None``: the resource does not exist."""


# ------------------------------------------------------------- resources


def _json_text(value: Any) -> str:
    """*value* as JSON text: deterministic (sorted keys), ``str()`` for the rest."""
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        try:
            value = dump(mode="json")
        except Exception:  # not a Pydantic model after all
            pass
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _is_model(value: Any) -> bool:
    kind = type(value)
    return hasattr(kind, "model_json_schema") and callable(getattr(value, "model_dump", None))


def _item(content: ResourceContent, uri: str, mime_type: str | None) -> dict[str, Any]:
    entry: dict[str, Any] = {"uri": content.uri if content.uri is not None else uri}
    mime = content.mime_type if content.mime_type is not None else mime_type
    if mime is not None:
        entry["mimeType"] = mime
    if content.text is not None:
        entry["text"] = content.text
    else:
        assert content.blob is not None
        entry["blob"] = _b64(content.blob)
    return entry


def to_resource_contents(result: Any, *, uri: str, mime_type: str | None) -> list[dict[str, Any]]:
    """The ``contents`` of a ``resources/read`` result for what a resource returned.

    *mime_type* is the resource's own (``None``: the default for the value).

    Raises:
        NotFound: *result* is ``None``.
        TypeError: *result* is of no supported type (the message names the type).
    """
    if result is None:
        raise NotFound
    if isinstance(result, ResourceContent):
        return [_item(result, uri, mime_type)]
    if (
        isinstance(result, list | tuple)
        and result
        and all(isinstance(item, ResourceContent) for item in result)
    ):
        return [_item(item, uri, mime_type) for item in result]
    if isinstance(result, str):
        return [{"uri": uri, "mimeType": mime_type or "text/plain", "text": result}]
    if isinstance(result, _BINARY):
        return [
            {"uri": uri, "mimeType": mime_type or "application/octet-stream", "blob": _b64(result)}
        ]
    if isinstance(result, list | tuple) and any(
        isinstance(item, ResourceContent) for item in result
    ):
        raise TypeError("a list mixing ResourceContent with other values")
    if _is_model(result) or isinstance(result, dict | list | tuple | int | float | bool):
        return [
            {"uri": uri, "mimeType": mime_type or "application/json", "text": _json_text(result)}
        ]
    raise TypeError(type(result).__name__)


# --------------------------------------------------------------- prompts


def _text(text: str) -> dict[str, Any]:
    return {"type": "text", "text": text}


# The keys a raw content mapping must carry, by its type.
_RAW_REQUIRED = {
    "text": ("text",),
    "image": ("data", "mimeType"),
    "audio": ("data", "mimeType"),
    "resource": ("resource",),
    "resource_link": ("uri", "name"),
}


def _embedded(content: ResourceContent) -> dict[str, Any]:
    if content.uri is None or content.mime_type is None:
        raise TypeError("an embedded ResourceContent needs a uri and a mime_type")
    return {"type": "resource", "resource": _item(content, content.uri, content.mime_type)}


def _link(link: ResourceLink) -> dict[str, Any]:
    entry: dict[str, Any] = {"type": "resource_link", "uri": link.uri, "name": link.name}
    if link.title is not None:
        entry["title"] = link.title
    if link.description is not None:
        entry["description"] = link.description
    if link.mime_type is not None:
        entry["mimeType"] = link.mime_type
    if link.size is not None:
        entry["size"] = link.size
    return entry


def _raw(content: Mapping[str, Any]) -> dict[str, Any]:
    kind = content.get("type")
    required = _RAW_REQUIRED.get(kind) if isinstance(kind, str) else None
    if required is None:
        raise TypeError(f"a content mapping of type {kind!r}")
    missing = [key for key in required if key not in content]
    if missing:
        raise TypeError(f"a {kind!r} content mapping without {', '.join(missing)}")
    if kind == "resource":
        resource = content["resource"]
        if (
            not isinstance(resource, Mapping)
            or "uri" not in resource
            or ("text" not in resource and "blob" not in resource)
        ):
            raise TypeError("a 'resource' content mapping without uri and text or blob")
    return dict(content)


def _content(content: Any) -> dict[str, Any]:
    if isinstance(content, str):
        return _text(content)
    if isinstance(content, Image):
        return {"type": "image", "data": _b64(content.data), "mimeType": content.mime_type}
    if isinstance(content, Audio):
        return {"type": "audio", "data": _b64(content.data), "mimeType": content.mime_type}
    if isinstance(content, ResourceContent):
        return _embedded(content)
    if isinstance(content, ResourceLink):
        return _link(content)
    if isinstance(content, Mapping):
        return _raw(content)
    raise TypeError(f"message content of type {type(content).__name__}")


def _message(item: Any) -> dict[str, Any]:
    if isinstance(item, str):
        return {"role": "user", "content": _text(item)}
    if isinstance(item, Message):
        return {"role": item.role, "content": _content(item.content)}
    if isinstance(item, Mapping):
        role = item.get("role")
        if role not in _ROLES or "content" not in item:
            raise TypeError("a message mapping needs a role ('user' or 'assistant') and content")
        return {"role": role, "content": _content(item["content"])}
    raise TypeError(f"a message of type {type(item).__name__}")


def to_prompt_messages(result: Any) -> list[dict[str, Any]]:
    """The ``messages`` of a ``prompts/get`` result for what a prompt returned.

    Raises:
        TypeError: Something that is no message (the message names it).
    """
    if isinstance(result, str | Message) or (isinstance(result, Mapping) and "role" in result):
        return [_message(result)]
    if isinstance(result, list | tuple):
        return [_message(item) for item in result]
    raise TypeError(f"a prompt result of type {type(result).__name__}")
