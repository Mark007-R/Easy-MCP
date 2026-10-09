"""Resource registration: concrete resources, URI templates, the registry and ``safe_path``.

A resource is a function whose return value is the content of a URI.  A URI
with ``{name}`` or ``{+name}`` in it is a template, and its variables become
the function's parameters (see :mod:`easy_mcp.uritemplate`).
"""

from __future__ import annotations

import inspect
import math
import threading
import types
import typing
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from .completion import CompletionSource, choices_source, source_from
from .content import ResourceContent, is_mime_type
from .exceptions import RegistrationError, ResourceNotFoundError, SchemaError
from .schema import (
    StringParameter,
    _unwrap_annotated,
    is_pydantic_model,
    parse_docstring,
    string_parameters,
)
from .uritemplate import UriTemplate, is_template, validate_uri

MAX_NAME_LENGTH = 128

_AUDIENCE = frozenset({"user", "assistant"})


def _check_annotations(annotations: Mapping[str, Any] | None, what: str) -> dict[str, Any] | None:
    """Validated MCP ``annotations``: ``audience``, ``priority``, ``lastModified``."""
    if annotations is None:
        return None
    if not isinstance(annotations, Mapping):
        raise RegistrationError(f"{what}: annotations must be a mapping")
    checked: dict[str, Any] = {}
    for key, value in annotations.items():
        if key == "audience":
            if (
                not isinstance(value, list | tuple)
                or not value
                or not all(isinstance(role, str) and role in _AUDIENCE for role in value)
            ):
                raise RegistrationError(
                    f"{what}: annotations.audience must list 'user' and/or 'assistant'"
                )
            checked[key] = list(dict.fromkeys(value))
        elif key == "priority":
            if isinstance(value, bool) or not isinstance(value, int | float) or not 0 <= value <= 1:
                raise RegistrationError(f"{what}: annotations.priority must be between 0 and 1")
            checked[key] = value
        elif key == "lastModified":
            if not isinstance(value, str) or not value:
                raise RegistrationError(
                    f"{what}: annotations.lastModified must be an ISO 8601 string"
                )
            checked[key] = value
        else:
            raise RegistrationError(
                f"{what}: unknown annotation {key!r} (use audience, priority, lastModified)"
            )
    return checked or None


def _names_content(annotation: Any) -> bool:
    """Whether *annotation* is ``ResourceContent`` or a union holding it."""
    annotation, _ = _unwrap_annotated(annotation)
    if annotation is ResourceContent:
        return True
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:
        return any(_names_content(arg) for arg in typing.get_args(annotation))
    return False


def _default_mime_type(fn: Callable[..., Any]) -> str | None:
    """The MIME type the return annotation implies: never guessed from anything else."""
    try:
        hints = typing.get_type_hints(fn, include_extras=True)
    except Exception:
        return None
    annotation, _ = _unwrap_annotated(hints.get("return", inspect.Parameter.empty))
    origin = typing.get_origin(annotation) or annotation
    if origin is str:
        return "text/plain"
    if origin in (bytes, bytearray, memoryview):
        return "application/octet-stream"
    if origin is list and any(_names_content(arg) for arg in typing.get_args(annotation)):
        return None  # each ResourceContent item carries its own, as a single one does
    if origin in (dict, list) or is_pydantic_model(annotation):
        return "application/json"
    return None


@dataclass(frozen=True, slots=True)
class ResourceDefinition:
    """Everything the server knows about one concrete resource."""

    uri: str
    name: str
    fn: Callable[..., Any]
    is_async: bool
    title: str | None = None
    description: str | None = None
    mime_type: str | None = None
    size: int | None = None
    annotations: Mapping[str, Any] | None = None
    requires_auth: bool = False
    scopes: frozenset[str] = frozenset()
    declared_scopes: tuple[str, ...] = ()
    timeout: float | None = None
    cache_ttl_ms: int = 0

    @property
    def key(self) -> str:
        """What it is registered and listed under: its URI."""
        return self.uri

    def to_mcp(self) -> dict[str, Any]:
        """Serialize it for a ``resources/list`` response."""
        entry: dict[str, Any] = {"uri": self.uri, "name": self.name}
        if self.title is not None:
            entry["title"] = self.title
        if self.description:
            entry["description"] = self.description
        if self.mime_type is not None:
            entry["mimeType"] = self.mime_type
        if self.size is not None:
            entry["size"] = self.size
        if self.annotations:
            entry["annotations"] = dict(self.annotations)
        return entry


@dataclass(frozen=True, slots=True)
class ResourceTemplateDefinition:
    """Everything the server knows about one resource template."""

    uri_template: str
    template: UriTemplate
    name: str
    fn: Callable[..., Any]
    is_async: bool
    params: Mapping[str, StringParameter]
    title: str | None = None
    description: str | None = None
    mime_type: str | None = None
    annotations: Mapping[str, Any] | None = None
    requires_auth: bool = False
    scopes: frozenset[str] = frozenset()
    declared_scopes: tuple[str, ...] = ()
    timeout: float | None = None
    cache_ttl_ms: int = 0
    completers: Mapping[str, CompletionSource] = field(default_factory=dict)

    @property
    def key(self) -> str:
        """What it is registered and listed under: the template as written."""
        return self.uri_template

    def to_mcp(self) -> dict[str, Any]:
        """Serialize it for a ``resources/templates/list`` response."""
        entry: dict[str, Any] = {"uriTemplate": self.uri_template, "name": self.name}
        if self.title is not None:
            entry["title"] = self.title
        if self.description:
            entry["description"] = self.description
        if self.mime_type is not None:
            entry["mimeType"] = self.mime_type
        if self.annotations:
            entry["annotations"] = dict(self.annotations)
        return entry

    def bind(self, uri: str) -> dict[str, Any] | None:
        """The function's arguments for *uri*, or ``None`` when it does not match.

        A URI matches when the template matches it whole, every value passes
        the traversal guard, and every value converts to its parameter's type.
        """
        values = self.template.match(uri)
        if values is None:
            return None
        bound: dict[str, Any] = {}
        for name, value in values.items():
            try:
                bound[name] = self.params[name].convert(value)
            except ValueError:
                return None
        return bound


def build_resource(
    fn: Callable[..., Any],
    uri: str,
    *,
    name: str | None = None,
    title: str | None = None,
    description: str | None = None,
    mime_type: str | None = None,
    size: int | None = None,
    annotations: Mapping[str, Any] | None = None,
    requires_auth: bool = False,
    scopes: Iterable[str] = (),
    timeout: float | None = None,
    cache_ttl: float = 0.0,
    complete: Mapping[str, Any] | None = None,
) -> ResourceDefinition | ResourceTemplateDefinition:
    """Introspect *fn* and produce the definition of the resource at *uri*.

    Raises:
        RegistrationError: The resource cannot be served safely.
    """
    if not callable(fn):
        raise RegistrationError(f"@resource target must be callable, got {type(fn).__name__}")
    if not isinstance(uri, str):
        raise RegistrationError(f"a resource URI must be a string, got {type(uri).__name__}")
    templated = is_template(uri)
    what = f"cannot register resource {uri!r}"
    template = UriTemplate(uri) if templated else None
    if template is None:
        validate_uri(uri)
    label = str(name if name is not None else getattr(fn, "__name__", "") or "")
    if not label or len(label) > MAX_NAME_LENGTH:
        raise RegistrationError(f"{what}: name must be 1 to {MAX_NAME_LENGTH} characters")
    if title is not None and (not isinstance(title, str) or not title):
        raise RegistrationError(f"{what}: title must be a non-empty string")
    if mime_type is not None and not is_mime_type(mime_type):
        raise RegistrationError(f"{what}: mime_type must look like 'type/subtype'")
    if timeout is not None and timeout <= 0:
        raise RegistrationError(f"{what}: timeout must be positive")
    if (
        isinstance(cache_ttl, bool)
        or not isinstance(cache_ttl, int | float)
        or not math.isfinite(cache_ttl)
        or cache_ttl < 0
    ):
        raise RegistrationError(f"{what}: cache_ttl must be a number of seconds >= 0")
    if size is not None:
        if templated:
            raise RegistrationError(f"{what}: a template has no size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise RegistrationError(f"{what}: size must be a non-negative int (bytes)")
    if complete is not None and not templated:
        raise RegistrationError(
            f"{what}: complete= needs a template; a concrete URI has nothing to complete"
        )
    checked_annotations = _check_annotations(annotations, what)

    summary, param_docs = parse_docstring(inspect.getdoc(fn))
    text = (description if description is not None else summary).strip() or None
    declared = tuple(dict.fromkeys(scopes))
    scope_set = frozenset(declared)
    common: dict[str, Any] = {
        "name": label,
        "fn": fn,
        "is_async": inspect.iscoroutinefunction(fn),
        "title": title,
        "description": text,
        "mime_type": mime_type if mime_type is not None else _default_mime_type(fn),
        "annotations": checked_annotations,
        "requires_auth": bool(requires_auth or scope_set),
        "scopes": scope_set,
        "declared_scopes": declared,
        "timeout": timeout,
        "cache_ttl_ms": round(cache_ttl * 1000),
    }
    parameters = inspect.signature(fn).parameters
    if template is None:
        if parameters:
            raise RegistrationError(
                f"{what}: a concrete resource takes no parameters (a template's "
                "{variables} would fill them)"
            )
        return ResourceDefinition(uri=uri, size=size, **common)

    try:
        params = string_parameters(fn, param_docs)
    except SchemaError as exc:
        raise RegistrationError(f"{what}: {exc}") from exc
    names = [param.name for param in params]
    if set(names) != set(template.variables):
        missing = sorted(set(template.variables) - set(names))
        extra = sorted(set(names) - set(template.variables))
        problems = []
        if missing:
            problems.append(f"no parameter for {missing}")
        if extra:
            problems.append(f"parameters {extra} are not template variables")
        raise RegistrationError(
            f"{what}: the function's parameters must be the template's variables: "
            + "; ".join(problems)
        )
    by_name = {param.name: param for param in params}
    completers: dict[str, CompletionSource] = {}
    for param in params:
        if param.choices is not None:
            completers[param.name] = choices_source(param.choices)
    for key, spec in (complete or {}).items():
        if key not in by_name:
            raise RegistrationError(f"{what}: complete= names {key!r}, which is no variable")
        completers[key] = source_from(spec, what=f"{what}: complete[{key!r}]")
    return ResourceTemplateDefinition(
        uri_template=uri,
        template=template,
        params=by_name,
        completers=completers,
        **common,
    )


class ResourceRegistry:
    """Thread-safe registry of concrete resources (by URI) and templates (by template)."""

    def __init__(self) -> None:
        self._resources: dict[str, ResourceDefinition] = {}
        self._templates: dict[str, ResourceTemplateDefinition] = {}
        self._lock = threading.Lock()
        # Bumped by every change, so a digest of the lists knows it is stale.
        self._version = 0

    def register(self, definition: ResourceDefinition | ResourceTemplateDefinition) -> None:
        """Add a resource or template; refuses one registered already."""
        with self._lock:
            if isinstance(definition, ResourceTemplateDefinition):
                if definition.uri_template in self._templates:
                    raise RegistrationError(
                        f"a resource template {definition.uri_template!r} is already registered"
                    )
                self._templates[definition.uri_template] = definition
            else:
                if definition.uri in self._resources:
                    raise RegistrationError(f"a resource {definition.uri!r} is already registered")
                self._resources[definition.uri] = definition
            self._version += 1

    def unregister(self, uri: str) -> ResourceDefinition | ResourceTemplateDefinition:
        """Remove and return the resource or template registered under *uri*."""
        with self._lock:
            removed: ResourceDefinition | ResourceTemplateDefinition | None
            removed = self._resources.pop(uri, None)
            if removed is None:
                removed = self._templates.pop(uri, None)
            if removed is None:
                raise RegistrationError(f"no resource or template {uri!r} is registered")
            self._version += 1
            return removed

    @property
    def version(self) -> int:
        """How many changes the registry has seen."""
        with self._lock:
            return self._version

    def get(self, uri: str) -> ResourceDefinition | None:
        """The concrete resource at *uri*, or ``None``."""
        with self._lock:
            return self._resources.get(uri)

    def get_template(self, template: str) -> ResourceTemplateDefinition | None:
        """The template registered as *template*, or ``None``."""
        with self._lock:
            return self._templates.get(template)

    def templates_by_specificity(self) -> list[ResourceTemplateDefinition]:
        """Templates in the order they are tried: most literal characters first, then by text."""
        with self._lock:
            templates = list(self._templates.values())
        return sorted(templates, key=lambda t: (-t.template.literal_length, t.uri_template))

    def snapshot(
        self,
    ) -> tuple[int, list[ResourceDefinition], list[ResourceTemplateDefinition]]:
        """The version, the resources by URI and the templates by template, together."""
        with self._lock:
            return (
                self._version,
                sorted(self._resources.values(), key=lambda r: r.uri),
                sorted(self._templates.values(), key=lambda t: t.uri_template),
            )

    def list_resources(self) -> list[ResourceDefinition]:
        """Concrete resources sorted by URI."""
        return self.snapshot()[1]

    def list_templates(self) -> list[ResourceTemplateDefinition]:
        """Templates sorted by template."""
        return self.snapshot()[2]

    def __bool__(self) -> bool:
        with self._lock:
            return bool(self._resources or self._templates)


def safe_path(root: str | Path, untrusted: str) -> Path:
    """``root / untrusted``, resolved, but only when it stays inside *root*.

    Symlinks are followed before the check, so a link pointing out of the
    folder is refused too.  Absolute paths, drive letters and UNC prefixes,
    backslashes and NUL are refused before anything is resolved.  Use it in
    any resource that turns a URI value into a file path::

        @server.resource("docs://guides/{+path}")
        def guide(path: str) -> str | None:
            file = safe_path(GUIDES, path)
            return file.read_text("utf-8") if file.is_file() else None

    Raises:
        ResourceNotFoundError: The path would leave *root*: the client is
            told the resource does not exist.
    """
    if not isinstance(untrusted, str) or not untrusted:
        raise ResourceNotFoundError()
    if "\x00" in untrusted or "\\" in untrusted:
        raise ResourceNotFoundError()
    if PurePosixPath(untrusted).is_absolute() or PureWindowsPath(untrusted).anchor:
        raise ResourceNotFoundError()
    base = Path(root).resolve()
    try:
        target = (base / untrusted).resolve()
    except (OSError, ValueError, RuntimeError):
        raise ResourceNotFoundError() from None
    if not target.is_relative_to(base):
        raise ResourceNotFoundError()
    return target
