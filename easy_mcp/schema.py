"""Type-hint → JSON Schema conversion, docstring parsing, and validation.

Only a deliberate subset of JSON Schema is generated and validated —
``type`` (with strict bool/int separation), ``items``, ``properties`` /
``required`` / ``additionalProperties``, ``enum``, ``anyOf`` and
``description``.  Keeping the
validator small and hand-written means there is no third-party dependency in
the request path and its behavior is easy to audit.
"""

from __future__ import annotations

import inspect
import json
import re
import types
import typing
from collections.abc import Callable
from typing import Any

from .exceptions import SchemaError, ValidationError

_SCALARS: dict[type, str] = {str: "string", int: "integer", float: "number", bool: "boolean"}

_SECTION_RE = re.compile(
    r"^(args|arguments|parameters|returns?|raises?|yields?|examples?|notes?|attributes)\s*:\s*$",
    re.IGNORECASE,
)
_PARAM_RE = re.compile(r"^\*{0,2}([A-Za-z_][A-Za-z0-9_]*)\s*(?:\([^)]*\))?\s*:\s*(.*)$")


def parse_docstring(doc: str | None) -> tuple[str, dict[str, str]]:
    """Split a docstring into ``(summary, {param_name: description})``.

    Understands Google-style ``Args:`` sections.  The summary is the first
    paragraph joined onto a single line — LLM clients choose tools by it.
    """
    if not doc:
        return "", {}
    lines = inspect.cleandoc(doc).splitlines()

    summary_parts: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or _SECTION_RE.match(stripped):
            break
        summary_parts.append(stripped)

    params: dict[str, str] = {}
    in_args = False
    current: str | None = None
    for line in lines:
        stripped = line.strip()
        section = _SECTION_RE.match(stripped)
        if section:
            in_args = section.group(1).lower() in ("args", "arguments", "parameters")
            current = None
            continue
        if not in_args:
            continue
        if not stripped:
            current = None
            continue
        match = _PARAM_RE.match(stripped)
        if match:
            current = match.group(1)
            params[current] = match.group(2).strip()
        elif current is not None:
            # Continuation line of the previous parameter's description.
            params[current] = f"{params[current]} {stripped}".strip()
    return " ".join(summary_parts), params


def _unwrap_annotated(annotation: Any) -> tuple[Any, str | None]:
    """Split ``Annotated[T, ...]`` into ``(T, description)``.

    The description is the first ``str`` in the metadata, so
    ``Annotated[int, "how many rows"]`` documents a parameter right where it is
    declared.  Non-string metadata (markers other libraries attach) is ignored,
    and a plain annotation comes back unchanged alongside ``None``.
    """
    metadata = getattr(annotation, "__metadata__", None)
    if metadata is None:
        return annotation, None
    description = next((item for item in metadata if isinstance(item, str)), None)
    return annotation.__origin__, description


def annotation_to_schema(annotation: Any) -> dict[str, Any]:
    """Convert a Python type annotation into a JSON Schema fragment.

    Supported: ``str``, ``int``, ``float``, ``bool``, ``list``/``list[T]``,
    ``dict``/``dict[str, T]``, ``Optional``/unions, ``Literal`` and ``Any`` --
    each optionally wrapped in ``Annotated[T, "description"]``, at any depth.

    Raises:
        SchemaError: For any annotation outside the supported set.
    """
    base, description = _unwrap_annotated(annotation)
    schema = _base_schema(base)
    if description:
        schema = {**schema, "description": description}
    return schema


def _base_schema(annotation: Any) -> dict[str, Any]:
    """``annotation_to_schema`` without the ``Annotated`` unwrapping."""
    if annotation is inspect.Parameter.empty:
        raise SchemaError("parameter is missing a type annotation")
    if annotation is None or annotation is type(None):
        return {"type": "null"}
    if annotation is Any:
        return {}
    origin = typing.get_origin(annotation)
    if origin is None:
        scalar = _SCALARS.get(annotation)
        if scalar is not None:
            return {"type": scalar}
        if annotation is list:
            return {"type": "array"}
        if annotation is dict:
            return {"type": "object"}
        raise SchemaError(f"unsupported type annotation: {annotation!r}")
    if origin is list:
        args = typing.get_args(annotation)
        if not args:
            return {"type": "array"}
        return {"type": "array", "items": annotation_to_schema(args[0])}
    if origin is dict:
        args = typing.get_args(annotation)
        if not args:
            return {"type": "object"}
        key_type, value_type = args
        if key_type is not str:
            raise SchemaError("dict keys must be str for JSON compatibility")
        return {"type": "object", "additionalProperties": annotation_to_schema(value_type)}
    if origin is typing.Union or origin is types.UnionType:
        return {"anyOf": [annotation_to_schema(arg) for arg in typing.get_args(annotation)]}
    if origin is typing.Literal:
        values = list(typing.get_args(annotation))
        for value in values:
            if not isinstance(value, (str, int, float, bool)):
                raise SchemaError(f"Literal values must be JSON scalars, got {value!r}")
        return {"enum": values}
    raise SchemaError(f"unsupported type annotation: {annotation!r}")


def build_input_schema(
    fn: Callable[..., Any], param_docs: dict[str, str] | None = None
) -> dict[str, Any]:
    """Build a strict JSON Schema object for *fn*'s parameters.

    Every parameter must have a supported type annotation.  ``*args`` /
    ``**kwargs`` and positional-only parameters are rejected because tool
    arguments arrive as a JSON object and are passed by keyword.

    A parameter's description comes from ``Annotated[T, "..."]`` when it has
    one, and otherwise from *param_docs* (the docstring's ``Args:`` section).
    The annotation wins because it travels with the parameter -- a docstring
    entry silently stops applying the moment the parameter is renamed.
    """
    param_docs = param_docs or {}
    signature = inspect.signature(fn)
    try:
        # include_extras keeps Annotated metadata: without it the parameter
        # descriptions are silently stripped before they can be read.
        hints = typing.get_type_hints(fn, include_extras=True)
    except Exception as exc:  # unresolvable forward references etc.
        raise SchemaError(f"could not resolve type hints: {exc}") from exc

    properties: dict[str, Any] = {}
    required: list[str] = []
    for name, param in signature.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise SchemaError("*args/**kwargs parameters are not supported")
        if param.kind is inspect.Parameter.POSITIONAL_ONLY:
            raise SchemaError("positional-only parameters are not supported")
        annotation = hints.get(name, param.annotation)
        try:
            prop = annotation_to_schema(annotation)
        except SchemaError as exc:
            raise SchemaError(f"parameter '{name}': {exc}") from exc
        if "description" not in prop and name in param_docs:
            prop = {**prop, "description": param_docs[name]}
        if param.default is inspect.Parameter.empty:
            required.append(name)
        else:
            try:
                json.dumps(param.default)
            except (TypeError, ValueError):
                pass  # non-JSON default: still optional, just not advertised
            else:
                prop = {**prop, "default": param.default}
        properties[name] = prop

    # additionalProperties: false makes unknown fields a hard error — clients
    # cannot smuggle unexpected arguments into a tool call.
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def build_output_schema(fn: Callable[..., Any]) -> dict[str, Any] | None:
    """The JSON Schema for *fn*'s return value, or ``None`` when it has none.

    MCP carries structured results in ``structuredContent``, which is a JSON
    *object*, so only a return annotation that maps to an object earns an
    ``outputSchema`` -- ``dict[str, int]`` does, ``str`` and ``list[int]`` do
    not and keep their text-only result.

    A missing or unsupported return annotation is not an error here.  Return
    types were never validated before, so raising would unregister tools that
    have worked since 0.1; they simply go without an output schema.
    """
    try:
        hints = typing.get_type_hints(fn, include_extras=True)
    except Exception:
        return None
    annotation = hints.get("return", inspect.Parameter.empty)
    if annotation is inspect.Parameter.empty:
        return None
    try:
        schema = annotation_to_schema(annotation)
    except SchemaError:
        return None
    return schema if schema.get("type") == "object" else None


def validate_result(result: Any, schema: dict[str, Any]) -> Any:
    """Validate a tool's return value against its output schema.

    The MCP spec is emphatic that a server "MUST provide structured results
    that conform to this schema", so this runs before the value is sent.

    Raises:
        ValidationError: Listing every violation found.
    """
    errors = _check(result, schema, "result")
    if errors:
        raise ValidationError(errors, message="Invalid tool result")
    return _normalize(result, schema)


def validate_arguments(arguments: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    """Validate *arguments* against *schema* and return them normalized.

    Normalization follows JSON Schema semantics: a float with no fractional
    part (``3.0``) is a valid ``integer`` and comes back as ``int`` so the
    tool receives the Python type its annotation promises.  Nothing else is
    coerced, and the input is never modified in place.

    Raises:
        ValidationError: Listing every violation found (not just the first).
    """
    errors = _check(arguments, schema, "arguments")
    if errors:
        raise ValidationError(errors)
    normalized = _normalize(arguments, schema)
    assert isinstance(normalized, dict)
    return normalized


def _type_ok(value: Any, expected: str) -> bool:
    # bool is a subclass of int in Python, but JSON treats them as distinct
    # types — so booleans are rejected wherever numbers are expected.
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        # JSON Schema: a number with a zero fractional part is an integer, so
        # 3.0 is accepted (and normalized to 3 before the tool runs).
        if isinstance(value, float):
            return value.is_integer()
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "string":
        return isinstance(value, str)
    if expected == "null":
        return value is None
    return False


def _check(value: Any, schema: dict[str, Any], path: str) -> list[str]:
    if "enum" in schema:
        allowed = schema["enum"]
        if any(type(value) is type(option) and value == option for option in allowed):
            return []
        return [f"{path}: must be one of {allowed!r}"]
    if "anyOf" in schema:
        for option in schema["anyOf"]:
            if not _check(value, option, path):
                return []
        return [f"{path}: does not match any allowed type"]
    expected = schema.get("type")
    if expected is None:
        return []  # Any: accept everything
    if expected == "array":
        if not isinstance(value, list):
            return [f"{path}: expected array, got {type(value).__name__}"]
        items = schema.get("items")
        if not items:
            return []
        errors: list[str] = []
        for index, item in enumerate(value):
            errors.extend(_check(item, items, f"{path}[{index}]"))
        return errors
    if expected == "object":
        if not isinstance(value, dict):
            return [f"{path}: expected object, got {type(value).__name__}"]
        errors = []
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        for name in schema.get("required", []):
            if name not in value:
                errors.append(f"{path}.{name}: missing required parameter")
        for key, item in value.items():
            if not isinstance(key, str):
                errors.append(f"{path}: object keys must be strings")
                continue
            if key in properties:
                errors.extend(_check(item, properties[key], f"{path}.{key}"))
            elif additional is False:
                errors.append(f"{path}.{key}: unexpected parameter")
            elif isinstance(additional, dict):
                errors.extend(_check(item, additional, f"{path}.{key}"))
        return errors
    if not _type_ok(value, expected):
        return [f"{path}: expected {expected}, got {type(value).__name__}"]
    return []


def _normalize(value: Any, schema: dict[str, Any]) -> Any:
    """Return *value* (already validated against *schema*) with integral
    floats converted to ``int`` wherever the schema asks for an integer."""
    if "enum" in schema:
        return value
    if "anyOf" in schema:
        for option in schema["anyOf"]:
            if not _check(value, option, ""):
                return _normalize(value, option)
        return value
    expected = schema.get("type")
    if expected == "integer" and isinstance(value, float):
        return int(value)
    if expected == "array" and isinstance(value, list):
        items = schema.get("items")
        if not items:
            return value
        return [_normalize(item, items) for item in value]
    if expected == "object" and isinstance(value, dict):
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key in properties:
                result[key] = _normalize(item, properties[key])
            elif isinstance(additional, dict):
                result[key] = _normalize(item, additional)
            else:
                result[key] = item
        return result
    return value
