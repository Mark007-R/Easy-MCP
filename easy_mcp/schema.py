"""Type-hint → JSON Schema conversion, docstring parsing, and validation.

Only a deliberate subset of JSON Schema is generated and validated —
``type`` (with strict bool/int separation), ``items``, ``properties`` /
``required`` / ``additionalProperties``, ``enum``, ``anyOf`` and
``description``.  Keeping the
validator small and hand-written means there is no third-party dependency in
the request path and its behavior is easy to audit.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import math
import re
import types
import typing
from collections.abc import Callable, Iterable, Mapping
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
        if is_pydantic_model(annotation):
            # Reachable only from inside another type, e.g. list[User]: a model
            # brings $defs, and hoisting those out of an arbitrary nesting depth
            # is not worth the ambiguity.  Wrapping it in a model is the fix.
            raise SchemaError(
                f"Pydantic model {annotation.__name__!r} must be a whole parameter or "
                "return type, not nested inside another type"
            )
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


def is_pydantic_model(annotation: Any) -> bool:
    """True for a Pydantic v2 model class.

    Deliberately duck-typed: easy_mcp never imports Pydantic, so projects that
    do not use it neither pay for the import nor have to install it.
    """
    return (
        isinstance(annotation, type)
        and hasattr(annotation, "model_json_schema")
        and hasattr(annotation, "model_validate")
        and hasattr(annotation, "model_dump")
    )


def model_schema(model: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """``(schema, defs)`` for a Pydantic model, with ``$defs`` split off.

    Pydantic writes ``{"$ref": "#/$defs/Address"}`` for a nested model, and
    that pointer resolves from the root of whatever document it lands in.  As
    one property of a tool's input schema the root is the *tool's* schema, so
    the definitions have to move up there for the pointers to still resolve.
    """
    schema = dict(model.model_json_schema())
    return schema, dict(schema.pop("$defs", {}))


def collect_param_models(fn: Callable[..., Any]) -> dict[str, Any]:
    """Parameters of *fn* annotated with a Pydantic model, by name."""
    try:
        hints = typing.get_type_hints(fn, include_extras=True)
    except Exception:
        return {}
    models: dict[str, Any] = {}
    for name, param in inspect.signature(fn).parameters.items():
        base, _ = _unwrap_annotated(hints.get(name, param.annotation))
        if is_pydantic_model(base):
            models[name] = base
    return models


def _pydantic_errors(prefix: str, exc: Exception) -> list[str]:
    """Pydantic's error list flattened into this package's message style."""
    details = getattr(exc, "errors", None)
    if not callable(details):
        return [f"{prefix}: {exc}"]
    messages = []
    for error in details():
        location = ".".join(str(part) for part in error.get("loc", ()))
        path = f"{prefix}.{location}" if location else prefix
        messages.append(f"{path}: {error.get('msg', 'invalid value')}")
    return messages or [f"{prefix}: invalid value"]


def build_validation_schema(
    input_schema: dict[str, Any], param_models: Mapping[str, Any]
) -> dict[str, Any]:
    """*input_schema* with every model-typed property loosened to a bare object.

    Clients are still shown the model's precise schema; this is only what the
    hand-written validator runs against.  Pydantic owns the inside of a model,
    and checking it here too would report a subset of Pydantic's findings and
    stop before Pydantic could report the rest -- so a client would see one
    error where there were three.
    """
    if not param_models:
        return input_schema
    properties = dict(input_schema.get("properties", {}))
    for name in param_models:
        prop = properties.get(name)
        if prop is None:
            continue
        loose: dict[str, Any] = {"type": "object"}
        if "description" in prop:
            loose["description"] = prop["description"]
        properties[name] = loose
    return {**input_schema, "properties": properties}


def build_param_models(
    models: Mapping[str, Any], arguments: dict[str, Any]
) -> dict[str, Any]:
    """Replace model-typed arguments with model instances.

    The hand-written validator has already confirmed each one is an object;
    Pydantic does the deep check, because re-implementing its constraints here
    would be a second, weaker copy of rules it already enforces.

    Raises:
        ValidationError: With Pydantic's own messages, in this package's shape.
    """
    if not models:
        return arguments
    built = dict(arguments)
    errors: list[str] = []
    for name, model in models.items():
        if name not in built:
            continue  # optional parameter left out: the default applies
        try:
            built[name] = model.model_validate(built[name])
        except Exception as exc:  # pydantic.ValidationError, which we cannot import
            errors.extend(_pydantic_errors(f"arguments.{name}", exc))
    if errors:
        raise ValidationError(errors)
    return built


def dump_model(model: Any, result: Any) -> Any:
    """A tool's return value as JSON-ready data, checked by its own model.

    Accepts an instance or anything the model can validate, so a tool may
    return a plain dict and still honour the schema it advertised.

    Raises:
        ValidationError: If the value does not fit the model.
    """
    try:
        return model.model_validate(result).model_dump(mode="json")
    except Exception as exc:
        raise ValidationError(
            _pydantic_errors("result", exc), message="Invalid tool result"
        ) from exc


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
    defs: dict[str, Any] = {}
    for name, param in signature.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise SchemaError("*args/**kwargs parameters are not supported")
        if param.kind is inspect.Parameter.POSITIONAL_ONLY:
            raise SchemaError("positional-only parameters are not supported")
        annotation = hints.get(name, param.annotation)
        base, annotated_description = _unwrap_annotated(annotation)
        model_description: str | None = None
        if is_pydantic_model(base):
            prop, model_defs = model_schema(base)
            for key, value in model_defs.items():
                if defs.setdefault(key, value) != value:
                    raise SchemaError(
                        f"parameter '{name}': two different models are named {key!r}"
                    )
            # A model's own docstring describes the type, not this parameter's
            # role, so it ranks below both other sources.
            model_description = prop.pop("description", None)
        else:
            try:
                prop = annotation_to_schema(annotation)
            except SchemaError as exc:
                raise SchemaError(f"parameter '{name}': {exc}") from exc
        description = annotated_description or param_docs.get(name) or model_description
        if description:
            prop = {**prop, "description": description}
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
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }
    if defs:
        # Hoisted out of the models above so their "#/$defs/..." pointers
        # resolve against this document, which is now their root.
        schema["$defs"] = defs
    return schema


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
    base, _ = _unwrap_annotated(annotation)
    if is_pydantic_model(base):
        # Here the model's schema is the whole document, so its own $defs stay
        # put and the pointers into them already resolve.
        return dict(base.model_json_schema())
    try:
        schema = annotation_to_schema(annotation)
    except SchemaError:
        return None
    return schema if schema.get("type") == "object" else None


def output_model(fn: Callable[..., Any]) -> Any | None:
    """The Pydantic model *fn* returns, or ``None``."""
    try:
        hints = typing.get_type_hints(fn, include_extras=True)
    except Exception:
        return None
    base, _ = _unwrap_annotated(hints.get("return", inspect.Parameter.empty))
    return base if is_pydantic_model(base) else None


# ----------------------------------------------- string parameters (prompts)
#
# Prompt arguments and URI template variables arrive as strings, so their
# parameters are limited to types with an exact, strict string form.

_STRING_TYPES_MESSAGE = (
    "prompt arguments and template variables arrive as strings; use str, int, float, "
    "bool or Literal"
)
_INTEGER = re.compile(r"[+-]?[0-9]+")
_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


class _Unconvertible(ValueError):
    """A string that is not a value of the parameter's type; the message says what was expected."""


@dataclasses.dataclass(frozen=True, slots=True)
class StringParameter:
    """One parameter whose value arrives as a string (a prompt argument, a template variable).

    Attributes:
        name: The parameter's name.
        description: From ``Annotated[T, "..."]`` or the docstring; ``None`` when absent.
        required: Whether it has no default.
        default: The default, when it has one.
        convert: Turns the wire string into the parameter's value, raising
            ``ValueError`` (whose message says what was expected) when it cannot.
        choices: The strings it accepts, for a ``Literal`` or ``bool``
            parameter (completed automatically); ``None`` otherwise.
        type_label: The parameter's type, for messages.
    """

    name: str
    description: str | None
    required: bool
    default: Any
    convert: Callable[[str], Any]
    choices: tuple[str, ...] | None
    type_label: str


def _convert_str(value: str) -> str:
    return value


def _convert_int(value: str) -> int:
    if _INTEGER.fullmatch(value) is None:
        raise _Unconvertible("expected integer")
    try:
        return int(value)
    except ValueError:  # beyond the interpreter's digit limit
        raise _Unconvertible("expected integer") from None


def _convert_float(value: str) -> float:
    if _NUMBER.fullmatch(value) is None:
        raise _Unconvertible("expected number")
    number = float(value)
    if not math.isfinite(number):
        raise _Unconvertible("expected a finite number")
    return number


def _convert_bool(value: str) -> bool:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise _Unconvertible("expected true or false")


def _literal_converter(members: tuple[Any, ...]) -> Callable[[str], Any]:
    by_text = {str(member): member for member in members}

    def convert(value: str) -> Any:
        try:
            return by_text[value]
        except KeyError:
            raise _Unconvertible(f"must be one of {list(by_text)!r}") from None

    return convert


def _string_type(annotation: Any) -> tuple[Callable[[str], Any], tuple[str, ...] | None, str]:
    """``(convert, choices, label)`` for one of the string-parameter types.

    Raises:
        SchemaError: Any other annotation.
    """
    if annotation is str:
        return _convert_str, None, "string"
    if annotation is bool:
        return _convert_bool, ("false", "true"), "boolean"
    if annotation is int:
        return _convert_int, None, "integer"
    if annotation is float:
        return _convert_float, None, "number"
    if typing.get_origin(annotation) is typing.Literal:
        members = typing.get_args(annotation)
        for member in members:
            if not isinstance(member, str | int | float | bool):
                raise SchemaError(f"Literal values must be str, int, float or bool, got {member!r}")
        choices = tuple(dict.fromkeys(str(member) for member in members))
        return _literal_converter(members), choices, "one of " + ", ".join(choices)
    raise SchemaError(_STRING_TYPES_MESSAGE)


def string_parameters(
    fn: Callable[..., Any], param_docs: Mapping[str, str] | None = None
) -> tuple[StringParameter, ...]:
    """The parameters of *fn*, each of a type that arrives as a string.

    Allowed: ``str``, ``int``, ``float``, ``bool``, ``Literal[...]`` of those,
    and ``T | None`` of them with a default, each optionally in
    ``Annotated[T, "description"]``.  A parameter without an annotation is
    a ``str``.

    Raises:
        SchemaError: Naming the parameter that is refused, and why.
    """
    param_docs = param_docs or {}
    try:
        hints = typing.get_type_hints(fn, include_extras=True)
    except Exception as exc:
        raise SchemaError(f"could not resolve type hints: {exc}") from exc
    result: list[StringParameter] = []
    for name, param in inspect.signature(fn).parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise SchemaError("*args/**kwargs parameters are not supported")
        if param.kind is inspect.Parameter.POSITIONAL_ONLY:
            raise SchemaError("positional-only parameters are not supported")
        annotation = hints.get(name, param.annotation)
        if annotation is inspect.Parameter.empty:
            annotation = str  # the value arrives as one, and nothing says to convert it
        base, description = _unwrap_annotated(annotation)
        has_default = param.default is not inspect.Parameter.empty
        origin = typing.get_origin(base)
        if origin is typing.Union or origin is types.UnionType:
            options = [arg for arg in typing.get_args(base) if arg is not type(None)]
            if len(options) != 1 or len(options) == len(typing.get_args(base)):
                raise SchemaError(f"parameter '{name}': {_STRING_TYPES_MESSAGE}")
            if not has_default:
                raise SchemaError(f"parameter '{name}': an optional parameter needs a default")
            base, inner_description = _unwrap_annotated(options[0])
            description = description or inner_description
        try:
            convert, choices, label = _string_type(base)
        except SchemaError as exc:
            raise SchemaError(f"parameter '{name}': {exc}") from exc
        result.append(
            StringParameter(
                name=name,
                description=description or param_docs.get(name) or None,
                required=not has_default,
                default=param.default if has_default else None,
                convert=convert,
                choices=choices,
                type_label=label,
            )
        )
    return tuple(result)


def bind_string_arguments(
    params: Iterable[StringParameter],
    raw: Mapping[str, Any],
    *,
    message: str = "Invalid prompt arguments",
) -> dict[str, Any]:
    """Convert string arguments to the values *params* take, checking every one.

    Every violation is reported together: a value that is not a string, an
    unknown key, a missing required argument, a value that does not convert.
    Arguments left out that have a default are left out (the default applies).

    Raises:
        ValidationError: ``-32602`` listing every violation, under *message*.
    """
    known = {param.name: param for param in params}
    errors: list[str] = []
    bound: dict[str, Any] = {}
    for key in raw:
        if not isinstance(key, str) or key not in known:
            errors.append(f"arguments.{key}: unexpected argument")
    for name, param in known.items():
        if name not in raw:
            if param.required:
                errors.append(f"arguments.{name}: missing required argument")
            continue
        value = raw[name]
        if not isinstance(value, str):
            errors.append(f"arguments.{name}: expected string, got {type(value).__name__}")
            continue
        try:
            bound[name] = param.convert(value)
        except ValueError as exc:
            errors.append(f"arguments.{name}: {exc}")
    if errors:
        raise ValidationError(errors, message=message)
    return bound


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
