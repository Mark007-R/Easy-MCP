"""Schema generation, docstring parsing, and argument validation."""

from __future__ import annotations

import typing

import pytest

from easy_mcp.exceptions import SchemaError, ValidationError
from easy_mcp.schema import (
    annotation_to_schema,
    build_input_schema,
    build_output_schema,
    parse_docstring,
    validate_arguments,
    validate_result,
)

# ------------------------------------------------------- annotation mapping


def test_scalar_mapping() -> None:
    assert annotation_to_schema(int) == {"type": "integer"}
    assert annotation_to_schema(float) == {"type": "number"}
    assert annotation_to_schema(str) == {"type": "string"}
    assert annotation_to_schema(bool) == {"type": "boolean"}


def test_container_mapping() -> None:
    assert annotation_to_schema(list) == {"type": "array"}
    assert annotation_to_schema(dict) == {"type": "object"}
    assert annotation_to_schema(list[int]) == {"type": "array", "items": {"type": "integer"}}
    assert annotation_to_schema(dict[str, float]) == {
        "type": "object",
        "additionalProperties": {"type": "number"},
    }


def test_optional_and_union() -> None:
    assert annotation_to_schema(int | None) == {
        "anyOf": [{"type": "integer"}, {"type": "null"}]
    }
    # The legacy Optional[...] spelling must keep working — tested on purpose.
    assert annotation_to_schema(typing.Optional[str]) == {  # noqa: UP045
        "anyOf": [{"type": "string"}, {"type": "null"}]
    }


def test_literal() -> None:
    assert annotation_to_schema(typing.Literal["asc", "desc"]) == {"enum": ["asc", "desc"]}


def test_any_accepts_everything() -> None:
    assert annotation_to_schema(typing.Any) == {}


def test_unsupported_annotations_rejected() -> None:
    with pytest.raises(SchemaError):
        annotation_to_schema(set)
    with pytest.raises(SchemaError):
        annotation_to_schema(dict[int, str])  # non-string keys
    with pytest.raises(SchemaError):
        annotation_to_schema(tuple[int, str])


def test_annotated_carries_a_description() -> None:
    assert annotation_to_schema(typing.Annotated[int, "How many rows"]) == {
        "type": "integer",
        "description": "How many rows",
    }
    # The wrapped type is still fully understood, not flattened to Any.
    assert annotation_to_schema(typing.Annotated[list[str], "Tags"]) == {
        "type": "array",
        "items": {"type": "string"},
        "description": "Tags",
    }
    assert annotation_to_schema(typing.Annotated[typing.Literal["asc", "desc"], "Order"]) == {
        "enum": ["asc", "desc"],
        "description": "Order",
    }


def test_annotated_nested_inside_a_container() -> None:
    assert annotation_to_schema(list[typing.Annotated[int, "A row id"]]) == {
        "type": "array",
        "items": {"type": "integer", "description": "A row id"},
    }


def test_annotated_ignores_non_string_metadata() -> None:
    # Markers meant for other libraries must not become the description.
    marker = object()
    assert annotation_to_schema(typing.Annotated[int, marker]) == {"type": "integer"}
    # With several pieces of metadata, the first string wins.
    assert annotation_to_schema(typing.Annotated[int, marker, "Real one", "Second"]) == {
        "type": "integer",
        "description": "Real one",
    }


def test_annotated_still_rejects_unsupported_types() -> None:
    with pytest.raises(SchemaError):
        annotation_to_schema(typing.Annotated[set, "Nope"])


# ----------------------------------------------------------- input schemas


def test_build_input_schema_required_and_defaults() -> None:
    def fn(a: int, b: str = "x") -> str:
        return b * a

    schema = build_input_schema(fn)
    assert schema["type"] == "object"
    assert schema["required"] == ["a"]
    assert schema["properties"]["a"] == {"type": "integer"}
    assert schema["properties"]["b"] == {"type": "string", "default": "x"}
    assert schema["additionalProperties"] is False


def test_build_input_schema_rejects_missing_annotation() -> None:
    def fn(a):  # type: ignore[no-untyped-def]
        return a

    with pytest.raises(SchemaError, match="missing a type annotation"):
        build_input_schema(fn)


def test_build_input_schema_rejects_var_args() -> None:
    def fn(*args: int) -> int:
        return sum(args)

    with pytest.raises(SchemaError):
        build_input_schema(fn)


def test_param_descriptions_from_docstring() -> None:
    def fn(a: int, b: int) -> int:
        """Add two numbers.

        Args:
            a: First operand.
            b: Second operand,
                possibly long.
        """
        return a + b

    summary, params = parse_docstring(fn.__doc__)
    assert summary == "Add two numbers."
    assert params == {"a": "First operand.", "b": "Second operand, possibly long."}

    schema = build_input_schema(fn, params)
    assert schema["properties"]["a"]["description"] == "First operand."


def test_param_descriptions_from_annotated() -> None:
    def fn(limit: typing.Annotated[int, "How many rows to return"]) -> int:
        return limit

    schema = build_input_schema(fn)
    assert schema["properties"]["limit"] == {
        "type": "integer",
        "description": "How many rows to return",
    }


def test_annotated_description_beats_the_docstring() -> None:
    def fn(a: typing.Annotated[int, "From the annotation"], b: int = 2) -> int:
        """Add two numbers.

        Args:
            a: From the docstring.
            b: Also from the docstring.
        """
        return a + b

    _, params = parse_docstring(fn.__doc__)
    schema = build_input_schema(fn, params)
    # The annotation travels with the parameter, so it wins...
    assert schema["properties"]["a"]["description"] == "From the annotation"
    # ...but the docstring still documents every parameter without one.
    assert schema["properties"]["b"]["description"] == "Also from the docstring."
    assert schema["properties"]["b"]["default"] == 2


def test_annotated_arguments_validate_as_their_base_type() -> None:
    def fn(limit: typing.Annotated[int, "How many rows"]) -> int:
        return limit

    schema = build_input_schema(fn)
    assert validate_arguments({"limit": 5}, schema) == {"limit": 5}
    with pytest.raises(ValidationError):
        validate_arguments({"limit": "five"}, schema)


def test_parse_docstring_empty() -> None:
    assert parse_docstring(None) == ("", {})
    assert parse_docstring("") == ("", {})


# ---------------------------------------------------------- output schemas


def test_output_schema_from_an_object_return() -> None:
    def fn() -> dict[str, int]:
        return {}

    assert build_output_schema(fn) == {
        "type": "object",
        "additionalProperties": {"type": "integer"},
    }


def test_output_schema_only_for_objects() -> None:
    # structuredContent is a JSON object, so nothing else earns a schema.
    def returns_str() -> str:
        return ""

    def returns_list() -> list[int]:
        return []

    assert build_output_schema(returns_str) is None
    assert build_output_schema(returns_list) is None


def test_output_schema_absent_rather_than_fatal() -> None:
    # Return types were never validated before, so an unannotated or
    # unsupported one must not stop a tool registering.
    def unannotated():  # type: ignore[no-untyped-def]
        return {}

    def unsupported() -> set:
        return set()

    assert build_output_schema(unannotated) is None
    assert build_output_schema(unsupported) is None


def test_validate_result_accepts_and_normalizes() -> None:
    schema = {"type": "object", "additionalProperties": {"type": "integer"}}
    assert validate_result({"a": 1}, schema) == {"a": 1}
    # Same JSON Schema rule as arguments: 3.0 is a valid integer.
    assert validate_result({"a": 3.0}, schema) == {"a": 3}


def test_validate_result_rejects_a_mismatch() -> None:
    schema = {"type": "object", "additionalProperties": {"type": "integer"}}
    with pytest.raises(ValidationError) as excinfo:
        validate_result([1, 2], schema)
    assert "result" in str(excinfo.value)


# -------------------------------------------------------------- validation


def _schema_for(fn):  # type: ignore[no-untyped-def]
    return build_input_schema(fn)


def test_validate_ok() -> None:
    def fn(a: int, b: str = "x") -> None: ...

    validate_arguments({"a": 1}, _schema_for(fn))
    validate_arguments({"a": 1, "b": "y"}, _schema_for(fn))


def test_validate_missing_required() -> None:
    def fn(a: int) -> None: ...

    with pytest.raises(ValidationError, match="missing required"):
        validate_arguments({}, _schema_for(fn))


def test_validate_unknown_field_rejected() -> None:
    def fn(a: int) -> None: ...

    with pytest.raises(ValidationError, match="unexpected parameter"):
        validate_arguments({"a": 1, "sneaky": True}, _schema_for(fn))


def test_validate_wrong_type() -> None:
    def fn(a: int) -> None: ...

    with pytest.raises(ValidationError, match="expected integer"):
        validate_arguments({"a": "1"}, _schema_for(fn))


def test_validate_bool_is_not_integer() -> None:
    def fn(a: int) -> None: ...

    # bool subclasses int in Python; JSON treats them as distinct types.
    with pytest.raises(ValidationError):
        validate_arguments({"a": True}, _schema_for(fn))


def test_validate_int_accepted_as_number() -> None:
    def fn(x: float) -> None: ...

    validate_arguments({"x": 3}, _schema_for(fn))
    validate_arguments({"x": 3.5}, _schema_for(fn))


def test_validate_nested_list_items() -> None:
    def fn(xs: list[int]) -> None: ...

    validate_arguments({"xs": [1, 2, 3]}, _schema_for(fn))
    with pytest.raises(ValidationError, match=r"xs\[1\]"):
        validate_arguments({"xs": [1, "two"]}, _schema_for(fn))


def test_validate_optional_null() -> None:
    def fn(x: int | None = None) -> None: ...

    validate_arguments({"x": None}, _schema_for(fn))
    validate_arguments({"x": 5}, _schema_for(fn))
    with pytest.raises(ValidationError):
        validate_arguments({"x": "no"}, _schema_for(fn))


def test_validate_dict_values() -> None:
    def fn(m: dict[str, int]) -> None: ...

    validate_arguments({"m": {"a": 1}}, _schema_for(fn))
    with pytest.raises(ValidationError):
        validate_arguments({"m": {"a": "1"}}, _schema_for(fn))


def test_validate_literal_enum() -> None:
    def fn(order: typing.Literal["asc", "desc"]) -> None: ...

    validate_arguments({"order": "asc"}, _schema_for(fn))
    with pytest.raises(ValidationError, match="must be one of"):
        validate_arguments({"order": "up"}, _schema_for(fn))


def test_validate_collects_multiple_errors() -> None:
    def fn(a: int, b: str) -> None: ...

    with pytest.raises(ValidationError) as excinfo:
        validate_arguments({"a": "x", "extra": 1}, _schema_for(fn))
    assert len(excinfo.value.errors) == 3  # wrong type + missing b + unexpected extra


def test_validate_integral_float_accepted_and_normalized() -> None:
    def fn(n: int, xs: list[int], m: dict[str, int], o: int | None = None) -> None: ...

    schema = _schema_for(fn)
    arguments = {"n": 3.0, "xs": [1, 2.0], "m": {"k": 4.0}, "o": 5.0}
    normalized = validate_arguments(arguments, schema)
    assert normalized == {"n": 3, "xs": [1, 2], "m": {"k": 4}, "o": 5}
    for value in (normalized["n"], normalized["xs"][1], normalized["m"]["k"], normalized["o"]):
        assert type(value) is int
    # The caller's mapping is left untouched.
    assert type(arguments["n"]) is float


def test_validate_fractional_float_rejected_for_integer() -> None:
    def fn(n: int) -> None: ...

    with pytest.raises(ValidationError, match="expected integer"):
        validate_arguments({"n": 3.5}, _schema_for(fn))
    with pytest.raises(ValidationError):
        validate_arguments({"n": float("inf")}, _schema_for(fn))
