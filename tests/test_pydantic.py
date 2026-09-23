"""Pydantic models as whole tool parameters and return types (optional extra)."""

from __future__ import annotations

import json
from typing import Annotated

import pytest
from conftest import make_context, rpc

from easy_mcp import MCPServer, ToolRegistrationError
from easy_mcp.exceptions import INVALID_PARAMS, SchemaError
from easy_mcp.schema import build_input_schema, is_pydantic_model

pydantic = pytest.importorskip("pydantic")
BaseModel = pydantic.BaseModel
Field = pydantic.Field


class Address(BaseModel):
    city: str
    zip_code: str | None = None


class User(BaseModel):
    """A person on file."""

    name: str = Field(min_length=1)
    age: int
    home: Address


class Saved(BaseModel):
    id: int
    label: str


class Shared(BaseModel):
    city: str


_SharedA = Shared


class Shared(BaseModel):  # noqa: F811  # as a second module would define it
    street: str


_SharedB = Shared


class HasSharedA(BaseModel):
    place: _SharedA


class HasSharedB(BaseModel):
    place: _SharedB


def tool_named(app: MCPServer, name: str):  # type: ignore[no-untyped-def]
    return next(definition for definition in app.tools if definition.name == name)


@pytest.fixture
def app() -> MCPServer:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def save_user(user: User, dry_run: bool = False) -> Saved:
        """Save a user.

        Args:
            user: The person to store.
        """
        return Saved(id=7, label=f"{user.name} of {user.home.city}")

    @server.tool
    def from_dict(user: User) -> Saved:
        """Returns a plain dict the model accepts."""
        return {"id": 1, "label": user.name}  # type: ignore[return-value]

    @server.tool
    def broken(user: User) -> Saved:
        """Returns something the model refuses."""
        return {"id": "not an int", "label": "x"}  # type: ignore[return-value]

    return server


def test_is_pydantic_model_is_duck_typed() -> None:
    assert is_pydantic_model(User)
    assert not is_pydantic_model(dict)
    assert not is_pydantic_model(User(name="a", age=1, home=Address(city="x")))


def test_model_parameter_schema_is_the_models_own(app: MCPServer) -> None:
    definition = tool_named(app, "save_user")
    user = definition.input_schema["properties"]["user"]
    # Pydantic's constraints survive, so a client sees the real contract.
    assert user["properties"]["name"]["minLength"] == 1
    assert user["properties"]["home"] == {"$ref": "#/$defs/Address"}
    assert definition.input_schema["required"] == ["user"]


def test_nested_model_defs_are_hoisted_so_refs_resolve(app: MCPServer) -> None:
    schema = tool_named(app, "save_user").input_schema
    # The $ref points at the root of the document it travels in, which is the
    # tool's input schema -- so the definitions have to live there.
    assert "Address" in schema["$defs"]
    assert schema["$defs"]["Address"]["required"] == ["city"]


def test_docstring_beats_the_models_own_description(app: MCPServer) -> None:
    user = tool_named(app, "save_user").input_schema["properties"]["user"]
    # "A person on file." describes the type; Args: describes this parameter.
    assert user["description"] == "The person to store."


def test_annotated_beats_everything() -> None:
    def fn(user: Annotated[User, "From the annotation"]) -> None:
        return None

    schema = build_input_schema(fn, {"user": "From the docstring"})
    assert schema["properties"]["user"]["description"] == "From the annotation"


def test_model_return_becomes_the_output_schema(app: MCPServer) -> None:
    definition = tool_named(app, "save_user")
    assert definition.output_schema is not None
    assert definition.output_schema["required"] == ["id", "label"]
    assert definition.output_model is Saved


def test_models_nested_in_other_types_are_refused() -> None:
    def fn(users: list[User]) -> None:
        return None

    with pytest.raises(SchemaError, match="whole parameter or return type"):
        build_input_schema(fn)


def test_two_models_sharing_a_name_are_refused() -> None:
    # Each model names its nested definition "Shared", and they disagree about
    # what that is -- hoisting both into one document would silently lose one.
    def fn(first: HasSharedA, second: HasSharedB) -> None:
        return None

    with pytest.raises(SchemaError, match="two different models are named"):
        build_input_schema(fn)


async def test_model_argument_arrives_as_an_instance(app: MCPServer) -> None:
    arguments = {"user": {"name": "Mark", "age": 30, "home": {"city": "Goa"}}}
    response = await app.dispatch(
        rpc("tools/call", {"name": "save_user", "arguments": arguments}), make_context()
    )
    result = response["result"]
    assert result["structuredContent"] == {"id": 7, "label": "Mark of Goa"}
    assert json.loads(result["content"][0]["text"]) == result["structuredContent"]


async def test_pydantic_reports_every_violation(app: MCPServer) -> None:
    arguments = {"user": {"name": "", "age": "x", "home": {}}}
    response = await app.dispatch(
        rpc("tools/call", {"name": "save_user", "arguments": arguments}), make_context()
    )
    assert response["error"]["code"] == INVALID_PARAMS
    errors = response["error"]["data"]["errors"]
    # All three, not just the first one the built-in validator would have hit.
    assert len(errors) == 3
    assert any("user.name" in error for error in errors)
    assert any("user.age" in error for error in errors)
    assert any("user.home.city" in error for error in errors)


async def test_unknown_top_level_argument_still_refused(app: MCPServer) -> None:
    arguments = {"user": {"name": "M", "age": 1, "home": {"city": "G"}}, "nope": 1}
    response = await app.dispatch(
        rpc("tools/call", {"name": "save_user", "arguments": arguments}), make_context()
    )
    assert response["error"]["code"] == INVALID_PARAMS
    assert "nope" in response["error"]["message"]


async def test_a_dict_the_model_accepts_is_fine(app: MCPServer) -> None:
    arguments = {"user": {"name": "Mark", "age": 30, "home": {"city": "Goa"}}}
    response = await app.dispatch(
        rpc("tools/call", {"name": "from_dict", "arguments": arguments}), make_context()
    )
    assert response["result"]["structuredContent"] == {"id": 1, "label": "Mark"}


async def test_a_result_the_model_refuses_is_an_error(app: MCPServer) -> None:
    arguments = {"user": {"name": "Mark", "age": 30, "home": {"city": "Goa"}}}
    response = await app.dispatch(
        rpc("tools/call", {"name": "broken", "arguments": arguments}), make_context()
    )
    result = response["result"]
    assert result["isError"] is True
    assert "output schema" in result["content"][0]["text"]


def test_model_output_schema_can_be_opted_out(server: MCPServer) -> None:
    @server.tool(output_schema={})
    def quiet(user: User) -> Saved:
        """No schema advertised."""
        return Saved(id=1, label="x")

    (definition,) = server.tools
    assert definition.output_schema is None
    assert definition.output_model is Saved  # still used to serialize


def test_non_object_output_schema_still_refused(server: MCPServer) -> None:
    with pytest.raises(ToolRegistrationError, match="must describe an object"):

        @server.tool(output_schema={"type": "string"})
        def bad(user: User) -> Saved:
            """Nope."""
            return Saved(id=1, label="x")
