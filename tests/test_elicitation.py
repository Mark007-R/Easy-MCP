"""Asking the user a question from a tool: elicit(), both protocol eras, requestState."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable
from typing import Annotated, Any, Literal

import httpx
import pytest
from conftest import LogCapture, Pushed, make_context, modern, notification, rpc

import easy_mcp.elicitation
import easy_mcp.server
from easy_mcp import (
    ClientIdentity,
    Elicitation,
    ElicitationUnavailableError,
    MCPServer,
    ToolCall,
    ToolOutcome,
    can_elicit,
    elicit,
)
from easy_mcp.elicitation import (
    MAX_FIELDS,
    MAX_MESSAGE_LENGTH,
    MAX_QUESTIONS,
    StateSigner,
    _InvalidAnswer,
    build_requested_schema,
    call_digest,
    check_answer,
    question_key,
    supports_form,
)
from easy_mcp.exceptions import (
    INVALID_PARAMS,
    MISSING_REQUIRED_CLIENT_CAPABILITY,
    ProtocolError,
)

LiveServer = Callable[[Any], str]

CAPS = "io.modelcontextprotocol/clientCapabilities"
FORM = {"elicitation": {"form": {}}}
ACCEPT = {"Accept": "application/json, text/event-stream"}
SECRET = "state-secret-" + "s" * 20


def init(capabilities: dict[str, Any] | None = None) -> dict[str, Any]:
    return rpc(
        "initialize",
        {
            "protocolVersion": "2025-11-25",
            "capabilities": FORM if capabilities is None else capabilities,
            "clientInfo": {"name": "tests", "version": "1.0"},
        },
        "init",
    )


def stateless(name: str, arguments: dict[str, Any] | None = None, **params: Any) -> dict[str, Any]:
    """A stateless tools/call from a client that can be asked."""
    capabilities = params.pop("capabilities", FORM)
    body = {"name": name, "arguments": arguments or {}, **params}
    return modern("tools/call", body, **{CAPS: capabilities})


def booking_server(**kwargs: Any) -> tuple[MCPServer, list[str]]:
    server = MCPServer(port=0, rate_limit_per_minute=None, **kwargs)
    runs: list[str] = []

    @server.tool
    def book(restaurant: str) -> str:
        """Book a table."""
        runs.append("start")
        answer = elicit("For how many people?", {"guests": int})
        if not answer.accepted:
            return f"not booked: {answer.action}"
        runs.append("booked")
        return f"{restaurant} for {answer['guests']}"

    @server.tool
    def trip() -> str:
        """Plan a trip: two questions."""
        runs.append("start")
        where = elicit("Where to?", {"city": str})
        if not where.accepted:
            return "no trip"
        how = elicit(f"How do you get to {where['city']}?", {"by": Literal["train", "plane"]})
        if not how.accepted:
            return "no trip"
        return f"{where['city']} by {how['by']}"

    return server, runs


def asked(response: dict[str, Any] | None) -> tuple[str, dict[str, Any]]:
    """The key and params of the one question an InputRequiredResult asks."""
    assert response is not None and "result" in response, response
    result = response["result"]
    assert result["resultType"] == "input_required", result
    ((key, request),) = result["inputRequests"].items()
    assert request["method"] == "elicitation/create"
    return key, request["params"]


def text(response: dict[str, Any] | None) -> str:
    assert response is not None and "result" in response, response
    return str(response["result"]["content"][0]["text"])


# ------------------------------------------------------------- the schema


def test_fields_become_a_flat_form() -> None:
    schema = build_requested_schema(
        {
            "name": str,
            "guests": Annotated[int, "How many people"],
            "budget": float | None,
            "outside": bool,
            "day": Literal["today", "tomorrow"],
            "extras": list[Literal["cake", "candles"]],
            "email": {"type": "string", "format": "email"},
            "seat": {"type": "string", "default": "any"},
        }
    )
    assert schema == {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "guests": {"type": "integer", "description": "How many people"},
            "budget": {"type": "number"},
            "outside": {"type": "boolean"},
            "day": {"type": "string", "enum": ["today", "tomorrow"]},
            "extras": {
                "type": "array",
                "items": {"type": "string", "enum": ["cake", "candles"]},
            },
            "email": {"type": "string", "format": "email"},
            "seat": {"type": "string", "default": "any"},
        },
        "required": ["name", "guests", "outside", "day", "extras", "email"],
    }


def test_a_written_out_schema_is_checked_and_kept() -> None:
    written = {
        "type": "object",
        "properties": {
            "color": {"type": "string", "oneOf": [{"const": "#f00", "title": "Red"}]},
            "count": {"type": "integer", "minimum": 1},
        },
        "required": ["color"],
    }
    assert build_requested_schema(written) == written
    with pytest.raises(ValueError, match="no field"):
        build_requested_schema({**written, "required": ["size"]})


@pytest.mark.parametrize(
    "fields",
    [
        {"when": bytes},
        {"who": dict},
        {"many": list[int]},
        {"many": list},
        {"pick": Literal[1, 2]},
        {"either": int | str},
        {"": str},
        {7: str},
    ],
)
def test_types_without_a_form_field_are_refused(fields: Any) -> None:
    with pytest.raises(TypeError):
        build_requested_schema(fields)


def test_shapes_a_form_cannot_show_are_refused() -> None:
    with pytest.raises(ValueError, match="at least one field"):
        build_requested_schema({})
    with pytest.raises(ValueError, match=f"at most {MAX_FIELDS}"):
        build_requested_schema({f"f{index}": str for index in range(MAX_FIELDS + 1)})
    with pytest.raises(ValueError, match="not 'object'"):
        build_requested_schema({"address": {"type": "object"}})
    with pytest.raises(ValueError, match="must list its choices"):
        build_requested_schema({"tags": {"type": "array", "items": {"type": "string"}}})
    with pytest.raises(TypeError, match="mapping"):
        build_requested_schema(["name"])  # type: ignore[arg-type]


# ------------------------------------------------------------- the answer

SCHEMA = build_requested_schema(
    {
        "guests": {"type": "integer", "minimum": 1, "maximum": 12},
        "name": {"type": "string", "minLength": 2, "maxLength": 5},
        "day": Literal["today", "tomorrow"],
        "extras": {
            "type": "array",
            "items": {"anyOf": [{"const": "cake"}, {"const": "candles"}]},
            "maxItems": 1,
            "default": [],
        },
        "outside": bool | None,
        "budget": float | None,
    }
)


def test_an_accepted_answer_is_checked_and_trimmed() -> None:
    answer = check_answer(
        SCHEMA,
        {
            "action": "accept",
            "content": {"guests": 4.0, "name": "Ada", "day": "today", "unasked": "dropped"},
        },
    )
    assert answer.accepted and answer.action == "accept"
    assert answer.content == {"guests": 4, "name": "Ada", "day": "today"}
    assert answer["guests"] == 4 and type(answer["guests"]) is int
    assert answer.get("outside") is None and answer.get("outside", True) is True
    with pytest.raises(KeyError):
        answer["unasked"]
    assert "Ada" in repr(answer)


@pytest.mark.parametrize("action", ["decline", "cancel"])
def test_a_refusal_carries_no_content(action: str) -> None:
    answer = check_answer(SCHEMA, {"action": action, "content": {"guests": 4}})
    assert not answer.accepted and answer.action == action and answer.content == {}


@pytest.mark.parametrize(
    ("content", "problem"),
    [
        ({"name": "Ada", "day": "today"}, "'guests' is required"),
        ({"guests": 0, "name": "Ada", "day": "today"}, "at least 1"),
        ({"guests": 13, "name": "Ada", "day": "today"}, "at most 12"),
        ({"guests": 2.5, "name": "Ada", "day": "today"}, "must be an integer"),
        ({"guests": True, "name": "Ada", "day": "today"}, "must be an integer"),
        ({"guests": "4", "name": "Ada", "day": "today"}, "must be an integer"),
        ({"guests": 4, "name": "A", "day": "today"}, "at least 2 characters"),
        ({"guests": 4, "name": "Adalbert", "day": "today"}, "at most 5 characters"),
        ({"guests": 4, "name": 7, "day": "today"}, "'name' must be a string"),
        ({"guests": 4, "name": "Ada", "day": "friday"}, "one of the choices"),
        ({"guests": 4, "name": "Ada", "day": "today", "extras": ["flowers"]}, "only the choices"),
        ({"guests": 4, "name": "Ada", "day": "today", "extras": ["cake", "candles"]}, "at most 1"),
        ({"guests": 4, "name": "Ada", "day": "today", "extras": "cake"}, "list of strings"),
        ({"guests": 4, "name": "Ada", "day": "today", "outside": "yes"}, "must be a boolean"),
        ({"guests": 4, "name": "Ada", "day": "today", "budget": "low"}, "must be a number"),
        ({"guests": 4, "name": "Ada", "day": "today", "budget": float("nan")}, "must be a number"),
    ],
)
def test_content_that_does_not_fit_the_form_is_refused(
    content: dict[str, Any], problem: str
) -> None:
    with pytest.raises(_InvalidAnswer, match=problem):
        check_answer(SCHEMA, {"action": "accept", "content": content})


@pytest.mark.parametrize(
    "result",
    [
        None,
        "accept",
        {},
        {"action": "maybe"},
        {"action": "accept"},
        {"action": "accept", "content": []},
    ],
)
def test_what_is_no_elicitation_result_is_refused(result: Any) -> None:
    with pytest.raises(_InvalidAnswer):
        check_answer(SCHEMA, result)


def test_a_question_is_keyed_by_its_place_and_what_it_asks() -> None:
    schema = build_requested_schema({"guests": int})
    key = question_key(1, "How many?", schema)
    assert key == question_key(1, "How many?", build_requested_schema({"guests": int}))
    assert key.startswith("q1-") and len(key) == 13
    assert key != question_key(2, "How many?", schema)
    assert key != question_key(1, "How many people?", schema)
    assert key != question_key(1, "How many?", build_requested_schema({"guests": float}))


def test_supports_form() -> None:
    assert supports_form({"elicitation": {}})
    assert supports_form({"elicitation": {"form": {}, "url": {}}})
    assert not supports_form({"elicitation": {"url": {}}})
    assert not supports_form({"elicitation": True})
    assert not supports_form({})
    assert not supports_form(None)


def test_outside_a_tool_call_nobody_can_be_asked() -> None:
    assert can_elicit() is False
    with pytest.raises(ElicitationUnavailableError, match="only inside a tool call"):
        elicit("How many?", {"guests": int})
    # The question itself is checked first, so a mistake shows without a client.
    with pytest.raises(TypeError):
        elicit("How many?", {"guests": bytes})
    with pytest.raises(ValueError, match="needs a message"):
        elicit("  ", {"guests": int})
    with pytest.raises(ValueError, match=f"at most {MAX_MESSAGE_LENGTH}"):
        elicit("x" * (MAX_MESSAGE_LENGTH + 1), {"guests": int})


# ----------------------------------------------------------- requestState


def test_a_state_round_trips_for_its_own_call() -> None:
    signer = StateSigner(SECRET)
    answers = {"q1-abc": {"action": "accept", "content": {"city": "Zürich"}}}
    state = signer.sign(principal="alice", digest="d1", answers=answers)
    assert signer.verify(state, principal="alice", digest="d1") == answers
    # Any worker holding the secret verifies it; one without does not.
    assert StateSigner(SECRET).verify(state, principal="alice", digest="d1") == answers
    with pytest.raises(ProtocolError, match="Invalid requestState"):
        StateSigner().verify(state, principal="alice", digest="d1")


def test_a_state_is_bound_to_principal_call_and_time(monkeypatch: pytest.MonkeyPatch) -> None:
    signer = StateSigner(SECRET)
    state = signer.sign(principal="alice", digest="d1", answers={})
    for principal, digest in (("bob", "d1"), (None, "d1"), ("alice", "d2")):
        with pytest.raises(ProtocolError) as refused:
            signer.verify(state, principal=principal, digest=digest)
        assert refused.value.code == INVALID_PARAMS
        assert str(refused.value) == "Invalid requestState"
    later = time.time() + easy_mcp.elicitation.REQUEST_STATE_TTL_SECONDS + 5
    monkeypatch.setattr(easy_mcp.elicitation.time, "time", lambda: later)
    with pytest.raises(ProtocolError, match="Invalid requestState"):
        signer.verify(state, principal="alice", digest="d1")


def test_a_tampered_or_malformed_state_is_refused() -> None:
    signer = StateSigner(SECRET)
    state = signer.sign(principal=None, digest="d1", answers={"q": {"action": "decline"}})
    body, mac = state.split(".")
    forged = easy_mcp.elicitation._b64(
        json.dumps({"v": 1, "exp": 9999999999, "sub": None, "req": "d1", "ans": {}}).encode()
    )
    for bad in (
        forged + "." + mac,
        body + "." + mac[:-2] + ("AA" if not mac.endswith("AA") else "BB"),
        body,
        body + "." + mac + ".x",
        "",
        "é.é",
        7,
        None,
        ["a"],
    ):
        with pytest.raises(ProtocolError, match="Invalid requestState"):
            signer.verify(bad, principal=None, digest="d1")


def test_the_secret_comes_from_the_argument_or_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EASY_MCP_STATE_SECRET", raising=False)
    assert StateSigner().shared is False
    assert StateSigner(SECRET).shared is True
    assert StateSigner(SECRET.encode()).shared is True
    with pytest.raises(ValueError, match="at least 16 bytes"):
        StateSigner("short")
    monkeypatch.setenv("EASY_MCP_STATE_SECRET", SECRET)
    from_env = StateSigner()
    assert from_env.shared is True
    state = from_env.sign(principal=None, digest="d", answers={})
    assert StateSigner(SECRET).verify(state, principal=None, digest="d") == {}
    with pytest.raises(ValueError, match="at least 16 bytes"):
        MCPServer(state_secret="short")


def test_the_digest_covers_the_tool_and_its_arguments() -> None:
    assert call_digest("book", {"a": 1, "b": 2}) == call_digest("book", {"b": 2, "a": 1})
    assert call_digest("book", {"a": 1}) != call_digest("book", {"a": 2})
    assert call_digest("book", {"a": 1}) != call_digest("cook", {"a": 1})


# -------------------------------------------------------- stateless calls


async def test_a_stateless_call_asks_then_answers_on_the_retry() -> None:
    server, runs = booking_server()
    first = await server.dispatch(stateless("book", {"restaurant": "Luigi"}), make_context())
    key, params = asked(first)
    assert params == {
        "mode": "form",
        "message": "For how many people?",
        "requestedSchema": {
            "type": "object",
            "properties": {"guests": {"type": "integer"}},
            "required": ["guests"],
        },
    }
    assert first is not None
    # Nothing was answered before, so nothing needs carrying.
    assert "requestState" not in first["result"]
    assert "content" not in first["result"]
    assert runs == ["start"]
    retry = stateless(
        "book",
        {"restaurant": "Luigi"},
        inputResponses={key: {"action": "accept", "content": {"guests": 4}}},
    )
    second = await server.dispatch(retry, make_context())
    assert text(second) == "Luigi for 4"
    assert second is not None and second["result"]["resultType"] == "complete"
    assert runs == ["start", "start", "booked"]


@pytest.mark.parametrize("action", ["decline", "cancel"])
async def test_a_refusal_reaches_the_tool(action: str) -> None:
    server, _ = booking_server()
    key, _ = asked(await server.dispatch(stateless("book", {"restaurant": "L"}), make_context()))
    retry = stateless("book", {"restaurant": "L"}, inputResponses={key: {"action": action}})
    assert text(await server.dispatch(retry, make_context())) == f"not booked: {action}"


async def test_earlier_answers_travel_in_the_signed_state() -> None:
    server, runs = booking_server()
    first = await server.dispatch(stateless("trip"), make_context())
    key1, _ = asked(first)
    answer1 = {key1: {"action": "accept", "content": {"city": "Rome"}}}
    second = await server.dispatch(stateless("trip", inputResponses=answer1), make_context())
    key2, params2 = asked(second)
    assert params2["message"] == "How do you get to Rome?"
    assert key2.startswith("q2-") and key1.startswith("q1-")
    assert second is not None
    state = second["result"]["requestState"]
    # The retry brings only the new answer, and the state it was handed.
    answer2 = {key2: {"action": "accept", "content": {"by": "train"}}}
    third = await server.dispatch(
        stateless("trip", inputResponses=answer2, requestState=state), make_context()
    )
    assert text(third) == "Rome by train"
    assert runs == ["start"] * 3
    # Without the state, the first question has no answer: it is asked again.
    again = await server.dispatch(stateless("trip", inputResponses=answer2), make_context())
    assert asked(again)[0] == key1


async def test_only_the_answers_the_tool_read_are_carried() -> None:
    server, _ = booking_server()
    key1, _ = asked(await server.dispatch(stateless("trip"), make_context()))
    responses = {
        key1: {"action": "accept", "content": {"city": "Rome"}},
        "q9-injected": {"action": "accept", "content": {"admin": True}},
    }
    second = await server.dispatch(stateless("trip", inputResponses=responses), make_context())
    assert second is not None
    carried = server._state.verify(
        second["result"]["requestState"], principal=None, digest=call_digest("trip", {})
    )
    assert carried == {key1: responses[key1]}


async def test_a_state_for_another_call_or_caller_is_refused() -> None:
    server, runs = booking_server()
    alice = ClientIdentity(fingerprint="a" * 12, scopes=frozenset())
    bob = ClientIdentity(fingerprint="b" * 12, scopes=frozenset())
    key1, _ = asked(await server.dispatch(stateless("trip"), make_context(identity=alice)))
    answer1 = {key1: {"action": "accept", "content": {"city": "Rome"}}}
    second = await server.dispatch(
        stateless("trip", inputResponses=answer1), make_context(identity=alice)
    )
    assert second is not None
    state = second["result"]["requestState"]
    runs.clear()
    refusals = [
        await server.dispatch(stateless("trip", requestState=state), make_context(identity=bob)),
        await server.dispatch(stateless("trip", requestState=state), make_context()),
        await server.dispatch(
            stateless("book", {"restaurant": "L"}, requestState=state),
            make_context(identity=alice),
        ),
        await server.dispatch(
            stateless("trip", requestState=state + "x"), make_context(identity=alice)
        ),
        await server.dispatch(stateless("trip", requestState=7), make_context(identity=alice)),
    ]
    for refused in refusals:
        assert refused is not None
        assert refused["error"]["code"] == INVALID_PARAMS
        assert refused["error"]["message"] == "Invalid requestState"
    assert runs == []  # refused before the tool ran
    kept = await server.dispatch(
        stateless("trip", requestState=state), make_context(identity=alice)
    )
    assert asked(kept)[1]["message"] == "How do you get to Rome?"


async def test_a_state_signed_by_another_server_is_refused() -> None:
    server, _ = booking_server()
    other, _ = booking_server()
    key1, _ = asked(await other.dispatch(stateless("trip"), make_context()))
    answer1 = {key1: {"action": "accept", "content": {"city": "Rome"}}}
    second = await other.dispatch(stateless("trip", inputResponses=answer1), make_context())
    assert second is not None
    state = second["result"]["requestState"]
    refused = await server.dispatch(stateless("trip", requestState=state), make_context())
    assert refused is not None and refused["error"]["message"] == "Invalid requestState"
    # Two servers sharing a secret are two workers of one deployment.
    one, _ = booking_server(state_secret=SECRET)
    two, _ = booking_server(state_secret=SECRET)
    second = await one.dispatch(stateless("trip", inputResponses=answer1), make_context())
    assert second is not None
    served = await two.dispatch(
        stateless("trip", requestState=second["result"]["requestState"]), make_context()
    )
    assert asked(served)[1]["message"] == "How do you get to Rome?"


async def test_an_answer_that_does_not_fit_is_minus_32602() -> None:
    server, _ = booking_server()
    key, _ = asked(await server.dispatch(stateless("book", {"restaurant": "L"}), make_context()))
    for bad in ({"action": "accept", "content": {"guests": "four"}}, {"action": "accept"}, "yes"):
        retry = stateless("book", {"restaurant": "L"}, inputResponses={key: bad})
        refused = await server.dispatch(retry, make_context())
        assert refused is not None and refused["error"]["code"] == INVALID_PARAMS
        assert refused["error"]["message"].startswith("Invalid inputResponses: ")
    not_an_object = stateless("book", {"restaurant": "L"}, inputResponses=[key])
    refused = await server.dispatch(not_an_object, make_context())
    assert refused is not None and refused["error"]["code"] == INVALID_PARAMS
    assert "'inputResponses' must be an object" in refused["error"]["message"]


@pytest.mark.parametrize("capabilities", [{}, {"elicitation": {"url": {}}}, {"sampling": {}}])
async def test_a_client_that_cannot_be_asked_gets_minus_32021(
    capabilities: dict[str, Any],
) -> None:
    server, runs = booking_server()
    message = stateless("book", {"restaurant": "L"}, capabilities=capabilities)
    refused = await server.dispatch(message, make_context())
    assert refused is not None
    assert refused["error"]["code"] == MISSING_REQUIRED_CLIENT_CAPABILITY
    assert refused["error"]["data"] == {"requiredCapabilities": {"elicitation": {"form": {}}}}
    assert "needs to ask the user a question" in refused["error"]["message"]
    assert runs == ["start"]


async def test_a_tool_may_carry_on_without_asking() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def greet() -> str:
        """Greet, by name when the user can be asked for it."""
        if not can_elicit():
            return "Hello!"
        answer = elicit("Your name?", {"name": str})
        return f"Hello, {answer.get('name', 'you')}!"

    @server.tool
    def stubborn() -> str:
        """Ask anyway, and catch the refusal."""
        try:
            elicit("Your name?", {"name": str})
        except ElicitationUnavailableError:
            return "Hello, whoever you are!"
        return "unreachable"

    plain = await server.dispatch(stateless("greet", capabilities={}), make_context())
    assert text(plain) == "Hello!"
    caught = await server.dispatch(stateless("stubborn", capabilities={}), make_context())
    assert text(caught) == "Hello, whoever you are!"
    key, _ = asked(await server.dispatch(stateless("greet"), make_context()))
    named = stateless(
        "greet", inputResponses={key: {"action": "accept", "content": {"name": "Ada"}}}
    )
    assert text(await server.dispatch(named, make_context())) == "Hello, Ada!"


async def test_a_tools_own_except_exception_does_not_swallow_the_question() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    async def careful() -> str:
        """Catch everything, as many tools do."""
        try:
            answer = elicit("Sure?", {"sure": bool})
        except Exception:
            return "swallowed"
        return f"sure={answer.get('sure')}"

    key, params = asked(await server.dispatch(stateless("careful"), make_context()))
    assert params["message"] == "Sure?"
    retry = stateless(
        "careful", inputResponses={key: {"action": "accept", "content": {"sure": True}}}
    )
    assert text(await server.dispatch(retry, make_context())) == "sure=True"


async def test_asking_does_not_count_against_the_session_cap() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool(max_calls_per_session=1)
    def once() -> str:
        """Callable once, after one question."""
        answer = elicit("Go?", {"go": bool})
        return f"went: {answer.get('go')}"

    context = make_context()
    key, _ = asked(await server.dispatch(stateless("once"), context))
    retry = stateless("once", inputResponses={key: {"action": "accept", "content": {"go": True}}})
    assert text(await server.dispatch(retry, context)) == "went: True"
    spent = await server.dispatch(retry, context)
    assert spent is not None and "Session limit" in spent["error"]["message"]


async def test_middleware_sees_the_question_as_an_outcome(logs: LogCapture) -> None:
    server, _ = booking_server()
    seen: list[tuple[str, bool, bool]] = []

    @server.tool_middleware
    async def watch(call: ToolCall, call_next: Any) -> ToolOutcome:
        outcome: ToolOutcome = await call_next()
        seen.append((outcome.status, outcome.ok, outcome.is_error))
        return outcome

    key, _ = asked(await server.dispatch(stateless("book", {"restaurant": "L"}), make_context()))
    retry = stateless(
        "book",
        {"restaurant": "L"},
        inputResponses={key: {"action": "accept", "content": {"guests": 2}}},
    )
    await server.dispatch(retry, make_context())
    assert seen == [("input_required", False, False), ("ok", True, False)]
    statuses = [event["status"] for event in logs.events("tool_call")]
    assert statuses == ["input_required", "ok"]
    requested = logs.events("elicitation_requested")
    assert [event["question"] for event in requested] == [key]
    # What the user answered is never logged.
    assert "guests" not in json.dumps(logs.events("elicitation_requested"))


async def test_a_tool_that_asks_without_end_is_stopped() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool
    def nosy() -> str:
        """Ask more questions than a call may."""
        for index in range(MAX_QUESTIONS + 1):
            elicit(f"Question {index}?", {"answer": str})
        return "done"

    context = make_context()
    answers: dict[str, Any] = {}
    state: str | None = None
    for _ in range(MAX_QUESTIONS):
        params: dict[str, Any] = {"inputResponses": dict(answers)}
        if state is not None:
            params["requestState"] = state
        response = await server.dispatch(stateless("nosy", **params), context)
        key, _ = asked(response)
        assert response is not None
        state = response["result"].get("requestState", state)
        answers = {key: {"action": "accept", "content": {"answer": "x"}}}
    last = await server.dispatch(
        stateless("nosy", inputResponses=answers, requestState=state), context
    )
    assert last is not None and last["result"]["isError"] is True


async def test_other_methods_cannot_ask() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    seen: list[bool] = []

    @server.resource("notes://all")
    def notes() -> str:
        seen.append(can_elicit())
        return "notes"

    read = modern("resources/read", {"uri": "notes://all"}, **{CAPS: FORM})
    response = await server.dispatch(read, make_context())
    assert response is not None and "result" in response
    assert seen == [False]


# --------------------------------------------------------------- sessions


async def session(
    server: MCPServer, capabilities: dict[str, Any] | None = None
) -> tuple[Pushed, Any]:
    """An initialized session on a multiplexed channel, as stdio is."""
    pushed = Pushed()
    context = make_context(push=pushed, multiplexed=True)
    answer = await server.dispatch(init(capabilities), context)
    assert answer is not None and "result" in answer
    await server.dispatch(notification("notifications/initialized"), context)
    pushed.frames.clear()
    return pushed, context


async def question(pushed: Pushed, count: int = 1) -> dict[str, Any]:
    """The *count*-th elicitation/create request pushed to the client."""
    deadline = time.monotonic() + 5
    while True:
        requests = [f for f in pushed.frames if f.get("method") == "elicitation/create"]
        if len(requests) >= count:
            return requests[count - 1]
        assert time.monotonic() < deadline, pushed.frames
        await asyncio.sleep(0.005)


def reply(request: dict[str, Any], action: str = "accept", **content: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"action": action}
    if action == "accept":
        result["content"] = content
    return {"jsonrpc": "2.0", "id": request["id"], "result": result}


async def test_a_session_is_asked_on_its_channel_and_the_tool_runs_again() -> None:
    server, runs = booking_server()
    pushed, context = await session(server)
    arguments = {"name": "book", "arguments": {"restaurant": "Luigi"}}
    call = asyncio.create_task(server.dispatch(rpc("tools/call", arguments), context))
    request = await question(pushed)
    assert request["jsonrpc"] == "2.0" and request["id"].startswith("easy-mcp-")
    # No "mode": clients older than 2025-11-25 know form questions only.
    assert request["params"] == {
        "message": "For how many people?",
        "requestedSchema": {
            "type": "object",
            "properties": {"guests": {"type": "integer"}},
            "required": ["guests"],
        },
    }
    assert not call.done() and runs == ["start"]
    assert await server.dispatch(reply(request, guests=6), context) is None
    response = await asyncio.wait_for(call, 5)
    assert text(response) == "Luigi for 6"
    assert response is not None and "resultType" not in response["result"]
    assert runs == ["start", "start", "booked"]
    assert server._asked == {}


async def test_a_session_answers_two_questions_within_one_call() -> None:
    server, runs = booking_server()
    pushed, context = await session(server)
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "trip"}), context))
    first = await question(pushed, 1)
    await server.dispatch(reply(first, city="Oslo"), context)
    second = await question(pushed, 2)
    assert second["params"]["message"] == "How do you get to Oslo?"
    assert second["id"] != first["id"]
    await server.dispatch(reply(second, by="plane"), context)
    assert text(await asyncio.wait_for(call, 5)) == "Oslo by plane"
    assert runs == ["start"] * 3


async def test_a_sync_tools_arguments_are_fresh_on_every_run() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)
    seen: list[list[str]] = []

    @server.tool
    def pack(items: list[str]) -> str:
        """Add to the list, then ask."""
        seen.append(list(items))
        items.append("socks")
        answer = elicit("Anything else?", {"more": str})
        return ", ".join([*items, answer.get("more", "")])

    pushed, context = await session(server)
    arguments = {"name": "pack", "arguments": {"items": ["hat"]}}
    call = asyncio.create_task(server.dispatch(rpc("tools/call", arguments), context))
    await server.dispatch(reply(await question(pushed), more="scarf"), context)
    assert text(await asyncio.wait_for(call, 5)) == "hat, socks, scarf"
    assert seen == [["hat"], ["hat"]]


@pytest.mark.parametrize("capabilities", [{}, {"elicitation": {"url": {}}}])
async def test_a_session_that_declared_no_form_elicitation_is_not_asked(
    capabilities: dict[str, Any],
) -> None:
    server, _ = booking_server()
    pushed, context = await session(server, capabilities)
    arguments = {"name": "book", "arguments": {"restaurant": "L"}}
    response = await server.dispatch(rpc("tools/call", arguments), context)
    assert response is not None and response["result"]["isError"] is True
    assert "did not declare the elicitation capability" in text(response)
    assert pushed.frames == []


async def test_a_channel_that_carries_no_requests_is_not_asked() -> None:
    server, _ = booking_server()
    context = make_context()  # no push, no notify
    await server.dispatch(init(), context)
    arguments = {"name": "book", "arguments": {"restaurant": "L"}}
    response = await server.dispatch(rpc("tools/call", arguments), context)
    assert response is not None and response["result"]["isError"] is True
    assert "cannot carry a question" in text(response)


async def test_a_client_that_answers_with_an_error_ends_the_call(logs: LogCapture) -> None:
    server, _ = booking_server()
    pushed, context = await session(server)
    arguments = {"name": "book", "arguments": {"restaurant": "L"}}
    call = asyncio.create_task(server.dispatch(rpc("tools/call", arguments), context))
    request = await question(pushed)
    error = {"jsonrpc": "2.0", "id": request["id"], "error": {"code": -32601, "message": "no"}}
    await server.dispatch(error, context)
    response = await asyncio.wait_for(call, 5)
    assert response is not None and response["result"]["isError"] is True
    assert "refused the question (code -32601)" in text(response)
    assert [event["action"] for event in logs.events("elicitation_answered")] == ["error"]


async def test_a_session_answer_that_does_not_fit_is_an_error_result() -> None:
    server, _ = booking_server()
    pushed, context = await session(server)
    arguments = {"name": "book", "arguments": {"restaurant": "L"}}
    call = asyncio.create_task(server.dispatch(rpc("tools/call", arguments), context))
    await server.dispatch(reply(await question(pushed), guests="many"), context)
    response = await asyncio.wait_for(call, 5)
    assert response is not None and response["result"]["isError"] is True
    assert "did not fit the question: 'guests' must be an integer" in text(response)


async def test_a_question_nobody_answers_counts_as_dismissed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(easy_mcp.server, "ELICITATION_TIMEOUT_SECONDS", 0.05)
    server, _ = booking_server()
    pushed, context = await session(server)
    arguments = {"name": "book", "arguments": {"restaurant": "L"}}
    response = await server.dispatch(rpc("tools/call", arguments), context)
    assert text(response) == "not booked: cancel"
    request = await question(pushed)
    cancelled = [f for f in pushed.frames if f.get("method") == "notifications/cancelled"]
    assert [frame["params"]["requestId"] for frame in cancelled] == [request["id"]]
    assert server._asked == {}
    # An answer that comes too late is nobody's.
    assert await server.dispatch(reply(request, guests=2), context) is None


async def test_cancelling_the_call_takes_the_question_back() -> None:
    server, runs = booking_server()
    pushed, context = await session(server)
    arguments = {"name": "book", "arguments": {"restaurant": "L"}}
    call = asyncio.create_task(server.dispatch(rpc("tools/call", arguments, 9), context))
    request = await question(pushed)
    await server.dispatch(notification("notifications/cancelled", {"requestId": 9}), context)
    assert await asyncio.wait_for(call, 5) is None
    cancelled = [f for f in pushed.frames if f.get("method") == "notifications/cancelled"]
    assert [frame["params"]["requestId"] for frame in cancelled] == [request["id"]]
    assert server._asked == {} and runs == ["start"]


async def test_only_the_session_that_was_asked_can_answer() -> None:
    server, _ = booking_server()
    pushed, context = await session(server)
    _, other = await session(server)
    other.session_id = "another-session"
    arguments = {"name": "book", "arguments": {"restaurant": "L"}}
    call = asyncio.create_task(server.dispatch(rpc("tools/call", arguments), context))
    request = await question(pushed)
    assert await server.dispatch(reply(request, guests=9), other) is None
    await asyncio.sleep(0.05)
    assert not call.done()
    await server.dispatch(reply(request, guests=2), context)
    assert text(await asyncio.wait_for(call, 5)) == "L for 2"


async def test_a_response_nobody_waits_for_is_dropped_quietly() -> None:
    server, _ = booking_server()
    _, context = await session(server)
    for message in (
        {"jsonrpc": "2.0", "id": "easy-mcp-unknown", "result": {"action": "accept"}},
        {"jsonrpc": "2.0", "id": 4, "result": {}},
        {"jsonrpc": "2.0", "id": None, "error": {"code": -1, "message": "x"}},
        {"jsonrpc": "2.0", "id": ["a"], "result": {}},
    ):
        assert await server.dispatch(message, context) is None


async def test_the_tools_timeout_does_not_run_while_the_user_thinks() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)

    @server.tool(timeout=0.2)
    def quick() -> str:
        """A short timeout, a slow user."""
        return f"ok: {elicit('Go?', {'go': bool}).get('go')}"

    pushed, context = await session(server)
    call = asyncio.create_task(server.dispatch(rpc("tools/call", {"name": "quick"}), context))
    request = await question(pushed)
    await asyncio.sleep(0.4)  # twice the tool's timeout
    await server.dispatch(reply(request, go=True), context)
    assert text(await asyncio.wait_for(call, 5)) == "ok: True"


# -------------------------------------------------------------------- HTTP


def read_until(lines: Any, wanted: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
    for line in lines:
        if line.startswith("data: "):
            event = json.loads(line[len("data: ") :])
            if wanted(event):
                return dict(event)
    raise AssertionError("the stream ended first")


def test_an_http_session_is_asked_on_the_calls_own_response(live_server: LiveServer) -> None:
    server, runs = booking_server()
    base = live_server(server)
    with httpx.Client(base_url=base, timeout=10) as client:
        opened = client.post("/mcp", json=init(), headers=ACCEPT)
        headers = {**ACCEPT, "MCP-Session-Id": opened.headers["mcp-session-id"]}
        client.post("/mcp", json=notification("notifications/initialized"), headers=headers)
        call = rpc("tools/call", {"name": "book", "arguments": {"restaurant": "Luigi"}}, 7)
        with client.stream("POST", "/mcp", json=call, headers=headers) as response:
            assert response.headers["content-type"].startswith("text/event-stream")
            lines = response.iter_lines()
            request = read_until(lines, lambda event: event.get("method") == "elicitation/create")
            answered = client.post("/mcp", json=reply(request, guests=3), headers=headers)
            assert answered.status_code == 202
            final = read_until(lines, lambda event: event.get("id") == 7)
        assert final["result"]["content"][0]["text"] == "Luigi for 3"
        # A call that asks nothing is still answered as JSON.
        plain = client.post("/mcp", json=rpc("tools/list", msg_id=8), headers=headers)
        assert plain.headers["content-type"].startswith("application/json")
    assert runs == ["start", "start", "booked"]


def test_stateless_http_asks_with_a_json_result_and_takes_the_retry(
    live_server: LiveServer,
) -> None:
    server, _ = booking_server()
    base = live_server(server)

    def post(client: httpx.Client, message: dict[str, Any]) -> httpx.Response:
        headers = {
            **ACCEPT,
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": message["params"]["name"],
        }
        return client.post("/mcp", json=message, headers=headers)

    with httpx.Client(base_url=base, timeout=10) as client:
        first = post(client, stateless("book", {"restaurant": "Luigi"}))
        assert first.status_code == 200
        assert first.headers["content-type"].startswith("application/json")
        key, _ = asked(first.json())
        retry = stateless(
            "book",
            {"restaurant": "Luigi"},
            inputResponses={key: {"action": "accept", "content": {"guests": 5}}},
        )
        assert text(post(client, retry).json()) == "Luigi for 5"
        refused = post(client, stateless("book", {"restaurant": "Luigi"}, capabilities={}))
        assert refused.status_code == 400
        assert refused.json()["error"]["code"] == MISSING_REQUIRED_CLIENT_CAPABILITY


def test_a_sync_tool_is_asked_from_its_thread(live_server: LiveServer) -> None:
    # elicit() unwinds the worker thread; nothing is left running there.
    server = MCPServer(port=0, rate_limit_per_minute=None)
    threads: list[int] = []

    @server.tool
    def where() -> str:
        """Record the thread, then ask."""
        threads.append(threading.get_ident())
        return str(elicit("Where?", {"city": str}).get("city"))

    base = live_server(server)
    message = stateless("where")
    headers = {
        **ACCEPT,
        "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": "tools/call",
        "Mcp-Name": "where",
    }
    with httpx.Client(base_url=base, timeout=10) as client:
        key, _ = asked(client.post("/mcp", json=message, headers=headers).json())
        retry = stateless(
            "where", inputResponses={key: {"action": "accept", "content": {"city": "Bern"}}}
        )
        assert text(client.post("/mcp", json=retry, headers=headers).json()) == "Bern"
    assert len(threads) == 2


def test_an_elicitation_can_be_built_by_hand() -> None:
    # For a tool's own tests, which call the function with a canned answer.
    declined = Elicitation("decline")
    assert not declined.accepted and declined.content == {} and declined.get("x", 1) == 1
    accepted = Elicitation("accept", {"guests": 2})
    content = accepted.content
    content["guests"] = 99
    assert accepted["guests"] == 2  # content is a copy
