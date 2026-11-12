"""Asking the user a question while a tool call is being served (MCP elicitation).

A tool that needs something only the user knows asks for it with
:func:`elicit`::

    from typing import Literal

    from easy_mcp import elicit

    @server.tool
    def book_table(restaurant: str) -> str:
        \"\"\"Book a table.\"\"\"
        answer = elicit(
            "For how many people, and when?",
            {"guests": int, "day": Literal["today", "tomorrow"]},
        )
        if not answer.accepted:
            return "Nothing was booked."
        return reserve(restaurant, answer["guests"], answer["day"])

**The tool function runs again from the top once the answer is in.**  The
first run stops at the :func:`elicit` call that has no answer yet; the
client shows the user a form; then the function is called anew with the
same arguments, and this time :func:`elicit` returns the answer.  So
everything a tool does before its last :func:`elicit` call must be safe to
repeat: ask first, act afterwards.

That one rule is what lets the same tool serve both protocol eras:

* **Stateless** (``2026-07-28``): the first run is answered with an
  ``InputRequiredResult`` naming the question, and the client retries the
  call with the answer in ``inputResponses``.  Answers to earlier questions
  of the same call travel in ``requestState``, which the server signs (see
  :class:`StateSigner`), so no server keeps anything between the two
  requests and any worker can serve the retry.
* **Sessions** (``2025-11-25`` and earlier): the server sends the client an
  ``elicitation/create`` request on the call's own channel, waits for the
  response, and runs the function again within the same call.

A client that cannot be asked (it declared no ``elicitation`` capability, or
its channel carries no requests) makes :func:`elicit` raise
:class:`ElicitationUnavailableError`; :func:`can_elicit` tells beforehand.

Only form mode is offered, and the specification forbids using it for
secrets: never ask for a password, an API key or a card number this way.
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import contextvars
import hashlib
import hmac
import json
import math
import os
import secrets
import time
import types
import typing
from collections.abc import Iterator, Mapping
from typing import Any, Literal, cast

from .exceptions import INVALID_PARAMS, EasyMCPError, ProtocolError

ELICIT_METHOD = "elicitation/create"

# How long a session's client has to answer one question before it counts as
# dismissed.  The tool's own timeout does not run meanwhile.
ELICITATION_TIMEOUT_SECONDS = 600.0

# How long a signed requestState is accepted: the time the user has to
# answer one question of a stateless call.
REQUEST_STATE_TTL_SECONDS = 900

# How many questions one call may ask before it is taken for a loop.
MAX_QUESTIONS = 10

# The longest message and the most fields one question may have.
MAX_MESSAGE_LENGTH = 2000
MAX_FIELDS = 20

STATE_SECRET_ENV_VAR = "EASY_MCP_STATE_SECRET"
# The shortest secret accepted: a shorter one could be guessed.
MIN_SECRET_BYTES = 16

Action = Literal["accept", "decline", "cancel"]
_ACTIONS = ("accept", "decline", "cancel")

_PRIMITIVES: dict[Any, str] = {str: "string", int: "integer", float: "number", bool: "boolean"}


class ElicitationUnavailableError(EasyMCPError):
    """The client of this call cannot be asked a question.

    Raised by :func:`elicit`.  A tool may catch it and carry on without the
    answer; left alone, it ends the call: with ``-32021`` naming the
    capability for a stateless client, and an ``isError`` result saying so
    for a session's.
    """


class Elicitation:
    """What the user did with a question.  Read-only.

    ``action`` is ``"accept"`` (the user submitted the form), ``"decline"``
    (the user said no) or ``"cancel"`` (the user dismissed it, or did not
    answer in time).  ``content`` holds the submitted fields when accepted,
    checked against what was asked, and is empty otherwise.  Indexing the
    object reads a field: ``answer["guests"]``.
    """

    __slots__ = ("_action", "_content")

    def __init__(self, action: Action, content: Mapping[str, Any] | None = None) -> None:
        self._action: Action = action
        self._content = dict(content or {})

    @property
    def action(self) -> Action:
        return self._action

    @property
    def accepted(self) -> bool:
        """Whether the user submitted an answer."""
        return self._action == "accept"

    @property
    def content(self) -> dict[str, Any]:
        """The submitted fields (a copy); empty unless accepted."""
        return dict(self._content)

    def __getitem__(self, name: str) -> Any:
        return self._content[name]

    def get(self, name: str, default: Any = None) -> Any:
        """A submitted field, or *default* when it was left out or nothing was submitted."""
        return self._content.get(name, default)

    def __repr__(self) -> str:
        return f"Elicitation(action={self._action!r}, content={self._content!r})"


class _InputRequired(BaseException):
    """Unwinds a tool at an :func:`elicit` call that has no answer yet.

    Not an ``Exception``: a tool's own ``except Exception`` must not swallow
    it, or the question would never be asked.
    """

    def __init__(self, key: str, params: dict[str, Any]) -> None:
        super().__init__(key)
        self.key = key
        self.params = params


class _InvalidAnswer(BaseException):
    """Unwinds a tool whose question was answered with something that is no answer to it."""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors


def question_signal(exc: BaseException) -> _InputRequired | _InvalidAnswer | None:
    """The question (or bad answer) that *exc* is, or carries in an exception group.

    ``elicit()`` called in a child of an ``asyncio.TaskGroup`` reaches the
    tool's caller wrapped in a ``BaseExceptionGroup``.
    """
    if isinstance(exc, _InputRequired | _InvalidAnswer):
        return exc
    if isinstance(exc, BaseExceptionGroup):
        for inner in exc.exceptions:
            found = question_signal(inner)
            if found is not None:
                return found
    return None


class _Asking:
    """The questions of one tool call: the answers so far, and whether more can be asked.

    ``unavailable`` says why the client cannot be asked, ``None`` when it
    can.  ``answers`` maps a question's key to the client's result object.
    """

    __slots__ = ("answers", "asked", "stateless", "unavailable", "used")

    def __init__(
        self,
        *,
        stateless: bool,
        unavailable: str | None,
        answers: dict[str, Any] | None = None,
    ) -> None:
        self.stateless = stateless
        self.unavailable = unavailable
        self.answers: dict[str, Any] = answers if answers is not None else {}
        # How many questions this run has asked so far: a question's place
        # among them is part of its key.
        self.asked = 0
        # The keys of the answers this run has read: what a requestState carries on.
        self.used: set[str] = set()

    def begin(self) -> None:
        """A run of the function starts: its questions are counted anew."""
        self.asked = 0
        self.used = set()


_current: contextvars.ContextVar[_Asking | None] = contextvars.ContextVar(
    "easy_mcp_asking", default=None
)


@contextlib.contextmanager
def _asking_scope(asking: _Asking | None) -> Iterator[None]:
    reset = _current.set(asking)
    try:
        yield
    finally:
        _current.reset(reset)


# ------------------------------------------------------------------ schemas


def _field_schema(name: str, spec: Any) -> tuple[dict[str, Any], bool]:
    """The JSON Schema of one form field, and whether it is required."""
    if isinstance(spec, Mapping):
        # Written out by the caller: taken as is, required unless it has a default.
        return dict(spec), "default" not in spec
    description: str | None = None
    if typing.get_origin(spec) is typing.Annotated:
        spec, *metadata = typing.get_args(spec)
        description = next((item for item in metadata if isinstance(item, str)), None)
    required = True
    origin = typing.get_origin(spec)
    if origin is typing.Union or origin is types.UnionType:
        members = [member for member in typing.get_args(spec) if member is not type(None)]
        if len(members) != 1 or len(members) == len(typing.get_args(spec)):
            raise TypeError(f"field {name!r}: a union must be 'T | None', an optional field")
        spec, required = members[0], False
        origin = typing.get_origin(spec)
    schema: dict[str, Any]
    if spec in _PRIMITIVES:
        schema = {"type": _PRIMITIVES[spec]}
    elif origin is Literal:
        schema = {"type": "string", "enum": _choices(name, spec)}
    elif origin is list:
        (item,) = typing.get_args(spec) or (None,)
        if typing.get_origin(item) is not Literal:
            raise TypeError(f"field {name!r}: a list must be list[Literal[...]], a multi-select")
        schema = {"type": "array", "items": {"type": "string", "enum": _choices(name, item)}}
    else:
        raise TypeError(
            f"field {name!r}: use str, int, float, bool, Literal[...] or list[Literal[...]] "
            f"(optionally '| None'), or a JSON Schema dict; got {spec!r}"
        )
    if description is not None:
        schema["description"] = description
    return schema, required


def _choices(name: str, literal: Any) -> list[str]:
    values = list(typing.get_args(literal))
    if not values or not all(isinstance(value, str) for value in values):
        raise TypeError(f"field {name!r}: Literal choices must be strings")
    return values


def build_requested_schema(fields: Mapping[str, Any]) -> dict[str, Any]:
    """The ``requestedSchema`` of a form asking for *fields*.

    *fields* maps a field name to its type (``str``, ``int``, ``float``,
    ``bool``, ``Literal["a", "b"]`` for one choice, ``list[Literal[...]]``
    for several; ``T | None`` makes the field optional and
    ``Annotated[T, "..."]`` describes it), or to that field's JSON Schema
    written out.  A mapping that already is an object schema (it has
    ``"type": "object"`` and ``"properties"``) is checked and returned.

    Raises:
        TypeError: A field's type has no form field.
        ValueError: No fields, too many, or a schema a form cannot show.
    """
    if not isinstance(fields, Mapping):
        raise TypeError("fields must be a mapping of field names to types")
    if fields.get("type") == "object" and isinstance(fields.get("properties"), Mapping):
        properties = {str(name): dict(spec) for name, spec in fields["properties"].items()}
        required = [str(name) for name in fields.get("required", ())]
    else:
        properties, required = {}, []
        for name, spec in fields.items():
            if not isinstance(name, str) or not name:
                raise TypeError("a field name must be a non-empty string")
            properties[name], needed = _field_schema(name, spec)
            if needed:
                required.append(name)
    if not properties:
        raise ValueError("a question needs at least one field")
    if len(properties) > MAX_FIELDS:
        raise ValueError(f"a question may have at most {MAX_FIELDS} fields")
    for name, spec in properties.items():
        _check_field(name, spec)
    unknown = [name for name in required if name not in properties]
    if unknown:
        raise ValueError(f"required names {unknown[0]!r}, which is no field")
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _options(spec: Mapping[str, Any]) -> list[Any] | None:
    """The values a choice field allows, or ``None`` when it is not one."""
    if isinstance(spec.get("enum"), list):
        return list(spec["enum"])
    for keyword in ("oneOf", "anyOf"):
        options = spec.get(keyword)
        if isinstance(options, list) and all(
            isinstance(option, Mapping) and "const" in option for option in options
        ):
            return [option["const"] for option in options]
    return None


def _check_field(name: str, spec: Mapping[str, Any]) -> None:
    """Refuse a field schema outside the flat, primitive subset forms are limited to."""
    kind = spec.get("type")
    if kind in ("string", "number", "integer", "boolean"):
        return
    if kind == "array":
        items = spec.get("items")
        if isinstance(items, Mapping) and _options(items) is not None:
            return
        raise ValueError(f"field {name!r}: an array must list its choices in 'items'")
    raise ValueError(
        f"field {name!r}: a form field is a string, number, integer, boolean or an "
        f"array of choices, not {kind!r}"
    )


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def _field_errors(name: str, spec: Mapping[str, Any], value: Any) -> list[str]:
    kind = spec.get("type")
    if kind == "boolean":
        return [] if isinstance(value, bool) else [f"'{name}' must be a boolean"]
    if kind == "integer":
        whole = _is_number(value) and float(value).is_integer()
        if not whole:
            return [f"'{name}' must be an integer"]
    if kind in ("integer", "number"):
        if not _is_number(value):
            return [f"'{name}' must be a number"]
        low, high = spec.get("minimum"), spec.get("maximum")
        if _is_number(low) and value < low:
            return [f"'{name}' must be at least {low}"]
        if _is_number(high) and value > high:
            return [f"'{name}' must be at most {high}"]
        return []
    if kind == "string":
        if not isinstance(value, str):
            return [f"'{name}' must be a string"]
        options = _options(spec)
        if options is not None and value not in options:
            return [f"'{name}' must be one of the choices offered"]
        shortest, longest = spec.get("minLength"), spec.get("maxLength")
        if isinstance(shortest, int) and len(value) < shortest:
            return [f"'{name}' must have at least {shortest} characters"]
        if isinstance(longest, int) and len(value) > longest:
            return [f"'{name}' must have at most {longest} characters"]
        return []
    # An array of choices.
    items = spec.get("items")
    options = _options(items) if isinstance(items, Mapping) else None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return [f"'{name}' must be a list of strings"]
    if options is not None and any(item not in options for item in value):
        return [f"'{name}' must hold only the choices offered"]
    fewest, most = spec.get("minItems"), spec.get("maxItems")
    if isinstance(fewest, int) and len(value) < fewest:
        return [f"'{name}' must hold at least {fewest} choices"]
    if isinstance(most, int) and len(value) > most:
        return [f"'{name}' must hold at most {most} choices"]
    return []


def check_answer(schema: Mapping[str, Any], result: Any) -> Elicitation:
    """The client's result for a question asked with *schema*, checked.

    Fields the form did not ask for are dropped.  An integer field takes
    ``3.0`` as ``3``.

    Raises:
        _InvalidAnswer: The result is no elicitation result, or its content
            does not fit the form.
    """
    if not isinstance(result, Mapping) or result.get("action") not in _ACTIONS:
        raise _InvalidAnswer(["an answer must carry an action: accept, decline or cancel"])
    action = cast(Action, result["action"])
    if action != "accept":
        return Elicitation(action)
    content = result.get("content")
    if not isinstance(content, Mapping):
        raise _InvalidAnswer(["an accepted answer must carry its content"])
    properties: Mapping[str, Any] = schema.get("properties", {})
    errors: list[str] = []
    accepted: dict[str, Any] = {}
    for name in schema.get("required", ()):
        if name not in content:
            errors.append(f"'{name}' is required")
    for name, spec in properties.items():
        if name not in content:
            continue
        value = content[name]
        problems = _field_errors(name, spec, value)
        errors.extend(problems)
        if not problems:
            if spec.get("type") == "integer":
                value = int(value)
            accepted[name] = value
    if errors:
        raise _InvalidAnswer(errors)
    return Elicitation("accept", accepted)


def question_key(index: int, message: str, schema: Mapping[str, Any]) -> str:
    """The key of the *index*-th question of a call: its place, and a digest of what it asks.

    A question that changed between two runs of the function gets another
    key, so an answer is never taken for a question it was not given to.
    """
    raw = json.dumps([message, schema], sort_keys=True, separators=(",", ":"), default=str)
    return f"q{index}-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:10]}"


# ---------------------------------------------------------------- public API


def can_elicit() -> bool:
    """Whether :func:`elicit` can ask the client of the tool call being served."""
    asking = _current.get()
    return asking is not None and asking.unavailable is None


def elicit(message: str, fields: Mapping[str, Any]) -> Elicitation:
    """Ask the user a question from inside a tool, and get the answer.

    The function calling this **runs again from the top** once the user has
    answered (see the module's description), so do nothing before it that
    must happen only once.  Works in sync and async tools alike; it never
    blocks.

    Args:
        message: What the user is asked, and why.
        fields: The form: each field's name and type (``str``, ``int``,
            ``float``, ``bool``, ``Literal[...]``, ``list[Literal[...]]``;
            ``T | None`` for an optional field, ``Annotated[T, "..."]`` to
            describe it), or its JSON Schema written out.  Never ask for a
            secret here.

    Returns:
        What the user did: check ``accepted`` before reading the fields.

    Raises:
        ElicitationUnavailableError: The client cannot be asked, or this is
            no tool call.
        TypeError: A field's type has no form field.
        ValueError: An empty or oversized question.
    """
    if not isinstance(message, str) or not message.strip():
        raise ValueError("a question needs a message")
    if len(message) > MAX_MESSAGE_LENGTH:
        raise ValueError(f"a question's message may have at most {MAX_MESSAGE_LENGTH} characters")
    schema = build_requested_schema(fields)
    asking = _current.get()
    if asking is None:
        raise ElicitationUnavailableError("elicit() works only inside a tool call")
    if asking.unavailable is not None:
        raise ElicitationUnavailableError(asking.unavailable)
    asking.asked += 1
    if asking.asked > MAX_QUESTIONS:
        raise RuntimeError(f"a tool call may ask at most {MAX_QUESTIONS} questions")
    key = question_key(asking.asked, message, schema)
    if key in asking.answers:
        asking.used.add(key)
        return check_answer(schema, asking.answers[key])
    params: dict[str, Any] = {"message": message, "requestedSchema": schema}
    if asking.stateless:
        params = {"mode": "form", **params}
    raise _InputRequired(key, params)


# ------------------------------------------------------------- requestState


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def call_digest(name: str, arguments: Mapping[str, Any]) -> str:
    """What a ``requestState`` is bound to: the tool, and the arguments of the call."""
    raw = json.dumps([name, arguments], sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


class StateSigner:
    """Signs and verifies the ``requestState`` of stateless calls that ask questions.

    The state carries the answers to a call's earlier questions through the
    client and back.  The specification makes it attacker-controlled input,
    so it is authenticated with HMAC-SHA256 and bound to the principal who
    was asked, the tool and arguments of the call, and an expiry; a state
    that fails any of these is refused.  It is signed, not encrypted: it
    holds only what its own client answered.

    Args:
        secret: The signing key.  ``None`` reads ``EASY_MCP_STATE_SECRET``,
            and without it makes a random key for this process, which only
            this process can verify: set one when several workers serve the
            same clients.

    Raises:
        ValueError: A secret shorter than 16 bytes.
    """

    def __init__(self, secret: str | bytes | None = None) -> None:
        if secret is None:
            secret = os.environ.get(STATE_SECRET_ENV_VAR) or None
        self.shared = secret is not None
        if secret is None:
            key = secrets.token_bytes(32)
        else:
            key = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
            if len(key) < MIN_SECRET_BYTES:
                raise ValueError(f"state_secret must be at least {MIN_SECRET_BYTES} bytes")
        self._key = key

    def _mac(self, body: str) -> str:
        return _b64(hmac.new(self._key, body.encode("ascii"), hashlib.sha256).digest())

    def sign(self, *, principal: str | None, digest: str, answers: Mapping[str, Any]) -> str:
        """The ``requestState`` carrying *answers* for *principal*'s call *digest*."""
        payload = {
            "v": 1,
            "exp": int(time.time()) + REQUEST_STATE_TTL_SECONDS,
            "sub": principal,
            "req": digest,
            "ans": dict(answers),
        }
        # ASCII: whatever a client answered (a lone surrogate, say) can be carried.
        raw = json.dumps(payload, separators=(",", ":"), default=str)
        body = _b64(raw.encode("ascii"))
        return f"{body}.{self._mac(body)}"

    def verify(self, state: object, *, principal: str | None, digest: str) -> dict[str, Any]:
        """The answers a ``requestState`` carries.

        Raises:
            ProtocolError: ``-32602`` for a state this server did not sign,
                one that has expired, or one issued to another principal or
                for another call.  The message never says which.
        """
        invalid = ProtocolError("Invalid requestState", code=INVALID_PARAMS)
        if not isinstance(state, str) or state.count(".") != 1:
            raise invalid
        body, mac = state.split(".")
        # Compared before anything in it is parsed.
        try:
            expected = self._mac(body).encode("ascii")
            presented = mac.encode("ascii")
        except UnicodeError:  # not ASCII, so nothing this server signed
            raise invalid from None
        if not hmac.compare_digest(expected, presented):
            raise invalid
        try:
            payload = json.loads(_unb64(body).decode("utf-8"))
        except (binascii.Error, ValueError, RecursionError):
            raise invalid from None
        if not isinstance(payload, dict) or payload.get("v") != 1:
            raise invalid
        expiry = payload.get("exp")
        if not isinstance(expiry, int) or expiry < time.time():
            raise invalid
        if payload.get("sub") != principal or payload.get("req") != digest:
            raise invalid
        answers = payload.get("ans")
        if not isinstance(answers, dict):
            raise invalid
        return answers


def supports_form(capabilities: object) -> bool:
    """Whether a client's declared capabilities include form mode elicitation."""
    if not isinstance(capabilities, Mapping):
        return False
    elicitation = capabilities.get("elicitation")
    if not isinstance(elicitation, Mapping):
        return False
    # An empty object means form mode, as it did before modes existed.
    return not elicitation or "form" in elicitation

