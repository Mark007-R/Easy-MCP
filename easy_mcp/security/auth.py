"""API-key authentication and per-tool authorization.

Design notes (security-sensitive):

* Key comparison uses :func:`hmac.compare_digest` over SHA-256 digests of
  the keys (fixed length, so timing cannot reveal a key's length either) and
  always iterates the full key set, so response timing does not reveal
  whether or where a presented key partially matched.
* Raw keys never appear in logs or errors — only SHA-256 *fingerprints*.
* "No key presented" is anonymous (public tools only); "wrong key presented"
  is an outright :class:`AuthenticationError`.

OAuth access tokens (:mod:`easy_mcp.security.oauth`) resolve to the same
:class:`ClientIdentity`, with the token's verified fields filled in.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import hmac
import logging
import os
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeGuard

from ..exceptions import AuthenticationError, AuthorizationError

logger = logging.getLogger("easy_mcp.security")

MIN_KEY_LENGTH = 16

# An RFC 6749 scope-token: printable ASCII except space, '"' and '\'.  Only
# these can appear in a WWW-Authenticate challenge.
_SCOPE_TOKEN = re.compile(r"[\x21\x23-\x5B\x5D-\x7E]+")


def _digest(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest()


def fingerprint(key: str) -> str:
    """A short, non-reversible identifier for an API key (safe to log)."""
    return _digest(key).hex()[:12]


def is_scope_token(value: object) -> TypeGuard[str]:
    """Whether *value* is an RFC 6749 scope-token (no space, ``"`` or ``\\``)."""
    return isinstance(value, str) and _SCOPE_TOKEN.fullmatch(value) is not None


class _ReadOnlyMapping(Mapping[str, Any]):
    """A read-only mapping that, unlike ``MappingProxyType``, survives
    :func:`copy.deepcopy`, :mod:`pickle` and :func:`dataclasses.asdict`."""

    __slots__ = ("_data",)

    def __init__(self, data: Mapping[str, Any]) -> None:
        self._data = dict(data)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (dict(self._data),))


def _no_claims() -> Mapping[str, Any]:
    return _ReadOnlyMapping({})


@dataclass(frozen=True, slots=True)
class ClientIdentity:
    """The authenticated caller: a fingerprint plus granted scopes.

    For an API key, ``fingerprint`` identifies the key and the other fields
    stay empty.  For an OAuth access token, ``fingerprint`` identifies the
    principal (issuer, subject and client), so it survives a refreshed or
    broader token; ``issuer`` is set exactly when the identity came from a
    token.  ``claims`` holds the token's verified claims, read-only; it is
    left out of ``repr``, equality and hashing.  The token itself is never
    kept.
    """

    fingerprint: str
    scopes: frozenset[str]
    subject: str | None = None
    client_id: str | None = None
    issuer: str | None = None
    expires_at: int | None = None
    claims: Mapping[str, Any] = field(
        default_factory=_no_claims, compare=False, hash=False, repr=False
    )


class Guarded(Protocol):
    """What authorization reads from a registered item: a tool, resource, template or prompt."""

    @property
    def name(self) -> str: ...

    @property
    def requires_auth(self) -> bool: ...

    @property
    def scopes(self) -> frozenset[str]: ...

    @property
    def declared_scopes(self) -> tuple[str, ...]: ...


_current_identity: contextvars.ContextVar[ClientIdentity | None] = contextvars.ContextVar(
    "easy_mcp_identity", default=None
)


def current_identity() -> ClientIdentity | None:
    """The authenticated caller of the tool call running in this task or thread.

    ``None`` for an anonymous caller, and outside a tool call.  For an OAuth
    token it carries the verified ``subject``, ``client_id``, ``issuer``,
    ``scopes`` and ``claims``, so a tool can key its own state by the user
    the token names; the token itself is never handed to tool code.  Works in
    sync tools (on their thread) and in tool middleware, as
    :func:`~easy_mcp.current_cancel_token` does, and in resources, prompts
    and completers too.
    """
    return _current_identity.get()


@contextlib.contextmanager
def _identity_scope(identity: ClientIdentity | None) -> Iterator[ClientIdentity | None]:
    reset = _current_identity.set(identity)
    try:
        yield identity
    finally:
        _current_identity.reset(reset)


class APIKeyAuth:
    """API-key authentication with per-key scopes.

    Args:
        keys: Mapping of API key → scopes.  Scopes may be an iterable of
            scope names, a single scope string, or ``"*"`` for all scopes.

    Example::

        auth = APIKeyAuth({
            "long-random-admin-key....": "*",
            "long-random-math-key.....": ["math"],
        })
    """

    def __init__(self, keys: Mapping[str, Iterable[str] | str | None]) -> None:
        if not keys:
            raise ValueError("APIKeyAuth requires at least one API key")
        normalized: list[tuple[bytes, str, frozenset[str]]] = []
        for key, scopes in keys.items():
            if not isinstance(key, str) or not key:
                raise ValueError("API keys must be non-empty strings")
            if len(key) < MIN_KEY_LENGTH:
                logger.warning(
                    "API key %s is shorter than %d characters; use a long random key",
                    fingerprint(key),
                    MIN_KEY_LENGTH,
                )
            if scopes is None or scopes == "*":
                scope_set = frozenset({"*"})
            elif isinstance(scopes, str):
                scope_set = frozenset({scopes})
            else:
                scope_set = frozenset(scopes)
            normalized.append((_digest(key), fingerprint(key), scope_set))
        self._keys = tuple(normalized)

    @classmethod
    def from_env(cls, var: str = "EASY_MCP_API_KEYS") -> APIKeyAuth:
        """Load keys from an environment variable (keeps keys out of code).

        Format: ``key1:scopeA|scopeB;key2:*`` — entries separated by ``;``,
        scopes separated by ``|``, missing/``*`` scopes meaning all scopes.
        """
        raw = os.environ.get(var)
        if not raw:
            raise ValueError(f"environment variable {var} is not set or empty")
        keys: dict[str, Iterable[str] | str] = {}
        for entry in raw.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            key, _, scope_part = entry.partition(":")
            scope_part = scope_part.strip()
            if scope_part in ("", "*"):
                keys[key.strip()] = "*"
            else:
                keys[key.strip()] = [s.strip() for s in scope_part.split("|") if s.strip()]
        return cls(keys)

    def authenticate(self, presented: str | None) -> ClientIdentity | None:
        """Resolve a presented key to an identity.

        Returns ``None`` for anonymous access (no key presented).

        Raises:
            AuthenticationError: If a key was presented but does not match.
        """
        if presented is None:
            return None
        matched = self.match(presented)
        if matched is None:
            raise AuthenticationError("Invalid API key")
        return matched

    def match(self, presented: str) -> ClientIdentity | None:
        """The identity of the key *presented*, or ``None`` if it is no key of ours.

        Never raises, so a caller can try a value as an API key before
        treating it as something else (an OAuth access token).
        """
        matched: ClientIdentity | None = None
        presented_digest = _digest(presented)
        # Iterate every key even after a match so timing stays independent
        # of match position; digests are fixed-length, so compare_digest is
        # constant-time regardless of the presented key's length.
        for digest, key_fingerprint, scopes in self._keys:
            if hmac.compare_digest(digest, presented_digest):
                matched = ClientIdentity(fingerprint=key_fingerprint, scopes=scopes)
        return matched


def authorize(identity: ClientIdentity | None, tool: Guarded, kind: str = "Tool") -> None:
    """Enforce an item's auth requirements against the caller's identity.

    *kind* names the item in messages (``"Tool"``, ``"Prompt"``, ...).

    Raises:
        AuthenticationError: The item is protected and the caller is anonymous.
        AuthorizationError: The caller lacks every scope the item requires.
    """
    if not tool.requires_auth:
        return
    if identity is None:
        raise AuthenticationError(f"{kind} '{tool.name}' requires authentication")
    if tool.scopes and "*" not in identity.scopes and not (identity.scopes & tool.scopes):
        raise AuthorizationError(
            f"{kind} '{tool.name}' requires one of scopes: {sorted(tool.scopes)}"
        )


def visible(identity: ClientIdentity | None, tool: Guarded) -> bool:
    """Whether *tool* (or a resource, template or prompt) is listed for this caller.

    Protected items are hidden from callers who could not use them, so
    unauthorized clients cannot even enumerate them.
    """
    if not tool.requires_auth:
        return True
    if identity is None:
        return False
    if not tool.scopes or "*" in identity.scopes or identity.scopes & tool.scopes:
        return True
    return False
