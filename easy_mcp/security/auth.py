"""API-key authentication and per-tool authorization.

Design notes (security-sensitive):

* Key comparison uses :func:`hmac.compare_digest` and always iterates the
  full key set, so response timing does not reveal whether or where a
  presented key partially matched.
* Raw keys never appear in logs or errors — only SHA-256 *fingerprints*.
* "No key presented" is anonymous (public tools only); "wrong key presented"
  is an outright :class:`AuthenticationError`.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..exceptions import AuthenticationError, AuthorizationError

if TYPE_CHECKING:
    from ..decorators import ToolDefinition

logger = logging.getLogger("easy_mcp.security")

MIN_KEY_LENGTH = 16


def fingerprint(key: str) -> str:
    """A short, non-reversible identifier for an API key (safe to log)."""
    return hashlib.sha256(key.encode()).hexdigest()[:12]


@dataclass(frozen=True, slots=True)
class ClientIdentity:
    """The authenticated caller: key fingerprint plus granted scopes."""

    fingerprint: str
    scopes: frozenset[str]


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
        normalized: dict[str, frozenset[str]] = {}
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
            normalized[key] = scope_set
        self._keys = normalized

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
        matched: ClientIdentity | None = None
        # Iterate every key even after a match so timing stays independent
        # of match position (constant-time comparison per key).
        for key, scopes in self._keys.items():
            if hmac.compare_digest(key.encode(), presented.encode()):
                matched = ClientIdentity(fingerprint=fingerprint(key), scopes=scopes)
        if matched is None:
            raise AuthenticationError("Invalid API key")
        return matched


def authorize(identity: ClientIdentity | None, tool: ToolDefinition) -> None:
    """Enforce a tool's auth requirements against the caller's identity.

    Raises:
        AuthenticationError: The tool is protected and the caller is anonymous.
        AuthorizationError: The caller lacks every scope the tool requires.
    """
    if not tool.requires_auth:
        return
    if identity is None:
        raise AuthenticationError(f"Tool '{tool.name}' requires authentication")
    if tool.scopes and "*" not in identity.scopes and not (identity.scopes & tool.scopes):
        raise AuthorizationError(
            f"Tool '{tool.name}' requires one of scopes: {sorted(tool.scopes)}"
        )


def visible(identity: ClientIdentity | None, tool: ToolDefinition) -> bool:
    """Whether *tool* should appear in ``tools/list`` for this caller.

    Protected tools are hidden from callers who could not invoke them, so
    unauthorized clients cannot even enumerate them.
    """
    if not tool.requires_auth:
        return True
    if identity is None:
        return False
    if not tool.scopes or "*" in identity.scopes or identity.scopes & tool.scopes:
        return True
    return False
