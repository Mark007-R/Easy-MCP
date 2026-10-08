"""OAuth 2.1 bearer tokens: the MCP server as a protected resource.

:class:`OAuthResourceServer` makes an :class:`~easy_mcp.MCPServer` an OAuth
2.1 resource server, as the MCP authorization specification describes for
HTTP transports.  It knows what the server publishes in its Protected
Resource Metadata (RFC 9728), and it verifies every access token: locally as
a JWT against the authorization server's published keys (the ``[oauth]``
extra, PyJWT), or remotely by token introspection (RFC 7662, standard
library only).

Security notes:

* A token must come from a configured authorization server (``iss`` is
  compared byte for byte, before anything is fetched) and carry this server
  in its audience (RFC 8707).  Keys come only from that issuer's published
  key set, never from the token's own headers (``jwk``, ``jku``, ``x5u``,
  ``x5c``), so no token can make the server fetch anything or trust a key
  of its choosing.
* Only asymmetric algorithms are accepted, and each key is bound to the
  algorithms its type and curve allow (RFC 8725).  Keys reach PyJWT only as
  ``cryptography`` key objects, never as PEM or JWK text.
* The raw token is never stored, logged or put on the identity; caches are
  keyed by its SHA-256.  Tokens bound to a proof of possession (``cnf``)
  are refused, since this server cannot check the proof.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import importlib
import json
import logging
import math
import os
import re
import secrets
import threading
import time
import weakref
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ..exceptions import AuthServerUnavailableError, InvalidTokenError
from ..logging import audit
from . import _fetch
from .auth import ClientIdentity, _ReadOnlyMapping, is_scope_token

logger = logging.getLogger("easy_mcp.security.oauth")

__all__ = [
    "DEFAULT_ALGORITHMS",
    "Introspection",
    "OAuthResourceServer",
    "canonical_uri",
    "principal_fingerprint",
]

#: The JWS algorithms accepted by default: asymmetric ones only.
DEFAULT_ALGORITHMS: tuple[str, ...] = (
    "RS256",
    "RS384",
    "RS512",
    "PS256",
    "PS384",
    "PS512",
    "ES256",
    "ES384",
    "ES512",
    "EdDSA",
)

# Fixed behaviour, not settings.
LEEWAY_SECONDS = 60  # clock skew allowed on exp, nbf and iat
MAX_TOKEN_BYTES = 16 * 1024
KEYS_TTL_SECONDS = 3600  # key sets (and metadata) are not used past an hour unrefreshed
KEY_REFRESH_AHEAD_SECONDS = 300  # the hourly refresh starts, in the background, 5 min early
KEY_REFRESH_COOLDOWN_SECONDS = 30  # at most one key-set fetch per issuer per 30 s
UNAVAILABLE_RETRY_SECONDS = 5  # with no keys at all, retry this often (and Retry-After)
REQUEST_TIMEOUT_SECONDS = 5.0
MAX_DOCUMENT_BYTES = 1024 * 1024  # metadata and key sets
MAX_INTROSPECTION_BYTES = 64 * 1024
MAX_CLAIM_DEPTH = 32  # objects and arrays nested in a token's claims
MIN_RSA_BITS = 2048
INTROSPECTION_TTL_SECONDS = 60  # never past the answer's exp
INTROSPECTION_REFUSAL_TTL_SECONDS = 10
INTROSPECTION_CACHE_SIZE = 10_000
MAX_INTROSPECTIONS_IN_FLIGHT = 8
# A thread for every introspection slot, plus one for the endpoint check and
# one for discovery or a key set: a request never waits for a thread.
FETCH_WORKERS = MAX_INTROSPECTIONS_IN_FLIGHT + 2

_INSTALL_HINT = (
    "verifying JWT access tokens needs the [oauth] extra:\n"
    'pip install "easy-mcp-kit[oauth]"   (or pass introspection=... to verify tokens remotely)'
)

_DEFAULT_PORTS = {"http": 80, "https": 443}

# RFC 6750's b64token: what a bearer token may consist of.
_B64TOKEN = re.compile(r"[A-Za-z0-9\-._~+/]+=*")
_B64URL = re.compile(r"[A-Za-z0-9_-]+")

_ACCEPTED_TYPES = frozenset({"at+jwt", "application/at+jwt", "jwt"})

# Which keys each algorithm may use: (kty, curves or None for any size-checked RSA).
_ALG_KEYS: dict[str, tuple[str, frozenset[str] | None]] = {
    **{alg: ("RSA", None) for alg in ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512")},
    "ES256": ("EC", frozenset({"P-256"})),
    "ES384": ("EC", frozenset({"P-384"})),
    "ES512": ("EC", frozenset({"P-521"})),
    "EdDSA": ("OKP", frozenset({"Ed25519", "Ed448"})),
}

_EC_COORDINATE_BYTES = {"P-256": 32, "P-384": 48, "P-521": 66}
_OKP_KEY_BYTES = {"Ed25519": 32, "Ed448": 57}


# --------------------------------------------------------------- helpers


def canonical_uri(value: str) -> str:
    """*value* in the canonical form the MCP spec uses for resource identifiers.

    Lowercase scheme and host, the default port dropped and any trailing
    ``/`` removed; the path keeps its case.

    Raises:
        ValueError: *value* is not an absolute URL.
    """
    parts = urlsplit(value)
    scheme = parts.scheme.lower()
    host = parts.hostname
    if not scheme or not host:
        raise ValueError(f"{value!r} is not an absolute URL")
    port = parts.port  # raises ValueError for a port out of range
    netloc = f"[{host}]" if ":" in host else host
    if parts.username is not None or parts.password is not None:
        userinfo = parts.netloc.rpartition("@")[0]
        netloc = f"{userinfo}@{netloc}"
    if port is not None and port != _DEFAULT_PORTS.get(scheme):
        netloc = f"{netloc}:{port}"
    return urlunsplit((scheme, netloc, parts.path.rstrip("/"), parts.query, parts.fragment))


def _canonical_or_none(value: str) -> str | None:
    try:
        parts = urlsplit(value)
        if parts.scheme.lower() not in _DEFAULT_PORTS:
            return None
        return canonical_uri(value)
    except ValueError:
        return None


def principal_fingerprint(issuer: str, subject: str | None, client_id: str | None) -> str:
    """The stable identifier of a token's principal: 32 hex digits, safe to log.

    The same issuer, subject and client give the same fingerprint for every
    token, refreshed or stepped up, so it can key rate limits and call
    counts.  It is 128 bits long, so no client can grind a client id (one it
    chooses, as with Client ID Metadata Documents) whose fingerprint matches
    another principal's, and it can never equal an API key's 12 hex digits.
    The three values are JSON-encoded first, so none can run into the next.
    """
    material = json.dumps([issuer, subject, client_id])  # ASCII: escapes everything else
    return hashlib.sha256(material.encode("ascii")).hexdigest()[:32]


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _check_url(value: object, what: str, *, allow_query: bool = False) -> str:
    """Validate a configured URL: https (http for loopback), no userinfo or fragment."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{what} must be a non-empty URL string")
    # A URL has none of these, and they would break a WWW-Authenticate header.
    if not (value.isascii() and value.isprintable()) or any(c in value for c in ' "\\<>'):
        raise ValueError(f"{what} {value!r} contains characters a URL cannot hold")
    try:
        parts = urlsplit(value)
        host = parts.hostname
        _ = parts.port  # raises ValueError for an invalid port
    except ValueError as exc:
        raise ValueError(f"{what} {value!r} is not a valid URL") from exc
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https") or not host:
        raise ValueError(f"{what} {value!r} must be an absolute https URL")
    if scheme == "http" and host not in _fetch.LOOPBACK_HOSTS:
        raise ValueError(
            f"{what} {value!r} must use https (http is allowed only for localhost, "
            "127.0.0.1 and ::1)"
        )
    if parts.username is not None or parts.password is not None:
        raise ValueError(f"{what} {value!r} must not contain user information")
    if parts.fragment or "#" in value:
        raise ValueError(f"{what} {value!r} must not contain a fragment")
    if parts.query and not allow_query:
        raise ValueError(f"{what} {value!r} must not contain a query")
    return value


def _as_tuple(value: str | Iterable[str], what: str) -> tuple[str, ...]:
    items = (value,) if isinstance(value, str) else tuple(value)
    for item in items:
        if not isinstance(item, str) or not item:
            raise ValueError(f"{what} must be non-empty strings")
    return tuple(dict.fromkeys(items))


def _split_env(value: str | None) -> list[str]:
    return [item for item in re.split(r"[\s,]+", value or "") if item]


def _freeze(value: Any, depth: int = 0) -> Any:
    """A deep read-only copy of decoded JSON (one that can still be pickled).

    Raises:
        ValueError: Objects and arrays nest deeper than ``MAX_CLAIM_DEPTH``,
            which would exhaust the recursion limit here or in any code that
            copies or pickles the identity.
    """
    if isinstance(value, dict | list):
        if depth >= MAX_CLAIM_DEPTH:
            raise ValueError("claims nested too deeply")
        if isinstance(value, dict):
            return _ReadOnlyMapping({key: _freeze(item, depth + 1) for key, item in value.items()})
        return tuple(_freeze(item, depth + 1) for item in value)
    return value


def _frozen_claims(claims: Mapping[str, Any], issuer: str) -> Mapping[str, Any]:
    """*claims* frozen for the identity.

    Raises:
        InvalidTokenError: ``malformed``, the claims nest too deeply.
    """
    try:
        frozen: Mapping[str, Any] = _freeze(dict(claims))
    except Exception:  # ValueError, or a RecursionError however it came about
        raise InvalidTokenError("malformed", issuer=issuer) from None
    return frozen


def _number(value: object) -> float | None:
    """A finite JSON number as a float; ``None`` for anything else (booleans included).

    JSON's ``Infinity`` and ``NaN`` (and ``1e999``) decode to floats that no
    time comparison can place, so they are no numbers here.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        number = float(value)
    except OverflowError:  # an integer too large for a float
        return None
    return number if math.isfinite(number) else None


def _scopes_from(value: object) -> frozenset[str]:
    """The scopes a ``scope``/``scp`` claim grants; ``*`` and invalid ones dropped."""
    if isinstance(value, str):
        items: Iterable[object] = value.split()
    elif isinstance(value, list):
        items = value
    else:
        return frozenset()
    return frozenset(item for item in items if item != "*" and is_scope_token(item))


def _b64url_bytes(value: object) -> bytes:
    if not isinstance(value, str) or not _B64URL.fullmatch(value) or len(value) % 4 == 1:
        raise ValueError("not base64url")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _json_segment(segment: str) -> dict[str, Any]:
    value = json.loads(_b64url_bytes(segment))
    if not isinstance(value, dict):
        raise ValueError("not a JSON object")
    return value


def _load_jwt() -> tuple[Any, SimpleNamespace]:
    """PyJWT and the ``cryptography`` key types, imported on first use."""
    try:
        jwt = importlib.import_module("jwt")
        algorithms = importlib.import_module("jwt.algorithms")
        crypto = SimpleNamespace(
            rsa=importlib.import_module("cryptography.hazmat.primitives.asymmetric.rsa"),
            ec=importlib.import_module("cryptography.hazmat.primitives.asymmetric.ec"),
            ed25519=importlib.import_module("cryptography.hazmat.primitives.asymmetric.ed25519"),
            ed448=importlib.import_module("cryptography.hazmat.primitives.asymmetric.ed448"),
        )
    except ImportError as exc:
        raise ImportError(_INSTALL_HINT) from exc
    if not getattr(algorithms, "has_crypto", False):
        raise ImportError(_INSTALL_HINT)
    return jwt, crypto


# ----------------------------------------------------------------- keys


@dataclass(frozen=True, slots=True)
class _Key:
    """One usable public key from an issuer's key set."""

    kid: str | None
    kty: str
    crv: str | None
    alg: str | None
    use: str | None
    key_ops: tuple[str, ...] | None
    key: Any  # a cryptography public key object
    bits: int = 0  # RSA modulus size

    def fits(self, alg: str) -> bool:
        """Whether this key may verify a signature made with *alg* (RFC 8725 3.1)."""
        kty, curves = _ALG_KEYS[alg]
        if self.kty != kty:
            return False
        if kty == "RSA" and self.bits < MIN_RSA_BITS:
            return False
        if curves is not None and self.crv not in curves:
            return False
        if self.use not in (None, "sig") or self.alg not in (None, alg):
            return False
        return self.key_ops is None or "verify" in self.key_ops

    def kind_fits(self, alg: str) -> bool:
        """Whether the key's type and curve match *alg*, whatever else is wrong."""
        kty, curves = _ALG_KEYS[alg]
        return self.kty == kty and (curves is None or self.crv in curves)


def _parse_jwk(jwk: object, crypto: SimpleNamespace) -> _Key:
    """One JWK as a :class:`_Key`.

    Raises:
        ValueError: The JWK is malformed, symmetric or of an unknown type.
    """
    if not isinstance(jwk, dict):
        raise ValueError("not a JSON object")
    kty = jwk.get("kty")
    kid = jwk.get("kid")
    alg = jwk.get("alg")
    use = jwk.get("use")
    key_ops = jwk.get("key_ops")
    for name, value in (("kid", kid), ("alg", alg), ("use", use)):
        if value is not None and not isinstance(value, str):
            raise ValueError(f"'{name}' is not a string")
    if key_ops is not None and (
        not isinstance(key_ops, list) or not all(isinstance(op, str) for op in key_ops)
    ):
        raise ValueError("'key_ops' is not a list of strings")
    ops = tuple(key_ops) if key_ops is not None else None
    if kty == "RSA":
        n = int.from_bytes(_b64url_bytes(jwk.get("n")), "big")
        e = int.from_bytes(_b64url_bytes(jwk.get("e")), "big")
        key = crypto.rsa.RSAPublicNumbers(e, n).public_key()
        return _Key(kid, "RSA", None, alg, use, ops, key, bits=key.key_size)
    if kty == "EC":
        crv = jwk.get("crv")
        if not isinstance(crv, str) or crv not in _EC_COORDINATE_BYTES:
            raise ValueError(f"unsupported curve {crv!r}")
        size = _EC_COORDINATE_BYTES[crv]
        x, y = _b64url_bytes(jwk.get("x")), _b64url_bytes(jwk.get("y"))
        if len(x) != size or len(y) != size:
            raise ValueError("coordinates of the wrong length")
        curves = {
            "P-256": crypto.ec.SECP256R1,
            "P-384": crypto.ec.SECP384R1,
            "P-521": crypto.ec.SECP521R1,
        }
        curve = curves[crv]()
        # Checks that the point is on the curve.
        key = crypto.ec.EllipticCurvePublicKey.from_encoded_point(curve, b"\x04" + x + y)
        return _Key(kid, "EC", crv, alg, use, ops, key)
    if kty == "OKP":
        crv = jwk.get("crv")
        expected = _OKP_KEY_BYTES.get(crv) if isinstance(crv, str) else None
        if expected is None:
            raise ValueError(f"unsupported curve {crv!r}")
        x = _b64url_bytes(jwk.get("x"))
        if len(x) != expected:
            raise ValueError("public key of the wrong length")
        if crv == "Ed25519":
            key = crypto.ed25519.Ed25519PublicKey.from_public_bytes(x)
        else:
            key = crypto.ed448.Ed448PublicKey.from_public_bytes(x)
        return _Key(kid, "OKP", crv, alg, use, ops, key)
    if kty == "oct":
        raise ValueError("symmetric (oct) keys are never used")
    raise ValueError(f"unsupported key type {kty!r}")


def _parse_jwks(
    document: Mapping[str, Any], crypto: SimpleNamespace, issuer: str
) -> tuple[_Key, ...]:
    """The usable keys of a JWK Set; each bad key is skipped on its own, with a warning."""
    entries = document.get("keys")
    if not isinstance(entries, list):
        return ()
    keys: list[_Key] = []
    for index, entry in enumerate(entries):
        try:
            keys.append(_parse_jwk(entry, crypto))
        except Exception as exc:  # one bad key must not cost the others
            kid = entry.get("kid") if isinstance(entry, dict) else None
            logger.warning(
                "jwk_skipped: key %s of %s is unusable: %s",
                kid if isinstance(kid, str) else f"#{index}",
                issuer,
                exc,
                extra={"event": {"type": "jwk_skipped", "issuer": issuer}},
            )
    return tuple(keys)


def _select_key(keys: Sequence[_Key], kid: str | None, alg: str, issuer: str) -> _Key | None:
    """The key to verify with; ``None`` when the set has none for this token.

    Raises:
        InvalidTokenError: ``bad_key``, the key the token names may not be
            used with its algorithm.
    """
    if kid is not None:
        named = [key for key in keys if key.kid == kid]
        if not named:
            return None
    else:
        # No kid: the one key whose type and curve suit the algorithm.
        named = [key for key in keys if key.kind_fits(alg)]
        if len(named) != 1:
            return None
    for key in named:
        if key.fits(alg):
            return key
    raise InvalidTokenError("bad_key", issuer=issuer)


@dataclass(slots=True)
class _IssuerState:
    """What is known about one authorization server.  Guarded by the owner's lock."""

    issuer: str
    jwks_uri: str | None = None
    introspection_endpoint: str | None = None
    metadata_at: float | None = None  # when discovery last succeeded
    keys: tuple[_Key, ...] | None = None
    keys_at: float = 0.0
    attempted_at: float | None = None  # last key-set fetch, successful or not
    attempts: int = 0
    # The last key-set fetch to finish failed: keys past their hour then keep
    # answering while the next fetches run in the background.
    refresh_failed: bool = False
    warned_at: float | None = None
    # Introspection mode: after a failed discovery or introspection request,
    # nothing is sent to the authorization server before this time.
    unavailable_until: float = 0.0
    unavailable_stage: str = "introspection"
    # Introspection mode: set with each outage window, cleared by the next
    # discovery or introspection request that succeeds (for /healthz).
    failing: bool = False


@dataclass(slots=True)
class _LoopState:
    """The asyncio objects of one event loop: they cannot be shared between loops.

    Each of them holds its loop, so none is kept once it is no longer in
    use: finished tasks are dropped, and the introspection slots exist only
    while introspections run.  A loop nothing runs on any more can then go.
    """

    refreshes: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    discoveries: dict[str, asyncio.Task[str]] = field(default_factory=dict)
    introspections: dict[str, asyncio.Future[ClientIdentity]] = field(default_factory=dict)
    endpoint_checks: dict[str, asyncio.Task[bool]] = field(default_factory=dict)
    slots: asyncio.Semaphore | None = None
    slot_users: int = 0

    @contextlib.asynccontextmanager
    async def introspection_slot(self) -> AsyncIterator[None]:
        """Hold one of the ``MAX_INTROSPECTIONS_IN_FLIGHT`` slots."""
        if self.slots is None:
            self.slots = asyncio.Semaphore(MAX_INTROSPECTIONS_IN_FLIGHT)
        slots = self.slots
        self.slot_users += 1
        try:
            async with slots:
                yield
        finally:
            self.slot_users -= 1
            if self.slot_users == 0:
                # Nobody holds or waits for a slot: a new semaphore serves
                # the next caller, and this one no longer pins the loop.
                self.slots = None


def _forget_when_done(pending: dict[str, Any], key: str, future: asyncio.Future[Any]) -> None:
    """Drop *future* from *pending* once it is done (a done task still holds its loop)."""

    def forget(done: asyncio.Future[Any]) -> None:
        if pending.get(key) is done:
            del pending[key]
        if not done.cancelled():
            done.exception()  # retrieved, even if every waiter left

    future.add_done_callback(forget)


# ---------------------------------------------------------- public API


@dataclass(frozen=True, slots=True)
class Introspection:
    """Verify tokens with the authorization server's RFC 7662 introspection endpoint.

    The server authenticates to the endpoint with HTTP Basic
    (``client_secret_basic``).  The secret never appears in a ``repr``, a log
    line or an error.

    Args:
        client_id: This resource server's client id at the authorization server.
        client_secret: Its secret.
        endpoint: The introspection endpoint; by default, the
            ``introspection_endpoint`` of the authorization server's metadata.
    """

    client_id: str
    client_secret: str = field(repr=False)
    endpoint: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.client_id, str) or not self.client_id:
            raise ValueError("Introspection client_id must be a non-empty string")
        if not isinstance(self.client_secret, str) or not self.client_secret:
            raise ValueError("Introspection client_secret must be a non-empty string")
        if self.endpoint is not None:
            _check_url(self.endpoint, "introspection endpoint", allow_query=True)


class OAuthResourceServer:
    """Verify OAuth 2.1 bearer tokens for one MCP server, the protected resource.

    Pass it as ``MCPServer(oauth=...)``.  On Streamable HTTP and SSE every
    request then needs a credential: a request without one is answered
    ``401`` with a ``WWW-Authenticate`` challenge pointing at the Protected
    Resource Metadata this object describes, so MCP clients find the
    authorization server and sign the user in.  stdio ignores it, as the
    spec asks.

    Example::

        oauth = OAuthResourceServer(
            resource="https://mcp.example.com/mcp",
            authorization_servers=["https://auth.example.com"],
            required_scopes=["mcp:access"],
        )
        server = MCPServer(host="0.0.0.0", oauth=oauth)

    Args:
        resource: This server's RFC 9728 resource identifier, and the
            audience a token must carry: the public URL of the MCP endpoint.
            ``https``, or ``http`` for a loopback host; no userinfo, query or
            fragment.  Stored in canonical form.
        authorization_servers: The issuers whose tokens are accepted, kept
            byte for byte: a token's ``iss`` must equal one exactly.
        audience: Accepted ``aud`` values, replacing ``resource`` (for an
            authorization server that maps the resource to another
            identifier).
        required_scopes: Scopes every token must hold, all of them.  They
            are also what a ``401`` challenge asks for.
        jwks_uri: Where the signing keys are; by default, from the
            authorization server's metadata.  Only with one authorization
            server.
        algorithms: Allowed JWS algorithms; can only narrow
            :data:`DEFAULT_ALGORITHMS`.
        introspection: Verify every token with RFC 7662 introspection instead
            of locally.  Only with one authorization server.
        step_up: ``True``: a signed-in caller sees every tool, and a call its
            token does not cover gets ``403 insufficient_scope`` naming the
            scope to ask for.  ``False``: protected tools stay invisible to
            tokens that cannot call them, as with API keys.
        clock: Seconds since the epoch; injectable for tests.

    Raises:
        ValueError: A setting is invalid (see each argument).
        ImportError: JWT verification is needed and PyJWT (the ``[oauth]``
            extra) is not installed.
    """

    def __init__(
        self,
        resource: str,
        authorization_servers: str | Sequence[str],
        *,
        audience: str | Sequence[str] | None = None,
        required_scopes: Iterable[str] = (),
        jwks_uri: str | None = None,
        algorithms: Iterable[str] = DEFAULT_ALGORITHMS,
        introspection: Introspection | None = None,
        step_up: bool = True,
        clock: Callable[[], float] = time.time,
    ) -> None:
        _check_url(resource, "resource")
        self._resource = canonical_uri(resource)

        issuers = _as_tuple(authorization_servers, "authorization_servers")
        if not issuers:
            raise ValueError("authorization_servers needs at least one issuer")
        for issuer in issuers:
            _check_url(issuer, "authorization server")
        self._issuers = issuers

        audiences = _as_tuple(audience, "audience") if audience is not None else (self._resource,)
        if not audiences:
            raise ValueError("audience needs at least one value")
        self._audience = audiences
        self._audience_canonical = frozenset(
            form for form in (_canonical_or_none(value) for value in audiences) if form
        )

        scopes = _as_tuple(required_scopes, "required_scopes")
        for scope in scopes:
            if not is_scope_token(scope):
                raise ValueError(
                    f"required scope {scope!r} is not a valid scope-token "
                    "(no spaces, quotes or backslashes)"
                )
            if scope == "offline_access":
                raise ValueError(
                    "offline_access is not a resource requirement and must not be required"
                )
        self._required_scopes = scopes

        allowed = _as_tuple(algorithms, "algorithms")
        if not allowed:
            raise ValueError("algorithms needs at least one algorithm")
        for alg in allowed:
            if alg.lower() == "none" or alg.upper().startswith("HS"):
                raise ValueError(
                    f"algorithm {alg!r} is refused: symmetric and unsigned tokens allow "
                    "algorithm confusion (RFC 8725); use asymmetric algorithms only"
                )
            if alg not in DEFAULT_ALGORITHMS:
                raise ValueError(
                    f"unknown algorithm {alg!r}; choose from {', '.join(DEFAULT_ALGORITHMS)}"
                )
        self._algorithms = allowed

        if jwks_uri is not None:
            if len(issuers) != 1:
                raise ValueError(
                    "jwks_uri needs exactly one authorization server: keys must belong to "
                    "the issuer whose tokens they verify"
                )
            _check_url(jwks_uri, "jwks_uri", allow_query=True)
        self._configured_jwks_uri = jwks_uri

        if introspection is not None:
            if not isinstance(introspection, Introspection):
                raise ValueError("introspection must be an Introspection")
            if len(issuers) != 1:
                raise ValueError(
                    "introspection needs exactly one authorization server: an opaque "
                    "token does not say who issued it, and must not be sent to the others"
                )
        self._introspection = introspection
        self._step_up = bool(step_up)
        self._clock = clock

        if introspection is None:
            self._jwt, self._crypto = _load_jwt()
        else:
            self._jwt, self._crypto = None, SimpleNamespace()

        path = urlsplit(self._resource).path
        self._metadata_path = "/.well-known/oauth-protected-resource" + path
        origin = urlsplit(self._resource)
        self._metadata_url = urlunsplit((origin.scheme, origin.netloc, self._metadata_path, "", ""))

        # Caches, guarded by a thread lock: the same object may serve several
        # event loops.  The asyncio primitives are per loop.
        self._lock = threading.Lock()
        self._states = {
            issuer: _IssuerState(
                issuer,
                jwks_uri=jwks_uri,
                introspection_endpoint=introspection.endpoint if introspection else None,
            )
            for issuer in issuers
        }
        self._loops: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _LoopState] = (
            weakref.WeakKeyDictionary()
        )
        self._introspected: OrderedDict[str, tuple[float, ClientIdentity | str]] = OrderedDict()
        self._executor: ThreadPoolExecutor | None = None
        self._warned_no_aud = False

    @classmethod
    def from_env(cls, prefix: str = "EASY_MCP_OAUTH_", **overrides: Any) -> OAuthResourceServer:
        """Build one from environment variables (lists are whitespace- or comma-separated).

        ``<prefix>RESOURCE`` and ``<prefix>AUTHORIZATION_SERVERS`` are
        required; ``AUDIENCE``, ``REQUIRED_SCOPES`` and ``JWKS_URI`` are
        optional, and ``INTROSPECTION_CLIENT_ID`` with
        ``INTROSPECTION_CLIENT_SECRET`` (and optionally
        ``INTROSPECTION_ENDPOINT``) select introspection.  *overrides* set
        what the environment does not carry, e.g. ``step_up=False``.

        Raises:
            ValueError: A required variable is missing, or only one of the
                introspection client id and secret is set.
        """

        def env(name: str) -> str | None:
            value = os.environ.get(prefix + name)
            return value.strip() if value and value.strip() else None

        resource = env("RESOURCE")
        if resource is None:
            raise ValueError(f"environment variable {prefix}RESOURCE is not set or empty")
        issuers = _split_env(env("AUTHORIZATION_SERVERS"))
        if not issuers:
            raise ValueError(
                f"environment variable {prefix}AUTHORIZATION_SERVERS is not set or empty"
            )
        options: dict[str, Any] = {}
        if audience := _split_env(env("AUDIENCE")):
            options["audience"] = audience
        if scopes := _split_env(env("REQUIRED_SCOPES")):
            options["required_scopes"] = scopes
        if (jwks_uri := env("JWKS_URI")) is not None:
            options["jwks_uri"] = jwks_uri
        client_id = env("INTROSPECTION_CLIENT_ID")
        secret = env("INTROSPECTION_CLIENT_SECRET")
        if (client_id is None) != (secret is None):
            raise ValueError(
                f"set both {prefix}INTROSPECTION_CLIENT_ID and "
                f"{prefix}INTROSPECTION_CLIENT_SECRET, or neither"
            )
        if client_id is not None and secret is not None:
            options["introspection"] = Introspection(
                client_id, secret, endpoint=env("INTROSPECTION_ENDPOINT")
            )
        options.update(overrides)
        return cls(resource, issuers, **options)

    # ------------------------------------------------------------ settings

    @property
    def resource(self) -> str:
        """The canonical resource identifier, exactly what the metadata document says."""
        return self._resource

    @property
    def authorization_servers(self) -> tuple[str, ...]:
        """The accepted issuers, as configured."""
        return self._issuers

    @property
    def audience(self) -> tuple[str, ...]:
        """The accepted ``aud`` values."""
        return self._audience

    @property
    def required_scopes(self) -> tuple[str, ...]:
        """Scopes every token must hold."""
        return self._required_scopes

    @property
    def algorithms(self) -> tuple[str, ...]:
        """The accepted JWS algorithms."""
        return self._algorithms

    @property
    def introspection(self) -> Introspection | None:
        """The introspection settings, or ``None`` for local JWT verification."""
        return self._introspection

    @property
    def step_up(self) -> bool:
        """Whether signed-in callers see every tool and are challenged for missing scopes."""
        return self._step_up

    @property
    def metadata_path(self) -> str:
        """The metadata path: ``/.well-known/oauth-protected-resource`` + the resource's path."""
        return self._metadata_path

    @property
    def metadata_url(self) -> str:
        """The absolute metadata URL: the ``resource_metadata`` of every challenge."""
        return self._metadata_url

    def __repr__(self) -> str:
        mode = "introspection" if self._introspection is not None else "jwt"
        return (
            f"OAuthResourceServer(resource={self._resource!r}, "
            f"authorization_servers={list(self._issuers)!r}, mode={mode!r})"
        )

    def _metadata(self, *, resource_name: str, scopes: Iterable[str]) -> dict[str, Any]:
        """The Protected Resource Metadata document (RFC 9728), zero values omitted."""
        document: dict[str, Any] = {
            "authorization_servers": list(self._issuers),
            "bearer_methods_supported": ["header"],
            "resource": self._resource,
            "resource_name": resource_name,
        }
        listed = [scope for scope in dict.fromkeys(scopes) if scope != "offline_access"]
        if listed:
            document["scopes_supported"] = listed
        return document

    def _ready(self) -> bool:
        """Whether tokens can be verified now, as ``/healthz`` reports it.

        With JWTs: every issuer's signing keys are cached.  With
        introspection: every endpoint is known, and the authorization server
        did not fail the last discovery or introspection request (one token
        refused by it does not count).
        """
        with self._lock:
            if self._introspection is not None:
                return all(
                    state.introspection_endpoint and not state.failing
                    for state in self._states.values()
                )
            return all(state.keys for state in self._states.values())

    # ---------------------------------------------------------- lifecycle

    async def warm_up(self) -> None:
        """Fetch metadata and keys for every issuer ahead of the first client.

        Best effort: a failure is logged, never raised, and the first
        requests then fetch (or get ``503`` until the issuer answers).
        """
        for issuer in self._issuers:
            try:
                if self._introspection is not None:
                    await self._introspection_endpoint(self._states[issuer])
                else:
                    await self._signing_keys(self._states[issuer])
            except Exception as exc:
                logger.warning("oauth warm-up for %s failed: %s", issuer, exc)

    def close(self) -> None:
        """Release the fetch threads; a later fetch starts new ones."""
        with self._lock:
            executor, self._executor = self._executor, None
        if executor is not None:
            executor.shutdown(wait=False)

    def _get_executor(self) -> ThreadPoolExecutor:
        with self._lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=FETCH_WORKERS, thread_name_prefix="easy-mcp-oauth"
                )
            return self._executor

    def _loop_state(self) -> _LoopState:
        loop = asyncio.get_running_loop()
        with self._lock:
            state = self._loops.get(loop)
            if state is None:
                state = self._loops[loop] = _LoopState()
            return state

    # -------------------------------------------------------- verification

    async def verify(self, token: str) -> ClientIdentity:
        """Verify an access token; returns the identity it grants.

        Raises:
            InvalidTokenError: The token is not valid for this resource; its
                ``reason`` says why.
            AuthServerUnavailableError: The authorization server's metadata,
                keys or introspection endpoint could not be reached and
                nothing usable is cached.
        """
        if len(token) > MAX_TOKEN_BYTES:
            raise InvalidTokenError("too_large")
        if not _B64TOKEN.fullmatch(token):
            raise InvalidTokenError("malformed")
        if self._introspection is not None:
            return await self._introspect(token)
        return await self._verify_jwt(token)

    async def _verify_jwt(self, token: str) -> ClientIdentity:
        parts = token.split(".")
        if len(parts) == 5:
            raise InvalidTokenError("encrypted")
        if len(parts) != 3:
            raise InvalidTokenError("malformed")
        try:
            header = _json_segment(parts[0])
            payload = _json_segment(parts[1])
            # An empty signature parses: alg "none" is then refused by name.
            if parts[2] and not _B64URL.fullmatch(parts[2]):
                raise ValueError("signature is not base64url")
        except Exception:  # deep nesting raises RecursionError: still just malformed
            raise InvalidTokenError("malformed") from None

        alg = header.get("alg")
        if not isinstance(alg, str) or alg not in self._algorithms:
            raise InvalidTokenError("unsupported_alg")
        typ = header.get("typ")
        if typ is not None and (not isinstance(typ, str) or typ.lower() not in _ACCEPTED_TYPES):
            raise InvalidTokenError("bad_type")
        if "crit" in header:
            raise InvalidTokenError("crit")
        kid = header.get("kid")
        if kid is not None and not isinstance(kid, str):
            raise InvalidTokenError("malformed")
        # jku, x5u, jwk and x5c are never read: keys come from the issuer only.

        # Before any network I/O: a forged token cannot make us fetch anything.
        issuer = payload.get("iss")
        if issuer is None:
            raise InvalidTokenError("missing_claims")
        if not isinstance(issuer, str) or issuer not in self._states:
            raise InvalidTokenError("wrong_issuer")
        state = self._states[issuer]

        keys = await self._signing_keys(state)
        key = _select_key(keys, kid, alg, issuer)
        if key is None:
            # A key rotated in since the last fetch: refetch, cooldown permitting.
            keys = await self._signing_keys(state, unknown_kid=True)
            key = _select_key(keys, kid, alg, issuer)
        if key is None:
            raise InvalidTokenError("unknown_key", issuer=issuer)

        claims = self._decode(token, key, alg, issuer)
        return self._identity_from_claims(claims, issuer)

    def _decode(self, token: str, key: _Key, alg: str, issuer: str) -> dict[str, Any]:
        """Check the signature with PyJWT; every claim is checked here, not there."""
        jwt = self._jwt
        try:
            claims = jwt.decode(
                token,
                key=key.key,
                algorithms=[alg],
                # A new dict on every call: a shared one could be changed
                # under a later call (CVE-2026-103001).
                options={
                    "verify_signature": True,
                    "verify_exp": False,
                    "verify_nbf": False,
                    "verify_iat": False,
                    "verify_aud": False,
                    "verify_iss": False,
                    "verify_sub": False,
                    "verify_jti": False,
                    "require": ["exp", "iss", "aud"],
                },
            )
        except jwt.InvalidSignatureError:
            raise InvalidTokenError("bad_signature", issuer=issuer) from None
        except jwt.MissingRequiredClaimError:
            raise InvalidTokenError("missing_claims", issuer=issuer) from None
        except jwt.DecodeError:
            raise InvalidTokenError("malformed", issuer=issuer) from None
        except Exception:  # whatever else PyJWT or cryptography raises
            raise InvalidTokenError("bad_signature", issuer=issuer) from None
        if not isinstance(claims, dict):
            raise InvalidTokenError("malformed", issuer=issuer)
        return claims

    def _identity_from_claims(self, claims: dict[str, Any], issuer: str) -> ClientIdentity:
        if claims.get("iss") != issuer:
            raise InvalidTokenError("wrong_issuer")
        now = self._clock()
        exp = _number(claims.get("exp"))
        if exp is None:
            raise InvalidTokenError("malformed", issuer=issuer)
        if exp <= now - LEEWAY_SECONDS:
            raise InvalidTokenError("expired", issuer=issuer)
        for name in ("nbf", "iat"):
            if name in claims:
                value = _number(claims[name])
                if value is None:
                    raise InvalidTokenError("malformed", issuer=issuer)
                if value > now + LEEWAY_SECONDS:
                    raise InvalidTokenError("not_yet_valid", issuer=issuer)
        if not self._audience_ok(claims.get("aud")):
            raise InvalidTokenError("wrong_audience", issuer=issuer)
        subject, client_id = self._principal(claims, issuer, extra_client_claim="azp")
        if "cnf" in claims:
            raise InvalidTokenError("bound_token", issuer=issuer)
        scope = claims.get("scope")
        scopes = _scopes_from(scope if scope is not None else claims.get("scp"))
        return ClientIdentity(
            fingerprint=principal_fingerprint(issuer, subject, client_id),
            scopes=scopes,
            subject=subject,
            client_id=client_id,
            issuer=issuer,
            expires_at=int(exp),
            claims=_frozen_claims(claims, issuer),
        )

    @staticmethod
    def _principal(
        claims: Mapping[str, Any], issuer: str, *, extra_client_claim: str | None
    ) -> tuple[str | None, str | None]:
        """The token's subject and OAuth client; at least one must be there."""
        subject = claims.get("sub")
        client_id = claims.get("client_id")
        if client_id is None and extra_client_claim is not None:
            client_id = claims.get(extra_client_claim)
        for value in (subject, client_id):
            if value is not None and not isinstance(value, str):
                raise InvalidTokenError("malformed", issuer=issuer)
        if not subject and not client_id:
            raise InvalidTokenError("missing_claims", issuer=issuer)
        return subject or None, client_id or None

    def _audience_ok(self, aud: object) -> bool:
        values = [aud] if isinstance(aud, str) else aud if isinstance(aud, list) else []
        for value in values:
            if not isinstance(value, str):
                continue
            if value in self._audience:
                return True
            canonical = _canonical_or_none(value)
            if canonical is not None and canonical in self._audience_canonical:
                return True
        return False

    # ----------------------------------------------------------- key sets

    def _wants_fetch(self, state: _IssuerState, unknown_kid: bool, now: float) -> bool:
        if state.keys is None:
            return (
                state.attempted_at is None or now - state.attempted_at >= UNAVAILABLE_RETRY_SECONDS
            )
        if unknown_kid or now - state.keys_at >= KEYS_TTL_SECONDS - KEY_REFRESH_AHEAD_SECONDS:
            return (
                state.attempted_at is None
                or now - state.attempted_at >= KEY_REFRESH_COOLDOWN_SECONDS
            )
        return False

    async def _signing_keys(
        self, state: _IssuerState, *, unknown_kid: bool = False
    ) -> tuple[_Key, ...]:
        """The issuer's keys: cached, or fetched (one fetch at a time per issuer and loop).

        The hourly refresh starts ``KEY_REFRESH_AHEAD_SECONDS`` early and runs
        in the background while the cached keys keep answering, so on a busy
        server no request waits for it.  A request waits for a fetch only
        when it cannot go on without one: no keys are cached, its token
        names a key the cache lacks, or the cached keys are an hour old (the
        server was idle, or the refresh has not finished), so a key the
        authorization server withdrew stops working within the hour.  Once a
        refresh has failed, keys past their hour keep answering while the
        next ones run in the background, so a silent authorization server
        holds up requests once, not every 30 s.  A request that needs a fetch
        also waits for one already running, which the cooldown would not let
        it start.

        Raises:
            AuthServerUnavailableError: No usable keys are cached and none
                could be fetched.
        """
        with self._lock:
            keys = state.keys
            now = self._clock()
            wanted = self._wants_fetch(state, unknown_kid, now)
            expired = (
                keys is not None
                and now - state.keys_at >= KEYS_TTL_SECONDS
                and not state.refresh_failed
            )
        needed = keys is None or unknown_kid or expired
        if wanted or needed:
            refreshes = self._loop_state().refreshes
            task = refreshes.get(state.issuer)
            if task is not None and task.done():
                task = None
            if task is None and wanted:
                # No await between the look and the start: requests arriving
                # meanwhile join this fetch instead of starting their own.
                task = refreshes[state.issuer] = asyncio.ensure_future(self._refresh_keys(state))
                _forget_when_done(refreshes, state.issuer, task)
            if task is not None and needed:
                # Shielded: the fetch serves every request waiting on it, so
                # one that is cancelled must not stop it.
                await asyncio.shield(task)
                with self._lock:
                    keys = state.keys
        if not keys:
            raise AuthServerUnavailableError(issuer=state.issuer, stage="keys")
        return keys

    async def _refresh_keys(self, state: _IssuerState) -> None:
        """Fetch the issuer's key set (and, hourly, its metadata); never raises.

        A fetch that fails keeps the cached keys.  A key set that arrives
        with no usable key withdraws them: the authorization server answered,
        and every key it no longer publishes must stop working.  A fetch cut
        short because its event loop is closing (``asyncio.run()`` returning,
        say) learnt nothing: the next request may try again at once.
        """
        now = self._clock()
        with self._lock:
            state.attempted_at = now
            state.attempts += 1
            attempt = state.attempts
            stale = state.keys is not None
        stage = "metadata"
        try:
            jwks_uri = await self._find_jwks_uri(state, now)
            stage = "keys"
            document = await self._get_json(jwks_uri, MAX_DOCUMENT_BYTES)
        except asyncio.CancelledError:
            # Nothing learnt: the next request may try again at once and, past
            # the hour, waits for that fetch (a loop that closes after every
            # verify() would otherwise never see one finish).
            with self._lock:
                if state.attempts == attempt:  # no other fetch started since
                    state.attempted_at = None
                    state.refresh_failed = False
            raise
        except (_fetch.FetchError, AuthServerUnavailableError) as exc:
            with self._lock:
                state.refresh_failed = True
            if stale:
                with self._lock:
                    warn = state.warned_at is None or now - state.warned_at >= KEYS_TTL_SECONDS
                    if warn:
                        state.warned_at = now
                if warn:
                    logger.warning(
                        "jwks_refresh_failed: keeping the cached keys of %s: %s",
                        state.issuer,
                        exc,
                        extra={"event": {"type": "jwks_refresh_failed", "issuer": state.issuer}},
                    )
                return
            logger.error("no usable signing keys for %s (%s): %s", state.issuer, stage, exc)
            audit("auth_unavailable", issuer=state.issuer, stage=stage)
            return
        keys = _parse_jwks(document, self._crypto, state.issuer)
        if not keys:
            with self._lock:
                state.keys = None
                state.keys_at = now
                state.refresh_failed = False
                state.warned_at = None
            logger.error(
                "no usable signing keys for %s (keys): the key set at %s holds no usable key",
                state.issuer,
                jwks_uri,
            )
            audit("auth_unavailable", issuer=state.issuer, stage="keys")
            return
        with self._lock:
            state.keys = keys
            state.keys_at = now
            state.refresh_failed = False
            state.warned_at = None
        logger.debug(
            "jwks_refreshed: %d key(s) for %s",
            len(keys),
            state.issuer,
            extra={"event": {"type": "jwks_refreshed", "issuer": state.issuer}},
        )

    async def _find_jwks_uri(self, state: _IssuerState, now: float) -> str:
        if self._configured_jwks_uri is not None:
            return self._configured_jwks_uri
        with self._lock:
            known = state.jwks_uri
            # Refreshed with the hourly key set, which starts ahead of the hour.
            fresh = (
                state.metadata_at is not None
                and now - state.metadata_at < KEYS_TTL_SECONDS - KEY_REFRESH_AHEAD_SECONDS
            )
        if known is not None and fresh:
            return known
        metadata = await self._discover(state.issuer)
        jwks_uri = metadata.get("jwks_uri")
        if not isinstance(jwks_uri, str) or not _fetch.allowed_url(jwks_uri):
            logger.error("the metadata of %s has no usable jwks_uri", state.issuer)
            raise _fetch.FetchError("no usable jwks_uri in the authorization server metadata")
        with self._lock:
            state.jwks_uri = jwks_uri
            state.metadata_at = now
        return jwks_uri

    async def _discover(self, issuer: str) -> dict[str, Any]:
        """The issuer's metadata, found as MCP clients look for it (RFC 8414, OIDC).

        Raises:
            FetchError: No candidate URL answered with this issuer's metadata.
        """
        parts = urlsplit(issuer)
        path = parts.path.rstrip("/")
        origin = f"{parts.scheme}://{parts.netloc}"
        candidates = list(
            dict.fromkeys(
                [
                    f"{origin}/.well-known/oauth-authorization-server{path}",
                    f"{origin}/.well-known/openid-configuration{path}",
                    f"{origin}{path}/.well-known/openid-configuration",
                ]
            )
        )
        failures: list[str] = []
        for url in candidates:
            try:
                document = await self._get_json(url, MAX_DOCUMENT_BYTES)
            except _fetch.FetchError as exc:
                failures.append(str(exc))
                continue
            # RFC 8414 section 3.3: the document must be the issuer's own.
            if document.get("issuer") != issuer:
                logger.error(
                    "authorization server metadata at %s names issuer %r, not %r; ignored",
                    url,
                    document.get("issuer"),
                    issuer,
                )
                failures.append(f"{url} names another issuer")
                continue
            return document
        raise _fetch.FetchError(
            f"no metadata for {issuer}: " + "; ".join(failures or ["no candidate URL"])
        )

    async def _get_json(self, url: str, max_bytes: int) -> dict[str, Any]:
        return await _fetch.fetch_json(
            url,
            max_bytes=max_bytes,
            timeout=REQUEST_TIMEOUT_SECONDS,
            executor=self._get_executor(),
        )

    # ------------------------------------------------------- introspection

    def _check_available(self, state: _IssuerState) -> None:
        """Refuse at once, without a request, while an outage window is open.

        Raises:
            AuthServerUnavailableError: The authorization server failed less
                than ``UNAVAILABLE_RETRY_SECONDS`` ago.
        """
        with self._lock:
            unavailable = self._clock() < state.unavailable_until
            stage = state.unavailable_stage
        if unavailable:
            raise AuthServerUnavailableError(issuer=state.issuer, stage=stage)

    def _open_outage(self, state: _IssuerState, stage: str) -> bool:
        """Open (or extend) the outage window after a failed request to the issuer.

        Returns whether it was closed until now: the failure is then logged
        and audited, once per window rather than once per request.
        """
        now = self._clock()
        with self._lock:
            opened = now >= state.unavailable_until
            state.unavailable_until = now + UNAVAILABLE_RETRY_SECONDS
            state.unavailable_stage = stage
            state.failing = True
        return opened

    async def _introspection_endpoint(self, state: _IssuerState) -> str:
        """The configured or discovered introspection endpoint.

        Discovery runs once at a time per issuer and loop, shared by every
        token waiting for it, and after a failure not again for
        ``UNAVAILABLE_RETRY_SECONDS``.

        Raises:
            AuthServerUnavailableError: No usable endpoint could be found.
        """
        with self._lock:
            known = state.introspection_endpoint
        if known is not None:
            return known
        self._check_available(state)
        discoveries = self._loop_state().discoveries
        task = discoveries.get(state.issuer)
        if task is None or task.done():
            task = discoveries[state.issuer] = asyncio.ensure_future(
                self._discover_introspection_endpoint(state)
            )
            _forget_when_done(discoveries, state.issuer, task)
        # Shielded: the discovery serves every token waiting on it.
        return await asyncio.shield(task)

    async def _discover_introspection_endpoint(self, state: _IssuerState) -> str:
        try:
            metadata = await self._discover(state.issuer)
        except _fetch.FetchError as exc:
            if self._open_outage(state, "metadata"):
                logger.error("cannot find the introspection endpoint of %s: %s", state.issuer, exc)
                audit("auth_unavailable", issuer=state.issuer, stage="metadata")
            raise AuthServerUnavailableError(issuer=state.issuer, stage="metadata") from None
        endpoint = metadata.get("introspection_endpoint")
        if not isinstance(endpoint, str) or not _fetch.allowed_url(endpoint):
            if self._open_outage(state, "metadata"):
                logger.error(
                    "the metadata of %s has no usable introspection_endpoint", state.issuer
                )
                audit("auth_unavailable", issuer=state.issuer, stage="metadata")
            raise AuthServerUnavailableError(issuer=state.issuer, stage="metadata")
        with self._lock:
            state.introspection_endpoint = endpoint
            state.failing = False
        return endpoint

    def _cached_introspection(self, digest: str, now: float) -> ClientIdentity | None:
        with self._lock:
            entry = self._introspected.get(digest)
            if entry is None:
                return None
            until, answer = entry
            if until <= now:
                del self._introspected[digest]
                return None
            self._introspected.move_to_end(digest)
        if isinstance(answer, str):
            raise InvalidTokenError(answer, issuer=self._issuers[0])
        return answer

    def _remember(self, digest: str, until: float, answer: ClientIdentity | str) -> None:
        with self._lock:
            self._introspected[digest] = (until, answer)
            self._introspected.move_to_end(digest)
            while len(self._introspected) > INTROSPECTION_CACHE_SIZE:
                self._introspected.popitem(last=False)

    async def _introspect(self, token: str) -> ClientIdentity:
        # The cache is keyed by a hash: the raw token is never kept.
        digest = _token_hash(token)
        cached = self._cached_introspection(digest, self._clock())
        if cached is not None:
            return cached
        loop_state = self._loop_state()
        pending = loop_state.introspections.get(digest)
        if pending is None:
            # Concurrent lookups of one token share one request.
            pending = asyncio.ensure_future(self._introspect_once(token, digest, loop_state))
            loop_state.introspections[digest] = pending
            _forget_when_done(loop_state.introspections, digest, pending)
        # Shielded: a cancelled waiter must not stop the others' answer.
        return await asyncio.shield(pending)

    async def _post_introspection(self, endpoint: str, token: str) -> dict[str, Any]:
        introspection = self._introspection
        assert introspection is not None
        return await _fetch.post_form_json(
            endpoint,
            {"token": token, "token_type_hint": "access_token"},
            auth=(introspection.client_id, introspection.client_secret),
            max_bytes=MAX_INTROSPECTION_BYTES,
            timeout=REQUEST_TIMEOUT_SECONDS,
            executor=self._get_executor(),
        )

    async def _introspect_once(
        self, token: str, digest: str, loop_state: _LoopState
    ) -> ClientIdentity:
        state = self._states[self._issuers[0]]
        self._check_available(state)
        # Before taking a slot: the slots bound introspection requests only,
        # and one discovery serves every token.
        endpoint = await self._introspection_endpoint(state)
        answer: dict[str, Any] | None = None
        failure: _fetch.FetchError | None = None
        async with loop_state.introspection_slot():
            # An outage found while this waited for its slot: no request.
            self._check_available(state)
            try:
                answer = await self._post_introspection(endpoint, token)
            except _fetch.FetchError as exc:
                failure = exc
        if failure is not None and not failure.malformed:
            # Outside the slot: telling the server's failure from this token's
            # may take a request of its own.
            await self._introspection_failed(state, endpoint, failure, loop_state)
            raise AuthServerUnavailableError(
                issuer=state.issuer, stage="introspection", sent_request=not failure.queued
            )
        with self._lock:
            state.failing = False
        now = self._clock()
        try:
            if answer is None:
                # JSON nested too deeply to parse: this token's answer, malformed.
                raise InvalidTokenError("malformed", issuer=state.issuer)
            identity = self._identity_from_introspection(answer, state.issuer, now)
        except InvalidTokenError as exc:
            self._remember(digest, now + INTROSPECTION_REFUSAL_TTL_SECONDS, exc.reason)
            raise
        until = now + INTROSPECTION_TTL_SECONDS
        if identity.expires_at is not None:
            until = min(until, float(identity.expires_at))  # RFC 7662 section 4
        self._remember(digest, until, identity)
        return identity

    async def _introspection_failed(
        self,
        state: _IssuerState,
        endpoint: str,
        exc: _fetch.FetchError,
        loop_state: _LoopState,
    ) -> None:
        """Log a failed introspection request; open the outage window if the server is down.

        The token sent can make its own request fail: a filter in front of
        the endpoint that matches it may refuse it (``4xx``), answer it with
        a page that is no JSON (``200`` included), drop it or reset the
        connection, or a proxy may answer ``5xx``.  So only a ``401`` (this
        server's client credentials refused) or a ``429`` shows the server
        down by itself.  When the endpoint could not be reached, timed out,
        answered ``5xx``, or answered ``2xx`` with no JSON object, it is asked
        about a token of no account (:meth:`_endpoint_answers`): the server is
        down only if that fails too.  Any other ``4xx``, an answer over the
        size cap, or a request that never got a fetch thread fails only the
        token sent, so no token can shut the others out.

        When the server is down, nothing is sent to it for
        ``UNAVAILABLE_RETRY_SECONDS``, and the failure is logged and audited
        once per window.
        """
        status = exc.status
        if exc.queued or exc.too_large:
            down = False
        elif status in (401, 429):
            down = True
        elif status is None or status >= 500 or 200 <= status < 300:
            down = not await self._endpoint_answers(state, endpoint, loop_state)
        else:
            down = False
        if not down:
            logger.error("token introspection failed: %s", exc)
            audit("auth_unavailable", issuer=state.issuer, stage="introspection")
            return
        if not self._open_outage(state, "introspection"):
            return
        if status == 401:
            logger.error(
                "introspection_credentials_rejected: %s refused this server's client "
                "credentials (HTTP 401); check the introspection client id and secret",
                endpoint,
                extra={
                    "event": {"type": "introspection_credentials_rejected", "issuer": state.issuer}
                },
            )
        else:
            logger.error("token introspection failed: %s", exc)
        audit("auth_unavailable", issuer=state.issuer, stage="introspection")

    async def _endpoint_answers(
        self, state: _IssuerState, endpoint: str, loop_state: _LoopState
    ) -> bool:
        """Whether the introspection endpoint answers a token of no account.

        Asked after a request failed in a way one token can cause.  One such
        request runs at a time per issuer and loop, shared by every failure
        waiting on it, outside the introspection slots; none is sent while
        an outage window is open.
        """
        with self._lock:
            if self._clock() < state.unavailable_until:
                return False
        checks = loop_state.endpoint_checks
        task = checks.get(state.issuer)
        if task is None or task.done():
            task = checks[state.issuer] = asyncio.ensure_future(
                self._check_endpoint(state, endpoint)
            )
            _forget_when_done(checks, state.issuer, task)
        # Shielded: the check serves every failure waiting on it.
        return await asyncio.shield(task)

    async def _check_endpoint(self, state: _IssuerState, endpoint: str) -> bool:
        try:
            # A random token: a working endpoint answers {"active": false}.
            await self._post_introspection(endpoint, secrets.token_urlsafe(32))
        except _fetch.FetchError:
            return False
        with self._lock:
            state.failing = False
        return True

    def _identity_from_introspection(
        self, answer: Mapping[str, Any], issuer: str, now: float
    ) -> ClientIdentity:
        if answer.get("active") is not True:
            raise InvalidTokenError("inactive", issuer=issuer)
        if "iss" in answer and answer["iss"] != issuer:
            raise InvalidTokenError("wrong_issuer")
        if "aud" not in answer:
            if not self._warned_no_aud:
                self._warned_no_aud = True
                logger.error(
                    "the introspection answers of %s carry no 'aud', so no token can be "
                    "accepted: enable the audience in its introspection responses",
                    issuer,
                )
            raise InvalidTokenError("wrong_audience", issuer=issuer)
        if not self._audience_ok(answer.get("aud")):
            raise InvalidTokenError("wrong_audience", issuer=issuer)
        expires_at: int | None = None
        if "exp" in answer:
            exp = _number(answer["exp"])
            if exp is None:
                raise InvalidTokenError("malformed", issuer=issuer)
            if exp <= now - LEEWAY_SECONDS:
                raise InvalidTokenError("expired", issuer=issuer)
            expires_at = int(exp)
        if "nbf" in answer:
            nbf = _number(answer["nbf"])
            if nbf is None:
                raise InvalidTokenError("malformed", issuer=issuer)
            if nbf > now + LEEWAY_SECONDS:
                raise InvalidTokenError("not_yet_valid", issuer=issuer)
        token_type = answer.get("token_type")
        if token_type is not None and (
            not isinstance(token_type, str) or token_type.lower() not in ("bearer", "access_token")
        ):
            raise InvalidTokenError("wrong_token_type", issuer=issuer)
        if "cnf" in answer:
            raise InvalidTokenError("bound_token", issuer=issuer)
        subject, client_id = self._principal(answer, issuer, extra_client_claim=None)
        return ClientIdentity(
            fingerprint=principal_fingerprint(issuer, subject, client_id),
            scopes=_scopes_from(answer.get("scope")),
            subject=subject,
            client_id=client_id,
            issuer=issuer,
            expires_at=expires_at,
            claims=_frozen_claims(answer, issuer),
        )
