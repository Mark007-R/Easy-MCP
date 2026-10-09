"""JWT access-token verification: every check, every refusal reason, key management.

Fetching is replaced by an in-memory authorization server (``FakeFetch``), and
time by ``clock=``, so these run without a network and without waiting.  The
fetch rules themselves (no redirects, size caps, https) are tested against the
local server of tests/oauth_fake_as.py.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import dataclasses
import datetime
import gc
import hashlib
import hmac
import ipaddress
import itertools
import json
import math
import pickle
import socket
import ssl
import threading
import time
import weakref
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from conftest import LogCapture
from oauth_fake_as import SigningKey, b64url, craft, mint_token

from easy_mcp import (
    APIKeyAuth,
    AuthServerUnavailableError,
    ClientIdentity,
    InvalidTokenError,
    OAuthResourceServer,
)
from easy_mcp.security import _fetch
from easy_mcp.security.oauth import (
    KEY_REFRESH_AHEAD_SECONDS,
    KEYS_TTL_SECONDS,
    MAX_CLAIM_DEPTH,
    MAX_DOCUMENT_BYTES,
    MAX_TOKEN_BYTES,
    principal_fingerprint,
)

ISSUER = "https://auth.example.com"
RESOURCE = "https://mcp.example.com/mcp"
METADATA_URL = f"{ISSUER}/.well-known/oauth-authorization-server"
OIDC_URL = f"{ISSUER}/.well-known/openid-configuration"
JWKS_URL = f"{ISSUER}/jwks"
NOW = 1_800_000_000.0


class Clock:
    def __init__(self, now: float = NOW) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class FakeFetch:
    """The authorization server's documents, served from memory; counts every fetch."""

    def __init__(self, keys: list[SigningKey], issuer: str = ISSUER) -> None:
        self.keys = keys
        self.issuer = issuer
        self.calls: Counter[str] = Counter()
        self.max_bytes: dict[str, int] = {}  # the cap each URL was last fetched with
        self.down = False
        self.documents: dict[str, dict[str, Any]] = {
            f"{issuer}/.well-known/oauth-authorization-server": {
                "issuer": issuer,
                "jwks_uri": f"{issuer}/jwks",
            }
        }
        self.raw_keys: list[Any] | None = None
        self.delay = 0.0

    async def __call__(
        self, url: str, *, max_bytes: int, timeout: float, executor: Any
    ) -> dict[str, Any]:
        self.calls[url] += 1
        self.max_bytes[url] = max_bytes
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.down:
            raise _fetch.FetchError(f"{url} unreachable")
        if url == f"{self.issuer}/jwks":
            if self.raw_keys is not None:
                return {"keys": self.raw_keys}
            return {"keys": [key.public_jwk() for key in self.keys]}
        if url in self.documents:
            return self.documents[url]
        raise _fetch.FetchError(f"{url} answered HTTP 404", status=404)

    @property
    def key_fetches(self) -> int:
        return self.calls[f"{self.issuer}/jwks"]

    @property
    def total(self) -> int:
        return sum(self.calls.values())


@pytest.fixture
def rsa(rsa_key: Any) -> SigningKey:
    return SigningKey("k1", "RS256", rsa_key)


@pytest.fixture
def fetch(monkeypatch: pytest.MonkeyPatch, rsa: SigningKey) -> Iterator[FakeFetch]:
    fake = FakeFetch([rsa])
    monkeypatch.setattr(_fetch, "fetch_json", fake)
    yield fake


@pytest.fixture
def clock() -> Clock:
    return Clock()


def make(clock: Clock, **kwargs: Any) -> OAuthResourceServer:
    kwargs.setdefault("authorization_servers", [ISSUER])
    return OAuthResourceServer(RESOURCE, clock=clock, **kwargs)


def mint(key: SigningKey, clock: Clock, **kwargs: Any) -> str:
    kwargs.setdefault("issuer", ISSUER)
    kwargs.setdefault("audience", RESOURCE)
    return mint_token(key, now=clock.now, **kwargs)


async def reason(oauth: OAuthResourceServer, token: str) -> str:
    with pytest.raises(InvalidTokenError) as caught:
        await oauth.verify(token)
    return caught.value.reason


async def settle(oauth: OAuthResourceServer) -> None:
    """Wait for the key-set refreshes running in the background of this loop."""
    await asyncio.gather(*oauth._loop_state().refreshes.values())


# ---------------------------------------------------------------- accepted


@pytest.mark.parametrize(
    ("fixture", "alg"),
    [
        ("rsa_key", "RS256"),
        ("rsa_key", "PS256"),
        ("ec_p256_key", "ES256"),
        ("ec_p384_key", "ES384"),
        ("ed25519_key", "EdDSA"),
    ],
)
async def test_valid_tokens_each_algorithm(
    request: pytest.FixtureRequest, fetch: FakeFetch, clock: Clock, fixture: str, alg: str
) -> None:
    key = SigningKey(f"{alg}-key", alg, request.getfixturevalue(fixture))
    fetch.keys = [key]
    oauth = make(clock)
    token = mint(key, clock, claims={"scope": "mcp:access files:read"})
    identity = await oauth.verify(token)
    assert identity.issuer == ISSUER
    assert identity.subject == "user-1"
    assert identity.client_id == "client-1"
    assert identity.scopes == frozenset({"mcp:access", "files:read"})
    assert identity.expires_at == int(NOW) + 300
    assert identity.claims["sub"] == "user-1"
    assert len(identity.fingerprint) == 32


async def test_principal_fingerprint_stable_across_tokens(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock)
    first = await oauth.verify(mint(rsa, clock))
    # 128 bits: no client can grind a client id whose fingerprint matches
    # another principal's, and none can equal an API key's 12 hex digits.
    assert len(first.fingerprint) == 32
    assert int(first.fingerprint, 16) >= 0
    # The three values cannot run into one another, whatever they hold.
    assert principal_fingerprint(ISSUER, "a\0b", "c") != principal_fingerprint(ISSUER, "a", "b\0c")
    assert principal_fingerprint(ISSUER, None, "c") != principal_fingerprint(ISSUER, "", "c")
    assert len(principal_fingerprint(ISSUER, "\ud800", None)) == 32  # a lone surrogate
    clock.now += 100
    refreshed = await oauth.verify(mint(rsa, clock, claims={"scope": "mcp:access more"}))
    assert first.fingerprint == refreshed.fingerprint
    other_client = await oauth.verify(mint(rsa, clock, claims={"client_id": "client-2"}))
    assert other_client.fingerprint != first.fingerprint
    other_user = await oauth.verify(mint(rsa, clock, claims={"sub": "user-2"}))
    assert other_user.fingerprint != first.fingerprint
    # azp names the client when client_id is absent.
    azp = await oauth.verify(mint(rsa, clock, claims={"azp": "client-1"}, drop=["client_id"]))
    assert azp.client_id == "client-1" and azp.fingerprint == first.fingerprint


async def test_scope_from_scope_string_and_scp_list(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock)
    spaced = await oauth.verify(mint(rsa, clock, claims={"scope": "a  b c"}))
    assert spaced.scopes == frozenset({"a", "b", "c"})
    listed = await oauth.verify(mint(rsa, clock, claims={"scp": ["x", "y"]}, drop=["scope"]))
    assert listed.scopes == frozenset({"x", "y"})
    scp_string = await oauth.verify(mint(rsa, clock, claims={"scp": "p q"}, drop=["scope"]))
    assert scp_string.scopes == frozenset({"p", "q"})
    none = await oauth.verify(mint(rsa, clock, drop=["scope"]))
    assert none.scopes == frozenset()


async def test_wildcard_and_invalid_scopes_dropped(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock)
    identity = await oauth.verify(
        mint(rsa, clock, claims={"scp": ["*", "ok", 'bad"quote', "bad\\slash", 7]}, drop=["scope"])
    )
    assert identity.scopes == frozenset({"ok"})
    starred = await oauth.verify(mint(rsa, clock, claims={"scope": "* files:read"}))
    assert "*" not in starred.scopes


# ---------------------------------------------------------- algorithms and keys


async def test_alg_none_rejected(fetch: FakeFetch, clock: Clock) -> None:
    oauth = make(clock)
    payload = {"iss": ISSUER, "aud": RESOURCE, "sub": "u", "exp": NOW + 300}
    # The classic form: an empty signature.
    assert await reason(oauth, craft({"alg": "none"}, payload, b"")) == "unsupported_alg"
    assert await reason(oauth, craft({"alg": "None"}, payload, b"")) == "unsupported_alg"
    assert await reason(oauth, craft({"alg": "none", "kid": "k1"}, payload)) == "unsupported_alg"
    assert fetch.total == 0


async def test_hs256_signed_with_public_key_rejected(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    from cryptography.hazmat.primitives import serialization

    public_pem = rsa.private.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    header = {"alg": "HS256", "typ": "at+jwt", "kid": "k1"}
    payload = {"iss": ISSUER, "aud": RESOURCE, "sub": "u", "exp": NOW + 300}
    signing_input = f"{b64url(header)}.{b64url(payload)}".encode()
    signature = hmac.new(public_pem, signing_input, hashlib.sha256).digest()
    forged = f"{signing_input.decode()}.{b64url(signature)}"
    assert await reason(make(clock), forged) == "unsupported_alg"


async def test_alg_outside_narrowed_list_rejected(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock, algorithms=["ES256"])
    assert await reason(oauth, mint(rsa, clock)) == "unsupported_alg"
    assert fetch.total == 0


async def test_key_alg_and_curve_must_match(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, ec_p256_key: Any, ec_p384_key: Any
) -> None:
    oauth = make(clock)
    # A token claiming ES256 but naming the RSA key.
    _, payload, signature = mint(rsa, clock).split(".")
    relabelled = f"{b64url({'alg': 'ES256', 'kid': 'k1'})}.{payload}.{signature}"
    assert await reason(oauth, relabelled) == "bad_key"
    # A P-384 key published under the kid an ES256 token names.
    p384 = SigningKey("ec", "ES384", ec_p384_key)
    fetch.keys = [p384]
    p256_signed = mint(SigningKey("ec", "ES256", ec_p256_key), clock)
    assert await reason(make(clock), p256_signed) == "bad_key"
    # A JWK pinned to RS256 cannot verify PS256.
    pinned = SigningKey("pinned", "PS256", rsa.private, jwk_extra={"alg": "RS256"})
    fetch.keys = [pinned]
    assert await reason(make(clock), mint(pinned, clock)) == "bad_key"
    # use=enc and key_ops without verify are no signing keys either.
    for extra in ({"use": "enc"}, {"key_ops": ["encrypt"]}):
        odd = SigningKey("odd", "RS256", rsa.private, jwk_extra=extra)
        fetch.keys = [odd]
        assert await reason(make(clock), mint(odd, clock)) == "bad_key"


@pytest.mark.filterwarnings("ignore:The RSA key is 1024 bits")
async def test_small_rsa_key_rejected(fetch: FakeFetch, clock: Clock, rsa_1024_key: Any) -> None:
    small = SigningKey("small", "RS256", rsa_1024_key)
    fetch.keys = [small]
    assert await reason(make(clock), mint(small, clock)) == "bad_key"


async def test_oct_and_malformed_jwks_skipped(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, logs: LogCapture
) -> None:
    good = rsa.public_jwk()
    fetch.raw_keys = [
        {"kty": "oct", "kid": "k1", "k": base64.urlsafe_b64encode(b"secret").decode()},
        {"kty": "RSA", "kid": "broken", "n": "!!", "e": "AQAB"},
        {"kty": "EC", "kid": "offcurve", "crv": "P-256", "x": "A" * 43, "y": "A" * 43},
        "not an object",
        {"kty": "XYZ"},
        good,
    ]
    identity = await make(clock).verify(mint(rsa, clock))
    assert identity.subject == "user-1"
    assert "jwk_skipped" in logs.text
    # An HMAC token naming the oct key's kid never gets near it.
    forged = craft({"alg": "HS256", "kid": "k1"}, {"iss": ISSUER})
    assert await reason(make(clock), forged) == "unsupported_alg"


# ---------------------------------------------------------------- issuer


async def test_wrong_issuer_rejected_before_any_fetch(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock)
    token = mint(rsa, clock, issuer="https://evil.example.com")
    with pytest.raises(InvalidTokenError) as caught:
        await oauth.verify(token)
    assert caught.value.reason == "wrong_issuer"
    assert caught.value.issuer is None  # not a configured issuer: never named
    assert fetch.total == 0


async def test_issuer_compared_exactly(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    for variant in (ISSUER + "/", "HTTPS://auth.example.com", "https://AUTH.example.com"):
        assert await reason(oauth, mint(rsa, clock, issuer=variant)) == "wrong_issuer"
    assert fetch.total == 0


# -------------------------------------------------------------- audience


async def test_wrong_audience_rejected(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    for audience in ("https://other.example.com/mcp", "https://mcp.example.com", "mcp"):
        assert await reason(oauth, mint(rsa, clock, audience=audience)) == "wrong_audience"
    assert await reason(oauth, mint(rsa, clock, audience=[])) == "wrong_audience"


async def test_audience_list_accepted(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    token = mint(rsa, clock, audience=["https://other.example.com", RESOURCE + "/"])
    assert (await oauth.verify(token)).subject == "user-1"


async def test_custom_audience_replaces_resource(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock, audience="api://mcp")
    assert (await oauth.verify(mint(rsa, clock, audience="api://mcp"))).subject == "user-1"
    assert await reason(oauth, mint(rsa, clock, audience=RESOURCE)) == "wrong_audience"


# ------------------------------------------------------------------ time


async def test_expiry_leeway(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    token = mint(rsa, clock, claims={"exp": int(NOW)})
    clock.now = NOW + 59
    assert (await oauth.verify(token)).expires_at == int(NOW)
    clock.now = NOW + 61
    assert await reason(oauth, token) == "expired"


async def test_not_yet_valid(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    for claim in ("nbf", "iat"):
        future = mint(rsa, clock, claims={claim: int(NOW) + 61, "exp": int(NOW) + 600})
        assert await reason(oauth, future) == "not_yet_valid"
        within = mint(rsa, clock, claims={claim: int(NOW) + 59, "exp": int(NOW) + 600})
        assert (await oauth.verify(within)).subject == "user-1"
    assert await reason(oauth, mint(rsa, clock, claims={"exp": "soon"})) == "malformed"
    assert await reason(oauth, mint(rsa, clock, claims={"nbf": True})) == "malformed"


async def test_non_finite_times_are_malformed(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    # JSON's Infinity and NaN (and 1e999) decode to floats no comparison can
    # place, and an integer this long to no float at all.
    oauth = make(clock)
    for claims in (
        {"exp": math.inf},
        {"exp": math.nan},
        {"exp": 10**400},
        {"nbf": math.nan},
        {"nbf": -math.inf},
        {"iat": math.nan},
    ):
        assert await reason(oauth, mint(rsa, clock, claims=claims)) == "malformed", claims


async def test_missing_required_claims(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    for claim in ("exp", "aud", "iss"):
        assert await reason(oauth, mint(rsa, clock, drop=[claim])) == "missing_claims"
    principal_less = mint(rsa, clock, drop=["sub", "client_id"])
    assert await reason(oauth, principal_less) == "missing_claims"
    assert (await oauth.verify(mint(rsa, clock, drop=["sub"]))).client_id == "client-1"
    assert (await oauth.verify(mint(rsa, clock, drop=["client_id"]))).subject == "user-1"


# ----------------------------------------------------------- header rules


async def test_typ_values(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    for typ in ("at+jwt", "application/at+jwt", "AT+JWT", "JWT", None):
        token = mint(rsa, clock, headers={"typ": typ})
        assert (await oauth.verify(token)).subject == "user-1", typ
    for typ in ("dpop+jwt", "logout+jwt", "secevent+jwt"):
        assert await reason(oauth, mint(rsa, clock, headers={"typ": typ})) == "bad_type"


async def test_crit_header_rejected(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    token = mint(rsa, clock, headers={"crit": ["exp"]})
    assert await reason(make(clock), token) == "crit"


async def test_embedded_and_remote_key_headers_ignored(
    fetch: FakeFetch, clock: Clock, rsa_key_2: Any
) -> None:
    attacker = SigningKey("k1", "RS256", rsa_key_2)
    headers = {
        "jwk": attacker.public_jwk(),
        "jku": "https://evil.example.com/jwks",
        "x5u": "https://evil.example.com/cert",
    }
    token = mint(attacker, clock, headers=headers)
    assert await reason(make(clock), token) == "bad_signature"
    assert not [url for url in fetch.calls if "evil" in url]


async def test_cnf_bound_token_rejected(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    token = mint(rsa, clock, claims={"cnf": {"jkt": "thumbprint"}})
    assert await reason(make(clock), token) == "bound_token"


# --------------------------------------------------------------- shape


async def test_encrypted_token_rejected(fetch: FakeFetch, clock: Clock) -> None:
    assert await reason(make(clock), "a.b.c.d.e") == "encrypted"
    assert fetch.total == 0


async def test_oversized_and_non_base64url_rejected(fetch: FakeFetch, clock: Clock) -> None:
    oauth = make(clock)
    assert MAX_TOKEN_BYTES == 16 * 1024  # what the CHANGELOG promises
    assert await reason(oauth, "a" * (16 * 1024 + 1)) == "too_large"
    assert await reason(oauth, "a" * (16 * 1024)) == "malformed"  # 16 KiB itself is no size
    for bad in ("not a token", "a.b", "a.b.c.d", "ab$.cd.ef", "a.b.c==x", "a..c", "é.b.c"):
        assert await reason(oauth, bad) == "malformed", bad
    header = b64url({"alg": "RS256"})
    assert await reason(oauth, f"{header}.{b64url(b'[1, 2]')}.c2ln") == "malformed"
    assert await reason(oauth, f"{header}.{b64url(b'not json')}.c2ln") == "malformed"
    assert fetch.total == 0


async def test_deeply_nested_payload_is_invalid_not_crash(fetch: FakeFetch, clock: Clock) -> None:
    oauth = make(clock)
    header = b64url({"alg": "RS256", "kid": "k1"})
    # Deep enough to exhaust the recursion limit, small enough for the 16 KiB cap.
    nested = b"[" * 5000 + b"]" * 5000
    assert await reason(oauth, f"{header}.{b64url(nested)}.c2ln") == "malformed"
    deeper = b'{"a":' * 1500 + b"1" + b"}" * 1500
    assert await reason(oauth, f"{b64url(deeper)}.{b64url(b'{}')}.c2ln") == "malformed"
    # Deeper still is refused by size before it is parsed at all.
    huge = b"[" * 10_000 + b"]" * 10_000
    assert await reason(oauth, f"{header}.{b64url(huge)}.c2ln") == "too_large"


async def test_payload_nested_past_the_claim_depth_is_malformed(
    fetch: FakeFetch, clock: Clock
) -> None:
    # Far too shallow to trouble any parser, so only the explicit depth check
    # can refuse it, on every platform alike.
    oauth = make(clock)
    header = b64url({"alg": "RS256", "kid": "k1"})
    nested = b'{"iss": "x", "a": ' + b"[" * 40 + b"]" * 40 + b"}"
    assert await reason(oauth, f"{header}.{b64url(nested)}.c2ln") == "malformed"
    deep_header = b'{"alg": "RS256", "x": ' + b"[" * 40 + b"]" * 40 + b"}"
    assert await reason(oauth, f"{b64url(deep_header)}.{b64url(b'{}')}.c2ln") == "malformed"


def nested_dict(depth: int) -> Any:
    value: Any = 1
    for _ in range(depth):
        value = {"a": value}
    return value


def nested_list(depth: int) -> Any:
    value: Any = 1
    for _ in range(depth):
        value = [value]
    return value


async def test_deeply_nested_signed_claims_are_malformed_not_a_crash(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    # Signed by the issuer, parsed fine, but too deep to copy onto the identity.
    oauth = make(clock)
    for nested in (nested_dict(600), nested_list(600)):
        token = mint(rsa, clock, claims={"x": nested})
        assert len(token) < 16 * 1024
        assert await reason(oauth, token) == "malformed"
    claims = (await oauth.verify(mint(rsa, clock, claims={"x": nested_list(10)}))).claims
    assert claims["x"] == ((((((((((1,),),),),),),),),),)


async def test_claims_nested_32_levels_deep_are_the_limit(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    # The claims object is level 1: MAX_CLAIM_DEPTH levels in all are kept,
    # one more is refused, far below any recursion limit.
    assert MAX_CLAIM_DEPTH == 32  # what the CHANGELOG promises
    oauth = make(clock)
    for nest in (nested_dict, nested_list):
        within = mint(rsa, clock, claims={"x": nest(MAX_CLAIM_DEPTH - 1)})
        assert (await oauth.verify(within)).subject == "user-1", nest.__name__
        beyond = mint(rsa, clock, claims={"x": nest(MAX_CLAIM_DEPTH)})
        assert await reason(oauth, beyond) == "malformed", nest.__name__


async def test_decode_gets_fresh_options_each_call(
    monkeypatch: pytest.MonkeyPatch, fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    import jwt

    seen: list[Any] = []
    real = jwt.decode

    def spy(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["options"])
        return real(*args, **kwargs)

    monkeypatch.setattr(jwt, "decode", spy)
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    await oauth.verify(mint(rsa, clock))
    assert len(seen) == 2
    assert seen[0] is not seen[1]
    assert seen[0] == seen[1]


# ----------------------------------------------------------- key sets


async def test_unknown_kid_triggers_one_refresh(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, rsa_key_2: Any
) -> None:
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    assert fetch.key_fetches == 1
    rotated = SigningKey("k2", "RS256", rsa_key_2)
    fetch.keys = [rsa, rotated]
    clock.now += 31  # past the refresh cooldown
    assert (await oauth.verify(mint(rotated, clock))).subject == "user-1"
    assert fetch.key_fetches == 2
    assert fetch.calls[METADATA_URL] == 1  # only the key set is refetched
    # Both keys are cached now.
    await oauth.verify(mint(rsa, clock))
    await oauth.verify(mint(rotated, clock))
    assert fetch.key_fetches == 2


async def test_kid_spray_bounded_by_cooldown(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    clock.now += 31
    before = fetch.key_fetches
    for index in range(200):
        clock.now += 0.005  # 200 tokens within one second
        sprayed = mint(rsa, clock, headers={"kid": f"random-{index}"})
        assert await reason(oauth, sprayed) == "unknown_key"
    assert fetch.key_fetches - before <= 1


async def test_keys_refresh_after_ttl(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    clock.now += KEYS_TTL_SECONDS - KEY_REFRESH_AHEAD_SECONDS - 1
    await oauth.verify(mint(rsa, clock))
    await settle(oauth)
    assert fetch.key_fetches == 1
    clock.now += 2  # the hourly refresh starts ahead of the hour
    await oauth.verify(mint(rsa, clock))
    await settle(oauth)
    assert fetch.key_fetches == 2
    assert fetch.calls[METADATA_URL] == 2  # metadata is refreshed with the keys


async def test_due_refresh_does_not_hold_up_verification(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, rsa_key_2: Any
) -> None:
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    fetch.delay = 0.5  # an authorization server that answers slowly, or not at all
    clock.now += KEYS_TTL_SECONDS - KEY_REFRESH_AHEAD_SECONDS + 1
    # The cached keys answer at once; the refresh runs behind them, ahead of the hour.
    identity = await asyncio.wait_for(oauth.verify(mint(rsa, clock)), 0.25)
    assert identity.subject == "user-1"
    # A token naming a key the cache lacks waits for that refresh, and
    # starts no other.
    rotated = SigningKey("k2", "RS256", rsa_key_2)
    fetch.keys = [rsa, rotated]
    assert (await oauth.verify(mint(rotated, clock))).subject == "user-1"
    assert fetch.key_fetches == 2
    await settle(oauth)
    assert fetch.key_fetches == 2


async def test_a_withdrawn_key_stops_working_once_the_keys_are_an_hour_old(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, rsa_key_2: Any
) -> None:
    # Keys an hour old are not trusted again before a refresh has been
    # tried, however slow the authorization server: requests wait for it.
    other = SigningKey("k2", "RS256", rsa_key_2)
    fetch.keys = [rsa, other]
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    fetch.keys = [other]  # k1 withdrawn
    fetch.delay = 0.2
    for idle in (KEYS_TTL_SECONDS + 1, 2 * 24 * 3600):
        clock.now += idle
        withdrawn, kept = await asyncio.gather(
            reason(oauth, mint(rsa, clock)), oauth.verify(mint(other, clock))
        )
        assert withdrawn == "unknown_key"
        assert kept.subject == "user-1"
        fetch.keys = [rsa, other]  # k1 back, then withdrawn again
        clock.now += 31
        assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"
        fetch.keys = [other]
    assert fetch.key_fetches == 5


async def test_a_request_arriving_during_the_first_fetch_waits_for_it(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    fetch.delay = 0.1
    oauth = make(clock)
    first = asyncio.ensure_future(oauth.verify(mint(rsa, clock)))
    await asyncio.sleep(0.02)  # the fetch is under way
    second = asyncio.ensure_future(oauth.verify(mint(rsa, clock, claims={"sub": "user-2"})))
    assert (await first).subject == "user-1"
    assert (await second).subject == "user-2"
    assert fetch.key_fetches == 1


async def test_a_rotated_key_arriving_during_its_refresh_waits_for_it(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, rsa_key_2: Any
) -> None:
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    rotated = SigningKey("k2", "RS256", rsa_key_2)
    fetch.keys = [rsa, rotated]
    fetch.delay = 0.1
    clock.now += 60  # past the refresh cooldown
    first = asyncio.ensure_future(oauth.verify(mint(rotated, clock)))
    await asyncio.sleep(0.02)  # its refresh is under way
    second = asyncio.ensure_future(oauth.verify(mint(rotated, clock, claims={"sub": "user-2"})))
    assert (await first).subject == "user-1"
    assert (await second).subject == "user-2"  # not unknown_key
    assert fetch.key_fetches == 2


async def test_stale_keys_used_when_refresh_fails(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, logs: LogCapture
) -> None:
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    fetch.down = True
    clock.now += 3601
    assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"
    await settle(oauth)
    assert "jwks_refresh_failed" in logs.text
    # Not retried on every request: once per cooldown.
    attempts = fetch.total
    for _ in range(10):
        await oauth.verify(mint(rsa, clock))
    assert fetch.total == attempts
    assert logs.text.count("jwks_refresh_failed") == 1
    # Once a refresh has failed, keys past their hour answer at once while
    # the next one runs behind them: a silent server holds requests up once.
    fetch.delay = 0.5
    clock.now += 31
    assert (await asyncio.wait_for(oauth.verify(mint(rsa, clock)), 0.25)).subject == "user-1"
    await settle(oauth)
    assert fetch.total > attempts
    assert logs.text.count("jwks_refresh_failed") == 1


async def test_a_failed_fetch_early_in_the_hour_does_not_keep_old_keys_answering(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, rsa_key_2: Any
) -> None:
    # A fetch for an unknown kid fails a few minutes in.  Then the
    # authorization server answers again, without k1.  That failure was no
    # refresh of the keys: once they are an hour old, the next request still
    # waits for their refresh, so the withdrawn k1 stops working.
    other = SigningKey("k2", "RS256", rsa_key_2)
    oauth = make(clock)
    assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"
    clock.now += 100
    fetch.down = True
    assert await reason(oauth, mint(other, clock)) == "unknown_key"
    assert fetch.key_fetches == 2  # the fetch for k2, which failed
    fetch.down = False
    fetch.keys = [other]  # k1 withdrawn
    fetch.delay = 0.2
    clock.now += KEYS_TTL_SECONDS + 1300  # idle until the keys are well past the hour
    withdrawn, kept = await asyncio.gather(
        reason(oauth, mint(rsa, clock)), oauth.verify(mint(other, clock))
    )
    assert withdrawn == "unknown_key"
    assert kept.subject == "user-1"
    assert fetch.key_fetches == 3


async def test_with_no_keys_the_wait_starts_when_a_slow_fetch_fails(
    monkeypatch: pytest.MonkeyPatch, clock: Clock, rsa: SigningKey
) -> None:
    # An authorization server that takes connections and never answers: each
    # discovery candidate times out, so a fetch fails 10 s after it began.
    # The 5 s before the next fetch count from that failure, so requests in
    # them get 503 at once rather than waiting for a fetch that is always
    # running.
    fetched: Counter[str] = Counter()

    async def silent(url: str, *, max_bytes: int, timeout: float, executor: Any) -> dict[str, Any]:
        fetched[url] += 1
        clock.now += timeout
        raise _fetch.FetchError(f"{url} timed out")

    monkeypatch.setattr(_fetch, "fetch_json", silent)
    oauth = make(clock)
    with pytest.raises(AuthServerUnavailableError):
        await oauth.verify(mint(rsa, clock))
    assert sum(fetched.values()) == 2  # RFC 8414, then OpenID Connect discovery
    for _ in range(2):
        with pytest.raises(AuthServerUnavailableError):
            await oauth.verify(mint(rsa, clock))
        assert sum(fetched.values()) == 2  # refused at once: no fetch started
        assert not oauth._loop_state().refreshes
        clock.now += 2.4
    clock.now += 0.2  # 5 s after the failure, the next request tries again
    with pytest.raises(AuthServerUnavailableError):
        await oauth.verify(mint(rsa, clock))
    assert sum(fetched.values()) == 4


async def test_a_key_set_without_usable_keys_withdraws_the_cached_ones(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, logs: LogCapture
) -> None:
    # The authorization server answers, with no key this server can use: an
    # emergency revocation, say.  The keys it withdrew stop working.
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    unusable = [
        [],
        [
            {"kty": "oct", "kid": "k1", "k": base64.urlsafe_b64encode(b"secret").decode()},
            {"kty": "RSA", "kid": "k1", "n": "!!", "e": "AQAB"},
        ],
    ]
    for round_, raw_keys in enumerate(unusable, start=1):
        fetch.raw_keys = raw_keys
        clock.now += KEYS_TTL_SECONDS - KEY_REFRESH_AHEAD_SECONDS + 1
        await oauth.verify(mint(rsa, clock))  # answered while the refresh runs
        await settle(oauth)
        with pytest.raises(AuthServerUnavailableError):
            await oauth.verify(mint(rsa, clock))
        assert not oauth._ready()
        assert (
            logs.events("auth_unavailable")
            == [{"type": "auth_unavailable", "issuer": ISSUER, "stage": "keys"}] * round_
        )
        assert "jwks_refresh_failed" not in logs.text
        # Asked again every 5 s; once a usable key is back, tokens verify.
        fetch.raw_keys = None
        clock.now += 6
        assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"
        assert oauth._ready()


async def test_no_keys_and_as_down_is_unavailable(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, logs: LogCapture
) -> None:
    fetch.down = True
    oauth = make(clock)
    with pytest.raises(AuthServerUnavailableError) as caught:
        await oauth.verify(mint(rsa, clock))
    assert caught.value.code == -32008
    assert caught.value.data == {"reason": "auth_server_unavailable"}
    assert logs.events("auth_unavailable") == [
        {"type": "auth_unavailable", "issuer": ISSUER, "stage": "metadata"}
    ]
    # An empty key set is no better.
    fetch.down = False
    fetch.raw_keys = []
    clock.now += 6
    with pytest.raises(AuthServerUnavailableError):
        await oauth.verify(mint(rsa, clock))
    assert logs.events("auth_unavailable")[-1]["stage"] == "keys"
    # Once it answers, tokens verify.
    fetch.raw_keys = None
    clock.now += 6
    assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"


async def test_metadata_discovery_order(
    monkeypatch: pytest.MonkeyPatch, clock: Clock, rsa: SigningKey
) -> None:
    issuer = "https://auth.example.com/tenant"
    fake = FakeFetch([rsa], issuer=issuer)
    fake.documents = {}
    monkeypatch.setattr(_fetch, "fetch_json", fake)
    oauth = make(clock, authorization_servers=[issuer])
    with pytest.raises(AuthServerUnavailableError):
        await oauth.verify(mint(rsa, clock, issuer=issuer))
    assert list(fake.calls) == [
        "https://auth.example.com/.well-known/oauth-authorization-server/tenant",
        "https://auth.example.com/.well-known/openid-configuration/tenant",
        "https://auth.example.com/tenant/.well-known/openid-configuration",
    ]
    # The last candidate (OIDC appended) answers.
    fake.documents[f"{issuer}/.well-known/openid-configuration"] = {
        "issuer": issuer,
        "jwks_uri": f"{issuer}/jwks",
    }
    clock.now += 6
    assert (await oauth.verify(mint(rsa, clock, issuer=issuer))).issuer == issuer


async def test_metadata_issuer_mismatch_ignored(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, logs: LogCapture
) -> None:
    fetch.documents[METADATA_URL] = {"issuer": "https://evil.example.com", "jwks_uri": JWKS_URL}
    oauth = make(clock)
    with pytest.raises(AuthServerUnavailableError):
        await oauth.verify(mint(rsa, clock))
    assert "names issuer 'https://evil.example.com'" in logs.text
    assert fetch.key_fetches == 0
    # The next candidate fits.
    fetch.documents[OIDC_URL] = {"issuer": ISSUER, "jwks_uri": JWKS_URL}
    clock.now += 6
    assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"
    # A jwks_uri the server may not fetch is no jwks_uri.
    fetch.documents[METADATA_URL] = {"issuer": ISSUER, "jwks_uri": "http://evil.example.com/k"}
    fetch.documents.pop(OIDC_URL)
    fresh = make(clock)
    with pytest.raises(AuthServerUnavailableError):
        await fresh.verify(mint(rsa, clock))


async def test_fetch_refuses_redirects_oversize_and_plain_http(fake_as: Any) -> None:
    executor = ThreadPoolExecutor(max_workers=2)
    try:

        async def fetch(url: str, max_bytes: int = 1024 * 1024) -> dict[str, Any]:
            return await _fetch.fetch_json(url, max_bytes=max_bytes, timeout=5.0, executor=executor)

        assert "keys" in await fetch(f"{fake_as.issuer}/jwks")
        fake_as.fail("jwks", "redirect")
        with pytest.raises(_fetch.FetchError, match="302") as redirected:
            await fetch(f"{fake_as.issuer}/jwks")
        assert redirected.value.status == 302
        fake_as.fail("jwks", "huge")
        with pytest.raises(_fetch.FetchError, match="exceeds") as oversize:
            await fetch(f"{fake_as.issuer}/jwks")
        assert oversize.value.too_large and oversize.value.status is None
        fake_as.fail("jwks", "not_json")
        with pytest.raises(_fetch.FetchError, match="JSON") as not_json:
            await fetch(f"{fake_as.issuer}/jwks")
        assert not not_json.value.too_large
        assert not_json.value.status == 200  # the server answered: not an unreachable one
        fake_as.fail("jwks", 500)
        with pytest.raises(_fetch.FetchError) as failed:
            await fetch(f"{fake_as.issuer}/jwks")
        assert failed.value.status == 500
        assert not failed.value.too_large
        calls = sum(fake_as.counters.values())
        for refused in (
            "http://example.com/jwks",
            "file:///etc/passwd",
            "ftp://127.0.0.1/x",
            "https://user:pw@example.com/",
        ):
            with pytest.raises(_fetch.FetchError, match="refusing"):
                await fetch(refused)
        assert sum(fake_as.counters.values()) == calls
    finally:
        executor.shutdown(wait=False)


async def test_metadata_and_key_sets_are_fetched_with_a_1_mib_cap(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    assert MAX_DOCUMENT_BYTES == 1024 * 1024  # what SECURITY.md promises
    assert (await make(clock).verify(mint(rsa, clock))).subject == "user-1"
    assert fetch.max_bytes == {METADATA_URL: 1024 * 1024, JWKS_URL: 1024 * 1024}


def padded(document: dict[str, Any], size: int) -> bytes:
    """*document* as JSON of exactly *size* bytes (spaces before the last brace)."""
    raw = json.dumps(document).encode()
    assert len(raw) < size
    return raw[:-1] + b" " * (size - len(raw)) + b"}"


@pytest.mark.parametrize("route", ["rfc8414", "jwks"])
async def test_a_document_of_1_mib_is_read_and_one_byte_more_refused(
    fake_as: Any, route: str
) -> None:
    clock = Clock(time.time())
    document = (
        fake_as.metadata()
        if route == "rfc8414"
        else {"keys": [key.public_jwk() for key in fake_as.keys]}
    )
    token = fake_as.mint(now=clock.now)
    for size, accepted in ((1024 * 1024, True), (1024 * 1024 + 1, False)):
        fake_as.fail(route, padded(document, size))
        oauth = OAuthResourceServer(RESOURCE, [fake_as.issuer], clock=clock)
        try:
            if accepted:
                assert (await oauth.verify(token)).subject == "user-1"
            else:
                with pytest.raises(AuthServerUnavailableError) as caught:
                    await oauth.verify(token)
                assert caught.value.stage in ("metadata", "keys")
                assert not oauth._ready()
        finally:
            oauth.close()


async def test_fetch_times_out(fake_as: Any) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        fake_as.fail("jwks", "timeout")
        with pytest.raises(_fetch.FetchError, match="timed out"):
            await _fetch.fetch_json(
                f"{fake_as.issuer}/jwks", max_bytes=1024, timeout=0.3, executor=executor
            )
    finally:
        executor.shutdown(wait=False)


# What a trickling server sends at once, then one byte at a time, then one
# byte of filler at a time for ever: every read brings a byte well within the
# socket timeout, so only a deadline for the whole exchange can end it.
DRIPS = {
    "status": (b"", b"HTTP/1.1 200 OK\r\nX-Drip: ", b"a"),
    "headers": (b"HTTP/1.1 200 OK\r\nX-Drip: ", b"", b"a"),
    "body": (b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n", b"", b" "),
    # A TLS handshake record announcing 16 KiB, which never arrives.
    "handshake": (b"\x16\x03\x03\x40\x00", b"", b"\x00"),
}


@pytest.fixture
def dripping() -> Iterator[Callable[[str], str]]:
    """Serve one of ``DRIPS`` on 127.0.0.1, a byte every 0.1 s; returns its URL."""
    stop = threading.Event()
    listeners: list[socket.socket] = []

    def start(phase: str) -> str:
        head, prefix, filler = DRIPS[phase]
        listener = socket.create_server(("127.0.0.1", 0))
        listener.settimeout(0.1)
        listeners.append(listener)

        def answer(conn: socket.socket) -> None:
            with conn:
                try:
                    conn.recv(65536)  # the request, or a TLS client hello
                    conn.sendall(head)
                    for byte in itertools.chain(prefix, itertools.repeat(filler[0])):
                        if stop.wait(0.1):
                            return
                        conn.sendall(bytes([byte]))
                except OSError:
                    pass  # the client hung up

        def serve() -> None:
            while not stop.is_set():
                try:
                    conn, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    return
                threading.Thread(target=answer, args=(conn,), daemon=True).start()

        threading.Thread(target=serve, daemon=True).start()
        scheme = "https" if phase == "handshake" else "http"
        return f"{scheme}://127.0.0.1:{listener.getsockname()[1]}/"

    yield start
    stop.set()
    for listener in listeners:
        listener.close()


@pytest.mark.parametrize("phase", list(DRIPS))
async def test_a_trickled_answer_is_cut_off_at_the_deadline(
    dripping: Callable[[str], str], phase: str
) -> None:
    # Wherever the trickle starts (status line, headers, body or the TLS
    # handshake), the exchange ends at its deadline, and so does its thread:
    # the next fetch gets it at once.
    url = dripping(phase)
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        started = time.monotonic()
        with pytest.raises(_fetch.FetchError, match="timed out") as caught:
            await _fetch.fetch_json(url, max_bytes=1024 * 1024, timeout=1.0, executor=executor)
        assert time.monotonic() - started < 1.5
        assert caught.value.status is None and not caught.value.queued
        assert executor.submit(lambda: "free").result(timeout=0.25) == "free"
    finally:
        executor.shutdown(wait=False)


def tls_certificates(host: str) -> tuple[bytes, bytes, bytes]:
    """A test CA, and a certificate it signed for *host*: (CA, certificate, key) as PEM."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.datetime.now(datetime.UTC)

    def name(common: str) -> x509.Name:
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common)])

    def usage(**granted: bool) -> x509.KeyUsage:
        fields = (
            "digital_signature content_commitment key_encipherment data_encipherment "
            "key_agreement key_cert_sign crl_sign encipher_only decipher_only"
        ).split()
        return x509.KeyUsage(**{field: granted.get(field, False) for field in fields})

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (
        x509.CertificateBuilder()
        .subject_name(name("easy-mcp test CA"))
        .issuer_name(name("easy-mcp test CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(usage(key_cert_sign=True, crl_sign=True), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    try:
        alt: x509.GeneralName = x509.IPAddress(ipaddress.ip_address(host))
    except ValueError:
        alt = x509.DNSName(host)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name(host))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(usage(digital_signature=True), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .add_extension(x509.SubjectAlternativeName([alt]), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return (
        ca.public_bytes(serialization.Encoding.PEM),
        certificate.public_bytes(serialization.Encoding.PEM),
        key_pem,
    )


async def test_https_fetch_checks_the_certificate_and_its_deadline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    # Over TLS the certificate chain and the name it certifies are checked,
    # and an answer trickled a record at a time ends at the deadline too.
    executor = ThreadPoolExecutor(max_workers=1)
    stop = threading.Event()
    servers: list[socket.socket] = []
    trusted = ssl.create_default_context()
    trusted.verify_flags &= ~ssl.VERIFY_X509_STRICT
    trusted.sslsocket_class = _fetch._tls_context().sslsocket_class

    def serve(certified_for: str, *, trickle: bool = False) -> str:
        ca, certificate, key = tls_certificates(certified_for)
        trusted.load_verify_locations(cadata=ca.decode())
        (tmp_path / f"{certified_for}.pem").write_bytes(certificate + key)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(tmp_path / f"{certified_for}.pem")
        listener = socket.create_server(("127.0.0.1", 0))
        listener.settimeout(0.1)
        servers.append(listener)
        body = b'{"keys": []}'
        answer_bytes = (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: %d\r\nConnection: close\r\n\r\n%s" % (len(body), body)
        )

        def answer(conn: socket.socket) -> None:
            try:
                with server_context.wrap_socket(conn, server_side=True) as tls:
                    tls.settimeout(5)
                    tls.recv(65536)
                    if not trickle:
                        tls.sendall(answer_bytes)
                        return
                    for byte in itertools.chain(answer_bytes[:20], itertools.repeat(97)):
                        if stop.wait(0.1):
                            return
                        tls.sendall(bytes([byte]))  # a TLS record each
            except OSError:
                pass  # the client refused the certificate, or hung up

        def accept() -> None:
            while not stop.is_set():
                try:
                    conn, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    return
                threading.Thread(target=answer, args=(conn,), daemon=True).start()

        threading.Thread(target=accept, daemon=True).start()
        return f"https://127.0.0.1:{listener.getsockname()[1]}/jwks"

    async def fetch(url: str, timeout: float = 5.0) -> dict[str, Any]:
        return await _fetch.fetch_json(url, max_bytes=1024, timeout=timeout, executor=executor)

    try:
        right = serve("127.0.0.1")
        wrong = serve("localhost")
        trickling = serve("127.0.0.1", trickle=True)
        monkeypatch.setattr(_fetch, "_tls_context", lambda: trusted)
        assert await fetch(right) == {"keys": []}
        with pytest.raises(_fetch.FetchError, match="unreachable") as refused:
            await fetch(wrong)
        assert refused.value.status is None
        started = time.monotonic()
        with pytest.raises(_fetch.FetchError, match="timed out"):
            await fetch(trickling, timeout=1.0)
        assert time.monotonic() - started < 1.5
        assert executor.submit(lambda: "free").result(timeout=0.25) == "free"
        monkeypatch.undo()  # the system's trusted certificates know no test CA
        with pytest.raises(_fetch.FetchError, match="unreachable"):
            await fetch(right)
    finally:
        stop.set()
        for listener in servers:
            listener.close()
        executor.shutdown(wait=False)


async def test_fetch_deadline_starts_when_a_thread_runs_it() -> None:
    # Waiting for a free thread is not part of the exchange: work queued
    # behind a slow fetch gets its whole deadline once a thread runs it.
    executor = ThreadPoolExecutor(max_workers=1)

    def slow() -> dict[str, Any]:
        time.sleep(0.6)
        return {"ok": True}

    try:
        # timeout=0: each deadline is 1 s; the second fetch ends 1.2 s in.
        results = await asyncio.gather(
            _fetch._run(executor, 0.0, slow), _fetch._run(executor, 0.0, slow)
        )
        assert results == [{"ok": True}, {"ok": True}]
    finally:
        executor.shutdown(wait=False)


async def test_a_fetch_that_never_gets_a_thread_sends_nothing() -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    release = threading.Event()
    ran: list[str] = []

    def blocking() -> dict[str, Any]:
        release.wait(10)
        return {}

    def queued() -> dict[str, Any]:
        ran.append("queued")
        return {}

    try:
        first = asyncio.ensure_future(_fetch._run(executor, 5.0, blocking))
        await asyncio.sleep(0.05)  # it holds the only thread
        with pytest.raises(_fetch.FetchError) as caught:
            await _fetch._run(executor, 0.0, queued)
        assert caught.value.queued
        assert caught.value.status is None
        release.set()
        assert await first == {}
    finally:
        release.set()
        executor.shutdown(wait=True)
    assert ran == []  # given up before a thread was free: never sent


def test_a_refresh_cut_short_by_its_loop_closing_is_tried_again(
    fake_as: Any, rsa_key_2: Any
) -> None:
    # verify() from sync code, one asyncio.run() per call: each loop closes
    # once verify() returns, cancelling a background refresh mid-fetch.  The
    # next call may try again at once and, past the hour, waits for it, so a
    # key the issuer withdrew stops working.
    clock = Clock()
    oauth = OAuthResourceServer(RESOURCE, [fake_as.issuer], clock=clock)
    first = fake_as.keys[0]
    second = SigningKey("k2", "RS256", rsa_key_2)
    fake_as.set_keys([first, second])

    async def verify_and_leave(token: str) -> str | None:
        identity = await oauth.verify(token)
        await asyncio.sleep(0.05)  # a background refresh is under way as the loop closes
        return identity.subject

    def outcome(key: SigningKey) -> str | None:
        try:
            return asyncio.run(verify_and_leave(fake_as.mint(key, now=clock.now)))
        except InvalidTokenError as exc:
            return exc.reason

    try:
        assert outcome(first) == "user-1"
        fake_as.rotate(second)  # k1 withdrawn
        fake_as.delays["jwks"] = 1.0
        clock.now += KEYS_TTL_SECONDS - 10
        assert outcome(first) == "user-1"  # still within the hour
        fake_as.delays.clear()
        clock.now += 15  # past the hour, within the cooldown of the attempt cut short
        assert outcome(first) == "unknown_key"
        assert outcome(second) == "user-1"
        keys = oauth._states[fake_as.issuer].keys or ()
        assert [key.kid for key in keys] == ["k2"]
    finally:
        oauth.close()


async def test_concurrent_verifications_single_flight(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    fetch.delay = 0.05
    oauth = make(clock)
    tokens = [mint(rsa, clock, claims={"sub": f"user-{index}"}) for index in range(50)]
    identities = await asyncio.gather(*(oauth.verify(token) for token in tokens))
    assert len({identity.fingerprint for identity in identities}) == 50
    assert fetch.calls[METADATA_URL] == 1
    assert fetch.key_fetches == 1


async def test_a_cancelled_waiter_does_not_stop_the_shared_fetch(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    fetch.delay = 0.1
    oauth = make(clock)
    first = asyncio.ensure_future(oauth.verify(mint(rsa, clock)))
    second = asyncio.ensure_future(oauth.verify(mint(rsa, clock)))
    await asyncio.sleep(0.02)
    first.cancel()
    assert (await second).subject == "user-1"
    assert fetch.key_fetches == 1


def test_verify_works_from_two_event_loops(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    token = mint(rsa, clock)
    assert asyncio.run(oauth.verify(token)).subject == "user-1"
    results: list[str | None] = []

    def other_thread() -> None:
        results.append(asyncio.run(oauth.verify(token)).subject)

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join(10)
    assert results == ["user-1"]
    clock.now += 3601  # a refresh, from yet another loop

    async def verify_and_refresh() -> str | None:
        subject = (await oauth.verify(mint(rsa, clock))).subject
        await settle(oauth)
        return subject

    assert asyncio.run(verify_and_refresh()) == "user-1"
    assert fetch.key_fetches == 2


def test_finished_event_loops_are_released(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    loops: list[weakref.ref[asyncio.AbstractEventLoop]] = []

    async def verify() -> None:
        loops.append(weakref.ref(asyncio.get_running_loop()))
        assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"
        await settle(oauth)

    for _ in range(3):
        clock.now += 3601  # every loop refreshes the keys
        asyncio.run(verify())
    assert fetch.key_fetches == 3
    gc.collect()
    assert [ref() for ref in loops] == [None, None, None]
    assert len(oauth._loops) == 0


async def test_warm_up_never_raises_and_close_releases_threads(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, logs: LogCapture
) -> None:
    fetch.down = True
    oauth = make(clock)
    await oauth.warm_up()  # logged, not raised
    assert "warm-up" in logs.text
    assert not oauth._ready()
    fetch.down = False
    clock.now += 6
    await oauth.warm_up()
    assert oauth._ready()
    executor = oauth._executor
    assert executor is not None
    oauth.close()
    assert oauth._executor is None
    assert executor._shutdown
    oauth.close()  # idempotent


async def test_close_stops_the_fetch_threads(fake_as: Any) -> None:
    oauth = OAuthResourceServer(RESOURCE, [fake_as.issuer])
    await oauth.warm_up()  # real fetches, on the server's own threads
    assert oauth._ready()
    executor = oauth._executor
    assert executor is not None
    workers = list(executor._threads)
    assert workers and all(worker.name.startswith("easy-mcp-oauth") for worker in workers)
    oauth.close()
    for worker in workers:
        worker.join(5)
    assert not any(worker.is_alive() for worker in workers)


def test_identity_claims_are_read_only(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    token = mint(rsa, clock, claims={"groups": ["a", "b"], "ext": {"tenant": "t1"}})
    identity = asyncio.run(make(clock).verify(token))
    with pytest.raises(TypeError):
        identity.claims["sub"] = "someone-else"  # type: ignore[index]
    assert identity.claims["groups"] == ("a", "b")
    with pytest.raises(TypeError):
        identity.claims["ext"]["tenant"] = "t2"
    assert token not in repr(identity)
    assert token not in json.dumps(dict(identity.claims), default=str)


def test_identities_copy_pickle_and_asdict(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    token = mint(rsa, clock, claims={"groups": ["a", "b"], "ext": {"tenant": "t1"}})
    from_token = asyncio.run(make(clock).verify(token))
    from_key = APIKeyAuth({"k" * 32: ["a"]}).authenticate("k" * 32)
    assert from_key is not None
    built = ClientIdentity("fingerprint1", frozenset({"x"}))
    for identity in (from_token, from_key, built):
        for copied in (copy.deepcopy(identity), pickle.loads(pickle.dumps(identity))):
            assert copied == identity
            assert dict(copied.claims) == dict(identity.claims)
            with pytest.raises(TypeError):
                copied.claims["sub"] = "someone-else"  # type: ignore[index]
        assert dataclasses.asdict(identity)["fingerprint"] == identity.fingerprint
        assert dataclasses.astuple(identity)[0] == identity.fingerprint
    copied = pickle.loads(pickle.dumps(from_token))
    assert copied.claims["ext"]["tenant"] == "t1"
    with pytest.raises(TypeError):
        copied.claims["ext"]["tenant"] = "t2"
    assert dataclasses.asdict(from_token)["claims"]["groups"] == ("a", "b")
