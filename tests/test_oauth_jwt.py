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
import gc
import hashlib
import hmac
import json
import math
import pickle
import threading
import weakref
from collections import Counter
from collections.abc import Iterator
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
    assert len(identity.fingerprint) == 12


async def test_principal_fingerprint_stable_across_tokens(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey
) -> None:
    oauth = make(clock)
    first = await oauth.verify(mint(rsa, clock))
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
    assert await reason(oauth, "a" * (16 * 1024 + 1)) == "too_large"
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
    clock.now += 3599
    await oauth.verify(mint(rsa, clock))
    assert fetch.key_fetches == 1
    clock.now += 2
    await oauth.verify(mint(rsa, clock))
    assert fetch.key_fetches == 2
    assert fetch.calls[METADATA_URL] == 2  # metadata is refreshed with the keys


async def test_stale_keys_used_when_refresh_fails(
    fetch: FakeFetch, clock: Clock, rsa: SigningKey, logs: LogCapture
) -> None:
    oauth = make(clock)
    await oauth.verify(mint(rsa, clock))
    fetch.down = True
    clock.now += 3601
    assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"
    assert "jwks_refresh_failed" in logs.text
    # Not retried on every request: once per cooldown.
    attempts = fetch.total
    for _ in range(10):
        await oauth.verify(mint(rsa, clock))
    assert fetch.total == attempts
    assert logs.text.count("jwks_refresh_failed") == 1


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
        with pytest.raises(_fetch.FetchError, match="exceeds"):
            await fetch(f"{fake_as.issuer}/jwks")
        fake_as.fail("jwks", "not_json")
        with pytest.raises(_fetch.FetchError, match="JSON"):
            await fetch(f"{fake_as.issuer}/jwks")
        fake_as.fail("jwks", 500)
        with pytest.raises(_fetch.FetchError) as failed:
            await fetch(f"{fake_as.issuer}/jwks")
        assert failed.value.status == 500
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
    assert asyncio.run(oauth.verify(mint(rsa, clock))).subject == "user-1"
    assert fetch.key_fetches == 2


def test_finished_event_loops_are_released(fetch: FakeFetch, clock: Clock, rsa: SigningKey) -> None:
    oauth = make(clock)
    loops: list[weakref.ref[asyncio.AbstractEventLoop]] = []

    async def verify() -> None:
        loops.append(weakref.ref(asyncio.get_running_loop()))
        assert (await oauth.verify(mint(rsa, clock))).subject == "user-1"

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
