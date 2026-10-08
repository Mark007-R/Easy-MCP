"""Token introspection (RFC 7662): what is accepted, the cache, failures, the request.

Most tests replace the introspection call with an in-memory answer; the
request's shape and the failure modes are tested against the local server of
tests/oauth_fake_as.py over real HTTP.
"""

from __future__ import annotations

import asyncio
import base64
import gc
import math
import pickle
import sys
import time
import weakref
from collections import Counter
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote_plus

import pytest
from conftest import LogCapture
from oauth_fake_as import CLIENT_ID, CLIENT_SECRET

from easy_mcp import (
    AuthServerUnavailableError,
    Introspection,
    InvalidTokenError,
    OAuthResourceServer,
)
from easy_mcp.security import _fetch
from easy_mcp.security import oauth as oauth_module

ISSUER = "https://auth.example.com"
RESOURCE = "https://mcp.example.com/mcp"
ENDPOINT = f"{ISSUER}/introspect"
NOW = 1_800_000_000.0
TOKEN = "opaque-token-value-0123456789"


class Clock:
    def __init__(self, now: float = NOW) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class FakeIntrospection:
    """The endpoint's answers by token; counts calls and how many run at once."""

    def __init__(self) -> None:
        self.answers: dict[str, dict[str, Any]] = {}
        self.calls: list[dict[str, Any]] = []
        self.delay = 0.0
        self.running = 0
        self.peak = 0
        self.error: _fetch.FetchError | None = None

    async def __call__(
        self,
        url: str,
        form: dict[str, str],
        *,
        auth: tuple[str, str],
        max_bytes: int,
        timeout: float,
        executor: Any,
    ) -> dict[str, Any]:
        self.calls.append({"url": url, "form": dict(form), "auth": auth})
        self.running += 1
        self.peak = max(self.peak, self.running)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.error is not None:
                raise self.error
            return dict(self.answers.get(form["token"], {"active": False}))
        finally:
            self.running -= 1


def active(**fields: Any) -> dict[str, Any]:
    answer: dict[str, Any] = {
        "active": True,
        "iss": ISSUER,
        "aud": RESOURCE,
        "sub": "user-1",
        "client_id": "client-1",
        "scope": "mcp:access files:read",
        "exp": int(NOW) + 3600,
        "token_type": "Bearer",
    }
    answer.update(fields)
    return {key: value for key, value in answer.items() if value is not None}


@pytest.fixture
def endpoint(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeIntrospection]:
    fake = FakeIntrospection()
    monkeypatch.setattr(_fetch, "post_form_json", fake)
    yield fake


@pytest.fixture
def clock() -> Clock:
    return Clock()


def make(
    clock: Clock, endpoint_url: str | None = ENDPOINT, issuer: str = ISSUER
) -> OAuthResourceServer:
    return OAuthResourceServer(
        RESOURCE,
        [issuer],
        introspection=Introspection(CLIENT_ID, CLIENT_SECRET, endpoint=endpoint_url),
        clock=clock,
    )


async def reason(oauth: OAuthResourceServer, token: str = TOKEN) -> str:
    with pytest.raises(InvalidTokenError) as caught:
        await oauth.verify(token)
    return caught.value.reason


async def test_active_token_accepted(endpoint: FakeIntrospection, clock: Clock) -> None:
    endpoint.answers[TOKEN] = active()
    identity = await make(clock).verify(TOKEN)
    assert identity.subject == "user-1"
    assert identity.client_id == "client-1"
    assert identity.issuer == ISSUER
    assert identity.scopes == frozenset({"mcp:access", "files:read"})
    assert identity.expires_at == int(NOW) + 3600
    assert identity.claims["sub"] == "user-1"
    # A client-credentials token names only its client.
    endpoint.answers["service"] = active(sub=None, client_id="robot")
    robot = await make(clock).verify("service")
    assert robot.subject is None and robot.client_id == "robot"


async def test_inactive_token_rejected(endpoint: FakeIntrospection, clock: Clock) -> None:
    oauth = make(clock)
    assert await reason(oauth) == "inactive"  # unknown: {"active": false}
    endpoint.answers["truthy"] = active(active="true")  # must be exactly true
    assert await reason(oauth, "truthy") == "inactive"
    endpoint.answers["nobody"] = active(sub=None, client_id=None)
    assert await reason(oauth, "nobody") == "missing_claims"
    endpoint.answers["other-issuer"] = active(iss="https://evil.example.com")
    assert await reason(oauth, "other-issuer") == "wrong_issuer"


async def test_aud_required_and_matched(
    endpoint: FakeIntrospection, clock: Clock, logs: LogCapture
) -> None:
    oauth = make(clock)
    endpoint.answers["no-aud"] = active(aud=None)
    assert await reason(oauth, "no-aud") == "wrong_audience"
    endpoint.answers["no-aud-2"] = active(aud=None)
    assert await reason(oauth, "no-aud-2") == "wrong_audience"
    # Operators are told once how to fix it.
    assert logs.text.count("carry no 'aud'") == 1
    endpoint.answers["other"] = active(aud="https://other.example.com/mcp")
    assert await reason(oauth, "other") == "wrong_audience"
    endpoint.answers["listed"] = active(aud=["https://other.example.com", RESOURCE])
    assert (await oauth.verify("listed")).subject == "user-1"


async def test_refresh_token_type_rejected(endpoint: FakeIntrospection, clock: Clock) -> None:
    oauth = make(clock)
    endpoint.answers["refresh"] = active(token_type="refresh_token")
    assert await reason(oauth, "refresh") == "wrong_token_type"
    endpoint.answers["typed"] = active(token_type="access_token")
    assert (await oauth.verify("typed")).subject == "user-1"
    endpoint.answers["untyped"] = active(token_type=None)
    assert (await oauth.verify("untyped")).subject == "user-1"
    endpoint.answers["bound"] = active(cnf={"x5t#S256": "abc"})
    assert await reason(oauth, "bound") == "bound_token"


async def test_exp_and_nbf_checked(endpoint: FakeIntrospection, clock: Clock) -> None:
    oauth = make(clock)
    endpoint.answers["expired"] = active(exp=int(NOW) - 61)
    assert await reason(oauth, "expired") == "expired"
    endpoint.answers["early"] = active(nbf=int(NOW) + 61)
    assert await reason(oauth, "early") == "not_yet_valid"
    endpoint.answers["odd"] = active(exp="tomorrow")
    assert await reason(oauth, "odd") == "malformed"
    endpoint.answers["leeway"] = active(exp=int(NOW) - 59)
    assert (await oauth.verify("leeway")).subject == "user-1"
    endpoint.answers["forever"] = active(exp=None)
    assert (await oauth.verify("forever")).expires_at is None


async def test_non_finite_times_are_malformed(endpoint: FakeIntrospection, clock: Clock) -> None:
    oauth = make(clock)
    for index, fields in enumerate(({"exp": math.inf}, {"exp": math.nan}, {"nbf": math.nan})):
        endpoint.answers[f"odd-{index}"] = active(**fields)
        assert await reason(oauth, f"odd-{index}") == "malformed", fields


async def test_identity_pickles(endpoint: FakeIntrospection, clock: Clock) -> None:
    endpoint.answers[TOKEN] = active(ext={"tenant": "t1"})
    identity = await make(clock).verify(TOKEN)
    copied = pickle.loads(pickle.dumps(identity))
    assert copied == identity
    assert copied.claims["ext"]["tenant"] == "t1"
    with pytest.raises(TypeError):
        copied.claims["ext"]["tenant"] = "t2"


async def test_request_shape(fake_as: Any, clock: Clock) -> None:
    oauth = make(clock, endpoint_url=None, issuer=fake_as.issuer)
    fake_as.set_introspection(TOKEN, active(iss=fake_as.issuer))
    try:
        assert (await oauth.verify(TOKEN)).subject == "user-1"
    finally:
        oauth.close()
    # The endpoint came from the authorization server's metadata.
    assert fake_as.counters["rfc8414"] == 1
    (sent,) = fake_as.introspection_requests
    assert sent["form"] == {"token": TOKEN, "token_type_hint": "access_token"}
    headers = sent["headers"]
    assert headers["content-type"] == "application/x-www-form-urlencoded"
    assert headers["accept"] == "application/json"
    assert headers["user-agent"].startswith("easy-mcp-kit/")
    credentials = f"{quote_plus(CLIENT_ID)}:{quote_plus(CLIENT_SECRET)}".encode()
    assert headers["authorization"] == "Basic " + base64.b64encode(credentials).decode()


async def test_positive_cache_bounded_by_exp_and_60s(
    endpoint: FakeIntrospection, clock: Clock
) -> None:
    oauth = make(clock)
    endpoint.answers[TOKEN] = active()
    await oauth.verify(TOKEN)
    clock.now += 59
    await oauth.verify(TOKEN)
    assert len(endpoint.calls) == 1
    clock.now += 2  # 61 s after the answer
    await oauth.verify(TOKEN)
    assert len(endpoint.calls) == 2
    # An answer expiring in 10 s is not kept for 60 (RFC 7662 section 4).
    endpoint.answers["short"] = active(exp=int(clock.now) + 10)
    await oauth.verify("short")
    clock.now += 11
    endpoint.answers["short"] = {"active": False}  # revoked meanwhile
    assert await reason(oauth, "short") == "inactive"
    assert len(endpoint.calls) == 4


async def test_negative_cache_10s(endpoint: FakeIntrospection, clock: Clock) -> None:
    oauth = make(clock)
    assert await reason(oauth) == "inactive"
    clock.now += 9
    assert await reason(oauth) == "inactive"
    assert len(endpoint.calls) == 1
    clock.now += 2
    endpoint.answers[TOKEN] = active()
    assert (await oauth.verify(TOKEN)).subject == "user-1"
    assert len(endpoint.calls) == 2


async def test_cache_keys_are_hashes(endpoint: FakeIntrospection, clock: Clock) -> None:
    oauth = make(clock)
    endpoint.answers[TOKEN] = active()
    await oauth.verify(TOKEN)
    await reason(oauth, "another-token-value")
    assert len(oauth._introspected) == 2
    for key, (_, answer) in oauth._introspected.items():
        assert TOKEN not in key and "another-token-value" not in key
        assert len(key) == 64
        assert TOKEN not in repr(answer)


async def test_endpoint_failures_are_503(
    fake_as: Any, clock: Clock, logs: LogCapture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(oauth_module, "REQUEST_TIMEOUT_SECONDS", 0.5)
    fake_as.stall_seconds = 1.5
    fake_as.set_introspection(TOKEN, active(iss=fake_as.issuer))
    oauth = make(clock, endpoint_url=f"{fake_as.issuer}/introspect", issuer=fake_as.issuer)
    try:
        for mode in (500, "timeout", "not_json", 401, 403):
            fake_as.fail("introspect", mode)
            with pytest.raises(AuthServerUnavailableError) as caught:
                await oauth.verify(TOKEN)
            assert caught.value.code == -32008
            assert caught.value.stage == "introspection"
            clock.now += 11  # failures are not cached, but be sure
        fake_as.heal()
        assert (await oauth.verify(TOKEN)).subject == "user-1"
        # Wrong client credentials: the server's own, not the token's, fault.
        wrong = OAuthResourceServer(
            RESOURCE,
            [fake_as.issuer],
            introspection=Introspection(
                "intruder", "guess", endpoint=f"{fake_as.issuer}/introspect"
            ),
            clock=clock,
        )
        with pytest.raises(AuthServerUnavailableError):
            await wrong.verify("another-token")
        wrong.close()
    finally:
        oauth.close()
    assert "introspection_credentials_rejected" in logs.text
    assert len(logs.events("auth_unavailable")) == 6
    assert CLIENT_SECRET not in logs.text
    assert quote_plus(CLIENT_SECRET) not in logs.text
    assert TOKEN not in logs.text


async def test_metadata_without_endpoint_is_503(
    fake_as: Any, clock: Clock, logs: LogCapture
) -> None:
    oauth = make(clock, endpoint_url=None, issuer=fake_as.issuer)
    fake_as.serve_rfc8414 = False
    try:
        with pytest.raises(AuthServerUnavailableError) as caught:
            await oauth.verify(TOKEN)
        assert caught.value.stage == "metadata"
    finally:
        oauth.close()
    assert not oauth._ready()


async def test_outage_window_bounds_introspection_calls(
    endpoint: FakeIntrospection, clock: Clock, logs: LogCapture
) -> None:
    oauth = make(clock)
    endpoint.answers[TOKEN] = active()
    await oauth.verify(TOKEN)  # cached before the outage
    endpoint.error = _fetch.FetchError("the endpoint answered HTTP 500", status=500)
    failed_at = clock.now
    for index in range(20):
        with pytest.raises(AuthServerUnavailableError) as caught:
            await oauth.verify(f"random-token-{index}")
        assert caught.value.stage == "introspection"
        clock.now += 0.2  # 20 requests within 4 s
    assert len(endpoint.calls) == 2  # the cached token's, then one failed attempt
    assert len(logs.events("auth_unavailable")) == 1
    assert logs.text.count("token introspection failed") == 1
    # Cached answers keep working meanwhile.
    assert (await oauth.verify(TOKEN)).subject == "user-1"
    # 5 s on, one request tries again.
    clock.now = failed_at + 5
    with pytest.raises(AuthServerUnavailableError):
        await oauth.verify("random-token-again")
    assert len(endpoint.calls) == 3
    # Wrong client credentials are told once per window too.
    endpoint.error = _fetch.FetchError("the endpoint answered HTTP 401", status=401)
    clock.now += 5
    for index in range(5):
        with pytest.raises(AuthServerUnavailableError):
            await oauth.verify(f"another-token-{index}")
    assert len(endpoint.calls) == 4
    assert logs.text.count("introspection_credentials_rejected") == 1
    endpoint.error = None
    clock.now += 5
    endpoint.answers["fresh"] = active()
    assert (await oauth.verify("fresh")).subject == "user-1"


async def test_outage_window_bounds_discovery(
    monkeypatch: pytest.MonkeyPatch, endpoint: FakeIntrospection, clock: Clock, logs: LogCapture
) -> None:
    fetched: Counter[str] = Counter()

    async def unreachable(
        url: str, *, max_bytes: int, timeout: float, executor: Any
    ) -> dict[str, Any]:
        fetched[url] += 1
        raise _fetch.FetchError(f"{url} answered HTTP 500", status=500)

    monkeypatch.setattr(_fetch, "fetch_json", unreachable)
    oauth = make(clock, endpoint_url=None)
    for index in range(10):
        with pytest.raises(AuthServerUnavailableError) as caught:
            await oauth.verify(f"random-token-{index}")
        assert caught.value.stage == "metadata"
        clock.now += 0.4  # 10 requests within 4 s
    assert sum(fetched.values()) == 2  # one discovery: RFC 8414, then OpenID Connect
    assert len(logs.events("auth_unavailable")) == 1
    assert endpoint.calls == []
    clock.now = NOW + 5
    with pytest.raises(AuthServerUnavailableError):
        await oauth.verify("random-token-again")
    assert sum(fetched.values()) == 4


async def test_discovery_is_shared_and_outside_the_slots(
    monkeypatch: pytest.MonkeyPatch, endpoint: FakeIntrospection, clock: Clock
) -> None:
    fetched: Counter[str] = Counter()
    metadata: dict[str, Any] = {}

    async def slow(url: str, *, max_bytes: int, timeout: float, executor: Any) -> dict[str, Any]:
        fetched[url] += 1
        await asyncio.sleep(0.2)
        if url in metadata:
            return metadata[url]
        raise _fetch.FetchError(f"{url} timed out")

    monkeypatch.setattr(_fetch, "fetch_json", slow)
    oauth = make(clock, endpoint_url=None)
    tokens = [f"token-{index}" for index in range(24)]
    started = time.monotonic()
    results = await asyncio.gather(
        *(oauth.verify(token) for token in tokens), return_exceptions=True
    )
    elapsed = time.monotonic() - started
    assert all(isinstance(result, AuthServerUnavailableError) for result in results)
    assert sum(fetched.values()) == 2  # one discovery for all 24 tokens
    assert elapsed < 1.0  # about one discovery (0.4 s), not one per 8 tokens
    # Once the metadata answers, one discovery serves every waiting token.
    fetched.clear()
    clock.now += 5
    metadata[f"{ISSUER}/.well-known/oauth-authorization-server"] = {
        "issuer": ISSUER,
        "introspection_endpoint": ENDPOINT,
    }
    for token in tokens:
        endpoint.answers[token] = active(sub=token)
    identities = await asyncio.gather(*(oauth.verify(token) for token in tokens))
    assert [identity.subject for identity in identities] == tokens
    assert sum(fetched.values()) == 1


def test_finished_event_loops_are_released(
    monkeypatch: pytest.MonkeyPatch, endpoint: FakeIntrospection, clock: Clock
) -> None:
    async def metadata(
        url: str, *, max_bytes: int, timeout: float, executor: Any
    ) -> dict[str, Any]:
        return {"issuer": ISSUER, "introspection_endpoint": ENDPOINT}

    monkeypatch.setattr(_fetch, "fetch_json", metadata)
    endpoint.delay = 0.01
    oauth = make(clock, endpoint_url=None)
    loops: list[weakref.ref[asyncio.AbstractEventLoop]] = []

    async def burst(round_: int) -> None:
        loops.append(weakref.ref(asyncio.get_running_loop()))
        tokens = [f"token-{round_}-{index}" for index in range(12)]
        for token in tokens:
            endpoint.answers[token] = active(sub=token)
        await asyncio.gather(*(oauth.verify(token) for token in tokens))

    for round_ in range(2):
        asyncio.run(burst(round_))
    assert endpoint.peak == 8  # callers waited for a slot
    gc.collect()
    assert [ref() for ref in loops] == [None, None]
    assert len(oauth._loops) == 0


async def test_concurrency_cap(endpoint: FakeIntrospection, clock: Clock) -> None:
    endpoint.delay = 0.05
    oauth = make(clock)
    tokens = [f"token-{index}" for index in range(30)]
    for token in tokens:
        endpoint.answers[token] = active(sub=token)
    identities = await asyncio.gather(*(oauth.verify(token) for token in tokens))
    assert [identity.subject for identity in identities] == tokens
    assert endpoint.peak == 8
    # Concurrent lookups of one token share one request.
    endpoint.calls.clear()
    endpoint.answers["shared"] = active()
    await asyncio.gather(*(oauth.verify("shared") for _ in range(20)))
    assert len(endpoint.calls) == 1


async def test_a_cancelled_waiter_does_not_cancel_the_lookup(
    endpoint: FakeIntrospection, clock: Clock
) -> None:
    endpoint.delay = 0.1
    endpoint.answers[TOKEN] = active()
    oauth = make(clock)
    first = asyncio.ensure_future(oauth.verify(TOKEN))
    second = asyncio.ensure_future(oauth.verify(TOKEN))
    await asyncio.sleep(0.02)
    first.cancel()
    assert (await second).subject == "user-1"
    assert len(endpoint.calls) == 1


async def test_works_without_pyjwt(
    endpoint: FakeIntrospection, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "jwt", None)
    endpoint.answers[TOKEN] = active()
    # JWT-shaped or not, every token goes to the endpoint.
    endpoint.answers["a.b.c"] = active(sub="jwt-shaped")
    oauth = make(clock)
    assert (await oauth.verify(TOKEN)).subject == "user-1"
    assert (await oauth.verify("a.b.c")).subject == "jwt-shaped"
    assert await reason(oauth, "x" * (16 * 1024 + 1)) == "too_large"
    assert await reason(oauth, "not a token") == "malformed"
    assert len(endpoint.calls) == 2
