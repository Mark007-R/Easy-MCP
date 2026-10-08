"""Authentication, authorization, tool visibility, and rate limiting."""

from __future__ import annotations

import pytest
from conftest import make_context, rpc

from easy_mcp import APIKeyAuth, MCPServer
from easy_mcp.decorators import build_tool
from easy_mcp.exceptions import (
    AUTHENTICATION_REQUIRED,
    INVALID_PARAMS,
    RATE_LIMITED,
    AuthenticationError,
    AuthorizationError,
    RateLimitError,
)
from easy_mcp.security.auth import authorize, fingerprint, visible
from easy_mcp.security.ratelimit import SlidingWindowRateLimiter

ADMIN_KEY = "admin-key-" + "a" * 22
MATH_KEY = "math-key-" + "b" * 23


def _protected_tool():  # type: ignore[no-untyped-def]
    def secret() -> str:
        """A protected tool."""
        return "s3cr3t"

    return build_tool(secret, scopes=("admin",))


def _public_tool():  # type: ignore[no-untyped-def]
    def hello() -> str:
        """A public tool."""
        return "hi"

    return build_tool(hello)


# ------------------------------------------------------------- APIKeyAuth


def test_authenticate_valid_key() -> None:
    auth = APIKeyAuth({ADMIN_KEY: "*", MATH_KEY: ["math"]})
    identity = auth.authenticate(ADMIN_KEY)
    assert identity is not None
    assert identity.scopes == frozenset({"*"})
    assert identity.fingerprint == fingerprint(ADMIN_KEY)
    assert ADMIN_KEY not in identity.fingerprint  # never the raw key

    math_identity = auth.authenticate(MATH_KEY)
    assert math_identity.scopes == frozenset({"math"})


def test_authenticate_no_key_is_anonymous() -> None:
    auth = APIKeyAuth({ADMIN_KEY: "*"})
    assert auth.authenticate(None) is None


def test_authenticate_wrong_key_rejected() -> None:
    auth = APIKeyAuth({ADMIN_KEY: "*"})
    with pytest.raises(AuthenticationError):
        auth.authenticate("wrong-key-000000000000")


def test_empty_keys_rejected() -> None:
    with pytest.raises(ValueError):
        APIKeyAuth({})


def test_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EASY_MCP_API_KEYS", f"{ADMIN_KEY}:*;{MATH_KEY}:math|stats")
    auth = APIKeyAuth.from_env()
    assert auth.authenticate(ADMIN_KEY).scopes == frozenset({"*"})
    assert auth.authenticate(MATH_KEY).scopes == frozenset({"math", "stats"})


def test_from_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EASY_MCP_API_KEYS", raising=False)
    with pytest.raises(ValueError):
        APIKeyAuth.from_env()


# ----------------------------------------------------- authorize / visible


def test_authorize_public_tool_for_anonymous() -> None:
    authorize(None, _public_tool())  # must not raise


def test_authorize_protected_tool_requires_identity() -> None:
    with pytest.raises(AuthenticationError):
        authorize(None, _protected_tool())


def test_authorize_scope_mismatch() -> None:
    auth = APIKeyAuth({MATH_KEY: ["math"]})
    identity = auth.authenticate(MATH_KEY)
    with pytest.raises(AuthorizationError):
        authorize(identity, _protected_tool())


def test_authorize_wildcard_scope() -> None:
    auth = APIKeyAuth({ADMIN_KEY: "*"})
    authorize(auth.authenticate(ADMIN_KEY), _protected_tool())  # must not raise


def test_visibility() -> None:
    auth = APIKeyAuth({ADMIN_KEY: ["admin"], MATH_KEY: ["math"]})
    protected = _protected_tool()
    public = _public_tool()
    assert visible(None, public) is True
    assert visible(None, protected) is False
    assert visible(auth.authenticate(ADMIN_KEY), protected) is True
    assert visible(auth.authenticate(MATH_KEY), protected) is False


# ------------------------------------------------- dispatch-level security


@pytest.fixture
def secured_server() -> MCPServer:
    server = MCPServer(
        port=0,
        rate_limit_per_minute=None,
        auth=APIKeyAuth({ADMIN_KEY: ["admin"], MATH_KEY: ["math"]}),
    )

    @server.tool
    def hello() -> str:
        """Public greeting."""
        return "hi"

    @server.tool(scopes=("admin",))
    def secret() -> str:
        """Protected tool."""
        return "s3cr3t"

    return server


async def test_anonymous_sees_only_public_tools(secured_server: MCPServer) -> None:
    response = await secured_server.dispatch(rpc("tools/list"), make_context())
    assert [t["name"] for t in response["result"]["tools"]] == ["hello"]


async def test_anonymous_cannot_call_protected_tool(secured_server: MCPServer) -> None:
    response = await secured_server.dispatch(
        rpc("tools/call", {"name": "secret", "arguments": {}}), make_context()
    )
    # Reported as unknown: unauthorized callers cannot enumerate protected tools.
    assert response["error"]["code"] == INVALID_PARAMS
    assert "Unknown tool" in response["error"]["message"]


async def test_authorized_client_can_call_protected_tool(secured_server: MCPServer) -> None:
    identity = secured_server.authenticate_key(ADMIN_KEY)
    response = await secured_server.dispatch(
        rpc("tools/call", {"name": "secret", "arguments": {}}),
        make_context(identity=identity, client_id=identity.fingerprint),
    )
    assert response["result"]["content"][0]["text"] == "s3cr3t"


async def test_wrong_scope_cannot_call_protected_tool(secured_server: MCPServer) -> None:
    identity = secured_server.authenticate_key(MATH_KEY)
    response = await secured_server.dispatch(
        rpc("tools/call", {"name": "secret", "arguments": {}}),
        make_context(identity=identity, client_id=identity.fingerprint),
    )
    assert response["error"]["code"] == INVALID_PARAMS


async def test_protected_tool_unreachable_without_auth_configured() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=None)  # no auth backend

    @server.tool(requires_auth=True)
    def secret() -> str:
        """Protected tool."""
        return "s3cr3t"

    response = await server.dispatch(
        rpc("tools/call", {"name": "secret", "arguments": {}}), make_context()
    )
    assert response["error"]["code"] == INVALID_PARAMS


# ------------------------------------------------------------ rate limiting


def test_sliding_window_limiter_deterministic() -> None:
    now = [0.0]
    limiter = SlidingWindowRateLimiter(2, 60.0, clock=lambda: now[0])
    limiter.check("client")
    limiter.check("client")
    with pytest.raises(RateLimitError) as excinfo:
        limiter.check("client")
    assert excinfo.value.retry_after_seconds == pytest.approx(60.0)

    limiter.check("other-client")  # independent budget per client

    now[0] = 61.0  # window slid past the first two events
    limiter.check("client")


async def test_dispatch_rate_limit() -> None:
    server = MCPServer(port=0, rate_limit_per_minute=2)
    ctx = make_context()
    assert (await server.dispatch(rpc("ping"), ctx)).get("result") == {}
    assert (await server.dispatch(rpc("ping", msg_id=2), ctx)).get("result") == {}
    third = await server.dispatch(rpc("ping", msg_id=3), ctx)
    assert third["error"]["code"] == RATE_LIMITED
    assert "retry_after_seconds" in third["error"]["data"]

    # A different client is unaffected.
    other = await server.dispatch(rpc("ping"), make_context(client_id="ip:other"))
    assert other.get("result") == {}


def test_error_codes_are_stable() -> None:
    # Public contract: documented codes must not drift between releases.
    assert AUTHENTICATION_REQUIRED == -32001
    assert RATE_LIMITED == -32003


def test_authenticate_length_mismatch_rejected() -> None:
    auth = APIKeyAuth({ADMIN_KEY: "*"})
    with pytest.raises(AuthenticationError):
        auth.authenticate(ADMIN_KEY[:-1])
    with pytest.raises(AuthenticationError):
        auth.authenticate(ADMIN_KEY + "x")


def test_rate_limiter_forgets_idle_clients() -> None:
    now = [0.0]
    limiter = SlidingWindowRateLimiter(5, 60.0, clock=lambda: now[0])
    for index in range(100):
        limiter.check(f"client-{index}")
    assert limiter.tracked_clients == 100

    now[0] = 61.0  # every recorded request has aged out of the window
    limiter.check("fresh")  # triggers the once-per-window sweep
    assert limiter.tracked_clients == 1


def test_api_key_match_non_raising() -> None:
    auth = APIKeyAuth({ADMIN_KEY: "*", MATH_KEY: ["math"]})
    assert auth.match("not-a-key") is None
    assert auth.match("") is None
    matched = auth.match(MATH_KEY)
    assert matched is not None and matched.scopes == frozenset({"math"})
    assert matched == auth.authenticate(MATH_KEY)
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        auth.authenticate("not-a-key")
    assert auth.authenticate(None) is None


def test_rate_limiter_exceeded_does_not_consume() -> None:
    now = [0.0]
    limiter = SlidingWindowRateLimiter(2, 60.0, clock=lambda: now[0])
    assert not limiter.exceeded("a")
    for _ in range(10):
        assert not limiter.exceeded("a")  # looking spends nothing
    limiter.check("a")
    assert not limiter.exceeded("a")
    limiter.check("a")
    assert limiter.exceeded("a")
    assert limiter._retry_after("a") == pytest.approx(60.0)
    now[0] = 30.0
    assert limiter._retry_after("a") == pytest.approx(30.0)
    now[0] = 60.5
    assert not limiter.exceeded("a")
    limiter.check("a")  # the budget is back
    assert limiter.tracked_clients == 1  # exceeded() made no entries
    assert not limiter.exceeded("never-seen")
    assert limiter.tracked_clients == 1


def test_identity_claims_not_in_repr_or_hash() -> None:
    from types import MappingProxyType

    from easy_mcp import ClientIdentity

    claims = MappingProxyType({"sub": "user-1", "email": "someone@example.com"})
    identity = ClientIdentity(
        fingerprint="f" * 12,
        scopes=frozenset({"a"}),
        subject="user-1",
        client_id="client-1",
        issuer="https://auth.example.com",
        expires_at=1,
        claims=claims,
    )
    assert "someone@example.com" not in repr(identity)
    assert "claims" not in repr(identity)
    same_but_other_claims = ClientIdentity(
        fingerprint="f" * 12,
        scopes=frozenset({"a"}),
        subject="user-1",
        client_id="client-1",
        issuer="https://auth.example.com",
        expires_at=1,
    )
    assert identity == same_but_other_claims
    assert hash(identity) == hash(same_but_other_claims)
    # API-key identities leave every new field empty and compare as before.
    key = APIKeyAuth({ADMIN_KEY: "*"}).authenticate(ADMIN_KEY)
    assert key is not None
    assert (key.subject, key.client_id, key.issuer, key.expires_at) == (None, None, None, None)
    assert dict(key.claims) == {}
    assert key == ClientIdentity(fingerprint=fingerprint(ADMIN_KEY), scopes=frozenset({"*"}))
