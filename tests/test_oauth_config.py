"""OAuthResourceServer settings: what is accepted, what is refused, what is published."""

from __future__ import annotations

import sys
from typing import Any

import pytest

from easy_mcp import Introspection, OAuthResourceServer
from easy_mcp.security.oauth import DEFAULT_ALGORITHMS, canonical_uri

ISSUER = "https://auth.example.com"
RESOURCE = "https://mcp.example.com/mcp"


def make(**kwargs: Any) -> OAuthResourceServer:
    kwargs.setdefault("resource", RESOURCE)
    kwargs.setdefault("authorization_servers", [ISSUER])
    return OAuthResourceServer(**kwargs)


def test_resource_requires_https_or_loopback_http() -> None:
    with pytest.raises(ValueError, match="https"):
        make(resource="http://example.com/mcp")
    with pytest.raises(ValueError, match="https"):
        make(resource="ftp://example.com/mcp")
    with pytest.raises(ValueError):
        make(resource="/mcp")
    for accepted in (
        "http://127.0.0.1:8000/mcp",
        "http://localhost:8000/mcp",
        "http://[::1]:8000/mcp",
        "https://x/mcp",
    ):
        assert make(resource=accepted).resource.endswith("/mcp")


def test_resource_rejects_query_fragment_userinfo() -> None:
    for bad in (
        "https://mcp.example.com/mcp?tenant=a",
        "https://mcp.example.com/mcp#frag",
        "https://user:pw@mcp.example.com/mcp",
        "https://mcp.example.com/m cp",
        'https://mcp.example.com/m"cp',
        "https://mcp.example.com:99999/mcp",
    ):
        with pytest.raises(ValueError):
            make(resource=bad)


def test_resource_is_canonicalised() -> None:
    oauth = make(resource="HTTPS://MCP.Example.com:443/MCP/")
    assert oauth.resource == "https://mcp.example.com/MCP"  # the path keeps its case
    assert make(resource="https://mcp.example.com:8443/mcp").resource == (
        "https://mcp.example.com:8443/mcp"
    )
    assert make(resource="http://LOCALHOST:80/").resource == "http://localhost"


def test_metadata_path_follows_resource_path() -> None:
    root = make(resource="https://mcp.example.com")
    assert root.metadata_path == "/.well-known/oauth-protected-resource"
    assert root.metadata_url == "https://mcp.example.com/.well-known/oauth-protected-resource"
    nested = make(resource="https://mcp.example.com:8443/a/b/")
    assert nested.metadata_path == "/.well-known/oauth-protected-resource/a/b"
    assert nested.metadata_url == (
        "https://mcp.example.com:8443/.well-known/oauth-protected-resource/a/b"
    )


def test_authorization_servers_validated_and_kept_exact() -> None:
    with pytest.raises(ValueError):
        make(authorization_servers=[])
    with pytest.raises(ValueError, match="https"):
        make(authorization_servers=["http://auth.example.com"])
    with pytest.raises(ValueError, match="query"):
        make(authorization_servers=["https://auth.example.com?x=1"])
    with pytest.raises(ValueError, match="fragment"):
        make(authorization_servers=["https://auth.example.com#x"])
    # Issuers are compared byte for byte, so they are kept as given.
    kept = make(authorization_servers=["https://Auth.Example.com/tenant/", ISSUER])
    assert kept.authorization_servers == ("https://Auth.Example.com/tenant/", ISSUER)
    assert make(authorization_servers=ISSUER).authorization_servers == (ISSUER,)
    assert make(authorization_servers=["http://127.0.0.1:9000"]).authorization_servers


def test_algorithms_refuse_none_and_hmac() -> None:
    for refused in ("none", "HS256", "HS512"):
        with pytest.raises(ValueError, match="RFC 8725"):
            make(algorithms=[refused])
    with pytest.raises(ValueError, match="unknown algorithm"):
        make(algorithms=["RS1"])
    with pytest.raises(ValueError):
        make(algorithms=[])
    assert make(algorithms=("ES256",)).algorithms == ("ES256",)
    assert make().algorithms == DEFAULT_ALGORITHMS


def test_jwks_uri_needs_single_issuer() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        make(
            authorization_servers=[ISSUER, "https://other.example.com"],
            jwks_uri="https://auth.example.com/jwks",
        )
    with pytest.raises(ValueError, match="https"):
        make(jwks_uri="http://auth.example.com/jwks")
    assert make(jwks_uri="https://auth.example.com/jwks?v=2")


def test_introspection_needs_single_issuer() -> None:
    introspection = Introspection("rs", "secret")
    with pytest.raises(ValueError, match="exactly one"):
        make(
            authorization_servers=[ISSUER, "https://other.example.com"], introspection=introspection
        )
    assert make(introspection=introspection).introspection is introspection
    with pytest.raises(ValueError):
        Introspection("", "secret")
    with pytest.raises(ValueError, match="https"):
        Introspection("rs", "secret", endpoint="http://auth.example.com/introspect")


def test_required_scopes_refuse_offline_access_and_bad_chars() -> None:
    for bad in ("offline_access", "a b", 'a"b', "a\\b", ""):
        with pytest.raises(ValueError):
            make(required_scopes=[bad])
    assert make(required_scopes=["mcp:access", "mcp:access", "x"]).required_scopes == (
        "mcp:access",
        "x",
    )


def test_missing_pyjwt_has_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "jwt", None)
    with pytest.raises(ImportError, match=r"\[oauth\]"):
        make()
    # Introspection needs no extra.
    assert make(introspection=Introspection("rs", "secret")).introspection is not None


def test_introspection_secret_not_in_repr() -> None:
    introspection = Introspection("rs-client", "very-secret-value")
    assert "very-secret-value" not in repr(introspection)
    assert "rs-client" in repr(introspection)
    oauth = make(introspection=introspection)
    assert "very-secret-value" not in repr(oauth)


def test_from_env_reads_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EASY_MCP_OAUTH_RESOURCE", RESOURCE)
    monkeypatch.setenv("EASY_MCP_OAUTH_AUTHORIZATION_SERVERS", f"{ISSUER}, https://b.example.com")
    monkeypatch.setenv("EASY_MCP_OAUTH_AUDIENCE", "api://mcp  https://mcp.example.com/mcp")
    monkeypatch.setenv("EASY_MCP_OAUTH_REQUIRED_SCOPES", "mcp:access,mcp:tools")
    oauth = OAuthResourceServer.from_env(step_up=False)
    assert oauth.resource == RESOURCE
    assert oauth.authorization_servers == (ISSUER, "https://b.example.com")
    assert oauth.audience == ("api://mcp", RESOURCE)
    assert oauth.required_scopes == ("mcp:access", "mcp:tools")
    assert oauth.step_up is False
    assert oauth.introspection is None

    monkeypatch.setenv("EASY_MCP_OAUTH_AUTHORIZATION_SERVERS", ISSUER)
    monkeypatch.setenv("EASY_MCP_OAUTH_JWKS_URI", f"{ISSUER}/jwks")
    monkeypatch.setenv("EASY_MCP_OAUTH_INTROSPECTION_CLIENT_ID", "rs")
    monkeypatch.setenv("EASY_MCP_OAUTH_INTROSPECTION_CLIENT_SECRET", "secret")
    monkeypatch.setenv("EASY_MCP_OAUTH_INTROSPECTION_ENDPOINT", f"{ISSUER}/introspect")
    introspected = OAuthResourceServer.from_env()
    assert introspected.introspection == Introspection("rs", "secret", f"{ISSUER}/introspect")

    monkeypatch.delenv("EASY_MCP_OAUTH_INTROSPECTION_CLIENT_SECRET")
    with pytest.raises(ValueError, match="both"):
        OAuthResourceServer.from_env()


def test_from_env_requires_resource(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EASY_MCP_OAUTH_RESOURCE", raising=False)
    monkeypatch.setenv("EASY_MCP_OAUTH_AUTHORIZATION_SERVERS", ISSUER)
    with pytest.raises(ValueError, match="EASY_MCP_OAUTH_RESOURCE"):
        OAuthResourceServer.from_env()
    monkeypatch.setenv("EASY_MCP_OAUTH_RESOURCE", RESOURCE)
    monkeypatch.setenv("EASY_MCP_OAUTH_AUTHORIZATION_SERVERS", "  ")
    with pytest.raises(ValueError, match="AUTHORIZATION_SERVERS"):
        OAuthResourceServer.from_env()


def test_metadata_document_fields() -> None:
    oauth = make(resource="HTTPS://MCP.example.com/mcp/", required_scopes=["mcp:access"])
    document = oauth._metadata(resource_name="files", scopes=oauth.required_scopes)
    assert document == {
        "authorization_servers": [ISSUER],
        "bearer_methods_supported": ["header"],
        "resource": "https://mcp.example.com/mcp",
        "resource_name": "files",
        "scopes_supported": ["mcp:access"],
    }
    assert list(document) == sorted(document)
    # Zero values are omitted (RFC 9728 section 3.2).
    assert "scopes_supported" not in make()._metadata(resource_name="x", scopes=())


def test_canonical_audience_matching() -> None:
    oauth = make(resource="https://mcp.example.com/mcp")
    assert oauth._audience_ok("https://mcp.example.com/mcp")
    assert oauth._audience_ok("HTTPS://MCP.EXAMPLE.COM:443/mcp/")
    assert oauth._audience_ok(["other", "https://mcp.example.com/mcp/"])
    assert not oauth._audience_ok("https://mcp.example.com/MCP")
    assert not oauth._audience_ok("https://mcp.example.com/mcp/x")
    assert not oauth._audience_ok("https://mcp.example.com")
    assert not oauth._audience_ok(None)
    assert not oauth._audience_ok([1, 2])
    abstract = make(audience=["api://mcp", "urn:Example:MCP"])
    assert abstract._audience_ok("api://mcp")
    assert not abstract._audience_ok("API://mcp")  # not http(s): compared exactly
    assert abstract._audience_ok("urn:Example:MCP")
    assert not abstract._audience_ok("urn:example:mcp")
    assert not abstract._audience_ok(RESOURCE)  # audience= replaces the resource
    assert canonical_uri("https://a.example.com:443") == "https://a.example.com"
