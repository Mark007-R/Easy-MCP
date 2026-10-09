"""OAuth with resources, prompts and completion: visibility, step-up, scope checks, HTTP 403.

Token identities are built directly, as ``authenticate_request`` would return
them, except in the HTTP test at the end, which signs real tokens.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from types import MappingProxyType
from typing import Any, Literal

import httpx
import pytest
from conftest import OAUTH_RESOURCE, LogCapture, headers_for, make_context, modern, rpc

from easy_mcp import (
    APIKeyAuth,
    ClientIdentity,
    MCPServer,
    OAuthResourceServer,
    RegistrationError,
    current_identity,
)
from easy_mcp.exceptions import AUTHENTICATION_REQUIRED, INVALID_PARAMS
from easy_mcp.security.oauth import principal_fingerprint

ISSUER = "https://auth.example.com"
KEY = "oauth-resources-key-" + "k" * 13
LEGACY_NOT_FOUND = -32002


def token_identity(*scopes: str) -> ClientIdentity:
    return ClientIdentity(
        fingerprint=principal_fingerprint(ISSUER, "user-1", "client-1"),
        scopes=frozenset(scopes),
        subject="user-1",
        client_id="client-1",
        issuer=ISSUER,
        expires_at=1_900_000_000,
        claims=MappingProxyType({"sub": "user-1"}),
    )


def oauth_server(*, step_up: bool = True, auth: APIKeyAuth | None = None) -> MCPServer:
    oauth = OAuthResourceServer(OAUTH_RESOURCE, [ISSUER], step_up=step_up)
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth, auth=auth)

    @server.resource("files://{name}", scopes=("files:read", "files:admin"))
    def file(name: str) -> str:
        """A file."""
        who = current_identity()
        return f"{name} for {who.subject if who else None}"

    @server.resource("public://readme")
    def readme() -> str:
        return "readme"

    @server.prompt(scopes=("prompts:use",))
    def guarded(mode: Literal["short", "long"]) -> str:
        """A guarded prompt."""
        return mode

    return server


async def call(server: MCPServer, message: dict[str, Any], who: Any = None) -> dict[str, Any]:
    response = await server.dispatch(message, make_context(identity=who))
    assert response is not None
    return response


def test_scopes_of_every_definition_must_be_scope_tokens() -> None:
    server = oauth_server()
    for bad in ("has space", 'quote"', "offline_access"):
        with pytest.raises(RegistrationError, match="cannot be used with OAuth"):
            server.register_resource(lambda: "x", "bad://x", name="bad", scopes=(bad,))
        with pytest.raises(RegistrationError, match="cannot be used with OAuth"):
            server.register_prompt(lambda: "x", name="bad", scopes=(bad,))


async def test_tokens_with_step_up_see_everything_and_get_403_on_use() -> None:
    server = oauth_server()
    narrow = token_identity("other")
    listed = await call(server, rpc("resources/templates/list"), narrow)
    assert [t["uriTemplate"] for t in listed["result"]["resourceTemplates"]] == ["files://{name}"]
    prompts = await call(server, rpc("prompts/list"), narrow)
    assert [p["name"] for p in prompts["result"]["prompts"]] == ["guarded"]
    messages = [
        (rpc("resources/read", {"uri": "files://a"}), "files:read", "resource 'files://a'"),
        (modern("resources/read", {"uri": "files://a"}), "files:read", "resource 'files://a'"),
        (
            rpc("prompts/get", {"name": "guarded", "arguments": {"mode": "short"}}),
            "prompts:use",
            "prompt 'guarded'",
        ),
        (
            rpc(
                "completion/complete",
                {
                    "ref": {"type": "ref/prompt", "name": "guarded"},
                    "argument": {"name": "mode", "value": ""},
                },
            ),
            "prompts:use",
            "prompt 'guarded'",
        ),
        (
            rpc(
                "completion/complete",
                {
                    "ref": {"type": "ref/resource", "uri": "files://{name}"},
                    "argument": {"name": "name", "value": ""},
                },
            ),
            "files:read",
            "resource template 'files://{name}'",
        ),
    ]
    for message, scope, named in messages:
        response = await call(server, message, narrow)
        assert response["error"]["code"] == AUTHENTICATION_REQUIRED, message
        assert response["error"]["message"] == f"Insufficient scope for {named}"
        assert response["error"]["data"] == {"error": "insufficient_scope", "scope": scope}
    allowed = await call(
        server, rpc("resources/read", {"uri": "files://a"}), token_identity("files:admin")
    )
    assert allowed["result"]["contents"][0]["text"] == "a for user-1"


async def test_without_step_up_tokens_see_only_what_they_may_use() -> None:
    server = oauth_server(step_up=False)
    narrow = token_identity("other")
    listed = await call(server, rpc("resources/templates/list"), narrow)
    assert listed["result"]["resourceTemplates"] == []
    missing = await call(server, rpc("resources/read", {"uri": "files://a"}), narrow)
    assert missing["error"]["code"] == LEGACY_NOT_FOUND
    unknown = await call(server, rpc("prompts/get", {"name": "guarded"}), narrow)
    assert unknown["error"] == {"code": INVALID_PARAMS, "message": "Unknown prompt: guarded"}


async def test_api_keys_keep_hidden_equals_missing() -> None:
    server = oauth_server(auth=APIKeyAuth({KEY: ["other"]}))
    key = ClientIdentity(fingerprint="k" * 12, scopes=frozenset({"other"}))
    response = await call(server, rpc("resources/read", {"uri": "files://a"}), key)
    assert response["error"]["code"] == LEGACY_NOT_FOUND
    stateless = await call(server, modern("resources/read", {"uri": "files://a"}), key)
    assert stateless["error"]["code"] == INVALID_PARAMS


async def test_reads_are_private_with_oauth(logs: LogCapture) -> None:
    server = oauth_server()
    response = await call(
        server, modern("resources/read", {"uri": "public://readme"}), token_identity()
    )
    assert response["result"]["cacheScope"] == "private"
    discover = await call(server, modern("server/discover"), token_identity())
    assert discover["result"]["cacheScope"] == "public"
    await call(server, rpc("resources/read", {"uri": "files://a"}), token_identity("other"))
    denied = [e for e in logs.events("resource_read") if e["status"] == "denied"]
    assert len(denied) == 1 and denied[0]["uri"] == "files://a"


def test_initial_scopes_cover_resources_and_prompts_without_step_up() -> None:
    server = oauth_server(step_up=False)
    assert set(server._initial_scopes()) == {"files:read", "files:admin", "prompts:use"}
    stepping = oauth_server(step_up=True)
    assert stepping._initial_scopes() == ()
    assert stepping._known_scopes() == frozenset({"files:read", "files:admin", "prompts:use"})


async def test_http_step_up_is_a_403_challenge(
    live_server: Callable[[Any], str], fake_as: Any
) -> None:
    oauth = OAuthResourceServer(OAUTH_RESOURCE, [fake_as.issuer], step_up=True)
    server = MCPServer(port=0, rate_limit_per_minute=None, oauth=oauth)
    server.register_resource(lambda: "secret", "secret://x", name="secret", scopes=("vault:read",))
    base = live_server(server)
    message = modern("resources/read", {"uri": "secret://x"})
    headers = headers_for(message, **{"Mcp-Name": "secret://x"})
    async with httpx.AsyncClient(timeout=10) as client:
        token = fake_as.mint(claims={"scope": "other"})  # minted right before it is used
        response = await client.post(
            f"{base}/mcp", json=message, headers={**headers, "Authorization": f"Bearer {token}"}
        )
    assert response.status_code == 403, response.text
    challenge = response.headers["www-authenticate"]
    assert 'error="insufficient_scope"' in challenge and 'scope="vault:read"' in challenge
    assert json.loads(response.text)["error"]["code"] == AUTHENTICATION_REQUIRED
