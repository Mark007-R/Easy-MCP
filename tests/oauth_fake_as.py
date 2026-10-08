"""A local OAuth authorization server for the OAuth tests (not collected by pytest).

It serves, on 127.0.0.1 through the ``live_server`` fixture, what a resource
server reads from an authorization server: RFC 8414 metadata (and, if asked,
OpenID Connect discovery), a JWK Set and an RFC 7662 introspection endpoint
that checks HTTP Basic credentials.  For the opt-in SDK client test it also
runs a minimal authorization-code flow: ``/authorize`` approves at once
(PKCE S256 required, ``resource`` honoured) and ``/token`` issues signed JWTs.

Everything is controlled from the test: ``rotate()``, ``set_keys()``,
``fail(route, mode)``, ``fail_token(token, mode)``, ``delays``,
``set_introspection(token, answer)``, ``counters``.
Keys are generated in the test process; nothing leaves the machine.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import secrets
import time
import uuid
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, quote_plus, urlencode

import jwt
from jwt.algorithms import ECAlgorithm, OKPAlgorithm, RSAAlgorithm
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Route

CLIENT_ID = "resource-server"
CLIENT_SECRET = "s3cret: with spaces & symbols"  # form-encoding matters


@dataclass
class SigningKey:
    """A private key the fake server signs with, and how it publishes it."""

    kid: str | None
    alg: str
    private: Any
    # Extra JWK members to publish (alg, use, key_ops), or ones to override.
    jwk_extra: dict[str, Any] = field(default_factory=dict)

    def public_jwk(self) -> dict[str, Any]:
        public = self.private.public_key()
        kind = type(public).__name__
        if "RSA" in kind:
            jwk = RSAAlgorithm.to_jwk(public, as_dict=True)
        elif "Ed25519" in kind or "Ed448" in kind:
            jwk = OKPAlgorithm.to_jwk(public, as_dict=True)
        else:
            jwk = ECAlgorithm.to_jwk(public, as_dict=True)
        jwk = {k: v for k, v in jwk.items() if k != "key_ops"}
        if self.kid is not None:
            jwk["kid"] = self.kid
        jwk.update(self.jwk_extra)
        return jwk


def b64url(data: bytes | Mapping[str, Any]) -> str:
    raw = json.dumps(dict(data)).encode() if isinstance(data, Mapping) else data
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def craft(header: Mapping[str, Any], payload: Mapping[str, Any], signature: bytes = b"sig") -> str:
    """An unsigned (or hand-signed) JWT, for tokens a library refuses to make."""
    return f"{b64url(header)}.{b64url(payload)}.{b64url(signature)}"


def mint_token(
    key: SigningKey,
    *,
    issuer: str,
    audience: str | list[str],
    claims: Mapping[str, Any] | None = None,
    headers: Mapping[str, Any] | None = None,
    drop: Iterable[str] = (),
    now: float | None = None,
    alg: str | None = None,
) -> str:
    """A signed access token; defaults to a valid RFC 9068 one for *issuer* and *audience*."""
    issued = int(now if now is not None else time.time())
    payload: dict[str, Any] = {
        "iss": issuer,
        "aud": audience,
        "sub": "user-1",
        "client_id": "client-1",
        "iat": issued,
        "exp": issued + 300,
        "jti": uuid.uuid4().hex,
        "scope": "mcp:access",
    }
    payload.update(claims or {})
    for name in drop:
        payload.pop(name, None)
    header: dict[str, Any] = {"typ": "at+jwt"}
    if key.kid is not None:
        header["kid"] = key.kid
    header.update(headers or {})
    return jwt.encode(payload, key.private, algorithm=alg or key.alg, headers=header)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class FakeAuthorizationServer:
    """The authorization server of the OAuth tests; ``issuer`` is set once it serves."""

    def __init__(self, *keys: SigningKey, audience: str = "") -> None:
        self.keys: list[SigningKey] = list(keys)
        self.issuer = ""  # set by the fixture once the port is known
        self.audience = audience
        self.counters: Counter[str] = Counter()
        self.failures: dict[str, Any] = {}
        self.delays: dict[str, float] = {}  # seconds a route waits before answering
        self.serve_rfc8414 = True
        self.serve_oidc = False
        self.metadata_issuer: str | None = None  # an override, to test mismatches
        self.introspection: dict[str, dict[str, Any]] = {}
        self.introspection_requests: list[dict[str, Any]] = []
        # Introspection failures for one token only (a WAF rule that matches it).
        self.token_failures: dict[str, Any] = {}
        self.grant_scopes: str = "mcp:access"
        # How long the "timeout" failure stalls; clients give up long before.
        self.stall_seconds = 2.0
        self._codes: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------ controls

    def rotate(self, key: SigningKey) -> None:
        """Publish *key* alone: tokens of the old keys no longer verify after a refresh."""
        self.keys = [key]

    def set_keys(self, keys: Iterable[SigningKey]) -> None:
        self.keys = list(keys)

    def fail(self, route: str, mode: Any) -> None:
        """Make *route* fail: an HTTP status, "timeout", "redirect", "huge" or "not_json"."""
        self.failures[route] = mode

    def heal(self) -> None:
        self.failures.clear()

    def fail_token(self, token: str, mode: Any) -> None:
        """Answer the introspection of *token* alone with *mode*.

        An HTTP status; "timeout" (stall); "not_json" (``200`` with an HTML
        page, as many WAFs block); or bytes, sent as a ``200`` JSON body as is.
        """
        self.token_failures[token_hash(token)] = mode

    def set_introspection(self, token: str, answer: Mapping[str, Any]) -> None:
        self.introspection[token_hash(token)] = dict(answer)

    def mint(self, key: SigningKey | None = None, **kwargs: Any) -> str:
        kwargs.setdefault("issuer", self.issuer)
        kwargs.setdefault("audience", self.audience)
        return mint_token(key or self.keys[0], **kwargs)

    # ----------------------------------------------------------------- app

    def metadata(self) -> dict[str, Any]:
        return {
            "issuer": self.metadata_issuer if self.metadata_issuer is not None else self.issuer,
            "jwks_uri": f"{self.issuer}/jwks",
            "introspection_endpoint": f"{self.issuer}/introspect",
            "authorization_endpoint": f"{self.issuer}/authorize",
            "token_endpoint": f"{self.issuer}/token",
            "registration_endpoint": f"{self.issuer}/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
            "authorization_response_iss_parameter_supported": True,
        }

    async def _failure(self, route: str) -> Response | None:
        self.counters[route] += 1
        if self.delays.get(route):
            await asyncio.sleep(self.delays[route])
        mode = self.failures.get(route)
        if mode is None:
            return None
        if mode == "timeout":
            await asyncio.sleep(self.stall_seconds)
            return Response(status_code=504)
        if mode == "redirect":
            return RedirectResponse("http://127.0.0.1:9/elsewhere", status_code=302)
        if mode == "huge":
            return Response(b'{"keys": [' + b" " * (2 * 1024 * 1024) + b"]}")
        if mode == "not_json":
            return Response(b"<html>not json</html>", media_type="text/html")
        return JSONResponse({"error": "failure"}, status_code=int(mode))

    async def _rfc8414(self, request: Request) -> Response:
        failed = await self._failure("rfc8414")
        if failed is not None:
            return failed
        if not self.serve_rfc8414:
            return Response(status_code=404)
        return JSONResponse(self.metadata())

    async def _oidc(self, request: Request) -> Response:
        failed = await self._failure("oidc")
        if failed is not None:
            return failed
        if not self.serve_oidc:
            return Response(status_code=404)
        return JSONResponse(self.metadata())

    async def _jwks(self, request: Request) -> Response:
        failed = await self._failure("jwks")
        if failed is not None:
            return failed
        return JSONResponse({"keys": [key.public_jwk() for key in self.keys]})

    async def _introspect(self, request: Request) -> Response:
        form = await _form(request)
        self.introspection_requests.append({"headers": dict(request.headers), "form": form})
        failed = await self._failure("introspect")
        if failed is not None:
            return failed
        mode = self.token_failures.get(token_hash(str(form.get("token", ""))))
        if mode == "timeout":
            await asyncio.sleep(self.stall_seconds)
            return Response(status_code=504)
        if mode == "not_json":
            return Response(b"<html>Request rejected</html>", media_type="text/html")
        if isinstance(mode, bytes):
            return Response(mode, media_type="application/json")
        if mode is not None:
            return JSONResponse({"error": "blocked"}, status_code=int(mode))
        expected = "Basic " + base64.b64encode(
            f"{quote_plus(CLIENT_ID)}:{quote_plus(CLIENT_SECRET)}".encode()
        ).decode("ascii")
        if request.headers.get("authorization") != expected:
            return JSONResponse({"error": "invalid_client"}, status_code=401)
        token = str(form.get("token", ""))
        return JSONResponse(self.introspection.get(token_hash(token), {"active": False}))

    async def _register(self, request: Request) -> Response:
        body = await request.json()
        return JSONResponse(
            {**body, "client_id": "sdk-client", "token_endpoint_auth_method": "none"},
            status_code=201,
        )

    async def _authorize(self, request: Request) -> Response:
        params = request.query_params
        if params.get("code_challenge_method") != "S256" or not params.get("code_challenge"):
            return JSONResponse({"error": "invalid_request"}, status_code=400)
        code = secrets.token_urlsafe(16)
        self._codes[code] = {
            "challenge": params["code_challenge"],
            "resource": params.get("resource"),
            "scope": params.get("scope") or self.grant_scopes,
        }
        query = {"code": code, "iss": self.issuer}
        if params.get("state"):
            query["state"] = params["state"]
        return RedirectResponse(f"{params['redirect_uri']}?{urlencode(query)}", status_code=302)

    async def _token(self, request: Request) -> Response:
        form = await _form(request)
        grant = self._codes.pop(str(form.get("code", "")), None)
        if grant is None:
            return JSONResponse({"error": "invalid_grant"}, status_code=400)
        verifier = str(form.get("code_verifier", ""))
        digest = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        if digest.rstrip(b"=").decode() != grant["challenge"]:
            return JSONResponse({"error": "invalid_grant"}, status_code=400)
        audience = form.get("resource") or grant["resource"] or self.audience
        token = self.mint(audience=str(audience), claims={"scope": grant["scope"]})
        return JSONResponse(
            {
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 300,
                "scope": grant["scope"],
            }
        )

    def app(self) -> Starlette:
        return Starlette(
            routes=[
                Route("/.well-known/oauth-authorization-server", self._rfc8414),
                Route("/.well-known/openid-configuration", self._oidc),
                Route("/jwks", self._jwks),
                Route("/introspect", self._introspect, methods=["POST"]),
                Route("/register", self._register, methods=["POST"]),
                Route("/authorize", self._authorize),
                Route("/token", self._token, methods=["POST"]),
            ]
        )


async def _form(request: Request) -> dict[str, str]:
    """An application/x-www-form-urlencoded body, without python-multipart."""
    return dict(parse_qsl((await request.body()).decode(), keep_blank_values=True))
