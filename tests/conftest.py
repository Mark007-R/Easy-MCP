"""Shared test helpers and fixtures."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
import uvicorn

from easy_mcp import MCPServer
from easy_mcp.security.auth import ClientIdentity
from easy_mcp.transport.base import ClientContext

# The stateless protocol revision the modern-era helpers below speak.
STATELESS_VERSION = "2026-07-28"


def make_context(
    identity: ClientIdentity | None = None,
    client_id: str = "ip:test",
    session_id: str = "test-session",
) -> ClientContext:
    """A fresh ClientContext, as a transport would build one."""
    return ClientContext(client_id=client_id, session_id=session_id, identity=identity)


def rpc(method: str, params: Any | None = None, msg_id: Any = 1) -> dict[str, Any]:
    """Build a JSON-RPC request message."""
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": msg_id, "method": method}
    if params is not None:
        message["params"] = params
    return message


def notification(method: str, params: Any | None = None) -> dict[str, Any]:
    """Build a JSON-RPC notification (no id)."""
    message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        message["params"] = params
    return message


def meta(**overrides: Any) -> dict[str, Any]:
    """The per-request ``_meta`` of a stateless request; ``None`` drops a key."""
    fields: dict[str, Any] = {
        "io.modelcontextprotocol/protocolVersion": STATELESS_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "tests", "version": "1.0"},
    }
    fields.update(overrides)
    return {key: value for key, value in fields.items() if value is not None}


def modern(
    method: str, params: dict[str, Any] | None = None, msg_id: Any = 1, **meta_overrides: Any
) -> dict[str, Any]:
    """Build a stateless (2026-07-28) JSON-RPC request."""
    return rpc(method, {**(params or {}), "_meta": meta(**meta_overrides)}, msg_id)


def headers_for(message: dict[str, Any], **extra: str) -> dict[str, str]:
    """The mirrored headers a conforming client sends with *message*."""
    headers = {
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": STATELESS_VERSION,
        "Mcp-Method": message["method"],
    }
    name = message.get("params", {}).get("name")
    if message["method"] == "tools/call" and name is not None:
        headers["Mcp-Name"] = name
    headers.update(extra)
    return headers


@pytest.fixture
def server() -> MCPServer:
    """A bare server with rate limiting disabled for deterministic tests."""
    return MCPServer(port=0, rate_limit_per_minute=None)


@pytest.fixture
def live_server() -> Iterator[Callable[[Any], str]]:
    """Serve an MCPServer (or a built ASGI app) on an ephemeral port in a
    background thread; returns the base URL.  Servers stop after the test."""
    running: list[tuple[uvicorn.Server, threading.Thread]] = []

    def start(target: Any) -> str:
        app = target.build_app() if isinstance(target, MCPServer) else target
        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
        uv = uvicorn.Server(config)
        thread = threading.Thread(target=uv.run, daemon=True)
        thread.start()
        deadline = time.time() + 10
        while not uv.started:
            if time.time() > deadline:
                raise RuntimeError("uvicorn failed to start within 10s")
            time.sleep(0.01)
        running.append((uv, thread))
        port = uv.servers[0].sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    yield start

    for uv, thread in running:
        uv.should_exit = True
        thread.join(timeout=5)


class LogCapture(logging.Handler):
    """Records from the ``easy_mcp`` loggers, audit events included.

    Attached to the ``easy_mcp`` logger itself: it does not propagate to the
    root logger (``configure_logging``), so pytest's ``caplog`` sees its
    records only by accident of test order.
    """

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    @property
    def text(self) -> str:
        return "\n".join(record.getMessage() for record in self.records)

    def events(self, kind: str) -> list[dict[str, Any]]:
        """The payloads of the audit events named *kind*."""
        return [
            record.event  # type: ignore[attr-defined]
            for record in self.records
            if record.name == "easy_mcp.audit" and record.getMessage() == kind
        ]


@pytest.fixture
def logs() -> Iterator[LogCapture]:
    """Capture what the server logs and audits during the test."""
    logger = logging.getLogger("easy_mcp")
    capture = LogCapture()
    level = logger.level
    if level == logging.NOTSET or level > logging.INFO:
        logger.setLevel(logging.INFO)
    logger.addHandler(capture)
    try:
        yield capture
    finally:
        logger.removeHandler(capture)
        logger.setLevel(level)


# ------------------------------------------------------------------- OAuth
#
# Keys are generated in the test process, once per session.  cryptography and
# PyJWT (the [dev] extra) are imported inside the fixtures, so the rest of the
# suite never depends on them.  Tokens are minted with oauth_fake_as.mint_token.

# The resource (and audience) of the OAuth test servers.  It need not be the
# live server's URL: it is what tokens and the metadata document name.
OAUTH_RESOURCE = "https://mcp.example.com/mcp"


@pytest.fixture(scope="session")
def rsa_key() -> Any:
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def rsa_key_2() -> Any:
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def rsa_1024_key() -> Any:
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=1024)


@pytest.fixture(scope="session")
def ec_p256_key() -> Any:
    from cryptography.hazmat.primitives.asymmetric import ec

    return ec.generate_private_key(ec.SECP256R1())


@pytest.fixture(scope="session")
def ec_p384_key() -> Any:
    from cryptography.hazmat.primitives.asymmetric import ec

    return ec.generate_private_key(ec.SECP384R1())


@pytest.fixture(scope="session")
def ed25519_key() -> Any:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    return ed25519.Ed25519PrivateKey.generate()


@pytest.fixture
def fake_as(live_server: Callable[[Any], str], rsa_key: Any) -> Any:
    """A local authorization server (tests/oauth_fake_as.py) signing with ``rsa_key``."""
    from oauth_fake_as import FakeAuthorizationServer, SigningKey

    fake = FakeAuthorizationServer(SigningKey("k1", "RS256", rsa_key), audience=OAUTH_RESOURCE)
    fake.issuer = live_server(fake.app())
    return fake
