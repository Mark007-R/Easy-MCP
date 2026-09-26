"""MCP protocol revisions this package speaks, and the per-request metadata.

Two eras of the protocol exist, and the server speaks both:

* **Modern** revisions (``2026-07-28``) are stateless.  There is no
  handshake: every request carries its protocol version and the client's
  capabilities in ``params._meta``, and every result carries a
  ``resultType``.  A client that wants to know the server up front calls
  ``server/discover``.
* **Legacy** revisions (``2025-11-25`` and earlier) open with an
  ``initialize`` handshake.  During it the server echoes the client's
  requested version when it is supported and otherwise offers
  :data:`LATEST_LEGACY_PROTOCOL_VERSION`, as the lifecycle spec requires.

The era is chosen per request: one that carries modern ``_meta`` is served
statelessly, anything else keeps the legacy behaviour.
"""

from __future__ import annotations

from typing import Any

from .exceptions import INVALID_PARAMS, UNSUPPORTED_PROTOCOL_VERSION, ProtocolError

MODERN_PROTOCOL_VERSIONS: tuple[str, ...] = ("2026-07-28",)

LEGACY_PROTOCOL_VERSIONS: tuple[str, ...] = (
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
)

SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = MODERN_PROTOCOL_VERSIONS + LEGACY_PROTOCOL_VERSIONS

LATEST_PROTOCOL_VERSION = MODERN_PROTOCOL_VERSIONS[0]
LATEST_LEGACY_PROTOCOL_VERSION = LEGACY_PROTOCOL_VERSIONS[0]

# Reserved _meta keys of the modern revision.
META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
META_CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities"
META_CLIENT_INFO = "io.modelcontextprotocol/clientInfo"
META_SERVER_INFO = "io.modelcontextprotocol/serverInfo"

_MODERN_REQUEST_KEYS = (META_PROTOCOL_VERSION, META_CLIENT_CAPABILITIES, META_CLIENT_INFO)

DISCOVER_METHOD = "server/discover"


def negotiate_protocol_version(requested: object) -> str:
    """The version to answer an ``initialize`` request with.

    ``initialize`` belongs to the legacy era, so a modern version requested
    through it is answered with the newest legacy one.
    """
    if isinstance(requested, str) and requested in LEGACY_PROTOCOL_VERSIONS:
        return requested
    return LATEST_LEGACY_PROTOCOL_VERSION


def is_modern_request(method: object, params: dict[str, Any]) -> bool:
    """Whether a request opted into the stateless (modern) era.

    Any reserved per-request key selects it, so a request that carries only
    some of them is judged -- and rejected -- as a malformed modern request
    instead of being quietly served under legacy rules.  ``server/discover``
    exists only in the modern era.
    """
    if method == DISCOVER_METHOD:
        return True
    meta = params.get("_meta")
    return isinstance(meta, dict) and any(key in meta for key in _MODERN_REQUEST_KEYS)


def check_request_meta(params: dict[str, Any]) -> None:
    """Validate the per-request fields a modern request must carry.

    Raises:
        ProtocolError: ``-32022`` (UnsupportedProtocolVersion) naming the
            versions to retry with, when the requested version is not a
            stateless one this server speaks; ``-32602`` when a required
            field is missing or malformed.
    """
    meta = params.get("_meta")
    if not isinstance(meta, dict):
        meta = {}
    version = meta.get(META_PROTOCOL_VERSION)
    if not isinstance(version, str):
        raise ProtocolError(
            f"Invalid params: _meta must carry '{META_PROTOCOL_VERSION}'",
            code=INVALID_PARAMS,
        )
    if version not in MODERN_PROTOCOL_VERSIONS:
        # A legacy version is listed as supported but cannot be used here: it
        # is spoken only after an initialize handshake.
        raise ProtocolError(
            "Unsupported protocol version",
            code=UNSUPPORTED_PROTOCOL_VERSION,
            data={"supported": list(SUPPORTED_PROTOCOL_VERSIONS), "requested": version},
        )
    if not isinstance(meta.get(META_CLIENT_CAPABILITIES), dict):
        raise ProtocolError(
            f"Invalid params: _meta must carry '{META_CLIENT_CAPABILITIES}' as an object",
            code=INVALID_PARAMS,
        )
