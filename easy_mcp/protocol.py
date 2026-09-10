"""MCP protocol revisions this package speaks.

Every revision here uses the ``initialize`` handshake.  During it the server
echoes the client's requested version when it is supported and otherwise
offers :data:`LATEST_PROTOCOL_VERSION`, as the MCP lifecycle spec requires.
"""

from __future__ import annotations

LATEST_PROTOCOL_VERSION = "2025-11-25"

SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = (
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
)


def negotiate_protocol_version(requested: object) -> str:
    """The version to answer an ``initialize`` request with."""
    if isinstance(requested, str) and requested in SUPPORTED_PROTOCOL_VERSIONS:
        return requested
    return LATEST_PROTOCOL_VERSION
