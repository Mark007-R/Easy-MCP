"""The package version: the single source of truth.

``pyproject.toml`` reads it at build time (hatch dynamic version) and
:class:`~easy_mcp.server.MCPServer` reports it during the MCP handshake by
default, so a release bump happens in exactly one place.
"""

__version__ = "0.2.4"
