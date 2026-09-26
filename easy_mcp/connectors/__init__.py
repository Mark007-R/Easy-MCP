"""Ready-made MCP servers built on easy_mcp's own ``@server.tool``.

Each connector exposes a ``build_server(...)`` factory returning a regular
:class:`~easy_mcp.server.MCPServer` (so every security default applies) and a
``main()`` entry point for launching it with one command::

    easy-mcp-github --transport stdio          # or: python -m easy_mcp.connectors.github
    easy-mcp-postgres --port 8010              # or: python -m easy_mcp.connectors.postgres
    easy-mcp-sqlite --database shop.db         # or: python -m easy_mcp.connectors.sqlite

Credentials are read from the environment, never from arguments (a SQLite
file path is not a secret, so it may also be passed as ``--database``):

* GitHub: ``GITHUB_TOKEN`` (optional; unauthenticated access is public-only
  and tightly rate limited by GitHub).
* Postgres: ``DATABASE_URL`` (a libpq connection string / URI).
* SQLite: ``SQLITE_PATH`` (the database file).

Protected tools (GitHub writes) and per-client scopes use the usual
``EASY_MCP_API_KEYS`` / ``EASY_MCP_STDIO_API_KEY`` mechanism.
"""

from __future__ import annotations
