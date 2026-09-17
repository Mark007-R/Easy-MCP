"""Postgres connector: schema discovery and read-only SQL.

Every tool runs inside a ``READ ONLY`` transaction with a statement timeout
and a row cap, so a client (or the LLM driving it) can explore and query a
database but cannot modify it, hold a connection for long, or pull an
unbounded result set.

Credentials: ``DATABASE_URL`` (a libpq URI or key=value connection string).
The connection string is never logged and never appears in an error.

Requires the optional driver: ``pip install "easy-mcp-kit[postgres]"``.

Launch::

    DATABASE_URL=postgresql://user:pass@host/db easy-mcp-postgres --transport stdio
    python -m easy_mcp.connectors.postgres --port 8011 --max-rows 200
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable, Sequence
from typing import Any

from ..exceptions import ToolError
from ..server import MCPServer
from . import _cli

DSN_ENV_VAR = "DATABASE_URL"
DEFAULT_STATEMENT_TIMEOUT = 10.0
DEFAULT_MAX_ROWS = 500
HARD_MAX_ROWS = 10_000
CONNECT_TIMEOUT = 10

_SYSTEM_SCHEMAS = ("pg_catalog", "information_schema")


def _require_driver() -> None:
    try:
        import psycopg  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "the Postgres connector needs psycopg: pip install 'easy-mcp-kit[postgres]'"
        ) from None


def connect(dsn: str, statement_timeout: float) -> Any:
    """Open a read-only connection with a statement timeout (per tool call).

    ``default_transaction_read_only=on`` and ``Connection.read_only`` both
    apply, so even ``SET TRANSACTION READ WRITE`` inside a query cannot
    escape: the session default is enforced server-side per statement.
    """
    import psycopg

    options = (
        f"-c statement_timeout={int(statement_timeout * 1000)} -c default_transaction_read_only=on"
    )
    connection = psycopg.connect(
        dsn,
        connect_timeout=CONNECT_TIMEOUT,
        options=options,
        application_name="easy-mcp-postgres",
    )
    connection.read_only = True
    return connection


def _is_driver_error(exc: BaseException) -> bool:
    # Checked by module name so the connector (and its tests) never import
    # the driver just to recognise its exceptions.
    return type(exc).__module__.split(".", 1)[0] == "psycopg"


def _run(
    connector: Callable[[], Any], sql: str, params: Sequence[Any] = (), *, limit: int
) -> tuple[list[str], list[list[Any]], bool]:
    """Execute *sql* read-only; returns ``(columns, rows, truncated)``."""
    try:
        with connector() as connection, connection.cursor() as cursor:
            cursor.execute(sql, params)
            if cursor.description is None:
                return [], [], False
            columns = [column.name for column in cursor.description]
            fetched = cursor.fetchmany(limit + 1)
            rows = [list(row) for row in fetched[:limit]]
            return columns, rows, len(fetched) > limit
    except Exception as exc:
        if not _is_driver_error(exc):
            raise
        # Database messages (syntax errors, unknown columns) are what the
        # client needs to fix its query; the connection string never appears.
        detail = getattr(getattr(exc, "diag", None), "message_primary", None) or str(exc)
        raise ToolError(f"Database error: {detail}") from None


def build_server(
    *,
    dsn: str | None = None,
    statement_timeout: float = DEFAULT_STATEMENT_TIMEOUT,
    max_rows: int = DEFAULT_MAX_ROWS,
    connector: Callable[[], Any] | None = None,
    **server_options: Any,
) -> MCPServer:
    """Build the Postgres connector server.

    Args:
        dsn: Connection string; defaults to the ``DATABASE_URL`` environment
            variable.
        statement_timeout: Seconds a single statement may run before Postgres
            cancels it.
        max_rows: Hard cap on rows returned by ``query`` (its ``limit``
            argument cannot exceed this).
        connector: Injectable zero-argument factory returning a DB-API style
            connection (tests).  Skips the driver check.
        **server_options: Passed to :class:`~easy_mcp.server.MCPServer`.

    Raises:
        ValueError: No connection string, or an out-of-range setting.
        RuntimeError: The ``psycopg`` driver is not installed.
    """
    if statement_timeout <= 0:
        raise ValueError("statement_timeout must be positive")
    if not 1 <= max_rows <= HARD_MAX_ROWS:
        raise ValueError(f"max_rows must be between 1 and {HARD_MAX_ROWS}")
    open_connection: Callable[[], Any]
    if connector is not None:
        open_connection = connector
    else:
        _require_driver()
        resolved = dsn if dsn is not None else os.environ.get(DSN_ENV_VAR)
        if not resolved:
            raise ValueError(f"no connection string: set {DSN_ENV_VAR}")

        def open_connection() -> Any:
            return connect(resolved, statement_timeout)

    server_options.setdefault("name", "easy-mcp-postgres")
    server_options.setdefault(
        "instructions",
        "Read-only Postgres access. Discover schemas and tables first, then "
        "query with standard SQL; every statement runs in a READ ONLY "
        f"transaction with a {statement_timeout:g}s timeout and at most "
        f"{max_rows} rows returned.",
    )
    server_options.setdefault("default_timeout", statement_timeout + CONNECT_TIMEOUT + 5)
    server = MCPServer(**server_options)

    @server.tool
    def list_schemas() -> list[str]:
        """List user schemas (system schemas are omitted)."""
        _, rows, _ = _run(
            open_connection,
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN (%s, %s) AND schema_name NOT LIKE 'pg_toast%%' "
            "AND schema_name NOT LIKE 'pg_temp%%' ORDER BY schema_name",
            _SYSTEM_SCHEMAS,
            limit=HARD_MAX_ROWS,
        )
        return [row[0] for row in rows]

    @server.tool
    def list_tables(schema: str = "public") -> list[dict[str, Any]]:
        """List tables and views in a schema.

        Args:
            schema: Schema name.
        """
        _, rows, _ = _run(
            open_connection,
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (schema,),
            limit=HARD_MAX_ROWS,
        )
        return [{"name": name, "type": kind} for name, kind in rows]

    @server.tool
    def describe_table(table: str, schema: str = "public") -> dict[str, Any]:
        """Describe a table's columns and primary key.

        Args:
            table: Table or view name.
            schema: Schema name.
        """
        _, columns, _ = _run(
            open_connection,
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
            limit=HARD_MAX_ROWS,
        )
        if not columns:
            raise ToolError(f"no table or view named {schema}.{table}")
        _, keys, _ = _run(
            open_connection,
            "SELECT kcu.column_name FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "ON kcu.constraint_name = tc.constraint_name "
            "AND kcu.constraint_schema = tc.constraint_schema "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "AND tc.table_schema = %s AND tc.table_name = %s "
            "ORDER BY kcu.ordinal_position",
            (schema, table),
            limit=HARD_MAX_ROWS,
        )
        return {
            "schema": schema,
            "table": table,
            "columns": [
                {
                    "name": name,
                    "type": data_type,
                    "nullable": nullable == "YES",
                    "default": default,
                }
                for name, data_type, nullable, default in columns
            ],
            "primary_key": [row[0] for row in keys],
        }

    @server.tool
    def query(sql: str, limit: int = 100) -> dict[str, Any]:
        """Run a read-only SQL statement and return the rows.

        The statement runs in a READ ONLY transaction with a timeout; data
        modification is rejected by the database.

        Args:
            sql: One SQL statement (SELECT, WITH, EXPLAIN, SHOW ...).
            limit: Maximum rows to return.
        """
        if not sql.strip():
            raise ToolError("sql must not be empty")
        if not 1 <= limit <= max_rows:
            raise ToolError(f"limit must be between 1 and {max_rows}")
        columns, rows, truncated = _run(open_connection, sql, limit=limit)
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
        }

    return server


# ---------------------------------------------------------------------- CLI


def main(argv: Sequence[str] | None = None) -> None:
    """Command-line entry point (``easy-mcp-postgres``)."""
    parser = _cli.build_parser(
        "Serve read-only Postgres access over MCP. "
        f"Reads the connection string from ${DSN_ENV_VAR}."
    )
    parser.add_argument(
        "--statement-timeout",
        type=float,
        default=DEFAULT_STATEMENT_TIMEOUT,
        metavar="SECONDS",
        help="per-statement timeout enforced by Postgres",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=DEFAULT_MAX_ROWS,
        metavar="N",
        help=f"hard cap on rows a query may return (at most {HARD_MAX_ROWS})",
    )

    def build(args: argparse.Namespace) -> MCPServer:
        return build_server(
            statement_timeout=args.statement_timeout,
            max_rows=args.max_rows,
            **_cli.server_kwargs(args),
        )

    _cli.run(build, parser, argv)


if __name__ == "__main__":
    main()
