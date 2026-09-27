"""SQLite connector: schema discovery and read-only SQL over a database file.

SQLite ships with Python, so this connector needs no extra install and no
server: point it at a file and it serves.  Three layers keep it read-only:

* the file is opened with ``mode=ro``, so SQLite itself refuses to write;
* an authorizer allows only reads, so ``ATTACH`` cannot reach other files on
  disk and ``PRAGMA`` is limited to the schema-inspecting ones; and
* ``PRAGMA query_only`` is set as well, for defence in depth.

SQLite has no statement timeout, so a progress handler aborts any statement
that runs past the deadline, and results are capped at a row limit.

The database path comes from ``--database`` or the ``SQLITE_PATH``
environment variable.

Launch::

    easy-mcp-sqlite --database shop.db --transport stdio
    SQLITE_PATH=shop.db python -m easy_mcp.connectors.sqlite --port 8012
"""

from __future__ import annotations

import argparse
import base64
import math
import os
import sqlite3
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ..exceptions import ToolError
from ..server import MCPServer
from . import _cli

PATH_ENV_VAR = "SQLITE_PATH"
DEFAULT_STATEMENT_TIMEOUT = 10.0
DEFAULT_MAX_ROWS = 500
HARD_MAX_ROWS = 10_000

# How many SQLite VM instructions run between deadline checks.
_PROGRESS_STEPS = 10_000

# Schema-inspecting pragmas; every other pragma (query_only, writable_schema,
# journal_mode, ...) is refused by the authorizer.
_READ_PRAGMAS = frozenset(
    {
        "table_info",
        "table_xinfo",
        "table_list",
        "index_list",
        "index_info",
        "index_xinfo",
        "foreign_key_list",
    }
)

_ALLOWED_ACTIONS = frozenset(
    {
        sqlite3.SQLITE_SELECT,
        sqlite3.SQLITE_READ,
        sqlite3.SQLITE_FUNCTION,
        getattr(sqlite3, "SQLITE_RECURSIVE", 33),  # WITH RECURSIVE
    }
)


def _authorizer(action: int, arg1: str | None, arg2: str | None, db: str | None, _: Any) -> int:
    if action in _ALLOWED_ACTIONS:
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_PRAGMA and arg1 is not None:
        # arg2 carries the pragma's value, set only when assigning one.
        if arg1.lower() in _READ_PRAGMAS:
            return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def _read_only_uri(path: Path) -> str:
    """A ``file:`` URI opening *path* read-only."""
    if path.drive.startswith("\\\\"):
        # A UNC path (\\server\share\...): as_uri() would put the server in
        # the URI's authority, which SQLite refuses, so it goes into the path.
        return f"file:{quote('//' + path.as_posix())}?mode=ro"
    return f"{path.as_uri()}?mode=ro"


def connect(path: Path, statement_timeout: float) -> sqlite3.Connection:
    """Open *path* read-only, with the authorizer and deadline installed."""
    connection = sqlite3.connect(_read_only_uri(path), uri=True)
    connection.execute("PRAGMA query_only = ON")
    connection.set_authorizer(_authorizer)
    deadline = time.monotonic() + statement_timeout
    # A nonzero return aborts the running statement with "interrupted".
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), _PROGRESS_STEPS)
    return connection


def _jsonable(value: Any) -> Any:
    # BLOBs have no JSON form; base64 keeps them lossless and printable.
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    # Neither has infinity (SQLite turns NaN into NULL): a bare Infinity token
    # would make the whole response invalid JSON.
    if isinstance(value, float) and math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def _run(
    connector: Callable[[], sqlite3.Connection],
    sql: str,
    params: Sequence[Any] = (),
    *,
    limit: int,
    timeout: float,
) -> tuple[list[str], list[list[Any]], bool]:
    """Execute *sql* read-only; returns ``(columns, rows, truncated)``."""
    try:
        connection = connector()
    except sqlite3.Error as exc:
        raise ToolError(f"Database error: cannot open the database ({exc})") from None
    try:
        cursor = connection.execute(sql, params)
        if cursor.description is None:
            return [], [], False
        columns = [column[0] for column in cursor.description]
        fetched = cursor.fetchmany(limit + 1)
        rows = [[_jsonable(value) for value in row] for row in fetched[:limit]]
        return columns, rows, len(fetched) > limit
    except sqlite3.Error as exc:
        message = str(exc)
        if message == "interrupted":
            message = f"statement exceeded the {timeout:g}s time limit"
        elif message in ("not authorized", "authorization denied"):
            message = "not authorized: this connector only reads"
        raise ToolError(f"Database error: {message}") from None
    finally:
        connection.close()


def build_server(
    *,
    path: str | os.PathLike[str] | None = None,
    statement_timeout: float = DEFAULT_STATEMENT_TIMEOUT,
    max_rows: int = DEFAULT_MAX_ROWS,
    **server_options: Any,
) -> MCPServer:
    """Build the SQLite connector server.

    Args:
        path: The database file; defaults to the ``SQLITE_PATH`` environment
            variable.  It must already exist -- a read-only connector has
            nothing to offer an empty new file.
        statement_timeout: Seconds a single statement may run before it is
            aborted.
        max_rows: Hard cap on rows returned by ``query`` (its ``limit``
            argument cannot exceed this).
        **server_options: Passed to :class:`~easy_mcp.server.MCPServer`.

    Raises:
        ValueError: No path, a missing file, or an out-of-range setting.
    """
    if statement_timeout <= 0:
        raise ValueError("statement_timeout must be positive")
    if not 1 <= max_rows <= HARD_MAX_ROWS:
        raise ValueError(f"max_rows must be between 1 and {HARD_MAX_ROWS}")
    raw = path if path is not None else os.environ.get(PATH_ENV_VAR)
    if not raw:
        raise ValueError(f"no database: pass --database or set {PATH_ENV_VAR}")
    database = Path(raw).expanduser().resolve()
    if not database.is_file():
        raise ValueError(f"no SQLite database at {database}")
    try:
        # Fail at startup, not on every call, for a file SQLite cannot open or
        # that is not a database (opening alone reads nothing).
        probe = connect(database, statement_timeout)
        try:
            probe.execute("SELECT count(*) FROM sqlite_master").fetchone()
        finally:
            probe.close()
    except sqlite3.Error as exc:
        raise ValueError(f"cannot open the SQLite database at {database}: {exc}") from None

    def open_connection() -> sqlite3.Connection:
        return connect(database, statement_timeout)

    def run(sql: str, params: Sequence[Any] = (), *, limit: int) -> Any:
        return _run(open_connection, sql, params, limit=limit, timeout=statement_timeout)

    server_options.setdefault("name", "easy-mcp-sqlite")
    server_options.setdefault(
        "instructions",
        f"Read-only SQLite access to {database.name}. List tables and describe "
        "them first, then query with SQLite SQL; statements that write are "
        f"refused, each statement may run {statement_timeout:g}s and at most "
        f"{max_rows} rows are returned.",
    )
    server_options.setdefault("default_timeout", statement_timeout + 5)
    server = MCPServer(**server_options)

    @server.tool
    def list_tables() -> list[dict[str, Any]]:
        """List tables and views (SQLite's internal tables are omitted)."""
        _, rows, _ = run(
            "SELECT name, type FROM sqlite_master "
            "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
            "ORDER BY name",
            limit=HARD_MAX_ROWS,
        )
        return [{"name": name, "type": kind} for name, kind in rows]

    @server.tool
    def describe_table(table: str) -> dict[str, Any]:
        """Describe a table's columns, primary key and foreign keys.

        Args:
            table: Table or view name.
        """
        _, columns, _ = run(
            'SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info(?)',
            (table,),
            limit=HARD_MAX_ROWS,
        )
        if not columns:
            raise ToolError(f"no table or view named {table}")
        _, foreign, _ = run(
            'SELECT "from", "table", "to" FROM pragma_foreign_key_list(?) ORDER BY id, seq',
            (table,),
            limit=HARD_MAX_ROWS,
        )
        return {
            "table": table,
            "columns": [
                {"name": name, "type": kind, "nullable": not notnull, "default": default}
                for name, kind, notnull, default, _ in columns
            ],
            "primary_key": [
                name for name, *_, pk in sorted(columns, key=lambda column: column[4]) if pk
            ],
            "foreign_keys": [
                {"column": column, "references_table": target, "references_column": to}
                for column, target, to in foreign
            ],
        }

    @server.tool
    def query(sql: str, limit: int = 100) -> dict[str, Any]:
        """Run one read-only SQL statement and return the rows.

        Statements that would write, attach another database or change a
        setting are refused.

        Args:
            sql: One SQLite statement (SELECT, WITH, EXPLAIN ...).
            limit: Maximum rows to return.
        """
        if not sql.strip():
            raise ToolError("sql must not be empty")
        if not 1 <= limit <= max_rows:
            raise ToolError(f"limit must be between 1 and {max_rows}")
        columns, rows, truncated = run(sql, limit=limit)
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
        }

    return server


# ---------------------------------------------------------------------- CLI


def main(argv: Sequence[str] | None = None) -> None:
    """Command-line entry point (``easy-mcp-sqlite``)."""
    parser = _cli.build_parser(
        f"Serve read-only access to a SQLite database file over MCP. "
        f"The file comes from --database or ${PATH_ENV_VAR}."
    )
    parser.add_argument(
        "--database",
        metavar="PATH",
        help=f"the database file (default: ${PATH_ENV_VAR})",
    )
    parser.add_argument(
        "--statement-timeout",
        type=float,
        default=DEFAULT_STATEMENT_TIMEOUT,
        metavar="SECONDS",
        help="per-statement time limit",
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
            path=args.database,
            statement_timeout=args.statement_timeout,
            max_rows=args.max_rows,
            **_cli.server_kwargs(args),
        )

    _cli.run(build, parser, argv)


if __name__ == "__main__":
    main()
