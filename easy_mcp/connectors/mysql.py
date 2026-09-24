"""MySQL connector: schema discovery and read-only SQL (MySQL and MariaDB).

Every statement runs on its own connection inside a ``READ ONLY``
transaction, with a statement deadline and a row cap, so a client (or the LLM
driving it) can explore and query a database but cannot modify it, hold a
connection for long, or pull an unbounded result set.

* The session is ``READ ONLY`` before anything runs, and each call opens a
  ``START TRANSACTION READ ONLY`` that is always rolled back.
* A read-only transaction does not cover everything a privileged account can
  do: ``SET GLOBAL`` changes the server and ``SELECT ... INTO OUTFILE`` writes
  a file on its disk.  So ``query`` also admits only statements that begin
  with a reading keyword (``SELECT``, ``WITH``, ``SHOW``, ``EXPLAIN``,
  ``DESCRIBE``, ``TABLE``, ``VALUES``) and refuses ``INTO OUTFILE`` /
  ``INTO DUMPFILE`` and MySQL's executable ``/*! ... */`` comments, judged
  with strings and comments stripped.  Connect with a ``SELECT``-only account
  all the same; these checks are the second layer, not the first.
* Multi-statement strings and ``LOAD DATA LOCAL`` are disabled in the driver.
* MySQL's own ``max_execution_time`` covers only ``SELECT``, so a watchdog
  issues ``KILL QUERY`` from a second connection once the deadline passes;
  that stops every kind of statement, ``DO SLEEP(...)`` included.

Credentials: ``MYSQL_URL`` (``mysql://user:password@host:3306/database``).
The URL is never logged and never appears in an error.

Requires the optional driver: ``pip install "easy-mcp-kit[mysql]"``.

Launch::

    MYSQL_URL=mysql://reader:secret@localhost/shop easy-mcp-mysql --transport stdio
    python -m easy_mcp.connectors.mysql --port 8013 --max-rows 200
"""

from __future__ import annotations

import argparse
import base64
import datetime
import decimal
import os
import re
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

from ..exceptions import ToolError
from ..server import MCPServer
from . import _cli

URL_ENV_VAR = "MYSQL_URL"
DEFAULT_STATEMENT_TIMEOUT = 10.0
DEFAULT_MAX_ROWS = 500
HARD_MAX_ROWS = 10_000
CONNECT_TIMEOUT = 10

_SYSTEM_DATABASES = ("information_schema", "mysql", "performance_schema", "sys")

_READ_KEYWORDS = frozenset(
    {"select", "with", "show", "explain", "describe", "desc", "table", "values"}
)
# The session's sql_mode, set outright rather than edited: NO_BACKSLASH_ESCAPES
# or ANSI_QUOTES (which combined modes such as ANSI switch back on) would change
# where a string ends, and so what check_read_statement() saw.  This is MySQL
# 8's default, and MariaDB knows every flag in it.
_SQL_MODE = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)
_FILE_WRITE = re.compile(r"\binto\s+(outfile|dumpfile)\b", re.IGNORECASE)


def _strip_literals(sql: str) -> str:
    """*sql* with string literals, quoted identifiers and comments blanked.

    What is left is only the statement's own keywords, so a check on it
    cannot be fooled by a keyword inside a string, nor dodged by a comment.

    Raises:
        ToolError: The statement holds a ``/*! ... */`` comment, whose body
            MySQL executes as SQL.
    """
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in "'\"`":
            # Both ' and " delimit strings with backslash escapes; _SQL_MODE
            # pins the session so the server reads them the same way.
            i += 1
            while i < n:
                if sql[i] == "\\" and ch != "`":
                    i += 2
                    continue
                if sql[i] == ch:
                    if i + 1 < n and sql[i + 1] == ch:  # a doubled quote
                        i += 2
                        continue
                    break
                i += 1
            i += 1
            out.append(" ? ")
        elif sql.startswith("/*", i):
            if sql.startswith("/*!", i):
                raise ToolError("executable /*! ... */ comments are not allowed")
            end = sql.find("*/", i + 2)
            i = n if end < 0 else end + 2
            out.append(" ")
        elif ch == "#" or (sql.startswith("--", i) and (i + 2 == n or sql[i + 2].isspace())):
            end = sql.find("\n", i)
            i = n if end < 0 else end + 1
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def check_read_statement(sql: str) -> None:
    """Refuse statements that could act beyond a read-only transaction.

    Raises:
        ToolError: The statement is not a read, or writes a server file.
    """
    bare = _strip_literals(sql)
    match = re.match(r"[\s(]*([A-Za-z]+)", bare)
    first = match.group(1).lower() if match else ""
    if first not in _READ_KEYWORDS:
        raise ToolError(
            "only reading statements are allowed "
            "(SELECT, WITH, SHOW, EXPLAIN, DESCRIBE, TABLE, VALUES)"
        )
    if _FILE_WRITE.search(bare):
        raise ToolError("INTO OUTFILE / INTO DUMPFILE writes a file on the server; not allowed")


@dataclass(frozen=True)
class ConnectionSettings:
    """Where and as whom to connect; parsed from ``MYSQL_URL``."""

    host: str
    port: int
    user: str
    password: str
    database: str | None

    @classmethod
    def from_url(cls, url: str) -> ConnectionSettings:
        """Parse ``mysql://user:password@host:port/database``.

        Raises:
            ValueError: Not a mysql:// URL, or no user.  The message never
                repeats the URL, which carries the password.
        """
        parts = urlsplit(url)
        if parts.scheme not in ("mysql", "mariadb"):
            raise ValueError(f"{URL_ENV_VAR} must start with mysql:// or mariadb://")
        if not parts.username:
            raise ValueError(f"{URL_ENV_VAR} must name a user (mysql://user:password@host/db)")
        try:
            port = parts.port or 3306
        except ValueError:
            raise ValueError(f"{URL_ENV_VAR} has an invalid port") from None
        database = unquote(parts.path.lstrip("/")) or None
        return cls(
            host=parts.hostname or "localhost",
            port=port,
            user=unquote(parts.username),
            password=unquote(parts.password or ""),
            database=database,
        )


def _require_driver() -> None:
    try:
        import pymysql  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "the MySQL connector needs PyMySQL: pip install 'easy-mcp-kit[mysql]'"
        ) from None


def connect(settings: ConnectionSettings, statement_timeout: float) -> Any:
    """Open a connection whose session is read-only."""
    import pymysql

    return pymysql.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
        connect_timeout=CONNECT_TIMEOUT,
        # A backstop: the watchdog normally ends a statement long before.
        read_timeout=int(statement_timeout) + CONNECT_TIMEOUT,
        charset="utf8mb4",
        autocommit=False,
        local_infile=False,  # the server must never pull client files
        init_command="SET SESSION TRANSACTION READ ONLY",
        program_name="easy-mcp-mysql",
    )


def _is_driver_error(exc: BaseException) -> bool:
    # Checked by module name so the connector (and its tests) never import
    # the driver just to recognise its exceptions.
    return type(exc).__module__.split(".", 1)[0] == "pymysql"


def _jsonable(value: Any) -> Any:
    if isinstance(value, bytes | bytearray):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, decimal.Decimal):
        return str(value)  # exact; a float would round money
    if isinstance(value, datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return str(value)  # MySQL TIME columns arrive as timedelta
    return value


def _run(
    connector: Callable[[], Any],
    killer: Callable[[int], None] | None,
    sql: str,
    params: Sequence[Any] = (),
    *,
    limit: int,
    timeout: float,
) -> tuple[list[str], list[list[Any]], bool]:
    """Execute *sql* read-only; returns ``(columns, rows, truncated)``."""
    timed_out = threading.Event()
    watchdog: threading.Timer | None = None
    try:
        connection = connector()
        try:
            if killer is not None:
                thread_id = connection.thread_id()

                def expire() -> None:
                    timed_out.set()
                    killer(thread_id)

                watchdog = threading.Timer(timeout, expire)
                watchdog.daemon = True
                watchdog.start()
            with connection.cursor() as cursor:
                cursor.execute(f"SET SESSION sql_mode = '{_SQL_MODE}'")
                cursor.execute("START TRANSACTION READ ONLY")
                cursor.execute(sql, tuple(params) or None)
                columns = [column[0] for column in cursor.description or ()]
                fetched = cursor.fetchmany(limit + 1) if columns else []
                if timed_out.is_set():
                    # A killed SLEEP() returns normally; its result is still
                    # the product of an interrupted statement.
                    raise ToolError(
                        f"Database error: statement exceeded the {timeout:g}s time limit"
                    )
                rows = [[_jsonable(value) for value in row] for row in fetched[:limit]]
                return columns, rows, len(fetched) > limit
        finally:
            if watchdog is not None:
                watchdog.cancel()
            try:
                connection.rollback()
            finally:
                connection.close()
    except Exception as exc:
        if not _is_driver_error(exc):
            raise
        if timed_out.is_set():
            raise ToolError(
                f"Database error: statement exceeded the {timeout:g}s time limit"
            ) from None
        # Server messages (syntax errors, unknown columns, a write refused in
        # a read-only transaction) are what the client needs to fix its
        # query; the URL never appears.
        args: tuple[Any, ...] = exc.args
        detail = args[1] if len(args) > 1 and isinstance(args[1], str) else str(exc)
        raise ToolError(f"Database error: {detail}") from None


def build_server(
    *,
    url: str | None = None,
    statement_timeout: float = DEFAULT_STATEMENT_TIMEOUT,
    max_rows: int = DEFAULT_MAX_ROWS,
    connector: Callable[[], Any] | None = None,
    **server_options: Any,
) -> MCPServer:
    """Build the MySQL connector server.

    Args:
        url: Connection URL; defaults to the ``MYSQL_URL`` environment
            variable.  Its database, if any, is the default for the tools.
        statement_timeout: Seconds a single statement may run before it is
            killed.
        max_rows: Hard cap on rows returned by ``query`` (its ``limit``
            argument cannot exceed this).
        connector: Injectable zero-argument factory returning a DB-API style
            connection (tests).  Skips the driver check and the watchdog.
        **server_options: Passed to :class:`~easy_mcp.server.MCPServer`.

    Raises:
        ValueError: No or malformed URL, or an out-of-range setting.
        RuntimeError: The ``pymysql`` driver is not installed.
    """
    if statement_timeout <= 0:
        raise ValueError("statement_timeout must be positive")
    if not 1 <= max_rows <= HARD_MAX_ROWS:
        raise ValueError(f"max_rows must be between 1 and {HARD_MAX_ROWS}")
    open_connection: Callable[[], Any]
    killer: Callable[[int], None] | None
    resolved = url if url is not None else os.environ.get(URL_ENV_VAR)
    settings = ConnectionSettings.from_url(resolved) if resolved else None
    default_database = settings.database if settings is not None else None
    if connector is not None:
        open_connection = connector
        killer = None
    else:
        _require_driver()
        if settings is None:
            raise ValueError(f"no connection URL: set {URL_ENV_VAR}")

        def open_connection() -> Any:
            return connect(settings, statement_timeout)

        def killer(thread_id: int) -> None:
            # A second connection, as the same user, may kill its own query.
            try:
                admin = connect(settings, statement_timeout)
                try:
                    with admin.cursor() as cursor:
                        cursor.execute("KILL QUERY %s", (thread_id,))
                finally:
                    admin.close()
            except Exception:  # the query may have finished meanwhile
                pass

    def run(sql: str, params: Sequence[Any] = (), *, limit: int) -> Any:
        return _run(open_connection, killer, sql, params, limit=limit, timeout=statement_timeout)

    def database_or_default(database: str | None) -> str:
        chosen = database or default_database
        if not chosen:
            raise ToolError("no database given and MYSQL_URL names none")
        return chosen

    server_options.setdefault("name", "easy-mcp-mysql")
    server_options.setdefault(
        "instructions",
        "Read-only MySQL access. List databases and tables first, then query "
        "with MySQL SQL; every statement runs in a READ ONLY transaction, is "
        f"stopped after {statement_timeout:g}s and returns at most {max_rows} rows.",
    )
    server_options.setdefault("default_timeout", statement_timeout + CONNECT_TIMEOUT + 5)
    server = MCPServer(**server_options)

    @server.tool
    def list_databases() -> list[str]:
        """List user databases (system databases are omitted)."""
        _, rows, _ = run(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN (%s, %s, %s, %s) ORDER BY schema_name",
            _SYSTEM_DATABASES,
            limit=HARD_MAX_ROWS,
        )
        return [row[0] for row in rows]

    @server.tool
    def list_tables(database: str | None = None) -> list[dict[str, Any]]:
        """List tables and views in a database.

        Args:
            database: Database name; defaults to the one in MYSQL_URL.
        """
        _, rows, _ = run(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (database_or_default(database),),
            limit=HARD_MAX_ROWS,
        )
        return [
            {"name": name, "type": "view" if kind == "VIEW" else "table"} for name, kind in rows
        ]

    @server.tool
    def describe_table(table: str, database: str | None = None) -> dict[str, Any]:
        """Describe a table's columns, primary key and foreign keys.

        Args:
            table: Table or view name.
            database: Database name; defaults to the one in MYSQL_URL.
        """
        schema = database_or_default(database)
        _, columns, _ = run(
            "SELECT column_name, column_type, is_nullable, column_default, column_key "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
            limit=HARD_MAX_ROWS,
        )
        if not columns:
            raise ToolError(f"no table or view named {schema}.{table}")
        _, keys, _ = run(
            "SELECT column_name, referenced_table_name, referenced_column_name, constraint_name "
            "FROM information_schema.key_column_usage "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY constraint_name, ordinal_position",
            (schema, table),
            limit=HARD_MAX_ROWS,
        )
        return {
            "database": schema,
            "table": table,
            "columns": [
                {"name": name, "type": kind, "nullable": nullable == "YES", "default": default}
                for name, kind, nullable, default, _ in columns
            ],
            "primary_key": [column for column, _, _, constraint in keys if constraint == "PRIMARY"],
            "foreign_keys": [
                {"column": column, "references_table": target, "references_column": to}
                for column, target, to, _ in keys
                if target is not None
            ],
        }

    @server.tool
    def query(sql: str, limit: int = 100) -> dict[str, Any]:
        """Run one read-only SQL statement and return the rows.

        The statement runs in a READ ONLY transaction with a time limit; data
        modification is rejected by the database, and statements that do not
        begin with a reading keyword or that write a file are refused.
        Unqualified table names resolve in the database named in MYSQL_URL.

        Args:
            sql: One SQL statement (SELECT, WITH, SHOW, EXPLAIN, DESCRIBE ...).
            limit: Maximum rows to return.
        """
        if not sql.strip():
            raise ToolError("sql must not be empty")
        if not 1 <= limit <= max_rows:
            raise ToolError(f"limit must be between 1 and {max_rows}")
        check_read_statement(sql)
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
    """Command-line entry point (``easy-mcp-mysql``)."""
    parser = _cli.build_parser(
        f"Serve read-only MySQL access over MCP. Reads the connection URL from ${URL_ENV_VAR}."
    )
    parser.add_argument(
        "--statement-timeout",
        type=float,
        default=DEFAULT_STATEMENT_TIMEOUT,
        metavar="SECONDS",
        help="per-statement time limit, enforced with KILL QUERY",
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
