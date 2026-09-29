"""SQLite connector, against real database files (SQLite ships with Python)."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from conftest import make_context, notification, rpc

from easy_mcp import CancelToken, MCPServer, cancel_scope
from easy_mcp.connectors import sqlite
from easy_mcp.exceptions import TOOL_TIMEOUT, ToolError


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "shop.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, avatar BLOB);
        CREATE TABLE orders (
            id INTEGER,
            line INTEGER,
            customer_id INTEGER REFERENCES customers(id),
            total REAL DEFAULT 0,
            PRIMARY KEY (id, line)
        );
        CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 100;
        CREATE INDEX orders_by_customer ON orders(customer_id);
        INSERT INTO customers VALUES (1, 'Ada', x'0102'), (2, 'Grace', NULL);
        INSERT INTO orders VALUES (1, 1, 1, 50.0), (1, 2, 1, 150.0), (2, 1, 2, 20.0);
        """
    )
    connection.commit()
    connection.close()
    return path


def make(db: Path, **kwargs: Any) -> MCPServer:
    return sqlite.build_server(path=db, rate_limit_per_minute=None, **kwargs)


async def call(server: MCPServer, tool: str, arguments: dict[str, Any] | None = None) -> Any:
    response = await server.dispatch(
        rpc("tools/call", {"name": tool, "arguments": arguments or {}}), make_context()
    )
    assert response is not None
    return response


def ok(response: dict[str, Any]) -> Any:
    assert response["result"]["isError"] is False, response
    return json.loads(response["result"]["content"][0]["text"])


def failure(response: dict[str, Any]) -> str:
    assert response["result"]["isError"] is True, response
    return str(response["result"]["content"][0]["text"])


async def test_list_tables(db: Path) -> None:
    tables = ok(await call(make(db), "list_tables"))
    assert tables == [
        {"name": "big_orders", "type": "view"},
        {"name": "customers", "type": "table"},
        {"name": "orders", "type": "table"},
    ]


async def test_describe_table(db: Path) -> None:
    described = ok(await call(make(db), "describe_table", {"table": "orders"}))
    assert described["primary_key"] == ["id", "line"]
    assert described["foreign_keys"] == [
        {"column": "customer_id", "references_table": "customers", "references_column": "id"}
    ]
    total = next(c for c in described["columns"] if c["name"] == "total")
    assert total == {"name": "total", "type": "REAL", "nullable": True, "default": "0"}

    customers = ok(await call(make(db), "describe_table", {"table": "customers"}))
    name = next(c for c in customers["columns"] if c["name"] == "name")
    assert name["nullable"] is False

    missing = failure(await call(make(db), "describe_table", {"table": "nope"}))
    assert "no table or view named nope" in missing


async def test_query(db: Path) -> None:
    server = make(db)
    result = ok(await call(server, "query", {"sql": "SELECT id, name, avatar FROM customers"}))
    assert result["columns"] == ["id", "name", "avatar"]
    # BLOBs come back as base64.
    assert result["rows"] == [[1, "Ada", "AQI="], [2, "Grace", None]]
    assert result["row_count"] == 2
    assert result["truncated"] is False

    joined = ok(
        await call(
            server,
            "query",
            {
                "sql": "WITH t AS (SELECT customer_id, sum(total) s FROM orders GROUP BY 1) "
                "SELECT name, s FROM t JOIN customers c ON c.id = t.customer_id ORDER BY s DESC"
            },
        )
    )
    assert joined["rows"] == [["Ada", 200.0], ["Grace", 20.0]]


async def test_row_cap(db: Path) -> None:
    server = make(db, max_rows=2)
    capped = ok(await call(server, "query", {"sql": "SELECT * FROM orders", "limit": 2}))
    assert capped["row_count"] == 2
    assert capped["truncated"] is True
    too_many = failure(await call(server, "query", {"sql": "SELECT 1", "limit": 3}))
    assert "limit must be between 1 and 2" in too_many


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO customers VALUES (3, 'Eve', NULL)",
        "UPDATE customers SET name = 'x'",
        "DELETE FROM orders",
        "DROP TABLE orders",
        "CREATE TABLE t (x)",
        "CREATE TEMP TABLE t (x)",
        "ATTACH DATABASE 'other.db' AS other",
        "PRAGMA query_only = OFF",
        "PRAGMA writable_schema = ON",
        "PRAGMA journal_mode = DELETE",
        "VACUUM",
        "BEGIN",
        "SELECT load_extension('evil')",
    ],
)
async def test_everything_but_reading_is_refused(db: Path, sql: str) -> None:
    before = db.read_bytes()
    message = failure(await call(make(db), "query", {"sql": sql}))
    assert message.startswith("Database error:")
    assert db.read_bytes() == before


async def test_attach_cannot_read_other_files(db: Path, tmp_path: Path) -> None:
    secret = tmp_path / "secret.db"
    connection = sqlite3.connect(secret)
    connection.execute("CREATE TABLE s (v)")
    connection.execute("INSERT INTO s VALUES ('hidden')")
    connection.commit()
    connection.close()
    sql = f"ATTACH DATABASE '{secret.as_posix()}' AS s2"
    assert "not authorized" in failure(await call(make(db), "query", {"sql": sql}))


async def test_one_statement_at_a_time(db: Path) -> None:
    message = failure(await call(make(db), "query", {"sql": "SELECT 1; DROP TABLE orders"}))
    assert "one statement" in message


async def test_statement_timeout(db: Path) -> None:
    endless = (
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT count(*) FROM r"
    )
    message = failure(await call(make(db, statement_timeout=0.2), "query", {"sql": endless}))
    assert "exceeded the 0.2s time limit" in message


async def test_bad_sql_is_reported(db: Path) -> None:
    message = failure(await call(make(db), "query", {"sql": "SELECT nope FROM customers"}))
    assert "no such column: nope" in message
    assert "sql must not be empty" in failure(await call(make(db), "query", {"sql": "  "}))


def test_configuration_errors(db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(sqlite.PATH_ENV_VAR, raising=False)
    with pytest.raises(ValueError, match="SQLITE_PATH"):
        sqlite.build_server()
    with pytest.raises(ValueError, match="no SQLite database"):
        sqlite.build_server(path=tmp_path / "missing.db")
    with pytest.raises(ValueError, match="max_rows"):
        sqlite.build_server(path=db, max_rows=0)
    with pytest.raises(ValueError, match="statement_timeout"):
        sqlite.build_server(path=db, statement_timeout=0)

    monkeypatch.setenv(sqlite.PATH_ENV_VAR, str(db))
    assert sqlite.build_server().name == "easy-mcp-sqlite"


def test_cli_refuses_a_missing_database(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        sqlite.main(["--database", str(tmp_path / "missing.db")])
    assert "no SQLite database" in capsys.readouterr().err


async def test_tools_advertise_output_schemas(db: Path) -> None:
    response = await make(db).dispatch(rpc("tools/list"), make_context())
    assert response is not None
    tools = {tool["name"]: tool for tool in response["result"]["tools"]}
    assert set(tools) == {"list_tables", "describe_table", "query"}
    assert tools["query"]["outputSchema"]["type"] == "object"


async def test_infinite_reals_stay_valid_json(db: Path) -> None:
    response = await call(make(db), "query", {"sql": "SELECT 1e999 AS big, -1e999 AS small"})
    assert ok(response)["rows"] == [["Infinity", "-Infinity"]]
    json.dumps(response, allow_nan=False)  # no bare Infinity token anywhere


def test_a_file_that_is_not_a_database_fails_at_startup(tmp_path: Path) -> None:
    junk = tmp_path / "junk.db"
    junk.write_bytes(b"not a database" * 50)
    with pytest.raises(ValueError, match="cannot open the SQLite database"):
        sqlite.build_server(path=junk)


@pytest.mark.skipif(sys.platform != "win32", reason="UNC paths are a Windows feature")
async def test_unc_paths_open(db: Path) -> None:
    # The administrative share (\\localhost\C$\...) reaches the same file.
    unc = Path(r"\\localhost" + "\\" + str(db).replace(":", "$", 1))
    if not unc.is_file():
        pytest.skip("the administrative share is not reachable here")
    assert "file:////localhost/" in sqlite._read_only_uri(unc)
    tables = ok(await call(make(unc), "list_tables"))
    assert {"name": "orders", "type": "table"} in tables


# ------------------------------------------------------------ cancellation

# Counts forever; only an interrupt ends it.
RUNAWAY = "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n) SELECT count(*) FROM n"


def query_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "easy-mcp-tool:query"]


async def wait_until(predicate: Any, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.01)
    return True


async def test_a_cancelled_query_stops_running(db: Path) -> None:
    server = make(db, statement_timeout=60)
    context = make_context()
    call = asyncio.create_task(
        server.dispatch(
            rpc("tools/call", {"name": "query", "arguments": {"sql": RUNAWAY}}, 5), context
        )
    )
    assert await wait_until(lambda: bool(query_threads()), 5)
    await asyncio.sleep(0.2)  # well into the statement
    began = time.perf_counter()
    await server.dispatch(notification("notifications/cancelled", {"requestId": 5}), context)
    assert await asyncio.wait_for(call, 5) is None
    assert await wait_until(lambda: not query_threads(), 1.0)
    assert time.perf_counter() - began < 1.0


async def test_the_server_timeout_stops_the_query_too(db: Path) -> None:
    # A server timeout below the statement limit (warned about at startup)
    # still stops the statement, instead of leaving it to run.
    server = make(db, statement_timeout=60, default_timeout=0.3)
    response = await call(server, "query", {"sql": RUNAWAY})
    assert response["error"]["code"] == TOOL_TIMEOUT
    assert await wait_until(lambda: not query_threads(), 1.0)


def test_a_cancel_just_before_the_statement_still_stops_it(db: Path) -> None:
    # interrupt() is a no-op when no statement runs yet; the progress
    # handler catches a cancel that lands in that gap.
    token = CancelToken()
    connection = sqlite.connect(db, 60, token)
    token.cancel()
    began = time.perf_counter()
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        connection.execute(RUNAWAY).fetchone()
    assert time.perf_counter() - began < 1.0
    connection.close()


def test_the_deadline_is_still_reported_as_such(db: Path) -> None:
    token = CancelToken()
    with cancel_scope(token):
        with pytest.raises(ToolError, match=r"exceeded the 0.2s time limit"):
            sqlite._run(lambda: sqlite.connect(db, 0.2, token), RUNAWAY, limit=5, timeout=0.2)


def test_a_server_timeout_below_the_statement_timeout_is_warned_about(
    db: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="easy_mcp"):
        make(db, statement_timeout=10, default_timeout=5)
    assert "is not longer than the statement timeout" in caplog.text
