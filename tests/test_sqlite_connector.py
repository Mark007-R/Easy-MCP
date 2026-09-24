"""SQLite connector, against real database files (SQLite ships with Python)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from conftest import make_context, rpc

from easy_mcp import MCPServer
from easy_mcp.connectors import sqlite


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
