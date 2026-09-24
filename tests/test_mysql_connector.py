"""MySQL connector, driven through the dispatcher with a fake DB-API connection
(no server and no driver required; the live checks are described in the PR)."""

from __future__ import annotations

import datetime
import decimal
import json
import threading
from typing import Any

import pytest
from conftest import make_context, rpc

from easy_mcp import MCPServer
from easy_mcp.connectors import mysql
from easy_mcp.connectors.mysql import ConnectionSettings, check_read_statement
from easy_mcp.exceptions import ToolError


class FakeDriverError(Exception):
    """Looks like a PyMySQL error: (errno, message) args, pymysql module."""


FakeDriverError.__module__ = "pymysql.err"


class FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.description: list[tuple[str]] | None = None
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def execute(self, sql: str, params: Any = None) -> None:
        self.conn.executed.append((sql, params))
        if sql.startswith(("SET SESSION", "START TRANSACTION")):
            return
        answer = self.conn.answer(sql, params)
        if isinstance(answer, Exception):
            raise answer
        columns, rows = answer
        self.description = [(c,) for c in columns] if columns is not None else None
        self._rows = rows

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        return self._rows[:size]


class FakeConnection:
    def __init__(self, answer: Any) -> None:
        self.answer = answer
        self.executed: list[tuple[str, Any]] = []
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def thread_id(self) -> int:
        return 42

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def make(answer: Any, **kwargs: Any) -> tuple[MCPServer, list[FakeConnection]]:
    opened: list[FakeConnection] = []

    def connector() -> FakeConnection:
        connection = FakeConnection(answer)
        opened.append(connection)
        return connection

    server = mysql.build_server(
        url="mysql://reader:pw@db/shop", connector=connector, rate_limit_per_minute=None, **kwargs
    )
    return server, opened


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


# --------------------------------------------------------- statement check


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "  (SELECT 1) UNION (SELECT 2)",
        "WITH a AS (SELECT 1) SELECT * FROM a",
        "show tables",
        "DESCRIBE orders",
        "desc orders",
        "EXPLAIN SELECT * FROM orders",
        "TABLE orders",
        "VALUES ROW(1, 2)",
        "SELECT 'into outfile' AS phrase",
        "SELECT `into outfile` FROM t",
        "/* leading comment */ SELECT 1",
        "-- a comment\nSELECT 1",
        "# a comment\nSELECT 1",
        "SELECT 1 INTO @v",
        "SELECT 'it''s' AS x",
        # With backslash escapes on (the connector pins them on) this is one
        # string followed by junk, so the server writes no file either.
        "SELECT 'a\\' INTO OUTFILE '/tmp/x' -- '",
    ],
)
def test_reads_are_admitted(sql: str) -> None:
    check_read_statement(sql)


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("INSERT INTO t VALUES (1)", "only reading"),
        ("SET GLOBAL max_connections = 1", "only reading"),
        ("SET PERSIST max_connections = 1", "only reading"),
        ("DO SLEEP(10)", "only reading"),
        ("CALL cleanup()", "only reading"),
        ("KILL 5", "only reading"),
        ("SHUTDOWN", "only reading"),
        ("LOAD DATA LOCAL INFILE 'x' INTO TABLE t", "only reading"),
        ("# comment\nSET GLOBAL a = 1", "only reading"),
        ("", "only reading"),
        ("SELECT * FROM t INTO OUTFILE '/tmp/x'", "INTO OUTFILE"),
        ("SELECT 'x' INTO DUMPFILE '/tmp/x'", "INTO OUTFILE"),
        ("SELECT 1 INTO/**/OUTFILE '/tmp/x'", "INTO OUTFILE"),
        ("select 1 into\n  outfile '/tmp/x'", "INTO OUTFILE"),
        # An escaped quote inside a string must not hide what follows it.
        ("SELECT 'a\\'' INTO OUTFILE '/tmp/x'", "INTO OUTFILE"),
        ('SELECT "a""" INTO OUTFILE \'/tmp/x\'', "INTO OUTFILE"),
        ("SELECT 1 /*!50000 INTO OUTFILE '/tmp/x' */", "/*!"),
        ("/*! SET GLOBAL a = 1 */ SELECT 1", "/*!"),
    ],
)
def test_everything_else_is_refused(sql: str, reason: str) -> None:
    with pytest.raises(ToolError, match=reason.replace("*", r"\*")):
        check_read_statement(sql)


# ----------------------------------------------------------------- settings


def test_url_parsing() -> None:
    settings = ConnectionSettings.from_url("mysql://read%40er:p%3Aw%21@db.example:3307/shop")
    assert settings == ConnectionSettings("db.example", 3307, "read@er", "p:w!", "shop")
    assert ConnectionSettings.from_url("mariadb://u@h").port == 3306
    assert ConnectionSettings.from_url("mysql://u@h").database is None


@pytest.mark.parametrize(
    "url", ["postgresql://u:secret@h/db", "mysql://h/db", "mysql://u:secret@h:notaport/db"]
)
def test_bad_urls_never_echo_the_password(url: str) -> None:
    with pytest.raises(ValueError) as caught:
        ConnectionSettings.from_url(url)
    assert "secret" not in str(caught.value)


def test_configuration_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    connector = lambda: FakeConnection(None)  # noqa: E731
    with pytest.raises(ValueError, match="max_rows"):
        mysql.build_server(connector=connector, max_rows=0)
    with pytest.raises(ValueError, match="statement_timeout"):
        mysql.build_server(connector=connector, statement_timeout=0)


# -------------------------------------------------------------------- tools


async def test_every_call_is_a_rolled_back_read_only_transaction() -> None:
    server, opened = make(lambda sql, params: (["x"], [(1,)]))
    ok(await call(server, "query", {"sql": "SELECT 1 AS x"}))
    (connection,) = opened
    statements = [sql for sql, _ in connection.executed]
    assert statements[1] == "START TRANSACTION READ ONLY"
    assert statements[2] == "SELECT 1 AS x"
    # User SQL is never %-formatted, so a literal % needs no escaping.
    assert connection.executed[2][1] is None
    assert connection.rolled_back and connection.closed


async def test_query_converts_values_and_caps_rows() -> None:
    rows = [
        (
            decimal.Decimal("12.50"),
            datetime.date(2026, 1, 2),
            b"\x01\x02",
            datetime.timedelta(hours=1),
        ),
        (None, None, None, None),
        (None, None, None, None),
    ]
    server, _ = make(lambda sql, params: (["a", "b", "c", "d"], rows), max_rows=5)
    result = ok(await call(server, "query", {"sql": "SELECT a, b, c, d FROM t", "limit": 2}))
    assert result["rows"] == [["12.50", "2026-01-02", "AQI=", "1:00:00"], [None, None, None, None]]
    assert result["truncated"] is True
    assert "limit must be between 1 and 5" in failure(
        await call(server, "query", {"sql": "SELECT 1", "limit": 6})
    )


async def test_refused_statements_never_reach_the_database() -> None:
    server, opened = make(lambda sql, params: ([], []))
    message = failure(await call(server, "query", {"sql": "SET GLOBAL max_connections = 1"}))
    assert "only reading statements" in message
    assert opened == []


async def test_database_errors_are_reported_without_the_url() -> None:
    error = FakeDriverError(1054, "Unknown column 'nope' in 'field list'")
    server, _ = make(lambda sql, params: error)
    message = failure(await call(server, "query", {"sql": "SELECT nope FROM t"}))
    assert message == "Database error: Unknown column 'nope' in 'field list'"


async def test_list_and_describe() -> None:
    def answer(sql: str, params: Any) -> Any:
        if "FROM information_schema.tables" in sql:
            assert params == ("shop",)
            return ["table_name", "table_type"], [("orders", "BASE TABLE"), ("v", "VIEW")]
        if "FROM information_schema.columns" in sql:
            return ["c"], [
                ("id", "int", "NO", None, "PRI"),
                ("customer_id", "int", "YES", None, "MUL"),
            ]
        if "FROM information_schema.key_column_usage" in sql:
            return ["c"], [
                ("id", None, None, "PRIMARY"),
                ("customer_id", "customers", "id", "orders_ibfk_1"),
            ]
        raise AssertionError(sql)

    server, _ = make(answer)
    assert ok(await call(server, "list_tables")) == [
        {"name": "orders", "type": "table"},
        {"name": "v", "type": "view"},
    ]
    described = ok(await call(server, "describe_table", {"table": "orders"}))
    assert described["database"] == "shop"
    assert described["primary_key"] == ["id"]
    assert described["foreign_keys"] == [
        {"column": "customer_id", "references_table": "customers", "references_column": "id"}
    ]
    assert described["columns"][0] == {
        "name": "id",
        "type": "int",
        "nullable": False,
        "default": None,
    }


async def test_describe_missing_table() -> None:
    server, _ = make(lambda sql, params: (["c"], []))
    assert "no table or view named shop.nope" in failure(
        await call(server, "describe_table", {"table": "nope"})
    )


def test_watchdog_kills_a_statement_past_its_deadline() -> None:
    release = threading.Event()
    killed: list[int] = []

    def answer(sql: str, params: Any) -> Any:
        release.wait(5)
        # A killed SLEEP() returns normally, so the deadline, not an error,
        # must be what marks the result as abandoned.
        return ["SLEEP(30)"], [(1,)]

    def killer(thread_id: int) -> None:
        killed.append(thread_id)
        release.set()

    with pytest.raises(ToolError, match="exceeded the 0.1s time limit"):
        mysql._run(lambda: FakeConnection(answer), killer, "SELECT SLEEP(30)", limit=5, timeout=0.1)
    assert killed == [42]


def test_fast_statements_are_not_killed() -> None:
    killed: list[int] = []
    columns, rows, _ = mysql._run(
        lambda: FakeConnection(lambda sql, params: (["x"], [(1,)])),
        killed.append,
        "SELECT 1",
        limit=5,
        timeout=5,
    )
    assert rows == [[1]]
    assert killed == []
