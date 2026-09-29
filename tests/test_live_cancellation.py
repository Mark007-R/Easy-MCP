"""Cancelling a long query must stop it on the database, not just drop the answer.

These run against real servers and are skipped unless their variable is set:

* ``EASY_MCP_LIVE_MYSQL_URL``     e.g. ``mysql://root:pw@127.0.0.1:3306/test``
* ``EASY_MCP_LIVE_POSTGRES_URL``  e.g. ``postgresql://postgres:pw@127.0.0.1/postgres``
* ``EASY_MCP_LIVE_MONGODB_URI``   e.g. ``mongodb://reader:pw@127.0.0.1:27017/shop``

Connect the connector as the least-privileged account you would deploy with
(``SELECT``-only, ``read`` role): stopping a query must work for it.  The
MongoDB test also needs a scratch collection with data in it, so it seeds one
through ``EASY_MCP_LIVE_MONGODB_SETUP_URI`` (an account that may write and
see every operation; the connector URI is used when it is unset).

Each test starts a query through the dispatcher, cancels it the way a client
does, and then asks the server's own activity view whether it is still there.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable
from typing import Any

import pytest
from conftest import make_context, notification, rpc

from easy_mcp import MCPServer

MYSQL_URL = os.environ.get("EASY_MCP_LIVE_MYSQL_URL")
POSTGRES_URL = os.environ.get("EASY_MCP_LIVE_POSTGRES_URL")
MONGODB_URI = os.environ.get("EASY_MCP_LIVE_MONGODB_URI")
MONGODB_ADMIN_URI = os.environ.get("EASY_MCP_LIVE_MONGODB_SETUP_URI")
MONGODB_SETUP_URI = MONGODB_ADMIN_URI or MONGODB_URI

# Far longer than any test waits: the query only ends early if it is stopped.
STATEMENT_TIMEOUT = 60


async def wait_until(predicate: Callable[[], bool], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.05)
    return True


async def cancel_and_check(
    server: MCPServer,
    tool: str,
    arguments: dict[str, Any],
    running: Callable[[], bool],
) -> None:
    """Start *tool*, see it on the server, cancel it, see it gone within ~1s."""
    context = make_context()
    call = asyncio.create_task(
        server.dispatch(rpc("tools/call", {"name": tool, "arguments": arguments}, 1), context)
    )
    try:
        assert await wait_until(running, 10), "the query never showed up as running"
        await asyncio.sleep(0.5)
        assert running(), "the query ended on its own"
        await server.dispatch(notification("notifications/cancelled", {"requestId": 1}), context)
        assert await asyncio.wait_for(call, 5) is None
        began = time.monotonic()
        assert await wait_until(lambda: not running(), 1.5), "still running after the cancel"
        assert time.monotonic() - began < 1.5
    finally:
        if not call.done():
            call.cancel()


@pytest.mark.skipif(not MYSQL_URL, reason="EASY_MCP_LIVE_MYSQL_URL is not set")
async def test_mysql_cancel_removes_the_query_from_the_processlist() -> None:
    import pymysql

    from easy_mcp.connectors import mysql

    assert MYSQL_URL is not None
    settings = mysql.ConnectionSettings.from_url(MYSQL_URL)
    server = mysql.build_server(
        url=MYSQL_URL, statement_timeout=STATEMENT_TIMEOUT, rate_limit_per_minute=None
    )
    sql = "SELECT SLEEP(30)"
    watcher = pymysql.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        autocommit=True,
    )

    def running() -> bool:
        with watcher.cursor() as cursor:
            # INFO = the statement text exactly, so this query never matches itself.
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO = %s", (sql,)
            )
            (count,) = cursor.fetchone()
        return bool(count)

    try:
        await cancel_and_check(server, "query", {"sql": sql}, running)
    finally:
        watcher.close()


@pytest.mark.skipif(not POSTGRES_URL, reason="EASY_MCP_LIVE_POSTGRES_URL is not set")
async def test_postgres_cancel_removes_the_query_from_pg_stat_activity() -> None:
    import psycopg

    from easy_mcp.connectors import postgres

    server = postgres.build_server(
        dsn=POSTGRES_URL, statement_timeout=STATEMENT_TIMEOUT, rate_limit_per_minute=None
    )
    sql = "SELECT pg_sleep(30)"
    assert POSTGRES_URL is not None
    watcher = psycopg.connect(POSTGRES_URL, autocommit=True)

    def running() -> bool:
        row = watcher.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query = %s", (sql,)
        ).fetchone()
        return bool(row and row[0])

    try:
        await cancel_and_check(server, "query", {"sql": sql}, running)
    finally:
        watcher.close()


@pytest.mark.skipif(not MONGODB_URI, reason="EASY_MCP_LIVE_MONGODB_URI is not set")
async def test_mongodb_cancel_removes_the_operation_from_current_op() -> None:
    from pymongo import MongoClient

    from easy_mcp.connectors import mongodb

    assert MONGODB_URI is not None
    client: Any = MongoClient(MONGODB_SETUP_URI, serverSelectionTimeoutMS=5000)
    database = client[MongoClient(MONGODB_URI, connect=False).get_default_database().name]
    probe = "easy_mcp_cancel_probe"
    database[probe].drop()
    database[probe].insert_many([{"n": n} for n in range(20_000)])
    server = mongodb.build_server(
        uri=MONGODB_URI, statement_timeout=STATEMENT_TIMEOUT, rate_limit_per_minute=None
    )
    # A correlated self-join with no index: every document scans every other
    # one.  (An uncorrelated sub-pipeline would be run once and cached.)
    pipeline = [
        {
            "$lookup": {
                "from": probe,
                "let": {"m": "$n"},
                "pipeline": [{"$match": {"$expr": {"$gte": ["$n", "$$m"]}}}, {"$count": "c"}],
                "as": "all",
            }
        },
        {"$count": "rows"},
    ]

    def running() -> bool:
        ops = client.admin.aggregate(
            [
                # Other users' operations need the admin account's inprog.
                {"$currentOp": {"allUsers": bool(MONGODB_ADMIN_URI)}},
                {"$match": {"command.aggregate": probe, "ns": f"{database.name}.{probe}"}},
            ]
        )
        return any(True for _ in ops)

    try:
        await cancel_and_check(
            server, "aggregate", {"collection": probe, "pipeline": pipeline}, running
        )
        # The killed session did not poison the driver's pool.
        again = await server.dispatch(
            rpc("tools/call", {"name": "count", "arguments": {"collection": probe}}, 2),
            make_context(),
        )
        assert again is not None and again["result"]["isError"] is False, again
    finally:
        database[probe].drop()
        client.close()
