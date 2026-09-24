"""MongoDB connector, driven through the dispatcher with a fake database
(no server needed; bson from the pymongo dev dependency handles Extended JSON)."""

from __future__ import annotations

import datetime
import json
from typing import Any

import pytest
from bson import ObjectId
from conftest import make_context, rpc

from easy_mcp import MCPServer
from easy_mcp.connectors import mongodb
from easy_mcp.connectors.mongodb import check_pipeline
from easy_mcp.exceptions import ToolError

OID = ObjectId("650000000000000000000001")
DOCS = [
    {"_id": OID, "name": "Ada", "total": 150, "created": datetime.datetime(2026, 1, 2)},
    {"_id": ObjectId("650000000000000000000002"), "name": "Grace", "total": 20},
    {"_id": ObjectId("650000000000000000000003"), "name": "Linus", "total": "n/a"},
]


class FakeTimeout(Exception):
    pass


FakeTimeout.__name__ = "ExecutionTimeout"
FakeTimeout.__module__ = "pymongo.errors"


class FakeCollection:
    def __init__(self, db: FakeDatabase, name: str) -> None:
        self.db = db
        self.name = name

    def find(self, filter: Any, projection: Any = None, **options: Any) -> list[Any]:
        self.db.calls.append(("find", self.name, filter, projection, options))
        if self.db.fail is not None:
            raise self.db.fail
        return (self.db.docs or DOCS)[: options["limit"]]

    def aggregate(self, pipeline: list[Any], **options: Any) -> list[Any]:
        self.db.calls.append(("aggregate", self.name, pipeline, options))
        if self.db.fail is not None:
            raise self.db.fail
        return list(DOCS)

    def count_documents(self, filter: Any, **options: Any) -> int:
        self.db.calls.append(("count", self.name, filter, options))
        return 3

    def estimated_document_count(self, **options: Any) -> int:
        return 3

    def index_information(self) -> dict[str, Any]:
        if self.name == "big_orders":
            # What MongoDB answers for a view (code 166).
            raise FakeOperationFailure("Namespace shop.big_orders is a view, not a collection")
        return {"_id_": {"key": [("_id", 1)]}, "by_name": {"key": [("name", 1), ("total", -1)]}}


class FakeOperationFailure(Exception):
    pass


FakeOperationFailure.__module__ = "pymongo.errors"


class FakeDatabase:
    def __init__(self) -> None:
        self.calls: list[Any] = []
        self.fail: Exception | None = None
        self.docs: list[Any] | None = None

    def list_collections(self, filter: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        infos = [
            {"name": "orders", "type": "collection"},
            {"name": "big_orders", "type": "view"},
            {"name": "system.views", "type": "collection"},
        ]
        if filter is None:
            return infos
        return [info for info in infos if info["name"] == filter["name"]]

    def __getitem__(self, name: str) -> FakeCollection:
        return FakeCollection(self, name)


def make(**kwargs: Any) -> tuple[MCPServer, FakeDatabase]:
    db = FakeDatabase()
    server = mongodb.build_server(
        database="shop", database_factory=lambda: db, rate_limit_per_minute=None, **kwargs
    )
    return server, db


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


# ----------------------------------------------------------------- checks


@pytest.mark.parametrize(
    "pipeline",
    [
        [{"$match": {"total": {"$gt": 10}}}, {"$group": {"_id": "$name", "n": {"$sum": 1}}}],
        [{"$lookup": {"from": "customers", "localField": "c", "foreignField": "_id", "as": "c"}}],
        [{"$lookup": {"from": "c", "pipeline": [{"$match": {}}], "as": "x"}}],
        [{"$unionWith": "archive"}, {"$sort": {"total": -1}}],
        [{"$unionWith": {"coll": "archive", "pipeline": [{"$project": {"name": 1}}]}}],
        [{"$facet": {"a": [{"$count": "n"}], "b": [{"$limit": 1}]}}],
        [{"$match": {"$expr": {"$gt": ["$total", 100]}}}],
    ],
)
def test_reading_pipelines_are_admitted(pipeline: list[Any]) -> None:
    check_pipeline(pipeline)


@pytest.mark.parametrize(
    ("pipeline", "reason"),
    [
        ([{"$out": "stolen"}], "$out is not allowed"),
        ([{"$merge": {"into": "orders"}}], "$merge is not allowed"),
        ([{"$currentOp": {}}], "$currentOp is not allowed"),
        ([{"$changeStream": {}}], "$changeStream is not allowed"),
        ([{"$listSessions": {}}], "$listSessions is not allowed"),
        ([{"$facet": {"a": [{"$out": "x"}]}}], "$out is not allowed"),
        ([{"$lookup": {"from": "c", "pipeline": [{"$merge": "x"}], "as": "x"}}], "$merge"),
        ([{"$unionWith": {"coll": "c", "pipeline": [{"$out": "x"}]}}], "$out"),
        ([{"$lookup": {"from": {"db": "admin", "coll": "system.users"}, "as": "x"}}], "collection"),
        ([{"$unionWith": {"db": "admin", "coll": "x"}}], "another database"),
        ([{"$lookup": {"from": "system.users", "as": "x"}}], "invalid collection"),
        ([{"$match": {"$where": "sleep(10000)"}}], "JavaScript"),
        ([{"$group": {"_id": 1, "x": {"$accumulator": {}}}}], "JavaScript"),
        (
            [{"$project": {"x": {"$function": {"body": "1", "args": [], "lang": "js"}}}}],
            "JavaScript",
        ),
        ([{"$match": {}, "$out": "x"}], "exactly one key"),
        ("not a list", "list of stages"),
    ],
)
def test_other_pipelines_are_refused(pipeline: Any, reason: str) -> None:
    with pytest.raises(ToolError) as caught:
        check_pipeline(pipeline)
    assert reason in str(caught.value)


# ------------------------------------------------------------------ tools


async def test_list_collections_hides_system_ones() -> None:
    server, _ = make()
    assert ok(await call(server, "list_collections")) == [
        {"name": "big_orders", "type": "view"},
        {"name": "orders", "type": "collection"},
    ]


async def test_describe_collection() -> None:
    server, _ = make()
    described = ok(await call(server, "describe_collection", {"collection": "orders"}))
    assert described["estimated_count"] == 3
    assert described["sampled"] == 3
    assert described["fields"] == {
        "_id": ["objectId"],
        "created": ["date"],
        "name": ["string"],
        "total": ["int", "string"],
    }
    assert described["indexes"] == [
        {"name": "_id_", "keys": [["_id", 1]]},
        {"name": "by_name", "keys": [["name", 1], ["total", -1]]},
    ]
    missing = failure(await call(server, "describe_collection", {"collection": "nope"}))
    assert "no collection named nope" in missing


async def test_find_speaks_extended_json_both_ways() -> None:
    server, db = make(statement_timeout=3)
    result = ok(
        await call(
            server,
            "find",
            {
                "collection": "orders",
                "filter": {"_id": {"$oid": str(OID)}},
                "projection": {"name": 1},
                "sort": {"total": -1},
                "limit": 2,
            },
        )
    )
    _, name, filter, projection, options = db.calls[0]
    assert name == "orders"
    assert filter == {"_id": OID}  # {"$oid": ...} became a real ObjectId
    assert projection == {"name": 1}
    assert options == {"sort": [("total", -1)], "limit": 3, "max_time_ms": 3000}
    assert result["count"] == 2
    assert result["truncated"] is True
    assert result["documents"][0]["_id"] == {"$oid": str(OID)}
    assert result["documents"][0]["created"] == {"$date": "2026-01-02T00:00:00Z"}


async def test_aggregate_appends_the_row_cap_and_time_limit() -> None:
    server, db = make(max_rows=10)
    pipeline = [{"$match": {"total": {"$gt": 10}}}]
    result = ok(
        await call(server, "aggregate", {"collection": "orders", "pipeline": pipeline, "limit": 5})
    )
    _, _, sent, options = db.calls[0]
    assert sent == [{"$match": {"total": {"$gt": 10}}}, {"$limit": 6}]
    assert options == {"maxTimeMS": 10000, "allowDiskUse": False}
    assert result["count"] == 3 and result["truncated"] is False


async def test_writes_never_reach_the_database() -> None:
    server, db = make()
    refused = [
        ("aggregate", {"collection": "orders", "pipeline": [{"$out": "copy"}]}),
        ("aggregate", {"collection": "orders", "pipeline": [{"$merge": {"into": "orders"}}]}),
        ("find", {"collection": "orders", "filter": {"$where": "true"}}),
        ("count", {"collection": "orders", "filter": {"$expr": {"$function": {}}}}),
        ("find", {"collection": "system.users"}),
        ("find", {"collection": "orders", "sort": {"total": 2}}),
        ("find", {"collection": "orders", "limit": 501}),
    ]
    for tool, arguments in refused:
        failure(await call(server, tool, arguments))
    assert db.calls == []


async def test_count() -> None:
    server, db = make()
    assert (
        ok(await call(server, "count", {"collection": "orders", "filter": {"total": {"$gt": 1}}}))
        == 3
    )
    assert db.calls[0][2] == {"total": {"$gt": 1}}


async def test_timeouts_are_reported_as_such() -> None:
    server, db = make(statement_timeout=2)
    db.fail = FakeTimeout("operation exceeded time limit")
    message = failure(await call(server, "aggregate", {"collection": "orders", "pipeline": []}))
    assert message == "Database error: operation exceeded the 2s time limit"


def test_configuration_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(mongodb.URI_ENV_VAR, raising=False)
    with pytest.raises(ValueError, match="MONGODB_URI"):
        mongodb.build_server()
    with pytest.raises(ValueError, match="no database"):
        mongodb.build_server(uri="mongodb://localhost")
    with pytest.raises(ValueError) as caught:
        mongodb.build_server(uri="mongodb://u:s3cret@host:bad/db")
    assert "s3cret" not in str(caught.value)
    with pytest.raises(ValueError, match="max_rows"):
        mongodb.build_server(database_factory=FakeDatabase, max_rows=0)
    # Building never touches the network: the client is created on first use.
    server = mongodb.build_server(uri="mongodb://127.0.0.1:1/shop", rate_limit_per_minute=None)
    assert server.name == "easy-mcp-mongodb"


# ------------------------------------------------------ review regressions


async def test_describe_a_view() -> None:
    server, _ = make()
    described = ok(await call(server, "describe_collection", {"collection": "big_orders"}))
    assert described["type"] == "view"
    assert described["indexes"] == []  # a view's indexes belong to its collection
    assert described["sampled"] == 3


@pytest.mark.parametrize(
    ("tool", "arguments", "message"),
    [
        (
            "find",
            {"filter": {"created": {"$date": "yesterday"}}},
            "invalid Extended JSON in filter",
        ),
        ("find", {"filter": {"n": {"$numberLong": "12x"}}}, "invalid Extended JSON in filter"),
        ("count", {"filter": {"d": {"$uuid": "nope"}}}, "invalid Extended JSON in filter"),
        ("find", {"filter": {"$date": "2026-01-02T00:00:00Z"}}, "filter must be an object"),
        ("find", {"projection": {"$oid": "650000000000000000000001"}}, "projection must be"),
        ("aggregate", {"pipeline": [{"$match": {"d": {"$date": "soon"}}}]}, "in pipeline"),
    ],
)
async def test_bad_extended_json_is_a_tool_error(
    tool: str, arguments: dict[str, Any], message: str
) -> None:
    server, db = make()
    text = failure(await call(server, tool, {"collection": "orders", **arguments}))
    assert message in text
    assert db.calls == []  # refused before reaching the database


async def test_out_of_range_dates_round_trip() -> None:
    from bson.datetime_ms import DatetimeMS

    server, db = make()
    ancient = DatetimeMS(-100_000_000_000_000_000)
    db.docs = [{"_id": 1, "born": ancient}]
    extended = {"$date": {"$numberLong": "-100000000000000000"}}
    result = ok(await call(server, "find", {"collection": "orders", "filter": {"born": extended}}))
    assert db.calls[0][2] == {"born": ancient}  # what came back can be sent back
    assert result["documents"] == [{"_id": 1, "born": extended}]
    assert mongodb._bson_type(ancient) == "date"


async def test_numbers_too_large_for_bson_are_a_tool_error() -> None:
    server, db = make()
    db.fail = OverflowError("MongoDB can only handle up to 8-byte ints")
    assert "too large for BSON" in failure(await call(server, "find", {"collection": "orders"}))


def test_every_lookup_must_name_a_collection() -> None:
    with pytest.raises(ToolError, match="must name a collection"):
        check_pipeline([{"$lookup": {"as": "x", "pipeline": [{"$match": {}}]}}])


def test_srv_uris_are_not_resolved_at_startup() -> None:
    import time

    started = time.monotonic()
    server = mongodb.build_server(
        uri="mongodb+srv://u:pw@cluster0.does-not-exist.invalid/shop?srvMaxHosts=2",
        rate_limit_per_minute=None,
    )
    assert server.name == "easy-mcp-mongodb"
    assert time.monotonic() - started < 2  # no DNS query was made
