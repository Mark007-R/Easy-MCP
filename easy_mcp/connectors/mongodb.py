"""MongoDB connector: collection discovery and read-only queries.

MongoDB has no read-only session to lean on, so the connector itself keeps
clients to reading:

* Only read operations exist as tools: ``find``, ``aggregate``, ``count``
  and the discovery tools.  Nothing inserts, updates or deletes.
* ``aggregate`` admits only reading stages.  ``$out`` and ``$merge``, the
  stages that write, are refused, and so are the diagnostic ones
  (``$currentOp``, ``$changeStream`` ...).  Nested pipelines in ``$facet``,
  ``$lookup`` and ``$unionWith`` are checked the same way, and they may not
  reach into another database.
* Server-side JavaScript (``$where``, ``$function``, ``$accumulator``) is
  refused anywhere in a filter, projection or pipeline.
* Every query (``find``, ``count``, ``aggregate`` and the sampling in
  ``describe_collection``) carries ``maxTimeMS``, and results are row-capped.
  The discovery commands, which MongoDB does not let carry ``maxTimeMS``, are
  bounded by the socket timeout instead.
* The tools see one database, and never its ``system.*`` collections.

Still connect as a user holding only the ``read`` role on that database; the
checks above are the second layer, not the first.

Values travel as MongoDB Extended JSON (relaxed), so an ``ObjectId`` comes
back as ``{"$oid": "..."}`` and can be sent back the same way in a filter.

Credentials: ``MONGODB_URI``; the database is ``--database`` or the one named
in the URI.  The URI is never logged and never appears in an error.

Requires the optional driver: ``pip install "easy-mcp-kit[mongodb]"``.

Launch::

    MONGODB_URI=mongodb://reader:secret@localhost/shop easy-mcp-mongodb --transport stdio
    python -m easy_mcp.connectors.mongodb --database shop --port 8014
"""

from __future__ import annotations

import argparse
import json
import os
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ..exceptions import ToolError
from ..server import MCPServer
from . import _cli

URI_ENV_VAR = "MONGODB_URI"
DEFAULT_STATEMENT_TIMEOUT = 10.0
DEFAULT_MAX_ROWS = 500
HARD_MAX_ROWS = 10_000
CONNECT_TIMEOUT = 10
DEFAULT_SAMPLE_SIZE = 20

# Operators that run JavaScript on the server, refused wherever they appear.
_FORBIDDEN_OPERATORS = frozenset({"$where", "$function", "$accumulator"})

# Aggregation stages that only read.  Anything else -- $out and $merge write,
# $currentOp / $listSessions / $changeStream reach beyond the data -- is
# refused.
_READ_STAGES = frozenset(
    {
        "$addFields",
        "$bucket",
        "$bucketAuto",
        "$count",
        "$densify",
        "$facet",
        "$fill",
        "$geoNear",
        "$graphLookup",
        "$group",
        "$limit",
        "$lookup",
        "$match",
        "$project",
        "$redact",
        "$replaceRoot",
        "$replaceWith",
        "$sample",
        "$set",
        "$setWindowFields",
        "$skip",
        "$sort",
        "$sortByCount",
        "$unionWith",
        "$unset",
        "$unwind",
    }
)


# ------------------------------------------------------------------- checks


def _check_operators(value: Any) -> None:
    """Refuse server-side JavaScript anywhere inside *value*."""
    if isinstance(value, dict):
        for key, inner in value.items():
            if key in _FORBIDDEN_OPERATORS:
                raise ToolError(f"{key} runs JavaScript on the server and is not allowed")
            _check_operators(inner)
    elif isinstance(value, list):
        for inner in value:
            _check_operators(inner)


def _check_collection_name(name: str) -> None:
    if not name or name.startswith("system.") or "$" in name or "\x00" in name:
        raise ToolError(f"invalid collection name: {name!r}")


def check_pipeline(pipeline: Any) -> None:
    """Refuse a pipeline that could write or look beyond the database.

    Raises:
        ToolError: A stage is not a reading one, or runs JavaScript.
    """
    if not isinstance(pipeline, list):
        raise ToolError("a pipeline must be a list of stages")
    for stage in pipeline:
        if not isinstance(stage, dict) or len(stage) != 1:
            raise ToolError("each pipeline stage must be an object with exactly one key")
        ((name, spec),) = stage.items()
        if name not in _READ_STAGES:
            raise ToolError(f"pipeline stage {name} is not allowed; only reading stages are")
        if name == "$facet":
            if not isinstance(spec, dict):
                raise ToolError("$facet takes an object of pipelines")
            for inner in spec.values():
                check_pipeline(inner)
        elif name in ("$lookup", "$graphLookup", "$unionWith"):
            _check_same_database(name, spec)
            if isinstance(spec, dict) and "pipeline" in spec:
                check_pipeline(spec["pipeline"])
    _check_operators(pipeline)


def _check_same_database(stage: str, spec: Any) -> None:
    # {"from": {"db": ..., "coll": ...}} and {"$unionWith": {"db": ...}} name
    # another database; the connector serves exactly one.
    if stage == "$unionWith":
        target = spec.get("coll") if isinstance(spec, dict) else spec
        if isinstance(spec, dict) and "db" in spec:
            raise ToolError("$unionWith may not reach another database")
    else:
        target = spec.get("from") if isinstance(spec, dict) else None
    if not isinstance(target, str):
        raise ToolError(f"{stage} must name a collection of this database")
    _check_collection_name(target)


# ---------------------------------------------------------- Extended JSON


def _from_json(value: Any, what: str) -> Any:
    """Client JSON (Extended JSON allowed) to BSON-ready Python values.

    Raises:
        ToolError: *value* is not valid Extended JSON (``{"$date": "soon"}``).
    """
    from bson import json_util
    from bson.codec_options import DatetimeConversion

    # Dates outside Python's range come back as {"$date": {"$numberLong": ...}}
    # (see _to_json); reading them the same way lets a client send them back.
    options = json_util.DEFAULT_JSON_OPTIONS.with_options(
        datetime_conversion=DatetimeConversion.DATETIME_AUTO
    )
    try:
        return json_util.loads(json.dumps(value), json_options=options)
    except Exception as exc:  # the decoder raises many kinds for bad input
        raise ToolError(f"invalid Extended JSON in {what}: {exc}") from None


def _object_from_json(value: Any, what: str) -> Mapping[str, Any]:
    decoded = _from_json(value, what)
    # {"$date": ...} or {"$code": ...} as the whole value decodes to a scalar.
    if not isinstance(decoded, Mapping):
        raise ToolError(f"{what} must be an object, not {type(decoded).__name__}")
    return decoded


def _to_json(value: Any) -> Any:
    """BSON values to plain JSON, as relaxed Extended JSON."""
    from bson import json_util

    return json.loads(json_util.dumps(value, json_options=json_util.RELAXED_JSON_OPTIONS))


_TYPE_NAMES = {
    "ObjectId": "objectId",
    "str": "string",
    "bool": "bool",
    "int": "int",
    "Int64": "long",
    "float": "double",
    "Decimal128": "decimal",
    "datetime": "date",
    "DatetimeMS": "date",  # a date outside Python's datetime range
    "dict": "object",
    "SON": "object",
    "list": "array",
    "NoneType": "null",
    "bytes": "binData",
    "Binary": "binData",
    "Timestamp": "timestamp",
    "Regex": "regex",
}


def _bson_type(value: Any) -> str:
    name = type(value).__name__
    return _TYPE_NAMES.get(name, name)


# ------------------------------------------------------------------ server


def _without_srv_lookup(uri: str) -> str:
    """*uri* in a form ``parse_uri`` checks without a DNS query.

    A ``mongodb+srv://`` URI is resolved through DNS as it is parsed, which
    would make startup wait on the network and report an outage as a bad URI.
    As a plain ``mongodb://`` URI it has the same syntax, credentials and
    database; the client still resolves the real one on first use.
    """
    parts = urlsplit(uri)
    if parts.scheme != "mongodb+srv":
        return uri
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in ("srvservicename", "srvmaxhosts")
    ]
    return urlunsplit(("mongodb", parts.netloc, parts.path, urlencode(query), parts.fragment))


def _is_driver_error(exc: BaseException) -> bool:
    return type(exc).__module__.split(".", 1)[0] in ("pymongo", "bson")


def _require_driver() -> None:
    try:
        import pymongo  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "the MongoDB connector needs pymongo: pip install 'easy-mcp-kit[mongodb]'"
        ) from None


def build_server(
    *,
    uri: str | None = None,
    database: str | None = None,
    statement_timeout: float = DEFAULT_STATEMENT_TIMEOUT,
    max_rows: int = DEFAULT_MAX_ROWS,
    database_factory: Callable[[], Any] | None = None,
    **server_options: Any,
) -> MCPServer:
    """Build the MongoDB connector server.

    Args:
        uri: Connection string; defaults to the ``MONGODB_URI`` environment
            variable.
        database: The one database the tools read; defaults to the database
            named in the URI.
        statement_timeout: Seconds an operation may run (``maxTimeMS``).
        max_rows: Hard cap on documents returned by ``find``/``aggregate``.
        database_factory: Injectable zero-argument factory returning a
            pymongo-style ``Database`` (tests).  Skips the driver check.
        **server_options: Passed to :class:`~easy_mcp.server.MCPServer`.

    Raises:
        ValueError: No URI or database, or an out-of-range setting.
        RuntimeError: The ``pymongo`` driver is not installed.
    """
    if statement_timeout <= 0:
        raise ValueError("statement_timeout must be positive")
    if not 1 <= max_rows <= HARD_MAX_ROWS:
        raise ValueError(f"max_rows must be between 1 and {HARD_MAX_ROWS}")
    max_time_ms = int(statement_timeout * 1000)

    get_database: Callable[[], Any]
    if database_factory is not None:
        get_database = database_factory
        database_name = database or "test"
    else:
        _require_driver()
        from pymongo.uri_parser import parse_uri

        resolved = uri if uri is not None else os.environ.get(URI_ENV_VAR)
        if not resolved:
            raise ValueError(f"no connection string: set {URI_ENV_VAR}")
        try:
            parsed = parse_uri(_without_srv_lookup(resolved))
        except Exception:
            # The driver's message may quote the URI, password included.
            raise ValueError(f"{URI_ENV_VAR} is not a valid MongoDB connection string") from None
        chosen = database or parsed.get("database")
        if not chosen:
            raise ValueError(f"no database: pass --database or name one in {URI_ENV_VAR}")
        database_name = chosen
        lock = threading.Lock()
        client: list[Any] = []

        def get_database() -> Any:
            # One pooled, thread-safe client, created on first use so that
            # starting the server does not wait on the network.
            with lock:
                if not client:
                    from pymongo import MongoClient

                    client.append(
                        MongoClient(
                            resolved,
                            appname="easy-mcp-mongodb",
                            serverSelectionTimeoutMS=CONNECT_TIMEOUT * 1000,
                            connectTimeoutMS=CONNECT_TIMEOUT * 1000,
                            socketTimeoutMS=max_time_ms + CONNECT_TIMEOUT * 1000,
                            # A date outside Python's range would otherwise fail
                            # the whole result; this keeps it as DatetimeMS.
                            datetime_conversion="DATETIME_AUTO",
                        )
                    )
            return client[0][database_name]

    def run(operation: Callable[[Any], Any]) -> Any:
        try:
            return operation(get_database())
        except ToolError:
            raise
        except OverflowError:
            raise ToolError("a number in the request is too large for BSON (64-bit)") from None
        except Exception as exc:
            if not _is_driver_error(exc):
                raise
            kind = type(exc).__name__
            if kind in ("ExecutionTimeout", "NetworkTimeout") or "exceeded time limit" in str(exc):
                raise ToolError(
                    f"Database error: operation exceeded the {statement_timeout:g}s time limit"
                ) from None
            if kind in ("ServerSelectionTimeoutError", "ConnectionFailure", "AutoReconnect"):
                raise ToolError("Database error: cannot reach the MongoDB server") from None
            details = getattr(exc, "details", None)
            message = details.get("errmsg") if isinstance(details, dict) else None
            raise ToolError(f"Database error: {message or exc}") from None

    def fetch(cursor: Any, limit: int) -> tuple[list[Any], bool]:
        documents = list(cursor)
        return _to_json(documents[:limit]), len(documents) > limit

    def check_limit(limit: int) -> None:
        if not 1 <= limit <= max_rows:
            raise ToolError(f"limit must be between 1 and {max_rows}")

    server_options.setdefault("name", "easy-mcp-mongodb")
    server_options.setdefault(
        "instructions",
        f"Read-only MongoDB access to the {database_name} database. List and "
        "describe collections first, then use find, count or aggregate with "
        'MongoDB Extended JSON ({"$oid": ...} for ids); writing stages and '
        f"server-side JavaScript are refused, each operation may run "
        f"{statement_timeout:g}s and at most {max_rows} documents are returned.",
    )
    server_options.setdefault("default_timeout", statement_timeout + CONNECT_TIMEOUT + 5)
    server = MCPServer(**server_options)

    @server.tool
    def list_collections() -> list[dict[str, Any]]:
        """List the database's collections and views (system collections are omitted)."""

        def operation(db: Any) -> list[dict[str, Any]]:
            found = [
                {"name": info["name"], "type": info.get("type", "collection")}
                for info in db.list_collections()
                if not info["name"].startswith("system.")
            ]
            return sorted(found, key=lambda info: info["name"])

        result: list[dict[str, Any]] = run(operation)
        return result

    @server.tool
    def describe_collection(
        collection: str, sample_size: int = DEFAULT_SAMPLE_SIZE
    ) -> dict[str, Any]:
        """Describe a collection: estimated size, indexes and the field types seen in a sample.

        MongoDB has no fixed schema, so field types come from a random sample
        of documents; a field can show several types.

        Args:
            collection: Collection name.
            sample_size: How many documents to sample (1-100).
        """
        _check_collection_name(collection)
        if not 1 <= sample_size <= 100:
            raise ToolError("sample_size must be between 1 and 100")

        def operation(db: Any) -> dict[str, Any]:
            infos = list(db.list_collections(filter={"name": collection}))
            if not infos:
                raise ToolError(f"no collection named {collection}")
            kind = infos[0].get("type", "collection")
            target = db[collection]
            fields: dict[str, set[str]] = {}
            sample = target.aggregate([{"$sample": {"size": sample_size}}], maxTimeMS=max_time_ms)
            sampled = 0
            for document in sample:
                sampled += 1
                for key, value in document.items():
                    fields.setdefault(key, set()).add(_bson_type(value))
            # A view has no indexes of its own (MongoDB refuses to list them);
            # the collection it reads from does.
            indexes = (
                []
                if kind == "view"
                else [
                    {"name": name, "keys": [[field, order] for field, order in info["key"]]}
                    for name, info in sorted(target.index_information().items())
                ]
            )
            return {
                "collection": collection,
                "type": kind,
                "estimated_count": target.estimated_document_count(maxTimeMS=max_time_ms),
                "sampled": sampled,
                "fields": {key: sorted(kinds) for key, kinds in sorted(fields.items())},
                "indexes": _to_json(indexes),
            }

        result: dict[str, Any] = run(operation)
        return result

    @server.tool
    def find(
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        sort: dict[str, int] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Find documents matching a filter.

        Args:
            collection: Collection name.
            filter: A MongoDB query filter, e.g. {"status": "open", "total": {"$gt": 100}}.
            projection: Fields to include or exclude, e.g. {"name": 1, "_id": 0}.
            sort: Sort order, e.g. {"created": -1}.
            limit: Maximum documents to return.
        """
        _check_collection_name(collection)
        check_limit(limit)
        for part in (filter, projection):
            _check_operators(part)
        if sort is not None and any(direction not in (1, -1) for direction in sort.values()):
            raise ToolError("sort directions must be 1 or -1")

        query = _object_from_json(filter or {}, "filter")
        fields = _object_from_json(projection, "projection") if projection else None

        def operation(db: Any) -> dict[str, Any]:
            cursor = db[collection].find(
                query,
                fields,
                sort=list(sort.items()) if sort else None,
                limit=limit + 1,
                max_time_ms=max_time_ms,
            )
            documents, truncated = fetch(cursor, limit)
            return {"documents": documents, "count": len(documents), "truncated": truncated}

        result: dict[str, Any] = run(operation)
        return result

    @server.tool
    def count(collection: str, filter: dict[str, Any] | None = None) -> int:
        """Count the documents matching a filter.

        Args:
            collection: Collection name.
            filter: A MongoDB query filter; all documents when omitted.
        """
        _check_collection_name(collection)
        _check_operators(filter)
        query = _object_from_json(filter or {}, "filter")
        counted: int = run(lambda db: db[collection].count_documents(query, maxTimeMS=max_time_ms))
        return counted

    @server.tool
    def aggregate(
        collection: str, pipeline: list[dict[str, Any]], limit: int = 100
    ) -> dict[str, Any]:
        """Run a read-only aggregation pipeline.

        Writing stages ($out, $merge), diagnostic stages and server-side
        JavaScript are refused.

        Args:
            collection: Collection name.
            pipeline: Aggregation stages, e.g. [{"$group": {"_id": "$status", "n": {"$sum": 1}}}].
            limit: Maximum documents to return.
        """
        _check_collection_name(collection)
        check_limit(limit)
        check_pipeline(pipeline)

        stages = _from_json(pipeline, "pipeline") + [{"$limit": limit + 1}]

        def operation(db: Any) -> dict[str, Any]:
            cursor = db[collection].aggregate(stages, maxTimeMS=max_time_ms, allowDiskUse=False)
            documents, truncated = fetch(cursor, limit)
            return {"documents": documents, "count": len(documents), "truncated": truncated}

        result: dict[str, Any] = run(operation)
        return result

    return server


# ---------------------------------------------------------------------- CLI


def main(argv: Sequence[str] | None = None) -> None:
    """Command-line entry point (``easy-mcp-mongodb``)."""
    parser = _cli.build_parser(
        f"Serve read-only MongoDB access over MCP. Reads the connection string from ${URI_ENV_VAR}."
    )
    parser.add_argument(
        "--database",
        metavar="NAME",
        help=f"the database to serve (default: the one named in ${URI_ENV_VAR})",
    )
    parser.add_argument(
        "--statement-timeout",
        type=float,
        default=DEFAULT_STATEMENT_TIMEOUT,
        metavar="SECONDS",
        help="per-query time limit (maxTimeMS)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=DEFAULT_MAX_ROWS,
        metavar="N",
        help=f"hard cap on documents a query may return (at most {HARD_MAX_ROWS})",
    )

    def build(args: argparse.Namespace) -> MCPServer:
        return build_server(
            database=args.database,
            statement_timeout=args.statement_timeout,
            max_rows=args.max_rows,
            **_cli.server_kwargs(args),
        )

    _cli.run(build, parser, argv)


if __name__ == "__main__":
    main()
