"""Ready-made connectors: GitHub and Postgres, driven through the dispatcher
with fake backends (no network, no database, no driver required)."""

from __future__ import annotations

import base64
import email.message
import io
import json
import sys
import urllib.error
from typing import Any

import pytest
from conftest import make_context, rpc

from easy_mcp import APIKeyAuth, MCPServer
from easy_mcp.connectors import github, postgres
from easy_mcp.connectors.github import GitHubClient
from easy_mcp.exceptions import ToolError

WRITE_KEY = "github-writer-key-" + "w" * 14
READ_KEY = "github-reader-key-" + "r" * 14


async def call(server: MCPServer, tool: str, arguments: dict[str, Any] | None = None, **ctx: Any):  # type: ignore[no-untyped-def]
    response = await server.dispatch(
        rpc("tools/call", {"name": tool, "arguments": arguments or {}}), make_context(**ctx)
    )
    assert response is not None
    return response


async def tool_names(server: MCPServer, **ctx: Any) -> list[str]:
    response = await server.dispatch(rpc("tools/list"), make_context(**ctx))
    assert response is not None
    return [tool["name"] for tool in response["result"]["tools"]]


def result_json(response: dict[str, Any]) -> Any:
    assert response["result"]["isError"] is False, response
    return json.loads(response["result"]["content"][0]["text"])


# ---------------------------------------------------------------- GitHub


class FakeGitHub(GitHubClient):
    """Records requests and answers from a canned table."""

    def __init__(self, responses: dict[str, Any], *, authenticated: bool = True) -> None:
        super().__init__("token" if authenticated else None)
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]] = []

    def request(self, method, path, *, params=None, body=None):  # type: ignore[no-untyped-def]
        self.calls.append((method, path, params, body))
        try:
            return self.responses[f"{method} {path}"]
        except KeyError:
            raise ToolError("GitHub returned 404: not found, or the token cannot see it") from None


ISSUE = {
    "number": 7,
    "title": "Bug",
    "state": "open",
    "user": {"login": "alice"},
    "labels": [{"name": "bug"}],
    "assignees": [],
    "comments": 2,
    "created_at": "2026-09-01T00:00:00Z",
    "updated_at": "2026-09-02T00:00:00Z",
    "html_url": "https://github.com/o/r/issues/7",
    "body": "It breaks.",
}
PR_AS_ISSUE = {**ISSUE, "number": 8, "title": "PR", "pull_request": {"url": "..."}}


def github_server(**kwargs: Any) -> tuple[MCPServer, FakeGitHub]:
    client = FakeGitHub(
        {
            "GET /repos/o/r/issues": [ISSUE, PR_AS_ISSUE],
            "GET /repos/o/r/issues/7": ISSUE,
            "GET /repos/o/r/contents/README.md": {
                "path": "README.md",
                "type": "file",
                "size": 5,
                "sha": "abc",
                "encoding": "base64",
                "content": base64.b64encode(b"hello").decode(),
            },
            "GET /repos/o/r/contents": [{"name": "README.md", "type": "file", "size": 5}],
            "POST /repos/o/r/issues": {**ISSUE, "number": 9, "title": "New"},
        },
        authenticated=kwargs.pop("authenticated", True),
    )
    server = github.build_server(client=client, rate_limit_per_minute=None, port=0, **kwargs)
    return server, client


async def test_github_read_tools_and_pr_filtering() -> None:
    server, client = github_server()
    assert await tool_names(server) == [
        "get_file",
        "get_issue",
        "get_pull_request",
        "get_repo",
        "list_issues",
        "list_pull_requests",
        "list_repos",
    ]
    issues = result_json(await call(server, "list_issues", {"repo": "o/r", "labels": "bug"}))
    assert [issue["number"] for issue in issues] == [7]  # the PR is filtered out
    assert issues[0]["labels"] == ["bug"] and "body" not in issues[0]
    assert client.calls[-1][2] == {
        "state": "open",
        "labels": "bug",
        "per_page": 30,
        "sort": "updated",
    }

    issue = result_json(await call(server, "get_issue", {"repo": "o/r", "number": 7}))
    assert issue["body"] == "It breaks."


async def test_github_get_file_decodes_content_and_lists_directories() -> None:
    server, _ = github_server()
    file = result_json(await call(server, "get_file", {"repo": "o/r", "path": "README.md"}))
    assert file == {
        "path": "README.md",
        "type": "file",
        "size": 5,
        "sha": "abc",
        "truncated": False,
        "content": "hello",
    }
    root = result_json(await call(server, "get_file", {"repo": "o/r", "path": "/"}))
    assert root["type"] == "dir" and root["entries"][0]["name"] == "README.md"


async def test_github_input_errors_are_tool_errors() -> None:
    server, _ = github_server()
    bad_repo = await call(server, "get_issue", {"repo": "not-a-repo", "number": 1})
    assert bad_repo["result"]["isError"] is True
    assert "owner/name" in bad_repo["result"]["content"][0]["text"]

    bad_limit = await call(server, "list_issues", {"repo": "o/r", "limit": 500})
    assert "limit must be between" in bad_limit["result"]["content"][0]["text"]

    missing = await call(server, "get_issue", {"repo": "o/r", "number": 404})
    assert "404" in missing["result"]["content"][0]["text"]


async def test_github_anonymous_needs_owner_for_list_repos() -> None:
    server, _ = github_server(authenticated=False)
    response = await call(server, "list_repos")
    assert "owner is required" in response["result"]["content"][0]["text"]


def test_github_write_requires_auth() -> None:
    with pytest.raises(ValueError, match="github:write"):
        github.build_server(client=FakeGitHub({}), enable_write=True)


async def test_github_write_tools_are_scope_gated() -> None:
    auth = APIKeyAuth({WRITE_KEY: [github.WRITE_SCOPE], READ_KEY: ["other"]})
    server, client = github_server(enable_write=True, auth=auth)

    assert "create_issue" not in await tool_names(server)  # anonymous
    reader = auth.authenticate(READ_KEY)
    assert "create_issue" not in await tool_names(server, identity=reader)
    writer = auth.authenticate(WRITE_KEY)
    names = await tool_names(server, identity=writer)
    assert {"create_issue", "comment_on_issue"} <= set(names)

    denied = await call(server, "create_issue", {"repo": "o/r", "title": "New"}, identity=reader)
    assert "error" in denied  # reported as unknown: not enumerable

    created = result_json(
        await call(server, "create_issue", {"repo": "o/r", "title": "New"}, identity=writer)
    )
    assert created["number"] == 9
    assert client.calls[-1] == ("POST", "/repos/o/r/issues", None, {"title": "New", "body": ""})


def test_github_http_error_messages_never_include_the_token() -> None:
    def http_error(code: int, body: dict[str, Any], **headers: str) -> urllib.error.HTTPError:
        message = email.message.Message()
        for key, value in headers.items():
            message[key.replace("_", "-")] = value
        return urllib.error.HTTPError(
            "https://api.github.com/x",
            code,
            "reason",
            message,
            io.BytesIO(json.dumps(body).encode()),
        )

    limited = github._describe_http_error(
        http_error(403, {"message": "API rate limit exceeded"}, X_RateLimit_Remaining="0")
    )
    assert limited.startswith("GitHub rate limit exceeded")
    assert github._describe_http_error(http_error(401, {"message": "Bad credentials"})) == (
        "GitHub rejected the credentials (check GITHUB_TOKEN)"
    )
    other = github._describe_http_error(http_error(422, {"message": "Validation Failed"}))
    assert other == "GitHub API error 422: Validation Failed"


def test_github_cli_help_and_flags() -> None:
    with pytest.raises(SystemExit) as excinfo:
        github.main(["--help"])
    assert excinfo.value.code == 0


# -------------------------------------------------------------- Postgres


class FakeDriverError(Exception):
    __module__ = "psycopg.errors"


class FakeCursor:
    def __init__(self, table: dict[str, tuple[list[str], list[tuple[Any, ...]]]]) -> None:
        self.table = table
        self.description: list[Any] | None = None
        self._rows: list[tuple[Any, ...]] = []
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        for needle, (columns, rows) in self.table.items():
            if needle in sql:
                self.description = [type("Col", (), {"name": name})() for name in columns]
                # Only the "users" table exists in this fake database.
                self._rows = [] if params and "nope" in params else list(rows)
                return
        raise FakeDriverError('relation "nope" does not exist')

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.opened = 0

    def cursor(self) -> FakeCursor:
        return self._cursor

    def __enter__(self) -> FakeConnection:
        self.opened += 1
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def postgres_server(**kwargs: Any) -> tuple[MCPServer, FakeCursor]:
    cursor = FakeCursor(
        {
            "information_schema.schemata": (["schema_name"], [("app",), ("public",)]),
            "information_schema.tables": (["table_name", "table_type"], [("users", "BASE TABLE")]),
            "information_schema.columns": (
                ["column_name", "data_type", "is_nullable", "column_default"],
                [("id", "integer", "NO", "nextval(...)"), ("email", "text", "YES", None)],
            ),
            "PRIMARY KEY": (["column_name"], [("id",)]),
            "SELECT id FROM users": (["id"], [(1,), (2,), (3,)]),
        }
    )
    connection = FakeConnection(cursor)
    server = postgres.build_server(
        connector=lambda: connection, rate_limit_per_minute=None, port=0, **kwargs
    )
    return server, cursor


async def test_postgres_discovery_tools() -> None:
    server, cursor = postgres_server()
    assert await tool_names(server) == ["describe_table", "list_schemas", "list_tables", "query"]

    assert result_json(await call(server, "list_schemas")) == ["app", "public"]
    assert result_json(await call(server, "list_tables", {"schema": "app"})) == [
        {"name": "users", "type": "BASE TABLE"}
    ]
    assert cursor.executed[-1][1] == ("app",)  # parameters, never interpolated

    described = result_json(await call(server, "describe_table", {"table": "users"}))
    assert described["primary_key"] == ["id"]
    assert described["columns"][1] == {
        "name": "email",
        "type": "text",
        "nullable": True,
        "default": None,
    }


async def test_postgres_query_caps_rows_and_reports_truncation() -> None:
    server, _ = postgres_server(max_rows=2)
    result = result_json(await call(server, "query", {"sql": "SELECT id FROM users", "limit": 2}))
    assert result == {"columns": ["id"], "rows": [[1], [2]], "row_count": 2, "truncated": True}

    over = await call(server, "query", {"sql": "SELECT id FROM users", "limit": 3})
    assert "limit must be between 1 and 2" in over["result"]["content"][0]["text"]
    empty = await call(server, "query", {"sql": "   "})
    assert "sql must not be empty" in empty["result"]["content"][0]["text"]


async def test_postgres_driver_errors_become_tool_errors() -> None:
    server, _ = postgres_server()
    response = await call(server, "query", {"sql": "SELECT * FROM nope"})
    assert response["result"]["isError"] is True
    assert response["result"]["content"][0]["text"] == (
        'Database error: relation "nope" does not exist'
    )
    unknown = await call(server, "describe_table", {"table": "nope"})
    assert "no table or view named public.nope" in unknown["result"]["content"][0]["text"]


def test_postgres_build_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="max_rows"):
        postgres.build_server(connector=lambda: None, max_rows=0)
    with pytest.raises(ValueError, match="statement_timeout"):
        postgres.build_server(connector=lambda: None, statement_timeout=0)

    monkeypatch.setitem(sys.modules, "psycopg", None)  # simulate a missing driver
    with pytest.raises(RuntimeError, match=r"easy-mcp-kit\[postgres\]"):
        postgres.build_server(dsn="postgresql://x")

    monkeypatch.delitem(sys.modules, "psycopg")
    monkeypatch.setattr(postgres, "_require_driver", lambda: None)
    monkeypatch.delenv(postgres.DSN_ENV_VAR, raising=False)
    with pytest.raises(ValueError, match="DATABASE_URL"):
        postgres.build_server()


def test_postgres_cli_reports_configuration_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(postgres.DSN_ENV_VAR, raising=False)
    with pytest.raises(SystemExit) as excinfo:
        postgres.main(["--transport", "stdio"])
    assert excinfo.value.code == 2  # argparse usage error, not a traceback
