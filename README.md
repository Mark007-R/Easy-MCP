# easy_mcp

[![PyPI](https://img.shields.io/pypi/v/easy-mcp-kit)](https://pypi.org/project/easy-mcp-kit/)
[![Python versions](https://img.shields.io/pypi/pyversions/easy-mcp-kit)](https://pypi.org/project/easy-mcp-kit/)
[![CI](https://github.com/Mark007-R/Easy-MCP/actions/workflows/ci.yml/badge.svg)](https://github.com/Mark007-R/Easy-MCP/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/easy-mcp-kit)](https://github.com/Mark007-R/Easy-MCP/blob/main/LICENSE)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/easy-mcp-kit?period=total&units=INTERNATIONAL_SYSTEM&left_color=GREY&right_color=BLUE&left_text=downloads)](https://pepy.tech/projects/easy-mcp-kit)

**Build secure MCP (Model Context Protocol) servers from plain Python functions.**

Watch the [demo video](docs/Demo.mp4) and browse the [server](docs/server.py.png) / [client](docs/client.py.png) code snapshots in [`docs/`](docs/).

`easy_mcp` is FastAPI-for-MCP: declare a function, add a decorator, run a server.
Schema generation, validation, authentication, rate limiting, timeouts,
structured logging, and sanitized error handling are all built in — and secure
by default.

```python
from easy_mcp import MCPServer

server = MCPServer(port=8000)

@server.tool
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

server.run()
```

That's a complete, MCP-compliant server. Connect any MCP client to
`http://127.0.0.1:8000/mcp` and the `add` tool is discoverable and callable —
with its JSON schema generated from the type hints and its description taken
from the docstring. Prefer a local, launch-on-demand server for a desktop MCP
host? Swap the last line for `server.run("stdio")`.

## Installation

```bash
pip install easy-mcp-kit
```

The package installs as `easy-mcp-kit`; the import name is `easy_mcp`.

Requires Python 3.11+. Only two runtime dependencies: `starlette` and `uvicorn`.

## Why easy_mcp?

| Concern | What you write | What easy_mcp does |
|---|---|---|
| Schemas | Type hints | Generates strict JSON Schema (`additionalProperties: false`) |
| Descriptions | Docstrings or `Annotated` | Parses summary + Google-style `Args:` into tool/param descriptions; `Annotated[int, "..."]` documents a parameter in place |
| Validation | Nothing | Rejects unknown fields, wrong types, missing params — before your code runs |
| Structured results | A return type | Publishes `outputSchema` and answers with `structuredContent` |
| Rich schemas | A Pydantic model (optional) | The model's own schema and validation, for parameters and results |
| Auth | `auth=APIKeyAuth({...})` | Constant-time key checks, per-tool scopes, hidden protected tools |
| Rate limits | `rate_limit_per_minute=120` | Sliding-window limiter per client |
| Errors | Just `raise` | Clients get a sanitized message + `error_id`; the log gets the traceback |
| Crashes | Nothing | One failing tool never takes down the server |
| Launching | Nothing | `easy-mcp run my_tools:server` serves a module on any transport |

## Quickstart tour

### Tool registration

```python
# Bare decorator — name, description, and schema are inferred:
@server.tool
def word_count(text: str) -> dict[str, int]:
    """Count words and characters in a text.

    Args:
        text: The text to analyze.
    """
    return {"words": len(text.split()), "characters": len(text)}

# Or document a parameter where it is declared, instead of in the docstring.
# Annotated is plain typing (from typing import Annotated) and the tool still
# receives an int; where both exist, the annotation wins.
@server.tool
def tail_log(lines: Annotated[int, "How many lines to return, newest first"]) -> list[str]:
    """Read the end of the log."""
    ...

# With options:
@server.tool(name="summarize", tags=("stats",), category="math",
             examples=({"arguments": {"values": [1, 2, 3]}},), timeout=5.0)
def summarize_numbers(values: list[float]) -> dict[str, float]:
    """Compute mean/min/max of a list of numbers."""
    ...

# Async tools just work:
@server.tool
async def fetch_status(url: str) -> str:
    """Fetch a status page."""
    ...

# Dynamic registration at runtime:
server.register_tool(my_function, name="late_tool")
server.unregister_tool("late_tool")
```

### Supported parameter types

| Python annotation | JSON Schema |
|---|---|
| `str`, `int`, `float`, `bool` | `string`, `integer`, `number`, `boolean` |
| `list`, `list[T]` | `array` (+ typed `items`) |
| `dict`, `dict[str, T]` | `object` (+ typed `additionalProperties`) |
| `T \| None`, `Optional[T]`, unions | `anyOf` |
| `Literal["a", "b"]` | `enum` |
| defaults (`x: int = 3`) | optional param + advertised `default` |

Anything else is rejected **at registration time** with a clear error — never
at call time. Validation is strict: booleans are not integers, unknown
arguments are hard errors, and every violation is reported (not just the first).
Following JSON Schema, a number with no fractional part (`3.0`) is a valid
integer; it reaches your function as `int`, so `range(times)` never sees a float.

### Structured results

A tool whose return annotation describes a JSON object publishes an
`outputSchema`, and its results carry `structuredContent` so clients get typed
data instead of a string they have to parse:

```python
@server.tool
def weather(city: str) -> dict[str, float]:
    """Get current weather."""
    return {"temperature": 22.5, "humidity": 65}
```

```jsonc
// tools/call result
{
  "content": [{"type": "text", "text": "{\"humidity\": 65, \"temperature\": 22.5}"}],
  "structuredContent": {"humidity": 65, "temperature": 22.5},
  "isError": false
}
```

The text block stays for clients that predate structured content — the spec
asks for both, and they always hold the same data.

MCP carries structured content as a JSON *object*, so only object-shaped
returns get a schema; `-> str` and `-> list[int]` tools are unchanged. A
missing or unsupported return annotation is not an error, it just means no
schema. Pass `output_schema={...}` to declare one yourself, or
`output_schema={}` to advertise none.

Because the schema is a promise to clients, results are checked against it
before they are sent. A tool that breaks its own contract fails the call with
an `error_id` rather than shipping data that does not match.

### Pydantic models (optional)

For a parameter too complex for a plain type hint, annotate it with a Pydantic
v2 model. Install it with `pip install "easy-mcp-kit[pydantic]"`; easy_mcp
never imports Pydantic itself, so projects that skip it pay nothing.

```python
class Address(BaseModel):
    city: str
    zip_code: str | None = None

class User(BaseModel):
    name: str = Field(min_length=1)
    age: int
    home: Address

@server.tool
def save_user(user: User) -> Saved:
    """Save a user.

    Args:
        user: The person to store.
    """
    return Saved(id=7, label=user.name)
```

Clients receive the model's own JSON Schema — constraints like `minLength`
included — and the tool receives a validated `User` instance, not a dict. A
model return type becomes the `outputSchema`, and the tool may return either an
instance or any dict the model accepts.

Pydantic does the validating inside a model rather than the built-in validator,
so you get every error it finds instead of the first one a weaker second copy
of its rules would hit. The outer guarantees are unchanged: unknown top-level
arguments are still a hard error.

Two limits worth knowing. A model must be a whole parameter or return type —
`list[User]` is refused at registration, because a model brings `$defs` and
hoisting those out of an arbitrary nesting depth is ambiguous; wrap it in a
model instead. And two different models that share a class name in one tool are
refused for the same reason: their definitions would collide.

### Transports: Streamable HTTP, SSE, or stdio

The same server object serves every transport; nothing else changes.

```python
server.run()          # HTTP on host:port — Streamable HTTP at /mcp, legacy SSE at /sse
server.run("sse")     # legacy HTTP + SSE only
server.run("stdio")   # stdin/stdout — desktop apps, CLI agents, local MCP hosts
```

**Streamable HTTP** is the MCP spec's current HTTP transport. One endpoint,
`/mcp`, takes one JSON-RPC message per `POST` and answers requests in the
response body. The same app keeps serving the legacy `/sse` + `/messages`
endpoints, so older clients connect unchanged.

**Both protocol eras are served, on every transport, with nothing to configure.**
MCP `2026-07-28` is stateless: there is no handshake and no session. Each
request carries its protocol version and client capabilities in `_meta`, and a
client can ask `server/discover` what the server speaks. Over HTTP the
`MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name` headers must each appear
once and match the body (`400` / `-32020` otherwise), every request needs an
`id`, and closing the connection cancels the call.
Clients that open with `initialize` get the handshake era instead:
`2024-11-05` through `2025-11-25` are negotiated there, and the
`MCP-Session-Id` header the client echoes on later requests identifies the
session. `DELETE /mcp` ends a session, and sessions idle for an hour expire.
The era is chosen per request, so old and new clients can share one server.

```python
from easy_mcp import StreamableHTTPTransport

server.run(StreamableHTTPTransport(
    server,
    path="/mcp",                 # the MCP endpoint
    legacy_sse=False,            # stop serving /sse + /messages
    session_idle_timeout=600.0,  # seconds; None keeps sessions until DELETE
))
```

Browsers get one more check: their `Origin` header must be allowlisted, which
stops DNS-rebinding pages from reaching a server on your machine. Loopback
origins are allowed by default; list any web app that should connect:

```python
server = MCPServer(allowed_origins=["https://app.example.com"])  # "*" allows any
```

Over stdio the MCP host launches your script as a child process and talks
JSON-RPC over its pipes. Logs go to stderr, and `sys.stdout` is redirected to
stderr while serving, so a stray `print()` inside a tool cannot corrupt the
protocol stream. Most desktop MCP hosts take a config entry like this:

```json
{
  "mcpServers": {
    "my-tools": {
      "command": "python",
      "args": ["/path/to/server.py"],
      "env": {"EASY_MCP_STDIO_API_KEY": "optional-key-for-protected-tools"}
    }
  }
}
```

Authentication works the same way as over SSE, except the credential is the
`EASY_MCP_STDIO_API_KEY` environment variable (or
`StdioTransport(server, api_key=...)`) instead of a header. An invalid key
fails at startup rather than silently downgrading to anonymous access.

### Launching from the command line

A module of `@server.tool` functions does not need a `__main__` block to be
runnable:

```bash
easy-mcp run my_tools:server              # Streamable HTTP on the server's own host/port
easy-mcp run my_tools --transport stdio   # attribute defaults to "server"
easy-mcp run my_tools:server --host 0.0.0.0 --port 9000
python -m easy_mcp run my_tools:server    # same thing, without the launcher
```

The target is `module:attribute`, resolved from the current directory;
`my_tools.py:server` works too. The attribute may be a server or a callable
returning one, so a factory that reads configuration at startup is fine.

`--host`, `--port` and `--debug` override the server's own constructor
arguments, and only when given — the transport is the one thing you usually
want to vary per host, since a desktop MCP client wants `stdio` where
everything else wants HTTP.

Importing a module runs it, so point this only at code you trust.

### Authentication and per-tool permissions

```python
from easy_mcp import APIKeyAuth, MCPServer

auth = APIKeyAuth({
    "long-random-admin-key...": "*",          # all scopes
    "long-random-viewer-key..": ["reports"],  # specific scopes
})
# Or keep keys out of code entirely:
# auth = APIKeyAuth.from_env()   # reads EASY_MCP_API_KEYS="key1:*;key2:reports|stats"

server = MCPServer(port=8000, auth=auth)

@server.tool
def public_tool() -> str:
    """Anyone can call this."""

@server.tool(requires_auth=True)
def protected_tool() -> str:
    """Any authenticated client can call this."""

@server.tool(scopes=("admin",))
def admin_tool() -> str:
    """Only keys holding the 'admin' scope can call this."""
```

Clients authenticate with `Authorization: Bearer <key>` or `X-API-Key`.
Protected tools are **invisible** to clients that cannot call them — they are
omitted from `tools/list` and reported as unknown on `tools/call`, so
unauthorized clients cannot even enumerate them.

### Rate limiting, payload caps, timeouts, session limits

```python
server = MCPServer(
    port=8000,
    rate_limit_per_minute=120,     # per client; None disables
    max_request_bytes=1_048_576,   # enforced while reading the body
    default_timeout=30.0,          # per tool call; override per tool
)

@server.tool(timeout=2.0, max_calls_per_session=5)
async def expensive(query: str) -> str:
    """A tool with its own timeout and a per-session usage cap."""
```

Clients can also cancel long-running calls with the standard MCP
`notifications/cancelled` message, or on a stateless HTTP request by closing
the connection. Stateless requests have no session, so `max_calls_per_session`
counts per client there (per API key, or per address for anonymous callers),
and the count lapses after the same idle time that would expire a session.

### Error handling

| Situation | What the client sees |
|---|---|
| Invalid arguments | JSON-RPC `-32602` listing every violation |
| Tool raises `ToolError("msg")` | `isError: true` with your message verbatim |
| Tool raises anything else | `isError: true` with `Tool execution failed (error_id=...)` — no traceback, no exception text |
| Tool exceeds its timeout | `-32005` timeout error |
| Rate limit exceeded | `-32003` with `retry_after_seconds` |
| Session cap reached | `-32006` |
| Stateless request names a version the server does not speak | `-32022` with `supported` and `requested` |
| HTTP headers disagree with the body (stateless) | `-32020`, HTTP `400` |

In `debug=True` mode (development only) clients receive full tracebacks. The
`error_id` in production responses matches the server-side log entry that
contains the real traceback, so you can correlate without leaking internals.

### Structured logging and audit trail

All logs are single-line JSON on stderr. Every tool call is audited with the
tool name, client id, duration, and outcome — never with API keys (only
SHA-256 fingerprints ever appear):

```json
{"timestamp": "2026-07-20T12:00:00.000Z", "level": "INFO", "logger": "easy_mcp.audit",
 "message": "tool_call", "event": {"type": "tool_call", "tool": "add",
 "client_id": "3f9c2a71b04d", "duration_ms": 0.42, "status": "ok"}}
```

## Connecting a client

```bash
# MCP Inspector (interactive UI):
npx @modelcontextprotocol/inspector      # Streamable HTTP, http://127.0.0.1:8000/mcp
```

From code, any MCP client SDK works. With the official Python SDK (v2):

```python
import asyncio

from mcp import Client


async def main() -> None:
    async with Client("http://127.0.0.1:8000/mcp") as client:
        result = await client.call_tool("add", {"a": 2, "b": 3})
        print(result.content[0].text)  # 5


asyncio.run(main())
```

Stdio servers are started by the MCP host itself, from a config entry like the
one in [Transports](#transports-streamable-http-sse-or-stdio).

## Ready-made connectors

Five servers ship with the package. They are built on the same `@server.tool`
decorator you use, so everything above (validation, scopes, rate limits,
timeouts, sanitized errors, audit log) applies to them unchanged.

| Connector | Command | Credential | Tools |
|---|---|---|---|
| GitHub | `easy-mcp-github` | `GITHUB_TOKEN` (optional; public data without it) | `list_repos`, `get_repo`, `list_issues`, `get_issue`, `list_pull_requests`, `get_pull_request`, `get_file`, and with `--allow-write`: `create_issue`, `comment_on_issue` |
| Postgres | `easy-mcp-postgres` | `DATABASE_URL` | `list_schemas`, `list_tables`, `describe_table`, `query` |
| SQLite | `easy-mcp-sqlite` | `--database` or `SQLITE_PATH` (a file path, not a secret) | `list_tables`, `describe_table`, `query` |
| MySQL / MariaDB | `easy-mcp-mysql` | `MYSQL_URL` | `list_databases`, `list_tables`, `describe_table`, `query` |
| MongoDB | `easy-mcp-mongodb` | `MONGODB_URI` (+ `--database`) | `list_collections`, `describe_collection`, `find`, `count`, `aggregate` |

```bash
pip install "easy-mcp-kit[postgres]"   # also [mysql] and [mongodb]; GitHub and SQLite need none

GITHUB_TOKEN=github_pat_... easy-mcp-github --transport stdio
DATABASE_URL=postgresql://user:pass@host/db easy-mcp-postgres --port 8011
easy-mcp-sqlite --database shop.db --transport stdio
MYSQL_URL=mysql://reader:pass@host/shop easy-mcp-mysql --port 8013
MONGODB_URI=mongodb://reader:pass@host/shop easy-mcp-mongodb --port 8014
```

All of them take `--transport {http,sse,stdio}`, `--host`, `--port`, `--rate-limit`
and `--debug`, and load API keys from `EASY_MCP_API_KEYS` when it is set.
`python -m easy_mcp.connectors.<name>` works as well, and
each module's `build_server(...)` returns a normal `MCPServer` for embedding.

**GitHub** is read-only by default. `--allow-write` registers `create_issue`
and `comment_on_issue`, which are gated by the `github:write` scope: only a
client presenting a key that holds it can see or call them, and starting
with `--allow-write` but no keys is refused. The token is sent only to
`GITHUB_API_URL` (default `https://api.github.com`) and never appears in logs
or errors. Use a fine-grained token scoped to the repositories you need.

```bash
export EASY_MCP_API_KEYS="a-long-random-key:github:write"   # key : scope
easy-mcp-github --allow-write --transport stdio             # stdio: EASY_MCP_STDIO_API_KEY=a-long-random-key
```

**Postgres** runs every statement in a `READ ONLY` transaction
(`default_transaction_read_only=on` is set for the session, so a query cannot
turn it off) with a statement timeout (`--statement-timeout`, default 10 s)
and a row cap (`--max-rows`, default 500; `query` returns `truncated: true`
when it hit the cap). Writes are rejected by the database itself, not by
parsing SQL. Still connect with a dedicated role holding only `SELECT`
grants: a read-only transaction does not stop side-effecting functions that
role is allowed to call.

**SQLite** needs no install and no server: point it at an existing database
file. The file is opened read-only (`mode=ro`), and an authorizer allows
only reads. Writes, schema changes, `ATTACH` (which could otherwise open any
other database file on disk), extension loading and every `PRAGMA` except the
schema-inspecting ones are refused before they run. SQLite has no statement
timeout, so the connector aborts a statement that runs past
`--statement-timeout` (default 10 s), and `--max-rows` caps results as for
Postgres. `describe_table` also lists foreign keys, BLOB values come back as
base64, and infinite REALs as the strings `"Infinity"` / `"-Infinity"`. A file
that is not a readable SQLite database is refused at startup.

**MySQL** (and MariaDB) runs each statement on its own connection in a
`READ ONLY` transaction that is always rolled back. A read-only transaction
does not stop everything a privileged account can do: `SET GLOBAL`
reconfigures the server, and `SELECT ... INTO OUTFILE` writes a file on its
disk. So `query` also admits only statements that begin with a reading
keyword (`SELECT`, `WITH`, `SHOW`, `EXPLAIN`, `DESCRIBE`, `TABLE`, `VALUES`),
and refuses `INTO OUTFILE`/`INTO DUMPFILE` and MySQL's executable `/*! */`
comments. The checks read the statement with strings and comments stripped.
MySQL's own `max_execution_time` covers only `SELECT`, so a watchdog sends
`KILL QUERY` once `--statement-timeout` passes. Multi-statement strings and
`LOAD DATA LOCAL` are off in the driver. Connect with a `SELECT`-only
account all the same.

**MongoDB** has no read-only session, so the connector only offers reading
operations: `find`, `count`, `aggregate` and discovery. `aggregate` accepts
only reading stages, checked recursively through `$facet`, `$lookup` and
`$unionWith`, so `$out`, `$merge`, `$currentOp` and `$changeStream` are
refused, and lookups cannot reach another database. Server-side JavaScript
(`$where`, `$function`, `$accumulator`) is refused anywhere in a query. Every
query carries `maxTimeMS` (the discovery commands, which MongoDB gives none,
are bounded by the socket timeout), results are row-capped, and `system.*`
collections are off limits. Values use relaxed Extended JSON, so an id comes
back as `{"$oid": "..."}` and can be sent back the same way, dates outside
Python's range included; malformed Extended JSON is reported as such.
`describe_collection` works on views too (they have no indexes of their own).
A `mongodb+srv://` URI is resolved on first use, not at startup. Connect as a
user with only the `read` role.

## Architecture

```
easy_mcp/
├── server.py        MCPServer: registration, dispatch, execution, lifecycle
├── decorators.py    @tool machinery, ToolDefinition, thread-safe registry
├── schema.py        type hints → JSON Schema; docstring parsing; validation
├── security/
│   ├── auth.py      APIKeyAuth (constant-time), scopes, visibility rules
│   └── ratelimit.py sliding-window per-client rate limiter
├── transport/
│   ├── base.py      Transport ABC + ClientContext
│   ├── _http.py     shared HTTP plumbing: Origin allowlist, credentials, uvicorn
│   ├── streamable_http.py  Streamable HTTP transport (/mcp, sessions)
│   ├── sse.py       legacy HTTP + SSE transport (Starlette/uvicorn)
│   └── stdio.py     stdin/stdout transport (desktop MCP hosts, local agents)
├── connectors/
│   ├── _cli.py      shared --transport/--host/--port options
│   ├── github.py    GitHub connector (stdlib HTTP; read-only unless --allow-write)
│   ├── postgres.py  Postgres connector (psycopg; READ ONLY, timeout, row cap)
│   ├── sqlite.py    SQLite connector (stdlib; read-only open + authorizer, timeout, row cap)
│   ├── mysql.py     MySQL/MariaDB connector (PyMySQL; READ ONLY + read-statement check, KILL QUERY)
│   └── mongodb.py   MongoDB connector (pymongo; read ops only, stage allow-list, maxTimeMS)
├── protocol.py      supported MCP protocol versions + negotiation
├── exceptions.py    error hierarchy + stable JSON-RPC error codes
└── logging.py       JSON logs + audit trail
```

The dispatcher (`MCPServer.dispatch`) is transport-independent: it takes one
decoded JSON-RPC message plus a `ClientContext` and returns the response.
Transports only resolve credentials, cap payload sizes, and move bytes —
so Streamable HTTP, SSE, and stdio all get the same checks, and a future
WebSocket transport cannot silently bypass one.

**Determinism:** tool listings are sorted, JSON output uses sorted keys, and
identical inputs produce byte-identical responses — useful for reproducible
agent runs and caching.

**Performance notes:** sync tools run in a worker thread pool so they never
block the event loop; async tools run natively. Schema validation is a small
hand-written walker (no dependency, ~microseconds for typical payloads). The
per-message overhead is dominated by JSON encode/decode; for large results
prefer returning compact structures over huge strings.

## Production deployment

- Run behind TLS (reverse proxy such as Caddy/nginx) — API keys travel in headers.
- Load keys from the environment (`APIKeyAuth.from_env()`), never hardcode them.
- Keep `debug=False`; it is the only thing standing between clients and tracebacks.
- Browser-based clients on other origins must be listed in `allowed_origins`.
- For multiple workers: stateless (`2026-07-28`) requests can go to any worker.
  Handshake-era sessions are not shared across processes — run one process,
  or route each `MCP-Session-Id` to the same worker. Rate limits and
  `max_calls_per_session` counters are per process either way.
- Read [SECURITY.md](SECURITY.md) before exposing a server beyond localhost.

## Development

```bash
pip install -e .[dev]
pytest            # 100+ tests: schema, dispatch, security, Streamable HTTP, SSE, stdio
ruff check .
mypy easy_mcp
```

CI runs the same three commands on Python 3.11 through 3.14. Releases are
listed in [CHANGELOG.md](CHANGELOG.md).

## License

MIT — see [LICENSE](LICENSE).
