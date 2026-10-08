# easy_mcp

[![PyPI](https://img.shields.io/pypi/v/easy-mcp-kit)](https://pypi.org/project/easy-mcp-kit/)
[![Python versions](https://img.shields.io/pypi/pyversions/easy-mcp-kit)](https://pypi.org/project/easy-mcp-kit/)
[![CI](https://github.com/Mark007-R/Easy-MCP/actions/workflows/ci.yml/badge.svg)](https://github.com/Mark007-R/Easy-MCP/actions/workflows/ci.yml)
[![Plugin Scanner](https://github.com/Mark007-R/Easy-MCP/actions/workflows/plugin-scanner.yml/badge.svg)](https://github.com/Mark007-R/Easy-MCP/actions/workflows/plugin-scanner.yml)
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
| Policy & telemetry | `@server.middleware`, `@server.tool_middleware` | Hooks around every request and tool call, after the built-in checks; refuse with an exception |
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
Over stdio, and in the handshake era over HTTP, `notifications/cancelled`
cancels any request still in flight except `initialize`.

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
    max_sync_workers=32,           # sync tools running at once; None removes the cap
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

A cancelled async tool gets `CancelledError`. A sync tool runs in a thread,
which Python cannot stop from outside, so it gets a cancel token instead: a
cancel, a timeout, a deleted session and stdio shutdown all trigger it. Use it
to stop whatever the tool started, or leave it alone and nothing changes:

```python
from easy_mcp import current_cancel_token

@server.tool
def report(query: str) -> list[dict]:
    token = current_cancel_token()
    connection = open_connection()
    remove = token.on_cancel(connection.cancel) if token else None  # runs off the event loop
    try:
        return run(connection, query)
    finally:
        if remove:
            remove()
```

`token.cancelled` and `token.reason` (`"cancelled"` or `"timeout"`) can be
polled between steps too. Each sync call has a thread of its own, and a tool
that ignores its token keeps that thread until it returns, so at most
`max_sync_workers` sync tools run at once; a call beyond that is refused with
`-32008` at once rather than queued behind them.

The threads are daemons, so the transports give them a bounded time to finish
as they shut down: stdio's `shutdown_timeout`, or 5 s over HTTP. That is when
a cancel callback's `KILL QUERY` gets out. If you drive `server.dispatch`
yourself, `await server.wait_for_tool_threads(5)` before exiting does the
same. `server.run()` closes open legacy SSE streams as shutdown begins,
cancelling the requests they carry (those get no answer), and refuses new
streams and messages with `503`. It gives the Streamable HTTP `/mcp` requests
still running 5 s to finish (a second Ctrl-C cuts that short) and then cancels
them, since uvicorn waits for every connection to close before it shuts the
app down. A `/mcp` request cancelled this way, or sent once shutdown has
begun, is answered `503` with `-32008` and `Retry-After: 1`, so the client can
retry; a `notifications/cancelled` sent meanwhile still cancels its call.
When you serve `server.build_app()` with your own uvicorn, pass
`--timeout-graceful-shutdown`, or shutdown waits for SSE clients to leave and
for running requests (one held in middleware included) to finish.
When you mount `server.build_app()` inside another Starlette or FastAPI app,
its lifespan does not run: call `await server.wait_for_tool_threads(5)` from
the host app's shutdown.

A thread takes its daemon flag from the thread that starts it, so a
`threading.Thread` or `threading.Timer` that a sync tool starts is a daemon
too. It stops, mid-way, when the process exits, and the shutdown wait does
not cover it. Work that must outlive its call needs `daemon=False`, or a
`ThreadPoolExecutor`, which the interpreter waits for at exit.

### Middleware: custom checks, tracing, metrics

Two decorators run your own async code around the server's work.
`@server.middleware` wraps every request; `@server.tool_middleware` wraps a
tool's execution. Each receives what is being served and `call_next`, and
returns what `call_next()` returned:

```python
import asyncio
import time

from easy_mcp import RequestInfo, RequestOutcome, ToolCall, ToolError, ToolOutcome
from easy_mcp.middleware import RequestNext, ToolNext

@server.middleware                       # around every request the server implements
async def timing(request: RequestInfo, call_next: RequestNext) -> RequestOutcome:
    started = time.perf_counter()
    try:
        outcome = await call_next()
    except asyncio.CancelledError:
        record(request.method, "cancelled", time.perf_counter() - started)
        raise                            # always re-raise a cancellation
    record(request.method, outcome.error_type or "ok", time.perf_counter() - started)
    return outcome                       # return exactly what call_next() returned

@server.tool_middleware                  # around one tool's execution
async def tenant_guard(call: ToolCall, call_next: ToolNext) -> ToolOutcome:
    tenant = call.arguments.get("tenant")
    allowed = call.identity is not None and f"tenant:{tenant}" in call.identity.scopes
    if tenant is not None and not allowed:
        raise ToolError(f"This key cannot read tenant {tenant!r}.")  # the tool never runs
    return await call_next()
```

Middleware runs after the built-in checks and cannot skip them: a request has
already passed the `Origin` check, authentication and the rate limit, and a
tool call has already passed visibility, scopes, `max_calls_per_session` and
argument validation. So `call.arguments` are validated, and `call.tool` is a
tool this caller may use. Requests for methods the server does not implement
never reach middleware.

To refuse, raise before `call_next()`. A `ProtocolError` such as
`AuthenticationError` or `RateLimitError` becomes that JSON-RPC error. A
`ToolError` becomes an `isError` result whose message the model can read (on
methods other than `tools/call` there is no result to carry it, so it becomes
`-32603` with your message). The tool does not run and the call does not
count against `max_calls_per_session`. (A call holds its unit of that cap
from the moment it passes the built-in checks, and gets it back if its tool
never starts, so while it waits in tool middleware, concurrent calls beyond
the cap get `-32006`.) Raising after `call_next()` replaces the answer, but
the tool has already run; the audit log records `tool_result_withheld`.

Middleware observes; it does not rewrite. `params`, `meta` and `arguments` are
read-only, and the outcome `call_next()` returns describes what the client
will get (`outcome.error_type`, `outcome.status`, `outcome.message`) without
letting you change it. Timeouts and a busy server arrive as outcomes too, so
`call_next()` raises nothing but a cancellation. Return exactly that object. A
middleware that returns anything else, or raises an unexpected exception,
fails the request with `-32603` and an `error_id`. If it failed before
`call_next()`, the tool did not run; if after, the tool has already run, and
the audit log records `tool_result_withheld`. `call_next()` may be called
once. You may await it in a task of your own (`asyncio.gather`, say), but that
work does not outlive your middleware: if you return or raise before it is
done, it is cancelled, and `call_next()` raises `RuntimeError` once your
middleware has returned.

The first middleware registered is the outermost. Request middleware always
encloses tool middleware. (Some web frameworks do the opposite and make the
last one added the outermost.)

A cancel reaches middleware as `CancelledError` wherever the call is. Re-raise
it: a middleware that swallows one is overruled, and no response is sent. A
`CancelledError` your middleware raises when nothing cancelled the request
(from a shared task someone else cancelled, say) is a failure like any other.
So is one the tool raises when nothing cancelled the call: `call_next()`
returns its `isError` outcome with an `error_id`, as for any tool error. A
tool's `timeout` covers the tool only, so bound your own awaits:
`async with asyncio.timeout(2): ...`. A middleware that waits forever holds
its request until it is cancelled, and in a Streamable HTTP session a closed
connection cancels nothing (SECURITY.md lists what does).

Middleware runs on the event loop, never in a sync tool's thread, so it must
not block: run blocking work with `await asyncio.to_thread(...)`. Context
variables you set before `call_next()` are visible inside the tool, sync tools
included. `current_tool_call()` gives a tool the caller's identity, the
request's `meta` and the `state` dict middleware can fill:

```python
from easy_mcp import current_tool_call

@server.tool
def report(tenant: str) -> dict[str, int]:
    call = current_tool_call()           # None when the function is called directly
    owner = call.identity.fingerprint if call and call.identity else "anonymous"
    ...
```

Notifications and `server/discover` pass through middleware for observation
only: they cannot be refused, so a cancel always works and clients can always
tell which protocol version the server speaks.

`request.meta` carries the request's `_meta`, including the W3C Trace Context
keys (`traceparent`, `tracestate`, `baggage`) clients use to link a call to
their trace. `request.transport.headers` holds the HTTP request's headers
without credentials (`Authorization`, `Proxy-Authorization`, `X-API-Key`,
`Cookie`, `MCP-Session-Id`). A header is only as trustworthy as the proxy
that sets it, and `clientInfo` is whatever the client says it is: never
authorize on it. Arguments, `_meta` and results may hold personal data or
secrets, so review what a middleware sends to logs or a tracing backend.

With request middleware registered, a stateless `tools/list` is marked
`cacheScope: "private"`, since your code may now answer it differently per
caller.

### Error handling

| Situation | What the client sees |
|---|---|
| Invalid arguments | JSON-RPC `-32602` listing every violation |
| Tool raises `ToolError("msg")` | `isError: true` with your message verbatim |
| Tool raises anything else | `isError: true` with `Tool execution failed (error_id=...)` — no traceback, no exception text |
| Tool exceeds its timeout | `-32005` timeout error |
| Rate limit exceeded | `-32003` with `retry_after_seconds` |
| Session cap reached | `-32006` |
| Every sync-tool worker busy (`max_sync_workers`) | `-32008`; retry shortly |
| Stateless request names a version the server does not speak | `-32022` with `supported` and `requested` |
| HTTP headers disagree with the body (stateless) | `-32020`, HTTP `400` |
| Middleware refuses with a `ProtocolError` | its code (e.g. `-32001`, `-32003`); on stateless HTTP, `-32020` to `-32022` get HTTP `400` and `-32601` gets `404` |
| Middleware raises `ToolError` | `isError: true` with your message verbatim (`-32603` with the message outside `tools/call`) |
| Middleware fails or breaks its contract | `-32603` with `error_id`; the tool does not run if it failed before `call_next()` |

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

Cancellation leaves a trail as well: `tool_cancelled` when a client cancels,
`cancel_callback_failed` when a tool's cancel callback raised (the detail is
in the log under its `error_id`), and `tool_finished_after_cancel` when a sync
tool finished after its call was cancelled or timed out. That last one is
worth watching for tools that write.

Middleware adds its own: `request_denied` and `tool_denied` (which names the
`middleware` that refused) when a middleware refuses before the tool runs,
`tool_result_withheld` when it replaces the answer of a tool that did run
(`status: "cancelled"` when the middleware stopped the tool before it
answered, with a timeout of its own, say), `middleware_failed` (with its
`error_id` and `stage`) when one fails or breaks its contract, and
`request_cancelled` when a request other than `tools/call` is cancelled. None
of them carries arguments, results, `_meta` or headers.

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

A database connector stops the statement on the database itself when its
call is cancelled or runs past the server's tool timeout: `KILL QUERY` on
MySQL, a cancel request on Postgres, `interrupt()` on SQLite, `killSessions`
on MongoDB. An abandoned call does not keep a query running until
`--statement-timeout`. On MongoDB the kill goes to the primary, so it does
not reach a read that a `readPreference` sent to a secondary; that read still
ends at `maxTimeMS`. A MongoDB call that the deployment will not give a
session (no session support, or a member that is not readable yet) runs
without one, as in 0.3.0, and a cancel cannot stop it either; the server logs
this once. The connectors set the server's `default_timeout`
longer than the statement timeout, so the database's own limit is what
normally ends a slow statement; one built with a shorter `default_timeout`
logs a warning at startup.

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
No request is sent for a call that has already been cancelled or timed out,
but one already sent cannot be recalled: a write cancelled mid-flight may
still open its issue or post its comment. The audit log records that as
`tool_finished_after_cancel`.

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
├── cancellation.py  CancelToken: a cancel or timeout reaching a sync tool's thread
├── middleware.py    request and tool middleware, current_tool_call()
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
│   ├── _cancel.py   stopping a connector's statement when its call is cancelled
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
WebSocket transport cannot silently bypass one. A custom transport should pass
`transport=TransportInfo(name=..., headers=...)` to `dispatch`, so middleware
knows how a message arrived.

**Determinism:** tool listings are sorted, JSON output uses sorted keys, and
identical inputs produce byte-identical responses — useful for reproducible
agent runs and caching.

**Performance notes:** each sync tool call runs in a worker thread of its own
(at most `max_sync_workers` at once) so it never blocks the event loop; async
tools run natively. Schema validation is a small
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
