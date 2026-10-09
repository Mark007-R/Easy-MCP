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
Tools, resources and prompts are all plain functions. Schema generation,
validation, authentication, rate limiting, timeouts, structured logging, and
sanitized error handling are all built in — and secure by default.

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
`pip install "easy-mcp-kit[oauth]"` adds PyJWT, to verify OAuth access tokens
locally (see [OAuth 2.1 bearer tokens](#oauth-21-bearer-tokens)).
Several worker processes sharing sessions and limits need Redis:
`pip install "easy-mcp-kit[redis]"` (see
[Running several workers](#running-several-workers)).

## Why easy_mcp?

| Concern | What you write | What easy_mcp does |
|---|---|---|
| Schemas | Type hints | Generates strict JSON Schema (`additionalProperties: false`) |
| Descriptions | Docstrings or `Annotated` | Parses summary + Google-style `Args:` into tool/param descriptions; `Annotated[int, "..."]` documents a parameter in place |
| Validation | Nothing | Rejects unknown fields, wrong types, missing params — before your code runs |
| Structured results | A return type | Publishes `outputSchema` and answers with `structuredContent` |
| Rich schemas | A Pydantic model (optional) | The model's own schema and validation, for parameters and results |
| Resources | `@server.resource("scheme://...")` | Lists, reads, URI templates, MIME types, binary as base64, path-traversal guard, subscriptions |
| Prompts | `@server.prompt` | Argument metadata from the signature, typed arguments, completion from `Literal` |
| Auth | `auth=APIKeyAuth({...})` or `oauth=OAuthResourceServer(...)` | Constant-time key checks, OAuth 2.1 bearer tokens with audience checks, per-tool scopes, hidden protected tools |
| Rate limits | `rate_limit_per_minute=120` | Sliding-window limiter per client |
| Several workers | `store=RedisStore.from_env()` | Sessions, call caps and rate limits shared between processes, no sticky routing |
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

Clients that are connected when a tool is registered or removed are told the
tool list changed, so they can fetch it again. Nothing to configure: bursts
are combined into one notice, and a client only hears about tools it is
allowed to see (see [Change notifications](#change-notifications)).

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

### Resources

A resource is data a client can read by URI: a configuration, a document, a
database schema. Declare it the way you declare a tool; the return value is
the content:

```python
from easy_mcp import ResourceContent, ResourceNotFoundError, safe_path

@server.resource("config://app", mime_type="application/json", cache_ttl=300)
def app_config() -> dict[str, Any]:
    """The application's runtime configuration."""
    return {"region": "eu-west-1", "features": {"beta": True}}

# A URI with {name} or {+name} is a template: its variables are the parameters.
@server.resource("users://{user_id}/avatar", mime_type="image/png", scopes=("users",))
def avatar(user_id: int) -> bytes | None:
    """A user's avatar image."""
    return load_avatar(user_id)          # None: "resource not found"

@server.resource("docs://guides/{+path}", mime_type="text/markdown")
def guide(path: Annotated[str, "Path below the guides folder"]) -> str | None:
    """A guide from the documentation folder."""
    file = safe_path(GUIDES_ROOT, path)  # refuses anything that leaves the folder
    return file.read_text("utf-8") if file.is_file() else None

server.register_resource(load_schema, "db://schema", mime_type="application/sql")
server.unregister_resource("db://schema")
server.resources, server.resource_templates   # what is registered
```

| The function returns | The client reads | Default `mimeType` |
|---|---|---|
| `None` (or raises `ResourceNotFoundError`) | "resource not found" | — |
| `str` | one text item | `text/plain` |
| `bytes` | one `blob` item, base64 | `application/octet-stream` |
| a dict, list, number or Pydantic model | one text item, JSON with sorted keys | `application/json` |
| `ResourceContent(text=...)` or `(blob=...)`, or a list of them | those items (several files for one read) | as given |

`mime_type=` wins over the default, which comes from the return annotation,
never from a file extension. The description comes from the docstring.
`resources/list` lists the concrete resources and `resources/templates/list`
the templates; both are paginated (100 per page) and sorted.

Template variables arrive as strings and are converted to the parameter's
type: `str` (also the type of a parameter without an annotation), `int`,
`float`, `bool` or a `Literal`. A value that does not convert means the URI
does not match. `{name}` matches one path segment, `{+name}` several; every
other RFC 6570 form is refused at registration. Values are percent-decoded,
and one that could walk out of a folder (a `.` or `..` segment, a backslash,
a control character, a `/` in `{name}`, a leading `/` in `{+name}`) never
matches, so your function never sees it. A concrete resource with the exact
URI wins over templates; among templates the one with the most literal
characters wins. `safe_path(root, path)` is the second layer for code that
touches files: it resolves symlinks and refuses anything outside `root` by
raising `ResourceNotFoundError`.

A missing resource is `-32602` for stateless (`2026-07-28`) clients and
`-32002` in the handshake era, as each revision specifies, with the URI in
`data.uri`. `requires_auth` and `scopes` work as for tools: a protected
resource or template is left out of the lists, and reads as missing to a
caller who cannot use it. `cache_ttl` (seconds, default 0) is the `ttlMs` a
stateless client may cache a read for; reads of protected resources are
`cacheScope: "private"`. A resource that reads `current_identity()` to tailor
its content should keep `cache_ttl=0` or require authentication.
`ToolError("message")` raised in a resource is `-32603` with your message;
anything else is `-32603` with an `error_id`.

### Prompts

A prompt is a message template a user picks in the client. Its parameters are
its arguments:

```python
from easy_mcp import Image, Message, ResourceContent, ResourceLink

@server.prompt
def summarize(text: str) -> str:
    """Summarize a passage in three bullet points."""
    return f"Summarize this in three bullet points:\n\n{text}"   # one user message

@server.prompt(title="Review code", scopes=("dev",))
def code_review(
    code: Annotated[str, "The code to review"],
    language: Literal["python", "go", "rust"] = "python",   # completes automatically
    max_issues: int = 5,                                      # "5" on the wire -> 5
) -> list[Message]:
    """Ask for a focused review of a snippet."""
    return [
        Message.user(f"Review this {language} code; list at most {max_issues} issues."),
        Message.user(ResourceContent(uri=f"docs://style/{language}", text=STYLE[language],
                                     mime_type="text/markdown")),
        Message.user(code),
    ]
```

Arguments arrive as strings and are converted like template variables (`str`,
`int`, `float`, `bool`, `Literal`, and `T | None` with a default); every
other type is refused at registration. Bad arguments (not a string, unknown,
missing, not convertible) are `-32602` with every violation listed in
`data.errors`. A prompt returns a string, a `Message`, or a list of strings
and messages; a message's content is text, `Image(data, mime_type)`,
`Audio(data, mime_type)`, an embedded `ResourceContent` (with `uri` and
`mime_type`) or a `ResourceLink(uri, name)`. `register_prompt`,
`unregister_prompt` and `server.prompts` mirror the tool API. Unknown and
hidden prompts answer alike: `-32602 "Unknown prompt"`.

### Completion

Clients ask `completion/complete` for values of a prompt argument or a
template variable as the user types. `Literal` and `bool` parameters complete
from their values with nothing to write; `complete=` adds a list or a
function for the others:

```python
@server.prompt(complete={"table": list_tables})          # fn(value, arguments) -> strings
def explain_table(table: str, schema: str = "public") -> str:
    """Explain what a database table holds."""
    return f"Explain the table {schema}.{table}."

@server.resource("db://{schema}/tables/{table}", complete={"schema": ["public", "audit"]})
def table_info(schema: str, table: str) -> str: ...
```

A list is matched case-insensitively, prefix matches first, then substring
matches. A function gets the partial value and the arguments the client
already filled in (of the same prompt or template only), may be async, and
ranks its own results; a sync one runs on a worker thread. At most 100 values
are returned, with `hasMore` and, when the size is known, `total`; an endless
generator is read no further than that. A completer runs only for callers who
may see its prompt or template, so treat it as data access.

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
session. `DELETE /mcp` ends a session, `GET /mcp` with the session's id opens
its notification stream, and sessions idle for an hour expire.
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

### Change notifications

The server announces tool-list changes on every transport, and advertises
`tools.listChanged: true` in `initialize` and `server/discover`. Prompt and
resource lists are announced the same way (`prompts.listChanged`,
`resources.listChanged`; templates count as resources) once the first prompt
or resource is registered. A capability, once advertised, stays: a kind
emptied later lists empty. Clients that
open with `initialize` get `notifications/tools/list_changed` on their
session's channel once the handshake is answered: stdout over stdio, the
`/sse` stream, or over Streamable HTTP a `GET /mcp` stream carrying the
session's `MCP-Session-Id` (and its credential). A session has one such
stream; a new one replaces the old. A change made while no stream is open is
announced as soon as one opens, and an open stream keeps its session from
expiring.

Stateless (`2026-07-28`) clients ask for what they want with
`subscriptions/listen`. The response is the stream: an acknowledgment first,
naming what the server will send, then the notifications the client opted
into, each tagged with the listen request's id. Over HTTP the answer is a
`text/event-stream`, so the client must accept one, and closing it ends the
subscription; over stdio the client sends `notifications/cancelled`. When the
server shuts down, each stream receives the listen request's result before it
ends (over stdio and legacy SSE followed by `notifications/cancelled`), and a
stream opened with an OAuth token ends the same way when the token expires.
Open the stream before listing tools, so no change falls in between:

```json
{"jsonrpc": "2.0", "id": "listen-1", "method": "subscriptions/listen",
 "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
                      "io.modelcontextprotocol/clientCapabilities": {}},
            "notifications": {"toolsListChanged": true}}}
```

Notifications carry no tool names and respect permissions: a change to a tool
a client cannot see is not announced to it, and neither is a tool added and
removed again. Changes within 0.1 s are combined. A client may hold 8 listen
streams at once in each process (each worker), and `max_sessions` caps them
in each process, even with a shared store (`-32007`, HTTP `503`, beyond
either). Opening a listen or `GET` stream costs one request
of the rate-limit budget; what the server sends on it costs nothing.

Request middleware sees listen requests and may refuse them; `call_next()`
returns once the stream ends. A listen that middleware cuts short after its
acknowledgment (a timeout around `call_next()`, say) is answered with the
middleware's error, over HTTP as the stream's last event. Once a stream has
had its result, or its client cancelled it, an error raised after
`call_next()` is not sent: a request gets one answer.

A custom transport takes part by setting `ClientContext.push` (how the server
sends on the client's channel) and `ClientContext.multiplexed`, and by calling
`server.close_subscriptions(context)` when the channel ends, and again once
the requests it was still running have finished (an `initialize` answered
meanwhile starts the session's notifications anew).

### Resource updates

When a resource changes, tell the clients watching it:

```python
server.notify_resource_updated("config://app")   # any thread; returns how many were told
```

Clients get the URI only, and read the resource again. Handshake-era clients
watch a resource with `resources/subscribe` (and stop with
`resources/unsubscribe`), and the update arrives as
`notifications/resources/updated` on the same channel as list changes:
stdout, the `/sse` stream, or the session's `GET /mcp` stream. With the
default store, an update made while a Streamable HTTP session has no
`GET /mcp` stream open is sent when one opens. Stateless clients put the
URIs in a listen's `resourceSubscriptions`; the acknowledgment lists those
honored (the ones they may read), and each update is tagged with the listen
request's id:

```json
{"jsonrpc": "2.0", "id": "watch", "method": "subscriptions/listen",
 "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
                      "io.modelcontextprotocol/clientCapabilities": {}},
            "notifications": {"resourceSubscriptions": ["config://app"]}}}
```

URIs match exactly: to tell the watchers of a "directory" URI, notify that
URI too. An update still waiting to be written is not queued again, so a burst
of updates reaches a client as one. A session may watch 1000 URIs and a
listen stream name 1000 (`-32007`, HTTP `503`, and `-32602` respectively,
beyond); a URI over 2048 characters cannot be watched. Subscriptions end with
the session (or the listen stream), and a resource re-registered as
protected keeps its watchers: they are told its URI, which they knew, but
cannot read it.

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

### Running several workers

`server.run()` serves from one process. To spread the load over several
processes or machines, give the server a shared store and run its app under
uvicorn's `--workers`, or as several copies behind a load balancer:

```python
# app.py
from easy_mcp import APIKeyAuth, MCPServer, RedisStore

server = MCPServer(
    name="reports",                # also the store's namespace
    auth=APIKeyAuth.from_env(),
    store=RedisStore.from_env(),   # reads EASY_MCP_REDIS_URL: a rediss:// URL with its ACL user
)

@server.tool(max_calls_per_session=5)
def expensive(query: str) -> str:
    """Run the expensive report."""
    ...

app = server.build_app()
```

```bash
pip install "easy-mcp-kit[redis]"
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4 \
    --proxy-headers --forwarded-allow-ips 10.0.0.5 --timeout-graceful-shutdown 10
```

No sticky routing is needed. With the store in place:

- any worker answers any request of a handshake-era session, on Streamable
  HTTP and on legacy SSE; `DELETE` ends a session on every worker, and
  sessions survive a worker restart;
- `notifications/cancelled` reaches a call wherever it runs, and so do the
  cancel token and a connector's `KILL QUERY` behind it;
- a legacy SSE message posted to one worker is answered on the stream another
  worker holds;
- `max_calls_per_session`, rate limits and `max_sessions` count across all
  workers together (`max_sessions` still separately for Streamable HTTP and
  legacy SSE; as the cap on `subscriptions/listen` streams it counts per
  worker).

Some things stay with one worker: a running call, an open stream,
`max_sync_workers`, timeouts, and tools, resources and prompts registered at
runtime (registering one while serving logs a warning). Every worker must
import the same module, with the same tools, resources, prompts, keys and
settings. Change notifications are per worker too: each tells the streams it
holds about its own lists, so a change made at runtime must be made in every
worker. What a session's client was last told is kept in the store, so its
`GET /mcp` stream, on whichever worker it opens, announces exactly the
changes since then. So are the resources a session subscribed to: a
`resources/subscribe` served by one worker reaches the stream another holds
(the store tells it to read them again). `notify_resource_updated` tells the
streams of its own worker only, so call it in every worker, and an update
made while a session has no stream open is lost (its next stream may open on
any worker). Stateless (`2026-07-28`) requests
never had sessions; the store shares their rate limits and per-client call
counts, which with `RedisStore` lapse after `session_idle_timeout` without a
counted (or refused) call, rather than without any request.

The store holds neither API keys nor session ids: sessions are filed under a
digest of their id, and workers authenticate the messages they exchange with
a key derived from it, so access to Redis is not enough to cancel, end or
answer someone else's call. If Redis cannot be reached, requests that need it
are refused with `-32008` (`data.reason: "store_unavailable"`, HTTP `503`
with `Retry-After`) instead of being served without their limits, and
`/healthz` answers `503` with `"store": "unreachable"`, so a load balancer
can take the worker out. A request is refused the same way when Redis
answers but refuses a write it needs (Redis full, read-only or failing to
persist), while `/healthz`, which checks only that Redis answers, stays
`200`. Stateless requests that need no store (a call to a tool without
`max_calls_per_session` when rate limiting is off, say) are still served. A
session's requests need it too. A legacy SSE message posted to the worker
holding its stream is still accepted (`202`), but the answer on the stream
is the same `-32008` unless rate limiting is off and the tool it calls has
no `max_calls_per_session`.

The store needs Redis 7.0 or later: an older Redis reports a write refused
inside a script (Redis full, read-only or failing to persist) as a generic
error, which is answered as an internal error rather than `503`. Give Redis
TLS (`rediss://`), a user limited to the `easy-mcp:` keys and channels (the
ACL is in [SECURITY.md](SECURITY.md); as written it needs Redis 7.2 or
later), and the default `noeviction` memory policy. Servers that share one
Redis need different `name=`s, since the name is the store's namespace, or
an explicit `RedisStore(..., namespace=...)`.
`RedisStore.from_client(client)` takes a `redis.asyncio` client you
configured yourself (a custom TLS context, a Sentinel master); Redis Cluster
is not supported. Client options may follow
in the URL's query string (`?socket_timeout=5`); the defaults are 2 s
timeouts and 64 connections per worker. `--proxy-headers` with
`--forwarded-allow-ips` lets anonymous clients be told apart by their own
address rather than the load balancer's. Mounted inside another app, run
`async with server.lifespan(): ...` from the host app's lifespan, which
connects the store and closes it.

The stdio transport always keeps its state in the process, whatever store is
configured.

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
unauthorized clients cannot even enumerate them. Resources, templates and
prompts take the same `requires_auth` and `scopes`: a protected one is left
out of its list, and reads, gets, completes and subscribes exactly like one
that does not exist.

### OAuth 2.1 bearer tokens

For people rather than scripts, let an OAuth authorization server issue the
credentials. The server becomes an OAuth resource server, as the MCP
authorization spec describes, on Streamable HTTP and SSE, in both protocol
eras:

```python
from easy_mcp import MCPServer, OAuthResourceServer

server = MCPServer(
    host="0.0.0.0",
    oauth=OAuthResourceServer(
        resource="https://mcp.example.com/mcp",          # the public URL of this endpoint
        authorization_servers=["https://auth.example.com"],
        required_scopes=["mcp:access"],                  # every token must carry these
    ),
)

@server.tool(scopes=("files:read", "files:write"))       # narrowest scope first
def read_file(path: str) -> str:
    """Read a file."""
```

`pip install "easy-mcp-kit[oauth]"` adds PyJWT to verify JWT access tokens
against the authorization server's published keys. Servers whose tokens are
opaque pass `introspection=Introspection(client_id, client_secret)` instead
(RFC 7662 token introspection) and need no extra.

What clients see:

- A request without a token gets `401` with
  `WWW-Authenticate: Bearer resource_metadata="..."`, pointing at the
  Protected Resource Metadata (RFC 9728) served at
  `/.well-known/oauth-protected-resource/mcp` (the path of `resource`). MCP
  clients find the authorization server from there and sign the user in.
- Every request, sessions included, must carry `Authorization: Bearer <token>`;
  tokens in query strings or bodies are never read. A token must come from a
  listed authorization server and name this server in its audience (`aud`).
  Anything else, an expired token included, gets `401` with
  `error="invalid_token"`. A session is bound to the signed-in user, not to
  the token, so a refreshed token keeps it.
- A tool's `scopes` are alternatives: list the narrowest first and broader
  scopes after it. A signed-in caller sees every tool. Calling one their token
  does not cover gets `403 insufficient_scope` naming the scope to ask for
  (with `required_scopes` too, if the token also lacks them), and the client
  asks the user for it; the broader token works at once, in the same session.
  Over legacy SSE the call is answered on the stream instead, with a `-32001`
  JSON-RPC error whose `data.error` is `"insufficient_scope"`, and the SSE
  `403` (sent before the body is read) names only `required_scopes`.
  Pass `step_up=False` to keep protected tools invisible to
  tokens that cannot call them, as with API keys. A token's `*` scope is never
  a wildcard. Resources, templates and prompts follow the same rule: a
  signed-in caller sees them all, and a read, a `prompts/get`, a completion or
  a `resources/subscribe` its token does not cover gets the same `403`. A
  listen's acknowledgment leaves such resources out instead.
- Inside a tool, `current_identity()` tells you who is calling: `subject`,
  `client_id`, `issuer`, `scopes`, `claims`. The token itself is never handed
  to your code: use your own credentials for anything the tool calls.

API keys keep working next to tokens (`auth=` and `oauth=` together); a key
holds its own scopes and `required_scopes` do not apply to it. Over stdio
OAuth does not apply: the spec has local servers take credentials from the
environment, so use `EASY_MCP_STDIO_API_KEY` with `auth=`.
`OAuthResourceServer.from_env()` and the ready-made connectors read
`EASY_MCP_OAUTH_RESOURCE`, `EASY_MCP_OAUTH_AUTHORIZATION_SERVERS`, and
optionally `EASY_MCP_OAUTH_AUDIENCE`, `EASY_MCP_OAUTH_REQUIRED_SCOPES`,
`EASY_MCP_OAUTH_JWKS_URI` and `EASY_MCP_OAUTH_INTROSPECTION_CLIENT_ID` /
`_CLIENT_SECRET` / `_ENDPOINT`.

Good to know:

- `server/discover` needs a token too (its `401` is what starts sign-in), but
  its answer is the same for everyone and stays `cacheScope: "public"`. Write
  `instructions` as public text: never put secrets in them.
- Signing keys are fetched at startup and refreshed hourly, in the background
  from 5 minutes before the hour, and when a token names a key the server has
  not seen, at most once every 30 s. Keys an hour old are not used again
  before a refresh has been tried (an idle server's next request waits for
  it), so a withdrawn key stops working within the hour. If the authorization
  server cannot be reached, the keys already fetched stay in use; with none
  fetched yet (or a key set that holds no usable key), token requests get
  `503` and `Retry-After: 5`, and `/healthz` reports `"oauth": "unavailable"`.
  With introspection, once the authorization server fails a discovery or
  introspection request it is not asked again for 5 s: tokens without a
  cached answer get `503` at once meanwhile, and `/healthz` reports
  `"unavailable"` until a request to it succeeds. Any other failure than a
  `401` or `429` (a `3xx` or `4xx`, no answer, a timeout, a `5xx`, or a `2xx`
  that is no JSON object or is over 64 KiB) fails only that token while the
  endpoint still answers a check with a random token. One client address's
  tokens take at most 4 of the 8 introspection requests in flight.
- Token checks are limited per client address with the server's
  `rate_limit_per_minute`: each failed check spends one unit and each check
  still running holds one, so past it presented credentials get `429`
  without being checked, however many arrive at once. A request without any
  token is never throttled, and an outage of the authorization server is
  charged to no one.
- The MCP Python SDK client answers one `401` or `403` per request. A client
  pinned to `2026-07-28` whose very first request calls a tool needing more
  than `required_scopes` signs in on that request's `401` and then gets the
  `403` back as an error; its next call steps up. A client that lists tools
  (or discovers) first, as clients usually do, steps up on the first call.
- Mounted inside another app, `build_app()` runs no lifespan of its own: run
  `async with server.lifespan(): ...` from the host app's, and serve
  `server.oauth.metadata_path` at the root of the host.
- Browser-based clients on other origins need CORS headers, which easy_mcp
  does not send yet; clients behind a proxy (such as the MCP Inspector's) are
  not affected.

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

The rate limit and `max_calls_per_session` count per process by default; with
a shared store they count across every worker (see
[Running several workers](#running-several-workers)). Timeouts, payload caps
and `max_sync_workers` always apply per process. Resources, prompts and
completers run like tools: `default_timeout` (or their own `timeout=`), a
cancel token, and the same `max_sync_workers` for sync functions, so a flood
of slow sync reads can make a sync tool answer `-32008`. Every request,
completion included, spends the rate limit. `max_calls_per_session` is for
tools only. Opening a notification
stream (`GET /mcp`, `GET /sse` or `subscriptions/listen`) costs one request of
the budget, like any message; neither timeouts nor `max_calls_per_session`
apply to it.

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
same. `server.run()` closes open SSE streams (legacy `/sse`, `GET /mcp` and
`subscriptions/listen`) as shutdown begins, sending each listen stream its
result first and cancelling the requests a legacy SSE stream carries (those
get no answer), and refuses new streams and messages with `503`. It gives the
Streamable HTTP `/mcp` requests still running 5 s to finish (a second Ctrl-C
cuts that short) and then cancels
them, since uvicorn waits for every connection to close before it shuts the
app down. A `/mcp` request cancelled this way, or sent once shutdown has
begun, is answered `503` with `-32008` and `Retry-After: 1`, so the client can
retry; a `notifications/cancelled` sent meanwhile still cancels its call.
When you serve `server.build_app()` with your own uvicorn, pass
`--timeout-graceful-shutdown`, or shutdown waits for the clients of `/sse`,
`GET /mcp` and `subscriptions/listen` streams to leave and for running
requests (one held in middleware included) to finish.
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
| Tool exceeds its timeout | `-32005` timeout error (also for a resource, prompt or completer) |
| Resource not found, or hidden from the caller | `-32602` (stateless) / `-32002` (handshake era), with `data.uri` |
| Unknown or hidden prompt, bad prompt arguments, invalid cursor | `-32602` (arguments: every violation in `data.errors`) |
| Resource, prompt or completer raises `ToolError("msg")` | `-32603` with your message verbatim |
| Resource, prompt or completer raises anything else, or returns what cannot be sent | `-32603` with `error_id` |
| A session watches 1000 resources already (`resources/subscribe`) | `-32007`; HTTP `503` |
| Rate limit exceeded | `-32003` with `retry_after_seconds` |
| Session cap reached | `-32006` |
| Too many open `subscriptions/listen` streams | `-32007`; HTTP `503` |
| Every sync-tool worker busy (`max_sync_workers`) | `-32008`; retry shortly |
| Stateless request names a version the server does not speak | `-32022` with `supported` and `requested` |
| HTTP headers disagree with the body (stateless) | `-32020`, HTTP `400` |
| Middleware refuses with a `ProtocolError` | its code (e.g. `-32001`, `-32003`); on stateless HTTP, `-32020` to `-32022` get HTTP `400` and `-32601` gets `404` |
| Middleware raises `ToolError` | `isError: true` with your message verbatim (`-32603` with the message outside `tools/call`) |
| Middleware fails or breaks its contract | `-32603` with `error_id`; the tool does not run if it failed before `call_next()` |
| No token, or an invalid or expired one (OAuth) | HTTP `401`, `-32001`, with `WWW-Authenticate: Bearer ...` |
| Token lacks a required or a tool's scope (OAuth) | HTTP `403`, `-32001` with `data.error = "insufficient_scope"` and the scope to ask for; over legacy SSE a missing tool scope arrives on the stream as that `-32001` error, not a `403` |
| Authorization server unreachable (OAuth) | HTTP `503`, `-32008` with `data.reason = "auth_server_unavailable"`; retry |
| Shared store unreachable | HTTP `503` with `Retry-After`, `-32008` with `data.reason = "store_unavailable"`; retry shortly |

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

Resources and prompts add `resource_read` (the URI, cut to 512 characters,
the `template` it matched, the client, duration and `status`: `ok`,
`not_found`, `denied`, `tool_error`, `error`, `timeout` or `busy`, with an
`error_id` when one was logged, and `hidden: true` when the URI matched an
item the caller may not see) and `prompt_get` (the prompt, the client,
duration and `status`, `denied` for bad arguments). Neither carries
contents or argument values. `resource_subscribe` and `resource_unsubscribe`
name the URI, the client and the session. `request_cancelled` covers reads,
prompts and completions, and `resource_finished_after_cancel`,
`prompt_finished_after_cancel` and `completion_finished_after_cancel` mirror
`tool_finished_after_cancel`. Completions are not audited one by one (they
arrive per keystroke); their failures are logged with an `error_id`, and list
requests are not audited, as `tools/list` is not.

Change notifications add `subscription_open` (the client, the listen
request's id, the list kinds it gets and how many `resources` it watches),
`subscription_close` (with its
`reason`: `client_cancelled`, `disconnected`, `shutdown`, `session_closed`,
`token_expired`, `undeliverable` or `closed`), `subscription_refused`
(`client_limit` or `server_limit`), and `stream_open` / `stream_close` for a
session's `GET /mcp` stream (closed as `client_closed`, `replaced`,
`session_closed`, `shutdown` or `token_expired`). The notifications
themselves are logged at debug level only.

OAuth adds `auth_failed` (why a token was refused, the client address and a
fingerprint of the token), `auth_rate_limited`, `auth_unavailable` (the
authorization server could not be reached) and `principal_seen`, logged once
per signed-in user and process with the token's issuer, subject and client;
every other event names that user by fingerprint only (32 hex digits, so no
client can pick a client id that shares another user's). `tool_denied`
carries the `scope` a step-up asked for. No event ever holds a token or a
secret.

Session events (`session_open`, `session_close`,
`session_credential_mismatch`) carry `session_ref`, a digest of the session
id that also names the session in the store. On Streamable HTTP,
`session_open` also carries the negotiated `protocol_version`; legacy SSE and
stdio sessions are audited as open before their `initialize` arrives, so
theirs carries none. The raw `session_id` is still there, but is dropped from
audit events in 0.4: key log processing on `session_ref`. With a shared
store, session events carry the `worker` that logged them (a session's
close is logged once, by the worker that removes it from the store or, if
the store lost it, by the worker serving it: `reason: "lease_lost"` on
legacy SSE, `"store_lost"` on Streamable HTTP),
`bus_message_rejected` records a message between workers that failed its
authentication (`reason: "mac"`) or names another identity
(`reason: "identity"`), and `sse_relay_failed` an answer that could not reach
the worker holding a legacy SSE stream (`reason: "too_large"`,
`"unserializable"` or `"owner_unreachable"`); the first two still reach the
stream, as a `-32603` error with an `error_id`.

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
and `--debug`, load API keys from `EASY_MCP_API_KEYS` when it is set, and
accept OAuth tokens when `EASY_MCP_OAUTH_RESOURCE` is set (see
[OAuth 2.1 bearer tokens](#oauth-21-bearer-tokens)).
`python -m easy_mcp.connectors.<name>` works as well, and
each module's `build_server(...)` returns a normal `MCPServer` for embedding.

**GitHub** is read-only by default. `--allow-write` registers `create_issue`
and `comment_on_issue`, which are gated by the `github:write` scope. A key
without it neither sees nor calls them. With OAuth every signed-in client
sees them, and a call from a token without `github:write` gets
`403 insufficient_scope` naming it, so the client can ask the user for it
(embedders who want them hidden pass
`oauth=OAuthResourceServer.from_env(step_up=False)` to `build_server`).
Starting with `--allow-write` but neither keys nor OAuth is refused, and so
is `--transport stdio` without keys, since OAuth does not apply over stdio.
The connector always calls GitHub with its own token, never the
client's credential. The token is sent only to
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
├── subscriptions.py change fan-out: list changes, resource updates, subscriptions/listen
├── cancellation.py  CancelToken: a cancel or timeout reaching a sync tool's thread
├── middleware.py    request and tool middleware, current_tool_call()
├── decorators.py    @tool machinery, ToolDefinition, thread-safe registries
├── resources.py     resources and URI templates, their registry, safe_path()
├── prompts.py       prompts and their arguments
├── content.py       ResourceContent, Message, Image, Audio, ResourceLink; rendering
├── completion.py    completion sources: lists, functions, Literal and bool
├── uritemplate.py   the RFC 6570 subset ({name}, {+name}) and the traversal guard
├── pagination.py    stable keyset cursors for the resource and prompt lists
├── schema.py        type hints → JSON Schema; docstring parsing; validation
├── security/
│   ├── auth.py      APIKeyAuth (constant-time), scopes, visibility rules
│   ├── oauth.py     OAuthResourceServer: token checks, resource metadata
│   ├── _fetch.py    outbound HTTP for OAuth (https only, no redirects, size caps)
│   └── ratelimit.py sliding-window per-client rate limiter
├── store/
│   ├── base.py      Store interface (provisional until 1.0)
│   ├── memory.py    MemoryStore: the default, state in this process
│   └── redis_store.py  RedisStore: sessions, counts and limits shared between workers
├── transport/
│   ├── base.py      Transport ABC + ClientContext
│   ├── _http.py     shared HTTP plumbing: Origin allowlist, credentials, uvicorn
│   ├── _sessions.py session records, cross-worker cancel and SSE relay
│   ├── _outbox.py   coalescing outbox and SSE framing of notification streams
│   ├── _bus.py      authenticated messages between workers
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

**Determinism:** tool, resource and prompt listings are sorted, JSON output
uses sorted keys, and identical inputs produce byte-identical responses — useful for reproducible
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
- With OAuth, set `resource` to the exact URL clients use, and behind a proxy
  forward `/.well-known/oauth-protected-resource/...` to the server too.
- Notification streams (`GET /mcp`, `subscriptions/listen`, `/sse`) stay
  open: keep proxy buffering off for them (the server sends
  `X-Accel-Buffering: no`) and proxy idle timeouts above 15 s, the keep-alive
  interval.
- For multiple workers, configure a shared store (see
  [Running several workers](#running-several-workers)). Without one,
  handshake-era sessions, rate limits and call caps are per process, so
  sessions need sticky routing (each `MCP-Session-Id` to the same worker).
  OAuth key and introspection caches, the failed-token throttle and
  `principal_seen` are per process either way.
- Register tools, resources and prompts before serving. One registered later
  still works, but its capability (the first resource or prompt) reaches
  clients that cached `server/discover` only when their copy expires, within
  the hour, and with several workers every worker must make the same change.
  `notify_resource_updated` is per process: run the code that calls it in
  every worker.
- A resource's content is held in memory and base64-encoded when binary, and
  nothing caps its size: serve large data in pieces through a template.
- Read [SECURITY.md](SECURITY.md) before exposing a server beyond localhost.

## Development

```bash
pip install -e .[dev]
pytest            # 1,200+ tests: schema, dispatch, resources, prompts, security, transports
ruff check .
mypy easy_mcp
```

CI runs the same three commands on Python 3.11 through 3.14, with a Redis
service for the shared-store live tests. Those run only when
`EASY_MCP_LIVE_REDIS_URL` names a scratch database, e.g.
`docker run -d -p 6379:6379 redis:7-alpine` and
`EASY_MCP_LIVE_REDIS_URL=redis://127.0.0.1:6379/15 pytest tests/test_live_redis.py`.
Interop tests against the official MCP Python SDK client run when
`EASY_MCP_LIVE_SDK_CLIENT=1` is set and `mcp` 2.3 or later is installed (best
in a virtualenv of its own: `pip install "mcp>=2.3" -e .`).
Releases are listed in [CHANGELOG.md](CHANGELOG.md).

## License

MIT — see [LICENSE](LICENSE).
