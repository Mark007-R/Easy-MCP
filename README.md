# easy_mcp

[![PyPI](https://img.shields.io/pypi/v/easy-mcp-kit)](https://pypi.org/project/easy-mcp-kit/)
[![Python versions](https://img.shields.io/pypi/pyversions/easy-mcp-kit)](https://pypi.org/project/easy-mcp-kit/)
[![CI](https://github.com/Mark007-R/Easy-MCP/actions/workflows/ci.yml/badge.svg)](https://github.com/Mark007-R/Easy-MCP/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/easy-mcp-kit)](https://github.com/Mark007-R/Easy-MCP/blob/main/LICENSE)

**Build secure MCP (Model Context Protocol) servers from plain Python functions.**

![Animated demo: a public live-weather tool and a scope-protected AWS tool on one server — the unauthorized client cannot even see the AWS tool, gets real Mumbai weather from get_weather, is rejected calling list_s3_buckets, and the tool appears once the client presents a key with the aws scope](https://raw.githubusercontent.com/Mark007-R/Easy-MCP/main/docs/demo.gif)

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
`http://127.0.0.1:8000/sse` and the `add` tool is discoverable and callable —
with its JSON schema generated from the type hints and its description taken
from the docstring.

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
| Descriptions | Docstrings | Parses summary + Google-style `Args:` into tool/param descriptions |
| Validation | Nothing | Rejects unknown fields, wrong types, missing params — before your code runs |
| Auth | `auth=APIKeyAuth({...})` | Constant-time key checks, per-tool scopes, hidden protected tools |
| Rate limits | `rate_limit_per_minute=120` | Sliding-window limiter per client |
| Errors | Just `raise` | Clients get a sanitized message + `error_id`; the log gets the traceback |
| Crashes | Nothing | One failing tool never takes down the server |

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
`notifications/cancelled` message.

### Error handling

| Situation | What the client sees |
|---|---|
| Invalid arguments | JSON-RPC `-32602` listing every violation |
| Tool raises `ToolError("msg")` | `isError: true` with your message verbatim |
| Tool raises anything else | `isError: true` with `Tool execution failed (error_id=...)` — no traceback, no exception text |
| Tool exceeds its timeout | `-32005` timeout error |
| Rate limit exceeded | `-32003` with `retry_after_seconds` |
| Session cap reached | `-32006` |

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
npx @modelcontextprotocol/inspector      # connect to http://127.0.0.1:8000/sse

# Claude Code:
claude mcp add --transport sse my-server http://127.0.0.1:8000/sse
```

Or run the raw wire-protocol walkthrough in
[`examples/raw_client.py`](examples/raw_client.py) against
[`examples/demo_server.py`](examples/demo_server.py).

## Real-world examples

| Example | What it shows |
|---|---|
| [`weather_server.py`](examples/weather_server.py) | Live weather for any city via the free Open-Meteo API — stdlib only, no API key needed |
| [`aws_server.py`](examples/aws_server.py) | Query your AWS account (S3 buckets, EC2 instances) with scope-protected tools; loads a local `.env` if present |
| [`demo_server.py`](examples/demo_server.py) | Kitchen-sink tour: auth, scopes, timeouts, session caps, async tools |

For the AWS example, copy [`.env.example`](.env.example) to `.env` and fill in your
own credentials — `.env` is gitignored, so your keys never get committed.

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
│   └── sse.py       HTTP + SSE transport (Starlette/uvicorn)
├── exceptions.py    error hierarchy + stable JSON-RPC error codes
└── logging.py       JSON logs + audit trail
```

The dispatcher (`MCPServer.dispatch`) is transport-independent: it takes one
decoded JSON-RPC message plus a `ClientContext` and returns the response.
Transports only resolve credentials, cap payload sizes, and move bytes —
so adding HTTP/WebSocket/stdio transports (see [ROADMAP.md](ROADMAP.md))
cannot silently bypass a security check.

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
- For multiple workers: `uvicorn "myapp:server.build_app" --factory` won't share
  sessions across processes — v0.1 targets a single process (see ROADMAP).
- Read [SECURITY.md](SECURITY.md) before exposing a server beyond localhost.

## Development

```bash
pip install -e .[dev]
pytest            # 60+ tests: schema, registration, dispatch, security, transport
ruff check .
```

## License

MIT — see [LICENSE](LICENSE).
