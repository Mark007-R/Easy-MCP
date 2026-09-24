# Changelog

All notable changes to `easy-mcp-kit` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/); the public API is not frozen until 1.0.

## [Unreleased]

### Added

- The stateless MCP revision `2026-07-28`, served next to the `initialize` era
  on every transport. A request whose `_meta` carries
  `io.modelcontextprotocol/protocolVersion` and `clientCapabilities` is served
  without a handshake or session. Its result carries `resultType: "complete"`
  and the server's identity in `_meta`. `server/discover` reports the supported
  versions, capabilities and instructions, and `tools/list` and
  `server/discover` carry `ttlMs`/`cacheScope` cache hints. The tool list is
  marked `private` when auth is configured, because it depends on the caller.

  A request that names an unsupported version gets
  `UnsupportedProtocolVersionError` (`-32022`) listing the versions to retry
  with. A request missing a required field is `-32602`. `ping` and
  `initialize` do not exist in the stateless era.

  Over Streamable HTTP a stateless request must mirror its version, method and
  tool name into the `MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name`
  headers, each exactly once. A missing, repeated or disagreeing header is
  rejected with `400` and `HeaderMismatch` (`-32020`), since an intermediary
  may act on the headers while the server executes the body. A message
  without an `id` is refused (`400`, `-32600`) unless it is a notification,
  and notifications are not acted on: this revision defines none from client
  to server over HTTP. Unknown methods answer `404`. A client that closes the
  connection cancels its call. `max_calls_per_session` is counted per client
  for these requests, since there is no session to count it on, and the count
  lapses after `session_idle_timeout` like a session would.

  Clients that send `initialize` keep the behaviour of 0.2.5, so older clients
  need no change. The one difference is on every transport: a `tools/call`
  sent as a notification, without an `id`, no longer runs the tool, since
  nobody could receive its answer. Verified against the official Python SDK 2.2
  client in its auto-detecting, pinned and legacy modes, over both HTTP and
  stdio.

- `easy-mcp-sqlite`, a ready-made connector for SQLite database files, with
  `list_tables`, `describe_table` (columns, primary key, foreign keys) and
  `query`. It uses the standard library's `sqlite3`, so it needs no extra and
  no database server. The file is opened read-only, and an authorizer admits
  only reads, which also stops `ATTACH` from opening other files on disk.
  `PRAGMA` is limited to the schema-inspecting ones. A deadline aborts
  statements past `--statement-timeout`, since SQLite has none of its own, and
  `--max-rows` caps results. The path comes from `--database` or
  `SQLITE_PATH`, UNC paths included, and a file that is not a readable
  database is refused at startup. Infinite REALs come back as the strings
  `"Infinity"` / `"-Infinity"`, since JSON has no infinity.

### Changed

- `PROTOCOL_VERSION` and `SUPPORTED_PROTOCOL_VERSIONS[0]` are now
  `"2026-07-28"`. `initialize` still negotiates only handshake-era versions
  and answers a request for `2026-07-28` with `2025-11-25`.

## [0.2.5] - 2026-09-23

### Fixed

- `python -m easy_mcp` and `python -m easy_mcp.cli` now run the command. `cli.py`
  had no `__main__` guard, so `python -m easy_mcp.cli` imported the module and
  exited without serving anything — silently, which is the worst way for a launch
  to fail. The console script is a generated `.exe` on Windows and application
  control sometimes refuses to launch one out of a fresh virtualenv, so `python -m`
  is the invocation that always works; the ready-made connectors already supported
  it.

## [0.2.4] - 2026-09-23

### Added

- Parameter descriptions from `Annotated[T, "description"]`, alongside the
  docstring's `Args:` section. The annotation sits next to the parameter, so it
  cannot quietly stop applying when the parameter is renamed; where both exist,
  the annotation wins. Nested forms such as `list[Annotated[int, "a row id"]]`
  are described too.

- Structured tool results. A tool whose return annotation describes a JSON object
  now publishes an `outputSchema` in `tools/list` and answers with
  `structuredContent` alongside the existing text block, which the spec keeps for
  older clients. `output_schema={...}` declares one by hand and `output_schema={}`
  opts out.

  Only object-shaped returns qualify, because `structuredContent` is a JSON
  object; `-> str` and `-> list[int]` tools are untouched. A missing or
  unsupported return annotation is not an error -- return types were never
  validated before, so tools that have worked since 0.1 keep working, just
  without a schema.

  Results are validated against the schema before they are sent, since the spec
  requires a server to honour the shape it advertised. A tool that breaks its own
  contract fails the call with an `error_id`; the offending data goes to the log,
  not to the client.

- Optional Pydantic v2 support for complex parameters and results, via the new
  `easy-mcp-kit[pydantic]` extra. A parameter annotated with a model advertises
  the model's own JSON Schema, constraints included, and reaches the tool as a
  validated instance; a model return type becomes the `outputSchema`, and the
  tool may return an instance or any dict the model accepts.

  Pydantic validates the inside of a model rather than the built-in validator,
  so a client sees every violation Pydantic finds instead of the first one a
  weaker second copy of its rules would hit. Unknown top-level arguments are
  still refused as before.

  `easy_mcp` never imports Pydantic -- models are recognised by duck typing --
  so projects that do not use it neither pay for the import nor install it.

  Two cases are refused at registration: a model nested inside another type
  (`list[User]`), and two different models sharing a class name in one tool.
  Both would need `$defs` hoisted out of an ambiguous position.

- `easy-mcp run my_tools:server`, a command that imports a module and serves the
  server it defines, so a module of `@server.tool` functions needs no `__main__`
  block to be launchable. The target resolves from the current directory and
  `my_tools.py:server` is accepted too; the attribute defaults to `server` and may
  be a callable returning one. `--transport` picks stdio or HTTP per host, and
  `--host`, `--port` and `--debug` override the server's own constructor arguments
  only when given.

### Fixed

- An `Annotated` parameter's description no longer vanishes from the generated
  schema. `get_type_hints()` strips annotation metadata unless asked not to, so
  the text was silently dropped and clients saw an undocumented parameter.

## [0.2.3] - 2026-09-13

### Added

- Ready-made connectors, each a normal `MCPServer` built with `@server.tool` and
  launchable with one command:
  - **GitHub** (`easy-mcp-github`): repositories, issues, pull requests and file
    contents through a `GITHUB_TOKEN` read from the environment, using only the
    standard library for HTTP. Read-only by default; `--allow-write` adds
    `create_issue` and `comment_on_issue`, gated by the `github:write` scope.
  - **Postgres** (`easy-mcp-postgres`, extra `easy-mcp-kit[postgres]`): schema and
    table discovery plus `query`, every statement in a `READ ONLY` transaction with
    a statement timeout and a row cap; `DATABASE_URL` read from the environment.

### Fixed

- `mypy` passes again (`build_tool` typed the tool name loosely) and now runs in CI,
  so type regressions cannot ship unnoticed.
- The sliding-window rate limiter forgets clients whose history has aged out of the
  window, so a long-running public server no longer grows memory with every distinct
  client it has ever seen.
- Opening a legacy SSE session (`GET /sse`) now spends the client's rate-limit budget
  and answers `429` with `Retry-After` when it is exhausted. Previously an anonymous
  client could fill `max_sessions` and lock everyone else out with `503`s.

### Changed

- API keys are compared as SHA-256 digests, so the constant-time check no longer
  reveals a key's length either.
- Following JSON Schema, a number with no fractional part (`3.0`) is accepted for an
  `int` parameter and reaches the tool as `int`. `validate_arguments` now returns the
  normalized arguments instead of `None`.
- The package version lives in one place (`easy_mcp/_version.py`); `pyproject.toml`
  reads it at build time and `MCPServer` reports it by default.
- `MCPServer.check_rate_limit()` is public so custom transports can charge the
  budget for work that happens before a message exists.
- Transports are typed against `MCPServer` instead of `Any`.
- CI tests Python 3.14 and uses the current `actions/checkout` and
  `actions/setup-python` releases.

## [0.2.2] - 2026-09-10

- Refreshed the demo video and code snapshots in `docs/`.
- Documentation describes MCP clients generically.

## [0.2.1] - 2026-09-10

### Added

- Streamable HTTP transport, the MCP spec's current HTTP transport: one `/mcp`
  endpoint with `MCP-Session-Id` sessions, `DELETE` to end a session, idle-session
  expiry, and an `Origin` allowlist (`allowed_origins`) against DNS rebinding.
  `server.run()` now serves it by default with the legacy `/sse` + `/messages`
  endpoints alongside.
- Protocol version negotiation for `2024-11-05` through `2025-11-25`, verified
  against the official Python and TypeScript SDK clients.

## [0.2.0] - 2026-09-03

### Added

- stdio transport (`server.run("stdio")`, `StdioTransport`) for desktop MCP hosts
  and local agents, with `EASY_MCP_STDIO_API_KEY` for protected tools and
  `sys.stdout` redirected to stderr while serving so stray prints cannot corrupt the
  protocol stream.
- Demo video, code snapshots, and status badges in the README.

## [0.1.0] - 2026-08-30

### Added

- Initial release: `@server.tool` registration with JSON Schema generated from type
  hints and docstrings, strict argument validation, `APIKeyAuth` with per-tool
  scopes and hidden protected tools, sliding-window rate limiting, payload caps,
  per-tool timeouts and per-session call caps, sanitized errors with `error_id`
  correlation, structured JSON logs with an audit trail, and the HTTP + SSE
  transport.

[Unreleased]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.5...HEAD
[0.2.5]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Mark007-R/Easy-MCP/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Mark007-R/Easy-MCP/releases/tag/v0.1.0
