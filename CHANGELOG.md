# Changelog

All notable changes to `easy-mcp-kit` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/); the public API is not frozen until 1.0.

## [Unreleased]

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

[Unreleased]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Mark007-R/Easy-MCP/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Mark007-R/Easy-MCP/releases/tag/v0.1.0
