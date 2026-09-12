# Changelog

All notable changes to `easy-mcp-kit` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/); the public API is not frozen until 1.0.

## [Unreleased]

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

[Unreleased]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Mark007-R/Easy-MCP/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Mark007-R/Easy-MCP/releases/tag/v0.1.0
