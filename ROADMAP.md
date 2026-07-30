# Roadmap

`v0.1.0` is intentionally small but complete: one solid transport, strict
validation, real security defaults. Each release below stays backwards
compatible until `v1.0` freezes the public API.

## v0.2 — Transports & richer schemas

- **stdio transport** (Claude Desktop and most local MCP clients).
- **Streamable HTTP transport** (the current MCP spec's successor to SSE).
- `Annotated[int, "description"]` parameter descriptions in addition to docstrings.
- Optional Pydantic model support for complex tool parameters and outputs.
- Structured output schemas (`outputSchema` / `structuredContent`) for typed results.
- `easy-mcp run examples.demo_server:server` CLI for zero-code launching.

## v0.3 — Protocol surface & operations

- MCP **resources** and **prompts** (not just tools).
- `listChanged` notifications when tools are registered/unregistered at runtime.
- Middleware hooks (before/after tool call) for custom auth, tracing, metrics.
- OpenTelemetry spans + Prometheus-style metrics endpoint.
- Shared session store (Redis) for multi-worker deployments.
- OAuth 2.1 / bearer-token verification per the MCP authorization spec.

## v1.0 — Stability & hardening

- Frozen public API with semantic versioning guarantees.
- WebSocket transport.
- Per-key quotas and cost accounting (beyond per-minute rate limits).
- Built-in subprocess sandbox runner for semi-trusted tools.
- Property-based fuzzing of the validator and dispatcher in CI.
- Third-party security review of auth, session, and transport code.
- Strict `mypy --strict` gate and 100% branch coverage on security paths.
