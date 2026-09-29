# Security Policy

`easy_mcp` is designed to be **secure by default**: the out-of-the-box
configuration binds to loopback, rate limits every client, caps payload
sizes, times out tool execution, and never leaks tracebacks. This document
describes the threat model, what the library does and does not protect
against, and how to deploy it safely.

## Trust model — read this first

**Tool functions are trusted code.** `easy_mcp` executes only the Python
functions *you* register — never code supplied by a client. There is no
`eval`, no dynamic import, no deserialization of executable content. The
security boundary is between untrusted *clients* (and the LLMs driving them)
and your server process.

Consequences:

- A malicious client cannot make the server run anything except registered
  tools, with schema-validated arguments.
- A *tool you wrote* can still do anything your process can do. If a tool
  shells out, reads files, or builds SQL from its arguments, those arguments
  are attacker-influenced input — sanitize them inside the tool.
- MCP clients are often driven by LLMs subject to prompt injection. Design
  tools so that even a confused client cannot cause irreversible damage
  (least privilege, scoped keys, idempotent operations, usage caps).

## Protections built in

| Threat | Mitigation |
|---|---|
| Credential stuffing / key probing | Constant-time comparison of SHA-256 digests over the full key set (`hmac.compare_digest`); timing reveals neither partial matches nor key length |
| Key leakage via logs | Raw keys never logged; only SHA-256 fingerprints appear in logs and audit events |
| Unauthorized tool use | Per-tool `requires_auth` and scope checks; protected tools are omitted from `tools/list` and report as unknown to unauthorized callers (no enumeration) |
| Session hijacking | Session ids are 192-bit random capability tokens; every request on a session (SSE POST, Streamable HTTP POST/DELETE) must present the same credential the session was opened with (403 otherwise) |
| Header/body disagreement (stateless HTTP) | A proxy may route or rate-limit on the mirrored `MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name` headers while the server executes the body, so the server rejects any request whose headers are missing, repeated or disagree with its body (`400`, `-32020`); Base64-encoded names are decoded before comparing. A message without an `id` never runs a method (no header checks apply to it), and the client notifications this revision leaves undefined over HTTP, `notifications/cancelled` included, are ignored, so one caller cannot cancel another's call |
| DNS rebinding / cross-site requests | Browser `Origin` headers on the HTTP transports must match `allowed_origins` (loopback origins by default) or get 403 before any route runs; Streamable HTTP also requires `Content-Type: application/json` |
| Malformed / hostile input | Strict schema validation: unknown fields rejected, types enforced (bool ≠ int), required params enforced, before any tool code runs |
| Oversized payloads | `max_request_bytes` enforced on the Content-Length header *and* while streaming the body (a lying header does not help) |
| Request flooding | Per-client sliding-window rate limiting on every method, including discovery and opening an SSE session; idle clients are dropped from the limiter so its memory stays bounded; concurrent session cap (`max_sessions`) |
| Session exhaustion (Streamable HTTP) | Sessions idle past `session_idle_timeout` (default 1 h) expire; `max_sessions` caps live sessions per endpoint (503 beyond it); `DELETE` ends a session early and cancels its running calls |
| Resource exhaustion via slow tools | Per-tool and server-default timeouts; sync tools run off the event loop so they cannot stall other clients |
| Information disclosure | Production errors are opaque (`error_id` only); tracebacks stay in server logs; `debug=True` is loudly warned about at startup |
| Accidental exposure | Default bind is `127.0.0.1`; binding non-loopback without auth logs a warning at startup |
| Crash amplification | Exceptions in one tool call are contained; the server keeps serving |
| Protocol-stream corruption (stdio) | `sys.stdout` is redirected to stderr while serving, so tool `print()` calls cannot inject bytes into the JSON-RPC stream; oversized input lines are discarded unbuffered |
| Silent auth downgrade (stdio) | An invalid `EASY_MCP_STDIO_API_KEY` aborts startup instead of falling back to anonymous access |

## Transport trust boundaries

- **Streamable HTTP** and legacy **SSE** — clients are remote and untrusted;
  credentials arrive in headers, sessions are capability tokens bound to the
  credential that opened them, browsers are held to the `Origin` allowlist,
  and every protection above applies.
- **stdio** — the client is the *parent process* that launched the server
  (a desktop app, a CLI agent, an agent runtime). There is no network
  surface, but the parent is still treated as an MCP client: schema
  validation, scopes, rate limits, timeouts, payload caps, and error
  sanitization all apply unchanged. Protected tools stay hidden unless the
  parent presents a valid key via `EASY_MCP_STDIO_API_KEY`. Anything the
  parent can pass as environment it can also read, so a stdio key is a
  scoping mechanism, not a secret from the host itself.

## Ready-made connectors

The connectors are ordinary tool functions and follow the trust model above:
the client is untrusted, the credential in the environment is trusted.

- **GitHub** — give it a fine-grained token limited to the repositories and
  permissions the tools need (contents, issues, pull requests: read). Write
  tools exist only with `--allow-write` and are hidden from every client whose
  API key lacks the `github:write` scope; starting with `--allow-write` and no
  keys is refused. The token is sent only to `GITHUB_API_URL` and never logged.
- **Postgres** — statements run in `READ ONLY` transactions with
  `default_transaction_read_only=on` at session level, a statement timeout and
  a row cap, so `INSERT`/`UPDATE`/`DDL` fail at the database. That does not
  prevent calling side-effecting functions the role may execute, so connect
  with a role that holds only `SELECT` on the schemas you want exposed.
  Error messages from the database are forwarded (they are what a client
  needs to fix its query); the connection string never is.
- **SQLite** — the file is opened read-only and an authorizer admits only
  reads, so writes, schema changes, `ATTACH`, extension loading and
  state-changing `PRAGMA`s are refused before they run, and `ATTACH` in
  particular cannot turn the connector into a reader of other database files
  on the host. A deadline aborts long statements, and results are
  row-capped. Anyone who can reach the server can read the whole file, so
  expose only files meant for those clients.
- **MySQL / MariaDB** — `READ ONLY` transactions stop data and schema
  changes, but not everything a privileged account can do: live testing
  against MySQL 8.0 showed `SET GLOBAL` and `SELECT ... INTO OUTFILE`
  succeeding inside one. `query` therefore also admits only statements that
  begin with a reading keyword, and refuses `INTO OUTFILE`/`DUMPFILE` and
  executable `/*! */` comments. It judges them with strings and comments
  stripped, with the session's `sql_mode` set to a fixed value (no
  `NO_BACKSLASH_ESCAPES`, no `ANSI_QUOTES`) so the server reads string
  boundaries the same way. A `KILL QUERY` watchdog enforces the time limit on
  every statement type. Connect with an account holding only `SELECT` (no
  `FILE`, no admin privileges); the statement check is the second layer.
- **MongoDB** — there is no read-only session, so the connector's own checks
  are what keep it reading. Only read operations are exposed, aggregation
  stages come from a reading allow-list checked through nested pipelines, no
  stage may reach another database or a `system.*` collection, and
  server-side JavaScript is refused. Connect as a user with only the `read`
  role on the one database served.

## Known limitations (v0.3)

- **Sync tool cancellation is cooperative.** Python cannot force-kill a
  thread. When a sync tool's call is cancelled or times out, the response is
  discarded and the call's cancel token is triggered (`current_cancel_token()`),
  but the thread itself stops only if the tool acts on the token. The
  database connectors do: they stop the running statement on the database
  (`KILL QUERY`, a Postgres cancel request, SQLite `interrupt()`, MongoDB
  `killSessions`). A tool that ignores its token runs to completion. Its
  thread counts against `max_sync_workers` until then, so such tools cannot
  pile up without limit, and the audit log records `tool_finished_after_cancel`
  when one finishes. A write that already went out, such as the GitHub
  connector's `create_issue`, cannot be recalled by a cancel and may still
  complete. Long-running work that cannot watch the token belongs in async
  tools or external workers.
- **Tool threads get a bounded time at shutdown.** Sync tools and cancel
  callbacks run on daemon threads. As they stop, the transports wait for them
  for stdio's `shutdown_timeout`, or 5 s over HTTP/SSE
  (`MCPServer.wait_for_tool_threads`). That leaves time for a connector's
  `KILL QUERY` to reach the database. A thread still running after that dies
  with the process, without running its `finally` blocks. So does one in a
  process that drives `dispatch` itself and exits without that wait. Under
  your own uvicorn (`build_app()`), set `--timeout-graceful-shutdown`: uvicorn
  waits for open SSE streams to close before it shuts the app down, and a
  forced exit skips that wait entirely. `server.run()` closes those streams
  itself. A mounted `build_app()` gets no lifespan at all, so the host app
  must call `wait_for_tool_threads` on shutdown. Threads a sync tool starts
  itself are daemons as well, because they inherit the flag, and nothing
  waits for them. Pass `daemon=False` for work that must finish.
- **A MongoDB cancel reaches only the primary, and only calls with a
  session.** `killSessions` is sent to the primary. With a `readPreference`
  that routes reads to a secondary, a cancelled read there keeps running until
  its `maxTimeMS`. A call that the deployment will not give a session (no
  session support, or a member that is not readable yet) runs without one and
  cannot be killed. It ends at `maxTimeMS`, or for the discovery commands at
  the socket timeout. The server logs this once.
- **No TLS.** Terminate TLS at a reverse proxy (Caddy, nginx, a cloud LB).
  API keys travel in headers and must not cross the network in plaintext.
- **Single-process sessions.** Handshake-era SSE and Streamable HTTP sessions
  live in process memory; running multiple workers requires sticky routing
  (roadmap: shared session store). Stateless `2026-07-28` requests need no
  routing, but rate limits and per-client call caps are still counted per
  process.
- **API keys are static bearer secrets.** Rotate them by redeploying with new
  values; OAuth2 support is on the roadmap.

## Running untrusted or semi-trusted workloads

If a tool must process untrusted *content* (user uploads, scraped pages) or
execute anything resembling untrusted code, isolate it — do not rely on
easy_mcp for sandboxing:

1. Run the server (or just the risky tool's worker) in a container with a
   read-only filesystem, dropped capabilities, no network egress, and CPU/
   memory limits (`docker run --read-only --cap-drop=ALL --memory=... --cpus=...`).
2. For stronger isolation use gVisor, Firecracker, or a dedicated VM.
3. Run as an unprivileged OS user; never as root/Administrator.
4. Give the process only the secrets the registered tools actually need.

## Deployment checklist

- [ ] `debug=False` (the default) in production.
- [ ] Auth configured (`APIKeyAuth.from_env()`), keys ≥ 32 random characters.
- [ ] TLS terminated in front of the server.
- [ ] `allowed_origins` lists only the browser origins that should reach the server (default: loopback).
- [ ] Rate limit and `max_request_bytes` tuned to your workload.
- [ ] Timeouts set for every tool that touches the network or disk.
- [ ] Audit logs (`easy_mcp.audit`) shipped to your log store and reviewed.
- [ ] Scoped keys per client application; no shared "god" key.
- [ ] Tools validate/sanitize their own argument *content* (paths, SQL, shell).

## Reporting a vulnerability

Please email **markrodrigues2004@gmail.com** with details and a reproduction.
Do not open a public issue for security reports. You will get an
acknowledgment within 72 hours. Fixes for supported versions are released as
patch versions and credited unless you prefer otherwise.

| Version | Supported |
|---|---|
| 0.2.x | ✅ |
| 0.1.x | Security fixes only until 0.3.0 |
