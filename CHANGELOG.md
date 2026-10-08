# Changelog

All notable changes to `easy-mcp-kit` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/); the public API is not frozen until 1.0.

## [Unreleased]

### Added

- Middleware. `@server.middleware` wraps every request the server implements
  and `@server.tool_middleware` wraps a tool's execution, each as an async
  function taking what is being served and `call_next`. Middleware runs after
  the built-in checks, which it cannot skip: the transport's checks, the rate
  limit and protocol validation, and for tool middleware also visibility,
  scopes, `max_calls_per_session` and argument validation. The first
  middleware registered is the outermost, and request middleware encloses tool
  middleware.

  Middleware refuses by raising before `call_next()`: a `ProtocolError`
  becomes that JSON-RPC error, and a `ToolError` an `isError` result on
  `tools/call` (on other methods, `-32603` with its message). A refused call
  does not run and does not count against `max_calls_per_session`. Raising
  after `call_next()` replaces the answer; the tool has already run, and the
  audit log records `tool_result_withheld`.

  Middleware observes and does not rewrite. `RequestInfo.params`, `.meta` and
  `ToolCall.arguments` are read-only, and `call_next()` returns a
  `RequestOutcome` or `ToolOutcome` describing what the client will get,
  timeouts and busy answers included. A middleware must return that object.
  One that returns anything else, or raises an unexpected exception, fails the
  request with `-32603` and an `error_id`. If it failed before `call_next()`,
  the tool does not run; if after, the tool has run and the audit log records
  `tool_result_withheld`. An error code from the range MCP reserves without
  defining (`-32023` to `-32099`) is treated the same way. `call_next()` may be
  awaited in a task of the middleware's own, but that work does not outlive
  the middleware: whatever is left of it when the middleware returns or
  raises is cancelled, and `call_next()` raises `RuntimeError` once the
  middleware has returned.

  A cancel (`notifications/cancelled`, a closed stateless connection, a
  deleted session, a closed SSE stream, shutdown) reaches middleware as
  `CancelledError`. A middleware that swallows it is overruled and no response
  is sent. A `CancelledError` a middleware raises when nothing cancelled the
  request is a failure like any other, and so is one the tool raises (see
  Fixed). Notifications and `server/discover` pass through middleware but
  cannot be refused.

  Middleware runs on the event loop. Context variables it sets before
  `call_next()` reach the tool, sync tools included. A tool's `timeout` still
  covers the tool only.

- `current_tool_call()` returns the running `ToolCall` inside tools and tool
  middleware: the caller's identity, the request's `_meta`, and a `state` dict
  middleware can fill.
- `TransportInfo`: how a message arrived (transport name, client address and
  port, HTTP version, and headers without `Authorization`,
  `Proxy-Authorization`, `X-API-Key`, `Cookie` or `MCP-Session-Id`).
  `MCPServer.dispatch()` takes it as the optional keyword `transport=`, and
  every built-in transport passes one. Custom transports should too; without
  it, middleware sees the name `"custom"`.
- `ClientContext.protocol_version` holds the version negotiated by
  `initialize`.
- Audit events `request_denied`, `tool_result_withheld`, `middleware_failed`
  and `request_cancelled`. `tool_denied` names the middleware that refused,
  when one did.
- OAuth 2.1 bearer tokens, per the MCP authorization spec, for both protocol
  eras. `MCPServer(oauth=OAuthResourceServer(resource=..., authorization_servers=[...]))`
  makes the server an OAuth resource server on Streamable HTTP and SSE. It
  serves Protected Resource Metadata (RFC 9728) at
  `/.well-known/oauth-protected-resource` followed by the path of `resource`.
  A request without a token gets `401` with a
  `WWW-Authenticate: Bearer resource_metadata="..."` challenge, so clients
  find the authorization server themselves. Once `oauth=` is set every
  request needs a credential (`server/discover` and notifications included).
  It is checked on every request, sessions included. On Streamable HTTP that
  is after the `Origin`, `Accept`, `Content-Type`, size and JSON checks and
  before the MCP header checks, the session lookup or any method. On legacy
  SSE `POST /messages` only the `Origin`, size and unknown-`session_id`
  (`404`) checks come first: its JSON is parsed after the credential.

  Tokens are verified locally as JWTs against the authorization server's
  published keys (new `[oauth]` extra, PyJWT 2.15+), or with token
  introspection (RFC 7662) when `introspection=Introspection(client_id,
  client_secret)` is passed, which needs no extra. A token must come from a
  listed authorization server and carry this server in `aud` (RFC 8707), or
  a value from `audience=`. Only asymmetric algorithms are accepted. `none`,
  HMAC, keys supplied in the token's header, `crit`, encrypted tokens,
  proof-of-possession-bound tokens and claims (or introspection answers)
  nested more than 32 levels deep are refused. Keys are refreshed hourly, in
  the background from 5 minutes before the hour so that a busy server holds
  up no request, and when a token names an unknown key, at most once every
  30 s (that token waits for the fetch). Keys an hour old are not used again
  before a refresh has been tried (a request waits for it), so a key the
  authorization server withdraws stops working within the hour. When the
  authorization server cannot be reached, the keys already fetched stay in
  use, and after one failed refresh they answer without waiting; a key set
  that arrives with no usable key withdraws them. With no keys the answer is
  `503` with `-32008` (`data.reason: "auth_server_unavailable"`) and
  `Retry-After: 5`. With introspection, when the authorization server fails
  a discovery or introspection request, it is not asked again for 5 s:
  meanwhile tokens without a cached answer get that `503` at once, and the
  failure is logged once. An introspection request shows the server failing
  when it gets `401` (this server's credentials) or `429`, or when it gets
  no answer, times out, or gets `5xx` or a `2xx` that is not a JSON object
  and a check with a random token fails too. Otherwise, as with any other
  `4xx` or an answer over 64 KiB, that token alone fails (`503`, charged to
  the sender's failed-token budget), so a token that a filter in front of
  the endpoint blocks or drops cannot shut the others out. An answer nested
  too deeply to parse is refused as malformed (`401`, cached). Each of the 8
  introspection requests in flight gets a fetch thread at once, so a slow
  but working authorization server is not taken for a failing one.

  Token scopes map onto the existing per-tool `scopes`, which are
  alternatives: list the narrowest first. A signed-in caller sees every tool,
  and a call its token does not cover gets `403 insufficient_scope` naming
  the scope to ask for (`step_up=False` keeps such tools invisible instead).
  `required_scopes` are needed by every request; on Streamable HTTP a
  `tools/call` whose token lacks them is also asked for the tool's scope, in
  the same challenge. Legacy SSE `POST /messages` checks the credential
  before it reads the body, so its `403` asks only for `required_scopes`; a
  missing tool scope arrives there as a `-32001` JSON-RPC error on the
  stream (`data.error = "insufficient_scope"`), not a `403`. A token's `*`
  scope is never a wildcard. The `401` and `insufficient_scope`
  `403` bodies carry `-32001`, never `-32002`; a handshake-era session used
  with another principal's token still gets `403` with `-32002`, as in
  0.3.1. A token's principal is identified by a 32-hex-digit fingerprint of
  its issuer, subject and client (the rate-limit and call-count key, and the
  `client_id` of its audit events), and a session is bound to all three.

  `easy_mcp.current_identity()` gives a tool the verified caller (`subject`,
  `client_id`, `issuer`, `scopes`, `claims`); the token itself is never
  exposed. API keys keep working next to tokens. Over stdio OAuth does not
  apply, as the spec asks. The connectors read `EASY_MCP_OAUTH_*`. New audit
  events: `auth_failed`, `auth_rate_limited`, `auth_unavailable` and
  `principal_seen`; `tool_denied` carries the `scope` a step-up asked for.
- `MCPServer.authenticate_request()` resolves an HTTP request's credential
  (API key or access token), and `MCPServer.auth_configured` says whether
  `auth` or `oauth` is set.
- `MCPServer.lifespan()`: the startup and shutdown of `build_app()` (OAuth
  key warm-up; closing streams, cancelling what is left, waiting for tool
  threads), for apps that mount it and so run no lifespan of its own.
- `/healthz` reports `"oauth": "ok"` or `"unavailable"` when OAuth is
  configured (still with status `200`). It is unavailable while no signing
  keys are cached or, with introspection, while the endpoint is unknown and
  from the moment the authorization server fails (as above) until a request
  to it succeeds again.
- `ClientIdentity` gains `subject`, `client_id`, `issuer`, `expires_at` and
  `claims`, all empty for API keys. `claims` is a read-only mapping, and an
  identity can still be copied, pickled and passed to
  `dataclasses.asdict()`. `ToolDefinition` gains `declared_scopes`,
  the tool's scopes in the order given. `APIKeyAuth.match()` tries a key
  without raising, and `SlidingWindowRateLimiter.exceeded()` looks at a
  budget without spending it.
- Token checks are rate-limited per client address, with the server's
  `rate_limit_per_minute` as the budget: every failed check spends a unit,
  and every check still running holds one. Past it, presented credentials
  get `429` without being checked, however many arrive at once.
- Exceptions `TokenRequiredError`, `InvalidTokenError` (`-32001`, `401`),
  `InsufficientScopeError` (`-32001`, `403`) and `AuthServerUnavailableError`
  (`-32008`, `503`). No new error codes.

### Changed

- Every request except `initialize` runs as its own task and can be cancelled
  with `notifications/cancelled`; before, only `tools/call` could. A cancelled
  request gets no response, as before for `tools/call`, and one other than
  `tools/call` is audited as `request_cancelled`.
- With request middleware registered, a stateless `tools/list` carries
  `cacheScope: "private"`, because its answer may now depend on the caller.
  `server/discover` stays `public`.
- A `tools/call` holds one unit of `max_calls_per_session` from the moment it
  passes the built-in checks. The unit is returned if the tool never starts:
  a middleware refusal or failure, a busy answer, or a cancel before the tool
  starts. So a refused call does not count, but while a call waits in tool
  middleware, concurrent calls beyond the cap get `-32006`. Before, only a
  call refused as busy was refunded.
- The `403` that refuses a browser `Origin` carries `-32600` when the
  request's `MCP-Protocol-Version` header names the stateless revision, which
  forbids `-32002` in any response. Older clients still get `-32002`.
- No error response carries `-32002` on a stateless request (it becomes
  `-32001`), or a code from `-32023` to `-32099`, which MCP reserves without
  defining (it becomes `-32603` with an `error_id`, and the original is
  logged).
- A stateless Streamable HTTP error carrying `-32020`, `-32021` or `-32022`,
  which middleware may raise, is answered with HTTP `400`, as the stateless
  revision requires for these codes. Before, only the transport's own checks
  answered them with `400`; any other source got `200`.
- A request (a message with an `id`) that names a `notifications/*` method is
  answered `-32601`, as an unknown method. Before, it was treated as that
  notification and got no response at all: over HTTP a bare `202`, on stdio
  nothing, so the client waited forever.
- Shutting down the Streamable HTTP transport with `server.run()` (Ctrl-C,
  `server.stop()`) gives the requests still running 5 s to finish, as stdio
  does, then cancels them and ends the sessions, before uvicorn waits for
  connections to close. A second Ctrl-C (uvicorn's forced quit) ends that wait
  at once. A request cancelled this way, or sent once shutdown has begun, is
  answered `503` with `-32008` (`data.reason: "shutdown"`) and
  `Retry-After: 1`, and a handshake cut short opens no session. A
  `notifications/cancelled` sent meanwhile still cancels the call it names,
  which then gets no answer. The legacy SSE endpoints close their streams as
  shutdown begins, cancelling the requests they carry (which get no answer,
  as before), and answer new streams and messages `503`. Before, shutdown
  waited for every running request to finish, without a bound for a tool with
  no timeout.
- In a handshake-era session, each request now runs with the identity its own
  credential resolves to, rather than the one that opened the session. For
  API keys the two are always the same. For OAuth it means a refreshed or
  broader token takes effect at once, in the same session. The session stays
  bound to the principal that opened it.
- With `oauth=` set, a stdio server refuses to start when
  `EASY_MCP_STDIO_API_KEY` is set but no API keys are configured, instead of
  serving anonymously.
- With `oauth=` set, tool scopes must be RFC 6749 scope-tokens other than
  `offline_access` (they appear in `WWW-Authenticate` challenges); such a
  registration raises `ToolRegistrationError`. Without `oauth=` nothing
  changes.
- The GitHub connector's `--allow-write` accepts OAuth as well as API keys on
  the HTTP transports. With OAuth every signed-in client sees the write
  tools, and a token without `github:write` is refused with
  `403 insufficient_scope`. Over stdio, where OAuth does not apply, it still
  needs `EASY_MCP_API_KEYS`.
- `StreamableHTTPTransport(path=...)` refuses a path under `/.well-known/`,
  where metadata is served.

### Fixed

- `dispatch` no longer swallows a cancellation of its own caller during a
  `tools/call`. Telling a client's cancel from the caller's by the state of
  the call's task failed, because asyncio cancels the awaited task in both
  cases: an `asyncio.timeout()` around `dispatch` never raised `TimeoutError`,
  and a transport that cancelled a dispatch saw it return normally. The
  caller's cancellation is now re-raised; a client's cancel still drops the
  response.
- A `tools/call` whose id was a JSON array or object answered `-32603` while
  its tool ran on in the background. Such an id is now served normally; it
  just cannot be cancelled.
- A tool that raises `CancelledError` when nothing cancelled the call
  (awaiting a shared future another waiter cancelled, say) has failed: the
  client gets an `isError` result with an `error_id`, and the audit log a
  `tool_call` with `status: "error"`. Before, the request got no response at
  all (over HTTP a bare `202`, on stdio nothing, so the client waited
  forever), was audited as `tool_cancelled`, and ran the tool's cancel
  callbacks. Any other request that raises `CancelledError` on its own is
  answered `-32603` with an `error_id`.
- A legacy SSE message whose stream closed while its body was being read is
  answered `404`, as a message for a closed session is. Before, it was
  served with no stream to answer on and nothing to cancel it; only the tool
  timeout ended it.

## [0.3.1] - 2026-09-30

### Added

- Cancellation reaches sync tools. Every tool call has a `CancelToken`,
  available inside the tool as `easy_mcp.current_cancel_token()`. It is
  triggered when the call is cancelled (`notifications/cancelled`, a closed
  stateless connection, a deleted session, stdio shutdown) or runs past its
  timeout. A tool registers callbacks with `token.on_cancel(...)`; they run on
  a thread of their own, off the event loop, and a failing one is logged and
  audited as `cancel_callback_failed`. `token.cancelled` and `token.reason`
  can be polled. `cancel_scope(token)` sets a token for code that calls a tool
  function directly. Tools that ignore the token behave as before.
- `max_sync_workers` (default 32) caps the sync tools running at once. A call
  beyond it is refused with the new `ServerBusyError` (`-32008`), and does not
  count against `max_calls_per_session`. The cap is shared by every client of
  the server. A worker is free again before a finished call's answer is sent,
  so a call is never refused because of its client's previous call, once that
  call has finished. A call that timed out or was cancelled keeps its worker
  until the tool returns: briefly for a tool that acts on its token, until it
  finishes for one that does not. A retry sent right away may therefore get
  `-32008`.
- The audit event `tool_finished_after_cancel` records a sync tool that
  finished after its call was cancelled or timed out, with its outcome.
- `MCPServer.wait_for_tool_threads(timeout)` gives sync tool threads and
  cancel callbacks a bounded time to finish, including calls cancelled just
  before that have yet to start their callbacks. Stdio calls it after its
  `shutdown_timeout` drain, with the same timeout. The HTTP and SSE transports
  call it for 5 s when their app shuts down, after cancelling the calls of
  the sessions they close. Call it yourself before exiting
  when you drive `dispatch` directly: the threads are daemons, so a process
  that exits first stops a callback before its `KILL QUERY` goes out.

### Changed

- Each sync tool call runs on a daemon thread of its own instead of asyncio's
  shared default executor. A thread left behind by a cancelled call no longer
  holds up unrelated calls queued behind it. At exit the transports wait for
  such threads only for the bounded time above. 0.3.0 waited until they
  finished, however long that took. A thread takes its daemon flag from the
  thread that starts it, so a `threading.Thread` or `threading.Timer` that a
  sync tool starts without `daemon=` is now a daemon too. It is stopped at
  exit without running its `finally`, and nothing waits for it. 0.3.0 waited.
  Pass `daemon=False`, or use a `ThreadPoolExecutor`, for work that must
  outlive the call. A mounted `build_app()` runs no lifespan, so the host app
  should call `wait_for_tool_threads` on shutdown.
- `server.run()` over HTTP closes open legacy SSE streams as shutdown begins.
  Before, uvicorn waited for SSE clients to disconnect before it would shut
  the app down, so Ctrl-C with a client connected hung until a second one
  forced the exit.
- Sync calls beyond `max_sync_workers` are refused with `-32008` at once.
  0.3.0 queued them on the default executor, which held at most
  min(32, CPUs + 4) threads. The workers are shared by all clients, so a
  refused call should be retried after a short delay, even by a client with
  few calls of its own.
- The database connectors warn at startup when the server's `default_timeout`
  is not longer than their statement timeout.
- The MongoDB connector passes `session=` to every operation except
  `estimated_document_count`. A `database_factory` test double therefore needs
  methods that accept it. Its `client` needs `start_session()` and
  `admin.command()` for a cancel to reach the server. A double without
  `start_session` (or one raising `NotImplementedError`, like mongomock) runs
  calls without a session.
- A `TimeoutError` raised by a tool itself, such as a socket read timing out,
  is now reported as a tool failure (`isError`, "Tool execution failed"). It
  used to be reported as the server's `-32005` timeout. The tool timeout is
  measured with `asyncio.timeout`, which tells the two apart.

### Fixed

- A cancelled or timed-out call no longer leaves its query running on the
  database until the statement deadline. MySQL sends `KILL QUERY` at once;
  before, only the deadline watchdog did. Postgres sends a cancel request
  (`cancel_safe` on psycopg 3.2+, `cancel` on 3.1). SQLite interrupts the
  statement, and its progress handler also catches a cancel that lands just
  before a statement starts. MongoDB runs each call in a session of its own
  and ends it with `killSessions`, which needs no privilege beyond `read` for
  one's own sessions. A session that a kill may still target is never put
  back in the driver's pool, so the kill cannot reach another call. A kill
  ends the operation running when it lands, and `describe_collection` checks
  for a cancel between its steps. The kill goes to the primary: with a
  `readPreference` that sends reads to a secondary, a cancelled read there
  runs on until `maxTimeMS`. A call that the deployment refuses a session (a
  server without session support, or a member that is not readable yet) runs
  without one and cannot be killed, as in 0.3.0; the next call tries again. A
  call cancelled before its statement starts never sends it.
- The GitHub connector sends no request for a call that has already been
  cancelled or timed out. A write that was already sent may still complete;
  this is documented, and audited as `tool_finished_after_cancel`.

## [0.3.0] - 2026-09-27

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

- `easy-mcp-mysql` (new `[mysql]` extra, PyMySQL), for MySQL and MariaDB:
  `list_databases`, `list_tables`, `describe_table` and `query`. Each statement
  runs on its own connection in a `READ ONLY` transaction that is always
  rolled back. Because such a transaction still lets a privileged account run
  `SET GLOBAL` or `SELECT ... INTO OUTFILE`, `query` also admits only
  statements that begin with a reading keyword. It refuses `INTO
  OUTFILE`/`DUMPFILE` and executable `/*! */` comments, judged with strings and
  comments stripped. A `KILL QUERY` watchdog enforces `--statement-timeout` on
  every statement type, since `max_execution_time` covers only `SELECT`.
  The session's `sql_mode` is set to a fixed value, so a server running with
  `ANSI_QUOTES` or `NO_BACKSLASH_ESCAPES` cannot read string boundaries
  differently from the check. Decimals come back as exact strings and BLOBs
  as base64.

- `easy-mcp-mongodb` (new `[mongodb]` extra, pymongo): `list_collections`,
  `describe_collection` (estimated count, indexes, field types from a sample),
  `find`, `count` and `aggregate` over one database. Only reading aggregation
  stages are admitted, checked through `$facet`, `$lookup` and `$unionWith`,
  so `$out` and `$merge` are refused and no stage can reach another database.
  Server-side JavaScript (`$where`, `$function`, `$accumulator`) is refused
  anywhere. Every query carries `maxTimeMS`; the discovery commands, which
  MongoDB gives none, are bounded by the socket timeout. Values travel as
  relaxed Extended JSON both ways, including dates outside Python's range, and
  malformed Extended JSON is a clear tool error. `describe_collection` also
  describes views, and a `mongodb+srv://` URI is resolved on first use rather
  than at startup.

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

[Unreleased]: https://github.com/Mark007-R/Easy-MCP/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/Mark007-R/Easy-MCP/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.5...v0.3.0
[0.2.5]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Mark007-R/Easy-MCP/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Mark007-R/Easy-MCP/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Mark007-R/Easy-MCP/releases/tag/v0.1.0
