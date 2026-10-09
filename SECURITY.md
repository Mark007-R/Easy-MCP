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
| Session hijacking | Session ids are 192-bit random capability tokens; every request on a session (SSE POST, Streamable HTTP GET/POST/DELETE), on whichever worker receives it, must present the same credential the session was opened with (403 otherwise), and its scopes come from that credential, never from stored session state; with OAuth, every request re-verifies its own token and the session is bound to the signed-in principal, comparing its issuer, subject and client in full; the principal's fingerprint (the rate-limit and call-count key) is 128 bits, so no client can grind a client id that shares another's |
| Tokens for other services or from other issuers | `aud` must name this server (`resource`, or `audience=`) and `iss` must equal a configured authorization server byte for byte, checked before anything is fetched; keys come only from that server's metadata; introspection answers must carry `aud` too |
| JWT algorithm confusion and forged keys | Asymmetric allow-list; `none`/HMAC refused at construction; each key's type, curve, size, `use`, `alg` and `key_ops` bound to the token's `alg`; `jwk`/`jku`/`x5u`/`x5c` headers ignored; symmetric keys never loaded |
| Token passthrough | Tools and middleware never receive the token (`Authorization` is withheld from middleware); `current_identity()` exposes only verified fields; the GitHub connector uses its own credential |
| Credential spraying against token verification | Token checks are rate-limited per client address: failed checks and checks still running share the `rate_limit_per_minute` budget, and past it a credential gets `429` without verification, however many arrive at once; key refreshes are bounded to one per 30 s per issuer; introspection is cached, shared between concurrent lookups and capped at 8 in flight (at most 4 for one client address), each with a fetch thread of its own, which no answer, however slowly it trickles in, holds past 5 s; once the authorization server itself fails, it is asked again at most every 5 s (`503` meanwhile, cached keys and answers still used, and no caller charged for its outage). A `401` for this server's credentials or a `429` is a failure of the server. So is any other failure (no answer, a timeout, a `3xx`, `4xx` or `5xx`, a `2xx` that is not a JSON object or is oversized), but only if the endpoint also fails a check with a random token (one at a time): a filter in front of it can do any of these to one token, by dropping or redirecting it or answering with a blocking page. Otherwise only the token sent fails, and it spends a unit of its sender's budget, so no token can shut the others out; an answer nested too deeply to parse is refused as malformed |
| Refresh tokens, ID tokens and bound tokens used as access tokens | Introspected `token_type` must be an access token; `typ` other than `at+jwt`/`JWT` refused; `aud` must be this server; tokens with `cnf` (DPoP, mTLS) refused |
| Server-side request forgery through OAuth | Only configured URLs and URLs from a configured issuer's validated metadata are fetched, `https` only (loopback `http` for development), redirects refused, bodies capped (1 MiB, 64 KiB for introspection), 5 s per fetch however slowly the answer arrives |
| Token leakage in logs | Tokens are never logged, kept or used as cache keys (only SHA-256 fingerprints); the introspection client secret stays out of every `repr`, log line and error |
| Header/body disagreement (stateless HTTP) | A proxy may route or rate-limit on the mirrored `MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name` headers while the server executes the body, so the server rejects any request whose headers are missing, repeated or disagree with its body (`400`, `-32020`); Base64-encoded names are decoded before comparing. A message without an `id` never runs a method (no header checks apply to it), and the client notifications this revision leaves undefined over HTTP, `notifications/cancelled` included, are ignored, so one caller cannot cancel another's call; a `subscriptions/listen` stream ends only when its own connection closes |
| DNS rebinding / cross-site requests | Browser `Origin` headers on the HTTP transports must match `allowed_origins` (loopback origins by default) or get 403 before any route runs; Streamable HTTP also requires `Content-Type: application/json` |
| Malformed / hostile input | Strict schema validation: unknown fields rejected, types enforced (bool ≠ int), required params enforced, before any tool code runs |
| Oversized payloads | `max_request_bytes` enforced on the Content-Length header *and* while streaming the body (a lying header does not help) |
| Request flooding | Per-client sliding-window rate limiting on every method, including discovery and opening an SSE session; idle clients are dropped from the limiter so its memory stays bounded; concurrent session cap (`max_sessions`) |
| Session exhaustion (Streamable HTTP) | Sessions idle past `session_idle_timeout` (default 1 h) expire; `max_sessions` caps the server's live sessions (503 beyond it), those of every endpoint serving it together and, with a shared store, of every worker; `DELETE` ends a session early and cancels its running calls, on every worker |
| Shared-store disclosure | A shared store holds no API keys, tokens or session ids: sessions are filed under SHA-256 digests of their ids, and call counts and rate limits under digests of client ids. Each session record keeps its client id in clear (a key fingerprint, a token principal's fingerprint, or `ip:<address>` for an anonymous client), the credential's fingerprint, a digest of a token's principal and 128-bit digests of the tool list its client was last told about; reading it yields nothing that opens a session |
| Cross-worker message injection | Cancels, session ends and relayed legacy SSE answers between workers are authenticated with a key derived from the session id, which the store never sees, checked against the session's credential fingerprint, and dropped when more than 60 s old; failures are audited as `bus_message_rejected`. Only integer or short string request ids are relayed, and relayed answers are capped at 4 MiB |
| Limits bypassed during a store outage | When the shared store cannot be reached, requests that depend on it (sessions, rate limits, `max_calls_per_session`) are refused (`-32008` with `data.reason = "store_unavailable"`, HTTP 503) rather than served without their limits; there is no fail-open setting, and `/healthz` answers 503 so a load balancer can drain the worker. A request fails closed the same way when Redis answers but refuses a write it needs (Redis full, read-only, failing to persist); `/healthz`, which checks only that Redis answers, stays 200 then |
| Resource exhaustion via slow tools | Per-tool and server-default timeouts; sync tools run off the event loop so they cannot stall other clients |
| Information disclosure | Production errors are opaque (`error_id` only); tracebacks stay in server logs; `debug=True` is loudly warned about at startup |
| Accidental exposure | Default bind is `127.0.0.1`; binding non-loopback without auth logs a warning at startup |
| Crash amplification | Exceptions in one tool call are contained; the server keeps serving |
| Protocol-stream corruption (stdio) | `sys.stdout` is redirected to stderr while serving, so tool `print()` calls cannot inject bytes into the JSON-RPC stream; oversized input lines are discarded unbuffered |
| Silent auth downgrade (stdio) | An invalid `EASY_MCP_STDIO_API_KEY` aborts startup instead of falling back to anonymous access |
| Change notifications revealing hidden tools | A client is told about a list change only when the list it may see changed: each recipient compares a digest of exactly what `tools/list` returns to its credential (on a session, its latest request's) with what it was last told. Notifications carry no names, so even a visible change reveals only "your list changed", which the next `tools/list` shows anyway |
| Stream exhaustion | `subscriptions/listen`: 8 streams per client and `max_sessions` per process, each opening one rate-limit unit, refused with `-32007` (HTTP 503) beyond; `GET /mcp`: one stream per session (a new one replaces the old) and one rate-limit unit per open, with sessions capped by `max_sessions`. Change notifications waiting to be written coalesce on every stream (`GET /mcp`, listen streams, legacy `/sse`), so a client that stops reading cannot grow a backlog of them (on `/sse`, answers to its own requests still queue); keep-alives every 15 s find dead peers, and a stream opened with an OAuth token ends when the token expires |
| Notifications after a cancel | A `notifications/cancelled` naming a listen stream (stdio, legacy SSE) ends it before anything else runs, so not even a change already pending is written for it; a closed HTTP listen stream cancels its request. Cancels reach only the subscriptions of the channel they arrive on, and duplicate subscription ids on one channel are refused (`-32600`) |
| Policy hooks weakening built-in checks | Middleware runs after the transport checks, the rate limit and protocol validation; tool middleware also after visibility, scopes, session caps and argument validation. It can refuse but cannot grant, cannot change the arguments a tool receives or the identity of the caller, and a failing middleware fails closed (`-32603`; the tool does not run if it failed before `call_next()`) |

## Transport trust boundaries

- **Streamable HTTP** and legacy **SSE** — clients are remote and untrusted;
  credentials arrive in headers, sessions are capability tokens bound to the
  credential that opened them, browsers are held to the `Origin` allowlist,
  and every protection above applies. With `oauth=`, every request needs a
  valid credential, checked before the MCP header checks
  (`MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`), `initialize`, the
  Streamable HTTP session lookup and any method. A few checks come first and
  are answered without one. On Streamable HTTP: the `Origin` allowlist
  (`403`), `Accept`, `Content-Type`, body size and JSON parsing (`406`,
  `415`, `413`, `400`) and a `GET /mcp` without a session id or for the
  stateless revision (`405`). On legacy SSE
  `POST /messages`: the `Origin` allowlist, body size and an unknown
  `session_id` (`404`) only; its JSON is parsed after the credential, so an
  unparseable body without a valid token gets `401`. Only the Protected
  Resource Metadata and `/healthz` serve anything without a credential.
- **Shared store** — Redis sits inside your trust boundary. Anyone who can
  write to it can end sessions and reset rate limits and call caps; they
  cannot take over a session, gain a scope, or inject messages into one.
  Anyone who can read it sees session metadata, client addresses and
  fingerprints and, for legacy SSE requests answered by another worker,
  those answers in transit. Use TLS (`rediss://`; the server warns about
  plaintext to a non-loopback host) and a dedicated user restricted to the
  `easy-mcp:` prefix, with exactly these rights (the live tests run as this
  user):

  ```
  ACL SETUSER easy-mcp on >CHANGE-ME resetkeys ~easy-mcp:1:* resetchannels &easy-mcp:1:* nocommands
      +ping +select +evalsha +script|load +publish +subscribe +unsubscribe +client|setinfo
      +exists +hget +hgetall +hset +hincrby +pexpire +del +zadd +zrem +zrange +zrangebyscore
      +zremrangebyscore +zcount +zcard +time
  ```

  As written, these rules need Redis 7.2 or later, the first with
  `CLIENT SETINFO`. On Redis 7.0 or 7.1, which refuse the whole
  `ACL SETUSER` over it, leave out `+client|setinfo`: redis-py carries on
  when the server refuses that command. Redis before 7.0 is not supported:
  it reports a write refused inside a script as a generic error, so a full
  or read-only Redis would fail requests as an internal error rather than
  with `503`.

  To fence one server off from another on the same Redis, narrow both
  patterns to its namespace, e.g. `~easy-mcp:1:{reports}:*` and
  `&easy-mcp:1:{reports}:*`. Keep the default `noeviction` memory policy and
  set `maxmemory`: when Redis is full, writes fail and the store fails
  closed, where an eviction policy would silently reset counters.
- **stdio** — the client is the *parent process* that launched the server
  (a desktop app, a CLI agent, an agent runtime). There is no network
  surface, but the parent is still treated as an MCP client: schema
  validation, scopes, rate limits, timeouts, payload caps, and error
  sanitization all apply unchanged. Protected tools stay hidden unless the
  parent presents a valid key via `EASY_MCP_STDIO_API_KEY`. Anything the
  parent can pass as environment it can also read, so a stdio key is a
  scoping mechanism, not a secret from the host itself. OAuth does not apply
  over stdio, as the MCP spec asks; a server with `oauth=` and no API keys
  refuses to start when a stdio key is set, rather than serving anonymously.

## Middleware

`@server.middleware` and `@server.tool_middleware` run your code around every
request and tool call. The same trust model applies to them as to tools:

- **Middleware is trusted code**, like tools. It runs in the server process,
  on the event loop.
- **What it sees:** validated tool arguments, the request's `_meta`, the
  caller's key fingerprint and scopes (for an OAuth token, also its verified
  subject, client, issuer and claims, never the token), and the HTTP
  request's headers with credentials removed (`Authorization`,
  `Proxy-Authorization`, `X-API-Key`, `Cookie`, `MCP-Session-Id`). Credentials are withheld so that a middleware
  cannot forward them; the MCP authorization spec forbids passing a client's
  token through to another service.
- **What it cannot do:** skip or reorder a built-in check, grant access, change
  the caller's identity or the arguments a tool receives, or rewrite a result.
  It can only refuse. A middleware that fails or breaks its contract fails the
  request closed with `-32603`. If it failed before `call_next()`, the tool
  does not run; if after, the tool has already run (a write it made stands),
  and `tool_result_withheld` is audited.
- **Treat headers, `_meta` and request ids as client-controlled.** Trust a
  header only if your proxy strips and re-sets it. Never authorize on
  `clientInfo`, which the client reports about itself.
- **Arguments, `_meta` and tool results can contain personal data or
  secrets.** Review what a middleware exports to logs or tracing backends.
  The audit events middleware adds carry none of them.
- **Session ids are capabilities for anonymous sessions.** Avoid exporting
  `request.session_id` in plain form.
- **Bound remote calls inside middleware with a timeout, on `initialize`
  too.** The tool timeout does not cover middleware, and a middleware that
  waits forever holds its request until it is cancelled:
  - on stateless HTTP, by the client closing the connection;
  - on stdio, legacy SSE and Streamable HTTP sessions, by
    `notifications/cancelled`; on legacy SSE also by the stream closing, and
    on a Streamable HTTP session also by `DELETE`;
  - everywhere, by shutdown.

  On a Streamable HTTP session a closed connection cancels nothing, and a
  session with a request running never idle-expires, so it keeps its
  `max_sessions` slot until one of the above. No client can cancel
  `initialize`: a middleware hung on it over Streamable HTTP holds a session
  slot (the client has no session id to cancel or delete with) until it
  returns or the server shuts down.
- **Never block the event loop.** A blocking middleware stalls every client;
  use `await asyncio.to_thread(...)` for blocking work.

## Ready-made connectors

The connectors are ordinary tool functions and follow the trust model above:
the client is untrusted, the credential in the environment is trusted.

- **GitHub** — give it a fine-grained token limited to the repositories and
  permissions the tools need (contents, issues, pull requests: read). Write
  tools exist only with `--allow-write` and are hidden from every client whose
  API key lacks the `github:write` scope (with OAuth, a token lacking it is
  refused); starting with `--allow-write` and neither keys nor OAuth is
  refused, and so is starting it over stdio without keys, since OAuth does
  not apply there. The token is sent only to `GITHUB_API_URL` and never
  logged, and the client's own credential never reaches GitHub.
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
  waits for open SSE streams (legacy `/sse`, `GET /mcp` and
  `subscriptions/listen`) to close and running requests to finish (one
  held in middleware never does) before it shuts the app down, and a forced
  exit skips that wait entirely. `server.run()` closes those streams itself
  and refuses new ones with `503`; the requests they carry are cancelled as
  shutdown begins and get no answer. Over Streamable HTTP it gives running
  `/mcp` requests 5 s to finish (a forced exit cuts that short) and then
  cancels them, answering each `503` with `-32008`. A mounted `build_app()`
  gets no lifespan at all, so the host app must run `server.lifespan()` from
  its own (it closes the streams, waits for the threads and, with OAuth,
  fetches the signing keys at startup). Threads a sync tool starts itself are daemons as well, because
  they inherit the flag, and nothing waits for them. Pass `daemon=False` for
  work that must finish.
- **A MongoDB cancel reaches only the primary, and only calls with a
  session.** `killSessions` is sent to the primary. With a `readPreference`
  that routes reads to a secondary, a cancelled read there keeps running until
  its `maxTimeMS`. A call that the deployment will not give a session (no
  session support, or a member that is not readable yet) runs without one and
  cannot be killed. It ends at `maxTimeMS`, or for the discovery commands at
  the socket timeout. The server logs this once.
- **No TLS.** Terminate TLS at a reverse proxy (Caddy, nginx, a cloud LB).
  API keys travel in headers and must not cross the network in plaintext.
- **Shared state needs a shared store.** By default, handshake-era sessions,
  rate limits and call caps live in process memory, so several workers need
  sticky routing and count limits per process. `store=RedisStore(...)`
  shares them. Running calls and open streams always stay in the worker that
  owns them. A cancel or end-session message lost while a worker's pub/sub
  connection is down lets the call run on until its timeout. A replica
  failover can lose the last few writes (a just-opened session, a few
  counts), and a call cancelled while its count was being taken may still
  spend it. Limits are only as trustworthy as the store: use an eviction
  policy of `noeviction`, or evicted counters reset their limits. Messages
  between workers can be replayed by someone who can read the store, within
  their 60 s window; that can only repeat a cancel or an answer the client
  has already settled.
- **Change notifications are per process.** Each worker tells the streams it
  holds about its own registry, so a tool registered at runtime in one worker
  only is announced by that worker alone (and the others list a different
  set). A session's `GET /mcp` stream replaced by a new one opened on another
  worker is not closed on the first: until that connection drops, a change
  may be announced on both. Over legacy SSE with a shared store, a
  `subscriptions/listen` posted to a worker that does not hold the stream is
  answered `-32601`.
- **A stdio client that stops reading stdout stalls the server** once the
  pipe buffer is full, as it always has: responses and notifications share
  the pipe. Notifications are rate-bounded (one per list per 0.1 s at most)
  and do not make this worse in practice.
- **An HTTP client that stops reading its stream holds up `server.run()`'s
  shutdown** once the connection's buffers are full: what the server has yet
  to send keeps the connection open, and uvicorn waits for every connection
  to close, until the client reads or disconnects. This holds for legacy
  `/sse`, `GET /mcp` and listen streams alike, and list changes can fill the
  buffers without the client sending anything. A second Ctrl-C (a forced
  exit) ends the wait; under your own uvicorn, `--timeout-graceful-shutdown`
  bounds it.
- **API keys are static bearer secrets.** Rotate them by redeploying with new
  values; for rotating, audience-bound credentials use `oauth=`.
- **A JWT access token stays valid until it expires, even if it is revoked.**
  With introspection, a revoked token is refused at most 60 s after
  revocation. Keep access tokens short-lived. A token's expiry during a
  running call does not cancel the call.
- **Proof-of-possession tokens (DPoP, mTLS-bound) are refused** rather than
  verified: this server cannot check the proof.
- **Over legacy SSE a missing tool scope arrives as a JSON-RPC error, not a
  `403`.** The POST has already been answered `202`, so SSE clients get no
  step-up challenge.
- **When the authorization server cannot be reached, cached signing keys stay
  in use**, without a time limit. While it answers, a key removed from its key
  set stops working within an hour: the hourly refresh starts 5 minutes
  early, and keys an hour old are not used again before a refresh has been
  tried (a request waits for it). Only once a refresh of keys that old has
  failed do they keep answering while later ones are tried in the
  background, at most every 30 s. A key set that arrives empty, or with no
  key this server can use, withdraws every cached key: tokens then get `503`
  until it publishes a usable one.
- **On Streamable HTTP with step-up, a token that lacks `required_scopes`
  learns whether a tool it calls exists**: the `403` that asks for the
  required scopes also names the tool's scope, so one sign-in covers the
  call. Such a token still comes from a trusted issuer for this audience, and
  would learn the same after one step-up; `step_up=False` keeps tools hidden.
  Legacy SSE checks the credential before it reads the body, so its `403`
  names only `required_scopes`.
- **OAuth caches and the failed-token throttle are per process.** Each worker
  fetches its own keys and counts failures on its own; behind a proxy, all
  clients share the proxy's address for that throttle, as for anonymous rate
  limiting.
- **No CORS headers yet.** A browser-based client on another origin cannot
  read `WWW-Authenticate` or the metadata cross-origin; clients behind a proxy
  are not affected.
- **Middleware is not covered by tool timeouts.** A tool's `timeout` bounds the
  tool function only; a middleware must bound its own awaits.
- **Some messages never reach middleware.** Requests the transport rejects,
  malformed messages, unknown methods and rate-limited messages are answered
  before any middleware runs, and only some of them are audited: rate-limited
  messages (`rate_limited`), refused browser origins (`origin_rejected`),
  stateless header mismatches (`header_mismatch`), session credential
  mismatches (`session_credential_mismatch`), oversized stdio lines
  (`payload_too_large`) and, with OAuth, refused tokens (`auth_failed`) and
  throttled addresses (`auth_rate_limited`). An invalid API key, a request
  without a token, unparseable JSON, a wrong `Content-Type`, an oversized
  HTTP body, a missing or unknown session, a malformed message and an unknown
  method leave no audit event; count them at your proxy if you need them.
- **A refusal after the tool ran cannot undo it.** A middleware that raises
  after `call_next()` replaces the answer, but the tool's side effects stand;
  this is audited as `tool_result_withheld`.

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
- [ ] With OAuth, `resource` is the exact public URL clients use, and the metadata path (`/.well-known/oauth-protected-resource/...`) reaches the server.
- [ ] Access tokens are short-lived and audience-restricted at the authorization server; `instructions` hold nothing secret (`server/discover` answers are public).
- [ ] TLS terminated in front of the server.
- [ ] `allowed_origins` lists only the browser origins that should reach the server (default: loopback).
- [ ] Rate limit and `max_request_bytes` tuned to your workload.
- [ ] Timeouts set for every tool that touches the network or disk.
- [ ] Audit logs (`easy_mcp.audit`) shipped to your log store and reviewed.
- [ ] Scoped keys per client application; no shared "god" key.
- [ ] Tools validate/sanitize their own argument *content* (paths, SQL, shell).
- [ ] Middleware that calls other services bounds each call with a timeout and exports no arguments, results or credentials without review.
- [ ] Multiple workers: a shared store, TLS to it (`rediss://`), a dedicated ACL user, `noeviction`, and a distinct server `name` (or `namespace=`) per server sharing it.

## Reporting a vulnerability

Please email **markrodrigues2004@gmail.com** with details and a reproduction.
Do not open a public issue for security reports. You will get an
acknowledgment within 72 hours. Fixes for supported versions are released as
patch versions and credited unless you prefer otherwise.

| Version | Supported |
|---|---|
| 0.3.x | ✅ |
| 0.2.x | Security fixes only until 0.4.0 |
| 0.1.x | ❌ |
