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
- Resource, prompt and completer functions are trusted code too, and
  resource template values and prompt arguments are attacker-influenced input
  like tool arguments. Resource contents may carry third-party text (files,
  web pages, tickets) into a model's context, an indirect prompt-injection
  channel the library cannot sanitize: mark content meant for people only
  with `annotations={"audience": ["user"]}`, and keep untrusted content out of
  resources that models read on their own.

## Protections built in

| Threat | Mitigation |
|---|---|
| Credential stuffing / key probing | Constant-time comparison of SHA-256 digests over the full key set (`hmac.compare_digest`); timing reveals neither partial matches nor key length |
| Key leakage via logs | Raw keys never logged; only SHA-256 fingerprints appear in logs and audit events |
| Unauthorized tool use | Per-tool `requires_auth` and scope checks; protected tools are omitted from `tools/list` and report as unknown to unauthorized callers (no enumeration) |
| Unauthorized resource or prompt access | The same `requires_auth`/scope checks as tools; protected resources, templates and prompts are omitted from lists and answer exactly like missing ones on read, get, subscribe and completion (same code, message and `data`); cursors reveal only keys the caller was shown. With OAuth step-up, a token is challenged (`403`) instead, as for tools, and a listen's acknowledgment leaves out resources it cannot read |
| Directory traversal through resource templates | Template values are percent-decoded as strict UTF-8 and refused before the function runs if they contain a `.` or `..` segment, a backslash or a control character (and any `/` for `{name}`, a leading one for `{+name}`); such a URI reads as missing. `safe_path()` confines file access to a root, symlinks included, refusing absolute, drive and UNC paths |
| Completion-based disclosure | Completers run only for callers who may see their prompt or template; at most 100 values; every request spends the rate limit; errors are sanitized |
| Cross-caller caching of protected data | Stateless lists are `cacheScope: "private"` whenever auth is configured or request middleware is registered; a `resources/read` is `private` for a protected resource, for any read of a public template a protected resource or template may answer for instead, with `oauth=`, with request middleware, or for a retry carrying `inputResponses`/`requestState`. Access is checked on every request whatever a cache holds |
| Notification flooding / subscription exhaustion | Updates carry only a URI and coalesce per recipient; a session watches at most 1000 URIs and a listen names at most 1000 (`-32007`, HTTP `503`, beyond), each at most 2048 characters; listen streams keep their caps (8 per client, `max_sessions` per process); opening a stream spends rate-limit budget |
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
| Shared-store disclosure | A shared store holds no API keys, tokens or session ids: sessions are filed under SHA-256 digests of their ids, and call counts and rate limits under digests of client ids. Each session record keeps its client id in clear (a key fingerprint, a token principal's fingerprint, or `ip:<address>` for an anonymous client), the credential's fingerprint, a digest of a token's principal, 128-bit digests of the lists its client was last told about, and the resource URIs it subscribed to, in clear (keep secrets out of URIs); reading it yields nothing that opens a session |
| Cross-worker message injection | Cancels, session ends, relayed legacy SSE answers and "read the subscriptions again" notices between workers are authenticated with a key derived from the session id, which the store never sees, checked against the session's credential fingerprint, and dropped when more than 60 s old; failures are audited as `bus_message_rejected`. Only integer or short string request ids are relayed, and relayed answers are capped at 4 MiB |
| Limits bypassed during a store outage | When the shared store cannot be reached, requests that depend on it (sessions, rate limits, `max_calls_per_session`) are refused (`-32008` with `data.reason = "store_unavailable"`, HTTP 503) rather than served without their limits; there is no fail-open setting, and `/healthz` answers 503 so a load balancer can drain the worker. A request fails closed the same way when Redis answers but refuses a write it needs (Redis full, read-only, failing to persist); `/healthz`, which checks only that Redis answers, stays 200 then |
| Resource exhaustion via slow tools | Per-tool and server-default timeouts; sync tools run off the event loop so they cannot stall other clients |
| Information disclosure | Production errors are opaque (`error_id` only); tracebacks stay in server logs; `debug=True` is loudly warned about at startup |
| Accidental exposure | Default bind is `127.0.0.1`; binding non-loopback without auth logs a warning at startup |
| Crash amplification | Exceptions in one tool call are contained; the server keeps serving |
| Protocol-stream corruption (stdio) | `sys.stdout` is redirected to stderr while serving, so tool `print()` calls cannot inject bytes into the JSON-RPC stream; oversized input lines are discarded unbuffered |
| Silent auth downgrade (stdio) | An invalid `EASY_MCP_STDIO_API_KEY` aborts startup instead of falling back to anonymous access |
| Change notifications revealing hidden tools, resources or prompts | A client is told about a list change only when the list it may see changed: each recipient compares a digest of exactly what the list returns to its credential (`tools/list`, `prompts/list`, or `resources/list` and `resources/templates/list` together) (on a session, its latest request's; with a shared store, the latest one the worker holding its `GET /mcp` stream served) with what it was last told, and a session whose credential changes what it may see is told once to list again (with no `GET /mcp` stream open, as soon as one opens). Notifications carry no names, so even a visible change reveals only "your list changed", which the next `tools/list` shows anyway |
| Stream exhaustion | `subscriptions/listen`: 8 streams per client and `max_sessions` per process, each opening one rate-limit unit, refused with `-32007` (HTTP 503) beyond; `GET /mcp`: one stream per session (a new one replaces the old) and one rate-limit unit per open, with sessions capped by `max_sessions`. Change notifications waiting to be written coalesce on every stream (`GET /mcp`, listen streams, legacy `/sse`), so a client that stops reading cannot grow a backlog of them (on `/sse`, answers to its own requests still queue); keep-alives every 15 s find dead peers, and a stream opened with an OAuth token ends when the token expires |
| Notifications after a cancel | A `notifications/cancelled` naming a listen stream (stdio, legacy SSE) ends it before anything else runs, so not even a change already pending is written for it; a closed HTTP listen stream cancels its request. Cancels reach only the subscriptions of the channel they arrive on, and duplicate subscription ids on one channel are refused (`-32600`) |
| Policy hooks weakening built-in checks | Middleware runs after the transport checks, the rate limit and protocol validation; tool middleware also after visibility, scopes, session caps and argument validation. It can refuse but cannot grant, cannot change the arguments a tool receives or the identity of the caller, and a failing middleware fails closed (`-32603`; the tool does not run if it failed before `call_next()`) |
| Metrics disclosure | `/metrics` is off unless `metrics=True`. With auth configured it needs an API key or token holding `metrics:read` (a key's `*` too; a token's `*` is no wildcard). Without auth only a loopback peer that addressed a loopback `Host`, with no proxy forwarding header (`Forwarded`, `X-Forwarded-For`, `X-Forwarded-Host`, `X-Real-IP`), gets it, so neither a remote host connecting directly (which can forge `Host`) nor a DNS-rebinding page in a local browser (which cannot) can read it. Behind a proxy that check is not enough: uvicorn's proxy-header trust (`--proxy-headers` with `--forwarded-allow-ips` or `FORWARDED_ALLOW_IPS`, `*` above all) makes the peer address client-controlled, so a server behind any proxy, local or remote, needs auth before it serves `/metrics`. The `Origin` allowlist applies, scrapes spend the rate limit, and refusals are audited (`metrics_denied`). Metrics carry no client ids, key fingerprints, session ids, addresses or request ids |
| Telemetry injection and cardinality | Method names outside the MCP method set are recorded as `_OTHER`, tool and prompt names only for registered ones, request ids cut to 128 characters and resource URIs to 512 (spans only, never span names or metric labels); trace-context values are read only from `traceparent`, `tracestate` and `baggage` in `_meta`, and over HTTP from the header fields of the configured propagators (those three by default), capped at 8192 characters and parsed by those propagators (the W3C parsers by default); values that name no valid span are ignored. Each metric keeps at most 2000 series, folding the rest into `_OTHER`. Rate-limited and transport-refused messages create no span. Spans hold no arguments, results, exception text, headers or raw session ids (a SHA-256 digest stands in), not even with `debug=True`; a failed call carries its `error_id` |
| Telemetry failures | Every telemetry step is guarded: a broken span processor, exporter or SDK never changes an answer (the first failure is logged as a warning, later ones at debug level), and easy_mcp itself never awaits an exporter; but span processors run inside `span.end()` on the event loop, so use `BatchSpanProcessor` (a `SimpleSpanProcessor` makes every request wait for the export) |

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
  `/metrics` (with `metrics=True`) follows the same `Origin` and credential
  rules, needs the `metrics:read` scope, and without auth answers only
  local requests addressed to a loopback host that carry no proxy
  forwarding header.
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
  No listener is opened for metrics over stdio: `render_metrics()` and an
  OpenTelemetry exporter are the ways out, and over stdio a console exporter
  must write to stderr (`ConsoleSpanExporter(out=sys.stderr)`), since it
  holds on to the real stdout.

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

## The surfaces 0.3.2 adds, together

0.3.2 adds long-lived streams, one new unauthenticated endpoint (the OAuth
Protected Resource Metadata; the existing `/healthz` gains fields),
`/metrics`, a shared store with messages between workers, and two telemetry
channels that leave the host. The table above has a row for each
protection; this section reviews the surfaces side by side, and then where
they meet.

| Surface | Who can reach it | What it exposes or accepts | What bounds it | Left to you |
|---|---|---|---|---|
| `GET /mcp` streams | the holder of a session id together with the credential that opened the session | list-change notifications without names, judged by what the session's latest credential may see; `notifications/resources/updated` with the URI | one per session (a new one replaces the old), sessions capped by `max_sessions`, one rate-limit unit per open, a coalescing outbox, keep-alives every 15 s, the expiry of the OAuth token it opened with | proxy buffering off, idle timeouts above 15 s, `--timeout-graceful-shutdown` under your own uvicorn |
| `subscriptions/listen` streams | any stateless client that passes authentication (anonymous ones unless `oauth=` is set; with `auth=` alone they hear only what public callers may see) | the same notifications, tagged with the listen's id, for resources the caller may read | 8 per client and `max_sessions` per process, 1000 URIs, one rate-limit unit per open; its identity is the listen request's for the stream's life, which ends when that request's OAuth token expires | as for `GET /mcp` |
| `/metrics` | with auth, a key or token holding `metrics:read` (or a key holding `*`); without auth, only a loopback peer that addressed a loopback host with no proxy forwarding header | request and tool counts and durations by method, registered tool and prompt name, outcome and protocol version; no client ids, fingerprints, session ids, addresses or request ids | off unless `metrics=True`; the `Origin` check; the rate limit; 2000 series per metric; refusals audited (`metrics_denied`) | auth behind any proxy, local or remote; a scrape key of its own, which is also a key on `/mcp` |
| `/.well-known/oauth-protected-resource/...` and `/healthz` | anyone: no credential, no rate limit | the metadata: `resource`, the authorization servers and `scopes_supported` (`required_scopes`, or with `step_up=False` every scope a tool, resource or prompt names); `/healthz`: the server's name and version, its tool count, and with OAuth or a shared store their status | small, constant answers (the metadata may be cached for 5 minutes); the `Origin` check | scope and server names that say nothing secret; filter `/healthz` at the proxy if its version and tool count should stay private |
| The shared store and the bus between workers | whoever reaches Redis with its credentials | session records (client ids, credential fingerprints, list digests, subscribed resource URIs in clear), counts and rate limits, relayed legacy SSE answers in transit | no API keys, tokens or session ids are stored; messages between workers are authenticated with a key derived from the session id, bound to the session's credential and dropped after 60 s; the store fails closed | TLS, the least-privilege ACL user, `noeviction` with `maxmemory`, a namespace per server: Redis is inside the trust boundary |
| Resource URIs in audit logs and spans | your log store, your tracing backend, and whoever can read them | each URI a client reads or watches, cut to 512 characters, template values included (`users://{user_id}/...` logs the id) | never in span names or metric labels; contents never logged | no secrets in URIs; logs and traces treated as holding the identifiers your URIs carry; a span processor that drops `mcp.resource.uri` if the tracing backend is less trusted than the log store |
| Client trace context | any client | `traceparent`, `tracestate` and `baggage` in `_meta`, and over HTTP as headers (the fields of the configured propagators) | 8192 characters each, the configured propagators' parsers (W3C by default), malformed values ignored and never echoed; log lines carry only this process's own span ids | `ParentBased(remote_parent_sampled=...)` to cap sampling, `OTEL_PROPAGATORS=tracecontext` to drop client baggage |
| Middleware | your code, on every request that passes the built-in checks | validated arguments, `_meta`, the caller's identity (never the token), headers without credentials | it cannot skip a check, grant access or rewrite a result; it fails closed; telemetry encloses it, so its refusals are still measured | bounded awaits; a review of what it logs or exports; no decisions on headers a client can set or on `clientInfo` |

Where they meet:

- **A resource URI leaves the host more than once.** The audit log and the
  request's span both carry it, an exporter sends the span to your tracing
  backend, and with a shared store a subscribed URI also sits in Redis. A
  template that puts an identifier into URIs puts it in all three places.
- **Client baggage can reach other services.** Baggage a client sends is
  current inside the tool, so an instrumented HTTP client there forwards it
  to whatever the tool calls: a client can put up to 8192 characters into
  another service's request headers. `OTEL_PROPAGATORS=tracecontext` stops
  it.
- **A client chooses the trace its spans join**, and under a parent-based
  sampler whether they are sampled. Spans carry `easy_mcp.client_id`, so the
  choice stays attributable.
- **Streams follow credentials differently.** A session's `GET /mcp` stream
  judges what to announce by the credential of the session's latest request
  (served by the worker holding the stream, with a shared store), so a
  broader token widens what it hears and a narrower one narrows it. A listen
  stream keeps the identity it opened with. Both end when the OAuth token
  they opened with expires; the legacy `/sse` stream does not, since every
  message posted to its session (`POST /messages`) is verified again.
- **Everything that streams, counts or traces is per process.** A scrape
  sees one worker; a listen hears the changes made in its own worker; a
  probe for hidden tools shows in telemetry (`easy_mcp_status="unauthorized"`
  and the span's tool name) but not in the audit log.
- **A scrape key is an API key.** It also authenticates on `/mcp` and can call
  public tools and `requires_auth` tools without scopes. `/metrics` also
  shows the scraper which registered tools and prompts were called,
  protected ones included.
- **The metadata and step-up trade disclosure.** With `step_up=False` the
  public metadata lists every item scope, so its scope names hint at what
  protected items exist; with step-up (the default) it lists only
  `required_scopes`, but every signed-in caller sees every item, and a
  `403` names the scope it needs.
- **Middleware writes into the request's span.** Attributes it adds leave
  through the exporter with the span, so review them as you review its logs.
- **A client that stops reading any stream holds up shutdown** once its
  buffers are full (see Known limitations); list changes can fill them
  without the client sending anything.

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
- **Resource updates are per process, and best effort.**
  `notify_resource_updated` tells the streams of its own worker only, so the
  code calling it must run in every worker. With a shared store, an update
  made while a session has no stream open is lost (its next stream may open
  on any worker); with the default store it waits for the next stream, and
  an update still queued on a stream whose client leaves is lost either way.
  A client that subscribed while a resource was public keeps being told its
  URI after it is re-registered as protected; reads are still refused.
- **Resource contents are not size-capped.** A resource's return value is
  held in memory and base64-encoded when binary; serve large data in pieces
  through a template.
- **Capabilities reflect what is registered.** The first resource or prompt
  registered after serving began reaches clients that cached
  `server/discover` only when their copy expires (within the hour; logged).
  URI templates support `{name}` and `{+name}` only.
- **URIs are audit subjects.** Resource URIs are logged (cut to 512
  characters) and, for subscriptions with a shared store, kept in the
  store: do not put secrets in URIs. Contents and prompt argument values are
  never logged.
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
  never names a tool's scope and reveals nothing about whether a tool exists.
  Like every handshake-era `403`, its challenge also lists the scopes the
  token already holds that this server checks (so a client asking again
  keeps them): a token that lacks `required_scopes` learns which of its own
  scopes the server uses, which `scopes_supported` does not show while
  step-up is on.
- **OAuth caches and the failed-token throttle are per process.** Each worker
  fetches its own keys and counts failures on its own; behind a proxy, all
  clients share the proxy's address for that throttle, as for anonymous rate
  limiting.
- **No CORS headers yet.** A browser-based client on another origin cannot
  read `WWW-Authenticate` or the metadata cross-origin; clients behind a proxy
  are not affected. The README's "Not in 0.3.2" shows a workaround with
  Starlette's `CORSMiddleware`; list exactly the origins you trust, in it and
  in `allowed_origins`.
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
  method leave no audit event. With `metrics=True`,
  `easy_mcp_rejected_total` counts the invalid credentials and oversized
  bodies among them (and unknown methods get a span and a duration sample,
  as `_OTHER`); count the rest at your proxy if you need them.
- **Client trace context is trusted for parenting.** A client that sends
  `traceparent` in `_meta` (or, over HTTP, as a header) chooses the trace its
  server spans join, and under the SDK's default parent-based sampler,
  whether they are sampled; spans carry `easy_mcp.client_id`, so who did it
  stays visible. Client baggage, from `_meta` or (over HTTP) a `baggage`
  header, reaches tool code and anything it calls with an instrumented
  client. Use `ParentBased(remote_parent_sampled=...)` to cap sampling and
  `OTEL_PROPAGATORS=tracecontext` to drop client baggage. A malformed value
  is ignored, never echoed, and never an error. Log lines carry the
  `trace_id` and `span_id` only of a span this process made: with
  no SDK configured they carry none, so a client's own ids never appear in
  the audit log as the server's.
- **A hidden tool's call leaves no audit event.** A call of a protected tool
  the caller cannot see is answered "Unknown tool", as for a missing one, and
  audited nowhere (the audit log's `tool_denied` events are unchanged in
  0.3.2); with telemetry it is counted as `easy_mcp_status="unauthorized"`
  and its span names the tool, so probing for protected tools shows there.
- **Metrics are per process.** With several workers behind one port, a
  scrape reaches one of them; `easy_mcp_sessions_open` can go negative on a
  worker that ends a session another opened (sum it across workers). With a
  shared store, an HTTP session that expires idle is counted closed but gets
  no `mcp_server_session_duration_seconds` sample: its record, with the time
  it opened, lapsed with its TTL.
- **A proxy can open `/metrics` without auth.** A request carrying a
  forwarding header (`Forwarded`, `X-Forwarded-For`, `X-Forwarded-Host`,
  `X-Real-IP`) is refused, but a reverse proxy on the same machine that
  sends none connects from loopback, and one that also rewrites `Host` to
  `localhost` passes the local-only check for anyone who reaches it. And
  uvicorn's proxy-header trust (`--proxy-headers` with
  `--forwarded-allow-ips` or `FORWARDED_ALLOW_IPS`, `*` above all) makes the
  peer address whatever the client's `X-Forwarded-For` says, wherever the
  proxy runs. Configure auth on any server behind a proxy, local or remote,
  before it serves `/metrics`.
- **A scrape key is a key.** One holding only `metrics:read` still
  authenticates on `/mcp`: it can call public tools and `requires_auth` tools
  that name no scopes. Give the tools a scraper must not reach scopes.
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
- [ ] Resource functions that touch files use `safe_path()`.
- [ ] Completers return only what every caller allowed to see their prompt or template may see.
- [ ] Middleware that calls other services bounds each call with a timeout and exports no arguments, results or credentials without review.
- [ ] Multiple workers: a shared store, TLS to it (`rediss://`), a dedicated ACL user, `noeviction`, and a distinct server `name` (or `namespace=`) per server sharing it.
- [ ] If `/metrics` is on: the scraper has its own key holding only `metrics:read`, tools it must not call have scopes, and behind any proxy, local or remote (or with uvicorn's `--proxy-headers`/`FORWARDED_ALLOW_IPS`), auth is configured.
- [ ] Trace and metric exporters use batch/periodic processors; over stdio nothing writes to stdout (`ConsoleSpanExporter(out=sys.stderr)`).
- [ ] Decide whether client trace context may choose your sampling (`ParentBased(remote_parent_sampled=...)`) and carry baggage into tools (`OTEL_PROPAGATORS=tracecontext` drops it).

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
