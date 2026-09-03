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
| Credential stuffing / key probing | Constant-time key comparison over the full key set (`hmac.compare_digest`); timing does not reveal partial matches |
| Key leakage via logs | Raw keys never logged; only SHA-256 fingerprints appear in logs and audit events |
| Unauthorized tool use | Per-tool `requires_auth` and scope checks; protected tools are omitted from `tools/list` and report as unknown to unauthorized callers (no enumeration) |
| Session hijacking | Session ids are 192-bit random capability tokens; every POST must present the same credential the session was opened with (403 otherwise) |
| Malformed / hostile input | Strict schema validation: unknown fields rejected, types enforced (bool ≠ int), required params enforced, before any tool code runs |
| Oversized payloads | `max_request_bytes` enforced on the Content-Length header *and* while streaming the body (a lying header does not help) |
| Request flooding | Per-client sliding-window rate limiting on every method, including discovery; concurrent session cap (`max_sessions`) |
| Resource exhaustion via slow tools | Per-tool and server-default timeouts; sync tools run off the event loop so they cannot stall other clients |
| Information disclosure | Production errors are opaque (`error_id` only); tracebacks stay in server logs; `debug=True` is loudly warned about at startup |
| Accidental exposure | Default bind is `127.0.0.1`; binding non-loopback without auth logs a warning at startup |
| Crash amplification | Exceptions in one tool call are contained; the server keeps serving |
| Protocol-stream corruption (stdio) | `sys.stdout` is redirected to stderr while serving, so tool `print()` calls cannot inject bytes into the JSON-RPC stream; oversized input lines are discarded unbuffered |
| Silent auth downgrade (stdio) | An invalid `EASY_MCP_STDIO_API_KEY` aborts startup instead of falling back to anonymous access |

## Transport trust boundaries

- **SSE (HTTP)** — clients are remote and untrusted; credentials arrive in
  headers, sessions are capability tokens, and every protection above applies.
- **stdio** — the client is the *parent process* that launched the server
  (Claude Desktop, Claude Code, an agent runtime). There is no network
  surface, but the parent is still treated as an MCP client: schema
  validation, scopes, rate limits, timeouts, payload caps, and error
  sanitization all apply unchanged. Protected tools stay hidden unless the
  parent presents a valid key via `EASY_MCP_STDIO_API_KEY`. Anything the
  parent can pass as environment it can also read, so a stdio key is a
  scoping mechanism, not a secret from the host itself.

## Known limitations (v0.1)

- **Sync tool timeouts are cooperative.** A timed-out or cancelled sync tool's
  worker thread cannot be force-killed by Python; the response is discarded
  but the thread runs to completion. Long-running work belongs in async tools
  or external workers.
- **No TLS.** Terminate TLS at a reverse proxy (Caddy, nginx, a cloud LB).
  API keys travel in headers and must not cross the network in plaintext.
- **Single-process sessions.** SSE sessions live in process memory; running
  multiple workers requires sticky routing (roadmap: shared session store).
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
| 0.1.x | ✅ |
