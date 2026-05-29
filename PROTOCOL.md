# Inter-Agent Bus — v1.0

Peer-to-peer mid-stream messaging between local Claude Code and Codex CLI sessions.

**Audience routing — pick one:**

- **Operator / user trying this out** → [`USAGE.md`](USAGE.md). Quickstart, testing, troubleshooting, two-codex walkthrough, common ops.
- **Future Claude/Codex session trying to coordinate with another live session** → use the local skill first, then [`USAGE.md`](USAGE.md) if needed.
  Claude: `/message`
  Codex: `~/.codex/skills/agent-bus/SKILL.md`
- **Implementer working on the bus itself** → continue here. This file documents the protocol, adapter contract, and permission model.

## Future session quick route

If you are a fresh Claude or Codex session and the user wants live agent-to-agent messaging, do this:

```sh
bus status
bus enable --name "task-short-name" --description "What this session is doing"
bus whoami
```

Then send as the live session:

```sh
bus send --from-session <peer-address> "message"
```

Important defaults:

- Normal wrapped `claude` / `codex` sessions are bus-capable from launch.
- Codex Desktop sessions are bus-enabled by default through `com.agentbus.codex-desktop-bus-autostart`; if the watcher is not running, the CLI starts an enabled Desktop control sidecar on demand when `CODEX_THREAD_ID` is present.
- Wrapped terminal sessions are **not listed** until they opt in with `bus enable`.
- The chosen `--name` becomes the actual bus address peers should message.
- If `bus list` still shows only stale hashed ids after a code update, restart the per-user broker:
  `launchctl kickstart -k gui/$(id -u)/com.agentbus.bus-broker`

## What's here

- `broker/broker.js` — persistent Unix-socket daemon. Per-user; run one.
- `client/client.js` — Node client library with auto-reconnect and silent re-register.
- `client/client.py` — Python client mirror used by the Codex adapter.
- `cli/bus` — operator CLI (`broker`, `list`, `status`, `whoami`, `enable`, `disable`, `send`, plus `send --from-session` for live participant-originated sends). `enable` also accepts `--name` and `--description`; the chosen `--name` becomes the session's actual messageable bus address.
- `adapters/claude-pty/` — Claude PTY wrapper + sidecar.
- `adapters/codex-pty/` — Codex PTY wrapper + sidecar; the primary interactive Codex path.
- `adapters/codex-desktop/` — Codex Desktop control sidecar and autostart watcher for `CODEX_THREAD_ID` / recent Desktop threads.
- `adapters/codex-app-server/` — Codex adapter, policy shim helpers, wrapper integration helpers.
- `tests/broker-smoke.test.js` — end-to-end smoke test proving register/list/send/receive + mailbox + lease rejection + socket perms.
- `adapters/claude-pty/contract-tests/adapter-contract.test.js` — Claude adapter contract tests.
- `adapters/codex-pty/contract-tests/adapter-contract.test.js` — Codex PTY adapter contract tests.
- `adapters/codex-app-server/contract-tests/adapter_contract_test.py` — Codex adapter contract tests.
- `adapters/codex-app-server/contract-tests/wrapper_integration_test.sh` — normal-wrapper integration test.

## Still open / next steps

- MCP server (`mcp/server.js`) for first-class `bus.send` / `bus.list` tools inside Claude and Codex sessions. Today the pragmatic in-session send surface is `bus send --from-session`.
- Fast-ack legacy Codex app-server delivery path (`wait_for_completion=False`) so the fallback non-PTY path can drop back below 5 s.
- Additional real-session acceptance fixtures for mid-stream / restart scenarios.

## Running

```bash
# Start the broker (per-user daemon)
node broker/broker.js

# In another shell: smoke tests
npm test
```

State locations (per canonical constraint #4: artifact-first continuity):

- Socket: `~/.agent-bus/broker.sock` (mode 0600)
- Message log: `~/.agent-bus/log.jsonl` (append-only; no rotation yet)
- Registry snapshot: `~/.agent-bus/registry.json` (never contains lease tokens)
- Mailbox: `~/.agent-bus/mailbox/<sha256(sessionId)>.jsonl` (TTL 5 min)

Override via `AGENT_BUS_STATE_DIR` / `AGENT_BUS_SOCKET`. If `AGENT_BUS_SOCKET` is unset, the JS and Python clients now derive the default socket as `<AGENT_BUS_STATE_DIR>/broker.sock`, which keeps isolated debug brokers from accidentally talking to the global daemon.

## Client usage (Node)

```js
const { BusClient } = require('./client/client');

const client = new BusClient();
const { participantId } = await client.register({
  kind: 'claude',
  sessionId: 'session-abc',
  displayName: 'claude-reviewer',
  project: 'agent-bus',
  cwd: process.cwd(),
  delivery: {
    adapter: 'claude-pty',
    socketPath: '/path/to/my/adapter.sock',  // where the adapter accepts deliveries
  },
});

const { participants } = await client.list();
const result = await client.send('codex-architecture', 'hello peer');
// result.status ∈ { 'delivered', 'queued' }

await client.unregister();
```

The client auto-heartbeats at the interval the broker returns (default 20 s) and auto-reconnects with exponential backoff capped at 5 s on broker loss, re-registering silently — no user prompting under any branch. Matches canonical constraint #1 (minimal user friction).

## Adapter socket contract

Each registered participant must expose a Unix socket at `delivery.socketPath` that accepts newline-delimited JSON deliveries. The `auth` field carries the per-session capability token (see Permission model); the broker sets it from the token the adapter registered. A delivery without the matching token is `rejected: unauthorized`.

Request:
```json
{"type": "deliver", "auth": "<per-session-token>", "envelope": {"id": "msg_...", "from": "claude:abcd", "to": "codex:efgh", "body": "...", "reply_to": null, "conversation_id": null, "source": "participant", "sent_at": "..."}}
```

Response:
```json
{"status": "accepted"}
```
or
```json
{"status": "rejected", "reason": "..."}
```

Adapters MUST render the envelope to a sender-tagged user-visible string before injecting. The rendered form is the adapter's responsibility, not the protocol's.

PTY-backed adapters (`claude-pty`, `codex-pty`) expose two local Unix sockets:

- an always-on **control socket** created when the wrapped session launches
- an **adapter delivery socket** that exists only while the session is bus-enabled

The control socket accepts:

```json
{"type": "status"}
{"type": "enable", "auth": "<per-session-token>", "displayName": "claude-lead", "description": "Coordinates work and asks Codex code questions"}
{"type": "disable", "auth": "<per-session-token>"}
{"type": "whoami", "auth": "<per-session-token>"}
```

That is what backs `bus status`, `bus enable`, `bus disable`, and `bus whoami`. `enable`, `disable`, and `whoami` require the per-session control token (the CLI reads it from the `0600` token file automatically); `status` is an unauthenticated liveness probe. Wrapped sessions are bus-capable from launch, but are not listed until `enable` is called (unless `AGENT_BUS_AUTO_ENABLE=1` is explicitly set). In practice, agents should pick a short task-specific slug like `codex-architecture` or `claude-reviewer` when they opt in, because that name becomes the actual bus address that peers message.

The enabled delivery socket also accepts a local request used by `bus send --from-session`:

```json
{"type": "send", "auth": "<per-session-token>", "to": "codex:efgh", "body": "hello peer", "reply_to": null, "conversation_id": null, "source": "participant"}
```

That request is handled by the live sidecar, which forwards the push through its already-registered broker lease. It carries the per-session capability token (the CLI reads it from the `0600` token file); a `send` without the token is `rejected: unauthorized`. Result: the broker stamps `from` as the live participant's chosen bus address (for example `claude-reviewer` / `codex-architecture`), not as an ephemeral `cli:...`.

## Protocol wire format

Newline-delimited JSON over Unix socket. Each client request gets exactly one response. Requests on the same connection are processed serially by the broker to preserve order.

## Permission model (v1)

- **Connection is not authority.** Filesystem mode (`~/.agent-bus/` `0700`, sockets `0600`) keeps *other* UIDs out, but does nothing against a *same-UID* peer — every other CLI agent you run shares your UID. So the privileged per-session sockets no longer treat "I connected" as "I am authorized":
  - The **adapter delivery socket** ({deliver} writes-and-autosubmits a turn into the live agent PTY; {send} emits a message as that participant) requires a per-session **capability token**. The owning wrapper mints a 256-bit token at startup, writes it to a `0600` file (`<sessionId>.token`) next to the socket, and hands it to the broker at register. The broker presents it on every {deliver}; the local CLI reads the `0600` file for {send}. Comparison is constant-time. A same-UID peer that connects without the token gets `rejected: unauthorized`.
  - The **control socket** (`enable` / `disable` / `whoami`) requires the same kind of per-session token. `status` stays unauthenticated as a read-only liveness probe.
- A per-connection peer-UID check (`SO_PEERCRED` / `LOCAL_PEERCRED`) would be strictly additive but needs native code on macOS, and would not close the same-UID case anyway. It is left as a flagged follow-up; the capability token is the load-bearing same-UID defense — it raises the bar from "connecting is authority" to "holding the per-session secret." It is not absolute: a same-UID process that can already read your files can read the `0600` token; only OS-level process isolation would close that, which the bus does not attempt.
- Per-registration `leaseToken` (16 random bytes, 128 bits) authenticates `heartbeat`, `push_message`, and `unregister_participant`. Broker stamps `from` on every push from the sender's lease; a sender cannot forge another's identity even by setting `from` in the envelope or by guessing a participant ID.
- `list_participants` never exposes `delivery.socketPath`, `delivery.threadId`, or the `delivery.adapterToken`. None are persisted in `registry.json` either — only `delivery.adapter` is written to disk.
- `push_message.to` is strictly validated against the participant-id grammar: either a human handle like `codex-architecture` / `claude-reviewer`, or the legacy hashed form `kind:deadbeefcafe`. Malformed IDs are rejected before any mailbox side effect. Mailbox files on disk are named `<sha256(sessionId)>.jsonl`; the wire-facing participantId is never a filename component.
- Mailbox queueing is scoped to participants the broker has observed register within the last 10 minutes ("recently open/discoverable" semantics). Pushes to never-seen IDs return `failed: no_such_participant`.
- **Reclaim refuses a live lease.** `sessionId` is not a peer secret (it is kept out of `list_participants` and `registry.json`, but can still be observed or guessed). Re-register reuses the original `participantId` only when the prior lease is no longer live (clean unregister, broker restart, or lease expiry) — preserving identity continuity across silent auto-reregister without user-visible ID churn. Re-registering a session whose lease is still live is rejected with `session_lease_active`, so a peer that learns a victim's `sessionId` cannot hijack its identity or invalidate its lease.
- Broker startup probes the socket path. If another broker is alive there, it refuses to start with exit code 2. Only a stale socket (no live listener) is unlinked.

Under single-user local trust, this is sufficient.
