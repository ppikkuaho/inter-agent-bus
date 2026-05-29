# inter-agent-bus

> A local broker that lets two running CLI coding agents message each other mid-stream.

`v1.0` · 49 Node + 6 Python tests · live Claude↔Codex delivery verified

## What it is

inter-agent-bus is a per-user message broker that lets independently-running AI coding-CLI sessions (Claude Code, Codex CLI) exchange messages while they are live. A message sent to a session arrives as a user turn in that session.

The hard part: that message must land as a clean user turn even while the recipient's model is mid-generation, and it must wake an idle session into a new turn — with no human relaying anything by hand. The broker is a single Unix-socket daemon; each session attaches through a delivery adapter that owns the recipient's terminal and renders incoming messages as sender-tagged input. Sender identity is stamped by the broker from a per-registration lease, so a peer cannot forge who a message came from.

```
┌──────────┐        ┌────────┐        ┌──────────┐
│ claude-N │◄──────►│ broker │◄──────►│ codex-M  │
└──────────┘        └────────┘        └──────────┘
      │                  │                   │
  claude-pty       registry +           codex-pty /
   adapter        log + mailbox      codex-app-server
```

Extracted from a larger personal system; it runs standalone — the broker, both clients, the CLI, and the PTY adapters have no dependency on the parent system.

## Proof it works

Two fresh, live PTY-backed sessions — a real Claude Code session and a real Codex CLI session — messaged each other through the broker, with no operator relaying the text. The broker's append-only log records both deliveries:

```json
{"event":"push","from":"claude:e77e9b996efa","to":"codex:06b91144f3c2","messageId":"msg_4ea6028ba99abf51","result":"delivered"}
{"event":"push","from":"codex:06b91144f3c2","to":"claude:e77e9b996efa","messageId":"msg_310188336925780b","result":"delivered"}
```

and the message landed on the recipient's live session surface as a sender-tagged user turn:

```text
[from:claude:e77e9b996efa · msg:msg_4ea6028ba99abf51 · 09:17Z]
CLAUDE_TO_CODEX_OK
```

The two log lines above and the rendered envelope are excerpted from a captured end-to-end run: broker log, rendered PTY envelopes, and the recipient's session-rollout lines all confirmed the message landed as a user turn.

## Technical approach / highlights

- **Peer broker, not hub-and-spoke.** All parties are symmetric peers; "reply to the sender" is just a push in the other direction. The broker owns presence, leases, an append-only message log, and a narrow TTL-bound offline mailbox.
- **Lease-authenticated sends.** Each registration returns a 128-bit `leaseToken` that authenticates `heartbeat`, `push_message`, and `unregister`. The broker stamps the `from` field on every delivered envelope from the sender's lease, so the `from` a sender writes into the wire is ignored.
- **Mirrored clients.** A Node client (`client/client.js`) and a Python client (`client/client.py`) speak the same newline-delimited-JSON-over-Unix-socket protocol, with the same auto-reconnect, exponential backoff capped at five seconds, and silent re-register on broker loss.
- **Adapters render to a real session.** The `claude-pty` and `codex-pty` adapters wrap a live agent in a PTY, expose a control socket and a delivery socket, and type each inbound envelope into the session as a user turn. A `codex-app-server` adapter delivers into a Codex thread over JSON-RPC for non-PTY contexts.
- **FIFO at the reattach boundary.** Live pushes and mailbox flushes for a recipient are serialized through a per-participant delivery chain, so a message queued while a peer was offline always precedes a message pushed after it reconnects. A deterministic smoke test exercises this interleaving under slow-adapter pressure.
- **Injection-boundary hardening.** Envelope `from` / `id` / `body` are stripped of carriage returns, line feeds, ANSI/CSI escapes, and other control characters before being typed into the live TTY, so a message body cannot smuggle an extra turn-submit or drive the terminal. The broker only ever dials a delivery socket bound to the trusted adapter-sockets directory and the registering session, and it never exposes a session's reclaim-keying `sessionId` over discovery.

Designed via an adversarial Claude/Codex review loop. The decision log covered lease-lifecycle tradeoffs, a self-found security finding where a 16-bit short participant ID was brute-force-collidable and got widened to 48 bits, and a 256-bit per-session capability token added so connection no longer implies authority.

## Status

Validated v1.0. The broker, both clients, the PTY adapters, the CLI, and the offline mailbox are implemented and exercised by an automated suite of 49 Node tests (`node --test`, including a 23-case broker smoke suite) and 6 Python client tests. End-to-end delivery between live Claude and Codex sessions is verified (see [Proof it works](#proof-it-works)). Forward scope includes first-class MCP `bus.send` / `bus.list` tools and a fast-acknowledge Codex delivery path.

## Run it

Requires Node ≥ 18. The PTY-backed wrappers additionally need `node-pty`, installed as an optional dependency.

```bash
# (from the repo root)
npm install          # optional; only needed for the PTY wrappers
npm test             # broker smoke + session-control CLI + both PTY adapter contract suites
python3 tests/python_client_smoke_test.py -v   # Python client against a scratch broker
```

To run the broker and send a message between two terminals:

```bash
# terminal 1 — the per-user broker
node broker/broker.js

# terminal 2 — launch a Claude session wrapped for the bus, then inside it:
node adapters/claude-pty/wrapper.js
#   bus enable --name "claude-lead" --description "what this session is doing"

# terminal 3 — discover and send
cli/bus list
cli/bus send claude-lead "Say PING and nothing else"
```

The operator walkthrough — quickstart, the two-Codex and Claude↔Codex flows, troubleshooting — is in [`USAGE.md`](USAGE.md). The protocol, adapter contract, and permission model are in [`PROTOCOL.md`](PROTOCOL.md).

## Repository layout

```
broker/broker.js         # per-user Unix-socket daemon
client/client.js|.py     # mirrored Node + Python clients
cli/bus                  # operator CLI
adapters/                # claude-pty, codex-pty, codex-app-server, codex-desktop, session-control
tests/                   # broker smoke + session-control + Python client
README.md / PROTOCOL.md / USAGE.md   # overview, protocol/contract, operator walkthrough
```

## Scope & trust boundary

This is single-user, same-machine infrastructure. The trust boundary is one logged-in user — the broker's state directory is mode `0700` and its socket is `0600`, which is what enforces "only this user can talk to the bus" on a platform where Node cannot read a Unix peer's UID without native code. It is not a network message queue, not multi-tenant, and does not attempt cross-user authentication. The clients and adapters are written against the Claude Code and Codex CLI injection surfaces, so the runnable surface is those two agents on macOS/Linux.

## Known limitations

- The `codex-app-server` and `codex-desktop` adapters integrate with an external Codex runtime module (`codex_runtime_ctl.py`) that is not part of this repository, so those paths require that runtime to be present. The broker, both clients, the CLI, and the PTY contract tests run without it.
- A peer-UID check (`SO_PEERCRED` / `LOCAL_PEERCRED`) is a flagged additive follow-up; it needs native code on macOS and would not change the same-UID trust boundary regardless.

## Notes

- State lives under `~/.agent-bus/` (socket, append-only `log.jsonl`, lease-token-free `registry.json` snapshot, per-session TTL mailbox). Override the root with `AGENT_BUS_STATE_DIR`.
- Licensed under the MIT License. See [`LICENSE`](LICENSE).
