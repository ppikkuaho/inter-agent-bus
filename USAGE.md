# Inter-Agent Bus — Usage

Operator walkthrough.

- [`PROTOCOL.md`](PROTOCOL.md) — implementer surface (protocol, adapter contract, permission model).
- [`README.md`](README.md) — overview and design highlights.

## Future sessions — start here

If a future Claude or Codex session needs to coordinate with another live session, this is the shortest correct path:

1. Use the normal wrapped launch path:
   - `claude --dangerously-skip-permissions`
   - `codex`
2. Inside the live session:
   ```sh
   bus status
   bus enable --name "task-short-name" --description "What this session is doing"
   bus whoami
   ```
3. Send as the live session:
   ```sh
   bus send --from-session <peer-address> "message"
   ```

The contract is exit-based: `bus send` exits `0` only when delivery to the live adapter is confirmed. If the CLI reports `queued`, the peer did not receive the message now, and the command exits non-zero unless you explicitly pass `--allow-queued`.

Remember:

- Sessions are bus-capable on normal launch but **not listed** until `bus enable`.
- The chosen `--name` becomes the real messageable address.
- Claude should load `/message` for this workflow.
- Codex should use the `agent-bus` skill at `~/.codex/skills/agent-bus/SKILL.md`.

## Mental model in 30 seconds

```
 Claude session  ─pty─▶ claude-pty adapter ─┐
 Codex session   ─pty─▶ codex-pty adapter  ─┼──────▶ broker ◀──── Unix socket ──── CLI / peer clients
                                            │
                                            └──────▶ legacy codex-app-server adapter (fallback / non-PTY)
                                   registry + mailbox + log
```

- **Broker** is one per-user daemon. Everyone else registers with it over a Unix socket.
- **Adapters** attach to a real recipient (Claude PTY, Codex thread) and own a small delivery socket. When the broker forwards a message, the adapter renders the envelope as a sender-tagged line and injects it as a user turn.
- **Clients** (the CLI, an MCP tool, another adapter) register their presence, push messages through the broker, and the broker stamps the `from` field from the sender's lease token so identity can't be forged.

Everything else — lease lifetimes, mailbox for brief offline windows, dedupe, FIFO at reattach — is invariant you can rely on without thinking about.

## Prerequisites

- Node ≥ 18 (`/opt/homebrew/bin/node` in this setup).
- `node-pty` is required for the PTY-backed wrappers (`claude-pty`, `codex-pty`). Install once:
  ```sh
  cd ~/inter-agent-bus
  npm install
  ```
  If `node-pty` fails to build on 1.1.x, downgrade:
  ```sh
  npm install node-pty@1.0.0 --save-optional
  ```
- The broker's state dir is `~/.agent-bus/` (mode 0700). It creates itself on first run.
- `bus` is available on this machine via `~/bin/bus`, which execs `cli/bus`. You can still use the repo-local path if you prefer.

## Quickstart — Claude-inbound (the working path)

Three terminals. Copy-paste friendly.

### Terminal 1 — start the broker

```sh
cd ~/inter-agent-bus
cli/bus broker
# ->  [broker] listening on ~/.agent-bus/broker.sock
#     [broker] state dir: ~/.agent-bus
```

Leave it running. If you close this terminal the broker dies and registered adapters will silently re-register when it comes back up.

### Terminal 2 — launch a Claude session via the bus wrapper

```sh
cd ~/inter-agent-bus   # Claude trusts this cwd; arbitrary cwds hit a "trust this workspace?" prompt
node adapters/claude-pty/wrapper.js
# Claude launches as usual. The session is bus-capable immediately, but not listed
# until you opt in with `bus enable`.
```

Inside that Claude session, run:

```sh
bus enable --name "claude-lead" --description "Coordinates work and asks Codex code questions"
```

Override the Claude launcher if your installation differs:
```sh
AGENT_BUS_CLAUDE_CMD=/path/to/claude node adapters/claude-pty/wrapper.js
```

### Terminal 3 — opt in, find the session, send a message

```sh
cli/bus list
# claude-lead              adapter=claude-pty        project=agent-bus  last=...
                            desc=Coordinates work and asks Codex code questions

cli/bus send claude-lead "Say PING and nothing else"
# delivered  from=cli:0011223344aa
```

Look at Terminal 2. Claude's prompt line now shows the rendered envelope, the turn submits after ~80 ms, and Claude replies.

That's the default surface now: wrapped Claude and Codex sessions are bus-capable on normal launch, but they are not listed until they explicitly opt in with `bus enable`.

## On-demand session control

Wrapped Claude and Codex sessions always expose a local control socket, even before they are bus-enabled. That gives you explicit lifecycle commands:

```sh
bus status
bus enable --name "short-name" --description "What this session is for"
bus whoami
bus disable
```

Inside a wrapped session those commands resolve the current session automatically through `AGENT_BUS_SESSION_ID` / `CODEX_RUNTIME_SESSION_ID`.

Inside Codex Desktop, tool shells expose `CODEX_THREAD_ID` instead of a wrapper runtime id. A per-user LaunchAgent (`com.agentbus.codex-desktop-bus-autostart`) watches recent Desktop threads and starts enabled Desktop sidecars by default. The CLI also derives a `codex-desktop-<thread-id>` session id and starts an enabled sidecar on demand if the watcher has not seen the thread yet.

```sh
bus status
bus whoami
bus send --from-session <peer-address> "message"
```

Use `bus enable --name ... --description ...` only when you want to rename the default Desktop address (`codex-desktop-<thread-id>`) or fix a disabled/error state.

For debugging from another terminal, pass the session id explicitly:

```sh
bus status --session-id codex-ondemand-20260419b
bus enable --session-id codex-ondemand-20260419b
bus whoami --session-id codex-ondemand-20260419b
bus disable --session-id codex-ondemand-20260419b
```

This is now the normal behavior for wrapped sessions. If you explicitly want the old always-listed behavior for a terminal-launched test or temporary automation, launch with:

```sh
AGENT_BUS_AUTO_ENABLE=1 codex
# or
AGENT_BUS_AUTO_ENABLE=1 claude --dangerously-skip-permissions
```

What each command does:

- `bus status` — shows whether the session is enabled, its session id, chosen bus address if enabled, adapter type, log path, and any last error.
- `bus enable` — starts/registers the live sidecar. `--name` is the actual messageable bus address, not just a cosmetic label. Use short task-specific handles such as `codex-architecture` or `claude-reviewer`, plus `--description` for the longer explanation. For Codex, if the session has no real thread yet, it returns `awaiting_thread` instead of failing quietly.
- `bus whoami` — prints only the live participant id, which is the chosen address peers should message.
- `bus disable` — unregisters the live participant but keeps the control socket alive, so you can re-enable later.

### Agent knowledge surfaces

If you wire the bus into your agents as a loadable skill, give each side a short prompt that teaches the same practical v1 flow: `bus status` -> `bus enable --name ... --description ...` if needed -> `bus whoami` -> `bus send --from-session ...`. For Codex, the conventional install location is `~/.codex/skills/agent-bus/SKILL.md`.

If you run isolated broker/debug sessions, setting only `AGENT_BUS_STATE_DIR` is now enough for the CLI and both client libraries to derive the matching `broker.sock`. You no longer need to set `AGENT_BUS_SOCKET` separately unless you want a nonstandard socket filename.

## Testing it end-to-end

Fastest sanity checks in increasing depth:

### 1. "Does the broker start?"
```sh
cli/bus broker  # leave running
# in another terminal:
cli/bus list    # -> (no participants registered)
```

### 2. "Does a real Claude session receive a bus message?"
Follow Quickstart above. Expected end state: Claude's response in Terminal 2 includes the string you sent in Terminal 3.

### 3. "Do all contract tests still pass?"
```sh
cd ~/inter-agent-bus
npm test
# -> broker smoke + claude-pty + codex-pty pass, exit 0

PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 tests/python_client_smoke_test.py -v
# -> 3/3 pass

PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 adapters/codex-app-server/contract-tests/adapter_contract_test.py -v
# -> 6/6 pass

zsh adapters/codex-app-server/contract-tests/wrapper_integration_test.sh
# -> 6/6 pass
```

## Session send / reply surface (v1)

The v1 agent-usable send surface is the existing CLI with `--from-session`:

```sh
bus send --from-session codex-architecture "hello from this live session"
```

When that command runs inside a wrapped Claude or Codex session, the CLI talks to the session's local sidecar socket and sends through the already-registered live participant lease. The broker therefore records the real sender identity:

```sh
delivered  from=claude-reviewer
# or
delivered  from=codex-architecture
```

If the broker can only queue the message, the CLI now surfaces that as a nondelivery by default:

```sh
queued (recipient_offline)  from=claude-reviewer
not delivered; recipient is offline or unavailable. Re-run with --allow-queued only if mailbox queueing is acceptable.
```

Use `--allow-queued` only when mailbox delivery later is genuinely acceptable:

```sh
bus send --from-session --allow-queued codex-architecture "message"
```

This is the current v1 reply surface for agent-to-agent messaging. The target should usually be the peer's chosen `--name` from `bus whoami`, not a random hash.

If the session is wrapped but not enabled, `bus send --from-session ...` now fails loudly with a concrete message telling you to run `bus enable` instead of surfacing a bare missing-socket error.

## Codex-inbound — how it works today

Codex delivery works on the live session surface through the PTY-backed path.

The normal `~/bin/codex` entrypoint now uses `adapters/codex-pty/wrapper.js` for interactive TTY launches. That wrapper:

1. Assigns a `runtime_session_id` up front.
2. Records `approval_mode`, `sandbox_mode`, and wrapper overrides into the per-session active-terminal record.
3. Owns the live Codex PTY directly.
4. Starts a control socket immediately for `bus status|enable|disable|whoami`.
5. Starts the `codex-pty` sidecar once the session's first `thread_id` exists and the session is bus-enabled.
6. Injects inbound bus turns into that same live PTY and exposes the same sidecar socket for `bus send --from-session`.

Net result today: launch normal `codex` and you get wrapper-owned control immediately; the session is not listed by default, and you opt it in with `bus enable`. If you explicitly want legacy always-listed behavior for a test, you can still launch with `AGENT_BUS_AUTO_ENABLE=1`.

Codex Desktop is not PTY-wrapped, so the LaunchAgent / CLI maps `CODEX_THREAD_ID` or recent Desktop thread rows to a `codex-desktop-<thread-id>` session and runs `adapters/codex-desktop/control.py` as the control sidecar. That sidecar uses `adapters/codex-app-server/` for delivery and session-originated sends.

The older `adapters/codex-app-server/` path remains in-tree as a legacy fallback for non-PTY contexts, Desktop support, and contract coverage, but it is no longer the primary operator path for terminal-launched interactive sessions.

## Two Codex sessions (Codex ↔ Codex)

**Verified working 2026-04-17** by a scripted two-Codex run (probe routing isolated, markers landed in the correct rollouts, no cross-contamination). What follows is the manual walkthrough for two interactive Codex sessions, which is the mechanical extrapolation of what that run verified.

### Required for each session (normal path)

- Broker running.
- Codex session alive with an assigned `thread_id` (i.e., you've submitted at least one prompt in the session, so Codex has created a thread).
- Session launched through the normal `~/bin/codex` wrapper (this is the standard `codex` entrypoint on this machine).

### Walkthrough (easy path — normal `codex`)

The normal `~/bin/codex` wrapper now gives you bus capability automatically, but not discoverability. Each session needs to opt in with `bus enable` before it appears in `bus list`.

Four terminals. T1 is the broker; T2 and T3 are your two normal `codex` sessions; T4 is the control terminal.

```sh
# T1 — broker
cd ~/inter-agent-bus
cli/bus broker

# T2 — Codex session A
cd ~/inter-agent-bus
codex
# Submit any short prompt so Codex creates a thread. Then run:
# bus enable --name "codex-a" --description "What this session is for"

# T3 — Codex session B
# Same as T2. Each invocation is an independent session with its own runtime_session_id.

# T4 — see them, send between them
cli/bus list
# Two human-readable names like codex-a / codex-b expected.

cli/bus send codex-b "Hi from outside. Reply PING_B."
# delivered
```

To check delivery, either look at B's live TUI (the `codex-pty` wrapper owns the live PTY and injects directly there) or tail the rollout:

```sh
# T4, find B's rollout and tail
B_THREAD_ID=$(jq -r .thread_id < ~/.codex/runtime/active-threads/<B-session-id>.json)
tail -20 $(find ~/.codex/sessions -name "*$B_THREAD_ID*.jsonl" | head -1)
```

When you quit a Codex session, the wrapper-owned supervisor sees the process exit and stops the adapter cleanly. No manual cleanup.

**Verified** by:
- `adapters/codex-pty/contract-tests/adapter-contract.test.js` (6/6 PASS) — proves live PTY delivery + live participant-originated sends.
- `adapters/codex-app-server/contract-tests/wrapper_integration_test.sh` (6/6 PASS) — proves policy capture and legacy non-PTY adapter integration still behave.
- a captured fresh live `Claude ↔ Codex` bidirectional run (PASS) — proves bidirectional messaging with sender identities stamped by the live session address rather than ephemeral `cli:...` senders.

### Walkthrough (manual fallback — if you can't use the normal wrapper path)

Four terminals. T1 is the broker; T2 and T3 are your two Codex sessions; T4 is the control terminal where you run the setup and `bus send`.

**T1 — broker:**
```sh
cd ~/inter-agent-bus
cli/bus broker
```

**T2 — Codex session A:**
```sh
cd ~/inter-agent-bus
codex
# Submit any short prompt (e.g. "hi"). This causes Codex to create a thread and
# triggers track-active-thread to record a thread_id for this session.
```

**T3 — Codex session B:**
Same as T2 (fresh directory or same cwd — unique `runtime_session_id` per invocation).

**T4 — identify, patch, and start adapters:**
```sh
cd ~/inter-agent-bus

# The two most recent active-terminal records are your A and B sessions.
# `ls -t` orders newest first.
A_ID=$(ls -t ~/.codex/runtime/active-terminals/*.json | sed -n '2p' | xargs basename | sed 's/.json//')
B_ID=$(ls -t ~/.codex/runtime/active-terminals/*.json | sed -n '1p' | xargs basename | sed 's/.json//')
echo "A=$A_ID"
echo "B=$B_ID"

# Confirm each has a thread_id (not "awaiting_thread"). If either shows
# "awaiting_thread", submit another prompt in that codex session.
cat ~/.codex/runtime/active-threads/$A_ID.json
cat ~/.codex/runtime/active-threads/$B_ID.json

# Patch each active-terminal record with explicit, non-escalating policy.
# This is only needed if the session was launched outside the normal wrapper path.
for S in $A_ID $B_ID; do
  /usr/bin/python3 -c "
import json, pathlib
p = pathlib.Path('$HOME/.codex/runtime/active-terminals/$S.json')
d = json.loads(p.read_text())
d['approval_mode'] = 'on-request'
d['sandbox_mode'] = 'workspace-write'
d.setdefault('wrapper_overrides', {})['source'] = 'two-codex-walkthrough'
p.write_text(json.dumps(d, indent=2))
print(f'patched {p.name}')
"
done

# Launch one adapter per session. Keep these terminals open (or use &).
AGENT_BUS_RUNTIME_SESSION_ID=$A_ID \
  /usr/bin/python3 adapters/codex-app-server/adapter.py &
ADAPTER_A=$!

AGENT_BUS_RUNTIME_SESSION_ID=$B_ID \
  /usr/bin/python3 adapters/codex-app-server/adapter.py &
ADAPTER_B=$!

# Verify both registered.
sleep 1
cli/bus list
# Expect two bus addresses (or legacy hashed rows if you skipped naming).
```

**T4 — cross-send:**
```sh
# Pick either of the codex participantIds listed above. Let's say B is codex-b.
cli/bus send codex-b "Hello from outside — please reply with PING_B."
# delivered  from=cli:...

# See the response land in B's rollout:
tail -20 $(find ~/.codex/sessions -name "*$(jq -r .thread_id < ~/.codex/runtime/active-threads/$B_ID.json)*.jsonl" | head -1)
# Look for: "role":"user" ... [from:cli:... · msg:... · HH:MMZ] Hello from outside ...
#    and:   "role":"assistant" ... PING_B
```

**Two codex sessions talking to each other** — give each one a bus-send command and the other's participantId. At a human prompt in Codex A:
```
You are Codex session A. Codex session B is at participantId codex-b.
To send B a message, run exactly:
  ~/inter-agent-bus/cli/bus send --from-session codex-b "your message"
Start by asking B what it's working on.
```

A's LLM will invoke the shell command, the message arrives in B's rollout as a user turn, B's LLM responds, and — if B is also told how to call `bus send --from-session` with A's id — it sends back with its own chosen bus name identity.

**Cleanup when done:**
```sh
kill $ADAPTER_A $ADAPTER_B
# Close the codex terminals (Ctrl+C in T2/T3). Broker handles unregister timeouts.
```

### Known limitations

- The Codex adapter now prefers **live terminal-input delivery** into the bound TTY, so an active Codex TUI should receive the turn on the same visible surface. If terminal injection is unavailable or blocked, the adapter falls back to **policy-safe thread delivery**, which updates the rollout reliably but may require `codex resume <thread>` to view the injected turn.
- `bus send` to a Codex session returns `delivered` iff the P3 helper returns within the broker's 30 s forward timeout. On slow Codex responses, you may see `queued` even when the injection succeeded — check the rollout to confirm. Fix tracked as v1.1 (switch adapter to fast-ack mode once P3 supports `wait_for_completion=False`).
- Two Codex sessions in the SAME terminal via tmux/iTerm split works the same way — each session has its own `runtime_session_id`.

## Common ops

### Send with a threaded reply
```sh
cli/bus send --reply-to msg_abc123... codex-architecture "following up on that"
```

### Send as the current live session
```sh
cli/bus send --from-session codex-architecture "hello from this Claude/Codex session"
```

If you run that inside a wrapped live session, the broker records the real sender as that session's chosen bus address from `bus whoami`.

### See what's flowing
```sh
tail -f ~/.agent-bus/log.jsonl
# every register, heartbeat, push, flush, unregister is a JSON line
```

### Force the broker to restart
```sh
pkill -f 'node.*broker/broker.js'
# registered adapters auto-reconnect silently within ~20 s (no user action).
```

### Clear everything
```sh
pkill -f 'node.*broker/broker.js'
rm -rf ~/.agent-bus/mailbox/*
rm -f ~/.agent-bus/registry.json
# then restart the broker.
```

## Troubleshooting

| Symptom | Likely cause | Check / fix |
|---|---|---|
| `bus list` says `(no participants registered)` but a Claude wrapper is running | Wrapper couldn't spawn node-pty, OR the broker isn't running, OR the wrapper connected to a different socket | In the wrapper terminal: look for `[claude-pty] failed to spawn ...`. Also check `echo $AGENT_BUS_SOCKET` matches broker's `listening on` line. |
| Envelope renders in Claude's prompt but never submits | Downgraded sidecar, or you're on an older build | Check `adapters/claude-pty/sidecar.js` for the 80 ms Enter-delay; it's the fix for this. |
| `bus send` returns `queued` for every message to a legacy Codex session | The fallback `codex-app-server` path is waiting for `turn.completed` on a slow thread | Normal on slow responses — queued means the broker couldn't confirm delivery within `FORWARD_TIMEOUT_MS` (30 s). The message almost certainly landed; check the rollout JSONL. This does not affect the primary `codex-pty` live path. |
| `bus send` returns `failed reason=no_such_participant` | The recipient never registered, OR registered >10 min ago and the broker GC'd them from the eligibility window | `bus list` first. If they SHOULD be there, confirm their adapter process is alive. |
| `bus list` shows only old hashed ids, or names you expect are missing even though sessions enabled with `--name` | You are talking to an older still-running broker process on `~/.agent-bus/broker.sock` | Restart the launchd broker: `launchctl kickstart -k gui/$(id -u)/com.agentbus.bus-broker`, then re-check `bus list`. |
| `bus send` returns `rejected reason=policy_unknown…` | Legacy `codex-app-server` recipient session has no captured policy metadata | Hand-patch the active-terminal record (`~/.codex/runtime/active-terminals/<session>.json`) with explicit `approval_mode` + `sandbox_mode`, or use the normal interactive `codex` PTY path instead. |
| `bus send --from-session` says no session id is available | The command is running outside a wrapped Claude/Codex session | Run it inside the live wrapped session, or pass `--session-id <id>` explicitly. |
| Broker refuses to start with exit code 2 | Another broker is already live on the same socket path | `ps aux \| grep broker.js` — use the running one or kill it before restarting. |
| `listen EINVAL` when running a test that uses `os.tmpdir()` | macOS `AF_UNIX` socket path >104 bytes | Use `/tmp/*` for adapter-socket dirs in tests, not `os.tmpdir()` (`/var/folders/ts/...` eats 70 bytes). |

## When to update this doc

Every time the operator story changes. Examples:

- A new CLI subcommand ships.
- A prerequisite install step changes.
- A new troubleshooting symptom becomes common.
- The primary live Claude/Codex operator path changes again.
- A new adapter kind joins the bus.

`USAGE.md` is the operator's reference. `PROTOCOL.md` is the implementer's. `README.md` is the overview. Keep all three in sync with reality.
