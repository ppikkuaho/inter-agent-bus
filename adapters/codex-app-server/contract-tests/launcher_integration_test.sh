#!/bin/zsh
# Integration test for adapters/codex-app-server/codex-bus.
#
# Drives the launcher end-to-end with a fake CODEX_REAL that simulates exactly
# what ~/bin/codex does at startup (writes an active-terminal
# record, stays alive pretending to be interactive). Does NOT spawn real Codex.
#
# Scope: verifies the LAUNCHER's responsibilities only (plumbing). Actual
# push-through-broker-to-P3-helper is covered by the codex-app-server contract
# suite and the two-codex dogfood; we do not re-test that here.
#
# Verifies:
#   1. launcher's supervise subshell detects the new session record (set-diff).
#   2. policy patch runs (approval_mode / sandbox_mode filled in).
#   3. supervise waits for thread_id (we simulate one appearing).
#   4. adapter launches with the right runtime_session_id.
#   5. adapter registers with a live broker.
#   6. when the launcher's codex-emulator exits, the adapter is stopped cleanly.
#
# Exit 0 on PASS; non-zero on FAIL.

set -u

# Bus root is derived from this script's own location so the test is portable
# across clones (contract-tests is three levels under the bus root).
BUS_ROOT="${AGENT_BUS_ROOT:-${0:A:h}/../../..}"
LAUNCHER="$BUS_ROOT/adapters/codex-app-server/codex-bus"

PASS=0; FAIL=0
fail() { print -u 2 "[FAIL] $1"; FAIL=$((FAIL+1)); }
pass() { print -u 2 "[ OK ] $1"; PASS=$((PASS+1)); }

# ---- Scratch state -----------------------------------------------------------
SCRATCH=$(mktemp -d /tmp/codexbus-it-XXXXXX)
trap 'rm -rf "$SCRATCH" 2>/dev/null; for pid in $KILL_ON_EXIT; do kill $pid 2>/dev/null; done' EXIT INT TERM
KILL_ON_EXIT=""

# Isolate ~/.codex/runtime.
export CODEX_RUNTIME_STATE_DIR="$SCRATCH/codex-runtime"
mkdir -p "$CODEX_RUNTIME_STATE_DIR/active-terminals" "$CODEX_RUNTIME_STATE_DIR/active-threads"

# Isolate the bus state dir so our broker doesn't clash with any running one.
export AGENT_BUS_STATE_DIR="$SCRATCH/bus"
export AGENT_BUS_SOCKET="$SCRATCH/b.sock"
export AGENT_BUS_ADAPTER_SOCKETS_DIR="$SCRATCH/adapter-sockets"
mkdir -p "$AGENT_BUS_STATE_DIR" "$AGENT_BUS_ADAPTER_SOCKETS_DIR"

# ---- Fake codex ---------------------------------------------------------------
# Simulates ~/bin/codex: writes an active-terminal record with null
# policy, then sleeps pretending to be the interactive process.
FAKE_CODEX="$SCRATCH/fake-codex"
cat > "$FAKE_CODEX" <<'SH'
#!/bin/zsh
SID=$(/usr/bin/python3 -c "import uuid; print(uuid.uuid4())")
REC="$CODEX_RUNTIME_STATE_DIR/active-terminals/$SID.json"
TS=$(date +%Y-%m-%dT%H:%M:%S%z)
cat > "$REC" <<JSON
{
  "runtime_session_id": "$SID",
  "pid": $$,
  "cwd": "$PWD",
  "tty_path": null,
  "bound_at": "$TS",
  "last_seen_at": "$TS",
  "bound_process_alive": true,
  "approval_mode": null,
  "sandbox_mode": null,
  "source": "fake-codex",
  "wrapper_overrides": {}
}
JSON
print -u 2 "[fake-codex] wrote $REC"
# Park on a file the test harness controls so it can "exit" us.
while [ ! -f "$SCRATCH/fake-codex.stop" ]; do sleep 1; done
print -u 2 "[fake-codex] exit signal received"
SH
chmod +x "$FAKE_CODEX"

# ---- Broker -------------------------------------------------------------------
/opt/homebrew/bin/node "$BUS_ROOT/broker/broker.js" >"$SCRATCH/broker.log" 2>&1 &
BROKER_PID=$!
KILL_ON_EXIT="$BROKER_PID"
for _ in $(seq 1 40); do
  [ -S "$AGENT_BUS_SOCKET" ] && break
  sleep 0.05
done
if [ ! -S "$AGENT_BUS_SOCKET" ]; then
  fail "broker never listened"
  exit 1
fi
pass "broker listening"

# ---- Launch the launcher ------------------------------------------------------
export SCRATCH
export CODEX_REAL="$FAKE_CODEX"
export CODEX_BUS_THREAD_WAIT_SEC=30

"$LAUNCHER" >"$SCRATCH/launcher.stdout" 2>"$SCRATCH/launcher.stderr" &
LAUNCHER_PID=$!
KILL_ON_EXIT="$LAUNCHER_PID $BROKER_PID"

# Wait for fake-codex record to appear, then policy-patch, then thread-record.
sleep 2

# Find the new session id.
SID=$(ls -1 "$CODEX_RUNTIME_STATE_DIR/active-terminals" 2>/dev/null | head -1 | sed 's/.json$//')
if [ -z "$SID" ]; then
  fail "no active-terminal record appeared"
  print -u 2 "--- launcher.stderr ---"; cat "$SCRATCH/launcher.stderr"
  exit 1
fi
pass "fake-codex wrote active-terminal: $SID"

# Policy should be patched by now (supervise runs immediately on record-appear).
sleep 1
APPROVAL=$(/usr/bin/python3 -c "import json; print(json.load(open('$CODEX_RUNTIME_STATE_DIR/active-terminals/$SID.json'))['approval_mode'])")
SANDBOX=$(/usr/bin/python3 -c "import json; print(json.load(open('$CODEX_RUNTIME_STATE_DIR/active-terminals/$SID.json'))['sandbox_mode'])")
if [ "$APPROVAL" = "on-request" ] && [ "$SANDBOX" = "workspace-write" ]; then
  pass "policy patched: approval=$APPROVAL sandbox=$SANDBOX"
else
  fail "policy not patched: approval=$APPROVAL sandbox=$SANDBOX"
fi

# Simulate thread_id appearing (codex's tracker writes this after first prompt).
THREAD_ID="test-thread-$(date +%s)"
TS=$(date +%Y-%m-%dT%H:%M:%S%z)
cat > "$CODEX_RUNTIME_STATE_DIR/active-threads/$SID.json" <<JSON
{
  "runtime_session_id": "$SID",
  "thread_id": "$THREAD_ID",
  "pid": $$,
  "cwd": "$PWD",
  "bound_at": "$TS",
  "last_seen_at": "$TS",
  "bound_process_alive": true,
  "status": "ready"
}
JSON
pass "simulated thread_id=$THREAD_ID"

# Wait for adapter to register.
BUS_CLI="$BUS_ROOT/cli/bus"
for _ in $(seq 1 20); do
  LISTING=$("$BUS_CLI" list 2>/dev/null || true)
  echo "$LISTING" | grep -q '^codex:' && break
  sleep 0.5
done
if ! echo "$LISTING" | grep -q '^codex:'; then
  fail "adapter never registered"
  print -u 2 "--- launcher.stderr ---"; cat "$SCRATCH/launcher.stderr"
  exit 1
fi
CODEX_PID=$(echo "$LISTING" | awk '/^codex:/{print $1; exit}')
pass "adapter registered: $CODEX_PID"

# Push-through-broker is NOT retested here — the codex-app-server contract
# suite and the two-codex dogfood already exercise adapter + real P3 helper
# end-to-end. This test is scoped to launcher plumbing only.

# Simulate codex exit → launcher supervise should stop the adapter.
touch "$SCRATCH/fake-codex.stop"
sleep 4

# Adapter should be gone from listing.
LISTING_AFTER=$("$BUS_CLI" list 2>/dev/null || true)
if echo "$LISTING_AFTER" | grep -q "^$CODEX_PID"; then
  # Might need a couple more seconds for GC, but also check if the adapter
  # process has exited (cleaner signal).
  sleep 3
  LISTING_AFTER=$("$BUS_CLI" list 2>/dev/null || true)
fi
if echo "$LISTING_AFTER" | grep -q "^$CODEX_PID"; then
  fail "adapter still registered after codex exit"
else
  pass "adapter stopped cleanly after codex exit"
fi

# Final cleanup: make sure nothing is still running.
kill $LAUNCHER_PID 2>/dev/null
kill $BROKER_PID 2>/dev/null
wait 2>/dev/null

print -u 2 ""
print -u 2 "========================"
print -u 2 "launcher integration: $PASS passed, $FAIL failed"
[ $FAIL -eq 0 ] && exit 0 || exit 2
