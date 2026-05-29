#!/bin/zsh
# Integration test for ~/bin/codex after bus auto-launch wiring.
#
# Uses a fake CODEX_REAL so we exercise the real wrapper logic without spawning
# a real Codex TUI. Verifies:
#   1. wrapper creates a per-session active-terminal record with non-null policy
#   2. wrapper starts the adapter supervisor once a thread_id appears
#   3. adapter registers with the broker
#   4. adapter stops cleanly when the wrapped process exits

set -u

# Bus root is derived from this script's own location (contract-tests is three
# levels under the bus root) so the test is portable across clones. WRAPPER is
# the real interactive `codex` launcher on this machine; override CODEX_REAL/
# WRAPPER if yours lives elsewhere.
BUS_ROOT="${AGENT_BUS_ROOT:-${0:A:h}/../../..}"
WRAPPER="${WRAPPER:-$HOME/bin/codex}"

PASS=0; FAIL=0
fail() { print -u 2 "[FAIL] $1"; FAIL=$((FAIL+1)); }
pass() { print -u 2 "[ OK ] $1"; PASS=$((PASS+1)); }

SCRATCH=$(mktemp -d /tmp/codex-wrapper-it-XXXXXX)
trap 'rm -rf "$SCRATCH" 2>/dev/null; for pid in $KILL_ON_EXIT; do kill $pid 2>/dev/null; done' EXIT INT TERM
KILL_ON_EXIT=""

export SCRATCH
export CODEX_RUNTIME_STATE_DIR="$SCRATCH/codex-runtime"
export AGENT_BUS_STATE_DIR="$SCRATCH/bus"
export AGENT_BUS_SOCKET="$SCRATCH/bus.sock"
export AGENT_BUS_ADAPTER_SOCKETS_DIR="$SCRATCH/adapter-sockets"
export AGENT_BUS_ROOT="$BUS_ROOT"
mkdir -p "$CODEX_RUNTIME_STATE_DIR/active-terminals" "$CODEX_RUNTIME_STATE_DIR/active-threads" "$AGENT_BUS_ADAPTER_SOCKETS_DIR"

FAKE_CODEX="$SCRATCH/fake-codex"
cat > "$FAKE_CODEX" <<'SH'
#!/bin/zsh
while true; do sleep 1; done
SH
chmod +x "$FAKE_CODEX"
export CODEX_REAL="$FAKE_CODEX"
export CODEX_BUS_THREAD_WAIT_SEC=30
export AGENT_BUS_AUTO_ENABLE=1

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

# Run the real wrapper behind `script` so `tty` resolves to a PTY even under
# non-interactive test execution.
script -q /dev/null "$WRAPPER" -a on-request -s workspace-write >"$SCRATCH/wrapper.stdout" 2>"$SCRATCH/wrapper.stderr" &
WRAPPER_PID=$!
KILL_ON_EXIT="$WRAPPER_PID $BROKER_PID"

SID=""
for _ in $(seq 1 80); do
  SID=$(find "$CODEX_RUNTIME_STATE_DIR/active-terminals" -maxdepth 1 -type f -name '*.json' | head -1)
  if [ -n "$SID" ]; then
    SID=$(basename "$SID" .json)
  fi
  [ -n "$SID" ] && break
  sleep 0.25
done
if [ -z "$SID" ]; then
  fail "no per-session active-terminal record appeared"
  print -u 2 "--- wrapper.stderr ---"; cat "$SCRATCH/wrapper.stderr"
  exit 1
fi
pass "wrapper wrote active-terminal record: $SID"

APPROVAL=$(/usr/bin/python3 -c "import json; print(json.load(open('$CODEX_RUNTIME_STATE_DIR/active-terminals/$SID.json'))['approval_mode'])")
SANDBOX=$(/usr/bin/python3 -c "import json; print(json.load(open('$CODEX_RUNTIME_STATE_DIR/active-terminals/$SID.json'))['sandbox_mode'])")
if [ "$APPROVAL" = "on-request" ] && [ "$SANDBOX" = "workspace-write" ]; then
  pass "wrapper captured policy: approval=$APPROVAL sandbox=$SANDBOX"
else
  fail "wrapper policy mismatch: approval=$APPROVAL sandbox=$SANDBOX"
fi

THREAD_ID="wrapper-test-thread-$(date +%s)"
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

BUS_CLI="$BUS_ROOT/cli/bus"
LISTING=""
for _ in $(seq 1 40); do
  LISTING=$("$BUS_CLI" list 2>/dev/null || true)
  echo "$LISTING" | grep -q '^codex:' && break
  sleep 0.5
done
if ! echo "$LISTING" | grep -q '^codex:'; then
  fail "adapter never registered"
  print -u 2 "--- wrapper.stderr ---"; cat "$SCRATCH/wrapper.stderr"
  print -u 2 "--- wrapper.stdout ---"; cat "$SCRATCH/wrapper.stdout"
  exit 1
fi
CODEX_PID=$(echo "$LISTING" | awk '/^codex:/{print $1; exit}')
pass "adapter registered: $CODEX_PID"

WRAPPED_PID=$(/usr/bin/python3 -c "import json; print(json.load(open('$CODEX_RUNTIME_STATE_DIR/active-terminals/$SID.json'))['pid'])")
kill "$WRAPPED_PID" 2>/dev/null || true
sleep 4

LISTING_AFTER=$("$BUS_CLI" list 2>/dev/null || true)
if echo "$LISTING_AFTER" | grep -q "^$CODEX_PID"; then
  sleep 3
  LISTING_AFTER=$("$BUS_CLI" list 2>/dev/null || true)
fi
if echo "$LISTING_AFTER" | grep -q "^$CODEX_PID"; then
  fail "adapter still registered after wrapped process exit"
else
  pass "adapter stopped cleanly after wrapped process exit"
fi

kill $WRAPPER_PID 2>/dev/null
kill $BROKER_PID 2>/dev/null
wait 2>/dev/null

print -u 2 ""
print -u 2 "========================"
print -u 2 "wrapper integration: $PASS passed, $FAIL failed"
[ $FAIL -eq 0 ] && exit 0 || exit 2
