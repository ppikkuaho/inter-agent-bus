'use strict';

// Per-session capability tokens for the bus's local Unix sockets.
//
// Why this exists: the adapter delivery socket ({deliver} writes-and-autosubmits
// into the live agent PTY; {send} emits as the participant) and the control
// socket (enable/disable) used to treat connection == authority. Filesystem
// mode 0600/0700 only keeps OTHER uids out; it does nothing against a same-uid
// peer (every other CLI agent the user runs shares the uid). A same-uid peer
// that merely connect()s could forge a user turn into a live session or flip a
// session's bus state.
//
// The fix raises the bar from "can connect" to "holds the per-session secret":
//   - the wrapper mints a random 256-bit token at startup and writes it to a
//     0600 file next to the socket (`<sessionId>.token`);
//   - privileged requests must carry that token; the broker (for {deliver}) and
//     the CLI (for {send}/enable/disable) read it and present it;
//   - comparison is timing-safe (constant-time) so a peer cannot byte-probe it.
//
// Residual (documented, not silently shipped): a same-uid process that can read
// arbitrary files can still read the 0600 token file. A peer-uid credential
// check (SO_PEERCRED / LOCAL_PEERCRED) would be strictly additive but needs
// native code on macOS and is left as a flagged follow-up; it would not close
// the same-uid case anyway. See DESIGN.md §6 and the auth decision-log entry.

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const TOKEN_BYTES = 32; // 256-bit secret.

// Generate a fresh capability token. Hex so it survives JSON/file round-trips.
function generateToken() {
  return crypto.randomBytes(TOKEN_BYTES).toString('hex');
}

// Constant-time string compare. Returns false (never throws) for any non-string
// or length-mismatched input, and still burns a compare on a fixed-length buffer
// so the early-exit does not leak length via timing.
function timingSafeEqualStr(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const ab = Buffer.from(a, 'utf8');
  const bb = Buffer.from(b, 'utf8');
  if (ab.length !== bb.length) {
    // Compare bb against itself to keep the work constant regardless of length.
    crypto.timingSafeEqual(bb, bb);
    return false;
  }
  return crypto.timingSafeEqual(ab, bb);
}

function tokenPathFor(socketDir, sessionId) {
  return path.join(socketDir, `${sessionId}.token`);
}

// Write a token to a 0600 file inside an (assumed already 0700) socket dir.
// Returns the token written.
function writeToken(socketDir, sessionId, token) {
  const file = tokenPathFor(socketDir, sessionId);
  // O_WRONLY|O_CREAT|O_TRUNC with explicit 0600 so the secret is never briefly
  // world/group-readable between create and chmod.
  const fd = fs.openSync(file, 'w', 0o600);
  try {
    fs.writeSync(fd, token);
  } finally {
    fs.closeSync(fd);
  }
  try { fs.chmodSync(file, 0o600); } catch {}
  return token;
}

// Read a token file. Returns the trimmed token string, or null if absent/empty.
function readToken(socketDir, sessionId) {
  try {
    const raw = fs.readFileSync(tokenPathFor(socketDir, sessionId), 'utf8').trim();
    return raw || null;
  } catch {
    return null;
  }
}

function removeToken(socketDir, sessionId) {
  try { fs.unlinkSync(tokenPathFor(socketDir, sessionId)); } catch {}
}

module.exports = {
  TOKEN_BYTES,
  generateToken,
  timingSafeEqualStr,
  tokenPathFor,
  writeToken,
  readToken,
  removeToken,
};
