#!/usr/bin/env node
// Inter-agent bus broker.
// Persistent per-user Unix-socket daemon.

'use strict';

const net = require('net');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');

// Protocol constants — keep in sync with the Lease lifecycle section of DESIGN.md.
const HEARTBEAT_INTERVAL_SEC = 20;
const LEASE_TTL_SEC = 75;
const STALE_GC_AFTER_SEC = 10 * 60;
const MAILBOX_TTL_SEC = 5 * 60;
const GC_TICK_MS = 10 * 1000;
// Broker waits up to FORWARD_TIMEOUT_MS for an adapter to acknowledge a
// delivery. Some adapters (notably codex-app-server, which waits for
// `turn.completed` on a real Codex thread) routinely take 10-30 s. Set the
// ceiling high enough that legitimate real-session deliveries don't get
// mis-labeled as `queued`. Individual adapters should still respond quickly
// per the contract, but the broker is not positioned to enforce hard latency
// on behalf of recipients that route through model generation.
const FORWARD_TIMEOUT_MS = 30000;
const PROBE_TIMEOUT_MS = 500;

const DEFAULT_STATE_DIR = path.join(os.homedir(), '.agent-bus');
const STATE_DIR = process.env.AGENT_BUS_STATE_DIR || DEFAULT_STATE_DIR;
const SOCKET_PATH = process.env.AGENT_BUS_SOCKET || path.join(STATE_DIR, 'broker.sock');
const LOG_PATH = path.join(STATE_DIR, 'log.jsonl');
const REGISTRY_PATH = path.join(STATE_DIR, 'registry.json');
const MAILBOX_PATH = path.join(STATE_DIR, 'mailbox');

// Trusted root for participant delivery sockets. Adapters always create their
// delivery socket at `<ADAPTER_SOCKETS_DIR>/<sessionId>.sock` (see
// adapters/*/sidecar.js). The broker therefore only ever dials a socket inside
// this directory whose basename is bound to the registering session. This
// closes the "register a participant whose delivery.socketPath points at an
// arbitrary same-UID socket and let the broker connect() to it on the next
// push" vector. Mirror the adapters' own resolution so the dirs always agree.
const ADAPTER_SOCKETS_DIR = process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR
  || path.join(STATE_DIR, 'adapter-sockets');

// Test/integration harnesses register against ad-hoc fake adapter sockets that
// do not live under the canonical adapter-sockets root (they use scratch temp
// dirs and arbitrary basenames). When this is explicitly set, the trusted-root +
// sessionId binding is relaxed to the always-on hardening only (reject empty,
// null-byte, and `..`-escaping paths). NEVER set in production — real adapters
// always create `<ADAPTER_SOCKETS_DIR>/<sessionId>.sock`, so production keeps the
// full binding. The relaxed mode still blocks traversal, so it is not a bypass
// of the path-safety guarantees, only of the per-session basename binding.
const ALLOW_UNBOUND_DELIVERY_SOCKET = /^(1|true|yes)$/i.test(
  process.env.AGENT_BUS_ALLOW_UNBOUND_DELIVERY_SOCKET || ''
);

// Resolve a participant-supplied delivery socket path against the trusted root
// and the registering session. Returns the normalized path on success, or null
// if the path is missing, escapes the trusted root (traversal / symlink-style
// `..`), or is not the `<sessionId>.sock` this session is allowed to own.
function resolveDeliverySocketPath(sessionId, socketPath) {
  if (typeof socketPath !== 'string' || socketPath.length === 0) return null;
  if (socketPath.indexOf('\0') !== -1) return null;
  if (ALLOW_UNBOUND_DELIVERY_SOCKET) {
    // Relaxed (test) mode: absolute path, no traversal segments.
    if (socketPath.split(path.sep).includes('..')) return null;
    return path.resolve(socketPath);
  }
  const root = path.resolve(ADAPTER_SOCKETS_DIR);
  const resolved = path.resolve(root, socketPath);
  const expected = path.join(root, `${sessionId}.sock`);
  if (resolved !== expected) return null;
  return resolved;
}

// Strict participantId grammar. Closes path traversal via `to` and keeps IDs
// addressable. See DESIGN.md "Participant IDs" and §6.
const KIND_RE = /^[a-z][a-z0-9_-]{0,31}$/;
const NAMED_PARTICIPANT_ID_RE = /^[a-z][a-z0-9_-]{0,63}$/;
const LEGACY_PARTICIPANT_ID_RE = /^[a-z][a-z0-9_-]{0,31}:[0-9a-f]{12}\d?$/;
const PARTICIPANT_ID_RE = /^(?:[a-z][a-z0-9_-]{0,63}|[a-z][a-z0-9_-]{0,31}:[0-9a-f]{12}\d?)$/;

// In-memory state.
const participants = new Map();           // leaseToken -> Participant
const participantsByID = new Map();       // participantId -> leaseToken
// Eligible-recipients window for mailbox queueing (§1 scope): participants we
// have actually seen register since broker start or within the GC window. We
// enqueue to mailbox ONLY for entries present here, never for arbitrary `to`.
const recentParticipants = new Map();     // participantId -> {sessionId, kind, lastSeenAt}
// Mailbox is keyed by sha256(sessionId). That makes the on-disk filename
// unspoofable from the wire protocol and prevents a short-id collision from
// claiming mail addressed to a different logical session.
const mailbox = new Map();                // sessionIdHash -> [{participantId, envelope, queuedAt}]

function nowISO() { return new Date().toISOString(); }
function epochMs() { return Date.now(); }
function genLeaseToken() { return 'lease_' + crypto.randomBytes(16).toString('hex'); }
function sessionHash(sessionId) { return crypto.createHash('sha256').update(String(sessionId)).digest('hex'); }
function shortId(sessionId) { return sessionHash(sessionId).slice(0, 12); }

function isValidParticipantId(id) { return typeof id === 'string' && PARTICIPANT_ID_RE.test(id); }
function isValidKind(kind) { return typeof kind === 'string' && KIND_RE.test(kind); }

function parseOptionalText(value, maxLength, invalidCode, tooLongCode) {
  if (value == null) return { ok: true, value: null };
  if (typeof value !== 'string') return { ok: false, error: invalidCode };
  const normalized = value.replace(/\s+/g, ' ').trim();
  if (!normalized) return { ok: true, value: null };
  if (normalized.length > maxLength) return { ok: false, error: tooLongCode };
  return { ok: true, value: normalized };
}

function normalizeNamedParticipantId(value) {
  if (value == null) return { ok: true, value: null };
  const slug = String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-{2,}/g, '-');
  if (!slug) return { ok: false, error: 'invalid_display_name' };
  if (!NAMED_PARTICIPANT_ID_RE.test(slug)) {
    return { ok: false, error: 'invalid_display_name' };
  }
  return { ok: true, value: slug };
}

function buildParticipantId(kind, sessionId) {
  const base = `${kind}:${shortId(sessionId)}`;
  if (!participantsByID.has(base)) return base;
  for (let i = 0; i < 10; i++) {
    const candidate = `${base}${i}`;
    if (!participantsByID.has(candidate)) return candidate;
  }
  throw new Error('participant ID collision unresolvable');
}

function deleteRecentAliasesForSession(sessionId, keepParticipantId = null) {
  for (const [participantId, meta] of recentParticipants) {
    if (meta.sessionId !== sessionId) continue;
    if (keepParticipantId && participantId === keepParticipantId) continue;
    recentParticipants.delete(participantId);
  }
}

function findParticipantBySessionId(sessionId) {
  for (const p of participants.values()) {
    if (p.sessionId === sessionId) return p;
  }
  return null;
}

function logEvent(event) {
  const line = JSON.stringify({ ts: nowISO(), ...event }) + '\n';
  try { fs.appendFileSync(LOG_PATH, line); }
  catch (e) { console.error('[broker] log append failed:', e.message); }
}

function persistRegistry() {
  const snapshot = [];
  for (const p of participants.values()) {
    snapshot.push({
      participantId: p.participantId,
      kind: p.kind,
      displayName: p.displayName,
      description: p.description,
      project: p.project,
      cwd: p.cwd,
      capabilities: p.capabilities,
      state: 'open',
      registeredAt: new Date(p.registeredAt).toISOString(),
      lastHeartbeat: new Date(p.lastHeartbeat).toISOString(),
      leaseExpiresAt: new Date(p.leaseExpiresAt).toISOString(),
      // delivery is intentionally narrowed — no socketPath / threadId on disk.
      delivery: { adapter: p.delivery ? p.delivery.adapter : null },
    });
  }
  const tmp = REGISTRY_PATH + '.tmp';
  try {
    fs.writeFileSync(tmp, JSON.stringify({ ts: nowISO(), participants: snapshot }, null, 2));
    fs.renameSync(tmp, REGISTRY_PATH);
  } catch (e) { console.error('[broker] registry snapshot failed:', e.message); }
}

function mailboxPathFor(sessionIdHashHex) {
  return path.join(MAILBOX_PATH, sessionIdHashHex + '.jsonl');
}

function rewriteMailboxFile(sessionIdHashHex) {
  const queue = mailbox.get(sessionIdHashHex) || [];
  const file = mailboxPathFor(sessionIdHashHex);
  if (queue.length === 0) {
    try { fs.unlinkSync(file); } catch {}
    return;
  }
  fs.writeFileSync(file, queue.map(q => JSON.stringify(q)).join('\n') + '\n');
}

function enqueueMailbox(sessionIdHashHex, participantId, envelope) {
  const entry = { participantId, envelope, queuedAt: epochMs() };
  const queue = mailbox.get(sessionIdHashHex) || [];
  queue.push(entry);
  mailbox.set(sessionIdHashHex, queue);
  rewriteMailboxFile(sessionIdHashHex);
}

function loadMailboxFromDisk() {
  if (!fs.existsSync(MAILBOX_PATH)) return;
  const now = epochMs();
  for (const f of fs.readdirSync(MAILBOX_PATH)) {
    if (!/^[0-9a-f]{64}\.jsonl$/.test(f)) continue;
    const sessionIdHashHex = f.slice(0, -'.jsonl'.length);
    const content = fs.readFileSync(path.join(MAILBOX_PATH, f), 'utf8');
    const queue = [];
    for (const line of content.split('\n')) {
      if (!line.trim()) continue;
      try {
        const entry = JSON.parse(line);
        if (now - entry.queuedAt < MAILBOX_TTL_SEC * 1000) queue.push(entry);
      } catch {}
    }
    if (queue.length) {
      mailbox.set(sessionIdHashHex, queue);
      rewriteMailboxFile(sessionIdHashHex);
    } else {
      try { fs.unlinkSync(path.join(MAILBOX_PATH, f)); } catch {}
    }
  }
}

function attemptForward(participant, envelope) {
  return new Promise((resolve) => {
    const socketPath = participant.delivery && participant.delivery.socketPath;
    if (!socketPath) { resolve({ status: 'rejected', reason: 'no_delivery_socket' }); return; }
    // Defense-in-depth: re-validate the bound socket path at dial time, not just
    // at register time, so a stored/mutated participant record can never make the
    // broker connect() to a path outside the trusted adapter-sockets root.
    if (!resolveDeliverySocketPath(participant.sessionId, socketPath)) {
      resolve({ status: 'rejected', reason: 'untrusted_delivery_socket' });
      return;
    }
    let done = false;
    const finish = (result) => { if (!done) { done = true; resolve(result); } };
    const socket = net.createConnection(socketPath);
    const timeout = setTimeout(() => { socket.destroy(); finish({ status: 'rejected', reason: 'forward_timeout' }); }, FORWARD_TIMEOUT_MS);
    let buffer = '';
    socket.on('connect', () => {
      // Present the adapter's own capability token so the adapter can prove the
      // delivery came from the broker (which learned the token at register
      // time) and not from an arbitrary same-uid peer that merely connected to
      // the 0600 delivery socket. Omitted when no token was registered (e.g.
      // send-only CLI participants have no delivery socket at all, so this
      // never fires for them).
      const auth = participant.delivery && participant.delivery.adapterToken;
      const frame = auth ? { type: 'deliver', auth, envelope } : { type: 'deliver', envelope };
      socket.write(JSON.stringify(frame) + '\n');
    });
    socket.on('data', (chunk) => {
      buffer += chunk.toString();
      const nl = buffer.indexOf('\n');
      if (nl === -1) return;
      const line = buffer.slice(0, nl);
      try {
        const response = JSON.parse(line);
        clearTimeout(timeout); socket.end(); finish(response);
      } catch {
        clearTimeout(timeout); socket.destroy();
        finish({ status: 'rejected', reason: 'invalid_response' });
      }
    });
    socket.on('error', (err) => {
      clearTimeout(timeout);
      finish({ status: 'rejected', reason: 'socket_error:' + err.code });
    });
  });
}

// Per-participant delivery queue. All forwards to a given participant (live
// pushes AND mailbox flushes) are serialized through the participant's own
// chain, preserving FIFO at the reattach boundary. Without this, a live push
// could interleave with an in-flight mailbox flush and reorder messages.
function enqueueDelivery(participant, forwardFn) {
  const prev = participant._deliveryChain || Promise.resolve();
  let release;
  const result = new Promise(resolve => { release = resolve; });
  participant._deliveryChain = prev.then(async () => {
    try { release(await forwardFn()); }
    catch (e) { release({ status: 'rejected', reason: 'delivery_error:' + (e && e.message) }); }
  });
  return result;
}

// Drain a participant's mailbox onto their delivery chain synchronously. Called
// inside handleRegister before the response returns, so any new push_message
// arriving after the register is guaranteed to chain AFTER the mail.
function enqueueMailboxFlushOnRegister(participant) {
  const hash = sessionHash(participant.sessionId);
  const queue = mailbox.get(hash) || [];
  if (queue.length === 0) return;
  mailbox.delete(hash);
  rewriteMailboxFile(hash);
  const now = epochMs();
  for (const entry of queue) {
    if (now - entry.queuedAt >= MAILBOX_TTL_SEC * 1000) continue;
    enqueueDelivery(participant, async () => {
      const res = await attemptForward(participant, entry.envelope);
      if (res.status === 'accepted') {
        logEvent({ event: 'push_flushed', from: entry.envelope.from, to: entry.participantId, messageId: entry.envelope.id, result: 'delivered' });
      } else {
        // Re-queue on failure. Still serialized through the chain, so ordering
        // within this flush batch is preserved even on partial failure.
        enqueueMailbox(hash, entry.participantId, entry.envelope);
      }
      return res;
    }).catch(() => {});
  }
}

function isLeaseValid(p) { return p && epochMs() < p.leaseExpiresAt; }
function renewLease(p) { p.leaseExpiresAt = epochMs() + LEASE_TTL_SEC * 1000; p.lastHeartbeat = epochMs(); }

function markRecent(p) {
  recentParticipants.set(p.participantId, { sessionId: p.sessionId, kind: p.kind, lastSeenAt: epochMs() });
}

function gcTick() {
  const now = epochMs();
  let changed = false;
  for (const [token, p] of participants) {
    if (now >= p.leaseExpiresAt + STALE_GC_AFTER_SEC * 1000) {
      participants.delete(token);
      if (participantsByID.get(p.participantId) === token) participantsByID.delete(p.participantId);
      // Do NOT call markRecent here — that would reset lastSeenAt to now and
      // extend the eligibility window by another STALE_GC_AFTER_SEC.
      // recentParticipants already holds this participant (from the last
      // register/heartbeat/unregister). Its lastSeenAt is the correct anchor
      // for the 10-min "recently open/discoverable" §1 scope.
      logEvent({ event: 'gc_expire', participantId: p.participantId });
      changed = true;
    }
  }
  for (const [pid, meta] of recentParticipants) {
    if (now - meta.lastSeenAt >= STALE_GC_AFTER_SEC * 1000) recentParticipants.delete(pid);
  }
  for (const [hash, queue] of mailbox) {
    const kept = queue.filter(e => now - e.queuedAt < MAILBOX_TTL_SEC * 1000);
    if (kept.length !== queue.length) {
      if (kept.length === 0) mailbox.delete(hash); else mailbox.set(hash, kept);
      rewriteMailboxFile(hash);
    }
  }
  if (changed) persistRegistry();
}

function handleRegister(req) {
  const p = req.participant || {};
  if (!isValidKind(p.kind)) return { status: 'error', error: 'invalid_kind' };
  if (typeof p.sessionId !== 'string' || p.sessionId.length === 0 || p.sessionId.length > 256) {
    return { status: 'error', error: 'invalid_sessionId' };
  }
  const displayName = parseOptionalText(p.displayName, 64, 'invalid_display_name', 'display_name_too_long');
  if (!displayName.ok) return { status: 'error', error: displayName.error };
  const description = parseOptionalText(p.description, 200, 'invalid_description', 'description_too_long');
  if (!description.ok) return { status: 'error', error: description.error };
  const preferredParticipantId = normalizeNamedParticipantId(displayName.value);
  if (!preferredParticipantId.ok) return { status: 'error', error: preferredParticipantId.error };

  // Bind any declared delivery socket to the trusted adapter-sockets root and
  // this session before we ever agree to dial it. A send-only participant
  // (e.g. the CLI) declares no socketPath and is fine; but if one is present it
  // must be exactly `<ADAPTER_SOCKETS_DIR>/<sessionId>.sock`. This is the
  // register-time half of the same-UID delivery-socket hardening; attemptForward
  // re-checks at dial time as defense-in-depth.
  let normalizedDelivery = p.delivery || null;
  if (normalizedDelivery && normalizedDelivery.socketPath != null) {
    const resolved = resolveDeliverySocketPath(p.sessionId, normalizedDelivery.socketPath);
    if (!resolved) return { status: 'error', error: 'invalid_delivery_socket_path' };
    normalizedDelivery = { ...normalizedDelivery, socketPath: resolved };
  }
  // The adapter's delivery-socket capability token. Kept in the in-memory
  // participant record so attemptForward can present it on each {deliver}, but
  // NEVER written to registry.json (persistRegistry narrows delivery to just
  // `adapter`) and NEVER returned by list_participants (handleList does the
  // same). If present it must be a sane-length hex-ish string.
  if (normalizedDelivery && normalizedDelivery.adapterToken != null) {
    const tok = normalizedDelivery.adapterToken;
    if (typeof tok !== 'string' || tok.length < 16 || tok.length > 256) {
      return { status: 'error', error: 'invalid_adapter_token' };
    }
  }

  // Reclaim: same sessionId reconnecting. Invalidate old lease, reuse id.
  // This is the silent-re-register path required by the Lease lifecycle
  // ("Clients detect loss-of-broker and auto-reregister ... No user prompting").
  //
  // Auth hardening: sessionId is NOT a secret to peers — although list_participants
  // hides it, it can be guessed/observed (env, process args, prior knowledge), and
  // it used to be the sole, unauthenticated reclaim key: any same-uid peer that
  // learned a victim's sessionId could re-register it, invalidating the victim's
  // live lease and hijacking its participant identity. We now refuse to reclaim a
  // STILL-LIVE lease. Legitimate silent re-register only happens after broker loss
  // (broker restart drops all leases) or after the prior lease has expired — in
  // both cases the old lease is not live, so the legitimate path is unaffected.
  const existing = findParticipantBySessionId(p.sessionId);
  let participantId;
  let reclaimed = false;
  if (existing) {
    if (existing.kind !== p.kind) return { status: 'error', error: 'kind_mismatch_for_sessionId' };
    if (isLeaseValid(existing)) {
      logEvent({ event: 'reclaim_refused', participantId: existing.participantId, reason: 'lease_still_live' });
      return { status: 'error', error: 'session_lease_active' };
    }
    participants.delete(existing.leaseToken);
    participantsByID.delete(existing.participantId);
    participantId = preferredParticipantId.value || existing.participantId;
    reclaimed = participantId === existing.participantId;
  } else {
    participantId = preferredParticipantId.value || buildParticipantId(p.kind, p.sessionId);
  }

  if (participantsByID.has(participantId)) {
    return { status: 'error', error: 'participant_id_in_use' };
  }

  const leaseToken = genLeaseToken();
  const now = epochMs();
  const participant = {
    participantId, kind: p.kind, sessionId: p.sessionId,
    displayName: displayName.value, description: description.value,
    project: p.project || null, cwd: p.cwd || null,
    capabilities: Array.isArray(p.capabilities) ? p.capabilities : ['text'],
    state: 'open',
    delivery: normalizedDelivery,
    registeredAt: now, lastHeartbeat: now,
    leaseExpiresAt: now + LEASE_TTL_SEC * 1000,
    leaseToken,
  };
  participants.set(leaseToken, participant);
  participantsByID.set(participantId, leaseToken);
  deleteRecentAliasesForSession(p.sessionId, participantId);
  markRecent(participant);
  logEvent({
    event: reclaimed ? 'reclaim' : 'register',
    participantId,
    kind: p.kind,
    adapter: p.delivery ? p.delivery.adapter : null,
    displayName: participant.displayName,
  });
  persistRegistry();
  // Synchronously drain mailbox onto the new participant's delivery chain.
  // Any subsequent push_message to this participant chains AFTER mail,
  // preserving FIFO at the reattach boundary.
  enqueueMailboxFlushOnRegister(participant);
  return {
    status: 'ok',
    participantId,
    leaseToken,
    leaseExpiresAt: new Date(participant.leaseExpiresAt).toISOString(),
    heartbeatIntervalSec: HEARTBEAT_INTERVAL_SEC,
    reclaimed,
  };
}

function handleHeartbeat(req) {
  const p = participants.get(req.leaseToken);
  if (!p) return { status: 'error', error: 'invalid_lease' };
  if (!isLeaseValid(p)) return { status: 'error', error: 'lease_expired' };
  renewLease(p);
  markRecent(p);
  return { status: 'ok', leaseExpiresAt: new Date(p.leaseExpiresAt).toISOString() };
}

function handleList() {
  const now = epochMs();
  const open = [];
  for (const p of participants.values()) {
    if (now < p.leaseExpiresAt) {
      // sessionId is intentionally NOT exposed over list_participants. It is the
      // re-register reclaim key (a same-UID peer that learns another session's
      // sessionId could reclaim its identity / invalidate its lease), so it must
      // not leak to peers that only have discovery access. It is likewise kept
      // out of registry.json (see persistRegistry). Peers address each other by
      // participantId / displayName, never by sessionId.
      open.push({
        participantId: p.participantId, kind: p.kind,
        displayName: p.displayName, description: p.description,
        project: p.project, cwd: p.cwd, capabilities: p.capabilities,
        state: 'open',
        lastHeartbeat: new Date(p.lastHeartbeat).toISOString(),
        delivery: { adapter: p.delivery ? p.delivery.adapter : null },
      });
    }
  }
  return { status: 'ok', participants: open };
}

async function handlePush(req) {
  const sender = participants.get(req.leaseToken);
  if (!sender || !isLeaseValid(sender)) return { status: 'error', error: 'invalid_or_expired_lease' };
  const toId = req.to;
  if (!isValidParticipantId(toId)) return { status: 'error', error: 'invalid_to_participant_id' };
  const msg = req.message || {};
  if (typeof msg.id !== 'string' || msg.id.length === 0 || msg.id.length > 128) {
    return { status: 'error', error: 'invalid_message_id' };
  }
  if (typeof msg.body !== 'string' || msg.body.length === 0) {
    return { status: 'error', error: 'invalid_message_body' };
  }
  const envelope = {
    id: msg.id,
    from: sender.participantId,
    to: toId,
    body: msg.body,
    reply_to: msg.reply_to || null,
    conversation_id: msg.conversation_id || null,
    source: msg.source || 'participant',
    sent_at: msg.sent_at || nowISO(),
  };

  const recipientToken = participantsByID.get(toId);
  const recipient = recipientToken ? participants.get(recipientToken) : null;
  if (recipient && isLeaseValid(recipient)) {
    // Route through the recipient's delivery chain so pushes and flushes
    // serialize cleanly. FIFO preserved at the reattach boundary.
    const result = await enqueueDelivery(recipient, () => attemptForward(recipient, envelope));
    if (result.status === 'accepted') {
      logEvent({ event: 'push', from: sender.participantId, to: toId, messageId: msg.id, result: 'delivered' });
      return { status: 'delivered', from: sender.participantId };
    }
    // Forward failed; queue to the recipient's own mailbox (sessionId-keyed).
    enqueueMailbox(sessionHash(recipient.sessionId), toId, envelope);
    logEvent({ event: 'push', from: sender.participantId, to: toId, messageId: msg.id, result: 'queued', reason: result.reason });
    return { status: 'queued', from: sender.participantId, reason: result.reason };
  }

  // Offline path: queue only if recipient is in the §1-scoped recent window.
  const recent = recentParticipants.get(toId);
  if (!recent) {
    logEvent({ event: 'push', from: sender.participantId, to: toId, messageId: msg.id, result: 'failed', reason: 'no_such_participant' });
    return { status: 'failed', from: sender.participantId, reason: 'no_such_participant' };
  }
  enqueueMailbox(sessionHash(recent.sessionId), toId, envelope);
  logEvent({ event: 'push', from: sender.participantId, to: toId, messageId: msg.id, result: 'queued', reason: 'recipient_offline' });
  return { status: 'queued', from: sender.participantId, reason: 'recipient_offline' };
}

function handleUnregister(req) {
  const p = participants.get(req.leaseToken);
  if (!p) return { status: 'error', error: 'invalid_lease' };
  participants.delete(req.leaseToken);
  if (participantsByID.get(p.participantId) === req.leaseToken) participantsByID.delete(p.participantId);
  markRecent(p);
  logEvent({ event: 'unregister', participantId: p.participantId });
  persistRegistry();
  return { status: 'ok' };
}

async function handleRequest(req) {
  switch (req.type) {
    case 'register_participant': return handleRegister(req);
    case 'heartbeat': return handleHeartbeat(req);
    case 'list_participants': return handleList();
    case 'push_message': return await handlePush(req);
    case 'unregister_participant': return handleUnregister(req);
    default: return { status: 'error', error: 'unknown_type' };
  }
}

function handleConnection(socket) {
  let buffer = '';
  let processing = Promise.resolve();
  socket.on('data', (chunk) => {
    buffer += chunk.toString();
    let nl;
    while ((nl = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      if (!line.trim()) continue;
      processing = processing.then(async () => {
        let req;
        try { req = JSON.parse(line); }
        catch { socket.write(JSON.stringify({ status: 'error', error: 'invalid_json' }) + '\n'); return; }
        try {
          const response = await handleRequest(req);
          socket.write(JSON.stringify(response) + '\n');
        } catch (e) {
          console.error('[broker] handler error:', e);
          socket.write(JSON.stringify({ status: 'error', error: 'internal', message: e.message }) + '\n');
        }
      });
    }
  });
  socket.on('error', () => {});
}

function ensureStateDirs() {
  fs.mkdirSync(STATE_DIR, { recursive: true, mode: 0o700 });
  fs.mkdirSync(MAILBOX_PATH, { recursive: true, mode: 0o700 });
  try { fs.chmodSync(STATE_DIR, 0o700); } catch {}
  try { fs.chmodSync(MAILBOX_PATH, 0o700); } catch {}
}

function probeExistingBroker(socketPath) {
  return new Promise((resolve) => {
    if (!fs.existsSync(socketPath)) { resolve({ alive: false, reason: 'not_present' }); return; }
    const s = net.createConnection(socketPath);
    const timer = setTimeout(() => { s.destroy(); resolve({ alive: false, reason: 'probe_timeout' }); }, PROBE_TIMEOUT_MS);
    s.on('connect', () => { clearTimeout(timer); s.end(); resolve({ alive: true }); });
    s.on('error', (err) => { clearTimeout(timer); resolve({ alive: false, reason: 'connect_error:' + err.code }); });
  });
}

async function main() {
  ensureStateDirs();
  loadMailboxFromDisk();

  if (fs.existsSync(SOCKET_PATH)) {
    const probe = await probeExistingBroker(SOCKET_PATH);
    if (probe.alive) {
      console.error(`[broker] another broker is live at ${SOCKET_PATH}; refusing to start (prevents split-brain)`);
      process.exit(2);
    }
    try { fs.unlinkSync(SOCKET_PATH); }
    catch (e) { console.error(`[broker] cannot unlink stale socket ${SOCKET_PATH}:`, e.message); process.exit(1); }
  }

  const server = net.createServer(handleConnection);
  server.listen(SOCKET_PATH, () => {
    fs.chmodSync(SOCKET_PATH, 0o600);
    console.log(`[broker] listening on ${SOCKET_PATH}`);
    console.log(`[broker] state dir: ${STATE_DIR}`);
  });
  server.on('error', (err) => { console.error('[broker] server error:', err); process.exit(1); });

  const gc = setInterval(gcTick, GC_TICK_MS);
  gc.unref();

  const shutdown = () => {
    console.log('[broker] shutting down');
    try { server.close(); } catch {}
    try { fs.unlinkSync(SOCKET_PATH); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) main().catch(err => { console.error('[broker] fatal:', err); process.exit(1); });

module.exports = { main, HEARTBEAT_INTERVAL_SEC, LEASE_TTL_SEC, MAILBOX_TTL_SEC };
