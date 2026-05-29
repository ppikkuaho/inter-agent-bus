// codex-pty adapter — bus sidecar.
// Owns broker registration, inbound delivery socket, envelope render, and
// writing the rendered message into a PTY. The wrapper passes in a `write`
// callback and owns the child Codex PTY lifecycle.

'use strict';

const net = require('net');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { BusClient } = require('../../client/client');
const sessionAuth = require('../session-auth');

const DEDUPE_LRU_SIZE = 256;
const SUBMIT_DELAY_MS = 80;

// Strip anything in an envelope field that could steer the live TTY before we
// type it into the wrapped session. The adapter owns the single submit-Enter
// at the end of a delivery (see handleRequest); no field may smuggle in extra
// submits, cursor moves, or screen ops:
//   - CR / LF would auto-submit the turn early or inject additional turns;
//   - ANSI/CSI escape sequences (ESC [ ... ) drive cursor/color/screen state;
//   - other C0/C1 control characters can corrupt the TUI input line.
// Applied to from / id / body before they are interpolated into the rendered
// line, so a hostile or malformed peer cannot turn a message body into terminal
// commands or a forged extra user turn. Newlines collapse to spaces so the body
// stays one visible line.
const ANSI_ESCAPE_RE = /\x1b(?:\[[0-9;?]*[ -\/]*[@-~]|[@-_])/g;
const CONTROL_CHARS_RE = /[\x00-\x1f\x7f-\x9f]/g;
function sanitizeForTTY(value) {
  return String(value == null ? '' : value)
    .replace(ANSI_ESCAPE_RE, '')
    .replace(CONTROL_CHARS_RE, ' ');
}

function resolveSocketDir(explicit) {
  if (explicit) return explicit;
  if (process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR) {
    return process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR;
  }
  return path.join(os.homedir(), '.agent-bus', 'adapter-sockets');
}

function renderEnvelope(env) {
  const t = typeof env.sent_at === 'string' ? env.sent_at.slice(11, 16) + 'Z' : '';
  const from = sanitizeForTTY(env.from);
  const id = sanitizeForTTY(env.id);
  const body = sanitizeForTTY(env.body);
  return `[from:${from} · msg:${id} · ${t}] ${body}`;
}

function startSidecar({ write, sessionId, project, cwd, socketDir, getRegistrationProfile }) {
  if (typeof write !== 'function') throw new Error('startSidecar: write must be a function');
  if (typeof sessionId !== 'string' || !sessionId) throw new Error('startSidecar: sessionId required');

  const effectiveSocketDir = resolveSocketDir(socketDir);
  const socketPath = path.join(effectiveSocketDir, `${sessionId}.sock`);
  const recentIds = new Map();
  // Per-session capability token. Required on {deliver} (so only the broker,
  // which learned it at register, can inject a turn) and on {send} (so only the
  // local CLI, which reads the 0600 token file, can emit as this participant).
  const adapterToken = sessionAuth.generateToken();
  let server = null;
  let client = null;

  async function cleanupStartedResources() {
    try { if (client) await client.unregister(); } catch {}
    client = null;
    const currentServer = server;
    server = null;
    if (currentServer && currentServer.listening) {
      await new Promise((resolve) => currentServer.close(() => resolve()));
    }
    try { fs.unlinkSync(socketPath); } catch {}
    sessionAuth.removeToken(effectiveSocketDir, sessionId);
  }

  function seenBefore(id) {
    if (recentIds.has(id)) return true;
    recentIds.set(id, Date.now());
    if (recentIds.size > DEDUPE_LRU_SIZE) {
      const oldest = recentIds.keys().next().value;
      recentIds.delete(oldest);
    }
    return false;
  }

  async function handleRequest(req) {
    if (req.type === 'deliver' && req.envelope) {
      // Connection != authority. Only the broker (holding adapterToken from
      // register) may drive the autosubmit-into-PTY path; reject an
      // unauthenticated same-uid peer before writing to the live session.
      if (!sessionAuth.timingSafeEqualStr(req.auth, adapterToken)) {
        return { status: 'rejected', reason: 'unauthorized' };
      }
      const env = req.envelope;
      if (typeof env.id !== 'string' || !env.id) {
        return { status: 'rejected', reason: 'missing_id' };
      }
      if (typeof env.body !== 'string') {
        return { status: 'rejected', reason: 'missing_body' };
      }
      if (seenBefore(env.id)) {
        return { status: 'rejected', reason: 'duplicate' };
      }

      const rendered = renderEnvelope(env);
      try {
        for (const ch of rendered) write(ch);
        setTimeout(() => {
          try { write('\r'); } catch {}
        }, SUBMIT_DELAY_MS);
        return { status: 'accepted' };
      } catch (err) {
        return { status: 'rejected', reason: 'write_error:' + (err && err.message) };
      }
    }

    if (req.type === 'send') {
      // Emitting as this participant is privileged (sends under our lease,
      // stamped with our identity). Require the capability token so a same-uid
      // peer cannot impersonate this session over its own socket.
      if (!sessionAuth.timingSafeEqualStr(req.auth, adapterToken)) {
        return { status: 'rejected', reason: 'unauthorized' };
      }
      if (!client || !client.participantId) {
        return { status: 'rejected', reason: 'not_registered' };
      }
      if (typeof req.to !== 'string' || !req.to) {
        return { status: 'rejected', reason: 'missing_to' };
      }
      if (typeof req.body !== 'string' || !req.body) {
        return { status: 'rejected', reason: 'missing_body' };
      }
      try {
        return await client.send(req.to, req.body, {
          id: req.id,
          replyTo: req.reply_to,
          conversationId: req.conversation_id,
          source: req.source || 'participant',
        });
      } catch (err) {
        return { status: 'rejected', reason: 'send_error:' + (err && err.message) };
      }
    }

    return { status: 'rejected', reason: 'unknown_type' };
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
          try {
            req = JSON.parse(line);
          } catch {
            socket.write(JSON.stringify({ status: 'rejected', reason: 'invalid_json' }) + '\n');
            return;
          }
          const response = await handleRequest(req);
          socket.write(JSON.stringify(response) + '\n');
        }).catch(() => {});
      }
    });
    socket.on('error', () => {});
  }

  async function start() {
    fs.mkdirSync(effectiveSocketDir, { recursive: true, mode: 0o700 });
    try { fs.chmodSync(effectiveSocketDir, 0o700); } catch {}
    if (fs.existsSync(socketPath)) {
      try { fs.unlinkSync(socketPath); } catch {}
    }

    try {
      server = net.createServer(handleConnection);
      await new Promise((resolve, reject) => {
        server.listen(socketPath, () => {
          fs.chmodSync(socketPath, 0o600);
          resolve();
        });
        server.once('error', reject);
      });

      // Publish the capability token (0600) for the local CLI's {send}, and hand
      // the same token to the broker via register so it can present it on
      // {deliver}. Written before register so no delivery can arrive first.
      sessionAuth.writeToken(effectiveSocketDir, sessionId, adapterToken);

      const profile = typeof getRegistrationProfile === 'function'
        ? (getRegistrationProfile() || {})
        : {};

      client = new BusClient();
      await client.register({
        kind: 'codex',
        sessionId,
        project: project || null,
        cwd: cwd || null,
        displayName: profile.displayName || null,
        description: profile.description || null,
        delivery: { adapter: 'codex-pty', socketPath, adapterToken },
      });

      return { socketPath, participantId: client.participantId };
    } catch (err) {
      await cleanupStartedResources();
      throw err;
    }
  }

  async function stop() {
    await cleanupStartedResources();
  }

  return {
    start,
    stop,
    get socketPath() { return socketPath; },
    get participantId() { return client ? client.participantId : null; },
  };
}

module.exports = { startSidecar, renderEnvelope };
