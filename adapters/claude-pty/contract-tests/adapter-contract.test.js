// Contract tests for the claude-pty adapter.
// Two tiers: (1) direct adapter-socket tests without a broker (dedupe, render
// shape, malformed input); (2) end-to-end through the real broker (register,
// delivery, envelope stamping). Real Claude PTY is NOT required — the
// `pty-recorder` fixture stands in for the PTY writer.

'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const net = require('net');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const { BusClient } = require('../../../client/client');
const { startSidecar, renderEnvelope } = require('../sidecar');
const { makeRecorder } = require('./pty-recorder');
const sessionAuth = require('../../session-auth');
const BROKER_JS = path.resolve(__dirname, '../../../broker/broker.js');

// The adapter's privileged ops ({deliver}, {send}) now require the per-session
// capability token (connection != authority). The legitimate callers (broker,
// CLI) present it; these direct-socket tests read it from the 0600 token file
// the sidecar publishes in its socket dir on start(), the same way the CLI does.
function readAdapterToken(sessionId) {
  return sessionAuth.readToken(ADAPTER_SOCKETS_DIR, sessionId);
}

const TMP_STATE = fs.mkdtempSync(path.join(os.tmpdir(), 'bus-pty-contract-'));
const BROKER_SOCKET = path.join(TMP_STATE, 'broker.sock');
// Unix socket paths on macOS are capped at ~104 bytes (sun_path). The default
// macOS `$TMPDIR` under `/var/folders/...` alone is ~70 bytes, so nesting an
// `adapter-sockets/<sessionId>.sock` under it blows the limit. Put adapter
// sockets under `/tmp` instead — state files stay in the real tempdir for
// clean isolation and cleanup.
const ADAPTER_SOCKETS_DIR = fs.mkdtempSync('/tmp/bus-ptysock-');
let broker;

async function waitForSocket(p, deadlineMs = 3000) {
  const stop = Date.now() + deadlineMs;
  while (Date.now() < stop) {
    if (fs.existsSync(p)) return;
    await new Promise(r => setTimeout(r, 30));
  }
  throw new Error('socket never appeared: ' + p);
}

function sendLine(socketPath, line) {
  return new Promise((resolve, reject) => {
    const c = net.createConnection(socketPath);
    let buffer = '';
    c.on('connect', () => c.write(line + '\n'));
    c.on('data', (chunk) => {
      buffer += chunk.toString();
      const nl = buffer.indexOf('\n');
      if (nl !== -1) {
        try { resolve(JSON.parse(buffer.slice(0, nl))); }
        catch (e) { reject(e); }
        c.end();
      }
    });
    c.on('error', reject);
    setTimeout(() => { c.destroy(); reject(new Error('timeout')); }, 2000).unref();
  });
}

async function startRecipient(kind = 'codex') {
  const socketPath = path.join(ADAPTER_SOCKETS_DIR, `recipient-${crypto.randomBytes(4).toString('hex')}.sock`);
  const envelopes = [];
  const server = net.createServer((socket) => {
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk.toString();
      const nl = buffer.indexOf('\n');
      if (nl === -1) return;
      const line = buffer.slice(0, nl);
      let req;
      try {
        req = JSON.parse(line);
      } catch {
        socket.write(JSON.stringify({ status: 'rejected', reason: 'invalid_json' }) + '\n');
        socket.end();
        return;
      }
      if (req.type !== 'deliver' || !req.envelope) {
        socket.write(JSON.stringify({ status: 'rejected', reason: 'unknown_type' }) + '\n');
        socket.end();
        return;
      }
      envelopes.push(req.envelope);
      socket.write(JSON.stringify({ status: 'accepted' }) + '\n');
      socket.end();
    });
  });
  await new Promise((resolve, reject) => {
    server.listen(socketPath, () => resolve());
    server.once('error', reject);
  });

  const client = new BusClient({ socketPath: BROKER_SOCKET });
  await client.connect();
  const reg = await client.register({
    kind,
    sessionId: 'recipient-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: `${kind}-test`, socketPath },
  });

  return {
    envelopes,
    participantId: reg.participantId,
    async stop() {
      await client.unregister();
      await new Promise((resolve) => server.close(() => resolve()));
      try { fs.unlinkSync(socketPath); } catch {}
    },
  };
}

before(async () => {
  const env = {
    ...process.env,
    AGENT_BUS_STATE_DIR: TMP_STATE,
    AGENT_BUS_SOCKET: BROKER_SOCKET,
    AGENT_BUS_ADAPTER_SOCKETS_DIR: ADAPTER_SOCKETS_DIR,
    // The fake recipients/senders in these fixtures use ad-hoc socket
    // basenames and `/nonexistent`, so relax the broker's delivery-socket
    // binding to the always-on traversal hardening for the test broker.
    // Production never sets this. See broker.js resolveDeliverySocketPath.
    AGENT_BUS_ALLOW_UNBOUND_DELIVERY_SOCKET: '1',
  };
  broker = spawn('node', [BROKER_JS], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  broker.stdout.on('data', d => process.stdout.write('[broker] ' + d.toString()));
  broker.stderr.on('data', d => process.stderr.write('[broker] ' + d.toString()));
  await waitForSocket(BROKER_SOCKET);
});

after(async () => {
  if (broker) {
    broker.kill('SIGTERM');
    await new Promise(resolve => {
      let done = false;
      const finish = () => { if (!done) { done = true; resolve(); } };
      broker.once('exit', finish);
      setTimeout(finish, 2000).unref();
    });
  }
  try { fs.rmSync(TMP_STATE, { recursive: true, force: true }); } catch {}
  try { fs.rmSync(ADAPTER_SOCKETS_DIR, { recursive: true, force: true }); } catch {}
});

test('render envelope shape matches DESIGN.md', () => {
  const env = {
    id: 'msg_01',
    from: 'codex:abcdef012345',
    to: 'claude:000000000001',
    body: 'hello peer',
    sent_at: '2026-04-17T09:55:12.345Z',
  };
  const text = renderEnvelope(env);
  assert.equal(text, '[from:codex:abcdef012345 · msg:msg_01 · 09:55Z] hello peer');
});

test('renderEnvelope strips control characters and ANSI from from/id/body (no TTY steering)', () => {
  // A hostile or malformed peer must not be able to smuggle a CR (early/extra
  // turn submit), an ANSI escape (cursor/screen control), or other control
  // characters into the rendered line typed into the live session. The adapter
  // owns the single submit-Enter; the rendered text must be control-char-free.
  const env = {
    id: 'msg_01\r/quit',
    from: 'codex:abc\x1b[31mEVIL',
    to: 'claude:000000000001',
    body: 'hello\r\nFAKE TURN\x1b[2J\x1b[H still here\t',
    sent_at: '2026-04-17T09:55:12.345Z',
  };
  const text = renderEnvelope(env);
  assert.ok(!text.includes('\r'), 'no CR survives');
  assert.ok(!text.includes('\n'), 'no LF survives');
  assert.ok(!text.includes('\x1b'), 'no ESC survives');
  assert.ok(!text.includes('\t'), 'no TAB survives');
  // The visible text remains, just neutralized to a single line.
  assert.ok(text.includes('still here'), 'body text preserved');
  // A clean message is untouched.
  assert.equal(
    renderEnvelope({ id: 'msg_2', from: 'codex:deadbeef0000', body: 'review the patch', sent_at: '2026-04-17T09:55:12.345Z' }),
    '[from:codex:deadbeef0000 · msg:msg_2 · 09:55Z] review the patch',
  );
});

test('adapter-socket delivery: accepts, renders to recorder, sends back accepted', async () => {
  process.env.AGENT_BUS_SOCKET = BROKER_SOCKET;
  process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR = ADAPTER_SOCKETS_DIR;
  const recorder = makeRecorder();
  const sessionId = 'session-A-' + crypto.randomBytes(4).toString('hex');
  const sidecar = startSidecar({
    write: recorder.write,
    sessionId,
    project: 'agent-bus',
    cwd: '/tmp',
    socketDir: ADAPTER_SOCKETS_DIR,
  });
  const { socketPath } = await sidecar.start();
  const auth = readAdapterToken(sessionId);

  const envelope = {
    id: 'msg_contract_ok',
    from: 'codex:deadbeef0000',
    to: 'claude:111111111111',
    body: 'hello from the test',
    sent_at: new Date().toISOString(),
  };
  const resp = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth, envelope }));
  assert.equal(resp.status, 'accepted');

  // The sidecar returns `accepted` immediately after writing the body, but the
  // Enter keystroke (\r) is submitted ~80 ms later to let Claude's TUI absorb
  // the typed characters first. Wait past that before asserting on the full
  // rendered text including the trailing \r.
  await new Promise(r => setTimeout(r, 200));

  const text = recorder.rendered();
  assert.match(text, /^\[from:codex:deadbeef0000 · msg:msg_contract_ok · /);
  assert.ok(text.includes(' hello from the test'), 'rendered body present: ' + JSON.stringify(text));
  assert.ok(text.endsWith('\r'), 'ends with CR for PTY submit');

  await sidecar.stop();
});

test('adapter dedupe: same envelope.id is rejected on the second delivery', async () => {
  process.env.AGENT_BUS_SOCKET = BROKER_SOCKET;
  process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR = ADAPTER_SOCKETS_DIR;
  const recorder = makeRecorder();
  const sessionId = 'session-B-' + crypto.randomBytes(4).toString('hex');
  const sidecar = startSidecar({
    write: recorder.write,
    sessionId,
    project: 'agent-bus',
    cwd: '/tmp',
    socketDir: ADAPTER_SOCKETS_DIR,
  });
  const { socketPath } = await sidecar.start();
  const auth = readAdapterToken(sessionId);

  const envelope = {
    id: 'msg_dupe_1',
    from: 'codex:000000000002',
    to: 'claude:222222222222',
    body: 'only once',
    sent_at: new Date().toISOString(),
  };
  const first = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth, envelope }));
  assert.equal(first.status, 'accepted');
  const second = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth, envelope }));
  assert.equal(second.status, 'rejected');
  assert.equal(second.reason, 'duplicate');
  // Recorder should only have one line.
  assert.equal(recorder.lines().length, 1);

  await sidecar.stop();
});

test('adapter rejects malformed deliveries without calling write', async () => {
  process.env.AGENT_BUS_SOCKET = BROKER_SOCKET;
  process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR = ADAPTER_SOCKETS_DIR;
  const recorder = makeRecorder();
  const sidecar = startSidecar({
    write: recorder.write,
    sessionId: 'session-C-' + crypto.randomBytes(4).toString('hex'),
    project: 'agent-bus',
    cwd: '/tmp',
    socketDir: ADAPTER_SOCKETS_DIR,
  });
  const { socketPath } = await sidecar.start();

  const bads = [
    { type: 'deliver' },                                          // no envelope
    { type: 'deliver', envelope: {} },                            // no id
    { type: 'deliver', envelope: { id: 'x' } },                   // no body
    { type: 'ping' },                                             // unknown type
  ];
  for (const bad of bads) {
    const resp = await sendLine(socketPath, JSON.stringify(bad));
    assert.equal(resp.status, 'rejected');
  }
  assert.equal(recorder.writes.length, 0, 'write never invoked for malformed deliveries');

  await sidecar.stop();
});

test('sidecar cleans up its delivery socket when broker registration fails', async () => {
  const previousSocket = process.env.AGENT_BUS_SOCKET;
  const socketDir = fs.mkdtempSync('/tmp/bus-ptysock-fail-');
  const sessionId = 'session-fail-' + crypto.randomBytes(4).toString('hex');
  const socketPath = path.join(socketDir, `${sessionId}.sock`);
  process.env.AGENT_BUS_SOCKET = path.join(socketDir, 'missing-broker.sock');

  const sidecar = startSidecar({
    write: () => {},
    sessionId,
    project: 'agent-bus',
    cwd: '/tmp',
    socketDir,
  });

  try {
    await assert.rejects(() => sidecar.start(), /ENOENT|ECONNREFUSED/);
    assert.equal(fs.existsSync(socketPath), false);
    await sidecar.stop();
  } finally {
    if (previousSocket == null) delete process.env.AGENT_BUS_SOCKET;
    else process.env.AGENT_BUS_SOCKET = previousSocket;
    try { fs.rmSync(socketDir, { recursive: true, force: true }); } catch {}
  }
});

test('end-to-end: broker routes a push to the adapter, recorder sees rendered envelope', async () => {
  process.env.AGENT_BUS_SOCKET = BROKER_SOCKET;
  process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR = ADAPTER_SOCKETS_DIR;
  const recorder = makeRecorder();
  const sidecar = startSidecar({
    write: recorder.write,
    sessionId: 'e2e-adapter-' + crypto.randomBytes(4).toString('hex'),
    project: 'agent-bus',
    cwd: '/tmp',
    socketDir: ADAPTER_SOCKETS_DIR,
  });
  await sidecar.start();

  const sender = new BusClient({ socketPath: BROKER_SOCKET });
  await sender.connect();
  const senderReg = await sender.register({
    kind: 'codex',
    sessionId: 'e2e-sender-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'codex-app-server', socketPath: '/nonexistent' },
  });

  const list = await sender.list();
  const claudeParticipant = list.participants.find(p => p.kind === 'claude');
  assert.ok(claudeParticipant);

  const result = await sender.send(claudeParticipant.participantId, 'hi from the bus', { id: 'msg_e2e_1' });
  assert.equal(result.status, 'delivered');
  assert.equal(result.from, senderReg.participantId);

  await new Promise(r => setTimeout(r, 200));
  const text = recorder.rendered();
  assert.match(text, /\[from:codex:[0-9a-f]{12} · msg:msg_e2e_1 · /);
  assert.ok(text.includes('hi from the bus'));

  await sender.unregister();
  await sidecar.stop();
});

test('sidecar local send uses the live claude participant identity', async () => {
  process.env.AGENT_BUS_SOCKET = BROKER_SOCKET;
  process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR = ADAPTER_SOCKETS_DIR;
  const recorder = makeRecorder();
  const sessionId = 'session-send-' + crypto.randomBytes(4).toString('hex');
  const sidecar = startSidecar({
    write: recorder.write,
    sessionId,
    project: 'agent-bus',
    cwd: '/tmp',
    socketDir: ADAPTER_SOCKETS_DIR,
  });
  const senderReg = await sidecar.start();
  const auth = readAdapterToken(sessionId);
  const recipient = await startRecipient('codex');

  const result = await sendLine(senderReg.socketPath, JSON.stringify({
    type: 'send',
    auth,
    to: recipient.participantId,
    body: 'claude outbound test',
    id: 'msg_sidecar_send_1',
  }));
  assert.equal(result.status, 'delivered');
  assert.equal(result.from, senderReg.participantId);

  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(recipient.envelopes.length, 1);
  assert.equal(recipient.envelopes[0].from, senderReg.participantId);
  assert.equal(recipient.envelopes[0].body, 'claude outbound test');

  await recipient.stop();
  await sidecar.stop();
});

test('unauthenticated peer cannot deliver into the live session (connection != authority)', async () => {
  // The core regression: a same-uid peer that can connect() to the 0600
  // delivery socket but does NOT hold the capability token must not be able to
  // forge a user turn into the wrapped session. Without a token, with a wrong
  // token, and with an empty token, {deliver} is rejected and write() never
  // fires. The legitimate (token-bearing) path is proven by the other tests.
  process.env.AGENT_BUS_SOCKET = BROKER_SOCKET;
  process.env.AGENT_BUS_ADAPTER_SOCKETS_DIR = ADAPTER_SOCKETS_DIR;
  const recorder = makeRecorder();
  const sessionId = 'session-unauth-' + crypto.randomBytes(4).toString('hex');
  const sidecar = startSidecar({
    write: recorder.write,
    sessionId,
    project: 'agent-bus',
    cwd: '/tmp',
    socketDir: ADAPTER_SOCKETS_DIR,
  });
  const { socketPath } = await sidecar.start();
  const goodToken = readAdapterToken(sessionId);

  const envelope = {
    id: 'msg_attacker',
    from: 'codex:deadbeef0000',
    to: 'claude:111111111111',
    body: 'forged turn',
    sent_at: new Date().toISOString(),
  };

  // No token.
  let r = await sendLine(socketPath, JSON.stringify({ type: 'deliver', envelope }));
  assert.equal(r.status, 'rejected');
  assert.equal(r.reason, 'unauthorized');

  // Wrong token (same length so the timing-safe compare path is exercised).
  r = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth: 'f'.repeat(goodToken.length), envelope }));
  assert.equal(r.status, 'rejected');
  assert.equal(r.reason, 'unauthorized');

  // Empty token.
  r = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth: '', envelope }));
  assert.equal(r.status, 'rejected');
  assert.equal(r.reason, 'unauthorized');

  // A privileged {send} without the token is rejected the same way.
  r = await sendLine(socketPath, JSON.stringify({ type: 'send', to: 'codex:000000000001', body: 'forged outbound' }));
  assert.equal(r.status, 'rejected');
  assert.equal(r.reason, 'unauthorized');

  // Nothing was ever written to the PTY.
  await new Promise((resolve) => setTimeout(resolve, 120));
  assert.equal(recorder.writes.length, 0, 'no characters written for unauthenticated deliveries');

  // And the legitimate token still works on the same socket.
  const ok = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth: goodToken, envelope: { ...envelope, id: 'msg_legit' } }));
  assert.equal(ok.status, 'accepted');

  await sidecar.stop();
});
