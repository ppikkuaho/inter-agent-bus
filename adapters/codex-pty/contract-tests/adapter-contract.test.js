// Contract tests for the codex-pty adapter.

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

// Privileged adapter ops ({deliver}, {send}) now require the per-session
// capability token. These direct-socket tests read it from the 0600 token file
// the sidecar publishes on start(), exactly as the CLI does.
function readAdapterToken(sessionId) {
  return sessionAuth.readToken(ADAPTER_SOCKETS_DIR, sessionId);
}

const TMP_STATE = fs.mkdtempSync(path.join(os.tmpdir(), 'bus-codex-pty-contract-'));
const BROKER_SOCKET = path.join(TMP_STATE, 'broker.sock');
const ADAPTER_SOCKETS_DIR = fs.mkdtempSync('/tmp/bus-codex-ptysock-');
let broker;

async function waitForSocket(socketPath, deadlineMs = 3000) {
  const stop = Date.now() + deadlineMs;
  while (Date.now() < stop) {
    if (fs.existsSync(socketPath)) return;
    await new Promise((resolve) => setTimeout(resolve, 30));
  }
  throw new Error('socket never appeared: ' + socketPath);
}

function sendLine(socketPath, line) {
  return new Promise((resolve, reject) => {
    const connection = net.createConnection(socketPath);
    let buffer = '';
    connection.on('connect', () => connection.write(line + '\n'));
    connection.on('data', (chunk) => {
      buffer += chunk.toString();
      const nl = buffer.indexOf('\n');
      if (nl !== -1) {
        try { resolve(JSON.parse(buffer.slice(0, nl))); }
        catch (err) { reject(err); }
        connection.end();
      }
    });
    connection.on('error', reject);
    setTimeout(() => {
      connection.destroy();
      reject(new Error('timeout'));
    }, 2000).unref();
  });
}

async function startRecipient(kind = 'claude') {
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
  broker.stdout.on('data', (d) => process.stdout.write('[broker] ' + d.toString()));
  broker.stderr.on('data', (d) => process.stderr.write('[broker] ' + d.toString()));
  await waitForSocket(BROKER_SOCKET);
});

after(async () => {
  if (broker) {
    broker.kill('SIGTERM');
    await new Promise((resolve) => {
      let done = false;
      const finish = () => {
        if (!done) {
          done = true;
          resolve();
        }
      };
      broker.once('exit', finish);
      setTimeout(finish, 2000).unref();
    });
  }
  try { fs.rmSync(TMP_STATE, { recursive: true, force: true }); } catch {}
  try { fs.rmSync(ADAPTER_SOCKETS_DIR, { recursive: true, force: true }); } catch {}
});

test('render envelope shape matches current bus convention', () => {
  const envelope = {
    id: 'msg_01',
    from: 'claude:abcdef012345',
    to: 'codex:000000000001',
    body: 'hello peer',
    sent_at: '2026-04-17T09:55:12.345Z',
  };
  assert.equal(
    renderEnvelope(envelope),
    '[from:claude:abcdef012345 · msg:msg_01 · 09:55Z] hello peer',
  );
});

test('adapter-socket delivery writes rendered text and submits CR', async () => {
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
    from: 'claude:deadbeef0000',
    to: 'codex:111111111111',
    body: 'hello from the test',
    sent_at: new Date().toISOString(),
  };
  const response = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth, envelope }));
  assert.equal(response.status, 'accepted');

  await new Promise((resolve) => setTimeout(resolve, 200));

  const text = recorder.rendered();
  assert.match(text, /^\[from:claude:deadbeef0000 · msg:msg_contract_ok · /);
  assert.ok(text.includes(' hello from the test'), 'rendered body present: ' + JSON.stringify(text));
  assert.ok(text.endsWith('\r'), 'ends with CR for PTY submit');

  await sidecar.stop();
});

test('adapter dedupe rejects the second delivery for the same envelope id', async () => {
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
    from: 'claude:000000000002',
    to: 'codex:222222222222',
    body: 'only once',
    sent_at: new Date().toISOString(),
  };
  const first = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth, envelope }));
  assert.equal(first.status, 'accepted');
  const second = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth, envelope }));
  assert.equal(second.status, 'rejected');
  assert.equal(second.reason, 'duplicate');
  assert.equal(recorder.lines().length, 1);

  await sidecar.stop();
});

test('adapter rejects malformed deliveries without writing to the PTY', async () => {
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

  const badPayloads = [
    { type: 'deliver' },
    { type: 'deliver', envelope: {} },
    { type: 'deliver', envelope: { id: 'x' } },
    { type: 'ping' },
  ];
  for (const payload of badPayloads) {
    const response = await sendLine(socketPath, JSON.stringify(payload));
    assert.equal(response.status, 'rejected');
  }
  assert.equal(recorder.writes.length, 0);

  await sidecar.stop();
});

test('sidecar cleans up its delivery socket when broker registration fails', async () => {
  const previousSocket = process.env.AGENT_BUS_SOCKET;
  const socketDir = fs.mkdtempSync('/tmp/bus-codex-ptysock-fail-');
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

test('end-to-end: broker routes a push to the codex PTY sidecar', async () => {
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
    kind: 'claude',
    sessionId: 'e2e-sender-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });

  const list = await sender.list();
  const codexParticipant = list.participants.find((participant) => participant.kind === 'codex');
  assert.ok(codexParticipant);

  const result = await sender.send(codexParticipant.participantId, 'hi from the bus', { id: 'msg_e2e_1' });
  assert.equal(result.status, 'delivered');
  assert.equal(result.from, senderReg.participantId);

  await new Promise((resolve) => setTimeout(resolve, 200));
  const text = recorder.rendered();
  assert.match(text, /\[from:claude:[0-9a-f]{12} · msg:msg_e2e_1 · /);
  assert.ok(text.includes('hi from the bus'));

  await sender.unregister();
  await sidecar.stop();
});

test('sidecar local send uses the live codex participant identity', async () => {
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
  const recipient = await startRecipient('claude');

  const result = await sendLine(senderReg.socketPath, JSON.stringify({
    type: 'send',
    auth,
    to: recipient.participantId,
    body: 'codex outbound test',
    id: 'msg_sidecar_send_1',
  }));
  assert.equal(result.status, 'delivered');
  assert.equal(result.from, senderReg.participantId);

  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(recipient.envelopes.length, 1);
  assert.equal(recipient.envelopes[0].from, senderReg.participantId);
  assert.equal(recipient.envelopes[0].body, 'codex outbound test');

  await recipient.stop();
  await sidecar.stop();
});

test('unauthenticated peer cannot deliver into the live codex session (connection != authority)', async () => {
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
    from: 'claude:deadbeef0000',
    to: 'codex:111111111111',
    body: 'forged turn',
    sent_at: new Date().toISOString(),
  };

  let response = await sendLine(socketPath, JSON.stringify({ type: 'deliver', envelope }));
  assert.equal(response.status, 'rejected');
  assert.equal(response.reason, 'unauthorized');

  response = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth: 'f'.repeat(goodToken.length), envelope }));
  assert.equal(response.status, 'rejected');
  assert.equal(response.reason, 'unauthorized');

  response = await sendLine(socketPath, JSON.stringify({ type: 'send', to: 'claude:000000000001', body: 'forged outbound' }));
  assert.equal(response.status, 'rejected');
  assert.equal(response.reason, 'unauthorized');

  await new Promise((resolve) => setTimeout(resolve, 120));
  assert.equal(recorder.writes.length, 0, 'no characters written for unauthenticated deliveries');

  const ok = await sendLine(socketPath, JSON.stringify({ type: 'deliver', auth: goodToken, envelope: { ...envelope, id: 'msg_legit' } }));
  assert.equal(ok.status, 'accepted');

  await sidecar.stop();
});
