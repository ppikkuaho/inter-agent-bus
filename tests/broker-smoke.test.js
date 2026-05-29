// Smoke tests for the inter-agent bus broker.
// Spawns a broker subprocess against a scratch state dir, exercises clients
// against fake adapter sockets, and verifies envelope stamping + mailbox behavior.

'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const net = require('net');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const { BusClient } = require('../client/client');
const BROKER_JS = path.resolve(__dirname, '../broker/broker.js');
const BUS_CLI = path.resolve(__dirname, '../cli/bus');

const TMP_STATE = fs.mkdtempSync(path.join(os.tmpdir(), 'bus-smoke-'));
const SOCKET_PATH = path.join(TMP_STATE, 'broker.sock');

let broker;

async function waitForSocket(deadlineMs = 3000) {
  const stop = Date.now() + deadlineMs;
  while (Date.now() < stop) {
    if (fs.existsSync(SOCKET_PATH)) return;
    await new Promise(r => setTimeout(r, 30));
  }
  throw new Error('broker did not start; socket never appeared');
}

function runCli(args, env = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [BUS_CLI, ...args], {
      env: { ...process.env, ...env },
      cwd: os.homedir(),
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
    child.on('error', reject);
    child.on('exit', (code) => resolve({ code, stdout, stderr }));
  });
}

function makeFakeAdapterServer(socketPath, onEnvelope) {
  return new Promise((resolve) => {
    const server = net.createServer((socket) => {
      let buffer = '';
      socket.on('data', (chunk) => {
        buffer += chunk.toString();
        let nl;
        while ((nl = buffer.indexOf('\n')) !== -1) {
          const line = buffer.slice(0, nl);
          buffer = buffer.slice(nl + 1);
          if (!line.trim()) continue;
          try {
            const req = JSON.parse(line);
            if (req.type === 'deliver') {
              onEnvelope(req.envelope);
              socket.write(JSON.stringify({ status: 'accepted' }) + '\n');
            } else {
              socket.write(JSON.stringify({ status: 'rejected', reason: 'unknown_type' }) + '\n');
            }
          } catch {
            socket.write(JSON.stringify({ status: 'rejected', reason: 'parse_error' }) + '\n');
          }
        }
      });
      socket.on('error', () => {});
    });
    server.listen(socketPath, () => resolve(server));
  });
}

before(async () => {
  // These fixtures register against ad-hoc fake adapter sockets in the scratch
  // dir (not the canonical adapter-sockets root), so relax the broker's
  // delivery-socket binding to the always-on traversal hardening for the test
  // broker. Production never sets this. See broker.js resolveDeliverySocketPath.
  const env = {
    ...process.env,
    AGENT_BUS_STATE_DIR: TMP_STATE,
    AGENT_BUS_SOCKET: SOCKET_PATH,
    AGENT_BUS_ALLOW_UNBOUND_DELIVERY_SOCKET: '1',
  };
  broker = spawn('node', [BROKER_JS], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  broker.stdout.on('data', d => process.stdout.write('[broker] ' + d.toString()));
  broker.stderr.on('data', d => process.stderr.write('[broker] ' + d.toString()));
  await waitForSocket();
});

after(async () => {
  if (broker) {
    broker.kill('SIGTERM');
    await new Promise((resolve) => {
      let done = false;
      const finish = () => { if (!done) { done = true; resolve(); } };
      broker.once('exit', finish);
      setTimeout(finish, 2000).unref();
    });
  }
  try { fs.rmSync(TMP_STATE, { recursive: true, force: true }); } catch {}
});

test('socket mode is 0600', () => {
  const st = fs.statSync(SOCKET_PATH);
  const mode = st.mode & 0o777;
  assert.equal(mode, 0o600);
});

test('register + list + send + receive with broker-stamped from', async () => {
  const recvSocketPath = path.join(TMP_STATE, 'recv.sock');
  const senderSocketPath = path.join(TMP_STATE, 'send.sock');
  const received = [];
  const sentBack = [];
  const recvServer = await makeFakeAdapterServer(recvSocketPath, (e) => received.push(e));
  const sendServer = await makeFakeAdapterServer(senderSocketPath, (e) => sentBack.push(e));

  const sender = new BusClient({ socketPath: SOCKET_PATH });
  await sender.connect();
  const senderReg = await sender.register({
    kind: 'claude',
    sessionId: 'sender-' + crypto.randomBytes(4).toString('hex'),
    project: 'agent-bus',
    cwd: '/tmp',
    delivery: { adapter: 'claude-pty', socketPath: senderSocketPath },
  });
  assert.match(senderReg.participantId, /^claude:[0-9a-f]{12}$/);
  assert.ok(senderReg.leaseToken.startsWith('lease_'));

  const receiver = new BusClient({ socketPath: SOCKET_PATH });
  await receiver.connect();
  const recvReg = await receiver.register({
    kind: 'codex',
    sessionId: 'recv-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'codex-app-server', socketPath: recvSocketPath },
  });
  assert.match(recvReg.participantId, /^codex:[0-9a-f]{12}$/);

  const listResp = await sender.list();
  const ids = new Set(listResp.participants.map(p => p.participantId));
  assert.ok(ids.has(senderReg.participantId));
  assert.ok(ids.has(recvReg.participantId));
  // Delivery.socketPath must not leak via list
  for (const p of listResp.participants) {
    assert.equal(p.delivery.socketPath, undefined);
  }

  const out1 = await sender.send(recvReg.participantId, 'hello codex');
  assert.equal(out1.status, 'delivered');
  assert.equal(out1.from, senderReg.participantId);

  await new Promise(r => setTimeout(r, 50));
  assert.equal(received.length, 1);
  assert.equal(received[0].body, 'hello codex');
  assert.equal(received[0].from, senderReg.participantId);
  assert.equal(received[0].to, recvReg.participantId);

  // Reverse direction
  const out2 = await receiver.send(senderReg.participantId, 'hi claude');
  assert.equal(out2.status, 'delivered');
  await new Promise(r => setTimeout(r, 50));
  assert.equal(sentBack.length, 1);
  assert.equal(sentBack[0].from, recvReg.participantId);

  // Broker-stamped from cannot be forged
  const spoofed = await sender._send({
    type: 'push_message',
    to: recvReg.participantId,
    leaseToken: sender.leaseToken,
    message: {
      id: 'msg_spoof',
      body: 'pretending to be someone else',
      from: 'evil:dead',
      sent_at: new Date().toISOString(),
    },
  });
  assert.equal(spoofed.status, 'delivered');
  await new Promise(r => setTimeout(r, 50));
  const last = received[received.length - 1];
  assert.equal(last.from, senderReg.participantId);

  await sender.unregister();
  await receiver.unregister();
  recvServer.close();
  sendServer.close();
});

test('list + registry preserve participant displayName and description', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  const reg = await c.register({
    kind: 'claude',
    sessionId: 'named-' + crypto.randomBytes(4).toString('hex'),
    displayName: 'claude-planner',
    description: 'Coordinates implementation with Codex',
    project: 'agent-bus',
    cwd: '/tmp',
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  assert.equal(reg.participantId, 'claude-planner');

  const list = await c.list();
  const listed = list.participants.find((participant) => participant.participantId === reg.participantId);
  assert.ok(listed);
  assert.equal(listed.displayName, 'claude-planner');
  assert.equal(listed.description, 'Coordinates implementation with Codex');

  await new Promise(r => setTimeout(r, 30));
  const snap = JSON.parse(fs.readFileSync(path.join(TMP_STATE, 'registry.json'), 'utf8'));
  const persisted = snap.participants.find((participant) => participant.participantId === reg.participantId);
  assert.ok(persisted);
  assert.equal(persisted.displayName, 'claude-planner');
  assert.equal(persisted.description, 'Coordinates implementation with Codex');

  await c.unregister();
});

test('displayName is slugged into a stable human bus address', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  const reg = await c.register({
    kind: 'codex',
    sessionId: 'slugged-' + crypto.randomBytes(4).toString('hex'),
    displayName: 'Codex Architecture',
    description: 'Owns architecture questions',
    delivery: { adapter: 'codex-pty', socketPath: '/nonexistent' },
  });
  assert.equal(reg.participantId, 'codex-architecture');

  const list = await c.list();
  const listed = list.participants.find((participant) => participant.participantId === 'codex-architecture');
  assert.ok(listed);
  assert.equal(listed.displayName, 'Codex Architecture');

  await c.unregister();
});

test('two live sessions cannot register the same human bus address', async () => {
  const first = new BusClient({ socketPath: SOCKET_PATH });
  await first.connect();
  await first.register({
    kind: 'claude',
    sessionId: 'collision-a-' + crypto.randomBytes(4).toString('hex'),
    displayName: 'shared-handle',
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });

  const second = new BusClient({ socketPath: SOCKET_PATH });
  await second.connect();
  await assert.rejects(
    () => second.register({
      kind: 'codex',
      sessionId: 'collision-b-' + crypto.randomBytes(4).toString('hex'),
      displayName: 'shared-handle',
      delivery: { adapter: 'codex-pty', socketPath: '/nonexistent' },
    }),
    /participant_id_in_use/
  );

  await first.unregister();
  await second.unregister();
});

test('cli resolves broker socket from AGENT_BUS_STATE_DIR when AGENT_BUS_SOCKET is unset', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  const reg = await c.register({
    kind: 'claude',
    sessionId: 'state-dir-cli-' + crypto.randomBytes(4).toString('hex'),
    displayName: 'state-dir-check',
    description: 'Verifies client socket derivation',
    project: 'agent-bus',
    cwd: '/tmp',
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });

  const result = await runCli(['list'], {
    BUS_JSON: '1',
    AGENT_BUS_STATE_DIR: TMP_STATE,
    AGENT_BUS_SOCKET: '',
  });
  assert.equal(result.code, 0, result.stderr);
  const participants = JSON.parse(result.stdout);
  const listed = participants.find((participant) => participant.participantId === reg.participantId);
  assert.ok(listed, result.stdout);
  assert.equal(listed.displayName, 'state-dir-check');

  await c.unregister();
});

test('push to offline recipient queues to mailbox; flushes on register', async () => {
  const senderSocketPath = path.join(TMP_STATE, 'send2.sock');
  const senderServer = await makeFakeAdapterServer(senderSocketPath, () => {});

  const sender = new BusClient({ socketPath: SOCKET_PATH });
  await sender.connect();
  await sender.register({
    kind: 'claude',
    sessionId: 'sender2-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: senderSocketPath },
  });

  const futureRecvSessionId = 'future-recv-' + crypto.randomBytes(4).toString('hex');
  // Mirror the broker's shortId (12 hex) to predict the future participantId.
  const expectedId = 'codex:' + crypto.createHash('sha256').update(futureRecvSessionId).digest('hex').slice(0, 12);

  // First, make sure the broker has observed this sessionId — otherwise §1
  // scope rightly refuses to queue. We register-and-immediately-unregister a
  // short-lived presence so the id enters recentParticipants.
  {
    const warmer = new BusClient({ socketPath: SOCKET_PATH });
    await warmer.connect();
    const warmReg = await warmer.register({
      kind: 'codex',
      sessionId: futureRecvSessionId,
      delivery: { adapter: 'codex-app-server', socketPath: '/nonexistent' },
    });
    assert.equal(warmReg.participantId, expectedId);
    await warmer.unregister();
  }

  const queued = await sender.send(expectedId, 'waiting for you');
  assert.equal(queued.status, 'queued');

  const recvSocketPath = path.join(TMP_STATE, 'recv2.sock');
  const received = [];
  const recvServer = await makeFakeAdapterServer(recvSocketPath, (e) => received.push(e));

  const receiver = new BusClient({ socketPath: SOCKET_PATH });
  await receiver.connect();
  const recvReg = await receiver.register({
    kind: 'codex',
    sessionId: futureRecvSessionId,
    delivery: { adapter: 'codex-app-server', socketPath: recvSocketPath },
  });
  assert.equal(recvReg.participantId, expectedId);

  // Give broker time to async-flush mailbox
  await new Promise(r => setTimeout(r, 200));
  assert.equal(received.length, 1);
  assert.equal(received[0].body, 'waiting for you');

  await sender.unregister();
  await receiver.unregister();
  recvServer.close();
  senderServer.close();
});

test('heartbeat with bogus lease fails', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  await assert.rejects(
    () => c._send({ type: 'heartbeat', leaseToken: 'lease_bogus' }),
    /invalid_lease/
  );
  if (c.socket) c.socket.end();
});

test('push with bogus lease fails', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  await assert.rejects(
    () => c._send({
      type: 'push_message',
      to: 'claude:dead',
      leaseToken: 'lease_bogus',
      message: { id: 'x', body: 'y' },
    }),
    /invalid_or_expired_lease/
  );
  if (c.socket) c.socket.end();
});

test('unregister removes participant from list', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  const reg = await c.register({
    kind: 'claude',
    sessionId: 'temp-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  let list = await c.list();
  assert.ok(list.participants.some(p => p.participantId === reg.participantId));
  await c.unregister();

  const c2 = new BusClient({ socketPath: SOCKET_PATH });
  await c2.connect();
  list = await c2.list();
  assert.ok(!list.participants.some(p => p.participantId === reg.participantId));
  if (c2.socket) c2.socket.end();
});

test('registry.json is persisted and excludes lease tokens + socketPath', async () => {
  const secretPath = '/tmp/very-secret-adapter-' + crypto.randomBytes(4).toString('hex') + '.sock';
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  await c.register({
    kind: 'claude',
    sessionId: 'persist-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: secretPath, threadId: 'secret-thread' },
  });
  await new Promise(r => setTimeout(r, 30));
  const snapText = fs.readFileSync(path.join(TMP_STATE, 'registry.json'), 'utf8');
  const snap = JSON.parse(snapText);
  assert.ok(Array.isArray(snap.participants));
  for (const p of snap.participants) assert.equal(p.leaseToken, undefined);
  // Neither socketPath nor threadId may ever reach disk via the registry snapshot.
  assert.ok(!snapText.includes(secretPath));
  assert.ok(!snapText.includes('secret-thread'));
  await c.unregister();
});

test('malformed `to` participantId is rejected (closes path traversal)', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  await c.register({
    kind: 'claude',
    sessionId: 'trav-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  for (const bad of ['../../../../etc/passwd', '..:abcd', 'codex:../owned', 'codex:ZZZZZZZZZZZZ', '', 'BadName', '-startsbad', 'has.dot']) {
    await assert.rejects(
      () => c.send(bad, 'payload'),
      (err) => /invalid_to_participant_id/.test(err.message)
    );
  }
  // And no mailbox file should have been written under a traversal path.
  const listing = fs.existsSync(path.join(TMP_STATE, 'mailbox')) ? fs.readdirSync(path.join(TMP_STATE, 'mailbox')) : [];
  for (const f of listing) assert.match(f, /^[0-9a-f]{64}\.jsonl$/);
  await c.unregister();
});

test('push to never-seen participant returns failed, not queued', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  await c.register({
    kind: 'claude',
    sessionId: 'never-seen-sender-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  // A syntactically-valid but never-registered id.
  const bogus = 'codex:' + crypto.randomBytes(6).toString('hex');
  const result = await c.send(bogus, 'anyone there?');
  assert.equal(result.status, 'failed');
  assert.equal(result.reason, 'no_such_participant');
  await c.unregister();
});

test('re-register preserves identity continuity once the prior lease is gone', async () => {
  // Legitimate silent re-register: the prior presence is no longer live (it
  // unregistered, or in production the broker restarted / the lease expired).
  // The durable invariant DESIGN.md requires is identity continuity — the
  // session gets the SAME participantId back and a fresh working lease — so
  // peers and queued mail still address it correctly. (The `reclaimed` flag is
  // true only on the expired-but-not-yet-GC'd branch where the old record is
  // still in memory; after a clean unregister the record is gone and the same
  // deterministic id is re-minted with reclaimed=false. Both preserve identity.)
  // Reclaiming a STILL-LIVE lease is the refused path — see the test below.
  const sessionId = 'reclaim-' + crypto.randomBytes(4).toString('hex');
  const first = new BusClient({ socketPath: SOCKET_PATH });
  await first.connect();
  const reg1 = await first.register({
    kind: 'claude',
    sessionId,
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  assert.equal(reg1.reclaimed, false);
  // Release the prior lease so the session is no longer live (the legitimate
  // precondition for reclaim — auto-reregister only fires after broker loss or
  // lease expiry, never against a live lease).
  await first.unregister();

  const second = new BusClient({ socketPath: SOCKET_PATH });
  await second.connect();
  const reg2 = await second.register({
    kind: 'claude',
    sessionId,
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  assert.equal(reg2.participantId, reg1.participantId, 'identity continuity preserved');
  assert.notEqual(reg2.leaseToken, reg1.leaseToken, 'a fresh lease is issued');
  // The fresh lease works.
  const hb = await second._send({ type: 'heartbeat', leaseToken: reg2.leaseToken });
  assert.equal(hb.status, 'ok');

  await second.unregister();
});

test('re-register is refused while the prior lease is still live (no identity hijack)', async () => {
  // Hardening regression: sessionId is not a peer secret. A same-uid peer that
  // learns/guesses a victim's sessionId must NOT be able to re-register it,
  // which would invalidate the victim's live lease and steal its identity. The
  // broker refuses reclaim while the prior lease is still valid.
  const sessionId = 'reclaim-live-' + crypto.randomBytes(4).toString('hex');
  const victim = new BusClient({ socketPath: SOCKET_PATH });
  await victim.connect();
  const vReg = await victim.register({
    kind: 'claude',
    sessionId,
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });

  const attacker = new BusClient({ socketPath: SOCKET_PATH });
  await attacker.connect();
  await assert.rejects(
    () => attacker.register({
      kind: 'claude',
      sessionId,
      delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
    }),
    /session_lease_active/,
  );
  if (attacker.socket) attacker.socket.end();

  // The victim's lease is untouched and still works.
  const hb = await victim._send({ type: 'heartbeat', leaseToken: vReg.leaseToken });
  assert.equal(hb.status, 'ok');

  await victim.unregister();
});

test('re-register with same sessionId but different kind is rejected', async () => {
  const sessionId = 'kindmismatch-' + crypto.randomBytes(4).toString('hex');
  const a = new BusClient({ socketPath: SOCKET_PATH });
  await a.connect();
  await a.register({
    kind: 'claude',
    sessionId,
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  const b = new BusClient({ socketPath: SOCKET_PATH });
  await b.connect();
  await assert.rejects(
    () => b.register({
      kind: 'codex',
      sessionId,
      delivery: { adapter: 'codex-app-server', socketPath: '/nonexistent' },
    }),
    /kind_mismatch_for_sessionId/
  );
  await a.unregister();
  // b.register() threw — pass-5 client no longer commits participant on reject,
  // so unregister() is a no-op RPC-wise but still sets _closed + closes the socket.
  await b.unregister();
});

test('second broker on same socket refuses to start (no split-brain)', async () => {
  // Try launching a second broker against the same running socket.
  const env = { ...process.env, AGENT_BUS_STATE_DIR: TMP_STATE, AGENT_BUS_SOCKET: SOCKET_PATH };
  const proc = spawn('node', [path.resolve(__dirname, '../broker/broker.js')], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  proc.stderr.on('data', d => { stderr += d.toString(); });
  const exitCode = await new Promise(resolve => proc.on('exit', (code) => resolve(code)));
  assert.equal(exitCode, 2, 'second broker must exit with code 2');
  assert.match(stderr, /another broker is live/);
});

test('state dir mode is 0700', () => {
  const st = fs.statSync(TMP_STATE);
  assert.equal(st.mode & 0o777, 0o700);
});

test('mailbox flush preserves FIFO against live pushes (reattach boundary)', async () => {
  // Scenario: queue a message to an offline recipient, they reconnect,
  // immediately a live message is pushed. Recipient must see mail-first,
  // live-second — never reversed.
  const senderSocketPath = path.join(TMP_STATE, 'fifo-send.sock');
  const senderServer = await makeFakeAdapterServer(senderSocketPath, () => {});

  const sender = new BusClient({ socketPath: SOCKET_PATH });
  await sender.connect();
  await sender.register({
    kind: 'claude',
    sessionId: 'fifo-sender-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: senderSocketPath },
  });

  const recvSessionId = 'fifo-recv-' + crypto.randomBytes(4).toString('hex');
  const recvExpectedId = 'codex:' + crypto.createHash('sha256').update(recvSessionId).digest('hex').slice(0, 12);

  // Warmer registration so recipient is in recentParticipants.
  {
    const warmer = new BusClient({ socketPath: SOCKET_PATH });
    await warmer.connect();
    await warmer.register({
      kind: 'codex',
      sessionId: recvSessionId,
      delivery: { adapter: 'codex-app-server', socketPath: '/nonexistent' },
    });
    await warmer.unregister();
  }

  // Queue three offline messages.
  for (let i = 1; i <= 3; i++) {
    const r = await sender.send(recvExpectedId, `queued-${i}`, { id: `q${i}` });
    assert.equal(r.status, 'queued');
  }

  // Recipient comes online with a slow-accepting adapter that forwards take
  // ~30 ms each, so we have real race pressure with interleaved live pushes.
  const received = [];
  const recvSocketPath = path.join(TMP_STATE, 'fifo-recv.sock');
  const recvServer = net.createServer((socket) => {
    let buf = '';
    socket.on('data', (chunk) => {
      buf += chunk.toString();
      let nl;
      while ((nl = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (!line.trim()) continue;
        try {
          const req = JSON.parse(line);
          if (req.type === 'deliver') {
            // Simulate slow adapter processing.
            setTimeout(() => {
              received.push(req.envelope);
              socket.write(JSON.stringify({ status: 'accepted' }) + '\n');
            }, 30);
          }
        } catch {
          socket.write(JSON.stringify({ status: 'rejected' }) + '\n');
        }
      }
    });
    socket.on('error', () => {});
  });
  await new Promise(r => recvServer.listen(recvSocketPath, r));

  const receiver = new BusClient({ socketPath: SOCKET_PATH });
  await receiver.connect();
  await receiver.register({
    kind: 'codex',
    sessionId: recvSessionId,
    delivery: { adapter: 'codex-app-server', socketPath: recvSocketPath },
  });

  // Immediately push live messages — before the mailbox flush could have finished.
  for (let i = 1; i <= 3; i++) {
    await sender.send(recvExpectedId, `live-${i}`, { id: `l${i}` });
  }

  // Wait for all 6 to land (flush + live).
  const deadline = Date.now() + 3000;
  while (received.length < 6 && Date.now() < deadline) await new Promise(r => setTimeout(r, 20));
  assert.equal(received.length, 6, `expected 6 messages, got ${received.length}`);

  const order = received.map(e => e.body);
  // All queued must precede all live.
  const firstLiveIdx = order.findIndex(b => b.startsWith('live-'));
  const lastQueuedIdx = order.map((b, i) => b.startsWith('queued-') ? i : -1).filter(i => i >= 0).pop();
  assert.ok(firstLiveIdx > lastQueuedIdx, `FIFO violated: order=${JSON.stringify(order)}`);
  // And each group is itself in order.
  const queuedOrder = order.filter(b => b.startsWith('queued-'));
  const liveOrder = order.filter(b => b.startsWith('live-'));
  assert.deepEqual(queuedOrder, ['queued-1', 'queued-2', 'queued-3']);
  assert.deepEqual(liveOrder, ['live-1', 'live-2', 'live-3']);

  await sender.unregister();
  await receiver.unregister();
  recvServer.close();
  senderServer.close();
});

test('list_participants never exposes sessionId (reclaim-key confidentiality)', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  const reg = await c.register({
    kind: 'claude',
    sessionId: 'no-leak-sessionid-' + crypto.randomBytes(6).toString('hex'),
    displayName: 'no-leak-check',
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });
  const list = await c.list();
  for (const p of list.participants) {
    assert.equal('sessionId' in p, false, 'sessionId must not appear in list output');
  }
  const me = list.participants.find(p => p.participantId === reg.participantId);
  assert.ok(me);
  await c.unregister();
});

test('broker forwards the registered adapterToken on deliver and never leaks it', async () => {
  // The deliver-path auth handshake: a participant registers an adapterToken;
  // the broker must present it as `auth` on every {deliver} so the adapter can
  // distinguish a broker-originated delivery from an unauthenticated same-uid
  // peer. The token must NOT appear in list_participants or registry.json.
  const recvSocketPath = path.join(TMP_STATE, 'authtok-recv.sock');
  const frames = [];
  const recvServer = await makeFakeAdapterServer(recvSocketPath, () => {});
  // makeFakeAdapterServer only records envelopes; we need the whole frame, so
  // wrap a dedicated server that captures `auth`.
  recvServer.close();
  const captureServer = net.createServer((socket) => {
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk.toString();
      let nl;
      while ((nl = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, nl);
        buffer = buffer.slice(nl + 1);
        if (!line.trim()) continue;
        const req = JSON.parse(line);
        frames.push(req);
        socket.write(JSON.stringify({ status: 'accepted' }) + '\n');
      }
    });
    socket.on('error', () => {});
  });
  await new Promise((r) => captureServer.listen(recvSocketPath, r));

  const token = crypto.randomBytes(32).toString('hex');
  const recv = new BusClient({ socketPath: SOCKET_PATH });
  await recv.connect();
  const recvReg = await recv.register({
    kind: 'codex',
    sessionId: 'authtok-recv-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'codex-app-server', socketPath: recvSocketPath, adapterToken: token },
  });

  const sender = new BusClient({ socketPath: SOCKET_PATH });
  await sender.connect();
  await sender.register({
    kind: 'claude',
    sessionId: 'authtok-send-' + crypto.randomBytes(4).toString('hex'),
    delivery: { adapter: 'claude-pty', socketPath: '/nonexistent' },
  });

  const out = await sender.send(recvReg.participantId, 'hello with auth', { id: 'msg_authtok_1' });
  assert.equal(out.status, 'delivered');
  await new Promise((r) => setTimeout(r, 80));
  assert.equal(frames.length, 1);
  assert.equal(frames[0].type, 'deliver');
  assert.equal(frames[0].auth, token, 'broker presents the registered adapterToken');

  // Never leaks via list.
  const list = await sender.list();
  for (const p of list.participants) {
    assert.equal(p.delivery.adapterToken, undefined);
    assert.ok(!JSON.stringify(p).includes(token), 'token must not appear in list output');
  }
  // Never leaks via registry.json.
  await new Promise((r) => setTimeout(r, 30));
  const snapText = fs.readFileSync(path.join(TMP_STATE, 'registry.json'), 'utf8');
  assert.ok(!snapText.includes(token), 'token must not be persisted to registry.json');

  await sender.unregister();
  await recv.unregister();
  captureServer.close();
});

test('register rejects a malformed adapterToken', async () => {
  const c = new BusClient({ socketPath: SOCKET_PATH });
  await c.connect();
  await assert.rejects(
    () => c.register({
      kind: 'claude',
      sessionId: 'badtok-' + crypto.randomBytes(4).toString('hex'),
      delivery: { adapter: 'claude-pty', socketPath: '/nonexistent', adapterToken: 'short' },
    }),
    /invalid_adapter_token/,
  );
  if (c.socket) c.socket.end();
});

test('strict delivery-socket binding rejects untrusted paths, accepts bound path', async () => {
  // A dedicated broker WITHOUT the test escape hatch, so the full trusted-root +
  // sessionId binding is enforced (production behavior). Proves a same-UID peer
  // cannot register a participant whose delivery.socketPath points at an
  // arbitrary socket the broker would later dial.
  const strictState = fs.mkdtempSync(path.join(os.tmpdir(), 'bus-strict-'));
  const strictSocket = path.join(strictState, 'broker.sock');
  const adapterDir = fs.mkdtempSync('/tmp/bus-strict-adapters-');
  const env = {
    ...process.env,
    AGENT_BUS_STATE_DIR: strictState,
    AGENT_BUS_SOCKET: strictSocket,
    AGENT_BUS_ADAPTER_SOCKETS_DIR: adapterDir,
  };
  delete env.AGENT_BUS_ALLOW_UNBOUND_DELIVERY_SOCKET;
  const strict = spawn('node', [path.resolve(__dirname, '../broker/broker.js')], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  try {
    const stop = Date.now() + 3000;
    while (Date.now() < stop && !fs.existsSync(strictSocket)) await new Promise(r => setTimeout(r, 30));
    assert.ok(fs.existsSync(strictSocket), 'strict broker started');

    const sid = 'strict-sess-' + crypto.randomBytes(6).toString('hex');

    const c1 = new BusClient({ socketPath: strictSocket });
    await c1.connect();
    await assert.rejects(
      () => c1.register({ kind: 'claude', sessionId: sid, delivery: { adapter: 'claude-pty', socketPath: '/tmp/evil-peer.sock' } }),
      /invalid_delivery_socket_path/,
    );
    if (c1.socket) c1.socket.end();

    const c2 = new BusClient({ socketPath: strictSocket });
    await c2.connect();
    await assert.rejects(
      () => c2.register({ kind: 'claude', sessionId: sid, delivery: { adapter: 'claude-pty', socketPath: path.join(adapterDir, '..', '..', 'etc', 'x.sock') } }),
      /invalid_delivery_socket_path/,
    );
    if (c2.socket) c2.socket.end();

    const c3 = new BusClient({ socketPath: strictSocket });
    await c3.connect();
    const reg = await c3.register({
      kind: 'claude',
      sessionId: sid,
      delivery: { adapter: 'claude-pty', socketPath: path.join(adapterDir, `${sid}.sock`) },
    });
    assert.equal(reg.status, 'ok');
    await c3.unregister();
  } finally {
    strict.kill('SIGTERM');
    await new Promise((resolve) => { strict.once('exit', resolve); setTimeout(resolve, 2000).unref(); });
    try { fs.rmSync(strictState, { recursive: true, force: true }); } catch {}
    try { fs.rmSync(adapterDir, { recursive: true, force: true }); } catch {}
  }
});
