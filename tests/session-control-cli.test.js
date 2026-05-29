'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const { createSessionLogger, startControlServer } = require('../adapters/session-control');
const sessionAuth = require('../adapters/session-auth');

const CLI = path.resolve(__dirname, '../cli/bus');

// Minimal direct request to a control socket (one request, one response), used
// by the auth regression test to bypass the CLI's automatic token attachment.
function controlRequest(socketPath, req) {
  return new Promise((resolve, reject) => {
    const c = require('node:net').createConnection(socketPath);
    let buffer = '';
    c.on('connect', () => c.write(JSON.stringify(req) + '\n'));
    c.on('data', (chunk) => {
      buffer += chunk.toString();
      const nl = buffer.indexOf('\n');
      if (nl === -1) return;
      try { resolve(JSON.parse(buffer.slice(0, nl))); }
      catch (err) { reject(err); }
      c.end();
    });
    c.on('error', reject);
    setTimeout(() => { c.destroy(); reject(new Error('timeout')); }, 2000).unref();
  });
}

function runCli(args, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [CLI, ...args], {
      env: { ...process.env, ...env },
      cwd: os.homedir(),
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
    child.on('error', reject);
    child.on('exit', (code) => resolve({ code, stdout, stderr }));
  });
}

async function createFixture({ kind = 'codex', adapter = 'codex-pty', enabled = false, lastError = null } = {}) {
  const sessionId = 'session-' + crypto.randomBytes(4).toString('hex');
  const participantId = `${kind}:${crypto.randomBytes(6).toString('hex')}`;
  const controlDir = fs.mkdtempSync('/tmp/bus-control-');
  const adapterDir = fs.mkdtempSync('/tmp/bus-adapter-');
  const logDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bus-control-log-'));
  const logger = createSessionLogger({ sessionId, label: 'bus-cli-test', logDir });
  const deliverySocketPath = path.join(adapterDir, `${sessionId}.sock`);
  const state = {
    kind,
    adapter,
    enabled,
    desiredEnabled: enabled,
    autoEnable: false,
    participantId: enabled ? participantId : null,
    lastError,
    displayName: null,
    description: null,
  };

  let server;
  function payload(status, reason = null) {
    return {
      status,
      reason,
      kind: state.kind,
      adapter: state.adapter,
      sessionId,
      participantId: state.participantId,
      desiredEnabled: state.desiredEnabled,
      enabled: state.enabled,
      autoEnable: state.autoEnable,
      displayName: state.displayName,
      description: state.description,
      state: state.enabled ? 'enabled' : (state.desiredEnabled ? 'pending' : 'disabled'),
      controlSocketPath: server ? server.socketPath : null,
      deliverySocketPath: state.enabled ? deliverySocketPath : null,
      lastError: state.lastError,
      logPath: logger.path,
    };
  }

  server = startControlServer({
    sessionId,
    socketDir: controlDir,
    logger,
    handleRequest: async (req) => {
      if (req.type === 'status') return payload('ok');
      if (req.type === 'enable') {
        state.desiredEnabled = true;
        state.enabled = true;
        state.participantId = participantId;
        state.lastError = null;
        state.displayName = req.displayName || null;
        state.description = req.description || null;
        return payload('enabled');
      }
      if (req.type === 'disable') {
        state.desiredEnabled = false;
        state.enabled = false;
        state.participantId = null;
        return payload('disabled');
      }
      if (req.type === 'whoami') {
        if (!state.enabled || !state.participantId) {
          return payload('error', 'bus_disabled');
        }
        return payload('ok');
      }
      return payload('error', 'unknown_type');
    },
  });
  await server.start();

  return {
    env: {
      AGENT_BUS_SESSION_ID: sessionId,
      AGENT_BUS_CONTROL_SOCKETS_DIR: controlDir,
      AGENT_BUS_ADAPTER_SOCKETS_DIR: adapterDir,
    },
    participantId,
    sessionId,
    state,
    async cleanup() {
      await server.stop();
      try { fs.rmSync(controlDir, { recursive: true, force: true }); } catch {}
      try { fs.rmSync(adapterDir, { recursive: true, force: true }); } catch {}
      try { fs.rmSync(logDir, { recursive: true, force: true }); } catch {}
    },
  };
}

async function createAdapterSocket(socketPath, responseFactory) {
  try { fs.unlinkSync(socketPath); } catch {}
  const server = require('node:net').createServer((socket) => {
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk.toString();
      const nl = buffer.indexOf('\n');
      if (nl === -1) return;
      const line = buffer.slice(0, nl);
      let req = null;
      try { req = JSON.parse(line); } catch {}
      socket.write(JSON.stringify(responseFactory(req)) + '\n');
      socket.end();
    });
  });
  await new Promise((resolve, reject) => {
    server.listen(socketPath, resolve);
    server.once('error', reject);
  });
  return server;
}

test('bus enable talks to the wrapper control socket and prints session state', async () => {
  const fx = await createFixture();
  try {
    const result = await runCli(['enable'], fx.env);
    assert.equal(result.code, 0);
    assert.match(result.stdout, /^enabled  kind=codex adapter=codex-pty session=/);
    assert.match(result.stdout, new RegExp(`participant=${fx.participantId}`));
  } finally {
    await fx.cleanup();
  }
});

test('bus enable forwards name and description to registration state', async () => {
  const fx = await createFixture();
  try {
    const result = await runCli(['enable', '--name', 'Planner', '--description', 'Coordinates code handoffs'], fx.env);
    assert.equal(result.code, 0);
    assert.match(result.stdout, /name=Planner/);
    assert.match(result.stdout, /description=Coordinates code handoffs/);
  } finally {
    await fx.cleanup();
  }
});

test('bus whoami prints only the live participant id', async () => {
  const fx = await createFixture({ kind: 'claude', adapter: 'claude-pty', enabled: true });
  try {
    const result = await runCli(['whoami'], fx.env);
    assert.equal(result.code, 0);
    assert.equal(result.stdout.trim(), fx.participantId);
  } finally {
    await fx.cleanup();
  }
});

test('bus status shows the last error for debugging', async () => {
  const fx = await createFixture({ lastError: 'broker_unreachable' });
  try {
    const result = await runCli(['status'], fx.env);
    assert.equal(result.code, 0);
    assert.match(result.stdout, /last_error=broker_unreachable/);
    assert.match(result.stdout, /log=/);
  } finally {
    await fx.cleanup();
  }
});

test('bus status times out instead of hanging on an unresponsive control socket', async () => {
  const sessionId = 'session-hang-' + crypto.randomBytes(4).toString('hex');
  const controlDir = fs.mkdtempSync('/tmp/bus-control-hang-');
  const socketPath = path.join(controlDir, `${sessionId}.sock`);
  const sockets = new Set();
  const server = require('node:net').createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  await new Promise((resolve, reject) => {
    server.listen(socketPath, resolve);
    server.once('error', reject);
  });

  try {
    const result = await runCli(['status'], {
      AGENT_BUS_SESSION_ID: sessionId,
      AGENT_BUS_CONTROL_SOCKETS_DIR: controlDir,
      AGENT_BUS_SOCKET_TIMEOUT_MS: '50',
    });
    assert.equal(result.code, 2);
    assert.match(result.stderr, /RETRY NOW:/);
    assert.match(result.stderr, /did not respond/);
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve) => server.close(resolve));
    try { fs.rmSync(controlDir, { recursive: true, force: true }); } catch {}
  }
});

test('bus send --from-session explains when the session is disabled', async () => {
  const fx = await createFixture();
  try {
    const result = await runCli(['send', '--from-session', 'codex:aaaaaaaaaaaa', 'hello'], fx.env);
    assert.equal(result.code, 2);
    assert.match(result.stderr, /RETRY NOW:/);
    assert.match(result.stderr, new RegExp(`session ${fx.sessionId} is not bus-enabled`));
    assert.match(result.stderr, /Run `bus enable --name "task-short-name" --description "What this session is doing"`/);
    assert.match(result.stderr, /Do not report this as a bus failure/);
  } finally {
    await fx.cleanup();
  }
});

test('bus send --from-session treats queued delivery as nondelivery by default', async () => {
  const fx = await createFixture({ enabled: true });
  const adapterSocketPath = path.join(fx.env.AGENT_BUS_ADAPTER_SOCKETS_DIR, `${fx.sessionId}.sock`);
  const server = await createAdapterSocket(adapterSocketPath, () => ({
    status: 'queued',
    from: fx.participantId,
    reason: 'recipient_offline',
  }));
  try {
    const result = await runCli(['send', '--from-session', 'codex:aaaaaaaaaaaa', 'hello'], fx.env);
    assert.equal(result.code, 2);
    assert.match(result.stderr, /queued \(recipient_offline\)/);
    assert.match(result.stderr, /RETRY NOW: message not delivered live/);
    assert.match(result.stderr, /Run `bus list` and confirm the peer is currently present/);
    assert.match(result.stderr, /Do not report this as sent/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fx.cleanup();
  }
});

test('bus send --from-session allows queued delivery when requested explicitly', async () => {
  const fx = await createFixture({ enabled: true });
  const adapterSocketPath = path.join(fx.env.AGENT_BUS_ADAPTER_SOCKETS_DIR, `${fx.sessionId}.sock`);
  const server = await createAdapterSocket(adapterSocketPath, () => ({
    status: 'queued',
    from: fx.participantId,
    reason: 'recipient_offline',
  }));
  try {
    const result = await runCli(['send', '--from-session', '--allow-queued', 'codex:aaaaaaaaaaaa', 'hello'], fx.env);
    assert.equal(result.code, 0);
    assert.match(result.stdout, /queued \(recipient_offline\)/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fx.cleanup();
  }
});

test('control socket rejects unauthenticated privileged ops, accepts token-bearing ones', async () => {
  // Connection != authority on the control surface: a same-uid peer that
  // connects but does not present the per-session control token cannot flip
  // enable/disable or read the live participant id via whoami. status stays
  // unauthenticated (a liveness probe). The legitimate token path still works.
  const sessionId = 'control-auth-' + crypto.randomBytes(4).toString('hex');
  const controlDir = fs.mkdtempSync('/tmp/bus-control-auth-');
  const logDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bus-control-auth-log-'));
  const logger = createSessionLogger({ sessionId, label: 'control-auth-test', logDir });
  let enabledCalls = 0;
  const server = startControlServer({
    sessionId,
    socketDir: controlDir,
    logger,
    handleRequest: async (req) => {
      if (req.type === 'enable') { enabledCalls += 1; return { status: 'enabled' }; }
      if (req.type === 'status') return { status: 'ok', state: 'disabled' };
      if (req.type === 'whoami') return { status: 'ok', participantId: 'codex:abcabcabcabc' };
      return { status: 'error', reason: 'unknown_type' };
    },
  });
  await server.start();
  const socketPath = server.socketPath;
  try {
    // status is unauthenticated — works with no token.
    const st = await controlRequest(socketPath, { type: 'status' });
    assert.equal(st.status, 'ok');

    // enable / disable / whoami without a token are rejected, and the handler
    // is never invoked (state-change is blocked at the gate).
    for (const type of ['enable', 'disable', 'whoami']) {
      const r = await controlRequest(socketPath, { type });
      assert.equal(r.status, 'error', `${type} should be rejected`);
      assert.equal(r.reason, 'unauthorized', `${type} reason`);
    }
    assert.equal(enabledCalls, 0, 'enable handler must not run without a token');

    // A wrong token is also rejected (timing-safe path).
    const wrong = await controlRequest(socketPath, { type: 'enable', auth: 'f'.repeat(64) });
    assert.equal(wrong.reason, 'unauthorized');
    assert.equal(enabledCalls, 0);

    // The token published to the 0600 file (what the CLI reads) authorizes the op.
    const token = sessionAuth.readToken(controlDir, sessionId);
    assert.ok(token, 'control token file present');
    assert.equal(token, server.controlToken);
    const okEnable = await controlRequest(socketPath, { type: 'enable', auth: token });
    assert.equal(okEnable.status, 'enabled');
    assert.equal(enabledCalls, 1);
  } finally {
    await server.stop();
    try { fs.rmSync(controlDir, { recursive: true, force: true }); } catch {}
    try { fs.rmSync(logDir, { recursive: true, force: true }); } catch {}
  }
});
