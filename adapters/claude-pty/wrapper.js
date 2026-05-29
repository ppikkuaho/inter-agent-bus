#!/usr/bin/env node
// claude-pty adapter — user-facing launcher wrapper.
// Spawns Claude in a PTY. Bus work is delegated to sidecar.js.

'use strict';

const path = require('path');
const os = require('os');
const crypto = require('crypto');

let pty;
try { pty = require('node-pty'); }
catch (err) {
  console.error('[claude-pty] node-pty is required for the wrapper. Install with: npm install (from the repo root)');
  console.error('[claude-pty] original error:', err.message);
  process.exit(1);
}

const { startSidecar } = require('./sidecar');
const { createSessionLogger, startControlServer } = require('../session-control');

const SESSION_ID = process.env.AGENT_BUS_SESSION_ID || ('claude-' + crypto.randomBytes(8).toString('hex'));
const PROJECT_NAME = path.basename(process.cwd());
const CLAUDE_CMD = process.env.AGENT_BUS_CLAUDE_CMD || path.join(os.homedir(), '.local', 'bin', 'claude');
const CLAUDE_ARGS = process.argv.slice(2);
const AUTO_ENABLE = /^(1|true|yes)$/i.test(process.env.AGENT_BUS_AUTO_ENABLE || '');
const DISPLAY_NAME_MAX = 64;
const DESCRIPTION_MAX = 200;

function normalizeRegistrationField(value, maxLength) {
  if (typeof value !== 'string') return null;
  const normalized = value.replace(/\s+/g, ' ').trim();
  if (!normalized) return null;
  return normalized.slice(0, maxLength);
}

const cols = process.stdout.columns || 80;
const rows = process.stdout.rows || 24;
const logger = createSessionLogger({ sessionId: SESSION_ID, label: 'claude-pty' });

let ptyProcess;
try {
  ptyProcess = pty.spawn(CLAUDE_CMD, CLAUDE_ARGS, {
    name: 'xterm-256color', cols, rows,
    cwd: process.cwd(),
    env: { CLAUDE_CODE_NO_FLICKER: '1', ...process.env, AGENT_BUS_SESSION_ID: SESSION_ID },
  });
} catch (err) {
  console.error(`[claude-pty] failed to spawn ${CLAUDE_CMD}: ${err.message}`);
  process.exit(1);
}
logger.info('wrapper_started', {
  autoEnable: AUTO_ENABLE,
  command: CLAUDE_CMD,
  cwd: process.cwd(),
});

if (process.stdin.isTTY) process.stdin.setRawMode(true);
ptyProcess.onData((d) => process.stdout.write(d));
process.stdin.on('data', (d) => ptyProcess.write(d));
process.stdout.on('resize', () => ptyProcess.resize(process.stdout.columns || 80, process.stdout.rows || 24));

const sidecar = startSidecar({
  write: (text) => { if (ptyProcess && !ptyProcess.killed) ptyProcess.write(text); },
  sessionId: SESSION_ID,
  project: PROJECT_NAME,
  cwd: process.cwd(),
  getRegistrationProfile: () => registrationProfile,
});
let controlServer = null;
let sidecarStarted = false;
let desiredEnabled = AUTO_ENABLE;
let enablePromise = null;
let lastError = null;
let lastTransitionAt = new Date().toISOString();
let registrationProfile = {
  displayName: normalizeRegistrationField(process.env.AGENT_BUS_DISPLAY_NAME, DISPLAY_NAME_MAX),
  description: normalizeRegistrationField(process.env.AGENT_BUS_DESCRIPTION, DESCRIPTION_MAX),
};

function updateRegistrationProfile(req = {}) {
  let changed = false;
  if (Object.prototype.hasOwnProperty.call(req, 'displayName')) {
    const next = normalizeRegistrationField(req.displayName, DISPLAY_NAME_MAX);
    if (registrationProfile.displayName !== next) {
      registrationProfile = { ...registrationProfile, displayName: next };
      changed = true;
    }
  }
  if (Object.prototype.hasOwnProperty.call(req, 'description')) {
    const next = normalizeRegistrationField(req.description, DESCRIPTION_MAX);
    if (registrationProfile.description !== next) {
      registrationProfile = { ...registrationProfile, description: next };
      changed = true;
    }
  }
  return changed;
}

function buildStatus(overrides = {}) {
  const state = overrides.state || (sidecarStarted ? 'enabled' : (desiredEnabled ? (lastError ? 'error' : 'pending') : 'disabled'));
  return {
    kind: 'claude',
    adapter: 'claude-pty',
    sessionId: SESSION_ID,
    participantId: sidecarStarted ? sidecar.participantId : null,
    desiredEnabled,
    enabled: sidecarStarted,
    autoEnable: AUTO_ENABLE,
    state,
    project: PROJECT_NAME,
    cwd: process.cwd(),
    displayName: registrationProfile.displayName,
    description: registrationProfile.description,
    controlSocketPath: controlServer ? controlServer.socketPath : null,
    deliverySocketPath: sidecarStarted ? sidecar.socketPath : null,
    lastError,
    lastTransitionAt,
    logPath: logger.path,
    ...overrides,
  };
}

async function enableBus(req = {}) {
  const profileChanged = updateRegistrationProfile(req);
  desiredEnabled = true;
  lastTransitionAt = new Date().toISOString();
  if (sidecarStarted) {
    if (!profileChanged) return { status: 'enabled', ...buildStatus({ state: 'enabled' }) };
    try {
      await sidecar.stop();
      sidecarStarted = false;
      lastError = null;
      logger.info('bus_profile_updated', {
        displayName: registrationProfile.displayName,
        description: registrationProfile.description,
      });
    } catch (err) {
      lastError = err.message || String(err);
      logger.error('bus_profile_update_failed', err);
      console.error('[claude-pty] bus profile update failed:', lastError);
      return { status: 'error', reason: lastError, ...buildStatus({ state: 'error' }) };
    }
  }
  if (enablePromise) {
    await enablePromise;
    return { status: sidecarStarted ? 'enabled' : 'error', ...buildStatus() };
  }

  enablePromise = (async () => {
    try {
      const reg = await sidecar.start();
      sidecarStarted = true;
      lastError = null;
      logger.info('bus_enabled', {
        deliverySocketPath: reg.socketPath,
        participantId: reg.participantId,
        displayName: registrationProfile.displayName,
        description: registrationProfile.description,
      });
      return reg;
    } catch (err) {
      lastError = err.message || String(err);
      logger.error('bus_enable_failed', err);
      console.error('[claude-pty] bus enable failed:', lastError);
      return null;
    } finally {
      enablePromise = null;
    }
  })();

  await enablePromise;
  return { status: sidecarStarted ? 'enabled' : 'error', ...buildStatus() };
}

async function disableBus() {
  desiredEnabled = false;
  lastTransitionAt = new Date().toISOString();
  if (enablePromise) {
    await enablePromise;
  }
  if (!sidecarStarted) return { status: 'disabled', ...buildStatus({ state: 'disabled' }) };
  try {
    await sidecar.stop();
    sidecarStarted = false;
    lastError = null;
    logger.info('bus_disabled');
    return { status: 'disabled', ...buildStatus({ state: 'disabled' }) };
  } catch (err) {
    lastError = err.message || String(err);
    logger.error('bus_disable_failed', err);
    console.error('[claude-pty] bus disable failed:', lastError);
    return { status: 'error', reason: lastError, ...buildStatus({ state: 'error' }) };
  }
}

async function handleControlRequest(req) {
  if (req.type === 'status') return { status: 'ok', ...buildStatus() };
  if (req.type === 'enable') return enableBus(req);
  if (req.type === 'disable') return disableBus();
  if (req.type === 'whoami') {
    if (sidecarStarted && sidecar.participantId) {
      return { status: 'ok', participantId: sidecar.participantId, ...buildStatus({ state: 'enabled' }) };
    }
    return {
      status: 'error',
      reason: desiredEnabled ? (lastError || 'bus_not_ready') : 'bus_disabled',
      ...buildStatus(),
    };
  }
  return { status: 'error', reason: 'unknown_type', ...buildStatus() };
}

controlServer = startControlServer({
  sessionId: SESSION_ID,
  logger,
  handleRequest: handleControlRequest,
});
controlServer.start().catch((err) => {
  logger.error('control_server_start_failed', err);
  console.error('[claude-pty] control server start failed:', err.message);
});

if (AUTO_ENABLE) {
  enableBus().catch(() => {});
}

ptyProcess.onExit(({ exitCode }) => {
  desiredEnabled = false;
  const sidecarCleanup = sidecarStarted ? sidecar.stop().catch(() => {}) : Promise.resolve();
  const controlCleanup = controlServer ? controlServer.stop().catch(() => {}) : Promise.resolve();
  Promise.allSettled([sidecarCleanup, controlCleanup]).finally(() => {
    if (process.stdin.isTTY) { try { process.stdin.setRawMode(false); } catch {} }
    process.exit(exitCode || 0);
  });
});

const forwardSignal = (sig) => {
  if (!ptyProcess || ptyProcess.killed) return;
  if (sig === 'SIGINT') ptyProcess.write('\x03');
  else ptyProcess.kill();
};
process.on('SIGINT', () => forwardSignal('SIGINT'));
process.on('SIGTERM', () => forwardSignal('SIGTERM'));
process.on('SIGHUP', () => forwardSignal('SIGHUP'));
