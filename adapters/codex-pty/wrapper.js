#!/usr/bin/env node
// codex-pty adapter — interactive Codex launcher wrapper.
// Spawns Codex in a PTY, preserves runtime-state tracking, and starts the bus
// sidecar once a real thread_id appears for the session.

'use strict';

const path = require('path');
const os = require('os');
const fs = require('fs');
const crypto = require('crypto');
const { spawn, spawnSync } = require('node:child_process');

let pty;
try { pty = require('node-pty'); }
catch (err) {
  console.error('[codex-pty] node-pty is required for the wrapper. Install with: npm install (from the repo root)');
  console.error('[codex-pty] original error:', err.message);
  process.exit(1);
}

const { startSidecar } = require('./sidecar');
const { createSessionLogger, startControlServer } = require('../session-control');

const BUS_ROOT = process.env.AGENT_BUS_ROOT || path.resolve(__dirname, '../..');
const RUNTIME_CTL = process.env.CODEX_RUNTIME_CTL || path.join(os.homedir(), 'Documents', 'codex', 'runtime', 'codex_runtime_ctl.py');
const POLICY_RESOLVER = path.join(BUS_ROOT, 'adapters', 'codex-app-server', 'resolve_wrapper_policy.py');
const STATE_DIR = process.env.CODEX_RUNTIME_STATE_DIR || path.join(os.homedir(), '.codex', 'runtime');
const SESSION_ID = process.env.CODEX_RUNTIME_SESSION_ID || crypto.randomUUID();
const CODEX_CMD = process.env.AGENT_BUS_CODEX_REAL || process.env.CODEX_REAL || '/opt/homebrew/bin/codex';
const CODEX_ARGS = process.argv.slice(2);
const THREAD_WAIT_SEC = Number(process.env.AGENT_BUS_CODEX_THREAD_WAIT_SEC || process.env.CODEX_BUS_THREAD_WAIT_SEC || '300');
const OUTER_TTY_PATH = process.env.AGENT_BUS_CODEX_TTY_PATH || '';
const TERMINAL_APP = process.env.AGENT_BUS_CODEX_TERMINAL_APP || 'auto';
const TERM_PROGRAM_VALUE = process.env.AGENT_BUS_CODEX_TERM_PROGRAM || process.env.TERM_PROGRAM || '';
const RESUME_THREAD_ID = process.env.AGENT_BUS_CODEX_RESUME_THREAD_ID || '';
const WATCHDOG_AUTOSTART = process.env.CODEX_WATCHDOG_AUTOSTART || '0';
const WATCHDOG_BACKEND = process.env.CODEX_WATCHDOG_BACKEND || 'subprocess';
const WATCHDOG_IDLE_SECONDS = process.env.CODEX_WATCHDOG_IDLE_SECONDS || '60';
const WATCHDOG_REPEAT_SECONDS = process.env.CODEX_WATCHDOG_REPEAT_SECONDS || '60';
const WATCHDOG_POLL_SECONDS = process.env.CODEX_WATCHDOG_POLL_SECONDS || '10';
const WATCHDOG_TRANSPORT = process.env.CODEX_WATCHDOG_TRANSPORT || 'auto';
const PROJECT_NAME = path.basename(process.cwd()) || process.cwd();
const THREAD_FILE = path.join(STATE_DIR, 'active-threads', `${SESSION_ID}.json`);
const AUTO_ENABLE = /^(1|true|yes)$/i.test(process.env.AGENT_BUS_AUTO_ENABLE || '');
const DISPLAY_NAME_MAX = 64;
const DESCRIPTION_MAX = 200;
const logger = createSessionLogger({ sessionId: SESSION_ID, label: 'codex-pty' });

function stderr(message) {
  process.stderr.write(message.replace(/\n?$/, '\n'));
}

function normalizeRegistrationField(value, maxLength) {
  if (typeof value !== 'string') return null;
  const normalized = value.replace(/\s+/g, ' ').trim();
  if (!normalized) return null;
  return normalized.slice(0, maxLength);
}

function resolvePolicy(args) {
  const result = spawnSync('python3', [POLICY_RESOLVER, '--', ...args], {
    encoding: 'utf8',
    env: process.env,
  });
  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || 'policy resolver failed').trim());
  }
  return JSON.parse(result.stdout);
}

function runDetached(command, args) {
  const child = spawn(command, args, {
    detached: true,
    stdio: 'ignore',
    env: process.env,
  });
  child.unref();
  return child;
}

function trackActiveTerminal(childPid, policy) {
  if (!OUTER_TTY_PATH || !OUTER_TTY_PATH.startsWith('/dev/')) return;
  const args = [
    RUNTIME_CTL,
    'track-active-terminal',
    '--pid', String(childPid),
    '--cwd', process.cwd(),
    '--tty-path', OUTER_TTY_PATH,
    '--terminal-app', TERMINAL_APP,
    '--term-program', TERM_PROGRAM_VALUE,
    '--source', 'codex_wrapper',
    '--runtime-session-id', SESSION_ID,
  ];
  if (policy.approval_mode) args.push('--approval-mode', policy.approval_mode);
  if (policy.sandbox_mode) args.push('--sandbox-mode', policy.sandbox_mode);
  if (policy.profile) args.push('--profile', policy.profile);
  for (const addDir of policy.add_dirs || []) args.push('--add-dir', addDir);
  for (const override of policy.config_overrides || []) args.push('--config-override', override);
  if (policy.full_auto) args.push('--full-auto');
  if (policy.dangerously_bypass) args.push('--dangerously-bypass');

  spawnSync('python3', args, {
    stdio: 'ignore',
    env: process.env,
  });
}

function startThreadTracker(childPid) {
  const args = [
    RUNTIME_CTL,
    'track-active-thread',
    '--pid', String(childPid),
    '--cwd', process.cwd(),
    '--launch-epoch', String(Math.floor(Date.now() / 1000)),
    '--runtime-session-id', SESSION_ID,
  ];
  if (RESUME_THREAD_ID) args.push('--thread-id', RESUME_THREAD_ID);
  runDetached('python3', args);
}

function maybeAutoStartWatchdog() {
  if (WATCHDOG_AUTOSTART === '0' || !OUTER_TTY_PATH.startsWith('/dev/')) return;
  runDetached('python3', [
    RUNTIME_CTL,
    'watchdog', 'enable',
    '--cwd', process.cwd(),
    '--backend', WATCHDOG_BACKEND,
    '--idle-seconds', WATCHDOG_IDLE_SECONDS,
    '--repeat-seconds', WATCHDOG_REPEAT_SECONDS,
    '--poll-seconds', WATCHDOG_POLL_SECONDS,
    '--transport', WATCHDOG_TRANSPORT,
  ]);
}

function loadThreadId() {
  try {
    const payload = JSON.parse(fs.readFileSync(THREAD_FILE, 'utf8'));
    return typeof payload.thread_id === 'string' && payload.thread_id ? payload.thread_id : null;
  } catch {
    return null;
  }
}

async function waitForThreadId(childPid) {
  const deadline = Date.now() + Math.max(1, THREAD_WAIT_SEC) * 1000;
  while (Date.now() < deadline) {
    const threadId = loadThreadId();
    if (threadId) return threadId;
    if (childPid) {
      try {
        process.kill(childPid, 0);
      } catch {
        return null;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return null;
}

const childEnv = {
  ...process.env,
  CODEX_RUNTIME_STATE_DIR: STATE_DIR,
  CODEX_RUNTIME_SESSION_ID: SESSION_ID,
  AGENT_BUS_SESSION_ID: SESSION_ID,
};

const cols = process.stdout.columns || 80;
const rows = process.stdout.rows || 24;
let ptyProcess;
try {
  ptyProcess = pty.spawn(CODEX_CMD, CODEX_ARGS, {
    name: 'xterm-256color',
    cols,
    rows,
    cwd: process.cwd(),
    env: childEnv,
  });
} catch (err) {
  stderr(`[codex-pty] failed to spawn ${CODEX_CMD}: ${err.message}`);
  process.exit(1);
}
logger.info('wrapper_started', {
  autoEnable: AUTO_ENABLE,
  command: CODEX_CMD,
  cwd: process.cwd(),
});

process.env.CODEX_RUNTIME_SESSION_ID = SESSION_ID;
process.env.CODEX_RUNTIME_STATE_DIR = STATE_DIR;

let policy;
try {
  policy = resolvePolicy(CODEX_ARGS);
} catch (err) {
  stderr(`[codex-pty] policy resolver failed: ${err.message}`);
  policy = {
    approval_mode: null,
    sandbox_mode: null,
    full_auto: false,
    dangerously_bypass: false,
    profile: null,
    add_dirs: [],
    config_overrides: [],
  };
}

trackActiveTerminal(ptyProcess.pid, policy);
startThreadTracker(ptyProcess.pid);
maybeAutoStartWatchdog();

if (process.stdin.isTTY) process.stdin.setRawMode(true);
ptyProcess.onData((data) => process.stdout.write(data));
process.stdin.on('data', (data) => ptyProcess.write(data));
process.stdout.on('resize', () => ptyProcess.resize(process.stdout.columns || 80, process.stdout.rows || 24));

const sidecar = startSidecar({
  write: (text) => {
    if (ptyProcess && !ptyProcess.killed) ptyProcess.write(text);
  },
  sessionId: SESSION_ID,
  project: PROJECT_NAME,
  cwd: process.cwd(),
  getRegistrationProfile: () => registrationProfile,
});

let sidecarStarted = false;
let controlServer = null;
let desiredEnabled = AUTO_ENABLE;
let enablePromise = null;
let threadWatchPromise = null;
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

function currentThreadInfo() {
  const threadId = loadThreadId();
  if (threadId) return { threadId, threadState: 'ready' };
  return { threadId: null, threadState: 'awaiting_thread' };
}

function buildStatus(overrides = {}) {
  const threadInfo = currentThreadInfo();
  const state = overrides.state || (sidecarStarted
    ? 'enabled'
    : desiredEnabled
      ? (threadInfo.threadState !== 'ready' ? 'awaiting_thread' : (lastError ? 'error' : 'pending'))
      : 'disabled');

  return {
    kind: 'codex',
    adapter: 'codex-pty',
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
    threadWaitActive: !!threadWatchPromise,
    ...threadInfo,
    ...overrides,
  };
}

async function startSidecarNow() {
  if (sidecarStarted) return { status: 'enabled', ...buildStatus({ state: 'enabled' }) };
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
        threadId: loadThreadId(),
        displayName: registrationProfile.displayName,
        description: registrationProfile.description,
      });
      return reg;
    } catch (err) {
      lastError = err.message || String(err);
      logger.error('bus_enable_failed', err, { threadId: loadThreadId() });
      stderr(`[codex-pty] bus enable failed: ${lastError}`);
      return null;
    } finally {
      enablePromise = null;
    }
  })();

  await enablePromise;
  return { status: sidecarStarted ? 'enabled' : 'error', ...buildStatus() };
}

function ensureThreadWatch() {
  if (threadWatchPromise) return threadWatchPromise;
  logger.info('thread_wait_started');
  threadWatchPromise = (async () => {
    const threadId = await waitForThreadId(ptyProcess.pid);
    if (!threadId) {
      if (desiredEnabled) {
        lastError = 'thread_id_not_available';
        logger.warn('thread_wait_failed', { reason: lastError });
      }
      return null;
    }
    logger.info('thread_ready', { threadId });
    if (desiredEnabled && !sidecarStarted) {
      await startSidecarNow();
    }
    return threadId;
  })().catch((err) => {
    lastError = err.message || String(err);
    logger.error('thread_watch_failed', err);
    stderr(`[codex-pty] thread watch failed: ${lastError}`);
    return null;
  }).finally(() => {
    threadWatchPromise = null;
  });
  return threadWatchPromise;
}

async function enableBus(req = {}) {
  const profileChanged = updateRegistrationProfile(req);
  desiredEnabled = true;
  lastTransitionAt = new Date().toISOString();
  const threadInfo = currentThreadInfo();
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
      stderr(`[codex-pty] bus profile update failed: ${lastError}`);
      return { status: 'error', reason: lastError, ...buildStatus({ state: 'error' }) };
    }
  }
  if (threadInfo.threadState !== 'ready') {
    lastError = null;
    ensureThreadWatch();
    return { status: 'awaiting_thread', reason: 'submit_first_prompt', ...buildStatus({ state: 'awaiting_thread' }) };
  }
  return startSidecarNow();
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
    stderr(`[codex-pty] bus disable failed: ${lastError}`);
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
  stderr(`[codex-pty] control server start failed: ${err.message}`);
});

if (AUTO_ENABLE) {
  enableBus().catch(() => {});
}

ptyProcess.onExit(({ exitCode }) => {
  desiredEnabled = false;
  const sidecarCleanup = sidecarStarted ? sidecar.stop().catch(() => {}) : Promise.resolve();
  const controlCleanup = controlServer ? controlServer.stop().catch(() => {}) : Promise.resolve();
  Promise.allSettled([sidecarCleanup, controlCleanup]).finally(() => {
    if (process.stdin.isTTY) {
      try { process.stdin.setRawMode(false); } catch {}
    }
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
