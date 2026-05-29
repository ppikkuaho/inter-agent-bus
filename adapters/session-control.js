'use strict';

const net = require('net');
const fs = require('fs');
const path = require('path');
const os = require('os');
const sessionAuth = require('./session-auth');

// Control-socket request types that change session state or reveal the live
// participant identity. These require the per-session capability token so a
// same-uid peer that merely connects to the 0600 control socket cannot flip a
// session's bus state. `status` is intentionally NOT here: it is a read-only
// liveness/diagnostic probe (the CLI uses it to detect a wedged session) and
// reveals nothing beyond what `bus list` already shows.
const PRIVILEGED_CONTROL_TYPES = new Set(['enable', 'disable', 'whoami']);

function resolveControlSocketDir(explicit) {
  if (explicit) return explicit;
  if (process.env.AGENT_BUS_CONTROL_SOCKETS_DIR) {
    return process.env.AGENT_BUS_CONTROL_SOCKETS_DIR;
  }
  return path.join(os.homedir(), '.agent-bus', 'control-sockets');
}

function resolveControlLogDir(explicit) {
  if (explicit) return explicit;
  if (process.env.AGENT_BUS_CONTROL_LOG_DIR) {
    return process.env.AGENT_BUS_CONTROL_LOG_DIR;
  }
  return path.join(os.homedir(), '.agent-bus', 'control-logs');
}

function ensurePrivateDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true, mode: 0o700 });
  try { fs.chmodSync(dirPath, 0o700); } catch {}
}

function serializeError(err) {
  if (!err) return null;
  return {
    message: err.message || String(err),
    stack: err.stack || null,
  };
}

function createSessionLogger({ sessionId, label, logDir } = {}) {
  if (!sessionId) throw new Error('createSessionLogger: sessionId required');
  const effectiveLogDir = resolveControlLogDir(logDir);
  ensurePrivateDir(effectiveLogDir);
  const logPath = path.join(effectiveLogDir, `${sessionId}.log`);

  function write(level, message, extra = {}) {
    const payload = {
      ts: new Date().toISOString(),
      level,
      label: label || 'session-control',
      message,
      ...extra,
    };
    try {
      fs.appendFileSync(logPath, JSON.stringify(payload) + '\n', { mode: 0o600 });
      fs.chmodSync(logPath, 0o600);
    } catch {}
  }

  return {
    path: logPath,
    info(message, extra) {
      write('info', message, extra);
    },
    warn(message, extra) {
      write('warn', message, extra);
    },
    error(message, err, extra = {}) {
      write('error', message, { ...extra, error: serializeError(err) });
    },
  };
}

function startControlServer({ sessionId, socketDir, logger, handleRequest }) {
  if (!sessionId) throw new Error('startControlServer: sessionId required');
  if (typeof handleRequest !== 'function') {
    throw new Error('startControlServer: handleRequest must be a function');
  }

  const effectiveSocketDir = resolveControlSocketDir(socketDir);
  ensurePrivateDir(effectiveSocketDir);
  const socketPath = path.join(effectiveSocketDir, `${sessionId}.sock`);
  // Per-session capability token for privileged control ops (enable/disable/
  // whoami). Minted here, published to a 0600 file in the control-socket dir on
  // start(), and required by PRIVILEGED_CONTROL_TYPES. The CLI reads the file.
  const controlToken = sessionAuth.generateToken();
  let server = null;

  function writeResponse(socket, payload) {
    socket.write(JSON.stringify(payload) + '\n');
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
          } catch (err) {
            logger && logger.warn('control_invalid_json');
            writeResponse(socket, { status: 'error', reason: 'invalid_json' });
            return;
          }

          // Connection != authority: privileged ops require the capability
          // token. Checked before handleRequest so an unauthenticated peer can
          // never flip enable/disable or learn the live participant id.
          if (req && PRIVILEGED_CONTROL_TYPES.has(req.type)
              && !sessionAuth.timingSafeEqualStr(req.auth, controlToken)) {
            logger && logger.warn('control_unauthorized', { type: req.type });
            writeResponse(socket, { status: 'error', reason: 'unauthorized' });
            return;
          }

          try {
            const response = await handleRequest(req);
            writeResponse(socket, response || { status: 'ok' });
          } catch (err) {
            logger && logger.error('control_request_failed', err, { type: req.type || null });
            writeResponse(socket, {
              status: 'error',
              reason: err.message || 'control_request_failed',
            });
          }
        }).catch(() => {});
      }
    });

    socket.on('error', (err) => {
      logger && logger.error('control_socket_error', err);
    });
  }

  async function start() {
    if (fs.existsSync(socketPath)) {
      try { fs.unlinkSync(socketPath); } catch {}
    }

    server = net.createServer(handleConnection);
    await new Promise((resolve, reject) => {
      server.listen(socketPath, () => {
        fs.chmodSync(socketPath, 0o600);
        resolve();
      });
      server.once('error', reject);
    });
    // Publish the control token (0600) after the socket is listening so the CLI
    // can authenticate enable/disable/whoami.
    sessionAuth.writeToken(effectiveSocketDir, sessionId, controlToken);
    logger && logger.info('control_server_started', { socketPath });
    return { socketPath };
  }

  async function stop() {
    if (server) {
      await new Promise((resolve) => server.close(() => resolve()));
    }
    try { fs.unlinkSync(socketPath); } catch {}
    sessionAuth.removeToken(effectiveSocketDir, sessionId);
    logger && logger.info('control_server_stopped', { socketPath });
  }

  return {
    start,
    stop,
    get socketPath() { return socketPath; },
    get controlToken() { return controlToken; },
  };
}

module.exports = {
  createSessionLogger,
  resolveControlLogDir,
  resolveControlSocketDir,
  startControlServer,
};
