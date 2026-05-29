// Inter-agent bus client.
// JSON-over-Unix-socket client with auto-reconnect + silent re-register.

'use strict';

const net = require('net');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const DEFAULT_STATE_DIR = path.join(os.homedir(), '.agent-bus');
const DEFAULT_SOCKET = path.join(DEFAULT_STATE_DIR, 'broker.sock');
const RECONNECT_BASE_MS = 200;
const RECONNECT_CAP_MS = 5000;
const DEFAULT_HEARTBEAT_MS = 20 * 1000;

function resolveSocketPath(explicitSocketPath) {
  if (process.env.AGENT_BUS_SOCKET) return process.env.AGENT_BUS_SOCKET;
  if (explicitSocketPath) return explicitSocketPath;
  if (process.env.AGENT_BUS_STATE_DIR) {
    return path.join(process.env.AGENT_BUS_STATE_DIR, 'broker.sock');
  }
  return DEFAULT_SOCKET;
}

class BusClient extends EventEmitter {
  constructor({ socketPath } = {}) {
    super();
    this.socketPath = resolveSocketPath(socketPath);
    this.socket = null;
    this.buffer = '';
    this.pending = [];
    this.leaseToken = null;
    this.participantId = null;
    this.participant = null;
    this.heartbeatTimer = null;
    this.heartbeatMs = DEFAULT_HEARTBEAT_MS;
    this._reconnectAttempts = 0;
    this._closed = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      const s = net.createConnection(this.socketPath);
      const onError = (err) => { s.removeAllListeners(); reject(err); };
      s.once('connect', () => {
        s.removeListener('error', onError);
        this.socket = s;
        this._reconnectAttempts = 0;
        s.on('data', (chunk) => this._onData(chunk));
        s.on('error', (err) => this.emit('error', err));
        s.on('close', () => this._onClose());
        resolve();
      });
      s.once('error', onError);
    });
  }

  _onData(chunk) {
    this.buffer += chunk.toString();
    let nl;
    while ((nl = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, nl);
      this.buffer = this.buffer.slice(nl + 1);
      if (!line.trim()) continue;
      const pending = this.pending.shift();
      if (!pending) continue;
      let response;
      try { response = JSON.parse(line); }
      catch { pending.reject(new Error('invalid_response')); continue; }
      if (response.status === 'error') pending.reject(new Error(response.error || 'broker_error'));
      else pending.resolve(response);
    }
  }

  _onClose() {
    for (const p of this.pending) p.reject(new Error('broker_disconnected'));
    this.pending = [];
    this.socket = null;
    if (this._closed) return;
    this.emit('disconnected');
    if (this.participant) this._scheduleReconnect();
  }

  _scheduleReconnect() {
    if (this._closed) return;
    const delay = Math.min(RECONNECT_BASE_MS * Math.pow(2, this._reconnectAttempts), RECONNECT_CAP_MS);
    this._reconnectAttempts += 1;
    // unref() so a background reconnect timer never keeps the process alive on
    // its own. If the caller really wants to stay up, something else in their
    // event loop will be holding it up anyway.
    const timer = setTimeout(async () => {
      if (this._closed) return;
      try {
        await this.connect();
        const response = await this._send({ type: 'register_participant', participant: this.participant });
        this.leaseToken = response.leaseToken;
        this.participantId = response.participantId;
        if (response.heartbeatIntervalSec) this.heartbeatMs = response.heartbeatIntervalSec * 1000;
        this._startHeartbeat();
        this.emit('reconnected', response);
      } catch {
        this._scheduleReconnect();
      }
    }, delay);
    if (timer.unref) timer.unref();
  }

  _send(req) {
    if (!this.socket) return Promise.reject(new Error('not_connected'));
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      this.socket.write(JSON.stringify(req) + '\n');
    });
  }

  async register(participant) {
    if (!this.socket) await this.connect();
    // Only commit `participant` after the broker accepts the register. If the
    // RPC rejects (e.g. kind mismatch), we do NOT want the next `_onClose` to
    // see `participant` set and start reconnecting a failed registration.
    const response = await this._send({ type: 'register_participant', participant });
    this.participant = participant;
    this.leaseToken = response.leaseToken;
    this.participantId = response.participantId;
    if (response.heartbeatIntervalSec) this.heartbeatMs = response.heartbeatIntervalSec * 1000;
    this._startHeartbeat();
    return response;
  }

  _startHeartbeat() {
    this._stopHeartbeat();
    this.heartbeatTimer = setInterval(async () => {
      if (!this.leaseToken || !this.socket) return;
      try { await this._send({ type: 'heartbeat', leaseToken: this.leaseToken }); }
      catch { /* disconnect/reconnect path handles it */ }
    }, this.heartbeatMs);
    this.heartbeatTimer.unref();
  }

  _stopHeartbeat() {
    if (this.heartbeatTimer) { clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; }
  }

  async list() { return this._send({ type: 'list_participants' }); }

  async send(to, body, { id, replyTo, conversationId, source } = {}) {
    const messageId = id || ('msg_' + crypto.randomBytes(8).toString('hex'));
    return this._send({
      type: 'push_message',
      to,
      leaseToken: this.leaseToken,
      message: {
        id: messageId,
        body,
        reply_to: replyTo || null,
        conversation_id: conversationId || null,
        source: source || 'participant',
        sent_at: new Date().toISOString(),
      },
    });
  }

  async unregister() {
    this._closed = true;
    this._stopHeartbeat();
    if (this.socket && this.leaseToken) {
      try { await this._send({ type: 'unregister_participant', leaseToken: this.leaseToken }); } catch {}
    }
    if (this.socket) { try { this.socket.end(); } catch {} }
    this.socket = null;
    this.leaseToken = null;
    this.participantId = null;
    this.participant = null;
  }
}

module.exports = { BusClient, DEFAULT_SOCKET, DEFAULT_STATE_DIR, resolveSocketPath };
