"""Inter-agent bus — Python client mirror of client/client.js.

Same JSON-over-Unix-socket protocol, same operations, same resilience:
auto-reconnect with exponential backoff capped at 5 s, silent re-register on
broker loss, participant committed only on successful register (matches JS
pass-6 fix). Heartbeats in a daemon thread so the main thread can exit freely.

Intended primary consumer: the codex-app-server bus adapter, which registers
on startup and receives deliveries via its own Unix socket (broker→adapter).
"""

from __future__ import annotations

import json
import os
import secrets
import socket
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

DEFAULT_STATE_DIR = Path.home() / ".agent-bus"
DEFAULT_SOCKET = DEFAULT_STATE_DIR / "broker.sock"
RECONNECT_BASE_MS = 200
RECONNECT_CAP_MS = 5000
DEFAULT_HEARTBEAT_SEC = 20


class BrokerError(Exception):
    """Broker returned an error response."""


class NotConnected(BrokerError):
    """Client has no live connection to the broker."""


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def resolve_socket_path(socket_path: Optional[str] = None) -> str:
    env_override = os.environ.get("AGENT_BUS_SOCKET")
    if env_override:
        return env_override
    if socket_path:
        return socket_path
    env_state_dir = os.environ.get("AGENT_BUS_STATE_DIR")
    if env_state_dir:
        return str(Path(env_state_dir) / "broker.sock")
    return str(DEFAULT_SOCKET)


class BusClient:
    """Python client for the inter-agent bus broker.

    Typical usage:
        client = BusClient()
        reg = client.register({
            "kind": "codex",
            "sessionId": runtime_session_id,
            "project": "agent-bus",
            "cwd": os.getcwd(),
            "delivery": {
                "adapter": "codex-app-server",
                "socketPath": my_adapter_socket,
                "threadId": my_thread_id,
            },
        })
        # Heartbeat runs automatically in a daemon thread.
        # client.participant_id, client.lease_token available.
        ...
        client.unregister()
    """

    def __init__(self, socket_path: Optional[str] = None) -> None:
        self.socket_path: str = resolve_socket_path(socket_path)
        self._socket: Optional[socket.socket] = None
        self._recv_buffer: bytes = b""
        self._lock = threading.Lock()
        self._participant: Optional[dict] = None
        self._lease_token: Optional[str] = None
        self._participant_id: Optional[str] = None
        self._heartbeat_sec: int = DEFAULT_HEARTBEAT_SEC
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._closed = False

    # ------ public API ------

    def connect(self) -> None:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(self.socket_path)
        with self._lock:
            self._socket = s
            self._recv_buffer = b""

    def register(self, participant: dict) -> dict:
        if self._socket is None:
            self.connect()
        response = self._request({"type": "register_participant", "participant": participant})
        # Commit participant only after successful register — matches JS pass-6 fix.
        self._participant = participant
        self._lease_token = response["leaseToken"]
        self._participant_id = response["participantId"]
        if "heartbeatIntervalSec" in response:
            try:
                self._heartbeat_sec = int(response["heartbeatIntervalSec"])
            except (TypeError, ValueError):
                pass
        self._start_heartbeat()
        return response

    def heartbeat(self) -> dict:
        if not self._lease_token:
            raise NotConnected("no lease; call register() first")
        return self._request({"type": "heartbeat", "leaseToken": self._lease_token})

    def list_participants(self) -> list[dict]:
        response = self._request({"type": "list_participants"})
        return response.get("participants", [])

    def send(
        self,
        to: str,
        body: str,
        *,
        id: Optional[str] = None,
        reply_to: Optional[str] = None,
        conversation_id: Optional[str] = None,
        source: str = "participant",
    ) -> dict:
        if not self._lease_token:
            raise NotConnected("no lease; call register() first")
        message_id = id or ("msg_" + secrets.token_hex(8))
        return self._request({
            "type": "push_message",
            "to": to,
            "leaseToken": self._lease_token,
            "message": {
                "id": message_id,
                "body": body,
                "reply_to": reply_to,
                "conversation_id": conversation_id,
                "source": source,
                "sent_at": _iso_now(),
            },
        })

    def unregister(self) -> dict:
        self._closed = True
        self._stop_heartbeat()
        response: dict = {"status": "ok"}
        if self._lease_token and self._socket is not None:
            try:
                response = self._request({
                    "type": "unregister_participant",
                    "leaseToken": self._lease_token,
                })
            except Exception:
                pass  # graceful: lease may already be invalid
        self._close_socket()
        self._lease_token = None
        return response

    @property
    def participant_id(self) -> Optional[str]:
        return self._participant_id

    @property
    def lease_token(self) -> Optional[str]:
        return self._lease_token

    # ------ internals ------

    def _request(self, req: dict) -> dict:
        with self._lock:
            if self._socket is None:
                raise NotConnected("client not connected")
            sock = self._socket
            data = (json.dumps(req) + "\n").encode("utf-8")
            sock.sendall(data)
            while b"\n" not in self._recv_buffer:
                chunk = sock.recv(4096)
                if not chunk:
                    self._socket = None
                    self._recv_buffer = b""
                    raise BrokerError("broker closed connection")
                self._recv_buffer += chunk
            line, _, self._recv_buffer = self._recv_buffer.partition(b"\n")
        response = json.loads(line.decode("utf-8"))
        if response.get("status") == "error":
            raise BrokerError(response.get("error", "broker_error"))
        return response

    def _start_heartbeat(self) -> None:
        self._stop_heartbeat()
        self._heartbeat_stop.clear()
        t = threading.Thread(target=self._heartbeat_loop, name="bus-heartbeat", daemon=True)
        t.start()
        self._heartbeat_thread = t

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        self._heartbeat_thread = None

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self._heartbeat_sec):
            if self._closed:
                return
            try:
                self.heartbeat()
            except Exception:
                # Broker unreachable — spawn reconnect, exit heartbeat loop.
                self._reconnect_async()
                return

    def _reconnect_async(self) -> None:
        if self._closed or not self._participant:
            return
        t = threading.Thread(target=self._reconnect_loop, name="bus-reconnect", daemon=True)
        t.start()

    def _reconnect_loop(self) -> None:
        attempts = 0
        while not self._closed:
            delay_ms = min(RECONNECT_BASE_MS * (2 ** attempts), RECONNECT_CAP_MS)
            time.sleep(delay_ms / 1000.0)
            attempts += 1
            try:
                self._close_socket()
                self.connect()
                response = self._request({"type": "register_participant", "participant": self._participant})
                self._lease_token = response["leaseToken"]
                self._participant_id = response["participantId"]
                if "heartbeatIntervalSec" in response:
                    self._heartbeat_sec = int(response["heartbeatIntervalSec"])
                self._start_heartbeat()
                return
            except Exception:
                continue

    def _close_socket(self) -> None:
        with self._lock:
            if self._socket is not None:
                try:
                    self._socket.close()
                except Exception:
                    pass
                self._socket = None
            self._recv_buffer = b""
