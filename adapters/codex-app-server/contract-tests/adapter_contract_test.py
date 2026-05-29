"""Contract tests for the Codex app-server bus adapter."""

from __future__ import annotations

import json
import os
import shutil
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
ADAPTER_DIR = THIS_DIR.parent
BUS_ROOT = ADAPTER_DIR.parents[1]
CLIENT_DIR = BUS_ROOT / "client"
BROKER_JS = BUS_ROOT / "broker" / "broker.js"

sys.path.insert(0, str(THIS_DIR))
sys.path.insert(0, str(ADAPTER_DIR))
sys.path.insert(0, str(CLIENT_DIR))

import adapter  # noqa: E402
import codex_thread_probe  # noqa: E402
from client import BusClient  # noqa: E402


def _send_line(socket_path: str, payload: dict) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(2)
        conn.connect(socket_path)
        conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))
        buffer = b""
        while b"\n" not in buffer:
            chunk = conn.recv(4096)
            if not chunk:
                raise RuntimeError("adapter socket closed before replying")
            buffer += chunk
    line, _, _ = buffer.partition(b"\n")
    return json.loads(line.decode("utf-8"))


def _wait_for(predicate, *, timeout: float = 2.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met before timeout")


class _ReceiverServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class _ReceiverHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        raw = self.rfile.readline()
        if not raw:
            return
        request = json.loads(raw.decode("utf-8"))
        self.server.deliveries.append(request)  # type: ignore[attr-defined]
        self.wfile.write(b'{"status":"accepted"}\n')
        self.wfile.flush()


class AdapterContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp_root = tempfile.mkdtemp(prefix="bca-", dir="/tmp")
        cls.broker_socket = os.path.join(cls.tmp_root, "broker.sock")
        env = os.environ.copy()
        env["AGENT_BUS_STATE_DIR"] = cls.tmp_root
        env["AGENT_BUS_SOCKET"] = cls.broker_socket
        cls.broker = subprocess.Popen(
            ["node", str(BROKER_JS)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.time() + 3.0
        while time.time() < deadline and not os.path.exists(cls.broker_socket):
            time.sleep(0.03)
        if not os.path.exists(cls.broker_socket):
            cls.broker.kill()
            stdout, stderr = cls.broker.communicate(timeout=2)
            raise RuntimeError(
                f"broker did not start. stdout={stdout!r} stderr={stderr!r}"
            )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.broker.terminate()
        try:
            cls.broker.wait(timeout=3)
        except subprocess.TimeoutExpired:
            cls.broker.kill()
            cls.broker.wait(timeout=3)
        shutil.rmtree(cls.tmp_root, ignore_errors=True)

    def _start_adapter(
        self,
        probe: codex_thread_probe.ProbeRecorder,
        *,
        runtime_session_id: str,
        thread_id: str,
        display_name: str | None = None,
        description: str | None = None,
    ) -> adapter.CodexAppServerAdapter:
        probe.set_session_thread(runtime_session_id, {"thread_id": thread_id})
        socket_dir = tempfile.mkdtemp(dir=self.tmp_root, prefix="s-")
        self.addCleanup(shutil.rmtree, socket_dir, True)
        adapter_instance = adapter.CodexAppServerAdapter(
            runtime_session_id=runtime_session_id,
            bus_client_factory=lambda: BusClient(socket_path=self.broker_socket),
            load_session_thread=probe.load_session_thread,
            send_terminal_input=probe.send_terminal_turn,
            send_turn_for_bus=probe.send_synthetic_turn_for_bus,
            policy_error_cls=codex_thread_probe.BusPolicyError,
            socket_dir=socket_dir,
            cwd=str(BUS_ROOT),
            display_name=display_name,
            description=description,
        )
        adapter_instance.start()
        self.addCleanup(adapter_instance.stop)
        return adapter_instance

    def _start_receiver(self) -> tuple[_ReceiverServer, str]:
        socket_dir = tempfile.mkdtemp(dir=self.tmp_root, prefix="r-")
        self.addCleanup(shutil.rmtree, socket_dir, True)
        socket_path = os.path.join(socket_dir, "receiver.sock")
        server = _ReceiverServer(socket_path, _ReceiverHandler)
        server.deliveries = []  # type: ignore[attr-defined]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server, socket_path

    def test_deliver_invokes_helper_with_rendered_text(self) -> None:
        probe = codex_thread_probe.ProbeRecorder()
        runtime_session_id = "deliver-" + os.urandom(4).hex()
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id=runtime_session_id,
            thread_id="thread-deliver",
        )
        envelope = {
            "id": "msg_contract_ok",
            "from": "claude:sender000001",
            "to": "codex:receiver0001",
            "body": "hello from the test",
            "sent_at": "2026-04-17T09:55:12.345Z",
        }

        response = _send_line(
            str(adapter_instance.socket_path),
            {"type": "deliver", "auth": adapter_instance._adapter_token, "envelope": envelope},
        )

        self.assertEqual(response, {"status": "accepted"})
        self.assertEqual(len(probe.terminal_calls), 1)
        self.assertEqual(len(probe.calls), 1)
        self.assertEqual(probe.calls[0]["thread_id"], "thread-deliver")
        self.assertEqual(probe.calls[0]["runtime_session_id"], runtime_session_id)
        self.assertEqual(probe.calls[0]["wait_timeout"], 15)
        self.assertEqual(
            probe.calls[0]["text"],
            "[from:claude:sender000001 · msg:msg_contract_ok · 09:55 Z] hello from the test",
        )

    def test_dedupe_rejects_duplicate_envelope_id(self) -> None:
        probe = codex_thread_probe.ProbeRecorder()
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id="dupe-" + os.urandom(4).hex(),
            thread_id="thread-dupe",
        )
        payload = {
            "type": "deliver",
            "auth": adapter_instance._adapter_token,
            "envelope": {
                "id": "msg_duplicate",
                "from": "claude:sender000002",
                "to": "codex:receiver0002",
                "body": "one delivery only",
                "sent_at": "2026-04-17T10:10:00.000Z",
            },
        }

        first = _send_line(str(adapter_instance.socket_path), payload)
        second = _send_line(str(adapter_instance.socket_path), payload)

        self.assertEqual(first, {"status": "accepted"})
        self.assertEqual(second, {"status": "rejected", "reason": "duplicate"})
        self.assertEqual(len(probe.terminal_calls), 1)
        self.assertEqual(len(probe.calls), 1)

    def test_malformed_deliver_rejected_without_helper_call(self) -> None:
        probe = codex_thread_probe.ProbeRecorder()
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id="malformed-" + os.urandom(4).hex(),
            thread_id="thread-malformed",
        )
        # Present the capability token so these reach the envelope-shape checks
        # (the point of this test) rather than tripping the auth gate first.
        tok = adapter_instance._adapter_token
        payloads = [
            {"type": "deliver", "auth": tok},
            {"type": "deliver", "auth": tok, "envelope": {}},
            {"type": "deliver", "auth": tok, "envelope": {"id": "msg_missing_body", "body": ""}},
            {"type": "ping", "auth": tok, "envelope": {"id": "msg_wrong_type", "body": "x"}},
        ]
        expected_reasons = ["unknown_type", "missing_id", "missing_body", "unknown_type"]

        responses = [
            _send_line(str(adapter_instance.socket_path), payload) for payload in payloads
        ]

        self.assertEqual(
            responses,
            [
                {"status": "rejected", "reason": expected_reasons[0]},
                {"status": "rejected", "reason": expected_reasons[1]},
                {"status": "rejected", "reason": expected_reasons[2]},
                {"status": "rejected", "reason": expected_reasons[3]},
            ],
        )
        self.assertEqual(probe.calls, [])
        self.assertEqual(probe.terminal_calls, [])

    def test_policy_unknown_fails_closed(self) -> None:
        probe = codex_thread_probe.ProbeRecorder(raise_policy_unknown=True)
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id="policy-" + os.urandom(4).hex(),
            thread_id="thread-policy",
        )
        payload = {
            "type": "deliver",
            "auth": adapter_instance._adapter_token,
            "envelope": {
                "id": "msg_policy_unknown",
                "from": "claude:sender000003",
                "to": "codex:receiver0003",
                "body": "fail closed",
                "sent_at": "2026-04-17T11:00:00.000Z",
            },
        }

        response = _send_line(str(adapter_instance.socket_path), payload)

        self.assertEqual(response["status"], "rejected")
        self.assertTrue(response["reason"].startswith("policy_unknown"))
        self.assertEqual(len(probe.terminal_calls), 1)
        self.assertEqual(len(probe.calls), 1)

    def test_live_terminal_delivery_short_circuits_thread_fallback(self) -> None:
        probe = codex_thread_probe.ProbeRecorder(raise_terminal_input=False)
        runtime_session_id = "live-" + os.urandom(4).hex()
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id=runtime_session_id,
            thread_id="thread-live",
        )
        envelope = {
            "id": "msg_live_ok",
            "from": "claude:sender000004",
            "to": "codex:receiver0004",
            "body": "show up in the active tui",
            "sent_at": "2026-04-19T08:50:00.000Z",
        }

        response = _send_line(
            str(adapter_instance.socket_path),
            {"type": "deliver", "auth": adapter_instance._adapter_token, "envelope": envelope},
        )

        self.assertEqual(response, {"status": "accepted"})
        self.assertEqual(len(probe.terminal_calls), 1)
        self.assertEqual(probe.terminal_calls[0]["runtime_session_id"], runtime_session_id)
        self.assertEqual(
            probe.terminal_calls[0]["text"],
            "[from:claude:sender000004 · msg:msg_live_ok · 08:50 Z] show up in the active tui",
        )
        self.assertEqual(probe.calls, [])

    def test_e2e_broker_routes_to_codex_adapter(self) -> None:
        probe = codex_thread_probe.ProbeRecorder()
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id="e2e-" + os.urandom(4).hex(),
            thread_id="thread-e2e",
        )
        sender = BusClient(socket_path=self.broker_socket)
        self.addCleanup(sender.unregister)
        sender_reg = sender.register({
            "kind": "claude",
            "sessionId": "sender-" + os.urandom(4).hex(),
            "project": "agent-bus",
            "cwd": str(BUS_ROOT),
            "delivery": {
                "adapter": "claude-pty",
                "socketPath": "/nonexistent",
            },
        })

        result = sender._request({
            "type": "push_message",
            "to": adapter_instance.participant_id,
            "leaseToken": sender.lease_token,
            "message": {
                "id": "msg_e2e_1",
                "body": "hi from the bus",
                "sent_at": "2026-04-17T09:55:12.345Z",
            },
        })

        self.assertEqual(result["status"], "delivered")
        self.assertEqual(result["from"], sender_reg["participantId"])
        _wait_for(lambda: len(probe.calls) == 1)
        self.assertEqual(probe.calls[0]["thread_id"], "thread-e2e")
        self.assertEqual(
            probe.calls[0]["text"],
            f"[from:{sender_reg['participantId']} · msg:msg_e2e_1 · 09:55 Z] hi from the bus",
        )

    def test_registration_forwards_display_name_and_description(self) -> None:
        probe = codex_thread_probe.ProbeRecorder()
        runtime_session_id = "named-" + os.urandom(4).hex()
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id=runtime_session_id,
            thread_id="thread-named",
            display_name="codex-desktop-test",
            description="Desktop app bus control test",
        )
        observer = BusClient(socket_path=self.broker_socket)
        self.addCleanup(observer.unregister)
        observer.register({
            "kind": "cli",
            "sessionId": "observer-" + os.urandom(4).hex(),
            "project": "tests",
            "cwd": str(BUS_ROOT),
            "delivery": {"adapter": "cli-ephemeral"},
        })

        participants = observer.list_participants()
        row = next(p for p in participants if p["participantId"] == adapter_instance.participant_id)

        self.assertEqual(adapter_instance.participant_id, "codex-desktop-test")
        self.assertEqual(row["displayName"], "codex-desktop-test")
        self.assertEqual(row["description"], "Desktop app bus control test")

    def test_adapter_socket_can_send_as_registered_codex_session(self) -> None:
        probe = codex_thread_probe.ProbeRecorder()
        adapter_instance = self._start_adapter(
            probe,
            runtime_session_id="sender-" + os.urandom(4).hex(),
            thread_id="thread-sender",
            display_name="codex-sender-test",
        )
        receiver_server, receiver_socket = self._start_receiver()
        receiver = BusClient(socket_path=self.broker_socket)
        self.addCleanup(receiver.unregister)
        receiver_reg = receiver.register({
            "kind": "claude",
            "sessionId": "receiver-" + os.urandom(4).hex(),
            "project": "agent-bus",
            "cwd": str(BUS_ROOT),
            "delivery": {
                "adapter": "claude-pty",
                "socketPath": receiver_socket,
            },
        })

        response = _send_line(
            str(adapter_instance.socket_path),
            {"type": "send", "auth": adapter_instance._adapter_token, "to": receiver_reg["participantId"], "body": "hello peer"},
        )

        self.assertEqual(response["status"], "delivered")
        self.assertEqual(response["from"], "codex-sender-test")
        _wait_for(lambda: len(receiver_server.deliveries) == 1)  # type: ignore[attr-defined]
        envelope = receiver_server.deliveries[0]["envelope"]  # type: ignore[attr-defined]
        self.assertEqual(envelope["from"], "codex-sender-test")
        self.assertEqual(envelope["body"], "hello peer")


if __name__ == "__main__":
    unittest.main(verbosity=2)
