"""Smoke test for the Python bus client.

Spawns the Node broker against a scratch state dir, exercises the Python
client's register / list / send / unregister against it, and confirms the wire
protocol is compatible end-to-end. Does NOT depend on the Node client or any
adapter; proves the Python client can stand alone.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
BUS_ROOT = THIS_DIR.parent
CLIENT_DIR = BUS_ROOT / "client"
BROKER_JS = BUS_ROOT / "broker" / "broker.js"

# Make `from client import BusClient` work.
sys.path.insert(0, str(CLIENT_DIR))
from client import BusClient, BrokerError  # noqa: E402


class TestPythonBusClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp_state = tempfile.mkdtemp(prefix="bus-pyclient-")
        cls.socket_path = os.path.join(cls.tmp_state, "broker.sock")
        env = os.environ.copy()
        env["AGENT_BUS_STATE_DIR"] = cls.tmp_state
        env["AGENT_BUS_SOCKET"] = cls.socket_path
        # This smoke test registers with `/nonexistent` delivery sockets, so
        # relax the broker's delivery-socket binding to the always-on traversal
        # hardening. Production never sets this. See broker.js
        # resolveDeliverySocketPath.
        env["AGENT_BUS_ALLOW_UNBOUND_DELIVERY_SOCKET"] = "1"
        cls.broker = subprocess.Popen(
            ["node", str(BROKER_JS)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.time() + 3
        while time.time() < deadline and not os.path.exists(cls.socket_path):
            time.sleep(0.03)
        if not os.path.exists(cls.socket_path):
            cls.broker.kill()
            out, err = cls.broker.communicate(timeout=2)
            raise RuntimeError(
                f"broker did not start. stdout={out!r} stderr={err!r}"
            )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.broker.terminate()
        try:
            cls.broker.wait(timeout=3)
        except subprocess.TimeoutExpired:
            cls.broker.kill()
        shutil.rmtree(cls.tmp_state, ignore_errors=True)

    def test_register_list_send_unregister(self) -> None:
        client = BusClient(socket_path=self.socket_path)
        reg = client.register({
            "kind": "codex",
            "sessionId": "py-smoke-" + os.urandom(4).hex(),
            "project": "agent-bus",
            "cwd": "/tmp",
            "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
        })
        import re
        self.assertRegex(reg["participantId"], r"^codex:[0-9a-f]{12}\d?$")
        self.assertTrue(reg["leaseToken"].startswith("lease_"))

        # List includes us.
        lst = client.list_participants()
        ids = [p["participantId"] for p in lst]
        self.assertIn(reg["participantId"], ids)
        # delivery.socketPath must NOT be exposed over list.
        for p in lst:
            self.assertNotIn("socketPath", p.get("delivery", {}))

        # Send to a never-seen id returns failed (recentParticipants scope).
        res = client.send("claude:ffffffffffff", "nope")
        self.assertEqual(res["status"], "failed")
        self.assertEqual(res["reason"], "no_such_participant")

        # Spoofed `from` in envelope is ignored; broker stamps from lease.
        res2 = client._request({
            "type": "push_message",
            "to": reg["participantId"],
            "leaseToken": client.lease_token,
            "message": {
                "id": "msg_self_" + os.urandom(4).hex(),
                "body": "to self",
                "from": "evil:dead",
                "sent_at": "2026-04-17T10:00:00.000Z",
            },
        })
        # Sending to self: recipient is live (us), forward will fail (socketPath=/nonexistent),
        # so broker queues to mailbox.
        self.assertIn(res2["status"], ("delivered", "queued"))
        self.assertEqual(res2["from"], reg["participantId"])

        client.unregister()
        # After unregister, lease token cleared.
        self.assertIsNone(client.lease_token)

    def test_malformed_to_rejected(self) -> None:
        client = BusClient(socket_path=self.socket_path)
        client.register({
            "kind": "codex",
            "sessionId": "py-bad-" + os.urandom(4).hex(),
            "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
        })
        with self.assertRaises(BrokerError) as cm:
            client.send("../../etc/passwd", "malicious")
        self.assertIn("invalid_to_participant_id", str(cm.exception))
        client.unregister()

    def test_reregister_preserves_identity_once_prior_lease_gone(self) -> None:
        # Legitimate silent re-register after the prior presence is no longer
        # live: identity continuity is preserved (same participantId, fresh
        # lease). Reclaiming a still-live lease is refused — see the test below.
        session_id = "py-reclaim-" + os.urandom(4).hex()
        first = BusClient(socket_path=self.socket_path)
        reg1 = first.register({
            "kind": "codex",
            "sessionId": session_id,
            "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
        })
        self.assertFalse(reg1["reclaimed"])
        # Release the prior lease so the session is no longer live.
        first.unregister()

        second = BusClient(socket_path=self.socket_path)
        reg2 = second.register({
            "kind": "codex",
            "sessionId": session_id,
            "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
        })
        self.assertEqual(reg2["participantId"], reg1["participantId"])
        self.assertNotEqual(reg2["leaseToken"], reg1["leaseToken"])
        # The fresh lease works.
        self.assertEqual(second.heartbeat()["status"], "ok")
        second.unregister()

    def test_reregister_refused_while_lease_live(self) -> None:
        # Hardening: sessionId is not a peer secret; a peer that learns it must
        # not be able to re-register it and hijack a live session's identity.
        session_id = "py-reclaim-live-" + os.urandom(4).hex()
        victim = BusClient(socket_path=self.socket_path)
        victim.register({
            "kind": "codex",
            "sessionId": session_id,
            "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
        })
        attacker = BusClient(socket_path=self.socket_path)
        with self.assertRaises(BrokerError) as cm:
            attacker.register({
                "kind": "codex",
                "sessionId": session_id,
                "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
            })
        self.assertIn("session_lease_active", str(cm.exception))
        # The victim's lease is untouched.
        self.assertEqual(victim.heartbeat()["status"], "ok")
        victim.unregister()

    def test_state_dir_env_resolves_default_socket(self) -> None:
        old_state_dir = os.environ.get("AGENT_BUS_STATE_DIR")
        old_socket = os.environ.get("AGENT_BUS_SOCKET")
        try:
            os.environ["AGENT_BUS_STATE_DIR"] = self.tmp_state
            if "AGENT_BUS_SOCKET" in os.environ:
                del os.environ["AGENT_BUS_SOCKET"]

            client = BusClient()
            reg = client.register({
                "kind": "codex",
                "sessionId": "py-state-dir-" + os.urandom(4).hex(),
                "displayName": "Py State Dir",
                "description": "Uses derived broker socket",
                "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
            })
            participants = client.list_participants()
            listed = next(p for p in participants if p["participantId"] == reg["participantId"])
            self.assertEqual(listed["displayName"], "Py State Dir")
            self.assertEqual(listed["description"], "Uses derived broker socket")
            client.unregister()
        finally:
            if old_state_dir is None:
                os.environ.pop("AGENT_BUS_STATE_DIR", None)
            else:
                os.environ["AGENT_BUS_STATE_DIR"] = old_state_dir

            if old_socket is None:
                os.environ.pop("AGENT_BUS_SOCKET", None)
            else:
                os.environ["AGENT_BUS_SOCKET"] = old_socket

    def test_display_name_becomes_human_participant_id(self) -> None:
        client = BusClient(socket_path=self.socket_path)
        reg = client.register({
            "kind": "codex",
            "sessionId": "py-human-id-" + os.urandom(4).hex(),
            "displayName": "Codex Architecture",
            "description": "Owns architecture questions",
            "delivery": {"adapter": "codex-app-server", "socketPath": "/nonexistent"},
        })
        self.assertEqual(reg["participantId"], "codex-architecture")
        participants = client.list_participants()
        listed = next(p for p in participants if p["participantId"] == "codex-architecture")
        self.assertEqual(listed["displayName"], "Codex Architecture")
        client.unregister()


if __name__ == "__main__":
    unittest.main()
