"""Auth regression for the codex-app-server adapter: connection != authority.

A same-uid peer that can reach the 0600 adapter socket but does not hold the
per-session capability token must not be able to inject a turn ({deliver}) or
emit as the participant ({send}). The legitimate token-bearing path is accepted.

This drives CodexAppServerAdapter.handle_request directly, so it needs no live
broker, no real Codex thread, and no app-server. The external codex_runtime_ctl
module (not part of this repo's isolated tree) is stubbed so the adapter module
imports cleanly here.
"""

import sys
import types
import unittest
from pathlib import Path

BUS_ROOT = Path(__file__).resolve().parents[3]
ADAPTER_DIR = BUS_ROOT / "adapters" / "codex-app-server"
ADAPTERS_DIR = BUS_ROOT / "adapters"
CLIENT_DIR = BUS_ROOT / "client"

# Stub codex_runtime_ctl before importing adapter (the real one lives in the
# external Codex runtime tree, intentionally not bundled).
if "codex_runtime_ctl" not in sys.modules:
    stub = types.ModuleType("codex_runtime_ctl")

    class _BusPolicyError(Exception):
        pass

    def _noop(*_args, **_kwargs):
        return {"status": "accepted"}

    stub.BusPolicyError = _BusPolicyError
    stub.send_synthetic_turn_for_bus = _noop
    stub.send_terminal_turn = _noop
    stub.load_session_thread = lambda *_a, **_k: {"thread_id": "thread_test"}
    sys.modules["codex_runtime_ctl"] = stub

for p in (str(ADAPTER_DIR), str(ADAPTERS_DIR), str(CLIENT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import session_auth  # type: ignore  # noqa: E402
import adapter as adapter_mod  # type: ignore  # noqa: E402


def _make_adapter():
    a = adapter_mod.CodexAppServerAdapter(runtime_session_id="auth-test-session")
    # Pretend it is registered and bound so the privileged paths reach the auth
    # gate rather than short-circuiting on not_registered / unresolved thread.
    a.participant_id = "codex:abcabcabcabc"
    a.thread_id = "thread_test"

    class _FakeBusClient:
        def send(self, *_a, **_k):
            return {"status": "delivered", "from": a.participant_id}

    a._bus_client = _FakeBusClient()
    return a


class TestAdapterAuth(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = _make_adapter()
        self.token = self.adapter._adapter_token
        self.envelope = {
            "id": "msg_auth_1",
            "from": "claude:deadbeef0000",
            "to": "codex:abcabcabcabc",
            "body": "hello",
            "sent_at": "2026-04-17T10:00:00.000Z",
        }

    def test_deliver_without_token_rejected(self) -> None:
        r = self.adapter.handle_request({"type": "deliver", "envelope": self.envelope})
        self.assertEqual(r["status"], "rejected")
        self.assertEqual(r["reason"], "unauthorized")

    def test_deliver_wrong_token_rejected(self) -> None:
        r = self.adapter.handle_request(
            {"type": "deliver", "auth": "f" * len(self.token), "envelope": self.envelope}
        )
        self.assertEqual(r["status"], "rejected")
        self.assertEqual(r["reason"], "unauthorized")

    def test_send_without_token_rejected(self) -> None:
        r = self.adapter.handle_request(
            {"type": "send", "to": "claude:000000000001", "body": "forged"}
        )
        self.assertEqual(r["status"], "rejected")
        self.assertEqual(r["reason"], "unauthorized")

    def test_deliver_with_token_accepted(self) -> None:
        r = self.adapter.handle_request(
            {"type": "deliver", "auth": self.token, "envelope": self.envelope}
        )
        self.assertEqual(r["status"], "accepted")

    def test_send_with_token_accepted(self) -> None:
        r = self.adapter.handle_request(
            {"type": "send", "auth": self.token, "to": "claude:000000000001", "body": "ok"}
        )
        self.assertEqual(r["status"], "delivered")

    def test_register_publishes_token_in_delivery_block(self) -> None:
        # The token the broker will present on {deliver} is exactly the one the
        # adapter checks. timing_safe_equal proves they agree.
        self.assertTrue(session_auth.timing_safe_equal(self.token, self.adapter._adapter_token))


if __name__ == "__main__":
    unittest.main()
