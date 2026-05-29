"""Hermetic tests for adapters/session_auth.py (the Python capability-token
primitive used by the codex-app-server adapter and codex-desktop control to
make connection != authority on their local sockets).

These do not import codex_runtime_ctl, so they run in isolation without the
external Codex runtime tree.
"""

import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path

BUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BUS_ROOT / "adapters"))

import session_auth  # type: ignore  # noqa: E402


class TestSessionAuth(unittest.TestCase):
    def test_tokens_are_long_and_unique(self) -> None:
        a = session_auth.generate_token()
        b = session_auth.generate_token()
        # 32 bytes -> 64 hex chars.
        self.assertEqual(len(a), 64)
        self.assertNotEqual(a, b)

    def test_timing_safe_equal(self) -> None:
        tok = session_auth.generate_token()
        self.assertTrue(session_auth.timing_safe_equal(tok, tok))
        self.assertFalse(session_auth.timing_safe_equal(tok, session_auth.generate_token()))
        # Wrong-but-same-length input is rejected.
        self.assertFalse(session_auth.timing_safe_equal(tok, "f" * len(tok)))
        # Non-strings and None never authenticate, never raise.
        self.assertFalse(session_auth.timing_safe_equal(None, tok))
        self.assertFalse(session_auth.timing_safe_equal(tok, None))
        self.assertFalse(session_auth.timing_safe_equal(123, tok))  # type: ignore[arg-type]
        # Empty matches empty (but the gate only fires for present requests).
        self.assertTrue(session_auth.timing_safe_equal("", ""))

    def test_token_file_is_0600_and_round_trips(self) -> None:
        d = tempfile.mkdtemp()
        try:
            tok = session_auth.generate_token()
            session_auth.write_token(d, "sess-1", tok)
            path = session_auth.token_path_for(d, "sess-1")
            mode = stat.S_IMODE(os.stat(path).st_mode)
            self.assertEqual(mode, 0o600, oct(mode))
            self.assertEqual(session_auth.read_token(d, "sess-1"), tok)
            session_auth.remove_token(d, "sess-1")
            self.assertIsNone(session_auth.read_token(d, "sess-1"))
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    def test_read_missing_token_returns_none(self) -> None:
        d = tempfile.mkdtemp()
        try:
            self.assertIsNone(session_auth.read_token(d, "never-written"))
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
