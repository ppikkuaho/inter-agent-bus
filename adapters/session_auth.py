"""Per-session capability tokens for the bus's local Unix sockets (Python mirror).

Mirror of adapters/session-auth.js. Same scheme: the wrapper/control process
mints a random 256-bit token at startup, writes it to a 0600 file next to the
socket (`<sessionId>.token`), and privileged requests ({deliver}, {send},
enable/disable) must carry it. Comparison is constant-time.

Why: the adapter delivery socket and control socket previously treated
connection == authority. Mode 0600/0700 only excludes OTHER uids; it does not
defend against a same-uid peer (any other CLI agent the user runs). Requiring
the per-session secret raises the bar from "can connect" to "holds the secret".

Residual (documented): a same-uid process that can read arbitrary files can read
the 0600 token file. A peer-uid credential check would be additive but needs
extra plumbing and would not close the same-uid case. See DESIGN.md §6.
"""

from __future__ import annotations

import hmac
import os
import secrets
from pathlib import Path
from typing import Optional

TOKEN_BYTES = 32  # 256-bit secret.


def generate_token() -> str:
    """Fresh capability token as hex (survives JSON/file round-trips)."""
    return secrets.token_hex(TOKEN_BYTES)


def timing_safe_equal(a: Optional[str], b: Optional[str]) -> bool:
    """Constant-time compare. False for any non-string; hmac.compare_digest is
    itself constant-time and length-safe."""
    if not isinstance(a, str) or not isinstance(b, str):
        return False
    return hmac.compare_digest(a, b)


def token_path_for(socket_dir: Path | str, session_id: str) -> Path:
    return Path(socket_dir) / f"{session_id}.token"


def write_token(socket_dir: Path | str, session_id: str, token: str) -> str:
    """Write a token to a 0600 file. Opens with O_CREAT|O_WRONLY|O_TRUNC and an
    explicit 0600 mode so the secret is never briefly group/world readable."""
    path = token_path_for(socket_dir, session_id)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, token.encode("utf-8"))
    finally:
        os.close(fd)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return token


def read_token(socket_dir: Path | str, session_id: str) -> Optional[str]:
    try:
        raw = token_path_for(socket_dir, session_id).read_text(encoding="utf-8").strip()
        return raw or None
    except OSError:
        return None


def remove_token(socket_dir: Path | str, session_id: str) -> None:
    try:
        token_path_for(socket_dir, session_id).unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass
