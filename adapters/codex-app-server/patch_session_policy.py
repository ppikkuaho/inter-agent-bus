#!/usr/bin/env python3
"""Idempotent helper to fill in missing policy metadata on a Codex active-terminal record.

Workaround for the v1.0 gap where `~/bin/codex` does not yet capture
`approval_mode` and `sandbox_mode` at session start. The bus adapter's P3
helper (`send_synthetic_turn_for_bus`) fails closed on null policy by design;
this script writes explicit, non-escalating defaults so a codex session can
receive bus messages. Tracked as a v1.1 item — once the wrapper captures
policy natively, this shim can be deleted.

Usage:
    patch_session_policy.py <runtime_session_id> [approval_mode] [sandbox_mode]

Defaults: approval_mode=on-request, sandbox_mode=workspace-write

Exit codes:
    0 — patched, or already had policy set (idempotent no-op).
    1 — record not found.
    2 — usage error.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys

DEFAULT_APPROVAL = "on-request"
DEFAULT_SANDBOX = "workspace-write"


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: patch_session_policy.py <runtime_session_id> [approval_mode] [sandbox_mode]", file=sys.stderr)
        return 2
    sid = argv[1]
    approval = argv[2] if len(argv) > 2 else DEFAULT_APPROVAL
    sandbox = argv[3] if len(argv) > 3 else DEFAULT_SANDBOX

    runtime_dir = pathlib.Path(os.environ.get(
        "CODEX_RUNTIME_STATE_DIR",
        os.path.expanduser("~/.codex/runtime"),
    ))
    path = runtime_dir / "active-terminals" / f"{sid}.json"
    if not path.exists():
        print(f"[patch_session_policy] record not found: {path}", file=sys.stderr)
        return 1

    record = json.loads(path.read_text())
    changed = False
    if record.get("approval_mode") is None:
        record["approval_mode"] = approval
        changed = True
    if record.get("sandbox_mode") is None:
        record["sandbox_mode"] = sandbox
        changed = True
    if changed:
        record.setdefault("wrapper_overrides", {})["source"] = "patch_session_policy"
        path.write_text(json.dumps(record, indent=2))
        print(f"[patch_session_policy] {sid}: approval_mode={approval} sandbox_mode={sandbox}")
    else:
        print(f"[patch_session_policy] {sid}: already set, no change")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
