#!/usr/bin/env python3
"""Resolve effective Codex launch policy metadata for the wrapper.

The bus adapter's P3 helper needs recipient launch policy metadata in the
per-session active-terminal record. The daily wrapper already sees the raw
command-line args, the user's config, and convenience flags; this helper turns
that into a single normalized view the wrapper can record.
"""

from __future__ import annotations

import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback
    tomllib = None  # type: ignore[assignment]


DEFAULT_CONFIG = Path.home() / ".codex" / "config.toml"


def _read_config(path: Path) -> dict[str, Any]:
    if tomllib is None or not path.exists():
        return {}
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _strip_quotes(text: str) -> str:
    stripped = text.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {'"', "'"}:
        return stripped[1:-1]
    return stripped


def _split_value(args: list[str], index: int, long_flag: str) -> tuple[str | None, int]:
    arg = args[index]
    if arg == long_flag:
        if index + 1 >= len(args):
            return None, index
        return args[index + 1], index + 1
    prefix = long_flag + "="
    if arg.startswith(prefix):
        return arg[len(prefix) :], index
    return None, index


def _parse_assignment(text: str) -> tuple[str | None, str | None]:
    if "=" not in text:
        return None, None
    key, value = text.split("=", 1)
    return key.strip(), _strip_quotes(value)


def resolve_policy(args: list[str]) -> dict[str, Any]:
    config_path = Path(os.environ.get("CODEX_CONFIG_FILE", str(DEFAULT_CONFIG))).expanduser()
    config = _read_config(config_path)

    approval_mode = config.get("approval_policy") or config.get("approval_mode")
    sandbox_mode = config.get("sandbox_mode") or config.get("sandbox")
    profile = None
    add_dirs: list[str] = []
    config_overrides: list[str] = []
    full_auto = False
    dangerously_bypass = False

    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--":
            break

        value, consumed = _split_value(args, index, "--approval-mode")
        if value is None:
            value, consumed = _split_value(args, index, "--approval-policy")
        if value is not None:
            approval_mode = value
            index = consumed + 1
            continue

        value, consumed = _split_value(args, index, "--sandbox-mode")
        if value is None:
            value, consumed = _split_value(args, index, "--sandbox")
        if value is not None:
            sandbox_mode = value
            index = consumed + 1
            continue

        value, consumed = _split_value(args, index, "--profile")
        if value is not None:
            profile = value
            index = consumed + 1
            continue

        value, consumed = _split_value(args, index, "--add-dir")
        if value is not None:
            add_dirs.append(value)
            index = consumed + 1
            continue

        value, consumed = _split_value(args, index, "--config")
        if value is None:
            value, consumed = _split_value(args, index, "--config-override")
        if value is not None:
            config_overrides.append(value)
            key, parsed = _parse_assignment(value)
            if key in {"approval_policy", "approval_mode"} and parsed:
                approval_mode = parsed
            elif key in {"sandbox", "sandbox_mode"} and parsed:
                sandbox_mode = parsed
            index = consumed + 1
            continue

        if arg == "-a":
            if index + 1 < len(args):
                approval_mode = args[index + 1]
                index += 2
                continue
        elif arg.startswith("-a="):
            approval_mode = arg.split("=", 1)[1]
            index += 1
            continue
        elif arg == "-s":
            if index + 1 < len(args):
                sandbox_mode = args[index + 1]
                index += 2
                continue
        elif arg.startswith("-s="):
            sandbox_mode = arg.split("=", 1)[1]
            index += 1
            continue
        elif arg == "-p":
            if index + 1 < len(args):
                profile = args[index + 1]
                index += 2
                continue
        elif arg.startswith("-p="):
            profile = arg.split("=", 1)[1]
            index += 1
            continue
        elif arg == "-c":
            if index + 1 < len(args):
                value = args[index + 1]
                config_overrides.append(value)
                key, parsed = _parse_assignment(value)
                if key in {"approval_policy", "approval_mode"} and parsed:
                    approval_mode = parsed
                elif key in {"sandbox", "sandbox_mode"} and parsed:
                    sandbox_mode = parsed
                index += 2
                continue

        if arg == "--full-auto":
            full_auto = True
            approval_mode = approval_mode or "on-request"
            sandbox_mode = sandbox_mode or "workspace-write"
            index += 1
            continue
        if arg == "--dangerously-bypass-approvals-and-sandbox":
            dangerously_bypass = True
            approval_mode = "never"
            sandbox_mode = "danger-full-access"
            index += 1
            continue

        index += 1

    if dangerously_bypass:
        approval_mode = "never"
        sandbox_mode = "danger-full-access"
    elif full_auto:
        approval_mode = "on-request"
        sandbox_mode = "workspace-write"

    return {
        "approval_mode": approval_mode,
        "sandbox_mode": sandbox_mode,
        "full_auto": full_auto,
        "dangerously_bypass": dangerously_bypass,
        "profile": profile,
        "add_dirs": add_dirs,
        "config_overrides": config_overrides,
        "config_path": str(config_path),
    }


def _shell_quote(value: str | None) -> str:
    return shlex.quote("" if value is None else value)


def _shell_array(name: str, values: list[str]) -> str:
    quoted = " ".join(shlex.quote(value) for value in values)
    return f"{name}=({quoted})"


def emit_shell(payload: dict[str, Any]) -> str:
    lines = [
        f"CODEX_WRAPPER_APPROVAL_MODE={_shell_quote(payload.get('approval_mode'))}",
        f"CODEX_WRAPPER_SANDBOX_MODE={_shell_quote(payload.get('sandbox_mode'))}",
        f"CODEX_WRAPPER_FULL_AUTO={'1' if payload.get('full_auto') else '0'}",
        f"CODEX_WRAPPER_DANGEROUSLY_BYPASS={'1' if payload.get('dangerously_bypass') else '0'}",
        f"CODEX_WRAPPER_PROFILE={_shell_quote(payload.get('profile'))}",
        _shell_array("CODEX_WRAPPER_ADD_DIRS", list(payload.get("add_dirs") or [])),
        _shell_array(
            "CODEX_WRAPPER_CONFIG_OVERRIDES",
            list(payload.get("config_overrides") or []),
        ),
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    shell_mode = False
    if args and args[0] == "--shell":
        shell_mode = True
        args = args[1:]
    if args and args[0] == "--":
        args = args[1:]

    payload = resolve_policy(args)
    if shell_mode:
        sys.stdout.write(emit_shell(payload) + "\n")
    else:
        json.dump(payload, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
