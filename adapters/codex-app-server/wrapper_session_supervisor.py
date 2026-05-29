#!/usr/bin/env python3
"""Detached supervisor for wrapper-launched Codex bus sessions."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


THIS_FILE = Path(__file__).resolve()
ADAPTER_SCRIPT = THIS_FILE.parent / "adapter.py"


def pid_is_usable(pid: int) -> bool:
    try:
        result = subprocess.run(
            ["ps", "-o", "state=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return False
    state = result.stdout.strip()
    return result.returncode == 0 and bool(state) and not state.startswith("Z")


def load_thread_id(path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    thread_id = payload.get("thread_id")
    if isinstance(thread_id, str) and thread_id.strip():
        return thread_id.strip()
    return None


def wait_for_thread(parent_pid: int, thread_file: Path, timeout_sec: int) -> str | None:
    deadline = time.time() + max(1, timeout_sec)
    while time.time() < deadline:
        thread_id = load_thread_id(thread_file)
        if thread_id:
            return thread_id
        if not pid_is_usable(parent_pid):
            return None
        time.sleep(1)
    return None


def terminate_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
        return
    except Exception:
        pass
    try:
        proc.kill()
        proc.wait(timeout=5)
    except Exception:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-pid", required=True, type=int)
    parser.add_argument("--runtime-session-id", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--thread-wait-sec", type=int, default=300)
    parser.add_argument("--adapter-log")
    args = parser.parse_args(argv)

    state_dir = Path(args.state_dir).expanduser()
    thread_file = state_dir / "active-threads" / f"{args.runtime_session_id}.json"
    adapter_log = (
        Path(args.adapter_log).expanduser()
        if args.adapter_log
        else state_dir / f"bus-adapter-{args.runtime_session_id}.log"
    )
    adapter_log.parent.mkdir(parents=True, exist_ok=True)

    thread_id = wait_for_thread(args.parent_pid, thread_file, args.thread_wait_sec)
    if thread_id is None:
        return 0

    env = dict(os.environ)
    env["AGENT_BUS_RUNTIME_SESSION_ID"] = args.runtime_session_id

    with adapter_log.open("ab") as log_handle:
        adapter = subprocess.Popen(
            [sys.executable, str(ADAPTER_SCRIPT)],
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
            close_fds=True,
        )
        try:
            while pid_is_usable(args.parent_pid):
                if adapter.poll() is not None:
                    return 0
                time.sleep(2)
        finally:
            terminate_process(adapter)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
