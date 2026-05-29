#!/usr/bin/env python3
"""Auto-enable the inter-agent bus for active Codex Desktop threads."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


THIS_FILE = Path(__file__).resolve()
CONTROL_SCRIPT = THIS_FILE.parent / "control.py"
STATE_DB = Path.home() / ".codex" / "state_5.sqlite"
LOG_PATH = Path.home() / ".agent-bus" / "codex-desktop-autostart.log"
CONTROL_SOCKET_DIR = Path(
    os.environ.get(
        "AGENT_BUS_CONTROL_SOCKETS_DIR",
        str(Path.home() / ".agent-bus" / "control-sockets"),
    )
).expanduser()

DEFAULT_POLL_SEC = 5
DEFAULT_ACTIVE_WINDOW_SEC = 6 * 60 * 60
DEFAULT_LIMIT = 8
CONNECT_TIMEOUT_SEC = 0.35
DESKTOP_SOURCES = {"vscode", "desktop", "codex-desktop"}


@dataclass(frozen=True)
class ThreadRow:
    thread_id: str
    cwd: str
    title: str
    source: str
    updated_at: int


def log(message: str, extra: dict[str, Any] | None = None) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        "message": message,
    }
    if extra:
        payload.update(extra)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
    try:
        os.chmod(LOG_PATH, 0o600)
    except Exception:
        pass


def valid_path_id(value: str) -> bool:
    return bool(value) and value not in {".", ".."} and "/" not in value and "\x00" not in value


def session_id_for(thread_id: str) -> str:
    return "codex-desktop-" + thread_id


def default_name(thread_id: str) -> str:
    return session_id_for(thread_id).lower()


def description_for(row: ThreadRow) -> str:
    label = (row.title or "").strip() or Path(row.cwd).name or row.cwd
    return ("Codex Desktop: " + label)[:200]


def control_socket_path(session_id: str) -> Path:
    return CONTROL_SOCKET_DIR / f"{session_id}.sock"


def request_control(session_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    socket_path = control_socket_path(session_id)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(CONNECT_TIMEOUT_SEC)
            conn.connect(str(socket_path))
            conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            buffer = b""
            while b"\n" not in buffer:
                chunk = conn.recv(4096)
                if not chunk:
                    return None
                buffer += chunk
    except Exception:
        return None
    line, _, _ = buffer.partition(b"\n")
    try:
        return json.loads(line.decode("utf-8"))
    except Exception:
        return None


def recent_desktop_threads(*, active_window_sec: int, limit: int) -> list[ThreadRow]:
    if not STATE_DB.exists():
        return []
    cutoff = int(time.time()) - active_window_sec
    con = sqlite3.connect(f"file:{STATE_DB}?mode=ro", uri=True, timeout=1.0)
    try:
        rows = con.execute(
            """
            select id, cwd, title, source, updated_at
            from threads
            where archived = 0
              and updated_at >= ?
              and source in ('vscode', 'desktop', 'codex-desktop')
            order by updated_at desc
            limit ?
            """,
            (cutoff, limit),
        ).fetchall()
    finally:
        con.close()

    result: list[ThreadRow] = []
    for thread_id, cwd, title, source, updated_at in rows:
        thread_id = str(thread_id or "").strip()
        if not valid_path_id(thread_id):
            continue
        source = str(source or "").strip()
        if source not in DESKTOP_SOURCES:
            continue
        result.append(
            ThreadRow(
                thread_id=thread_id,
                cwd=str(cwd or str(Path.home())),
                title=str(title or ""),
                source=source,
                updated_at=int(updated_at or 0),
            )
        )
    return result


def spawn_control(row: ThreadRow) -> None:
    session_id = session_id_for(row.thread_id)
    log_file = LOG_PATH.parent / f"{session_id}.launcher.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("ab") as log_handle:
        proc = subprocess.Popen(
            [
                sys.executable,
                str(CONTROL_SCRIPT),
                "--session-id",
                session_id,
                "--thread-id",
                row.thread_id,
                "--cwd",
                row.cwd,
                "--auto-enable",
                "--name",
                default_name(row.thread_id),
                "--description",
                description_for(row),
            ],
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
            close_fds=True,
            env=os.environ.copy(),
        )
    log(
        "spawned_control",
        {"sessionId": session_id, "threadId": row.thread_id, "pid": proc.pid},
    )


def ensure_enabled(row: ThreadRow) -> None:
    session_id = session_id_for(row.thread_id)
    status = request_control(session_id, {"type": "status"})
    if status is None:
        spawn_control(row)
        return
    if status.get("enabled"):
        return
    result = request_control(
        session_id,
        {
            "type": "enable",
            "displayName": default_name(row.thread_id),
            "description": description_for(row),
        },
    )
    log(
        "enabled_existing_control",
        {
            "sessionId": session_id,
            "threadId": row.thread_id,
            "status": None if result is None else result.get("status"),
            "reason": None if result is None else result.get("reason"),
        },
    )


def scan_once(*, active_window_sec: int, limit: int) -> int:
    count = 0
    for row in recent_desktop_threads(active_window_sec=active_window_sec, limit=limit):
        ensure_enabled(row)
        count += 1
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-sec", type=int, default=DEFAULT_POLL_SEC)
    parser.add_argument("--active-window-sec", type=int, default=DEFAULT_ACTIVE_WINDOW_SEC)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args(argv)

    log(
        "autostart_started",
        {
            "once": args.once,
            "pollSec": args.poll_sec,
            "activeWindowSec": args.active_window_sec,
            "limit": args.limit,
        },
    )
    while True:
        try:
            count = scan_once(
                active_window_sec=max(60, args.active_window_sec),
                limit=max(1, args.limit),
            )
            log("scan_complete", {"threadCount": count})
        except Exception as error:
            log("scan_failed", {"error": str(error)})
        if args.once:
            return 0
        time.sleep(max(1, args.poll_sec))


if __name__ == "__main__":
    raise SystemExit(main())
