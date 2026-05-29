#!/usr/bin/env python3
"""Codex Desktop bus control sidecar.

Codex Desktop exposes CODEX_THREAD_ID to tool shells, but it is not launched
through the PTY wrapper that normally owns the inter-agent bus control sockets. This
sidecar gives a Desktop thread the same control surface as wrapped sessions:
status, enable, disable, and whoami.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socketserver
import sys
import threading
import time
from pathlib import Path
from typing import Any


THIS_FILE = Path(__file__).resolve()
BUS_ROOT = THIS_FILE.parents[2]
ADAPTERS_DIR = THIS_FILE.parents[1]
CODEX_APP_SERVER_DIR = THIS_FILE.parents[1] / "codex-app-server"
DOCUMENTS_DIR = THIS_FILE.parents[6]
CODEX_RUNTIME_DIR = DOCUMENTS_DIR / "codex" / "runtime"

for import_path in (str(ADAPTERS_DIR), str(CODEX_APP_SERVER_DIR), str(CODEX_RUNTIME_DIR)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

import session_auth  # type: ignore  # noqa: E402
from adapter import (  # type: ignore  # noqa: E402
    DESCRIPTION_MAX,
    DISPLAY_NAME_MAX,
    CodexAppServerAdapter,
)
import codex_runtime_ctl  # type: ignore  # noqa: E402


APPROVAL_DEFAULT = "never"
SANDBOX_DEFAULT = "danger-full-access"

# Control-socket ops that change state or reveal the live participant id; they
# require the per-session capability token. `status` is an unauthenticated
# liveness probe. Mirror of PRIVILEGED_CONTROL_TYPES in session-control.js.
PRIVILEGED_CONTROL_TYPES = frozenset({"enable", "disable", "whoami"})


def _stderr(message: str) -> None:
    sys.stderr.write(message.rstrip() + "\n")
    sys.stderr.flush()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


def _normalize_registration_field(value: str | None, max_length: int) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    if not normalized:
        return None
    return normalized[:max_length]


def _validate_path_id(value: str, *, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError(f"{label} is required")
    if text in {".", ".."} or "/" in text or "\x00" in text:
        raise RuntimeError(f"invalid {label}: {text!r}")
    return text


def desktop_session_id(thread_id: str) -> str:
    return "codex-desktop-" + _validate_path_id(thread_id, label="thread_id")


def _default_control_socket_dir() -> Path:
    override = os.environ.get("AGENT_BUS_CONTROL_SOCKETS_DIR")
    if isinstance(override, str) and override.strip():
        return Path(override).expanduser()
    return Path.home() / ".agent-bus" / "control-sockets"


def _default_control_log_dir() -> Path:
    override = os.environ.get("AGENT_BUS_CONTROL_LOG_DIR")
    if isinstance(override, str) and override.strip():
        return Path(override).expanduser()
    return Path.home() / ".agent-bus" / "control-logs"


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        os.chmod(path, 0o700)
    except Exception:
        pass


def _write_jsonl(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception:
        pass


class _ThreadedUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class _ControlRequestHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        controller: DesktopControl = self.server.controller  # type: ignore[attr-defined]
        while True:
            raw = self.rfile.readline()
            if not raw:
                return
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                request = json.loads(line)
            except json.JSONDecodeError:
                response = {"status": "error", "reason": "invalid_json"}
            else:
                response = controller.handle_request(request)
            self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))
            self.wfile.flush()


class DesktopControl:
    def __init__(
        self,
        *,
        session_id: str,
        thread_id: str,
        cwd: str,
        approval_mode: str,
        sandbox_mode: str,
        auto_enable: bool = False,
        display_name: str | None = None,
        description: str | None = None,
        control_socket_dir: Path | None = None,
        control_log_dir: Path | None = None,
    ) -> None:
        self.session_id = _validate_path_id(session_id, label="session_id")
        self.thread_id = _validate_path_id(thread_id, label="thread_id")
        self.cwd = str(Path(cwd or os.getcwd()).resolve())
        self.approval_mode = approval_mode.strip() or APPROVAL_DEFAULT
        self.sandbox_mode = sandbox_mode.strip() or SANDBOX_DEFAULT
        self.display_name = _normalize_registration_field(display_name, DISPLAY_NAME_MAX)
        self.description = _normalize_registration_field(description, DESCRIPTION_MAX)
        self.auto_enable = auto_enable
        self.desired_enabled = auto_enable
        self.last_error: str | None = None
        self.last_transition_at = _now_iso()
        self._adapter: CodexAppServerAdapter | None = None
        self._lock = threading.RLock()
        self.control_socket_dir = control_socket_dir or _default_control_socket_dir()
        self.control_log_dir = control_log_dir or _default_control_log_dir()
        self.control_socket_path = self.control_socket_dir / f"{self.session_id}.sock"
        self.log_path = self.control_log_dir / f"{self.session_id}.log"
        # Per-session capability token for privileged control ops. Published to a
        # 0600 file in the control-socket dir on start(); the CLI reads it.
        self._control_token = session_auth.generate_token()
        self._server: _ThreadedUnixServer | None = None
        self._server_thread: threading.Thread | None = None

    def log(self, level: str, message: str, extra: dict[str, Any] | None = None) -> None:
        payload = {
            "ts": _now_iso(),
            "level": level,
            "label": "codex-desktop-control",
            "message": message,
            "sessionId": self.session_id,
            "threadId": self.thread_id,
        }
        if extra:
            payload.update(extra)
        _write_jsonl(self.log_path, payload)

    def _thread_record_payload(self) -> dict[str, Any]:
        record = codex_runtime_ctl.fetch_thread_record(self.thread_id)
        payload: dict[str, Any] = {
            "thread_id": self.thread_id,
            "session_id": self.thread_id,
            "cwd": self.cwd,
            "source": "codex-desktop",
            "bound_at": _now_iso(),
            "last_seen_at": _now_iso(),
            "status": "bound",
            "binding_source": "codex_desktop_bus",
            "pid": os.getpid(),
            "bound_process_alive": True,
        }
        if record is not None:
            payload.update(
                {
                    "cwd": record.cwd or self.cwd,
                    "rollout_path": record.rollout_path,
                    "source": record.source or "codex-desktop",
                    "thread_created_at": record.created_at,
                    "thread_updated_at": record.updated_at,
                    "title": record.title,
                }
            )
        return payload

    def _terminal_record_payload(self) -> dict[str, Any]:
        return {
            "tty_path": "not a tty",
            "terminal_app": "codex-desktop",
            "term_program": "Codex Desktop",
            "bound_at": _now_iso(),
            "source": "codex_desktop_bus",
            "cwd": self.cwd,
            "thread_id": self.thread_id,
            "last_seen_at": _now_iso(),
            "bound_process_alive": True,
            "approval_mode": self.approval_mode,
            "sandbox_mode": self.sandbox_mode,
            "wrapper_overrides": {
                "source": "codex_desktop_bus",
                "approval_mode": self.approval_mode,
                "sandbox_mode": self.sandbox_mode,
            },
            "pid": os.getpid(),
        }

    def ensure_runtime_records(self) -> None:
        codex_runtime_ctl.atomic_write_json(
            codex_runtime_ctl.session_thread_path(self.session_id),
            self._thread_record_payload(),
        )
        codex_runtime_ctl.atomic_write_json(
            codex_runtime_ctl.session_terminal_path(self.session_id),
            self._terminal_record_payload(),
        )

    def _participant_id(self) -> str | None:
        adapter = self._adapter
        if adapter is None:
            return None
        return adapter.participant_id

    def _status_payload(self, status: str = "ok", reason: str | None = None) -> dict[str, Any]:
        enabled = self._adapter is not None and self._participant_id() is not None
        state = "enabled" if enabled else ("error" if self.last_error else "disabled")
        if self.desired_enabled and not enabled and not self.last_error:
            state = "pending"
        payload: dict[str, Any] = {
            "status": status,
            "kind": "codex",
            "adapter": "codex-desktop",
            "sessionId": self.session_id,
            "participantId": self._participant_id(),
            "desiredEnabled": self.desired_enabled,
            "enabled": enabled,
            "autoEnable": False,
            "state": state,
            "project": Path(self.cwd).name or self.cwd,
            "cwd": self.cwd,
            "displayName": self.display_name,
            "description": self.description,
            "controlSocketPath": str(self.control_socket_path),
            "deliverySocketPath": str(self._adapter.socket_path) if self._adapter else None,
            "lastError": self.last_error,
            "lastTransitionAt": self.last_transition_at,
            "logPath": str(self.log_path),
            "threadState": "ready",
            "threadId": self.thread_id,
        }
        if reason:
            payload["reason"] = reason
        return payload

    def enable(self, request: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            display_name = _normalize_registration_field(
                request.get("displayName"), DISPLAY_NAME_MAX
            )
            description = _normalize_registration_field(
                request.get("description"), DESCRIPTION_MAX
            )
            if display_name is not None:
                self.display_name = display_name
            if "description" in request:
                self.description = description

            self.desired_enabled = True
            self.last_transition_at = _now_iso()

            existing = self._adapter
            if existing is not None:
                if (
                    existing.display_name == self.display_name
                    and existing.description == self.description
                ):
                    return self._status_payload("enabled")
                existing.stop()
                self._adapter = None

            try:
                self.ensure_runtime_records()
                adapter = CodexAppServerAdapter(
                    runtime_session_id=self.session_id,
                    cwd=self.cwd,
                    display_name=self.display_name,
                    description=self.description,
                )
                adapter.start()
                self._adapter = adapter
                self.last_error = None
                self.log(
                    "info",
                    "bus_enabled",
                    {
                        "participantId": adapter.participant_id,
                        "displayName": self.display_name,
                        "description": self.description,
                    },
                )
                return self._status_payload("enabled")
            except Exception as error:
                self.last_error = str(error)
                self.log("error", "bus_enable_failed", {"error": self.last_error})
                return self._status_payload("error", self.last_error)

    def disable(self) -> dict[str, Any]:
        with self._lock:
            self.desired_enabled = False
            self.last_transition_at = _now_iso()
            adapter = self._adapter
            self._adapter = None
            if adapter is not None:
                adapter.stop()
            self.last_error = None
            self.log("info", "bus_disabled")
            return self._status_payload("disabled")

    def whoami(self) -> dict[str, Any]:
        if self._participant_id():
            return self._status_payload("ok")
        return self._status_payload("error", "bus_disabled")

    def handle_request(self, request: Any) -> dict[str, Any]:
        if not isinstance(request, dict):
            return self._status_payload("error", "unknown_type")
        req_type = request.get("type")
        # Connection != authority: privileged ops require the capability token.
        if req_type in PRIVILEGED_CONTROL_TYPES and not session_auth.timing_safe_equal(
            request.get("auth"), self._control_token
        ):
            self.log("warn", "control_unauthorized", {"type": req_type})
            return self._status_payload("error", "unauthorized")
        if req_type == "status":
            try:
                self.ensure_runtime_records()
            except Exception as error:
                self.last_error = str(error)
                self.log("error", "runtime_record_refresh_failed", {"error": self.last_error})
            return self._status_payload("ok")
        if req_type == "enable":
            return self.enable(request)
        if req_type == "disable":
            return self.disable()
        if req_type == "whoami":
            return self.whoami()
        return self._status_payload("error", "unknown_type")

    def start(self) -> None:
        _ensure_private_dir(self.control_socket_dir)
        _ensure_private_dir(self.control_log_dir)
        try:
            self.control_socket_path.unlink()
        except FileNotFoundError:
            pass
        self.ensure_runtime_records()
        server = _ThreadedUnixServer(str(self.control_socket_path), _ControlRequestHandler)
        server.controller = self  # type: ignore[attr-defined]
        os.chmod(self.control_socket_path, 0o600)
        session_auth.write_token(self.control_socket_dir, self.session_id, self._control_token)
        self._server = server
        self._server_thread = threading.Thread(
            target=server.serve_forever,
            name=f"codex-desktop-control-{self.session_id}",
            daemon=True,
        )
        self._server_thread.start()
        self.log("info", "control_started", {"controlSocketPath": str(self.control_socket_path)})
        if self.auto_enable:
            result = self.enable(
                {
                    "type": "enable",
                    "displayName": self.display_name,
                    "description": self.description,
                }
            )
            if result.get("status") == "error":
                self.log("error", "auto_enable_failed", {"reason": result.get("reason")})

    def stop(self) -> None:
        with self._lock:
            adapter = self._adapter
            self._adapter = None
        if adapter is not None:
            adapter.stop()
        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception:
                pass
            try:
                self._server.server_close()
            except Exception:
                pass
            self._server = None
        try:
            self.control_socket_path.unlink()
        except FileNotFoundError:
            pass
        session_auth.remove_token(self.control_socket_dir, self.session_id)
        self.log("info", "control_stopped")

    def wait(self) -> None:
        if self._server_thread is not None:
            self._server_thread.join()


def _resolve_thread_id(args: argparse.Namespace) -> str:
    return _validate_path_id(
        args.thread_id
        or os.environ.get("AGENT_BUS_THREAD_ID")
        or os.environ.get("CODEX_THREAD_ID")
        or "",
        label="thread_id",
    )


def _resolve_session_id(args: argparse.Namespace, thread_id: str) -> str:
    return _validate_path_id(
        args.session_id
        or os.environ.get("AGENT_BUS_RUNTIME_SESSION_ID")
        or os.environ.get("CODEX_RUNTIME_SESSION_ID")
        or desktop_session_id(thread_id),
        label="session_id",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id")
    parser.add_argument("--thread-id")
    parser.add_argument("--cwd", default=os.environ.get("AGENT_BUS_DESKTOP_CWD") or os.getcwd())
    parser.add_argument("--auto-enable", action="store_true")
    parser.add_argument("--name", dest="display_name")
    parser.add_argument("--description")
    parser.add_argument(
        "--approval-mode",
        default=os.environ.get("CODEX_DESKTOP_BUS_APPROVAL_MODE")
        or os.environ.get("CODEX_BUS_APPROVAL_MODE")
        or APPROVAL_DEFAULT,
    )
    parser.add_argument(
        "--sandbox-mode",
        default=os.environ.get("CODEX_DESKTOP_BUS_SANDBOX_MODE")
        or os.environ.get("CODEX_BUS_SANDBOX_MODE")
        or SANDBOX_DEFAULT,
    )
    args = parser.parse_args(argv)

    try:
        thread_id = _resolve_thread_id(args)
        session_id = _resolve_session_id(args, thread_id)
        control = DesktopControl(
            session_id=session_id,
            thread_id=thread_id,
            cwd=args.cwd,
            approval_mode=args.approval_mode,
            sandbox_mode=args.sandbox_mode,
            auto_enable=args.auto_enable
            or os.environ.get("AGENT_BUS_AUTO_ENABLE", "").lower()
            in {"1", "true", "yes"},
            display_name=args.display_name or os.environ.get("AGENT_BUS_DISPLAY_NAME"),
            description=args.description or os.environ.get("AGENT_BUS_DESCRIPTION"),
        )
        control.start()
    except Exception as error:
        _stderr(str(error))
        return 2

    def _shutdown(_signum: int, _frame: Any) -> None:
        control.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        control.wait()
    finally:
        control.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
