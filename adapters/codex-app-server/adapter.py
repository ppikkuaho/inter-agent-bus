#!/usr/bin/env python3
"""Codex app-server adapter for broker-delivered inter-agent messages."""

from __future__ import annotations

import json
import os
import signal
import socketserver
import sys
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable

THIS_FILE = Path(__file__).resolve()
BUS_ROOT = THIS_FILE.parents[2]
CLIENT_DIR = BUS_ROOT / "client"
ADAPTERS_DIR = THIS_FILE.parents[1]
DOCUMENTS_DIR = THIS_FILE.parents[6]
CODEX_RUNTIME_DIR = DOCUMENTS_DIR / "codex" / "runtime"

for import_path in (str(CLIENT_DIR), str(ADAPTERS_DIR), str(CODEX_RUNTIME_DIR)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

import session_auth  # type: ignore  # noqa: E402
from client import BusClient  # type: ignore  # noqa: E402
import codex_runtime_ctl  # type: ignore  # noqa: E402
from codex_runtime_ctl import (  # type: ignore  # noqa: E402
    BusPolicyError,
    send_synthetic_turn_for_bus,
    send_terminal_turn,
)

DEDUPE_LRU_SIZE = 256
WAIT_TIMEOUT_SEC = 15
DISPLAY_NAME_MAX = 64
DESCRIPTION_MAX = 200
RUNTIME_SESSION_ENV_KEYS = (
    "AGENT_BUS_RUNTIME_SESSION_ID",
    "CODEX_RUNTIME_SESSION_ID",
)


def _stderr(message: str) -> None:
    sys.stderr.write(message.rstrip() + "\n")
    sys.stderr.flush()


def _resolve_runtime_session_id(argv: list[str], env: dict[str, str]) -> str:
    for key in RUNTIME_SESSION_ENV_KEYS:
        value = env.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if argv and str(argv[0]).strip():
        return str(argv[0]).strip()
    expected = " or ".join(RUNTIME_SESSION_ENV_KEYS)
    raise RuntimeError(
        f"missing runtime_session_id; set {expected}, or pass it as argv[1]"
    )


def _default_socket_dir() -> Path:
    override = os.environ.get("AGENT_BUS_ADAPTER_SOCKETS_DIR")
    if isinstance(override, str) and override.strip():
        return Path(override).expanduser()
    return Path.home() / ".agent-bus" / "adapter-sockets"


def _ensure_socket_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def _render_time_fragment(sent_at: Any) -> str:
    if not isinstance(sent_at, str):
        return ""
    text = sent_at.strip()
    if len(text) < 16 or text[10] != "T" or text[13] != ":":
        return ""
    hhmm = text[11:16]
    if len(hhmm) != 5 or hhmm[2] != ":":
        return ""
    return f"{hhmm} Z"


def render_envelope(envelope: dict[str, Any]) -> str:
    return (
        f"[from:{envelope.get('from', '')} · msg:{envelope.get('id', '')} · "
        f"{_render_time_fragment(envelope.get('sent_at'))}] {envelope.get('body', '')}"
    )


def _brief_message(error: BaseException) -> str:
    text = str(error).strip().replace("\n", " ")
    if not text:
        text = error.__class__.__name__
    return text[:80]


def _normalize_registration_field(value: str | None, max_length: int) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    if not normalized:
        return None
    return normalized[:max_length]


class _DedupeLRU:
    def __init__(self, size: int) -> None:
        self.size = size
        self._items: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.Lock()

    def seen_before(self, value: str) -> bool:
        with self._lock:
            if value in self._items:
                self._items.move_to_end(value)
                return True
            self._items[value] = None
            if len(self._items) > self.size:
                self._items.popitem(last=False)
            return False


class _ThreadedUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class _AdapterRequestHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        adapter: CodexAppServerAdapter = self.server.adapter  # type: ignore[attr-defined]
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
                response = {"status": "rejected", "reason": "invalid_json"}
            else:
                response = adapter.handle_request(request)
            self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))
            self.wfile.flush()


class CodexAppServerAdapter:
    def __init__(
        self,
        *,
        runtime_session_id: str,
        bus_client_factory: Callable[[], Any] = BusClient,
        load_session_thread: Callable[[str | None], dict[str, Any] | None] | None = None,
        send_terminal_input: Callable[..., dict[str, Any]] | None = None,
        send_turn_for_bus: Callable[..., dict[str, Any]] | None = None,
        policy_error_cls: type[BaseException] = BusPolicyError,
        socket_dir: str | Path | None = None,
        cwd: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
    ) -> None:
        session_id = str(runtime_session_id).strip()
        if not session_id:
            raise RuntimeError("runtime_session_id is required")
        self.runtime_session_id = session_id
        self._bus_client_factory = bus_client_factory
        self._load_session_thread = load_session_thread or codex_runtime_ctl.load_session_thread
        self._send_terminal_input = send_terminal_input or send_terminal_turn
        self._send_turn_for_bus = send_turn_for_bus or send_synthetic_turn_for_bus
        self._policy_error_cls = policy_error_cls
        self.cwd = cwd or os.getcwd()
        self.project = os.path.basename(self.cwd) or self.cwd
        self.display_name = _normalize_registration_field(display_name, DISPLAY_NAME_MAX)
        self.description = _normalize_registration_field(description, DESCRIPTION_MAX)
        self.socket_dir = Path(socket_dir).expanduser() if socket_dir is not None else _default_socket_dir()
        self.socket_path = self.socket_dir / f"{self.runtime_session_id}.sock"
        # Per-session capability token. Required on {deliver} (presented by the
        # broker, which learns it at register) and {send} (presented by the CLI,
        # which reads the 0600 token file). Connection != authority.
        self._adapter_token = session_auth.generate_token()
        self.thread_id: str | None = None
        self.participant_id: str | None = None
        self._dedupe = _DedupeLRU(DEDUPE_LRU_SIZE)
        self._state_lock = threading.Lock()
        self._server: _ThreadedUnixServer | None = None
        self._server_thread: threading.Thread | None = None
        self._bus_client: Any | None = None

    def _resolve_thread_id(self) -> str:
        record = self._load_session_thread(self.runtime_session_id)
        if not isinstance(record, dict):
            raise RuntimeError(
                f"no thread record found for runtime_session_id={self.runtime_session_id!r}"
            )
        thread_id = record.get("thread_id")
        if not isinstance(thread_id, str) or not thread_id.strip():
            raise RuntimeError(
                f"thread record missing thread_id for runtime_session_id={self.runtime_session_id!r}"
            )
        self.thread_id = thread_id.strip()
        return self.thread_id

    def handle_request(self, request: Any) -> dict[str, str]:
        if not isinstance(request, dict):
            return {"status": "rejected", "reason": "unknown_type"}

        if request.get("type") == "send":
            return self.handle_send_request(request)

        envelope = request.get("envelope")
        if request.get("type") != "deliver" or not isinstance(envelope, dict):
            return {"status": "rejected", "reason": "unknown_type"}

        # Connection != authority: only the broker (holding the token from
        # register) may drive the autosubmit-into-Codex path. Reject an
        # unauthenticated same-uid peer before any turn is injected.
        if not session_auth.timing_safe_equal(request.get("auth"), self._adapter_token):
            return {"status": "rejected", "reason": "unauthorized"}

        envelope_id = envelope.get("id")
        if not isinstance(envelope_id, str) or not envelope_id or len(envelope_id) > 128:
            return {"status": "rejected", "reason": "missing_id"}

        body = envelope.get("body")
        if not isinstance(body, str) or not body:
            return {"status": "rejected", "reason": "missing_body"}

        if self._dedupe.seen_before(envelope_id):
            return {"status": "rejected", "reason": "duplicate"}

        rendered_text = render_envelope(envelope)
        try:
            # Deliver into the bound TTY first so the already-open Codex TUI
            # receives the turn on its live surface. Fall back to the policy-
            # preserving app-server helper when no live terminal injection path
            # is available.
            self._send_terminal_input(
                text=rendered_text,
                source="bus",
                runtime_session_id=self.runtime_session_id,
            )
            return {"status": "accepted"}
        except Exception:
            pass

        try:
            # Note: the current P3 helper `send_synthetic_turn_for_bus` waits
            # for `turn.completed`, which means this call can block 10-30+ s
            # on a real Codex thread. The broker's FORWARD_TIMEOUT_MS was
            # bumped to 30 s to accommodate that. A future P3 refinement to
            # accept `wait_for_completion=False` would let this return as soon
            # as app-server accepts the `turn/start`, matching the "accepted
            # for injection" semantics DESIGN.md §Delivery result defines.
            self._send_turn_for_bus(
                thread_id=self.thread_id or self._resolve_thread_id(),
                text=rendered_text,
                runtime_session_id=self.runtime_session_id,
                wait_timeout=WAIT_TIMEOUT_SEC,
            )
            return {"status": "accepted"}
        except self._policy_error_cls as error:
            return {"status": "rejected", "reason": str(error)[:80]}
        except Exception as error:  # pragma: no cover - exercised in integration if needed
            return {
                "status": "rejected",
                "reason": f"internal_error:{_brief_message(error)}",
            }

    def handle_send_request(self, request: dict[str, Any]) -> dict[str, Any]:
        # Emitting as this participant is privileged (sends under our lease,
        # stamped with our identity). Require the capability token so a same-uid
        # peer cannot impersonate this session over its own socket.
        if not session_auth.timing_safe_equal(request.get("auth"), self._adapter_token):
            return {"status": "rejected", "reason": "unauthorized"}
        bus_client = self._bus_client
        if bus_client is None or not self.participant_id:
            return {"status": "rejected", "reason": "not_registered"}

        to = request.get("to")
        if not isinstance(to, str) or not to.strip():
            return {"status": "rejected", "reason": "missing_to"}
        body = request.get("body")
        if not isinstance(body, str) or not body:
            return {"status": "rejected", "reason": "missing_body"}

        try:
            return bus_client.send(
                to.strip(),
                body,
                id=request.get("id") if isinstance(request.get("id"), str) else None,
                reply_to=request.get("reply_to")
                if isinstance(request.get("reply_to"), str)
                else None,
                conversation_id=request.get("conversation_id")
                if isinstance(request.get("conversation_id"), str)
                else None,
                source=request.get("source")
                if isinstance(request.get("source"), str) and request.get("source")
                else "participant",
            )
        except Exception as error:
            return {"status": "rejected", "reason": f"send_error:{_brief_message(error)}"}

    def start(self) -> dict[str, str | None]:
        with self._state_lock:
            if self._server is not None:
                return {
                    "socketPath": str(self.socket_path),
                    "participantId": self.participant_id,
                }

            self._resolve_thread_id()
            _ensure_socket_dir(self.socket_dir)
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass

            server = _ThreadedUnixServer(str(self.socket_path), _AdapterRequestHandler)
            server.adapter = self  # type: ignore[attr-defined]
            os.chmod(self.socket_path, 0o600)

            # Publish the capability token (0600) for the CLI's {send}, before
            # register hands the same token to the broker for {deliver}.
            session_auth.write_token(self.socket_dir, self.runtime_session_id, self._adapter_token)

            server_thread = threading.Thread(
                target=server.serve_forever,
                name=f"codex-bus-adapter-{self.runtime_session_id}",
                daemon=True,
            )
            server_thread.start()

            bus_client = self._bus_client_factory()
            try:
                registration = bus_client.register({
                    "kind": "codex",
                    "sessionId": self.runtime_session_id,
                    "project": self.project,
                    "cwd": self.cwd,
                    "displayName": self.display_name,
                    "description": self.description,
                    "delivery": {
                        "adapter": "codex-app-server",
                        "socketPath": str(self.socket_path),
                        "threadId": self.thread_id,
                        "adapterToken": self._adapter_token,
                    },
                })
            except Exception:
                server.shutdown()
                server.server_close()
                server_thread.join(timeout=2)
                try:
                    self.socket_path.unlink()
                except FileNotFoundError:
                    pass
                session_auth.remove_token(self.socket_dir, self.runtime_session_id)
                raise

            self._server = server
            self._server_thread = server_thread
            self._bus_client = bus_client
            self.participant_id = getattr(bus_client, "participant_id", None) or registration.get(
                "participantId"
            )
            return {
                "socketPath": str(self.socket_path),
                "participantId": self.participant_id,
            }

    def stop(self) -> None:
        with self._state_lock:
            bus_client = self._bus_client
            server = self._server
            server_thread = self._server_thread
            self._bus_client = None
            self._server = None
            self._server_thread = None
            self.participant_id = None

        if bus_client is not None:
            try:
                bus_client.unregister()
            except Exception:
                pass

        if server is not None:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                server.server_close()
            except Exception:
                pass

        if (
            server_thread is not None
            and server_thread.is_alive()
            and server_thread is not threading.current_thread()
        ):
            server_thread.join(timeout=2)

        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass
        session_auth.remove_token(self.socket_dir, self.runtime_session_id)

    def wait(self) -> None:
        server_thread = self._server_thread
        if server_thread is not None:
            server_thread.join()


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        runtime_session_id = _resolve_runtime_session_id(args, dict(os.environ))
        adapter = CodexAppServerAdapter(runtime_session_id=runtime_session_id)
        adapter.start()
    except Exception as error:
        _stderr(str(error))
        return 2

    def _handle_shutdown(_signum: int, _frame: Any) -> None:
        adapter.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    try:
        adapter.wait()
    finally:
        adapter.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
